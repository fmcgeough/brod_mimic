defmodule BrodMimic.GroupSubscriber do
  @moduledoc """
    A group subscriber is a GenServer which subscribes to partition consumers
    (poller) and calls the user-defined callback functions for message
    processing.

    An overview of what it does behind the scene:

    * Start a consumer group coordinator to manage the consumer group states,
      see {@link brod_group_coordinator:start_link/6}
    * Start (if not already started) topic-consumers (pollers) and subscribe
      to the partition workers when group assignment is received from the
      group leader, see {@link brod:start_consumer/3}
    * Call `CallbackModule:handle_message/4' when messages are received from
      the partition consumers.
    * Send acknowledged offsets to group coordinator which will be committed
      to kafka periodically.
  """
  use GenServer

  import Record, only: [defrecord: 2, extract: 2]

  alias BrodMimic.Brod
  alias BrodMimic.GroupCoordinator, as: BrodGroupCoordinator
  alias BrodMimic.Utils, as: BrodUtils

  # -behaviour(brod_group_member).

  # -export([ ack/4
  #         , ack/5
  #         , commit/1
  #         , commit/4
  #         , start_link/7
  #         , start_link/8
  #         , stop/1
  #         ]).

  # %% callbacks for brod_group_coordinator
  # -export([ get_committed_offsets/2
  #         , assignments_received/4
  #         , assignments_revoked/1
  #         , assign_partitions/3
  #         , user_data/1
  #         ]).

  # -export([ code_change/3
  #         , handle_call/3
  #         , handle_cast/2
  #         , handle_info/2
  #         , init/1
  #         , terminate/2
  #         ]).

  @type cb_state() :: term()
  @type member_id() :: Brod.group_member_id()

  # Initialize the callback module s state.
  @callback init(Brod.group_id(), term()) :: {:ok, cb_state()}

  @doc """
    Handle a message. Return one of:

     * `{:ok, new_callback_state}`:
       The subscriber has received the message for processing async-ly.
       It should call brod_group_subscriber:ack/4 to acknowledge later.

     * `{:ok, ack, new_callback_state}`
       The subscriber has completed processing the message.

     * `{:ok, :ack_no_commit, new_callback_state}`
       The subscriber has completed processing the message, but it
       is not ready to commit offset yet. It should call
       brod_group_subscriber:commit/4 later.

     While this callback function is being evaluated, the fetch-ahead
     partition-consumers are fetching more messages behind the scene
     unless prefetch_count and prefetch_bytes are set to 0 in consumer config.
  """
  @callback handle_message(
              Brod.topic(),
              Brod.partition(),
              Brod.message() | Brod.message_set(),
              cb_state()
            ) ::
              {:ok, cb_state()}
              | {:ok, :ack, cb_state()}
              | {:ok, :ack_no_commit, cb_state()}

  @doc """
    This callback is called only when subscriber is to commit offsets locally
    instead of kafka.
    Return {ok, Offsets, cb_state()} where Offsets can be [],
    or only the ones that are found in e.g. local storage or database.
    For the topic-partitions which have no committed offset found,
    the consumer will take 'begin_offset' in consumer config as the start point
    of data stream. If 'begin_offset' is not found in consumer config, the
    default value -1 (latest) is used.

    commented out as it's an optional callback

  ```
    @callback get_committed_offsets(
      Brod.group_id(),
      [{Brod.topic(), Brod.partition()}], cb_state()) ::
      {:ok, [{{Brod.topic(), Brod.partition()}, Brod.offset()}], cb_state()}
  ```
   This function is called only when 'partition_assignment_strategy' is
   'callback_implemented' in group config.
   The first element in the group member list is ensured to be the group leader.
  commented out as it's an optional callback

  ```
    @callback assign_partitions([Brod.group_member()],[{Brod.topic(), Brod.partition()}], cb_state()) :: [{Brod.group_member_id(), [Brod.partition_assignment()]}]
  """
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  defrecord(:consumer,
    topic_partition: {:undefined, :undefined},
    consumer_pid: :undefined,
    consumer_mref: :undefined,
    begin_offset: :undefined,
    acked_offset: :undefined,
    last_offset: :undefined
  )

  @type consumer() ::
          record(
            :consumer,
            topic_partition: {Brod.topic(), Brod.partition()},
            consumer_pid: :undefined | pid() | {:down, String.t(), any()},
            consumer_mref: :undefined | reference(),
            begin_offset: :undefined | Brod.offset(),
            acked_offset: :undefined | Brod.offset(),
            last_offset: :undefined | Brod.offset()
          )

  @type ack_ref() :: {Brod.topic(), Brod.partition(), Brod.offset()}

  defrecord(:brod_received_assignment, topic: nil, partition: nil, begin_offset: :undefined)

  @type brod_received_assignment() ::
          record(:brod_received_assignment,
            topic: Brod.topic(),
            partition: Brod.partition(),
            begin_offset: :undefined | Brod.offset()
          )

  defrecord(:kafka_message_set, topic: nil, partition: nil, high_wm_offset: 0, messages: [])

  @type kafka_message_set() ::
          record(:kafka_message_set,
            topic: Brod.topic(),
            partition: Brod.partition(),
            high_wm_offset: integer(),
            messages: [Brod.message()] | :kpro.incomplete_batch()
          )

  defrecord(:state,
    client: nil,
    client_mref: nil,
    group_id: nil,
    member_id: :undefined,
    generation_id: :undefined,
    coordinator: nil,
    consumers: [],
    consumer_config: nil,
    is_blocked: false,
    subscribe_tref: :undefined,
    cb_module: nil,
    cb_state: nil,
    message_type: nil
  )

  @type state() ::
          record(
            :state,
            client: Brod.client(),
            client_mref: reference(),
            group_id: Brod.group_id(),
            member_id: :undefined | member_id(),
            generation_id: :undefined | Brod.group_generation_id(),
            coordinator: pid(),
            consumers: [consumer()],
            consumer_config: Brod.consumer_config(),
            is_blocked: boolean(),
            subscribe_tref: :undefined | reference(),
            cb_module: module(),
            cb_state: cb_state(),
            message_type: :message | :message_set
          )

  require Logger

  # delay 2 seconds retry the failed subscription to partition consumer process
  @resubcribe_delay 2000
  @lo_cmd_subscribe_partitions '$subscribe_partitions'

  @spec start_link(
          Brod.client(),
          Brod.group_id(),
          [Brod.topic()],
          Brod.group_config(),
          Brod.consumer_config(),
          module(),
          term()
        ) :: {:ok, pid()} | {:error, any()}
  def start_link(client, group_id, topics, group_config, consumer_config, cb_module, cb_init_arg) do
    start_link(
      client,
      group_id,
      topics,
      group_config,
      consumer_config,
      :message,
      cb_module,
      cb_init_arg
    )
  end

  @doc """
  Start (link) a group subscriber.

   `Client': Client ID (or pid, but not recommended) of the brod client.

   `GroupId': Consumer group ID which should be unique per kafka cluster

   `Topics': Predefined set of topic names to join the group.

     NOTE: The group leader member will collect topics from all members and
           assign all collected topic-partitions to members in the group.
           i.e. members can join with arbitrary set of topics.

   `GroupConfig': For group coordinator, see
      {@link brod_group_coordinator:start_link/6}

   `ConsumerConfig': For partition consumer, see
   {@link brod_consumer:start_link/4}

   `MessageType':
     The type of message that is going to be handled by the callback
     module. Can be either `message' or `message_set'.

   `CbModule':
     Callback module which should have the callback functions
     implemented for message processing.

   `CbInitArg':
     The term() that is going to be passed to `CbModule:init/2' as a
     second argument when initializing the subscriber.
   @end
  """
  @spec start_link(
          Brod.client(),
          Brod.group_id(),
          [Brod.topic()],
          Brod.group_config(),
          Brod.consumer_config(),
          :message | :message_set,
          module(),
          term()
        ) :: {:ok, pid()} | {:error, any()}
  def start_link(
        client,
        group_id,
        topics,
        group_config,
        consumer_config,
        message_type,
        cb_module,
        cb_init_arg
      ) do
    args =
      {client, group_id, topics, group_config, consumer_config, message_type, cb_module,
       cb_init_arg}

    GenServer.start_link(__MODULE__, args, [])
  end

  @doc """
  Stop group subscriber, wait for pid `DOWN' before return.
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    mref = Process.monitor(pid)
    :ok = GenServer.cast(pid, :stop)

    receive do
      {:DOWN, ^mref, :process, _pid, _reason} -> :ok
    end
  end

  @doc """
  Acknowledge and commit an offset.

  The subscriber may ack a later (greater) offset which will be considered
  as multi-acking the earlier (smaller) offsets. This also means that
  disordered acks may overwrite offset commits and lead to unnecessary
  message re-delivery in case of restart.
  """
  @spec ack(pid(), Brod.topic(), Brod.partition(), Brod.offset()) :: :ok
  def ack(pid, topic, partition, offset) do
    ack(pid, topic, partition, offset, true)
  end

  @doc """
  Acknowledge an offset.

    This call may or may not commit group subscriber offset depending on
    the value of `Commit' argument
  """
  @spec ack(pid(), Brod.topic(), Brod.partition(), Brod.offset(), boolean()) :: :ok
  def ack(pid, topic, partition, offset, commit) do
    GenServer.cast(pid, {:ack, topic, partition, offset, commit})
  end

  @doc """
    Commit all acked offsets. NOTE: This is an async call.
  """
  @spec commit(pid()) :: :ok
  def commit(pid) do
    GenServer.cast(pid, :commit_offsets)
  end

  @doc """
  Commit offset for a topic. This is an asynchronous call
  """
  @spec commit(pid(), Brod.topic(), Brod.partition(), Brod.offset()) :: :ok
  def commit(pid, topic, partition, offset) do
    GenServer.cast(pid, {:commit_offset, topic, partition, offset})
  end

  # user_data(_Pid) -> <<>>.

  ### APIs for group coordinator ===============================================

  @doc """
  Called by group coordinator when there is new assignment received.
  """
  @spec assignments_received(pid(), member_id(), integer(), Brod.received_assignments()) :: :ok
  def assignments_received(pid, member_id, generation_id, topic_assignments) do
    GenServer.cast(pid, {:new_assignments, member_id, generation_id, topic_assignments})
  end

  @doc """
  Called by group coordinator before re-joining the consumer group.
  """
  @spec assignments_revoked(pid()) :: :ok
  def assignments_revoked(pid) do
    GenServer.call(pid, :unsubscribe_all_partitions, :infinity)
  end

  @doc """
  This function is called only when `partition_assignment_strategy'
  is set for `callback_implemented' in group config.
  """
  @spec assign_partitions(pid(), [Brod.group_member()], [{Brod.topic(), Brod.partition()}]) :: [
          {member_id(), [Brod.partition_assignment()]}
        ]
  def assign_partitions(pid, members, topic_partition_list) do
    call = {:assign_partitions, members, topic_partition_list}
    GenServer.call(pid, call, :infinity)
  end

  @doc """
  Called by group coordinator when initializing the assignments
  for subscriber

   NOTE: This function is called only when `offset_commit_policy' is set to
         `consumer_managed' in group config.

   NOTE: The committed offsets should be the offsets for successfully processed
         (acknowledged) messages, not the `begin_offset' to start fetching from.
  """
  @spec get_committed_offsets(pid(), [{Brod.topic(), Brod.partition()}]) ::
          {:ok, [{{Brod.topic(), Brod.partition()}, Brod.offset()}]}
  def get_committed_offsets(pid, topic_partitions) do
    GenServer.call(pid, {:get_committed_offsets, topic_partitions}, :infinity)
  end

  #### gen_server callbacks =====================================================

  def init(
        {client, group_id, topics, group_config, consumer_config, message_type, cb_module,
         cb_init_arg}
      ) do
    :ok = BrodUtils.assert_client(client)
    :ok = BrodUtils.assert_group_id(group_id)
    :ok = BrodUtils.assert_topics(topics)
    {:ok, cb_state} = cb_module.init(group_id, cb_init_arg)

    {:ok, pid} =
      BrodGroupCoordinator.start_link(client, group_id, topics, group_config, __MODULE__, self())

    state =
      state(
        client: client,
        client_mref: Process.monitor(client),
        group_id: group_id,
        coordinator: pid,
        consumer_config: consumer_config,
        cb_module: cb_module,
        cb_state: cb_state,
        message_type: message_type
      )

    {:ok, state}
  end

  def handle_info({_consumer_pid, kafka_message_set() = msg_set}, state0) do
    state = handle_consumer_delivery(msg_set, state0)
    {:noreply, state}
  end

  def handle_info({:DOWN, mref, _process, _pid, _reason}, state(client_mref: mref) = state) do
    # restart, my supervisor should restart me
    # brod_client DOWN reason is discarded as it should have logged
    # in its crash log
    {:stop, :client_down, state}
  end

  def handle_info({:DOWN, _mref, _process, pid, reason}, state(consumers: consumers) = state) do
    case get_consumer(pid, consumers) do
      false ->
        {:noreply, state}

      consumer ->
        new_consumer = consumer(consumer, consumer_pid: down(reason), consumer_mref: :undefined)
        new_consumers = put_consumer(new_consumer, consumers)
        new_state = state(state, consumers: new_consumers)
        {:noreply, new_state}
    end
  end

  def handle_info(@lo_cmd_subscribe_partitions, state) do
    new_state =
      case state.is_blocked do
        true ->
          state

        false ->
          {:ok, state} = subscribe_partitions(state)
          state
      end

    tref = start_subscribe_timer(:undefined, @resubcribe_delay)
    {:noreply, state(new_state, subscribe_tref: tref)}
  end

  def handle_info(
        info_msg,
        state(group_id: group_id, member_id: member_id, generation_id: generation_id) = state
      ) do
    details = %{
      group_id: group_id,
      member_id: member_id,
      generation_id: generation_id,
      pid: self()
    }

    Logger.info("group subscriber (#{inspect(details)}). discarded message: #{inspect(info_msg)}")
    {:noreply, state}
  end

  def handle_call(
        {:get_committed_offsets, topic_partitions},
        _from,
        state(group_id: group_id, cb_module: cb_module, cb_state: cb_state) = state
      ) do
    case cb_module.get_committed_offsets(group_id, topic_partitions, cb_state) do
      {:ok, result, new_cb_state} ->
        new_state = state(state, cb_state: new_cb_state)
        {:reply, {:ok, result}, new_state}

      unknown ->
        :erlang.error({:bad_return_value, {cb_module, :get_committed_offsets, unknown}})
    end
  end

  def handle_call(
        {:assign_partitions, members, topic_partitions},
        _from,
        state(cb_module: cb_module, cb_state: cb_state) = state
      ) do
    case cb_module.assign_partitions(members, topic_partitions, cb_state) do
      {new_cb_state, result} ->
        {:reply, result, state(state, cb_state: new_cb_state)}

      ## Returning an updated cb_state is optional and clients that implemented
      ## brod prior to version 3.7.1 need this backwards compatibly case clause
      result when is_list(result) ->
        {:reply, result, state}
    end
  end

  def handle_call(:unsubscribe_all_partitions, _from, state(consumers: consumers) = state) do
    Enum.each(consumers, fn consumer(consumer_pid: consumer_pid, consumer_mref: consumer_mref) ->
      case is_pid(consumer_pid) do
        true ->
          _ = Brod.unsubscribe(consumer_pid, self())
          _ = Process.demonitor(consumer_mref, [:flush])

        false ->
          :ok
      end
    end)

    {:reply, :ok, state(state, consumers: [], is_blocked: true)}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def handle_cast({:ack, topic, partition, offset, commit}, state) do
    ack_ref = {topic, partition, offset}
    new_state = handle_ack(ack_ref, state, commit)
    {:noreply, new_state}
  end

  def handle_cast(:commit_offsets, state(coordinator: coordinator) = state) do
    :ok = BrodGroupCoordinator.commit_offsets(coordinator)
    {:noreply, state}
  end

  def handle_cast({:commit_offset, topic, partition, offset, offset}, state) do
    state(coordinator: coordinator, generation_id: generation_id) = state
    do_commit_ack(coordinator, generation_id, topic, partition, offset)
    {:noreply, state}
  end

  def handle_cast(
        {:new_assignments, member_id, generation_id, assignments},
        state(client: client, consumer_config: consumer_config, subscribe_tref: tref) = state
      ) do
    all_topics = Enum.map(assignments, fn brod_received_assignment(topic: topic) -> topic end)

    all_topics
    |> :lists.usort()
    |> Enum.each(fn topic ->
      :ok = Brod.start_consumer(client, topic, consumer_config)
    end)

    consumers =
      Enum.map(assignments, fn assignment ->
        case assignment do
          consumer(
            topic_partition: {_topic, _partition},
            consumer_pid: :undefined,
            begin_offset: _begin_offset,
            acked_offset: :undefined
          ) ->
            nil

          brod_received_assignment(
            topic: _topic,
            partition: _partition,
            begin_offset: _begin_offset
          ) ->
            nil
        end
      end)

    new_state =
      state(state,
        consumers: consumers,
        is_blocked: false,
        member_id: member_id,
        generation_id: generation_id,
        subscribe_tref: start_subscribe_timer(tref, 0)
      )

    {:noreply, new_state}
  end

  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_cast(_cast, state) do
    {:noreply, state}
  end

  def code_change(_old_vsn, state, _extra) do
    {:ok, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  # @ %%%_* Internal Functions =======================================================

  def handle_consumer_delivery(
        kafka_message_set(topic: topic, partition: partition, messages: messages) = msg_set,
        state(message_type: message_type, consumers: consumers0) = state0
      ) do
    case get_consumer({topic, partition}, consumers0) do
      consumer() = c ->
        consumers = update_last_offset(messages, c, consumers0)
        state = state(state0, consumers: consumers)

        case message_type do
          :message -> handle_messages(topic, partition, messages, state)
          :message_set -> handle_message_set(msg_set, state)
        end

      false ->
        state0
    end
  end

  def update_last_offset(messages, consumer0, consumers) do
    # brod_consumer never delivers empty message set, lists:last is safe
    kafka_message(offset: last_offset) = :lists.last(messages)
    consumer = consumer(consumer0, last_offset: last_offset)
    put_consumer(consumer, consumers)
  end

  @spec start_subscribe_timer(:undefined | reference(), timeout()) :: reference()
  def start_subscribe_timer(:undefined, delay) do
    Process.send_after(self(), @lo_cmd_subscribe_partitions, delay)
  end

  def start_subscribe_timer(ref, _delay) when is_reference(ref) do
    # The old timer is not expired, keep waiting
    # A bit delay on subscribing to brod_consumer is fine
    ref
  end

  def handle_message_set(message_set, state) do
    kafka_message_set(topic: topic, partition: partition, messages: messages) = message_set
    state(cb_module: cb_module, cb_state: cb_state) = state

    {ack_now, commit_now, new_cb_state} =
      case cb_module.handle_message(topic, partition, message_set, cb_state) do
        {:ok, new_cb_state_} ->
          {false, false, new_cb_state_}

        {:ok, :ack, new_cb_state_} ->
          {true, true, new_cb_state_}

        {:ok, :ack_no_commit, new_cb_state_} ->
          {true, false, new_cb_state_}

        unknown ->
          :erlang.error({:bad_return_value, {cb_module, :handle_message, unknown}})
      end

    state1 = state(state, cb_state: new_cb_state)

    case ack_now do
      true ->
        last_message = :lists.last(messages)
        # kafka_message.offset
        last_offset = kafka_message(last_message, :offset)
        ack_ref = {topic, partition, last_offset}
        handle_ack(ack_ref, state1, commit_now)

      false ->
        state1
    end
  end

  def handle_messages(_topic, _partition, [], state) do
    state
  end

  def handle_messages(topic, partition, [msg | rest], state) do
    kafka_message(offset: offset) = msg
    state(cb_module: cb_module, cb_state: cb_state) = state
    ack_ref = {topic, partition, offset}

    {ack_now, commit_now, new_cb_state} =
      case cb_module.handle_message(topic, partition, msg, cb_state) do
        {:ok, new_cb_state_} ->
          {false, false, new_cb_state_}

        {:ok, :ack, new_cb_state_} ->
          {true, true, new_cb_state_}

        {:ok, :ack_no_commit, new_cb_state_} ->
          {true, false, new_cb_state_}

        unknown ->
          :erlang.error({:bad_return_value, {cb_module, :handle_message, unknown}})
      end

    state1 = state(state, cb_state: new_cb_state)

    new_state =
      case ack_now do
        true -> handle_ack(ack_ref, state1, commit_now)
        false -> state1
      end

    handle_messages(topic, partition, rest, new_state)
  end

  @spec handle_ack(ack_ref(), state(), boolean()) :: state()
  def handle_ack(
        ack_ref,
        state(generation_id: generation_id, consumers: consumers, coordinator: coordinator) =
          state,
        commit_now
      ) do
    {topic, partition, offset} = ack_ref

    case get_consumer({topic, partition}, consumers) do
      consumer(consumer_pid: consumer_pid) = consumer when commit_now ->
        :ok = consume_ack(consumer_pid, offset)
        :ok = do_commit_ack(coordinator, generation_id, topic, partition, offset)
        new_consumer = consumer(consumer, acked_offset: offset)
        new_consumers = put_consumer(new_consumer, consumers)
        state(state, consumers: new_consumers)

      consumer(consumer_pid: consumer_pid) ->
        :ok = consume_ack(consumer_pid, offset)
        state

      false ->
        # Stale async-ack, discard.
        state
    end
  end

  # Tell consumer process to fetch more (if pre-fetch count/byte limit allows).
  def consume_ack(pid, offset) do
    is_pid(pid) and Brod.consume_ack(pid, offset)
    :ok
  end

  # Send an async message to group coordinator for offset commit.
  def do_commit_ack(pid, generation_id, topic, partition, offset) do
    :ok = BrodGroupCoordinator.ack(pid, generation_id, topic, partition, offset)
  end

  def subscribe_partitions(state(client: client, consumers: consumers0) = state) do
    consumers = Enum.map(consumers0, fn c -> subscribe_partition(client, c) end)
    {:ok, state(state, consumers: consumers)}
  end

  def subscribe_partition(client, consumer) do
    consumer(
      topic_partition: {topic, partition},
      consumer_pid: pid,
      begin_offset: begin_offset0,
      acked_offset: acked_offset,
      last_offset: last_offset
    ) = consumer

    case BrodUtils.is_pid_alive(pid) do
      true ->
        consumer

      false when acked_offset != last_offset and last_offset != :undefined ->
        # The last fetched offset is not yet acked,
        # do not re-subscribe now to keep it simple and slow.
        # Otherwise if we subscribe with {begin_offset, LastOffset + 1}
        # we may exceed pre-fetch window size.
        consumer

      false ->
        # fetch from the last acked offset + 1
        # otherwise fetch from the assigned begin_offset
        begin_offset =
          case acked_offset do
            :undefined -> begin_offset0
            n when n >= 0 -> n + 1
          end

        options =
          case begin_offset == :undefined do
            # fetch from 'begin_offset' in consumer config
            true -> []
            false -> [{:begin_offset, begin_offset}]
          end

        case Brod.subscribe(client, self(), topic, partition, options) do
          {:ok, consumer_pid} ->
            mref = Process.monitor(consumer_pid)
            consumer(consumer, consumer_pid: consumer_pid, consumer_mref: mref)

          {:error, reason} ->
            consumer(consumer, consumer_pid: down(reason), consumer_mref: :undefined)
        end
    end
  end

  defp get_consumer(pid, consumers) when is_pid(pid) do
    :lists.keyfind(pid, consumer(:consumer_pid), consumers)
  end

  defp get_consumer({_, _} = tp, consumers) do
    :lists.keyfind(tp, consumer(:topic_partition), consumers)
  end

  defp put_consumer(consumer(topic_partition: tp) = c, consumers) do
    :lists.keyreplace(tp, consumer(:topic_partition), consumers, c)
  end

  defp down(reason) do
    {:down, DateTime.utc_now() |> DateTime.to_string(), reason}
  end
end
