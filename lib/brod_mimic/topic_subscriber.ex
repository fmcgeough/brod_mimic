defmodule BrodMimic.TopicSubscriber do
  @moduledoc """
  A topic subscriber is a GenServer which subscribes to all or a given set
  of partition consumers (pollers) of a given topic and calls the user-defined
  callback functions for message processing.

  Callbacks are documented in the source code of this module.
  """
  use BrodMimic.Macros
  use GenServer

  import Record, only: [defrecordp: 2]

  alias BrodMimic.Brod
  alias BrodMimic.Utils, as: BrodUtils

  defrecordp(:cbm_init_data,
    committed_offsets: :undefined,
    cb_fun: :undefined,
    cb_data: :undefined
  )

  defrecordp(:consumer,
    partition: :undefined,
    consumer_pid: :undefined,
    consumer_mref: :undefined,
    acked_offset: :undefined,
    last_offset: :undefined
  )

  defrecordp(:state,
    client: :undefined,
    client_mref: :undefined,
    topic: :undefined,
    consumers: [],
    cb_module: :undefined,
    cb_state: :undefined,
    message_type: :undefined
  )

  @type committed_offsets() :: [{partition(), offset()}]
  @type cb_state() :: term()
  @type cb_ret() :: {:ok, cb_state()} | {:ok, :ack, cb_state()}
  @type cb_fun() :: (partition(), Brod.message() | Brod.message_set(), cb_state() -> cb_ret())
  @type consumer() ::
          record(:consumer,
            partition: partition(),
            consumer_pid: :undefined | pid() | {:down, String.t(), any()},
            consumer_mref: :undefined | reference(),
            acked_offset: :undefined | Brod.offset(),
            last_offset: :undefined | Brod.offset()
          )

  @typedoc """
  Type definition for the `Record` used for `BrodMimic.TopicSubscriber` GenServer state
  """
  @type state() ::
          record(:state,
            client: Brod.client(),
            client_mref: reference(),
            topic: topic(),
            consumers: [consumer()],
            cb_module: module(),
            cb_state: cb_state(),
            message_type: :message | :message_set
          )

  @typedoc """
  Configuration for a topic subscriber

  ## Possible Keys

  - `client`: Client ID (or pid, but not recommended) of the brod client.
    Mandatory
  - `topic`: Topic to consume from. Mandatory
  - `cb_module`: Callback module which should have the callback functions
    implemented for message processing. Mandatory
  - `consumer_config`: For partition consumer, See
    BrodMimic.TopicSubscriber.start_link/6`. Optional, defaults to `[]`
  - `message_type`: The type of message that is going to be handled by the
    callback module. Can be either `:message` or `:message_set`. Optional,
    defaults to `:message_set`
  - `init_data`: The `term()' that is going to be passed to `CbModule:init/2`
    when initializing the subscriber. Optional, defaults to `:undefined`
  - `partitions`: List of partitions to consume from, or atom `:all`. Optional,
    defaults to `:all`
  """
  @type topic_subscriber_config() ::
          %{
            required(:client) => client(),
            required(:topic) => topic(),
            required(:cb_module) => module(),
            optional(:init_data) => term(),
            optional(:message_type) => :message | :message_set,
            optional(:consumer_config) => Brod.consumer_config(),
            optional(:partitions) => :all | [partition()]
          }

  # behaviour callbacks ======================================================

  @doc """
  Initialize the callback modules state.

  Return `{ok, committed_offsets, cb_state}` where `commited_offset` is the "last
  seen" before start/restart offsets of each topic in a tuple list The offset+1
  of each partition will be used as the start point when fetching messages from
  kafka.

  OBS: If there is no offset committed before for certain (or all) partitions
       e.g. CommittedOffsets = [], the consumer will use `latest` by default, or
       `begin_offset` in consumer config (if found) to start fetching. cb_state
  is the user's looping state for message processing.
  """
  @callback init(topic(), term()) :: {:ok, committed_offsets(), cb_state()}

  @doc """
  Handle a message. Return one of:

   `{:ok, new_callback_state}`
     The subscriber has received the message for processing async-ly.
     It should call brod_topic_subscriber:ack/3 to acknowledge later.

   `{:ok, ack, new_callback_state}`
     The subscriber has completed processing the message

   NOTE: While this callback function is being evaluated, the fetch-ahead
         partition-consumers are polling for more messages behind the scene
         unless prefetch_count and prefetch_bytes are set to 0 in consumer
         config.
  """
  @callback handle_message(
              partition(),
              Brod.message() | message_set(),
              cb_state()
            ) :: cb_ret()

  @doc """
  This callback is called before stopping the subscriber
  """
  @callback terminate(any(), cb_state()) :: any()

  @optional_callbacks [terminate: 2]

  @doc """
  Equivalent to `start_link(client, topic, partitions, consumer_config, :message, cb_module, cb_init_arg)`
  """
  @deprecated "Please use `start_link/1` instead"
  @spec start_link(client(), topic(), :all | [partition()], Brod.consumer_config(), module(), term()) ::
          {:ok, pid()} | {:error, any()}
  def start_link(client, topic, partitions, consumer_config, cb_module, cb_init_arg) do
    args = %{
      client: client,
      topic: topic,
      partitions: partitions,
      consumer_config: consumer_config,
      message_type: :message,
      cb_module: cb_module,
      init_data: cb_init_arg
    }

    start_link(args)
  end

  @doc """
  Start (link) a topic subscriber which receives and processes the messages or
  message sets from the given partition set. Use atom `:all` to subscribe to all
  partitions. Messages are handled by calling `CbModule:handle_message`
  """
  @deprecated "Please use `start_link/1` instead"
  @spec start_link(
          client(),
          topic(),
          :all | [partition()],
          Brod.consumer_config(),
          :message | :message_set,
          module(),
          term()
        ) :: {:ok, pid()} | {:error, any()}
  def start_link(client, topic, partitions, consumer_config, message_type, cb_module, cb_init_arg) do
    args = %{
      client: client,
      topic: topic,
      partitions: partitions,
      consumer_config: consumer_config,
      message_type: message_type,
      cb_module: cb_module,
      init_data: cb_init_arg
    }

    start_link(args)
  end

  @doc """
   Start (link) a topic subscriber which receives and processes the
  messages from the given partition set. Use atom `:all` to subscribe to all
  partitions. Messages are handled by calling the callback function.

  NOTE: `committed_offsets` are the offsets for the messages that have
  been successfully processed (acknowledged), not the begin-offset
  to start fetching from.
  """
  @deprecated "Please use `start_link/1` instead"
  @spec start_link(
          client(),
          topic(),
          :all | [partition()],
          Brod.consumer_config(),
          committed_offsets(),
          :message | :message_set,
          cb_fun(),
          cb_state()
        ) :: {:ok, pid()} | {:error, any()}
  def start_link(
        client,
        topic,
        partitions,
        consumer_config,
        committed_offsets,
        message_type,
        cb_fun,
        cb_initial_state
      ) do
    init_data = cbm_init_data(committed_offsets: committed_offsets, cb_fun: cb_fun, cb_data: cb_initial_state)

    args = %{
      client: client,
      topic: topic,
      partitions: partitions,
      consumer_config: consumer_config,
      message_type: message_type,
      cb_module: :brod_topic_subscriber_cb_fun,
      init_data: init_data
    }

    start_link(args)
  end

  @doc """
  Start (link) a topic subscriber which receives and processes the messages from
  a given partition set.

  See `t:topic_subscriber_config/0` for information on parameter.
  """
  @spec start_link(topic_subscriber_config()) :: {:ok, pid()} | {:error, any()}
  def start_link(config) do
    GenServer.start_link(BrodMimic.TopicSubscriber, config, [])
  end

  @doc """
  Stop the process
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    mref = Process.monitor(pid)
    :ok = GenServer.cast(pid, :stop)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  @doc """
  Acknowledge that message has been successfully consumed.
  """
  @spec ack(pid(), partition(), offset()) :: :ok
  def ack(pid, partition, offset) do
    GenServer.cast(pid, {:ack, partition, offset})
  end

  @impl GenServer
  def init(config) do
    defaults = %{
      message_type: :message_set,
      init_data: :undefined,
      consumer_config: [],
      partitions: :all
    }

    %{
      client: client,
      topic: topic,
      cb_module: cb_module,
      init_data: init_data,
      message_type: message_type,
      consumer_config: consumer_config,
      partitions: partitions
    } = Map.merge(defaults, config)

    {:ok, committed_offsets, cb_state} = cb_module.init(topic, init_data)

    :ok = BrodUtils.assert_client(client)
    :ok = BrodUtils.assert_topic(topic)
    send(self(), {:"$start_consumer", consumer_config, committed_offsets, partitions})

    state =
      state(
        client: client,
        client_mref: Process.monitor(client),
        topic: topic,
        cb_module: cb_module,
        cb_state: cb_state,
        message_type: message_type
      )

    {:ok, state}
  end

  @impl GenServer
  def handle_info({_consumer_pid, kafka_message_set() = msg_set}, state0) do
    state = handle_consumer_delivery(msg_set, state0)
    {:noreply, state}
  end

  def handle_info(
        {:"$start_consumer", consumer_config, committed_offsets, partitions0},
        state(client: client, topic: topic) = state
      ) do
    :ok = Brod.start_consumer(client, topic, consumer_config)

    {:ok, partitions_count} = Brod.get_partitions_count(client, topic)

    all_partitions = :lists.seq(0, partitions_count - 1)

    partitions =
      case partitions0 do
        :all ->
          all_partitions

        l when is_list(l) ->
          ps = :lists.usort(l)

          case :lists.min(ps) >= 0 and :lists.max(ps) < partitions_count do
            true ->
              ps

            false ->
              :erlang.error({:bad_partitions, partitions0, partitions_count})
          end
      end

    consumers =
      Enum.map(partitions, fn partition ->
        acked_offset =
          case :lists.keyfind(partition, 1, committed_offsets) do
            {^partition, offset} ->
              offset

            false ->
              :undefined
          end

        consumer(partition: partition, acked_offset: acked_offset)
      end)

    new_state = state(state, consumers: consumers)
    _ = send_lo_cmd(:"$subscribe_partitions")
    {:noreply, new_state}
  end

  def handle_info(:"$subscribe_partitions", state) do
    {:ok, state() = new_state} = subscribe_partitions(state)
    _ = send_lo_cmd(:"$subscribe_partitions", 2000)
    {:noreply, new_state}
  end

  def handle_info({:DOWN, mref, :process, _pid, _reason}, state(client_mref: mref) = state) do
    {:stop, :client_down, state}
  end

  def handle_info({:DOWN, _mref, :process, pid, reason}, state(consumers: consumers) = state) do
    case get_consumer(pid, consumers) do
      consumer() = c ->
        consumer =
          consumer(c,
            consumer_pid: {:down, BrodUtils.os_time_utc_str(), reason},
            consumer_mref: :undefined
          )

        new_consumers = put_consumer(consumer, consumers)
        new_state = state(state, consumers: new_consumers)
        {:noreply, new_state}

      false ->
        {:noreply, state}
    end
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  @impl GenServer
  def handle_cast({:ack, partition, offset}, state) do
    ack_ref = {partition, offset}
    new_state = handle_ack(ack_ref, state)
    {:noreply, new_state}
  end

  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_cast(_cast, state) do
    {:noreply, state}
  end

  @impl GenServer
  def code_change(_old_vsn, state, _extra) do
    {:ok, state}
  end

  @impl GenServer
  def terminate(reason, state(cb_module: cb_module, cb_state: cb_state)) do
    BrodUtils.optional_callback(cb_module, :terminate, [reason, cb_state], :ok)
    :ok
  end

  defp handle_consumer_delivery(
         kafka_message_set(topic: topic, partition: partition, messages: messages) = msg_set,
         state(topic: topic, message_type: msg_type) = state0
       ) do
    state = update_last_offset(partition, messages, state0)

    case msg_type do
      :message ->
        handle_messages(partition, messages, state)

      :message_set ->
        handle_message_set(msg_set, state)
    end
  end

  defp update_last_offset(partition, messages, state(consumers: consumers) = state) do
    last_offset = messages |> :lists.last() |> kafka_message(:offset)
    c = get_consumer(partition, consumers)
    consumer = consumer(c, last_offset: last_offset)
    state(state, consumers: put_consumer(consumer, consumers))
  end

  defp subscribe_partitions(state(client: client, topic: topic, consumers: consumers0) = state) do
    consumers =
      :lists.map(
        fn c ->
          subscribe_partition(client, topic, c)
        end,
        consumers0
      )

    {:ok, state(state, consumers: consumers)}
  end

  defp subscribe_partition(client, topic, consumer) do
    consumer(
      partition: partition,
      consumer_pid: pid,
      acked_offset: acked_offset,
      last_offset: last_offset
    ) = consumer

    case BrodUtils.is_pid_alive(pid) do
      true ->
        consumer

      false
      when acked_offset !== last_offset and last_offset !== :undefined ->
        consumer

      false ->
        options = resolve_begin_offset(acked_offset)

        case Brod.subscribe(client, self(), topic, partition, options) do
          {:ok, consumer_pid} ->
            mref = Process.monitor(consumer_pid)

            consumer(consumer, consumer_pid: consumer_pid, consumer_mref: mref)

          {:error, reason} ->
            consumer(consumer,
              consumer_pid: {:down, BrodUtils.os_time_utc_str(), reason},
              consumer_mref: :undefined
            )
        end
    end
  end

  defp resolve_begin_offset(:undefined) do
    []
  end

  defp resolve_begin_offset(offset)
       when offset === :earliest or offset === :latest or offset === -2 or offset === -1 do
    [{:begin_offset, offset}]
  end

  defp resolve_begin_offset(offset) do
    begin_offset = offset + 1
    begin_offset >= 0 or :erlang.error({:invalid_offset, offset})
    [{:begin_offset, begin_offset}]
  end

  defp handle_message_set(message_set, state) do
    kafka_message_set(partition: partition, messages: messages) = message_set
    state(cb_module: cb_module, cb_state: cb_state) = state

    {ack_now, new_cb_state} =
      case cb_module.handle_message(partition, message_set, cb_state) do
        {:ok, new_cb_state_} ->
          {false, new_cb_state_}

        {:ok, :ack, new_cb_state_} ->
          {true, new_cb_state_}
      end

    state1 = state(state, cb_state: new_cb_state)

    case ack_now do
      true ->
        last_message = :lists.last(messages)
        last_offset = kafka_message(last_message, :offset)
        ack_ref = {partition, last_offset}
        handle_ack(ack_ref, state1)

      false ->
        state1
    end
  end

  defp handle_messages(_partition, [], state) do
    state
  end

  defp handle_messages(partition, [msg | rest], state) do
    offset = kafka_message(msg, :offset)
    state(cb_module: cb_module, cb_state: cb_state) = state
    ack_ref = {partition, offset}

    {ack_now, new_cb_state} =
      case cb_module.handle_message(partition, msg, cb_state) do
        {:ok, new_cb_state_} ->
          {false, new_cb_state_}

        {:ok, :ack, new_cb_state_} ->
          {true, new_cb_state_}
      end

    state1 = state(state, cb_state: new_cb_state)

    new_state =
      case ack_now do
        true ->
          handle_ack(ack_ref, state1)

        false ->
          state1
      end

    handle_messages(partition, rest, new_state)
  end

  defp handle_ack(ack_ref, state(consumers: consumers) = state) do
    {partition, offset} = ack_ref

    consumer(consumer_pid: pid) =
      consumer =
      get_consumer(
        partition,
        consumers
      )

    :ok = consume_ack(pid, offset)
    new_consumer = consumer(consumer, acked_offset: offset)
    new_consumers = put_consumer(new_consumer, consumers)
    state(state, consumers: new_consumers)
  end

  defp get_consumer(partition, consumers)
       when is_integer(partition) do
    :lists.keyfind(partition, consumer(:partition), consumers)
  end

  defp get_consumer(pid, consumers) when is_pid(pid) do
    :lists.keyfind(pid, consumer(:consumer_pid), consumers)
  end

  defp put_consumer(consumer(partition: p) = consumer, consumers) do
    :lists.keyreplace(p, consumer(:partition), consumers, consumer)
  end

  defp consume_ack(pid, offset) do
    is_pid(pid) and Brod.consume_ack(pid, offset)
    :ok
  end

  defp send_lo_cmd(cmd) do
    send_lo_cmd(cmd, 0)
  end

  defp send_lo_cmd(cmd, 0) do
    send(self(), cmd)
  end

  defp send_lo_cmd(cmd, delay_ms) do
    Process.send_after(self(), cmd, delay_ms)
  end
end
