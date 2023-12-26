defmodule BrodMimic.GroupSubscriberv2 do
  @moduledoc """
  This module implements an improved version of `BrodMimic.GroupSubscriber` behaviour.
  The key difference is that each partition worker runs in a separate process, allowing
  parallel message processing.

  Callbacks are documented in the source code of this module.
  """
  @behaviour BrodMimic.GroupMember

  use BrodMimic.Macros

  use GenServer

  import Record, only: [defrecordp: 2]

  alias BrodMimic.Brod
  alias BrodMimic.GroupCoordinator, as: BrodGroupCoordinator
  alias BrodMimic.TopicSubscriber, as: BrodTopicSubscriber
  alias BrodMimic.Utils, as: BrodUtils

  require Logger

  @worker_crashed "group_subscriber_v2 worker crashed.~n  group_id = ~s~n  topic = ~s~n  partition = ~p~n  pid = ~p~n  reason = ~p"
  @shutting_down "Received EXIT:~p from ~p, shutting down"
  @commit_flush_failed "group_subscriber_v2 ~s failed to flush commits before termination ~p"
  @terminating_worker "Terminating worker pid=~p"

  defrecordp(:state,
    config: :undefined,
    message_type: :undefined,
    group_id: :undefined,
    coordinator: :undefined,
    generation_id: :undefined,
    workers: %{},
    committed_offsets: %{},
    cb_module: :undefined,
    cb_config: :undefined,
    client: :undefined
  )

  @type commit_fun() :: (offset() -> :ok)

  @type subscriber_config() :: %{
          required(:client) => Brod.client(),
          required(:group_id) => Brod.group_id(),
          required(:topics) => [topic()],
          required(:cb_module) => module(),
          required(:init_data) => term(),
          required(:message_type) => :message | :message_set,
          required(:consumer_config) => Brod.consumer_config(),
          required(:group_config) => Brod.group_config()
        }

  @type init_info() :: %{
          required(:group_id) => Brod.group_id(),
          required(:topic) => topic(),
          required(:partition) => partition(),
          required(:commit_fun) => commit_fun()
        }

  @type cb_config() :: term()
  @type state() :: term()
  @type member_id() :: Brod.group_member_id()
  @type reason() :: term()
  @type worker() :: pid()
  @type workers() :: %{Brod.topic_partition() => worker()}

  # Callbacks
  @doc """
  Initialize the callback module's state.
  """
  @callback init(init_info(), cb_config()) :: {:ok, state()}

  @doc """
  Callback to handle an incoming message
  """
  @callback handle_message(Brod.message(), state()) ::
              {:ok, :commit, state()} | {:ok, :ack, state()} | {:ok, state()}

  @doc """
  Get committed offset (in case it is managed by the subscriber). Optional.
  """
  @callback get_committed_offset(cb_config(), topic(), partition()) ::
              {:ok, offset() | {:begin_offset, Brod.offset_time()}} | :undefined

  @doc """
  Assign partitions (in case `partition_assignment_strategy` is set
  for `callback_implemented` in group config). Optional.
  """
  @callback assign_partitions(cb_config(), [Brod.group_member()], [Brod.topic_partition()]) :: [
              {member_id(), [Brod.partition_assignment()]}
            ]

  @doc """
  Let callback module know we are terminating. Optional.
  """
  @callback terminate(reason(), state()) :: any()
  @optional_callbacks assign_partitions: 3, get_committed_offset: 3, terminate: 2

  @doc """
  Start (link) a group subscriber.

  Possible `config` keys:

  - `:client`: Client ID (or pid, but not recommended) of the brod client.
    Mandatory
  - `:group_id`: Consumer group ID which should be unique per Kafka cluster.
    Mandatory
  - `:topics`: Predefined set of topic names to join the group. Mandatory. _The
    group leader member will collect topics from all members and assign all
    collected topic-partitions to members in the group. i.e. members can join
    with arbitrary set of topics_.
  - `:cb_module`: Callback module which should have the callback functions
    implemented for message processing. Mandatory
  - `:group_config`: For group coordinator, see
    `BrodMimic.GroupCoordinator.start_link/6`. Optional
  - `:consumer_config`: For partition consumer,
    `BrodMimic.TopicSubscriber.start_link/6`. Optional
  - `:message_type`: The type of message that is going to be handled by the
      callback module. Can be either message or message set. Optional, defaults
      to `:message`
  - `:init_data`: The `term()` that is going to be passed to `cb_module.init/2`
    when initializing the subscriber. Optional, defaults to `:undefined`
  """
  @spec start_link(subscriber_config()) :: {:ok, pid()} | {:error, any()}
  def start_link(config) do
    GenServer.start_link(BrodMimic.GroupSubscriberv2, config, [])
  end

  @doc """
  Stop group subscriber, wait for pid `:DOWN` before return
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    mref = Process.monitor(pid)
    Process.unlink(pid)
    Process.exit(pid, :shutdown)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  @doc """
  Commit offset for a topic-partition, but don't commit it to
  Kafka. This is an asynchronous call
  """
  @spec ack(pid(), topic(), partition(), offset()) :: :ok
  def ack(pid, topic, partition, offset) do
    GenServer.cast(pid, {:ack_offset, topic, partition, offset})
  end

  @doc """
  Ack offset for a topic-partition. This is an asynchronous call
  """
  @spec commit(pid(), topic(), partition(), offset()) :: :ok
  def commit(pid, topic, partition, offset) do
    GenServer.cast(pid, {:commit_offset, topic, partition, offset})
  end

  @doc """
  Returns a map from Topic-Partitions to worker PIDs for the
  given group.  Useful for health checking.  This is a synchronous
  call.
  """
  @spec get_workers(pid()) :: workers()
  def get_workers(pid) do
    get_workers(pid, :infinity)
  end

  @doc """
  Returns a map from Topic-Partitions to worker PIDs for the
  given group.  Useful for health checking.  This is a synchronous
  call. Uses the caller's supplied timeout.
  """
  @spec get_workers(pid(), timeout()) :: workers()
  def get_workers(pid, timeout) do
    GenServer.call(pid, :get_workers, timeout)
  end

  @impl BrodMimic.GroupMember
  def assignments_received(pid, member_id, generation_id, topic_assignments) do
    GenServer.cast(pid, {:new_assignments, member_id, generation_id, topic_assignments})
  end

  @impl BrodMimic.GroupMember
  def assignments_revoked(pid) do
    GenServer.call(pid, :unsubscribe_all_partitions, :infinity)
  end

  @impl BrodMimic.GroupMember
  def get_committed_offsets(pid, topic_partitions) do
    GenServer.call(pid, {:get_committed_offsets, topic_partitions}, :infinity)
  end

  @impl BrodMimic.GroupMember
  def assign_partitions(pid, members, topic_partition_list) do
    call = {:assign_partitions, members, topic_partition_list}
    GenServer.call(pid, call, :infinity)
  end

  @impl GenServer
  def init(config) do
    %{client: client, group_id: group_id, topics: topics, cb_module: cb_module} = config
    Process.flag(:trap_exit, true)
    message_type = Map.get(config, :message_type, :message_set)
    default_group_config = []
    group_config = Map.get(config, :group_config, default_group_config)
    cb_config = Map.get(config, :init_data, :undefined)
    :ok = BrodUtils.assert_client(client)
    :ok = BrodUtils.assert_group_id(group_id)
    :ok = BrodUtils.assert_topics(topics)

    {:ok, pid} =
      BrodGroupCoordinator.start_link(
        client,
        group_id,
        topics,
        group_config,
        __MODULE__,
        self()
      )

    state =
      state(
        config: config,
        message_type: message_type,
        client: client,
        coordinator: pid,
        cb_module: cb_module,
        cb_config: cb_config,
        group_id: group_id
      )

    {:ok, state}
  end

  @impl GenServer
  def handle_call(
        {:get_committed_offsets, topic_partitions},
        _from,
        state(cb_module: cb_module, cb_config: cb_config) = state
      ) do
    fun = fn tp = {topic, partition} ->
      case cb_module.get_committed_offset(cb_config, topic, partition) do
        {:ok, offset} ->
          {true, {tp, offset}}

        :undefined ->
          false
      end
    end

    result = :lists.filtermap(fun, topic_partitions)
    {:reply, {:ok, result}, state}
  end

  def handle_call(:unsubscribe_all_partitions, _from, state(workers: workers) = state) do
    terminate_all_workers(workers)
    {:reply, :ok, state(state, workers: %{})}
  end

  def handle_call({:assign_partitions, members, topic_partition_list}, _from, state) do
    state(cb_module: cb_module, cb_config: cb_config) = state
    reply = cb_module.assign_partitions(cb_config, members, topic_partition_list)
    {:reply, reply, state}
  end

  def handle_call(:get_workers, _from, state(workers: workers) = state) do
    {:reply, workers, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  @impl GenServer
  def handle_cast({:commit_offset, topic, partition, offset}, state) do
    state(coordinator: coordinator, generation_id: generation_id) = state
    do_ack(topic, partition, offset, state)
    :ok = BrodGroupCoordinator.ack(coordinator, generation_id, topic, partition, offset)
    {:noreply, state}
  end

  def handle_cast({:ack_offset, topic, partition, offset}, state) do
    do_ack(topic, partition, offset, state)
    {:noreply, state}
  end

  def handle_cast({:new_assignments, member_id, generation_id, assignments}, state(config: config) = state0) do
    default_consumer_config = []
    consumer_config = Map.get(config, :consumer_config, default_consumer_config)
    state1 = state(state0, generation_id: generation_id)

    state =
      :lists.foldl(
        fn assignment, state_ ->
          brod_received_assignment(topic: topic, partition: partition, begin_offset: begin_offset) = assignment
          maybe_start_worker(member_id, consumer_config, topic, partition, begin_offset, state_)
        end,
        state1,
        assignments
      )

    {:noreply, state}
  end

  def handle_cast(_cast, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:EXIT, pid, _reason}, state(coordinator: pid) = state) do
    {:stop, {:shutdown, :coordinator_failure}, state(state, coordinator: :undefined)}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    case (for {tp, pid1} <- Map.to_list(state(state, :workers)),
              pid1 === pid do
            tp
          end) do
      [topic_partition | _] ->
        :ok = handle_worker_failure(topic_partition, pid, reason, state)
        {:stop, :shutdown, state}

      _ ->
        Logger.info(:io_lib.format(@shutting_down, [reason, pid]), %{domain: [:brod]})
        {:stop, :shutdown, state}
    end
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state(workers: workers, coordinator: coordinator, group_id: group_id)) do
    :ok = terminate_all_workers(workers)
    :ok = flush_offset_commits(group_id, coordinator)
  end

  defp flush_offset_commits(group_id, coordinator)
       when is_pid(coordinator) do
    case BrodGroupCoordinator.commit_offsets(coordinator) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(:io_lib.format(@commit_flush_failed, [group_id, reason]), %{domain: [:brod]})
        :ok
    end
  end

  defp flush_offset_commits(_, _) do
    :ok
  end

  defp handle_worker_failure({topic, partition}, pid, reason, state) do
    state(group_id: group_id) = state

    @worker_crashed
    |> :io_lib.format([group_id, topic, partition, pid, reason])
    |> to_string()
    |> Logger.error(%{domain: [:brod]})

    :ok
  end

  defp terminate_all_workers(workers) do
    Enum.each(workers, fn worker ->
      Logger.info(:io_lib.format(@terminating_worker, [worker]), %{domain: [:brod]})
      terminate_worker(worker)
    end)

    :ok
  end

  defp terminate_worker(worker_pid) do
    case :erlang.is_process_alive(worker_pid) do
      true ->
        :erlang.unlink(worker_pid)
        BrodTopicSubscriber.stop(worker_pid)

      false ->
        :ok
    end
  end

  defp maybe_start_worker(_member_id, consumer_config, topic, partition, begin_offset, state) do
    state(
      workers: workers,
      client: client,
      cb_module: cb_module,
      cb_config: cb_config,
      group_id: group_id,
      message_type: message_type
    ) = state

    topic_partition = {topic, partition}

    case workers do
      %{^topic_partition => _worker} ->
        state

      _ ->
        self = self()

        commit_fun = fn offset ->
          commit(self, topic, partition, offset)
        end

        start_options = %{
          cb_module: cb_module,
          cb_config: cb_config,
          partition: partition,
          begin_offset: begin_offset,
          group_id: group_id,
          commit_fun: commit_fun,
          topic: topic
        }

        {:ok, pid} = start_worker(client, topic, message_type, partition, consumer_config, start_options)

        new_workers = Map.put(workers, topic_partition, pid)
        state(state, workers: new_workers)
    end
  end

  defp start_worker(client, topic, message_type, partition, consumer_config, start_options) do
    args = %{
      client: client,
      topic: topic,
      partitions: [partition],
      consumer_config: consumer_config,
      message_type: message_type,
      cb_module: BrodGroupSubscriberWorker,
      init_data: start_options
    }

    {:ok, pid} = BrodTopicSubscriber.start_link(args)
    {:ok, pid}
  end

  defp do_ack(topic, partition, offset, state(workers: workers)) do
    topic_partition = {topic, partition}

    case workers do
      %{^topic_partition => pid} ->
        BrodTopicSubscriber.ack(pid, partition, offset)
        :ok

      _ ->
        {:error, :unknown_topic_or_partition}
    end
  end
end
