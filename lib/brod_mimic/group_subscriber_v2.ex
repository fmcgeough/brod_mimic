defmodule BrodMimic.GroupSubscriberv2 do
  @moduledoc """
  This module implements an improved version of `BrodMimic.GroupSubscriber` behaviour.
  The key difference is that each partition worker runs in a separate process, allowing
  parallel message processing.

  Callbacks are documented in the source code of this module.
  """
  @behaviour BrodMimic.GroupMember

  use GenServer

  import Record, only: [defrecord: 3]

  alias BrodMimic.GroupCoordinator, as: BrodGroupCoordinator
  alias BrodMimic.TopicSubscriber, as: BrodTopicSubscriber
  alias BrodMimic.Utils, as: BrodUtils

  require Logger

  @worker_crashed "group_subscriber_v2 worker crashed.~n  group_id = ~s~n  topic = ~s~n  partition = ~p~n  pid = ~p~n  reason = ~p"
  @shutting_down "Received EXIT:~p from ~p, shutting down"
  @commit_flush_failed "group_subscriber_v2 ~s failed to flush commits before termination ~p"
  @terminating_worker "Terminating worker pid=~p"

  defrecord(:r_brod_received_assignment, :brod_received_assignment,
    topic: :undefined,
    partition: :undefined,
    begin_offset: :undefined
  )

  defrecord(:r_state, :state,
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

  def start_link(config) do
    GenServer.start_link(:brod_group_subscriber_v2, config, [])
  end

  def stop(pid) do
    mref = Process.monitor(pid)
    Process.unlink(pid)
    Process.exit(pid, :shutdown)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  def ack(pid, topic, partition, offset) do
    GenServer.cast(pid, {:ack_offset, topic, partition, offset})
  end

  def commit(pid, topic, partition, offset) do
    GenServer.cast(pid, {:commit_offset, topic, partition, offset})
  end

  def get_workers(pid) do
    get_workers(pid, :infinity)
  end

  defp get_workers(pid, timeout) do
    GenServer.call(pid, :get_workers, timeout)
  end

  def assignments_received(pid, member_id, generation_id, topic_assignments) do
    GenServer.cast(
      pid,
      {:new_assignments, member_id, generation_id, topic_assignments}
    )
  end

  def assignments_revoked(pid) do
    GenServer.call(pid, :unsubscribe_all_partitions, :infinity)
  end

  def get_committed_offsets(pid, topic_partitions) do
    GenServer.call(pid, {:get_committed_offsets, topic_partitions}, :infinity)
  end

  def assign_partitions(pid, members, topic_partition_list) do
    call = {:assign_partitions, members, topic_partition_list}
    GenServer.call(pid, call, :infinity)
  end

  def init(config) do
    %{client: client, group_id: group_id, topics: topics, cb_module: cb_module} = config
    Process.flag(:trap_exit, true)
    message_type = :maps.get(:message_type, config, :message_set)
    default_group_config = []
    group_config = :maps.get(:group_config, config, default_group_config)
    cb_config = :maps.get(:init_data, config, :undefined)
    :ok = BrodUtils.assert_client(client)
    :ok = BrodUtils.assert_group_id(group_id)
    :ok = BrodUtils.assert_topics(topics)

    {:ok, pid} =
      BrodGroupCoordinator.start_link(
        client,
        group_id,
        topics,
        group_config,
        :brod_group_subscriber_v2,
        self()
      )

    state =
      r_state(
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

  def handle_call(
        {:get_committed_offsets, topic_partitions},
        _from,
        r_state(cb_module: cb_module, cb_config: cb_config) = state
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

  def handle_call(:unsubscribe_all_partitions, _from, r_state(workers: workers) = state) do
    terminate_all_workers(workers)
    {:reply, :ok, r_state(state, workers: %{})}
  end

  def handle_call({:assign_partitions, members, topic_partition_list}, _from, state) do
    r_state(cb_module: cb_module, cb_config: cb_config) = state
    reply = cb_module.assign_partitions(cb_config, members, topic_partition_list)
    {:reply, reply, state}
  end

  def handle_call(:get_workers, _from, r_state(workers: workers) = state) do
    {:reply, workers, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def handle_cast(
        {:commit_offset, topic, partition, offset},
        state
      ) do
    r_state(
      coordinator: coordinator,
      generation_id: generation_id
    ) = state

    do_ack(topic, partition, offset, state)
    :ok = BrodGroupCoordinator.ack(coordinator, generation_id, topic, partition, offset)
    {:noreply, state}
  end

  def handle_cast(
        {:ack_offset, topic, partition, offset},
        state
      ) do
    do_ack(topic, partition, offset, state)
    {:noreply, state}
  end

  def handle_cast(
        {:new_assignments, member_id, generation_id, assignments},
        r_state(config: config) = state0
      ) do
    default_consumer_config = []
    consumer_config = :maps.get(:consumer_config, config, default_consumer_config)
    state1 = r_state(state0, generation_id: generation_id)

    state =
      :lists.foldl(
        fn assignment, state_ ->
          r_brod_received_assignment(
            topic: topic,
            partition: partition,
            begin_offset: begin_offset
          ) = assignment

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

  def handle_info({:EXIT, pid, _reason}, r_state(coordinator: pid) = state) do
    {:stop, {:shutdown, :coordinator_failure}, r_state(state, coordinator: :undefined)}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    case (for {tp, pid1} <- :maps.to_list(r_state(state, :workers)),
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

  def terminate(_reason, r_state(workers: workers, coordinator: coordinator, group_id: group_id)) do
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
    r_state(group_id: group_id) = state

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
    r_state(
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

        {:ok, pid} =
          start_worker(client, topic, message_type, partition, consumer_config, start_options)

        new_workers = Map.put(workers, topic_partition, pid)
        r_state(state, workers: new_workers)
    end
  end

  defp start_worker(client, topic, message_type, partition, consumer_config, start_options) do
    {:ok, pid} =
      BrodTopicSubscriber.start_link(
        client,
        topic,
        [partition],
        consumer_config,
        message_type,
        BrodGroupSubscriberWorker,
        start_options
      )

    {:ok, pid}
  end

  defp do_ack(topic, partition, offset, r_state(workers: workers)) do
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
