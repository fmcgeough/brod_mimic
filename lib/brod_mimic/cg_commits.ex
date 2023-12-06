defmodule BrodMimic.CgCommits do
  @moduledoc false
  use GenServer

  import Record, only: [defrecord: 3]

  @behaviour BrodMimic.GroupMember

  alias BrodMimic.GroupCoordinator, as: BrodGroupCoordinator
  alias BrodMimic.Utils, as: BrodUtils

  require Logger
  require Record

  @partitions_not_received "Partitions ~p are not received in assignment, There is probably another active group member subscribing to topic ~s, stop it and retry"

  defrecord(:r_kafka_message_set, :kafka_message_set,
    topic: :undefined,
    partition: :undefined,
    high_wm_offset: :undefined,
    messages: :undefined
  )

  defrecord(:r_kafka_fetch_error, :kafka_fetch_error,
    topic: :undefined,
    partition: :undefined,
    error_code: :undefined,
    error_desc: ''
  )

  defrecord(:r_brod_call_ref, :brod_call_ref,
    caller: :undefined,
    callee: :undefined,
    ref: :undefined
  )

  defrecord(:r_brod_produce_reply, :brod_produce_reply,
    call_ref: :undefined,
    base_offset: :undefined,
    result: :undefined
  )

  defrecord(:r_kafka_group_member_metadata, :kafka_group_member_metadata,
    version: :undefined,
    topics: :undefined,
    user_data: :undefined
  )

  defrecord(:r_brod_received_assignment, :brod_received_assignment,
    topic: :undefined,
    partition: :undefined,
    begin_offset: :undefined
  )

  defrecord(:r_brod_cg, :brod_cg,
    id: :undefined,
    protocol_type: :undefined
  )

  defrecord(:r_socket, :socket,
    pid: :undefined,
    host: :undefined,
    port: :undefined,
    node_id: :undefined
  )

  defrecord(:r_cbm_init_data, :cbm_init_data,
    committed_offsets: :undefined,
    cb_fun: :undefined,
    cb_data: :undefined
  )

  defrecord(:r_state, :state,
    client: :undefined,
    group_id: :undefined,
    memberId: :undefined,
    generation_id: :undefined,
    coordinator: :undefined,
    topic: :undefined,
    offsets: :undefined,
    is_elected: false,
    pending_sync: :undefined,
    is_done: false
  )

  def run(client_id, group_input) do
    {:ok, pid} = start_link(client_id, group_input)
    :ok = sync(pid)
    :ok = stop(pid)
  end

  def start_link(client, group_input) do
    GenServer.start_link(__MODULE__, {client, group_input}, [])
  end

  def stop(pid) do
    mref = :erlang.monitor(:process, pid)
    :ok = GenServer.cast(pid, :stop)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  def sync(pid) do
    :ok = GenServer.call(pid, :sync, :infinity)
  end

  def assignments_received(pid, memberId, generation_id, topicAssignments) do
    GenServer.cast(
      pid,
      {:new_assignments, memberId, generation_id, topicAssignments}
    )
  end

  def assignments_revoked(pid) do
    GenServer.call(pid, :unsubscribe_all_partitions, :infinity)
  end

  def assign_partitions(pid, members, topicPartitionList) do
    call = {:assign_partitions, members, topicPartitionList}
    GenServer.call(pid, call, :infinity)
  end

  def get_committed_offsets(_pid, _topic_partitions) do
    {:ok, []}
  end

  def init({client, group_input}) do
    :ok = BrodUtils.assert_client(client)
    group_id = :proplists.get_value(:id, group_input)
    :ok = BrodUtils.assert_group_id(group_id)
    topic = :proplists.get_value(:topic, group_input)

    protocol_name = :proplists.get_value(:protocol, group_input)

    retention = :proplists.get_value(:retention, group_input)
    offsets = :proplists.get_value(:offsets, group_input)

    config = [
      {:partition_assignment_strategy, :callback_implemented},
      {:offset_retention_seconds, retention},
      {:protocol_name, protocol_name},
      {:rejoin_delay_seconds, 2}
    ]

    {:ok, pid} =
      BrodGroupCoordinator.start_link(
        client,
        group_id,
        [topic],
        config,
        __MODULE__,
        self()
      )

    state =
      r_state(
        client: client,
        group_id: group_id,
        coordinator: pid,
        topic: topic,
        offsets: offsets
      )

    {:ok, state}
  end

  def handle_info(info, state) do
    log(state, :info, 'Info discarded:~p', [info])
    {:noreply, state}
  end

  def handle_call(:sync, from, state0) do
    state1 = r_state(state0, pending_sync: from)
    state = maybe_reply_sync(state1)
    {:noreply, state}
  end

  def handle_call(
        {:assign_partitions, members, topic_partitions},
        _from,
        r_state(topic: my_topic, offsets: offsets) = state
      ) do
    Logger.info("Assigning all topic partitions to self")

    my_tp =
      for {p, _} <- offsets do
        {my_topic, p}
      end

    pred = fn tp ->
      not :lists.member(tp, topic_partitions)
    end

    case :lists.filter(pred, my_tp) do
      [] ->
        :ok

      bad_partitions ->
        partition_numbers =
          for {_t, p} <- bad_partitions do
            p
          end

        log(state, :error, 'Nonexisting partitions in input: ~p', [partition_numbers])
        :erlang.exit({:non_existing_partitions, partition_numbers})
    end

    result = assign_all_to_self(members, my_tp)
    {:reply, result, r_state(state, is_elected: true)}
  end

  def handle_call(:unsubscribe_all_partitions, _from, r_state() = state) do
    {:reply, :ok, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def handle_cast(
        {:new_assignments, _member_id, generation_id, assignments},
        r_state(
          is_elected: is_leader,
          offsets: offsets_to_commit,
          coordinator: pid,
          topic: my_topic
        ) = state
      ) do
    is_leader or log(state, :info, 'Not elected', [])

    groupped0 =
      BrodUtils.group_per_key(
        fn r_brod_received_assignment(
             topic: topic,
             partition: partition,
             begin_offset: offset
           ) ->
          {topic, {partition, offset}}
        end,
        assignments
      )

    groupped =
      :lists.filter(
        fn {topic, _} ->
          topic === my_topic
        end,
        groupped0
      )

    log(state, :info, 'current offsets:\n~p', [groupped])

    case groupped do
      [] ->
        log(state, :error, 'Topic ~s is not received in assignment', [my_topic])
        :erlang.exit({:bad_topic_assignment, groupped0})

      [{^my_topic, partition_offset_list}] ->
        my_partitions =
          for {p, _o} <- offsets_to_commit do
            p
          end

        received_partitions =
          for {p, _o} <- partition_offset_list do
            p
          end

        case my_partitions -- received_partitions do
          [] ->
            :ok

          left ->
            @partitions_not_received
            |> :io_lib.format([my_topic, left])
            |> to_string()
            |> Logger.error()

            :erlang.exit({:unexpected_assignments, left})
        end
    end

    :lists.foreach(
      fn {partition, offset} ->
        offset_to_commit = offset - 1
        BrodGroupCoordinator.ack(pid, generation_id, my_topic, partition, offset_to_commit)
      end,
      offsets_to_commit
    )

    case BrodGroupCoordinator.commit_offsets(pid) do
      :ok ->
        :ok

      {:error, reason} ->
        log(state, :error, 'Failed to commit, reason:\n~p', [reason])
        :erlang.exit(:commit_failed)
    end

    {:noreply, set_done(state)}
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

  def terminate(_reason, r_state()) do
    :ok
  end

  defp set_done(state) do
    maybe_reply_sync(r_state(state, is_done: true))
  end

  defp maybe_reply_sync(r_state(is_done: false) = state) do
    state
  end

  defp maybe_reply_sync(r_state(pending_sync: :undefined) = state) do
    state
  end

  defp maybe_reply_sync(r_state(pending_sync: from) = state) do
    GenServer.reply(from, :ok)
    log(state, :info, 'done\n', [])
    r_state(state, pending_sync: :undefined)
  end

  defp assign_all_to_self([{my_member_id, _} | members], topic_partitions) do
    groupped = BrodUtils.group_per_key(topic_partitions)

    [
      {my_member_id, groupped}
      | for {id, _member_meta} <- members do
          {id, []}
        end
    ]
  end

  defp log(r_state(group_id: group_id), level, fmt, args) do
    case :logger.allow(level, __MODULE__) do
      true ->
        :erlang.apply(:logger, :macro_log, [
          %{mfa: {__MODULE__, :log, 4}, line: 308, file: '../brod/src/brod_cg_commits.erl'},
          level,
          'Group member (~s,coor=~p):\n' ++ fmt,
          [group_id, self() | args],
          %{domain: [:brod]}
        ])

      false ->
        :ok
    end
  end
end
