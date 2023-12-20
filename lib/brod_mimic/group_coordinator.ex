defmodule BrodMimic.GroupCoordinator do
  @moduledoc """
  Kafka consumer group coordinator
  """

  use BrodMimic.Macros
  use GenServer

  import Record, only: [defrecord: 2]

  alias BrodMimic.Brod
  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.KafkaRequest, as: BrodKafkaRequest
  alias BrodMimic.Utils, as: BrodUtils

  require Logger

  @leaving_group "Leaving group, reason: ~p\n"
  @rejoining_group "re-joining group, reason:~p"
  @failed_to_join_group "failed to join group\nreason: ~p"
  @elected "elected=~p"
  @assignments_received "assignments received:~s"
  @group_member_info "Group member (~s,coor=~p,cb=~p,generation=~p):\n"

  defrecord(:state,
    client: :undefined,
    group_id: :undefined,
    member_id: "",
    leader_id: :undefined,
    generation_id: 0,
    topics: [],
    connection: :undefined,
    hb_ref: :undefined,
    members: [],
    is_in_group: false,
    member_pid: :undefined,
    member_module: :undefined,
    acked_offsets: [],
    offset_commit_timer: :undefined,
    partition_assignment_strategy: :undefined,
    session_timeout_seconds: :undefined,
    rebalance_timeout_seconds: :undefined,
    heartbeat_rate_seconds: :undefined,
    max_rejoin_attempts: :undefined,
    rejoin_delay_seconds: :undefined,
    offset_retention_seconds: :undefined,
    offset_commit_policy: :undefined,
    offset_commit_interval_seconds: :undefined,
    protocol_name: :undefined
  )

  @type protocol_name() :: String.t()
  @type ts() :: :erlang.timestamp()
  @typedoc """
  How commits to Kafka are handled. Default is `:commit_to_kafka_v2`
  """
  @type brod_offset_commit_policy() :: :commit_to_kafka_v2 | :consumer_managed

  @typedoc """
  How to assign partitions. Default is `:roundrobin_v2`.
  """
  @type brod_partition_assignment_strategy() :: :roundrobin_v2 | :callback_implemented
  @type partition_assignment_strategy() :: brod_partition_assignment_strategy()
  @type offset_commit_policy() :: brod_offset_commit_policy()
  @type member_id() :: Brod.group_member_id()
  @type topic_partition_offset() :: {topic(), partition(), offset()}
  @type topic_partition_list() :: [Brod.topic_partition()]

  @typedoc """
  GenServer state

  - group_member_id -  Group member ID, which should be set to empty in the
    first join group request, then a new member id is assigned by the group
    coordinator and in join group response. This field may change if the member
    has lost connection to the coordinator and received 'UnknownMemberId'
    exception in response messages.
  - leader_id - `member_id == leader_id` if elected as group leader by the coordinator.
  - topics - A set of topic names where the group members consumes from
  - connection - This socket is dedicated for group management requests for join
    group, sync group, offset commit, and heartbeat. We can not use a payload
    connection managed by `BrodMimic.Client` because connections in the Client
    are shared resources, but the connection to group coordinator has to be
    dedicated to one group member.
  - hb_ref - heartbeat reference, to discard stale responses
  - members - all group members received in the join group response
  - is_in_group - Set to false before joining the group then set to true when
    successfully joined the group. This is by far only used to prevent the
    timer-triggered loopback command message sending a HeartbeatRequest to the
    group coordinator broker.
  - member_pid - The process which is responsible to subscribe/unsubscribe to all
    assigned topic-partitions.
  - member_module - The module which implements group member functionality
  - acked_offsets - The offsets that has been acknowledged by the member i.e.
    the offsets that are ready for commit. NOTE: this field is not used if
    offset_commit_policy is `:consumer_managed`
  - offset_commit_timer - The reference of the timer which triggers offset commit
  """
  @type state() ::
          record(:state,
            client: Brod.client(),
            group_id: Brod.group_id(),
            member_id: member_id(),
            leader_id: :undefined | member_id(),
            generation_id: integer(),
            topics: [topic()],
            connection: :undefined | :kpro.connection(),
            hb_ref: :undefined | {reference(), ts()},
            members: [Brod.group_member()],
            is_in_group: boolean(),
            member_pid: pid(),
            member_module: module(),
            acked_offsets: [{{topic(), partition()}, offset()}],
            offset_commit_timer: :undefined | reference(),
            partition_assignment_strategy: partition_assignment_strategy(),
            session_timeout_seconds: pos_integer(),
            rebalance_timeout_seconds: pos_integer(),
            heartbeat_rate_seconds: pos_integer(),
            max_rejoin_attempts: non_neg_integer(),
            rejoin_delay_seconds: non_neg_integer(),
            offset_retention_seconds: :undefined | integer(),
            offset_commit_policy: offset_commit_policy(),
            offset_commit_interval_seconds: pos_integer(),
            protocol_name: protocol_name()
          )

  @doc """
  Start a kafka consumer group coordinator.

  ## Parameters

  - client - `client_id` (or pid, but not recommended)
  - group_id - Predefined globally unique (in a Kafka cluster) binary string
  - topics - Predefined set of topic names to join the group
  - config - The group coordinator configs in a proplist, possible values:
    - `:partition_assignment_strategy` (optional, default = `:roundrobin_v2`).
      Possible values:
      - `:roundrobin_v2` Take all topic-offset (sorted
        `t:topic_partition_list/0`), assign one to each member in a roundrobin
        fashion. Only partitions in the subscription topic list are assigned
      - `:callback_implemented` Call `cb_module.assign_partitions/2` to assign
        partitions
    - `:session_timeout_seconds` (optional, default = 30). Time in seconds for
      the group coordinator broker to consider a member `:down` if no heartbeat
      or any kind of requests received from a broker in the past N seconds. A
      group member may also consider the coordinator broker `:down` if no
      heartbeat response response received in the past N seconds.
    - `:rebalance_timeout_seconds` (optional, default = 30). Time in seconds for
      each worker to join the group once a rebalance has begun. If the timeout
      is exceeded, then the worker will be removed from the group, which will
      cause offset commit failures.
    - `:heartbeat_rate_seconds` (optional, default = 5). Time in seconds for the
      member to 'ping' the group coordinator. Care should be taken when picking
      the number, on one hand, we do not want to flush the broker with requests
      if we set it too low, on the other hand, if set it too high, it may take
      too long for the members to realize status changes of the group such as
      assignment rebalancing or group coordinator switchover etc.
    - `:max_rejoin_attempts` (optional, default = 5). Maximum number of times
      allowed for a member to re-join the group. The gen_server will stop if it
      reached the maximum number of retries. 'let it crash' may not be the
      optimal strategy here because the group member id is kept in the
      gen_server looping state and it is reused when re-joining the group.
    - `:rejoin_delay_seconds` (optional, default = 1). Delay in seconds before
      re-joining the group.
    - `:offset_commit_policy` (optional, default = `:commit_to_kafka_v2`).
      How/where to commit offsets, possible values:
      - `:commit_to_kafka_v2` Group coordinator will commit the offsets to Kafka
        using version 2 `OffsetCommitRequest`.
      - `:consumer_managed` The group member (e.g. BrodMimic.GroupSubscriber`)
        is responsible for persisting offsets to a local or centralized storage.
        And the callback `get_committed_offsets' should be implemented to allow
        group coordinator to retrieve the committed offsets.
    - `:offset_commit_interval_seconds` (optional, default = 5). The time
      interval between two `OffsetCommitRequest` messages. This config is
      irrelevant if `:offset_commit_policy` is `:consumer_managed`.
    - `:offset_retention_seconds` (optional, default = -1). How long the time is
      to be kept in Kafka before it is deleted. default special value -1
      indicates that the consumer_offsets topic retention policy is used. This
      config is irrelevant if `:offset_commit_policy` is `:consumer_managed`.
    - `:protocol_name` (optional, default = `:roundrobin_v2`). This is the
      protocol name used when join a group, if not given, by default
      `partition_assignment_strategy' is used as the protocol name. Setting a
      protocol name allows to interact with consumer group member designed in
      other programming languages. For example, `range` is the most commonly
      used protocol name for JAVA client. However, only roundrobin is supported
      by the library. In order to mimic 'range` protocol one will have
      to do it via `callback_implemented` assignment strategy.
  - cb_module - The module which implements group coordinator callbacks
  - member_pid - The member process pid

  """
  def start_link(client, group_id, topics, config, cb_module, member_pid) do
    args = {client, group_id, topics, config, cb_module, member_pid}
    GenServer.start_link(BrodMimic.GroupCoordinator, args, [])
  end

  @doc """
  For group member to call to acknowledge a consumed message offset
  """
  @spec ack(pid(), integer(), topic(), partition(), offset()) :: :ok
  def ack(pid, generation_id, topic, partition, offset) do
    send(pid, {:ack, generation_id, topic, partition, offset})
    :ok
  end

  @doc """
  Force commit collected (acked) offsets immediately
  """
  @spec commit_offsets(pid()) :: :ok | {:error, any()}
  def commit_offsets(coordinator_pid) do
    commit_offsets(coordinator_pid, _offsets = [])
  end

  @doc """
  Force commit collected (acked) offsets plus the given extra offsets
  immediately
  """
  @spec commit_offsets(pid(), [topic_partition_offset()]) :: :ok | {:error, any()}
  def commit_offsets(coordinator_pid, offsets0) do
    offsets = :lists.ukeysort(1, offsets0)
    GenServer.call(coordinator_pid, {:commit_offsets, offsets}, 5000)
  catch
    :exit, {:timeout, _} ->
      {:error, :timeout}
  end

  @doc """
  Update the list of topics the `BrodMimic.GroupCoordinator` follow which
  triggers a join group rebalance
  """
  @spec update_topics(pid(), [topic()]) :: :ok
  def update_topics(coordinator_pid, topics) do
    GenServer.cast(coordinator_pid, {:update_topics, topics})
  end

  @doc """
  Stop group coordinator, wait for pid `DOWN` before return
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    mref = Process.monitor(pid)
    Process.exit(pid, :shutdown)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  @impl GenServer
  def init({client, group_id, topics, config, cb_module, member_pid}) do
    Process.flag(:trap_exit, true)

    get_cfg = fn name, default ->
      :proplists.get_value(name, config, default)
    end

    pa_strategy = get_cfg.(:partition_assignment_strategy, :roundrobin_v2)

    session_timeout_sec = get_cfg.(:session_timeout_seconds, 30)

    rebalance_timeout_sec = get_cfg.(:rebalance_timeout_seconds, 30)

    hb_rate_sec = get_cfg.(:heartbeat_rate_seconds, 5)
    max_rejoin_attempts = get_cfg.(:max_rejoin_attempts, 5)
    rejoin_delay_seconds = get_cfg.(:rejoin_delay_seconds, 1)

    offset_retention_seconds = get_cfg.(:offset_retention_seconds, :undefined)

    offset_commit_policy = get_cfg.(:offset_commit_policy, :commit_to_kafka_v2)

    offset_commit_interval_seconds = get_cfg.(:offset_commit_interval_seconds, 5)

    protocol_name = get_cfg.(:protocol_name, pa_strategy)
    send(self(), {:lo_cmd_stabilize, 0, :undefined})
    :ok = start_heartbeat_timer(hb_rate_sec)

    state =
      state(
        client: client,
        group_id: group_id,
        topics: topics,
        member_pid: member_pid,
        member_module: cb_module,
        partition_assignment_strategy: pa_strategy,
        session_timeout_seconds: session_timeout_sec,
        rebalance_timeout_seconds: rebalance_timeout_sec,
        heartbeat_rate_seconds: hb_rate_sec,
        max_rejoin_attempts: max_rejoin_attempts,
        rejoin_delay_seconds: rejoin_delay_seconds,
        offset_retention_seconds: offset_retention_seconds,
        offset_commit_policy: offset_commit_policy,
        offset_commit_interval_seconds: offset_commit_interval_seconds,
        protocol_name: protocol_name
      )

    {:ok, state}
  end

  @impl GenServer
  def handle_info({:ack, generation_id, topic, partition, offset}, state) do
    {:noreply, handle_ack(state, generation_id, topic, partition, offset)}
  end

  def handle_info(:lo_cmd_commit_offsets, state(is_in_group: true) = state) do
    {:ok, new_state} =
      try do
        do_commit_offsets(state)
      catch
        reason ->
          stabilize(state, 0, reason)
      end

    {:noreply, new_state}
  end

  def handle_info(
        {:lo_cmd_stabilize, attempt_count, _reason},
        state(max_rejoin_attempts: max) = state
      )
      when attempt_count >= max do
    {:stop, :max_rejoin_attempts, state}
  end

  def handle_info({:lo_cmd_stabilize, attempt_count, reason}, state) do
    {:ok, new_state} = stabilize(state, attempt_count, reason)
    {:noreply, new_state}
  end

  def handle_info({:EXIT, pid, reason}, state(connection: pid) = state) do
    {:ok, new_state} = stabilize(state, 0, {:connection_down, reason})
    {:noreply, state(new_state, connection: :undefined)}
  end

  def handle_info({:EXIT, pid, reason}, state(member_pid: pid) = state) do
    case reason do
      :shutdown -> {:stop, :shutdown, state}
      {:shutdown, _} -> {:stop, :shutdown, state}
      :normal -> {:stop, :normal, state}
      _ -> {:stop, :member_down, state}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    {:stop, :shutdown, state}
  end

  def handle_info(
        :lo_cmd_send_heartbeat,
        state(hb_ref: hb_ref, session_timeout_seconds: session_timeout_seconds) = state
      ) do
    _ = start_heartbeat_timer(state(state, :heartbeat_rate_seconds))

    case hb_ref do
      :undefined ->
        {:ok, new_state} = maybe_send_heartbeat(state)
        {:noreply, new_state}

      {_ref, sent_time} ->
        elapsed = :timer.now_diff(:os.timestamp(), sent_time)

        case elapsed < session_timeout_seconds * 1_000_000 do
          true ->
            # keep waiting for heartbeat response
            {:noreply, state}

          false ->
            # try leave group and re-join when restarted by supervisor
            {:stop, :hb_timeout, state}
        end
    end
  end

  def handle_info(
        {:msg, _pid, kpro_rsp(api: :heartbeat, ref: hb_ref, msg: body)},
        state(hb_ref: {hb_ref, _sent_time}) = state0
      ) do
    ec = :kpro.find(:error_code, body)
    state = state(state0, hb_ref: :undefined)

    case is_error(ec) do
      true ->
        {:ok, new_state} = stabilize(state, 0, ec)
        {:noreply, new_state}

      false ->
        {:noreply, state}
    end
  end

  def handle_info(_info, state() = state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(
        {:commit_offsets, extra_offsets},
        from,
        state(acked_offsets: acked_offsets) = state
      ) do
    offsets = merge_acked_offsets(acked_offsets, extra_offsets)

    {:ok, new_state} = do_commit_offsets(state(state, acked_offsets: offsets))
    {:reply, :ok, new_state}
  catch
    reason ->
      GenServer.reply(from, {:error, reason})
      {:ok, new_state_} = stabilize(state, 0, reason)
      {:noreply, new_state_}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  @impl GenServer
  def handle_cast({:update_topics, topics}, state) do
    new_state0 = state(state, topics: topics)
    {:ok, new_state} = stabilize(new_state0, 0, :topics)
    {:noreply, new_state}
  end

  def handle_cast(_cast, state() = state) do
    {:noreply, state}
  end

  @impl GenServer
  def code_change(_old_vsn, state() = state, _extra) do
    {:ok, state}
  end

  @impl GenServer
  def terminate(
        reason,
        state(connection: connection, group_id: group_id, member_id: member_id) = state
      ) do
    Logger.info(fn -> log_string(state, @leaving_group, [reason]) end)
    body = [{:group_id, group_id}, {:member_id, member_id}]
    _ = try_commit_offsets(state)
    request = :kpro.make_request(:leave_group, _v = 0, body)

    try do
      _ = send_sync(connection, request, 1000)
      :ok
    catch
      _, _ ->
        :ok
    end
  end

  def discover_coordinator(state(client: client, connection: connection0, group_id: group_id) = state) do
    {endpoint, conn_config0} =
      (fn ->
         case BrodClient.get_group_coordinator(
                client,
                group_id
              ) do
           {:ok, result} ->
             result

           {:error, reason} ->
             throw(reason)
         end
       end).()

    case is_already_connected(state, endpoint) do
      true ->
        {:ok, state}

      false ->
        is_pid(connection0) and :kpro.close_connection(connection0)
        client_id = make_group_connection_client_id()
        conn_config = Map.put(conn_config0, :client_id, client_id)

        connection =
          (fn ->
             case :kpro.connect(endpoint, conn_config) do
               {:ok, result} ->
                 result

               {:error, reason} ->
                 throw(reason)
             end
           end).()

        {:ok, state(state, connection: connection)}
    end
  end

  defp is_already_connected(state(connection: conn), _) when not is_pid(conn) do
    false
  end

  defp is_already_connected(state(connection: conn), {host, port}) do
    {host0, port0} =
      (fn ->
         case :kpro_connection.get_endpoint(conn) do
           {:ok, result} ->
             result

           {:error, reason} ->
             throw(reason)
         end
       end).()

    :erlang.iolist_to_binary(host0) === :erlang.iolist_to_binary(host) and port0 === port
  end

  defp receive_pending_acks(state) do
    receive do
      {:ack, generation_id, topic, partition, offset} ->
        new_state = handle_ack(state, generation_id, topic, partition, offset)
        receive_pending_acks(new_state)
    after
      0 ->
        state
    end
  end

  def stabilize(
        state(
          rejoin_delay_seconds: rejoin_delay_seconds,
          member_module: member_module,
          member_pid: member_pid,
          offset_commit_timer: offset_commit_timer
        ) = state0,
        attempt_num,
        reason
      ) do
    is_reference(offset_commit_timer) and Process.cancel_timer(offset_commit_timer)

    if reason != :undefined do
      Logger.info(fn -> log_string(state0, @rejoining_group, [reason]) end)
    end

    # 1. unsubscribe all currently assigned partitions
    :ok = member_module.assignments_revoked(member_pid)

    # 2. some brod_group_member implementations may wait for messages
    #    to finish processing when assignments_revoked is called.
    #    The acknowledments of those messages would then be sitting
    #    in our inbox. So we do an explicit pass to collect all pending
    #    acks so they are included in the best-effort commit below.
    state1 = receive_pending_acks(state0)

    # 3. try to commit current offsets before re-joinning the group.
    #    try only on the first re-join attempt
    #    do not try if it was illegal generation or unknown member id
    #    exception received because it will fail on the same exception
    #     again
    state2 =
      case attempt_num === 0 and reason != :illegal_generation and reason != :unknown_member_id do
        true ->
          {:ok, state2} = try_commit_offsets(state1)
          state2

        false ->
          state1
      end

    state3 = state(state2, is_in_group: false)

    # 4. Clean up state based on the last failure reason
    state = maybe_reset_member_id(state3, reason)

    # 5. ensure we have a connection to the (maybe new) group coordinator
    f1 = &discover_coordinator/1
    # 6. join group
    f2 = &join_group/1
    # 7. sync assignments
    f3 = &sync_group/1

    retry_fun = fn state_in, new_reason ->
      Logger.info(fn -> log_string(state_in, @failed_to_join_group, [new_reason]) end)

      case attempt_num === 0 do
        true ->
          # do not delay before the first retry
          send(self(), {:lo_cmd_stabilize, attempt_num, reason})

        false ->
          milliseconds = :timer.seconds(rejoin_delay_seconds)

          Process.send_after(
            self(),
            {:lo_cmd_stabilize, attempt_num + 1, new_reason},
            milliseconds
          )
      end

      {:ok, state_in}
    end

    do_stabilize([f1, f2, f3], retry_fun, state)
  end

  defp do_stabilize([], _retry_fun, state) do
    {:ok, state}
  end

  defp do_stabilize([f | rest], retry_fun, state) do
    {:ok, state() = new_state} = f.(state)
    do_stabilize(rest, retry_fun, new_state)
  catch
    reason ->
      retry_fun.(state, reason)
  end

  defp maybe_reset_member_id(state, reason) do
    case should_reset_member_id(reason) do
      true ->
        state(state, member_id: <<>>)

      false ->
        state
    end
  end

  # we are likely kicked out from the group, rejoin with empty member id
  def should_reset_member_id(:unknown_member_id), do: true

  # the coordinator have moved to another broker, set it to :undefined to trigger a re-discover
  def should_reset_member_id(:not_coordinator), do: true

  # old connection was down, new connection will lead
  # to a new member id
  def should_reset_member_id({:connection_down, _reason}), do: true

  def should_reset_member_id(_), do: false

  defp join_group(
         state(
           group_id: group_id,
           member_id: member_id0,
           topics: topics,
           connection: connection,
           session_timeout_seconds: session_timeout_sec,
           rebalance_timeout_seconds: rebalance_timeout_sec,
           protocol_name: protocol_name,
           member_module: member_module,
           member_pid: member_pid
         ) = state0
       ) do
    meta = [
      {:version, 0},
      {:topics, topics},
      {:user_data,
       user_data(
         member_module,
         member_pid
       )}
    ]

    protocol = [{:name, protocol_name}, {:metadata, meta}]
    session_timeout = :timer.seconds(session_timeout_sec)
    rebalance_timeout = :timer.seconds(rebalance_timeout_sec)

    body = [
      {:group_id, group_id},
      {:session_timeout_ms, session_timeout},
      {:rebalance_timeout_ms, rebalance_timeout},
      {:member_id, member_id0},
      {:protocol_type, "consumer"},
      {:protocols, [protocol]}
    ]

    req = BrodKafkaRequest.join_group(connection, body)
    rsp_body = send_sync(connection, req, session_timeout)
    generation_id = :kpro.find(:generation_id, rsp_body)
    leader_id = :kpro.find(:leader, rsp_body)
    member_id = :kpro.find(:member_id, rsp_body)
    members0 = :kpro.find(:members, rsp_body)
    members1 = translate_members(members0)
    members = ensure_leader_at_hd(leader_id, members1)
    is_group_leader = leader_id === member_id

    state =
      state(state0,
        member_id: member_id,
        leader_id: leader_id,
        generation_id: generation_id,
        members: members
      )

    Logger.info(fn -> log_string(state, @elected, [is_group_leader]) end)
    {:ok, state}
  end

  defp sync_group(
         state(
           group_id: group_id,
           generation_id: generation_id,
           member_id: member_id,
           connection: connection,
           member_pid: member_pid,
           member_module: member_module
         ) = state
       ) do
    req_body = [
      {:group_id, group_id},
      {:generation_id, generation_id},
      {:member_id, member_id},
      {:assignments, assign_partitions(state)}
    ]

    sync_req = BrodKafkaRequest.sync_group(connection, req_body)

    rsp_body = send_sync(connection, sync_req)
    assignment = :kpro.find(:assignment, rsp_body)

    topic_assignments = get_topic_assignments(state, assignment)

    :ok = member_module.assignments_received(member_pid, member_id, generation_id, topic_assignments)

    new_state = state(state, is_in_group: true)

    Logger.info(fn ->
      log_string(new_state, @assignments_received, [format_assignments(topic_assignments)])
    end)

    start_offset_commit_timer(new_state)
  end

  defp handle_ack(state, generation_id, _topic, _partition, _offset)
       when generation_id < state(state, :generation_id) do
    state
  end

  defp handle_ack(
         state(acked_offsets: acked_offsets) = state,
         _generation_id,
         topic,
         partition,
         offset
       ) do
    new_acked_offsets = merge_acked_offsets(acked_offsets, [{{topic, partition}, offset}])
    state(state, acked_offsets: new_acked_offsets)
  end

  defp merge_acked_offsets(acked_offsets, offsets_to_ack) do
    :lists.ukeymerge(1, offsets_to_ack, acked_offsets)
  end

  defp format_assignments([]) do
    '[]'
  end

  defp format_assignments(assignments) do
    groupped =
      BrodUtils.group_per_key(
        fn brod_received_assignment(topic: topic, partition: partition, begin_offset: offset) ->
          {topic, {partition, offset}}
        end,
        assignments
      )

    :lists.map(
      fn {topic, partitions} ->
        ['\n  ', topic, ':', format_partition_assignments(partitions)]
      end,
      groupped
    )
  end

  defp format_partition_assignments([]) do
    []
  end

  defp format_partition_assignments([{partition, begin_offset} | rest]) do
    [
      :io_lib.format('\n    partition=~p begin_offset=~p', [partition, begin_offset]),
      format_partition_assignments(rest)
    ]
  end

  defp try_commit_offsets(state() = state) do
    {:ok, state()} = do_commit_offsets(state)
  catch
    _, _ ->
      {:ok, state}
  end

  defp do_commit_offsets(state) do
    {:ok, new_state} = do_commit_offsets_(state)
    start_offset_commit_timer(new_state)
  end

  defp do_commit_offsets_(state(acked_offsets: []) = state) do
    {:ok, state}
  end

  defp do_commit_offsets_(state(offset_commit_policy: :consumer_managed) = state) do
    {:ok, state}
  end

  defp do_commit_offsets_(
         state(
           group_id: group_id,
           member_id: member_id,
           generation_id: generation_id,
           connection: connection,
           offset_retention_seconds: offset_retention_secs,
           acked_offsets: acked_offsets
         ) = state
       ) do
    metadata = make_offset_commit_metadata()

    topic_offsets0 =
      BrodUtils.group_per_key(
        fn {{topic, partition}, offset} ->
          partition_offset = [
            {:partition_index, partition},
            {:committed_offset, offset + 1},
            {:committed_metadata, metadata}
          ]

          {topic, partition_offset}
        end,
        acked_offsets
      )

    topic_offsets =
      :lists.map(
        fn {topic, partition_offsets} ->
          [{:name, topic}, {:partitions, partition_offsets}]
        end,
        topic_offsets0
      )

    retention =
      case is_default_offset_retention(offset_retention_secs) do
        true ->
          -1

        false ->
          :timer.seconds(offset_retention_secs)
      end

    req_body = [
      {:group_id, group_id},
      {:generation_id, generation_id},
      {:member_id, member_id},
      {:retention_time_ms, retention},
      {:topics, topic_offsets}
    ]

    req =
      BrodKafkaRequest.offset_commit(
        connection,
        req_body
      )

    rsp_body = send_sync(connection, req)
    topics = :kpro.find(:topics, rsp_body)
    :ok = assert_commit_response(topics)
    new_state = state(state, acked_offsets: [])
    {:ok, new_state}
  end

  defp assert_commit_response(topics) do
    error_set = collect_commit_response_error_codes(topics)

    case :gb_sets.to_list(error_set) do
      [] -> :ok
      [ec] -> escalate_ec(ec)
      _ -> :erlang.error({:commit_offset_failed, topics})
    end
  end

  defp collect_commit_response_error_codes(topics) do
    List.foldl(topics, :gb_sets.new(), fn topic, acc1 ->
      partitions = :kpro.find(:partitions, topic)
      collect_partitions_response_error_codes(partitions, acc1)
    end)
  end

  defp collect_partitions_response_error_codes(partitions, gb_set) do
    List.foldl(partitions, gb_set, fn partition, acc2 ->
      ec = :kpro.find(:error_code, partition)

      case is_error(ec) do
        true -> :gb_sets.add_element(ec, acc2)
        false -> acc2
      end
    end)
  end

  defp assign_partitions(state) do
    # only leader can assign partitions to members
    if state(state, :member_id) == state(state, :leader_id) do
      []
    else
      all_topics = all_topics(state(state, :members))

      all_partitions =
        for topic <- all_topics, partition <- get_partitions(state(state, :client), topic) do
          {topic, partition}
        end

      assignments =
        case state(state, :partition_assignment_strategy) == :callback_implemented do
          true ->
            member_module = state(state, :member_module)

            member_module.assign_partitions(
              state(state, :member_pid),
              state(state, :members),
              all_partitions
            )

          false ->
            do_assign_partitions(
              state(state, :partition_assignment_strategy),
              state(state, :members),
              all_partitions
            )
        end

      :lists.map(
        fn {member_id, topics_} ->
          partition_assignments =
            :lists.map(
              fn {topic, partitions} ->
                [{:topic, topic}, {:partitions, partitions}]
              end,
              topics_
            )

          [
            {:member_id, member_id},
            {:assignment, [{:version, 0}, {:topic_partitions, partition_assignments}, {:user_data, <<>>}]}
          ]
        end,
        assignments
      )
    end
  end

  defp ensure_leader_at_hd(leader_id, members) do
    case :lists.keytake(leader_id, 1, members) do
      {:value, leader, followers} ->
        [leader | followers]

      false ->
        members
    end
  end

  defp translate_members(members) do
    Enum.map(members, fn member ->
      member_id = :kpro.find(:member_id, member)
      meta = :kpro.find(:metadata, member)
      version = :kpro.find(:version, meta)
      topics = :kpro.find(:topics, meta)
      user_data = :kpro.find(:user_data, meta)

      member_data = kafka_group_member_metadata(version: version, topics: topics, user_data: user_data)

      {member_id, member_data}
    end)
  end

  defp all_topics(members) do
    :lists.usort(
      :lists.append(
        :lists.map(
          fn {_member_id, m} ->
            kafka_group_member_metadata(m, :topics)
          end,
          members
        )
      )
    )
  end

  defp get_partitions(client, topic) do
    count =
      (fn ->
         case BrodClient.get_partitions_count(client, topic) do
           {:ok, result} ->
             result

           {:error, reason} ->
             throw(reason)
         end
       end).()

    :lists.seq(0, count - 1)
  end

  defp do_assign_partitions(:roundrobin_v2, members, all_partitions) do
    f = fn {member_id, m} ->
      subscribed_topics = kafka_group_member_metadata(m, :topics)

      is_valid_assignment = fn topic, _partition ->
        :lists.member(topic, subscribed_topics)
      end

      {member_id, is_valid_assignment, []}
    end

    member_assignment = :lists.map(f, members)

    for {member_id, _validation_fun, assignments} <-
          roundrobin_assign_loop(all_partitions, member_assignment, []) do
      {member_id, assignments}
    end
  end

  defp roundrobin_assign_loop([], pending_members, assigned_members) do
    :lists.reverse(assigned_members) ++ pending_members
  end

  defp roundrobin_assign_loop(partitions, [], assigned_members) do
    roundrobin_assign_loop(partitions, :lists.reverse(assigned_members), [])
  end

  defp roundrobin_assign_loop(
         [{topic, partition} | rest] = topic_partitions,
         [
           {member_id, is_valid_assignment, assigned_topics} = member0
           | pending_members
         ],
         assigned_members
       ) do
    case is_valid_assignment.(topic, partition) do
      true ->
        new_topics = :orddict.append_list(topic, [partition], assigned_topics)
        member = {member_id, is_valid_assignment, new_topics}
        roundrobin_assign_loop(rest, pending_members, [member | assigned_members])

      false ->
        roundrobin_assign_loop(topic_partitions, pending_members, [member0 | assigned_members])
    end
  end

  def get_topic_assignments(_state, :undefined), do: []
  def get_topic_assignments(_state, %{topic_partitions: []}), do: []

  def get_topic_assignments(state, assignment) do
    partition_assignments = :kpro.find(:topic_partitions, assignment)

    topic_partitions0 =
      :lists.map(
        fn partition_assignment ->
          topic = :kpro.find(:topic, partition_assignment)
          partitions = :kpro.find(:partitions, partition_assignment)

          for a_topic <- [topic], partition <- partitions do
            {a_topic, partition}
          end
        end,
        partition_assignments
      )

    topic_partitions = :lists.append(topic_partitions0)
    committed_offsets = get_committed_offsets(state, :topic_partitions)
    is_consumer_managed = state(offset_commit_policy: :consumer_managed)
    resolve_begin_offsets(topic_partitions, committed_offsets, is_consumer_managed)
  end

  def get_committed_offsets(
        state(
          offset_commit_policy: :consumer_managed,
          member_pid: member_pid,
          member_module: member_module
        ),
        topic_partitions
      ) do
    {:ok, r} = member_module.get_committed_offsets(member_pid, topic_partitions)
    r
  end

  def get_committed_offsets(
        state(offset_commit_policy: :commit_to_kafka_v2, group_id: group_id, connection: conn),
        topic_partitions
      ) do
    groupped_partitions = BrodUtils.group_per_key(topic_partitions)
    req = BrodKafkaRequest.offset_fetch(conn, group_id, groupped_partitions)
    rsp_body = send_sync(conn, req)
    # error_code is introduced in version 2
    escalate_ec(:kpro.find(:error_code, rsp_body, :no_error))
    topic_offsets = :kpro.find(:topics, rsp_body)

    committed_offsets0 = committed_offsets_from_topic_offsets(topic_offsets)

    :lists.append(committed_offsets0)
  end

  def resolve_begin_offsets([], _committed_offsets, _is_consumer_managed) do
    []
  end

  def resolve_begin_offsets([{topic, partition} | rest], committed_offsets, is_consumer_managed) do
    begin_offset =
      case :lists.keyfind({topic, partition}, 1, committed_offsets) do
        {_, {:begin_offset, offset}} ->
          resolve_special_offset(offset)

        {_, offset} when is_consumer_managed ->
          offset + 1

        {_, offset} ->
          offset

        false ->
          :undefined
      end

    assignment = brod_received_assignment(topic: topic, partition: partition, begin_offset: begin_offset)

    [assignment | resolve_begin_offsets(rest, committed_offsets, is_consumer_managed)]
  end

  defp committed_offsets_from_topic_offsets(topic_offsets) do
    :lists.map(
      fn topic_offset ->
        topic = :kpro.find(:name, topic_offset)
        partition_offsets = :kpro.find(:partitions, topic_offset)
        committed_offsets_from_partition_offsets(topic, partition_offsets)
      end,
      topic_offsets
    )
  end

  defp committed_offsets_from_partition_offsets(topic, partition_offsets) do
    :lists.foldl(
      fn partition_offset, acc ->
        partition = :kpro.find(:partition_index, partition_offset)
        offset0 = :kpro.find(:committed_offset, partition_offset)
        metadata = :kpro.find(:metadata, partition_offset)
        ec = :kpro.find(:error_code, partition_offset)
        escalate_ec(ec)
        # Offset -1 in offset_fetch_response is an indicator of 'no-value'
        case offset0 === -1 do
          true ->
            acc

          false ->
            offset = maybe_upgrade_from_roundrobin_v1(offset0, metadata)
            [{{topic, partition}, offset} | acc]
        end
      end,
      [],
      partition_offsets
    )
  end

  defp resolve_special_offset(0) do
    :earliest
  end

  defp resolve_special_offset(other) do
    other
  end

  defp start_heartbeat_timer(hb_rate_sec) do
    Process.send_after(self(), :lo_cmd_send_heartbeat, :timer.seconds(hb_rate_sec))
    :ok
  end

  defp start_offset_commit_timer(state(offset_commit_timer: old_timer) = state) do
    state(
      offset_commit_policy: policy,
      offset_commit_interval_seconds: seconds
    ) = state

    case policy do
      :consumer_managed ->
        {:ok, state}

      :commit_to_kafka_v2 ->
        is_reference(old_timer) and Process.cancel_timer(old_timer)

        receive do
          :lo_cmd_commit_offsets ->
            :ok
        after
          0 ->
            :ok
        end

        timeout = :timer.seconds(seconds)
        timer = Process.send_after(self(), :lo_cmd_commit_offsets, timeout)
        {:ok, state(state, offset_commit_timer: timer)}
    end
  end

  def maybe_send_heartbeat(
        state(
          is_in_group: true,
          group_id: group_id,
          member_id: member_id,
          generation_id: generation_id,
          connection: connection
        ) = state
      ) do
    req_body = [{:group_id, group_id}, {:generation_id, generation_id}, {:member_id, member_id}]
    req = :kpro.make_request(:heartbeat, 0, req_body)
    :ok = :kpro.request_async(connection, req)
    new_state = state(state, hb_ref: {kpro_req(req, :ref), :os.timestamp()})
    {:ok, new_state}
  end

  def maybe_send_heartbeat(state() = state) do
    {:ok, state(state, hb_ref: :undefined)}
  end

  defp send_sync(connection, request) do
    send_sync(connection, request, 5000)
  end

  defp send_sync(connection, request, timeout) do
    (fn ->
       case BrodUtils.request_sync(connection, request, timeout) do
         {:ok, result} ->
           result

         {:error, reason} ->
           throw(reason)
       end
     end).()
  end

  defp make_offset_commit_metadata do
    bin(['+1/', coordinator_id()])
  end

  defp user_data(module, pid) do
    BrodUtils.optional_callback(module, :user_data, [pid], <<>>)
  end

  defp make_group_connection_client_id do
    coordinator_id()
  end

  defp coordinator_id do
    bin(:io_lib.format('~p/~p', [node(), self()]))
  end

  defp bin(x) do
    :erlang.iolist_to_binary(x)
  end

  @doc """
  Before roundrobin_v2, brod had two versions of commit metadata:

  1. "ts() node() pid()"
    e.g. "2017-10-24:18:20:55.475670 'nodename@host-name' <0.18980.6>"
  2. "node()/pid()"
    e.g. "'nodename@host-name'/<0.18980.6>"

  Then roundrobin_v2:

  "+1/node()/pid()"
    e.g. "+1/'nodename@host-name'/<0.18980.6>"

  Here we try to recognize brod commits using a regexp,
  then check the +1 prefix to exclude roundrobin_v2.
  """
  def is_roundrobin_v1_commit(:kpro_null), do: false
  # def is_roundrobin_v1_commit(<<"+1/", _ / binary>>), do: false

  def is_roundrobin_v1_commit(metadata) do
    case :re.run(metadata, ".*@.*[/|\s]<0\.[0-9]+\.[0-9]+>$") do
      :nomatch -> false
      {:match, _} -> true
    end
  end

  defp maybe_upgrade_from_roundrobin_v1(offset, metadata) do
    case is_roundrobin_v1_commit(metadata) do
      true ->
        offset + 1

      false ->
        offset
    end
  end

  defp is_default_offset_retention(-1) do
    true
  end

  defp is_default_offset_retention(:undefined) do
    true
  end

  defp is_default_offset_retention(_) do
    false
  end

  defp log_string(
         state(group_id: group_id, generation_id: generation_id, member_pid: member_pid),
         format_string,
         args
       ) do
    :io_lib.format(@group_member_info <> format_string, [
      group_id,
      self(),
      member_pid,
      generation_id | args
    ])
  end
end
