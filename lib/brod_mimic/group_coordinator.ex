defmodule BrodMimic.GroupCoordinator do
  use GenServer

  import Bitwise
  import Record, only: [defrecord: 2, extract: 2]

  defrecord(:kpro_req, extract(:kpro_req, from_lib: "kafka_protocol/include/kpro.hrl"))

  Record.defrecord(:r_kafka_message_set, :kafka_message_set,
    topic: :undefined,
    partition: :undefined,
    high_wm_offset: :undefined,
    messages: :undefined
  )

  Record.defrecord(:r_kafka_fetch_error, :kafka_fetch_error,
    topic: :undefined,
    partition: :undefined,
    error_code: :undefined,
    error_desc: ''
  )

  Record.defrecord(:r_brod_call_ref, :brod_call_ref,
    caller: :undefined,
    callee: :undefined,
    ref: :undefined
  )

  Record.defrecord(:r_brod_produce_reply, :brod_produce_reply,
    call_ref: :undefined,
    base_offset: :undefined,
    result: :undefined
  )

  Record.defrecord(:r_kafka_group_member_metadata, :kafka_group_member_metadata,
    version: :undefined,
    topics: :undefined,
    user_data: :undefined
  )

  Record.defrecord(:r_brod_received_assignment, :brod_received_assignment,
    topic: :undefined,
    partition: :undefined,
    begin_offset: :undefined
  )

  Record.defrecord(:r_brod_cg, :brod_cg,
    id: :undefined,
    protocol_type: :undefined
  )

  Record.defrecord(:r_socket, :socket,
    pid: :undefined,
    host: :undefined,
    port: :undefined,
    node_id: :undefined
  )

  Record.defrecord(:r_cbm_init_data, :cbm_init_data,
    committed_offsets: :undefined,
    cb_fun: :undefined,
    cb_data: :undefined
  )

  Record.defrecord(:r_state, :state,
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

  def start_link(client, group_id, topics, config, cb_module, member_pid) do
    args = {client, group_id, topics, config, cb_module, member_pid}
    GenServer.start_link(:brod_group_coordinator, args, [])
  end

  def ack(pid, generation_id, topic, partition, offset) do
    send(pid, {:ack, generation_id, topic, partition, offset})
    :ok
  end

  def commit_offsets(coordinator_pid) do
    commit_offsets(coordinator_pid, _offsets = [])
  end

  def commit_offsets(coordinator_pid, offsets0) do
    offsets = :lists.ukeysort(1, offsets0)

    try do
      GenServer.call(coordinator_pid, {:commit_offsets, offsets}, 5000)
    catch
      :exit, {:timeout, _} ->
        {:error, :timeout}
    end
  end

  def update_topics(coordinator_pid, topics) do
    GenServer.cast(coordinator_pid, {:update_topics, topics})
  end

  def stop(pid) do
    mref = :erlang.monitor(:process, pid)
    :erlang.exit(pid, :shutdown)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  def init({client, group_id, topics, config, cb_module, member_pid}) do
    :erlang.process_flag(:trap_exit, true)

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
      r_state(
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

  def handle_call({:commit_offsets, extra_offsets}, from, state) do
    try do
      offsets = merge_acked_offsets(r_state(state, :acked_offsets), extra_offsets)

      {:ok, new_state} = do_commit_offsets(r_state(state, acked_offsets: offsets))
      {:reply, :ok, new_state}
    catch
      reason ->
        GenServer.reply(from, {:error, reason})
        {:ok, new_state_} = stabilize(state, 0, reason)
        {:noreply, new_state_}
    end
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def handle_cast({:update_topics, topics}, state) do
    new_state0 = r_state(state, topics: topics)
    {:ok, new_state} = stabilize(new_state0, 0, :topics)
    {:noreply, new_state}
  end

  def handle_cast(_cast, r_state() = state) do
    {:noreply, state}
  end

  def code_change(_old_vsn, r_state() = state, _extra) do
    {:ok, state}
  end

  def terminate(
        reason,
        r_state(connection: connection, group_id: group_id, member_id: member_id) = state
      ) do
    log(state, :info, 'Leaving group, reason: ~p\n', [reason])
    body = [{:group_id, group_id}, {:member_id, member_id}]
    _ = try_commit_offsets(state)
    request = :kpro.make_request(:leave_group, _V = 0, body)

    try do
      _ = send_sync(connection, request, 1000)
      :ok
    catch
      _, _ ->
        :ok
    end
  end

  defp discover_coordinator(
         r_state(client: client, connection: connection0, group_id: group_id) = state
       ) do
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
        connConfig = Map.put(conn_config0, :client_id, client_id)

        connection =
          (fn ->
             case :kpro.connect(endpoint, connConfig) do
               {:ok, result} ->
                 result

               {:error, reason} ->
                 throw(reason)
             end
           end).()

        {:ok, r_state(state, connection: connection)}
    end
  end

  defp is_already_connected(r_state(connection: conn), _) when not is_pid(conn) do
    false
  end

  defp is_already_connected(r_state(connection: conn), {host, port}) do
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

  defp do_stabilize([], _retry_fun, state) do
    {:ok, state}
  end

  defp do_stabilize([f | rest], retry_fun, state) do
    try do
      {:ok, r_state() = new_state} = f.(state)
      do_stabilize(rest, retry_fun, new_state)
    catch
      reason ->
        retry_fun.(state, reason)
    end
  end

  defp maybe_reset_member_id(state, reason) do
    case should_reset_member_id(reason) do
      true ->
        r_state(state, member_id: <<>>)

      false ->
        state
    end
  end

  defp join_group(
         r_state(
           group_id: group_id,
           member_id: member_id0,
           topics: topics,
           connection: connection,
           session_timeout_seconds: session_timeout_sec,
           rebalance_timeout_seconds: rebalance_timeout_sec,
           protocol_name: protocol_name,
           member_module: memberModule,
           member_pid: member_pid
         ) = state0
       ) do
    meta = [
      {:version, 0},
      {:topics, topics},
      {:user_data,
       user_data(
         memberModule,
         member_pid
       )}
    ]

    protocol = [{:name, protocol_name}, {:metadata, meta}]
    sessionTimeout = :timer.seconds(session_timeout_sec)
    rebalanceTimeout = :timer.seconds(rebalance_timeout_sec)

    body = [
      {:group_id, group_id},
      {:session_timeout_ms, sessionTimeout},
      {:rebalance_timeout_ms, rebalanceTimeout},
      {:member_id, member_id0},
      {:protocol_type, "consumer"},
      {:protocols, [protocol]}
    ]

    req = BrodKafkaRequest.join_group(connection, body)
    rsp_body = send_sync(connection, req, sessionTimeout)
    generation_id = :kpro.find(:generation_id, rsp_body)
    leader_id = :kpro.find(:leader, rsp_body)
    member_id = :kpro.find(:member_id, rsp_body)
    members0 = :kpro.find(:members, rsp_body)
    members1 = translate_members(members0)
    members = ensure_leader_at_hd(leader_id, members1)
    isGroupLeader = leader_id === member_id

    state =
      r_state(state0,
        member_id: member_id,
        leader_id: leader_id,
        generation_id: generation_id,
        members: members
      )

    log(state, :info, 'elected=~p', [isGroupLeader])
    {:ok, state}
  end

  defp sync_group(
         r_state(
           group_id: group_id,
           generation_id: generation_id,
           member_id: member_id,
           connection: connection,
           member_pid: member_pid,
           member_module: memberModule
         ) = state
       ) do
    reqBody = [
      {:group_id, group_id},
      {:generation_id, generation_id},
      {:member_id, member_id},
      {:assignments, assign_partitions(state)}
    ]

    syncReq =
      BrodKafkaRequest.sync_group(
        connection,
        reqBody
      )

    rsp_body = send_sync(connection, syncReq)
    assignment = :kpro.find(:assignment, rsp_body)

    topicAssignments =
      get_topic_assignments(
        state,
        assignment
      )

    :ok =
      memberModule.assignments_received(member_pid, member_id, generation_id, topicAssignments)

    new_state = r_state(state, is_in_group: true)
    log(new_state, :info, 'assignments received:~s', [format_assignments(topicAssignments)])
    start_offset_commit_timer(new_state)
  end

  defp handle_ack(state, generation_id, _topic, _partition, _Offset)
       when generation_id < r_state(state, :generation_id) do
    state
  end

  defp handle_ack(
         r_state(acked_offsets: acked_offsets) = state,
         _GenerationId,
         topic,
         partition,
         offset
       ) do
    newAckedOffsets =
      merge_acked_offsets(
        acked_offsets,
        [{{topic, partition}, offset}]
      )

    r_state(state, acked_offsets: newAckedOffsets)
  end

  defp merge_acked_offsets(acked_offsets, offsetsToAck) do
    :lists.ukeymerge(1, offsetsToAck, acked_offsets)
  end

  defp format_assignments([]) do
    '[]'
  end

  defp format_assignments(assignments) do
    groupped =
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

  defp try_commit_offsets(r_state() = state) do
    try do
      {:ok, r_state()} = do_commit_offsets(state)
    catch
      _, _ ->
        {:ok, state}
    end
  end

  defp do_commit_offsets(state) do
    {:ok, new_state} = do_commit_offsets_(state)
    start_offset_commit_timer(new_state)
  end

  defp do_commit_offsets_(r_state(acked_offsets: []) = state) do
    {:ok, state}
  end

  defp do_commit_offsets_(r_state(offset_commit_policy: :consumer_managed) = state) do
    {:ok, state}
  end

  defp do_commit_offsets_(
         r_state(
           group_id: group_id,
           member_id: member_id,
           generation_id: generation_id,
           connection: connection,
           offset_retention_seconds: offset_retention_secs,
           acked_offsets: acked_offsets
         ) = state
       ) do
    metadata = make_offset_commit_metadata()

    topicOffsets0 =
      BrodUtils.group_per_key(
        fn {{topic, partition}, offset} ->
          partitionOffset = [
            {:partition_index, partition},
            {:committed_offset, offset + 1},
            {:committed_metadata, metadata}
          ]

          {topic, partitionOffset}
        end,
        acked_offsets
      )

    topicOffsets =
      :lists.map(
        fn {topic, partition_offsets} ->
          [{:name, topic}, {:partitions, partition_offsets}]
        end,
        topicOffsets0
      )

    retention =
      case is_default_offset_retention(offset_retention_secs) do
        true ->
          -1

        false ->
          :timer.seconds(offset_retention_secs)
      end

    reqBody = [
      {:group_id, group_id},
      {:generation_id, generation_id},
      {:member_id, member_id},
      {:retention_time_ms, retention},
      {:topics, topicOffsets}
    ]

    req =
      BrodKafkaRequest.offset_commit(
        connection,
        reqBody
      )

    rsp_body = send_sync(connection, req)
    topics = :kpro.find(:topics, rsp_body)
    :ok = assert_commit_response(topics)
    new_state = r_state(state, acked_offsets: [])
    {:ok, new_state}
  end

  defp assign_partitions(state)
       when r_state(state, :leader_id) === r_state(state, :member_id) do
    r_state(
      client: client,
      members: members,
      partition_assignment_strategy: strategy,
      member_pid: member_pid,
      member_module: memberModule
    ) = state

    all_topics = all_topics(members)

    all_partitions =
      for topic <- all_topics,
          partition <- get_partitions(client, topic) do
        {topic, partition}
      end

    assignments =
      case strategy === :callback_implemented do
        true ->
          memberModule.assign_partitions(member_pid, members, all_partitions)

        false ->
          do_assign_partitions(strategy, members, all_partitions)
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
          {:assignment,
           [{:version, 0}, {:topic_partitions, partition_assignments}, {:user_data, <<>>}]}
        ]
      end,
      assignments
    )
  end

  defp assign_partitions(r_state()) do
    []
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
    :lists.map(
      fn member ->
        member_id = :kpro.find(:member_id, member)
        meta = :kpro.find(:metadata, member)
        version = :kpro.find(:version, meta)
        topics = :kpro.find(:topics, meta)
        userData = :kpro.find(:user_data, meta)

        {member_id,
         r_kafka_group_member_metadata(version: version, topics: topics, user_data: userData)}
      end,
      members
    )
  end

  defp all_topics(members) do
    :lists.usort(
      :lists.append(
        :lists.map(
          fn {_MemberId, m} ->
            r_kafka_group_member_metadata(m, :topics)
          end,
          members
        )
      )
    )
  end

  defp get_partitions(client, topic) do
    count =
      (fn ->
         case BrodClient.get_partitions_count(
                client,
                topic
              ) do
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
      subscribed_topics = r_kafka_group_member_metadata(m, :topics)

      is_valid_assignment = fn topic, _Partition ->
        :lists.member(topic, subscribed_topics)
      end

      {member_id, is_valid_assignment, []}
    end

    memberAssignment = :lists.map(f, members)

    for {member_id, _ValidationFun, assignments} <-
          roundrobin_assign_loop(all_partitions, memberAssignment, []) do
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
        newTopics = :orddict.append_list(topic, [partition], assigned_topics)
        member = {member_id, is_valid_assignment, newTopics}
        roundrobin_assign_loop(rest, pending_members, [member | assigned_members])

      false ->
        roundrobin_assign_loop(topic_partitions, pending_members, [member0 | assigned_members])
    end
  end

  defp resolve_begin_offsets([], _CommittedOffsets, _IsConsumerManaged) do
    []
  end

  defp resolve_begin_offsets([{topic, partition} | rest], committed_offsets, is_consumer_managed) do
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

    assignment =
      r_brod_received_assignment(topic: topic, partition: partition, begin_offset: begin_offset)

    [assignment | resolve_begin_offsets(rest, committed_offsets, is_consumer_managed)]
  end

  defp resolve_special_offset(0) do
    :earliest
  end

  defp resolve_special_offset(other) do
    other
  end

  defp start_heartbeat_timer(hb_rate_sec) do
    :erlang.send_after(:timer.seconds(hb_rate_sec), self(), :lo_cmd_send_heartbeat)
    :ok
  end

  defp start_offset_commit_timer(r_state(offset_commit_timer: old_timer) = state) do
    r_state(
      offset_commit_policy: policy,
      offset_commit_interval_seconds: seconds
    ) = state

    case policy do
      :consumer_managed ->
        {:ok, state}

      :commit_to_kafka_v2 ->
        is_reference(old_timer) and :erlang.cancel_timer(old_timer)

        receive do
          :lo_cmd_commit_offsets ->
            :ok
        after
          0 ->
            :ok
        end

        timeout = :timer.seconds(seconds)
        timer = :erlang.send_after(timeout, self(), :lo_cmd_commit_offsets)
        {:ok, r_state(state, offset_commit_timer: timer)}
    end
  end

  defp maybe_send_heartbeat(
         r_state(
           is_in_group: true,
           group_id: group_id,
           member_id: member_id,
           generation_id: generation_id,
           connection: connection
         ) = state
       ) do
    reqBody = [{:group_id, group_id}, {:generation_id, generation_id}, {:member_id, member_id}]
    req = :kpro.make_request(:heartbeat, 0, reqBody)
    :ok = :kpro.request_async(connection, req)
    new_state = r_state(state, hb_ref: {kpro_req(req, :ref), :os.timestamp()})
    {:ok, new_state}
  end

  defp maybe_send_heartbeat(r_state() = state) do
    {:ok, r_state(state, hb_ref: :undefined)}
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

  defp log(
         r_state(group_id: group_id, generation_id: generation_id, member_pid: member_pid),
         level,
         fmt,
         args
       ) do
    case :logger.allow(level, :brod_group_coordinator) do
      true ->
        :erlang.apply(:logger, :macro_log, [
          %{
            mfa: {:brod_group_coordinator, :log, 4},
            line: 1099,
            file: '../brod/src/brod_group_coordinator.erl'
          },
          level,
          'Group member (~s,coor=~p,cb=~p,generation=~p):\n' ++ fmt,
          [
            group_id,
            self(),
            member_pid,
            generation_id
            | args
          ],
          %{domain: [:brod]}
        ])

      false ->
        :ok
    end
  end

  defp make_offset_commit_metadata() do
    bin(['+1/', coordinator_id()])
  end

  defp user_data(module, pid) do
    BrodUtils.optional_callback(module, :user_data, [pid], <<>>)
  end

  defp make_group_connection_client_id() do
    coordinator_id()
  end

  defp coordinator_id() do
    bin(:io_lib.format('~p/~p', [node(), self()]))
  end

  defp bin(x) do
    :erlang.iolist_to_binary(x)
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
end
