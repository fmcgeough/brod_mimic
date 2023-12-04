defmodule BrodMimic.Consumer do
  use GenServer

  import Bitwise

  import Record, only: [defrecord: 2, extract: 2]

  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.KafkaRequest, as: BrodKafkaRequest
  alias BrodMimic.Utils, as: BrodUtils

  defrecord(:kpro_req, extract(:kpro_req, from_lib: "kafka_protocol/include/kpro.hrl"))
  defrecord(:kpro_rsp, extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl"))

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

  Record.defrecord(:r_pending_acks, :pending_acks, count: 0, bytes: 0, queue: :queue.new())

  Record.defrecord(:r_state, :state,
    bootstrap: :undefined,
    connection: :undefined,
    topic: :undefined,
    partition: :undefined,
    begin_offset: :undefined,
    max_wait_time: :undefined,
    min_bytes: :undefined,
    max_bytes_orig: :undefined,
    sleep_timeout: :undefined,
    prefetch_count: :undefined,
    last_req_ref: :undefined,
    subscriber: :undefined,
    subscriber_mref: :undefined,
    pending_acks: :undefined,
    is_suspended: :undefined,
    offset_reset_policy: :undefined,
    avg_bytes: :undefined,
    max_bytes: :undefined,
    size_stat_window: :undefined,
    prefetch_bytes: :undefined,
    connection_mref: :undefined,
    isolation_level: :undefined
  )

  def start_link(bootstrap, topic, partition, config) do
    start_link(bootstrap, topic, partition, config, [])
  end

  def start_link(bootstrap, topic, partition, config, debug) do
    args = {bootstrap, topic, partition, config}
    GenServer.start_link(__MODULE__, args, [{:debug, debug}])
  end

  def stop(pid) do
    safe_gen_call(pid, :stop, :infinity)
  end

  def stop_maybe_kill(pid, timeout) do
    try do
      GenServer.call(pid, :stop, timeout)
    catch
      :exit, {:noproc, _} ->
        :ok

      :exit, {:timeout, _} ->
        :erlang.exit(pid, :kill)
        :ok
    end
  end

  def subscribe(pid, subscriber_pid, consumer_options) do
    safe_gen_call(pid, {:subscribe, subscriber_pid, consumer_options}, :infinity)
  end

  def unsubscribe(pid, subscriber_pid) do
    safe_gen_call(pid, {:unsubscribe, subscriber_pid}, :infinity)
  end

  def ack(pid, offset) do
    GenServer.cast(pid, {:ack, offset})
  end

  def debug(pid, :none) do
    do_debug(pid, :no_debug)
  end

  def debug(pid, :print) do
    do_debug(pid, {:trace, true})
  end

  def debug(pid, file) when is_list(file) do
    do_debug(pid, {:log_to_file, file})
  end

  def get_connection(pid) do
    GenServer.call(pid, :get_connection)
  end

  def handle_info(
        :init_connection,
        r_state(subscriber: subscriber) = state0
      ) do
    case BrodUtils.is_pid_alive(subscriber) and maybe_init_connection(state0) do
      false ->
        {:noreply, state0}

      {:ok, state1} ->
        state = maybe_send_fetch_request(state1)
        {:noreply, state}

      {{:error, _reason}, state} ->
        :ok = maybe_send_init_connection(state)
        {:noreply, state}
    end
  end

  def handle_info({:msg, _Pid, rsp}, state) do
    handle_fetch_response(rsp, state)
  end

  def handle_info(:send_fetch_request, state0) do
    state = maybe_send_fetch_request(state0)
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, _MonitorRef, :process, pid, _reason},
        r_state(subscriber: pid) = state
      ) do
    newState =
      reset_buffer(
        r_state(state,
          subscriber: :undefined,
          subscriber_mref: :undefined
        )
      )

    {:noreply, newState}
  end

  def handle_info(
        {:DOWN, _MonitorRef, :process, pid, _reason},
        r_state(connection: pid) = state
      ) do
    {:noreply, handle_conn_down(state)}
  end

  def handle_info(
        {:EXIT, pid, _reason},
        r_state(connection: pid) = state
      ) do
    {:noreply, handle_conn_down(state)}
  end

  def handle_info(info, state) do
    case :logger.allow(:warning, :brod_consumer) do
      true ->
        :erlang.apply(:logger, :macro_log, [
          %{
            mfa: {:brod_consumer, :handle_info, 2},
            line: 368,
            file: '../brod/src/brod_consumer.erl'
          },
          :warning,
          '~p ~p got unexpected info: ~p',
          [:brod_consumer, self(), info],
          %{domain: [:brod]}
        ])

      false ->
        :ok
    end

    {:noreply, state}
  end

  def handle_call(:get_connection, _From, r_state(connection: c) = state) do
    {:reply, c, state}
  end

  def handle_call({:subscribe, pid, options}, _From, r_state(subscriber: subscriber) = state0) do
    case not BrodUtils.is_pid_alive(subscriber) or subscriber === pid do
      true ->
        case maybe_init_connection(state0) do
          {:ok, state} ->
            handle_subscribe_call(pid, options, state)

          {{:error, reason}, state} ->
            {:reply, {:error, reason}, state}
        end

      false ->
        {:reply, {:error, {:already_subscribed_by, subscriber}}, state0}
    end
  end

  def handle_call(
        {:unsubscribe, subscriber_pid},
        _From,
        r_state(
          subscriber: current_subscriber,
          subscriber_mref: mref
        ) = state
      ) do
    case subscriber_pid === current_subscriber do
      true ->
        is_reference(mref) and :erlang.demonitor(mref, [:flush])

        newState =
          r_state(state,
            subscriber: :undefined,
            subscriber_mref: :undefined
          )

        {:reply, :ok, reset_buffer(newState)}

      false ->
        {:reply, {:error, :ignored}, state}
    end
  end

  def handle_call(:stop, _From, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(call, _From, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def handle_cast({:ack, offset}, r_state(pending_acks: pending_acks) = state0) do
    newPendingAcks = handle_ack(pending_acks, offset)
    state1 = r_state(state0, pending_acks: newPendingAcks)
    state = maybe_send_fetch_request(state1)
    {:noreply, state}
  end

  def handle_cast(cast, state) do
    case :logger.allow(:warning, :brod_consumer) do
      true ->
        :erlang.apply(:logger, :macro_log, [
          %{
            mfa: {:brod_consumer, :handle_cast, 2},
            line: 417,
            file: '../brod/src/brod_consumer.erl'
          },
          :warning,
          '~p ~p got unexpected cast: ~p',
          [:brod_consumer, self(), cast],
          %{domain: [:brod]}
        ])

      false ->
        :ok
    end

    {:noreply, state}
  end

  def terminate(
        reason,
        r_state(bootstrap: bootstrap, topic: topic, partition: partition, connection: connection)
      ) do
    isShared = is_shared_conn(bootstrap)
    isNormal = BrodUtils.is_normal_reason(reason)

    isShared and isNormal and
      BrodClient.deregister_consumer(
        bootstrap,
        topic,
        partition
      )

    case not isShared and is_pid(connection) do
      true ->
        :kpro.close_connection(connection)

      false ->
        :ok
    end

    isNormal or
      case :logger.allow(
             :error,
             :brod_consumer
           ) do
        true ->
          :erlang.apply(:logger, :macro_log, [
            %{
              mfa: {:brod_consumer, :terminate, 2},
              line: 438,
              file: '../brod/src/brod_consumer.erl'
            },
            :error,
            'Consumer ~s-~w terminate reason: ~p',
            [topic, partition, reason],
            %{domain: [:brod]}
          ])

        false ->
          :ok
      end

    :ok
  end

  def code_change(_OldVsn, state, _Extra) do
    {:ok, state}
  end

  defp handle_conn_down(state0) do
    state =
      r_state(state0,
        connection: :undefined,
        connection_mref: :undefined
      )

    :ok = maybe_send_init_connection(state)
    state
  end

  defp do_debug(pid, debug) do
    {:ok, _} = :gen.call(pid, :system, {:debug, debug}, :infinity)
    :ok
  end

  defp handle_fetch_response(r_kpro_rsp(), r_state(subscriber: :undefined) = state0) do
    state = r_state(state0, last_req_ref: :undefined)
    {:noreply, state}
  end

  defp handle_fetch_response(r_kpro_rsp(ref: ref1), r_state(last_req_ref: ref2) = state)
       when ref1 !== ref2 do
    {:noreply, state}
  end

  defp handle_fetch_response(
         r_kpro_rsp(ref: ref) = rsp,
         r_state(topic: topic, partition: partition, last_req_ref: ref) = state0
       ) do
    state = r_state(state0, last_req_ref: :undefined)

    case BrodUtils.parse_rsp(rsp) do
      {:ok, %{header: header, batches: batches}} ->
        handle_batches(header, batches, state)

      {:error, errorCode} ->
        error = r_kafka_fetch_error(topic: topic, partition: partition, error_code: errorCode)
        handle_fetch_error(error, state)
    end
  end

  defp add_pending_acks(pending_acks, messages) do
    :lists.foldl(&add_pending_ack/2, pending_acks, messages)
  end

  defp add_pending_ack(
         r_kafka_message(offset: offset, key: key, value: value),
         r_pending_acks(queue: queue, count: count, bytes: bytes) = pending_acks
       ) do
    size = :erlang.size(key) + :erlang.size(value)
    newQueue = :queue.in({offset, size}, queue)
    r_pending_acks(pending_acks, queue: newQueue, count: count + 1, bytes: bytes + size)
  end

  defp maybe_shrink_max_bytes(
         r_state(
           size_stat_window: w,
           max_bytes_orig: maxBytesOrig
         ) = state,
         _
       )
       when w < 1 do
    r_state(state, max_bytes: maxBytesOrig)
  end

  defp maybe_shrink_max_bytes(state0, messages) do
    r_state(
      prefetch_count: prefetchCount,
      max_bytes_orig: maxBytesOrig,
      max_bytes: maxBytes,
      avg_bytes: avgBytes
    ) =
      state =
      update_avg_size(
        state0,
        messages
      )

    estimatedSetSize = :erlang.round(prefetchCount * avgBytes)

    newMaxBytes =
      :erlang.max(
        estimatedSetSize,
        maxBytesOrig
      )

    r_state(state, max_bytes: :erlang.min(newMaxBytes, maxBytes))
  end

  defp update_avg_size(r_state() = state, []) do
    state
  end

  defp update_avg_size(
         r_state(
           avg_bytes: avgBytes,
           size_stat_window: windowSize
         ) = state,
         [r_kafka_message(key: key, value: value) | rest]
       ) do
    msgBytes = :erlang.size(key) + :erlang.size(value) + 40
    newAvgBytes = ((windowSize - 1) * avgBytes + msgBytes) / windowSize
    update_avg_size(r_state(state, avg_bytes: newAvgBytes), rest)
  end

  defp handle_fetch_error(
         r_kafka_fetch_error(error_code: errorCode) = error,
         r_state(
           topic: topic,
           partition: partition,
           subscriber: subscriber,
           connection_mref: mRef
         ) = state
       ) do
    case err_op(errorCode) do
      :reset_connection ->
        case :logger.allow(:info, :brod_consumer) do
          true ->
            :erlang.apply(:logger, :macro_log, [
              %{
                mfa: {:brod_consumer, :handle_fetch_error, 2},
                line: 614,
                file: '../brod/src/brod_consumer.erl'
              },
              :info,
              'Fetch error ~s-~p: ~p',
              [topic, partition, errorCode],
              %{domain: [:brod]}
            ])

          false ->
            :ok
        end

        is_reference(mRef) and :erlang.demonitor(mRef)

        newState =
          r_state(state,
            connection: :undefined,
            connection_mref: :undefined
          )

        :ok = maybe_send_init_connection(newState)
        {:noreply, newState}

      :retry ->
        {:noreply, maybe_send_fetch_request(state)}

      :stop ->
        :ok = cast_to_subscriber(subscriber, error)

        case :logger.allow(:error, :brod_consumer) do
          true ->
            :erlang.apply(:logger, :macro_log, [
              %{
                mfa: {:brod_consumer, :handle_fetch_error, 2},
                line: 631,
                file: '../brod/src/brod_consumer.erl'
              },
              :error,
              'Consumer ~s-~p shutdown\nreason: ~p',
              [topic, partition, errorCode],
              %{domain: [:brod]}
            ])

          false ->
            :ok
        end

        {:stop, :normal, state}

      :reset_offset ->
        handle_reset_offset(state, error)

      :restart ->
        :ok = cast_to_subscriber(subscriber, error)
        {:stop, {:restart, errorCode}, state}
    end
  end

  defp handle_reset_offset(
         r_state(
           subscriber: subscriber,
           offset_reset_policy: :reset_by_subscriber
         ) = state,
         error
       ) do
    :ok = cast_to_subscriber(subscriber, error)

    case :logger.allow(:info, :brod_consumer) do
      true ->
        :erlang.apply(:logger, :macro_log, [
          %{
            mfa: {:brod_consumer, :handle_reset_offset, 2},
            line: 646,
            file: '../brod/src/brod_consumer.erl'
          },
          :info,
          '~p ~p consumer is suspended, waiting for subscriber ~p to resubscribe with new begin_offset',
          [:brod_consumer, self(), subscriber],
          %{domain: [:brod]}
        ])

      false ->
        :ok
    end

    {:noreply, r_state(state, is_suspended: true)}
  end

  defp handle_reset_offset(
         r_state(offset_reset_policy: policy) = state,
         _Error
       ) do
    case :logger.allow(:info, :brod_consumer) do
      true ->
        :erlang.apply(:logger, :macro_log, [
          %{
            mfa: {:brod_consumer, :handle_reset_offset, 2},
            line: 651,
            file: '../brod/src/brod_consumer.erl'
          },
          :info,
          '~p ~p offset out of range, applying reset policy ~p',
          [:brod_consumer, self(), policy],
          %{domain: [:brod]}
        ])

      false ->
        :ok
    end

    beginOffset =
      case policy do
        :reset_to_earliest ->
          :earliest

        :reset_to_latest ->
          :latest
      end

    state1 =
      r_state(state,
        begin_offset: beginOffset,
        pending_acks: r_pending_acks()
      )

    {:ok, state2} = resolve_begin_offset(state1)
    newState = maybe_send_fetch_request(state2)
    {:noreply, newState}
  end

  defp handle_ack(
         r_pending_acks(queue: queue, bytes: bytes, count: count) = pending_acks,
         offset
       ) do
    case :queue.out(queue) do
      {{:value, {o, size}}, queue1} when o <= offset ->
        handle_ack(
          r_pending_acks(pending_acks, queue: queue1, count: count - 1, bytes: bytes - size),
          offset
        )

      _ ->
        pending_acks
    end
  end

  defp cast_to_subscriber(pid, msg) do
    try do
      send(pid, {self(), msg})
      :ok
    catch
      _, _ ->
        :ok
    end
  end

  defp maybe_delay_fetch_request(r_state(sleep_timeout: t) = state) when t > 0 do
    _ = :erlang.send_after(t, self(), :send_fetch_request)
    state
  end

  defp maybe_delay_fetch_request(state) do
    maybe_send_fetch_request(state)
  end

  defp maybe_send_fetch_request(r_state(subscriber: :undefined) = state) do
    state
  end

  defp maybe_send_fetch_request(r_state(connection: :undefined) = state) do
    state
  end

  defp maybe_send_fetch_request(r_state(is_suspended: true) = state) do
    state
  end

  defp maybe_send_fetch_request(r_state(last_req_ref: r) = state) when is_reference(r) do
    state
  end

  defp maybe_send_fetch_request(
         r_state(
           pending_acks: r_pending_acks(count: count, bytes: bytes),
           prefetch_count: prefetchCount,
           prefetch_bytes: prefetchBytes
         ) = state
       ) do
    case count > prefetchCount and bytes > prefetchBytes do
      true ->
        state

      false ->
        send_fetch_request(state)
    end
  end

  defp send_fetch_request(
         r_state(
           begin_offset: beginOffset,
           connection: connection
         ) = state
       ) do
    (is_integer(beginOffset) and beginOffset >= 0) or
      :erlang.error({:bad_begin_offset, beginOffset})

    request =
      BrodKafkaRequest.fetch(
        connection,
        r_state(state, :topic),
        r_state(state, :partition),
        r_state(state, :begin_offset),
        r_state(state, :max_wait_time),
        r_state(state, :min_bytes),
        r_state(state, :max_bytes),
        r_state(state, :isolation_level)
      )

    case :kpro.request_async(connection, request) do
      :ok ->
        r_state(state, last_req_ref: kpro_req(request, :ref))

      {:error, {:connection_down, _reason}} ->
        state
    end
  end

  defp handle_subscribe_call(pid, options, r_state(subscriber_mref: oldMref) = state0) do
    case update_options(options, state0) do
      {:ok, state1} ->
        is_reference(oldMref) and
          :erlang.demonitor(
            oldMref,
            [:flush]
          )

        mref = :erlang.monitor(:process, pid)

        state2 =
          r_state(state1,
            subscriber: pid,
            subscriber_mref: mref
          )

        state3 = reset_buffer(state2)
        state4 = r_state(state3, is_suspended: false)
        state = maybe_send_fetch_request(state4)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state0}
    end
  end

  defp update_options(
         options,
         r_state(begin_offset: oldBeginOffset) = state
       ) do
    f = fn name, default ->
      :proplists.get_value(name, options, default)
    end

    newBeginOffset = f.(:begin_offset, oldBeginOffset)

    offsetResetPolicy =
      f.(
        :offset_reset_policy,
        r_state(state, :offset_reset_policy)
      )

    state1 =
      r_state(state,
        begin_offset: newBeginOffset,
        min_bytes: f.(:min_bytes, r_state(state, :min_bytes)),
        max_bytes_orig:
          f.(
            :max_bytes,
            r_state(state, :max_bytes_orig)
          ),
        max_wait_time:
          f.(
            :max_wait_time,
            r_state(state, :max_wait_time)
          ),
        sleep_timeout:
          f.(
            :sleep_timeout,
            r_state(state, :sleep_timeout)
          ),
        prefetch_count:
          f.(
            :prefetch_count,
            r_state(state, :prefetch_count)
          ),
        prefetch_bytes:
          f.(
            :prefetch_bytes,
            r_state(state, :prefetch_bytes)
          ),
        offset_reset_policy: offsetResetPolicy,
        max_bytes: f.(:max_bytes, r_state(state, :max_bytes)),
        size_stat_window:
          f.(
            :size_stat_window,
            r_state(state, :size_stat_window)
          )
      )

    newState =
      case newBeginOffset !== oldBeginOffset do
        true ->
          r_state(state1, pending_acks: r_pending_acks())

        false ->
          state1
      end

    resolve_begin_offset(newState)
  end

  defp resolve_begin_offset(
         r_state(
           begin_offset: beginOffset,
           connection: connection,
           topic: topic,
           partition: partition
         ) = state
       )
       when beginOffset === :earliest or beginOffset === :latest or beginOffset === -2 or
              beginOffset === -1 do
    case resolve_offset(connection, topic, partition, beginOffset) do
      {:ok, newBeginOffset} ->
        {:ok, r_state(state, begin_offset: newBeginOffset)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_begin_offset(state) do
    {:ok, state}
  end

  defp resolve_offset(connection, topic, partition, beginOffset) do
    try do
      BrodUtils.resolve_offset(connection, topic, partition, beginOffset)
    catch
      reason ->
        {:error, reason}
    end
  end

  defp reset_buffer(
         r_state(
           pending_acks: r_pending_acks(queue: queue),
           begin_offset: beginOffset0
         ) = state
       ) do
    beginOffset =
      case :queue.peek(queue) do
        {:value, {offset, _}} ->
          offset

        :empty ->
          beginOffset0
      end

    r_state(state,
      begin_offset: beginOffset,
      pending_acks: r_pending_acks(),
      last_req_ref: :undefined
    )
  end

  defp safe_gen_call(server, call, timeout) do
    try do
      GenServer.call(server, call, timeout)
    catch
      :exit, {reason, _} ->
        {:error, reason}
    end
  end

  defp maybe_init_connection(
         r_state(bootstrap: bootstrap, topic: topic, partition: partition, connection: :undefined) =
           state0
       ) do
    case connect_leader(bootstrap, topic, partition) do
      {:ok, connection} ->
        mref =
          case is_shared_conn(bootstrap) do
            true ->
              :erlang.monitor(:process, connection)

            false ->
              :undefined
          end

        state =
          r_state(state0, last_req_ref: :undefined, connection: connection, connection_mref: mref)

        {:ok, state}

      {:error, reason} ->
        {{:error, {:connect_leader, reason}}, state0}
    end
  end

  defp maybe_init_connection(state) do
    {:ok, state}
  end

  defp connect_leader(clientPid, topic, partition)
       when is_pid(clientPid) do
    BrodClient.get_leader_connection(clientPid, topic, partition)
  end

  defp connect_leader(endpoints, topic, partition)
       when is_list(endpoints) do
    connect_leader({endpoints, []}, topic, partition)
  end

  defp connect_leader({endpoints, connCfg}, topic, partition) do
    :kpro.connect_partition_leader(endpoints, connCfg, topic, partition)
  end

  defp maybe_send_init_connection(r_state(subscriber: subscriber)) do
    timeout = 1000

    BrodUtils.is_pid_alive(subscriber) and
      :erlang.send_after(
        timeout,
        self(),
        :init_connection
      )

    :ok
  end

  defp is_shared_conn(bootstrap) do
    is_pid(bootstrap)
  end
end
