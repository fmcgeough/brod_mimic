defmodule BrodMimic.Consumer do
  @moduledoc false

  use BrodMimic.Macros
  use GenServer

  import Record, only: [defrecord: 2, extract: 2]

  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.KafkaRequest, as: BrodKafkaRequest
  alias BrodMimic.Utils, as: BrodUtils

  require Logger

  @fetch_error "Fetch error ~s-~p: ~p"

  @default_min_bytes 0
  # 1 MB
  @default_max_bytes 1_048_576
  # 10 sec
  @default_max_wait_time 10_000
  # 1 sec
  @default_sleep_timeout 1000
  @default_prefetch_count 10
  # 100 KB
  @default_prefetch_bytes 102_400
  @default_offset_reset_policy :reset_by_subscriber
  @connection_retry_delay_ms 1_000
  @default_offset_reset_policy :reset_by_subscriber
  @default_isolation_level :read_committed
  @default_avg_window 5
  @default_begin_offset 0

  defrecord(:kpro_req, extract(:kpro_req, from_lib: "kafka_protocol/include/kpro.hrl"))
  defrecord(:kpro_rsp, extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl"))
  defrecord(:kafka_message, extract(:kafka_message, from_lib: "kafka_protocol/include/kpro.hrl"))

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
    GenServer.call(pid, :stop, timeout)
  catch
    :exit, {:noproc, _} ->
      :ok

    :exit, {:timeout, _} ->
      :erlang.exit(pid, :kill)
      :ok
  end

  def init({bootstrap, topic, partition, config}) do
    :erlang.process_flag(:trap_exit, true)

    cfg = fn name, default ->
      :proplists.get_value(name, config, default)
    end

    min_bytes = cfg.(:min_bytes, @default_min_bytes)
    max_bytes = cfg.(:max_bytes, @default_max_bytes)
    max_wait_time = cfg.(:max_wait_time, @default_max_wait_time)
    sleep_timeout = cfg.(:sleep_timeout, @default_sleep_timeout)
    prefetch_count = :erlang.max(cfg.(:prefetch_count, @default_prefetch_count), 0)
    prefetch_bytes = :erlang.max(cfg.(:prefetch_bytes, @default_prefetch_bytes), 0)
    begin_offset = cfg.(:begin_offset, @default_begin_offset)
    offset_reset_policy = cfg.(:offset_reset_policy, @default_offset_reset_policy)
    isolation_level = cfg.(:isolation_level, @default_isolation_level)

    # If bootstrap is a client pid, register self to the client
    case is_shared_conn(bootstrap) do
      true ->
        :ok = BrodClient.register_consumer(bootstrap, topic, partition)

      false ->
        :ok
    end

    {:ok,
     r_state(
       bootstrap: bootstrap,
       topic: topic,
       partition: partition,
       begin_offset: begin_offset,
       max_wait_time: max_wait_time,
       min_bytes: min_bytes,
       max_bytes_orig: max_bytes,
       sleep_timeout: sleep_timeout,
       prefetch_count: prefetch_count,
       prefetch_bytes: prefetch_bytes,
       connection: :undef,
       pending_acks: r_pending_acks(),
       is_suspended: false,
       offset_reset_policy: offset_reset_policy,
       avg_bytes: 0,
       max_bytes: max_bytes,
       size_stat_window: cfg.(:size_stat_window, @default_avg_window),
       connection_mref: :undef,
       isolation_level: isolation_level
     )}
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

  def handle_info({:msg, _pid, rsp}, state) do
    handle_fetch_response(rsp, state)
  end

  def handle_info(:send_fetch_request, state0) do
    state = maybe_send_fetch_request(state0)
    {:noreply, state}
  end

  def handle_info({:DOWN, _monitor_ref, :process, pid, _reason}, r_state(subscriber: pid) = state) do
    new_state = reset_buffer(r_state(state, subscriber: :undefined, subscriber_mref: :undefined))
    {:noreply, new_state}
  end

  def handle_info({:DOWN, _monitor_ref, :process, pid, _reason}, r_state(connection: pid) = state) do
    {:noreply, handle_conn_down(state)}
  end

  def handle_info({:EXIT, pid, _reason}, r_state(connection: pid) = state) do
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

  def handle_call(:get_connection, _from, r_state(connection: c) = state) do
    {:reply, c, state}
  end

  def handle_call({:subscribe, pid, options}, _from, r_state(subscriber: subscriber) = state0) do
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
        _from,
        r_state(
          subscriber: current_subscriber,
          subscriber_mref: mref
        ) = state
      ) do
    case subscriber_pid === current_subscriber do
      true ->
        is_reference(mref) and :erlang.demonitor(mref, [:flush])

        new_state =
          r_state(state,
            subscriber: :undefined,
            subscriber_mref: :undefined
          )

        {:reply, :ok, reset_buffer(new_state)}

      false ->
        {:reply, {:error, :ignored}, state}
    end
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def handle_cast({:ack, offset}, r_state(pending_acks: pending_acks) = state0) do
    new_pending_acks = handle_ack(pending_acks, offset)
    state1 = r_state(state0, pending_acks: new_pending_acks)
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
    is_shared = is_shared_conn(bootstrap)
    is_normal = BrodUtils.is_normal_reason(reason)

    is_shared and is_normal and
      BrodClient.deregister_consumer(
        bootstrap,
        topic,
        partition
      )

    case not is_shared and is_pid(connection) do
      true ->
        :kpro.close_connection(connection)

      false ->
        :ok
    end

    is_normal or
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

  def code_change(_old_vsn, state, _extra) do
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

  defp handle_fetch_response(kpro_rsp(), r_state(subscriber: :undefined) = state0) do
    state = r_state(state0, last_req_ref: :undefined)
    {:noreply, state}
  end

  defp handle_fetch_response(kpro_rsp(ref: ref1), r_state(last_req_ref: ref2) = state)
       when ref1 !== ref2 do
    {:noreply, state}
  end

  defp handle_fetch_response(
         kpro_rsp(ref: ref) = rsp,
         r_state(topic: topic, partition: partition, last_req_ref: ref) = state0
       ) do
    state = r_state(state0, last_req_ref: :undefined)

    case BrodUtils.parse_rsp(rsp) do
      {:ok, %{header: header, batches: batches}} ->
        handle_batches(header, batches, state)

      {:error, error_code} ->
        error = r_kafka_fetch_error(topic: topic, partition: partition, error_code: error_code)
        handle_fetch_error(error, state)
    end
  end

  def handle_batches(:undef, [], state0) do
    # It is only possible to end up here in a incremental
    # fetch session, empty fetch response implies no
    # new messages to fetch, and no changes in partition
    # metadata (e.g. high watermark offset, or last stable offset) either.
    # Do not advance offset, try again (maybe after a delay) with
    # the last begin_offset in use.
    state = maybe_delay_fetch_request(state0)
    {:noreply, state}
  end

  def handle_batches(_header, {:incomplete_batch, size}, r_state(max_bytes: max_bytes) = state0) do
    # max_bytes is too small to fetch ONE complete batch
    true = size > max_bytes
    state1 = r_state(state0, max_bytes: size)
    state = maybe_send_fetch_request(state1)
    {:noreply, state}
  end

  def handle_batches(header, [], r_state(begin_offset: begin_offset) = state0) do
    stable_offset = BrodUtils.get_stable_offset(header)

    state =
      case begin_offset < stable_offset do
        true ->
          # There are chances that kafka may return empty message set
          # when messages are deleted from a compacted topic.
          # Since there is no way to know how big the 'hole' is
          # we can only bump begin_offset with +1 and try again.
          state1 = r_state(state0, begin_offset: begin_offset + 1)
          maybe_send_fetch_request(state1)

        false ->
          # we have either reached the end of a partition
          # or trying to read uncommitted messages
          # try to poll again (maybe after a delay)
          maybe_delay_fetch_request(state0)
      end

    {:noreply, state}
  end

  def handle_batches(
        header,
        batches,
        r_state(
          subscriber: subscriber,
          pending_acks: pending_acks,
          begin_offset: begin_offset,
          topic: topic,
          partition: partition
        ) = state0
      ) do
    stable_offset = BrodUtils.get_stable_offset(header)
    {new_begin_offset, messages} = BrodUtils.flatten_batches(begin_offset, header, batches)
    state1 = r_state(state0, begin_offset: new_begin_offset)

    state =
      case messages != [] do
        true ->
          # All messages are before requested offset, hence dropped
          state1

        false ->
          msg_set =
            r_kafka_message_set(
              topic: topic,
              partition: partition,
              high_wm_offset: stable_offset,
              messages: messages
            )

          :ok = cast_to_subscriber(subscriber, msg_set)
          new_pending_acks = add_pending_acks(pending_acks, messages)
          state2 = r_state(state1, pending_acks: new_pending_acks)
          maybe_shrink_max_bytes(state2, messages)
      end

    {:noreply, maybe_send_fetch_request(state)}
  end

  def add_pending_acks(pending_acks, messages) do
    :lists.foldl(&add_pending_ack/2, pending_acks, messages)
  end

  def add_pending_ack(
        kafka_message(offset: offset, key: key, value: value),
        r_pending_acks(queue: queue, count: count, bytes: bytes) = pending_acks
      ) do
    size = :erlang.size(key) + :erlang.size(value)
    new_queue = :queue.in({offset, size}, queue)
    r_pending_acks(pending_acks, queue: new_queue, count: count + 1, bytes: bytes + size)
  end

  defp maybe_shrink_max_bytes(
         r_state(
           size_stat_window: w,
           max_bytes_orig: max_bytes_orig
         ) = state,
         _
       )
       when w < 1 do
    r_state(state, max_bytes: max_bytes_orig)
  end

  defp maybe_shrink_max_bytes(state0, messages) do
    r_state(
      prefetch_count: prefetch_count,
      max_bytes_orig: max_bytes_orig,
      max_bytes: max_bytes,
      avg_bytes: avg_bytes
    ) =
      state =
      update_avg_size(
        state0,
        messages
      )

    estimated_set_size = :erlang.round(prefetch_count * avg_bytes)
    new_max_bytes = max(estimated_set_size, max_bytes_orig)

    r_state(state, max_bytes: :erlang.min(new_max_bytes, max_bytes))
  end

  def update_avg_size(r_state() = state, []) do
    state
  end

  def update_avg_size(
        r_state(
          avg_bytes: avg_bytes,
          size_stat_window: window_size
        ) = state,
        [kafka_message(key: key, value: value) | rest]
      ) do
    msg_bytes = :erlang.size(key) + :erlang.size(value) + 40
    new_avg_bytes = ((window_size - 1) * avg_bytes + msg_bytes) / window_size
    update_avg_size(r_state(state, avg_bytes: new_avg_bytes), rest)
  end

  def err_op(:request_timed_out), do: :retry
  def err_op(:invalid_topic_exception), do: :stop
  def err_op(:offset_out_of_range), do: :reset_offset
  def err_op(:leader_not_available), do: :reset_connection
  def err_op(:not_leader_for_partition), do: :reset_connection
  def err_op(:unknown_topic_or_partition), do: :reset_connection
  def err_op(_), do: :restart

  defp handle_fetch_error(
         r_kafka_fetch_error(error_code: error_code) = error,
         r_state(
           topic: topic,
           partition: partition,
           subscriber: subscriber,
           connection_mref: mref
         ) = state
       ) do
    case err_op(error_code) do
      :reset_connection ->
        Logger.info(fn -> log_string(@fetch_error, [topic, partition, error_code]) end, %{
          domain: [:brod]
        })

        is_reference(mref) and :erlang.demonitor(mref)

        new_state = r_state(state, connection: :undefined, connection_mref: :undefined)

        :ok = maybe_send_init_connection(new_state)
        {:noreply, new_state}

      :retry ->
        {:noreply, maybe_send_fetch_request(state)}

      :stop ->
        :ok = cast_to_subscriber(subscriber, error)

        case :logger.allow(:error, :brod_consumer) do
          true ->
            "Consumer ~s-~p shutdown\nreason: ~p"
            |> :io_lib.format([topic, partition, error_code])
            |> to_string()
            |> Logger.error(%{domain: [:brod]})

          false ->
            :ok
        end

        {:stop, :normal, state}

      :reset_offset ->
        handle_reset_offset(state, error)

      :restart ->
        :ok = cast_to_subscriber(subscriber, error)
        {:stop, {:restart, error_code}, state}
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

    begin_offset =
      case policy do
        :reset_to_earliest ->
          :earliest

        :reset_to_latest ->
          :latest
      end

    state1 =
      r_state(state,
        begin_offset: begin_offset,
        pending_acks: r_pending_acks()
      )

    {:ok, state2} = resolve_begin_offset(state1)
    new_state = maybe_send_fetch_request(state2)
    {:noreply, new_state}
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
    send(pid, {self(), msg})
    :ok
  catch
    _, _ ->
      :ok
  end

  defp maybe_delay_fetch_request(r_state(sleep_timeout: t) = state) when t > 0 do
    _ = Process.send_after(self(), :send_fetch_request, t)
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
           prefetch_count: prefetch_count,
           prefetch_bytes: prefetch_bytes
         ) = state
       ) do
    case count > prefetch_count and bytes > prefetch_bytes do
      true ->
        state

      false ->
        send_fetch_request(state)
    end
  end

  defp send_fetch_request(
         r_state(
           begin_offset: begin_offset,
           connection: connection
         ) = state
       ) do
    (is_integer(begin_offset) and begin_offset >= 0) or
      :erlang.error({:bad_begin_offset, begin_offset})

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

  defp handle_subscribe_call(pid, options, r_state(subscriber_mref: old_mref) = state0) do
    case update_options(options, state0) do
      {:ok, state1} ->
        if is_reference(old_mref) do
          :erlang.demonitor(old_mref, [:flush])
        end

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
         r_state(begin_offset: old_begin_offset) = state
       ) do
    f = fn name, default ->
      :proplists.get_value(name, options, default)
    end

    new_begin_offset = f.(:begin_offset, old_begin_offset)

    offset_reset_policy =
      f.(
        :offset_reset_policy,
        r_state(state, :offset_reset_policy)
      )

    state1 =
      r_state(state,
        begin_offset: new_begin_offset,
        min_bytes: f.(:min_bytes, r_state(state, :min_bytes)),
        max_bytes_orig: f.(:max_bytes, r_state(state, :max_bytes_orig)),
        max_wait_time: f.(:max_wait_time, r_state(state, :max_wait_time)),
        sleep_timeout: f.(:sleep_timeout, r_state(state, :sleep_timeout)),
        prefetch_count: f.(:prefetch_count, r_state(state, :prefetch_count)),
        prefetch_bytes: f.(:prefetch_bytes, r_state(state, :prefetch_bytes)),
        offset_reset_policy: offset_reset_policy,
        max_bytes: f.(:max_bytes, r_state(state, :max_bytes)),
        size_stat_window: f.(:size_stat_window, r_state(state, :size_stat_window))
      )

    new_state =
      case new_begin_offset !== old_begin_offset do
        true ->
          r_state(state1, pending_acks: r_pending_acks())

        false ->
          state1
      end

    resolve_begin_offset(new_state)
  end

  defp resolve_begin_offset(
         r_state(
           begin_offset: begin_offset,
           connection: connection,
           topic: topic,
           partition: partition
         ) = state
       )
       when begin_offset === :earliest or begin_offset === :latest or begin_offset === -2 or
              begin_offset === -1 do
    case resolve_offset(connection, topic, partition, begin_offset) do
      {:ok, new_begin_offset} ->
        {:ok, r_state(state, begin_offset: new_begin_offset)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_begin_offset(state) do
    {:ok, state}
  end

  defp resolve_offset(connection, topic, partition, begin_offset) do
    BrodUtils.resolve_offset(connection, topic, partition, begin_offset)
  catch
    reason ->
      {:error, reason}
  end

  defp reset_buffer(
         r_state(
           pending_acks: r_pending_acks(queue: queue),
           begin_offset: begin_offset0
         ) = state
       ) do
    begin_offset =
      case :queue.peek(queue) do
        {:value, {offset, _}} ->
          offset

        :empty ->
          begin_offset0
      end

    r_state(state,
      begin_offset: begin_offset,
      pending_acks: r_pending_acks(),
      last_req_ref: :undefined
    )
  end

  defp safe_gen_call(server, call, timeout) do
    GenServer.call(server, call, timeout)
  catch
    :exit, {reason, _} ->
      {:error, reason}
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

  defp connect_leader(client_pid, topic, partition)
       when is_pid(client_pid) do
    BrodClient.get_leader_connection(client_pid, topic, partition)
  end

  defp connect_leader(endpoints, topic, partition)
       when is_list(endpoints) do
    connect_leader({endpoints, []}, topic, partition)
  end

  defp connect_leader({endpoints, connCfg}, topic, partition) do
    :kpro.connect_partition_leader(endpoints, connCfg, topic, partition)
  end

  defp maybe_send_init_connection(r_state(subscriber: subscriber)) do
    timeout = @connection_retry_delay_ms

    if BrodUtils.is_pid_alive(subscriber) do
      Process.send_after(self(), :init_connection, timeout)
    end

    :ok
  end

  defp is_shared_conn(bootstrap) do
    is_pid(bootstrap)
  end

  defp log_string(format_string, args) do
    :io_lib.format(format_string, args)
  end
end
