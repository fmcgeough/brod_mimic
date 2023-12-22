defmodule BrodMimic.Consumer do
  @moduledoc """
  Kafka consumers work in poll mode. In brod, `brod_consumer' is the poller,
  which is constantly asking for more data from the kafka node which is a leader
  for the given partition.

  By subscribing to `Consumer' a process should receive the polled message
  sets (not individual messages) into its mailbox. Shape of the message is
  documented at `BrodMimic.Brod.subscribe/5`.

  Messages processed by the subscriber has to be acked by calling
  `ack/2` (or `BrodMimic.Brod.consume_ack/4`) to notify the consumer
  that all messages before the acknowledged offsets are processed,
  hence more messages can be fetched and sent to the subscriber and the
  subscriber won't be overwhelmed by it.

  Each consumer can have only one subscriber.
  """
  use BrodMimic.Macros
  use GenServer

  import Record, only: [defrecordp: 2]

  alias BrodMimic.Brod
  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.KafkaRequest, as: BrodKafkaRequest
  alias BrodMimic.Utils, as: BrodUtils

  require Logger

  @fetch_error "Fetch error ~s-~p: ~p"
  @unexpected_info "~p ~p got unexpected info: ~p"
  @unexpected_cast "~p ~p got unexpected cast: ~p"
  @consumer_terminate "Consumer ~s-~w terminate reason: ~p"
  @consumer_shutdown "Consumer ~s-~p shutdown\nreason: ~p"
  @consumer_suspended "~p ~p consumer is suspended, waiting for subscriber ~p to resubscribe with new begin_offset"
  @offset_out_of_range "~p ~p offset out of range, applying reset policy ~p"

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

  @type isolation_level() :: :kpro.isolation_level()
  @type offset_reset_policy() :: :reset_by_subscriber | :reset_to_earliest | :reset_to_latest
  @type bytes() :: non_neg_integer()
  @type config() :: Brod.consumer_config()
  @type debug() :: GenServer.debug()

  defrecordp(:pending_acks, count: 0, bytes: 0, queue: :queue.new())

  defrecordp(:state,
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

  @typedoc """
  Type definition for the Record `:pending_acks` used internally
  """
  @type pending_acks() :: record(:pending_acks, count: integer(), bytes: integer(), queue: :queue.queue())

  @typedoc """
  Type definition for the `Record` used for `BrodMimic.Consumer` GenServer state
  """
  @type state() ::
          record(:state,
            bootstrap: pid() | Brod.bootstrap(),
            connection: :undefined | pid(),
            topic: binary(),
            partition: integer(),
            begin_offset: Brod.offset_time(),
            max_wait_time: integer(),
            min_bytes: bytes(),
            max_bytes_orig: bytes(),
            sleep_timeout: integer(),
            prefetch_count: integer(),
            last_req_ref: :undefined | reference(),
            subscriber: :undefined | pid(),
            subscriber_mref: :undefined | reference(),
            pending_acks: pending_acks(),
            is_suspended: boolean(),
            offset_reset_policy: offset_reset_policy(),
            avg_bytes: number(),
            max_bytes: bytes(),
            size_stat_window: non_neg_integer(),
            prefetch_bytes: non_neg_integer(),
            connection_mref: :undefined | reference(),
            isolation_level: isolation_level()
          )

  @doc """
  Equivalent to `start_link/5` with the `debug` parameter set to an empty List
  """
  @spec start_link(Brod.bootstrap() | pid(), topic(), partition(), config()) ::
          {:ok, pid()} | {:error, any()}
  def start_link(bootstrap, topic, partition, config) do
    start_link(bootstrap, topic, partition, config, [])
  end

  @doc """
  Start (link) a partition consumer

  ## Parameters

  - bootstrap - The Kafka endpoints data or a pid
  - topic - The topic we are consuming
  - partition - The partition we are consuming
  - config - A proplist defining the consumer config. See `t:config/0`
  - debug - `t:debug/0`
  """
  @spec start_link(
          Brod.bootstrap() | pid(),
          topic(),
          partition(),
          config(),
          debug()
        ) ::
          {:ok, pid()} | {:error, any()}
  def start_link(bootstrap, topic, partition, config, debug) do
    args = {bootstrap, topic, partition, config}
    GenServer.start_link(__MODULE__, args, [{:debug, debug}])
  end

  @doc """
  Stop the consumer process
  """
  @spec stop(pid()) :: :ok | {:error, any()}
  def stop(pid) do
    safe_gen_call(pid, :stop, :infinity)
  end

  @doc """
  Stop the consumer process using a timeout. If the timeout expires then
  the process is killed.
  """
  @spec stop_maybe_kill(pid(), timeout()) :: :ok
  def stop_maybe_kill(pid, timeout) do
    GenServer.call(pid, :stop, timeout)
  catch
    :exit, {:noproc, _} ->
      :ok

    :exit, {:timeout, _} ->
      Process.exit(pid, :kill)
      :ok
  end

  @impl GenServer
  def init({bootstrap, topic, partition, config}) do
    Process.flag(:trap_exit, true)

    min_bytes = :proplists.get_value(:min_bytes, config, @default_min_bytes)
    max_bytes = :proplists.get_value(:max_bytes, config, @default_max_bytes)
    max_wait_time = :proplists.get_value(:max_wait_time, config, @default_max_wait_time)
    sleep_timeout = :proplists.get_value(:sleep_timeout, config, @default_sleep_timeout)
    prefetch_count = max(:proplists.get_value(:prefetch_count, config, @default_prefetch_count), 0)
    prefetch_bytes = max(:proplists.get_value(:prefetch_bytes, config, @default_prefetch_bytes), 0)
    begin_offset = :proplists.get_value(:begin_offset, config, @default_begin_offset)
    offset_reset_policy = :proplists.get_value(:offset_reset_policy, config, @default_offset_reset_policy)
    isolation_level = :proplists.get_value(:isolation_level, config, @default_isolation_level)

    # If bootstrap is a client pid, register self to the client
    case is_shared_conn(bootstrap) do
      true ->
        :ok = BrodClient.register_consumer(bootstrap, topic, partition)

      false ->
        :ok
    end

    {:ok,
     state(
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
       connection: :undefined,
       pending_acks: pending_acks(),
       is_suspended: false,
       offset_reset_policy: offset_reset_policy,
       avg_bytes: 0,
       max_bytes: max_bytes,
       size_stat_window: :proplists.get_value(:size_stat_window, config, @default_avg_window),
       connection_mref: :undefined,
       isolation_level: isolation_level
     )}
  end

  @doc """
  Subscribe or resubscribe on messages from a partition.

  Caller may specify a set of options extending consumer config.
  It is possible to update parameters such as `max_bytes` and
  `max_wait_time`, or the starting point (`begin_offset`) of the data
  stream. Note that you currently cannot update `isolation_level`.

  Possible options are documented at `start_link/5`.
  """
  @spec subscribe(pid(), pid(), config()) :: :ok | {:error, any()}
  def subscribe(pid, subscriber_pid, consumer_options) do
    safe_gen_call(pid, {:subscribe, subscriber_pid, consumer_options}, :infinity)
  end

  @doc """
  Unsubscribe the current subscriber
  """
  @spec unsubscribe(pid(), pid()) :: :ok | {:error, any()}
  def unsubscribe(pid, subscriber_pid) do
    safe_gen_call(pid, {:unsubscribe, subscriber_pid}, :infinity)
  end

  @doc """
  Subscriber confirms that a message (identified by offset) has been
  consumed, consumer process now may continue to fetch more messages.
  """
  @spec ack(pid(), offset()) :: :ok
  def ack(pid, offset) do
    GenServer.cast(pid, {:ack, offset})
  end

  @doc """
  Enable/disable debugging on the consumer process.

  - prints debug info to stdout.
  `debug(pid, :print)`

  - prints debug info to the file `file`.
  `debug(pid, file)`
  """
  @spec debug(pid(), :print | charlist() | :none) :: :ok
  def debug(pid, :none) do
    do_debug(pid, :no_debug)
  end

  def debug(pid, :print) do
    do_debug(pid, {:trace, true})
  end

  def debug(pid, file) when is_list(file) do
    do_debug(pid, {:log_to_file, file})
  end

  @doc """
  Get connection pid. Test/debug only.
  """
  @spec get_connection(pid()) :: :undefined | pid()
  def get_connection(pid) do
    GenServer.call(pid, :get_connection)
  end

  @impl GenServer
  def handle_info(
        :init_connection,
        state(subscriber: subscriber) = state0
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

  def handle_info({:DOWN, _monitor_ref, :process, pid, _reason}, state(subscriber: pid) = state) do
    new_state = reset_buffer(state(state, subscriber: :undefined, subscriber_mref: :undefined))
    {:noreply, new_state}
  end

  def handle_info({:DOWN, _monitor_ref, :process, pid, _reason}, state(connection: pid) = state) do
    {:noreply, handle_conn_down(state)}
  end

  def handle_info({:EXIT, pid, _reason}, state(connection: pid) = state) do
    {:noreply, handle_conn_down(state)}
  end

  def handle_info(info, state) do
    Logger.warning(:io_lib.format(@unexpected_info, [:brod_consumer, self(), info]), %{
      domain: [:brod]
    })

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:get_connection, _from, state(connection: c) = state) do
    {:reply, c, state}
  end

  def handle_call({:subscribe, pid, options}, _from, state(subscriber: subscriber) = state0) do
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
        state(
          subscriber: current_subscriber,
          subscriber_mref: mref
        ) = state
      ) do
    case subscriber_pid === current_subscriber do
      true ->
        is_reference(mref) and Process.demonitor(mref, [:flush])
        new_state = state(state, subscriber: :undefined, subscriber_mref: :undefined)

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

  @impl GenServer
  def handle_cast({:ack, offset}, state(pending_acks: pending_acks) = state0) do
    new_pending_acks = handle_ack(pending_acks, offset)
    state1 = state(state0, pending_acks: new_pending_acks)
    state = maybe_send_fetch_request(state1)
    {:noreply, state}
  end

  def handle_cast(cast, state) do
    Logger.warning(:io_lib.format(@unexpected_cast, [:brod_consumer, self(), cast]), %{
      domain: [:brod]
    })

    {:noreply, state}
  end

  @impl GenServer
  def terminate(
        reason,
        state(bootstrap: bootstrap, topic: topic, partition: partition, connection: connection)
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

    if not is_normal do
      Logger.error(:io_lib.format(@consumer_terminate, [topic, partition, reason]), %{
        domain: [:brod]
      })
    end

    :ok
  end

  @impl GenServer
  def code_change(_old_vsn, state, _extra) do
    {:ok, state}
  end

  defp handle_conn_down(state0) do
    state = state(state0, connection: :undefined, connection_mref: :undefined)
    :ok = maybe_send_init_connection(state)
    state
  end

  defp do_debug(pid, debug) do
    {:ok, _} = :gen.call(pid, :system, {:debug, debug}, :infinity)
    :ok
  end

  defp handle_fetch_response(kpro_rsp(), state(subscriber: :undefined) = state0) do
    state = state(state0, last_req_ref: :undefined)
    {:noreply, state}
  end

  defp handle_fetch_response(kpro_rsp(ref: ref1), state(last_req_ref: ref2) = state)
       when ref1 !== ref2 do
    {:noreply, state}
  end

  defp handle_fetch_response(
         kpro_rsp(ref: ref) = rsp,
         state(topic: topic, partition: partition, last_req_ref: ref) = state0
       ) do
    state = state(state0, last_req_ref: :undefined)

    case BrodUtils.parse_rsp(rsp) do
      {:ok, %{header: header, batches: batches}} ->
        handle_batches(header, batches, state)

      {:error, error_code} ->
        error = kafka_fetch_error(topic: topic, partition: partition, error_code: error_code)
        handle_fetch_error(error, state)
    end
  end

  defp handle_batches(:undefined, [], state0) do
    # It is only possible to end up here in a incremental
    # fetch session, empty fetch response implies no
    # new messages to fetch, and no changes in partition
    # metadata (e.g. high watermark offset, or last stable offset) either.
    # Do not advance offset, try again (maybe after a delay) with
    # the last begin_offset in use.
    state = maybe_delay_fetch_request(state0)
    {:noreply, state}
  end

  defp handle_batches(_header, {:incomplete_batch, size}, state(max_bytes: max_bytes) = state0) do
    # max_bytes is too small to fetch ONE complete batch
    true = size > max_bytes
    state1 = state(state0, max_bytes: size)
    state = maybe_send_fetch_request(state1)
    {:noreply, state}
  end

  defp handle_batches(header, [], state(begin_offset: begin_offset) = state0) do
    stable_offset = BrodUtils.get_stable_offset(header)

    state =
      case begin_offset < stable_offset do
        true ->
          # There are chances that kafka may return empty message set
          # when messages are deleted from a compacted topic.
          # Since there is no way to know how big the 'hole' is
          # we can only bump begin_offset with +1 and try again.
          state1 = state(state0, begin_offset: begin_offset + 1)
          maybe_send_fetch_request(state1)

        false ->
          # we have either reached the end of a partition
          # or trying to read uncommitted messages
          # try to poll again (maybe after a delay)
          maybe_delay_fetch_request(state0)
      end

    {:noreply, state}
  end

  defp handle_batches(
         header,
         batches,
         state(
           subscriber: subscriber,
           pending_acks: pending_acks,
           begin_offset: begin_offset,
           topic: topic,
           partition: partition
         ) = state0
       ) do
    stable_offset = BrodUtils.get_stable_offset(header)
    {new_begin_offset, messages} = BrodUtils.flatten_batches(begin_offset, header, batches)
    state1 = state(state0, begin_offset: new_begin_offset)

    state =
      case messages != [] do
        true ->
          # All messages are before requested offset, hence dropped
          state1

        false ->
          msg_set =
            kafka_message_set(
              topic: topic,
              partition: partition,
              high_wm_offset: stable_offset,
              messages: messages
            )

          :ok = cast_to_subscriber(subscriber, msg_set)
          new_pending_acks = add_pending_acks(pending_acks, messages)
          state2 = state(state1, pending_acks: new_pending_acks)
          maybe_shrink_max_bytes(state2, messages)
      end

    {:noreply, maybe_send_fetch_request(state)}
  end

  defp add_pending_acks(pending_acks, messages) do
    :lists.foldl(&add_pending_ack/2, pending_acks, messages)
  end

  defp add_pending_ack(
         kafka_message(offset: offset, key: key, value: value),
         pending_acks(queue: queue, count: count, bytes: bytes) = pending_acks
       ) do
    size = :erlang.size(key) + :erlang.size(value)
    new_queue = :queue.in({offset, size}, queue)
    pending_acks(pending_acks, queue: new_queue, count: count + 1, bytes: bytes + size)
  end

  defp maybe_shrink_max_bytes(
         state(
           size_stat_window: w,
           max_bytes_orig: max_bytes_orig
         ) = state,
         _
       )
       when w < 1 do
    state(state, max_bytes: max_bytes_orig)
  end

  defp maybe_shrink_max_bytes(state0, messages) do
    state(
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

    state(state, max_bytes: :erlang.min(new_max_bytes, max_bytes))
  end

  defp update_avg_size(state() = state, []) do
    state
  end

  defp update_avg_size(
         state(
           avg_bytes: avg_bytes,
           size_stat_window: window_size
         ) = state,
         [kafka_message(key: key, value: value) | rest]
       ) do
    msg_bytes = :erlang.size(key) + :erlang.size(value) + 40
    new_avg_bytes = ((window_size - 1) * avg_bytes + msg_bytes) / window_size
    update_avg_size(state(state, avg_bytes: new_avg_bytes), rest)
  end

  defp err_op(:request_timed_out), do: :retry
  defp err_op(:invalid_topic_exception), do: :stop
  defp err_op(:offset_out_of_range), do: :reset_offset
  defp err_op(:leader_not_available), do: :reset_connection
  defp err_op(:not_leader_for_partition), do: :reset_connection
  defp err_op(:unknown_topic_or_partition), do: :reset_connection
  defp err_op(_), do: :restart

  defp handle_fetch_error(
         kafka_fetch_error(error_code: error_code) = error,
         state(
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

        is_reference(mref) and Process.demonitor(mref)

        new_state = state(state, connection: :undefined, connection_mref: :undefined)

        :ok = maybe_send_init_connection(new_state)
        {:noreply, new_state}

      :retry ->
        {:noreply, maybe_send_fetch_request(state)}

      :stop ->
        :ok = cast_to_subscriber(subscriber, error)
        error_string = :io_lib.format(@consumer_shutdown, [topic, partition, error_code])
        Logger.error(error_string, %{domain: [:brod]})

        {:stop, :normal, state}

      :reset_offset ->
        handle_reset_offset(state, error)

      :restart ->
        :ok = cast_to_subscriber(subscriber, error)
        {:stop, {:restart, error_code}, state}
    end
  end

  defp handle_reset_offset(
         state(
           subscriber: subscriber,
           offset_reset_policy: :reset_by_subscriber
         ) = state,
         error
       ) do
    :ok = cast_to_subscriber(subscriber, error)

    Logger.info(:io_lib.format(@consumer_suspended, [:brod_consumer, self(), subscriber]), %{
      domain: [:brod]
    })

    {:noreply, state(state, is_suspended: true)}
  end

  defp handle_reset_offset(
         state(offset_reset_policy: policy) = state,
         _error
       ) do
    Logger.info(:io_lib.format(@offset_out_of_range, [:brod_consumer, self(), policy]), %{
      domain: [:brod]
    })

    begin_offset =
      case policy do
        :reset_to_earliest ->
          :earliest

        :reset_to_latest ->
          :latest
      end

    state1 = state(state, begin_offset: begin_offset, pending_acks: pending_acks())

    {:ok, state2} = resolve_begin_offset(state1)
    new_state = maybe_send_fetch_request(state2)
    {:noreply, new_state}
  end

  defp handle_ack(
         pending_acks(queue: queue, bytes: bytes, count: count) = pending_acks,
         offset
       ) do
    case :queue.out(queue) do
      {{:value, {o, size}}, queue1} when o <= offset ->
        handle_ack(
          pending_acks(pending_acks, queue: queue1, count: count - 1, bytes: bytes - size),
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

  defp maybe_delay_fetch_request(state(sleep_timeout: t) = state) when t > 0 do
    _ = Process.send_after(self(), :send_fetch_request, t)
    state
  end

  defp maybe_delay_fetch_request(state) do
    maybe_send_fetch_request(state)
  end

  defp maybe_send_fetch_request(state(subscriber: :undefined) = state) do
    state
  end

  defp maybe_send_fetch_request(state(connection: :undefined) = state) do
    state
  end

  defp maybe_send_fetch_request(state(is_suspended: true) = state) do
    state
  end

  defp maybe_send_fetch_request(state(last_req_ref: r) = state) when is_reference(r) do
    state
  end

  defp maybe_send_fetch_request(
         state(
           pending_acks: pending_acks(count: count, bytes: bytes),
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

  defp send_fetch_request(state() = state) do
    begin_offset = state(state, :begin_offset)

    # Raise an exception with stack trace if begin offset is invalid
    if valid_begin_offset?(begin_offset) == false do
      :erlang.error({:bad_begin_offset, begin_offset})
    end

    request =
      BrodKafkaRequest.fetch(
        state(state, :connection),
        state(state, :topic),
        state(state, :partition),
        state(state, :begin_offset),
        state(state, :max_wait_time),
        state(state, :min_bytes),
        state(state, :max_bytes),
        state(state, :isolation_level)
      )

    case :kpro.request_async(state(state, :connection), request) do
      :ok ->
        state(state, last_req_ref: kpro_req(request, :ref))

      {:error, {:connection_down, _reason}} ->
        state
    end
  end

  defp handle_subscribe_call(pid, options, state(subscriber_mref: old_mref) = state0) do
    case update_options(options, state0) do
      {:ok, state1} ->
        if is_reference(old_mref) do
          Process.demonitor(old_mref, [:flush])
        end

        mref = Process.monitor(pid)

        state2 = state(state1, subscriber: pid, subscriber_mref: mref)

        state3 = reset_buffer(state2)
        state4 = state(state3, is_suspended: false)
        state = maybe_send_fetch_request(state4)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state0}
    end
  end

  defp update_options(
         options,
         state(begin_offset: old_begin_offset) = state
       ) do
    f = fn name, default ->
      :proplists.get_value(name, options, default)
    end

    new_begin_offset = f.(:begin_offset, old_begin_offset)
    offset_reset_policy = f.(:offset_reset_policy, state(state, :offset_reset_policy))

    state1 =
      state(state,
        begin_offset: new_begin_offset,
        min_bytes: f.(:min_bytes, state(state, :min_bytes)),
        max_bytes_orig: f.(:max_bytes, state(state, :max_bytes_orig)),
        max_wait_time: f.(:max_wait_time, state(state, :max_wait_time)),
        sleep_timeout: f.(:sleep_timeout, state(state, :sleep_timeout)),
        prefetch_count: f.(:prefetch_count, state(state, :prefetch_count)),
        prefetch_bytes: f.(:prefetch_bytes, state(state, :prefetch_bytes)),
        offset_reset_policy: offset_reset_policy,
        max_bytes: f.(:max_bytes, state(state, :max_bytes)),
        size_stat_window: f.(:size_stat_window, state(state, :size_stat_window))
      )

    new_state =
      case new_begin_offset !== old_begin_offset do
        true ->
          state(state1, pending_acks: pending_acks())

        false ->
          state1
      end

    resolve_begin_offset(new_state)
  end

  defp resolve_begin_offset(
         state(
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
        {:ok, state(state, begin_offset: new_begin_offset)}

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

  defp reset_buffer(state(pending_acks: pending_acks(queue: queue), begin_offset: begin_offset0) = state) do
    begin_offset =
      case :queue.peek(queue) do
        {:value, {offset, _}} ->
          offset

        :empty ->
          begin_offset0
      end

    state(state,
      begin_offset: begin_offset,
      pending_acks: pending_acks(),
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
         state(bootstrap: bootstrap, topic: topic, partition: partition, connection: :undefined) = state0
       ) do
    case connect_leader(bootstrap, topic, partition) do
      {:ok, connection} ->
        mref =
          case is_shared_conn(bootstrap) do
            true ->
              Process.monitor(connection)

            false ->
              :undefined
          end

        state = state(state0, last_req_ref: :undefined, connection: connection, connection_mref: mref)

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

  defp maybe_send_init_connection(state(subscriber: subscriber)) do
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

  defp valid_begin_offset?(begin_offset) do
    is_integer(begin_offset) and begin_offset >= 0
  end
end
