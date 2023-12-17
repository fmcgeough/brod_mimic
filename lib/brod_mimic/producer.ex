defmodule BrodMimic.Producer do
  @moduledoc """
  Responsible for producing messages to a given partition of a given topic
  """

  use GenServer

  import Kernel, except: [send: 2]
  import Record, only: [defrecord: 2, defrecord: 3, extract: 2]

  alias BrodMimic.Brod
  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.KafkaApis, as: BrodKafkaApis
  alias BrodMimic.KafkaRequest, as: BrodKafkaRequest
  alias BrodMimic.ProducerBuffer
  alias BrodMimic.Utils, as: BrodUtils

  require Logger
  require Record

  @failed_init_connection "Failed to (re)init connection, reason:\n~p"

  @type milli_sec() :: non_neg_integer()
  @type delay_send_ref() :: :undefined | {reference(), reference()}
  @type topic() :: Brod.topic()
  @type partition() :: Brod.partition()
  @type offset() :: Brod.offset()
  @type config() :: :proplists.proplist()
  @type call_ref() :: Brod.call_ref()
  @type conn() :: :kpro.connection()

  defrecord(:kpro_req, extract(:kpro_req, from_lib: "kafka_protocol/include/kpro.hrl"))

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

  defrecord(:r_state, :state,
    client_pid: :undefined,
    topic: :undefined,
    partition: :undefined,
    connection: :undefined,
    conn_mref: :undefined,
    buffer: :undefined,
    retry_backoff_ms: :undefined,
    retry_tref: :undefined,
    delay_send_ref: :undefined,
    produce_req_vsn: :undefined
  )

  @doc """
  Start (link) a partition producer

  Possible configs (passed as a proplist):

  - `:required_acks` (optional, default = -1). How many acknowledgements the
    Kafka broker should receive from the clustered replicas before acking
    producer.
    - 0: the broker will not send any response (this is the only case where the
      broker will not reply to a request)
    - 1: the leader will wait the data is written to the local log before
      sending a response
    - -1: If it is -1 the broker will block until the message is committed by
      all in sync replicas before acking
  - `:ack_timeout` (optional, default = 10000 ms). Maximum time in milliseconds
    the broker can await the receipt of the number of acknowledgements in
    `:required_acks`. The timeout is not an exact limit on the request time for
    a few reasons: (1) it does not include network latency, (2) the timer begins
    at the beginning of the processing of this request so if many requests are
    queued due to broker overload that wait time will not be included, (3) Kafka
    leader will not terminate a local write so if the local write time exceeds
    this timeout it will not be respected.
  - `:partition_buffer_limit` (optional, default = 256). How many requests
    (per-partition) can be buffered without blocking the caller. The callers are
    released (by receiving the `brod_produce_req_buffered` reply) once the
    request is taken into buffer and after the request has been put on wire,
    then the caller may expect a reply `brod_produce_req_acked` when the request
    is accepted by Kafka.
  - `:partition_onwire_limit` (optional, default = 1). How many message sets
    (per-partition) can be sent to Kafka broker asynchronously before receiving
    ACKs from broker. NOTE: setting a number greater than 1 may cause messages
    being persisted in an order different from the order they were produced.
  - `:max_batch_size` (in bytes, optional, default = 1M). In case callers are
    producing faster than brokers can handle (or congestion on wire), try to
    accumulate small requests into batches as much as possible but not exceeding
    max_batch_size. OBS: If compression is enabled, care should be taken when
    picking the max batch size, because a compressed batch will be produced as
    one message and this message might be larger than `max.message.bytes` in
    Kafka config (or topic config)
  - `:max_retries` (optional, default = 3). If `{max_retries, n}` is given, the
    producer retry produce request for n times before crashing in case of
    failures like connection being shutdown by remote or exceptions received in
    produce response from Kafka. The special value -1 means "retry indefinitely"
  - `:retry_backoff_ms` (optional, default = 500). Time in milliseconds to sleep
    before retry the failed produce request.
  - `compression` (optional, default = `:no_compression`). `gzip` or `snappy` to
    enable compression.
  - `:max_linger_ms` (optional, default = 0). Messages are allowed to 'linger'
    in buffer for this amount of milliseconds before being sent. Definition of
    'linger': A message is in "linger" state when it is allowed to be sent
    on-wire, but chosen not to (for better batching). The default value is 0 for 2 reasons:
      - Backward compatibility (for 2.x releases)
      - Not to surprise `BrodMimic.Brod` `produce_sync` callers
  - `:max_linger_count` (optional, default = 0). At most this amount (count not
    size) of messages are allowed to "linger" in buffer. Messages will be sent
    regardless of "linger" age when this threshold is hit. NOTE: It does not make sense to have this value set larger than
    the value of `:partition_buffer_limit`.
  - `:produce_req_vsn` (optional, default = `:undefined`). User determined
    produce API version to use, discard the API version range received from
    Kafka. This is to be used when a topic in newer version Kafka is configured
    to store older version message format. e.g. When a topic in Kafka 0.11 is
    configured to have message format 0.10, sending message with headers would
    result in `:unknown_server_error` error code.
  """
  @spec start_link(pid(), Brod.topic(), Brod.partition(), config()) :: {:ok, pid()}
  def start_link(client_pid, topic, partition, config) do
    GenServer.start_link(__MODULE__, {client_pid, topic, partition, config}, [])
  end

  def produce(pid, key, value) do
    produce_cb(pid, key, value, :undefined)
  end

  def produce_no_ack(pid, key, value) do
    call_ref = r_brod_call_ref(caller: :undefined)
    ack_cb = &__MODULE__.do_no_ack/2
    batch = BrodUtils.make_batch_input(key, value)
    send(pid, {:produce, call_ref, batch, ack_cb})
    :ok
  end

  def produce_cb(pid, key, value, ack_cb) do
    call_ref = r_brod_call_ref(caller: self(), callee: pid, ref: mref = Process.monitor(pid))

    batch = BrodUtils.make_batch_input(key, value)
    send(pid, {:produce, call_ref, batch, ack_cb})

    receive do
      r_brod_produce_reply(
        call_ref: r_brod_call_ref(ref: ^mref),
        result: :brod_produce_req_buffered
      ) ->
        Process.demonitor(mref, [:flush])

        case ack_cb do
          :undefined ->
            {:ok, call_ref}

          _ ->
            :ok
        end

      {:DOWN, ^mref, :process, _pid, reason} ->
        {:error, {:producer_down, reason}}
    end
  end

  def sync_produce_request(call_ref, timeout) do
    r_brod_call_ref(caller: caller, callee: callee, ref: ref) = call_ref
    ^caller = self()
    mref = Process.monitor(callee)

    receive do
      r_brod_produce_reply(
        call_ref: r_brod_call_ref(ref: ^ref),
        base_offset: offset,
        result: :brod_produce_req_acked
      ) ->
        Process.demonitor(mref, [:flush])
        {:ok, offset}

      {:DOWN, ^mref, :process, _pid, reason} ->
        {:error, {:producer_down, reason}}
    after
      timeout ->
        Process.demonitor(mref, [:flush])
        {:error, :timeout}
    end
  end

  def stop(pid) do
    :ok = GenServer.call(pid, :stop)
  end

  def init({client_pid, topic, partition, config}) do
    Process.flag(:trap_exit, true)
    buffer_limit = :proplists.get_value(:partition_buffer_limit, config, 512)
    on_wire_limit = :proplists.get_value(:partition_onwire_limit, config, 1)
    max_batch_size = :proplists.get_value(:max_batch_size, config, 1_048_576)
    max_retries = :proplists.get_value(:max_retries, config, 3)
    retry_backoff_ms = :proplists.get_value(:retry_backoff_ms, config, 500)
    required_acks = :proplists.get_value(:required_acks, config, -1)
    ack_timeout = :proplists.get_value(:ack_timeout, config, 10_000)
    compression = :proplists.get_value(:compression, config, :no_compression)
    max_linger_ms = :proplists.get_value(:max_linger_ms, config, 0)
    max_linger_count = :proplists.get_value(:max_linger_count, config, 0)
    send_fun = make_send_fun(topic, partition, required_acks, ack_timeout, compression)

    buffer =
      ProducerBuffer.new(
        buffer_limit,
        on_wire_limit,
        max_batch_size,
        max_retries,
        max_linger_ms,
        max_linger_count,
        send_fun
      )

    default_vsn = BrodKafkaApis.default_version(:produce)

    req_version =
      case :proplists.get_value(:produce_req_vsn, config, :undefined) do
        :undefined ->
          {:default, default_vsn}

        vsn ->
          {:configured, vsn}
      end

    state =
      r_state(
        client_pid: client_pid,
        topic: topic,
        partition: partition,
        buffer: buffer,
        retry_backoff_ms: retry_backoff_ms,
        connection: :undefined,
        produce_req_vsn: req_version
      )

    :ok = BrodClient.register_producer(client_pid, topic, partition)
    {:ok, state}
  end

  def handle_call(:stop, _from, r_state() = state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(call, _from, r_state() = state) do
    {:reply, {:error, {:unsupported_call, call}}, state}
  end

  def handle_cast(_cast, r_state() = state) do
    {:noreply, state}
  end

  def code_change(_old_vsn, r_state() = state, _extra) do
    {:ok, state}
  end

  def terminate(
        reason,
        r_state(client_pid: client_pid, topic: topic, partition: partition)
      ) do
    case BrodUtils.is_normal_reason(reason) do
      true ->
        BrodClient.deregister_producer(client_pid, topic, partition)

      false ->
        :ok
    end

    :ok
  end

  def format_status(:normal, [_pdict, state = r_state()]) do
    [{:data, [{'State', state}]}]
  end

  def format_status(:terminate, [_pdict, state = r_state(buffer: buffer)]) do
    r_state(state, buffer: ProducerBuffer.empty_buffers(buffer))
  end

  defp make_send_fun(topic, partition, required_acks, ack_timeout, compression) do
    extra_arg = {topic, partition, required_acks, ack_timeout, compression}
    {&__MODULE__.do_send_fun/4, extra_arg}
  end

  def do_send_fun(extra_arg, conn, batch_input, vsn) do
    {topic, partition, required_acks, ack_timeout, compression} = extra_arg

    produce_request =
      BrodKafkaRequest.produce(
        vsn,
        topic,
        partition,
        batch_input,
        required_acks,
        ack_timeout,
        compression
      )

    case send(conn, produce_request) do
      :ok when kpro_req(produce_request, :no_ack) ->
        :ok

      :ok ->
        {:ok, kpro_req(produce_request, :ref)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def do_no_ack(_partition, _base_offset) do
    :ok
  end

  def log_error_code(topic, partition, offset, error_code) do
    "Produce error ~s-~B Offset: ~B Error: ~p"
    |> :io_lib.format([topic, partition, offset, error_code])
    |> to_string()
    |> Logger.error(%{domain: [:brod]})
  end

  def make_bufcb(call_ref, ack_cb, partition) do
    {&BrodMimic.Producer.do_bufcb/2, _extra_arg = {call_ref, ack_cb, partition}}
  end

  def do_bufcb({call_ref, ack_cb, partition}, arg) do
    r_brod_call_ref(caller: pid) = call_ref

    case arg do
      :brod_produce_req_buffered when is_pid(pid) ->
        reply = r_brod_produce_reply(call_ref: call_ref, result: :brod_produce_req_buffered)
        send(pid, reply)

      :brod_produce_req_buffered ->
        :ok

      {:brod_produce_req_acked, base_offset} when ack_cb === :undefined ->
        reply =
          r_brod_produce_reply(
            call_ref: call_ref,
            base_offset: base_offset,
            result: :brod_produce_req_acked
          )

        send(pid, reply)

      {:brod_produce_req_acked, base_offset} when is_function(ack_cb, 2) ->
        ack_cb.(partition, base_offset)
    end
  end

  def handle_produce(buf_cb, batch, r_state(retry_tref: ref) = state) when is_reference(ref) do
    do_handle_produce(buf_cb, batch, state)
  end

  def handle_produce(buf_cb, batch, r_state(connection: pid) = state)
      when is_pid(pid) do
    do_handle_produce(buf_cb, batch, state)
  end

  def handle_produce(buf_cb, batch, r_state() = state) do
    {:ok, new_state} = maybe_reinit_connection(state)
    do_handle_produce(buf_cb, batch, new_state)
  end

  defp do_handle_produce(buf_cb, batch, r_state(buffer: buffer) = state) do
    new_buffer = ProducerBuffer.add(buffer, buf_cb, batch)
    state1 = r_state(state, buffer: new_buffer)
    {:ok, new_state} = maybe_produce(state1)
    {:noreply, new_state}
  end

  defp maybe_reinit_connection(
         r_state(
           client_pid: client_pid,
           connection: old_connection,
           conn_mref: old_conn_mref,
           topic: topic,
           partition: partition,
           buffer: buffer0,
           produce_req_vsn: req_version
         ) = state
       ) do
    case BrodClient.get_leader_connection(client_pid, topic, partition) do
      {:ok, ^old_connection} ->
        {:ok, state}

      {:ok, connection} ->
        :ok = maybe_demonitor(old_conn_mref)
        conn_mref = Process.monitor(connection)

        buffer = ProducerBuffer.nack_all(buffer0, :new_leader)

        {:ok,
         r_state(state,
           connection: connection,
           conn_mref: conn_mref,
           buffer: buffer,
           produce_req_vsn: req_vsn(connection, req_version)
         )}

      {:error, reason} ->
        :ok = maybe_demonitor(old_conn_mref)
        buffer = ProducerBuffer.nack_all(buffer0, :no_leader_connection)
        Logger.warning(:io_lib.format(@failed_init_connection, [reason]), %{domain: [:brod]})

        {:ok, r_state(state, connection: :undefined, conn_mref: :undefined, buffer: buffer)}
    end
  end

  defp maybe_produce(r_state(retry_tref: ref) = state) when is_reference(ref) do
    {:ok, state}
  end

  defp maybe_produce(
         r_state(
           buffer: buffer0,
           connection: connection,
           delay_send_ref: delay_send_ref0,
           produce_req_vsn: {_, vsn}
         ) = state
       ) do
    _ = cancel_delay_send_timer(delay_send_ref0)

    case ProducerBuffer.maybe_send(buffer0, connection, vsn) do
      {:ok, buffer} ->
        {:ok, r_state(state, buffer: buffer)}

      {{:delay, timeout}, buffer} ->
        delay_send_ref = start_delay_send_timer(timeout)

        new_state = r_state(state, buffer: buffer, delay_send_ref: delay_send_ref)

        {:ok, new_state}

      {:retry, buffer} ->
        schedule_retry(r_state(state, buffer: buffer))
    end
  end

  defp req_vsn(_, {:configured, vsn}) do
    {:configured, vsn}
  end

  defp req_vsn(conn, _not_configured) when is_pid(conn) do
    {:resolved, BrodKafkaApis.pick_version(conn, :produce)}
  end

  defp start_delay_send_timer(timeout) do
    msg_ref = make_ref()
    t_ref = Process.send_after(self(), {:delayed_send, msg_ref}, timeout)
    {t_ref, msg_ref}
  end

  defp cancel_delay_send_timer(:undefined) do
    :ok
  end

  defp cancel_delay_send_timer({tref, _Msg}) do
    _ = Process.cancel_timer(tref)
  end

  defp maybe_demonitor(:undefined) do
    :ok
  end

  defp maybe_demonitor(mref) do
    true = Process.demonitor(mref, [:flush])
    :ok
  end

  defp schedule_retry(
         r_state(
           retry_tref: :undefined,
           retry_backoff_ms: timeout
         ) = state
       ) do
    t_ref = Process.send_after(self(), :retry, timeout)
    {:ok, r_state(state, retry_tref: t_ref)}
  end

  defp schedule_retry(state) do
    {:ok, state}
  end

  defp send(:undefined, _kafka_req) do
    {:error, :no_leader_connection}
  end

  defp send(connection, kafka_req) do
    :kpro.request_async(connection, kafka_req)
  end
end
