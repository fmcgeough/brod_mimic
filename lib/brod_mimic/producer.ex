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

  @type milli_sec() :: non_neg_integer()
  @type delay_send_ref() :: :undef | {reference(), reference()}
  @type topic() :: Brod.topic()
  @type partition() :: Brod.partition()
  @type offset() :: Brod.offset()
  @type config() :: :proplists.proplist()
  @type call_ref() :: Brod.call_ref()
  @type conn() :: :kpro.connection()

  defrecord(:kpro_req, extract(:kpro_req, from_lib: "kafka_protocol/include/kpro.hrl"))

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
    call_ref =
      r_brod_call_ref(caller: self(), callee: pid, ref: mref = :erlang.monitor(:process, pid))

    batch = BrodUtils.make_batch_input(key, value)
    send(pid, {:produce, call_ref, batch, ack_cb})

    receive do
      r_brod_produce_reply(
        call_ref: r_brod_call_ref(ref: ^mref),
        result: :brod_produce_req_buffered
      ) ->
        :erlang.demonitor(mref, [:flush])

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
    mref = :erlang.monitor(:process, callee)

    receive do
      r_brod_produce_reply(
        call_ref: r_brod_call_ref(ref: ^ref),
        base_offset: offset,
        result: :brod_produce_req_acked
      ) ->
        :erlang.demonitor(mref, [:flush])
        {:ok, offset}

      {:DOWN, ^mref, :process, _pid, reason} ->
        {:error, {:producer_down, reason}}
    after
      timeout ->
        :erlang.demonitor(mref, [:flush])
        {:error, :timeout}
    end
  end

  def stop(pid) do
    :ok = GenServer.call(pid, :stop)
  end

  def init({client_pid, topic, partition, config}) do
    :erlang.process_flag(:trap_exit, true)
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
        conn_mref = :erlang.monitor(:process, connection)

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

        case :logger.allow(:warning, :brod_producer) do
          true ->
            :erlang.apply(:logger, :macro_log, [
              %{
                mfa: {:brod_producer, :maybe_reinit_connection, 1},
                line: 519,
                file: '../brod/src/brod_producer.erl'
              },
              :warning,
              'Failed to (re)init connection, reason:\n~p',
              [reason],
              %{domain: [:brod]}
            ])

          false ->
            :ok
        end

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
    _ = :erlang.cancel_timer(tref)
  end

  defp maybe_demonitor(:undefined) do
    :ok
  end

  defp maybe_demonitor(mref) do
    true = :erlang.demonitor(mref, [:flush])
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
