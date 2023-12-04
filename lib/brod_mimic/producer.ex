defmodule BrodMimic.Producer do
  use GenServer

  import Bitwise
  import Kernel, except: [send: 2]
  import Record, only: [defrecord: 2, extract: 2]

  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.KafkaApis, as: BrodKafkaApis
  alias BrodMimic.KafkaRequest, as: BrodKafkaRequest
  alias BrodMimic.Utils, as: BrodUtils

  require Record

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

      {:DOWN, ^mref, :process, _Pid, reason} ->
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

      {:DOWN, ^mref, :process, _Pid, reason} ->
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
    ack_timeout = :proplists.get_value(:ack_timeout, config, 10000)
    compression = :proplists.get_value(:compression, config, :no_compression)
    max_linger_ms = :proplists.get_value(:max_linger_ms, config, 0)
    max_linger_count = :proplists.get_value(:max_linger_count, config, 0)
    sendFun = make_send_fun(topic, partition, required_acks, ack_timeout, compression)

    buffer =
      __MODULE__Buffer.new(
        buffer_limit,
        on_wire_limit,
        max_batch_size,
        max_retries,
        max_linger_ms,
        max_linger_count,
        sendFun
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

  def code_change(_OldVsn, r_state() = state, _Extra) do
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

  def format_status(:normal, [_PDict, state = r_state()]) do
    [{:data, [{'State', state}]}]
  end

  def format_status(
        :terminate,
        [_PDict, state = r_state(buffer: buffer)]
      ) do
    r_state(state, buffer: ProducerBuffer.empty_buffers(buffer))
  end

  defp make_send_fun(topic, partition, required_acks, ack_timeout, compression) do
    extraArg = {topic, partition, required_acks, ack_timeout, compression}
    {&__MODULE__.do_send_fun/4, extraArg}
  end

  def do_send_fun(extraArg, conn, batchInput, vsn) do
    {topic, partition, required_acks, ack_timeout, compression} = extraArg

    produceRequest =
      BrodKafkaRequest.produce(
        vsn,
        topic,
        partition,
        batchInput,
        required_acks,
        ack_timeout,
        compression
      )

    case send(conn, produceRequest) do
      :ok when kpro_req(produceRequest, :no_ack) ->
        :ok

      :ok ->
        {:ok, kpro_req(produceRequest, :ref)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def do_no_ack(_partition, _base_offset) do
    :ok
  end

  defp log_error_code(topic, partition, offset, error_code) do
    case :logger.allow(:error, :brod_producer) do
      true ->
        :erlang.apply(:logger, :macro_log, [
          %{
            mfa: {:brod_producer, :log_error_code, 4},
            line: 444,
            file: '../brod/src/brod_producer.erl'
          },
          :error,
          'Produce error ~s-~B Offset: ~B Error: ~p',
          [topic, partition, offset, error_code],
          %{domain: [:brod]}
        ])

      false ->
        :ok
    end
  end

  defp make_bufcb(call_ref, ack_cb, partition) do
    {&:brod_producer.do_bufcb/2, _ExtraArg = {call_ref, ack_cb, partition}}
  end

  def do_bufcb({call_ref, ack_cb, partition}, arg) do
    r_brod_call_ref(caller: pid) = call_ref

    case arg do
      :brod_produce_req_buffered when is_pid(pid) ->
        reply = r_brod_produce_reply(call_ref: call_ref, result: :brod_produce_req_buffered)
        :erlang.send(pid, reply)

      :brod_produce_req_buffered ->
        :ok

      {:brod_produce_req_acked, base_offset} when ack_cb === :undefined ->
        reply =
          r_brod_produce_reply(
            call_ref: call_ref,
            base_offset: base_offset,
            result: :brod_produce_req_acked
          )

        :erlang.send(pid, reply)

      {:brod_produce_req_acked, base_offset} when is_function(ack_cb, 2) ->
        ack_cb.(partition, base_offset)
    end
  end

  defp handle_produce(buf_cb, batch, r_state(retry_tref: ref) = state)
       when is_reference(ref) do
    do_handle_produce(buf_cb, batch, state)
  end

  defp handle_produce(buf_cb, batch, r_state(connection: pid) = state)
       when is_pid(pid) do
    do_handle_produce(buf_cb, batch, state)
  end

  defp handle_produce(buf_cb, batch, r_state() = state) do
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
        connMref = :erlang.monitor(:process, connection)

        buffer = ProducerBuffer.nack_all(buffer0, :new_leader)

        {:ok,
         r_state(state,
           connection: connection,
           conn_mref: connMref,
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
    tRef = :erlang.send_after(timeout, self(), {:delayed_send, msg_ref})
    {tRef, msg_ref}
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
    tRef = :erlang.send_after(timeout, self(), :retry)
    {:ok, r_state(state, retry_tref: tRef)}
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
