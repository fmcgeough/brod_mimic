defmodule BrodMimic.ProducerBuffer do
  @moduledoc false

  import Record, only: [defrecord: 3]

  alias BrodMimic.Utils, as: BrodUtils

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

  defrecord(:r_req, :req,
    buf_cb: :undefined,
    data: :undefined,
    bytes: :undefined,
    msg_cnt: :undefined,
    ctime: :undefined,
    failures: :undefined
  )

  defrecord(:r_buf, :buf,
    buffer_limit: 1,
    onwire_limit: 1,
    max_batch_size: 1,
    max_retries: 0,
    max_linger_ms: 0,
    max_linger_count: 0,
    send_fun: &__MODULE__.bad_init_error/0,
    buffer_count: 0,
    onwire_count: 0,
    pending: :queue.new(),
    buffer: :queue.new(),
    onwire: []
  )

  def new(
        buffer_limit,
        on_wire_limit,
        max_batch_size,
        max_retry,
        max_linger_ms,
        max_linger_count0,
        send_fun
      ) do
    true = buffer_limit > 0
    true = on_wire_limit > 0
    true = max_batch_size > 0
    true = max_linger_count0 >= 0

    max_linger_count = :erlang.min(buffer_limit, max_linger_count0)

    r_buf(
      buffer_limit: buffer_limit,
      onwire_limit: on_wire_limit,
      max_batch_size: max_batch_size,
      max_retries: max_retry,
      max_linger_ms: max_linger_ms,
      max_linger_count: max_linger_count,
      send_fun: send_fun
    )
  end

  def bad_init_error do
    :erlang.error(:bad_init)
  end

  def add(r_buf(pending: pending) = buf, buf_cb, batch) do
    req =
      r_req(
        buf_cb: buf_cb,
        data: batch,
        bytes: data_size(batch),
        msg_cnt: length(batch),
        ctime: now_ms(),
        failures: 0
      )

    maybe_buffer(r_buf(buf, pending: :queue.in(req, pending)))
  end

  def maybe_send(r_buf() = buf, conn, vsn) do
    case take_reqs(buf) do
      false ->
        {:ok, buf}

      {:delay, t} ->
        {{:delay, t}, buf}

      {reqs, new_buf} ->
        do_send(reqs, new_buf, conn, vsn)
    end
  end

  def ack(buf, ref) do
    ack(buf, ref, -1)
  end

  def ack(
        r_buf(onwire_count: on_wire_count, onwire: [{ref, reqs} | rest]) = buf,
        ref,
        base_offset
      ) do
    _ = :lists.foldl(&eval_acked/2, base_offset, reqs)
    r_buf(buf, onwire_count: on_wire_count - 1, onwire: rest)
  end

  def nack(r_buf(onwire: [{ref, _reqs} | _]) = buf, ref, reason) do
    nack_all(buf, reason)
  end

  def nack_all(r_buf(onwire: on_wire) = buf, reason) do
    all_on_wire_reqs = :lists.flatmap(fn {_ref, reqs} -> reqs end, on_wire)
    new_buf = r_buf(buf, onwire_count: 0, onwire: [])
    rebuffer_or_crash(all_on_wire_reqs, new_buf, reason)
  end

  def is_empty(r_buf(pending: pending, buffer: buffer, onwire: onwire)) do
    :queue.is_empty(pending) and :queue.is_empty(buffer) and onwire === []
  end

  defp take_reqs(r_buf(onwire_count: on_wire_count, onwire_limit: on_wire_limit))
       when on_wire_count >= on_wire_limit do
    false
  end

  defp take_reqs(r_buf(buffer: buffer, pending: pending) = buf) do
    case :queue.is_empty(buffer) and :queue.is_empty(pending) do
      true ->
        false

      false ->
        new_buf = maybe_buffer(buf)
        do_take_reqs(new_buf)
    end
  end

  defp do_take_reqs(r_buf(max_linger_count: max_linger_count, buffer_count: buffer_count) = buf)
       when buffer_count >= max_linger_count do
    take_reqs_loop(buf, _acc = [], _acc_bytes = 0)
  end

  defp do_take_reqs(r_buf(max_linger_ms: max_linger_ms, buffer: buffer) = buf) do
    {:value, req} = :queue.peek(buffer)
    age = now_ms() - r_req(req, :ctime)

    case age < max_linger_ms do
      true ->
        {:delay, max_linger_ms - age}

      false ->
        take_reqs_loop(buf, _acc = [], _acc_bytes = 0)
    end
  end

  defp take_reqs_loop(r_buf(buffer_count: 0, pending: pending) = buf, acc, acc_bytes) do
    case :queue.is_empty(pending) do
      true ->
        [_ | _] = acc
        {:lists.reverse(acc), buf}

      false ->
        new_buf = maybe_buffer(buf)
        take_reqs_loop_2(new_buf, acc, acc_bytes)
    end
  end

  defp take_reqs_loop(buf, acc, acc_bytes) do
    take_reqs_loop_2(buf, acc, acc_bytes)
  end

  defp take_reqs_loop_2(
         r_buf(buffer_count: buffer_count, buffer: buffer, max_batch_size: max_batch_size) = buf,
         acc,
         acc_bytes
       ) do
    {:value, req} = :queue.peek(buffer)
    batch_size = acc_bytes + r_req(req, :bytes)

    case acc !== [] and batch_size > max_batch_size do
      true ->
        new_buf = maybe_buffer(buf)
        {:lists.reverse(acc), new_buf}

      false ->
        {_, rest} = :queue.out(buffer)
        new_buf = r_buf(buf, buffer_count: buffer_count - 1, buffer: rest)
        take_reqs_loop(new_buf, [req | acc], batch_size)
    end
  end

  defp do_send(
         reqs,
         r_buf(onwire_count: on_wire_count, onwire: on_wire, send_fun: send_fun) = buf,
         conn,
         vsn
       ) do
    batch = :lists.flatmap(fn r_req(data: data) -> data end, reqs)

    case apply_sendfun(send_fun, conn, batch, vsn) do
      :ok ->
        :ok = :lists.foreach(&eval_acked/1, reqs)
        maybe_send(buf, conn, vsn)

      {:ok, ref} ->
        new_buf = r_buf(buf, onwire_count: on_wire_count + 1, onwire: on_wire ++ [{ref, reqs}])
        maybe_send(new_buf, conn, vsn)

      {:error, reason} ->
        new_buf = rebuffer_or_crash(reqs, buf, reason)
        {:retry, new_buf}
    end
  end

  defp apply_sendfun({send_fun, extra_arg}, conn, batch, vsn) do
    send_fun.(extra_arg, conn, batch, vsn)
  end

  defp apply_sendfun(send_fun, conn, batch, vsn) do
    send_fun.(conn, batch, vsn)
  end

  defp rebuffer_or_crash([r_req(failures: failures) | _], r_buf(max_retries: max_retries), reason)
       when max_retries >= 0 and failures >= max_retries do
    exit({:reached_max_retries, reason})
  end

  defp rebuffer_or_crash(reqs0, r_buf(buffer: buffer, buffer_count: buffer_count) = buf, _reason) do
    reqs =
      :lists.map(
        fn r_req(failures: failures) = req -> r_req(req, failures: failures + 1) end,
        reqs0
      )

    new_buffer =
      :lists.foldr(fn req, acc_buffer -> :queue.in_r(req, acc_buffer) end, buffer, reqs)

    r_buf(buf, buffer: new_buffer, buffer_count: length(reqs) + buffer_count)
  end

  defp maybe_buffer(
         r_buf(
           buffer_limit: buffer_limit,
           buffer_count: buffer_count,
           pending: pending,
           buffer: buffer
         ) = buf
       )
       when buffer_count < buffer_limit do
    case :queue.out(pending) do
      {{:value, req}, new_pending} ->
        :ok = eval_buffered(req)

        new_buf =
          r_buf(buf,
            buffer_count: buffer_count + 1,
            pending: new_pending,
            buffer: :queue.in(req, buffer)
          )

        maybe_buffer(new_buf)

      {:empty, _} ->
        buf
    end
  end

  defp maybe_buffer(r_buf() = buf) do
    buf
  end

  defp eval_buffered(r_req(buf_cb: buf_cb)) do
    _ = apply_bufcb(buf_cb, :brod_produce_req_buffered)
    :ok
  end

  defp eval_acked(req) do
    _ = eval_acked(req, -1)
    :ok
  end

  defp eval_acked(r_req(buf_cb: buf_cb, msg_cnt: count), offset) do
    _ = apply_bufcb(buf_cb, {:brod_produce_req_acked, offset})
    next_base_offset(offset, count)
  end

  defp apply_bufcb({buf_cb, extra_arg}, arg) do
    buf_cb.(extra_arg, arg)
  end

  defp apply_bufcb(buf_cb, arg) do
    buf_cb.(arg)
  end

  defp next_base_offset(-1, _) do
    -1
  end

  defp next_base_offset(offset, count) do
    offset + count
  end

  defp data_size(data) do
    BrodUtils.bytes(data)
  end

  defp now_ms do
    {m, s, micro} = :os.timestamp()
    (m * 1_000_000 + s) * 1000 + div(micro, 1000)
  end

  def empty_buffers(r_buf() = buffer) do
    r_buf(buffer, pending: :queue.new(), buffer: :queue.new(), onwire: [])
  end
end
