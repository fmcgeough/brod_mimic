defmodule BrodMimic.ProducerBuffer do
  @moduledoc """
  Buffers used to produce messages
  """
  use BrodMimic.Macros

  import Record, only: [defrecordp: 2]

  alias BrodMimic.Brod
  alias BrodMimic.Utils, as: BrodUtils

  defrecordp(:req,
    buf_cb: :undefined,
    data: :undefined,
    bytes: :undefined,
    msg_cnt: :undefined,
    ctime: :undefined,
    failures: :undefined
  )

  defrecordp(:buf,
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

  @type batch() :: [{Brod.key(), Brod.value()}]
  @type milli_ts() :: pos_integer()
  @type milli_sec() :: non_neg_integer()
  @type count() :: non_neg_integer()
  @type buf_cb_arg() :: :buffered | {:acked, offset()}
  @type buf_cb() :: (buf_cb_arg() -> :ok) | {(any(), buf_cb_arg() -> :ok), any()}

  @type vsn() :: BrodMimic.KafkaApis.vsn()
  @type send_fun_res() :: :ok | {:ok, reference()} | {:error, any()}

  @type send_fun() ::
          (pid(), batch(), vsn() -> send_fun_res())
          | {(any(), pid(), batch(), vsn() -> send_fun_res()), any()}

  @type send_action() :: :ok | :retry | {:delay, milli_sec()}

  @typedoc """
  - `msg_cnt` - messages in the set
  - `ctime` - time when request was created
  - `failures` - the number of failed attempts
  """
  @type req() ::
          record(:req,
            buf_cb: buf_cb(),
            data: batch(),
            bytes: non_neg_integer(),
            msg_cnt: non_neg_integer(),
            ctime: milli_ts(),
            failures: non_neg_integer()
          )

  @type buf() ::
          record(:buf,
            buffer_limit: pos_integer(),
            onwire_limit: pos_integer(),
            max_batch_size: pos_integer(),
            max_retries: integer(),
            max_linger_ms: milli_sec(),
            max_linger_count: count(),
            send_fun: send_fun(),
            buffer_count: non_neg_integer(),
            onwire_count: non_neg_integer(),
            pending: :queue.queue(),
            buffer: :queue.queue(),
            onwire: [{reference(), [req()]}]
          )

  @doc """
  Create a new buffer

  For more details: See `BrodMimic.Producer.start_link/4`
  """
  @spec new(
          pos_integer(),
          pos_integer(),
          pos_integer(),
          integer(),
          milli_sec(),
          count(),
          send_fun()
        ) :: buf()
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

    buf(
      buffer_limit: buffer_limit,
      onwire_limit: on_wire_limit,
      max_batch_size: max_batch_size,
      max_retries: max_retry,
      max_linger_ms: max_linger_ms,
      max_linger_count: max_linger_count,
      send_fun: send_fun,
      pending: :queue.new(),
      buffer: :queue.new()
    )
  end

  def bad_init_error do
    :erlang.error(:bad_init)
  end

  @doc """
  Buffer a produce request

  Respond to caller immediately if the buffer limit is not yet reached.
  """
  @spec add(buf(), buf_cb(), Brod.batch_input()) :: buf()
  def add(buf(pending: pending) = buf, buf_cb, batch) do
    req =
      req(
        buf_cb: buf_cb,
        data: batch,
        bytes: data_size(batch),
        msg_cnt: length(batch),
        ctime: now_ms(),
        failures: 0
      )

    maybe_buffer(buf(buf, pending: :queue.in(req, pending)))
  end

  @doc """
  Maybe (if there is any produce requests buffered) send the produce
  request to Kafka. In case a request has been tried for more maximum allowed
  times an 'exit' exception is raised.
  Return `{res, new_buffer}`, where `res` can be:

  - `:ok` There is no more message left to send, or allowed to send due to
    onwire limit, caller should wait for the next event to trigger a new
    `maybe_send` call.  Such event can either be a new produce request or
    a produce response from kafka.
  - `{:delay, time}` Caller should retry after the returned milli-seconds.
  - `:retry` Failed to send a batch, caller should schedule a delayed retry.
  """
  @spec maybe_send(buf(), pid(), vsn()) :: {send_action(), buf()}
  def maybe_send(buf() = buf, conn, vsn) do
    case take_reqs(buf) do
      false ->
        {:ok, buf}

      {:delay, t} ->
        {{:delay, t}, buf}

      {reqs, new_buf} ->
        do_send(reqs, new_buf, conn, vsn)
    end
  end

  @doc """
  Reply `acked` to callers
  """
  @spec ack(buf(), reference()) :: buf()
  def ack(buf, ref) do
    ack(buf, ref, -1)
  end

  def ack(
        buf(onwire_count: on_wire_count, onwire: [{ref, reqs} | rest]) = buf,
        ref,
        base_offset
      ) do
    _ = :lists.foldl(&eval_acked/2, base_offset, reqs)
    buf(buf, onwire_count: on_wire_count - 1, onwire: rest)
  end

  def nack(buf(onwire: [{ref, _reqs} | _]) = buf, ref, reason) do
    nack_all(buf, reason)
  end

  def nack_all(buf(onwire: on_wire) = buf, reason) do
    all_on_wire_reqs = :lists.flatmap(fn {_ref, reqs} -> reqs end, on_wire)
    new_buf = buf(buf, onwire_count: 0, onwire: [])
    rebuffer_or_crash(all_on_wire_reqs, new_buf, reason)
  end

  def is_empty(buf(pending: pending, buffer: buffer, onwire: onwire)) do
    :queue.is_empty(pending) and :queue.is_empty(buffer) and onwire === []
  end

  defp take_reqs(buf(onwire_count: on_wire_count, onwire_limit: on_wire_limit))
       when on_wire_count >= on_wire_limit do
    false
  end

  defp take_reqs(buf(buffer: buffer, pending: pending) = buf) do
    case :queue.is_empty(buffer) and :queue.is_empty(pending) do
      true ->
        false

      false ->
        new_buf = maybe_buffer(buf)
        do_take_reqs(new_buf)
    end
  end

  defp do_take_reqs(buf(max_linger_count: max_linger_count, buffer_count: buffer_count) = buf)
       when buffer_count >= max_linger_count do
    take_reqs_loop(buf, _acc = [], _acc_bytes = 0)
  end

  defp do_take_reqs(buf(max_linger_ms: max_linger_ms, buffer: buffer) = buf) do
    {:value, req} = :queue.peek(buffer)
    age = now_ms() - req(req, :ctime)

    case age < max_linger_ms do
      true ->
        {:delay, max_linger_ms - age}

      false ->
        take_reqs_loop(buf, _acc = [], _acc_bytes = 0)
    end
  end

  defp take_reqs_loop(buf(buffer_count: 0, pending: pending) = buf, acc, acc_bytes) do
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
         buf(buffer_count: buffer_count, buffer: buffer, max_batch_size: max_batch_size) = buf,
         acc,
         acc_bytes
       ) do
    {:value, req} = :queue.peek(buffer)
    batch_size = acc_bytes + req(req, :bytes)

    case acc !== [] and batch_size > max_batch_size do
      true ->
        new_buf = maybe_buffer(buf)
        {:lists.reverse(acc), new_buf}

      false ->
        {_, rest} = :queue.out(buffer)
        new_buf = buf(buf, buffer_count: buffer_count - 1, buffer: rest)
        take_reqs_loop(new_buf, [req | acc], batch_size)
    end
  end

  defp do_send(
         reqs,
         buf(onwire_count: on_wire_count, onwire: on_wire, send_fun: send_fun) = buf,
         conn,
         vsn
       ) do
    batch = :lists.flatmap(fn req(data: data) -> data end, reqs)

    case apply_sendfun(send_fun, conn, batch, vsn) do
      :ok ->
        :ok = :lists.foreach(&eval_acked/1, reqs)
        maybe_send(buf, conn, vsn)

      {:ok, ref} ->
        new_buf = buf(buf, onwire_count: on_wire_count + 1, onwire: on_wire ++ [{ref, reqs}])
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

  defp rebuffer_or_crash([req(failures: failures) | _], buf(max_retries: max_retries), reason)
       when max_retries >= 0 and failures >= max_retries do
    exit({:reached_max_retries, reason})
  end

  defp rebuffer_or_crash(reqs0, buf(buffer: buffer, buffer_count: buffer_count) = buf, _reason) do
    reqs =
      :lists.map(
        fn req(failures: failures) = req -> req(req, failures: failures + 1) end,
        reqs0
      )

    new_buffer = :lists.foldr(fn req, acc_buffer -> :queue.in_r(req, acc_buffer) end, buffer, reqs)

    buf(buf, buffer: new_buffer, buffer_count: length(reqs) + buffer_count)
  end

  defp maybe_buffer(
         buf(
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
          buf(buf,
            buffer_count: buffer_count + 1,
            pending: new_pending,
            buffer: :queue.in(req, buffer)
          )

        maybe_buffer(new_buf)

      {:empty, _} ->
        buf
    end
  end

  defp maybe_buffer(buf() = buf) do
    buf
  end

  defp eval_buffered(req(buf_cb: buf_cb)) do
    _ = apply_bufcb(buf_cb, :brod_produce_req_buffered)
    :ok
  end

  defp eval_acked(req) do
    _ = eval_acked(req, -1)
    :ok
  end

  defp eval_acked(req(buf_cb: buf_cb, msg_cnt: count), offset) do
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

  def empty_buffers(buf() = buffer) do
    buf(buffer, pending: :queue.new(), buffer: :queue.new(), onwire: [])
  end
end
