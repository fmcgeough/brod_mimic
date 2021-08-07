defmodule BrodMimic.Utils do
  @moduledoc """
  Collection of generally useful utility functions
  """

  require BrodMimic.Macros
  import Record, only: [defrecord: 2, extract: 2]

  alias BrodMimic.{Brod, Macros, KafkaRequest}

  @type req_fun() :: (Brod.offset(), :kpro.count() -> :kpro.req())
  @type fetch_fun() ::
          (Brod.offset() -> {:ok, {Brod.offset(), [Brod.message()]}} | {:error, any()})
  @type connection() :: Brod.connection()
  @type conn_config() :: Brod.conn_config()
  @type topic() :: Brod.topic()
  @type topic_config() :: Brod.topic_config()
  @type partition() :: Brod.partition()
  @type endpoint() :: Brod.endpoint()
  @type offset_time() :: Brod.offset_time()
  @type group_id() :: Brod.group_id()

  defrecord :kpro_req, extract(:kpro_req, from_lib: "kafka_protocol/include/kpro.hrl")

  @doc """
  This is equivalent to the `create_topics(hosts, topic_configs, request_configs, [])`
  """
  @spec create_topics([endpoint()], [topic_config()], %{
          timeout: :kpro.int32(),
          validate_only: boolean()
        }) ::
          {:ok, Brod.topic_config()} | {:error, any()} | :ok
  def create_topics(hosts, topic_configs, request_configs) do
    create_topics(hosts, topic_configs, request_configs, _conn_cfg = [])
  end

  @doc """
  Try to connect to the controller node using the given
  connection options and create the given topics with configs
  """
  @spec create_topics(
          [endpoint()],
          [topic_config()],
          %{timeout: :kpro.int32(), validate_only: boolean()},
          conn_config()
        ) ::
          {:ok, Brod.topic_config()} | {:error, any()} | :ok
  def create_topics(hosts, topic_configs, request_configs, conn_cfg) do
    with_conn(:kpro.connect_controller(hosts, nolink(conn_cfg)), fn pid ->
      request = :brod_kafka_request.create_topics(pid, topic_configs, request_configs)
      request_sync(pid, request)
    end)
  end

  @doc """
  @equiv delete_topics(Hosts, Topics, Timeout, [])
  """
  @spec delete_topics([endpoint()], [topic()], pos_integer()) ::
          {:ok, :kpro.struct()} | {:error, any()}
  def delete_topics(hosts, topics, timeout) do
    delete_topics(hosts, topics, timeout, _conn_cfg = [])
  end

  @doc """
  Try to connect to the controller node using the given
  connection options and delete the given topics with a timeout
  """
  @spec delete_topics([endpoint()], [topic()], pos_integer(), conn_config()) ::
          {:ok, :kpro.struct()} | {:error, any()}
  def delete_topics(hosts, topics, timeout, conn_cfg) do
    with_conn(
      :kpro.connect_controller(hosts, nolink(conn_cfg)),
      fn pid ->
        request = :brod_kafka_request.delete_topics(pid, topics, timeout)
        request_sync(pid, request, timeout)
      end
    )
  end

  @doc """
  Try to connect to any of the bootstrap nodes and fetch metadata
  all topics
  """
  @spec get_metadata([endpoint()]) :: {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(hosts) do
    get_metadata(hosts, :all)
  end

  @doc """
  Try to connect to any of the bootstrap nodes and fetch metadata
  for the given topics
  """
  @spec get_metadata([endpoint()], :all | [topic()]) :: {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(hosts, topics) do
    get_metadata(hosts, topics, _conn_cfg = [])
  end

  @doc """
  Try to connect to any of the bootstrap nodes using the given
  connection options and fetch metadata for the given topics.
  """
  @spec get_metadata([endpoint()], :all | [topic()], conn_config()) ::
          {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(hosts, topics, conn_cfg) do
    with_conn(hosts, conn_cfg, fn pid ->
      request = :brod_kafka_request.metadata(pid, topics)
      request_sync(pid, request)
    end)
  end

  @doc """
  Resolve timestamp to real offset.
  Pass connect_timeout prop as the default timeout
  for `kpro:connect_partition_leader/5`.
  """
  @spec resolve_offset([endpoint()], topic(), partition(), offset_time(), conn_config()) ::
          {:ok, Brod.offset()} | {:error, any()}
  def resolve_offset(hosts, topic, partition, time, conn_cfg) do
    timeout = :proplists.get_value(:connect_timeout, conn_cfg, Macros.brod_default_timeout())
    opts = %{timeout: timeout}
    resolve_offset(hosts, topic, partition, time, conn_cfg, opts)
  end

  @doc """
  Resolve timestamp to real offset.
  """
  @spec resolve_offset([endpoint()], topic(), partition(), offset_time(), conn_config(), any()) ::
          {:ok, Brod.offset()} | {:error, any()}
  def resolve_offset(hosts, topic, partition, time, conn_cfg, opts) do
    with_conn(
      :kpro.connect_partition_leader(hosts, nolink(conn_cfg), topic, partition, opts),
      fn pid -> resolve_offset(pid, topic, partition, time) end
    )
  end

  @doc """
  Resolve timestamp or semantic offset to real offset.
  The give pid should be the connection to partition leader broker.
  """
  @spec resolve_offset(pid(), topic(), partition(), offset_time()) ::
          {:ok, Brod.offset()} | {:error, any()}
  def resolve_offset(pid, topic, partition, time) do
    req = KafkaRequest.list_offsets(pid, topic, partition, time)

    case request_sync(pid, req) do
      {:ok, %{error_code: ec}} when Macros.is_error(ec) ->
        {:error, ec}

      {:ok, %{offset: offset}} ->
        {:ok, offset}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec request_sync(connection(), Brod.req()) :: :ok | {:ok, term()} | {:error, any()}
  def request_sync(conn, req) do
    request_sync(conn, req, :infinity)
  end

  @spec request_sync(connection(), Brod.req(), :infinity | timeout()) ::
          :ok | {:ok, term()} | {:error, any()}
  def request_sync(conn, kpro_req(ref: _ref) = req, timeout) when is_pid(conn) do
    # kpro_connection has a global 'request_timeout' option
    # the connection pid will exit if that one times out
    case :kpro.request_sync(conn, req, timeout) do
      {:ok, %{ref: _ref} = rsp} -> parse_rsp(rsp)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Parse decoded kafka response (`#kpro_rsp{}') into a more generic representation

  * Return `:ok' if it is a trivial 'ok or not' response without data fields
  * Return `{:ok, Result}' for some of the APIs when no error-code found in
    response. Result could be a transformed representation of response message
    body `#kpro_rsp.msg' or the response body itself.
  * For some APIs, it returns `{:error, code_or_message}` when error-code is not
    `no_error` in the message body.

  NOTE: Not all error codes are interpreted as `{:error, code_or_message}` tuple.
         for some of the complex response bodies, error-codes are retained
         for caller to parse.
  """
  @spec parse_rsp(map()) :: :ok | {:ok, term()} | {:error, any()}
  def parse_rsp(%{api: api, vsn: vsn, msg: msg} = _kpro_resp) do
    case parse(api, vsn, msg) do
      :ok -> :ok
      result -> {:ok, result}
    end
  catch
    error_code_or_message ->
      {:error, error_code_or_message}
  end

  def with_conn({:ok, pid}, fun) do
    fun.(pid)
  after
    :kpro.close_connection(pid)
  end

  def with_conn({:error, reason}, _run) do
    {:error, reason}
  end

  def with_conn(endpoints, conn_cfg, fun) when is_list(conn_cfg) do
    with_conn(endpoints, :maps.from_list(conn_cfg), fun)
  end

  def with_conn(endpoints, conn_cfg, fun) do
    :kpro_brokers.with_connection(endpoints, conn_cfg, fun)
  end

  def parse(:produce, _Vsn, msg) do
    :kpro.find(:base_offset, get_partition_rsp(msg))
  end

  def parse(_, _, _), do: :ok

  def get_partition_rsp(struct) do
    [topic_rsp] = :kpro.find(:responses, struct)
    [partition_rsp] = :kpro.find(:partition_responses, topic_rsp)
    partition_rsp
  end

  def nolink(c) when is_list(c), do: [{:nolink, true} | c]
  def nolink(c) when is_map(c), do: %{nolink: true}

  @doc """
  Check terminate reason for a GenServer
  """
  def is_normal_reason(:normal), do: true
  def is_normal_reason(:shutdown), do: true
  def is_normal_reason({:shutdown, _}), do: true
  def is_normal_reason(_), do: false

  def is_pid_alive(pid) do
    is_pid(pid) && Process.alive?(pid)
  end

  @doc """
  Get now timestamp, and format as UTC string.
  """
  @spec os_time_utc_str :: [any()]
  def os_time_utc_str do
    ts = :os.timestamp()
    {{y, m, d}, {h, min, sec}} = :calendar.now_to_universal_time(ts)
    {_, _, micro} = ts

    s =
      :io_lib.format(
        "~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
        [y, m, d, h, min, sec, micro]
      )

    :lists.flatten(s)
  end

  @doc """
  Execute a callback from a module, if present.
  """
  @spec optional_callback(module(), atom(), list(), any()) :: any()
  def optional_callback(module, function, args, default) do
    arity = length(args)

    case Kernel.function_exported?(module, function, arity) do
      true ->
        Kernel.apply(module, function, args)

      false ->
        default
    end
  end

  @doc """
  Milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
  """
  @spec epoch_ms :: :kpro.msg_ts()
  def epoch_ms do
    DateTime.utc_now() |> DateTime.to_unix(:millisecond)
  end

  @doc """
  Raise an 'error' exception when first argument is not 'true'.
  The second argument is used as error reason.
  """
  @spec ok_when(boolean(), any()) :: :ok | no_return()
  def ok_when(true, _), do: :ok
  def ok_when(_, reason), do: :erlang.error(reason)
end
