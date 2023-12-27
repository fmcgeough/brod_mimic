defmodule BrodMimic.Utils do
  @moduledoc """
  Collection of generally useful utility functions
  """

  use BrodMimic.Macros

  import Bitwise

  alias BrodMimic.Brod
  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.KafkaRequest, as: BrodKafkaRequest

  @type req_fun() :: (offset(), :kpro.count() -> :kpro.req())
  @type fetch_fun() ::
          (offset() -> {:ok, {offset(), [Brod.message()]}} | {:error, any()})
  @type connection() :: Brod.connection()
  @type conn_config() :: Brod.conn_config()
  @type topic_config() :: Brod.topic_config()
  @type endpoint() :: Brod.endpoint()

  @doc """
  This is equivalent to the `create_topics(hosts, topic_configs, request_configs, [])`
  """
  @spec create_topics([endpoint()], [topic_config()], request_configs()) ::
          {:ok, topic_config()} | {:error, any()} | :ok
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
          request_configs(),
          conn_config()
        ) ::
          {:ok, Brod.topic_config()} | {:error, any()} | :ok
  def create_topics(hosts, topic_configs, request_configs, conn_cfg) do
    with_conn(:kpro.connect_controller(hosts, nolink(conn_cfg)), fn pid ->
      request = BrodKafkaRequest.create_topics(pid, topic_configs, request_configs)
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
        request = BrodKafkaRequest.delete_topics(pid, topics, timeout)
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
      request = BrodKafkaRequest.metadata(pid, topics)
      request_sync(pid, request)
    end)
  end

  @doc """
  Resolve timestamp to real offset.
  Pass connect_timeout prop as the default timeout
  for `kpro:connect_partition_leader/5`.
  """
  @spec resolve_offset([endpoint()], topic(), partition(), offset_time(), conn_config()) ::
          {:ok, offset()} | {:error, any()}
  def resolve_offset(hosts, topic, partition, time, conn_cfg) do
    timeout = :proplists.get_value(:connect_timeout, conn_cfg, brod_default_timeout())
    opts = %{timeout: timeout}
    resolve_offset(hosts, topic, partition, time, conn_cfg, opts)
  end

  @doc """
  Resolve timestamp to real offset.
  """
  @spec resolve_offset([endpoint()], topic(), partition(), offset_time(), conn_config(), any()) ::
          {:ok, offset()} | {:error, any()}
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
          {:ok, offset()} | {:error, any()}
  def resolve_offset(pid, topic, partition, time) do
    req = BrodKafkaRequest.list_offsets(pid, topic, partition, time)

    case request_sync(pid, req) do
      {:ok, %{error_code: ec}} when ec != :no_error ->
        {:error, ec}

      {:ok, %{offset: offset}} ->
        {:ok, offset}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Check terminate reason for a GenServer
  """
  @spec is_normal_reason(any()) :: boolean()
  def is_normal_reason(:normal), do: true
  def is_normal_reason(:shutdown), do: true
  def is_normal_reason({:shutdown, _}), do: true
  def is_normal_reason(_), do: false

  @doc """
  Is the process passed as argument alive?
  """
  @spec is_pid_alive(pid()) :: boolean()
  def is_pid_alive(pid) do
    is_pid(pid) && Process.alive?(pid)
  end

  @doc """
  Get now timestamp, and format as UTC string.

  This should probably be changed to just:
  `DateTime.utc_now() |> DateTime.to_iso8601(:extended, 0)`
  """
  @spec os_time_utc_str :: [any()]
  def os_time_utc_str do
    DateTime.utc_now()
    |> DateTime.to_naive()
    |> NaiveDateTime.to_string()
    |> String.replace(" ", ":")
    |> String.to_charlist()
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
  Assert client_id is an atom()
  """
  @spec assert_client(Brod.client_id() | pid()) :: :ok | no_return()
  def assert_client(client) do
    ok_when(
      is_atom(client) or is_pid(client),
      {:bad_client, client}
    )
  end

  @doc """
  Assert group_id is a binary()
  """
  @spec assert_group_id(group_id()) :: :ok | no_return()
  def assert_group_id(group_id) do
    ok_when(
      is_binary(group_id) and :erlang.size(group_id) > 0,
      {:bad_group_id, group_id}
    )
  end

  @doc """
  Assert a list of topic names [binary()]
  """
  @spec assert_topics([topic()]) :: :ok | no_return()
  def assert_topics(topics) do
    pred = fn topic ->
      :ok === assert_topic(topic)
    end

    ok_when(
      is_list(topics) and topics !== [] and
        :lists.all(
          pred,
          topics
        ),
      {:bad_topics, topics}
    )
  end

  @doc """
  Assert topic is a binary()
  """
  @spec assert_topic(topic()) :: :ok | no_return()
  def assert_topic(topic) do
    ok_when(
      is_binary(topic) and :erlang.size(topic) > 0,
      {:bad_topic, topic}
    )
  end

  @doc """
  Make a flat message list from decoded batch list.

  Return the next begin-offset together with the messages
  """
  @spec flatten_batches(offset(), map(), [:kpro.batch()]) :: {offset(), [:kpro.message()]}
  def flatten_batches(begin_offset, _, []) do
    # empty batch implies we have reached the end of a partition,
    # we do not want to advance begin-offset here,
    # instead, we should try again (after a delay) with the same offset
    {begin_offset, []}
  end

  def flatten_batches(begin_offset, header, batches0) do
    {last_meta, _} = :lists.last(batches0)
    batches = drop_aborted(header, batches0)

    msg_list =
      :lists.append(
        for {meta, msgs} <- batches,
            not is_control(meta) do
          msgs
        end
      )

    case last_meta do
      %{last_offset: last_offset} ->
        {last_offset + 1, drop_old_messages(begin_offset, msg_list)}

      _ when msg_list !== [] ->
        kafka_message(offset: offset) = :lists.last(msg_list)
        {offset + 1, drop_old_messages(begin_offset, msg_list)}

      _ ->
        {begin_offset + 1, []}
    end
  end

  @doc """
  Fetch a single message set from the given topic-partition
  """
  @spec fetch(connection() | Brod.client_id() | Brod.bootstrap(), topic(), partition(), offset(), Brod.fetch_opts()) ::
          {:ok, {offset(), [Brod.message()]}} | {:error, any()}
  def fetch(hosts, topic, partition, offset, opts)
      when is_list(hosts) do
    fetch({hosts, []}, topic, partition, offset, opts)
  end

  def fetch({hosts, conn_cfg}, topic, partition, offset, opts) do
    kpro_opts = kpro_connection_options(conn_cfg)

    with_conn(
      :kpro.connect_partition_leader(hosts, nolink(conn_cfg), topic, partition, kpro_opts),
      fn conn ->
        fetch(conn, topic, partition, offset, opts)
      end
    )
  end

  def fetch(client, topic, partition, offset, opts)
      when is_atom(client) do
    case BrodClient.get_leader_connection(client, topic, partition) do
      {:ok, conn} ->
        fetch(conn, topic, partition, offset, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def fetch(conn, topic, partition, offset, opts) do
    fetch = make_fetch_fun(conn, topic, partition, opts)
    fetch.(offset)
  end

  @spec fold(
          connection() | Brod.client_id() | Brod.bootstrap(),
          topic(),
          partition(),
          offset(),
          Brod.fetch_opts(),
          Brod.fold_acc(),
          Brod.fold_fun(),
          Brod.fold_limits()
        ) :: Brod.fold_result()
  def fold(hosts, topic, partition, offset, opts, acc, fun, limits)
      when is_list(hosts) do
    fold({hosts, []}, topic, partition, offset, opts, acc, fun, limits)
  end

  def fold({hosts, conn_cfg}, topic, partition, offset, opts, acc, fun, limits) do
    kpro_opts = kpro_connection_options(conn_cfg)

    case with_conn(
           :kpro.connect_partition_leader(hosts, nolink(conn_cfg), topic, partition, kpro_opts),
           fn conn ->
             fold(conn, topic, partition, offset, opts, acc, fun, limits)
           end
         ) do
      {:error, reason} ->
        {acc, offset, {:error, reason}}

      {_, _, _} = fold_result ->
        fold_result
    end
  end

  def fold(client, topic, partition, offset, opts, acc, fun, limits)
      when is_atom(client) do
    case BrodClient.get_leader_connection(client, topic, partition) do
      {:ok, conn} ->
        fold(conn, topic, partition, offset, opts, acc, fun, limits)

      {:error, reason} ->
        {acc, offset, {:error, reason}}
    end
  end

  def fold(conn, topic, partition, offset, opts, acc, fun, limits) do
    fetch = make_fetch_fun(conn, topic, partition, opts)
    infinity = 1 <<< 64
    end_offset = Map.get(limits, :reach_offset, infinity)
    count_limit = Map.get(limits, :message_count, infinity)
    count_limit < 1 and :erlang.error(:bad_message_count)

    spawn = fn o ->
      spawn_monitor(fn ->
        exit(fetch.(o))
      end)
    end

    do_fold(spawn, spawn.(offset), offset, acc, fun, end_offset, count_limit)
  end

  @doc """
  Make a fetch function which should expand `max_bytes` when
  it is not big enough to fetch one single message.
  """
  @spec make_fetch_fun(pid(), topic(), partition(), Brod.fetch_opts()) :: fetch_fun()
  def make_fetch_fun(conn, topic, partition, fetch_opts) do
    wait_time = Map.get(fetch_opts, :max_wait_time, 1000)
    min_bytes = Map.get(fetch_opts, :min_bytes, 1)
    max_bytes = Map.get(fetch_opts, :max_bytes, bsl(1, 20))
    isolation_level = Map.get(fetch_opts, :isolation_level, :kpro_read_committed)
    req_fun = make_req_fun(conn, topic, partition, wait_time, min_bytes, isolation_level)
    fn offset -> __MODULE__.fetch(conn, req_fun, offset, max_bytes) end
  end

  @doc """
  Make a partition function
  """
  @spec make_part_fun(Brod.partitioner()) :: Brod.partition_fun()
  def make_part_fun(:random) do
    fn _, partition_count, _, _ ->
      {:ok, :rand.uniform(partition_count) - 1}
    end
  end

  def make_part_fun(:hash) do
    fn _, partition_count, key, _ ->
      {:ok, rem(:erlang.phash2(key), partition_count)}
    end
  end

  def make_part_fun(f) do
    f
  end

  @doc """
  Hide sasl plain password in an anonymous function to avoid
  the plain text being dumped to crash logs
  """
  @spec init_sasl_opt(Brod.client_config()) :: Brod.client_config()
  def init_sasl_opt(config) do
    case get_sasl_opt(config) do
      {mechanism, user, pass} when mechanism !== :callback ->
        replace_prop(:sasl, {mechanism, user, fn -> pass end}, config)

      _other ->
        config
    end
  end

  @doc """
  Fetch committed offsets for the given topics in a consumer group.

  1. try find out the group coordinator broker from the bootstrap hosts
  2. send `offset_fetch` request and wait for response.

  If Topics is an empty list, fetch offsets for all topics in the group
  """
  @spec fetch_committed_offsets([endpoint()], conn_config(), group_id(), [topic()]) ::
          {:ok, [:kpro.struct()]} | {:error, any()}
  def fetch_committed_offsets(bootstrap_endpoints, conn_cfg, group_id, topics) do
    kpro_opts = kpro_connection_options(conn_cfg)

    args = Map.merge(kpro_opts, %{type: :group, id: group_id})

    with_conn(
      :kpro.connect_coordinator(bootstrap_endpoints, nolink(conn_cfg), args),
      fn pid ->
        do_fetch_committed_offsets(pid, group_id, topics)
      end
    )
  end

  @doc """
   Fetch committed offsets for the given topics in a consumer group

  1. Get broker endpoint by calling `BrodMimic.Client.get_group_coordinator`
  2. Establish a connection to the discovered endpoint.
  3. send `offset_fetch` request and wait for response.

  If Topics is an empty list, fetch offsets for all topics in the group
  """
  @spec fetch_committed_offsets(Brod.client(), group_id(), [topic()]) :: {:ok, [:kpro.struct()]} | {:error, any()}
  def fetch_committed_offsets(client, group_id, topics) do
    case BrodClient.get_group_coordinator(client, group_id) do
      {:ok, {endpoint, conn_cfg}} ->
        case :kpro.connect(endpoint, conn_cfg) do
          {:ok, conn} ->
            rsp = do_fetch_committed_offsets(conn, group_id, topics)
            :kpro.close_connection(conn)
            rsp

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_sasl_opt(config) do
    case :proplists.get_value(:sasl, config) do
      :undefined ->
        {:sasl, :undefined}

      {:callback, module, args} ->
        {:callback, module, args}

      {mechanism, file} when is_list(file) or is_binary(file) ->
        {mechanism, file}

      other ->
        other
    end
  end

  defp do_fetch_committed_offsets(conn, group_id, topics) when is_pid(conn) do
    req = BrodKafkaRequest.offset_fetch(conn, group_id, topics)

    case request_sync(conn, req) do
      {:ok, msg} ->
        {:ok, :kpro.find(:topics, msg)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetch a message-set. If the given `max_bytes` is not enough to fetch a
  single message, expand it to fetch exactly one message
  """
  @spec fetch(connection(), req_fun(), offset(), :kpro.count()) :: {:ok, {offset(), [Brod.message()]}} | {:error, any()}
  def fetch(conn, req_fun, offset, max_bytes) do
    request = req_fun.(offset, max_bytes)

    case request_sync(conn, request, :infinity) do
      {:ok, %{error_code: error_code}} when error_code != :no_error ->
        {:error, error_code}

      {:ok, %{batches: {:incomplete_batch, size}}} ->
        fetch(conn, req_fun, offset, size)

      {:ok, %{header: header, batches: batches}} ->
        stable_offset = get_stable_offset(header)
        {new_begin_offset, msgs} = flatten_batches(offset, header, batches)

        case offset < stable_offset and msgs == [] do
          true ->
            # Not reached the latest stable offset yet,
            # but received an empty batch-set (all messages are dropped).
            # try again with new begin-offset
            fetch(conn, req_fun, new_begin_offset, max_bytes)

          false ->
            {:ok, {stable_offset, msgs}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  List all groups in the given cluster

  _Exception if failed against any of the coordinator brokers_.
  """
  @spec list_all_groups([endpoint()], conn_config()) :: [{endpoint(), [Brod.cg()] | {:error, any()}}]
  def list_all_groups(endpoints, options) do
    {:ok, metadata} = get_metadata(endpoints, [], options)
    brokers0 = :kpro.find(:brokers, metadata)

    brokers =
      for b <- brokers0 do
        {:erlang.binary_to_list(:kpro.find(:host, b)), :kpro.find(:port, b)}
      end

    :lists.foldl(
      fn broker, acc ->
        case list_groups(broker, options) do
          {:ok, groups} ->
            [{broker, groups} | acc]

          {:error, reason} ->
            [{broker, {:error, reason}} | acc]
        end
      end,
      [],
      brokers
    )
  end

  @doc """
  List all groups in the given coordinator broker
  """
  @spec list_groups(endpoint(), conn_config()) :: {:ok, [cg()]} | {:error, any()}
  def list_groups(endpoint, conn_cfg) do
    with_conn([endpoint], conn_cfg, fn pid ->
      request = BrodKafkaRequest.list_groups(pid)

      case request_sync(pid, request) do
        {:ok, groups0} ->
          groups = map_list_groups_result(groups0)
          {:ok, groups}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  @doc """
  Send describe_groups_request and wait for describe_groups_response.
  """
  @spec describe_groups(endpoint(), conn_config(), [Brod.group_id()]) :: {:ok, :kpro.struct()} | {:error, any()}
  def describe_groups(coordinator_endpoint, conn_cfg, ids) do
    with_conn([coordinator_endpoint], conn_cfg, fn pid ->
      req = :kpro.make_request(:describe_groups, 0, [{:groups, ids}])
      request_sync(pid, req)
    end)
  end

  @doc """
  Return message set size in number of bytes.

  _This does not include the overheads of encoding protocol.
  such as magic bytes, attributes, and length tags etc_.
  """
  @spec bytes(Brod.batch_input()) :: non_neg_integer()
  def bytes(msgs) do
    f = fn %{key: key, value: value} = msg, acc ->
      header_size =
        :lists.foldl(
          fn {k, v}, acc_h ->
            :erlang.size(k) + :erlang.size(v) + acc_h
          end,
          0,
          Map.get(msg, :headers, [])
        )

      :erlang.size(key) + :erlang.size(value) + header_size + 8 + acc
    end

    :lists.foldl(f, 0, msgs)
  end

  @doc """
  Group values per-key in a key-value list
  """
  @spec group_per_key([{term(), term()}]) :: [{term(), [term()]}]
  def group_per_key(list) do
    :lists.foldl(fn {key, value}, acc -> :orddict.append_list(key, [value], acc) end, [], list)
  end

  @doc """
  Group values per-key for the map result of a list
  """
  @spec group_per_key((term() -> {term(), term()}), [term()]) :: [{term(), [term()]}]
  def group_per_key(map_fun, list) do
    group_per_key(:lists.map(map_fun, list))
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
  @spec parse_rsp(kpro_rsp()) :: :ok | {:ok, term()} | {:error, any()}
  def parse_rsp(kpro_rsp(api: api, vsn: vsn, msg: msg)) do
    parse(api, vsn, msg)
  catch
    error_code_or_message ->
      {:error, error_code_or_message}
  else
    :ok ->
      :ok

    result ->
      {:ok, result}
  end

  @spec request_sync(connection(), Brod.req()) :: :ok | {:ok, term()} | {:error, any()}
  def request_sync(conn, req) do
    request_sync(conn, req, :infinity)
  end

  @spec request_sync(connection(), Brod.req(), :infinity | timeout()) ::
          :ok | {:ok, term()} | {:error, any()}
  def request_sync(conn, req, timeout) when is_pid(conn) do
    # kpro_connection has a global 'request_timeout' option
    # the connection pid will exit if that one times out
    # request_sync can return :ok but the brod code wasn't
    # handling that
    case :kpro.request_sync(conn, req, timeout) do
      {:ok, rsp} -> parse_rsp(rsp)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Make batch input for Kafka protocol library
  """
  @spec make_batch_input(Brod.key(), Brod.value()) :: Brod.batch_input()
  def make_batch_input(key, value) do
    case is_batch(value) do
      true ->
        unify_batch(value)

      false ->
        [unify_msg(make_msg_input(key, value))]
    end
  end

  @doc """
  last_stable_offset is added in fetch response version 4

  This function takes high watermark offset as last_stable_offset
  in case it's missing.
  """
  @spec get_stable_offset(struct()) :: :kpro.field_value()
  def get_stable_offset(header) do
    high_wm_offset = :kpro.find(:high_watermark, header)
    stable_offset = :kpro.find(:last_stable_offset, header, high_wm_offset)
    min(stable_offset, high_wm_offset)
  end

  @doc """
  Get kpro connection options from brod connection config
  """
  @spec kpro_connection_options(conn_config()) :: %{required(:timeout) => integer()}
  def kpro_connection_options(conn_cfg) do
    timeout =
      case conn_cfg do
        list when is_list(list) ->
          :proplists.get_value(:connect_timeout, list, :timer.seconds(5))

        map when is_map(map) ->
          Map.get(map, :connect_timeout, :timer.seconds(5))
      end

    %{timeout: timeout}
  end

  defp map_list_groups_result(groups0) do
    Enum.map(groups0, fn struct ->
      id = :kpro.find(:group_id, struct)
      type = :kpro.find(:protocol_type, struct)
      brod_cg(id: id, protocol_type: type)
    end)
  end

  defp do_fold(spawn, {pid, mref}, offset, acc, fun, end__, count) do
    receive do
      {:DOWN, ^mref, :process, ^pid, result} ->
        handle_fetch_rsp(spawn, result, offset, acc, fun, end__, count)
    end
  end

  defp handle_fetch_rsp(_spawn, {:error, reason}, offset, acc, _fun, _, _) do
    {acc, offset, {:fetch_failure, reason}}
  end

  defp handle_fetch_rsp(_spawn, {:ok, {stable_offset, []}}, offset, acc, _fun, _end, _count)
       when offset >= stable_offset do
    {acc, offset, :reached_end_of_partition}
  end

  defp handle_fetch_rsp(spawn, {:ok, {_StableOffset, msgs}}, offset, acc, fun, end__, count) do
    kafka_message(offset: last_offset) = :lists.last(msgs)

    fetcher =
      case last_offset < end__ and length(msgs) < count do
        true ->
          spawn.(last_offset + 1)

        false ->
          :undefined
      end

    do_acc(spawn, fetcher, offset, acc, fun, msgs, end__, count)
  end

  defp do_acc(_spawn, fetcher, offset, acc, _fun, _, _end, 0) do
    :undefined = fetcher
    {acc, offset, :reached_message_count_limit}
  end

  defp do_acc(_spawn, fetcher, offset, acc, _fun, _, end__, _count)
       when offset > end__ do
    :undefined = fetcher
    {acc, offset, :reached_target_offset}
  end

  defp do_acc(spawn, fetcher, offset, acc, fun, [], end__, count) do
    do_fold(spawn, fetcher, offset, acc, fun, end__, count)
  end

  defp do_acc(spawn, fetcher, offset, acc, fun, [msg | rest], end__, count) do
    fun.(msg, acc)
  catch
    c, e ->
      :ok
      kill_fetcher(fetcher)
      :erlang.raise(c, e, __STACKTRACE__)
  else
    {:ok, new_acc} ->
      next_offset = kafka_message(msg, :offset) + 1
      do_acc(spawn, fetcher, next_offset, new_acc, fun, rest, end__, count - 1)

    {:error, reason} ->
      :ok = kill_fetcher(fetcher)
      {acc, offset, reason}
  end

  defp kill_fetcher(:undefined) do
    :ok
  end

  defp kill_fetcher({pid, mref}) do
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^mref, :process, _, _} ->
        :ok
    end
  end

  defp drop_aborted(%{aborted_transactions: :undefined}, batches) do
    batches
  end

  defp drop_aborted(%{aborted_transactions: aborted_l}, batches) do
    :lists.foldl(
      fn %{producer_id: producer_id, first_offset: first_offset}, batches_in ->
        do_drop_aborted(producer_id, first_offset, batches_in, [])
      end,
      batches,
      aborted_l
    )
  end

  defp drop_aborted(_, batches) do
    batches
  end

  defp do_drop_aborted(_, _, [], acc) do
    :lists.reverse(acc)
  end

  defp do_drop_aborted(producer_id, first_offset, [{_Meta, []} | batches], acc) do
    do_drop_aborted(producer_id, first_offset, batches, acc)
  end

  defp do_drop_aborted(producer_id, first_offset, [{meta, msgs} | batches], acc) do
    kafka_message(offset: base_offset) = hd(msgs)

    case {is_txn(meta, producer_id), is_control(meta)} do
      {true, true} ->
        :lists.reverse(acc) ++ batches

      {true, false} when base_offset >= first_offset ->
        do_drop_aborted(producer_id, first_offset, batches, acc)

      _ ->
        do_drop_aborted(producer_id, first_offset, batches, [{meta, msgs} | acc])
    end
  end

  defp is_txn(%{is_transaction: true, producer_id: id}, id) do
    true
  end

  defp is_txn(_producer_id, _Meta) do
    false
  end

  defp is_control(%{is_control: true}) do
    true
  end

  defp is_control(_) do
    false
  end

  defp make_req_fun(conn, topic, partition, wait_time, min_bytes, isolation_level) do
    fn offset, max_bytes ->
      BrodKafkaRequest.fetch(
        conn,
        topic,
        partition,
        offset,
        wait_time,
        min_bytes,
        max_bytes,
        isolation_level
      )
    end
  end

  @doc """
  Parse fetch response into a more user-friendly representation.
  """
  def parse_fetch_rsp(msg) do
    ec1 = :kpro.find(:error_code, msg, :no_error)
    session_id = :kpro.find(:session_id, msg, 0)

    {header, batches, ec2} =
      case :kpro.find(:responses, msg) do
        [] ->
          # a session init without data
          {:undefined, [], :no_error}

        _ ->
          partition_rsp = get_partition_rsp(msg)
          header_x = :kpro.find(:partition_header, partition_rsp)
          throw_error_code([header_x])
          records = :kpro.find(:record_set, partition_rsp)
          ecx = :kpro.find(:error_code, header_x)
          {header_x, :kpro.decode_batches(records), ecx}
      end

    error_code =
      case ec2 === :no_error do
        true -> ec1
        false -> ec2
      end

    case is_error(error_code) do
      true -> :erlang.throw(error_code)
      false -> %{session_id: session_id, header: header, batches: batches}
    end
  end

  defp get_partition_rsp(struct) do
    [topic_rsp] = :kpro.find(:responses, struct)
    [partition_rsp] = :kpro.find(:partition_responses, topic_rsp)
    partition_rsp
  end

  @spec replace_prop(term(), term(), :proplists.proplist()) :: :proplists.proplist()
  defp replace_prop(key, value, prop_l0) do
    prop_l = :proplists.delete(key, prop_l0)
    [{key, value} | prop_l]
  end

  defp drop_old_messages(_begin_offset, []) do
    []
  end

  defp drop_old_messages(begin_offset, [message | rest] = all) do
    case kafka_message(message, :offset) < begin_offset do
      true ->
        drop_old_messages(begin_offset, rest)

      false ->
        all
    end
  end

  #
  # Raise an 'error' exception when first argument is not 'true'.
  # The second argument is used as error reason.
  #
  @spec ok_when(boolean(), any()) :: :ok | no_return()
  defp ok_when(true, _) do
    :ok
  end

  defp ok_when(_, reason) do
    :erlang.error(reason)
  end

  defp with_conn({:ok, pid}, fun) do
    fun.(pid)
  after
    :kpro.close_connection(pid)
  end

  defp with_conn({:error, reason}, _run) do
    {:error, reason}
  end

  defp with_conn(endpoints, conn_cfg, fun) when is_list(conn_cfg) do
    with_conn(endpoints, Map.new(conn_cfg), fun)
  end

  defp with_conn(endpoints, conn_cfg, fun) do
    :kpro_brokers.with_connection(endpoints, conn_cfg, fun)
  end

  defp parse(:produce, _vsn, msg) do
    :kpro.find(:base_offset, get_partition_rsp(msg))
  end

  defp parse(:fetch, _vsn, msg) do
    parse_fetch_rsp(msg)
  end

  defp parse(:list_offsets, _, msg) do
    case get_partition_rsp(msg) do
      %{offsets: []} = m ->
        Map.put(m, :offset, -1)

      %{offsets: [offset]} = m ->
        Map.put(m, :offset, offset)

      %{offset: _} = m ->
        m
    end
  end

  defp parse(:metadata, _, msg) do
    :ok = throw_error_code(:kpro.find(:topics, msg))
    msg
  end

  defp parse(:find_coordinator, _, msg) do
    :ok = throw_error_code([msg])
    msg
  end

  defp parse(:join_group, _, msg) do
    :ok = throw_error_code([msg])
    msg
  end

  defp parse(:heartbeat, _, msg) do
    :ok = throw_error_code([msg])
    msg
  end

  defp parse(:leave_group, _, msg) do
    :ok = throw_error_code([msg])
  end

  defp parse(:sync_group, _, msg) do
    :ok = throw_error_code([msg])
    msg
  end

  defp parse(:describe_groups, _, msg) do
    groups = :kpro.find(:groups, msg)
    :ok = throw_error_code(groups)
    groups
  end

  defp parse(:list_groups, _, msg) do
    :ok = throw_error_code([msg])
    :kpro.find(:groups, msg)
  end

  defp parse(:create_topics, _, msg) do
    :ok = throw_error_code(:kpro.find(:topics, msg))
  end

  defp parse(:delete_topics, _, msg) do
    :ok = throw_error_code(:kpro.find(:responses, msg))
  end

  defp parse(:init_producer_id, _, msg) do
    :ok = throw_error_code([msg])
    msg
  end

  defp parse(:create_partitions, _, msg) do
    :ok = throw_error_code(:kpro.find(:topic_errors, msg))
  end

  defp parse(:end_txn, _, msg) do
    :ok = throw_error_code([msg])
  end

  defp parse(:describe_acls, _, msg) do
    :ok = throw_error_code([msg])
    msg
  end

  defp parse(:create_acls, _, msg) do
    :ok =
      throw_error_code(
        :kpro.find(
          :creation_responses,
          msg
        )
      )
  end

  defp parse(_api, _vsn, msg) do
    msg
  end

  #
  # This function takes a list of kpro structs,
  # return ok if all structs have 'no_error' as error code.
  # Otherwise throw an exception with the first error.
  #
  defp throw_error_code([]), do: :ok

  defp throw_error_code([struct | structs]) do
    ec = :kpro.find(:error_code, struct)

    case is_error(ec) do
      true ->
        err = :kpro.find(:error_message, struct, ec)
        :erlang.throw(err)

      false ->
        throw_error_code(structs)
    end
  end

  defp make_msg_input(key, {ts, value}) when is_integer(ts) do
    %{ts: ts, key: key, value: value}
  end

  defp make_msg_input(key, m) when is_map(m) do
    ensure_ts(ensure_key(m, key))
  end

  defp make_msg_input(key, value) do
    ensure_ts(%{key: key, value: value})
  end

  defp ensure_key(%{key: _} = m, _) do
    m
  end

  defp ensure_key(m, key) do
    Map.put(m, :key, key)
  end

  defp ensure_ts(%{ts: _} = m) do
    m
  end

  defp ensure_ts(m) do
    Map.put(m, :ts, :kpro_lib.now_ts())
  end

  defp unify_batch(batch_input) do
    f = fn m, acc ->
      [unify_msg(m) | acc]
    end

    :lists.reverse(foldl_batch(f, [], batch_input))
  end

  defp bin(:undefined) do
    <<>>
  end

  defp bin(x) do
    :erlang.iolist_to_binary(x)
  end

  defp unify_msg({k, v}) do
    %{ts: :kpro_lib.now_ts(), key: bin(k), value: bin(v)}
  end

  defp unify_msg({t, k, v}) do
    %{ts: t, key: bin(k), value: bin(v)}
  end

  defp unify_msg(m) when is_map(m) do
    Map.merge(m, %{
      key: bin(Map.get(m, :key, <<>>)),
      value: bin(Map.get(m, :value, <<>>)),
      headers:
        :lists.map(
          fn {k, v} ->
            {bin(k), bin(v)}
          end,
          Map.get(m, :headers, [])
        )
    })
  end

  defp nested({_k, [msg | _] = nested}) when is_tuple(msg) do
    nested
  end

  defp nested({_t, _k, [msg | _] = nested})
       when is_tuple(msg) do
    nested
  end

  defp nested(_Msg) do
    false
  end

  defp foldl_batch(_Fun, acc, []) do
    acc
  end

  defp foldl_batch(fun, acc, [msg | rest]) do
    new_acc =
      case nested(msg) do
        false ->
          fun.(msg, acc)

        nested ->
          foldl_batch(fun, acc, nested)
      end

    foldl_batch(fun, new_acc, rest)
  end

  defp is_batch([m | _]) when is_map(m) do
    true
  end

  defp is_batch([t | _]) when is_tuple(t) do
    true
  end

  defp is_batch(_) do
    false
  end

  defp nolink(c) when is_list(c), do: [{:nolink, true} | c]
  defp nolink(c) when is_map(c), do: %{nolink: true}
end
