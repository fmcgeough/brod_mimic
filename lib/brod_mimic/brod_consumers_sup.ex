defmodule BrodMimic.BrodConsumersSup do
  @behaviour :supervisor3

  @topics_sup :brod_consumers_sup
  @partitions_sup :brod_consumers_sup2

  # By default, restart ?PARTITIONS_SUP after a 10-seconds delay
  @default_partitions_sup_restart_delay 10

  # By default, restart partition consumer worker process after a 2-seconds delay
  @default_consumer_restart_delay 2

  alias BrodMimic.Client

  # APIs =====================================================================

  @doc """
  Start a root consumers supervisor.
  """
  @spec start_link() :: {:ok, pid()}
  def start_link do
    :supervisor3.start_link(__MODULE__, @topics_sup)
  end

  @doc """
  Dynamically start a per-topic supervisor.
  """
  @spec start_consumer(pid(), pid(), Brod.topic(), Brod.consumer_config()) ::
          {:ok, pid()} | {:error, any()}
  def start_consumer(sup_pid, client_pid, topic_name, config) do
    spec = consumers_sup_spec(client_pid, topic_name, config)
    :supervisor3.start_child(sup_pid, spec)
  end

  @doc """
  Dynamically stop a per-topic supervisor.
  """
  @spec stop_consumer(pid(), Brod.topic()) :: :ok | {:error, any()}
  def stop_consumer(sup_pid, topic_name) do
    :supervisor3.terminate_child(sup_pid, topic_name)
    :supervisor3.delete_child(sup_pid, topic_name)
  end

  @doc """
  Find a brod_consumer process pid running under @PARTITIONS_SUP
  """
  @spec find_consumer(pid(), Brod.topic(), Brod.partition()) :: {:ok, pid()} | {:error, any()}
  def find_consumer(sup_pid, topic, partition) do
    case :supervisor3.find_child(sup_pid, topic) do
      [] ->
        # no such topic worker started,
        # check sys.config or brod:start_link_client args
        {:error, {:consumer_not_found, topic}}

      [partitions_sup_pid] ->
        try do
          case :supervisor3.find_child(partitions_sup_pid, partition) do
            [] ->
              # no such partition?
              {:error, {:consumer_not_found, topic, partition}}

            [pid] ->
              {:ok, pid}
          end
        catch
          :exit, {:noproc, _} ->
            {:error, {:consumer_down, :noproc}}
        end
    end
  end

  @doc """
  supervisor3 callback.
  """
  def init(@topics_sup) do
    {:ok, {{:one_for_one, 0, 1}, []}}
  end

  def init({@partitions_sup, _client_pid, _topic, _config} = args) do
    post_init(args)
  end

  def post_init({@partitions_sup, client_pid, topic, config}) do
    # spawn consumer process for every partition
    # in a topic if partitions are not set explicitly
    # in the config
    # TODO: make it dynamic when consumer groups API is ready
    case get_partitions(client_pid, topic, config) do
      {:ok, partitions} ->
        children =
          for partition <- partitions, do: consumer_spec(client_pid, topic, partition, config)

        {:ok, {{:one_for_one, 0, 1}, children}}

      error ->
        error
    end
  end

  def post_init(_) do
    :ignore
  end

  def get_partitions(client_pid, topic, config) do
    case :proplists.get_value(:partitions, config, []) do
      [] ->
        get_all_partitions(client_pid, topic)

      [_ | _] = list ->
        {:ok, list}
    end
  end

  def get_all_partitions(client_pid, topic) do
    case Client.get_partitions_count(client_pid, topic) do
      {:ok, partitions_cnt} ->
        {:ok, :lists.seq(0, partitions_cnt - 1)}

      {:error, _} = error ->
        error
    end
  end

  def consumers_sup_spec(client_pid, topic_name, config0) do
    delay_secs =
      :proplists.get_value(
        :topic_restart_delay_seconds,
        config0,
        @default_partitions_sup_restart_delay
      )

    config = :proplists.delete(:topic_restart_delay_seconds, config0)
    args = [__MODULE__, {@partitions_sup, client_pid, topic_name, config}]

    {
      _id = topic_name,
      _start = {:supervisor3, :start_link, args},
      _restart = {:permanent, delay_secs},
      _shutdown = :infinity,
      _type = :supervisor,
      _module = [__MODULE__]
    }
  end

  def consumer_spec(client_pid, topic, partition, config0) do
    delay_secs =
      :proplists.get_value(
        :partition_restart_delay_seconds,
        config0,
        @default_consumer_restart_delay
      )

    config = :proplists.delete(:partition_restart_delay_seconds, config0)
    args = [client_pid, topic, partition, config]

    {
      _id = partition,
      _start = {:brod_consumer, :start_link, args},
      # restart only when not normal exit
      _restart = {:transient, delay_secs},
      _shutdown = 5000,
      _type = :worker,
      _module = [:brod_consumer]
    }
  end
end
