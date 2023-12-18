defmodule BrodMimic.ConsumersSup do
  @moduledoc """
  Consumers supervisor
  """
  @behaviour BrodMimic.Supervisor3

  use BrodMimic.Macros

  alias BrodMimic.Brod
  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.Supervisor3, as: BrodSupervisor3

  @topics_sup :brod_consumers_sup
  @partitions_sup :brod_consumers_sup2

  # By default, restart ?PARTITIONS_SUP after a 10-seconds delay
  @default_partitions_sup_restart_delay 10

  # By default, restart partition consumer worker process after a 2-seconds delay
  @default_consumer_restart_delay 2

  # APIs =====================================================================

  @doc """
  Start a root consumers supervisor.
  """
  @spec start_link() :: {:ok, pid()}
  def start_link do
    BrodSupervisor3.start_link(__MODULE__, @topics_sup)
  end

  @doc """
  Dynamically start a per-topic supervisor.
  """
  @spec start_consumer(pid(), pid(), topic(), Brod.consumer_config()) ::
          {:ok, pid()} | {:error, any()}
  def start_consumer(sup_pid, client_pid, topic_name, config) do
    spec = consumers_sup_spec(client_pid, topic_name, config)
    BrodSupervisor3.start_child(sup_pid, spec)
  end

  @doc """
  Dynamically stop a per-topic supervisor.
  """
  @spec stop_consumer(pid(), topic()) :: :ok | {:error, any()}
  def stop_consumer(sup_pid, topic_name) do
    BrodSupervisor3.terminate_child(sup_pid, topic_name)
    BrodSupervisor3.delete_child(sup_pid, topic_name)
  end

  @doc """
  Find a brod_consumer process pid running under the partition's supervisor
  """
  @spec find_consumer(pid(), topic(), Brod.partition()) :: {:ok, pid()} | {:error, any()}
  def find_consumer(sup_pid, topic, partition) do
    case BrodSupervisor3.find_child(sup_pid, topic) do
      [] ->
        # no such topic worker started,
        # check sys.config or brod:start_link_client args
        {:error, {:consumer_not_found, topic}}

      [partitions_sup_pid] ->
        try do
          case BrodSupervisor3.find_child(partitions_sup_pid, partition) do
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
  supervisor3 callback
  """
  @impl true
  def init(@topics_sup) do
    {:ok, {{:one_for_one, 0, 1}, []}}
  end

  def init({@partitions_sup, _client_pid, _topic, _config} = args) do
    post_init(args)
  end

  @impl true
  def post_init({@partitions_sup, client_pid, topic, config}) do
    # spawn consumer process for every partition
    # in a topic if partitions are not set explicitly
    # in the config
    # MODIFY: make it dynamic when consumer groups API is ready
    case get_partitions(client_pid, topic, config) do
      {:ok, partitions} ->
        children =
          for partition <- partitions, do: consumer_spec(client_pid, topic, partition, config)

        {:ok, {{:one_for_one, 0, 1}, children}}

      error ->
        error
    end
  end

  @impl true
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
    case BrodClient.get_partitions_count(
           client_pid,
           topic
         ) do
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

    args = [__MODULE__, {:brod_consumers_sup2, client_pid, topic_name, config}]

    {
      _id = topic_name,
      _start = {BrodSupervisor3, :start_link, args},
      _restart = {:permanent, delay_secs},
      _shut_down = :infinity,
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
      _start = {BrodMimic.Consumer, :start_link, args},
      _restart = {:transient, delay_secs},
      _shut_down = 5000,
      _type = :worker,
      _module = [BrodMimic.Consumer]
    }
  end
end
