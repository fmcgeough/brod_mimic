defmodule BrodMimic.ProducersSup do
  @moduledoc """
  Root producers supervisor
  """

  @behaviour BrodMimic.Supervisor3

  alias BrodMimic.Brod
  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.Supervisor3, as: BrodSupervisor3

  @type find_producer_error() ::
          {:producer_not_found, Brod.topic()}
          | {:producer_not_found, Brod.topic(), Brod.partition()}
          | {:producer_down, any()}

  @doc """
  Start a root producers supervisor

  For more details: `BrodMimic.Producer.start_link/4`
  """
  @spec start_link() :: {:ok, pid()}
  def start_link do
    BrodSupervisor3.start_link(__MODULE__, __MODULE__)
  end

  @doc """
  Dynamically start a per-topic supervisor
  """
  @spec start_producer(pid(), pid(), Brod.topic(), Brod.producer_config()) ::
          {:ok, pid()} | {:error, any()}
  def start_producer(sup_pid, client_pid, topic_name, config) do
    spec = producers_sup_spec(client_pid, topic_name, config)
    BrodSupervisor3.start_child(sup_pid, spec)
  end

  @doc """
  Dynamically stop a per-topic supervisor
  """
  @spec stop_producer(pid(), Brod.topic()) :: :ok
  def stop_producer(sup_pid, topic_name) do
    BrodSupervisor3.terminate_child(sup_pid, topic_name)
    BrodSupervisor3.delete_child(sup_pid, topic_name)
  end

  @doc """
  Find a brod_producer process pid running under the partitions supervisor
  """
  @spec find_producer(pid(), Brod.topic(), Brod.partition()) ::
          {:ok, pid()} | {:error, find_producer_error()}
  def find_producer(sup_pid, topic, partition) do
    case BrodSupervisor3.find_child(sup_pid, topic) do
      [] ->
        {:error, {:producer_not_found, topic}}

      [partitions_sup_pid] ->
        try do
          case BrodSupervisor3.find_child(
                 partitions_sup_pid,
                 partition
               ) do
            [] ->
              {:error, {:producer_not_found, topic, partition}}

            [pid] ->
              {:ok, pid}
          end
        catch
          :exit, {reason, _} ->
            {:error, {:producer_down, reason}}
        end
    end
  end

  def init(__MODULE__) do
    {:ok, {{:one_for_one, 0, 1}, []}}
  end

  def init({:brod_producers_sup2, _client_pid, _topic, _config}) do
    :post_init
  end

  def post_init({:brod_producers_sup2, client_pid, topic, config}) do
    case BrodClient.get_partitions_count(client_pid, topic) do
      {:ok, partitions_cnt} ->
        children =
          Enum.map(0..(partitions_cnt - 1), &producer_spec(client_pid, topic, &1, config))

        # Producer may crash in case of exception in case of network failure,
        # or error code received in produce response (e.g. leader transition)
        # In any case, restart right away will very likely fail again.
        # Hence set MaxR=0 here to cool-down for a configurable N-seconds
        # before supervisor tries to restart it.
        {:ok, {{:one_for_one, 0, 1}, children}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp producers_sup_spec(client_pid, topic_name, config0) do
    {config, delay_secs} = take_delay_secs(config0, :topic_restart_delay_seconds, 10)
    args = [BrodMimic.ProducersSup, {:brod_producers_sup2, client_pid, topic_name, config}]

    {
      _id = topic_name,
      _start = {BrodSupervisor3, :start_link, args},
      _restart = {:permanent, delay_secs},
      _shutdown = :infinity,
      _type = :supervisor,
      _module = [__MODULE__]
    }
  end

  defp producer_spec(client_pid, topic, partition, config0) do
    {config, delay_secs} = take_delay_secs(config0, :partition_restart_delay_seconds, 5)
    args = [client_pid, topic, partition, config]

    {
      _id = partition,
      _start = {:brod_producer, :start_link, args},
      _restart = {:permanent, delay_secs},
      _shutdown = 5000,
      _type = :worker,
      _module = [BrodMimic.Producer]
    }
  end

  defp take_delay_secs(config, name, default_value) do
    secs =
      case :proplists.get_value(name, config) do
        n when is_integer(n) and n >= 1 ->
          n

        _ ->
          default_value
      end

    {:proplists.delete(name, config), secs}
  end
end
