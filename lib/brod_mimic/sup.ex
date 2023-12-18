defmodule BrodMimic.Sup do
  @moduledoc """
  BrodMimic Supervisor

  ```
   Hierarchy:
     BrodMimic.Sup (one_for_one)
       |
       +--BrodMimic.Client client_1
       |    |
       |    +-- BrodMimic.ProducersSup level 1
       |    |     |
       |    |     +-- BrodMimic.ProducersSup level 2 for topic 1
       |    |     |     |
       |    |     |     +-- partition_0_worker
       |    |     |     |
       |    |     |     +-- partition_1_worker
       |    |     |     |...
       |    |     |
       |    |     +-- BrodMimic.ProducersSup level 2 for topic 2
       |    |     |     |...
       |    |     |...
       |    |
       |    +-- BrodMimic.ConsumersSup level 1
       |          |
       |          +-- BrodMimic.ConsumersSup level 2 for topic 1
       |          |     |
       |          |     +-- partition_0_worker
       |          |     |
       |          |     +-- partition_1_worker
       |          |     |...
       |          |
       |          +-- BrodMimic.ConsumersSup level 2 for topic 2
       |          |     |...
       |          |...
       |
       +-- BrodMimic.Client client_2
       |     |...
       |...
  ```
  """

  @behaviour BrodMimic.Supervisor3

  alias BrodMimic.Brod
  alias BrodMimic.KafkaApis, as: BrodKafkaApis
  alias BrodMimic.Supervisor3
  alias BrodMimic.Utils, as: BrodUtils

  @doc """
  Start root supervisor
  """
  @spec start_link() :: {:ok, pid()}
  def start_link do
    Supervisor3.start_link({:local, :brod_sup}, BrodMimic.Sup, :clients_sup)
  end

  @spec start_client([Brod.endpoint()], Brod.client_id(), Brod.client_config()) ::
          :ok | {:error, any()}
  def start_client(endpoints, client_id, config) do
    client_spec = client_spec(endpoints, client_id, config)

    case Supervisor3.start_child(:brod_sup, client_spec) do
      {:ok, _pid} ->
        :ok

      error ->
        error
    end
  end

  @spec stop_client(Brod.client_id()) :: :ok | {:error, any()}
  def stop_client(client_id) do
    _ = Supervisor3.terminate_child(:brod_sup, client_id)
    Supervisor3.delete_child(:brod_sup, client_id)
  end

  def find_client(client) do
    Supervisor3.find_child(:brod_sup, client)
  end

  @impl Supervisor3
  def init(:clients_sup) do
    {:ok, _} = BrodKafkaApis.start_link()
    clients = Application.get_env(:brod_mimic, :clients, [])

    client_specs =
      :lists.map(
        fn {client_id, args} ->
          is_atom(client_id) or exit({:bad_client_id, client_id})
          client_spec(client_id, args)
        end,
        clients
      )

    {:ok, {{:one_for_one, 0, 1}, client_specs}}
  end

  @impl Supervisor3
  def post_init(_) do
    :ignore
  end

  defp client_spec(client_id, config) do
    endpoints = :proplists.get_value(:endpoints, config, [])
    client_spec(endpoints, client_id, config)
  end

  defp client_spec([], client_id, _Config) do
    error =
      :lists.flatten(
        :io_lib.format('No endpoints found in brod client \'~p\' config', [client_id])
      )

    exit(error)
  end

  defp client_spec(endpoints, client_id, config0) do
    delay_secs = :proplists.get_value(:restart_delay_seconds, config0, 10)

    config1 =
      :proplists.delete(
        :restart_delay_seconds,
        config0
      )

    config = BrodUtils.init_sasl_opt(config1)
    start_args = [endpoints, client_id, config]

    {_id = client_id, _start = {BrodMimic.Client, :start_link, start_args},
     _restart = {:permanent, delay_secs}, _shutdown = 5000, _type = :worker,
     _module = [BrodMimic.Client]}
  end
end
