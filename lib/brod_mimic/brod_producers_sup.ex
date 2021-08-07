defmodule BrodMimic.BrodProducersSup do
  @behaviour :supervisor3

  @topics_sup __MODULE__
  @partitions_sup BrodMimic.BrodProducersSup2
  # By default, restart partition producer worker process after a 5-seconds delay
  @default_producer_restart_delay 5
  # Minimum delay seconds to work with supervisor3
  @min_supervisor3_delay_secs 1

  alias BrodMimic.Client

  @doc """
  Start a root producers supervisor

   For more details: @see brod_producer:start_link/4
  """
  @spec start_link() :: {:ok, pid()}
  def start_link do
    :supervisor3.start_link(__MODULE__, @topics_sup)
  end

  @impl true
  def init(@topics_sup) do
    {:ok, {{:one_for_one, 0, 1}, []}}
  end

  @impl true
  def init({@partitions_sup, client_pid, topic, config}) do
    post_init({@partitions_sup, client_pid, topic, config})
  end

  @impl true
  def post_init({@partitions_sup, client_pid, topic, config}) do
    case Client.get_partitions_count(client_pid, topic) do
      {:ok, partitions_cnt} ->
        # In brod code this has the following odd looking syntax
        #
        # Children = [ producer_spec(ClientPid, Topic, Partition, Config)
        #   || Partition <- lists:seq(0, PartitionsCnt - 1) ],
        #
        children =
          Enum.map(0..(partitions_cnt - 1), fn partition ->
            producer_spec(client_pid, topic, partition, config)
          end)

        ## Producer may crash in case of exception in case of network failure,
        ## or error code received in produce response (e.g. leader transition)
        ## In any case, restart right away will erry likely fail again.
        ## Hence set MaxR=0 here to cool-down for a configurable N-seconds
        ## before supervisor tries to restart it.
        {:ok, {{:one_for_one, 0, 1}, children}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def producer_spec(client_pid, topic, partition, config0) do
    {config, delay_secs} =
      take_delay_secs(config0, :partition_restart_delay_seconds, @default_producer_restart_delay)

    args = [client_pid, topic, partition, config]

    {_id = partition, _start = {:brod_producer, :start_link, args},
     _restart = {:permanent, delay_secs}, _shutdown = 5000, _type = :worker,
     _module = [:brod_producer]}
  end

  #### Internal Functions =======================================================

  @spec take_delay_secs(BrodMimic.Brod.producer_config(), atom(), integer()) ::
          {BrodMimic.Brod.producer_config(), integer()}
  def take_delay_secs(config, name, default_value) do
    secs =
      case :proplists.get_value(name, config) do
        n when is_integer(n) and n >= @min_supervisor3_delay_secs ->
          n

        _ ->
          default_value
      end

    {:proplists.delete(name, config), secs}
  end
end
