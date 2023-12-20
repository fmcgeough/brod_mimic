defmodule BrodMimic.CgCommits do
  @moduledoc """
  This is a utility module to help force commit offsets to Kafka
  """
  @behaviour BrodMimic.GroupMember

  use BrodMimic.Macros
  use GenServer

  import Record, only: [defrecordp: 2]

  alias BrodMimic.Brod
  alias BrodMimic.GroupCoordinator, as: BrodGroupCoordinator
  alias BrodMimic.GroupMember, as: BrodGroupMember
  alias BrodMimic.Utils, as: BrodUtils

  require Logger
  require Record

  defrecordp(:state,
    client: :undefined,
    group_id: :undefined,
    member_id: :undefined,
    generation_id: :undefined,
    coordinator: :undefined,
    topic: :undefined,
    offsets: :undefined,
    is_elected: false,
    pending_sync: :undefined,
    is_done: false
  )

  @partitions_not_received "Partitions ~p are not received in assignment, There is probably another active group member subscribing to topic ~s, stop it and retry"
  @nonexistent_partitions "Nonexisting partitions in input: ~p"
  @topic_not_in_assignment "Topic ~s is not received in assignment"

  @type group_id() :: Brod.group_id()
  @type member_id() :: Brod.group_member_id()
  #  -1 to use whatever configured in kafka
  @type retention() :: integer()
  @type offsets() :: :latest | :earliest | [{partition(), offset()}]
  @type prop_key() :: :id | :topic | :retention | :protocol | :offsets
  @type prop_val() ::
          group_id() | topic() | retention() | offsets() | BrodGroupCoordinator.protocol_name()
  @type group_input() :: [{prop_key(), prop_val()}]
  @type pending_sync() :: :undefined | GenServer.from()
  @type state() ::
          record(:state,
            client: Brod.client(),
            group_id: Brod.group_id(),
            member_id: :undefined | member_id(),
            generation_id: :undefined | Brod.group_generation_id(),
            coordinator: pid(),
            topic: :undefined | topic(),
            offsets: :undefined | offsets(),
            is_elected: boolean(),
            pending_sync: pending_sync(),
            is_done: boolean()
          )

  @doc """
  Force commit offsets
  """
  def run(client_id, group_input) do
    {:ok, pid} = start_link(client_id, group_input)
    :ok = sync(pid)
    :ok = stop(pid)
  end

  @doc """
  Start (link) a group member.

  The member will try to join the consumer group and get assignments for the
  given topic-partitions, then commit given offsets to Kafka. In case not all
  given partitions are assigned to it, it will terminate with an exit exception
  """
  @spec start_link(Brod.client(), group_input()) :: {:ok, pid()} | {:error, any()}
  def start_link(client, group_input) do
    GenServer.start_link(__MODULE__, {client, group_input}, [])
  end

  @doc """
  Stop the process.
  """
  @spec stop(pid()) :: :ok
  def stop(pid) do
    mref = Process.monitor(pid)
    :ok = GenServer.cast(pid, :stop)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  @doc """
  Make a call to the resetter process, the call will be blocked until offsets
  are committed.
  """
  @spec sync(pid()) :: :ok
  def sync(pid) do
    :ok = GenServer.call(pid, :sync, :infinity)
  end

  @doc """
  Called by group coordinator before re-joinning the consumer group.
  """
  @impl BrodGroupMember
  @spec assignments_received(pid(), member_id(), integer(), Brod.received_assignments()) :: :ok
  def assignments_received(pid, member_id, generation_id, topic_assignments) do
    msg = {:new_assignments, member_id, generation_id, topic_assignments}
    GenServer.cast(pid, msg)
  end

  @doc """
  Called by group coordinator before re-joinning the consumer group.
  """
  @impl BrodGroupMember
  @spec assignments_revoked(pid()) :: :ok
  def assignments_revoked(pid) do
    GenServer.call(pid, :unsubscribe_all_partitions, :infinity)
  end

  @doc """
  This function is called only when `partition_assignment_strategy` is set for
  `callback_implemented` in group config
  """
  @impl BrodGroupMember
  @spec assign_partitions(pid(), [Brod.group_member()], [{topic(), partition()}]) :: [
          {member_id(), [Brod.partition_assignment()]}
        ]
  def assign_partitions(pid, members, topic_partition_list) do
    call = {:assign_partitions, members, topic_partition_list}
    GenServer.call(pid, call, :infinity)
  end

  @doc """
  Called by group coordinator when initializing the assignments for subscriber.

  NOTE: this function is called only when it is DISABLED to commit offsets to
  Kafka. i.e. offset_commit_policy is set to consumer_managed
  """
  @impl BrodGroupMember
  @spec get_committed_offsets(pid(), [{topic(), partition()}]) ::
          {:ok, [{{topic(), partition()}, offset()}]}
  def get_committed_offsets(_pid, _topic_partitions) do
    {:ok, []}
  end

  @impl GenServer
  def init({client, group_input}) do
    :ok = BrodUtils.assert_client(client)
    group_id = :proplists.get_value(:id, group_input)
    :ok = BrodUtils.assert_group_id(group_id)
    topic = :proplists.get_value(:topic, group_input)

    protocol_name = :proplists.get_value(:protocol, group_input)

    retention = :proplists.get_value(:retention, group_input)
    offsets = :proplists.get_value(:offsets, group_input)

    config = [
      {:partition_assignment_strategy, :callback_implemented},
      {:offset_retention_seconds, retention},
      {:protocol_name, protocol_name},
      {:rejoin_delay_seconds, 2}
    ]

    {:ok, pid} =
      BrodGroupCoordinator.start_link(
        client,
        group_id,
        [topic],
        config,
        __MODULE__,
        self()
      )

    state =
      state(
        client: client,
        group_id: group_id,
        coordinator: pid,
        topic: topic,
        offsets: offsets
      )

    {:ok, state}
  end

  @impl GenServer
  def handle_info(info, state) do
    Logger.info(fn -> log_string(state, "Info discarded:~p", [info]) end)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:sync, from, state0) do
    state1 = state(state0, pending_sync: from)
    state = maybe_reply_sync(state1)
    {:noreply, state}
  end

  def handle_call(
        {:assign_partitions, members, topic_partitions},
        _from,
        state(topic: my_topic, offsets: offsets) = state
      ) do
    Logger.info("Assigning all topic partitions to self")

    my_tp = Enum.map(offsets, fn {p, _} -> {my_topic, p} end)

    # Assert that my topic partitions are included in
    # subscriptions collected from ALL members
    case Enum.filter(my_tp, &Enum.member?(topic_partitions, &1)) do
      [] ->
        # all of the give partitions are valid
        :ok

      bad_partitions ->
        partition_numbers = Enum.map(bad_partitions, &elem(&1, 1))

        Logger.error(fn -> log_string(state, @nonexistent_partitions, [partition_numbers]) end)

        Process.exit(self(), {:non_existing_partitions, partition_numbers})
    end

    # To keep it simple, assign all my topic-partitions to self
    # but discard all other topic-partitions.
    # After all, I will leave group as soon as offsets are committed
    result = assign_all_to_self(members, my_tp)
    {:reply, result, state(state, is_elected: true)}
  end

  def handle_call(:unsubscribe_all_partitions, _from, state() = state) do
    # nothing to do, because I do not subscribe to any partition
    {:reply, :ok, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  @impl GenServer
  def handle_cast(
        {:new_assignments, _member_id, generation_id, assignments},
        state(
          is_elected: is_leader,
          offsets: offsets_to_commit,
          coordinator: pid,
          topic: my_topic
        ) = state
      ) do
    # Write a log if I am not a leader,
    # hope the desired partitions are all assigned to me
    if not is_leader do
      Logger.info(fn -> log_string(state, "Not elected", []) end)
    end

    groupped0 =
      BrodUtils.group_per_key(
        fn brod_received_assignment(topic: topic, partition: partition, begin_offset: offset) ->
          {topic, {partition, offset}}
        end,
        assignments
      )

    # Discard other topics if for whatever reason the group leader assigns
    # irrelevant topic-partitions to me
    groupped = Enum.filter(groupped0, fn {topic, _} -> topic === my_topic end)

    Logger.info(fn -> log_string(state, "current offsets:\n~p", [groupped]) end)

    # Assert all desired partitions are in assignment
    case groupped do
      [] ->
        Logger.error(fn -> log_string(state, @topic_not_in_assignment, [my_topic]) end)
        Process.exit(self(), {:bad_topic_assignment, groupped0})

      [{^my_topic, partition_offset_list}] ->
        my_partitions = Enum.map(offsets_to_commit, &elem(&1, 0))
        received_partitions = Enum.map(partition_offset_list, &elem(&1, 0))

        case my_partitions -- received_partitions do
          [] ->
            :ok

          left ->
            @partitions_not_received
            |> :io_lib.format([my_topic, left])
            |> to_string()
            |> Logger.error()

            Process.exit(self(), {:unexpected_assignments, left})
        end
    end

    # Stage all offsets in coordinator process
    Enum.each(offsets_to_commit, fn {partition, offset} ->
      # -1 here, because BrodGroupCoordinator +1 to commit
      offset_to_commit = offset - 1
      BrodGroupCoordinator.ack(pid, generation_id, my_topic, partition, offset_to_commit)
    end)

    # Now force it to commit
    case BrodGroupCoordinator.commit_offsets(pid) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(fn -> log_string(state, "Failed to commit, reason:\n~p", [reason]) end)
        Process.exit(self(), :commit_failed)
    end

    {:noreply, set_done(state)}
  end

  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_cast(_cast, state) do
    {:noreply, state}
  end

  @impl GenServer
  def code_change(_old_vsn, state, _extra) do
    {:ok, state}
  end

  @impl GenServer
  def terminate(_reason, state()) do
    :ok
  end

  defp set_done(state) do
    maybe_reply_sync(state(state, is_done: true))
  end

  defp maybe_reply_sync(state(is_done: false) = state) do
    state
  end

  defp maybe_reply_sync(state(pending_sync: :undefined) = state) do
    state
  end

  defp maybe_reply_sync(state(pending_sync: from) = state) do
    GenServer.reply(from, :ok)
    Logger.info(fn -> log_string(state, "done\n", []) end)
    state(state, pending_sync: :undefined)
  end

  # I am the current leader because I am assigning partitions.
  # My member ID should be positioned at the head of the member list.
  @spec assign_all_to_self([Brod.group_member()], [{topic(), partition()}]) ::
          [{member_id(), [Brod.partition_assignment()]}]
  defp assign_all_to_self([{my_member_id, _} | members], topic_partitions) do
    groupped = BrodUtils.group_per_key(topic_partitions)

    [
      {my_member_id, groupped}
      | for {id, _member_meta} <- members do
          {id, []}
        end
    ]
  end

  defp log_string(state(group_id: group_id), format_string, args) do
    :io_lib.format("Group member (~s,coor=~p):\n" <> format_string, [group_id, self() | args])
  end
end
