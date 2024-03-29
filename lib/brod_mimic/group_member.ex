defmodule BrodMimic.GroupMember do
  @moduledoc """
  Implement `BrodMimic.GroupMember` behaviour callbacks to allow a process to act as
  a group member without having to deal with Kafka group protocol details. A
  typical workflow:

  1. Spawn a group coordinator by calling
     `BrodMimic.GroupCoordinator.start_link/6`
  2. Subscribe to partitions received in the assignments
     from `assignments_received/4` callback.
  3. Receive messages from the assigned partitions (delivered by the partition
     workers (the pollers) implemented in `BrodMimic.Consumer`).
  4. Unsubscribe from all previously subscribed partitions when
     `assignments_revoked/1` is called.

  For group members that commit offsets to Kafka, do:

  1. Call `BrodMimic.GroupCoordinator.ack/5` to acknowledge successful
     consumption of the messages. Group coordinator will commit the acknowledged
     offsets at configured interval.
  2. Call `BrodMimic.GroupCoordinator.commit_offsets/2` to force an immediate
     offset commit if necessary.

  For group members that manage offsets locally, do:

  1. Implement the `get_committed_offsets/2` callback. This callback is
     evaluated every time when new assignments are received.
  """
  use BrodMimic.Macros

  alias BrodMimic.Brod

  @type group_member() :: Brod.group_member()
  @type group_member_id() :: Brod.group_member_id()
  @type group_generation_id() :: Brod.group_generation_id()
  @type received_assignments() :: Brod.received_assignments()
  @type partition_assignment() :: Brod.partition_assignment()
  @type topic_partition() :: Brod.topic_partition()
  @type assigned_partitions() :: [{group_member_id(), [partition_assignment()]}]

  @doc """
  Call the callback module to initialize assignments.

  NOTE: This function is called only when `:offset_commit_policy` is
  `:consumer_managed` in group config. See
  `BrodMimic.GroupCoordinator.start_link/6`. for more group config details.

  NOTE: The committed offsets should be the offsets for successfully processed
  (acknowledged) messages, not the begin-offset to start fetching from.
  """
  @callback get_committed_offsets(pid(), [{topic(), partition()}]) :: {:ok, [{topic_partition(), offset()}]}

  @doc """
  Called when the member is elected as the consumer group leader.

  The first element in the group member list is ensured to be the leader.

  NOTE: this function is called only when ':partition_assignment_strategy` is
  ':callback_implemented` in group config.

  See `BrodMimic.GroupCoordinator.start_link/6`. for more group config details.
  """
  @callback assign_partitions(pid(), [group_member()], [topic_partition()]) :: assigned_partitions()

  @doc """
  Called when assignments are received from group leader.

  The member process should now call `BrodMimic.Brod.subscribe/5` to start receiving message from Kafka.
  """
  @callback assignments_received(pid(), group_member_id(), group_generation_id(), received_assignments()) :: :ok

  @doc """
  Called before group re-balancing, the member should call
  `BrodMimic.Brod.unsubscribe/3` to unsubscribe from all currently subscribed partitions.
  """
  @callback assignments_revoked(pid()) :: :ok

  @doc """
  Called when making join request. This metadata is to let group leader know
  more details about the member. e.g. its location and or capacity etc.
  so that leader can make smarter decisions when assigning partitions to it.
  """
  @callback user_data(pid()) :: binary()

  @optional_callbacks [assign_partitions: 3, user_data: 1]
end
