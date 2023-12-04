defmodule BrodMimic.GroupMember do
  @optional_callbacks [assign_partitions: 3, user_data: 1]

  @doc """
  Call the callback module to initialize assignments.

  NOTE: This function is called only when `offset_commit_policy' is
  `consumer_managed' in group config.

  see brod_group_coordinator:start_link/6. for more group config details

  NOTE: The committed offsets should be the offsets for successfully processed
  (acknowledged) messages, not the begin-offset to start fetching from.
  """
  @callback get_committed_offsets(pid(), [{Brod.topic(), Brod.partition()}]) ::
              {:ok, [{{Brod.topic(), Brod.partition()}, Brod.offset()}]}

  @doc """
  Called when the member is elected as the consumer group leader.

  The first element in the group member list is ensured to be the leader.

  NOTE: this function is called only when 'partition_assignment_strategy' is
  'callback_implemented' in group config.

  see brod_group_coordinator:start_link/6. for more group config details.
  """
  @callback assign_partitions(pid(), [Brod.group_member()], [{Brod.topic(), Brod.partition()}]) ::
              [{Brod.group_member_id(), [Brod.partition_assignment()]}]

  @doc """
  Called when assignments are received from group leader.

  the member process should now call Brod.subscribe/5
  to start receiving message from kafka.
  """
  @callback assignments_received(
              pid(),
              Brod.group_member_id(),
              Brod.group_generation_id(),
              Brod.received_assignments()
            ) :: :ok

  @doc """
  Called before group re-balancing, the member should call
  Brod.unsubscribe/3 to unsubscribe from all currently subscribed partitions.
  """
  @callback assignments_revoked(pid()) :: :ok

  @doc """
  Called when making join request. This metadata is to let group leader know
  more details about the member. e.g. its location and or capacity etc.
  so that leader can make smarter decisions when assigning partitions to it.
  """
  @callback user_data(pid()) :: binary()
end
