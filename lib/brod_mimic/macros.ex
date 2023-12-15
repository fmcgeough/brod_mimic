defmodule BrodMimic.Macros do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      alias BrodMimic.Brod

      defp offset_earliest, do: :earliest
      defp offset_latest, do: :latest
      defp unknown_topic_cache_expire_seconds, do: 120
      defp topic_metadata_key(topic), do: {:topic_metadata, topic}
      defp is_error(ec), do: ec != :no_error

      defp escalate_ec(ec) do
        if is_error(ec) do
          throw(ec)
        end
      end

      defp kv(key, value), do: {key, value}
      defp tkv(ts, key, value), do: {ts, key, value}
      defp brod_fold_ret(acc, next_offset, reason), do: {acc, next_offset, reason}
      defp brod_default_timeout, do: :timer.seconds(5)
      defp consumer_key(topic, partition), do: {:consumer, topic, partition}
      defp producer_key(topic, partition), do: {:producer, topic, partition}
      # defp producer(pid), do: [{:producer, _, _}, pid]
      defp producer(topic, partition, pid), do: {producer_key(topic, partition), pid}
      # defp consumer(pid), do: [{:consumer, _, _}, pid]
      defp consumer(topic, partition, pid), do: {consumer_key(topic, partition), pid}
      defp incomplete_batch(expected_size), do: {:incomplete_batch, expected_size}
    end
  end
end
