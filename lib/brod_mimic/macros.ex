defmodule BrodMimic.Macros do
  @moduledoc false

  defmacro __using__(_) do
    quote do
      alias BrodMimic.Brod

      def offset_earliest, do: :earliest
      def offset_latest, do: :latest
      def unknown_topic_cache_expire_seconds, do: 120
      def topic_metadata_key(topic), do: {:topic_metadata, topic}
      def is_error(ec), do: ec != :no_error

      def escalate_ec(ec) do
        if is_error(ec) do
          throw(ec)
        end
      end

      def kv(key, value), do: {key, value}
      def tkv(ts, key, value), do: {ts, key, value}
      def brod_fold_ret(acc, next_offset, reason), do: {acc, next_offset, reason}
      def brod_default_timeout, do: :timer.seconds(5)
      def consumer_key(topic, partition), do: {:consumer, topic, partition}
      def producer_key(topic, partition), do: {:producer, topic, partition}
      # def producer(pid), do: [{:producer, _, _}, pid]
      def producer(topic, partition, pid), do: {producer_key(topic, partition), pid}
      # def consumer(pid), do: [{:consumer, _, _}, pid]
      def consumer(topic, partition, pid), do: {consumer_key(topic, partition), pid}
      def incomplete_batch(expected_size), do: {:incomplete_batch, expected_size}
    end
  end
end
