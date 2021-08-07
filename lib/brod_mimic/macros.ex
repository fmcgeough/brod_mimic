defmodule BrodMimic.Macros do
  @moduledoc false

  defmacro offset_earliest do
    quote do
      :earliest
    end
  end

  defmacro unknown_topic_cache_expire_seconds do
    120
  end

  defmacro topic_metadata_key(topic) do
    quote do
      {:topic_metadata, unquote(topic)}
    end
  end

  defmacro is_error(ec) do
    quote do
      unquote(ec) != :no_error
    end
  end

  defmacro offset_latest do
    quote do
      :latest
    end
  end

  defmacro kv(key, value) do
    quote do
      {unquote(key), unquote(value)}
    end
  end

  defmacro tkv(ts, key, value) do
    quote do
      {unquote(ts), unquote(key), unquote(value)}
    end
  end

  defmacro brod_fold_ret(acc, next_offset, reason) do
    quote do
      {unquote(acc), unquote(next_offset), unquote(reason)}
    end
  end

  defmacro brod_default_timeout do
    :timer.seconds(5)
  end

  defmacro consumer_key(topic, partition) do
    quote do
      {:consumer, unquote(topic), unquote(partition)}
    end
  end

  defmacro producer_key(topic, partition) do
    quote do
      {:producer, unquote(topic), unquote(partition)}
    end
  end

  defmacro producer(pid) do
    quote do
      [{:producer, _, _}, unquote(pid)]
    end
  end

  defmacro producer(topic, partition, pid) do
    quote do
      {unquote(producer_key(topic, partition)), unquote(pid)}
    end
  end

  defmacro consumer(pid) do
    quote do
      [{:consumer, _, _}, unquote(pid)]
    end
  end

  defmacro consumer(topic, partition, pid) do
    quote do
      {unquote(consumer_key(topic, partition)), unquote(pid)}
    end
  end
end
