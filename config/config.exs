import Config

config :logger, :console, level: :debug, metadata: [:module, :function, :line]
