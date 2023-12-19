defmodule BrodMimic.MixProject do
  use Mix.Project

  def project do
    [
      app: :brod_mimic,
      version: "0.0.2",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      # Docs
      name: "brod-mimic",
      source_url: "https://github.com/fmcgeough/brod_mimic",
      docs: [
        main: "readme",
        extras: ["README.md"],
        language: "en",
        output: "dist/docs"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {BrodMimic.Brod, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:kafka_protocol, "~> 4.1"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.30.9", only: :dev, runtime: false},
      {:doctor, "~> 0.21.0", only: :dev, runtime: false}
    ]
  end
end
