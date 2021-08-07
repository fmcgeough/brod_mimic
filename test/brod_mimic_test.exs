defmodule BrodMimicTest do
  use ExUnit.Case
  doctest BrodMimic

  test "greets the world" do
    assert BrodMimic.hello() == :world
  end
end
