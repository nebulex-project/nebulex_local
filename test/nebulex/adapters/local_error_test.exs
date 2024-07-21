defmodule Nebulex.Adapters.LocalErrorTest do
  use ExUnit.Case, async: true
  use Mimic

  # Inherit error tests
  use Nebulex.Cache.KVErrorTest
  use Nebulex.Cache.KVExpirationErrorTest

  setup do
    Nebulex.Cache.Registry
    |> expect(:lookup, fn _ ->
      %{adapter: Nebulex.FakeAdapter, telemetry: true, telemetry_prefix: [:nebulex, :test]}
    end)

    {:ok, cache: Nebulex.Adapters.Local.TestCache, name: :local_error_cache}
  end

  describe "put!/3" do
    test "raises an error", %{cache: cache} do
      assert_raise Nebulex.Error, ~r"runtime error", fn ->
        cache.put!(:error, %RuntimeError{})
      end
    end
  end
end
