# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
from collections import OrderedDict
from decimal import Decimal

from process.ptg_parts import provider_cache, row_helpers


def test_source_witness_row_helpers_cover_date_and_normalization_edges(monkeypatch):
    assert row_helpers._coerce_date(datetime.date(2026, 7, 21)) == datetime.date(2026, 7, 21)
    assert row_helpers._coerce_date("  ") is None
    assert row_helpers._coerce_date("2026-07-21 trailing") == datetime.date(2026, 7, 21)
    assert row_helpers._coerce_date("2026-99-99") is None
    assert row_helpers._coerce_date("July 21, 2026") == datetime.date(2026, 7, 21)
    monkeypatch.setattr(row_helpers, "parse_date", lambda _text: datetime.date(2026, 7, 22))
    assert row_helpers._coerce_date("fallback") == datetime.date(2026, 7, 22)
    assert row_helpers._normalized_modifier_list(["a,b", None, " b "]) == ["A", "B"]
    assert row_helpers._normalize_code_component(None) is None


def test_source_witness_provider_cache_covers_persistent_edges(tmp_path, monkeypatch):
    assert provider_cache._normalize_provider_ref("²") == "²"
    assert provider_cache._json_default(datetime.date(2026, 7, 21)) == "2026-07-21"
    assert provider_cache._json_default(Decimal("2.00")) == "2"
    assert provider_cache._json_default(Decimal("2.50")) == "2.5"
    assert provider_cache._json_default(object()).startswith("<object object")

    monkeypatch.setenv(provider_cache.PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV, "1")
    cache = provider_cache.PTG2ProviderReferenceCache(
        tmp_path / "provider.sqlite",
        initial={
            "1": [{"__hash__": 1, "date": datetime.date(2026, 7, 21)}],
            "2": [{"value": Decimal("2.5")}],
        },
    )
    assert cache.get(None) == []
    assert cache.get("missing") == []
    assert cache.get("2") == [{"value": Decimal("2.5")}]
    assert cache.get("1")[0]["__hash__"] == 1
    cache.put(None, [{"__hash__": 3}])
    cache.put("3", [])
    cache.memory_limit = 0
    cache._remember("ignored", [{"__hash__": 4}])
    cache.commit()
    cache.close()


def test_source_witness_provider_cache_covers_in_memory_edges():
    memory = provider_cache.PTG2InMemoryProviderReferenceCache(
        {"1": [{"__hash__": 1}], "2": [{"value": 2}]}
    )
    memory.put(None, [{"__hash__": 3}])
    memory.put("3", [])
    assert memory.get(None) == []
    assert memory.get("missing") == []
    assert memory.get("1") == [{"__hash__": 1}]
    assert memory.stats()["provider_cache_memory_hits"] == 1
    assert memory.commit() is None
    assert memory.close() is None

    groups_by_ref = {"7": [{"__hash__": 7}]}
    assert provider_cache._provider_cache_get(groups_by_ref, "7") == groups_by_ref["7"]
    provider_cache._provider_cache_put(groups_by_ref, 8, [{"__hash__": 8}])
    assert provider_cache._provider_combo_cache_key([2, "1", None, 2]) == ("1", "2")
    assert provider_cache._provider_cache_hashes(memory) == {1}
    assert provider_cache._provider_cache_hashes(groups_by_ref) == {7, 8}


def test_source_witness_provider_combo_cache_covers_limit_and_lookup_edges():
    combo = OrderedDict()
    combo_stats_by_name = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
    }
    assert provider_cache._provider_combo_cache_get(combo, ("1",), combo_stats_by_name) is None
    provider_cache._provider_combo_cache_put(
        combo, ("1",), {"__hash__": 1}, combo_stats_by_name, limit=0
    )
    provider_cache._provider_combo_cache_put(
        combo, (), {"__hash__": 1}, combo_stats_by_name, limit=1
    )
    provider_cache._provider_combo_cache_put(
        combo, ("1",), {"__hash__": 1}, combo_stats_by_name, limit=1
    )
    assert provider_cache._provider_combo_cache_get(combo, ("1",), combo_stats_by_name) == {
        "__hash__": 1
    }
    provider_cache._provider_combo_cache_put(
        combo, ("2",), {"__hash__": 2}, combo_stats_by_name, limit=1
    )
    assert list(combo) == [("2",)]
