# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import orjson
import pytest

from api import ptg2_price_hydration_cache as price_cache
from api import ptg2_serving


def _serving_tables(**overrides):
    fields_by_name = {
        "arch_version": "postgres_binary_v3",
        "storage": "manifest_snapshot",
        "shared_snapshot_key": 41,
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "atom_key_bits": 24,
        "price_key_block_span": 512,
        "atom_key_block_span": 512,
        "price_atom_constant_values": {"billing_class": "professional"},
    }
    fields_by_name.update(overrides)
    return ptg2_serving.PTG2ServingTables(**fields_by_name)


def _layout(**overrides):
    fields_by_name = {
        "shared_snapshot_key": 41,
        "storage_generation": "shared_blocks_v3",
        "atom_key_bits": 24,
        "price_key_block_span": 512,
        "atom_key_block_span": 512,
        "constant_values_fingerprint": "constants-a",
    }
    fields_by_name.update(overrides)
    return price_cache.PriceHydrationLayout(**fields_by_name)


def _rows(price_key):
    return [
        {
            "negotiated_rate": f"{price_key}.00",
            "service_code": ["11", "22"],
            "billing_code_modifier": ["25"],
            "additional_information": {"kind": "exact"},
        }
    ]


def _hydration(price_keys):
    return ptg2_serving._VersionThreePriceHydration(
        {price_key: (price_key + 100,) for price_key in price_keys},
        {price_key: _rows(price_key) for price_key in price_keys},
    )


@pytest.fixture
def isolated_cache(monkeypatch):
    cache = price_cache.PriceHydrationCache(1024 * 1024)
    monkeypatch.setattr(ptg2_serving, "PRICE_HYDRATION_CACHE", cache)
    return cache


@pytest.mark.asyncio
async def test_price_hydration_cache_batches_only_misses_and_preserves_wire_payload(
    monkeypatch,
    isolated_cache,
):
    hydrate_calls = []

    async def hydrate(_session, _tables, price_keys, **_kwargs):
        normalized_keys = tuple(price_keys)
        hydrate_calls.append(normalized_keys)
        return _hydration(normalized_keys)

    monkeypatch.setattr(ptg2_serving, "_version_three_price_hydration", hydrate)
    tables = _serving_tables()
    miss_rows = await ptg2_serving._version_three_prices_by_key(
        object(), tables, [2, 1, 2]
    )
    partial_rows = await ptg2_serving._version_three_prices_by_key(
        object(), tables, [3, 2]
    )
    hit_rows = await ptg2_serving._version_three_prices_by_key(
        object(), tables, [3, 2, 1]
    )

    assert hydrate_calls == [(1, 2), (3,)]
    assert tuple(miss_rows) == (1, 2)
    assert tuple(partial_rows) == (2, 3)
    assert tuple(hit_rows) == (1, 2, 3)
    assert orjson.dumps(hit_rows, option=orjson.OPT_NON_STR_KEYS) == orjson.dumps(
        {price_key: _rows(price_key) for price_key in (1, 2, 3)},
        option=orjson.OPT_NON_STR_KEYS,
    )
    assert isolated_cache.metrics().entries == 3


@pytest.mark.asyncio
async def test_bounded_price_hydration_rejects_all_cache_overflow_without_decode(
    monkeypatch,
    isolated_cache,
):
    tables = _serving_tables()
    layout = ptg2_serving._version_three_price_cache_layout(tables)
    isolated_cache.admit_many(
        layout,
        {
            1: [
                {"negotiated_rate": str(atom_ordinal)}
                for atom_ordinal in range(257)
            ]
        },
    )
    decode_missing = AsyncMock()
    monkeypatch.setattr(
        ptg2_serving,
        "_bounded_v3_price_hydration",
        decode_missing,
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="atom limit"):
        await ptg2_serving._version_three_bounded_prices_by_key(
            object(),
            tables,
            [1],
            maximum_atom_count=256,
        )

    decode_missing.assert_not_awaited()
    assert isolated_cache.metrics().entries == 1


@pytest.mark.asyncio
async def test_bounded_price_hydration_rejects_mixed_overflow_before_decode_or_admission(
    monkeypatch,
    isolated_cache,
):
    tables = _serving_tables()
    layout = ptg2_serving._version_three_price_cache_layout(tables)
    isolated_cache.admit_many(
        layout,
        {
            1: [
                {"negotiated_rate": str(atom_ordinal)}
                for atom_ordinal in range(200)
            ]
        },
    )
    membership_reader = AsyncMock(return_value={2: tuple(range(57))})
    atom_reader = AsyncMock(return_value={})
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_price_atom_memberships_from_db",
        membership_reader,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_shared_price_atoms_from_db",
        atom_reader,
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="atom limit"):
        await ptg2_serving._version_three_bounded_prices_by_key(
            object(),
            tables,
            [1, 2],
            maximum_atom_count=256,
        )

    membership_reader.assert_awaited_once()
    assert (
        membership_reader.await_args.kwargs["maximum_selected_atom_count"]
        == 56
    )
    atom_reader.assert_not_awaited()
    cached_rows, missing_keys = isolated_cache.get_many(layout, (1, 2))
    assert tuple(cached_rows) == (1,)
    assert missing_keys == (2,)
    assert isolated_cache.metrics().entries == 1


@pytest.mark.asyncio
async def test_price_hydration_cache_returns_mutation_isolated_native_rows(
    monkeypatch,
    isolated_cache,
):
    hydrated_key_batches = []

    async def hydrate(_session, _tables, price_keys, **_kwargs):
        hydrated_key_batches.append(tuple(price_keys))
        return _hydration(tuple(price_keys))

    monkeypatch.setattr(ptg2_serving, "_version_three_price_hydration", hydrate)
    tables = _serving_tables()
    miss_rows = await ptg2_serving._version_three_prices_by_key(
        object(), tables, [7]
    )
    miss_rows[7][0]["service_code"].append("mutated")
    miss_rows[7].append({"negotiated_rate": "999.00"})
    first_hit = await ptg2_serving._version_three_prices_by_key(
        object(), tables, [7]
    )
    first_hit[7][0]["additional_information"]["kind"] = "changed"
    second_hit = await ptg2_serving._version_three_prices_by_key(
        object(), tables, [7]
    )

    assert hydrated_key_batches == [(7,)]
    assert second_hit == {7: _rows(7)}
    assert isinstance(second_hit[7], list)
    assert isinstance(second_hit[7][0], dict)
    assert isinstance(second_hit[7][0]["service_code"], list)
    assert isolated_cache.metrics().hits == 2


@pytest.mark.asyncio
async def test_price_hydration_cache_separates_snapshot_and_layout_coordinates(
    monkeypatch,
    isolated_cache,
):
    hydrate_calls = []

    async def hydrate(_session, tables, price_keys, **_kwargs):
        hydrate_calls.append((tables.shared_snapshot_key, tables.storage_generation))
        return _hydration(tuple(price_keys))

    monkeypatch.setattr(ptg2_serving, "_version_three_price_hydration", hydrate)
    table_variants = (
        _serving_tables(),
        _serving_tables(shared_snapshot_key=42),
        _serving_tables(atom_key_bits=32),
        _serving_tables(price_key_block_span=256),
        _serving_tables(atom_key_block_span=256),
        _serving_tables(price_atom_constant_values={"billing_class": "institutional"}),
        _serving_tables(
            storage_generation="shared_blocks_v4",
            shared_block_layout="packed_snapshot_maps_v4",
        ),
    )
    for tables in table_variants:
        assert await ptg2_serving._version_three_prices_by_key(
            object(), tables, [5]
        ) == {5: _rows(5)}

    assert len(hydrate_calls) == len(table_variants)
    assert isolated_cache.metrics().entries == len(table_variants)


@pytest.mark.asyncio
@pytest.mark.parametrize("field_value", [None, True, 0, "bad"])
async def test_price_hydration_cache_rejects_invalid_layout_before_hit(
    monkeypatch,
    isolated_cache,
    field_value,
):
    async def hydrate(_session, _tables, price_keys, **_kwargs):
        return _hydration(tuple(price_keys))

    monkeypatch.setattr(ptg2_serving, "_version_three_price_hydration", hydrate)
    await ptg2_serving._version_three_prices_by_key(
        object(), _serving_tables(), [5]
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="block_span"):
        await ptg2_serving._version_three_prices_by_key(
            object(),
            _serving_tables(price_key_block_span=field_value),
            [5],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("failed"), asyncio.CancelledError()])
async def test_failed_or_cancelled_hydration_never_enters_cache(
    monkeypatch,
    isolated_cache,
    failure,
):
    hydrated_key_batches = []

    async def hydrate(_session, _tables, price_keys, **_kwargs):
        hydrated_key_batches.append(tuple(price_keys))
        if len(hydrated_key_batches) == 1:
            raise failure
        return _hydration(tuple(price_keys))

    monkeypatch.setattr(ptg2_serving, "_version_three_price_hydration", hydrate)
    with pytest.raises(type(failure)):
        await ptg2_serving._version_three_prices_by_key(
            object(), _serving_tables(), [9]
        )
    assert isolated_cache.metrics().entries == 0

    assert await ptg2_serving._version_three_prices_by_key(
        object(), _serving_tables(), [9]
    ) == {9: _rows(9)}
    assert hydrated_key_batches == [(9,), (9,)]


@pytest.mark.asyncio
async def test_partial_hydration_result_fails_closed_without_admission(
    monkeypatch,
    isolated_cache,
):
    async def incomplete_hydration(_session, _tables, _price_keys, **_kwargs):
        return ptg2_serving._VersionThreePriceHydration({1: (101,)}, {1: _rows(1)})

    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_price_hydration",
        incomplete_hydration,
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="omitted"):
        await ptg2_serving._version_three_prices_by_key(
            object(),
            _serving_tables(),
            [1, 2],
        )
    assert isolated_cache.metrics().entries == 0


@pytest.mark.asyncio
async def test_copy_payloads_false_bypasses_shared_cache(monkeypatch, isolated_cache):
    hydrated_key_batches = []

    async def hydrate(_session, _tables, price_keys, **_kwargs):
        hydrated_key_batches.append(tuple(price_keys))
        return _hydration(tuple(price_keys))

    monkeypatch.setattr(ptg2_serving, "_version_three_price_hydration", hydrate)
    for _call_index in range(2):
        assert await ptg2_serving._version_three_prices_by_key(
            object(),
            _serving_tables(),
            [3],
            copy_payloads=False,
        ) == {3: _rows(3)}

    assert hydrated_key_batches == [(3,), (3,)]
    assert isolated_cache.metrics().entries == 0


def test_price_hydration_cache_uses_lru_eviction():
    layout = _layout()
    probe = price_cache.PriceHydrationCache(1024 * 1024)
    probe.admit_many(layout, {1: _rows(1)})
    assert probe.metrics().entries == 1
    one_entry_bytes = probe.metrics().retained_bytes
    cache = price_cache.PriceHydrationCache(one_entry_bytes * 2)
    cache.admit_many(layout, {1: _rows(1), 2: _rows(2)})
    assert cache.metrics().entries == 2
    assert cache.get_many(layout, (1,))[1] == ()
    cache.admit_many(layout, {1: _rows(1), 3: _rows(3)})

    cached_rows, missing_keys = cache.get_many(layout, (1, 2, 3))
    assert tuple(cached_rows) == (1, 3)
    assert missing_keys == (2,)
    assert cache.metrics().evictions == 1
    assert cache.metrics().retained_bytes <= cache.metrics().maximum_bytes


def test_disabled_and_empty_cache_admission_are_noops():
    """Disabled or empty admission never mutates counters or retained state."""

    disabled_cache = price_cache.PriceHydrationCache(0)
    disabled_cache.admit_many(_layout(), {1: _rows(1)})
    enabled_cache = price_cache.PriceHydrationCache(1024)
    enabled_cache.admit_many(_layout(), {})

    assert disabled_cache.metrics().entries == 0
    assert disabled_cache.metrics().rejected_batches == 0
    assert enabled_cache.metrics().entries == 0


def test_private_copy_weight_is_rechecked_before_atomic_admission(monkeypatch):
    """A copy that grows beyond its preflight estimate is rejected atomically."""

    layout = _layout()
    rows_by_key = {1: _rows(1)}
    probe = price_cache.PriceHydrationCache(1024 * 1024)
    maximum_bytes = probe._batch_weight(layout, rows_by_key) + 1
    cache = price_cache.PriceHydrationCache(maximum_bytes)
    expanded_rows = [{"blob": b"x" * maximum_bytes, "flags": {"a", "b"}}]
    monkeypatch.setattr(price_cache, "_copy_private_rows", lambda _rows: expanded_rows)

    cache.admit_many(layout, rows_by_key)

    assert cache.metrics().entries == 0
    assert cache.metrics().rejected_batches == 1


def test_batch_replacement_skips_the_replaced_key_during_eviction():
    """Atomic replacement evicts an older peer without evicting its own key."""

    layout = _layout()
    probe = price_cache.PriceHydrationCache(1024 * 1024)
    probe.admit_many(layout, {1: _rows(1)})
    one_entry_bytes = probe.metrics().retained_bytes
    cache = price_cache.PriceHydrationCache(one_entry_bytes * 2)
    cache.admit_many(layout, {1: _rows(1), 2: _rows(2)})

    cache.admit_many(layout, {1: _rows(1), 3: _rows(3)})

    cached_rows, missing_keys = cache.get_many(layout, (1, 2, 3))
    assert tuple(cached_rows) == (1, 3)
    assert missing_keys == (2,)
    assert cache.metrics().evictions == 1


def test_oversized_batch_is_rejected_before_private_copy(monkeypatch):
    cache = price_cache.PriceHydrationCache(1)

    def unexpected_copy(_rows):
        raise AssertionError("oversized rows must not be copied")

    monkeypatch.setattr(price_cache, "_copy_private_rows", unexpected_copy)
    cache.admit_many(_layout(), {1: _rows(1)})
    assert cache.metrics().entries == 0
    assert cache.metrics().rejected_batches == 1


def test_batch_copy_failure_is_atomic(monkeypatch):
    cache = price_cache.PriceHydrationCache(1024 * 1024)
    original_copy = price_cache._copy_private_rows
    copied_row_batches = []

    def fail_second_copy(rows):
        copied_row_batches.append(rows)
        if len(copied_row_batches) == 2:
            raise RuntimeError("copy failed")
        return original_copy(rows)

    monkeypatch.setattr(price_cache, "_copy_private_rows", fail_second_copy)
    with pytest.raises(RuntimeError, match="copy failed"):
        cache.admit_many(_layout(), {1: _rows(1), 2: _rows(2)})

    assert cache.metrics().entries == 0
    assert cache.metrics().retained_bytes == 0


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        (None, 8 * 1024 * 1024),
        ("0", 0),
        ("4096", 4096),
        (str(32 * 1024 * 1024), 16 * 1024 * 1024),
        ("invalid", 8 * 1024 * 1024),
    ],
)
def test_price_hydration_cache_configuration_is_guarded(
    monkeypatch,
    configured,
    expected,
):
    if configured is None:
        monkeypatch.delenv("HLTHPRT_PTG2_PRICE_HYDRATION_CACHE_BYTES", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_PTG2_PRICE_HYDRATION_CACHE_BYTES", configured)

    assert price_cache.configured_price_hydration_cache_bytes() == expected
