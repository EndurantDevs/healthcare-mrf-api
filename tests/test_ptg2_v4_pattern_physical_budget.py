# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Integrated physical-work bounds for V4 pattern completion."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars as sidecars
from api import ptg2_serving as serving
from api.ptg2_shared_blocks import SharedBlockPayload
from tests.test_ptg2_v4_filtered_reverse_serving import (
    _projection_fixture_for,
    _projection_rule,
    _provider_set_id,
    _tables,
)


def _encode_uvarint(integer: int) -> bytes:
    encoded_bytes = bytearray()
    remaining_integer = int(integer)
    while remaining_integer >= 0x80:
        encoded_bytes.append((remaining_integer & 0x7F) | 0x80)
        remaining_integer >>= 7
    encoded_bytes.append(remaining_integer)
    return bytes(encoded_bytes)


def _dense_fragment(occurrence_count: int) -> SharedBlockPayload:
    payload_bytes = bytearray([2, 1, 0])
    payload_bytes.extend(_encode_uvarint(1))
    payload_bytes.extend(_encode_uvarint(occurrence_count))
    for price_key in range(occurrence_count):
        payload_bytes.extend(_encode_uvarint(price_key))
    return SharedBlockPayload(
        block_key=sidecars._forward_provider_shard_block_key(7, 1),
        fragment_no=0,
        entry_count=1,
        payload=bytes(payload_bytes),
    )


def _provider_fragment(provider_count: int) -> SharedBlockPayload:
    payload_bytes = bytearray([2, 1, 0])
    for provider_set_key in range(1, provider_count + 1):
        payload_bytes.extend(_encode_uvarint(1))
        payload_bytes.extend(_encode_uvarint(1))
        payload_bytes.extend(_encode_uvarint(provider_set_key - 1))
    return SharedBlockPayload(
        block_key=sidecars._forward_provider_shard_block_key(7, 1),
        fragment_no=0,
        entry_count=provider_count,
        payload=bytes(payload_bytes),
    )


def _install_forward_stream(monkeypatch, fragment):
    stream_calls = []

    async def stream_fragments(_session, **kwargs):
        stream_calls.append(kwargs)
        yield fragment

    monkeypatch.setattr(sidecars, "stream_shared_blocks", stream_fragments)
    monkeypatch.setattr(
        sidecars,
        "_discover_forward_shard_keys",
        AsyncMock(return_value={7: (fragment.block_key,)}),
    )
    async def provider_counts(_session, **kwargs):
        return {
            provider_key: 1
            for provider_key, _price_key, _source_key in kwargs["decoded_keys"]
        }

    monkeypatch.setattr(
        sidecars,
        "_provider_counts_for_decoded_keys",
        AsyncMock(side_effect=provider_counts),
    )
    monkeypatch.setattr(
        sidecars,
        "_serving_binary_dictionary_values_for_keys",
        AsyncMock(
            side_effect=lambda _session, **kwargs: {
                price_key: f"{price_key:032x}"
                for price_key in kwargs["item_keys"]
            }
        ),
    )
    return stream_calls


def _install_pattern_graph(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_version_three_forward_page_rows",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        serving,
        "_version_three_provider_pages_for_keys",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={1: _provider_set_id(1)}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_intersections",
        AsyncMock(return_value={1: (7,)}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_v4_relation_members",
        AsyncMock(return_value={7: (1,)}),
    )
    monkeypatch.setattr(
        serving,
        "v4_npi_values_for_keys",
        AsyncMock(return_value={1: 1_000_000_001}),
    )


async def _late_provider_ids(_session, _tables, provider_set_keys):
    return {
        provider_set_key: _provider_set_id(provider_set_key)
        for provider_set_key in provider_set_keys
    }


async def _late_set_patterns(_session, **kwargs):
    return {
        provider_set_key: ((7,) if provider_set_key == 70 else ())
        for provider_set_key in kwargs["owner_keys"]
    }


def _install_late_pattern_graph(monkeypatch):
    replacement_by_attribute = {
        "_version_three_forward_page_rows": AsyncMock(return_value=None),
        "_version_three_provider_pages_for_keys": AsyncMock(return_value=None),
        "_provider_set_ids_for_keys": AsyncMock(
            side_effect=_late_provider_ids
        ),
        "lookup_v4_relation_intersections": AsyncMock(
            side_effect=_late_set_patterns
        ),
        "lookup_v4_relation_members": AsyncMock(return_value={7: (70,)}),
        "v4_npi_values_for_keys": AsyncMock(
            return_value={1: 1_000_000_001}
        ),
        "_selected_provider_rows_by_set": AsyncMock(return_value={}),
    }
    for attribute_name, replacement in replacement_by_attribute.items():
        monkeypatch.setattr(serving, attribute_name, replacement)


def _late_pattern_runtime(monkeypatch):
    projection_manifest, candidates = _projection_fixture_for(
        (1,),
        {7: (1,)},
    )
    projection_rule = replace(
        _projection_rule(projection_manifest),
        max_online_inferred_taxonomy_graph_pages=6,
    )
    serving_tables = replace(
        _tables(projection_manifest),
        price_dictionary_item_count=256,
        price_dictionary_block_bytes=4096,
        provider_shard_span=1024,
    )
    stream_calls = _install_forward_stream(
        monkeypatch,
        _provider_fragment(200),
    )
    _install_late_pattern_graph(monkeypatch)
    full_materialization = AsyncMock(
        side_effect=AssertionError("bounded completion used the full reader")
    )
    monkeypatch.setattr(
        serving,
        "lookup_serving_binary_by_code_from_db",
        full_materialization,
    )
    scan_budget = sidecars.ForwardReadBudget(
        maximum_fragments=6,
        maximum_raw_payload_bytes=(
            projection_rule.max_online_inferred_taxonomy_graph_bytes
        ),
        maximum_row_capacity=(
            projection_rule.max_online_filtered_reverse_code_occurrences + 1
        ),
    )
    monkeypatch.setattr(
        serving,
        "ForwardReadBudget",
        lambda **_kwargs: scan_budget,
    )
    return (
        candidates,
        projection_rule,
        serving_tables,
        stream_calls,
        full_materialization,
        scan_budget,
    )


@pytest.mark.asyncio
async def test_pattern_completion_shares_typed_physical_scan_budget(
    monkeypatch,
):
    projection_manifest, candidates = _projection_fixture_for(
        (1,),
        {7: (1,)},
    )
    projection_rule = replace(
        _projection_rule(projection_manifest),
        max_online_inferred_taxonomy_graph_pages=3,
    )
    serving_tables = replace(
        _tables(projection_manifest),
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2048,
        provider_shard_span=1024,
    )
    stream_calls = _install_forward_stream(
        monkeypatch,
        _dense_fragment(65),
    )
    _install_pattern_graph(monkeypatch)
    full_materialization = AsyncMock(
        side_effect=AssertionError("bounded completion used the full reader")
    )
    monkeypatch.setattr(
        serving,
        "lookup_serving_binary_by_code_from_db",
        full_materialization,
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await serving._select_v4_pattern_taxonomy_expansion(
            object(),
            serving_tables,
            code_rows=[
                {
                    "code_key": 7,
                    "rate_count": 65,
                    "reported_code_system": "CPT",
                    "reported_code": "70553",
                }
            ],
            args={"code_system": "CPT", "code": "70553"},
            snapshot_id="snapshot",
            source_trace_set_hash=None,
            network_names=[],
            target_count=1,
            descending=False,
            projection_rule=projection_rule,
            candidates=candidates,
        )

    assert exc_info.value.dimension == "forward_scan"
    assert len(stream_calls) == 2
    full_materialization.assert_not_awaited()


@pytest.mark.asyncio
async def test_late_pattern_match_releases_prefix_and_admits_completion_peak(
    monkeypatch,
):
    """Release old prefixes while retaining the final one during completion."""

    (
        candidates,
        projection_rule,
        serving_tables,
        stream_calls,
        full_materialization,
        scan_budget,
    ) = _late_pattern_runtime(monkeypatch)

    selection = await serving._select_v4_pattern_taxonomy_expansion(
        object(),
        serving_tables,
        code_rows=[
            {
                "code_key": 7,
                "rate_count": 200,
                "reported_code_system": "CPT",
                "reported_code": "70553",
            }
        ],
        args={"code_system": "CPT", "code": "70553"},
        snapshot_id="snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=1,
        descending=False,
        projection_rule=projection_rule,
        candidates=candidates,
    )

    assert [
        selected_row["_ptg_provider_set_key"]
        for selected_row in selection.row_data
    ] == [70]
    assert len(stream_calls) == 3
    assert scan_budget.peak_read_row_capacity == 328
    assert scan_budget.peak_result_row_capacity == 328
    assert scan_budget.active_read_row_capacity == 0
    assert scan_budget.active_result_row_capacity == 0
    full_materialization.assert_not_awaited()
