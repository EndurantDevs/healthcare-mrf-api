# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-binary proof for the single-scan V4 exact-NPI serving path."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_sidecars as sidecars
from api import ptg2_serving as serving
from api.ptg2_shared_blocks import SharedBlockPayload
from api.ptg2_v4_graph import V4GraphRoot
from tests.ptg2_v4_orchestration_support import _tables


_NPI = 1_093_356_685


def _uvarint(value: int) -> bytes:
    encoded = bytearray()
    remaining = int(value)
    while remaining >= 0x80:
        encoded.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    encoded.append(remaining)
    return bytes(encoded)


def _grouped_payload(entries: tuple[tuple[int, int], ...]) -> bytes:
    payload = bytearray([2, 1, 0])
    previous_provider_set_key = 0
    for provider_set_key, price_key in entries:
        payload.extend(_uvarint(provider_set_key - previous_provider_set_key))
        payload.extend(_uvarint(1))
        payload.extend(_uvarint(price_key))
        previous_provider_set_key = provider_set_key
    return bytes(payload)


def _forward_fragment(
    provider_set_key: int,
    price_key: int,
    *,
    payload: bytes | None = None,
    entry_count: int = 1,
) -> dict[str, object]:
    return {
        "block_key": (7 << 31) | (provider_set_key // 1024),
        "block_no": 0,
        "entry_count": entry_count,
        "_decoded_payload": (
            payload
            if payload is not None
            else _grouped_payload(((provider_set_key, price_key),))
        ),
    }


def _binary_tables() -> serving.PTG2ServingTables:
    return replace(
        _tables(),
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=2_048,
        provider_shard_span=1_024,
    )


def _install_binary_forward_read(
    monkeypatch,
    provider_set_keys: tuple[int, ...],
    fragment_rows: tuple[dict[str, object], ...],
):
    """Install native binary decoding behind bounded V4 NPI membership."""

    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={_NPI: provider_set_keys}),
    )
    preliminary_rate_scope = AsyncMock(
        side_effect=AssertionError("exact NPI scope scanned code twice")
    )
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        preliminary_rate_scope,
    )
    provider_pages = AsyncMock(return_value=None)
    monkeypatch.setattr(
        serving,
        "_version_three_provider_pages_for_keys",
        provider_pages,
    )
    binary_spies = _install_binary_decoder_spies(
        monkeypatch,
        fragment_rows,
    )
    return (
        preliminary_rate_scope,
        provider_pages,
        *binary_spies,
    )


def _install_binary_decoder_spies(
    monkeypatch,
    fragment_rows: tuple[dict[str, object], ...],
):
    """Instrument the real decoder and its retained dictionary hydration."""

    stream_calls: list[dict[str, object]] = []

    async def stream_fragments(_session, **kwargs):
        stream_calls.append(dict(kwargs))
        requested_block_keys = set(kwargs["block_keys"])
        for fragment_row in fragment_rows:
            if int(fragment_row["block_key"]) in requested_block_keys:
                yield SharedBlockPayload(
                    block_key=int(fragment_row["block_key"]),
                    fragment_no=int(fragment_row["block_no"]),
                    entry_count=int(fragment_row["entry_count"]),
                    payload=bytes(fragment_row["_decoded_payload"]),
                )

    monkeypatch.setattr(sidecars, "stream_shared_blocks", stream_fragments)
    provider_counts = AsyncMock(
        side_effect=lambda _session, **kwargs: {
            key: key * 10
            for key, _price_key, _source_key in kwargs["decoded_keys"]
        }
    )
    price_dictionary = AsyncMock(
        side_effect=lambda _session, **kwargs: {
            key: f"{key:032x}" for key in kwargs["item_keys"]
        }
    )
    monkeypatch.setattr(
        sidecars,
        "_provider_counts_for_decoded_keys",
        provider_counts,
    )
    monkeypatch.setattr(
        sidecars,
        "_serving_binary_dictionary_values_for_keys",
        price_dictionary,
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(
            side_effect=lambda _session, _tables, keys: {
                key: f"{key:032x}" for key in set(keys)
            }
        ),
    )
    visit_fragment = Mock(
        wraps=sidecars._visit_serving_binary_by_code_record
    )
    monkeypatch.setattr(
        sidecars,
        "_visit_serving_binary_by_code_record",
        visit_fragment,
    )
    return stream_calls, provider_counts, price_dictionary, visit_fragment


async def _read_exact_npi_rows():
    tables = _binary_tables()
    scope = await serving._version_three_explicit_npi_graph_scope(
        object(),
        tables,
        {
            "npi": _NPI,
            "plan_id": "plan-1",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "74329",
        },
    )
    assert scope is not None
    return await serving._merge_manifest_code_variant_rows(
        object(),
        tables,
        code_rows=[
            {
                "code_key": 7,
                "plan_id": "plan-1",
                "plan_market_type": "group",
                "reported_code_system": "CPT",
                "reported_code": "74329",
                "negotiation_arrangement": "FFS",
                "rate_count": 2,
            }
        ],
        provider_set_keys=scope.provider_set_keys,
        source_trace_set_hash=None,
        network_names=["Synthetic Network"],
        limit=25,
        offset=0,
    )


@pytest.mark.asyncio
async def test_exact_npi_binary_shards_are_visited_once(monkeypatch) -> None:
    """Decode each selected shard once and hydrate only retained rows."""

    probes = _install_binary_forward_read(
        monkeypatch,
        (5, 1025),
        (
            _forward_fragment(
                5,
                8,
                payload=_grouped_payload(((5, 8), (6, 10))),
                entry_count=2,
            ),
            _forward_fragment(1025, 9),
        ),
    )

    serving_rows = await _read_exact_npi_rows()

    assert serving_rows is not None
    assert [
        (
            serving_row["_ptg_provider_set_key"],
            serving_row["price_key"],
            serving_row["price_set_global_id_128"],
            serving_row["source_key"],
        )
        for serving_row in serving_rows
    ] == [(5, 8, f"{8:032x}", 0), (1025, 9, f"{9:032x}", 0)]
    rate_scope, pages, stream_calls, counts, prices, visits = probes
    rate_scope.assert_not_awaited()
    pages.assert_awaited_once()
    assert len(stream_calls) == 1
    counts.assert_awaited_once()
    prices.assert_awaited_once()
    assert visits.call_count == 2


@pytest.mark.asyncio
async def test_exact_npi_binary_empty_scope_avoids_hydration(
    monkeypatch,
) -> None:
    """Treat an absent selected shard as empty without dictionary reads."""

    probes = _install_binary_forward_read(monkeypatch, (2050,), ())
    missing_block = AsyncMock()
    monkeypatch.setattr(serving, "_raise_missing_v3_block", missing_block)

    assert await _read_exact_npi_rows() == []

    rate_scope, _pages, stream_calls, counts, prices, visits = probes
    rate_scope.assert_not_awaited()
    assert len(stream_calls) == 1
    counts.assert_not_awaited()
    prices.assert_not_awaited()
    assert visits.call_count == 0
    missing_block.assert_awaited_once()


@pytest.mark.asyncio
async def test_exact_npi_binary_malformed_shard_fails_before_hydration(
    monkeypatch,
) -> None:
    """Reject malformed selected bytes without a second traversal."""

    probes = _install_binary_forward_read(
        monkeypatch,
        (5,),
        (_forward_fragment(5, 8, payload=b"\x02"),),
    )

    with pytest.raises(serving.PTG2ManifestArtifactError):
        await _read_exact_npi_rows()

    rate_scope, _pages, stream_calls, counts, prices, visits = probes
    rate_scope.assert_not_awaited()
    assert len(stream_calls) == 1
    counts.assert_not_awaited()
    prices.assert_not_awaited()
    assert visits.call_count == 1
