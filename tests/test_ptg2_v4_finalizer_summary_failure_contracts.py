# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed contracts for packed-finalizer receipt summaries."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_v4_finalizer_mapping_receipt as mapping_receipt
from process.ptg_parts import ptg2_v4_finalizer_mapping_summary as mapping_summary
from process.ptg_parts import ptg2_v4_finalizer_native as native
from process.ptg_parts.ptg2_shared_blocks import _SharedMappingBinaryCopyDigest
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from tests.test_ptg2_v4_finalizer_mapping_summary import (
    _Session as _MappingSummarySession,
)
from tests.test_ptg2_v4_finalizer_mapping_summary import (
    _fixture as _mapping_summary_fixture,
)
from tests.test_ptg2_v4_finalizer_maps import _Rows

def _price_aggregates(fixture) -> dict[str, tuple[int, int, int, int]]:
    return {
        row["object_kind"]: (
            row["mapping_count"],
            row["unique_block_count"],
            row["entry_count"],
            row["logical_byte_count"],
        )
        for row in fixture["aggregates"]
    }


@pytest.mark.parametrize("case", ("contract", "kinds", "digest"))
@pytest.mark.asyncio
async def test_native_mapping_receipt_rejects_identity_drift(case: str) -> None:
    fixture = _mapping_summary_fixture()
    root_by_field = dict(fixture["root"])
    aggregates = _price_aggregates(fixture)
    if case == "contract":
        root_by_field["root_contract"] = "unknown"
        message = "contract is unavailable"
    elif case == "kinds":
        aggregates.pop(next(iter(aggregates)))
        message = "exactly two relational price mappings"
    else:
        root_by_field["root_canonical_mapping_digest"] = b"short"
        message = "native receipt is incomplete"
    session = _MappingSummarySession(fixture)
    with pytest.raises(RuntimeError, match=message):
        await mapping_receipt.summarize_native_finalizer_mapping_receipts(
            session,
            schema='"mrf"',
            snapshot_key=41,
            root_by_name=root_by_field,
            aggregate_by_object_kind=aggregates,
            copy_from_query=session.driver.copy_from_query,
        )


@pytest.mark.parametrize("case", ("root", "geometry"))
def test_native_mapping_receipt_rejects_root_drift(case: str) -> None:
    root_by_field = dict(_mapping_summary_fixture()["root"])
    if case == "root":
        root_by_field["completed_at"] = None
        message = "root is incomplete or incompatible"
    else:
        root_by_field["target_block_count"] = 0
        message = "root geometry is invalid"
    with pytest.raises(RuntimeError, match=message):
        mapping_receipt._validated_root_counts(root_by_field)


@pytest.mark.parametrize("case", ("count", "entries"))
@pytest.mark.asyncio
async def test_native_price_receipt_rejects_copy_aggregate_drift(case: str) -> None:
    fixture = _mapping_summary_fixture()
    aggregates = _price_aggregates(fixture)
    first_kind = next(iter(aggregates))
    values = list(aggregates[first_kind])
    values[0 if case == "count" else 2] += 1
    aggregates[first_kind] = tuple(values)
    request = mapping_receipt._HybridRequest(
        session=object(),
        schema='"mrf"',
        snapshot_key=41,
        aggregate_by_object_kind=aggregates,
        copy_from_query=_MappingSummarySession(fixture).driver.copy_from_query,
    )
    with pytest.raises(RuntimeError, match="mapping count changed|receipt changed"):
        await mapping_receipt._summarize_price_mappings(request)


@pytest.mark.asyncio
async def test_hybrid_summary_rejects_missing_and_full_page_packed_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _mapping_summary_fixture()
    request = mapping_receipt._HybridRequest(
        session=object(),
        schema='"mrf"',
        snapshot_key=41,
        aggregate_by_object_kind=_price_aggregates(fixture),
        copy_from_query=None,
    )
    totals = mapping_summary._PackedTotals()
    canonical = _SharedMappingBinaryCopyDigest()
    packed = _SharedMappingBinaryCopyDigest()
    monkeypatch.setattr(
        mapping_summary,
        "_load_pack_batch",
        AsyncMock(side_effect=([{}] * mapping_summary._PACK_BATCH_ROWS, [])),
    )
    monkeypatch.setattr(
        mapping_summary,
        "_decode_persisted_map_rows",
        lambda *_args: ((), set()),
    )
    monkeypatch.setattr(mapping_summary, "_load_target_metadata", AsyncMock(return_value={}))
    with pytest.raises(RuntimeError, match="map is missing"):
        await mapping_summary._summarize_packed_kind(
            request,
            object_kind=PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS[0],
            totals=totals,
            canonical_digest=canonical,
            packed_digest=packed,
        )


@pytest.mark.asyncio
async def test_hybrid_price_copy_rejects_count_drift() -> None:
    fixture = _mapping_summary_fixture()
    aggregates = _price_aggregates(fixture)
    object_kind = next(iter(aggregates))
    values = list(aggregates[object_kind])
    values[0] += 1
    aggregates[object_kind] = tuple(values)
    request = mapping_receipt._HybridRequest(
        session=object(), schema='"mrf"', snapshot_key=41,
        aggregate_by_object_kind=aggregates,
        copy_from_query=_MappingSummarySession(fixture).driver.copy_from_query,
    )
    with pytest.raises(RuntimeError, match="mapping count changed"):
        await mapping_summary._copy_price_kind(
            request,
            object_kind=object_kind,
            canonical_digest=_SharedMappingBinaryCopyDigest(),
            price_digest=_SharedMappingBinaryCopyDigest(),
        )


@pytest.mark.asyncio
async def test_hybrid_target_validation_rejects_invalid_anchor_count() -> None:
    class Session:
        async def execute(self, *_args, **_kwargs):
            return _Rows(({"target_count": 2, "valid_target_count": 1},))

    request = mapping_receipt._HybridRequest(
        session=Session(), schema='"mrf"', snapshot_key=41,
        aggregate_by_object_kind={}, copy_from_query=None,
    )
    with pytest.raises(RuntimeError, match="target anchors are invalid"):
        await mapping_summary._validated_target_count(request)


def test_hybrid_summary_rejects_root_totals_and_canonical_aggregates() -> None:
    fixture = _mapping_summary_fixture()
    root = mapping_receipt._validated_root_counts(fixture["root"])
    totals = mapping_summary._PackedTotals()
    with pytest.raises(RuntimeError, match="root disagrees"):
        mapping_summary._validate_packed_totals(
            totals,
            root,
            bytes(fixture["root"]["root_map_digest"]),
        )

    request = mapping_receipt._HybridRequest(
        session=object(), schema='"mrf"', snapshot_key=41,
        aggregate_by_object_kind=_price_aggregates(fixture), copy_from_query=None,
    )
    with pytest.raises(RuntimeError, match="canonical mapping aggregates changed"):
        mapping_summary._finish_hybrid_summary(
            request,
            root_counts=root,
            target_block_count=root.target_block_count,
            canonical_digest=_SharedMappingBinaryCopyDigest(),
            packed_digest=_SharedMappingBinaryCopyDigest(),
            price_digest=_SharedMappingBinaryCopyDigest(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("kinds", "targets"))
async def test_hybrid_summary_rejects_layout_or_target_count_drift(case: str) -> None:
    fixture = _mapping_summary_fixture()
    aggregates = _price_aggregates(fixture)
    if case == "kinds":
        aggregates.pop(next(iter(aggregates)))
        message = "exactly two relational price mappings"
    else:
        fixture["root"]["target_block_count"] -= 1
        message = "target anchor count changed"
    with pytest.raises(RuntimeError, match=message):
        await mapping_summary.summarize_hybrid_finalizer_mappings(
            _MappingSummarySession(fixture),
            schema='"mrf"',
            snapshot_key=41,
            root_by_name=fixture["root"],
            aggregate_by_object_kind=aggregates,
            copy_from_query=_MappingSummarySession(fixture).driver.copy_from_query,
        )
