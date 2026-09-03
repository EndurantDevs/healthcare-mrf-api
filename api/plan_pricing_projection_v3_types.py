# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared state and bounded inserts for pricing projection v3."""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Iterable

from sqlalchemy import text

from api.plan_pricing_aggregate_pack import (
    MAX_AGGREGATE_PACK_DECODED_BYTES,
    MAX_AGGREGATE_PACK_RECORDS,
)
from api.plan_pricing_projection_contract import INSERT_BATCH_SIZE
from api.ptg2_db_sidecars import _PriceMembershipAliasCache


MAX_PREWARM_SHAPES = 768
_BROAD_EM_CODES = frozenset(str(code) for code in range(99202, 99216))


@dataclass(frozen=True)
class ProjectionV3Counts:
    """Sealing counts for one complete factorized candidate."""

    provider_membership_count: int
    provider_cell_count: int
    provider_fragment_byte_count: int
    aggregate_entry_count: int
    aggregate_pack_count: int
    aggregate_raw_byte_count: int
    aggregate_stored_byte_count: int
    prewarm_shape_count: int
    rate_profile_count: int = 0
    provider_state_count: int = 0
    rate_occurrence_count: int = 0

    def __post_init__(self) -> None:
        counts = (
            self.provider_membership_count,
            self.provider_cell_count,
            self.provider_fragment_byte_count,
            self.aggregate_entry_count,
            self.aggregate_pack_count,
            self.aggregate_raw_byte_count,
            self.aggregate_stored_byte_count,
            self.prewarm_shape_count,
            self.rate_profile_count,
            self.provider_state_count,
            self.rate_occurrence_count,
        )
        if any(type(count) is not int or count < 0 for count in counts):
            raise ValueError("factorized projection counts are invalid")
        if (
            self.aggregate_pack_count > self.aggregate_entry_count
            or self.aggregate_entry_count
            > self.aggregate_pack_count * MAX_AGGREGATE_PACK_RECORDS
            or self.aggregate_raw_byte_count < self.aggregate_pack_count
            or self.aggregate_raw_byte_count
            > self.aggregate_pack_count * MAX_AGGREGATE_PACK_DECODED_BYTES
            or self.aggregate_stored_byte_count < self.aggregate_pack_count
            or self.prewarm_shape_count
            > min(MAX_PREWARM_SHAPES, self.aggregate_entry_count)
            or (self.aggregate_entry_count == 0)
            != (self.aggregate_pack_count == 0)
            or (self.provider_cell_count == 0)
            != (self.provider_fragment_byte_count == 0)
            or (
                self.rate_profile_count > 0
                and self.provider_membership_count == 0
            )
            or (
                self.provider_state_count > 0
                and self.provider_cell_count == 0
            )
            or (
                self.rate_occurrence_count > 0
                and self.rate_profile_count == 0
            )
        ):
            raise ValueError("factorized projection counts are inconsistent")


@dataclass(frozen=True)
class _PrewarmShape:
    code_system: str
    code: str
    geo_cell: str
    provider_count: int

    @property
    def identity(self) -> tuple[str, str, str]:
        """Return the deterministic code-and-ZIP tie-break identity."""

        return self.code_system, self.code, self.geo_cell


@dataclass(order=False)
class _PrewarmHeapItem:
    shape: _PrewarmShape

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, _PrewarmHeapItem):
            return NotImplemented
        if self.shape.provider_count != other.shape.provider_count:
            return self.shape.provider_count < other.shape.provider_count
        return self.shape.identity > other.shape.identity


@dataclass
class _BuildState:
    content_digest: Any
    prewarm_heap: list[_PrewarmHeapItem] = field(default_factory=list)
    staged_provider_set_count: int = 0
    provider_membership_count: int = 0
    provider_cell_count: int = 0
    provider_fragment_byte_count: int = 0
    aggregate_entry_count: int = 0
    aggregate_pack_count: int = 0
    aggregate_raw_byte_count: int = 0
    aggregate_stored_byte_count: int = 0
    rate_profile_count: int = 0
    provider_state_count: int = 0
    rate_occurrence_count: int = 0
    membership_probe_work_rows: int = 0
    member_cell_work_rows: int = 0
    rate_profile_work_rows: int = 0
    aggregate_work_rows: int = 0
    price_membership_alias_cache: _PriceMembershipAliasCache = field(
        default_factory=_PriceMembershipAliasCache
    )


async def _insert_batches(
    session: Any,
    statement: str,
    rows: Iterable[dict[str, Any]],
) -> None:
    batch_rows: list[dict[str, Any]] = []
    prepared_statement = text(statement)
    for row in rows:
        batch_rows.append(row)
        if len(batch_rows) >= INSERT_BATCH_SIZE:
            await session.execute(prepared_statement, batch_rows)
            batch_rows.clear()
    if batch_rows:
        await session.execute(prepared_statement, batch_rows)


def _retain_prewarm_shape(
    heap: list[_PrewarmHeapItem],
    code_identity: tuple[str, str],
    record: Any,
) -> None:
    if (
        code_identity[0] in {"CPT", "HCPCS"}
        and code_identity[1] in _BROAD_EM_CODES
    ):
        return
    item = _PrewarmHeapItem(
        _PrewarmShape(
            code_identity[0],
            code_identity[1],
            record.zip5,
            record.provider_count,
        )
    )
    if len(heap) < MAX_PREWARM_SHAPES:
        heapq.heappush(heap, item)
    elif heap[0] < item:
        heapq.heapreplace(heap, item)


def _ordered_prewarm_shapes(
    heap: Iterable[_PrewarmHeapItem],
) -> tuple[_PrewarmShape, ...]:
    return tuple(
        sorted(
            (item.shape for item in heap),
            key=lambda shape: (-shape.provider_count, *shape.identity),
        )
    )
