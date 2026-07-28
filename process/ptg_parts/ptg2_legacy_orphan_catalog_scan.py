# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded catalog classification for legacy PTG cleanup."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LegacyBlockedSuffix,
    LegacySweepCandidate,
    LegacySweepLimits,
    build_bounded_legacy_sweep_plan,
    canonical_sha256,
    classify_legacy_suffix,
)
from process.ptg_parts.ptg2_legacy_orphan_models import (
    LegacyCatalogProgress,
)
from process.ptg_parts.ptg2_legacy_orphan_store import (
    load_legacy_catalog_inventory,
    load_legacy_ownership,
    load_legacy_relation_catalog,
)
from process.ptg_parts.ptg2_legacy_orphan_store_window import (
    bounded_catalog_segments,
    LegacyCatalogInventory,
    LegacyCatalogRelationKey,
    LegacyOversizedCatalogSuffix,
)


@dataclass(frozen=True)
class LegacyCatalogScan:
    """Classified catalog slice plus global inventory progress."""

    catalog_digest: str
    candidates: tuple[LegacySweepCandidate, ...]
    blocked_suffixes: tuple[LegacyBlockedSuffix, ...]
    progress: LegacyCatalogProgress


@dataclass(frozen=True)
class _LegacyCatalogContext:
    """Immutable inputs shared by every classified catalog window."""

    schema_name: str
    control_schema_name: str
    authority_digest: str
    present_optional_tables: frozenset[str]
    inventory: LegacyCatalogInventory


@dataclass
class _LegacyCatalogAccumulator:
    """Mutable scan state confined to one plan construction."""

    candidates: list[LegacySweepCandidate] = field(default_factory=list)
    blocked_suffixes: list[LegacyBlockedSuffix] = field(default_factory=list)
    window_digests: list[str] = field(default_factory=list)
    scanned_suffix_count: int = 0

    def progress(self, catalog_suffix_count: int) -> LegacyCatalogProgress:
        """Freeze current scan counters for plan and audit output."""

        return LegacyCatalogProgress(
            catalog_suffix_count,
            self.scanned_suffix_count,
        )


def _catalog_window_digest(
    *,
    inventory_digest: str,
    inventory_suffix_count: int,
    scanned_suffix_count: int,
    window_digests: list[str],
) -> str:
    """Bind the full suffix inventory and every inspected catalog window."""

    return canonical_sha256(
        {
            "contract": "ptg2_legacy_orphan_catalog_window_v1",
            "inventory_digest": inventory_digest,
            "inventory_suffix_count": inventory_suffix_count,
            "scanned_suffix_count": scanned_suffix_count,
            "window_digests": window_digests,
        }
    )


async def _classify_catalog_window(
    executor: Any,
    *,
    context: _LegacyCatalogContext,
    suffix_window: tuple[str, ...],
    relation_keys: tuple[LegacyCatalogRelationKey, ...],
) -> tuple[
    list[LegacySweepCandidate],
    list[LegacyBlockedSuffix],
    str,
]:
    """Validate and classify one fixed lexical suffix window."""

    catalog = await load_legacy_relation_catalog(
        executor,
        schema_name=context.schema_name,
        probe_rows=True,
        relation_keys=relation_keys,
    )
    if set(catalog.relations_by_suffix) != set(suffix_window):
        raise RuntimeError(
            "legacy_sweep_catalog_window_changed:"
            f"{len(catalog.relations_by_suffix)}:{len(suffix_window)}"
        )
    ownership_by_suffix = await load_legacy_ownership(
        executor,
        schema_name=context.schema_name,
        control_schema_name=context.control_schema_name,
        catalog=catalog,
        present_optional_table_names=context.present_optional_tables,
    )
    classifications = [
        classify_legacy_suffix(
            suffix,
            catalog.relations_by_suffix[suffix],
            ownership_by_suffix[suffix],
        )
        for suffix in suffix_window
    ]
    candidates = [
        classification
        for classification in classifications
        if isinstance(classification, LegacySweepCandidate)
    ]
    blocked_suffixes = [
        classification
        for classification in classifications
        if isinstance(classification, LegacyBlockedSuffix)
    ]
    return candidates, blocked_suffixes, catalog.catalog_digest


def _is_catalog_batch_full(
    context: _LegacyCatalogContext,
    accumulator: _LegacyCatalogAccumulator,
    limits: LegacySweepLimits,
) -> bool:
    provisional_plan = build_bounded_legacy_sweep_plan(
        schema_name=context.schema_name,
        control_schema_name=context.control_schema_name,
        authority_digest=context.authority_digest,
        catalog_digest="0" * 64,
        eligible_candidates=accumulator.candidates,
        blocked=accumulator.blocked_suffixes,
        limits=limits,
        catalog_progress=accumulator.progress(
            len(context.inventory.root_suffixes)
        ),
    )
    return (
        len(provisional_plan.candidates) >= limits.max_suffixes
        or provisional_plan.table_count >= limits.max_tables
        or provisional_plan.relation_count >= limits.max_relations
        or provisional_plan.total_bytes >= limits.max_bytes
    )


def _oversized_suffix_block(
    oversized: LegacyOversizedCatalogSuffix,
) -> LegacyBlockedSuffix:
    return LegacyBlockedSuffix(
        suffix=oversized.suffix,
        reasons=("catalog_window_relation_ceiling_exceeded",),
        table_count=oversized.root_table_count,
        total_bytes=0,
    )


def _oversized_suffix_digest(
    oversized: LegacyOversizedCatalogSuffix,
) -> str:
    return canonical_sha256(
        {
            "contract": "ptg2_legacy_orphan_oversized_catalog_suffix_v1",
            "suffix": oversized.suffix,
            "relation_count": oversized.relation_count,
            "root_table_count": oversized.root_table_count,
        }
    )


async def _collect_catalog_windows(
    executor: Any,
    *,
    context: _LegacyCatalogContext,
    limits: LegacySweepLimits,
) -> _LegacyCatalogAccumulator:
    accumulator = _LegacyCatalogAccumulator()
    if any(
        limit == 0
        for limit in (
            limits.max_suffixes,
            limits.max_tables,
            limits.max_relations,
            limits.max_bytes,
        )
    ):
        return accumulator
    for segment in bounded_catalog_segments(context.inventory):
        if isinstance(segment, LegacyOversizedCatalogSuffix):
            accumulator.blocked_suffixes.append(
                _oversized_suffix_block(segment)
            )
            accumulator.window_digests.append(
                _oversized_suffix_digest(segment)
            )
            accumulator.scanned_suffix_count += 1
            continue
        window_candidates, window_blocked, window_digest = (
            await _classify_catalog_window(
                executor,
                context=context,
                suffix_window=segment.suffixes,
                relation_keys=segment.relation_keys,
            )
        )
        accumulator.candidates.extend(window_candidates)
        accumulator.blocked_suffixes.extend(window_blocked)
        accumulator.window_digests.append(window_digest)
        accumulator.scanned_suffix_count += len(segment.suffixes)
        if _is_catalog_batch_full(context, accumulator, limits):
            break
    return accumulator


async def scan_legacy_catalog(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    authority: Any,
    limits: LegacySweepLimits,
) -> LegacyCatalogScan:
    """Scan indexed windows until the requested cleanup batch is full."""

    catalog_inventory = await load_legacy_catalog_inventory(
        executor,
        schema_name=schema_name,
    )
    context = _LegacyCatalogContext(
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        authority_digest=authority.catalog_digest,
        present_optional_tables=frozenset(
            authority.present_optional_table_names
        ),
        inventory=catalog_inventory,
    )
    accumulator = await _collect_catalog_windows(
        executor,
        context=context,
        limits=limits,
    )
    suffix_count = len(catalog_inventory.root_suffixes)
    return LegacyCatalogScan(
        catalog_digest=_catalog_window_digest(
            inventory_digest=catalog_inventory.catalog_digest,
            inventory_suffix_count=suffix_count,
            scanned_suffix_count=accumulator.scanned_suffix_count,
            window_digests=accumulator.window_digests,
        ),
        candidates=tuple(accumulator.candidates),
        blocked_suffixes=tuple(accumulator.blocked_suffixes),
        progress=accumulator.progress(suffix_count),
    )
