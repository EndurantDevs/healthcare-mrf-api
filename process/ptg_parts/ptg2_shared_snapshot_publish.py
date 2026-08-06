# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""End-to-end physical publication for strict shared-block PTG V3."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import shutil
import stat
import struct
import sys
import tempfile
import time
import uuid
from contextlib import asynccontextmanager, contextmanager, nullcontext, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from db.connection import db
from api.ptg2_code_filters import INFERRED_PROVIDER_TAXONOMY_RULES
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_COLD_LOOKUP_CONTRACT,
    PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
    PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    PTG2_V3_SHARED_BLOCK_LAYOUT,
    PTG2_V3_SHARED_GENERATION,
    SharedBlockReference,
    SharedLayoutBuildOwnership,
    SharedMappingDigestSummary,
    seal_shared_layout,
    shared_support_digest,
    summarize_shared_snapshot_mappings,
    touch_shared_layout_build,
)
from process.ptg_parts.ptg2_shared_audit import (
    publish_shared_audit_sample,
    sealed_audit_sample_metadata,
)
from process.ptg_parts.ptg2_shared_finalize import (
    PTG2_V3_DURABLE_SCRATCH_DURABILITY,
    PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY,
    observe_v3_finalizer_progress,
    run_v3_direct_finalizer,
)
from process.ptg_parts.ptg2_shared_graph import SharedGraphConversionResult
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    lock_writable_snapshot,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.rust_scanner import (
    convert_membership_shards_to_shared_graph_rust,
)
from process.ptg_parts.ptg2_shared_price import (
    PTG2_V3_PRICE_KEY_ORDER,
    PreparedSharedPriceArtifacts,
    PreparedSharedPriceKeyMap,
    _await_cleanup_task,
    cleanup_prepared_shared_price_artifacts,
    export_shared_price_key_map,
    observe_shared_price_progress,
    prepare_shared_price_artifacts,
    publish_shared_price_artifacts,
)
from process.ptg_parts.ptg2_shared_publish import (
    SharedBlockCopyMetrics,
    V4CASBlockStagePublication,
    _validated_coverage_scope_id,
    copy_shared_block_binary_file,
    create_shared_block_stage,
    _copy_binary_file_to_stage,
    publish_shared_block_stage,
    publish_shared_finalizer_dictionaries,
    publish_shared_graph,
    _publish_v4_cas_in_session,
    shared_block_stage_name,
    shared_graph_bundles_from_artifacts,
)
from process.ptg_parts.ptg2_shared_reuse import (
    SharedLogicalPlanScope,
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    deterministic_source_key_assignments,
    normalized_full_rebuild_scope_digest,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_source_witness_store import publish_shared_source_witness
from process.ptg_parts.ptg2_v4_graph_compiler import (
    V4GraphCompilationResult,
    V4GraphNpiScopePreparation,
    compile_provider_graph_v4_rust,
    prepare_provider_graph_v4_npi_scope_rust,
    v4_adaptive_layout_decision_from_summary,
)
from process.ptg_parts.ptg2_v4_audit import publish_v4_audit_sample
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
    PTG2_V4_GRAPH_RESOURCE_FIELDS,
    PTG2_V4_NPI_TABLE,
    PTG2_V4_SHARED_GENERATION,
    V4SnapshotMapSummary,
    lock_v4_shared_layout_for_map_write,
    publish_v4_heavy_owners,
    publish_v4_relation_manifests,
    publish_v4_snapshot_maps,
    seal_v4_shared_layout,
    touch_v4_shared_layout_build,
)
from process.ptg_parts.ptg2_v4_taxonomy_candidates import (
    V4InferredTaxonomyPublication,
    publish_prepared_v4_inferred_taxonomy_candidates,
    stage_v4_inferred_taxonomy_compiler_copy,
)
from process.ptg_parts.ptg2_tax_identity_source_artifact import (
    prepare_tax_identity_source_projection,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourcePublication,
)
from process.ptg_parts.ptg2_tax_identity_source_publish import (
    publish_staged_tax_identity_source_projection,
)
from process.ptg_parts.ptg2_tax_identity_source_stage import (
    stage_tax_identity_source_projection,
)


_REQUIRED_OBJECT_KINDS = frozenset(
    {
        "by_code_provider_shard_v1",
        "by_code_price_page_v4",
        "by_code_price_dictionary",
        "provider_set_count_dictionary",
        "provider_set_codes_v3",
        "provider_set_page_v3_s2",
        "price_set_atom_memberships_v3",
        "price_atoms_v3",
        "graph_npi_groups_v1",
        "graph_group_npis_v1",
        "graph_group_provider_sets_v1",
        "graph_provider_set_groups_v1",
    }
)
_REQUIRED_PRICE_OBJECT_KINDS = _REQUIRED_OBJECT_KINDS - {
    "graph_npi_groups_v1",
    "graph_group_npis_v1",
    "graph_group_provider_sets_v1",
    "graph_provider_set_groups_v1",
}
_V4_DICTIONARY_PUBLICATION_BATCH_CONTRACT = "ptg2_v4_dictionary_publication_adaptive_v1"
_V4_DICTIONARY_DEFAULT_RANGE_ROWS = 100_000
_V4_DICTIONARY_FALLBACK_RANGE_ROWS = 10_000
_V4_DICTIONARY_MAX_ESTIMATED_ROW_WORK_BYTES = 16 * 1024 * 1024
_V4_DICTIONARY_FIXED_WORK_OVERHEAD_BYTES = 64 * 1024
_V4_DICTIONARY_ESTIMATED_ROW_BYTES = 160
_V4_DICTIONARY_SLOW_STATEMENT_SECONDS = 4.0
_V4_DICTIONARY_RECOVERY_STATEMENT_SECONDS = 2.0
_V4_DICTIONARY_HEARTBEAT_SECONDS = 4.0
_V4_TAX_IDENTITY_MANIFEST_CONTRACT = "ptg2_provider_group_tax_identity_v1"
_V4_TAX_IDENTITY_PROJECTION_CONTRACT = "ptg2_provider_tax_identity_projection_v1"
_V4_TAX_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_V4_TAX_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_V4_TAX_CANDIDATE_PREFIX_CONTRACT = "tin_id_128=first_16_bytes(tin_hmac_sha256)"
_V4_TAX_AUTHORITY_CONTRACT = "tin_hmac_sha256_full_32_bytes_authoritative"
_V4_TAX_SOURCE_ORDINAL_CONTRACT = "snapshot_shard_id_sorted_lsb0_bitmap_v1"
_V4_TAX_POLICY_DESCRIPTOR_DOMAIN = b"PTG2V4TINPOLICY\x01"
_V4_TAX_SOURCE_ORDINAL_DOMAIN = b"PTG2V4TAXORD\x01"
_V4_TAX_CONTENT_DOMAIN = b"PTG2V4TAXCONTENT\x01"
_V4_TAX_POLICY_ID = re.compile(
    r"ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})\Z"
)
_V4_TAX_STATE_CODE = {
    "matched_ein": 1,
    "missing": 2,
    "malformed": 3,
    "unsupported_type": 4,
}
_V4_TAX_SUMMARY_FIELDS = frozenset(
    {
        "contract",
        "token_policy_id",
        "token_policy_descriptor_sha256",
        "normalization_contract",
        "hmac_contract",
        "candidate_prefix_contract",
        "authority_contract",
        "source_ordinal_contract",
        "source_ordinal_map",
        "source_ordinal_map_digest",
        "source_shard_count",
        "source_bitmap_bytes",
        "provider_group_count",
        "tax_identity_count",
        "matched_ein_count",
        "missing_count",
        "malformed_count",
        "unsupported_type_count",
        "content_digest",
    }
)


@dataclass(frozen=True)
class SharedSnapshotPublication:
    snapshot_key: int
    serving_index: Mapping[str, Any]
    object_kinds: tuple[str, ...]
    mapping_count: int
    unique_block_count: int
    mapping_digest: bytes
    finalizer_summary: Mapping[str, Any]
    layout_reused_at_seal: bool
    stored_byte_count: int


@dataclass(frozen=True)
class _PreparedFinalizer:
    """Finalizer output produced while independent price stages are still running."""

    summary: Mapping[str, Any]
    price_key_map_export_seconds: float
    finalizer_seconds: float
    overlap_wall_seconds: float


@dataclass(frozen=True)
class _PreparedPricePublication:
    """Price blocks published while the Rust finalizer is still running."""

    publication: Any
    publish_seconds: float


@dataclass(frozen=True)
class _FinalizerBlockPublicationResult:
    """Keep durable publication and immutable COPY proof together."""

    publication: Any
    serving_copy: SharedBlockCopyMetrics
    price_dictionary_copy: SharedBlockCopyMetrics

    def copy_manifest(self) -> dict[str, Any]:
        """Return per-lane and total selective-staging proof."""

        total = SharedBlockCopyMetrics.combine(
            self.serving_copy,
            self.price_dictionary_copy,
        )
        return {
            "contract": "selective_shared_block_copy_v1",
            "serving": self.serving_copy.as_dict(),
            "price_dictionary": self.price_dictionary_copy.as_dict(),
            "total": total.as_dict(),
        }


@dataclass(frozen=True)
class _V4DenseDictionaryStage:
    """One V4 stage with bounded validation and publication metadata."""

    stage_table: str
    key_name: str
    expected_count: int
    target_table: str
    columns: tuple[str, ...]
    value_predicate: str
    sum_expression: str = "0"
    expected_sum: int | None = None
    dense_keys: bool = True
    estimated_row_bytes: int = _V4_DICTIONARY_ESTIMATED_ROW_BYTES


@dataclass(frozen=True)
class _V4DictionaryPublicationBatchContract:
    """Authenticated runtime contract for bounded V4 dictionary row work."""

    contract: str = _V4_DICTIONARY_PUBLICATION_BATCH_CONTRACT
    default_range_rows: int = _V4_DICTIONARY_DEFAULT_RANGE_ROWS
    fallback_range_rows: int = _V4_DICTIONARY_FALLBACK_RANGE_ROWS
    max_estimated_row_work_bytes: int = _V4_DICTIONARY_MAX_ESTIMATED_ROW_WORK_BYTES
    fixed_work_overhead_bytes: int = _V4_DICTIONARY_FIXED_WORK_OVERHEAD_BYTES
    estimated_row_bytes: int = _V4_DICTIONARY_ESTIMATED_ROW_BYTES
    slow_statement_millis: int = int(_V4_DICTIONARY_SLOW_STATEMENT_SECONDS * 1_000)
    recovery_statement_millis: int = int(
        _V4_DICTIONARY_RECOVERY_STATEMENT_SECONDS * 1_000
    )
    heartbeat_millis: int = int(_V4_DICTIONARY_HEARTBEAT_SECONDS * 1_000)

    def as_dict(self) -> dict[str, int | str]:
        """Return the exact manifest-safe publication policy."""

        return {
            "contract": self.contract,
            "default_range_rows": self.default_range_rows,
            "fallback_range_rows": self.fallback_range_rows,
            "max_estimated_row_work_bytes": self.max_estimated_row_work_bytes,
            "fixed_work_overhead_bytes": self.fixed_work_overhead_bytes,
            "estimated_row_bytes": self.estimated_row_bytes,
            "slow_statement_millis": self.slow_statement_millis,
            "recovery_statement_millis": self.recovery_statement_millis,
            "heartbeat_millis": self.heartbeat_millis,
        }


_V4_DICTIONARY_BATCH_CONTRACT = _V4DictionaryPublicationBatchContract()


class _V4DictionaryBatchSizer:
    """Adapt row ranges without weakening the estimated row-work ceiling."""

    def __init__(self, *, estimated_row_bytes: int) -> None:
        normalized_row_bytes = int(estimated_row_bytes)
        if normalized_row_bytes <= 0:
            raise ValueError("PTG V4 dictionary row estimate must be positive")
        payload_budget = (
            _V4_DICTIONARY_MAX_ESTIMATED_ROW_WORK_BYTES
            - _V4_DICTIONARY_FIXED_WORK_OVERHEAD_BYTES
        )
        if payload_budget <= 0:
            raise RuntimeError("PTG V4 dictionary row-work budget is invalid")
        byte_limited_rows = max(1, payload_budget // normalized_row_bytes)
        self.maximum_rows = min(
            _V4_DICTIONARY_DEFAULT_RANGE_ROWS,
            byte_limited_rows,
        )
        self.fallback_rows = min(
            _V4_DICTIONARY_FALLBACK_RANGE_ROWS,
            self.maximum_rows,
        )
        self.current_rows = self.maximum_rows

    def observe(self, elapsed_seconds: float) -> None:
        """Shrink slow ranges and recover fast ranges geometrically."""

        normalized_elapsed = max(float(elapsed_seconds), 0.0)
        if normalized_elapsed >= _V4_DICTIONARY_SLOW_STATEMENT_SECONDS:
            self.current_rows = max(
                self.fallback_rows,
                self.current_rows // 2,
            )
        elif (
            normalized_elapsed <= _V4_DICTIONARY_RECOVERY_STATEMENT_SECONDS
            and self.current_rows < self.maximum_rows
        ):
            self.current_rows = min(
                self.maximum_rows,
                self.current_rows * 2,
            )


@dataclass(frozen=True)
class _V4TaxIdentityContract:
    """Independently authenticated tax-identity publication contract."""

    token_policy_id: str
    token_policy_descriptor_sha256: bytes
    source_ordinal_map: tuple[Mapping[str, Any], ...]
    source_ordinal_map_digest: bytes
    source_shard_count: int
    source_bitmap_bytes: int
    provider_group_count: int
    tax_identity_count: int
    matched_ein_count: int
    missing_count: int
    malformed_count: int
    unsupported_type_count: int
    content_digest: bytes


@dataclass(frozen=True)
class _V4TaxIdentityPublication:
    """Exact manifest and relational storage accounted beneath one V4 root."""

    manifest: Mapping[str, Any]
    provider_group_count: int
    tax_identity_count: int
    artifact_byte_count: int


class _MeasuredPublicationProgress:
    """Coalesce exact lane counters into live movement at most every four seconds."""

    def __init__(
        self,
        stage: str,
        callback: Callable[[str, Mapping[str, int]], None] | None,
        *,
        interval_seconds: float = 4.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.stage = stage
        self._callback = callback
        self._interval_seconds = max(float(interval_seconds), 0.01)
        self._clock = clock
        self._totals: dict[str, int] = {}
        self._last_emitted: dict[str, int] = {}
        self._next_emit_at = self._clock() + self._interval_seconds

    def add(self, metric: str, amount: int) -> None:
        """Record one exact positive work delta and emit on the cadence."""

        normalized_amount = int(amount)
        if normalized_amount <= 0:
            return
        metric_name = str(metric or "").strip()
        if not metric_name:
            raise ValueError("publication progress metric must be non-empty")
        self._totals[metric_name] = self._totals.get(metric_name, 0) + normalized_amount
        now = self._clock()
        if now >= self._next_emit_at:
            self.flush(now=now)

    def flush(self, *, now: float | None = None) -> None:
        """Publish changed exact counters, including the final partial interval."""

        if self._callback is None or self._totals == self._last_emitted:
            return
        observed_at = self._clock() if now is None else float(now)
        self._callback(self.stage, dict(self._totals))
        self._last_emitted = dict(self._totals)
        self._next_emit_at = observed_at + self._interval_seconds

    def heartbeat(self) -> None:
        """Re-emit only completed counters while one bounded query is active."""

        if self._callback is None or not self._totals:
            return
        observed_at = self._clock()
        if observed_at < self._next_emit_at:
            return
        self._callback(self.stage, dict(self._totals))
        self._last_emitted = dict(self._totals)
        self._next_emit_at = observed_at + self._interval_seconds


def _progress_callback_kwargs(
    progress_callback: Callable[..., None] | None,
) -> dict[str, Callable[..., None]]:
    """Return an optional progress keyword without perturbing the V3 call shape."""

    return (
        {"progress_callback": progress_callback}
        if progress_callback is not None
        else {}
    )


@dataclass(frozen=True)
class _V4GraphPublication:
    """Published V4 graph CAS, packed maps, and relational dictionaries."""

    object_kinds: tuple[str, ...]
    mapping_count: int
    unique_block_count: int
    block_count: int
    owner_count: int
    provider_group_count: int
    npi_count: int
    support_digest: bytes
    logical_byte_count: int
    stored_byte_count: int
    map_summary: V4SnapshotMapSummary
    representation: str
    adaptive_layout: Mapping[str, Any]
    compiler_summary: Mapping[str, Any]
    dictionary_publication: Mapping[str, Any]
    inferred_taxonomy_candidates: Mapping[str, Any]
    provider_tax_identity: Mapping[str, Any]
    provider_tax_identity_source: Mapping[str, Any]
    audit_witness_path: Path


@dataclass(frozen=True)
class _V4AtomicPublishContext:
    """Immutable database coordinates for one atomic V4 publication."""

    schema_name: str
    block_stage: str
    logical_snapshot_id: str
    snapshot_key: int
    build_token: str


@dataclass(frozen=True)
class _V4GraphCoordinates:
    """Immutable layout coordinates used before the graph stage exists."""

    schema_name: str
    logical_snapshot_id: str
    snapshot_key: int
    build_token: str


def _completed_prepared_price(
    prepare_task: asyncio.Task[tuple[PreparedSharedPriceArtifacts, float]],
) -> PreparedSharedPriceArtifacts | None:
    """Return a successful completed preparation without masking its caller's error."""

    if not prepare_task.done() or prepare_task.cancelled():
        return None
    try:
        return prepare_task.result()[0]
    except BaseException:
        return None


def _validate_authoritative_mapping_summary(
    summary: SharedMappingDigestSummary,
    *lane_publications: Any,
) -> None:
    """Cross-check bounded lane aggregates against the authoritative mapping set."""

    lane_kinds: list[str] = []
    expected_mapping_count = 0
    expected_unique_block_count = 0
    expected_logical_byte_count = 0
    for publication in lane_publications:
        publication_kinds = tuple(publication.object_kinds)
        if publication_kinds != tuple(sorted(set(publication_kinds))):
            raise RuntimeError(
                "strict V3 publication lane returned invalid object kinds"
            )
        duplicate_kinds = set(lane_kinds).intersection(publication_kinds)
        if duplicate_kinds:
            raise RuntimeError(
                "strict V3 publication lanes overlap object kinds: "
                f"{sorted(duplicate_kinds)}"
            )
        lane_kinds.extend(publication_kinds)
        expected_mapping_count += int(publication.mapping_count)
        expected_unique_block_count += int(publication.unique_block_count)
        expected_logical_byte_count += int(publication.logical_byte_count)

    expected_kinds = tuple(sorted(lane_kinds))
    expected_by_field = {
        "object_kinds": expected_kinds,
        "mapping_count": expected_mapping_count,
        "unique_block_count": expected_unique_block_count,
        "logical_byte_count": expected_logical_byte_count,
    }
    for field_name, expected_value in expected_by_field.items():
        observed_value = getattr(summary, field_name)
        if observed_value != expected_value:
            raise RuntimeError(
                "strict V3 authoritative mapping summary disagrees with publication "
                f"lanes for {field_name}: expected {expected_value!r}, "
                f"observed {observed_value!r}"
            )


async def _run_independent_publication_lanes(
    *,
    finalizer_blocks: Callable[[], Awaitable[Any]],
    provider_graph: Callable[[], Awaitable[Any]],
    price: Callable[[], Awaitable[Any]],
    source_witness: Callable[[], Awaitable[Any]],
) -> tuple[Any, Any, Any, Any]:
    """Run independent durable outputs concurrently and fail as one unit."""

    async with asyncio.TaskGroup() as task_group:
        finalizer_block_task = task_group.create_task(finalizer_blocks())
        provider_graph_task = task_group.create_task(provider_graph())
        price_task = task_group.create_task(price())
        source_witness_task = task_group.create_task(source_witness())
    return (
        finalizer_block_task.result(),
        provider_graph_task.result(),
        price_task.result(),
        source_witness_task.result(),
    )


async def _export_price_map_and_run_finalizer(
    *,
    prepared_price_key: PreparedSharedPriceKeyMap,
    raw_work_directory: str | Path,
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    progress_callback: Callable[[str, int], None] | None = None,
) -> _PreparedFinalizer:
    """Export and finalize as soon as the independent price-key map is ready."""

    overlap_started_at = time.monotonic()
    stage_started_at = time.monotonic()
    price_key_map_path = await export_shared_price_key_map(
        prepared_price_key,
        Path(raw_work_directory) / "price-key-map.copy",
    )
    price_key_map_export_seconds = time.monotonic() - stage_started_at
    stage_started_at = time.monotonic()
    with observe_v3_finalizer_progress(progress_callback):
        finalizer_summary = await run_v3_direct_finalizer(
            work_directory=raw_work_directory,
            serving_run_entries=serving_run_entries,
            code_dictionary_entries=code_dictionary_entries,
            provider_set_metadata_entries=provider_set_metadata_entries,
            expected_source_identities=expected_source_identities,
            price_key_map_input=price_key_map_path,
            price_key_map_row_count=prepared_price_key.price_set_count,
            scratch_durability=PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY,
        )
    return _PreparedFinalizer(
        summary=dict(finalizer_summary),
        price_key_map_export_seconds=price_key_map_export_seconds,
        finalizer_seconds=time.monotonic() - stage_started_at,
        overlap_wall_seconds=time.monotonic() - overlap_started_at,
    )


async def _prepare_price_with_early_finalizer(
    *,
    schema_name: str,
    manifest_stage_table: str,
    price_set_summary_source_count: int | None,
    raw_work_directory: str | Path,
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    publish_prepared_price: (
        Callable[[PreparedSharedPriceArtifacts], Awaitable[Any]] | None
    ) = None,
    finalizer_progress_callback: Callable[[str, int], None] | None = None,
) -> tuple[
    PreparedSharedPriceArtifacts,
    float,
    _PreparedFinalizer | None,
    _PreparedPricePublication | None,
]:
    """Prepare price stages and overlap the finalizer when early readiness exists."""

    loop = asyncio.get_running_loop()
    price_key_ready: asyncio.Future[PreparedSharedPriceKeyMap] = loop.create_future()

    def notify_price_key_ready(prepared_key: PreparedSharedPriceKeyMap) -> None:
        """Announce the single validated price-key map to the finalizer lane."""

        if price_key_ready.done():
            raise RuntimeError("strict V3 price-key stage reported readiness twice")
        price_key_ready.set_result(prepared_key)

    price_prepare_started_at = time.monotonic()

    async def prepare_price() -> tuple[PreparedSharedPriceArtifacts, float]:
        """Prepare price artifacts and retain their measured wall time."""

        prepared = await prepare_shared_price_artifacts(
            schema_name=schema_name,
            manifest_stage_table=manifest_stage_table,
            price_set_summary_source_count=price_set_summary_source_count,
            price_key_ready=notify_price_key_ready,
        )
        return prepared, time.monotonic() - price_prepare_started_at

    prepare_task = asyncio.create_task(prepare_price())
    try:
        completed, _pending = await asyncio.wait(
            (prepare_task, price_key_ready),
            return_when=asyncio.FIRST_COMPLETED,
        )
    except BaseException:
        prepare_task.cancel()
        await _await_cleanup_task(asyncio.gather(prepare_task, return_exceptions=True))
        prepared_after_cancellation = _completed_prepared_price(prepare_task)
        if prepared_after_cancellation is not None:
            await _await_cleanup_task(
                asyncio.create_task(
                    cleanup_prepared_shared_price_artifacts(prepared_after_cancellation)
                )
            )
        if not price_key_ready.done():
            price_key_ready.cancel()
        raise
    if price_key_ready not in completed:
        prepared, price_prepare_seconds = await prepare_task
        prepared_price_publication = None
        if publish_prepared_price is not None:
            price_publish_started_at = time.monotonic()
            prepared_price_publication = _PreparedPricePublication(
                publication=await publish_prepared_price(prepared),
                publish_seconds=time.monotonic() - price_publish_started_at,
            )
        return prepared, price_prepare_seconds, None, prepared_price_publication

    finalizer_task = asyncio.create_task(
        _export_price_map_and_run_finalizer(
            prepared_price_key=price_key_ready.result(),
            raw_work_directory=raw_work_directory,
            serving_run_entries=serving_run_entries,
            code_dictionary_entries=code_dictionary_entries,
            provider_set_metadata_entries=provider_set_metadata_entries,
            expected_source_identities=expected_source_identities,
            progress_callback=finalizer_progress_callback,
        )
    )
    early_price_task: asyncio.Task[_PreparedPricePublication] | None = None
    if publish_prepared_price is not None:

        async def publish_after_price_preparation() -> _PreparedPricePublication:
            """Publish prepared price blocks while the finalizer remains active."""

            prepared, _price_prepare_seconds = await prepare_task
            price_publish_started_at = time.monotonic()
            return _PreparedPricePublication(
                publication=await publish_prepared_price(prepared),
                publish_seconds=time.monotonic() - price_publish_started_at,
            )

        early_price_task = asyncio.create_task(publish_after_price_preparation())
    try:
        gathered = await asyncio.gather(
            prepare_task,
            finalizer_task,
            *([early_price_task] if early_price_task is not None else []),
        )
    except BaseException:
        active_tasks = tuple(
            task
            for task in (prepare_task, finalizer_task, early_price_task)
            if task is not None
        )
        for task in active_tasks:
            task.cancel()
        await _await_cleanup_task(asyncio.gather(*active_tasks, return_exceptions=True))
        prepared_after_failure = _completed_prepared_price(prepare_task)
        if prepared_after_failure is not None:
            await _await_cleanup_task(
                asyncio.create_task(
                    cleanup_prepared_shared_price_artifacts(prepared_after_failure)
                )
            )
        raise
    prepare_result = gathered[0]
    prepared_finalizer = gathered[1]
    prepared_price_publication = gathered[2] if early_price_task is not None else None
    prepared, price_prepare_seconds = prepare_result
    return (
        prepared,
        price_prepare_seconds,
        prepared_finalizer,
        prepared_price_publication,
    )


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _snapshot_source_rows(
    *,
    snapshot_id: str,
    assignments: Iterable[SharedSnapshotSourceAssignment],
) -> list[dict[str, Any]]:
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id or len(normalized_snapshot_id) > 96:
        raise ValueError("strict V3 source publication requires a valid snapshot_id")
    source_records = [
        {
            "snapshot_id": normalized_snapshot_id,
            "source_key": int(assignment.source_key),
            **assignment.identity.as_dict(),
            "raw_container_sha256": assignment.raw_container_sha256,
            "logical_json_sha256": assignment.logical_json_sha256,
            "logical_hash_deferred": assignment.logical_hash_deferred,
            "source_trace_set_hash": str(assignment.source_trace_set_hash),
        }
        for assignment in assignments
    ]
    if not source_records or [
        source_record["source_key"] for source_record in source_records
    ] != list(range(len(source_records))):
        raise ValueError("strict V3 snapshot source keys must be complete and dense")
    expected_dense = deterministic_source_key_assignments(
        {
            field_name: source_record[field_name]
            for field_name in ("source_type", "identity_kind", "identity_sha256")
        }
        for source_record in source_records
    )
    if any(
        source_key != source_record["source_key"]
        or identity.as_dict()
        != {
            field_name: source_record[field_name]
            for field_name in ("source_type", "identity_kind", "identity_sha256")
        }
        for source_record, (source_key, identity) in zip(source_records, expected_dense)
    ):
        raise ValueError(
            "strict V3 snapshot source keys do not match physical artifact ordinals"
        )
    return source_records


def _logical_snapshot_source_set_update_sql(schema: str) -> str:
    """Build the guarded logical source-set update without touching layouts."""

    return f"""
        WITH expected_source_set AS (
            SELECT jsonb_build_object(
                'contract', CAST(:source_set_contract AS text),
                'source_count', CAST(:source_set_count AS integer),
                'raw_container_sha256_digest', CAST(:source_set_digest AS text)
            ) AS value
        )
        UPDATE {schema}.ptg2_snapshot AS snapshot
           SET manifest = CAST(
               jsonb_set(
                   COALESCE(snapshot.manifest::jsonb, '{{}}'::jsonb),
                   '{{serving_index}}',
                   COALESCE(
                       snapshot.manifest::jsonb->'serving_index',
                       '{{}}'::jsonb
                   ) || jsonb_build_object(
                       'source_set', expected_source_set.value
                   ),
                   true
               ) AS json
           )
          FROM expected_source_set
         WHERE snapshot.snapshot_id = :snapshot_id
           AND snapshot.status = 'building'
           AND jsonb_typeof(COALESCE(
               snapshot.manifest::jsonb->'serving_index',
               '{{}}'::jsonb
           )) = 'object'
           AND (
               snapshot.manifest::jsonb #> '{{serving_index,source_set}}' IS NULL
               OR snapshot.manifest::jsonb #> '{{serving_index,source_set}}'
                   = expected_source_set.value
           )
        RETURNING snapshot.manifest::jsonb
            #> '{{serving_index,source_set}}' AS snapshot_source_set
    """


async def _persist_logical_snapshot_source_set(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
    source_set_by_field: Mapping[str, Any],
) -> None:
    """Seal one logical source set without changing reusable physical metadata."""

    update_result = await session.execute(
        db.text(_logical_snapshot_source_set_update_sql(schema)),
        {
            "snapshot_id": str(snapshot_id),
            "source_set_contract": source_set_by_field["contract"],
            "source_set_count": source_set_by_field["source_count"],
            "source_set_digest": source_set_by_field["raw_container_sha256_digest"],
        },
    )
    updated_row = update_result.first()
    persisted_source_set = (
        _row_mapping(updated_row).get("snapshot_source_set")
        if updated_row is not None
        else None
    )
    if persisted_source_set != dict(source_set_by_field):
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} has a conflicting logical source-set seal"
        )


async def publish_shared_v3_snapshot_sources(
    *,
    schema_name: str,
    snapshot_id: str,
    plan_scopes: Iterable[SharedLogicalPlanScope],
    coverage_scope_id: bytes,
    assignments: Iterable[SharedSnapshotSourceAssignment],
) -> tuple[dict[str, Any], ...]:
    """Publish immutable source and logical-plan mappings for one snapshot."""

    source_records = _snapshot_source_rows(
        snapshot_id=snapshot_id,
        assignments=assignments,
    )
    source_set_by_field = shared_source_set_metadata(
        source_record["raw_container_sha256"] for source_record in source_records
    )
    scope_id = _validated_coverage_scope_id(coverage_scope_id)
    normalized_plan_scopes = tuple(
        sorted(
            {
                SharedLogicalPlanScope(
                    plan_id=str(scope.plan_id or "").strip(),
                    plan_id_type=str(scope.plan_id_type or "").strip().lower(),
                    plan_market_type=str(scope.plan_market_type or "").strip().lower(),
                )
                for scope in plan_scopes
                if str(scope.plan_id or "").strip()
            }
        )
    )
    if not normalized_plan_scopes:
        raise ValueError("strict V3 source publication requires logical plans")
    primary_plan = normalized_plan_scopes[0]
    schema = _quote_ident(schema_name)
    async with db.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        await lock_writable_snapshot(
            session,
            db,
            schema_name=schema_name,
            snapshot_id=str(snapshot_id),
        )
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_scope
                    (snapshot_id, plan_id, plan_market_type, coverage_scope_id)
                VALUES
                    (:snapshot_id, :plan_id, :plan_market_type, :coverage_scope_id)
                ON CONFLICT (snapshot_id) DO NOTHING
                """
            ),
            {
                "snapshot_id": str(snapshot_id),
                "plan_id": primary_plan.plan_id,
                "plan_market_type": primary_plan.plan_market_type,
                "coverage_scope_id": scope_id,
            },
        )
        scope_result = await session.execute(
            db.text(
                f"""
                SELECT plan_id, plan_market_type, coverage_scope_id
                  FROM {schema}.ptg2_v3_snapshot_scope
                 WHERE snapshot_id = :snapshot_id
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        scope_row = scope_result.first()
        scope = _row_mapping(scope_row)
        if (
            str(scope.get("plan_id") or "") != primary_plan.plan_id
            or str(scope.get("plan_market_type") or "") != primary_plan.plan_market_type
            or bytes(scope.get("coverage_scope_id") or b"") != scope_id
        ):
            raise RuntimeError(
                f"PTG snapshot {snapshot_id} already has another immutable source scope"
            )
        plan_scope_records = [
            {
                "snapshot_id": str(snapshot_id),
                "plan_id": logical_plan.plan_id,
                "plan_market_type": logical_plan.plan_market_type,
            }
            for logical_plan in normalized_plan_scopes
        ]
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope
                    (snapshot_id, plan_id, plan_market_type)
                VALUES
                    (:snapshot_id, :plan_id, :plan_market_type)
                ON CONFLICT (snapshot_id, plan_id, plan_market_type) DO NOTHING
                """
            ),
            plan_scope_records,
        )
        observed_plan_result = await session.execute(
            db.text(
                f"""
                SELECT plan_id, plan_market_type
                  FROM {schema}.ptg2_v3_snapshot_plan_scope
                 WHERE snapshot_id = :snapshot_id
                 ORDER BY plan_id, plan_market_type
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        observed_plans = {
            (
                str(_row_mapping(plan_scope_row).get("plan_id") or ""),
                str(_row_mapping(plan_scope_row).get("plan_market_type") or ""),
            )
            for plan_scope_row in observed_plan_result
        }
        expected_plans = {
            (logical_plan.plan_id, logical_plan.plan_market_type)
            for logical_plan in normalized_plan_scopes
        }
        if observed_plans != expected_plans:
            raise RuntimeError(
                f"PTG snapshot {snapshot_id} has stale logical plan mappings"
            )
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_source
                    (snapshot_id, source_key, source_type, identity_kind,
                     identity_sha256, raw_container_sha256, logical_json_sha256,
                     logical_hash_deferred, source_trace_set_hash)
                VALUES
                    (:snapshot_id, :source_key, :source_type, :identity_kind,
                     :identity_sha256, :raw_container_sha256, :logical_json_sha256,
                     :logical_hash_deferred, :source_trace_set_hash)
                ON CONFLICT (snapshot_id, source_key) DO NOTHING
                """
            ),
            source_records,
        )
        observed_result = await session.execute(
            db.text(
                f"""
                SELECT snapshot_id, source_key, source_type, identity_kind,
                       identity_sha256, raw_container_sha256, logical_json_sha256,
                       logical_hash_deferred, source_trace_set_hash
                  FROM {schema}.ptg2_v3_snapshot_source
                 WHERE snapshot_id = :snapshot_id
                 ORDER BY source_key
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        observed_source_records = [
            _row_mapping(source_record) for source_record in observed_result
        ]
        if observed_source_records != source_records:
            raise RuntimeError(
                f"PTG snapshot {snapshot_id} already has a conflicting source-key mapping"
            )
        await _persist_logical_snapshot_source_set(
            session,
            schema=schema,
            snapshot_id=str(snapshot_id),
            source_set_by_field=source_set_by_field,
        )
    return tuple(source_records)


async def _delete_unbound_snapshot_source_metadata(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
) -> None:
    """Delete the two logical-source relations while no binding exists."""

    for table_name in (
        "ptg2_v3_snapshot_source",
        "ptg2_v3_snapshot_scope",
    ):
        await session.execute(
            db.text(
                f"""
                DELETE FROM {schema}.{table_name} AS owned
                 WHERE owned.snapshot_id = :snapshot_id
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_v3_snapshot_binding AS binding
                         WHERE binding.snapshot_id = owned.snapshot_id
                   )
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )


async def delete_unpublished_snapshot_sources(
    *,
    schema_name: str,
    snapshot_id: str,
    internal_run_id: str | None = None,
) -> None:
    """Remove failed logical metadata without releasing or changing shared layouts."""

    schema = _quote_ident(schema_name)
    async with db.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        await lock_writable_snapshot(
            session,
            db,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
        )
        snapshot_result = await session.execute(
            db.text(
                f"""
                SELECT snapshot.status,
                       EXISTS (
                           SELECT 1
                             FROM {schema}.ptg2_v3_snapshot_binding AS binding
                            WHERE binding.snapshot_id = snapshot.snapshot_id
                       ) AS is_bound
                  FROM {schema}.ptg2_snapshot AS snapshot
                 WHERE snapshot.snapshot_id = :snapshot_id
                 FOR UPDATE
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        snapshot_row = snapshot_result.first()
        if not _can_delete_unpublished_sources(snapshot_row):
            return
        await _delete_unbound_snapshot_source_metadata(
            session,
            schema=schema,
            snapshot_id=snapshot_id,
        )


def _can_delete_unpublished_sources(snapshot_row: Any) -> bool:
    """Allow cleanup only for unbound logical snapshots that never published."""

    snapshot_state = _row_mapping(snapshot_row) if snapshot_row is not None else {}
    return not snapshot_state.get("is_bound") and str(
        snapshot_state.get("status") or ""
    ) in {"building", "failed"}


async def validate_reused_snapshot_sources(
    *,
    schema_name: str,
    snapshot_key: int,
    logical_snapshot_id: str,
    expected_generation: str = PTG2_V3_SHARED_GENERATION,
) -> dict[str, Any]:
    """Validate reused physical audit source keys against this logical dictionary."""

    async with db.transaction() as session:
        return await sealed_audit_sample_metadata(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            logical_snapshot_id=str(logical_snapshot_id),
            expected_generation=expected_generation,
        )


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"strict V3 finalizer summary is missing {name}")
    return dict(value)


def _integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}") from exc
    if normalized < 0:
        raise RuntimeError(f"strict V3 finalizer summary has negative {name}")
    return normalized


def _output_file(summary: Mapping[str, Any], section: Mapping[str, Any]) -> Path:
    root = Path(str(summary.get("output_directory") or "")).resolve()
    path = (root / str(section.get("path") or "")).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(
            "strict V3 finalizer block path escapes its output directory"
        ) from exc
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError(
            f"strict V3 finalizer block output is missing or empty: {path}"
        )
    return path


async def _export_provider_set_key_map(
    *,
    schema_name: str,
    snapshot_key: int,
    output_path: Path,
) -> Path:
    """Stream the authoritative dense map without materializing database rows."""

    schema = _quote_ident(schema_name)
    query = f"""
        SELECT encode(provider_set_global_id_128, 'hex'), provider_set_key
          FROM {schema}.ptg2_v3_provider_set
         WHERE snapshot_key = {int(snapshot_key)}
         ORDER BY provider_set_global_id_128
    """
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_from_query = getattr(driver_conn, "copy_from_query", None)
        if copy_from_query is None:
            raise NotImplementedError("active database driver does not expose COPY TO")
        with output_path.open("wb") as output:
            await copy_from_query(
                query,
                output=output,
                format="text",
                delimiter="\t",
                null="\\N",
            )
    return output_path


async def _copy_finalizer_block(
    finalizer_summary: Mapping[str, Any],
    block_summary: Mapping[str, Any],
    *,
    schema_name: str,
    stage_table: str,
    progress_callback: Callable[[str, int], None] | None,
) -> SharedBlockCopyMetrics | None:
    """Copy one authenticated finalizer block while preserving selective reuse."""

    return await copy_shared_block_binary_file(
        _output_file(finalizer_summary, block_summary),
        schema_name=schema_name,
        stage_table=stage_table,
        expected_copy_bytes=_integer(
            block_summary.get("copy_bytes"),
            "finalizer block COPY bytes",
        ),
        expected_copy_sha256=str(block_summary.get("copy_sha256") or ""),
        reuse_existing=True,
        **_progress_callback_kwargs(progress_callback),
    )


async def _convert_shared_graph_natively(
    *,
    graph_artifact_entries: Iterable[dict[str, Any]],
    provider_set_key_map_path: Path,
    work_directory: Path,
) -> SharedGraphConversionResult:
    graph_bundles = shared_graph_bundles_from_artifacts(graph_artifact_entries)
    return await convert_membership_shards_to_shared_graph_rust(
        shards=graph_bundles,
        provider_set_key_map_path=Path(provider_set_key_map_path),
        output_directory=Path(work_directory) / "provider-graph-native",
    )


def _v4_compiler_artifact(
    compilation: V4GraphCompilationResult,
    name: str,
) -> Any:
    matches = tuple(
        artifact for artifact in compilation.output_artifacts if artifact.name == name
    )
    if len(matches) != 1:
        raise RuntimeError(f"PTG V4 compiler output is missing {name!r}")
    return matches[0]


@dataclass(frozen=True)
class _V4ReferenceContract:
    """Authenticated compiler-file measurements checked after streaming."""

    byte_count: int
    sha256: str
    row_count: int


def _v4_reference_contract(
    expected_byte_count: int | None,
    expected_sha256: str | None,
    expected_row_count: int | None,
) -> _V4ReferenceContract | None:
    """Require either every reference measurement or no measurements."""

    if (
        expected_byte_count is None
        and expected_sha256 is None
        and expected_row_count is None
    ):
        return None
    if (
        expected_byte_count is None
        or expected_sha256 is None
        or expected_row_count is None
    ):
        raise ValueError("PTG V4 graph reference authentication is incomplete")
    return _V4ReferenceContract(
        byte_count=expected_byte_count,
        sha256=expected_sha256,
        row_count=expected_row_count,
    )


def _parse_v4_reference_line(
    reference_line: bytes,
    line_number: int,
    previous_coordinate: tuple[str, int, int] | None,
) -> tuple[SharedBlockReference, tuple[str, int, int]]:
    """Parse and validate one bounded, monotonically ordered reference."""

    if not reference_line or len(reference_line) > 64 * 1024:
        raise RuntimeError("PTG V4 graph reference record is not bounded")
    try:
        reference_fields = json.loads(reference_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"PTG V4 graph reference {line_number} is invalid JSON"
        ) from exc
    if (
        not isinstance(reference_fields, dict)
        or reference_fields.get("codec") != "none"
    ):
        raise RuntimeError("PTG V4 graph reference has an invalid codec")
    try:
        object_kind = str(reference_fields["object_kind"])
        block_key = int(reference_fields["block_key"])
        fragment_no = int(reference_fields["fragment_no"])
        entry_count = int(reference_fields["entry_count"])
        raw_byte_count = int(reference_fields["raw_byte_count"])
        stored_byte_count = int(reference_fields["stored_byte_count"])
        block_hash = bytes.fromhex(str(reference_fields["hash"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("PTG V4 graph reference fields are invalid") from exc
    coordinate = (object_kind, block_key, fragment_no)
    if (
        not object_kind.startswith("v4_")
        or min(block_key, fragment_no, entry_count, raw_byte_count) < 0
        or stored_byte_count != raw_byte_count
        or len(block_hash) != 32
        or (
            previous_coordinate is not None
            and coordinate <= previous_coordinate
        )
    ):
        raise RuntimeError("PTG V4 graph reference ordering or metadata changed")
    return (
        SharedBlockReference(
            object_kind=object_kind,
            block_key=block_key,
            fragment_no=fragment_no,
            entry_count=entry_count,
            block_hash=block_hash,
            raw_byte_count=raw_byte_count,
        ),
        coordinate,
    )


def _require_v4_reference_digest(
    contract: _V4ReferenceContract | None,
    observed_byte_count: int,
    observed_row_count: int,
    observed_digest: str,
) -> None:
    """Authenticate the fully consumed reference stream when requested."""

    if contract is None:
        return
    if (
        observed_byte_count != int(contract.byte_count)
        or observed_row_count != int(contract.row_count)
        or not hmac.compare_digest(
            observed_digest,
            str(contract.sha256),
        )
    ):
        raise RuntimeError("PTG V4 graph reference authentication changed")


def _iter_v4_block_references(
    path: Path,
    *,
    expected_byte_count: int | None = None,
    expected_sha256: str | None = None,
    expected_row_count: int | None = None,
) -> Iterable[SharedBlockReference]:
    """Re-read compiler coordinates as a bounded, optionally authenticated stream."""

    reference_contract = _v4_reference_contract(
        expected_byte_count,
        expected_sha256,
        expected_row_count,
    )
    previous_coordinate: tuple[str, int, int] | None = None
    observed_byte_count = 0
    observed_row_count = 0
    observed_sha256 = hashlib.sha256()
    with path.open("rb") as reference_file:
        for line_number, reference_line in enumerate(reference_file, 1):
            observed_byte_count += len(reference_line)
            observed_row_count += 1
            observed_sha256.update(reference_line)
            reference, coordinate = _parse_v4_reference_line(
                reference_line,
                line_number,
                previous_coordinate,
            )
            previous_coordinate = coordinate
            yield reference
    _require_v4_reference_digest(
        reference_contract,
        observed_byte_count,
        observed_row_count,
        observed_sha256.hexdigest(),
    )


def _require_v4_atomic_coordinate_counts(
    expected_block_count: int,
    cas_publication: V4CASBlockStagePublication,
    map_summary: V4SnapshotMapSummary,
) -> None:
    """Require compiler, CAS-stage, and packed-map coordinate parity."""

    if (
        int(cas_publication.staged_row_count) != int(expected_block_count)
        or int(map_summary.coordinate_count) != int(expected_block_count)
    ):
        raise RuntimeError("PTG V4 CAS and packed-map coordinate counts changed")


def _require_v4_compilation_layout_selection(
    compilation: V4GraphCompilationResult,
) -> Mapping[str, Any]:
    """Reject disagreement between the compiler result and adaptive summary."""

    adaptive_layout = v4_adaptive_layout_decision_from_summary(compilation.summary)
    expected_representation = (
        "pattern_v1" if compilation.selected_layout == "pattern" else "direct_v1"
    )
    if adaptive_layout["selected_representation"] != expected_representation:
        raise RuntimeError("PTG V4 adaptive layout publication selection changed")
    return adaptive_layout


def _require_v4_atomic_map_publication(
    compilation: V4GraphCompilationResult,
    cas_publication: V4CASBlockStagePublication,
    map_summary: V4SnapshotMapSummary,
) -> None:
    """Reject CAS/map count or adaptive-plan drift before atomic commit."""

    _require_v4_atomic_coordinate_counts(
        int(compilation.block_count),
        cas_publication,
        map_summary,
    )
    adaptive_layout = _require_v4_compilation_layout_selection(compilation)
    selected_layout_evidence = adaptive_layout[
        "pattern" if compilation.selected_layout == "pattern" else "direct"
    ]
    if (
        int(map_summary.stored_map_byte_count)
        != int(selected_layout_evidence["map_payload_encoded_bytes"])
        or int(map_summary.coordinate_count)
        != int(selected_layout_evidence["map_coordinate_count"])
        or int(map_summary.map_pack_count)
        != int(selected_layout_evidence["map_pack_count"])
        or int(map_summary.object_kind_count)
        != int(selected_layout_evidence["map_object_kind_count"])
    ):
        raise RuntimeError(
            "PTG V4 adaptive layout packed-map plan differs from publication"
        )


async def _queue_failed_v4_graph_blocks(
    *,
    schema_name: str,
    reference_manifest_path: Path,
) -> None:
    """Queue orphanable compiler CAS hashes; the normal sweep rechecks reachability."""

    schema = _quote_ident(schema_name)
    hashes: list[bytes] = []

    async def flush() -> None:
        """Persist the currently buffered unreachable block hashes."""

        if not hashes:
            return
        async with db.transaction() as session:
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v3_gc_candidate AS candidate
                        (block_hash, eligible_at, queued_at)
                    SELECT DISTINCT requested.block_hash, transaction_timestamp(),
                           transaction_timestamp()
                      FROM unnest(CAST(:block_hashes AS bytea[]))
                           AS requested(block_hash)
                      JOIN {schema}.ptg2_v3_block AS stored
                        ON stored.block_hash = requested.block_hash
                    ON CONFLICT (block_hash) DO UPDATE
                        SET eligible_at = GREATEST(
                            candidate.eligible_at,
                            EXCLUDED.eligible_at
                        )
                    """
                ),
                {"block_hashes": list(hashes)},
            )
        hashes.clear()

    for reference in _iter_v4_block_references(reference_manifest_path):
        hashes.append(bytes(reference.block_hash))
        if len(hashes) >= 8_192:
            await flush()
    await flush()


def _v4_dictionary_ranges(
    expected_count: int,
    *,
    estimated_row_bytes: int = _V4_DICTIONARY_ESTIMATED_ROW_BYTES,
) -> Iterable[tuple[int, int]]:
    """Yield default byte-bounded ranges for deterministic planning and tests."""

    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=estimated_row_bytes,
    )
    normalized_count = int(expected_count)
    for range_start in range(0, normalized_count, sizer.current_rows):
        yield range_start, min(
            range_start + sizer.current_rows,
            normalized_count,
        )


def _validated_dense_dictionary_range_sum(
    range_summary: Sequence[Any],
    *,
    range_start: int,
    range_end: int,
) -> int:
    """Validate one dense dictionary range and return its observed sum."""
    expected_rows = range_end - range_start
    if (
        int(range_summary[0]) != expected_rows
        or range_summary[1] != range_start
        or range_summary[2] != range_end - 1
        or not bool(range_summary[3])
    ):
        raise RuntimeError("PTG V4 dictionary COPY changed or duplicated keys")
    return int(range_summary[4])


async def _has_out_of_range_dictionary_key(
    session: Any,
    *,
    schema: str,
    stage_table: str,
    key_name: str,
    expected_count: int,
    heartbeat_callback: Callable[[], None] | None,
) -> bool:
    """Probe indexed lower and upper dense-key boundaries."""

    parameters_by_name = {"expected_count": int(expected_count)}
    for predicate, ordering in (
        (f"{key_name} < 0", f"{key_name} DESC"),
        (f"{key_name} >= :expected_count", key_name),
    ):
        invalid_key, _elapsed_seconds = await _await_v4_dictionary_statement(
            session.scalar(
                db.text(
                    f"""
                    SELECT {key_name}
                      FROM {schema}.{stage_table}
                     WHERE {predicate}
                     ORDER BY {ordering}
                     LIMIT 1
                    """
                ),
                parameters_by_name,
            ),
            heartbeat_callback=heartbeat_callback,
        )
        if invalid_key is not None:
            return True
    return False


async def _await_v4_dictionary_statement(
    statement_awaitable: Awaitable[Any],
    *,
    heartbeat_callback: Callable[[], None] | None,
    heartbeat_seconds: float = _V4_DICTIONARY_HEARTBEAT_SECONDS,
) -> tuple[Any, float]:
    """Await one statement while repeating only already-completed progress."""

    normalized_heartbeat = float(heartbeat_seconds)
    if normalized_heartbeat <= 0:
        raise ValueError("PTG V4 dictionary heartbeat must be positive")
    started_at = time.monotonic()
    statement_task = asyncio.ensure_future(statement_awaitable)
    try:
        while True:
            try:
                statement_result = await asyncio.wait_for(
                    asyncio.shield(statement_task),
                    timeout=normalized_heartbeat,
                )
                return statement_result, time.monotonic() - started_at
            except asyncio.TimeoutError:
                if heartbeat_callback is not None:
                    heartbeat_callback()
    except BaseException:
        if not statement_task.done():
            statement_task.cancel()
        with suppress(BaseException):
            await statement_task
        raise


def _v4_dictionary_range_end(
    *,
    range_start: int,
    expected_count: int,
    sizer: _V4DictionaryBatchSizer,
) -> int:
    """Return the next exclusive boundary under the current adaptive size."""

    return min(
        int(range_start) + int(sizer.current_rows),
        int(expected_count),
    )


async def _validate_v4_dictionary_stage(
    session: Any,
    *,
    schema: str,
    stage: _V4DenseDictionaryStage,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Validate dense stage keys and values in real bounded ranges."""

    if not stage.dense_keys:
        await _validate_v4_sparse_dictionary_stage(
            session,
            schema=schema,
            stage=stage,
            progress_callback=progress_callback,
            heartbeat_callback=heartbeat_callback,
        )
        return
    stage_table = _quote_ident(stage.stage_table)
    key_name = _quote_ident(stage.key_name)
    observed_sum = await _validated_v4_dense_dictionary_sum(
        session,
        schema=schema,
        stage=stage,
        stage_table=stage_table,
        key_name=key_name,
        progress_callback=progress_callback,
        heartbeat_callback=heartbeat_callback,
    )
    has_out_of_range_key = await _has_out_of_range_dictionary_key(
        session,
        schema=schema,
        stage_table=stage_table,
        key_name=key_name,
        expected_count=stage.expected_count,
        heartbeat_callback=heartbeat_callback,
    )
    if bool(has_out_of_range_key) or (
        stage.expected_sum is not None and observed_sum != int(stage.expected_sum)
    ):
        raise RuntimeError("PTG V4 dictionary COPY changed or duplicated keys")


async def _validated_v4_dense_dictionary_sum(
    session: Any,
    *,
    schema: str,
    stage: _V4DenseDictionaryStage,
    stage_table: str,
    key_name: str,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None,
) -> int:
    """Validate bounded dense ranges and return their observed value sum."""

    observed_sum = 0
    range_start = 0
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=stage.estimated_row_bytes,
    )
    while range_start < int(stage.expected_count):
        range_end = _v4_dictionary_range_end(
            range_start=range_start,
            expected_count=stage.expected_count,
            sizer=sizer,
        )
        range_summary_result, elapsed_seconds = await _await_v4_dictionary_statement(
            session.execute(
                db.text(
                    f"""
                        SELECT COUNT(*)::bigint, MIN({key_name}), MAX({key_name}),
                               COALESCE(BOOL_AND({stage.value_predicate}), TRUE),
                               COALESCE(SUM({stage.sum_expression}), 0)::bigint
                          FROM {schema}.{stage_table}
                         WHERE {key_name} >= :range_start
                           AND {key_name} < :range_end
                        """
                ),
                {"range_start": range_start, "range_end": range_end},
            ),
            heartbeat_callback=heartbeat_callback,
        )
        range_summary = range_summary_result.one()
        expected_rows = range_end - range_start
        observed_sum += _validated_dense_dictionary_range_sum(
            range_summary,
            range_start=range_start,
            range_end=range_end,
        )
        if progress_callback is not None:
            progress_callback("validated_dictionary_rows", expected_rows)
            progress_callback("publish_batches", 1)
        sizer.observe(elapsed_seconds)
        range_start = range_end
    return observed_sum


async def _v4_sparse_dictionary_summary(
    session: Any,
    *,
    schema: str,
    stage_table: str,
    key_name: str,
    stage: _V4DenseDictionaryStage,
    previous_key: int,
    batch_rows: int,
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[Any, float]:
    """Read one ordered sparse validation summary under the adaptive limit."""

    return await _await_v4_dictionary_statement(
        session.execute(
            db.text(
                f"""
                WITH batch AS MATERIALIZED (
                    SELECT *
                      FROM {schema}.{stage_table}
                     WHERE {key_name} > :previous_key
                     ORDER BY {key_name}
                     LIMIT :batch_rows
                )
                SELECT COUNT(*)::bigint, MIN({key_name}), MAX({key_name}),
                       COALESCE(BOOL_AND({stage.value_predicate}), TRUE),
                       COALESCE(SUM({stage.sum_expression}), 0)::bigint
                  FROM batch
                """
            ),
            {
                "previous_key": previous_key,
                "batch_rows": int(batch_rows),
            },
        ),
        heartbeat_callback=heartbeat_callback,
    )


async def _validate_v4_sparse_dictionary_stage(
    session: Any,
    *,
    schema: str,
    stage: _V4DenseDictionaryStage,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Validate one sparse-key dictionary through ordered physical batches."""

    stage_table = _quote_ident(stage.stage_table)
    key_name = _quote_ident(stage.key_name)
    previous_key = -1
    observed_count = 0
    observed_sum = 0
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=stage.estimated_row_bytes,
    )
    while True:
        batch_result, elapsed_seconds = await _v4_sparse_dictionary_summary(
            session,
            schema=schema,
            stage_table=stage_table,
            key_name=key_name,
            stage=stage,
            previous_key=previous_key,
            batch_rows=sizer.current_rows,
            heartbeat_callback=heartbeat_callback,
        )
        batch_summary = batch_result.one()
        batch_count = int(batch_summary[0])
        if batch_count == 0:
            break
        first_key = int(batch_summary[1])
        last_key = int(batch_summary[2])
        if first_key <= previous_key or not bool(batch_summary[3]):
            raise RuntimeError("PTG V4 dictionary COPY changed or duplicated keys")
        observed_count += batch_count
        observed_sum += int(batch_summary[4])
        previous_key = last_key
        if progress_callback is not None:
            progress_callback("validated_dictionary_rows", batch_count)
            progress_callback("publish_batches", 1)
        sizer.observe(elapsed_seconds)
    if observed_count != int(stage.expected_count) or (
        stage.expected_sum is not None and observed_sum != int(stage.expected_sum)
    ):
        raise RuntimeError("PTG V4 dictionary COPY changed or duplicated keys")


async def _publish_v4_dictionary_stage_ranges(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    stage: _V4DenseDictionaryStage,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Publish one validated dictionary through bounded key-range statements."""

    if not stage.dense_keys:
        await _publish_v4_sparse_dictionary_stage_ranges(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            stage=stage,
            progress_callback=progress_callback,
            heartbeat_callback=heartbeat_callback,
        )
        return
    stage_table = _quote_ident(stage.stage_table)
    target_table = _quote_ident(stage.target_table)
    key_name = _quote_ident(stage.key_name)
    quoted_columns = tuple(_quote_ident(column) for column in stage.columns)
    columns = ", ".join(quoted_columns)
    matching_columns = " AND ".join(
        f"stored.{column} = staged.{column}" for column in quoted_columns
    )
    range_start = 0
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=stage.estimated_row_bytes,
    )
    while range_start < int(stage.expected_count):
        range_end = _v4_dictionary_range_end(
            range_start=range_start,
            expected_count=stage.expected_count,
            sizer=sizer,
        )
        range_parameters_by_name = {
            "snapshot_key": int(snapshot_key),
            "range_start": range_start,
            "range_end": range_end,
        }
        _, insert_seconds = await _await_v4_dictionary_statement(
            session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.{target_table} (snapshot_key, {columns})
                    SELECT :snapshot_key, {columns}
                      FROM {schema}.{stage_table}
                     WHERE {key_name} >= :range_start
                       AND {key_name} < :range_end
                     ORDER BY {key_name}
                    ON CONFLICT DO NOTHING
                    """
                ),
                range_parameters_by_name,
            ),
            heartbeat_callback=heartbeat_callback,
        )
        matching_count, verification_seconds = await _await_v4_dictionary_statement(
            session.scalar(
                db.text(
                    f"""
                        SELECT COUNT(*)::bigint
                          FROM {schema}.{stage_table} AS staged
                          JOIN {schema}.{target_table} AS stored
                            ON stored.snapshot_key = :snapshot_key
                           AND {matching_columns}
                         WHERE staged.{key_name} >= :range_start
                           AND staged.{key_name} < :range_end
                        """
                ),
                range_parameters_by_name,
            ),
            heartbeat_callback=heartbeat_callback,
        )
        expected_rows = range_end - range_start
        if int(matching_count or 0) != expected_rows:
            raise RuntimeError("PTG V4 persisted dictionary rows changed")
        if progress_callback is not None:
            progress_callback("published_dictionary_rows", expected_rows)
            progress_callback("publish_batches", 1)
        sizer.observe(max(insert_seconds, verification_seconds))
        range_start = range_end
    await _reject_v4_dictionary_extra_keys(
        session,
        schema=schema,
        target_table=target_table,
        key_name=key_name,
        snapshot_key=int(snapshot_key),
        expected_count=int(stage.expected_count),
        heartbeat_callback=heartbeat_callback,
    )


async def _reject_v4_dictionary_extra_keys(
    session: Any,
    *,
    schema: str,
    target_table: str,
    key_name: str,
    snapshot_key: int,
    expected_count: int,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Reject indexed target keys outside the authenticated dense span."""

    parameters_by_name = {
        "snapshot_key": snapshot_key,
        "expected_count": expected_count,
    }
    for predicate, ordering in (
        (f"{key_name} < 0", f"{key_name} DESC"),
        (f"{key_name} >= :expected_count", key_name),
    ):
        invalid_key, _elapsed_seconds = await _await_v4_dictionary_statement(
            session.scalar(
                db.text(
                    f"""
                    SELECT {key_name}
                      FROM {schema}.{target_table}
                     WHERE snapshot_key = :snapshot_key
                       AND {predicate}
                     ORDER BY {ordering}
                     LIMIT 1
                    """
                ),
                parameters_by_name,
            ),
            heartbeat_callback=heartbeat_callback,
        )
        if invalid_key is not None:
            raise RuntimeError("PTG V4 persisted dictionary rows changed")


async def _v4_sparse_batch_boundary(
    session: Any,
    *,
    schema: str,
    stage_table: str,
    key_name: str,
    previous_key: int,
    batch_rows: int,
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[int, int | None, float]:
    """Read one sparse batch count and inclusive upper key."""

    batch_result, elapsed_seconds = await _await_v4_dictionary_statement(
        session.execute(
            db.text(
                f"""
                SELECT COUNT(*)::bigint, MAX({key_name})
                  FROM (
                        SELECT {key_name}
                          FROM {schema}.{stage_table}
                         WHERE {key_name} > :previous_key
                         ORDER BY {key_name}
                         LIMIT :batch_rows
                       ) AS batch
                """
            ),
            {
                "previous_key": previous_key,
                "batch_rows": int(batch_rows),
            },
        ),
        heartbeat_callback=heartbeat_callback,
    )
    batch_count, raw_last_key = batch_result.one()
    return (
        int(batch_count),
        None if raw_last_key is None else int(raw_last_key),
        elapsed_seconds,
    )


async def _insert_v4_sparse_batch(
    session: Any,
    *,
    schema: str,
    stage_table: str,
    target_table: str,
    key_name: str,
    columns: str,
    parameters_by_name: Mapping[str, int],
    heartbeat_callback: Callable[[], None] | None,
) -> float:
    """Insert one exact sparse dictionary batch."""

    _, insert_seconds = await _await_v4_dictionary_statement(
        session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.{target_table} (snapshot_key, {columns})
                SELECT :snapshot_key, {columns}
                  FROM {schema}.{stage_table}
                 WHERE {key_name} > :previous_key
                   AND {key_name} <= :last_key
                 ORDER BY {key_name}
                ON CONFLICT DO NOTHING
                """
            ),
            parameters_by_name,
        ),
        heartbeat_callback=heartbeat_callback,
    )
    return insert_seconds


async def _matching_v4_sparse_batch_count(
    session: Any,
    *,
    schema: str,
    stage_table: str,
    target_table: str,
    key_name: str,
    matching_columns: str,
    parameters_by_name: Mapping[str, int],
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[int, float]:
    """Count exact target rows for one sparse batch."""

    matching_count, verification_seconds = await _await_v4_dictionary_statement(
        session.scalar(
            db.text(
                f"""
                    SELECT COUNT(*)::bigint
                      FROM {schema}.{stage_table} AS staged
                      JOIN {schema}.{target_table} AS stored
                        ON stored.snapshot_key = :snapshot_key
                       AND {matching_columns}
                     WHERE staged.{key_name} > :previous_key
                       AND staged.{key_name} <= :last_key
                    """
            ),
            parameters_by_name,
        ),
        heartbeat_callback=heartbeat_callback,
    )
    return int(matching_count or 0), verification_seconds


async def _publish_v4_sparse_batch(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    stage: _V4DenseDictionaryStage,
    previous_key: int,
    last_key: int,
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[int, float]:
    """Insert and verify one exact sparse dictionary batch."""

    stage_table = _quote_ident(stage.stage_table)
    target_table = _quote_ident(stage.target_table)
    key_name = _quote_ident(stage.key_name)
    quoted_columns = tuple(_quote_ident(column) for column in stage.columns)
    parameters_by_name = {
        "snapshot_key": int(snapshot_key),
        "previous_key": previous_key,
        "last_key": last_key,
    }
    insert_seconds = await _insert_v4_sparse_batch(
        session,
        schema=schema,
        stage_table=stage_table,
        target_table=target_table,
        key_name=key_name,
        columns=", ".join(quoted_columns),
        parameters_by_name=parameters_by_name,
        heartbeat_callback=heartbeat_callback,
    )
    matching_count, verification_seconds = await _matching_v4_sparse_batch_count(
        session,
        schema=schema,
        stage_table=stage_table,
        target_table=target_table,
        key_name=key_name,
        matching_columns=" AND ".join(
            f"stored.{column} = staged.{column}" for column in quoted_columns
        ),
        parameters_by_name=parameters_by_name,
        heartbeat_callback=heartbeat_callback,
    )
    return matching_count, max(insert_seconds, verification_seconds)


def _normalized_v4_target_key(raw_key: Any, previous_key: int | bytes) -> int | bytes:
    """Normalize one ordered target key without changing its key domain."""

    if isinstance(previous_key, bytes):
        return bytes(raw_key)
    return int(raw_key)


async def _count_v4_target_keys(
    session: Any,
    *,
    schema: str,
    target_table: str,
    key_name: str,
    snapshot_key: int,
    initial_key: int | bytes,
    estimated_row_bytes: int,
    heartbeat_callback: Callable[[], None] | None,
) -> int:
    """Enumerate persisted keys through adaptive indexed pages."""

    previous_key = initial_key
    observed_count = 0
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=estimated_row_bytes,
    )
    while True:
        key_result, elapsed_seconds = await _await_v4_dictionary_statement(
            session.execute(
                db.text(
                    f"""
                    SELECT {key_name}
                      FROM {schema}.{target_table}
                     WHERE snapshot_key = :snapshot_key
                       AND {key_name} > :previous_key
                     ORDER BY {key_name}
                     LIMIT :batch_rows
                    """
                ),
                {
                    "snapshot_key": int(snapshot_key),
                    "previous_key": previous_key,
                    "batch_rows": int(sizer.current_rows),
                },
            ),
            heartbeat_callback=heartbeat_callback,
        )
        key_records = _v4_tax_result_rows(key_result)
        if not key_records:
            return observed_count
        for key_record in key_records:
            key = _normalized_v4_target_key(key_record[0], previous_key)
            if key <= previous_key:
                raise RuntimeError("PTG V4 persisted dictionary rows changed")
            previous_key = key
        observed_count += len(key_records)
        sizer.observe(elapsed_seconds)


async def _publish_v4_sparse_ranges(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    stage: _V4DenseDictionaryStage,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Publish one sparse dictionary using ordered, bounded key batches."""

    stage_table = _quote_ident(stage.stage_table)
    target_table = _quote_ident(stage.target_table)
    key_name = _quote_ident(stage.key_name)
    previous_key = -1
    published_count = 0
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=stage.estimated_row_bytes,
    )
    while True:
        batch_count, last_key, boundary_seconds = await _v4_sparse_batch_boundary(
            session,
            schema=schema,
            stage_table=stage_table,
            key_name=key_name,
            previous_key=previous_key,
            batch_rows=sizer.current_rows,
            heartbeat_callback=heartbeat_callback,
        )
        if batch_count == 0 or last_key is None:
            break
        matching_count, publication_seconds = await _publish_v4_sparse_batch(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            stage=stage,
            previous_key=previous_key,
            last_key=last_key,
            heartbeat_callback=heartbeat_callback,
        )
        if int(matching_count or 0) != batch_count:
            raise RuntimeError("PTG V4 persisted dictionary rows changed")
        published_count += batch_count
        previous_key = last_key
        if progress_callback is not None:
            progress_callback("published_dictionary_rows", batch_count)
            progress_callback("publish_batches", 1)
        sizer.observe(max(boundary_seconds, publication_seconds))
    await _validate_v4_sparse_target(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        target_table=target_table,
        key_name=key_name,
        stage=stage,
        published_count=published_count,
        heartbeat_callback=heartbeat_callback,
    )


async def _validate_v4_sparse_target(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    target_table: str,
    key_name: str,
    stage: _V4DenseDictionaryStage,
    published_count: int,
    heartbeat_callback: Callable[[], None] | None,
) -> None:
    """Verify that sparse publication retained exactly the authenticated keys."""

    target_count = await _count_v4_target_keys(
        session,
        schema=schema,
        target_table=target_table,
        key_name=key_name,
        snapshot_key=int(snapshot_key),
        initial_key=-1,
        estimated_row_bytes=stage.estimated_row_bytes,
        heartbeat_callback=heartbeat_callback,
    )
    if published_count != int(stage.expected_count) or int(target_count or 0) != int(
        stage.expected_count
    ):
        raise RuntimeError("PTG V4 persisted dictionary rows changed")


_publish_v4_sparse_dictionary_stage_ranges = _publish_v4_sparse_ranges


def _v4_tax_length_prefixed_digest(
    domain: bytes,
    fields: Iterable[bytes],
) -> bytes:
    """Hash independently framed contract fields exactly as the Rust compiler."""

    digest = hashlib.sha256()
    digest.update(domain)
    for field in fields:
        digest.update(struct.pack(">I", len(field)))
        digest.update(field)
    return digest.digest()


def _v4_tax_policy_descriptor(token_policy_id: str) -> bytes:
    """Rebuild the policy descriptor without trusting compiler output."""

    return _v4_tax_length_prefixed_digest(
        _V4_TAX_POLICY_DESCRIPTOR_DOMAIN,
        (
            token_policy_id.encode("ascii"),
            _V4_TAX_NORMALIZATION_CONTRACT.encode("ascii"),
            _V4_TAX_HMAC_CONTRACT.encode("ascii"),
            _V4_TAX_CANDIDATE_PREFIX_CONTRACT.encode("ascii"),
            _V4_TAX_AUTHORITY_CONTRACT.encode("ascii"),
        ),
    )


def _v4_tax_source_ordinal_digest(
    source_ordinal_map: Iterable[Mapping[str, Any]],
) -> bytes:
    """Bind stable source ordinals and names through an independent digest."""

    entries = tuple(source_ordinal_map)
    digest = hashlib.sha256()
    digest.update(_V4_TAX_SOURCE_ORDINAL_DOMAIN)
    digest.update(struct.pack(">I", len(entries)))
    for expected_ordinal, entry in enumerate(entries):
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"shard_id", "ordinal"}
            or entry.get("ordinal") != expected_ordinal
            or not isinstance(entry.get("shard_id"), str)
            or not entry["shard_id"]
        ):
            raise RuntimeError("PTG V4 tax identity source ordinal map changed")
        encoded_shard_id = entry["shard_id"].encode("utf-8")
        digest.update(struct.pack(">I", len(encoded_shard_id)))
        digest.update(encoded_shard_id)
        digest.update(struct.pack(">I", expected_ordinal))
    return digest.digest()


def _v4_tax_summary_digest(value: Any, label: str) -> bytes:
    """Decode one exact lowercase SHA-256 field."""

    if not isinstance(value, str) or len(value) != 64 or value.lower() != value:
        raise RuntimeError(f"PTG V4 tax identity {label} changed")
    try:
        decoded = bytes.fromhex(value)
    except ValueError as exc:
        raise RuntimeError(f"PTG V4 tax identity {label} changed") from exc
    if len(decoded) != 32:
        raise RuntimeError(f"PTG V4 tax identity {label} changed")
    return decoded


def _v4_tax_summary_count(summary: Mapping[str, Any], name: str) -> int:
    """Read one strict nonnegative counter without accepting booleans."""

    value = summary.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RuntimeError(f"PTG V4 tax identity {name} changed")
    return int(value)


def _has_expected_v4_tax_contract(
    tax_summary: Mapping[str, Any],
    token_policy_id: Any,
) -> bool:
    """Return whether scalar policy fields match the frozen Release-1 wire."""

    return (
        set(tax_summary) == _V4_TAX_SUMMARY_FIELDS
        and tax_summary.get("contract") == _V4_TAX_IDENTITY_PROJECTION_CONTRACT
        and isinstance(token_policy_id, str)
        and _V4_TAX_POLICY_ID.fullmatch(token_policy_id) is not None
        and len(token_policy_id.encode("ascii")) <= 55
        and tax_summary.get("normalization_contract") == _V4_TAX_NORMALIZATION_CONTRACT
        and tax_summary.get("hmac_contract") == _V4_TAX_HMAC_CONTRACT
        and tax_summary.get("candidate_prefix_contract")
        == _V4_TAX_CANDIDATE_PREFIX_CONTRACT
        and tax_summary.get("authority_contract") == _V4_TAX_AUTHORITY_CONTRACT
        and tax_summary.get("source_ordinal_contract")
        == _V4_TAX_SOURCE_ORDINAL_CONTRACT
    )


def _v4_tax_contract_header(
    tax_summary: Mapping[str, Any],
) -> tuple[str, tuple[Mapping[str, Any], ...], bytes, bytes]:
    """Validate descriptor fields and return independently rebuilt bindings."""

    token_policy_id = tax_summary.get("token_policy_id")
    if not _has_expected_v4_tax_contract(tax_summary, token_policy_id):
        raise RuntimeError("PTG V4 tax identity contract changed")
    raw_source_ordinals = tax_summary.get("source_ordinal_map")
    if not isinstance(raw_source_ordinals, list) or not raw_source_ordinals:
        raise RuntimeError("PTG V4 tax identity source map changed")
    source_ordinals = tuple(
        dict(entry) if isinstance(entry, Mapping) else entry
        for entry in raw_source_ordinals
    )
    source_shard_ids = tuple(
        entry.get("shard_id") for entry in source_ordinals if isinstance(entry, Mapping)
    )
    if source_shard_ids != tuple(sorted(set(source_shard_ids))):
        raise RuntimeError("PTG V4 tax identity source map changed")
    source_ordinal_map_digest = _v4_tax_source_ordinal_digest(source_ordinals)
    token_policy_descriptor = _v4_tax_policy_descriptor(token_policy_id)
    if not hmac.compare_digest(
        token_policy_descriptor,
        _v4_tax_summary_digest(
            tax_summary.get("token_policy_descriptor_sha256"),
            "policy descriptor",
        ),
    ) or not hmac.compare_digest(
        source_ordinal_map_digest,
        _v4_tax_summary_digest(
            tax_summary.get("source_ordinal_map_digest"),
            "source ordinal digest",
        ),
    ):
        raise RuntimeError("PTG V4 tax identity descriptor changed")
    return (
        token_policy_id,
        source_ordinals,
        token_policy_descriptor,
        source_ordinal_map_digest,
    )


def _v4_tax_contract_counts(
    tax_summary: Mapping[str, Any],
    *,
    expected_group_count: int,
) -> Mapping[str, int]:
    """Validate source width and exact per-state publication counts."""

    source_shard_count = _v4_tax_summary_count(tax_summary, "source_shard_count")
    source_bitmap_bytes = _v4_tax_summary_count(tax_summary, "source_bitmap_bytes")
    if source_bitmap_bytes != (source_shard_count + 7) // 8 or source_shard_count <= 0:
        raise RuntimeError("PTG V4 tax identity source shape changed")
    count_by_name = {
        name: _v4_tax_summary_count(tax_summary, name)
        for name in (
            "provider_group_count",
            "tax_identity_count",
            "matched_ein_count",
            "missing_count",
            "malformed_count",
            "unsupported_type_count",
        )
    }
    if (
        sum(
            count_by_name[name]
            for name in (
                "matched_ein_count",
                "missing_count",
                "malformed_count",
                "unsupported_type_count",
            )
        )
        != count_by_name["provider_group_count"]
        or count_by_name["tax_identity_count"] > count_by_name["matched_ein_count"]
        or count_by_name["provider_group_count"] != expected_group_count
    ):
        raise RuntimeError("PTG V4 tax identity counts changed")
    return {
        **count_by_name,
        "source_shard_count": source_shard_count,
        "source_bitmap_bytes": source_bitmap_bytes,
    }


def _validated_v4_tax_identity_contract(
    compilation: V4GraphCompilationResult,
) -> _V4TaxIdentityContract:
    """Recompute all non-row tax contracts before any durable persistence."""

    tax_summary = compilation.summary.get("tax_identity")
    if not isinstance(tax_summary, Mapping):
        raise RuntimeError("PTG V4 tax identity summary is missing")
    (
        token_policy_id,
        source_ordinals,
        token_policy_descriptor,
        source_ordinal_map_digest,
    ) = _v4_tax_contract_header(tax_summary)
    count_by_name = _v4_tax_contract_counts(
        tax_summary,
        expected_group_count=int(compilation.observe.get("group_count") or 0),
    )
    if count_by_name["source_shard_count"] != len(source_ordinals):
        raise RuntimeError("PTG V4 tax identity source shape changed")
    return _V4TaxIdentityContract(
        token_policy_id=token_policy_id,
        token_policy_descriptor_sha256=token_policy_descriptor,
        source_ordinal_map=source_ordinals,
        source_ordinal_map_digest=source_ordinal_map_digest,
        source_shard_count=count_by_name["source_shard_count"],
        source_bitmap_bytes=count_by_name["source_bitmap_bytes"],
        provider_group_count=count_by_name["provider_group_count"],
        tax_identity_count=count_by_name["tax_identity_count"],
        matched_ein_count=count_by_name["matched_ein_count"],
        missing_count=count_by_name["missing_count"],
        malformed_count=count_by_name["malformed_count"],
        unsupported_type_count=count_by_name["unsupported_type_count"],
        content_digest=_v4_tax_summary_digest(
            tax_summary.get("content_digest"),
            "content digest",
        ),
    )


def _v4_tax_artifact_byte_count(
    compilation: V4GraphCompilationResult,
) -> int:
    """Account both mandatory token-only COPY artifacts separately."""

    artifact_by_name = {
        artifact.name: artifact for artifact in compilation.output_artifacts
    }
    expected_names = (
        "provider_tax_identities",
        "provider_group_tax_identities",
    )
    if any(name not in artifact_by_name for name in expected_names):
        raise RuntimeError("PTG V4 tax identity artifacts are missing")
    return sum(int(artifact_by_name[name].byte_count) for name in expected_names)


def _v4_tax_result_rows(result: Any) -> tuple[Any, ...]:
    """Normalize the bounded SQLAlchemy row result used by publication."""

    rows = result.all()
    return tuple(rows)


async def _v4_tax_token_batch(
    session: Any,
    *,
    schema: str,
    tax_stage: str,
    range_start: int,
    range_end: int,
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[tuple[Any, ...], float]:
    """Read one dense token batch in canonical key order."""

    token_result, elapsed_seconds = await _await_v4_dictionary_statement(
        session.execute(
            db.text(
                f"""
                SELECT tin_key, tin_id_128, tin_hmac_sha256
                  FROM {schema}.{tax_stage}
                 WHERE tin_key >= :range_start
                   AND tin_key < :range_end
                 ORDER BY tin_key
                """
            ),
            {"range_start": range_start, "range_end": range_end},
        ),
        heartbeat_callback=heartbeat_callback,
    )
    return _v4_tax_result_rows(token_result), elapsed_seconds


def _append_v4_tax_token_rows(
    token_rows: Iterable[Any],
    *,
    range_start: int,
    content_digest: Any,
) -> int:
    """Validate and append one canonical token batch."""

    observed_count = 0
    for expected_key, token_row in enumerate(token_rows, range_start):
        tin_key = int(token_row[0])
        tin_id_128 = bytes(token_row[1])
        tin_hmac_sha256 = bytes(token_row[2])
        if (
            tin_key != expected_key
            or len(tin_id_128) != 16
            or len(tin_hmac_sha256) != 32
            or not hmac.compare_digest(tin_id_128, tin_hmac_sha256[:16])
        ):
            raise RuntimeError("PTG V4 tax identity dictionary changed")
        content_digest.update(tin_hmac_sha256)
        observed_count += 1
    return observed_count


async def _validate_v4_tax_token_rows(
    session: Any,
    *,
    schema: str,
    tax_identity_stage: str,
    contract: _V4TaxIdentityContract,
    content_digest: Any,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Authenticate the dense token dictionary and append it to the digest."""

    tax_stage = _quote_ident(tax_identity_stage)
    observed_token_count = 0
    range_start = 0
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=_V4_DICTIONARY_ESTIMATED_ROW_BYTES,
    )
    while range_start < int(contract.tax_identity_count):
        range_end = _v4_dictionary_range_end(
            range_start=range_start,
            expected_count=contract.tax_identity_count,
            sizer=sizer,
        )
        token_rows, elapsed_seconds = await _v4_tax_token_batch(
            session,
            schema=schema,
            tax_stage=tax_stage,
            range_start=range_start,
            range_end=range_end,
            heartbeat_callback=heartbeat_callback,
        )
        if len(token_rows) != range_end - range_start:
            raise RuntimeError("PTG V4 tax identity dictionary changed")
        observed_token_count += _append_v4_tax_token_rows(
            token_rows,
            range_start=range_start,
            content_digest=content_digest,
        )
        if progress_callback is not None:
            progress_callback(
                "validated_dictionary_rows",
                len(token_rows),
            )
            progress_callback("publish_batches", 1)
        sizer.observe(elapsed_seconds)
        range_start = range_end
    if observed_token_count != contract.tax_identity_count:
        raise RuntimeError("PTG V4 tax identity dictionary changed")


def _validated_v4_tax_group_row(
    group_row: Any,
    *,
    previous_group_id: bytes,
    contract: _V4TaxIdentityContract,
) -> tuple[bytes, str, int | None, bytes]:
    """Validate one ordered group row and its state-specific token reference."""

    group_id = bytes(group_row[0])
    tax_state = str(group_row[1])
    raw_tin_key = group_row[2]
    source_bitmap = bytes(group_row[3])
    graph_group_present = bool(group_row[4])
    tin_key = None if raw_tin_key is None else int(raw_tin_key)
    if (
        len(group_id) != 16
        or group_id <= previous_group_id
        or not graph_group_present
        or tax_state not in _V4_TAX_STATE_CODE
        or len(source_bitmap) != contract.source_bitmap_bytes
        or not any(source_bitmap)
        or (
            tax_state == "matched_ein"
            and (
                tin_key is None or tin_key < 0 or tin_key >= contract.tax_identity_count
            )
        )
        or (tax_state != "matched_ein" and tin_key is not None)
    ):
        raise RuntimeError("PTG V4 provider-group tax identity changed")
    unused_bits = contract.source_bitmap_bytes * 8 - contract.source_shard_count
    if unused_bits and (source_bitmap[-1] & (0xFF << (8 - unused_bits))):
        raise RuntimeError("PTG V4 provider-group source bitmap changed")
    return group_id, tax_state, tin_key, source_bitmap


def _append_v4_tax_group_digest(
    content_digest: Any,
    *,
    group_id: bytes,
    tax_state: str,
    tin_key: int | None,
    source_bitmap: bytes,
) -> None:
    """Append one canonical provider-group row to the content digest."""

    content_digest.update(group_id)
    content_digest.update(bytes([_V4_TAX_STATE_CODE[tax_state]]))
    if tin_key is None:
        content_digest.update(b"\x00")
    else:
        content_digest.update(b"\x01")
        content_digest.update(struct.pack(">I", tin_key))
    content_digest.update(struct.pack(">I", len(source_bitmap)))
    content_digest.update(source_bitmap)


async def _v4_tax_group_rows_batch(
    session: Any,
    *,
    schema: str,
    group_tax_stage: str,
    graph_group_stage: str,
    previous_group_id: bytes,
    batch_rows: int,
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[tuple[Any, ...], float]:
    """Read one ordered provider-group tax sidecar batch."""

    group_result, elapsed_seconds = await _await_v4_dictionary_statement(
        session.execute(
            db.text(
                f"""
                SELECT sidecar.provider_group_global_id_128,
                       sidecar.tax_identity_state,
                       sidecar.tin_key, sidecar.source_bitmap,
                       graph_group.provider_group_global_id_128 IS NOT NULL
                  FROM {schema}.{group_tax_stage} AS sidecar
                  LEFT JOIN {schema}.{graph_group_stage} AS graph_group
                    ON graph_group.provider_group_global_id_128 =
                       sidecar.provider_group_global_id_128
                 WHERE sidecar.provider_group_global_id_128 >
                       :previous_group_id
                 ORDER BY sidecar.provider_group_global_id_128
                 LIMIT :batch_rows
                """
            ),
            {
                "previous_group_id": previous_group_id,
                "batch_rows": int(batch_rows),
            },
        ),
        heartbeat_callback=heartbeat_callback,
    )
    return _v4_tax_result_rows(group_result), elapsed_seconds


def _consume_v4_tax_group_rows(
    group_rows: Iterable[Any],
    *,
    previous_group_id: bytes,
    contract: _V4TaxIdentityContract,
    content_digest: Any,
    count_by_state: dict[str, int],
    referenced_token_bits: bytearray,
) -> tuple[bytes, int]:
    """Authenticate one sidecar batch and count newly referenced tokens."""

    latest_group_id = previous_group_id
    newly_referenced_tokens = 0
    for group_row in group_rows:
        group_id, tax_state, tin_key, source_bitmap = _validated_v4_tax_group_row(
            group_row,
            previous_group_id=latest_group_id,
            contract=contract,
        )
        _append_v4_tax_group_digest(
            content_digest,
            group_id=group_id,
            tax_state=tax_state,
            tin_key=tin_key,
            source_bitmap=source_bitmap,
        )
        count_by_state[tax_state] += 1
        if tin_key is not None:
            byte_index, bit_index = divmod(tin_key, 8)
            bit_mask = 1 << bit_index
            if not referenced_token_bits[byte_index] & bit_mask:
                referenced_token_bits[byte_index] |= bit_mask
                newly_referenced_tokens += 1
        latest_group_id = group_id
    return latest_group_id, newly_referenced_tokens


def _tax_group_row_estimate(
    contract: _V4TaxIdentityContract,
) -> int:
    """Estimate one group-tax validation row including its source bitmap."""

    return _V4_DICTIONARY_ESTIMATED_ROW_BYTES + int(contract.source_bitmap_bytes)


async def _validate_v4_tax_group_rows(
    session: Any,
    *,
    schema: str,
    group_dictionary_stage: str,
    group_tax_identity_stage: str,
    contract: _V4TaxIdentityContract,
    content_digest: Any,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> Mapping[str, int]:
    """Authenticate ordered provider-group sidecars in bounded batches."""

    group_tax_stage = _quote_ident(group_tax_identity_stage)
    graph_group_stage = _quote_ident(group_dictionary_stage)
    previous_group_id = b""
    observed_group_count = 0
    referenced_token_count = 0
    referenced_token_bits = bytearray((contract.tax_identity_count + 7) // 8)
    count_by_state = {name: 0 for name in _V4_TAX_STATE_CODE}
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=_tax_group_row_estimate(contract),
    )
    while True:
        group_rows, elapsed_seconds = await _v4_tax_group_rows_batch(
            session,
            schema=schema,
            group_tax_stage=group_tax_stage,
            graph_group_stage=graph_group_stage,
            previous_group_id=previous_group_id,
            batch_rows=sizer.current_rows,
            heartbeat_callback=heartbeat_callback,
        )
        if not group_rows:
            break
        previous_group_id, new_token_count = _consume_v4_tax_group_rows(
            group_rows,
            previous_group_id=previous_group_id,
            contract=contract,
            content_digest=content_digest,
            count_by_state=count_by_state,
            referenced_token_bits=referenced_token_bits,
        )
        referenced_token_count += new_token_count
        observed_group_count += len(group_rows)
        if progress_callback is not None:
            progress_callback(
                "validated_dictionary_rows",
                len(group_rows),
            )
            progress_callback("publish_batches", 1)
        sizer.observe(elapsed_seconds)
    return {
        **count_by_state,
        "provider_group_count": observed_group_count,
        "referenced_tax_identity_count": referenced_token_count,
    }


def _v4_tax_content_hasher(
    contract: _V4TaxIdentityContract,
) -> Any:
    """Initialize the independently reconstructed content digest."""

    content_digest = hashlib.sha256()
    content_digest.update(_V4_TAX_CONTENT_DOMAIN)
    content_digest.update(contract.token_policy_descriptor_sha256)
    content_digest.update(contract.source_ordinal_map_digest)
    content_digest.update(struct.pack(">Q", contract.tax_identity_count))
    return content_digest


async def _validate_v4_tax_identity_stages(
    session: Any,
    *,
    schema: str,
    group_dictionary_stage: str,
    tax_identity_stage: str,
    group_tax_identity_stage: str,
    contract: _V4TaxIdentityContract,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Recompute content and relational completeness from staged COPY rows."""

    content_digest = _v4_tax_content_hasher(contract)
    await _validate_v4_tax_token_rows(
        session,
        schema=schema,
        tax_identity_stage=tax_identity_stage,
        contract=contract,
        content_digest=content_digest,
        progress_callback=progress_callback,
        heartbeat_callback=heartbeat_callback,
    )
    content_digest.update(struct.pack(">Q", contract.provider_group_count))
    count_by_state = await _validate_v4_tax_group_rows(
        session,
        schema=schema,
        group_dictionary_stage=group_dictionary_stage,
        group_tax_identity_stage=group_tax_identity_stage,
        contract=contract,
        content_digest=content_digest,
        progress_callback=progress_callback,
        heartbeat_callback=heartbeat_callback,
    )
    expected_counts = (
        contract.provider_group_count,
        contract.matched_ein_count,
        contract.missing_count,
        contract.malformed_count,
        contract.unsupported_type_count,
        contract.tax_identity_count,
    )
    observed_counts = (
        count_by_state["provider_group_count"],
        count_by_state["matched_ein"],
        count_by_state["missing"],
        count_by_state["malformed"],
        count_by_state["unsupported_type"],
        count_by_state["referenced_tax_identity_count"],
    )
    if observed_counts != expected_counts or not hmac.compare_digest(
        content_digest.digest(),
        contract.content_digest,
    ):
        raise RuntimeError("PTG V4 tax identity content digest changed")


def _v4_tax_manifest_values(
    *,
    snapshot_key: int,
    contract: _V4TaxIdentityContract,
) -> Mapping[str, Any]:
    """Build exact database manifest values from the validated contract."""

    return {
        "snapshot_key": int(snapshot_key),
        "contract": _V4_TAX_IDENTITY_MANIFEST_CONTRACT,
        "token_policy_id": contract.token_policy_id,
        "token_policy_descriptor_sha256": (contract.token_policy_descriptor_sha256),
        "normalization_contract": _V4_TAX_NORMALIZATION_CONTRACT,
        "hmac_contract": _V4_TAX_HMAC_CONTRACT,
        "source_ordinal_contract": _V4_TAX_SOURCE_ORDINAL_CONTRACT,
        "source_ordinal_map": [dict(entry) for entry in contract.source_ordinal_map],
        "source_ordinal_map_digest": contract.source_ordinal_map_digest,
        "source_shard_count": contract.source_shard_count,
        "provider_group_count": contract.provider_group_count,
        "tax_identity_count": contract.tax_identity_count,
        "matched_ein_count": contract.matched_ein_count,
        "missing_count": contract.missing_count,
        "malformed_count": contract.malformed_count,
        "unsupported_type_count": contract.unsupported_type_count,
        "content_digest": contract.content_digest,
    }


async def _insert_v4_tax_manifest(
    session: Any,
    *,
    schema: str,
    expected_by_name: Mapping[str, Any],
) -> tuple[str, ...]:
    """Insert the manifest idempotently and return its stable column order."""

    parameters_by_name = {
        **expected_by_name,
        "source_ordinal_map": json.dumps(
            expected_by_name["source_ordinal_map"],
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    columns = tuple(expected_by_name)
    value_expressions = tuple(
        (
            "CAST(:source_ordinal_map AS jsonb)"
            if column == "source_ordinal_map"
            else f":{column}"
        )
        for column in columns
    )
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.ptg2_provider_tax_identity_manifest
                ({", ".join(columns)})
            VALUES
                ({", ".join(value_expressions)})
            ON CONFLICT DO NOTHING
            """
        ),
        parameters_by_name,
    )
    return columns


async def _stored_v4_tax_manifest(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    columns: tuple[str, ...],
) -> Mapping[str, Any]:
    """Read the immutable manifest through the same canonical column order."""

    stored_result = await session.execute(
        db.text(
            f"""
            SELECT {", ".join(columns[1:])}
              FROM {schema}.ptg2_provider_tax_identity_manifest
             WHERE snapshot_key = :snapshot_key
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    return dict(zip(columns[1:], stored_result.one()))


async def _publish_v4_tax_identity_manifest(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    contract: _V4TaxIdentityContract,
) -> Mapping[str, Any]:
    """Publish and re-read the immutable manifest before any child rows."""

    expected_by_name = _v4_tax_manifest_values(
        snapshot_key=snapshot_key,
        contract=contract,
    )
    columns = await _insert_v4_tax_manifest(
        session,
        schema=schema,
        expected_by_name=expected_by_name,
    )
    stored_by_name = await _stored_v4_tax_manifest(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        columns=columns,
    )
    for name, expected in expected_by_name.items():
        if name == "snapshot_key":
            continue
        observed = stored_by_name[name]
        if isinstance(expected, bytes):
            is_matching = hmac.compare_digest(bytes(observed), expected)
        else:
            is_matching = observed == expected
        if not is_matching:
            raise RuntimeError("PTG V4 tax identity manifest replay changed")
    return {
        name: (
            manifest_value.hex()
            if isinstance(manifest_value, bytes)
            else manifest_value
        )
        for name, manifest_value in expected_by_name.items()
    }


async def _v4_tax_group_batch_boundary(
    session: Any,
    *,
    schema: str,
    stage: str,
    previous_group_id: bytes,
    batch_rows: int,
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[int, bytes | None, float]:
    """Return one bounded group batch count and inclusive upper key."""

    batch_result, elapsed_seconds = await _await_v4_dictionary_statement(
        session.execute(
            db.text(
                f"""
                SELECT COUNT(*)::bigint,
                       MAX(provider_group_global_id_128)
                  FROM (
                        SELECT provider_group_global_id_128
                          FROM {schema}.{stage}
                         WHERE provider_group_global_id_128 >
                               :previous_group_id
                         ORDER BY provider_group_global_id_128
                         LIMIT :batch_rows
                       ) AS batch
                """
            ),
            {
                "previous_group_id": previous_group_id,
                "batch_rows": int(batch_rows),
            },
        ),
        heartbeat_callback=heartbeat_callback,
    )
    batch_count, raw_last_group_id = batch_result.one()
    return (
        int(batch_count),
        None if raw_last_group_id is None else bytes(raw_last_group_id),
        elapsed_seconds,
    )


async def _publish_v4_tax_group_batch(
    session: Any,
    *,
    schema: str,
    stage: str,
    parameters_by_name: Mapping[str, Any],
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[int, float]:
    """Insert one group batch and return its exact replay match count."""

    _, insert_seconds = await _await_v4_dictionary_statement(
        session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_provider_group_tax_identity
                    (snapshot_key, provider_group_global_id_128,
                     tax_identity_state, tin_key, source_bitmap)
                SELECT :snapshot_key, provider_group_global_id_128,
                       tax_identity_state, tin_key, source_bitmap
                  FROM {schema}.{stage}
                 WHERE provider_group_global_id_128 > :previous_group_id
                   AND provider_group_global_id_128 <= :last_group_id
                 ORDER BY provider_group_global_id_128
                ON CONFLICT DO NOTHING
                """
            ),
            parameters_by_name,
        ),
        heartbeat_callback=heartbeat_callback,
    )
    matching_count, verification_seconds = await _await_v4_dictionary_statement(
        session.scalar(
            db.text(
                f"""
                    SELECT COUNT(*)::bigint
                      FROM {schema}.{stage} AS staged
                      JOIN {schema}.ptg2_provider_group_tax_identity AS stored
                        ON stored.snapshot_key = :snapshot_key
                       AND stored.provider_group_global_id_128 =
                           staged.provider_group_global_id_128
                       AND stored.tax_identity_state = staged.tax_identity_state
                       AND stored.tin_key IS NOT DISTINCT FROM staged.tin_key
                       AND stored.source_bitmap = staged.source_bitmap
                     WHERE staged.provider_group_global_id_128 > :previous_group_id
                       AND staged.provider_group_global_id_128 <= :last_group_id
                    """
            ),
            parameters_by_name,
        ),
        heartbeat_callback=heartbeat_callback,
    )
    return (
        int(matching_count or 0),
        max(insert_seconds, verification_seconds),
    )


async def _reject_tax_group_count(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    expected_count: int,
    published_count: int,
    estimated_row_bytes: int,
    heartbeat_callback: Callable[[], None] | None,
) -> None:
    """Reject missing or extra sidecars through adaptive key enumeration."""

    target_count = await _count_v4_target_keys(
        session,
        schema=schema,
        target_table="ptg2_provider_group_tax_identity",
        key_name="provider_group_global_id_128",
        snapshot_key=int(snapshot_key),
        initial_key=b"",
        estimated_row_bytes=estimated_row_bytes,
        heartbeat_callback=heartbeat_callback,
    )
    if int(published_count) != int(expected_count) or int(target_count or 0) != int(
        expected_count
    ):
        raise RuntimeError("PTG V4 persisted provider-group tax identity changed")


async def _publish_tax_group_ranges(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    stage: str,
    sizer: _V4DictionaryBatchSizer,
    progress_callback: Callable[[str, int], None] | None,
    heartbeat_callback: Callable[[], None] | None,
) -> int:
    """Publish every ordered provider-group tax batch and return its row count."""

    previous_group_id = b""
    published_count = 0
    while True:
        batch_count, last_group_id, boundary_seconds = (
            await _v4_tax_group_batch_boundary(
                session,
                schema=schema,
                stage=stage,
                previous_group_id=previous_group_id,
                batch_rows=sizer.current_rows,
                heartbeat_callback=heartbeat_callback,
            )
        )
        if batch_count == 0 or last_group_id is None:
            return published_count
        parameters_by_name = {
            "snapshot_key": int(snapshot_key),
            "previous_group_id": previous_group_id,
            "last_group_id": last_group_id,
        }
        matching_count, publication_seconds = await _publish_v4_tax_group_batch(
            session,
            schema=schema,
            stage=stage,
            parameters_by_name=parameters_by_name,
            heartbeat_callback=heartbeat_callback,
        )
        if matching_count != batch_count:
            raise RuntimeError("PTG V4 persisted provider-group tax identity changed")
        published_count += batch_count
        previous_group_id = last_group_id
        if progress_callback is not None:
            progress_callback("published_dictionary_rows", batch_count)
            progress_callback("publish_batches", 1)
        sizer.observe(max(boundary_seconds, publication_seconds))


async def _publish_v4_tax_group_ranges(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    stage_table: str,
    expected_count: int,
    progress_callback: Callable[[str, int], None] | None,
    source_bitmap_bytes: int = 0,
    heartbeat_callback: Callable[[], None] | None = None,
) -> None:
    """Publish fixed-width group sidecars through bounded byte-key ranges."""

    stage = _quote_ident(stage_table)
    sizer = _V4DictionaryBatchSizer(
        estimated_row_bytes=(
            _V4_DICTIONARY_ESTIMATED_ROW_BYTES + max(int(source_bitmap_bytes), 0)
        ),
    )
    published_count = await _publish_tax_group_ranges(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        stage=stage,
        sizer=sizer,
        progress_callback=progress_callback,
        heartbeat_callback=heartbeat_callback,
    )
    await _reject_tax_group_count(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        expected_count=expected_count,
        published_count=published_count,
        estimated_row_bytes=(
            _V4_DICTIONARY_ESTIMATED_ROW_BYTES + max(int(source_bitmap_bytes), 0)
        ),
        heartbeat_callback=heartbeat_callback,
    )


@dataclass(frozen=True)
class _V4CompilerCopySpec:
    schema_name: str
    stage_table: str
    columns: tuple[str, ...]
    expected_byte_count: int
    expected_sha256: str
    label: str


class _V4MeasuredCopyReader:
    """Report exact bytes handed to PostgreSQL from one authenticated file."""

    def __init__(
        self,
        source: Any,
        progress_callback: Callable[[str, int], None] | None,
    ) -> None:
        self._source = source
        self._progress_callback = progress_callback

    def read(self, size: int = -1) -> bytes:
        """Read and report bytes from the already-authenticated descriptor."""

        chunk = self._source.read(size)
        if chunk and self._progress_callback is not None:
            self._progress_callback("copy_bytes", len(chunk))
        return chunk


@contextmanager
def _authenticated_v4_copy_file(
    path: Path,
    spec: _V4CompilerCopySpec,
):
    """Open one regular compiler file without following a replaced symlink."""

    try:
        path_metadata = path.lstat()
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise RuntimeError(f"PTG V4 {spec.label} is unavailable") from exc
    copy_file = os.fdopen(descriptor, "rb")
    try:
        opened_metadata = os.fstat(copy_file.fileno())
        _validate_v4_copy_file(
            copy_file,
            opened_metadata,
            spec,
            path_metadata=path_metadata,
            phase="at COPY open",
        )
        yield copy_file, opened_metadata
    finally:
        copy_file.close()


def _validate_v4_copy_file(
    copy_file: Any,
    observed_metadata: os.stat_result,
    spec: _V4CompilerCopySpec,
    *,
    path_metadata: os.stat_result,
    phase: str,
) -> None:
    """Authenticate one open descriptor against its expected file identity."""

    copy_file.seek(0)
    observed_sha256 = _sha256_path_from_open_file(copy_file)
    if (
        stat.S_ISLNK(path_metadata.st_mode)
        or not stat.S_ISREG(path_metadata.st_mode)
        or observed_metadata.st_dev != path_metadata.st_dev
        or observed_metadata.st_ino != path_metadata.st_ino
        or observed_metadata.st_size != spec.expected_byte_count
        or observed_metadata.st_mtime_ns != path_metadata.st_mtime_ns
        or observed_sha256 != spec.expected_sha256
    ):
        raise RuntimeError(f"PTG V4 {spec.label} changed {phase}")


async def _copy_authenticated_v4_compiler_input(
    session: Any,
    path: Path,
    *,
    spec: _V4CompilerCopySpec,
    progress_callback: Callable[[str, int], None] | None,
) -> None:
    """Authenticate the same open descriptor before and after binary COPY."""

    with _authenticated_v4_copy_file(path, spec) as (
        copy_file,
        opened_metadata,
    ):
        copy_file.seek(0)
        connection = await session.connection()
        raw_connection = await connection.get_raw_connection()
        driver_connection = getattr(
            raw_connection,
            "driver_connection",
            raw_connection,
        )
        copy_to_table = getattr(
            driver_connection,
            "copy_to_table",
            None,
        )
        if copy_to_table is None:
            raise NotImplementedError(
                "active database driver does not expose binary COPY"
            )
        await copy_to_table(
            spec.stage_table,
            source=_V4MeasuredCopyReader(copy_file, progress_callback),
            schema_name=spec.schema_name,
            columns=list(spec.columns),
            format="binary",
        )
        _validate_v4_copy_file(
            copy_file,
            os.fstat(copy_file.fileno()),
            spec,
            path_metadata=opened_metadata,
            phase="during COPY",
        )


@asynccontextmanager
async def _v4_taxonomy_scope_session(stage_table: str):
    """Pin one backend across TEMP creation and the read-only source lookup."""

    if db.engine is None or db.session_factory is None:
        await db.connect()
    if db.engine is None or db.session_factory is None:
        raise RuntimeError("PTG V4 taxonomy database is unavailable")
    quoted_stage = _quote_ident(stage_table)
    async with db.engine.connect() as connection:
        session = db.session_factory(bind=connection)
        try:
            async with session.begin():
                await session.execute(
                    db.text(
                        f"CREATE TEMP TABLE {quoted_stage} "
                        "(npi_key integer PRIMARY KEY CHECK (npi_key >= 0), "
                        " npi bigint NOT NULL UNIQUE "
                        " CHECK (npi BETWEEN 1000000000 AND 9999999999)) "
                        "ON COMMIT PRESERVE ROWS"
                    )
                )
            async with session.begin():
                await session.execute(
                    db.text(
                        "SET TRANSACTION ISOLATION LEVEL " "REPEATABLE READ READ ONLY"
                    )
                )
                yield session
        finally:
            cleanup_task = asyncio.create_task(
                _close_v4_taxonomy_scope(
                    session,
                    connection,
                    quoted_stage,
                )
            )
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
                raise


async def _close_v4_taxonomy_scope(
    session: Any,
    connection: Any,
    quoted_stage: str,
) -> None:
    """Drop one run-owned TEMP scope or invalidate its pinned connection."""

    try:
        if session.in_transaction():
            await session.rollback()
        async with session.begin():
            await session.execute(db.text(f"DROP TABLE IF EXISTS {quoted_stage}"))
    except BaseException:
        await session.close()
        await connection.invalidate()
        raise
    await session.close()


def _sha256_path_from_open_file(source: Any) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def _v4_taxonomy_copy_progress(
    progress_callback: Callable[[str, Mapping[str, int]], None] | None,
) -> Callable[[str, int], None] | None:
    """Adapt exact COPY byte movement to the importer progress contract."""

    if progress_callback is None:
        return None

    def report_copy_progress(name: str, count: int) -> None:
        """Forward one measured taxonomy COPY counter."""

        progress_callback(
            "taxonomy input preparation",
            {str(name): int(count)},
        )

    return report_copy_progress


def _taxonomy_input_complete(
    prepared_input: Mapping[str, Any],
    progress_callback: Callable[[str, Mapping[str, int]], None] | None,
) -> None:
    """Report the authenticated taxonomy-input boundary exactly once."""

    if progress_callback is None:
        return
    member_contract = _mapping(
        prepared_input.get("members"),
        "taxonomy member artifact",
    )
    rule_contracts = prepared_input.get("rules")
    if not isinstance(rule_contracts, list):
        raise RuntimeError("PTG V4 taxonomy preparation rules changed")
    progress_callback(
        "taxonomy input preparation",
        {
            "completed_rows": sum(
                int(_mapping(rule, "taxonomy rule")["member_count"])
                for rule in rule_contracts
            ),
            "completed_bytes": int(member_contract["byte_count"]),
            "completed_batches": 1,
        },
    )


def _taxonomy_copy_complete(
    compilation: V4GraphCompilationResult,
    progress_callback: Callable[[str, Mapping[str, int]], None] | None,
) -> None:
    """Report the selected taxonomy COPY only after publication succeeds."""

    if progress_callback is None:
        return
    taxonomy_artifact = _v4_compiler_artifact(
        compilation,
        "inferred_taxonomy_candidates",
    )
    progress_callback(
        "selected taxonomy copy publication",
        {
            "published_rows": int(taxonomy_artifact.row_count),
            "published_bytes": int(taxonomy_artifact.byte_count),
            "completed_batches": 1,
        },
    )


async def _wait_for_v4_graph_compilation(
    compile_task: asyncio.Task[V4GraphCompilationResult],
    touch_build: Callable[[], Awaitable[Any]],
) -> V4GraphCompilationResult:
    """Wait for native compilation while retaining the shared-build lease."""

    while True:
        try:
            return await asyncio.wait_for(
                asyncio.shield(compile_task),
                timeout=30.0,
            )
        except TimeoutError:
            await touch_build()


def _cleanup_v4_graph_inputs(
    input_directory: Path,
    npi_scope: V4GraphNpiScopePreparation | None,
    taxonomy_input: Mapping[str, Any] | None,
) -> None:
    """Remove every run-owned compiler input after success or failure."""

    if taxonomy_input is not None:
        Path(str(taxonomy_input["members"]["path"])).unlink(missing_ok=True)
    if npi_scope is not None:
        npi_scope.cleanup()
    shutil.rmtree(input_directory, ignore_errors=True)


async def _compile_v4_provider_graph(
    *,
    graph_artifact_entries: Iterable[Mapping[str, Any]],
    provider_set_key_map_path: Path,
    work_directory: Path,
    schema_name: str,
    touch_build: Callable[[], Awaitable[Any]],
    progress_callback: Callable[[str, Mapping[str, int]], None] | None,
) -> V4GraphCompilationResult:
    """Prepare, compile, and always remove one adaptive V4 input bundle."""

    input_directory = work_directory / f"provider-graph-v4-input-{uuid.uuid4().hex}"
    input_directory.mkdir(mode=0o700)
    npi_scope: V4GraphNpiScopePreparation | None = None
    taxonomy_input: Mapping[str, Any] | None = None
    compile_task: asyncio.Task[V4GraphCompilationResult] | None = None
    try:
        npi_scope = await prepare_provider_graph_v4_npi_scope_rust(
            graph_artifact_entries=graph_artifact_entries,
            output_path=input_directory / "npi-scope.copy",
        )
        await touch_build()
        taxonomy_input = await _prepare_v4_taxonomy_compiler_input(
            npi_scope,
            schema_name=schema_name,
            work_directory=input_directory,
            progress_callback=progress_callback,
        )
        await touch_build()
        compile_task = asyncio.create_task(
            compile_provider_graph_v4_rust(
                graph_artifact_entries=npi_scope.graph_artifact_entries,
                provider_set_key_map_path=provider_set_key_map_path,
                npi_scope=npi_scope,
                inferred_taxonomy=taxonomy_input,
                output_directory=work_directory / "provider-graph-v4-native",
            )
        )
        return await _wait_for_v4_graph_compilation(compile_task, touch_build)
    finally:
        if compile_task is not None and not compile_task.done():
            compile_task.cancel()
            await asyncio.gather(compile_task, return_exceptions=True)
        _cleanup_v4_graph_inputs(input_directory, npi_scope, taxonomy_input)


async def _copy_v4_taxonomy_scope(
    session: Any,
    npi_scope: V4GraphNpiScopePreparation,
    *,
    stage_table: str,
    progress_callback: Callable[[str, int], None] | None,
) -> None:
    """COPY and validate one authenticated dense source-local NPI scope."""

    scope_manifest = npi_scope.manifest
    await _copy_authenticated_v4_compiler_input(
        session,
        npi_scope.copy_path,
        spec=_V4CompilerCopySpec(
            schema_name="pg_temp",
            stage_table=stage_table,
            columns=("npi_key", "npi"),
            expected_byte_count=int(scope_manifest["output_byte_count"]),
            expected_sha256=str(scope_manifest["output_sha256"]),
            label="NPI scope prepass",
        ),
        progress_callback=progress_callback,
    )
    temporary_schema = _quote_ident("pg_temp")
    quoted_stage = _quote_ident(stage_table)
    scope_result = await session.execute(
        db.text(
            f"SELECT COUNT(*)::bigint, "
            f"COUNT(DISTINCT npi_key)::bigint, "
            f"COUNT(DISTINCT npi)::bigint "
            f"FROM {temporary_schema}.{quoted_stage}"
        )
    )
    expected_count = int(scope_manifest["row_count"])
    if tuple(map(int, scope_result.one())) != (
        expected_count,
        expected_count,
        expected_count,
    ):
        raise RuntimeError("PTG V4 NPI scope stage changed before taxonomy lookup")


async def _prepare_v4_taxonomy_compiler_input(
    npi_scope: V4GraphNpiScopePreparation,
    *,
    schema_name: str,
    work_directory: Path,
    progress_callback: Callable[[str, Mapping[str, int]], None] | None,
) -> Mapping[str, Any]:
    """Resolve bounded taxonomy evidence from one immutable scope transaction."""

    from process.ptg_parts.ptg2_v4_taxonomy_candidates import (
        prepare_v4_inferred_taxonomy_compiler_input,
    )

    stage_table = f"ptg2_v4_npi_scope_input_{uuid.uuid4().hex[:20]}"
    members_path = work_directory / "v4-inferred-taxonomy-members.u32le"
    members_path.unlink(missing_ok=True)
    try:
        scope_manifest = npi_scope.manifest
        async with _v4_taxonomy_scope_session(stage_table) as session:
            await _copy_v4_taxonomy_scope(
                session,
                npi_scope,
                stage_table=stage_table,
                progress_callback=_v4_taxonomy_copy_progress(progress_callback),
            )
            prepared_input = await prepare_v4_inferred_taxonomy_compiler_input(
                session,
                schema_name=schema_name,
                npi_scope_stage_table=stage_table,
                npi_scope_stage_schema_name="pg_temp",
                npi_scope_sha256=str(scope_manifest["output_sha256"]),
                rules=INFERRED_PROVIDER_TAXONOMY_RULES,
                members_path=members_path,
            )
            _taxonomy_input_complete(prepared_input, progress_callback)
            return prepared_input
    except BaseException:
        members_path.unlink(missing_ok=True)
        raise


async def _drop_v4_dictionary_stages(
    schema: str,
    stages: Iterable[str],
) -> None:
    """Remove only this publication attempt's randomized unlogged stages."""

    stage_names = tuple(stages)
    if not stage_names:
        return
    await db.status(
        "DROP TABLE IF EXISTS "
        + ", ".join(f"{schema}.{_quote_ident(stage)}" for stage in stage_names)
        + ";"
    )


async def _cleanup_v4_dictionary_attempt(
    *,
    schema: str,
    stages: Iterable[str],
    prepared_tax_identity_source: Any,
    preserve_primary_error: bool,
) -> None:
    """Drop publication stages and always release the ephemeral source COPY."""

    try:
        await _drop_v4_dictionary_stages(schema, stages)
    except BaseException:
        if not preserve_primary_error:
            raise
    finally:
        if prepared_tax_identity_source is not None:
            prepared_tax_identity_source.cleanup()


async def _publish_v4_dictionaries_and_maps(
    compilation: V4GraphCompilationResult,
    *,
    publication_context: _V4AtomicPublishContext,
    compressed_acquisition_bytes: int,
    empty_npi_tin_only_normalization_count: int,
    tax_identity_source_artifacts: Iterable[Mapping[str, Any]] | None = None,
    progress_callback: Callable[[str, int], None] | None = None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> tuple[
    V4CASBlockStagePublication,
    V4SnapshotMapSummary,
    V4InferredTaxonomyPublication,
    _V4TaxIdentityPublication,
    TaxIdentitySourcePublication | None,
]:
    """Atomically publish CAS reachability, dictionaries, metadata, and maps."""

    if int(compressed_acquisition_bytes) <= 0:
        raise RuntimeError("PTG V4 compressed acquisition bytes must be positive")
    if int(empty_npi_tin_only_normalization_count) < 0:
        raise RuntimeError(
            "PTG V4 empty-NPI TIN-only normalization count cannot be negative"
        )
    schema_name = publication_context.schema_name
    block_stage = publication_context.block_stage
    snapshot_key = publication_context.snapshot_key
    build_token = publication_context.build_token
    schema = _quote_ident(schema_name)
    token = uuid.uuid4().hex[:20]
    expected_group_count = int(compilation.observe.get("group_count") or 0)
    expected_component_count = int(compilation.observe.get("component_count") or 0)
    expected_npi_count = int(compilation.observe.get("npi_count") or 0)
    root_representation = (
        "pattern_v1" if compilation.selected_layout == "pattern" else "direct_v1"
    )
    root_pattern_count = (
        int(compilation.observe.get("pattern_count") or 0)
        if root_representation == "pattern_v1"
        else 0
    )
    expected_prefix_owner_count = int(
        compilation.observe.get("npi_prefix_override_owner_count") or 0
    )
    expected_prefix_member_count = int(
        compilation.observe.get("npi_prefix_override_member_count") or 0
    )
    tax_identity_contract = _validated_v4_tax_identity_contract(compilation)
    tax_identity_artifact_bytes = _v4_tax_artifact_byte_count(compilation)
    source_artifacts = tuple(tax_identity_source_artifacts or ())
    taxonomy_artifact = _v4_compiler_artifact(
        compilation,
        "inferred_taxonomy_candidates",
    )
    reference_artifact = _v4_compiler_artifact(
        compilation,
        "graph_references",
    )
    prefix_target = int(compilation.summary["npi_prefix_target"])
    group_stage = f"ptg2_v4_group_stage_{token}"
    component_stage = f"ptg2_v4_component_stage_{token}"
    npi_stage = f"ptg2_v4_npi_stage_{token}"
    pattern_stage = f"ptg2_v4_pattern_stage_{token}"
    prefix_stage = f"ptg2_v4_npi_prefix_stage_{token}"
    tax_identity_stage = f"ptg2_v4_tax_identity_stage_{token}"
    group_tax_identity_stage = f"ptg2_v4_group_tax_identity_stage_{token}"
    stages = [
        group_stage,
        component_stage,
        npi_stage,
        prefix_stage,
        tax_identity_stage,
        group_tax_identity_stage,
    ]
    if compilation.pattern_copy_path is not None:
        stages.append(pattern_stage)
    stage_create_statements = [
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(group_stage)} "
        "(provider_group_key integer PRIMARY KEY CHECK (provider_group_key >= 0), "
        " provider_group_global_id_128 bytea NOT NULL UNIQUE "
        " CHECK (octet_length(provider_group_global_id_128) = 16))",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(component_stage)} "
        "(component_key integer PRIMARY KEY CHECK (component_key >= 0), "
        " component_global_id_128 bytea NOT NULL "
        " CHECK (octet_length(component_global_id_128) = 16))",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(npi_stage)} "
        "(npi_key integer PRIMARY KEY CHECK (npi_key >= 0), "
        " npi bigint NOT NULL UNIQUE CHECK (npi BETWEEN 1000000000 AND 9999999999))",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(prefix_stage)} "
        "(provider_set_key integer PRIMARY KEY CHECK (provider_set_key >= 0), "
        f" member_count integer NOT NULL CHECK (member_count BETWEEN 0 AND {prefix_target}), "
        " member_digest bytea NOT NULL CHECK (octet_length(member_digest) = 32))",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(tax_identity_stage)} "
        "(tin_key integer PRIMARY KEY CHECK (tin_key >= 0), "
        " tin_id_128 bytea NOT NULL "
        " CHECK (octet_length(tin_id_128) = 16), "
        " tin_hmac_sha256 bytea NOT NULL UNIQUE "
        " CHECK (octet_length(tin_hmac_sha256) = 32))",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(group_tax_identity_stage)} "
        "(provider_group_global_id_128 bytea PRIMARY KEY "
        " CHECK (octet_length(provider_group_global_id_128) = 16), "
        " tax_identity_state text NOT NULL, tin_key integer, "
        " source_bitmap bytea NOT NULL)",
    ]
    if compilation.pattern_copy_path is not None:
        stage_create_statements.append(
            f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(pattern_stage)} "
            "(pattern_key integer PRIMARY KEY CHECK (pattern_key >= 0), "
            " pattern_digest bytea NOT NULL "
            " CHECK (octet_length(pattern_digest) = 32), "
            " set_count bigint NOT NULL CHECK (set_count >= 0))"
        )
    prepared_tax_identity_source = (
        prepare_tax_identity_source_projection(
            source_artifacts,
            scratch_parent=(
                compilation.provider_group_tax_identity_copy_path.parent
            ),
            token_policy_id=tax_identity_contract.token_policy_id,
            token_policy_descriptor_sha256=(
                tax_identity_contract.token_policy_descriptor_sha256
            ),
            source_ordinal_map=tax_identity_contract.source_ordinal_map,
            source_ordinal_map_digest=(
                tax_identity_contract.source_ordinal_map_digest
            ),
            aggregate_tax_content_digest=tax_identity_contract.content_digest,
        )
        if source_artifacts
        else None
    )
    created_stages: list[str] = []
    try:
        for stage, statement in zip(
            stages,
            stage_create_statements,
            strict=True,
        ):
            await db.status(statement)
            created_stages.append(stage)
            if progress_callback is not None:
                progress_callback("publish_batches", 1)
    except BaseException:
        await _cleanup_v4_dictionary_attempt(
            schema=schema,
            stages=created_stages,
            prepared_tax_identity_source=prepared_tax_identity_source,
            preserve_primary_error=True,
        )
        raise
    try:
        await _copy_binary_file_to_stage(
            compilation.group_copy_path,
            schema_name=schema_name,
            stage_table=group_stage,
            columns=("provider_group_key", "provider_group_global_id_128"),
            **_progress_callback_kwargs(progress_callback),
        )
        await _copy_binary_file_to_stage(
            compilation.component_copy_path,
            schema_name=schema_name,
            stage_table=component_stage,
            columns=("component_key", "component_global_id_128"),
            **_progress_callback_kwargs(progress_callback),
        )
        await _copy_binary_file_to_stage(
            compilation.npi_copy_path,
            schema_name=schema_name,
            stage_table=npi_stage,
            columns=("npi_key", "npi"),
            **_progress_callback_kwargs(progress_callback),
        )
        await _copy_binary_file_to_stage(
            compilation.provider_set_npi_prefix_override_copy_path,
            schema_name=schema_name,
            stage_table=prefix_stage,
            columns=("provider_set_key", "member_count", "member_digest"),
            **_progress_callback_kwargs(progress_callback),
        )
        await _copy_binary_file_to_stage(
            compilation.provider_tax_identity_copy_path,
            schema_name=schema_name,
            stage_table=tax_identity_stage,
            columns=("tin_key", "tin_id_128", "tin_hmac_sha256"),
            **_progress_callback_kwargs(progress_callback),
        )
        await _copy_binary_file_to_stage(
            compilation.provider_group_tax_identity_copy_path,
            schema_name=schema_name,
            stage_table=group_tax_identity_stage,
            columns=(
                "provider_group_global_id_128",
                "tax_identity_state",
                "tin_key",
                "source_bitmap",
            ),
            **_progress_callback_kwargs(progress_callback),
        )
        if compilation.pattern_copy_path is not None:
            await _copy_binary_file_to_stage(
                compilation.pattern_copy_path,
                schema_name=schema_name,
                stage_table=pattern_stage,
                columns=("pattern_key", "pattern_digest", "set_count"),
                **_progress_callback_kwargs(progress_callback),
            )
        dictionary_stages = [
            _V4DenseDictionaryStage(
                stage_table=group_stage,
                key_name="provider_group_key",
                expected_count=expected_group_count,
                target_table="ptg2_v3_provider_group",
                columns=(
                    "provider_group_key",
                    "provider_group_global_id_128",
                ),
                value_predicate=("octet_length(provider_group_global_id_128) = 16"),
            ),
            _V4DenseDictionaryStage(
                stage_table=component_stage,
                key_name="component_key",
                expected_count=expected_component_count,
                target_table="ptg2_v4_provider_component",
                columns=("component_key", "component_global_id_128"),
                value_predicate="octet_length(component_global_id_128) = 16",
            ),
            _V4DenseDictionaryStage(
                stage_table=npi_stage,
                key_name="npi_key",
                expected_count=expected_npi_count,
                target_table=PTG2_V4_NPI_TABLE,
                columns=("npi_key", "npi"),
                value_predicate="npi BETWEEN 1000000000 AND 9999999999",
            ),
            _V4DenseDictionaryStage(
                stage_table=prefix_stage,
                key_name="provider_set_key",
                expected_count=expected_prefix_owner_count,
                target_table="ptg2_v4_provider_set_npi_prefix",
                columns=("provider_set_key", "member_count", "member_digest"),
                value_predicate=(
                    f"member_count BETWEEN 0 AND {prefix_target} "
                    "AND octet_length(member_digest) = 32"
                ),
                sum_expression="member_count",
                expected_sum=expected_prefix_member_count,
                dense_keys=False,
            ),
        ]
        if compilation.pattern_copy_path is not None:
            dictionary_stages.append(
                _V4DenseDictionaryStage(
                    stage_table=pattern_stage,
                    key_name="pattern_key",
                    expected_count=root_pattern_count,
                    target_table="ptg2_v4_pattern",
                    columns=("pattern_key", "pattern_digest", "set_count"),
                    value_predicate=(
                        "octet_length(pattern_digest) = 32 AND set_count >= 0"
                    ),
                )
            )
        tax_dictionary_stage = _V4DenseDictionaryStage(
            stage_table=tax_identity_stage,
            key_name="tin_key",
            expected_count=tax_identity_contract.tax_identity_count,
            target_table="ptg2_provider_tax_identity",
            columns=("tin_key", "tin_id_128", "tin_hmac_sha256"),
            value_predicate=(
                "octet_length(tin_id_128) = 16 "
                "AND octet_length(tin_hmac_sha256) = 32 "
                "AND tin_id_128 = substring(tin_hmac_sha256 FROM 1 FOR 16)"
            ),
        )
        async with db.transaction() as session:
            tax_identity_source_stage = (
                await stage_tax_identity_source_projection(
                    session,
                    prepared_tax_identity_source,
                )
                if prepared_tax_identity_source is not None
                else None
            )
            snapshot_parameter_map = {"snapshot_key": int(snapshot_key)}
            await lock_v4_shared_layout_for_map_write(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
            )
            # Protect payload-free reuse rows as soon as the layout fence is
            # held. GC does not acquire that layout fence, so delaying these
            # globally ordered block locks until after dictionary validation
            # would let it remove a reused CAS row during a long validation.
            # The helper restores the caller's prior lock_timeout before the
            # remaining publication SQL proceeds.
            cas_publication = await _publish_v4_cas_in_session(
                session,
                schema_name=schema_name,
                stage_table=block_stage,
                progress_callback=progress_callback,
            )
            taxonomy_stage = await stage_v4_inferred_taxonomy_compiler_copy(
                session,
                copy_path=compilation.inferred_taxonomy_copy_path,
                expected_byte_count=taxonomy_artifact.byte_count,
                expected_sha256=taxonomy_artifact.sha256,
            )
            for dictionary_stage in dictionary_stages:
                await _validate_v4_dictionary_stage(
                    session,
                    schema=schema,
                    stage=dictionary_stage,
                    progress_callback=progress_callback,
                    heartbeat_callback=heartbeat_callback,
                )
            await _validate_v4_dictionary_stage(
                session,
                schema=schema,
                stage=tax_dictionary_stage,
                progress_callback=progress_callback,
                heartbeat_callback=heartbeat_callback,
            )
            await _validate_v4_tax_identity_stages(
                session,
                schema=schema,
                group_dictionary_stage=group_stage,
                tax_identity_stage=tax_identity_stage,
                group_tax_identity_stage=group_tax_identity_stage,
                contract=tax_identity_contract,
                progress_callback=progress_callback,
                heartbeat_callback=heartbeat_callback,
            )

            # The CAS rows and candidate cancellation above remain locked
            # until this authenticated map makes them durably reachable.  The
            # transaction therefore exposes neither half of the publication
            # without the other. Metadata tables are trigger-fenced by the
            # building map root and attach beneath it later in this transaction.
            map_summary = await publish_v4_snapshot_maps(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
                representation=root_representation,
                references=_iter_v4_block_references(
                    compilation.reference_manifest_path,
                    expected_byte_count=int(reference_artifact.byte_count),
                    expected_sha256=str(reference_artifact.sha256),
                    expected_row_count=int(reference_artifact.row_count),
                ),
                **_progress_callback_kwargs(progress_callback),
            )
            _require_v4_atomic_map_publication(
                compilation,
                cas_publication,
                map_summary,
            )

            tax_identity_manifest = await _publish_v4_tax_identity_manifest(
                session,
                schema=schema,
                snapshot_key=int(snapshot_key),
                contract=tax_identity_contract,
            )
            if progress_callback is not None:
                progress_callback("published_dictionary_rows", 1)
                progress_callback("publish_batches", 1)

            for dictionary_stage in dictionary_stages:
                await _publish_v4_dictionary_stage_ranges(
                    session,
                    schema=schema,
                    snapshot_key=int(snapshot_key),
                    stage=dictionary_stage,
                    progress_callback=progress_callback,
                    heartbeat_callback=heartbeat_callback,
                )
            await _publish_v4_dictionary_stage_ranges(
                session,
                schema=schema,
                snapshot_key=int(snapshot_key),
                stage=tax_dictionary_stage,
                progress_callback=progress_callback,
                heartbeat_callback=heartbeat_callback,
            )
            await _publish_v4_tax_group_ranges(
                session,
                schema=schema,
                snapshot_key=int(snapshot_key),
                stage_table=group_tax_identity_stage,
                expected_count=tax_identity_contract.provider_group_count,
                progress_callback=progress_callback,
                source_bitmap_bytes=tax_identity_contract.source_bitmap_bytes,
                heartbeat_callback=heartbeat_callback,
            )
            diagnostic_parameters_by_name = {
                "snapshot_key": int(snapshot_key),
                "compressed_acquisition_bytes": int(compressed_acquisition_bytes),
                "input_factor_bytes": int(
                    compilation.resource_admission["input_factor_bytes"]
                ),
                "factor_edge_count": int(
                    compilation.resource_admission["factor_edge_count"]
                ),
                "empty_npi_tin_only_normalization_count": int(
                    empty_npi_tin_only_normalization_count
                ),
                "npi_prefix_target": prefix_target,
                "max_set_patterns_per_set": int(
                    compilation.summary["max_set_patterns_per_set"]
                ),
                "max_set_components_per_fallback_set": int(
                    compilation.summary["max_set_components_per_fallback_set"]
                ),
                "max_online_group_keys_per_set": int(
                    compilation.summary["max_online_group_keys_per_set"]
                ),
                "max_online_source_owners_per_set": int(
                    compilation.summary["max_online_source_owners_per_set"]
                ),
                "max_online_source_members_per_set": int(
                    compilation.summary["max_online_source_members_per_set"]
                ),
                "max_online_source_pages_per_set": int(
                    compilation.summary["max_online_source_pages_per_set"]
                ),
                "max_online_source_bytes_per_set": int(
                    compilation.summary["max_online_source_bytes_per_set"]
                ),
                "online_group_npi_batch_size": int(
                    compilation.summary["online_group_npi_batch_size"]
                ),
                "max_online_group_npi_members_per_set": int(
                    compilation.summary["max_online_group_npi_members_per_set"]
                ),
                "max_online_group_npi_locator_pages_per_set": int(
                    compilation.summary["max_online_group_npi_locator_pages_per_set"]
                ),
                "max_online_group_npi_member_pages_per_set": int(
                    compilation.summary["max_online_group_npi_member_pages_per_set"]
                ),
                "max_online_group_npi_bytes_per_set": int(
                    compilation.summary["max_online_group_npi_bytes_per_set"]
                ),
                "max_online_group_npi_batches_per_set": int(
                    compilation.summary["max_online_group_npi_batches_per_set"]
                ),
                "provider_expansion_rate_page_rows": int(
                    compilation.summary["provider_expansion_rate_page_rows"]
                ),
                "max_online_provider_expansion_rate_rows": int(
                    compilation.summary["max_online_provider_expansion_rate_rows"]
                ),
                "max_online_provider_expansion_provider_sets": int(
                    compilation.summary["max_online_provider_expansion_provider_sets"]
                ),
                "max_online_provider_expansion_graph_batches": int(
                    compilation.summary["max_online_provider_expansion_graph_batches"]
                ),
                "maximum_group_npi_member_work": int(
                    compilation.observe["maximum_online_group_npi_member_work"]
                ),
                "maximum_group_npi_locator_page_work": int(
                    compilation.observe["maximum_online_group_npi_locator_page_work"]
                ),
                "maximum_group_npi_member_page_work": int(
                    compilation.observe["maximum_online_group_npi_member_page_work"]
                ),
                "maximum_group_npi_byte_work": int(
                    compilation.observe["maximum_online_group_npi_byte_work"]
                ),
                "maximum_group_npi_batch_work": int(
                    compilation.observe["maximum_online_group_npi_batch_work"]
                ),
                "group_unsafe_set_count": int(
                    compilation.observe["npi_prefix_group_unsafe_set_count"]
                ),
                "physical_unsafe_set_count": int(
                    compilation.observe["npi_prefix_physical_unsafe_set_count"]
                ),
                "simulated_set_count": int(
                    compilation.observe["npi_prefix_simulated_set_count"]
                ),
                "override_owner_count": expected_prefix_owner_count,
                "override_member_count": expected_prefix_member_count,
                "override_raw_bytes": int(
                    compilation.observe["npi_prefix_override_raw_bytes"]
                ),
                "worst_provider_set_key": compilation.observe[
                    "npi_prefix_worst_provider_set_key"
                ],
                "worst_groups_to_target": int(
                    compilation.observe["npi_prefix_worst_groups_to_target"]
                ),
                "worst_uses_override": bool(
                    compilation.observe["npi_prefix_worst_provider_set_uses_override"]
                ),
                "worst_uses_component_fallback": bool(
                    compilation.observe["npi_prefix_worst_uses_component_fallback"]
                ),
                "worst_member_count": int(
                    compilation.observe["npi_prefix_worst_member_count"]
                ),
                "worst_member_digest": (
                    bytes.fromhex(
                        str(compilation.observe["npi_prefix_worst_member_digest"])
                    )
                    if compilation.observe.get("npi_prefix_worst_member_digest")
                    is not None
                    else None
                ),
                "worst_source_owner_work": int(
                    compilation.observe["npi_prefix_worst_source_owner_work"]
                ),
                "worst_source_member_work": int(
                    compilation.observe["npi_prefix_worst_source_member_work"]
                ),
                "worst_source_page_work": int(
                    compilation.observe["npi_prefix_worst_source_page_work"]
                ),
                "worst_source_byte_work": int(
                    compilation.observe["npi_prefix_worst_source_byte_work"]
                ),
                "worst_group_npi_member_work": int(
                    compilation.observe["npi_prefix_worst_group_npi_member_work"]
                ),
                "worst_group_npi_locator_page_work": int(
                    compilation.observe["npi_prefix_worst_group_npi_locator_page_work"]
                ),
                "worst_group_npi_member_page_work": int(
                    compilation.observe["npi_prefix_worst_group_npi_member_page_work"]
                ),
                "worst_group_npi_byte_work": int(
                    compilation.observe["npi_prefix_worst_group_npi_byte_work"]
                ),
                "worst_group_npi_batch_work": int(
                    compilation.observe["npi_prefix_worst_group_npi_batch_work"]
                ),
                "worst_online_provider_set_key": compilation.observe[
                    "npi_prefix_worst_online_provider_set_key"
                ],
                "worst_online_groups_to_target": int(
                    compilation.observe["npi_prefix_worst_online_groups_to_target"]
                ),
                "worst_online_groups_to_target_exact": bool(
                    compilation.observe[
                        "npi_prefix_worst_online_groups_to_target_exact"
                    ]
                ),
                "worst_online_uses_component_fallback": bool(
                    compilation.observe[
                        "npi_prefix_worst_online_uses_component_fallback"
                    ]
                ),
                "worst_online_group_work_bound": int(
                    compilation.observe["npi_prefix_worst_online_group_work_bound"]
                ),
                "worst_online_member_count": int(
                    compilation.observe["npi_prefix_worst_online_member_count"]
                ),
                "worst_online_member_digest": (
                    bytes.fromhex(
                        str(
                            compilation.observe["npi_prefix_worst_online_member_digest"]
                        )
                    )
                    if compilation.observe.get("npi_prefix_worst_online_member_digest")
                    is not None
                    else None
                ),
                "worst_online_source_owner_work": int(
                    compilation.observe["npi_prefix_worst_online_source_owner_work"]
                ),
                "worst_online_source_member_work": int(
                    compilation.observe["npi_prefix_worst_online_source_member_work"]
                ),
                "worst_online_source_page_work": int(
                    compilation.observe["npi_prefix_worst_online_source_page_work"]
                ),
                "worst_online_source_byte_work": int(
                    compilation.observe["npi_prefix_worst_online_source_byte_work"]
                ),
                "worst_online_group_npi_member_work": int(
                    compilation.observe["npi_prefix_worst_online_group_npi_member_work"]
                ),
                "worst_online_group_npi_locator_page_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_npi_locator_page_work"
                    ]
                ),
                "worst_online_group_npi_member_page_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_npi_member_page_work"
                    ]
                ),
                "worst_online_group_npi_byte_work": int(
                    compilation.observe["npi_prefix_worst_online_group_npi_byte_work"]
                ),
                "worst_online_group_npi_batch_work": int(
                    compilation.observe["npi_prefix_worst_online_group_npi_batch_work"]
                ),
            }
            diagnostic_columns = (
                *PTG2_V4_GRAPH_RESOURCE_FIELDS,
                *PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
            )
            diagnostic_column_sql = ", ".join(diagnostic_columns)
            diagnostic_value_sql = ", ".join(
                f":{column}" for column in diagnostic_columns
            )
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v4_provider_graph_diagnostic
                        (snapshot_key, {diagnostic_column_sql})
                    VALUES
                        (:snapshot_key, {diagnostic_value_sql})
                    ON CONFLICT DO NOTHING
                    """
                ),
                diagnostic_parameters_by_name,
            )
            diagnostic_result = await session.execute(
                db.text(
                    f"""
                    SELECT {diagnostic_column_sql}
                      FROM {schema}.ptg2_v4_provider_graph_diagnostic
                     WHERE snapshot_key = :snapshot_key
                    """
                ),
                snapshot_parameter_map,
            )
            if tuple(diagnostic_result.one()) != tuple(
                diagnostic_parameters_by_name[column] for column in diagnostic_columns
            ):
                raise RuntimeError("PTG V4 persisted graph diagnostics changed")

            if compilation.pattern_copy_path is None:
                has_unexpected_patterns = await session.scalar(
                    db.text(
                        f"""
                        SELECT EXISTS (
                            SELECT 1
                              FROM {schema}.ptg2_v4_pattern
                             WHERE snapshot_key = :snapshot_key
                             LIMIT 1
                        )
                        """
                    ),
                    snapshot_parameter_map,
                )
                if bool(has_unexpected_patterns):
                    raise RuntimeError("PTG V4 persisted dictionary rows changed")

            await publish_v4_relation_manifests(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
                entries=tuple(
                    sorted(
                        compilation.relation_summaries, key=lambda row: row["relation"]
                    )
                ),
            )
            await publish_v4_heavy_owners(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
                entries=tuple(
                    {
                        **dict(bitmap_summary),
                        "fragment_count": int(bitmap_summary["block_count"]),
                    }
                    for bitmap_summary in sorted(
                        compilation.heavy_bitmaps,
                        key=lambda summary: (
                            summary["relation"],
                            int(summary["owner_key"]),
                        ),
                    )
                ),
            )

            # Candidate keys are snapshot-local NPI coordinates. Publish this
            # immutable sidecar only after the dense dictionary and complete
            # authenticated building graph are available in this transaction.
            taxonomy_publication = (
                await publish_prepared_v4_inferred_taxonomy_candidates(
                    session,
                    schema_name=schema_name,
                    snapshot_key=int(snapshot_key),
                    build_token=build_token,
                    stage_table=taxonomy_stage.table_name,
                    rules=INFERRED_PROVIDER_TAXONOMY_RULES,
                    npi_count=expected_npi_count,
                    pattern_count=root_pattern_count,
                )
            )
            if progress_callback is not None:
                progress_callback(
                    "published_dictionary_rows",
                    int(taxonomy_publication.rule_count)
                    + int(taxonomy_publication.observe_only_rule_count),
                )
                progress_callback("publish_batches", 1)
            tax_identity_source_publication = (
                await publish_staged_tax_identity_source_projection(
                    session,
                    schema_name=schema_name,
                    logical_snapshot_id=publication_context.logical_snapshot_id,
                    snapshot_key=int(snapshot_key),
                    staged=tax_identity_source_stage,
                    prepared=prepared_tax_identity_source,
                    heartbeat_callback=heartbeat_callback,
                )
                if prepared_tax_identity_source is not None
                and tax_identity_source_stage is not None
                else None
            )
            return (
                cas_publication,
                map_summary,
                taxonomy_publication,
                _V4TaxIdentityPublication(
                    manifest={
                        **dict(tax_identity_manifest),
                        "artifact_byte_count": (tax_identity_artifact_bytes),
                    },
                    provider_group_count=(tax_identity_contract.provider_group_count),
                    tax_identity_count=tax_identity_contract.tax_identity_count,
                    artifact_byte_count=tax_identity_artifact_bytes,
                ),
                tax_identity_source_publication,
            )
    finally:
        await _cleanup_v4_dictionary_attempt(
            schema=schema,
            stages=stages,
            prepared_tax_identity_source=prepared_tax_identity_source,
            preserve_primary_error=sys.exc_info()[0] is not None,
        )


async def _publish_v4_graph(
    compilation: V4GraphCompilationResult,
    *,
    publication_context: _V4GraphCoordinates,
    compressed_acquisition_bytes: int,
    empty_npi_tin_only_normalization_count: int,
    tax_identity_source_artifacts: Iterable[Mapping[str, Any]] | None = None,
    progress_callback: Callable[[str, int], None] | None = None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> _V4GraphPublication:
    """Publish graph blocks only to CAS, then make packed maps authoritative."""

    schema_name = publication_context.schema_name
    snapshot_key = publication_context.snapshot_key
    build_token = publication_context.build_token
    _require_v4_compilation_layout_selection(compilation)
    block_artifact = _v4_compiler_artifact(compilation, "graph_blocks")
    block_stage = shared_block_stage_name(f"v4-graph-{snapshot_key}")
    await create_shared_block_stage(schema_name=schema_name, stage_table=block_stage)
    try:
        await copy_shared_block_binary_file(
            compilation.block_copy_path,
            schema_name=schema_name,
            stage_table=block_stage,
            expected_copy_bytes=int(block_artifact.byte_count),
            expected_copy_sha256=str(block_artifact.sha256),
            reuse_existing=True,
            **_progress_callback_kwargs(progress_callback),
        )
        (
            cas_publication,
            map_summary,
            taxonomy_publication,
            tax_identity_publication,
            tax_identity_source_publication,
        ) = await _publish_v4_dictionaries_and_maps(
            compilation,
            publication_context=_V4AtomicPublishContext(
                schema_name=schema_name,
                block_stage=block_stage,
                logical_snapshot_id=publication_context.logical_snapshot_id,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
            ),
            compressed_acquisition_bytes=int(compressed_acquisition_bytes),
            empty_npi_tin_only_normalization_count=int(
                empty_npi_tin_only_normalization_count
            ),
            tax_identity_source_artifacts=tax_identity_source_artifacts,
            **_progress_callback_kwargs(progress_callback),
            heartbeat_callback=heartbeat_callback,
        )
    except BaseException:
        await _queue_failed_v4_graph_blocks(
            schema_name=schema_name,
            reference_manifest_path=compilation.reference_manifest_path,
        )
        raise
    finally:
        await db.status(
            "DROP TABLE IF EXISTS "
            f"{_quote_ident(schema_name)}.{_quote_ident(block_stage)};"
        )

    artifact_contracts = tuple(
        {
            "name": artifact.name,
            "sha256": artifact.sha256,
            "row_count": int(artifact.row_count),
            "byte_count": int(artifact.byte_count),
        }
        for artifact in compilation.output_artifacts
    )
    adaptive_layout = v4_adaptive_layout_decision_from_summary(compilation.summary)
    support_digest = shared_support_digest(
        {
            "contract_version": 2,
            "compiler_format": compilation.summary.get("format"),
            "selected_layout": compilation.selected_layout,
            "adaptive_layout": adaptive_layout,
            "map_digest": map_summary.map_digest.hex(),
            "artifacts": artifact_contracts,
            "relation_summaries": tuple(compilation.relation_summaries),
            "heavy_bitmaps": tuple(compilation.heavy_bitmaps),
            "inferred_taxonomy_candidates": dict(taxonomy_publication.manifest),
            "provider_tax_identity": dict(tax_identity_publication.manifest),
            "provider_tax_identity_source": (
                tax_identity_source_publication.as_dict()
                if tax_identity_source_publication is not None
                else {}
            ),
            "observe": dict(compilation.observe),
            "resource_admission": {
                "compressed_acquisition_bytes": int(compressed_acquisition_bytes),
                "empty_npi_tin_only_normalization_count": int(
                    empty_npi_tin_only_normalization_count
                ),
                **dict(compilation.resource_admission),
            },
        }
    )
    return _V4GraphPublication(
        object_kinds=map_summary.object_kinds,
        mapping_count=map_summary.coordinate_count,
        unique_block_count=int(cas_publication.unique_block_count),
        block_count=int(compilation.block_count),
        owner_count=sum(
            int(relation.get("owner_count") or 0)
            for relation in compilation.relation_summaries
        ),
        provider_group_count=int(compilation.observe.get("group_count") or 0),
        npi_count=int(compilation.observe.get("npi_count") or 0),
        support_digest=support_digest,
        logical_byte_count=(
            int(cas_publication.logical_byte_count)
            + int(taxonomy_publication.packed_byte_count)
            + int(taxonomy_publication.pattern_member_bytes)
            + int(tax_identity_publication.artifact_byte_count)
            + int(
                tax_identity_source_publication.artifact_byte_count
                if tax_identity_source_publication is not None
                else 0
            )
        ),
        stored_byte_count=(
            int(cas_publication.stored_byte_count)
            + int(map_summary.stored_map_byte_count)
            + int(taxonomy_publication.packed_byte_count)
            + int(taxonomy_publication.pattern_member_bytes)
            + int(tax_identity_publication.artifact_byte_count)
            + int(
                tax_identity_source_publication.artifact_byte_count
                if tax_identity_source_publication is not None
                else 0
            )
        ),
        map_summary=map_summary,
        representation=(
            "pattern_v1" if compilation.selected_layout == "pattern" else "direct_v1"
        ),
        adaptive_layout=adaptive_layout,
        compiler_summary=dict(compilation.summary),
        dictionary_publication=_V4_DICTIONARY_BATCH_CONTRACT.as_dict(),
        inferred_taxonomy_candidates=dict(taxonomy_publication.manifest),
        provider_tax_identity=dict(tax_identity_publication.manifest),
        provider_tax_identity_source=(
            tax_identity_source_publication.as_dict()
            if tax_identity_source_publication is not None
            else {}
        ),
        audit_witness_path=compilation.provider_set_audit_npi_copy_path,
    )


async def _sealed_shared_serving_index(
    *,
    schema_name: str,
    snapshot_key: int,
    expected_generation: str,
) -> dict[str, Any]:
    """Read back the exact serving index committed by the physical seal."""

    schema = _quote_ident(schema_name)
    async with db.transaction() as session:
        manifest_result = await session.execute(
            db.text(
                f"""
                SELECT layout_manifest
                  FROM {schema}.ptg2_v3_snapshot_layout
                 WHERE snapshot_key = :snapshot_key
                   AND state = 'sealed'
                   AND generation = :generation
                """
            ),
            {
                "snapshot_key": int(snapshot_key),
                "generation": str(expected_generation),
            },
        )
        manifest = manifest_result.scalar()
    serving_index = (
        manifest.get("serving_index") if isinstance(manifest, Mapping) else None
    )
    if not isinstance(serving_index, Mapping):
        raise RuntimeError("sealed shared PTG layout is missing its serving index")
    return dict(serving_index)


def _attach_v4_dictionary_publication_contract(
    serving_index: dict[str, Any],
    graph_publication: Any,
) -> None:
    """Attach the non-semantic adaptive policy only when V4 supplied it."""

    publication_contract = getattr(
        graph_publication,
        "dictionary_publication",
        None,
    )
    if isinstance(publication_contract, Mapping) and publication_contract:
        serving_index["provider_graph"]["dictionary_publication"] = dict(
            publication_contract
        )


def _physical_serving_index(
    *,
    snapshot_key: int,
    coverage_scope_id: bytes,
    finalizer_summary: Mapping[str, Any],
    price_publication: Any,
    graph_publication: Any,
    code_count: int,
    audit_sample: Mapping[str, Any],
    source_witness: Mapping[str, Any],
    provider_identifier_quarantine: Mapping[str, Any],
    finalizer_block_copy: Mapping[str, Any],
    stored_byte_count: int,
    full_rebuild_scope_digest: str | None = None,
) -> dict[str, Any]:
    """Build the physical serving index from validated publication summaries."""

    blocks = _mapping(finalizer_summary.get("blocks"), "blocks")
    dense_keys = _mapping(finalizer_summary.get("dense_keys"), "dense_keys")
    price_dense = _mapping(dense_keys.get("price"), "dense price keys")
    price_encoder = _mapping(
        blocks.get("price_dictionary_encoder"),
        "price dictionary encoder",
    )
    assigned_encoder = _mapping(blocks.get("assigned_encoder"), "assigned encoder")
    membership_summary_map = dict(
        price_publication.stream_summaries["price_set_atom_memberships_v3"]
    )
    atom_summary_map = dict(price_publication.stream_summaries["price_atoms_v3"])
    serving_rate_count = _integer(
        _mapping(finalizer_summary.get("preservation"), "preservation").get(
            "encoded_records"
        ),
        "encoded_records",
    )
    source_count = _integer(finalizer_summary.get("source_count"), "source_count")
    if source_count <= 0:
        raise RuntimeError("strict V3 source_count must be positive")
    quarantine = validate_provider_identifier_quarantine(provider_identifier_quarantine)
    price_dictionary = {
        **price_encoder,
        "price_set_count": _integer(price_dense.get("count"), "price key count"),
    }
    serving_index = {
        "storage": "manifest_snapshot",
        "type": "ptg2_shared_blocks_v3",
        "snapshot_scoped": True,
        "arch_version": "postgres_binary_v3",
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "cold_lookup_contract": PTG2_V3_COLD_LOOKUP_CONTRACT,
        "price_membership_semantics": PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
        "serving_multiplicity_semantics": (PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS),
        "shared_snapshot_key": int(snapshot_key),
        "coverage_scope_id": coverage_scope_id.hex(),
        "serving_binary_table": None,
        "table": None,
        "materialized_tables": {},
        "provider_scope_strategy": "postgres_shared_graph",
        "id_storage": "binary128",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": PTG2_V3_SHARED_BLOCK_LAYOUT,
        "source_count": source_count,
        "code_count": int(code_count),
        "serving_rates": serving_rate_count,
        "rate_count": serving_rate_count,
        "atom_key_bits": int(price_publication.atom_key_bits),
        "price_atom_constant_keys": dict(price_publication.price_atom_constant_keys),
        "price_atom_constant_values": dict(
            price_publication.price_atom_constant_values
        ),
        "price_stage": dict(price_publication.stage_metrics),
        "serving_binary": {
            "format": "postgres_binary_v3",
            "price_dictionary": price_dictionary,
            "price_set_atom_memberships_v3": membership_summary_map,
            "price_atoms_v3": atom_summary_map,
            "assigned_encoder": assigned_encoder,
        },
        "provider_graph": {
            "owner_count": int(graph_publication.owner_count),
            "provider_group_count": int(graph_publication.provider_group_count),
            "npi_count": int(graph_publication.npi_count),
            "block_count": int(graph_publication.block_count),
            "representation": str(getattr(graph_publication, "representation", "")),
            "adaptive_layout": dict(getattr(graph_publication, "adaptive_layout", {})),
            "inferred_taxonomy_candidates": dict(
                getattr(
                    graph_publication,
                    "inferred_taxonomy_candidates",
                    {},
                )
            ),
            "provider_tax_identity": dict(
                getattr(
                    graph_publication,
                    "provider_tax_identity",
                    {},
                )
            ),
            "provider_tax_identity_source": dict(
                getattr(
                    graph_publication,
                    "provider_tax_identity_source",
                    {},
                )
            ),
        },
        "provider_identifier_quarantine": quarantine,
        "finalizer_block_copy": dict(finalizer_block_copy),
        "audit_sample": dict(audit_sample),
        "source_witness": dict(source_witness),
        "storage_bytes": int(stored_byte_count),
        "timings": dict(finalizer_summary.get("timings") or {}),
    }
    _attach_v4_dictionary_publication_contract(
        serving_index,
        graph_publication,
    )
    normalized_rebuild_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    if normalized_rebuild_digest is not None:
        serving_index["full_rebuild_scope_digest"] = normalized_rebuild_digest
    return serving_index


def _shared_layout_support_digest(
    *,
    core_support: Mapping[str, Any],
    audit_sample: Mapping[str, Any],
    source_witness: Mapping[str, Any],
    full_rebuild_scope_digest: str | None = None,
) -> bytes:
    """Seal support metadata, optionally isolating one controlled rebuild."""

    support_by_field = {
        **dict(core_support),
        "audit_sample": dict(audit_sample),
        "source_witness": dict(source_witness),
    }
    normalized_rebuild_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    if normalized_rebuild_digest is not None:
        support_by_field["full_rebuild_scope_digest"] = normalized_rebuild_digest
    return shared_support_digest(support_by_field)


async def _publish_prepared_shared_layout(
    *,
    schema_name: str,
    manifest_stage_table: str,
    reserved_snapshot_key: int,
    build_token: str,
    expected_coverage_scope_id: bytes,
    logical_snapshot_id: str,
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    source_audit_witness_entries: Iterable[Mapping[str, Any]],
    expected_raw_source_sha256: Iterable[str],
    graph_artifact_entries: Iterable[dict[str, Any]],
    provider_identifier_quarantine: Mapping[str, Any],
    prepared_price: PreparedSharedPriceArtifacts,
    publication_started_at: float,
    price_prepare_seconds: float,
    scratch_parent: str | Path | None = None,
    prepared_work_directory: str | Path | None = None,
    prepared_finalizer: _PreparedFinalizer | None = None,
    prepared_price_publication: _PreparedPricePublication | None = None,
    full_rebuild_scope_digest: str | None = None,
    provider_graph_v4: bool = False,
    compressed_acquisition_bytes: int | None = None,
    empty_npi_tin_only_normalization_count: int | None = None,
    tax_identity_source_artifacts: Iterable[Mapping[str, Any]] | None = None,
    progress_callback: Callable[[str, Mapping[str, int]], None] | None = None,
    progress_interval_seconds: float = 4.0,
) -> SharedSnapshotPublication:
    """Finalize, validate, publish, and atomically seal one physical layout."""

    publication_timing_map: dict[str, float] = {
        "price_prepare_seconds": float(price_prepare_seconds),
    }

    def record_stage(stage_name: str, started_at: float) -> None:
        """Record elapsed wall time for a named publication stage."""

        publication_timing_map[f"{stage_name}_seconds"] = time.monotonic() - started_at

    def completed_stage(stage_name: str, **counters_by_name: int) -> None:
        """Report exact completed rows/bytes/batches for one bounded stage."""

        if progress_callback is None:
            return
        normalized_by_name = {
            str(name): int(value)
            for name, value in counters_by_name.items()
            if int(value) >= 0
        }
        normalized_by_name["completed_batches"] = (
            normalized_by_name.get("completed_batches", 0) + 1
        )
        progress_callback(stage_name, normalized_by_name)

    shared_generation = (
        PTG2_V4_SHARED_GENERATION if provider_graph_v4 else PTG2_V3_SHARED_GENERATION
    )
    if provider_graph_v4 and (
        compressed_acquisition_bytes is None
        or int(compressed_acquisition_bytes) <= 0
        or empty_npi_tin_only_normalization_count is None
        or int(empty_npi_tin_only_normalization_count) < 0
    ):
        raise RuntimeError(
            "PTG V4 publication requires authenticated resource evidence"
        )
    configured_schema = resolve_ptg2_schema()
    if str(schema_name).strip() != configured_schema:
        raise RuntimeError(
            "strict shared publication must use the configured PostgreSQL schema"
        )
    coverage_scope_id = _validated_coverage_scope_id(expected_coverage_scope_id)
    quarantine = validate_provider_identifier_quarantine(provider_identifier_quarantine)

    async def touch_build() -> None:
        """Refresh the reserved layout's build heartbeat transactionally."""

        async with db.transaction() as session:
            touch = (
                touch_v4_shared_layout_build
                if provider_graph_v4
                else touch_shared_layout_build
            )
            await touch(
                session,
                schema_name=schema_name,
                snapshot_key=int(reserved_snapshot_key),
                build_token=str(build_token),
            )

    await touch_build()

    work_directory_context = (
        tempfile.TemporaryDirectory(
            prefix=(
                "ptg2-v4-shared-publish-"
                if provider_graph_v4
                else "ptg2-v3-shared-publish-"
            ),
            dir=str(scratch_parent) if scratch_parent is not None else None,
        )
        if prepared_work_directory is None
        else nullcontext(str(prepared_work_directory))
    )
    with work_directory_context as raw_work_directory:
        if prepared_finalizer is None:
            direct_finalizer_progress = _MeasuredPublicationProgress(
                "finalizer",
                progress_callback,
                interval_seconds=progress_interval_seconds,
            )
            stage_started_at = time.monotonic()
            price_key_map_path = await export_shared_price_key_map(
                prepared_price,
                Path(raw_work_directory) / "price-key-map.copy",
            )
            record_stage("price_key_map_export", stage_started_at)
            if progress_callback is not None:
                completed_stage(
                    "price key map export",
                    exported_bytes=price_key_map_path.stat().st_size,
                    exported_rows=int(prepared_price.price_set_count),
                )
            stage_started_at = time.monotonic()
            try:
                with observe_v3_finalizer_progress(
                    direct_finalizer_progress.add
                    if progress_callback is not None
                    else None
                ):
                    finalizer_summary_by_field = await run_v3_direct_finalizer(
                        work_directory=raw_work_directory,
                        serving_run_entries=serving_run_entries,
                        code_dictionary_entries=code_dictionary_entries,
                        provider_set_metadata_entries=provider_set_metadata_entries,
                        expected_source_identities=expected_source_identities,
                        price_key_map_input=price_key_map_path,
                        price_key_map_row_count=prepared_price.price_set_count,
                        scratch_durability=(
                            PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY
                            if prepared_work_directory is None
                            else PTG2_V3_DURABLE_SCRATCH_DURABILITY
                        ),
                    )
            finally:
                direct_finalizer_progress.flush()
            record_stage("finalizer", stage_started_at)
        else:
            finalizer_summary_by_field = dict(prepared_finalizer.summary)
            publication_timing_map.update(
                {
                    "price_key_map_export_seconds": float(
                        prepared_finalizer.price_key_map_export_seconds
                    ),
                    "finalizer_seconds": float(prepared_finalizer.finalizer_seconds),
                    "price_key_ready_finalizer_wall_seconds": float(
                        prepared_finalizer.overlap_wall_seconds
                    ),
                }
            )
        await touch_build()
        finalizer_blocks = _mapping(
            finalizer_summary_by_field.get("blocks"),
            "blocks",
        )
        serving_block_summary = _mapping(
            finalizer_blocks.get("serving"), "serving blocks"
        )
        price_block_summary = _mapping(
            finalizer_blocks.get("price_dictionary"),
            "price dictionary blocks",
        )
        stage_started_at = time.monotonic()
        dictionary_publication = await publish_shared_finalizer_dictionaries(
            dict(finalizer_summary_by_field),
            schema_name=schema_name,
            snapshot_key=int(reserved_snapshot_key),
            build_token=build_token,
            expected_coverage_scope_id=coverage_scope_id,
            provider_set_metadata_entries=provider_set_metadata_entries,
            expected_generation=shared_generation,
        )
        record_stage("dictionary_publish", stage_started_at)
        if progress_callback is not None:
            completed_stage(
                "dictionary publication",
                published_rows=(
                    int(dictionary_publication.code_count)
                    + int(dictionary_publication.provider_set_count)
                    + int(dictionary_publication.serving_rate_count)
                ),
            )
        await touch_build()
        stage_started_at = time.monotonic()
        provider_set_keys = await _export_provider_set_key_map(
            schema_name=schema_name,
            snapshot_key=int(reserved_snapshot_key),
            output_path=Path(raw_work_directory) / "provider-set-authoritative.tsv",
        )
        record_stage("provider_set_key_export", stage_started_at)
        if progress_callback is not None:
            completed_stage(
                "provider set key export",
                exported_bytes=provider_set_keys.stat().st_size,
                exported_rows=int(dictionary_publication.provider_set_count),
            )
        stage_started_at = time.monotonic()
        if provider_graph_v4:
            graph_artifact_entries = tuple(
                dict(entry) for entry in graph_artifact_entries
            )
            graph_conversion = await _compile_v4_provider_graph(
                graph_artifact_entries=graph_artifact_entries,
                provider_set_key_map_path=provider_set_keys,
                work_directory=Path(raw_work_directory),
                schema_name=schema_name,
                touch_build=touch_build,
                progress_callback=progress_callback,
            )
        else:
            graph_conversion = await _convert_shared_graph_natively(
                graph_artifact_entries=graph_artifact_entries,
                provider_set_key_map_path=provider_set_keys,
                work_directory=Path(raw_work_directory),
            )
        record_stage("provider_graph_convert", stage_started_at)
        if progress_callback is not None:
            completed_stage(
                "provider graph conversion",
                converted_blocks=int(graph_conversion.block_count),
            )

        block_stage = shared_block_stage_name(f"final-{reserved_snapshot_key}")

        async def publish_finalizer_blocks() -> Any:
            """Publish finalizer serving and price blocks."""

            lane_progress = _MeasuredPublicationProgress(
                "serving block publication",
                progress_callback,
                interval_seconds=progress_interval_seconds,
            )
            stage_started_at = time.monotonic()
            await create_shared_block_stage(
                schema_name=schema_name,
                stage_table=block_stage,
            )
            try:
                serving_copy_metrics = await _copy_finalizer_block(
                    finalizer_summary_by_field,
                    serving_block_summary,
                    schema_name=schema_name,
                    stage_table=block_stage,
                    progress_callback=(
                        lane_progress.add if progress_callback is not None else None
                    ),
                )
                price_copy_metrics = await _copy_finalizer_block(
                    finalizer_summary_by_field,
                    price_block_summary,
                    schema_name=schema_name,
                    stage_table=block_stage,
                    progress_callback=(
                        lane_progress.add if progress_callback is not None else None
                    ),
                )
                if serving_copy_metrics is None or price_copy_metrics is None:
                    raise RuntimeError(
                        "strict V3 finalizer block COPY did not return selective proof"
                    )
                publication = await publish_shared_block_stage(
                    schema_name=schema_name,
                    stage_table=block_stage,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=build_token,
                    expected_generation=shared_generation,
                    **_progress_callback_kwargs(
                        lane_progress.add if progress_callback is not None else None
                    ),
                )
                return _FinalizerBlockPublicationResult(
                    publication=publication,
                    serving_copy=serving_copy_metrics,
                    price_dictionary_copy=price_copy_metrics,
                )
            finally:
                lane_progress.flush()
                await db.status(
                    "DROP TABLE IF EXISTS "
                    f"{_quote_ident(schema_name)}.{_quote_ident(block_stage)};"
                )
                record_stage("serving_block_publish", stage_started_at)

        async def publish_provider_graph() -> Any:
            """Publish provider graph blocks and relational owner metadata."""

            lane_progress = _MeasuredPublicationProgress(
                "provider graph publication",
                progress_callback,
                interval_seconds=progress_interval_seconds,
            )
            stage_started_at = time.monotonic()
            try:
                if provider_graph_v4:
                    graph_publication = await _publish_v4_graph(
                        graph_conversion,
                        publication_context=_V4GraphCoordinates(
                            schema_name=schema_name,
                            logical_snapshot_id=logical_snapshot_id,
                            snapshot_key=int(reserved_snapshot_key),
                            build_token=build_token,
                        ),
                        compressed_acquisition_bytes=int(
                            compressed_acquisition_bytes or 0
                        ),
                        empty_npi_tin_only_normalization_count=int(
                            empty_npi_tin_only_normalization_count or 0
                        ),
                        tax_identity_source_artifacts=(
                            tax_identity_source_artifacts
                        ),
                        **_progress_callback_kwargs(
                            lane_progress.add if progress_callback is not None else None
                        ),
                        heartbeat_callback=(
                            lane_progress.heartbeat
                            if progress_callback is not None
                            else None
                        ),
                    )
                    _taxonomy_copy_complete(
                        graph_conversion,
                        progress_callback,
                    )
                    return graph_publication
                return await publish_shared_graph(
                    graph_conversion,
                    schema_name=schema_name,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=build_token,
                    **_progress_callback_kwargs(
                        lane_progress.add if progress_callback is not None else None
                    ),
                )
            finally:
                lane_progress.flush()
                record_stage("provider_graph_publish", stage_started_at)

        dense_keys = _mapping(
            finalizer_summary_by_field.get("dense_keys"),
            "dense keys",
        )
        price_dense = _mapping(dense_keys.get("price"), "dense price keys")
        expected_price_set_count = _integer(price_dense.get("count"), "price key count")
        expected_price_key_order = str(price_dense.get("ordering") or "")
        if (
            prepared_price.price_set_count != expected_price_set_count
            or expected_price_key_order != PTG2_V3_PRICE_KEY_ORDER
        ):
            raise RuntimeError(
                "strict V3 finalizer price keys disagree with prepared price publication"
            )

        async def publish_price() -> Any:
            """Publish dense price dictionaries and membership blocks."""

            if prepared_price_publication is not None:
                publication_timing_map["price_publish_seconds"] = float(
                    prepared_price_publication.publish_seconds
                )
                return prepared_price_publication.publication
            lane_progress = _MeasuredPublicationProgress(
                "price publication",
                progress_callback,
                interval_seconds=progress_interval_seconds,
            )
            stage_started_at = time.monotonic()
            try:
                with observe_shared_price_progress(
                    lane_progress.add if progress_callback is not None else None
                ):
                    return await publish_shared_price_artifacts(
                        schema_name=schema_name,
                        manifest_stage_table=manifest_stage_table,
                        snapshot_key=int(reserved_snapshot_key),
                        build_token=build_token,
                        expected_price_set_count=expected_price_set_count,
                        expected_price_key_order=expected_price_key_order,
                        prepared=prepared_price,
                        expected_generation=shared_generation,
                    )
            finally:
                lane_progress.flush()
                record_stage("price_publish", stage_started_at)

        async def publish_source_witness() -> Any:
            """Publish the bounded source-fidelity witness."""

            lane_progress = _MeasuredPublicationProgress(
                "source witness publication",
                progress_callback,
                interval_seconds=progress_interval_seconds,
            )
            stage_started_at = time.monotonic()
            try:
                return await publish_shared_source_witness(
                    schema_name=schema_name,
                    build_ownership=SharedLayoutBuildOwnership(
                        snapshot_key=int(reserved_snapshot_key),
                        build_token=build_token,
                    ),
                    entries=tuple(source_audit_witness_entries),
                    expected_raw_source_sha256=tuple(expected_raw_source_sha256),
                    expected_generation=shared_generation,
                    **_progress_callback_kwargs(
                        lane_progress.add if progress_callback is not None else None
                    ),
                )
            finally:
                lane_progress.flush()
                record_stage("source_witness_publish", stage_started_at)

        independent_publish_started_at = time.monotonic()
        try:
            (
                finalizer_block_result,
                graph_publication,
                price_publication,
                source_witness_publication,
            ) = await _run_independent_publication_lanes(
                finalizer_blocks=publish_finalizer_blocks,
                provider_graph=publish_provider_graph,
                price=publish_price,
                source_witness=publish_source_witness,
            )
        finally:
            # The V4 audit re-authenticates the compiler's bounded witness
            # against the packed graph before seal.  Keep that witness alive
            # until the audit has completed; the V3 publisher has no such
            # post-publication dependency.
            if not provider_graph_v4:
                graph_conversion.cleanup()
        record_stage(
            "independent_publish_wall",
            independent_publish_started_at,
        )
        finalizer_block_publication = finalizer_block_result.publication
        finalizer_block_copy_manifest = finalizer_block_result.copy_manifest()
        await touch_build()
        stage_started_at = time.monotonic()
        async with db.transaction() as session:
            mapping_summary = await summarize_shared_snapshot_mappings(
                session,
                schema_name=schema_name,
                snapshot_key=int(reserved_snapshot_key),
            )
        record_stage("mapping_summary", stage_started_at)
        if progress_callback is not None:
            completed_stage(
                "mapping summary",
                summarized_rows=int(mapping_summary.mapping_count),
                summarized_blocks=int(mapping_summary.unique_block_count),
            )
        _validate_authoritative_mapping_summary(
            mapping_summary,
            finalizer_block_publication,
            *(() if provider_graph_v4 else (graph_publication,)),
            price_publication,
        )
        observed_kinds = set(mapping_summary.object_kinds)
        missing_kinds = (
            _REQUIRED_PRICE_OBJECT_KINDS
            if provider_graph_v4
            else _REQUIRED_OBJECT_KINDS
        ) - observed_kinds
        if missing_kinds:
            raise RuntimeError(
                f"strict V3 physical layout is missing required blocks: {sorted(missing_kinds)}"
            )
        core_support_map = {
            "contract_version": 2 if provider_graph_v4 else 1,
            "serving_multiplicity_semantics": (PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS),
            "finalizer_dictionaries": dictionary_publication.support_digest.hex(),
            "provider_graph": graph_publication.support_digest.hex(),
            "price_attributes": price_publication.support_digest.hex(),
            "source_witness": source_witness_publication.support_digest.hex(),
            "provider_identifier_quarantine": quarantine["sha256"],
        }
        if provider_graph_v4:
            # The V4 root digest owns only the factored provider graph.  Bind
            # the unchanged V3 rate/finalizer mappings into the support digest
            # so a graph-identical but rate-different layout cannot be reused.
            source_projection_by_field = dict(
                graph_publication.provider_tax_identity_source
            )
            if not source_projection_by_field:
                raise RuntimeError(
                    "PTG V4 publication omitted source-local tax identity evidence"
                )
            core_support_map["provider_tax_identity_source"] = (
                source_projection_by_field
            )
            core_support_map["price_finalizer_mapping_digest"] = (
                mapping_summary.mapping_digest.hex()
            )
            core_support_map["price_finalizer_mapping_count"] = int(
                mapping_summary.mapping_count
            )
        core_support_digest = shared_support_digest(core_support_map)
        price_membership_summary = _mapping(
            price_publication.stream_summaries.get("price_set_atom_memberships_v3"),
            "price membership stream summary",
        )
        price_membership_block_span = _integer(
            price_membership_summary.get("block_span"),
            "price membership block span",
        )
        if price_membership_block_span <= 0:
            raise RuntimeError("strict V3 price membership block span must be positive")
        stage_started_at = time.monotonic()
        audit_build_ownership = SharedLayoutBuildOwnership(
            snapshot_key=int(reserved_snapshot_key),
            build_token=build_token,
        )
        if provider_graph_v4:
            try:
                audit_publication = await publish_v4_audit_sample(
                    schema_name=schema_name,
                    build_ownership=audit_build_ownership,
                    logical_snapshot_id=str(logical_snapshot_id),
                    finalizer_summary=finalizer_summary_by_field,
                    mapping_digest=graph_publication.map_summary.map_digest,
                    core_support_digest=core_support_digest,
                    atom_key_bits=int(price_publication.atom_key_bits),
                    price_membership_block_span=price_membership_block_span,
                    graph_compilation=graph_conversion,
                    inferred_taxonomy_candidates=(
                        graph_publication.inferred_taxonomy_candidates
                    ),
                )
            finally:
                graph_conversion.cleanup()
        else:
            audit_publication = await publish_shared_audit_sample(
                schema_name=schema_name,
                build_ownership=audit_build_ownership,
                logical_snapshot_id=str(logical_snapshot_id),
                finalizer_summary=finalizer_summary_by_field,
                mapping_digest=mapping_summary.mapping_digest,
                core_support_digest=core_support_digest,
                atom_key_bits=int(price_publication.atom_key_bits),
                price_membership_block_span=price_membership_block_span,
            )
        record_stage("audit_publish", stage_started_at)
        if progress_callback is not None:
            completed_stage(
                "audit publication",
                published_rows=int(audit_publication.row_count),
            )
        await touch_build()
        support_digest = _shared_layout_support_digest(
            core_support=core_support_map,
            audit_sample=audit_publication.metadata,
            source_witness=source_witness_publication.metadata,
            full_rebuild_scope_digest=full_rebuild_scope_digest,
        )
        stored_byte_count = (
            int(finalizer_block_publication.stored_byte_count)
            + int(graph_publication.stored_byte_count)
            + int(price_publication.stored_byte_count)
            + int(source_witness_publication.stored_byte_count)
        )
        provisional_serving_index = _physical_serving_index(
            snapshot_key=int(reserved_snapshot_key),
            coverage_scope_id=coverage_scope_id,
            finalizer_summary=finalizer_summary_by_field,
            price_publication=price_publication,
            graph_publication=graph_publication,
            code_count=dictionary_publication.code_count,
            audit_sample=audit_publication.metadata,
            source_witness=source_witness_publication.metadata,
            provider_identifier_quarantine=quarantine,
            finalizer_block_copy=finalizer_block_copy_manifest,
            stored_byte_count=stored_byte_count,
            full_rebuild_scope_digest=full_rebuild_scope_digest,
        )
        provisional_serving_index["timings"] = {
            **dict(provisional_serving_index.get("timings") or {}),
            **publication_timing_map,
        }
        stage_started_at = time.monotonic()
        seal_progress = (
            _MeasuredPublicationProgress(
                "snapshot seal",
                progress_callback,
                interval_seconds=progress_interval_seconds,
            )
            if provider_graph_v4
            else None
        )
        async with db.transaction() as session:
            if provider_graph_v4:
                sealed = await seal_v4_shared_layout(
                    session,
                    schema_name=schema_name,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=str(build_token),
                    expected_summary=graph_publication.map_summary,
                    support_digest=support_digest,
                    layout_manifest={"serving_index": provisional_serving_index},
                    progress_callback=seal_progress.add,
                )
                sealed_audit_metadata_map = dict(audit_publication.metadata)
            else:
                sealed = await seal_shared_layout(
                    session,
                    schema_name=schema_name,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=str(build_token),
                    expected_summary=mapping_summary,
                    support_digest=support_digest,
                    layout_manifest={"serving_index": provisional_serving_index},
                )
                sealed_audit_metadata_map = (
                    await sealed_audit_sample_metadata(
                        session,
                        schema_name=schema_name,
                        snapshot_key=int(sealed.snapshot_key),
                        logical_snapshot_id=str(logical_snapshot_id),
                    )
                    if sealed.reused
                    else dict(audit_publication.metadata)
                )
        if seal_progress is not None:
            seal_progress.flush()
        record_stage("seal", stage_started_at)
        if progress_callback is not None:
            completed_stage(
                "snapshot seal",
                sealed_rows=int(
                    graph_publication.mapping_count
                    if provider_graph_v4
                    else mapping_summary.mapping_count
                ),
            )
        publication_timing_map["shared_publish_total_seconds"] = (
            time.monotonic() - publication_started_at
        )
        serving_index = (
            await _sealed_shared_serving_index(
                schema_name=schema_name,
                snapshot_key=int(sealed.snapshot_key),
                expected_generation=PTG2_V4_SHARED_GENERATION,
            )
            if provider_graph_v4
            else dict(provisional_serving_index)
        )
        serving_index["timings"] = {
            **dict(serving_index.get("timings") or {}),
            **publication_timing_map,
        }
        serving_index["shared_snapshot_key"] = int(sealed.snapshot_key)
        serving_index["audit_sample"] = sealed_audit_metadata_map
        return SharedSnapshotPublication(
            snapshot_key=int(sealed.snapshot_key),
            serving_index=serving_index,
            object_kinds=(
                graph_publication.object_kinds
                if provider_graph_v4
                else mapping_summary.object_kinds
            ),
            mapping_count=(
                graph_publication.mapping_count
                if provider_graph_v4
                else mapping_summary.mapping_count
            ),
            unique_block_count=(
                graph_publication.unique_block_count
                if provider_graph_v4
                else mapping_summary.unique_block_count
            ),
            mapping_digest=(
                graph_publication.map_summary.map_digest
                if provider_graph_v4
                else mapping_summary.mapping_digest
            ),
            finalizer_summary=dict(finalizer_summary_by_field),
            layout_reused_at_seal=bool(sealed.reused),
            stored_byte_count=stored_byte_count,
        )


async def publish_strict_shared_v3_layout(
    *,
    schema_name: str,
    manifest_stage_table: str,
    reserved_snapshot_key: int,
    build_token: str,
    expected_coverage_scope_id: bytes,
    logical_snapshot_id: str,
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    source_audit_witness_entries: Iterable[Mapping[str, Any]],
    price_set_summary_source_count: int | None = None,
    expected_raw_source_sha256: Iterable[str],
    graph_artifact_entries: Iterable[dict[str, Any]],
    provider_identifier_quarantine: Mapping[str, Any],
    scratch_parent: str | Path | None = None,
    full_rebuild_scope_digest: str | None = None,
    provider_graph_v4: bool = False,
    compressed_acquisition_entries: Iterable[Mapping[str, Any]] | None = None,
    empty_npi_tin_only_normalization_count: int | None = None,
    tax_identity_source_artifacts: Iterable[Mapping[str, Any]] | None = None,
    progress_callback: Callable[[str, Mapping[str, int]], None] | None = None,
    progress_interval_seconds: float = 4.0,
) -> SharedSnapshotPublication:
    """Prepare exact price ranks once, then publish and clean every temporary map."""

    normalized_rebuild_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    serving_run_entries = tuple(serving_run_entries)
    code_dictionary_entries = tuple(code_dictionary_entries)
    provider_set_metadata_entries = tuple(provider_set_metadata_entries)
    expected_source_identities = tuple(expected_source_identities)
    expected_raw_source_digests = tuple(
        str(raw_hash or "").strip().lower() for raw_hash in expected_raw_source_sha256
    )
    source_artifacts = tuple(
        dict(entry) for entry in (tax_identity_source_artifacts or ())
    )
    compressed_acquisition_bytes: int | None = None
    if provider_graph_v4:
        if (
            empty_npi_tin_only_normalization_count is None
            or int(empty_npi_tin_only_normalization_count) < 0
        ):
            raise RuntimeError(
                "PTG V4 empty-NPI TIN-only normalization evidence is invalid"
            )
        byte_count_by_hash: dict[str, int] = {}
        for raw_entry in tuple(compressed_acquisition_entries or ()):
            raw_hash = str(raw_entry.get("raw_sha256") or "").strip().lower()
            try:
                byte_count = int(raw_entry.get("byte_count"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "PTG V4 compressed acquisition entry is invalid"
                ) from exc
            if (
                len(raw_hash) != 64
                or any(character not in "0123456789abcdef" for character in raw_hash)
                or byte_count <= 0
                or (
                    raw_hash in byte_count_by_hash
                    and byte_count_by_hash[raw_hash] != byte_count
                )
            ):
                raise RuntimeError("PTG V4 compressed acquisition entry is invalid")
            byte_count_by_hash[raw_hash] = byte_count
        if set(byte_count_by_hash) != set(expected_raw_source_digests):
            raise RuntimeError(
                "PTG V4 compressed acquisition inputs do not match source hashes"
            )
        compressed_acquisition_bytes = sum(byte_count_by_hash.values())
        if compressed_acquisition_bytes <= 0:
            raise RuntimeError("PTG V4 compressed acquisition bytes must be positive")
        if not source_artifacts:
            raise RuntimeError(
                "PTG V4 publication requires source-local tax identity evidence"
            )
    publication_started_at = time.monotonic()
    configured_schema = resolve_ptg2_schema()
    if str(schema_name).strip() != configured_schema:
        raise RuntimeError(
            "strict V3 publication must use the configured PostgreSQL schema"
        )
    shared_generation = (
        PTG2_V4_SHARED_GENERATION if provider_graph_v4 else PTG2_V3_SHARED_GENERATION
    )
    async with db.transaction() as session:
        touch = (
            touch_v4_shared_layout_build
            if provider_graph_v4
            else touch_shared_layout_build
        )
        await touch(
            session,
            schema_name=schema_name,
            snapshot_key=int(reserved_snapshot_key),
            build_token=str(build_token),
        )
    with tempfile.TemporaryDirectory(
        prefix=(
            "ptg2-v4-shared-publish-"
            if provider_graph_v4
            else "ptg2-v3-shared-publish-"
        ),
        dir=str(scratch_parent) if scratch_parent is not None else None,
    ) as raw_work_directory:
        price_work_progress = _MeasuredPublicationProgress(
            "price preparation and publication",
            progress_callback,
            interval_seconds=progress_interval_seconds,
        )
        finalizer_progress = _MeasuredPublicationProgress(
            "finalizer",
            progress_callback,
            interval_seconds=progress_interval_seconds,
        )

        async def publish_prepared_price_early(
            prepared: PreparedSharedPriceArtifacts,
        ) -> Any:
            """Publish price blocks while the independent finalizer is active."""

            return await publish_shared_price_artifacts(
                schema_name=schema_name,
                manifest_stage_table=manifest_stage_table,
                snapshot_key=int(reserved_snapshot_key),
                build_token=build_token,
                expected_price_set_count=int(prepared.price_set_count),
                expected_price_key_order=PTG2_V3_PRICE_KEY_ORDER,
                prepared=prepared,
                expected_generation=shared_generation,
            )

        with observe_shared_price_progress(
            price_work_progress.add if progress_callback is not None else None
        ):
            (
                prepared_price,
                price_prepare_seconds,
                prepared_finalizer,
                prepared_price_publication,
            ) = await _prepare_price_with_early_finalizer(
                schema_name=schema_name,
                manifest_stage_table=manifest_stage_table,
                price_set_summary_source_count=price_set_summary_source_count,
                raw_work_directory=raw_work_directory,
                serving_run_entries=serving_run_entries,
                code_dictionary_entries=code_dictionary_entries,
                provider_set_metadata_entries=provider_set_metadata_entries,
                expected_source_identities=expected_source_identities,
                publish_prepared_price=publish_prepared_price_early,
                finalizer_progress_callback=(
                    finalizer_progress.add if progress_callback is not None else None
                ),
            )
        price_work_progress.flush()
        finalizer_progress.flush()
        try:
            publication = await _publish_prepared_shared_layout(
                schema_name=schema_name,
                manifest_stage_table=manifest_stage_table,
                reserved_snapshot_key=int(reserved_snapshot_key),
                build_token=build_token,
                expected_coverage_scope_id=expected_coverage_scope_id,
                logical_snapshot_id=logical_snapshot_id,
                expected_source_identities=expected_source_identities,
                serving_run_entries=serving_run_entries,
                code_dictionary_entries=code_dictionary_entries,
                provider_set_metadata_entries=provider_set_metadata_entries,
                source_audit_witness_entries=source_audit_witness_entries,
                expected_raw_source_sha256=expected_raw_source_digests,
                graph_artifact_entries=graph_artifact_entries,
                provider_identifier_quarantine=provider_identifier_quarantine,
                prepared_price=prepared_price,
                publication_started_at=publication_started_at,
                price_prepare_seconds=price_prepare_seconds,
                scratch_parent=scratch_parent,
                prepared_work_directory=raw_work_directory,
                prepared_finalizer=prepared_finalizer,
                prepared_price_publication=prepared_price_publication,
                full_rebuild_scope_digest=normalized_rebuild_digest,
                provider_graph_v4=provider_graph_v4,
                compressed_acquisition_bytes=compressed_acquisition_bytes,
                empty_npi_tin_only_normalization_count=(
                    empty_npi_tin_only_normalization_count
                ),
                tax_identity_source_artifacts=source_artifacts,
                progress_callback=progress_callback,
                progress_interval_seconds=progress_interval_seconds,
            )
        except BaseException:
            cleanup_task = asyncio.create_task(
                cleanup_prepared_shared_price_artifacts(prepared_price)
            )
            await _await_cleanup_task(cleanup_task)
            raise
        cleanup_task = asyncio.create_task(
            cleanup_prepared_shared_price_artifacts(prepared_price)
        )
        await _await_cleanup_task(cleanup_task, propagate_cancellation=True)
        return publication


delete_unpublished_shared_v3_snapshot_sources = delete_unpublished_snapshot_sources
validate_reused_shared_v3_snapshot_sources = validate_reused_snapshot_sources


__all__ = [
    "SharedSnapshotPublication",
    "delete_unpublished_shared_v3_snapshot_sources",
    "delete_unpublished_snapshot_sources",
    "publish_shared_v3_snapshot_sources",
    "publish_strict_shared_v3_layout",
    "validate_reused_shared_v3_snapshot_sources",
    "validate_reused_snapshot_sources",
]
