# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off fixed synthetic seed-candidate canary with zero sockets."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from typing import Any

from db.models import db
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.source import load_enabled_source
from process.formulary_fhir.synchronizer import SynchronizationResult
from process.formulary_fhir.synchronizer import _run_verified_sync
from process.formulary_fhir.synthetic_canary_contract import CANARY_CUTOFF
from process.formulary_fhir.synthetic_canary_contract import CANARY_ENABLED_ENV
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_FINAL_TABLE_COUNTS,
)
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_LOCK_RETRY_SECONDS,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_LOCK_WAIT_SECONDS
from process.formulary_fhir.synthetic_canary_contract import CANARY_RUN_ID
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_BASE
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_SOURCE_DISPLAY_NAME,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import CANARY_TIMEOUT_SECONDS
from process.formulary_fhir.synthetic_canary_contract import canary_metadata
from process.formulary_fhir.synthetic_canary_contract import canary_runtime_config
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_canary_transport import SyntheticCanaryClient


TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
ERROR_MESSAGES = {
    "busy": "synthetic formulary canary source is busy",
    "catalog": "synthetic formulary canary catalog is not isolated",
    "cleanup": "synthetic formulary canary cleanup failed",
    "disabled": "synthetic formulary canary is disabled",
    "evidence": "synthetic formulary canary evidence is invalid",
    "lock_unavailable": "synthetic formulary canary lock is unavailable",
    "source": "synthetic formulary canary source is invalid",
}


class SyntheticCanaryError(RuntimeError):
    """Expose only one fixed candidate-canary failure code."""

    def __init__(self, code: str) -> None:
        self.code = code if code in ERROR_MESSAGES else "evidence"
        super().__init__(ERROR_MESSAGES[self.code])


@dataclass(frozen=True, slots=True)
class SyntheticSeedCandidateResult:
    """Expose only exact verification evidence and bounded request metrics."""

    dataset_id: str
    source_configuration_hash: str
    acquisition_contract_hash: str
    list_count: int
    alias_count: int
    medication_membership_count: int
    coverage_hash: str
    membership_hash: str
    full_aliases: int
    resumed_aliases: int
    request_count: int


def _is_canary_enabled() -> bool:
    raw_value = os.getenv(CANARY_ENABLED_ENV, "")
    return raw_value.strip().lower() in TRUE_ENV_VALUES


def _source_values(*, enabled: bool) -> dict[str, object]:
    return {
        "source_id": CANARY_SOURCE_ID,
        "canonical_base": CANARY_SOURCE_BASE,
        "display_name": CANARY_SOURCE_DISPLAY_NAME,
        "enabled": enabled,
        "runtime_config_json": canary_runtime_config(),
        "metadata_json": canary_metadata(),
    }


def _is_exact_source(
    source_by_field: dict[str, Any],
    *,
    enabled: bool,
) -> bool:
    observed_by_field = {
        field_name: source_by_field.get(field_name)
        for field_name in _source_values(enabled=enabled)
    }
    return json_text(observed_by_field) == json_text(
        _source_values(enabled=enabled)
    )


async def _catalog_sources(database: Any) -> tuple[dict[str, Any], ...]:
    source_rows = await database.all(
        f"SELECT source_id, canonical_base, display_name, enabled, "
        f"runtime_config_json, metadata_json FROM "
        f"{table_name('fhir_formulary_source')} ORDER BY source_id LIMIT 2;"
    )
    return tuple(row_mapping(source_row) for source_row in source_rows)


async def _require_empty_pointer(database: Any) -> None:
    pointer_row = await database.first(
        f"SELECT source_id, dataset_id FROM "
        f"{table_name('fhir_formulary_current')} LIMIT 1;"
    )
    if row_mapping(pointer_row):
        raise SyntheticCanaryError("catalog")


async def _catalog_datasets(database: Any) -> tuple[dict[str, Any], ...]:
    dataset_rows = await database.all(
        f"SELECT source_id, dataset_id, run_id, previous_dataset_id, "
        f"cutoff_at, summary_json ->> 'acquisition_contract_hash' AS "
        f"acquisition_contract_hash, status, publish_requested, "
        f"seed_eligible FROM {table_name('fhir_formulary_dataset')} "
        "ORDER BY source_id, dataset_id LIMIT 2;"
    )
    return tuple(row_mapping(dataset_row) for dataset_row in dataset_rows)


async def _catalog_table_counts(database: Any) -> dict[str, int]:
    return {
        table: int(
            await database.scalar(f"SELECT count(*) FROM {table_name(table)};")
            or 0
        )
        for table in CANARY_FINAL_TABLE_COUNTS
    }


def _is_recoverable_dataset(dataset_by_field: dict[str, Any]) -> bool:
    expected_by_field = expected_evidence()
    return (
        dataset_by_field.get("source_id") == CANARY_SOURCE_ID
        and dataset_by_field.get("dataset_id") == expected_by_field["dataset_id"]
        and dataset_by_field.get("run_id") == CANARY_RUN_ID
        and dataset_by_field.get("previous_dataset_id") is None
        and dataset_by_field.get("cutoff_at") == CANARY_CUTOFF
        and dataset_by_field.get("acquisition_contract_hash")
        == expected_by_field["acquisition_contract_hash"]
        and dataset_by_field.get("status") in {"building", "verified"}
        and dataset_by_field.get("publish_requested") is False
        and dataset_by_field.get("seed_eligible") is True
    )


async def _require_recoverable_catalog(database: Any) -> None:
    await _require_empty_pointer(database)
    dataset_rows = await _catalog_datasets(database)
    table_counts = await _catalog_table_counts(database)
    if len(dataset_rows) > 1:
        raise SyntheticCanaryError("catalog")
    if dataset_rows and not _is_recoverable_dataset(dataset_rows[0]):
        raise SyntheticCanaryError("catalog")
    if not dataset_rows:
        counts_by_non_source_table = {
            table: count
            for table, count in table_counts.items()
            if table not in {"fhir_formulary_source", "fhir_formulary_current"}
        }
        if any(counts_by_non_source_table.values()):
            raise SyntheticCanaryError("catalog")
    elif any(
        table_counts[table] > maximum_count
        for table, maximum_count in CANARY_FINAL_TABLE_COUNTS.items()
    ):
        raise SyntheticCanaryError("catalog")


async def _require_exact_verified_graph(database: Any) -> None:
    await _require_empty_pointer(database)
    sources = await _catalog_sources(database)
    datasets = await _catalog_datasets(database)
    if len(sources) != 1 or not _is_exact_source(sources[0], enabled=True):
        raise SyntheticCanaryError("catalog")
    if (
        len(datasets) != 1
        or not _is_recoverable_dataset(datasets[0])
        or datasets[0].get("status") != "verified"
    ):
        raise SyntheticCanaryError("evidence")
    if await _catalog_table_counts(database) != CANARY_FINAL_TABLE_COUNTS:
        raise SyntheticCanaryError("evidence")


async def _insert_exact_source(database: Any, source_table: str) -> None:
    inserted_count = await database.status(
        f"INSERT INTO {source_table} (source_id, canonical_base, "
        "display_name, enabled, runtime_config_json, metadata_json) "
        "VALUES (:source_id, :canonical_base, :display_name, true, "
        "CAST(:runtime_config_json AS jsonb), "
        "CAST(:metadata_json AS jsonb));",
        source_id=CANARY_SOURCE_ID,
        canonical_base=CANARY_SOURCE_BASE,
        display_name=CANARY_SOURCE_DISPLAY_NAME,
        runtime_config_json=json_text(canary_runtime_config()),
        metadata_json=json_text(canary_metadata()),
    )
    if inserted_count != 1:
        raise SyntheticCanaryError("source")


async def _enable_existing_source(
    database: Any,
    source_table: str,
    source_by_field: dict[str, Any],
) -> None:
    if _is_exact_source(source_by_field, enabled=True):
        return
    if not _is_exact_source(source_by_field, enabled=False):
        raise SyntheticCanaryError("catalog")
    updated_count = await database.status(
        f"UPDATE {source_table} SET enabled = true, "
        "updated_at = transaction_timestamp() WHERE "
        "source_id = :source_id AND enabled = false;",
        source_id=CANARY_SOURCE_ID,
    )
    if updated_count != 1:
        raise SyntheticCanaryError("source")


async def _enable_exact_source(database: Any) -> None:
    source_table = table_name("fhir_formulary_source")
    async with database.transaction():
        await database.status(
            f"LOCK TABLE {source_table} IN SHARE ROW EXCLUSIVE MODE;"
        )
        source_rows = await _catalog_sources(database)
        if not source_rows:
            await _insert_exact_source(database, source_table)
        elif len(source_rows) == 1:
            await _enable_existing_source(database, source_table, source_rows[0])
        else:
            raise SyntheticCanaryError("catalog")
        await _require_recoverable_catalog(database)
        enabled_sources = await _catalog_sources(database)
        if len(enabled_sources) != 1 or not _is_exact_source(
            enabled_sources[0],
            enabled=True,
        ):
            raise SyntheticCanaryError("source")


async def _disable_exact_source(
    database: Any,
    *,
    require_verified_graph: bool,
    is_reserved_source_claimed: bool,
) -> None:
    source_table = table_name("fhir_formulary_source")
    state_error: SyntheticCanaryError | None = None
    async with database.transaction():
        await database.status(
            f"LOCK TABLE {source_table} IN SHARE ROW EXCLUSIVE MODE;"
        )
        if require_verified_graph:
            try:
                await _require_exact_verified_graph(database)
            except SyntheticCanaryError as error:
                state_error = error
        source_by_field = row_mapping(
            await database.first(
                f"SELECT source_id, canonical_base, display_name, enabled, "
                f"runtime_config_json, metadata_json FROM {source_table} "
                "WHERE source_id = :source_id FOR UPDATE;",
                source_id=CANARY_SOURCE_ID,
            )
        )
        if (
            not source_by_field
            and not require_verified_graph
            and not is_reserved_source_claimed
        ):
            return
        if not source_by_field:
            state_error = state_error or SyntheticCanaryError("source")
            raise state_error
        is_source_enabled = source_by_field.get("enabled") is True
        is_source_exact = _is_exact_source(
            source_by_field,
            enabled=is_source_enabled,
        )
        if not is_source_exact and not is_reserved_source_claimed:
            return
        if require_verified_graph and not is_source_exact:
            state_error = state_error or SyntheticCanaryError("source")
        if is_source_enabled:
            updated_count = await database.status(
                f"UPDATE {source_table} SET enabled = false, "
                "updated_at = transaction_timestamp() WHERE "
                "source_id = :source_id AND enabled = true;",
                source_id=CANARY_SOURCE_ID,
            )
            if updated_count != 1:
                raise SyntheticCanaryError("cleanup")
    if state_error is not None:
        raise state_error


def _candidate_result(
    source_configuration_hash: str,
    synchronization_result: SynchronizationResult,
) -> SyntheticSeedCandidateResult:
    expected_by_field = expected_evidence()
    invariant_by_field = {
        "source_configuration_hash": source_configuration_hash,
        "dataset_id": synchronization_result.dataset_id,
        "acquisition_contract_hash": (
            synchronization_result.acquisition_contract_hash
        ),
        "coverage_hash": synchronization_result.coverage_hash,
        "membership_hash": synchronization_result.membership_hash,
        "list_count": synchronization_result.list_count,
        "alias_count": synchronization_result.alias_count,
        "medication_membership_count": (
            synchronization_result.medication_membership_count
        ),
        "reused_aliases": synchronization_result.reused_aliases,
        "transient_retry_count": synchronization_result.transient_retry_count,
        "throttle_count": synchronization_result.throttle_count,
    }
    if any(
        invariant_by_field.get(field_name) != expected_by_field.get(field_name)
        for field_name in invariant_by_field
    ):
        raise SyntheticCanaryError("evidence")
    acquired_aliases = (
        synchronization_result.full_aliases
        + synchronization_result.reused_aliases
    )
    is_verified_replay = (
        acquired_aliases == 0 and synchronization_result.resumed_aliases == 2
    )
    is_build_completion = (
        acquired_aliases == 2
        and 0 <= synchronization_result.resumed_aliases <= 2
    )
    pending_aliases = 2 - synchronization_result.resumed_aliases
    if (
        not (is_verified_replay or is_build_completion)
        or synchronization_result.request_count != 3 + (3 * pending_aliases)
    ):
        raise SyntheticCanaryError("evidence")
    return SyntheticSeedCandidateResult(
        synchronization_result.dataset_id,
        source_configuration_hash,
        synchronization_result.acquisition_contract_hash,
        synchronization_result.list_count,
        synchronization_result.alias_count,
        synchronization_result.medication_membership_count,
        synchronization_result.coverage_hash,
        synchronization_result.membership_hash,
        synchronization_result.full_aliases,
        synchronization_result.resumed_aliases,
        synchronization_result.request_count,
    )


async def _verify_enabled_candidate(
    database: Any,
) -> SyntheticSeedCandidateResult:
    binding = await load_enabled_source(CANARY_SOURCE_ID, database=database)
    repository = FHIRFormularyRepository(
        source_id=CANARY_SOURCE_ID,
        database=database,
    )
    async with SyntheticCanaryClient(binding.config) as client:
        synchronization_result = await _run_verified_sync(
            binding=binding,
            client=client,
            repository=repository,
            database=database,
            run_id=CANARY_RUN_ID,
            cutoff_at=CANARY_CUTOFF,
            intent="seed",
        )
    return _candidate_result(
        binding.configuration_hash,
        synchronization_result,
    )


async def verify_synthetic_seed_candidate(
    *,
    database: Any = db,
) -> SyntheticSeedCandidateResult:
    """Build one fixed verified seed candidate, then disable its source."""

    if not _is_canary_enabled():
        raise SyntheticCanaryError("disabled")
    is_reserved_source_claimed = False
    try:
        async with manual_lock.manual_source_lease(
            database,
            CANARY_SOURCE_ID,
            wait_seconds=CANARY_LOCK_WAIT_SECONDS,
            retry_seconds=CANARY_LOCK_RETRY_SECONDS,
        ):
            try:
                    async with asyncio.timeout(CANARY_TIMEOUT_SECONDS):
                        await _enable_exact_source(database)
                        is_reserved_source_claimed = True
                        candidate_result = await _verify_enabled_candidate(database)
            except BaseException:
                try:
                    await manual_lock._drain(
                        _disable_exact_source(
                            database,
                            require_verified_graph=False,
                            is_reserved_source_claimed=is_reserved_source_claimed,
                        ),
                        preserve_cancellation=False,
                    )
                except BaseException:
                    raise SyntheticCanaryError("cleanup") from None
                raise
            try:
                await manual_lock._drain(
                    _disable_exact_source(
                        database,
                        require_verified_graph=True,
                        is_reserved_source_claimed=True,
                    ),
                    preserve_cancellation=True,
                    should_prefer_operation_error=True,
                )
            except (asyncio.CancelledError, SyntheticCanaryError):
                raise
            except BaseException:
                raise SyntheticCanaryError("cleanup") from None
            return candidate_result
    except manual_lock.ManualSourceLockError as error:
        raise SyntheticCanaryError(error.code) from None
    except asyncio.CancelledError:
        raise
    except SyntheticCanaryError:
        raise


def candidate_result_json(result: SyntheticSeedCandidateResult) -> str:
    """Serialize the fixed safe candidate evidence schema."""

    if type(result) is not SyntheticSeedCandidateResult:
        raise SyntheticCanaryError("evidence")
    return json_text(
        {
            "status": "verified",
            "dataset_id": result.dataset_id,
            "source_configuration_hash": result.source_configuration_hash,
            "acquisition_contract_hash": result.acquisition_contract_hash,
            "list_count": result.list_count,
            "alias_count": result.alias_count,
            "medication_membership_count": result.medication_membership_count,
            "coverage_hash": result.coverage_hash,
            "membership_hash": result.membership_hash,
            "full_aliases": result.full_aliases,
            "resumed_aliases": result.resumed_aliases,
            "request_count": result.request_count,
        }
    )


__all__ = (
    "SyntheticCanaryError",
    "SyntheticSeedCandidateResult",
    "candidate_result_json",
    "verify_synthetic_seed_candidate",
)
