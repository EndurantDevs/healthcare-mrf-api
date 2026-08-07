# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off exact publisher for the fixed synthetic formulary seed."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass
import os
from typing import Any

from db.models import db
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.repository_shared import json_object
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_verify import (
    _recompute_dataset_verification,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_CUTOFF
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_FINAL_TABLE_COUNTS,
)
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_LOCK_RETRY_SECONDS,
)
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_LOCK_WAIT_SECONDS,
)
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_PUBLICATION_TIMEOUT_SECONDS,
)
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_PUBLISHED_TABLE_COUNTS,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_RUN_ID
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_BASE
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_SOURCE_DISPLAY_NAME,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import canary_metadata
from process.formulary_fhir.synthetic_canary_contract import canary_runtime_config
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_canary_contract import (
    SEED_PUBLICATION_ENABLED_ENV,
)


TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
ERROR_MESSAGES = {
    "busy": "synthetic formulary seed source is busy",
    "catalog": "synthetic formulary seed catalog is not isolated",
    "cleanup": "synthetic formulary seed cleanup failed",
    "disabled": "synthetic formulary seed publication is disabled",
    "evidence": "synthetic formulary seed evidence is invalid",
    "lock_unavailable": "synthetic formulary seed lock is unavailable",
    "publication": "synthetic formulary seed publication failed",
}


class SyntheticSeedPublicationError(RuntimeError):
    """Expose only one stable fixed-publisher failure code."""

    def __init__(self, code: str) -> None:
        self.code = code if code in ERROR_MESSAGES else "publication"
        super().__init__(ERROR_MESSAGES[self.code])


@dataclass(frozen=True, slots=True)
class SyntheticSeedPublicationResult:
    """Expose only the exact published evidence and pointer identity."""

    dataset_id: str
    generation: int
    published_at: dt.datetime
    source_configuration_hash: str
    acquisition_contract_hash: str
    list_count: int
    alias_count: int
    medication_membership_count: int
    coverage_hash: str
    membership_hash: str


def _is_publication_enabled() -> bool:
    raw_setting = os.getenv(SEED_PUBLICATION_ENABLED_ENV, "")
    return raw_setting.strip().lower() in TRUE_ENV_VALUES


def _source_values() -> dict[str, object]:
    return {
        "source_id": CANARY_SOURCE_ID,
        "canonical_base": CANARY_SOURCE_BASE,
        "display_name": CANARY_SOURCE_DISPLAY_NAME,
        "enabled": False,
        "runtime_config_json": canary_runtime_config(),
        "metadata_json": canary_metadata(),
    }


def _is_exact_disabled_source(source_by_field: dict[str, Any]) -> bool:
    observed_by_field = {
        field_name: source_by_field.get(field_name)
        for field_name in _source_values()
    }
    return json_text(observed_by_field) == json_text(_source_values())


async def _source_rows(database: Any) -> tuple[dict[str, Any], ...]:
    source_records = await database.all(
        f"SELECT source_id, canonical_base, display_name, enabled, "
        f"runtime_config_json, metadata_json FROM "
        f"{table_name('fhir_formulary_source')} ORDER BY source_id "
        "LIMIT 2 FOR UPDATE;"
    )
    return tuple(row_mapping(source_record) for source_record in source_records)


async def _lock_exact_source(database: Any) -> None:
    await database.status(
        f"LOCK TABLE {table_name('fhir_formulary_source')} "
        "IN SHARE ROW EXCLUSIVE MODE;"
    )
    source_records = await _source_rows(database)
    if len(source_records) != 1 or not _is_exact_disabled_source(
        source_records[0]
    ):
        raise SyntheticSeedPublicationError("catalog")


def _expected_summary() -> dict[str, object]:
    expected_by_field = expected_evidence()
    return {
        "acquisition_contract_hash": expected_by_field[
            "acquisition_contract_hash"
        ],
        "list_count": expected_by_field["list_count"],
        "alias_count": expected_by_field["alias_count"],
        "medication_membership_count": expected_by_field[
            "medication_membership_count"
        ],
    }


async def _locked_dataset_row(database: Any) -> dict[str, Any]:
    dataset_record = await database.first(
        f"SELECT source_id, dataset_id, run_id, previous_dataset_id, cutoff_at, "
        "status, publish_requested, seed_eligible, list_count, alias_count, "
        "medication_count, coverage_hash, membership_hash, summary_json, "
        "verified_at, published_at, failed_at, error_json FROM "
        f"{table_name('fhir_formulary_dataset')} WHERE "
        "source_id = :source_id AND dataset_id = :dataset_id "
        "AND run_id = :run_id FOR UPDATE;",
        source_id=CANARY_SOURCE_ID,
        dataset_id=expected_evidence()["dataset_id"],
        run_id=CANARY_RUN_ID,
    )
    return row_mapping(dataset_record)


def _candidate_dataset(dataset_by_field: dict[str, Any]) -> DatasetRef:
    expected_by_field = expected_evidence()
    has_exact_fields = (
        dataset_by_field.get("source_id") == CANARY_SOURCE_ID
        and dataset_by_field.get("dataset_id") == expected_by_field["dataset_id"]
        and dataset_by_field.get("run_id") == CANARY_RUN_ID
        and dataset_by_field.get("previous_dataset_id") is None
        and dataset_by_field.get("cutoff_at") == CANARY_CUTOFF
        and dataset_by_field.get("publish_requested") is False
        and dataset_by_field.get("seed_eligible") is True
        and type(dataset_by_field.get("list_count")) is int
        and dataset_by_field.get("list_count") == expected_by_field["list_count"]
        and type(dataset_by_field.get("alias_count")) is int
        and dataset_by_field.get("alias_count") == expected_by_field["alias_count"]
        and type(dataset_by_field.get("medication_count")) is int
        and dataset_by_field.get("medication_count")
        == expected_by_field["medication_membership_count"]
        and dataset_by_field.get("coverage_hash")
        == expected_by_field["coverage_hash"]
        and dataset_by_field.get("membership_hash")
        == expected_by_field["membership_hash"]
    )
    has_exact_lifecycle_fields = (
        dataset_by_field.get("status") in {"verified", "published"}
        and type(dataset_by_field.get("verified_at")) is dt.datetime
        and dataset_by_field["verified_at"].tzinfo is not None
        and dataset_by_field.get("failed_at") is None
        and dataset_by_field.get("error_json") is None
    )
    try:
        is_summary_exact = (
            json_object(dataset_by_field.get("summary_json"))
            == _expected_summary()
        )
    except RuntimeError:
        is_summary_exact = False
    if not (has_exact_fields and has_exact_lifecycle_fields and is_summary_exact):
        raise SyntheticSeedPublicationError("evidence")
    return DatasetRef(
        CANARY_SOURCE_ID,
        expected_by_field["dataset_id"],
        CANARY_RUN_ID,
        None,
        CANARY_CUTOFF,
        expected_by_field["acquisition_contract_hash"],
        "seed",
        dataset_by_field["status"],
    )


async def _locked_pointer(database: Any) -> dict[str, Any]:
    pointer_record = await database.first(
        f"SELECT source_id, dataset_id, generation, published_at FROM "
        f"{table_name('fhir_formulary_current')} WHERE "
        "source_id = :source_id FOR UPDATE;",
        source_id=CANARY_SOURCE_ID,
    )
    return row_mapping(pointer_record)


async def _table_counts(database: Any) -> dict[str, int]:
    return {
        table: int(
            await database.scalar(f"SELECT count(*) FROM {table_name(table)};")
            or 0
        )
        for table in CANARY_PUBLISHED_TABLE_COUNTS
    }


def _require_exact_state(
    dataset_by_field: dict[str, Any],
    pointer_by_field: dict[str, Any],
    counts_by_table: dict[str, int],
) -> None:
    status = dataset_by_field.get("status")
    published_at = dataset_by_field.get("published_at")
    if status == "verified":
        is_exact = (
            published_at is None
            and not pointer_by_field
            and counts_by_table == CANARY_FINAL_TABLE_COUNTS
        )
    else:
        is_exact = (
            type(published_at) is dt.datetime
            and published_at.tzinfo is not None
            and pointer_by_field.get("source_id") == CANARY_SOURCE_ID
            and pointer_by_field.get("dataset_id")
            == expected_evidence()["dataset_id"]
            and type(pointer_by_field.get("generation")) is int
            and pointer_by_field.get("generation") == 1
            and pointer_by_field.get("published_at") == published_at
            and counts_by_table == CANARY_PUBLISHED_TABLE_COUNTS
        )
    if not is_exact:
        raise SyntheticSeedPublicationError("catalog")


def _require_exact_verification(
    verification: DatasetVerification,
) -> None:
    expected_by_field = expected_evidence()
    is_exact_evidence = (
        type(verification) is DatasetVerification
        and verification.source_id == CANARY_SOURCE_ID
        and verification.dataset_id == expected_by_field["dataset_id"]
        and type(verification.list_count) is int
        and verification.list_count == expected_by_field["list_count"]
        and type(verification.alias_count) is int
        and verification.alias_count == expected_by_field["alias_count"]
        and type(verification.medication_membership_count) is int
        and verification.medication_membership_count
        == expected_by_field["medication_membership_count"]
        and verification.coverage_hash == expected_by_field["coverage_hash"]
        and verification.membership_hash == expected_by_field["membership_hash"]
    )
    if not is_exact_evidence:
        raise SyntheticSeedPublicationError("evidence")


async def _preflight(database: Any) -> DatasetRef:
    await _lock_exact_source(database)
    dataset_by_field = await _locked_dataset_row(database)
    dataset = _candidate_dataset(dataset_by_field)
    verification = await _recompute_dataset_verification(
        database,
        CANARY_SOURCE_ID,
        dataset,
    )
    _require_exact_verification(verification)
    pointer_by_field = await _locked_pointer(database)
    _require_exact_state(
        dataset_by_field,
        pointer_by_field,
        await _table_counts(database),
    )
    return dataset


def _require_exact_publication(
    publication: PublicationResult,
    dataset_by_field: dict[str, Any],
    pointer_by_field: dict[str, Any],
) -> None:
    published_at = dataset_by_field.get("published_at")
    is_exact = (
        type(publication) is PublicationResult
        and publication.source_id == CANARY_SOURCE_ID
        and publication.dataset_id == expected_evidence()["dataset_id"]
        and type(publication.generation) is int
        and publication.generation == 1
        and type(publication.published_at) is dt.datetime
        and publication.published_at.tzinfo is not None
        and publication.published_at == published_at
        and pointer_by_field.get("published_at") == published_at
    )
    if not is_exact:
        raise SyntheticSeedPublicationError("publication")


async def _postflight(
    database: Any,
    publication: PublicationResult,
) -> SyntheticSeedPublicationResult:
    source_records = await _source_rows(database)
    if len(source_records) != 1 or not _is_exact_disabled_source(
        source_records[0]
    ):
        raise SyntheticSeedPublicationError("publication")
    dataset_by_field = await _locked_dataset_row(database)
    dataset = _candidate_dataset(dataset_by_field)
    pointer_by_field = await _locked_pointer(database)
    _require_exact_state(
        dataset_by_field,
        pointer_by_field,
        await _table_counts(database),
    )
    if dataset.status != "published":
        raise SyntheticSeedPublicationError("publication")
    _require_exact_publication(publication, dataset_by_field, pointer_by_field)
    return _publication_result(publication)


def _publication_result(
    publication: PublicationResult,
) -> SyntheticSeedPublicationResult:
    expected_by_field = expected_evidence()
    return SyntheticSeedPublicationResult(
        dataset_id=publication.dataset_id,
        generation=publication.generation,
        published_at=publication.published_at,
        source_configuration_hash=expected_by_field[
            "source_configuration_hash"
        ],
        acquisition_contract_hash=expected_by_field[
            "acquisition_contract_hash"
        ],
        list_count=expected_by_field["list_count"],
        alias_count=expected_by_field["alias_count"],
        medication_membership_count=expected_by_field[
            "medication_membership_count"
        ],
        coverage_hash=expected_by_field["coverage_hash"],
        membership_hash=expected_by_field["membership_hash"],
    )


async def _publish_transaction(database: Any) -> SyntheticSeedPublicationResult:
    async with database.transaction():
        dataset = await _preflight(database)
        repository = FHIRFormularyRepository(
            source_id=CANARY_SOURCE_ID,
            database=database,
        )
        publication = await repository.publish_verified_seed(dataset=dataset)
        return await _postflight(database, publication)


async def publish_synthetic_seed(
    *,
    database: Any = db,
) -> SyntheticSeedPublicationResult:
    """Publish or exactly replay the one fixed synthetic generation-one seed."""

    if not _is_publication_enabled():
        raise SyntheticSeedPublicationError("disabled")
    try:
        async with manual_lock.manual_source_lease(
            database,
            CANARY_SOURCE_ID,
            wait_seconds=CANARY_LOCK_WAIT_SECONDS,
            retry_seconds=CANARY_LOCK_RETRY_SECONDS,
        ):
            async with asyncio.timeout(CANARY_PUBLICATION_TIMEOUT_SECONDS):
                return await _publish_transaction(database)
    except manual_lock.ManualSourceLockError as error:
        raise SyntheticSeedPublicationError(error.code) from None
    except (asyncio.CancelledError, TimeoutError, SyntheticSeedPublicationError):
        raise
    except Exception:
        raise SyntheticSeedPublicationError("publication") from None


def publication_result_json(
    publication: SyntheticSeedPublicationResult,
) -> str:
    """Serialize the fixed safe generation-one publication evidence."""

    expected_by_field = expected_evidence()
    is_exact_publication = (
        type(publication) is SyntheticSeedPublicationResult
        and publication.dataset_id == expected_by_field["dataset_id"]
        and type(publication.generation) is int
        and publication.generation == 1
        and type(publication.published_at) is dt.datetime
        and publication.published_at.tzinfo is not None
        and publication.source_configuration_hash
        == expected_by_field["source_configuration_hash"]
        and publication.acquisition_contract_hash
        == expected_by_field["acquisition_contract_hash"]
        and publication.coverage_hash == expected_by_field["coverage_hash"]
        and publication.membership_hash == expected_by_field["membership_hash"]
        and type(publication.list_count) is int
        and publication.list_count == expected_by_field["list_count"]
        and type(publication.alias_count) is int
        and publication.alias_count == expected_by_field["alias_count"]
        and type(publication.medication_membership_count) is int
        and publication.medication_membership_count
        == expected_by_field["medication_membership_count"]
    )
    if not is_exact_publication:
        raise SyntheticSeedPublicationError("evidence")
    return json_text(
        {
            "status": "published",
            "dataset_id": publication.dataset_id,
            "generation": publication.generation,
            "published_at": publication.published_at.astimezone(dt.UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "source_configuration_hash": publication.source_configuration_hash,
            "acquisition_contract_hash": publication.acquisition_contract_hash,
            "list_count": publication.list_count,
            "alias_count": publication.alias_count,
            "medication_membership_count": publication.medication_membership_count,
            "coverage_hash": publication.coverage_hash,
            "membership_hash": publication.membership_hash,
        }
    )


__all__ = (
    "SyntheticSeedPublicationError",
    "SyntheticSeedPublicationResult",
    "publication_result_json",
    "publish_synthetic_seed",
)
