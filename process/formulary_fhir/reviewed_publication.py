# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off publication of the sole fixed reviewed formulary admission."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from db.models import db
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_admission import ADMISSION_COLUMNS
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_admission_types import result_from_row
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.reviewed_operation import require_publication_gate
from process.formulary_fhir.reviewed_operation import ReviewedOperationError
from process.formulary_fhir.reviewed_operation import ReviewedRunIdentities
from process.formulary_fhir.reviewed_operation import reviewed_run_identities
from process.formulary_fhir.reviewed_source import _is_exact_source
from process.formulary_fhir.reviewed_source import _matching_source_rows
from process.formulary_fhir.reviewed_source import LOCK_RETRY_SECONDS
from process.formulary_fhir.reviewed_source import LOCK_WAIT_SECONDS
from process.formulary_fhir.reviewed_source import ReviewedSourceError
from process.formulary_fhir.reviewed_source import ReviewedSourceManifest
from process.formulary_fhir.reviewed_source import reviewed_source_manifest


PUBLICATION_TIMEOUT_SECONDS = 120


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedPublicationResult:
    """Expose the exact admission evidence bound to one current pointer."""

    candidate_dataset_id: str
    predecessor_dataset_id: str | None
    cutoff_at: dt.datetime
    generation: int
    published_at: dt.datetime
    source_configuration_hash: str = field(repr=False)
    acquisition_contract_hash: str = field(repr=False)
    list_count: int
    alias_count: int
    medication_count: int
    coverage_hash: str = field(repr=False)
    membership_hash: str = field(repr=False)
    alternative_count: int
    alternative_hash: str = field(repr=False)
    admitted_at: dt.datetime

    def __post_init__(self) -> None:
        strict_text(self.candidate_dataset_id, "candidate dataset id", 64)
        if self.predecessor_dataset_id is not None:
            strict_text(self.predecessor_dataset_id, "predecessor dataset id", 64)
        for label, timestamp_value in (
            ("publication cutoff", self.cutoff_at),
            ("publication timestamp", self.published_at),
            ("admission timestamp", self.admitted_at),
        ):
            utc_timestamp(timestamp_value, label)
        if type(self.generation) is not int or self.generation <= 0:
            raise ValueError("reviewed publication generation is invalid")
        for label, evidence_hash in (
            ("source configuration hash", self.source_configuration_hash),
            ("acquisition contract hash", self.acquisition_contract_hash),
            ("coverage hash", self.coverage_hash),
            ("membership hash", self.membership_hash),
            ("alternative hash", self.alternative_hash),
        ):
            strict_hash(evidence_hash, label)
        for count_value in (
            self.list_count,
            self.alias_count,
            self.medication_count,
        ):
            if type(count_value) is not int or count_value <= 0:
                raise ValueError("reviewed publication count is invalid")
        if type(self.alternative_count) is not int or self.alternative_count < 0:
            raise ValueError("reviewed publication alternative count is invalid")


async def _lock_exact_source(
    database: Any,
    manifest: ReviewedSourceManifest,
) -> None:
    await database.status(
        f"LOCK TABLE {table_name('fhir_formulary_source')} "
        "IN SHARE ROW EXCLUSIVE MODE;"
    )
    source_rows = await _matching_source_rows(database, manifest)
    if len(source_rows) != 1 or not _is_exact_source(source_rows[0], manifest):
        raise ReviewedOperationError("evidence")


async def _candidate_rows(
    database: Any,
    manifest: ReviewedSourceManifest,
    identities: ReviewedRunIdentities,
) -> tuple[dict[str, Any], ...]:
    admission_columns = ", ".join(
        f"admission.{column_name}" for column_name in ADMISSION_COLUMNS
    )
    rows = await database.all(
        f"SELECT {admission_columns}, dataset.status AS candidate_status, "
        "dataset.publish_requested AS candidate_publish_requested, "
        "dataset.seed_eligible AS candidate_seed_eligible, "
        "dataset.previous_dataset_id AS candidate_previous_dataset_id, "
        "dataset.cutoff_at AS candidate_cutoff_at FROM "
        f"{table_name('fhir_formulary_twin_admission')} AS admission JOIN "
        f"{table_name('fhir_formulary_dataset')} AS dataset ON "
        "dataset.source_id = admission.source_id AND "
        "dataset.dataset_id = admission.candidate_dataset_id WHERE "
        "admission.source_id = :source_id AND "
        "admission.baseline_run_id = :baseline_run_id AND "
        "admission.candidate_run_id = :candidate_run_id AND "
        "admission.cutoff_at = :cutoff_at ORDER BY admission.candidate_dataset_id "
        "LIMIT 2;",
        source_id=manifest.source_id,
        baseline_run_id=identities.baseline_run_id,
        candidate_run_id=identities.candidate_run_id,
        cutoff_at=identities.cutoff_at,
    )
    return tuple(row_mapping(row) for row in rows)


def _candidate_from_row(
    candidate_by_field: dict[str, Any],
    manifest: ReviewedSourceManifest,
    identities: ReviewedRunIdentities,
) -> tuple[TwinAdmissionResult, DatasetRef]:
    try:
        admission = result_from_row(candidate_by_field)
        expected_baseline_dataset_id = stable_id(
            "ffd_",
            manifest.source_id,
            identities.baseline_run_id,
        )
        expected_candidate_dataset_id = stable_id(
            "ffd_",
            manifest.source_id,
            identities.candidate_run_id,
        )
        has_exact_admission = bool(
            admission.source_id == manifest.source_id
            and admission.baseline_run_id == identities.baseline_run_id
            and admission.candidate_run_id == identities.candidate_run_id
            and admission.baseline_dataset_id == expected_baseline_dataset_id
            and admission.candidate_dataset_id == expected_candidate_dataset_id
            and admission.cutoff_at == identities.cutoff_at
        )
        status = candidate_by_field.get("candidate_status")
        has_exact_dataset = bool(
            status in {"verified", "published"}
            and candidate_by_field.get("candidate_publish_requested") is True
            and candidate_by_field.get("candidate_seed_eligible") is False
            and candidate_by_field.get("candidate_previous_dataset_id")
            == admission.predecessor_dataset_id
            and candidate_by_field.get("candidate_cutoff_at")
            == admission.cutoff_at
        )
        if not (has_exact_admission and has_exact_dataset):
            raise ValueError("candidate mismatch")
        candidate = DatasetRef(
            manifest.source_id,
            admission.candidate_dataset_id,
            admission.candidate_run_id,
            admission.predecessor_dataset_id,
            admission.cutoff_at,
            admission.acquisition_contract_hash,
            "requested",
            status,
        )
        return admission, candidate
    except (TypeError, ValueError):
        raise ReviewedOperationError("evidence") from None


async def _admitted_candidate(
    database: Any,
    manifest: ReviewedSourceManifest,
    identities: ReviewedRunIdentities,
) -> tuple[TwinAdmissionResult, DatasetRef]:
    candidate_rows = await _candidate_rows(
        database,
        manifest,
        identities,
    )
    if len(candidate_rows) != 1:
        raise ReviewedOperationError("missing")
    return _candidate_from_row(candidate_rows[0], manifest, identities)


def _publication_result(
    admission: TwinAdmissionResult,
    publication: PublicationResult,
) -> ReviewedPublicationResult:
    if not (
        type(publication) is PublicationResult
        and publication.source_id == admission.source_id
        and publication.dataset_id == admission.candidate_dataset_id
        and type(publication.generation) is int
        and type(publication.published_at) is dt.datetime
    ):
        raise ReviewedOperationError("evidence")
    verification = admission.verification
    alternative = admission.alternative
    try:
        return ReviewedPublicationResult(
            admission.candidate_dataset_id,
            admission.predecessor_dataset_id,
            admission.cutoff_at,
            publication.generation,
            publication.published_at,
            admission.source_configuration_hash,
            admission.acquisition_contract_hash,
            verification.list_count,
            verification.alias_count,
            verification.medication_membership_count,
            verification.coverage_hash,
            verification.membership_hash,
            alternative.count,
            alternative.evidence_hash,
            admission.admitted_at,
        )
    except (TypeError, ValueError):
        raise ReviewedOperationError("evidence") from None


async def _publish_transaction(
    database: Any,
    identities: ReviewedRunIdentities,
) -> ReviewedPublicationResult:
    manifest = reviewed_source_manifest()
    async with database.transaction():
        await _lock_exact_source(database, manifest)
        admission, candidate = await _admitted_candidate(
            database,
            manifest,
            identities,
        )
        repository = FHIRFormularyRepository(
            source_id=manifest.source_id,
            database=database,
        )
        publication = await repository.publish_dataset(dataset=candidate)
        return _publication_result(admission, publication)


def _publication_error(error: BaseException) -> ReviewedOperationError:
    error_code = getattr(error, "code", "publication")
    if error_code not in {"busy", "missing"}:
        error_code = "publication"
    return ReviewedOperationError(error_code)


async def publish_reviewed_candidate(
    *,
    cutoff: dt.datetime,
    database: Any = db,
) -> ReviewedPublicationResult:
    """Publish or replay the exact admitted candidate for one fixed cutoff."""

    require_publication_gate()
    identities = reviewed_run_identities(cutoff)
    manifest = reviewed_source_manifest()
    try:
        async with manual_lock.manual_source_lease(
            database,
            manifest.source_id,
            wait_seconds=LOCK_WAIT_SECONDS,
            retry_seconds=LOCK_RETRY_SECONDS,
        ):
            async with asyncio.timeout(PUBLICATION_TIMEOUT_SECONDS):
                return await _publish_transaction(database, identities)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ReviewedOperationError:
        raise
    except (ReviewedSourceError, TwinAdmissionError) as error:
        raise _publication_error(error) from None
    except manual_lock.ManualSourceLockError as error:
        raise _publication_error(error) from None
    except Exception:
        raise ReviewedOperationError("publication") from None


def publication_result_json(
    publication_result: ReviewedPublicationResult,
) -> str:
    """Serialize only exact pointer and immutable admission evidence."""

    if type(publication_result) is not ReviewedPublicationResult:
        raise ReviewedOperationError("evidence")
    try:
        publication_result.__post_init__()
    except (TypeError, ValueError):
        raise ReviewedOperationError("evidence") from None
    return json_text(
        {
            "status": "published",
            "candidate_dataset_id": publication_result.candidate_dataset_id,
            "predecessor_dataset_id": (
                publication_result.predecessor_dataset_id
            ),
            "cutoff": publication_result.cutoff_at.isoformat().replace(
                "+00:00", "Z"
            ),
            "generation": publication_result.generation,
            "published_at": publication_result.published_at.isoformat().replace(
                "+00:00", "Z"
            ),
            "source_configuration_hash": (
                publication_result.source_configuration_hash
            ),
            "acquisition_contract_hash": (
                publication_result.acquisition_contract_hash
            ),
            "list_count": publication_result.list_count,
            "alias_count": publication_result.alias_count,
            "medication_count": publication_result.medication_count,
            "coverage_hash": publication_result.coverage_hash,
            "membership_hash": publication_result.membership_hash,
            "alternative_count": publication_result.alternative_count,
            "alternative_hash": publication_result.alternative_hash,
            "admitted_at": publication_result.admitted_at.isoformat().replace(
                "+00:00", "Z"
            ),
        }
    )


__all__ = (
    "ReviewedPublicationResult",
    "publication_result_json",
    "publish_reviewed_candidate",
)
