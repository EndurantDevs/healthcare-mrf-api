# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant source-qualified formulary repository and verifier."""

from __future__ import annotations

import datetime as dt
from typing import Any

from db.models import db
from process.formulary_fhir.repository_checkpoint import FHIRFormularyCheckpointMixin
from process.formulary_fhir.repository_publish import FHIRFormularyPublicationMixin
from process.formulary_fhir.repository_shared import AliasRef
from process.formulary_fhir.repository_shared import AliasVersionResult
from process.formulary_fhir.repository_shared import AliasVersionWrite
from process.formulary_fhir.repository_shared import CheckpointWrite
from process.formulary_fhir.repository_shared import CompletedAliasCheckpoint
from process.formulary_fhir.repository_shared import CoveragePlanWriteResult
from process.formulary_fhir.repository_shared import CurrentSnapshot
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.repository_shared import flags_intent
from process.formulary_fhir.repository_shared import intent_flags
from process.formulary_fhir.repository_shared import json_object
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import lock_source
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.repository_shared import persisted_membership_proof
from process.formulary_fhir.repository_shared import PriorAliasState
from process.formulary_fhir.repository_shared import PublicationIntent
from process.formulary_fhir.repository_shared import PublicationResult
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.repository_verify import FHIRFormularyVerificationMixin
from process.formulary_fhir.repository_verify import snapshot_alias_rows
from process.formulary_fhir.repository_write import FHIRFormularyWriteMixin


def _contract_hash(summary_json: Any) -> str:
    summary_by_field = json_object(summary_json)
    return strict_hash(
        summary_by_field.get("acquisition_contract_hash"),
        "stored acquisition contract hash",
    )


def _dataset_ref(dataset_by_field: dict[str, Any]) -> DatasetRef:
    return DatasetRef(
        source_id=dataset_by_field["source_id"],
        dataset_id=dataset_by_field["dataset_id"],
        run_id=dataset_by_field["run_id"],
        previous_dataset_id=dataset_by_field.get("previous_dataset_id"),
        cutoff_at=dataset_by_field["cutoff_at"],
        acquisition_contract_hash=_contract_hash(
            dataset_by_field.get("summary_json")
        ),
        intent=flags_intent(
            dataset_by_field.get("publish_requested"),
            dataset_by_field.get("seed_eligible"),
        ),
        status=dataset_by_field["status"],
    )


async def _current_dataset_row(database: Any, source_id: str) -> dict[str, Any]:
    current_row = await database.first(
        f"SELECT dataset.source_id, dataset.dataset_id, dataset.run_id, "
        "dataset.previous_dataset_id, dataset.cutoff_at, dataset.status, "
        "dataset.publish_requested, dataset.seed_eligible, dataset.summary_json "
        f"FROM {table_name('fhir_formulary_current')} AS current JOIN "
        f"{table_name('fhir_formulary_dataset')} AS dataset "
        "ON dataset.source_id = current.source_id "
        "AND dataset.dataset_id = current.dataset_id "
        "WHERE current.source_id = :source_id;",
        source_id=source_id,
    )
    return row_mapping(current_row)


async def _insert_dataset(
    database: Any,
    source_id: str,
    dataset_id: str,
    run_id: str,
    previous_dataset_id: str | None,
    cutoff_at: dt.datetime,
    intent: PublicationIntent,
    acquisition_contract_hash: str,
) -> None:
    publish_requested, seed_eligible = intent_flags(intent)
    inserted_count = await database.status(
        f"INSERT INTO {table_name('fhir_formulary_dataset')} ("
        "dataset_id, source_id, run_id, previous_dataset_id, cutoff_at, "
        "status, publish_requested, seed_eligible, summary_json) VALUES ("
        ":dataset_id, :source_id, :run_id, :previous_dataset_id, :cutoff_at, "
        "'building', :publish_requested, :seed_eligible, "
        "CAST(:summary_json AS jsonb)) ON CONFLICT (run_id) DO NOTHING;",
        dataset_id=dataset_id,
        source_id=source_id,
        run_id=run_id,
        previous_dataset_id=previous_dataset_id,
        cutoff_at=cutoff_at,
        publish_requested=publish_requested,
        seed_eligible=seed_eligible,
        summary_json=json_text(
            {"acquisition_contract_hash": acquisition_contract_hash}
        ),
    )
    if inserted_count not in {0, 1}:
        raise RuntimeError("FHIR formulary dataset insert count is invalid")


async def _dataset_by_run(
    database: Any,
    source_id: str,
    run_id: str,
) -> dict[str, Any]:
    dataset_row = await database.first(
        f"SELECT source_id, dataset_id, run_id, previous_dataset_id, cutoff_at, "
        "status, publish_requested, seed_eligible, summary_json FROM "
        f"{table_name('fhir_formulary_dataset')} "
        "WHERE source_id = :source_id AND run_id = :run_id;",
        source_id=source_id,
        run_id=run_id,
    )
    return row_mapping(dataset_row)


def _validate_resumed_dataset(
    dataset: DatasetRef,
    *,
    expected_dataset_id: str,
    cutoff_at: dt.datetime,
    intent: PublicationIntent,
    acquisition_contract_hash: str,
) -> None:
    if dataset.dataset_id != expected_dataset_id:
        raise RuntimeError("FHIR formulary run identity collision")
    expected_values = (cutoff_at, intent, acquisition_contract_hash)
    stored_values = (
        dataset.cutoff_at,
        dataset.intent,
        dataset.acquisition_contract_hash,
    )
    if stored_values != expected_values:
        raise RuntimeError("FHIR formulary run resume parameters changed")
    if dataset.status not in {"building", "verified"}:
        raise RuntimeError("FHIR formulary dataset is not resumable")


async def _clear_dataset_error(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
) -> None:
    updated_count = await database.status(
        f"UPDATE {table_name('fhir_formulary_dataset')} SET error_json = NULL "
        "WHERE source_id = :source_id AND dataset_id = :dataset_id "
        "AND status IN ('building', 'verified');",
        source_id=source_id,
        dataset_id=dataset.dataset_id,
    )
    if updated_count != 1:
        raise RuntimeError("FHIR formulary dataset resume failed")


def _prior_aliases(
    source_id: str,
    alias_rows: list[Any],
) -> dict[tuple[str, str], PriorAliasState]:
    aliases_by_key: dict[tuple[str, str], PriorAliasState] = {}
    for alias_row in alias_rows:
        alias_by_field = row_mapping(alias_row)
        alias_key = (
            alias_by_field["public_id"],
            alias_by_field["source_plan_identifier"],
        )
        aliases_by_key[alias_key] = PriorAliasState(
            source_id=source_id,
            public_id=alias_by_field["public_id"],
            alias_id=alias_by_field["alias_id"],
            source_plan_identifier=alias_by_field["source_plan_identifier"],
            alias_version_id=alias_by_field["alias_version_id"],
            expected_count=alias_by_field["expected_count"],
            cutoff_at=alias_by_field["cutoff_at"],
            variants_by_medication_id={},
            membership_hash=alias_by_field["membership_hash"],
        )
    return aliases_by_key


class FHIRFormularyRepository(
    FHIRFormularyWriteMixin,
    FHIRFormularyCheckpointMixin,
    FHIRFormularyVerificationMixin,
    FHIRFormularyPublicationMixin,
):
    """Coordinate dormant persistence, verification, and publication."""

    def __init__(self, *, source_id: str, database: Any = db) -> None:
        self.source_id = strict_text(source_id, "source id", 64)
        self._database = database

    async def begin_dataset(
        self,
        *,
        run_id: str,
        cutoff_at: dt.datetime,
        acquisition_contract_hash: str,
        intent: PublicationIntent = "none",
    ) -> DatasetRef:
        """Create or validate one resumable source-owned candidate."""

        normalized_run_id = strict_text(run_id, "run id", 64)
        normalized_cutoff = utc_timestamp(cutoff_at, "dataset cutoff")
        normalized_contract_hash = strict_hash(
            acquisition_contract_hash,
            "acquisition contract hash",
        )
        intent_flags(intent)
        dataset_id = stable_id(
            "ffd_",
            self.source_id,
            normalized_run_id,
        )
        async with self._database.transaction():
            await lock_source(self._database, self.source_id)
            current_by_field = await _current_dataset_row(
                self._database,
                self.source_id,
            )
            previous_dataset_id = current_by_field.get("dataset_id")
            await _insert_dataset(
                self._database,
                self.source_id,
                dataset_id,
                normalized_run_id,
                previous_dataset_id,
                normalized_cutoff,
                intent,
                normalized_contract_hash,
            )
            dataset_by_field = await _dataset_by_run(
                self._database,
                self.source_id,
                normalized_run_id,
            )
            if not dataset_by_field:
                raise RuntimeError("FHIR formulary run identity collision")
            dataset = _dataset_ref(dataset_by_field)
            _validate_resumed_dataset(
                dataset,
                expected_dataset_id=dataset_id,
                cutoff_at=normalized_cutoff,
                intent=intent,
                acquisition_contract_hash=normalized_contract_hash,
            )
            await _clear_dataset_error(
                self._database,
                self.source_id,
                dataset,
            )
            return dataset

    async def current_snapshot(self) -> CurrentSnapshot:
        """Load published alias headers without loading all memberships."""

        dataset_by_field = await _current_dataset_row(
            self._database,
            self.source_id,
        )
        if not dataset_by_field:
            return CurrentSnapshot(None, {})
        dataset = _dataset_ref(dataset_by_field)
        if dataset.status != "published":
            raise RuntimeError("FHIR formulary current dataset is not published")
        alias_rows = await snapshot_alias_rows(
            self._database,
            self.source_id,
            dataset.dataset_id,
        )
        return CurrentSnapshot(
            dataset,
            _prior_aliases(self.source_id, alias_rows),
        )

    async def load_prior_alias_state(
        self,
        prior: PriorAliasState,
    ) -> PriorAliasState:
        """Load and validate one prior membership on demand."""

        if prior.source_id != self.source_id:
            raise RuntimeError("FHIR formulary prior alias source is invalid")
        if prior.variants_by_medication_id:
            variants_by_medication_id = dict(prior.variants_by_medication_id)
            if (
                len(variants_by_medication_id) != prior.expected_count
                or membership_hash(variants_by_medication_id)
                != prior.membership_hash
            ):
                raise RuntimeError("FHIR formulary prior membership is invalid")
            return prior
        count, persisted_hash, variants_by_medication_id = (
            await persisted_membership_proof(
                self._database,
                self.source_id,
                prior.alias_version_id,
            )
        )
        if (count, persisted_hash) != (
            prior.expected_count,
            prior.membership_hash,
        ):
            raise RuntimeError("FHIR formulary prior membership is incomplete")
        return PriorAliasState(
            prior.source_id,
            prior.public_id,
            prior.alias_id,
            prior.source_plan_identifier,
            prior.alias_version_id,
            prior.expected_count,
            prior.cutoff_at,
            variants_by_medication_id,
            prior.membership_hash,
        )

    async def fail_dataset(self, dataset: DatasetRef, exc: BaseException) -> None:
        """Mark one candidate failed without persisting exception content."""

        async with self._database.transaction():
            await lock_dataset(
                self._database,
                self.source_id,
                dataset,
                allowed_statuses={"building", "verified"},
            )
            updated_count = await self._database.status(
                f"UPDATE {table_name('fhir_formulary_dataset')} SET "
                "status = 'failed', failed_at = transaction_timestamp(), "
                "error_json = CAST(:error_json AS jsonb) "
                "WHERE source_id = :source_id AND dataset_id = :dataset_id "
                "AND status IN ('building', 'verified');",
                source_id=self.source_id,
                dataset_id=dataset.dataset_id,
                error_json=json_text({"type": type(exc).__name__}),
            )
            if updated_count != 1:
                raise RuntimeError("FHIR formulary failure transition failed")

    async def interrupt_dataset(
        self,
        dataset: DatasetRef,
        exc: BaseException,
    ) -> None:
        """Retain one candidate for an exact same-run retry."""

        async with self._database.transaction():
            await lock_dataset(
                self._database,
                self.source_id,
                dataset,
                allowed_statuses={"building", "verified"},
            )
            updated_count = await self._database.status(
                f"UPDATE {table_name('fhir_formulary_dataset')} SET "
                "error_json = CAST(:error_json AS jsonb) "
                "WHERE source_id = :source_id AND dataset_id = :dataset_id "
                "AND status IN ('building', 'verified');",
                source_id=self.source_id,
                dataset_id=dataset.dataset_id,
                error_json=json_text(
                    {"type": type(exc).__name__, "resumable": True}
                ),
            )
            if updated_count != 1:
                raise RuntimeError("FHIR formulary interruption write failed")


__all__ = (
    "AliasRef",
    "AliasVersionResult",
    "AliasVersionWrite",
    "CheckpointWrite",
    "CompletedAliasCheckpoint",
    "CoveragePlanWriteResult",
    "CurrentSnapshot",
    "DatasetRef",
    "DatasetVerification",
    "FHIRFormularyRepository",
    "PriorAliasState",
    "PublicationResult",
)
