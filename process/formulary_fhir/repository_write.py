# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable CoveragePlan, alias-version, and dataset-link persistence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from process.formulary_fhir.repository_batch import insert_alias_content
from process.formulary_fhir.repository_checkpoint import require_alias
from process.formulary_fhir.repository_checkpoint import save_checkpoint_row
from process.formulary_fhir.repository_coverage import put_coverage_plan
from process.formulary_fhir.repository_shared import AliasRef
from process.formulary_fhir.repository_shared import AliasVersionResult
from process.formulary_fhir.repository_shared import AliasVersionWrite
from process.formulary_fhir.repository_shared import CheckpointWrite
from process.formulary_fhir.repository_shared import CoveragePlanWriteResult
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import PriorAliasState
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.repository_shared import persisted_membership_proof
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.types import CoveragePlanRecord, MedicationRecord


@dataclass(frozen=True, slots=True)
class _PreparedAliasVersion:
    alias_version_id: str
    membership_hash: str
    medications_by_id: dict[str, MedicationRecord]
    variants_by_id: dict[str, str]


def _prepare_alias_version(
    source_id: str,
    write: AliasVersionWrite,
) -> _PreparedAliasVersion:
    medications_by_id: dict[str, MedicationRecord] = {}
    for medication in write.medications:
        strict_hash(medication.content_hash, "medication content hash")
        if (
            medication.source_plan_identifiers
            and write.alias.source_plan_identifier
            not in medication.source_plan_identifiers
        ):
            raise RuntimeError("FHIR formulary medication source plan is invalid")
        medication_id = medication.upstream_medication_id
        if medication_id in medications_by_id:
            raise RuntimeError("FHIR formulary alias has duplicate medication ids")
        medications_by_id[medication_id] = medication
    if len(medications_by_id) != write.expected_count:
        raise RuntimeError("FHIR formulary exact alias count is inconsistent")
    variants_by_id = {
        medication_id: medication_variant_hash(medication)
        for medication_id, medication in medications_by_id.items()
    }
    computed_hash = membership_hash(variants_by_id)
    return _PreparedAliasVersion(
        alias_version_id=stable_id(
            "ffav_",
            source_id,
            write.alias.alias_id,
            computed_hash,
        ),
        membership_hash=computed_hash,
        medications_by_id=medications_by_id,
        variants_by_id=variants_by_id,
    )


async def _insert_alias_version(
    database: Any,
    source_id: str,
    write: AliasVersionWrite,
    prepared: _PreparedAliasVersion,
) -> str:
    await database.status(
        f"INSERT INTO {table_name('fhir_formulary_drug_plan_alias_version')} ("
        "alias_version_id, source_id, alias_id, expected_count, "
        "membership_count, membership_hash, cutoff_at, acquisition_mode, "
        "summary_json) VALUES (:alias_version_id, :source_id, :alias_id, "
        ":expected_count, :membership_count, :membership_hash, :cutoff_at, "
        "'full', CAST(:summary_json AS jsonb)) "
        "ON CONFLICT (alias_id, membership_hash) DO NOTHING;",
        alias_version_id=prepared.alias_version_id,
        source_id=source_id,
        alias_id=write.alias.alias_id,
        expected_count=write.expected_count,
        membership_count=len(prepared.variants_by_id),
        membership_hash=prepared.membership_hash,
        cutoff_at=write.dataset.cutoff_at,
        summary_json=json_text(
            {"exact_count": write.expected_count, "materialization": "full"}
        ),
    )
    version_row = await database.first(
        f"SELECT alias_version_id, expected_count, membership_count, "
        "membership_hash FROM "
        f"{table_name('fhir_formulary_drug_plan_alias_version')} "
        "WHERE source_id = :source_id AND alias_id = :alias_id "
        "AND membership_hash = :membership_hash;",
        source_id=source_id,
        alias_id=write.alias.alias_id,
        membership_hash=prepared.membership_hash,
    )
    version_by_field = row_mapping(version_row)
    expected_by_field = {
        "alias_version_id": prepared.alias_version_id,
        "expected_count": write.expected_count,
        "membership_count": write.expected_count,
        "membership_hash": prepared.membership_hash,
    }
    if version_by_field != expected_by_field:
        raise RuntimeError("FHIR formulary alias version collision")
    return prepared.alias_version_id


async def _assert_persisted_membership(
    database: Any,
    source_id: str,
    alias_version_id: str,
    expected_count: int,
    expected_hash: str,
) -> None:
    persisted_count, persisted_hash, _variants = await persisted_membership_proof(
        database,
        source_id,
        alias_version_id,
    )
    if (persisted_count, persisted_hash) != (expected_count, expected_hash):
        raise RuntimeError("FHIR formulary persisted membership is inconsistent")


async def _link_alias_version(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    alias: AliasRef,
    alias_version_id: str,
) -> None:
    await database.status(
        f"INSERT INTO {table_name('fhir_formulary_dataset_alias')} ("
        "source_id, dataset_id, alias_id, alias_version_id) VALUES ("
        ":source_id, :dataset_id, :alias_id, :alias_version_id) "
        "ON CONFLICT DO NOTHING;",
        source_id=source_id,
        dataset_id=dataset.dataset_id,
        alias_id=alias.alias_id,
        alias_version_id=alias_version_id,
    )
    link_row = await database.first(
        f"SELECT source_id, alias_version_id FROM "
        f"{table_name('fhir_formulary_dataset_alias')} "
        "WHERE source_id = :source_id AND dataset_id = :dataset_id "
        "AND alias_id = :alias_id;",
        source_id=source_id,
        dataset_id=dataset.dataset_id,
        alias_id=alias.alias_id,
    )
    if row_mapping(link_row) != {
        "source_id": source_id,
        "alias_version_id": alias_version_id,
    }:
        raise RuntimeError("FHIR formulary dataset alias link is inconsistent")


def _completed_checkpoint_write(
    dataset: DatasetRef,
    alias: AliasRef,
    *,
    fence_token: int,
    acquisition_mode: str,
    expected_count: int,
    membership_hash_value: str,
) -> CheckpointWrite:
    return CheckpointWrite(
        dataset=dataset,
        alias=alias,
        fence_token=fence_token,
        acquisition_mode=acquisition_mode,
        expected_count=expected_count,
        processed_count=expected_count,
        membership_hash=membership_hash_value,
        completed=True,
    )


async def _persist_full_alias(
    database: Any,
    source_id: str,
    write: AliasVersionWrite,
    prepared: _PreparedAliasVersion,
) -> str:
    await lock_dataset(
        database,
        source_id,
        write.dataset,
        allowed_statuses={"building"},
    )
    await require_alias(database, source_id, write.alias)
    alias_version_id = await _insert_alias_version(
        database,
        source_id,
        write,
        prepared,
    )
    await insert_alias_content(
        database,
        source_id,
        alias_version_id,
        prepared.medications_by_id,
        prepared.variants_by_id,
    )
    await _assert_persisted_membership(
        database,
        source_id,
        alias_version_id,
        write.expected_count,
        prepared.membership_hash,
    )
    await _link_alias_version(
        database,
        source_id,
        write.dataset,
        write.alias,
        alias_version_id,
    )
    await save_checkpoint_row(
        database,
        source_id,
        _completed_checkpoint_write(
            write.dataset,
            write.alias,
            fence_token=write.fence_token,
            acquisition_mode="full",
            expected_count=write.expected_count,
            membership_hash_value=prepared.membership_hash,
        ),
    )
    return alias_version_id


async def _require_predecessor_alias(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    alias: AliasRef,
    prior: PriorAliasState,
) -> None:
    if dataset.previous_dataset_id is None:
        raise RuntimeError("FHIR formulary reuse requires a predecessor")
    predecessor_row = await database.first(
        f"SELECT dataset_alias.source_id, plan_alias.public_id, "
        "dataset_alias.alias_id, plan_alias.source_plan_identifier, "
        "dataset_alias.dataset_id AS predecessor_dataset_id, "
        "dataset_alias.alias_version_id, alias_version.expected_count, "
        "alias_version.membership_count, alias_version.membership_hash, "
        f"alias_version.cutoff_at FROM {table_name('fhir_formulary_dataset_alias')} "
        f"AS dataset_alias JOIN {table_name('fhir_formulary_dataset')} "
        "AS predecessor ON predecessor.source_id = dataset_alias.source_id "
        "AND predecessor.dataset_id = dataset_alias.dataset_id JOIN "
        f"{table_name('fhir_formulary_drug_plan_alias')} AS plan_alias "
        "ON plan_alias.source_id = dataset_alias.source_id "
        "AND plan_alias.alias_id = dataset_alias.alias_id JOIN "
        f"{table_name('fhir_formulary_drug_plan_alias_version')} AS alias_version "
        "ON alias_version.source_id = dataset_alias.source_id "
        "AND alias_version.alias_id = dataset_alias.alias_id "
        "AND alias_version.alias_version_id = dataset_alias.alias_version_id "
        "WHERE dataset_alias.source_id = :source_id "
        "AND dataset_alias.dataset_id = :previous_dataset_id "
        "AND dataset_alias.alias_id = :alias_id "
        "AND dataset_alias.alias_version_id = :alias_version_id "
        "AND predecessor.status = 'published';",
        source_id=source_id,
        previous_dataset_id=dataset.previous_dataset_id,
        alias_id=alias.alias_id,
        alias_version_id=prior.alias_version_id,
    )
    expected_by_field = {
        "source_id": source_id,
        "public_id": alias.public_id,
        "alias_id": alias.alias_id,
        "source_plan_identifier": alias.source_plan_identifier,
        "predecessor_dataset_id": dataset.previous_dataset_id,
        "alias_version_id": prior.alias_version_id,
        "expected_count": prior.expected_count,
        "membership_count": prior.expected_count,
        "membership_hash": prior.membership_hash,
        "cutoff_at": prior.cutoff_at,
    }
    if row_mapping(predecessor_row) != expected_by_field:
        raise RuntimeError("FHIR formulary predecessor alias is inconsistent")


async def _persist_reused_alias(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    alias: AliasRef,
    prior: PriorAliasState,
    fence_token: int,
) -> None:
    await lock_dataset(
        database,
        source_id,
        dataset,
        allowed_statuses={"building"},
    )
    await require_alias(database, source_id, alias)
    await _require_predecessor_alias(database, source_id, dataset, alias, prior)
    await _assert_persisted_membership(
        database,
        source_id,
        prior.alias_version_id,
        prior.expected_count,
        prior.membership_hash,
    )
    await _link_alias_version(
        database,
        source_id,
        dataset,
        alias,
        prior.alias_version_id,
    )
    await save_checkpoint_row(
        database,
        source_id,
        _completed_checkpoint_write(
            dataset,
            alias,
            fence_token=fence_token,
            acquisition_mode="reuse",
            expected_count=prior.expected_count,
            membership_hash_value=prior.membership_hash,
        ),
    )


class FHIRFormularyWriteMixin:
    """Persist source-owned immutable content without activating acquisition."""

    _database: Any
    source_id: str

    async def put_coverage_plan(
        self,
        *,
        dataset: DatasetRef,
        plan: CoveragePlanRecord,
    ) -> CoveragePlanWriteResult:
        """Store one CoveragePlan version and its immutable alias identities."""

        return await put_coverage_plan(
            self._database,
            self.source_id,
            dataset,
            plan,
        )

    async def put_alias_version(
        self,
        write: AliasVersionWrite,
    ) -> AliasVersionResult:
        """Atomically persist, link, and complete one full exact alias."""

        prepared = _prepare_alias_version(self.source_id, write)
        async with self._database.transaction():
            alias_version_id = await _persist_full_alias(
                self._database,
                self.source_id,
                write,
                prepared,
            )
        return AliasVersionResult(
            self.source_id,
            write.dataset.dataset_id,
            write.alias.alias_id,
            alias_version_id,
            write.expected_count,
            prepared.membership_hash,
            "full",
        )

    async def link_reused_alias(
        self,
        *,
        dataset: DatasetRef,
        alias: AliasRef,
        prior: PriorAliasState,
        fence_token: int,
    ) -> AliasVersionResult:
        """Atomically link and complete one unchanged immutable alias."""

        expected_prior = (
            self.source_id,
            alias.public_id,
            alias.alias_id,
            alias.source_plan_identifier,
        )
        actual_prior = (
            prior.source_id,
            prior.public_id,
            prior.alias_id,
            prior.source_plan_identifier,
        )
        if actual_prior != expected_prior:
            raise RuntimeError("FHIR formulary prior alias ownership is invalid")
        async with self._database.transaction():
            await _persist_reused_alias(
                self._database,
                self.source_id,
                dataset,
                alias,
                prior,
                fence_token,
            )
        return AliasVersionResult(
            self.source_id,
            dataset.dataset_id,
            alias.alias_id,
            prior.alias_version_id,
            prior.expected_count,
            prior.membership_hash,
            "reuse",
        )


__all__ = ("FHIRFormularyWriteMixin",)
