# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact source-qualified formulary dataset verification."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from process.formulary_fhir.repository_checkpoint import completed_checkpoint
from process.formulary_fhir.repository_shared import aggregate_hash
from process.formulary_fhir.repository_shared import AliasRef
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.repository_shared import json_object
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name


async def snapshot_alias_rows(
    database: Any,
    source_id: str,
    dataset_id: str,
) -> list[Any]:
    """Load exact alias-version headers for one source-owned dataset."""

    return await database.all(
        f"SELECT alias.public_id, alias.source_plan_identifier, alias.alias_id, "
        "version.alias_version_id, version.expected_count, "
        "version.membership_hash, version.cutoff_at FROM "
        f"{table_name('fhir_formulary_dataset_alias')} AS dataset_alias JOIN "
        f"{table_name('fhir_formulary_drug_plan_alias')} AS alias "
        "ON alias.source_id = dataset_alias.source_id "
        "AND alias.alias_id = dataset_alias.alias_id JOIN "
        f"{table_name('fhir_formulary_drug_plan_alias_version')} AS version "
        "ON version.source_id = dataset_alias.source_id "
        "AND version.alias_id = dataset_alias.alias_id "
        "AND version.alias_version_id = dataset_alias.alias_version_id "
        "WHERE dataset_alias.source_id = :source_id "
        "AND dataset_alias.dataset_id = :dataset_id "
        "ORDER BY alias.public_id, alias.source_plan_identifier;",
        source_id=source_id,
        dataset_id=dataset_id,
    )


async def _coverage_rows(
    database: Any,
    source_id: str,
    dataset_id: str,
) -> list[Any]:
    return await database.all(
        f"SELECT plan.public_id, plan.canonical_identity, "
        "version.coverage_version_id, version.content_hash, "
        f"version.metadata_json FROM {table_name('fhir_formulary_dataset_coverage_plan')} "
        f"AS dataset_plan JOIN {table_name('fhir_formulary_coverage_plan')} AS plan "
        "ON plan.source_id = dataset_plan.source_id "
        "AND plan.public_id = dataset_plan.public_id JOIN "
        f"{table_name('fhir_formulary_coverage_plan_version')} AS version "
        "ON version.public_id = dataset_plan.public_id "
        "AND version.coverage_version_id = dataset_plan.coverage_version_id "
        "WHERE dataset_plan.source_id = :source_id "
        "AND dataset_plan.dataset_id = :dataset_id ORDER BY plan.public_id;",
        source_id=source_id,
        dataset_id=dataset_id,
    )


def _coverage_proof(
    source_id: str,
    coverage_rows: list[Any],
) -> tuple[dict[str, set[str]], list[str]]:
    expected_aliases_by_public_id: dict[str, set[str]] = {}
    coverage_proof_rows: list[str] = []
    for coverage_row in coverage_rows:
        coverage_by_field = row_mapping(coverage_row)
        metadata = json_object(coverage_by_field.get("metadata_json"))
        raw_aliases = metadata.get("source_plan_identifiers")
        if type(raw_aliases) is not list or not raw_aliases:
            raise RuntimeError("FHIR formulary plan alias coverage is incomplete")
        aliases = {
            strict_text(alias, "stored source plan identifier", 512)
            for alias in raw_aliases
        }
        if len(aliases) != len(raw_aliases):
            raise RuntimeError("FHIR formulary plan alias coverage has duplicates")
        public_id = coverage_by_field["public_id"]
        if public_id in expected_aliases_by_public_id:
            raise RuntimeError("FHIR formulary dataset has duplicate plans")
        expected_aliases_by_public_id[public_id] = aliases
        coverage_proof_rows.append(
            "\x1f".join(
                (
                    source_id,
                    public_id,
                    coverage_by_field["canonical_identity"],
                    coverage_by_field["coverage_version_id"],
                    coverage_by_field["content_hash"],
                )
            )
        )
    return expected_aliases_by_public_id, coverage_proof_rows


async def _alias_proof(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    alias_rows: list[Any],
) -> tuple[dict[str, set[str]], list[str], int]:
    observed_aliases: dict[str, set[str]] = defaultdict(set)
    membership_proof_rows: list[str] = []
    medication_count = 0
    for alias_row in alias_rows:
        alias_by_field = row_mapping(alias_row)
        alias = AliasRef(
            source_id,
            alias_by_field["public_id"],
            alias_by_field["alias_id"],
            alias_by_field["source_plan_identifier"],
        )
        checkpoint = await completed_checkpoint(
            database,
            source_id,
            dataset,
            alias,
        )
        if checkpoint is None:
            raise RuntimeError("FHIR formulary alias checkpoint is incomplete")
        observed_aliases[alias.public_id].add(alias.source_plan_identifier)
        medication_count += checkpoint.expected_count
        membership_proof_rows.append(
            "\x1f".join(
                (
                    source_id,
                    alias.public_id,
                    alias.source_plan_identifier,
                    alias.alias_id,
                    checkpoint.alias_version_id,
                    str(checkpoint.expected_count),
                    checkpoint.membership_hash,
                )
            )
        )
    return dict(observed_aliases), membership_proof_rows, medication_count


def _verification_result(
    source_id: str,
    dataset_id: str,
    coverage_rows: list[str],
    membership_rows: list[str],
    medication_count: int,
) -> DatasetVerification:
    return DatasetVerification(
        source_id=source_id,
        dataset_id=dataset_id,
        list_count=len(coverage_rows),
        alias_count=len(membership_rows),
        medication_membership_count=medication_count,
        coverage_hash=aggregate_hash("fhir-formulary-coverage-v1", coverage_rows),
        membership_hash=aggregate_hash(
            "fhir-formulary-membership-v1",
            membership_rows,
        ),
    )


def _is_stored_verification_exact(
    dataset_by_field: dict[str, Any],
    verification: DatasetVerification,
) -> bool:
    return (
        dataset_by_field.get("list_count") == verification.list_count
        and dataset_by_field.get("alias_count") == verification.alias_count
        and dataset_by_field.get("medication_count")
        == verification.medication_membership_count
        and dataset_by_field.get("coverage_hash") == verification.coverage_hash
        and dataset_by_field.get("membership_hash") == verification.membership_hash
    )


async def _mark_verified(
    database: Any,
    dataset: DatasetRef,
    verification: DatasetVerification,
) -> None:
    summary_by_field = {
        "acquisition_contract_hash": dataset.acquisition_contract_hash,
        "list_count": verification.list_count,
        "alias_count": verification.alias_count,
        "medication_membership_count": verification.medication_membership_count,
    }
    updated_count = await database.status(
        f"UPDATE {table_name('fhir_formulary_dataset')} SET "
        "status = 'verified', list_count = :list_count, "
        "alias_count = :alias_count, medication_count = :medication_count, "
        "coverage_hash = :coverage_hash, membership_hash = :membership_hash, "
        "summary_json = CAST(:summary_json AS jsonb), "
        "verified_at = transaction_timestamp() WHERE source_id = :source_id "
        "AND dataset_id = :dataset_id AND status = 'building';",
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        list_count=verification.list_count,
        alias_count=verification.alias_count,
        medication_count=verification.medication_membership_count,
        coverage_hash=verification.coverage_hash,
        membership_hash=verification.membership_hash,
        summary_json=json_text(summary_by_field),
    )
    if updated_count != 1:
        raise RuntimeError("FHIR formulary verification transition failed")


async def _recompute_dataset_verification(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
) -> DatasetVerification:
    coverage_records = await _coverage_rows(
        database,
        source_id,
        dataset.dataset_id,
    )
    if not coverage_records:
        raise RuntimeError("FHIR formulary dataset has no coverage plans")
    expected_aliases, coverage_proof_rows = _coverage_proof(
        source_id,
        coverage_records,
    )
    alias_records = await snapshot_alias_rows(
        database,
        source_id,
        dataset.dataset_id,
    )
    observed_aliases, membership_proof_rows, medication_count = (
        await _alias_proof(
            database,
            source_id,
            dataset,
            alias_records,
        )
    )
    if observed_aliases != expected_aliases:
        raise RuntimeError("FHIR formulary plan alias coverage is incomplete")
    return _verification_result(
        source_id,
        dataset.dataset_id,
        coverage_proof_rows,
        membership_proof_rows,
        medication_count,
    )


class FHIRFormularyVerificationMixin:
    """Recompute exact content evidence before freezing a candidate."""

    _database: Any
    source_id: str

    async def verify_dataset(
        self,
        *,
        dataset: DatasetRef,
    ) -> DatasetVerification:
        """Recompute exact graph evidence and freeze one candidate."""

        async with self._database.transaction():
            dataset_by_field = await lock_dataset(
                self._database,
                self.source_id,
                dataset,
                allowed_statuses={"building", "verified"},
            )
            verification = await _recompute_dataset_verification(
                self._database,
                self.source_id,
                dataset,
            )
            if dataset_by_field.get("status") == "verified":
                if not _is_stored_verification_exact(
                    dataset_by_field,
                    verification,
                ):
                    raise RuntimeError("FHIR formulary stored verification changed")
                return verification
            await _mark_verified(self._database, dataset, verification)
            return verification


__all__ = ("FHIRFormularyVerificationMixin", "snapshot_alias_rows")
