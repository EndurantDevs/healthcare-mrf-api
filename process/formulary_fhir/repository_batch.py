# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded immutable medication and alias-membership persistence."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from process.formulary_fhir.parser import resolve_alternative_references
from process.formulary_fhir.repository_shared import WRITE_BATCH_SIZE
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.types import MedicationRecord


def _batches(rows: Sequence[Any]) -> Iterator[Sequence[Any]]:
    for offset in range(0, len(rows), WRITE_BATCH_SIZE):
        yield rows[offset : offset + WRITE_BATCH_SIZE]


def medication_version_id(source_id: str, medication: MedicationRecord) -> str:
    """Return the immutable source-qualified medication version identity."""

    return stable_id(
        "ffm_",
        source_id,
        medication.upstream_medication_id,
        medication.content_hash,
    )


def _coding_json(medication: MedicationRecord) -> list[dict[str, Any]]:
    return [
        {
            "system": coding.system,
            "code": coding.code,
            "display": coding.display,
            "version": coding.version,
        }
        for coding in medication.codings
    ]


def _medication_values(
    source_id: str,
    batch: Sequence[MedicationRecord],
) -> tuple[str, dict[str, Any]]:
    value_rows: list[str] = []
    params_by_name: dict[str, Any] = {}
    for index, medication in enumerate(batch):
        suffix = str(index)
        parameter_names = (
            f":medication_version_id_{suffix}",
            f":source_id_{suffix}",
            f":upstream_medication_id_{suffix}",
            f":upstream_version_id_{suffix}",
            f":upstream_last_updated_{suffix}",
            f":status_{suffix}",
            f":drug_name_{suffix}",
            f":rxnorm_id_{suffix}",
            f":ndc11_{suffix}",
            f"CAST(:codings_json_{suffix} AS jsonb)",
            f":content_hash_{suffix}",
            f"CAST(:metadata_json_{suffix} AS jsonb)",
        )
        value_rows.append("(" + ", ".join(parameter_names) + ")")
        params_by_name.update(
            {
                f"medication_version_id_{suffix}": medication_version_id(
                    source_id,
                    medication,
                ),
                f"source_id_{suffix}": source_id,
                f"upstream_medication_id_{suffix}": (
                    medication.upstream_medication_id
                ),
                f"upstream_version_id_{suffix}": medication.upstream_version_id,
                f"upstream_last_updated_{suffix}": (
                    medication.upstream_last_updated
                ),
                f"status_{suffix}": medication.status,
                f"drug_name_{suffix}": medication.drug_name,
                f"rxnorm_id_{suffix}": medication.rxnorm_id,
                f"ndc11_{suffix}": medication.ndc11,
                f"codings_json_{suffix}": json_text(_coding_json(medication)),
                f"content_hash_{suffix}": medication.content_hash,
                f"metadata_json_{suffix}": json_text(
                    {
                        "raw_extensions": medication.raw_extensions,
                        "source_plan_identifiers": (
                            medication.source_plan_identifiers
                        ),
                        "alternative_references": (
                            medication.alternative_references
                        ),
                    }
                ),
            }
        )
    return ", ".join(value_rows), params_by_name


async def _assert_medication_batch(
    database: Any,
    source_id: str,
    batch: Sequence[MedicationRecord],
) -> None:
    expected_by_id = {
        medication_version_id(source_id, medication): (
            medication.upstream_medication_id,
            medication.content_hash,
        )
        for medication in batch
    }
    stored_rows = await database.all(
        f"SELECT medication_version_id, upstream_medication_id, content_hash "
        f"FROM {table_name('fhir_formulary_medication')} "
        "WHERE source_id = :source_id "
        "AND medication_version_id = ANY(:medication_version_ids);",
        source_id=source_id,
        medication_version_ids=list(expected_by_id),
    )
    stored_by_id = {
        row_mapping(row)["medication_version_id"]: (
            row_mapping(row)["upstream_medication_id"],
            row_mapping(row)["content_hash"],
        )
        for row in stored_rows
    }
    if stored_by_id != expected_by_id:
        raise RuntimeError("FHIR formulary medication collision is inconsistent")


async def _insert_medications(
    database: Any,
    source_id: str,
    medications: Sequence[MedicationRecord],
) -> None:
    for batch in _batches(medications):
        values_sql, params_by_name = _medication_values(source_id, batch)
        await database.status(
            f"INSERT INTO {table_name('fhir_formulary_medication')} ("
            "medication_version_id, source_id, upstream_medication_id, "
            "upstream_version_id, upstream_last_updated, status, drug_name, "
            "rxnorm_id, ndc11, codings_json, content_hash, metadata_json) "
            f"VALUES {values_sql} ON CONFLICT (source_id, "
            "upstream_medication_id, content_hash) DO NOTHING;",
            **params_by_name,
        )
        await _assert_medication_batch(database, source_id, batch)


def _membership_values(
    source_id: str,
    alias_version_id: str,
    batch: Sequence[MedicationRecord],
    variants_by_id: Mapping[str, str],
) -> tuple[str, dict[str, Any]]:
    value_rows: list[str] = []
    params_by_name: dict[str, Any] = {
        "source_id": source_id,
        "alias_version_id": alias_version_id,
    }
    for index, medication in enumerate(batch):
        suffix = str(index)
        medication_id = medication.upstream_medication_id
        parameter_names = (
            ":source_id",
            ":alias_version_id",
            f":upstream_medication_id_{suffix}",
            f":medication_version_id_{suffix}",
            f":rxnorm_id_{suffix}",
            f":drug_tier_{suffix}",
            f":prior_authorization_{suffix}",
            f":step_therapy_{suffix}",
            f":quantity_limit_{suffix}",
            f":variant_hash_{suffix}",
        )
        value_rows.append("(" + ", ".join(parameter_names) + ")")
        params_by_name.update(
            {
                f"upstream_medication_id_{suffix}": medication_id,
                f"medication_version_id_{suffix}": medication_version_id(
                    source_id,
                    medication,
                ),
                f"rxnorm_id_{suffix}": medication.rxnorm_id,
                f"drug_tier_{suffix}": medication.drug_tier,
                f"prior_authorization_{suffix}": (
                    medication.prior_authorization
                ),
                f"step_therapy_{suffix}": medication.step_therapy,
                f"quantity_limit_{suffix}": medication.quantity_limit,
                f"variant_hash_{suffix}": variants_by_id[medication_id],
            }
        )
    return ", ".join(value_rows), params_by_name


async def _insert_memberships(
    database: Any,
    source_id: str,
    alias_version_id: str,
    medications: Sequence[MedicationRecord],
    variants_by_id: Mapping[str, str],
) -> None:
    for batch in _batches(medications):
        values_sql, params_by_name = _membership_values(
            source_id,
            alias_version_id,
            batch,
            variants_by_id,
        )
        await database.status(
            f"INSERT INTO {table_name('fhir_formulary_alias_membership')} ("
            "source_id, alias_version_id, upstream_medication_id, "
            "medication_version_id, rxnorm_id, drug_tier, "
            "prior_authorization, step_therapy, quantity_limit, variant_hash) "
            f"VALUES {values_sql} ON CONFLICT "
            "(alias_version_id, upstream_medication_id) DO NOTHING;",
            **params_by_name,
        )


def _alternative_rows(
    medications: Sequence[MedicationRecord],
    known_medication_ids: set[str],
) -> list[tuple[str, Any]]:
    alternative_rows: list[tuple[str, Any]] = []
    for medication in medications:
        evidence_rows = resolve_alternative_references(
            medication.alternative_references,
            known_medication_ids=known_medication_ids,
        )
        alternative_rows.extend(
            (medication.upstream_medication_id, evidence)
            for evidence in evidence_rows
        )
    return alternative_rows


def _alternative_values(
    alias_version_id: str,
    batch: Sequence[tuple[str, Any]],
) -> tuple[str, dict[str, Any]]:
    params_by_name: dict[str, Any] = {
        "alias_version_id": alias_version_id,
        "evidence_json": json_text({"same_alias": True}),
    }
    value_rows: list[str] = []
    for index, (medication_id, evidence) in enumerate(batch):
        suffix = str(index)
        parameter_names = (
            ":alias_version_id",
            f":upstream_medication_id_{suffix}",
            f":raw_reference_{suffix}",
            f":corrected_reference_{suffix}",
            f":resolved_medication_id_{suffix}",
            f":resolved_{suffix}",
            f":rule_version_{suffix}",
            "CAST(:evidence_json AS jsonb)",
        )
        value_rows.append("(" + ", ".join(parameter_names) + ")")
        params_by_name.update(
            {
                f"upstream_medication_id_{suffix}": medication_id,
                f"raw_reference_{suffix}": evidence.raw_reference,
                f"corrected_reference_{suffix}": evidence.corrected_reference,
                f"resolved_medication_id_{suffix}": (
                    evidence.resolved_medication_id
                ),
                f"resolved_{suffix}": evidence.is_resolved,
                f"rule_version_{suffix}": evidence.rule_version,
            }
        )
    return ", ".join(value_rows), params_by_name


async def _insert_alternatives(
    database: Any,
    alias_version_id: str,
    alternative_rows: Sequence[tuple[str, Any]],
) -> None:
    for batch in _batches(alternative_rows):
        values_sql, params_by_name = _alternative_values(
            alias_version_id,
            batch,
        )
        await database.status(
            f"INSERT INTO {table_name('fhir_formulary_alternative')} ("
            "alias_version_id, upstream_medication_id, raw_reference, "
            "corrected_reference, resolved_medication_id, resolved, "
            f"rule_version, evidence_json) VALUES {values_sql} "
            "ON CONFLICT DO NOTHING;",
            **params_by_name,
        )


def _expected_alternatives(
    alternative_rows: Sequence[tuple[str, Any]],
) -> list[tuple[Any, ...]]:
    return sorted(
        (
            medication_id,
            evidence.raw_reference,
            evidence.corrected_reference,
            evidence.resolved_medication_id,
            evidence.is_resolved,
            evidence.rule_version,
        )
        for medication_id, evidence in alternative_rows
    )


async def _assert_alternatives(
    database: Any,
    source_id: str,
    alias_version_id: str,
    expected_rows: Sequence[tuple[str, Any]],
) -> None:
    stored_rows = await database.all(
        f"SELECT alternative.upstream_medication_id, "
        "alternative.raw_reference, alternative.corrected_reference, "
        "alternative.resolved_medication_id, alternative.resolved, "
        f"alternative.rule_version FROM {table_name('fhir_formulary_alternative')} "
        "AS alternative JOIN "
        f"{table_name('fhir_formulary_alias_membership')} AS membership "
        "ON membership.alias_version_id = alternative.alias_version_id "
        "AND membership.upstream_medication_id = "
        "alternative.upstream_medication_id "
        "WHERE membership.source_id = :source_id "
        "AND alternative.alias_version_id = :alias_version_id "
        "ORDER BY alternative.upstream_medication_id, "
        "alternative.raw_reference;",
        source_id=source_id,
        alias_version_id=alias_version_id,
    )
    stored_values = [
        (
            row_mapping(stored_alternative_row)["upstream_medication_id"],
            row_mapping(stored_alternative_row)["raw_reference"],
            row_mapping(stored_alternative_row)["corrected_reference"],
            row_mapping(stored_alternative_row)["resolved_medication_id"],
            row_mapping(stored_alternative_row)["resolved"],
            row_mapping(stored_alternative_row)["rule_version"],
        )
        for stored_alternative_row in stored_rows
    ]
    if stored_values != _expected_alternatives(expected_rows):
        raise RuntimeError("FHIR formulary alternative evidence is inconsistent")


async def insert_alias_content(
    database: Any,
    source_id: str,
    alias_version_id: str,
    medications_by_id: Mapping[str, MedicationRecord],
    variants_by_id: Mapping[str, str],
) -> None:
    """Persist one exact alias in bounded statements inside one transaction."""

    medications = tuple(
        medications_by_id[medication_id]
        for medication_id in sorted(medications_by_id)
    )
    await _insert_medications(database, source_id, medications)
    await _insert_memberships(
        database,
        source_id,
        alias_version_id,
        medications,
        variants_by_id,
    )
    alternative_rows = _alternative_rows(medications, set(variants_by_id))
    await _insert_alternatives(database, alias_version_id, alternative_rows)
    await _assert_alternatives(
        database,
        source_id,
        alias_version_id,
        alternative_rows,
    )


__all__ = ("WRITE_BATCH_SIZE", "insert_alias_content", "medication_version_id")
