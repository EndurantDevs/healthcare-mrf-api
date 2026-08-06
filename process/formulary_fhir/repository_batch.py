# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded multi-row persistence for high-volume formulary aliases."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from db.models import db
from process.formulary_fhir.parser import resolve_alternative_references
from process.formulary_fhir.repository_shared import SOURCE_ID
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_shared import upstream_time
from process.formulary_fhir.types import MedicationRecord

WRITE_BATCH_SIZE = 1_000


def _batches(
    rows: Sequence[MedicationRecord],
) -> Iterator[Sequence[MedicationRecord]]:
    for offset in range(0, len(rows), WRITE_BATCH_SIZE):
        yield rows[offset : offset + WRITE_BATCH_SIZE]


def medication_version_id(medication: MedicationRecord) -> str:
    """Return the immutable version identity used by all alias memberships."""

    return stable_id(
        "ffm_",
        SOURCE_ID,
        medication.upstream_medication_id,
        medication.content_hash,
    )


def _medication_values(
    batch: Sequence[MedicationRecord],
) -> tuple[str, dict[str, Any]]:
    value_rows: list[str] = []
    params_by_name: dict[str, Any] = {}
    for index, medication in enumerate(batch):
        suffix = str(index)
        value_rows.append(
            "("
            + ", ".join(
                (
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
            )
            + ")"
        )
        params_by_name.update(
            {
                f"medication_version_id_{suffix}": medication_version_id(medication),
                f"source_id_{suffix}": SOURCE_ID,
                f"upstream_medication_id_{suffix}": (medication.upstream_medication_id),
                f"upstream_version_id_{suffix}": medication.upstream_version_id,
                f"upstream_last_updated_{suffix}": upstream_time(
                    medication.upstream_last_updated
                ),
                f"status_{suffix}": medication.status,
                f"drug_name_{suffix}": medication.drug_name,
                f"rxnorm_id_{suffix}": medication.rxnorm_id,
                f"ndc11_{suffix}": medication.ndc11,
                f"codings_json_{suffix}": json_text(
                    [coding.__dict__ for coding in medication.codings]
                ),
                f"content_hash_{suffix}": medication.content_hash,
                f"metadata_json_{suffix}": json_text(
                    {
                        "raw_extensions": medication.raw_extensions,
                        "source_plan_identifiers": (medication.source_plan_identifiers),
                    }
                ),
            }
        )
    return ", ".join(value_rows), params_by_name


async def _insert_medications(
    medications: Sequence[MedicationRecord],
) -> None:
    for batch in _batches(medications):
        values_sql, params_by_name = _medication_values(batch)
        await db.status(
            f"INSERT INTO {table_name('fhir_formulary_medication')} ("
            "medication_version_id, source_id, upstream_medication_id, "
            "upstream_version_id, upstream_last_updated, status, drug_name, "
            "rxnorm_id, ndc11, codings_json, content_hash, metadata_json) "
            f"VALUES {values_sql} ON CONFLICT (source_id, "
            "upstream_medication_id, content_hash) DO NOTHING;",
            **params_by_name,
        )


def _membership_values(
    alias_version_id: str,
    batch: Sequence[MedicationRecord],
    variants_by_id: Mapping[str, str],
) -> tuple[str, dict[str, Any]]:
    value_rows: list[str] = []
    params_by_name: dict[str, Any] = {"alias_version_id": alias_version_id}
    for index, medication in enumerate(batch):
        suffix = str(index)
        medication_id = medication.upstream_medication_id
        value_rows.append(
            "("
            + ", ".join(
                (
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
            )
            + ")"
        )
        params_by_name.update(
            {
                f"upstream_medication_id_{suffix}": medication_id,
                f"medication_version_id_{suffix}": medication_version_id(medication),
                f"rxnorm_id_{suffix}": medication.rxnorm_id,
                f"drug_tier_{suffix}": medication.drug_tier,
                f"prior_authorization_{suffix}": medication.prior_authorization,
                f"step_therapy_{suffix}": medication.step_therapy,
                f"quantity_limit_{suffix}": medication.quantity_limit,
                f"variant_hash_{suffix}": variants_by_id[medication_id],
            }
        )
    return ", ".join(value_rows), params_by_name


async def _insert_memberships(
    alias_version_id: str,
    medications: Sequence[MedicationRecord],
    variants_by_id: Mapping[str, str],
) -> None:
    for batch in _batches(medications):
        values_sql, params_by_name = _membership_values(
            alias_version_id,
            batch,
            variants_by_id,
        )
        await db.status(
            f"INSERT INTO {table_name('fhir_formulary_alias_membership')} ("
            "alias_version_id, upstream_medication_id, medication_version_id, "
            "rxnorm_id, drug_tier, prior_authorization, step_therapy, "
            f"quantity_limit, variant_hash) VALUES {values_sql} "
            "ON CONFLICT (alias_version_id, upstream_medication_id) DO NOTHING;",
            **params_by_name,
        )


def _alternative_rows(
    medications: Iterable[MedicationRecord],
    known_medication_ids: set[str],
    *,
    apply_california_rule: bool,
) -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    for medication in medications:
        evidence_rows = resolve_alternative_references(
            medication.alternative_references,
            known_medication_ids=known_medication_ids,
            apply_california_rule=apply_california_rule,
        )
        rows.extend(
            (medication.upstream_medication_id, evidence) for evidence in evidence_rows
        )
    return rows


async def _insert_alternatives(
    alias_version_id: str,
    alternative_rows: Sequence[tuple[str, Any]],
) -> None:
    for offset in range(0, len(alternative_rows), WRITE_BATCH_SIZE):
        batch = alternative_rows[offset : offset + WRITE_BATCH_SIZE]
        params_by_name: dict[str, Any] = {
            "alias_version_id": alias_version_id,
            "evidence_json": json_text({"same_source": True}),
        }
        value_rows: list[str] = []
        for index, (medication_id, evidence) in enumerate(batch):
            suffix = str(index)
            value_rows.append(
                "("
                + ", ".join(
                    (
                        ":alias_version_id",
                        f":upstream_medication_id_{suffix}",
                        f":raw_reference_{suffix}",
                        f":corrected_reference_{suffix}",
                        f":resolved_medication_id_{suffix}",
                        f":resolved_{suffix}",
                        f":rule_version_{suffix}",
                        "CAST(:evidence_json AS jsonb)",
                    )
                )
                + ")"
            )
            params_by_name.update(
                {
                    f"upstream_medication_id_{suffix}": medication_id,
                    f"raw_reference_{suffix}": evidence.raw_reference,
                    f"corrected_reference_{suffix}": (evidence.corrected_reference),
                    f"resolved_medication_id_{suffix}": (
                        evidence.resolved_medication_id
                    ),
                    f"resolved_{suffix}": evidence.resolved,
                    f"rule_version_{suffix}": evidence.rule_version,
                }
            )
        await db.status(
            f"INSERT INTO {table_name('fhir_formulary_alternative')} ("
            "alias_version_id, upstream_medication_id, raw_reference, "
            "corrected_reference, resolved_medication_id, resolved, "
            f"rule_version, evidence_json) VALUES {', '.join(value_rows)} "
            "ON CONFLICT DO NOTHING;",
            **params_by_name,
        )


async def insert_changed_alias_rows(
    alias_version_id: str,
    medications_by_id: Mapping[str, MedicationRecord],
    variants_by_id: Mapping[str, str],
    *,
    apply_california_rule: bool,
) -> None:
    """Persist one alias in bounded statements inside the caller transaction."""

    medications = tuple(
        medications_by_id[medication_id] for medication_id in sorted(medications_by_id)
    )
    await _insert_medications(medications)
    await _insert_memberships(
        alias_version_id,
        medications,
        variants_by_id,
    )
    await _insert_alternatives(
        alias_version_id,
        _alternative_rows(
            medications,
            set(variants_by_id),
            apply_california_rule=apply_california_rule,
        ),
    )
