# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Materialize strict UHC drug spool rows into formulary repository records."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Any
from typing import Callable

from process.formulary_fhir.identity import canonical_list_identity
from process.formulary_fhir.identity import fhir_content_hash
from process.formulary_fhir.identity import public_formulary_id
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.types import CoveragePlanRecord
from process.formulary_fhir.types import FHIRCoding
from process.formulary_fhir.types import MedicationRecord
from process.formulary_fhir.types import RXNORM_SYSTEM_URI
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugPlanKey
from process.formulary_fhir.uhc_drug_parser_contract import (
    UHCDrugPlanMaterialization,
)
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_drug_normalization import SPOOL_CONTRACT
from process.formulary_fhir.uhc_drug_spool_reader import decode_spool_json
from process.formulary_fhir.uhc_drug_spool_reader import open_uhc_drug_spool
from process.formulary_fhir.uhc_drug_spool_reader import (
    open_verified_uhc_drug_spool,
)
from process.formulary_fhir.uhc_drug_spool_reader import SEMANTIC_FIELDS
from process.formulary_fhir.uhc_drug_spool_reader import spool_plan_key
from process.formulary_fhir.uhc_drug_spool_reader import spool_policy_value
from process.formulary_fhir.uhc_drug_spool_reader import spool_timestamp
from process.formulary_fhir.uhc_drug_spool_reader import spooled_uhc_plan_keys
from process.formulary_fhir.uhc_drug_spool_reader import (
    validated_spool_provenance,
)
from process.formulary_fhir.uhc_drug_spool_reader import (
    verify_spooled_uhc_evidence,
)
from process.formulary_fhir.uhc_drug_spool_reader import _VerifiedUHCDrugSpool


PLAN_PROJECTION_CONTRACT = "uhc-official-drug-plan-projection-v1"
MEDICATION_PROJECTION_CONTRACT = "uhc-official-drug-medication-projection-v1"
MAX_MEDICATIONS_PER_PLAN = 250_000
MAX_PLAN_MATERIALIZED_BYTES = 536_870_912


@dataclass(frozen=True, slots=True, repr=False)
class _MedicationSource:
    updated_at: dt.datetime
    semantic_by_field: dict[str, Any]
    provenance_rows: tuple[dict[str, Any], ...]
    timestamp_bases: tuple[str, ...]
    prior_authorization: bool | None
    step_therapy: bool | None
    quantity_limit: bool | None


def _plan_provenance(
    database_rows: list[sqlite3.Row],
    cancel_check: Callable[[], None] | None,
) -> dict[str, Any]:
    artifact_hashes: set[str] = set()
    source_file_ids: set[str] = set()
    file_names: set[str] = set()
    timestamp_bases: set[str] = set()
    for row_index, database_row in enumerate(database_rows, start=1):
        if cancel_check is not None and row_index % 1_024 == 0:
            cancel_check()
        for provenance in validated_spool_provenance(
            database_row["provenance_json"],
            semantic_json=database_row["semantic_json"],
            family=database_row["family"],
        ):
            artifact_hash = provenance.get("artifact_sha256")
            source_file_id = provenance.get("source_file_id")
            file_name = provenance.get("file_name")
            timestamp_basis = provenance.get("timestamp_basis")
            provenance_values = (
                artifact_hash,
                source_file_id,
                file_name,
                timestamp_basis,
            )
            if not all(
                type(provenance_value) is str and provenance_value
                for provenance_value in provenance_values
            ):
                raise RuntimeError("UHC drug spool provenance is invalid")
            artifact_hashes.add(artifact_hash)
            source_file_ids.add(source_file_id)
            file_names.add(file_name)
            timestamp_bases.add(timestamp_basis)
    return {
        "artifact_sha256s": sorted(artifact_hashes),
        "file_names": sorted(file_names),
        "source_file_ids": sorted(source_file_ids),
        "timestamp_bases": sorted(timestamp_bases),
    }


def _plan_list_id(source_id: str, plan_key: UHCDrugPlanKey) -> str:
    return stable_id(
        "uhc-",
        source_id,
        plan_key.family,
        plan_key.plan_id_type,
        plan_key.plan_id,
        str(plan_key.plan_year) if plan_key.plan_year is not None else "none",
    )


def _plan_period(
    plan_key: UHCDrugPlanKey,
) -> tuple[dt.datetime | None, dt.datetime | None]:
    if plan_key.plan_year is None:
        return None, None
    return (
        dt.datetime(plan_key.plan_year, 1, 1, tzinfo=dt.UTC),
        dt.datetime(plan_key.plan_year + 1, 1, 1, tzinfo=dt.UTC),
    )


def _coverage_plan(
    key: UHCDrugPlanKey,
    database_rows: list[sqlite3.Row],
    *,
    source_id: str,
    canonical_base: str,
    cancel_check: Callable[[], None] | None,
) -> CoveragePlanRecord:
    plan_updated_at = max(
        spool_timestamp(database_row["effective_updated_at"])
        for database_row in database_rows
    )
    list_id = _plan_list_id(source_id, key)
    raw_identifiers = (
        {
            "family": key.family,
            "plan_id": key.plan_id,
            "plan_id_type": key.plan_id_type,
            "plan_year": key.plan_year,
        },
    )
    raw_extensions = (
        {
            "contract": PLAN_PROJECTION_CONTRACT,
            "provenance": _plan_provenance(database_rows, cancel_check),
        },
    )
    period_start, period_end = _plan_period(key)
    normalized_by_field = {
        "canonical_identity": canonical_list_identity(canonical_base, list_id),
        "contract": PLAN_PROJECTION_CONTRACT,
        "list_id": list_id,
        "period_end": period_end.isoformat() if period_end else None,
        "period_start": period_start.isoformat() if period_start else None,
        "raw_extensions": list(raw_extensions),
        "raw_identifiers": list(raw_identifiers),
        "source_plan_identifiers": [key.source_plan_identifier],
        "upstream_last_updated": plan_updated_at.isoformat(),
    }
    return CoveragePlanRecord(
        upstream_list_id=list_id,
        public_id=public_formulary_id(canonical_base, list_id),
        canonical_identity=normalized_by_field["canonical_identity"],
        upstream_version_id=None,
        upstream_last_updated=plan_updated_at,
        status=None,
        title=None,
        name=None,
        upstream_date=None,
        period_start=period_start,
        period_end=period_end,
        source_plan_identifiers=(key.source_plan_identifier,),
        raw_identifiers=raw_identifiers,
        raw_extensions=raw_extensions,
        content_hash=fhir_content_hash(normalized_by_field),
    )


def _validated_medication_source(
    key: UHCDrugPlanKey,
    database_row: sqlite3.Row,
) -> _MedicationSource:
    semantic_by_field = decode_spool_json(database_row["semantic_json"], dict)
    if set(semantic_by_field) != SEMANTIC_FIELDS or semantic_by_field.get(
        "contract"
    ) != SPOOL_CONTRACT:
        raise RuntimeError("UHC drug spool semantics are invalid")
    provenance_rows = validated_spool_provenance(
        database_row["provenance_json"],
        semantic_json=database_row["semantic_json"],
        family=key.family,
    )
    timestamp_bases = {
        provenance_row.get("timestamp_basis")
        for provenance_row in provenance_rows
        if type(provenance_row) is dict
    }
    if not timestamp_bases or not timestamp_bases.issubset(
        {"artifact.catalog_modified_at", "record.last_updated_on"}
    ):
        raise RuntimeError("UHC drug spool provenance is invalid")
    prior_authorization = spool_policy_value(database_row["prior_authorization"])
    step_therapy = spool_policy_value(database_row["step_therapy"])
    quantity_limit = spool_policy_value(database_row["quantity_limit"])
    expected_by_field = {
        "drug_name": database_row["drug_name"],
        "drug_tier": database_row["drug_tier"],
        "prior_authorization": prior_authorization,
        "quantity_limit": quantity_limit,
        "rxnorm_id": database_row["rxnorm_id"],
        "step_therapy": step_therapy,
    }
    if any(
        semantic_by_field.get(field_name) != expected_value
        for field_name, expected_value in expected_by_field.items()
    ):
        raise RuntimeError("UHC drug spool semantics are inconsistent")
    return _MedicationSource(
        updated_at=spool_timestamp(database_row["effective_updated_at"]),
        semantic_by_field=semantic_by_field,
        provenance_rows=tuple(provenance_rows),
        timestamp_bases=tuple(sorted(timestamp_bases)),
        prior_authorization=prior_authorization,
        step_therapy=step_therapy,
        quantity_limit=quantity_limit,
    )


def _medication(
    key: UHCDrugPlanKey,
    database_row: sqlite3.Row,
) -> MedicationRecord:
    medication_source = _validated_medication_source(key, database_row)
    raw_extensions = (
        {
            "contract": MEDICATION_PROJECTION_CONTRACT,
            "provenance": list(medication_source.provenance_rows),
            "source_semantics": medication_source.semantic_by_field,
            "timestamp_bases": list(medication_source.timestamp_bases),
        },
    )
    coding = FHIRCoding(
        system=RXNORM_SYSTEM_URI,
        code=database_row["rxnorm_id"],
        display=database_row["drug_name"],
        version=None,
    )
    normalized_by_field = {
        "alias": key.source_plan_identifier,
        "coding": {
            "code": coding.code,
            "display": coding.display,
            "system": coding.system,
            "version": coding.version,
        },
        "contract": MEDICATION_PROJECTION_CONTRACT,
        "drug_name": database_row["drug_name"],
        "drug_tier": database_row["drug_tier"],
        "prior_authorization": medication_source.prior_authorization,
        "quantity_limit": medication_source.quantity_limit,
        "raw_extensions": list(raw_extensions),
        "rxnorm_id": database_row["rxnorm_id"],
        "step_therapy": medication_source.step_therapy,
        "upstream_last_updated": medication_source.updated_at.isoformat(),
    }
    return MedicationRecord(
        upstream_medication_id=database_row["rxnorm_id"],
        upstream_version_id=None,
        upstream_last_updated=medication_source.updated_at,
        status=None,
        drug_name=database_row["drug_name"],
        rxnorm_id=database_row["rxnorm_id"],
        ndc11=None,
        codings=(coding,),
        raw_extensions=raw_extensions,
        source_plan_identifiers=(key.source_plan_identifier,),
        drug_tier=database_row["drug_tier"],
        prior_authorization=normalized_by_field["prior_authorization"],
        step_therapy=normalized_by_field["step_therapy"],
        quantity_limit=normalized_by_field["quantity_limit"],
        alternative_references=(),
        content_hash=fhir_content_hash(normalized_by_field),
    )


def _materialized_medications(
    key: UHCDrugPlanKey,
    database_rows: list[sqlite3.Row],
    cancel_check: Callable[[], None] | None,
) -> tuple[MedicationRecord, ...]:
    medications = []
    for row_index, database_row in enumerate(database_rows, start=1):
        if cancel_check is not None and row_index % 1_024 == 0:
            cancel_check()
        medications.append(_medication(key, database_row))
    return tuple(medications)


def _require_plan_materialization_request(
    spool_path: _VerifiedUHCDrugSpool,
    key: UHCDrugPlanKey,
    source_id: str,
    evidence: UHCDrugSpoolEvidence,
) -> None:
    if (
        type(spool_path) is not _VerifiedUHCDrugSpool
        or spool_path.source_id != source_id
        or spool_path.spool_content_sha256 != evidence.spool_content_sha256
        or spool_path.artifact_set_sha256 != evidence.artifact_set_sha256
        or type(key) is not UHCDrugPlanKey
        or type(evidence) is not UHCDrugSpoolEvidence
        or evidence.source_id != source_id
    ):
        raise ValueError("UHC drug plan key is invalid")


def _database_row_bytes(database_row: sqlite3.Row) -> int:
    byte_count = 512
    for field_value in database_row:
        if isinstance(field_value, bytes):
            byte_count += len(field_value)
        elif isinstance(field_value, str):
            byte_count += len(field_value.encode("utf-8"))
        else:
            byte_count += 16
    return byte_count


def _spooled_plan_database_rows(
    spool_path: _VerifiedUHCDrugSpool,
    key: UHCDrugPlanKey,
    evidence: UHCDrugSpoolEvidence,
    cancel_check: Callable[[], None] | None,
) -> list[sqlite3.Row]:
    database_rows: list[sqlite3.Row] = []
    materialized_byte_count = 0
    row_limit = min(
        MAX_MEDICATIONS_PER_PLAN,
        evidence.medication_membership_count,
    ) + 1
    with open_uhc_drug_spool(spool_path) as connection:
        database_cursor = connection.execute(
            "SELECT * FROM membership WHERE source_plan_identifier = ? "
            "ORDER BY rxnorm_id LIMIT ?",
            (key.source_plan_identifier, row_limit),
        )
        for row_index, database_row in enumerate(database_cursor, start=1):
            if cancel_check is not None and row_index % 1_024 == 0:
                cancel_check()
            materialized_byte_count += _database_row_bytes(database_row)
            if row_index > MAX_MEDICATIONS_PER_PLAN:
                raise RuntimeError("UHC drug spool plan is incomplete")
            if materialized_byte_count > MAX_PLAN_MATERIALIZED_BYTES:
                raise RuntimeError("UHC drug spool plan is too large")
            database_rows.append(database_row)
    if cancel_check is not None:
        cancel_check()
    if not database_rows or any(
        spool_plan_key(database_row) != key for database_row in database_rows
    ):
        raise RuntimeError("UHC drug spool plan is incomplete")
    return database_rows


def load_spooled_uhc_plan(
    spool_path: _VerifiedUHCDrugSpool,
    key: UHCDrugPlanKey,
    *,
    source_id: str,
    canonical_base: str,
    evidence: UHCDrugSpoolEvidence,
    cancel_check: Callable[[], None] | None = None,
) -> UHCDrugPlanMaterialization:
    """Materialize one exact plan and its sorted medication membership."""

    _require_plan_materialization_request(spool_path, key, source_id, evidence)
    if cancel_check is not None:
        cancel_check()
    database_rows = _spooled_plan_database_rows(
        spool_path,
        key,
        evidence,
        cancel_check,
    )
    return UHCDrugPlanMaterialization(
        key=key,
        coverage_plan=_coverage_plan(
            key,
            database_rows,
            source_id=source_id,
            canonical_base=canonical_base,
            cancel_check=cancel_check,
        ),
        medications=_materialized_medications(
            key,
            database_rows,
            cancel_check,
        ),
    )


__all__ = (
    "MEDICATION_PROJECTION_CONTRACT",
    "MAX_MEDICATIONS_PER_PLAN",
    "MAX_PLAN_MATERIALIZED_BYTES",
    "open_verified_uhc_drug_spool",
    "PLAN_PROJECTION_CONTRACT",
    "load_spooled_uhc_plan",
    "spooled_uhc_plan_keys",
    "verify_spooled_uhc_evidence",
)
