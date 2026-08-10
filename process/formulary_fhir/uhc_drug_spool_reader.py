# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read and reverify one private UHC drug normalization spool."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Any, Callable, Iterator

from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.uhc_drug_normalization import SPOOL_CONTRACT
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugPlanKey
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_drug_parser_contract import uhc_drug_plan_alias
from process.formulary_fhir.uhc_drug_spool_contract import artifact_proof_rows
from process.formulary_fhir.uhc_drug_spool_contract import (
    SPOOL_ARTIFACT_PROOF_FIELDS,
)
from process.formulary_fhir.uhc_drug_spool_contract import SPOOL_EVIDENCE_FIELDS
from process.formulary_fhir.uhc_drug_spool_contract import spool_evidence_payload
from process.formulary_fhir.uhc_drug_spool_storage import open_uhc_drug_spool
from process.formulary_fhir.uhc_drug_spool_storage import pin_uhc_drug_spool
from process.formulary_fhir.uhc_drug_spool_storage import PinnedUHCDrugSpool
from process.formulary_fhir.uhc_drug_spool_storage import (
    verified_spool_capability,
)
from process.formulary_fhir.uhc_drug_spool_storage import _VerifiedUHCDrugSpool


PROVENANCE_FIELDS = frozenset(
    {
        "artifact_sha256",
        "catalog_modified_at",
        "family",
        "file_name",
        "plan_ordinal",
        "record_ordinal",
        "selected",
        "semantic_sha256",
        "source_file_id",
        "timestamp_basis",
    }
)
SEMANTIC_FIELDS = frozenset(
    {
        "contract",
        "drug_name",
        "drug_tier",
        "plan_extension",
        "prior_authorization",
        "quantity_limit",
        "record_extension",
        "rxnorm_id",
        "step_therapy",
    }
)


def decode_spool_json(raw_value: object, expected_type: type) -> Any:
    """Decode one canonical JSON cell as the required exact container type."""

    if type(raw_value) is not str:
        raise RuntimeError("UHC drug spool JSON is invalid")
    try:
        decoded_value = json.loads(raw_value)
    except json.JSONDecodeError:
        raise RuntimeError("UHC drug spool JSON is invalid") from None
    if type(decoded_value) is not expected_type or json_text(decoded_value) != raw_value:
        raise RuntimeError("UHC drug spool JSON is not canonical")
    return decoded_value


def spool_policy_value(raw_value: object) -> bool | None:
    """Recover only exact SQLite NULL, zero, or one policy values."""

    if raw_value is None:
        return None
    if type(raw_value) is not int or raw_value not in {0, 1}:
        raise RuntimeError("UHC drug spool policy flag is invalid")
    return bool(raw_value)


def spool_timestamp(raw_value: object) -> dt.datetime:
    """Recover one canonical UTC timestamp written by the spool producer."""

    if type(raw_value) is not str:
        raise RuntimeError("UHC drug spool timestamp is invalid")
    try:
        parsed_timestamp = dt.datetime.fromisoformat(raw_value)
    except ValueError:
        raise RuntimeError("UHC drug spool timestamp is invalid") from None
    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
        raise RuntimeError("UHC drug spool timestamp is invalid")
    normalized_timestamp = parsed_timestamp.astimezone(dt.UTC)
    if normalized_timestamp.isoformat() != raw_value:
        raise RuntimeError("UHC drug spool timestamp is not canonical")
    return normalized_timestamp


def spool_plan_key(database_record: sqlite3.Row) -> UHCDrugPlanKey:
    """Rebuild and rehash one exact source plan identity from a spool row."""

    plan_key = UHCDrugPlanKey(
        family=database_record["family"],
        plan_id_type=database_record["plan_id_type"],
        plan_id=database_record["plan_id"],
        plan_year=database_record["plan_year"],
        source_plan_identifier=database_record["source_plan_identifier"],
    )
    expected_alias = uhc_drug_plan_alias(
        plan_key.family,
        plan_key.plan_id_type,
        plan_key.plan_id,
        plan_key.plan_year,
    )
    if plan_key.source_plan_identifier != expected_alias:
        raise RuntimeError("UHC drug spool plan identity is inconsistent")
    return plan_key


def validated_spool_provenance(
    raw_value: object,
    *,
    semantic_json: str | None = None,
    family: str | None = None,
) -> tuple[dict[str, Any], ...]:
    """Validate every source-file witness bound to one selected membership."""

    provenance_records = decode_spool_json(raw_value, list)
    expected_semantic_hash = (
        hashlib.sha256(semantic_json.encode("utf-8")).hexdigest()
        if semantic_json is not None
        else None
    )
    selected_count = 0
    for provenance_record in provenance_records:
        if (
            type(provenance_record) is not dict
            or set(provenance_record) != PROVENANCE_FIELDS
        ):
            raise RuntimeError("UHC drug spool provenance is invalid")
        try:
            strict_hash(provenance_record["artifact_sha256"], "artifact hash")
            strict_hash(provenance_record["semantic_sha256"], "semantic hash")
            strict_hash(provenance_record["source_file_id"], "source file id")
        except ValueError:
            raise RuntimeError("UHC drug spool provenance is invalid") from None
        is_invalid = bool(
            provenance_record["family"] not in {"cs", "ifp"}
            or (family is not None and provenance_record["family"] != family)
            or type(provenance_record["file_name"]) is not str
            or not provenance_record["file_name"]
            or type(provenance_record["catalog_modified_at"]) is not str
            or type(provenance_record["record_ordinal"]) is not int
            or provenance_record["record_ordinal"] <= 0
            or type(provenance_record["plan_ordinal"]) is not int
            or provenance_record["plan_ordinal"] <= 0
            or type(provenance_record["selected"]) is not bool
            or provenance_record["timestamp_basis"]
            not in {
                "artifact.catalog_modified_at",
                "record.last_updated_on",
            }
            or (
                expected_semantic_hash is not None
                and provenance_record["selected"]
                != (
                    provenance_record["semantic_sha256"]
                    == expected_semantic_hash
                )
            )
        )
        if is_invalid:
            raise RuntimeError("UHC drug spool provenance is invalid")
        selected_count += int(provenance_record["selected"])
    if not provenance_records or selected_count <= 0:
        raise RuntimeError("UHC drug spool provenance is invalid")
    return tuple(provenance_records)


def spooled_uhc_plan_keys(
    spool_path: Path | str | PinnedUHCDrugSpool,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> tuple[UHCDrugPlanKey, ...]:
    """Return all exact plan keys in deterministic repository-write order."""

    with open_uhc_drug_spool(spool_path) as connection:
        database_cursor = connection.execute(
            "SELECT source_plan_identifier, family, plan_id_type, plan_id, "
            "plan_year FROM membership GROUP BY source_plan_identifier, family, "
            "plan_id_type, plan_id, plan_year ORDER BY source_plan_identifier"
        )
        plan_keys_list = []
        for record_index, database_record in enumerate(database_cursor, start=1):
            if cancel_check is not None and record_index % 1_024 == 0:
                cancel_check()
            plan_keys_list.append(spool_plan_key(database_record))
    plan_keys = tuple(plan_keys_list)
    unique_aliases = {plan_key.source_plan_identifier for plan_key in plan_keys}
    if not plan_keys or len(unique_aliases) != len(plan_keys):
        raise RuntimeError("UHC drug spool plan census is invalid")
    return plan_keys


def _verified_spool_metadata_records(
    connection: sqlite3.Connection,
    evidence: UHCDrugSpoolEvidence,
    artifact_set: VerifiedSourceArtifactSet,
) -> tuple[tuple[str, str], dict[str, dict[str, Any]]]:
    metadata_rows = connection.execute(
        "SELECT evidence_json, artifact_proof_json FROM spool_metadata "
        "WHERE singleton = 1"
    ).fetchall()
    if len(metadata_rows) != 1:
        raise RuntimeError("UHC drug spool metadata is invalid")
    evidence_by_field = decode_spool_json(metadata_rows[0][0], dict)
    artifact_proof_by_index = decode_spool_json(metadata_rows[0][1], list)
    expected_artifact_proof_rows = list(artifact_proof_rows(artifact_set))
    if (
        set(evidence_by_field) != SPOOL_EVIDENCE_FIELDS
        or evidence_by_field != spool_evidence_payload(evidence)
        or artifact_proof_by_index != expected_artifact_proof_rows
        or any(
            type(proof_by_field) is not dict
            or set(proof_by_field) != SPOOL_ARTIFACT_PROOF_FIELDS
            for proof_by_field in artifact_proof_by_index
        )
    ):
        raise RuntimeError("UHC drug spool metadata is inconsistent")
    proof_by_source_file_id = {
        proof_by_field["source_file_id"]: proof_by_field
        for proof_by_field in artifact_proof_by_index
    }
    if len(proof_by_source_file_id) != len(artifact_proof_by_index):
        raise RuntimeError("UHC drug spool artifact proof is invalid")
    return tuple(metadata_rows[0]), proof_by_source_file_id


def _require_provenance_artifact(
    provenance_by_field: dict[str, Any],
    artifact_by_source_file_id: dict[str, dict[str, Any]],
) -> None:
    source_file_id = provenance_by_field["source_file_id"]
    artifact_by_field = artifact_by_source_file_id.get(source_file_id)
    expected_values = (
        artifact_by_field.get("artifact_sha256") if artifact_by_field else None,
        artifact_by_field.get("catalog_modified_at") if artifact_by_field else None,
        artifact_by_field.get("family") if artifact_by_field else None,
        artifact_by_field.get("file_name") if artifact_by_field else None,
        source_file_id if artifact_by_field else None,
    )
    observed_values = (
        provenance_by_field["artifact_sha256"],
        provenance_by_field["catalog_modified_at"],
        provenance_by_field["family"],
        provenance_by_field["file_name"],
        provenance_by_field["source_file_id"],
    )
    if observed_values != expected_values:
        raise RuntimeError("UHC drug spool provenance artifact is invalid")


def _verified_membership_proof(
    connection: sqlite3.Connection,
    metadata_row: tuple[str, str],
    artifact_by_source_file_id: dict[str, dict[str, Any]],
    cancel_check: Callable[[], None] | None,
) -> tuple[str, int, int, dt.datetime | None]:
    digest = hashlib.sha256()
    digest.update(SPOOL_CONTRACT.encode("ascii"))
    digest.update(b"\n")
    digest.update(json_text(list(metadata_row)).encode("utf-8"))
    digest.update(b"\n")
    membership_count = 0
    plan_identifiers: set[str] = set()
    witnessed_source_file_ids: set[str] = set()
    maximum_updated_at: dt.datetime | None = None
    database_records = connection.execute(
        "SELECT source_plan_identifier, family, plan_id_type, plan_id, "
        "plan_year, rxnorm_id, drug_name, drug_tier, prior_authorization, "
        "step_therapy, quantity_limit, effective_updated_at, semantic_json, "
        "provenance_json FROM membership ORDER BY source_plan_identifier, "
        "rxnorm_id"
    )
    for record_index, database_record in enumerate(database_records, start=1):
        if cancel_check is not None and record_index % 1_024 == 0:
            cancel_check()
        digest.update(json_text(list(database_record)).encode("utf-8"))
        digest.update(b"\n")
        membership_count += 1
        plan_identifiers.add(database_record[0])
        record_updated_at = spool_timestamp(database_record[11])
        maximum_updated_at = max(
            record_updated_at,
            maximum_updated_at or record_updated_at,
        )
        provenance_records = validated_spool_provenance(
            database_record[13],
            semantic_json=database_record[12],
            family=database_record[1],
        )
        for provenance_by_field in provenance_records:
            _require_provenance_artifact(
                provenance_by_field,
                artifact_by_source_file_id,
            )
            witnessed_source_file_ids.add(provenance_by_field["source_file_id"])
    if witnessed_source_file_ids != set(artifact_by_source_file_id):
        raise RuntimeError("UHC drug spool artifact census is incomplete")
    return (
        digest.hexdigest(),
        len(plan_identifiers),
        membership_count,
        maximum_updated_at,
    )


def verify_spooled_uhc_evidence(
    spool_path: Path | str | PinnedUHCDrugSpool,
    evidence: UHCDrugSpoolEvidence,
    artifact_set: VerifiedSourceArtifactSet,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> None:
    """Recompute the immutable final spool graph before repository writes."""

    if (
        type(evidence) is not UHCDrugSpoolEvidence
        or type(artifact_set) is not VerifiedSourceArtifactSet
        or evidence.source_id != artifact_set.source_id
        or evidence.source_file_set_sha256
        != artifact_set.source_file_set_sha256
        or evidence.artifact_set_sha256 != artifact_set.artifact_set_sha256
    ):
        raise ValueError("UHC drug spool evidence is invalid")
    with open_uhc_drug_spool(spool_path) as connection:
        metadata_row, artifact_by_source_file_id = _verified_spool_metadata_records(
            connection,
            evidence,
            artifact_set,
        )
        observed_proof = _verified_membership_proof(
            connection,
            metadata_row,
            artifact_by_source_file_id,
            cancel_check,
        )
    expected_proof = (
        evidence.spool_content_sha256,
        evidence.plan_count,
        evidence.medication_membership_count,
        evidence.max_last_updated_at,
    )
    if observed_proof != expected_proof:
        raise RuntimeError("UHC drug spool evidence changed")


def verify_and_bind_uhc_drug_spool(
    spool: PinnedUHCDrugSpool,
    evidence: UHCDrugSpoolEvidence,
    artifact_set: VerifiedSourceArtifactSet,
    *,
    cancel_check: Callable[[], None] | None = None,
) -> _VerifiedUHCDrugSpool:
    """Recompute one pinned inode and return its evidence-bound capability."""

    if type(spool) is not PinnedUHCDrugSpool:
        raise ValueError("UHC drug spool is unavailable")
    verify_spooled_uhc_evidence(
        spool,
        evidence,
        artifact_set,
        cancel_check=cancel_check,
    )
    return verified_spool_capability(spool, evidence)


@contextmanager
def open_verified_uhc_drug_spool(
    spool_path: Path | str,
    evidence: UHCDrugSpoolEvidence,
    artifact_set: VerifiedSourceArtifactSet,
) -> Iterator[_VerifiedUHCDrugSpool]:
    """Pin and fully verify one spool for a bounded synchronous operation."""

    with pin_uhc_drug_spool(spool_path) as pinned_spool:
        yield verify_and_bind_uhc_drug_spool(
            pinned_spool,
            evidence,
            artifact_set,
        )


__all__ = (
    "SEMANTIC_FIELDS",
    "PinnedUHCDrugSpool",
    "decode_spool_json",
    "open_uhc_drug_spool",
    "open_verified_uhc_drug_spool",
    "pin_uhc_drug_spool",
    "spool_plan_key",
    "spool_policy_value",
    "spool_timestamp",
    "spooled_uhc_plan_keys",
    "validated_spool_provenance",
    "verify_spooled_uhc_evidence",
    "verify_and_bind_uhc_drug_spool",
)
