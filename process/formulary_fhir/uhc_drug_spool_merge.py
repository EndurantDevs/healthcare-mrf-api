# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic duplicate and supersession writes for UHC drug spools."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3

from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.uhc_drug_normalization import (
    NormalizedUHCDrugMembership,
)
from process.formulary_fhir.uhc_drug_normalization import (
    UHCDrugNormalizationError,
)


MAX_PROVENANCE_RECORDS_PER_MEMBERSHIP = 256
MAX_PROVENANCE_JSON_BYTES = 262_144


def _merged_provenance(
    stored_json: str,
    incoming_json: str,
    *,
    selected_semantic_json: str,
) -> str:
    if (
        len(stored_json.encode("utf-8")) > MAX_PROVENANCE_JSON_BYTES
        or len(incoming_json.encode("utf-8")) > MAX_PROVENANCE_JSON_BYTES
    ):
        raise UHCDrugNormalizationError("UHC drug provenance is too large")
    try:
        stored_records = json.loads(stored_json)
        incoming_records = json.loads(incoming_json)
    except json.JSONDecodeError:
        raise RuntimeError("UHC drug spool provenance is invalid") from None
    if type(stored_records) is not list or type(incoming_records) is not list:
        raise RuntimeError("UHC drug spool provenance is invalid")
    selected_semantic_sha256 = hashlib.sha256(
        selected_semantic_json.encode("utf-8")
    ).hexdigest()
    provenance_by_text: dict[str, dict[str, object]] = {}
    for provenance_record in (*stored_records, *incoming_records):
        if type(provenance_record) is not dict:
            raise RuntimeError("UHC drug spool provenance is invalid")
        normalized_provenance_by_field = dict(provenance_record)
        normalized_provenance_by_field["selected"] = (
            normalized_provenance_by_field.get("semantic_sha256")
            == selected_semantic_sha256
        )
        provenance_by_text[json_text(normalized_provenance_by_field)] = (
            normalized_provenance_by_field
        )
        if len(provenance_by_text) > MAX_PROVENANCE_RECORDS_PER_MEMBERSHIP:
            raise UHCDrugNormalizationError("UHC drug provenance is too large")
    merged_json = json_text(
        [
            provenance_by_text[provenance_text]
            for provenance_text in sorted(provenance_by_text)
        ]
    )
    if len(merged_json.encode("utf-8")) > MAX_PROVENANCE_JSON_BYTES:
        raise UHCDrugNormalizationError("UHC drug provenance is too large")
    return merged_json


def _membership_values(
    membership: NormalizedUHCDrugMembership,
) -> tuple[object, ...]:
    return (
        membership.key.source_plan_identifier,
        membership.key.family,
        membership.key.plan_id_type,
        membership.key.plan_id,
        membership.key.plan_year,
        membership.rxnorm_id,
        membership.drug_name,
        membership.drug_tier,
        membership.prior_authorization,
        membership.step_therapy,
        membership.quantity_limit,
        membership.effective_updated_at.isoformat(),
        membership.semantic_json,
        membership.provenance_json,
    )


def _update_selected_membership(
    connection: sqlite3.Connection,
    membership: NormalizedUHCDrugMembership,
    merged_provenance: str,
) -> None:
    connection.execute(
        "UPDATE membership SET family = ?, plan_id_type = ?, plan_id = ?, "
        "plan_year = ?, drug_name = ?, drug_tier = ?, prior_authorization = ?, "
        "step_therapy = ?, quantity_limit = ?, effective_updated_at = ?, "
        "semantic_json = ?, provenance_json = ? WHERE "
        "source_plan_identifier = ? AND rxnorm_id = ?",
        (
            membership.key.family,
            membership.key.plan_id_type,
            membership.key.plan_id,
            membership.key.plan_year,
            membership.drug_name,
            membership.drug_tier,
            membership.prior_authorization,
            membership.step_therapy,
            membership.quantity_limit,
            membership.effective_updated_at.isoformat(),
            membership.semantic_json,
            merged_provenance,
            membership.key.source_plan_identifier,
            membership.rxnorm_id,
        ),
    )


def _update_observation_provenance(
    connection: sqlite3.Connection,
    membership: NormalizedUHCDrugMembership,
    merged_provenance: str,
    *,
    selected_timestamp: dt.datetime | None = None,
) -> None:
    if selected_timestamp is not None:
        connection.execute(
            "UPDATE membership SET effective_updated_at = ?, provenance_json = ? "
            "WHERE source_plan_identifier = ? AND rxnorm_id = ?",
            (
                selected_timestamp.isoformat(),
                merged_provenance,
                membership.key.source_plan_identifier,
                membership.rxnorm_id,
            ),
        )
        return
    connection.execute(
        "UPDATE membership SET provenance_json = ? WHERE "
        "source_plan_identifier = ? AND rxnorm_id = ?",
        (
            merged_provenance,
            membership.key.source_plan_identifier,
            membership.rxnorm_id,
        ),
    )


def upsert_spool_membership(
    connection: sqlite3.Connection,
    membership: NormalizedUHCDrugMembership,
) -> tuple[int, int]:
    """Insert or deterministically select one source observation."""

    existing = connection.execute(
        "SELECT semantic_json, effective_updated_at, provenance_json "
        "FROM membership WHERE source_plan_identifier = ? AND rxnorm_id = ?",
        (membership.key.source_plan_identifier, membership.rxnorm_id),
    ).fetchone()
    if existing is None:
        connection.execute(
            "INSERT INTO membership VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            _membership_values(membership),
        )
        return 0, 0
    stored_semantic, stored_timestamp, stored_provenance = existing
    stored_updated_at = dt.datetime.fromisoformat(stored_timestamp)
    if stored_semantic == membership.semantic_json:
        merged_provenance = _merged_provenance(
            stored_provenance,
            membership.provenance_json,
            selected_semantic_json=stored_semantic,
        )
        selected_timestamp = max(stored_updated_at, membership.effective_updated_at)
        _update_observation_provenance(
            connection,
            membership,
            merged_provenance,
            selected_timestamp=selected_timestamp,
        )
        return 1, 0
    if stored_updated_at == membership.effective_updated_at:
        raise UHCDrugNormalizationError(
            "UHC drug membership has an equal-time content conflict"
        )
    is_incoming_newer = membership.effective_updated_at > stored_updated_at
    selected_semantic_json = (
        membership.semantic_json if is_incoming_newer else stored_semantic
    )
    merged_provenance = _merged_provenance(
        stored_provenance,
        membership.provenance_json,
        selected_semantic_json=selected_semantic_json,
    )
    if is_incoming_newer:
        _update_selected_membership(connection, membership, merged_provenance)
    else:
        _update_observation_provenance(connection, membership, merged_provenance)
    return 0, 1


__all__ = (
    "MAX_PROVENANCE_JSON_BYTES",
    "MAX_PROVENANCE_RECORDS_PER_MEMBERSHIP",
    "upsert_spool_membership",
)
