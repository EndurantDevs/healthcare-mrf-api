# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare private supporting bytes with the independent protected MRF header."""

from __future__ import annotations

import hmac
import json
import re

from sqlalchemy import text

from db.plan_release_storage import storage_identifier
from process.plan_pricing_control import pricing_control_digest

_MARKER = "supporting_binding_metadata_digest"
_RECEIPTS = "supporting_rate_file_receipts"
_DOMAIN = "hp_supporting_binding_metadata_v1"
MAX_SUPPORTING_METADATA_BYTES = 16 * 1024 * 1024

_COMMITMENTS_SQL = """WITH requested AS (
 SELECT * FROM jsonb_to_recordset(CAST($1 AS jsonb)) AS selected(
   plan_release_id text,serving_revision_id text,binding_set_digest text,expected_binding_count integer)
), supporting_commitments AS MATERIALIZED (
 SELECT selected.*,header.source_manifest ? 'supporting_binding_metadata_digest' AS marker_present,
   jsonb_typeof(header.source_manifest->'supporting_binding_metadata_digest') AS marker_type,
   header.source_manifest->>'supporting_binding_metadata_digest' AS marker,
   header.source_manifest->>'contract' AS manifest_contract,
   binding.role,binding.ordinal AS binding_ordinal,
   jsonb_typeof(CAST(binding.metadata_json AS jsonb)) AS metadata_type,
   CASE WHEN CAST(binding.metadata_json AS jsonb) ? 'supporting_rate_file_receipts'
     THEN jsonb_build_object('supporting_rate_file_receipts',
       CAST(binding.metadata_json AS jsonb)->'supporting_rate_file_receipts')
     ELSE jsonb_build_object() END AS supporting_metadata
 FROM requested selected
 JOIN {schema}.plan_release_serving_revision header
   ON header.plan_release_id=selected.plan_release_id
  AND header.serving_revision_id=selected.serving_revision_id
  AND header.binding_set_digest=selected.binding_set_digest
  AND header.expected_binding_count=selected.expected_binding_count
 LEFT JOIN {registry_schema}.hp_plan_release_binding binding
   ON binding.serving_revision_id=selected.serving_revision_id
  AND binding.plan_release_id=selected.plan_release_id
), bounds AS (
 SELECT coalesce(sum(octet_length(to_jsonb(commitment)::text)+2),0)+2 AS byte_count
 FROM supporting_commitments commitment
)
SELECT byte_count <= CAST($2 AS bigint) AS bounded,
 CASE WHEN byte_count <= CAST($2 AS bigint) THEN (
   SELECT coalesce(jsonb_agg(to_jsonb(commitment) ORDER BY commitment.plan_release_id,
     commitment.role,commitment.binding_ordinal),'[]'::jsonb)::text
   FROM supporting_commitments commitment
 ) END AS rows_json FROM bounds"""


def _supporting_slots(commitment_rows):
    supporting_by_slot, seen_slots, case_ids = {}, set(), set()
    for commitment in commitment_rows:
        if type(commitment) is not dict:
            raise ValueError("plan_release_supporting_metadata_invalid")
        role, ordinal = commitment.get("role"), commitment.get("binding_ordinal")
        if (
            type(role) is not str
            or role not in {"in_network", "allowed_amounts"}
            or type(ordinal) is not int
            or not 0 <= ordinal <= 2_147_483_647
            or (role, ordinal) in seen_slots
            or commitment.get("metadata_type") != "object"
        ):
            raise ValueError("plan_release_supporting_metadata_slot_invalid")
        seen_slots.add((role, ordinal))
        metadata = commitment.get("supporting_metadata")
        if type(metadata) is not dict or set(metadata) - {_RECEIPTS}:
            raise ValueError("plan_release_supporting_metadata_invalid")
        if _RECEIPTS not in metadata:
            continue
        receipts = metadata[_RECEIPTS]
        if type(receipts) is not list or not receipts:
            raise ValueError("plan_release_supporting_metadata_invalid")
        for receipt in receipts:
            case_id = receipt.get("review_case_id") if type(receipt) is dict else None
            if (
                type(case_id) is not str
                or re.fullmatch(r"hpreview_[0-9a-f]{32}", case_id) is None
                or case_id in case_ids
            ):
                raise ValueError("plan_release_supporting_receipt_invalid")
            case_ids.add(case_id)
        supporting_by_slot[(role, ordinal)] = metadata
    return supporting_by_slot


def require_supporting_metadata_commitment(commitment_rows):
    """Verify bytes only; Source publication owns the original review authority."""
    if type(commitment_rows) is not list or not commitment_rows:
        raise ValueError("plan_release_supporting_metadata_invalid")
    supporting_by_slot = _supporting_slots(commitment_rows)
    header = commitment_rows[0]
    header_fields = ("marker_present", "marker_type", "marker", "manifest_contract")
    if any(
        any(commitment.get(field) != header.get(field) for field in header_fields) for commitment in commitment_rows
    ):
        raise ValueError("plan_release_supporting_header_ambiguous")
    if header.get("marker_present") is False and not supporting_by_slot:
        return
    marker = header.get("marker")
    if (
        header.get("marker_present") is not True
        or header.get("marker_type") != "string"
        or type(marker) is not str
        or re.fullmatch(r"[0-9a-f]{64}", marker) is None
        or header.get("manifest_contract") != "hp-plan-serving-revision-v1"
        or not supporting_by_slot
    ):
        raise ValueError("plan_release_supporting_metadata_commitment_invalid")
    ordered_slots = [[role, ordinal, metadata] for (role, ordinal), metadata in sorted(supporting_by_slot.items())]
    try:
        actual = pricing_control_digest(ordered_slots, domain=_DOMAIN)
    except (TypeError, ValueError, OverflowError, RecursionError) as error:
        raise ValueError("plan_release_supporting_metadata_invalid") from error
    if not hmac.compare_digest(marker, actual):
        raise ValueError("plan_release_supporting_metadata_commitment_changed")


def _require_complete_commitments(commitment_rows, admissions):
    by_release_id = {admission.plan_release_id: [] for admission in admissions}
    expected_by_release_id = {admission.plan_release_id: admission for admission in admissions}
    for commitment in commitment_rows:
        if type(commitment) is not dict:
            raise ValueError("plan_release_supporting_metadata_invalid")
        admission = expected_by_release_id.get(commitment.get("plan_release_id"))
        if admission is None or any(
            commitment.get(field) != getattr(admission, field)
            for field in ("serving_revision_id", "binding_set_digest", "expected_binding_count")
        ):
            raise ValueError("plan_release_supporting_identity_invalid")
        by_release_id[admission.plan_release_id].append(commitment)
    for identity, group in by_release_id.items():
        if len(group) != expected_by_release_id[identity].expected_binding_count:
            raise ValueError("plan_release_source_binding_incomplete")
        require_supporting_metadata_commitment(group)


async def require_supporting_metadata_fetch(fetch, schema, registry_schema, admissions, *, max_bytes):
    """Read all admitted releases in one bounded batch in the caller's transaction."""
    if not admissions:
        return
    requested_admissions = [
        {
            field: getattr(admission, field)
            for field in ("plan_release_id", "serving_revision_id", "binding_set_digest", "expected_binding_count")
        }
        for admission in admissions
    ]
    if len({entry["plan_release_id"] for entry in requested_admissions}) != len(requested_admissions):
        raise ValueError("plan_release_supporting_identity_invalid")
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("plan_release_supporting_metadata_limit_invalid")
    pages = await fetch(
        _COMMITMENTS_SQL.format(schema=storage_identifier(schema), registry_schema=storage_identifier(registry_schema)),
        json.dumps(requested_admissions),
        max_bytes,
    )
    if len(pages) != 1 or pages[0]["bounded"] is not True:
        raise ValueError("plan_release_supporting_metadata_unavailable")
    encoded = pages[0]["rows_json"]
    if type(encoded) is not str or len(encoded.encode()) > max_bytes:
        raise ValueError("plan_release_supporting_metadata_invalid")
    try:
        commitment_rows = json.loads(encoded)
    except (ValueError, RecursionError) as error:
        raise ValueError("plan_release_supporting_metadata_invalid") from error
    if type(commitment_rows) is not list:
        raise ValueError("plan_release_supporting_metadata_invalid")
    _require_complete_commitments(commitment_rows, admissions)


async def require_session_supporting_metadata(session, schema, registry_schema, admission):
    """Use the same complete check without leaving the exact ORM Reader session."""

    async def fetch(sql, *parameters):
        """Translate fixed parameters without acquiring another Reader connection."""
        sql = re.sub(r"\$(\d+)", lambda match: ":p" + match[1], sql)
        result = await session.execute(
            text(sql), {"p" + str(index): value for index, value in enumerate(parameters, 1)}
        )
        return [dict(getattr(row, "_mapping", row)) for row in result]

    await require_supporting_metadata_fetch(
        fetch, schema, registry_schema, [admission], max_bytes=MAX_SUPPORTING_METADATA_BYTES
    )
