# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain reviewed target decisions without approving a mapping or changing search."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import json
import re
from uuid import UUID, uuid4

import asyncpg

from process.registry_required_target_store import (
    _COPY_COLUMNS,
    _COPY_HEADER,
    MAX_COPY_BYTES,
    MAX_INPUT_BYTES,
    RegistryRequiredTargetEdition,
    RegistryRequiredTargetError,
    _artifact_digest,
    _is_replay_after_retention,
)
from process.registry_source_observation_store import _namespace
from process.registry_source_selection_receipt import _unique_object

PARSER_VERSION = "registry-target-review-v1"
EVIDENCE_PREFIX = "required-target-review:"
MAX_LEDGER_TEXT_BYTES = 32 * 1024 * 1024
_DESCRIPTOR_FIELDS = {
    "component",
    "revision",
    "parser_version",
    "snapshot_id",
    "source_sha256",
    "artifact_sha256",
    "ledger_snapshot_id",
    "ledger_artifact_sha256",
    "decision_count",
    "resolved_count",
    "physical_records",
}


async def _read_ledger(connection, namespace, snapshot_id):
    entry = await connection.fetchrow(
        f"""SELECT snapshot.artifact_sha256,snapshot.input_sha256,
          CASE WHEN octet_length(observation.observation_json::text)<=$2
            THEN observation.observation_json::text END AS document
        FROM {namespace}.registry_source_snapshot snapshot
        JOIN {namespace}.registry_source_observation observation USING(snapshot_id)
        WHERE snapshot.snapshot_id=$1 AND snapshot.source_system='required-network-targets'
          AND snapshot.source_id='required-networks' AND snapshot.parser_version='registry-target-ledger-v1'
          AND snapshot.edition_id=snapshot.input_sha256
          AND snapshot.reporting_year IS NULL AND snapshot.published_at IS NULL
          AND observation.source_record_key='ledger:v1' AND observation.source_row_number=1
          AND observation.status='accepted' AND observation.issues_json='[]'::jsonb
          AND (SELECT count(*) FROM {namespace}.registry_source_observation complete
            WHERE complete.snapshot_id=snapshot.snapshot_id)=1
        FOR SHARE OF snapshot,observation""",
        snapshot_id,
        MAX_LEDGER_TEXT_BYTES,
    )
    if entry is None or entry["document"] is None:
        raise RegistryRequiredTargetError("registry_required_target_review_ledger_unavailable")
    return entry


def _encode(input_bytes, ledger, edition, ledger_snapshot_id):
    try:
        native = importlib.import_module("ptg2_address_canon")
        encoded = native.encode_registry_required_target_review_artifact(
            input_bytes, ledger["document"].encode(), str(edition.snapshot_id)
        )
    except ImportError, AttributeError, RuntimeError, TypeError:
        raise RegistryRequiredTargetError("registry_required_target_review_native_unavailable") from None
    except ValueError:
        raise RegistryRequiredTargetError("registry_required_target_review_input_invalid") from None
    if type(encoded) is not tuple or len(encoded) != 2:
        raise RegistryRequiredTargetError("registry_required_target_review_native_unavailable")
    copy_bytes, descriptor_bytes = encoded
    if (
        type(copy_bytes) is not bytes
        or not 21 <= len(copy_bytes) <= MAX_COPY_BYTES
        or not copy_bytes.startswith(_COPY_HEADER)
        or not copy_bytes.endswith(b"\xff\xff")
        or type(descriptor_bytes) is not bytes
        or not 1 <= len(descriptor_bytes) <= 4096
    ):
        raise RegistryRequiredTargetError("registry_required_target_review_native_unavailable")
    try:
        descriptor = json.loads(descriptor_bytes, object_pairs_hook=_unique_object)
        if (
            type(descriptor) is not dict
            or set(descriptor) != _DESCRIPTOR_FIELDS
            or descriptor["component"] != "registry_required_target_review"
            or type(descriptor["revision"]) is not int
            or descriptor["revision"] != 1
            or descriptor["parser_version"] != PARSER_VERSION
            or descriptor["snapshot_id"] != str(edition.snapshot_id)
            or descriptor["source_sha256"] != edition.input_sha256
            or descriptor["ledger_snapshot_id"] != str(ledger_snapshot_id)
            or descriptor["ledger_artifact_sha256"] != ledger["artifact_sha256"]
            or type(descriptor["artifact_sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", descriptor["artifact_sha256"]) is None
            or descriptor["artifact_sha256"] != _artifact_digest(copy_bytes)
            or type(descriptor["physical_records"]) is not int
            or descriptor["physical_records"] != 1
            or type(descriptor["decision_count"]) is not int
            or not 1 <= descriptor["decision_count"] <= 5000
            or type(descriptor["resolved_count"]) is not int
            or not 0 <= descriptor["resolved_count"] <= descriptor["decision_count"]
        ):
            raise ValueError
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryRequiredTargetError("registry_required_target_review_native_unavailable") from None
    return copy_bytes, descriptor


async def _stage(connection, namespace, copy_bytes, edition, ledger_snapshot_id, ledger, descriptor):
    table = "registry_required_review_" + uuid4().hex
    staging = '"' + table + '"'
    await connection.execute(
        f"CREATE TEMP TABLE {staging} (LIKE {namespace}.registry_source_observation INCLUDING ALL) ON COMMIT DROP"
    )
    with io.BytesIO(copy_bytes) as copy_stream:
        status = await connection.copy_to_table(
            table, schema_name="pg_temp", columns=_COPY_COLUMNS, format="binary", source=copy_stream
        )
        if status != "COPY 1" or copy_stream.tell() != len(copy_bytes):
            raise RegistryRequiredTargetError("registry_required_target_review_copy_incomplete")
    valid = await connection.fetchval(
        f"""WITH documents AS MATERIALIZED (
          SELECT *,CASE WHEN jsonb_typeof(observation_json->'decisions')='array'
            THEN observation_json->'decisions' ELSE '[]'::jsonb END AS decisions FROM {staging}
        ),decisions AS MATERIALIZED (
          SELECT decision FROM documents CROSS JOIN LATERAL jsonb_array_elements(decisions) decision
        ),resolved AS MATERIALIZED (
          SELECT decision,CASE WHEN jsonb_typeof(decision->'network_id')='number'
            AND decision->>'network_id' ~ '^[1-9][0-9]{{0,9}}$'
            THEN CASE WHEN (decision->>'network_id')::numeric<=2147483647
              THEN (decision->>'network_id')::integer END END AS network_id
          FROM decisions WHERE decision->>'resolution_status'='resolved'
        ) SELECT count(*)=1 AND COALESCE(bool_and(
          snapshot_id=$1 AND source_record_key='review:v1' AND source_row_number=1
          AND status='accepted' AND issues_json='[]'::jsonb
          AND observation_json->>'component'='registry_required_target_review'
          AND observation_json->'revision'='1'::jsonb AND observation_json->>'parser_version'=$2
          AND observation_json-ARRAY['component','revision','parser_version','source_sha256',
            'ledger_snapshot_id','ledger_artifact_sha256','ledger_source_sha256','decisions']='{{}}'::jsonb
          AND observation_json->>'source_sha256'=$3
          AND observation_json->>'ledger_snapshot_id'=$4 AND observation_json->>'ledger_artifact_sha256'=$5
          AND observation_json->>'ledger_source_sha256'=$6
          AND jsonb_typeof(observation_json->'decisions')='array'
          AND jsonb_array_length(decisions)=$7),false)
          AND (SELECT count(*) FROM resolved)=$8
          AND NOT EXISTS(SELECT 1 FROM resolved
            LEFT JOIN {namespace}.network_registry_identity identity USING(network_id)
            LEFT JOIN {namespace}.network_registry_record network USING(network_id)
            WHERE identity.network_id IS NULL OR network.network_id IS NULL OR network.archived)
        FROM documents""",
        edition.snapshot_id,
        PARSER_VERSION,
        edition.input_sha256,
        str(ledger_snapshot_id),
        ledger["artifact_sha256"],
        ledger["input_sha256"],
        descriptor["decision_count"],
        descriptor["resolved_count"],
    )
    if valid is not True:
        raise RegistryRequiredTargetError("registry_required_target_review_landing_invalid")
    return staging


async def admit_registry_required_target_review(
    connection, input_bytes, edition, ledger_snapshot_id, *, control_schema=None
):
    """Trusted operators retain decisions; existing binding approval controls their use."""
    if (
        type(edition) is not RegistryRequiredTargetEdition
        or type(ledger_snapshot_id) is not UUID
        or not ledger_snapshot_id.int
        or type(input_bytes) is not bytes
        or not 1 <= len(input_bytes) <= MAX_INPUT_BYTES
        or hashlib.sha256(input_bytes).hexdigest() != edition.input_sha256
    ):
        raise RegistryRequiredTargetError("registry_required_target_review_input_invalid")
    if not connection.is_in_transaction():
        raise RegistryRequiredTargetError("registry_required_target_review_transaction_required")
    namespace = _namespace(control_schema)
    try:
        async with connection.transaction():
            ledger = await _read_ledger(connection, namespace, ledger_snapshot_id)
            copy_bytes, descriptor = await asyncio.to_thread(_encode, input_bytes, ledger, edition, ledger_snapshot_id)
            staging = await _stage(connection, namespace, copy_bytes, edition, ledger_snapshot_id, ledger, descriptor)
            replayed = await _is_replay_after_retention(
                connection,
                namespace,
                staging,
                edition,
                descriptor,
                source_system="required-network-target-reviews",
                source_id="required-network-review",
                parser_version=PARSER_VERSION,
            )
            return {
                **descriptor,
                "evidence_id": EVIDENCE_PREFIX + str(edition.snapshot_id),
                "copy_sha256": hashlib.sha256(copy_bytes).hexdigest(),
                "copy_bytes": len(copy_bytes),
                "replayed": replayed,
            }
    except asyncpg.PostgresError:
        raise RegistryRequiredTargetError("registry_required_target_review_retention_failed") from None
