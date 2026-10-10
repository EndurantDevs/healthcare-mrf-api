# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain a complete native target ledger outside rotating source snapshots."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import json
import re
from dataclasses import dataclass
from uuid import UUID, uuid4

import asyncpg

from process.registry_source_observation_store import _namespace
from process.registry_source_selection_receipt import _unique_object

MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_COPY_BYTES = MAX_ARTIFACT_BYTES + 1024
PARSER_VERSION = "registry-target-ledger-v1"
_COPY_HEADER = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0"
_COPY_COLUMNS = ("snapshot_id", "source_record_key", "source_row_number", "status", "observation_json", "issues_json")
_DESCRIPTOR_FIELDS = {
    "component",
    "revision",
    "parser_version",
    "snapshot_id",
    "source_sha256",
    "artifact_sha256",
    "source_rows",
    "target_count",
    "physical_records",
}


def _artifact_digest(copy_bytes):
    """Inspect six physical COPY fields without decoding the logical source rows."""
    view = memoryview(copy_bytes)
    offset = len(_COPY_HEADER)
    if view[offset : offset + 2] != b"\x00\x06":
        raise ValueError
    offset += 2
    artifact = None
    for index in range(6):
        if offset + 4 > len(view) - 2:
            raise ValueError
        length = int.from_bytes(view[offset : offset + 4], signed=True)
        offset += 4
        if length < 0 or offset + length > len(view) - 2:
            raise ValueError
        if index == 4:
            if not 2 <= length <= MAX_ARTIFACT_BYTES + 1 or view[offset] != 1:
                raise ValueError
            artifact = view[offset + 1 : offset + length]
        offset += length
    if offset != len(view) - 2 or artifact is None:
        raise ValueError
    return hashlib.sha256(artifact).hexdigest()


class RegistryRequiredTargetError(ValueError):
    """Invalid input or conflicting immutable evidence leaves no partial ledger."""


@dataclass(frozen=True)
class RegistryRequiredTargetEdition:
    """Operator-owned source identity; target identifiers are never canonical IDs."""

    snapshot_id: UUID
    source_url: str
    input_sha256: str

    def __post_init__(self):
        if type(self.snapshot_id) is not UUID or not self.snapshot_id.int:
            raise RegistryRequiredTargetError("registry_required_target_identity_invalid")
        if (
            type(self.source_url) is not str
            or not self.source_url
            or self.source_url.strip() != self.source_url
            or len(self.source_url.encode()) > 2048
            or not self.source_url.isprintable()
        ):
            raise RegistryRequiredTargetError("registry_required_target_source_invalid")
        if type(self.input_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", self.input_sha256) is None:
            raise RegistryRequiredTargetError("registry_required_target_digest_invalid")


def _encode(input_bytes, edition):
    """One native pass validates every source row and emits one bounded COPY artifact."""
    try:
        native = importlib.import_module("ptg2_address_canon")
        encoded = native.encode_registry_target_ledger_artifact(input_bytes, str(edition.snapshot_id))
    except ImportError, AttributeError, RuntimeError, TypeError:
        raise RegistryRequiredTargetError("registry_required_target_native_unavailable") from None
    except ValueError:
        raise RegistryRequiredTargetError("registry_required_target_input_invalid") from None
    if type(encoded) is not tuple or len(encoded) != 2:
        raise RegistryRequiredTargetError("registry_required_target_native_unavailable")
    copy_bytes, descriptor_bytes = encoded
    if (
        type(copy_bytes) is not bytes
        or not 21 <= len(copy_bytes) <= MAX_COPY_BYTES
        or not copy_bytes.startswith(_COPY_HEADER)
        or not copy_bytes.endswith(b"\xff\xff")
        or type(descriptor_bytes) is not bytes
        or not 1 <= len(descriptor_bytes) <= 4096
    ):
        raise RegistryRequiredTargetError("registry_required_target_native_unavailable")
    try:
        descriptor = json.loads(descriptor_bytes, object_pairs_hook=_unique_object)
        if (
            type(descriptor) is not dict
            or set(descriptor) != _DESCRIPTOR_FIELDS
            or descriptor["component"] != "registry_required_target_ledger"
            or type(descriptor["revision"]) is not int
            or descriptor["revision"] != 1
            or descriptor["parser_version"] != PARSER_VERSION
            or descriptor["snapshot_id"] != str(edition.snapshot_id)
            or descriptor["source_sha256"] != edition.input_sha256
            or type(descriptor["artifact_sha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", descriptor["artifact_sha256"]) is None
            or descriptor["artifact_sha256"] != _artifact_digest(copy_bytes)
            or type(descriptor["physical_records"]) is not int
            or descriptor["physical_records"] != 1
            or type(descriptor["source_rows"]) is not int
            or not 1 <= descriptor["source_rows"] <= 50_000
            or type(descriptor["target_count"]) is not int
            or not 1 <= descriptor["target_count"] <= 5000
        ):
            raise ValueError
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistryRequiredTargetError("registry_required_target_native_unavailable") from None
    return copy_bytes, descriptor


async def _stage(connection, namespace, copy_bytes, edition, descriptor):
    table = "registry_required_targets_" + uuid4().hex
    staging = '"' + table + '"'
    await connection.execute(
        f"CREATE TEMP TABLE {staging} (LIKE {namespace}.registry_source_observation INCLUDING ALL) ON COMMIT DROP"
    )
    with io.BytesIO(copy_bytes) as copy_stream:
        status = await connection.copy_to_table(
            table, schema_name="pg_temp", columns=_COPY_COLUMNS, format="binary", source=copy_stream
        )
        if status != "COPY 1" or copy_stream.tell() != len(copy_bytes):
            raise RegistryRequiredTargetError("registry_required_target_copy_incomplete")
    valid = await connection.fetchval(
        f"""SELECT count(*)=1 AND COALESCE(bool_and(
          snapshot_id=$1 AND source_record_key='ledger:v1' AND source_row_number=1
          AND status='accepted' AND issues_json='[]'::jsonb
          AND observation_json->>'component'='registry_required_target_ledger'
          AND observation_json->'revision'='1'::jsonb AND observation_json->>'parser_version'=$2
          AND observation_json-ARRAY['component','revision','parser_version','ledger']='{{}}'::jsonb
          AND jsonb_typeof(observation_json->'ledger')='object'
          AND observation_json->'ledger'->>'source_sha256'=$3
          AND observation_json->'ledger'->'row_count'=to_jsonb($4::integer)
          AND jsonb_typeof(observation_json->'ledger'->'observations')='array'
          AND jsonb_typeof(observation_json->'ledger'->'targets')='array'
          AND jsonb_array_length(CASE WHEN jsonb_typeof(observation_json->'ledger'->'observations')='array'
            THEN observation_json->'ledger'->'observations' ELSE '[]'::jsonb END)=$4
          AND jsonb_array_length(CASE WHEN jsonb_typeof(observation_json->'ledger'->'targets')='array'
            THEN observation_json->'ledger'->'targets' ELSE '[]'::jsonb END)=$5),false) FROM {staging}""",
        edition.snapshot_id,
        PARSER_VERSION,
        edition.input_sha256,
        descriptor["source_rows"],
        descriptor["target_count"],
    )
    if valid is not True:
        raise RegistryRequiredTargetError("registry_required_target_landing_invalid")
    return staging


async def _is_replay_after_retention(
    connection,
    namespace,
    staging,
    edition,
    descriptor,
    *,
    source_system="required-network-targets",
    source_id="required-networks",
    parser_version=PARSER_VERSION,
):
    """Retain validated evidence and report whether it was already present."""
    await connection.execute(
        f"""INSERT INTO {namespace}.registry_source_snapshot
          (snapshot_id,source_system,source_id,edition_id,source_url,artifact_sha256,input_sha256,parser_version)
          VALUES($1,$6,$7,$2,$3,$4,$2,$5)
          ON CONFLICT(snapshot_id) DO NOTHING""",
        edition.snapshot_id,
        edition.input_sha256,
        edition.source_url,
        descriptor["artifact_sha256"],
        parser_version,
        source_system,
        source_id,
    )
    matches = await connection.fetchval(
        f"""SELECT source_system=$6 AND source_id=$7
          AND edition_id=$2 AND source_url=$3 AND artifact_sha256=$4 AND input_sha256=$2
          AND parser_version=$5 AND reporting_year IS NULL AND published_at IS NULL
          FROM {namespace}.registry_source_snapshot WHERE snapshot_id=$1 FOR UPDATE""",
        edition.snapshot_id,
        edition.input_sha256,
        edition.source_url,
        descriptor["artifact_sha256"],
        parser_version,
        source_system,
        source_id,
    )
    if matches is not True:
        raise RegistryRequiredTargetError("registry_required_target_edition_conflict")
    retained = await connection.fetchrow(
        f"""SELECT count(*) AS records,count(*) FILTER(WHERE EXISTS(
          SELECT 1 FROM {staging} expected WHERE expected.snapshot_id=actual.snapshot_id
            AND expected.source_record_key=actual.source_record_key
            AND (expected.source_row_number,expected.status,expected.observation_json,expected.issues_json)
              =(actual.source_row_number,actual.status,actual.observation_json,actual.issues_json))) AS matching
          FROM {namespace}.registry_source_observation actual WHERE snapshot_id=$1""",
        edition.snapshot_id,
    )
    if retained["records"]:
        if (retained["records"], retained["matching"]) != (1, 1):
            raise RegistryRequiredTargetError("registry_required_target_artifact_conflict")
        return True
    status = await connection.execute(f"INSERT INTO {namespace}.registry_source_observation SELECT * FROM {staging}")
    if status != "INSERT 0 1":
        raise RegistryRequiredTargetError("registry_required_target_retention_incomplete")
    return False


async def admit_registry_required_targets(connection, input_bytes, edition, *, control_schema=None):
    """Trusted operators retain evidence atomically without changing drafts or serving.

    The caller owns authorization and its transaction. A private landing is
    completely validated before retention; a savepoint keeps an invalid import
    from poisoning the caller or leaving partial source evidence.
    """
    if (
        type(edition) is not RegistryRequiredTargetEdition
        or type(input_bytes) is not bytes
        or not 1 <= len(input_bytes) <= MAX_INPUT_BYTES
        or hashlib.sha256(input_bytes).hexdigest() != edition.input_sha256
    ):
        raise RegistryRequiredTargetError("registry_required_target_input_invalid")
    if not connection.is_in_transaction():
        raise RegistryRequiredTargetError("registry_required_target_transaction_required")
    namespace = _namespace(control_schema)
    copy_bytes, descriptor = await asyncio.to_thread(_encode, input_bytes, edition)
    try:
        async with connection.transaction():
            staging = await _stage(connection, namespace, copy_bytes, edition, descriptor)
            replayed = await _is_replay_after_retention(connection, namespace, staging, edition, descriptor)
            return {
                **descriptor,
                "copy_sha256": hashlib.sha256(copy_bytes).hexdigest(),
                "copy_bytes": len(copy_bytes),
                "replayed": replayed,
            }
    except asyncpg.PostgresError:
        raise RegistryRequiredTargetError("registry_required_target_retention_failed") from None
