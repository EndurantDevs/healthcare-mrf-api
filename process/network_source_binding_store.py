# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic reviewed source bindings; imported aliases and serving heads stay pinned."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
from dataclasses import dataclass
from uuid import uuid4

import asyncpg

from process.ext.address_fast import _fast_module
from process.registry_record_store import (
    RegistryActor,
    RegistryAddressUnavailable,
    RegistryRecordConflict,
    _bounded_text,
    _validated_actor,
)
from process.registry_required_target_review_references import (
    require_required_target_review_references,
)
from process.registry_source_observation_store import _namespace

_MAX_INPUT_BYTES = 8 * 1024 * 1024
_MAX_COPY_BYTES = 16 * 1024 * 1024
_MAX_RECEIPT_BYTES = 1024 * 1024
_MAX_REVISION = 9223372036854775805
_COPY_HEADER = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0"
COPY_COLUMNS = (
    "binding_id",
    "source_system",
    "source_id",
    "dataset_schema",
    "dataset_id",
    "producer_id",
    "edition_id",
    "source_key",
    "source_scope_json",
    "binding_key",
    "network_id",
    "evidence_id",
    "evidence_sha256",
    "operation",
    "expected_revision",
    "expected_network_id",
)


@dataclass(frozen=True)
class NetworkSourceBindingBatchCommand:
    """The complete native input and review metadata form one actor-bound retry."""

    input_bytes: bytes
    reason: str
    idempotency_key: str


def _metadata(command, actor):
    """Bind exact submitted bytes, bounded review text and the complete actor."""
    if not isinstance(command, NetworkSourceBindingBatchCommand) or type(command.input_bytes) is not bytes:
        raise ValueError("registry_source_binding_input_invalid")
    if not 1 <= len(command.input_bytes) <= _MAX_INPUT_BYTES:
        raise ValueError("registry_source_binding_input_invalid")
    reason = _bounded_text(command.reason, 1000, "reason")
    if (
        _bounded_text(command.idempotency_key, 128, "idempotency_key") != command.idempotency_key
        or not command.idempotency_key.isprintable()
    ):
        raise ValueError("registry_idempotency_key_invalid")
    actor_document = _validated_actor(actor)
    actor_json = json.dumps(actor_document, sort_keys=True, separators=(",", ":"))
    actor_key = hashlib.sha256(actor_json.encode()).hexdigest()
    review_json = json.dumps(
        {"actor": actor_document, "reason": reason, "idempotency_key": command.idempotency_key},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    request = hashlib.sha256(b"registry_network_source_binding_batch:v1:")
    request.update(len(review_json).to_bytes(8, "big"))
    request.update(review_json)
    request.update(command.input_bytes)
    return actor_json, actor_key, reason, request.hexdigest()


def _encode(input_bytes):
    """Fail closed when the installed whole-batch native encoder is unavailable."""
    native = _fast_module()
    if native is None or not callable(getattr(native, "encode_network_source_binding_batch", None)):
        raise RegistryAddressUnavailable("registry_source_binding_native_unavailable")
    try:
        encoded = native.encode_network_source_binding_batch(input_bytes)
    except ValueError:
        raise ValueError("registry_source_binding_input_invalid") from None
    except AttributeError, RuntimeError, TypeError:
        raise RegistryAddressUnavailable("registry_source_binding_native_unavailable") from None
    if type(encoded) is not tuple or len(encoded) != 2:
        raise RegistryAddressUnavailable("registry_source_binding_native_unavailable")
    copy_bytes, row_count = encoded
    if type(row_count) is int and row_count == 0:
        raise ValueError("registry_source_binding_input_invalid")
    if (
        type(copy_bytes) is not bytes
        or type(row_count) is not int
        or not 1 <= row_count <= 5000
        or not 21 <= len(copy_bytes) <= _MAX_COPY_BYTES
        or not copy_bytes.startswith(_COPY_HEADER)
        or not copy_bytes.endswith(b"\xff\xff")
    ):
        raise RegistryAddressUnavailable("registry_source_binding_native_unavailable")
    return copy_bytes, row_count


async def _stage(connection, copy_bytes, row_count):
    """One native COPY lands all coordinates and explicit CAS values privately."""
    table = "registry_source_binding_" + uuid4().hex
    staging = '"' + table + '"'
    await connection.execute(f"""CREATE TEMP TABLE {staging} (
      binding_id uuid PRIMARY KEY,source_system text NOT NULL,source_id text NOT NULL,
      dataset_schema text NOT NULL,dataset_id text NOT NULL,producer_id text NOT NULL,
      edition_id text NOT NULL,source_key text NOT NULL,source_scope_json jsonb NOT NULL,
      binding_key text NOT NULL UNIQUE,network_id integer NOT NULL,evidence_id text NOT NULL,
      evidence_sha256 text NOT NULL,operation text NOT NULL,expected_revision bigint NOT NULL,
      expected_network_id integer) ON COMMIT DROP""")
    with io.BytesIO(copy_bytes) as source:
        status = await connection.copy_to_table(
            table,
            schema_name="pg_temp",
            columns=COPY_COLUMNS,
            format="binary",
            source=source,
        )
        if status != f"COPY {row_count}" or source.tell() != len(copy_bytes):
            raise RegistryAddressUnavailable("registry_source_binding_copy_unavailable")
    return staging


_VALIDATION_SQL = """SELECT
  COALESCE(bool_and(identity.network_id IS NOT NULL AND
    (target.operation='close' OR (network.network_id IS NOT NULL AND NOT network.archived))),false) AS targets_valid,
  COALESCE(bool_and(CASE WHEN target.operation='bind' THEN head.binding_id IS NULL AND keyed.binding_id IS NULL
    ELSE head.binding_id IS NOT NULL AND head.revision=target.expected_revision
      AND head.network_id=target.expected_network_id AND (target.operation<>'close' OR NOT head.archived)
      AND head.source_system=target.source_system AND head.source_id=target.source_id
      AND head.dataset_schema=target.dataset_schema AND head.dataset_id=target.dataset_id
      AND head.producer_id=target.producer_id AND head.edition_id=target.edition_id
      AND head.source_key=target.source_key AND head.source_scope_json=target.source_scope_json
      AND head.binding_key=target.binding_key END),false) AS revisions_valid,
  COALESCE(bool_or(starts_with(target.evidence_id,'required-target-review:')),false) AS required_reviews
FROM {staging} target LEFT JOIN {namespace}.network_registry_identity identity USING(network_id)
LEFT JOIN {namespace}.network_registry_record network USING(network_id)
LEFT JOIN {namespace}.registry_network_binding head USING(binding_id)
LEFT JOIN {namespace}.registry_network_binding keyed ON keyed.binding_key=target.binding_key"""

_WRITE_SQL = """WITH created AS (
  INSERT INTO {namespace}.registry_network_binding AS head(binding_id,source_system,source_id,dataset_schema,
    dataset_id,producer_id,edition_id,source_key,source_scope_json,binding_key,network_id,evidence_id,evidence_sha256)
  SELECT binding_id,source_system,source_id,dataset_schema,dataset_id,producer_id,edition_id,source_key,
    source_scope_json,binding_key,network_id,evidence_id,evidence_sha256 FROM {staging} WHERE operation='bind'
  RETURNING head.*
), changed AS (
  UPDATE {namespace}.registry_network_binding head SET network_id=target.network_id,evidence_id=target.evidence_id,
    evidence_sha256=target.evidence_sha256,archived=(target.operation='close'),revision=target.expected_revision+1
  FROM {staging} target WHERE target.operation IN ('rebind','close') AND head.binding_id=target.binding_id
    AND head.revision=target.expected_revision AND head.network_id=target.expected_network_id RETURNING head.*
), written AS (SELECT * FROM created UNION ALL SELECT * FROM changed), history AS (
  INSERT INTO {namespace}.registry_record_history(record_kind,record_key,revision,custom_revision,record_json,
    actor_json,reason,idempotency_key,request_sha256)
  SELECT 'network_binding',binding_id::text,revision,$1,to_jsonb(written),$2::jsonb,$3,$4,$5 FROM written
  RETURNING record_key,revision,record_json
), receipt AS (
  SELECT count(*) AS written_count,jsonb_build_object('custom_revision',$1::bigint,'records',
    COALESCE(jsonb_agg(jsonb_build_object('record_kind','network_binding','record_id',record_key,'revision',revision,
      'network_id',(record_json->>'network_id')::integer,'archived',(record_json->>'archived')::boolean)
      ORDER BY record_key),'[]'::jsonb)) AS document FROM history
) SELECT written_count,document::text AS receipt_json,octet_length(document::text) AS receipt_bytes FROM receipt"""


async def _save(connection, namespace, staging, row_count, revision, metadata, key):
    """Retain full immutable history and one bounded receipt with one draft step."""
    actor_json, actor_key, reason, request_sha256 = metadata
    written = await connection.fetchrow(
        _WRITE_SQL.format(namespace=namespace, staging=staging),
        revision,
        actor_json,
        reason,
        "source-binding:" + uuid4().hex,
        request_sha256,
    )
    if written["written_count"] != row_count:
        raise RegistryRecordConflict("registry_source_binding_revision_conflict")
    if written["receipt_bytes"] > _MAX_RECEIPT_BYTES:
        raise ValueError("registry_source_binding_receipt_limit")
    await connection.execute(f"UPDATE {namespace}.registry_revision_control SET draft_revision=$1 WHERE id=1", revision)
    await connection.execute(
        f"INSERT INTO {namespace}.registry_network_binding_batch(actor_key,idempotency_key,request_sha256,receipt_json) "
        "VALUES($1,$2,$3,$4::jsonb)",
        actor_key,
        key,
        request_sha256,
        written["receipt_json"],
    )
    await connection.execute(f"DROP TABLE {staging}")
    return json.loads(written["receipt_json"])


async def _apply(connection, command, metadata, namespace, published_plan_store):
    """Serialize low-volume control once, then validate every reference together."""
    from process.registry_published_plan_binding_refs import (
        require_published_plan_binding_references,
    )

    await require_published_plan_binding_references(connection, namespace, command.input_bytes, published_plan_store)
    control = await connection.fetchrow(
        f"SELECT draft_revision FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
    )
    if control is None:
        raise RegistryRecordConflict("registry_source_binding_control_unavailable")
    replay = await connection.fetchrow(
        f"SELECT request_sha256,receipt_json::text AS receipt_json FROM {namespace}.registry_network_binding_batch "
        "WHERE actor_key=$1 AND idempotency_key=$2",
        metadata[1],
        command.idempotency_key,
    )
    if replay is not None:
        if replay["request_sha256"] != metadata[3]:
            raise RegistryRecordConflict("registry_source_binding_idempotency_conflict")
        if len(replay["receipt_json"].encode()) > _MAX_RECEIPT_BYTES:
            raise RegistryAddressUnavailable("registry_source_binding_receipt_limit")
        return json.loads(replay["receipt_json"])
    if control["draft_revision"] > _MAX_REVISION:
        raise RegistryRecordConflict("registry_source_binding_control_unavailable")
    copy_bytes, row_count = await asyncio.to_thread(_encode, command.input_bytes)
    staging = await _stage(connection, copy_bytes, row_count)
    status = await connection.fetchrow(_VALIDATION_SQL.format(namespace=namespace, staging=staging))
    if not status["targets_valid"]:
        raise ValueError("registry_source_binding_target_invalid")
    if not status["revisions_valid"]:
        raise RegistryRecordConflict("registry_source_binding_revision_conflict")
    if status["required_reviews"]:
        await require_required_target_review_references(connection, namespace, staging)
    return await _save(
        connection,
        namespace,
        staging,
        row_count,
        control["draft_revision"] + 1,
        metadata,
        command.idempotency_key,
    )


async def apply_network_source_binding_batch(
    connection,
    command,
    actor: RegistryActor,
    *,
    control_schema=None,
    published_plan_store=None,
):
    """Use the caller's transaction and roll back only this batch on failure.

    Callers verify live action grants, including on replay. This store only
    changes management drafts, history and actor-bound retry evidence. Legacy
    aliases remain observations; they do not veto an explicit reviewed mapping.
    """
    metadata = _metadata(command, actor)
    namespace = _namespace(control_schema)
    if not connection.is_in_transaction():
        raise ValueError("registry_source_binding_requires_caller_transaction")
    try:
        async with connection.transaction():
            return await _apply(connection, command, metadata, namespace, published_plan_store)
    except asyncpg.UniqueViolationError:
        raise RegistryRecordConflict("registry_source_binding_revision_conflict") from None
    except RegistryAddressUnavailable:
        raise
    except asyncpg.PostgresError, ConnectionError, OSError, RuntimeError:
        raise RegistryAddressUnavailable("registry_source_binding_store_unavailable") from None


async def list_network_source_bindings(
    connection,
    network_id,
    *,
    source_system=None,
    binding_key=None,
    limit=50,
    offset=0,
    control_schema=None,
):
    """Return bounded exact stored heads, including explicit closed bindings."""
    if type(network_id) is not int or not 0 < network_id <= 2147483647:
        raise ValueError("registry_network_id_invalid")
    if source_system is not None and (type(source_system) is not str or source_system not in {"aca", "ptg", "fhir"}):
        raise ValueError("registry_source_binding_system_invalid")
    if binding_key is not None and (type(binding_key) is not str or re.fullmatch(r"[0-9a-f]{64}", binding_key) is None):
        raise ValueError("registry_source_binding_key_invalid")
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= 1000000:
        raise ValueError("registry_source_binding_page_invalid")
    namespace = _namespace(control_schema)
    try:
        document = await connection.fetchval(
            f"""WITH page AS MATERIALIZED (
          SELECT binding_id,to_jsonb(head) AS document FROM {namespace}.registry_network_binding head
          WHERE network_id=$1 AND ($2::text IS NULL OR source_system=$2) AND ($3::text IS NULL OR binding_key=$3)
          ORDER BY binding_id LIMIT $4 OFFSET $5
        ) SELECT CASE WHEN COALESCE(sum(octet_length(document::text)),0)+$4*2 <= $6 THEN
          COALESCE(jsonb_agg(document ORDER BY binding_id),'[]'::jsonb)::text END FROM page""",
            network_id,
            source_system,
            binding_key,
            limit,
            offset,
            _MAX_INPUT_BYTES,
        )
    except asyncpg.PostgresError, ConnectionError, OSError, RuntimeError:
        raise RegistryAddressUnavailable("registry_source_binding_store_unavailable") from None
    if document is None:
        raise RegistryAddressUnavailable("registry_source_binding_response_limit")
    return json.loads(document)
