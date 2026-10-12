# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded explicit provider mutations and current identity references."""

import hashlib
import json
from uuid import UUID, uuid4

import asyncpg

from db.models import NPIData
from process.provider_directory_projection_fhir_values import is_valid_npi
from process.registry_record_store import (
    RegistryRecordCommand,
    RegistryRecordConflict,
    _command_request,
    _uuid,
    _validated_actor,
    _validated_command,
)
from process.registry_source_observation_store import _namespace

MAX_BATCH_BYTES = 1048576
_COPY_COLUMNS = (
    "ordinal",
    "row_kind",
    "provider_id",
    "provider_system",
    "reference_key",
    "operation",
    "expected_revision",
    "fields_json",
    "reason",
    "idempotency_key",
    "request_sha256",
)


def _json(document):
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _reference_pair(reference):
    if type(reference) is not dict or set(reference) != {"provider_system", "provider_id"}:
        raise ValueError("registry_provider_reference_invalid")
    system, identity = reference["provider_system"], reference["provider_id"]
    if type(system) is not str or type(identity) is not str:
        raise ValueError("registry_provider_reference_invalid")
    if system == "manual":
        try:
            parsed = _uuid(UUID(identity))
        except ValueError, TypeError, AttributeError:
            raise ValueError("registry_provider_reference_invalid") from None
        if str(parsed) != identity:
            raise ValueError("registry_provider_reference_invalid")
    elif (
        system != "npi"
        or not identity.isascii()
        or len(identity) != 10
        or identity[0] not in "12"
        or not is_valid_npi(identity)
    ):
        raise ValueError("registry_provider_reference_invalid")
    return system, identity


def _prepared_rows(commands, references, actor):
    """Freeze validated fields, existing command digests and reference order before awaits."""
    if type(commands) is not list or type(references) is not list or not 1 <= len(commands) + len(references) <= 100:
        raise ValueError("registry_provider_batch_bounds_invalid")
    actor_by_field = _validated_actor(actor)
    staged_rows, identities = [], set()
    input_bytes = len(_json({"commands": [], "references": []}).encode())
    input_bytes += max(0, len(commands) - 1) + max(0, len(references) - 1)
    for ordinal, command in enumerate(commands):
        if not isinstance(command, RegistryRecordCommand) or command.record_kind != "provider":
            raise ValueError("registry_provider_batch_command_invalid")
        fields = _validated_command(command)
        if command._undo_provenance_required:
            raise ValueError("registry_manual_undo_provenance_required")
        if command.record_id in identities:
            raise ValueError("registry_provider_batch_duplicate_command")
        identities.add(command.record_id)
        request = _command_request(command, actor_by_field)
        input_bytes += len(_json(json.loads(request)["command"]).encode())
        staged_rows.append(
            (
                ordinal,
                "command",
                command.record_id,
                None,
                None,
                command.operation,
                command.expected_revision,
                _json(fields),
                command.reason.strip(),
                command.idempotency_key,
                hashlib.sha256(request.encode()).hexdigest(),
            )
        )
    for ordinal, reference in enumerate(references, len(commands)):
        system, identity = _reference_pair(reference)
        input_bytes += len(_json(reference).encode())
        staged_rows.append(
            (
                ordinal,
                "reference",
                UUID(identity) if system == "manual" else None,
                system,
                identity,
                None,
                None,
                "{}",
                None,
                None,
                None,
            )
        )
    if input_bytes > MAX_BATCH_BYTES:
        raise ValueError("registry_provider_batch_input_limit")
    return tuple(staged_rows), _json(actor_by_field)


async def _stage(connection, stage_name, staged_rows):
    stage = f'pg_temp."{stage_name}"'
    await connection.execute(f"""CREATE TEMP TABLE {stage} (
      ordinal integer PRIMARY KEY,row_kind text NOT NULL,provider_id uuid,provider_system text,
      reference_key text,operation text,expected_revision bigint,fields_json jsonb NOT NULL,
      reason text,idempotency_key text,request_sha256 text) ON COMMIT DROP""")
    status = await connection.copy_records_to_table(
        stage_name, schema_name="pg_temp", columns=_COPY_COLUMNS, records=staged_rows
    )
    if status != f"COPY {len(staged_rows)}":
        raise ValueError("registry_provider_batch_copy_count_invalid")
    return stage


_VALIDATION_SQL = """WITH checked AS MATERIALIZED (
 SELECT target.*,head.revision AS head_revision,head.npi AS head_npi,head.archived,
   replay.revision AS replay_revision,replay.request_sha256 AS replay_digest,replay.event_json,
   CASE WHEN target.operation='restore' THEN head.npi
     WHEN target.operation IN ('create','correct') THEN target.fields_json->>'npi' END AS claimed_npi
 FROM {stage} target LEFT JOIN {namespace}.manual_provider_registry head USING(provider_id)
 LEFT JOIN {namespace}.registry_record_history replay ON target.row_kind='command'
   AND replay.record_kind='provider' AND replay.record_key=target.provider_id::text
   AND replay.idempotency_key=target.idempotency_key
), new_commands AS MATERIALIZED (
 SELECT * FROM checked WHERE row_kind='command' AND replay_revision IS NULL
)
SELECT
 NOT EXISTS(SELECT 1 FROM checked WHERE row_kind='command' AND replay_revision IS NOT NULL
   AND (replay_digest<>request_sha256 OR (event_json IS NOT NULL AND event_json->'operation_context'<>'{{}}'::jsonb))) AS replays_valid,
 NOT EXISTS(SELECT 1 FROM new_commands WHERE NOT
   ((operation='create' AND head_revision IS NULL) OR
    (operation<>'create' AND head_revision=expected_revision)) OR
   (operation<>'create' AND head_revision IS NULL)) AS revisions_valid,
 NOT EXISTS(SELECT 1 FROM checked reference WHERE row_kind='reference' AND NOT
   ((provider_system='manual' AND head_revision IS NOT NULL AND NOT archived
     AND NOT EXISTS(SELECT 1 FROM new_commands planned WHERE planned.provider_id=reference.provider_id
       AND planned.operation='archive')) OR
    (provider_system='npi' AND EXISTS(SELECT 1 FROM {source}.npi npi WHERE npi.npi=CASE WHEN reference.provider_system='npi' THEN reference.reference_key::bigint END)))) AS references_valid,
 NOT EXISTS(SELECT 1 FROM new_commands command JOIN {source}.npi npi ON npi.npi=command.claimed_npi::bigint) AS source_claims_valid,
 NOT EXISTS(SELECT 1 FROM new_commands command JOIN {namespace}.manual_provider_registry other
   ON other.npi=command.claimed_npi AND other.provider_id<>command.provider_id)
 AND NOT EXISTS(SELECT 1 FROM new_commands WHERE claimed_npi IS NOT NULL GROUP BY claimed_npi HAVING count(*)>1) AS manual_claims_valid,
 (SELECT count(*) FROM new_commands) AS mutation_count"""

_WRITE_SQL = """WITH changed AS (
 INSERT INTO {namespace}.manual_provider_registry AS current
   (provider_id,display_name,provider_kind,aliases,npi,archived,revision)
 SELECT target.provider_id,
   CASE WHEN target.operation IN ('create','correct') THEN target.fields_json->>'display_name' ELSE head.display_name END,
   CASE WHEN target.operation IN ('create','correct') THEN target.fields_json->>'provider_kind' ELSE head.provider_kind END,
   CASE WHEN target.operation IN ('create','correct') THEN ARRAY(SELECT jsonb_array_elements_text(target.fields_json->'aliases')) ELSE head.aliases END,
   CASE WHEN target.operation IN ('create','correct') THEN target.fields_json->>'npi' ELSE head.npi END,
   CASE target.operation WHEN 'archive' THEN true WHEN 'restore' THEN false WHEN 'create' THEN false ELSE head.archived END,
   target.expected_revision+1
 FROM {stage} target LEFT JOIN {namespace}.manual_provider_registry head USING(provider_id)
 WHERE target.row_kind='command'
   AND (target.operation='create' OR head.revision=target.expected_revision)
   AND NOT EXISTS(SELECT 1 FROM {namespace}.registry_record_history replay WHERE replay.record_kind='provider'
     AND replay.record_key=target.provider_id::text AND replay.idempotency_key=target.idempotency_key)
 ON CONFLICT(provider_id) DO UPDATE SET display_name=excluded.display_name,provider_kind=excluded.provider_kind,
   aliases=excluded.aliases,npi=excluded.npi,archived=excluded.archived,revision=excluded.revision
 WHERE current.revision=excluded.revision-1 RETURNING current.*
), history AS (
 INSERT INTO {namespace}.registry_record_history(record_kind,record_key,revision,custom_revision,record_json,
   actor_json,reason,idempotency_key,request_sha256,event_json)
 SELECT 'provider',changed.provider_id::text,changed.revision,$1,to_jsonb(changed),$2::jsonb,
   target.reason,target.idempotency_key,target.request_sha256,
   jsonb_build_object('operation',target.operation,'before_revision',target.expected_revision,'operation_context','{{}}'::jsonb)
 FROM changed JOIN {stage} target USING(provider_id) WHERE target.row_kind='command' RETURNING revision
) SELECT count(*) FROM history"""

_RECEIPT_SQL = """WITH document AS (
 SELECT jsonb_build_object('records',coalesce((SELECT jsonb_agg(jsonb_build_object(
   'record_kind','provider','record_id',history.record_key,'revision',history.revision,
   'custom_revision',history.custom_revision,'record',history.record_json) ORDER BY target.ordinal)
   FROM {stage} target JOIN {namespace}.registry_record_history history ON history.record_kind='provider'
     AND history.record_key=target.provider_id::text AND history.idempotency_key=target.idempotency_key
   WHERE target.row_kind='command'),'[]'::jsonb),
   'references',coalesce((SELECT jsonb_agg(jsonb_build_object('provider_system',provider_system,
     'provider_id',reference_key) ORDER BY ordinal) FROM {stage} WHERE row_kind='reference'),'[]'::jsonb)) AS body
) SELECT CASE WHEN octet_length(body::text)<=$1 THEN body::text END FROM document"""


def _check_validation(status):
    if not status["replays_valid"]:
        raise RegistryRecordConflict("registry_record_idempotency_conflict")
    if not status["references_valid"]:
        raise RegistryRecordConflict("registry_provider_reference_unavailable")
    if not status["revisions_valid"]:
        raise RegistryRecordConflict("registry_record_revision_conflict")
    if not status["source_claims_valid"]:
        raise RegistryRecordConflict("registry_provider_npi_reuse_required")
    if not status["manual_claims_valid"]:
        raise RegistryRecordConflict("registry_provider_npi_claimed")


async def _write_batch(connection, namespace, stage, status, draft_revision, actor_json):
    if not status["mutation_count"]:
        return
    if draft_revision >= 9223372036854775807:
        raise RegistryRecordConflict("registry_provider_batch_revision_exhausted")
    try:
        written = await connection.fetchval(
            _WRITE_SQL.format(namespace=namespace, stage=stage), draft_revision + 1, actor_json
        )
    except asyncpg.UniqueViolationError as error:
        if error.constraint_name == "manual_provider_npi_unique":
            raise RegistryRecordConflict("registry_provider_npi_claimed") from None
        raise
    if written != status["mutation_count"]:
        raise RegistryRecordConflict("registry_record_revision_conflict")
    await connection.execute(
        f"UPDATE {namespace}.registry_revision_control SET draft_revision=$1 WHERE id=1", draft_revision + 1
    )


async def apply_registry_provider_batch(
    connection, commands, references, actor, *, control_schema=None, source_schema=None
):
    """Apply bounded commands and read current references in the caller's pinned transaction.

    References must already be active and create no head or history. The caller
    authenticates the actor and owns the outer transaction; one savepoint keeps
    all mutations atomic while preserving earlier caller work and exact retries.
    """
    staged_rows, actor_json = _prepared_rows(commands, references, actor)
    namespace = _namespace(control_schema)
    source = _namespace(source_schema if source_schema is not None else NPIData.__table__.schema)
    if not connection.is_in_transaction():
        raise ValueError("registry_requires_caller_transaction")
    async with connection.transaction():
        control = await connection.fetchrow(
            f"SELECT draft_revision FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
        )
        if control is None:
            raise RegistryRecordConflict("registry_control_unavailable")
        stage = await _stage(connection, "registry_provider_batch_" + uuid4().hex, staged_rows)
        status = await connection.fetchrow(_VALIDATION_SQL.format(namespace=namespace, source=source, stage=stage))
        _check_validation(status)
        await _write_batch(connection, namespace, stage, status, control["draft_revision"], actor_json)
        encoded = await connection.fetchval(_RECEIPT_SQL.format(namespace=namespace, stage=stage), MAX_BATCH_BYTES)
        if encoded is None:
            raise ValueError("registry_provider_batch_output_limit")
        await connection.execute(f"DROP TABLE {stage}")
        return json.loads(encoded)
