# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native FHIR identity COPY with source-scoped, atomic set writes."""

from __future__ import annotations

import asyncio
import io
import json
from uuid import uuid4

import asyncpg
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ProviderDirectoryInsuranceNetworkSourceBinding
from process.ext.address_fast import _fast_module
from process.provider_directory_rooted_graph_acquisition_worker import drain_operation
from process.registry_record_store import RegistryAddressUnavailable

_MAX_INPUT_BYTES = 32 * 1024 * 1024
_MAX_COPY_BYTES = 64 * 1024 * 1024
_COPY_HEADER = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0"
COPY_COLUMNS = (
    "source_row_ordinal",
    "resource_id",
    "payload_sha256",
    "resource_json",
    "observation_json",
)
_UNIQUE = "SELECT DISTINCT ON (resource_id) * FROM {stage} ORDER BY resource_id,source_row_ordinal"
_TARGETS_SQL = """
CREATE TEMP TABLE {targets} ON COMMIT DROP AS
WITH resources AS ({unique})
SELECT resource_id AS network_resource_id,NULL::text AS plan_resource_id,
       payload_sha256,resource_json,observation_json
FROM resources WHERE :resource_type='Organization'
  AND (observation_json->>'network_role')::boolean
UNION ALL
SELECT candidate.value,r.resource_id,r.payload_sha256,r.resource_json,r.observation_json
FROM resources r CROSS JOIN LATERAL
  jsonb_array_elements_text(r.observation_json->'candidate_local_target_ids') candidate
JOIN {namespace}.provider_directory_entity_release_evidence e
  ON e.source_id=:source_id AND e.resource_type='Organization'
 AND e.release_id=:release_id AND e.resource_id=candidate.value
WHERE :resource_type='InsurancePlan'
LIMIT 5001
"""
_VALIDATE_SQL = """
WITH resources AS ({unique})
SELECT CASE
 WHEN (SELECT count(*) FROM {targets})>5000 THEN 'expansion_limit'
 WHEN :resource_type='InsurancePlan' AND EXISTS (
   SELECT 1 FROM resources r JOIN {namespace}.provider_directory_insurance_network_plan_evidence p
     ON p.source_id=:source_id AND p.release_id=:release_id
    AND p.insurance_plan_resource_id=r.resource_id
   WHERE p.plan_payload_sha256<>r.payload_sha256
 ) THEN 'release_plan_conflict'
 WHEN :resource_type='InsurancePlan' AND EXISTS (
   SELECT 1 FROM {targets} t WHERE NOT
     (t.observation_json->'exact_local_target_ids' ? t.network_resource_id)
 ) THEN 'ref_missing'
 WHEN EXISTS (
   SELECT 1 FROM {targets} t LEFT JOIN {namespace}.provider_directory_entity_source_binding b
     ON b.source_id=:source_id AND b.resource_type='Organization'
    AND b.resource_id=t.network_resource_id
   WHERE b.organization_id IS NULL
 ) THEN 'organization_binding_missing'
 WHEN :resource_type='Organization' AND EXISTS (
   SELECT 1 FROM {targets} t LEFT JOIN {namespace}.provider_directory_entity_release_evidence e
     ON e.source_id=:source_id AND e.resource_type='Organization'
    AND e.resource_id=t.network_resource_id AND e.release_id=:release_id
   WHERE e.payload_sha256 IS DISTINCT FROM t.payload_sha256
 ) THEN 'release_evidence_missing'
 ELSE NULL END
"""
_ALLOCATE_SQL = """
CREATE TEMP TABLE {allocations} ON COMMIT DROP AS
SELECT DISTINCT t.network_resource_id,gen_random_uuid() AS network_id
FROM (SELECT DISTINCT network_resource_id FROM {targets}) t
LEFT JOIN {namespace}.provider_directory_insurance_network_source_binding b
 ON b.source_id=:source_id AND b.resource_id=t.network_resource_id
WHERE b.network_id IS NULL
"""
_IDENTITIES_SQL = """
WITH added AS (
 INSERT INTO {namespace}.provider_directory_insurance_network_identity
 SELECT network_id,now() FROM {allocations} RETURNING 1
) SELECT count(*) FROM added
"""
_BINDINGS_SQL = """
INSERT INTO {namespace}.provider_directory_insurance_network_source_binding
 (source_id,resource_type,resource_id,network_id,created_at)
SELECT :source_id,'Organization',network_resource_id,network_id,now() FROM {allocations}
"""
_EVIDENCE_SQL = """
WITH added AS (
 INSERT INTO {namespace}.provider_directory_insurance_network_plan_evidence
 (source_id,release_id,network_resource_type,network_resource_id,insurance_plan_resource_id,
  network_refs,owned_by_ref,administered_by_ref,plan_payload_sha256,plan_payload_json,observed_at)
 SELECT :source_id,:release_id,'Organization',network_resource_id,plan_resource_id,
        observation_json->'network_refs',observation_json->>'owned_by_ref',
        observation_json->>'administered_by_ref',payload_sha256,resource_json,now()
 FROM {targets} WHERE plan_resource_id IS NOT NULL
 ON CONFLICT DO NOTHING RETURNING 1
) SELECT count(*) FROM added
"""


def _encode(input_bytes):
    native = _fast_module()
    encoder = getattr(native, "encode_fhir_network_identity_batch", None)
    if not callable(encoder):
        raise RegistryAddressUnavailable("provider_directory_insurance_network_native_unavailable")
    try:
        encoded = encoder(input_bytes)
    except ValueError:
        raise ValueError("provider_directory_insurance_network_batch_invalid") from None
    except AttributeError, RuntimeError, TypeError:
        raise RegistryAddressUnavailable("provider_directory_insurance_network_native_unavailable") from None
    if type(encoded) is not tuple or len(encoded) != 4:
        raise RegistryAddressUnavailable("provider_directory_insurance_network_native_unavailable")
    copy_bytes, row_count, input_count, duplicate_count = encoded
    if (
        type(copy_bytes) is not bytes
        or not 21 <= len(copy_bytes) <= _MAX_COPY_BYTES
        or not copy_bytes.startswith(_COPY_HEADER)
        or not copy_bytes.endswith(b"\xff\xff")
        or type(row_count) is not int
        or type(input_count) is not int
        or type(duplicate_count) is not int
        or not 1 <= input_count <= 1000
        or row_count != input_count
        or not 0 <= duplicate_count < input_count
    ):
        raise RegistryAddressUnavailable("provider_directory_insurance_network_native_unavailable")
    return encoded


def _envelope(source_id, release_id, resource_type, resources):
    if type(resources) is not list or not 1 <= len(resources) <= 1000:
        raise ValueError("provider_directory_insurance_network_batch_invalid")
    try:
        encoded = json.dumps(
            {"source_id": source_id, "release_id": release_id, "resource_type": resource_type, "resources": resources},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except TypeError, ValueError, UnicodeError, RecursionError:
        raise ValueError("provider_directory_insurance_network_batch_invalid") from None
    if len(encoded) > _MAX_INPUT_BYTES:
        raise ValueError("provider_directory_insurance_network_batch_invalid")
    return encoded


async def _copy_stage(connection, stage, encoded):
    raw_connection = await connection.get_raw_connection()
    driver = raw_connection.driver_connection
    source = io.BytesIO(encoded[0])
    status = await drain_operation(
        driver.copy_to_table(stage, schema_name="pg_temp", columns=COPY_COLUMNS, source=source, format="binary"),
        preserve_cancellation=True,
    )
    if status != f"COPY {encoded[1]}" or source.tell() != len(encoded[0]):
        raise RegistryAddressUnavailable("provider_directory_insurance_network_copy_unavailable")


def _sql_names(connection):
    schema = ProviderDirectoryInsuranceNetworkSourceBinding.__table__.schema
    mapping = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = mapping.get(schema, schema)
    if not isinstance(schema, str) or not schema:
        raise ValueError("provider_directory_insurance_network_schema_invalid")
    namespace = connection.dialect.identifier_preparer.quote_schema(schema)
    suffix = uuid4().hex
    stage = f"insurance_network_stage_{suffix}"
    targets = f"insurance_network_targets_{suffix}"
    allocations = f"insurance_network_ids_{suffix}"
    return {
        "namespace": namespace,
        "stage": stage,
        "targets": targets,
        "allocations": allocations,
        "unique": _UNIQUE.format(stage=stage),
    }


async def _write_batch(connection, encoded, parameters):
    names = _sql_names(connection)
    await connection.execute(
        text(
            f"CREATE TEMP TABLE {names['stage']} (source_row_ordinal integer PRIMARY KEY,"
            "resource_id text NOT NULL,payload_sha256 text NOT NULL,resource_json jsonb NOT NULL,"
            "observation_json jsonb NOT NULL) ON COMMIT DROP"
        )
    )
    await _copy_stage(connection, names["stage"], encoded)
    # ponytail: one source lock; partition only with equivalent cross-batch conflict fences.
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key,0))"),
        {"lock_key": "provider-directory-insurance-network-batch:" + parameters["source_id"]},
    )
    await connection.execute(text(_TARGETS_SQL.format(**names)), parameters)
    error = await connection.scalar(text(_VALIDATE_SQL.format(**names)), parameters)
    if error is not None:
        raise ValueError("provider_directory_insurance_network_" + error)
    counts = (
        await connection.execute(
            text(
                f"SELECT count(*) FILTER (WHERE plan_resource_id IS NULL),"
                f"count(*) FILTER (WHERE plan_resource_id IS NOT NULL) FROM {names['targets']}"
            )
        )
    ).one()
    await connection.execute(text(_ALLOCATE_SQL.format(**names)), parameters)
    new_identity_count = await connection.scalar(text(_IDENTITIES_SQL.format(**names)), parameters)
    await connection.execute(text(_BINDINGS_SQL.format(**names)), parameters)
    new_evidence_count = await connection.scalar(text(_EVIDENCE_SQL.format(**names)), parameters)
    await connection.execute(text(f"DROP TABLE {names['stage']},{names['targets']},{names['allocations']}"))
    return {
        "input_count": encoded[2],
        "duplicate_count": encoded[3],
        "organization_count": counts[0],
        "plan_link_count": counts[1],
        "new_identity_count": new_identity_count,
        "new_plan_evidence_count": new_evidence_count,
    }


async def record_insurance_network_batch(
    session: AsyncSession,
    *,
    source_id,
    release_id,
    resource_type,
    resources,
) -> dict:
    """Savepoint-atomic evidence only; caller controls transaction and publication."""
    if not isinstance(session, AsyncSession) or not session.in_transaction():
        raise ValueError("provider_directory_insurance_network_transaction_required")
    input_bytes = _envelope(source_id, release_id, resource_type, resources)
    encoded = await drain_operation(asyncio.to_thread(_encode, input_bytes), preserve_cancellation=True)
    try:
        async with session.begin_nested():
            connection = await session.connection()
            return await _write_batch(
                connection,
                encoded,
                {
                    "source_id": source_id,
                    "release_id": release_id,
                    "resource_type": resource_type,
                },
            )
    except SQLAlchemyError, asyncpg.PostgresError, OSError:
        raise RegistryAddressUnavailable("provider_directory_insurance_network_store_unavailable") from None
