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
JOIN {entity_evidence} e
  ON e.source_id=:source_id AND e.resource_type='Organization'
 AND e.release_id=:release_id AND e.resource_id=candidate.value
WHERE :resource_type='InsurancePlan' AND (CAST(:network_resource_id AS text) IS NULL OR candidate.value=:network_resource_id)
LIMIT 5001
"""
_VALIDATE_SQL = """
WITH resources AS ({unique}), known_plans AS (
 SELECT * FROM {plan_evidence} UNION ALL SELECT * FROM {global_plan_evidence}
)
SELECT CASE
 WHEN (SELECT count(*) FROM {targets})>5000 THEN 'expansion_limit'
 WHEN :resource_type='InsurancePlan' AND EXISTS (
   SELECT 1 FROM resources r JOIN known_plans p
     ON p.source_id=:source_id AND p.release_id=:release_id
    AND p.insurance_plan_resource_id=r.resource_id
   WHERE p.plan_payload_sha256<>r.payload_sha256 OR p.plan_payload_json<>r.resource_json
      OR p.network_refs IS DISTINCT FROM r.observation_json->'network_refs'
      OR p.owned_by_ref IS DISTINCT FROM r.observation_json->>'owned_by_ref'
      OR p.administered_by_ref IS DISTINCT FROM r.observation_json->>'administered_by_ref'
 ) THEN 'release_plan_conflict'
 WHEN :resource_type='InsurancePlan' AND EXISTS (
   SELECT 1 FROM {targets} t WHERE NOT
     (t.observation_json->'exact_local_target_ids' ? t.network_resource_id)
 ) THEN 'ref_missing'
 WHEN EXISTS (
   SELECT 1 FROM {targets} t LEFT JOIN {entity_binding} b
     ON b.source_id=:source_id AND b.resource_type='Organization'
    AND b.resource_id=t.network_resource_id
   WHERE b.organization_id IS NULL
 ) THEN 'organization_binding_missing'
 WHEN :resource_type='Organization' AND EXISTS (
   SELECT 1 FROM {targets} t LEFT JOIN {entity_evidence} e
     ON e.source_id=:source_id AND e.resource_type='Organization'
    AND e.resource_id=t.network_resource_id AND e.release_id=:release_id
   WHERE e.payload_sha256 IS DISTINCT FROM t.payload_sha256
 ) THEN 'release_evidence_missing'
 ELSE NULL END
"""
_ALLOCATE_SQL = """
CREATE TEMP TABLE {allocations} ON COMMIT DROP AS
SELECT DISTINCT t.network_resource_id,coalesce(incumbent.network_id,gen_random_uuid()) AS network_id,
       coalesce(incumbent.created_at,now()) AS created_at,incumbent.network_id IS NULL AS newly_allocated
FROM (SELECT DISTINCT network_resource_id FROM {targets}) t
LEFT JOIN {network_binding} b
 ON b.source_id=:source_id AND b.resource_id=t.network_resource_id
LEFT JOIN {global_network_binding} incumbent
 ON incumbent.source_id=:source_id AND incumbent.resource_id=t.network_resource_id
WHERE b.network_id IS NULL
"""
_BINDINGS_SQL = """
INSERT INTO {network_binding}
 (source_id,resource_type,resource_id,network_id,created_at)
SELECT :source_id,'Organization',network_resource_id,network_id,created_at FROM {allocations}
"""
_EVIDENCE_SQL = """
WITH added AS (
 INSERT INTO {plan_evidence}
 (source_id,release_id,network_resource_type,network_resource_id,insurance_plan_resource_id,
  network_refs,owned_by_ref,administered_by_ref,plan_payload_sha256,plan_payload_json,observed_at)
 SELECT :source_id,:release_id,'Organization',target.network_resource_id,target.plan_resource_id,
        observation_json->'network_refs',observation_json->>'owned_by_ref',
        observation_json->>'administered_by_ref',payload_sha256,resource_json,coalesce(incumbent.observed_at,now())
 FROM {targets} target
 LEFT JOIN {global_plan_evidence} incumbent ON incumbent.source_id=:source_id AND incumbent.release_id=:release_id
  AND incumbent.network_resource_id=target.network_resource_id AND incumbent.insurance_plan_resource_id=target.plan_resource_id
 WHERE plan_resource_id IS NOT NULL
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
    from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
    from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session

    raw_connection = await connection.get_raw_connection()
    driver = raw_connection.driver_connection
    if driver is not bound_publisher().connection:
        raise RuntimeError("provider_directory_insurance_network_physical_transaction_changed")
    source = io.BytesIO(encoded[0])
    try:
        status = await drain_operation(
            driver.copy_to_table(stage, schema_name="pg_temp", columns=COPY_COLUMNS, source=source, format="binary"),
            preserve_cancellation=True,
        )
    except (asyncio.CancelledError, TimeoutError) as interruption:
        try:
            await close_interrupted_copy(driver)
            await invalidate_copy_owner(source_copy_session(), driver)
        except (Exception, asyncio.CancelledError) as cleanup_error:
            raise interruption from cleanup_error
        raise
    if status != f"COPY {encoded[1]}" or source.tell() != len(encoded[0]):
        raise RegistryAddressUnavailable("provider_directory_insurance_network_copy_unavailable")


def _sql_names(connection, publisher, receipts):
    from db.plan_release_storage import storage_relation
    from process.provider_directory_cms_identity_candidate import CMS_IDENTITY_TABLES

    schema = publisher.policy.schema
    namespace = connection.dialect.identifier_preparer.quote_schema(schema)
    suffix = uuid4().hex
    stage = f"insurance_network_stage_{suffix}"
    names_by_field = dict(
        namespace=namespace,
        stage=stage,
        targets=f"insurance_network_targets_{suffix}",
        allocations=f"insurance_network_ids_{suffix}",
        unique=_UNIQUE.format(stage=stage),
    )
    for key, root in zip(
        ("entity_binding", "entity_evidence", "network_binding", "plan_evidence"), CMS_IDENTITY_TABLES, strict=True
    ):
        names_by_field[key] = storage_relation(schema, receipts[root]["name"])
    names_by_field["global_network_binding"] = storage_relation(schema, CMS_IDENTITY_TABLES[2])
    names_by_field["global_plan_evidence"] = storage_relation(schema, CMS_IDENTITY_TABLES[3])
    return names_by_field


async def _write_batch(connection, encoded, parameters):
    from process.provider_directory_cms_identity_candidate import identity_candidate
    from process.provider_directory_generic_source_policy import source_copy_session

    publisher, identity, receipts = await identity_candidate(source_copy_session())
    if (
        parameters["source_id"] != "cms-npd"
        or parameters["release_id"] != identity["metadata"]["source_release"]["vector_sha256"]
    ):
        raise ValueError("provider_directory_insurance_network_scope_changed")
    names = _sql_names(connection, publisher, receipts)
    await connection.execute(
        text(
            f"CREATE TEMP TABLE {names['stage']} (source_row_ordinal integer PRIMARY KEY,"
            "resource_id text NOT NULL,payload_sha256 text NOT NULL,resource_json jsonb NOT NULL,"
            "observation_json jsonb NOT NULL) ON COMMIT DROP"
        )
    )
    await _copy_stage(connection, names["stage"], encoded)
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
    new_identity_count = await connection.scalar(
        text(f"SELECT count(*) FROM {names['allocations']} WHERE newly_allocated")
    )
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


async def _record_network_batch(
    session: AsyncSession,
    *,
    source_id,
    release_id,
    resource_type,
    resources,
    network_resource_id=None,
) -> dict:
    """Savepoint-atomic evidence only; caller controls transaction and publication."""
    if not isinstance(session, AsyncSession) or not session.in_transaction():
        raise ValueError("provider_directory_insurance_network_transaction_required")
    from process.provider_directory_cms_identity_candidate import identity_candidate

    await identity_candidate(session)
    input_bytes = _envelope(source_id, release_id, resource_type, resources)
    encoded = await drain_operation(asyncio.to_thread(_encode, input_bytes), preserve_cancellation=True)
    interruption = None
    try:
        async with session.begin_nested():
            try:
                connection = await session.connection()
                return await _write_batch(
                    connection,
                    encoded,
                    {
                        "source_id": source_id,
                        "release_id": release_id,
                        "resource_type": resource_type,
                        "network_resource_id": network_resource_id,
                    },
                )
            except (asyncio.CancelledError, TimeoutError) as error:
                interruption = error
                raise
    except SQLAlchemyError, asyncpg.PostgresError, OSError:
        if interruption is not None:
            raise interruption
        raise RegistryAddressUnavailable("provider_directory_insurance_network_store_unavailable") from None


async def record_insurance_network_batch(
    session: AsyncSession, *, source_id, release_id, resource_type, resources
) -> dict:
    """Stage one bounded native batch; no shared/global writer fallback."""
    return await _record_network_batch(
        session, source_id=source_id, release_id=release_id, resource_type=resource_type, resources=resources
    )
