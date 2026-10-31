# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare deterministic resource identities without mutating accepted mappings."""

import asyncio
import json
import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone

import asyncpg

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_cms_identity_candidate import (
    CMSIdentityReprepareRequired,
    _canonical_schema,
    copy_identity_rows,
)
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session

RESOURCE_IDENTITY_ROOT = "provider_directory_resource_identity"
ROOT = RESOURCE_IDENTITY_ROOT
RESOURCE_IDENTITY_COLUMNS = ("source_id", "resource_type", "resource_id", "entity_id", "created_at")
_ACTIVE = ContextVar("resource_identity_candidate", default=None)


def _require(condition, code):
    if not condition:
        raise GenericCandidateError("provider_directory_resource_identity_candidate_" + code)


def _stage_name(receipt):
    _require(
        type(receipt) is dict
        and set(receipt) == {"name", "oid", "owner", "layout_sha256"}
        and type(receipt["name"]) is str
        and re.fullmatch(r"pdgs_[0-9a-f]{32}_re", receipt["name"]) is not None
        and type(receipt["oid"]) is int
        and receipt["oid"] > 0
        and type(receipt["owner"]) is int
        and receipt["owner"] > 0
        and type(receipt["layout_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", receipt["layout_sha256"]) is not None,
        "stage_receipt_changed",
    )
    return receipt["name"]


def _source_release(publisher, identity):
    metadata = identity.get("metadata", {})
    release = metadata.get("source_release", {})
    producer = metadata.get("network_bindings", {})
    _require(
        metadata.get("source_ids") in (["cms-npd"], ("cms-npd",))
        and release.get("source_id") == "cms-npd"
        and type(release.get("vector_sha256")) is str
        and re.fullmatch(r"[0-9a-f]{64}", release["vector_sha256"]) is not None
        and producer.get("source_system") == "fhir"
        and producer.get("source_id") == "cms-npd"
        and producer.get("dataset_schema") == _canonical_schema(publisher.policy)
        and producer.get("dataset_id") == identity.get("dataset_id")
        and producer.get("edition_id") == identity.get("dataset_id")
        and producer.get("producer_id") == "cms-npd-bulk-fhir-v1"
        and producer.get("alias_scope") == "cms-npd"
        and producer.get("source_key_kind") == "organization_resource_id",
        "source_identity_changed",
    )
    return release["vector_sha256"]


def _publisher_transaction(publisher):
    _require(publisher is bound_publisher(), "publisher_changed")
    source_copy_session()
    _require(publisher.connection.is_in_transaction() is True, "transaction_required")


async def _protected_stage(publisher, identity, receipt, phases):
    _publisher_transaction(publisher)
    _stage_name(receipt)
    _source_release(publisher, identity)
    raw = await publisher.connection.fetchval(
        f"SELECT candidate_storage_json FROM {storage_relation(publisher.policy.schema, 'provider_directory_endpoint_dataset')} "
        "WHERE dataset_id=$1 FOR UPDATE",
        identity["dataset_id"],
    )
    binding = json.loads(raw) if isinstance(raw, str) else raw
    from process.provider_directory_generic_source_storage import _validate_binding

    _validate_binding(binding, identity, phases=("prepared",))
    auxiliary = binding.get("auxiliary", {}) if type(binding) is dict else {}
    _require(
        type(binding) is dict
        and binding.get("identity") == identity
        and auxiliary.get("phase") in phases
        and auxiliary.get("tables", {}).get(RESOURCE_IDENTITY_ROOT) == receipt,
        "protected_binding_changed",
    )
    return binding


@dataclass(frozen=True)
class _ResourceScope:
    publisher: object
    identity: dict
    receipt: dict
    task: object


@asynccontextmanager
async def bind_resource_identity_candidate(publisher, full_identity, receipt):
    """Bind the declared fifth stage to the genuine same-task COPY session."""
    _publisher_transaction(publisher)
    _stage_name(receipt)
    _source_release(publisher, full_identity)
    _require(_ACTIVE.get() is None, "scope_already_bound")
    token = _ACTIVE.set(_ResourceScope(publisher, full_identity, receipt, asyncio.current_task()))
    try:
        yield
    finally:
        _ACTIVE.reset(token)


async def stage_resource_identities(session, *, source_id, resource_type, identities_by_resource_id):
    """COPY one validated batch to the private heap; no accepted-table observation."""
    from process.provider_directory_resource_identity import source_resource_uuid

    scope = _ACTIVE.get()
    _require(
        scope is not None and scope.task is asyncio.current_task() and session is source_copy_session(),
        "physical_session_required",
    )
    _require(
        source_id == "cms-npd"
        and type(identities_by_resource_id) is dict
        and 0 < len(identities_by_resource_id) <= 1000,
        "batch_scope_changed",
    )
    _require(
        all(
            entity == source_resource_uuid(source_id, resource_type, key)
            for key, entity in identities_by_resource_id.items()
        ),
        "identity_changed",
    )
    await _protected_stage(scope.publisher, scope.identity, scope.receipt, ("loading",))
    now = datetime.now(timezone.utc)
    rows = [(source_id, resource_type, key, entity, now) for key, entity in identities_by_resource_id.items()]
    await copy_identity_rows(scope.publisher, _stage_name(scope.receipt), RESOURCE_IDENTITY_COLUMNS, rows)


async def _verify_raw_identities(connection, relation):
    """One bounded cursor preserves the public UUID5 encoder without SQL extensions."""
    from process.provider_directory_resource_identity import source_resource_uuid

    query = f"SELECT DISTINCT source_id,resource_type,resource_id,entity_id FROM {relation}"
    async for row in connection.cursor(query, prefetch=512):
        _require(
            row["entity_id"] == source_resource_uuid(row["source_id"], row["resource_type"], row["resource_id"]),
            "identity_changed",
        )


async def _complete_source(connection, publisher, identity, binding, receipt):
    stage = storage_relation(publisher.policy.schema, _stage_name(receipt))
    witness_receipt = binding.get("tables", {}).get("provider_directory_cms_npd_resource_witness", {})
    expected = _stage_name(receipt).removesuffix("_re") + "_w"
    _require(witness_receipt.get("name") == expected, "witness_changed")
    witness = storage_relation(publisher.policy.schema, expected)
    await verify_resource_identity_source(
        connection, stage, witness, identity["dataset_id"], _source_release(publisher, identity)
    )


async def verify_resource_identity_source(connection, stage, witness, dataset_id, release_id):
    """Compare complete raw-resource coverage on caller-qualified coordinates."""
    invalid = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {stage} WHERE source_id<>'cms-npd' "
        "OR resource_type NOT IN ('InsurancePlan','PractitionerRole') "
        "OR resource_id !~ '^[A-Za-z0-9.-]{1,64}$') OR EXISTS("
        f"SELECT FROM {stage} GROUP BY source_id,resource_type,resource_id HAVING count(DISTINCT entity_id)<>1) "
        f"OR EXISTS(SELECT FROM {stage} c LEFT JOIN {witness} w ON w.dataset_id=$1 AND w.source_id=c.source_id "
        "AND w.resource_type=c.resource_type AND w.resource_id=c.resource_id "
        "WHERE w.resource_id IS NULL OR w.release_id<>$2) OR EXISTS("
        f"SELECT FROM {witness} w WHERE w.dataset_id=$1 AND w.resource_type IN ('InsurancePlan','PractitionerRole') "
        f"AND (w.source_id<>'cms-npd' OR w.release_id<>$2 OR NOT EXISTS(SELECT FROM {stage} c "
        "WHERE (c.source_id,c.resource_type,c.resource_id)=(w.source_id,w.resource_type,w.resource_id))))",
        dataset_id,
        release_id,
    )
    _require(invalid is False, "source_evidence_incomplete")


async def _global_replay(connection, schema, relation, *, complete=False):
    root = storage_relation(schema, RESOURCE_IDENTITY_ROOT)
    join, missing = ("LEFT JOIN", "incumbent.source_id IS NULL OR ") if complete else ("JOIN", "")
    invalid = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {relation} candidate {join} {root} incumbent "
        "USING(source_id,resource_type,resource_id) "
        f"WHERE {missing}candidate.entity_id IS DISTINCT FROM incumbent.entity_id)"
    )
    if invalid is not False:
        raise CMSIdentityReprepareRequired("provider_directory_resource_identity_immutable_replay_conflict")


async def finish_resource_identity_stage(publisher, identity, receipt):
    """Reduce raw duplicates, finish native indexes and validate the complete family."""
    from process.provider_directory_generic_source_storage import _close_grants

    connection, schema = publisher.connection, publisher.policy.schema
    canonical_schema = _canonical_schema(publisher.policy)
    binding = await _protected_stage(publisher, identity, receipt, ("loading",))
    name = _stage_name(receipt)
    raw, reduced = storage_relation(schema, name), storage_relation(schema, name + "_u")
    async with connection.transaction():
        await _verify_raw_identities(connection, raw)
        await _global_replay(connection, canonical_schema, raw)
        await connection.execute(
            f"CREATE TABLE {reduced} (LIKE {storage_relation(canonical_schema, RESOURCE_IDENTITY_ROOT)} "
            "INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
        )
        await _close_grants(connection, publisher.policy, name + "_u")
        await connection.execute(
            f"INSERT INTO {reduced} SELECT candidate.source_id,candidate.resource_type,candidate.resource_id,"
            "candidate.entity_id,COALESCE(incumbent.created_at,candidate.created_at) "
            f"FROM (SELECT source_id,resource_type,resource_id,entity_id,min(created_at) AS created_at FROM {raw} "
            "GROUP BY source_id,resource_type,resource_id,entity_id) candidate "
            f"LEFT JOIN {storage_relation(canonical_schema, RESOURCE_IDENTITY_ROOT)} incumbent USING(source_id,resource_type,resource_id)"
        )
        await connection.execute(f"ALTER TABLE {reduced} ADD PRIMARY KEY(source_id,resource_type,resource_id)")
        await connection.execute(
            f"CREATE UNIQUE INDEX {storage_identifier(name + '_seek')} ON {reduced}(source_id,resource_type,entity_id)"
        )
        await connection.execute(f"ANALYZE {reduced}")
        await connection.execute(f"DROP TABLE {raw}")
        await connection.execute(f"ALTER TABLE {reduced} RENAME TO {storage_identifier(name)}")
        await _complete_source(connection, publisher, identity, binding, receipt)


async def publish_resource_identity_relation(connection, relation, canonical_schema):
    """Merge a caller-qualified resource map; its caller owns authority and rollback.

    Preserve the existing Source UUID replay contract. Prepared CMS publication
    additionally verifies exact timestamps and reverse-key collisions outside it.
    """
    _require(connection.is_in_transaction() is True, "transaction_required")
    fields = ",".join(RESOURCE_IDENTITY_COLUMNS)
    await _global_replay(connection, canonical_schema, relation)
    await connection.execute(
        f"INSERT INTO {storage_relation(canonical_schema, RESOURCE_IDENTITY_ROOT)}({fields}) "
        f"SELECT {fields} FROM {relation} ORDER BY source_id,resource_type,resource_id "
        "ON CONFLICT(source_id,resource_type,resource_id) DO NOTHING"
    )
    from process.provider_directory_generic_source_storage import refresh_statistics

    await refresh_statistics(connection, canonical_schema, (RESOURCE_IDENTITY_ROOT,))
    await _global_replay(connection, canonical_schema, relation, complete=True)


async def publish_resource_identity_stage(publisher, identity, receipt):
    """Publish set-wise, retaining accepted timestamps and rejecting immutable drift."""
    connection, schema = publisher.connection, publisher.policy.schema
    canonical_schema = _canonical_schema(publisher.policy)
    await _protected_stage(publisher, identity, receipt, ("sealed",))
    relation = storage_relation(schema, _stage_name(receipt))
    try:
        async with connection.transaction():
            await publish_resource_identity_relation(connection, relation, canonical_schema)
    except (asyncpg.SerializationError, asyncpg.UniqueViolationError) as error:
        raise CMSIdentityReprepareRequired(
            "provider_directory_resource_identity_whole_candidate_reprepare_required"
        ) from error
