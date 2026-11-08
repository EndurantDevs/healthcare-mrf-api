# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate published native generic snapshots through the original Reader."""

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE, require
from process.network_fhir_source_custody import _require_routine_capabilities
from process.plan_release_storage_catalog import CATALOG_SQL, ROLE_SQL
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_provision import (
    PAYLOAD_ROOTS,
    _native_control_references,
    _require_reader_family,
)
from process.provider_directory_resource_hash import SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT


def published_generic_binding(header, *, dataset_id, endpoint_id, root_run_id, source_id):
    """Check publication metadata only; a valid document grants no native capability."""
    require(header["completion_proof_required_version"] is None)
    binding = storage._json(header["candidate_storage_json"])
    if binding is None:
        return None
    require(type(binding) is dict and binding.get("contract") == storage.CONTRACT)
    identity = binding.get("identity")
    require(type(identity) is dict and type(identity.get("metadata")) is dict)
    storage._validate_binding(binding, identity, phases=("published",))
    require(
        (identity["dataset_id"], identity["endpoint_id"], identity["acquisition_root_run_id"])
        == (dataset_id, endpoint_id, root_run_id)
        and identity["metadata"].get("source_ids") == [source_id]
        and identity["metadata"].get("resource_hash_contract") == SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    )
    return binding


async def _require_source_actors(reader, writer, origin, policy):
    """Use the fixed host policy, excluding Source authority from the retained Writer."""
    require(
        type(policy) is GenericSourcePolicy
        and policy.schema == origin.schema_name
        and origin.admission_profile == SEMANTIC_PROFILE
        and origin.reader_role in policy.runtime_roles
        and origin.reader_role not in policy.control_runtime_roles
        and reader is not writer
        and reader.is_in_transaction()
        and writer.is_in_transaction()
    )
    actor = await reader.fetchrow(
        "SELECT session_user::text AS login,current_user::text AS actor,current_setting('role') AS role,"
        "current_setting('transaction_read_only') AS readonly,current_setting('transaction_isolation') AS isolation"
    )
    require(
        (actor["login"], actor["actor"], actor["role"], actor["readonly"], actor["isolation"])
        == (origin.reader_role, origin.reader_role, "none", "on", "repeatable read")
    )
    roles = await reader.fetch(
        ROLE_SQL,
        [origin.reader_role],
        policy.owner_role,
        policy.publisher_role,
        policy.schema,
    )
    require(len(roles) == 1 and roles[0]["namespace_owner_protected"] is True)
    require(
        await writer.fetchval(
            "SELECT session_user=current_user AND NOT EXISTS(SELECT FROM unnest($1::text[]) source_roles(name) "
            "WHERE pg_has_role(session_user,name,'MEMBER') OR pg_has_role(session_user,name,'USAGE') "
            "OR pg_has_role(session_user,name,'SET'))",
            [policy.owner_role, policy.publisher_role],
        )
        is True
    )


def _require_bound_children(family, binding, origin):
    """Require each published leaf's exact named parent and complete native proof."""
    by_name = {row["relname"]: row for row in family}
    require(by_name[storage.RESOURCE]["oid"] == origin.resource_parent_oid)
    require(binding["tables"][storage.RESOURCE]["oid"] == origin.resource_leaf_oid)
    for root in storage.RELATIONS:
        proof = binding["tables"][root]
        leaf = by_name.get(proof["name"])
        require(
            leaf is not None
            and list(leaf["parents"]) == [by_name[root]["oid"]]
            and storage._table_proof(leaf, private=False) == proof
        )


async def _require_header_binding(reader, origin, binding):
    """Match every immutable acquisition field without exporting raw metadata."""
    require(
        await reader.fetchval(
            f"SELECT d.tableoid::oid::bigint=$2 AND d.endpoint_id=$3 AND d.acquisition_root_run_id=$4 "
            "AND d.dataset_hash=$5 AND d.status IN ('published','superseded') AND d.published_at IS NOT NULL "
            "AND d.candidate_storage_json=$6::jsonb AND NOT EXISTS(SELECT FROM jsonb_each($7::jsonb) acquisition(key,value) "
            "WHERE coalesce(d.publication_metadata_json->acquisition.key,'null'::jsonb) IS DISTINCT FROM acquisition.value) "
            f"FROM {storage_relation(origin.schema_name, storage.HEADER)} d WHERE d.dataset_id=$1",
            origin.dataset_id,
            origin.header_oid,
            origin.endpoint_id,
            origin.root_run_id,
            origin.dataset_sha256,
            storage._encoded(binding),
            storage._encoded(binding["identity"]["metadata"]),
        )
        is True
    )


async def require_native_generic_snapshot(reader, writer, origin, policy, binding):
    """Use the frozen Source factory policy's full ordinary/control role tuple.

    The concrete policy arrives only from the postcommit owning continuation;
    serialized capture, candidate and request documents never select these roles.
    """
    await _require_source_actors(reader, writer, origin, policy)
    require(binding is not None)
    await _require_header_binding(reader, origin, binding)
    await reader.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(policy.schema, binding["tables"][root]["name"]) for root in storage.RELATIONS)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    owner, readers = await storage_roles(reader, policy.schema, **policy.role_arguments())
    family = await reader.fetch(
        CATALOG_SQL,
        [storage_relation(policy.schema, root) for root in PAYLOAD_ROOTS],
        owner,
        readers,
    )
    _require_reader_family(
        family, policy.schema, owner, await _native_control_references(reader, policy.schema, family)
    )
    _require_bound_children(family, binding, origin)
    writer_login = await writer.fetchval("SELECT session_user::text")
    require(type(writer_login) is str and bool(writer_login))
    ordinary_readers = tuple(role for role in policy.runtime_roles if role not in policy.control_runtime_roles)
    await _require_routine_capabilities(reader, (*ordinary_readers, writer_login), policy.schema, owner)
    return [dict(relation_by_field) for relation_by_field in family]
