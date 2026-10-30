# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close metadata candidate writers and verify exact retained native identities."""

import json
from dataclasses import dataclass
from uuid import UUID

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import TABLES
from process.network_membership_writer_closure import (
    transfer_and_grant_writer_closure,
    verify_writer_closure,
    verify_writer_closure_identity,
    verify_writer_roles,
    writer_closure_receipt,
    writer_role_names,
)
from process.registry_fhir_catalog_validation import validate_fhir_catalog_candidate, verify_fhir_catalog_validation

PHYSICAL_TABLES = frozenset(set(TABLES) - {"registry_fhir_catalog_snapshot"})


@dataclass(frozen=True)
class FHIRCatalogTarget:
    dataset_id: str
    schema_id: str
    producer_id: str
    candidate_id: str
    schema_name: str

    def __post_init__(self):
        for value in (self.dataset_id, self.schema_id, self.producer_id, self.candidate_id):
            if type(value) is not str or not UUID(value).int or str(UUID(value)) != value:
                raise ValueError("registry_fhir_catalog_target_invalid")
        if self.schema_name != "fhir_catalog_" + UUID(self.candidate_id).hex:
            raise ValueError("registry_fhir_catalog_target_invalid")


async def _catalog(connection, copy_target):
    namespace = await connection.fetchrow(
        "SELECT oid::bigint,nspowner::bigint FROM pg_namespace WHERE nspname=$1", copy_target.schema_name
    )
    if namespace is None:
        raise ValueError("registry_fhir_catalog_custody_invalid")
    relations = await connection.fetch(
        "SELECT c.oid::bigint,c.relname,c.relkind,c.relpersistence,c.relowner::bigint,c.relacl,"
        "i.indrelid::bigint FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid "
        "WHERE c.relnamespace=$1::oid ORDER BY c.relname LIMIT 129",
        namespace["oid"],
    )
    heap_oid_by_name = {relation["relname"]: relation["oid"] for relation in relations if relation["relkind"] == b"r"}
    if (
        set(heap_oid_by_name) != PHYSICAL_TABLES
        or len(relations) > 128
        or any(
            relation["relkind"] not in (b"r", b"i")
            or relation["relpersistence"] != b"p"
            or relation["relkind"] == b"i"
            and (relation["indrelid"] not in heap_oid_by_name.values() or relation["relacl"])
            for relation in relations
        )
    ):
        raise ValueError("registry_fhir_catalog_custody_invalid")
    unsupported = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_proc WHERE pronamespace=$1::oid) OR EXISTS("
        "SELECT FROM pg_class c WHERE c.relnamespace=$1::oid AND "
        "(c.relrowsecurity OR c.relforcerowsecurity OR c.relispartition OR "
        "EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) OR "
        "EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) OR "
        "EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)))",
        namespace["oid"],
    )
    if unsupported:
        raise ValueError("registry_fhir_catalog_custody_invalid")
    return {
        "namespace": dict(namespace),
        "relations": [dict(relation) for relation in relations],
        "heaps": heap_oid_by_name,
    }


def _metadata_receipt(target, roles, catalog):
    receipt = writer_closure_receipt(target, roles, catalog)
    receipt["component"] = "registry_fhir_catalog_writer_closure"
    return receipt


async def freeze_fhir_catalog_snapshot(
    connection,
    copy_target,
    *,
    writer_roles,
    expected_snapshot,
    control_schema,
    standing_grant=None,
    observation_verifier=None,
):
    """Close writers, validate the final fresh postimage, then attest its exact inputs."""
    if not connection.is_in_transaction() or type(copy_target) is not FHIRCatalogTarget:
        raise ValueError("registry_fhir_catalog_transaction_required")
    if await connection.fetchval("SELECT current_setting('transaction_isolation')") != "read committed":
        raise ValueError("registry_fhir_catalog_fresh_snapshot_required")
    if type(writer_roles) is not dict or set(writer_roles) != {"owner_role", "loader_roles", "reader_roles"}:
        raise ValueError("registry_fhir_catalog_writer_roles_invalid")
    if any(type(writer_roles[field]) is not list for field in ("loader_roles", "reader_roles")):
        raise ValueError("registry_fhir_catalog_writer_roles_invalid")
    roles = writer_role_names(
        writer_roles["owner_role"], tuple(writer_roles["loader_roles"]), tuple(writer_roles["reader_roles"])
    )
    await verify_writer_roles(connection, roles)
    namespace = storage_identifier(copy_target.schema_name)
    for table in sorted(PHYSICAL_TABLES):
        await connection.execute(f"LOCK TABLE {namespace}.{table} IN ACCESS EXCLUSIVE MODE")
    catalog = await _catalog(connection, copy_target)
    await transfer_and_grant_writer_closure(connection, copy_target, roles, catalog)
    closed = await _catalog(connection, copy_target)
    await verify_writer_closure(connection, roles, closed)
    requires_replay = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {namespace}.registry_fhir_catalog_source "
        "WHERE descriptor_json->'admission'->>'kind'='catalog_observation') OR EXISTS("
        f"SELECT FROM {storage_identifier(control_schema)}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network_binding' "
        "AND record_json->'archived'='false'::jsonb AND record_json->>'source_system'='fhir' "
        "AND (record_json->'source_scope_json' ? 'insurance_plan_id' "
        "OR record_json->>'source_key' LIKE 'InsurancePlan/%')) OR EXISTS("
        f"SELECT FROM {namespace}.registry_fhir_catalog_binding "
        "WHERE document_json->'source_scope' ? 'insurance_plan_id' "
        "OR document_json->>'source_key' LIKE 'InsurancePlan/%')",
        expected_snapshot["metadata_approved_revision"],
    )
    if requires_replay:
        if standing_grant is None or not callable(observation_verifier):
            raise ValueError("registry_fhir_catalog_observation_verifier_required")
        await observation_verifier(
            connection=connection, copy_target=copy_target, snapshot=expected_snapshot, standing_grant=standing_grant
        )
    validation = await validate_fhir_catalog_candidate(
        connection, copy_target, expected_snapshot, control_schema=control_schema, standing_grant=standing_grant
    )
    return {**_metadata_receipt(copy_target, roles, closed), "validation": validation}


async def verify_fhir_catalog_snapshot(connection, snapshot):
    """Reject changed object identities, owners, grants or runtime SET paths."""
    if not connection.is_in_transaction():
        raise ValueError("registry_fhir_catalog_transaction_required")
    receipt = snapshot["custody_json"]
    if type(receipt) is str:
        receipt = json.loads(receipt)
    if type(receipt) is not dict or receipt.get("component") != "registry_fhir_catalog_writer_closure":
        raise ValueError("registry_fhir_catalog_custody_invalid")
    native_by_field = {key: value for key, value in receipt.items() if key != "validation"}
    verify_fhir_catalog_validation(snapshot, receipt.get("validation"))
    target = FHIRCatalogTarget(**receipt["scope"])
    if target.schema_name != snapshot["schema_name"]:
        raise ValueError("registry_fhir_catalog_custody_invalid")
    roles = writer_role_names(receipt["owner_role"], tuple(receipt["loader_roles"]), tuple(receipt["reader_roles"]))
    catalog = await _catalog(connection, target)
    current = _metadata_receipt(target, roles, catalog)
    verify_writer_closure_identity(current, native_by_field, extending=False)
    await verify_writer_closure(connection, roles, catalog)
