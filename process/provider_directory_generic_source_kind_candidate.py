# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-transaction rooted/UHC candidates using native application-owned DDL."""

from contextlib import asynccontextmanager
from uuid import uuid4

from db.migration_provider_dataset_online import _expected_bulk_relationships
from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_completion import _indexes
from process.provider_directory_generic_source_policy import source_owner_transaction
from process.provider_directory_generic_source_storage import (
    RELATIONS,
    RESOURCE,
    _capture,
    _close_grants,
    _require,
)

_PROVENANCE = {
    "rooted": "provider_directory_rooted_graph_dataset_resource",
    "practitioner": "provider_directory_uhc_flex_practitioner_dataset_resource",
}

_ORIGINAL_CHECKS_SQL = """
SELECT c.oid::bigint,c.relname,n.nspname,c.relkind::text,
       c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS is_owned,
       to_regclass($2)::oid::bigint AS current_parent,
       ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) AS parents,
       ARRAY(SELECT ARRAY[conname::text,pg_get_expr(conbin,conrelid)]
             FROM pg_constraint WHERE conrelid=c.oid AND contype='c' ORDER BY oid) AS checks
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1
"""


async def _validate_original_checks(connection, policy, relations, dataset_id):
    """Apply the original heap's data checks, retaining SQL CHECK null semantics."""
    from process.provider_directory_generic_source_provision import _completed_legacy_dataset_plan

    plan = await _completed_legacy_dataset_plan(connection, policy)
    for root, name in relations.items():
        identity = plan["tables"].get(root)
        _require(
            type(identity) is dict
            and type(identity.get("original_oid")) is int
            and type(identity.get("parent_oid")) is int,
            "legacy_history_identity_changed",
        )
        keys = identity.get("keys", plan.get("keys"))
        _require(type(keys) is list, "legacy_history_identity_changed")
        history = await connection.fetchrow(
            _ORIGINAL_CHECKS_SQL, identity["original_oid"], storage_relation(policy.schema, root)
        )
        _require(
            history is not None
            and history["oid"] == identity["original_oid"]
            and history["relname"] == "pd_dataset_history_" + str(identity["original_oid"])
            and history["nspname"] == policy.schema
            and history["relkind"] == "r"
            and history["is_owned"] is True
            and history["current_parent"] == identity["parent_oid"]
            and history["parents"] == ([identity["parent_oid"]] if keys else []),
            "legacy_history_identity_changed",
        )
        predicates = [
            f"NOT ({expression})"
            for check_name, expression in history["checks"]
            if check_name != "pd_dataset_history_bound"
        ]
        if predicates:
            invalid = await connection.fetchval(
                f"SELECT EXISTS(SELECT FROM {storage_relation(policy.schema, name)} "
                "WHERE dataset_id=$1 AND (" + " OR ".join(predicates) + "))",
                dataset_id,
            )
            _require(invalid is False, "candidate_content_changed")


async def _validate_content(connection, schema, relations, dataset_id, kind):
    """Retain the complete normalized resource and provenance set proof before attachment."""
    resource = storage_relation(schema, relations[RESOURCE])
    invalid = await connection.fetchval(
        f"""SELECT EXISTS(SELECT FROM {resource} resource
            WHERE resource.dataset_id IS DISTINCT FROM $1
               OR resource.resource_type NOT IN ('Practitioner','PractitionerRole',
                   'OrganizationAffiliation','Organization','Location','HealthcareService','InsurancePlan','Endpoint')
               OR resource.resource_id !~ '^[A-Za-z0-9.-]{{1,64}}$'
               OR resource.payload_hash !~ '^[0-9a-f]{{64}}$'
               OR jsonb_typeof(resource.payload_json::jsonb) IS DISTINCT FROM 'object'
               OR resource.payload_json::jsonb->>'resource_id' IS DISTINCT FROM resource.resource_id
               OR resource.acquired_resource_sha256 IS NOT NULL)""",
        dataset_id,
    )
    _require(invalid is False, "candidate_content_changed")
    provenance = _PROVENANCE[kind]
    evidence = storage_relation(schema, relations[provenance])
    header = storage_relation(schema, provenance.removesuffix("_resource"))
    predicate = (
        "resource.payload_hash IS DISTINCT FROM evidence.published_payload_hash "
        "OR evidence.root_dataset_id IS DISTINCT FROM header.root_dataset_id "
        "OR evidence.publication_acquisition_id IS DISTINCT FROM header.publication_acquisition_id"
        if kind == "rooted"
        else "resource.resource_type IS DISTINCT FROM 'Practitioner' "
        "OR resource.payload_hash IS DISTINCT FROM evidence.payload_hash "
        "OR resource.payload_json::jsonb->>'npi' IS DISTINCT FROM evidence.requested_npi::text "
        "OR resource.payload_json::jsonb ?| "
        "ARRAY['_acquired_resource_sha256','last_seen_run_id','observed_at','source_id','updated_at'] "
        "OR evidence.candidate_acquisition_id IS DISTINCT FROM header.candidate_acquisition_id"
    )
    invalid = await connection.fetchval(
        f"""SELECT EXISTS(SELECT FROM {resource} resource FULL JOIN {evidence} evidence
            USING(dataset_id,resource_type,resource_id)
            WHERE resource.dataset_id IS NULL OR evidence.dataset_id IS NULL OR {predicate})
            FROM {header} header WHERE header.dataset_id=$1""",
        dataset_id,
    )
    _require(invalid is False, "candidate_content_changed")


async def _validate_relationships(connection, schema, relations, dataset_id):
    for source, target, source_columns, target_columns, _ in sorted(_expected_bulk_relationships()):
        if source not in relations:
            continue
        source_ref = storage_relation(schema, relations[source])
        target_ref = storage_relation(schema, relations.get(target, target))
        predicate = " AND ".join(
            f"s.{storage_identifier(left)}=t.{storage_identifier(right)}"
            for left, right in zip(source_columns, target_columns)
        )
        nonnull = " AND ".join(f"s.{storage_identifier(column)} IS NOT NULL" for column in source_columns)
        missing = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {source_ref} s WHERE s.dataset_id<>$1 OR ({nonnull} "
            f"AND NOT EXISTS(SELECT FROM {target_ref} t WHERE {predicate})))",
            dataset_id,
        )
        _require(missing is False, "provenance_relationship_changed")


async def _provenance_indexes(connection, schema, name, kind):
    columns = "dataset_id,resource_type,resource_id" if kind == "rooted" else "dataset_id,resource_id"
    await connection.execute(f"ALTER TABLE {storage_relation(schema, name)} ADD PRIMARY KEY ({columns})")
    lookup = "publication_acquisition_id,resource_type,resource_id" if kind == "rooted" else "dataset_id,requested_npi"
    await connection.execute(
        f"CREATE INDEX {storage_identifier(name + '_lookup')} ON {storage_relation(schema, name)} ({lookup})"
    )
    await connection.execute(f"ANALYZE {storage_relation(schema, name)}")


async def _attach_partitions(connection, policy, relations_by_root, dataset_id):
    """Bound parent lock waits, retaining stricter caller limits and restoring success."""
    previous_timeout, timeout_ms = await connection.fetchrow(
        "SELECT current_setting('lock_timeout'),setting::bigint FROM pg_settings WHERE name='lock_timeout'"
    )
    if timeout_ms == 0 or timeout_ms > 1000:
        await connection.execute("SET LOCAL lock_timeout='1s'")
    literal = "'" + dataset_id.replace("'", "''") + "'"
    roots = [storage_relation(policy.schema, root) for root in relations_by_root]
    await connection.execute("LOCK TABLE " + ",".join(roots) + " IN SHARE UPDATE EXCLUSIVE MODE")
    for root, name in relations_by_root.items():
        candidate_ref = storage_relation(policy.schema, name)
        await connection.execute(f"ALTER TABLE {candidate_ref} ADD CHECK (dataset_id={literal})")
        await _close_grants(connection, policy, name, readable=True)
        await connection.execute(
            f"ALTER TABLE {storage_relation(policy.schema, root)} ATTACH PARTITION {candidate_ref} "
            f"FOR VALUES IN ({literal})"
        )
    # An error aborts the owner's savepoint, which also restores SET LOCAL.
    await connection.execute("SELECT set_config('lock_timeout',$1,true)", previous_timeout)


@asynccontextmanager
async def native_kind_candidate(database, directory, dataset_id, *, kind):
    """Keep the complete output group private until COPY, indexes and anti-joins finish."""
    _require(kind in _PROVENANCE and type(dataset_id) is str and 1 <= len(dataset_id) <= 96, "kind_invalid")
    async with source_owner_transaction(database) as publisher:
        connection, policy = publisher.connection, publisher.policy
        _require(policy.schema == directory._schema(), "schema_changed")
        provenance = _PROVENANCE[kind]
        header = storage_relation(policy.schema, provenance.removesuffix("_resource"))
        state = await connection.fetchrow(
            f"SELECT status,is_current FROM {header} WHERE dataset_id=$1 FOR UPDATE", dataset_id
        )
        _require(state is not None and state["status"] == "building" and state["is_current"] is False, "header_sealed")
        relations_by_root = {root: "pdgs_" + uuid4().hex + "_r" for root in (*RELATIONS[:4], provenance)}
        for root, name in relations_by_root.items():
            await connection.execute(
                f"CREATE TABLE {storage_relation(policy.schema, name)} (LIKE {storage_relation(policy.schema, root)} "
                "INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            )
            await _close_grants(connection, policy, name)
        prepared = await _capture(connection, policy, list(relations_by_root.values()))
        with directory._provider_directory_artifact_relation_scope(relations_by_root):
            yield relations_by_root
        actual = await _capture(connection, policy, list(relations_by_root.values()))
        _require(actual == prepared, "storage_binding_changed")
        for root, name in relations_by_root.items():
            if root == provenance:
                await _provenance_indexes(connection, policy.schema, name, kind)
            else:
                await _indexes(connection, policy, root, name)
        await _validate_content(connection, policy.schema, relations_by_root, dataset_id, kind)
        await _validate_original_checks(connection, policy, relations_by_root, dataset_id)
        await _validate_relationships(connection, policy.schema, relations_by_root, dataset_id)
        await _attach_partitions(connection, policy, relations_by_root, dataset_id)
        await _capture(connection, policy, list(relations_by_root.values()), private=False)
