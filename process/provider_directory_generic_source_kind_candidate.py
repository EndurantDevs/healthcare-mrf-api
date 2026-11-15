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


async def _validate_original_checks(connection, policy, relations, dataset_id, *, canonical_schema=None):
    """Apply canonical native CHECK predicates to the candidate row set."""
    schema = canonical_schema or policy.schema
    for root, name in relations.items():
        checks = await connection.fetch(
            "SELECT pg_get_expr(conbin,conrelid) AS expression FROM pg_constraint "
            "WHERE conrelid=to_regclass($1) AND contype='c'",
            storage_relation(schema, root),
        )
        predicates = [f"({check['expression']}) IS FALSE" for check in checks]
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
    header = storage_relation(
        schema, relations.get(provenance.removesuffix("_resource"), provenance.removesuffix("_resource"))
    )
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


async def _validate_relationships(connection, schema, relations, dataset_id, *, canonical_schema=None):
    from process.provider_directory_rooted_graph_source_runtime import rooted_graph_relation_coordinates

    for source, target, source_columns, target_columns, _ in sorted(_expected_bulk_relationships()):
        if source not in relations:
            continue
        source_ref = storage_relation(schema, relations[source])
        if target in relations:
            coordinates = (schema, relations[target])
        else:
            coordinates = (canonical_schema or schema, target)
            if canonical_schema is not None:
                coordinates = rooted_graph_relation_coordinates(canonical_schema, target, target) or coordinates
        target_ref = storage_relation(*coordinates)
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


async def _attach_partitions(connection, policy, relations_by_root, dataset_id, *, prepared=False):
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
        if not prepared:
            await connection.execute(f"ALTER TABLE {candidate_ref} ADD CHECK (dataset_id={literal})")
        await _close_grants(connection, policy, name, readable=True)
        await connection.execute(
            f"ALTER TABLE {storage_relation(policy.schema, root)} ATTACH PARTITION {candidate_ref} "
            f"FOR VALUES IN ({literal})"
        )
    # An error aborts the owner's savepoint, which also restores SET LOCAL.
    await connection.execute("SELECT set_config('lock_timeout',$1,true)", previous_timeout)


async def _finish_kind_candidate(connection, policy, relations, dataset_id, kind, finish, detached):
    """Index resource lookups before deriving and proving the complete private group."""
    await _indexes(connection, policy, RESOURCE, relations[RESOURCE])
    if finish is not None:
        await finish()
    for root, name in relations.items():
        if root == _PROVENANCE[kind]:
            await _provenance_indexes(connection, policy.schema, name, kind)
        elif root != RESOURCE:
            await _indexes(connection, policy, root, name)
    await _validate_content(connection, policy.schema, relations, dataset_id, kind)
    await _validate_original_checks(connection, policy, relations, dataset_id)
    await _validate_relationships(connection, policy.schema, relations, dataset_id)
    if detached:
        literal = "'" + dataset_id.replace("'", "''") + "'"
        for name in relations.values():
            await connection.execute(
                f"ALTER TABLE {storage_relation(policy.schema, name)} ADD CHECK (dataset_id={literal})"
            )
        await _capture(connection, policy, list(relations.values()))
    else:
        await _attach_partitions(connection, policy, relations, dataset_id)
        await _capture(connection, policy, list(relations.values()), private=False)


async def _finish_rooted_runtime_candidate(connection, runtime, relations_by_root, roots, dataset_id, finish, prepared):
    """Finish indexes, native constraints and set proofs before handing off the private group."""
    policy = runtime.candidate_policy
    actual = await _capture(connection, policy, list(relations_by_root.values()))
    _require(actual == prepared, "storage_binding_changed")
    await _indexes(connection, policy, RESOURCE, relations_by_root[RESOURCE])
    if finish is not None:
        await finish()
    for root, name in relations_by_root.items():
        if root in roots[-2:]:
            await connection.execute(f"ALTER TABLE {storage_relation(policy.schema, name)} ADD PRIMARY KEY(dataset_id)")
        elif root == _PROVENANCE["rooted"]:
            await _provenance_indexes(connection, policy.schema, name, "rooted")
        elif root != RESOURCE:
            await _indexes(connection, policy, root, name)
    await _validate_content(connection, policy.schema, relations_by_root, dataset_id, "rooted")
    output_relations_by_root = {root: relations_by_root[root] for root in roots[:-2]}
    await _validate_original_checks(
        connection, policy, output_relations_by_root, dataset_id, canonical_schema=runtime.catalog_schema
    )
    await _validate_relationships(
        connection, policy.schema, relations_by_root, dataset_id, canonical_schema=runtime.catalog_schema
    )
    literal = "'" + dataset_id.replace("'", "''") + "'"
    for name in relations_by_root.values():
        await connection.execute(f"ALTER TABLE {storage_relation(policy.schema, name)} ADD CHECK(dataset_id={literal})")
    await _capture(connection, policy, list(relations_by_root.values()))


@asynccontextmanager
async def _rooted_runtime_candidate(database, directory, dataset_id, runtime, finish):
    """Prepare the exact private output and two header snapshots for protected freeze."""
    roots = (
        *RELATIONS[:4],
        _PROVENANCE["rooted"],
        "provider_directory_endpoint_dataset",
        "provider_directory_rooted_graph_dataset",
    )
    async with runtime.transaction(database) as transaction:
        connection, policy = transaction.connection, runtime.candidate_policy
        relations_by_root = {root: "pdgs_" + uuid4().hex + "_r" for root in roots}
        for root, name in relations_by_root.items():
            template_schema = policy.schema if root == "provider_directory_endpoint_dataset" else runtime.catalog_schema
            await connection.execute(
                f"CREATE TABLE {storage_relation(policy.schema, name)} "
                f"(LIKE {storage_relation(template_schema, root)} "
                "INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            )
            if root == "provider_directory_rooted_graph_dataset":
                from process.provider_directory_rooted_graph_header_checks import defer_rooted_header_check

                await defer_rooted_header_check(connection, runtime.catalog_schema, policy.schema, name)
            await connection.execute(
                f"ALTER TABLE {storage_relation(policy.schema, name)} OWNER TO {storage_identifier(policy.owner_role)}"
            )
            await _close_grants(connection, policy, name)
        prepared = await _capture(connection, policy, list(relations_by_root.values()))
        with directory._provider_directory_artifact_relation_scope(relations_by_root):
            yield relations_by_root
            await _finish_rooted_runtime_candidate(
                connection, runtime, relations_by_root, roots, dataset_id, finish, prepared
            )


@asynccontextmanager
async def native_kind_candidate(database, directory, dataset_id, *, kind, finish=None, detached=False):
    """Keep the complete output group private until COPY, indexes and anti-joins finish."""
    _require(kind in _PROVENANCE and type(dataset_id) is str and 1 <= len(dataset_id) <= 96, "kind_invalid")
    from process.provider_directory_rooted_graph_source_runtime import current_rooted_graph_runtime

    runtime = current_rooted_graph_runtime(database)
    if kind == "rooted" and runtime is None:
        raise RuntimeError(
            "Rooted writes require a configured isolated generation prepared through the Source operator"
        )
    if kind == "rooted" and runtime is not None:
        _require(detached, "rooted_preparation_required")
        async with _rooted_runtime_candidate(database, directory, dataset_id, runtime, finish) as relations:
            yield relations
        return
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
            await _finish_kind_candidate(connection, policy, relations_by_root, dataset_id, kind, finish, detached)
