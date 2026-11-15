# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reduce native identities while reusing unchanged compact payload storage."""

import asyncio
import hashlib
import json
import logging
from uuid import uuid4

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_storage as layout
from process import provider_directory_generic_candidate as candidate_rows
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_policy as policy_scope
from process import provider_directory_generic_source_storage as storage
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner

_LOGGER = logging.getLogger(__name__)


def _output_tables(policy, binding):
    tables = layout.payload_tables(policy, binding["tables"])
    return compact.CompactPayloadTables(
        policy.schema, binding["tables"]["resource_output_aliases"]["name"], tables.reference_sets, tables.body_blocks
    )


async def _prepare_relations(connection, policy, previous):
    """Allocate only final alias metadata, a resource view and empty derived heaps."""
    tables_by_root = dict(previous["tables"])
    prefix = tables_by_root["resource_input"]["name"].removesuffix("_ri")
    aliases = storage_relation(policy.schema, tables_by_root["resource_aliases"]["name"])
    name = prefix + "_oa"
    output = storage_relation(policy.schema, name)
    await connection.execute(
        f"CREATE TABLE {output} (LIKE {aliases} INCLUDING CONSTRAINTS INCLUDING STORAGE)" + storage.PRIVATE_HEAP_OPTIONS
    )
    tables_by_root["resource_output_aliases"] = {"schema": policy.schema, "name": name}
    await storage._close_grants(connection, policy, name)
    for root in storage.RELATIONS[1:]:
        name = prefix + "_" + storage._SUFFIXES[root]
        relation = storage_relation(policy.schema, name)
        await connection.execute(
            f"CREATE TABLE {relation} (LIKE {storage_relation(policy.schema, root)} "
            "INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            + storage.PRIVATE_HEAP_OPTIONS
        )
        await storage._close_grants(connection, policy, name)
        tables_by_root[root] = {"schema": policy.schema, "name": name}
    name = prefix + "_r"
    tables_by_root[storage.RESOURCE] = {"schema": policy.schema, "name": name}
    output_tables = _output_tables(policy, {**previous, "tables": tables_by_root})
    await connection.execute(
        f"CREATE VIEW {storage_relation(policy.schema, name)} AS " + compact.compact_resource_sql(output_tables)
    )
    await storage._close_grants(connection, policy, name)
    return tables_by_root


async def _select_latest_aliases(connection, policy, previous, tables_by_root):
    aliases = storage_relation(policy.schema, previous["tables"]["resource_aliases"]["name"])
    output = storage_relation(policy.schema, tables_by_root["resource_output_aliases"]["name"])
    columns = ",".join((*compact._ALIAS_COLUMNS, "npi_text"))
    await connection.execute(
        f"INSERT INTO {output} ({columns}) SELECT DISTINCT ON "
        f'(resource_type COLLATE "C",resource_id COLLATE "C") {columns} FROM {aliases} '
        + completion._active_where("resource_input")
        + 'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal DESC,row_ordinal DESC',
        *completion._active_arguments(previous),
    )


async def _conflicting_keys(connection, policy, binding):
    """Reconstruct repeated keys only, preserving the complete divergence predicate."""
    name = "compact_conflicts_" + uuid4().hex
    relation = storage_relation("pg_temp", name)
    await connection.execute(
        f"CREATE TEMP TABLE {storage_identifier(name)} (resource_type varchar(64),resource_id varchar(256),"
        "PRIMARY KEY(resource_type,resource_id)) ON COMMIT DROP"
    )
    tables = layout.payload_tables(policy, binding["tables"])
    resource = tables.relation("aliases")
    logical_sql = compact.compact_resource_sql(tables, observations=True, alias_cte="selected")
    await connection.execute(
        f"WITH repeated AS MATERIALIZED (SELECT resource_type,resource_id FROM {resource} "
        + completion._active_where("resource_input")
        + "GROUP BY resource_type,resource_id HAVING count(*)>1),selected AS MATERIALIZED ("
        + f"SELECT a.* FROM {resource} a JOIN repeated USING(resource_type,resource_id) "
        + completion._active_where("resource_input")
        + f") INSERT INTO {relation} SELECT resource_type,resource_id FROM ({logical_sql}) logical "
        + f"GROUP BY resource_type,resource_id HAVING count(DISTINCT ({completion._DIVERGENCE_FIELDS['resource_input']}))>1",
        *completion._active_arguments(binding),
    )
    return relation


async def _bounded_resources(connection, tables, dataset_id, query, *arguments):
    """Fetch bounded locator pages before reconstructing their exact JSON payloads."""
    selected, charge = [], candidate_rows.COPY_FRAME_BYTES
    async for key in connection.cursor(query, *arguments, prefetch=candidate_rows.COPY_MAX_ROWS):
        if selected and (
            len(selected) == candidate_rows.COPY_MAX_ROWS or charge + key["read_bytes"] > candidate_rows.COPY_MAX_BYTES
        ):
            for resource in await compact.read_compact_payload_keys(connection, tables, dataset_id, selected):
                yield dict(resource)
            selected, charge = [], candidate_rows.COPY_FRAME_BYTES
        selected.append(key)
        charge += key["read_bytes"]
    if selected:
        for resource in await compact.read_compact_payload_keys(connection, tables, dataset_id, selected):
            yield dict(resource)


async def _conflicting_rows(connection, policy, previous, conflicts):
    tables = layout.payload_tables(policy, previous["tables"])
    query = (
        f"SELECT a.batch_ordinal,a.row_ordinal,a.read_bytes FROM {tables.relation('aliases')} a "
        f"JOIN {conflicts} c USING(resource_type,resource_id) "
        + completion._active_where("resource_input")
        + 'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",batch_ordinal,row_ordinal'
    )
    async for resource in _bounded_resources(
        connection, tables, previous["identity"]["dataset_id"], query, *completion._active_arguments(previous)
    ):
        yield resource


async def _changed_resources(connection, policy, previous, candidate, directory, conflicts):
    """Reduce all observations of divergent keys with the unchanged pure callbacks."""
    prepare, merge = completion._resource_callbacks(directory, candidate)
    latest_by_key = {}

    async def observed_rows():
        """Retain at most the current and next complete key for exact provenance reuse."""
        async for resource in _conflicting_rows(connection, policy, previous, conflicts):
            latest_by_key[resource["resource_type"], resource["resource_id"]] = resource
            yield resource

    reduced = completion._reduced_rows(observed_rows(), dataset_id=candidate.dataset_id, prepare=prepare, merge=merge)
    async for resource in reduced:
        latest = prepare(latest_by_key.pop((resource["resource_type"], resource["resource_id"])))
        if storage._encoded(resource) != storage._encoded(latest):
            yield resource


async def _write_overrides(connection, policy, binding, resource_rows, *, session):
    tables = _output_tables(policy, binding)
    aliases = layout.payload_tables(policy, binding["tables"]).relation("aliases")
    await connection.execute(
        f"DELETE FROM {tables.relation('aliases')} WHERE dataset_id=$1 AND "
        "(resource_type,resource_id) IN(SELECT * FROM unnest($2::text[],$3::text[]))",
        binding["identity"]["dataset_id"],
        [resource["resource_type"] for resource in resource_rows],
        [resource["resource_id"] for resource in resource_rows],
    )
    matched = await _reusable_aliases(connection, policy, binding, resource_rows)
    try:
        await compact._copy(connection, tables, "aliases", (*compact._ALIAS_COLUMNS, "npi_text"), matched)
    except asyncio.CancelledError, TimeoutError:
        await close_interrupted_copy(connection)
        await invalidate_copy_owner(session, connection)
        raise
    reused_keys = {(resource[3], resource[4]) for resource in matched}
    resource_rows = [
        resource
        for resource in resource_rows
        if (resource["resource_type"], resource["resource_id"]) not in reused_keys
    ]
    if not resource_rows:
        return
    ordinal = await connection.fetchval("SELECT nextval(pg_get_serial_sequence($1,'batch_ordinal'))", aliases)
    storage._require(type(ordinal) is int and binding["ordinal"] < ordinal < 2**63 - 1, "ordinal_exhausted")
    await storage._append_compact_resources(
        connection,
        policy,
        {**binding, "tables": {**binding["tables"], "resource_aliases": binding["tables"]["resource_output_aliases"]}},
        resource_rows,
        ordinal,
    )


def _expected_bodies(rows, dataset_id):
    aliases, blocks, _ = compact.prepare_compact_payload_batch(
        rows, dataset_id=dataset_id, batch_ordinal=1, format_id=compact.FORMAT
    )
    bodies_by_block = {block[2]: json.loads(block[3]) for block in blocks}
    for alias in aliases:
        yield (
            alias[3],
            alias[4],
            alias[5],
            alias[6],
            compact._json(bodies_by_block[alias[7]][alias[8]]),
            alias[10],
            alias[9],
            alias[11],
        )


async def _reusable_aliases(connection, policy, binding, rows):
    """Reuse an earlier exact observation when reduction only selected its provenance."""
    tables = layout.payload_tables(policy, binding["tables"])
    matched_rows = []
    fields = ",".join("a." + name for name in (*compact._ALIAS_COLUMNS, "npi_text"))
    for batch in candidate_rows.copy_record_batches(_expected_bodies(rows, binding["identity"]["dataset_id"])):
        records = await connection.fetch(
            "WITH expected AS (SELECT * FROM unnest($3::text[],$4::text[],$5::text[],$6::text[],$7::jsonb[],"
            "$8::smallint[],$9::bytea[],$10::boolean[]) e(type,id,hash,acquired,body,state,refs,restore)) "
            f'SELECT DISTINCT ON (a.resource_type COLLATE "C",a.resource_id COLLATE "C") {fields} '
            f"FROM {tables.relation('aliases')} a JOIN expected e ON (a.resource_type,a.resource_id)=(e.type,e.id) "
            f"JOIN {tables.relation('body_blocks')} b "
            "ON (b.dataset_id,b.batch_ordinal,b.block_ordinal)=(a.dataset_id,a.batch_ordinal,a.block_ordinal) "
            "WHERE a.batch_ordinal>greatest($1::bigint,coalesce(($2::jsonb->>a.resource_type)::bigint,0)) "
            "AND a.resource_type NOT LIKE 'LU:%:pass:%' AND a.payload_hash=e.hash "
            "AND a.acquired_resource_sha256 IS NOT DISTINCT FROM e.acquired "
            "AND a.service_field_state=e.state AND a.service_set_hash IS NOT DISTINCT FROM e.refs "
            "AND a.restore_resource_id=e.restore AND b.bodies->a.body_ordinal=e.body "
            'ORDER BY a.resource_type COLLATE "C",a.resource_id COLLATE "C",a.batch_ordinal DESC,a.row_ordinal DESC',
            *completion._active_arguments(binding),
            *([record[column] for record in batch] for column in range(8)),
        )
        matched_rows.extend(tuple(record) for record in records)
    return matched_rows


async def _store_changed_resources(connection, policy, binding, rows, *, session):
    pending, charge = [], candidate_rows.COPY_FRAME_BYTES
    async for resource in rows:
        values = tuple(
            storage._encoded(resource[name]) if name == "payload_json" else resource[name]
            for name in candidate_rows.RESOURCE_COLUMNS
        )
        size = candidate_rows.copy_record_bytes(values)
        if pending and (len(pending) == candidate_rows.COPY_MAX_ROWS or charge + size > candidate_rows.COPY_MAX_BYTES):
            await _write_overrides(connection, policy, binding, pending, session=session)
            pending, charge = [], candidate_rows.COPY_FRAME_BYTES
        pending.append(resource)
        charge += size
    if pending:
        await _write_overrides(connection, policy, binding, pending, session=session)


async def _finish_indexes(connection, policy, binding):
    output = _output_tables(policy, binding)
    relation = output.relation("aliases")
    await connection.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY(dataset_id,resource_type,resource_id)")
    definitions = (
        *completion._INDEXES[storage.RESOURCE][:-1],
        "(npi_text,dataset_id,resource_type) WHERE resource_type IN('Practitioner','PractitionerRole')",
    )
    for index, definition in enumerate(definitions):
        await connection.execute(
            f"CREATE INDEX {storage_identifier(output.aliases + '_i' + str(index))} ON {relation} {definition}"
        )
    await completion._indexes(connection, policy, storage.WITNESS, binding["tables"][storage.WITNESS]["name"])
    await storage.refresh_statistics(
        connection,
        policy.schema,
        [
            binding["tables"][root]["name"]
            for root in (
                "resource_aliases",
                "resource_output_aliases",
                "resource_reference_sets",
                "resource_body_blocks",
            )
        ],
    )


def _all_aliases_sql(policy, binding):
    roots = ["resource_aliases"]
    if "resource_output_aliases" in binding["tables"]:
        roots.append("resource_output_aliases")
    return " UNION ALL ".join(
        "SELECT * FROM " + storage_relation(policy.schema, binding["tables"][root]["name"]) for root in roots
    )


async def _validate_aliases(connection, tables, alias_sql, dataset_id):
    invalid = await connection.fetchval(
        f"WITH aliases AS ({alias_sql}) SELECT EXISTS(SELECT FROM aliases a "
        f"LEFT JOIN {tables.relation('body_blocks')} b "
        "ON (b.dataset_id,b.batch_ordinal,b.block_ordinal)=(a.dataset_id,a.batch_ordinal,a.block_ordinal) "
        f"LEFT JOIN {tables.relation('reference_sets')} s ON (s.dataset_id,s.set_hash)=(a.dataset_id,a.service_set_hash) "
        "WHERE a.dataset_id<>$1 OR a.resource_type='' OR a.resource_id='' "
        "OR a.payload_hash!~'^[0-9a-f]{64}$' "
        "OR (a.acquired_resource_sha256 IS NOT NULL AND a.acquired_resource_sha256!~'^[0-9a-f]{64}$') "
        "OR b.dataset_id IS NULL OR a.body_ordinal>=jsonb_array_length(b.bodies) "
        "OR jsonb_typeof(b.bodies->a.body_ordinal) NOT IN('object','string') "
        "OR (a.service_field_state=2 AND s.set_hash IS NULL) "
        f"OR a.npi_text IS DISTINCT FROM {compact._npi_expression_sql()})",
        dataset_id,
    )
    storage._require(not invalid, "compact_alias_changed")


async def _validate_blocks(connection, tables, alias_sql, dataset_id):
    invalid = await connection.fetchval(
        f"WITH aliases AS ({alias_sql}),uses AS (SELECT dataset_id,batch_ordinal,block_ordinal,"
        "count(DISTINCT body_ordinal) AS used FROM aliases GROUP BY dataset_id,batch_ordinal,block_ordinal) "
        f"SELECT EXISTS(SELECT FROM {tables.relation('body_blocks')} b LEFT JOIN uses u "
        "USING(dataset_id,batch_ordinal,block_ordinal) WHERE b.dataset_id<>$1 "
        "OR u.used IS DISTINCT FROM jsonb_array_length(b.bodies)::bigint)",
        dataset_id,
    )
    storage._require(not invalid, "compact_block_changed")


async def _validate_reference_sets(connection, tables, alias_sql, dataset_id):
    references = (
        "CASE WHEN s.canonical_refs IS NOT NULL THEN ARRAY(SELECT "
        "'HealthcareService/'||replace(e.reference::text,'-','') FROM unnest(s.canonical_refs) "
        "WITH ORDINALITY e(reference,ordinal) ORDER BY e.ordinal) ELSE s.fallback_refs END"
    )
    invalid = await connection.fetchval(
        f"WITH aliases AS ({alias_sql}),uses AS (SELECT DISTINCT dataset_id,service_set_hash FROM aliases "
        f"WHERE service_set_hash IS NOT NULL) SELECT EXISTS(SELECT FROM {tables.relation('reference_sets')} s "
        "LEFT JOIN uses u ON (u.dataset_id,u.service_set_hash)=(s.dataset_id,s.set_hash) "
        "WHERE s.dataset_id<>$1 OR u.service_set_hash IS NULL "
        "OR coalesce(array_ndims(s.canonical_refs),1)<>1 OR coalesce(array_ndims(s.fallback_refs),1)<>1 "
        "OR array_position(s.canonical_refs,NULL) IS NOT NULL OR array_position(s.fallback_refs,NULL) IS NOT NULL "
        "OR s.set_hash<>sha256(convert_to('['||coalesce((SELECT string_agg(to_json(e.reference)::text,',' "
        f"ORDER BY e.ordinal) FROM unnest({references}) WITH ORDINALITY e(reference,ordinal)),'')||']','UTF8')))",
        dataset_id,
    )
    storage._require(not invalid, "compact_reference_changed")


async def validate_storage(connection, policy, binding):
    """Exhaustively check logical identity, block coverage, ordered sets and NPI data."""
    tables = layout.payload_tables(policy, binding["tables"])
    alias_sql = _all_aliases_sql(policy, binding)
    dataset_id = binding["identity"]["dataset_id"]
    await _validate_aliases(connection, tables, alias_sql, dataset_id)
    await _validate_blocks(connection, tables, alias_sql, dataset_id)
    await _validate_reference_sets(connection, tables, alias_sql, dataset_id)
    invalid = await connection.fetchval(
        f"SELECT coalesce(max(batch_ordinal),0)<>$1 OR EXISTS(SELECT FROM {tables.relation('aliases')} "
        "GROUP BY batch_ordinal HAVING min(row_ordinal)<>0 OR max(row_ordinal)<>count(*)-1) "
        f"FROM {tables.relation('aliases')}",
        binding["ordinal"],
    )
    storage._require(not invalid, "compact_observation_ordinals_changed")
    witness = storage_relation(policy.schema, binding["tables"]["witness_input"]["name"])
    storage._require(
        not await connection.fetchval(f"SELECT EXISTS(SELECT FROM {witness})"), "compact_witness_unsupported"
    )


async def _validate_output_keys(connection, policy, binding):
    source = storage_relation(policy.schema, binding["tables"]["resource_aliases"]["name"])
    output = _output_tables(policy, binding).relation("aliases")
    invalid = await connection.fetchval(
        f"WITH expected AS (SELECT DISTINCT resource_type,resource_id FROM {source} "
        + completion._active_where("resource_input")
        + f"),actual AS (SELECT resource_type,resource_id FROM {output}) SELECT EXISTS("
        "SELECT FROM ((SELECT * FROM expected EXCEPT SELECT * FROM actual) UNION ALL "
        "(SELECT * FROM actual EXCEPT SELECT * FROM expected)) differences)",
        *completion._active_arguments(binding),
    )
    storage._require(not invalid, "compact_resource_keys_changed")


async def _validate_selected_aliases(connection, policy, binding):
    source = storage_relation(policy.schema, binding["tables"]["resource_aliases"]["name"])
    output = _output_tables(policy, binding).relation("aliases")
    columns = (*compact._ALIAS_COLUMNS, "npi_text")
    expected = ",".join("a." + name for name in columns)
    selected = ",".join("o." + name for name in columns)
    invalid = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {output} o LEFT JOIN {source} a "
        "ON (a.dataset_id,a.batch_ordinal,a.row_ordinal)=(o.dataset_id,o.batch_ordinal,o.row_ordinal) "
        f"WHERE o.batch_ordinal<=$1 AND ROW({expected}) IS DISTINCT FROM ROW({selected}))",
        binding["ordinal"],
    )
    storage._require(not invalid, "compact_selected_observation_changed")


async def validate_output_structure(connection, policy, binding):
    """Check the complete final key set and metadata for reused observations."""
    await _validate_output_keys(connection, policy, binding)
    await _validate_selected_aliases(connection, policy, binding)


async def validate_output(connection, policy, binding, candidate, directory):
    """Validate every small result or a repeatable logical-key sample of a large one."""
    await validate_output_structure(connection, policy, binding)
    tables = _output_tables(policy, binding)
    population = await connection.fetchval(f"SELECT count(*) FROM {tables.relation('aliases')}")
    identity_by_field = {
        name: binding["identity"][name] for name in ("dataset_id", "endpoint_id", "acquisition_root_run_id")
    }
    seed = hashlib.sha256(storage._encoded(identity_by_field).encode()).hexdigest()
    query = (
        f"SELECT batch_ordinal,row_ordinal,read_bytes FROM {tables.relation('aliases')} "
        'ORDER BY md5(resource_type||chr(31)||resource_id||$1),resource_type COLLATE "C",resource_id COLLATE "C" LIMIT $2'
    )
    validate, _ = completion._resource_callbacks(directory, candidate)
    sampled = 0
    async for resource in _bounded_resources(
        connection, tables, candidate.dataset_id, query, seed, completion._RECHECK_ROWS
    ):
        validate(resource)
        sampled += 1
    storage._require(sampled == min(population, completion._RECHECK_ROWS), "compact_validation_incomplete")
    _LOGGER.info("compact_completion_validation population=%s sampled=%s seed=%s", population, sampled, seed)


async def complete(publisher, previous, candidate, directory):
    """Finish a closed native-ID projection without making a second payload heap."""
    connection, policy = publisher.connection, publisher.policy
    if previous["phase"] == "prepared":
        await validate_storage(connection, policy, previous)
        await validate_output(connection, policy, previous, candidate, directory)
        return previous
    tables_by_root = await _prepare_relations(connection, policy, previous)
    binding_by_field = {**previous, "phase": "prepared", "tables": tables_by_root}
    await _select_latest_aliases(connection, policy, previous, tables_by_root)
    conflicts = await _conflicting_keys(connection, policy, previous)
    changed = _changed_resources(connection, policy, previous, candidate, directory, conflicts)
    await _store_changed_resources(
        connection, policy, binding_by_field, changed, session=policy_scope.source_copy_session()
    )
    await connection.execute(f"DROP TABLE {conflicts}")
    await _finish_indexes(connection, policy, binding_by_field)
    await validate_storage(connection, policy, binding_by_field)
    await validate_output(connection, policy, binding_by_field, candidate, directory)
    storage._validate_binding(binding_by_field, previous["identity"], phases=("prepared",))
    await storage._store(connection, policy, previous["identity"], previous, binding_by_field)
    return binding_by_field
