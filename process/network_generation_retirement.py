# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""List or explicitly retire selected older network generations and their published catalogs."""

import argparse
import asyncio
import json
import re
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from db.plan_release_storage import storage_identifier, storage_relation
from db.registry_schema import registry_schema
from process.network_compact_layout import INDEPENDENT_STORAGE, INDEPENDENT_STORAGE_KEY
from process.network_compact_retention import _MAX_METADATA_BYTES, _candidate_dependencies, _page_sql, _walk_references

_TYPED_SCHEMA_PATTERN = re.compile(r"registry_fhir_typed_([0-9a-f]{32})")
_BOOTSTRAP_SCHEMA_PATTERN = re.compile(r"network_bootstrap_([0-9a-f]{32})")
_TYPED_COMMENT_PREFIXES = ("registry_fhir_typed_capture:5:", "registry_fhir_typed_capture:4:")
_MAX_TYPED_COMMENT_BYTES = 262144


class NetworkGenerationRetirementError(ValueError):
    """The selection changed or a reader, build or retained consumer still needs it."""


def _require(condition, reason):
    if not condition:
        raise NetworkGenerationRetirementError("network_retirement_" + reason)


async def _control_versions(connection, control):
    # Compare MVCC row versions only across this inventory and its final locked check.
    return await connection.fetch(
        f"""SELECT 'candidate' AS source,candidate_id::text AS key,xmin::text AS version
        FROM {control}.network_membership_candidate WHERE state<>'rejected'
        UNION ALL SELECT 'manifest',generation_id::text,xmin::text FROM {control}.network_serving_manifest
        UNION ALL SELECT 'catalog',metadata_generation,xmin::text FROM {control}.registry_fhir_catalog_snapshot
        UNION ALL SELECT 'head',id::text,xmin::text FROM {control}.network_serving_control
        UNION ALL SELECT 'request',request_id::text,xmin::text FROM {control}.registry_publication_request
        WHERE state IN ('queued','running')
        ORDER BY source,key"""
    )


async def _selection(connection, control, generations, expected_head):
    head = await connection.fetchrow(
        f"""SELECT h.generation_id,h.metadata_generation,
          (h.metadata_generation IS NULL AND m.source_generations->>'fhir_catalog' IS NULL
           OR s.published_at IS NOT NULL AND s.network_generation=h.generation_id AND s.manifest_sha256=m.manifest_sha256
             AND coalesce(m.source_generations->>'fhir_catalog',h.metadata_generation)=h.metadata_generation) AS paired
        FROM {control}.network_serving_control h
        JOIN {control}.network_serving_manifest m USING(generation_id)
        JOIN {control}.network_membership_candidate c USING(candidate_id)
        LEFT JOIN {control}.registry_fhir_catalog_snapshot s ON s.metadata_generation=h.metadata_generation
        WHERE h.id=1 AND m.eligible AND c.state='published'"""
    )
    _require(head is not None and head["generation_id"] == expected_head, "head_changed")
    _require(head["paired"] is True, "active_catalog_changed")
    networks = await connection.fetch(
        f"""SELECT m.generation_id,m.candidate_id,m.manifest_sha256,c.schema_name,
          c.validation_json#>>'{{address_source,schema_name}}' AS address_schema
        FROM {control}.network_serving_manifest m JOIN {control}.network_membership_candidate c USING(candidate_id)
        WHERE m.generation_id=ANY($1::bigint[]) AND c.state='published' ORDER BY m.generation_id""",
        generations,
    )
    _require([network["generation_id"] for network in networks] == generations, "target_unavailable")
    _require(
        all(network["schema_name"] == "network_candidate_" + network["candidate_id"].hex for network in networks),
        "schema_invalid",
    )
    catalogs = await connection.fetch(
        f"""SELECT s.metadata_generation,s.schema_name,s.manifest_sha256=m.manifest_sha256 AS paired
        FROM {control}.registry_fhir_catalog_snapshot s JOIN {control}.network_serving_manifest m
        ON m.generation_id=s.network_generation WHERE s.network_generation=ANY($1::bigint[])
        AND s.published_at IS NOT NULL ORDER BY s.metadata_generation""",
        generations,
    )
    _require(all(catalog["paired"] is True for catalog in catalogs), "catalog_pair_changed")
    _require(
        all(re.fullmatch(r"fhir_catalog_[0-9a-f]{32}", catalog["schema_name"]) for catalog in catalogs),
        "schema_invalid",
    )
    _require(
        all(catalog["metadata_generation"] != head["metadata_generation"] for catalog in catalogs), "active_catalog"
    )
    return networks, catalogs


async def _target_relations(connection, schemas):
    rows = await connection.fetch(
        """SELECT n.nspname AS schema,c.relname AS name FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=ANY($1::text[]) AND c.relkind IN ('r','p') ORDER BY n.nspname,c.relname""",
        schemas,
    )
    relations = {(row["schema"], row["name"]) for row in rows}
    _require({schema for schema, _ in relations} == set(schemas), "storage_missing")
    return relations


async def _require_catalog_sources(connection, control, retired_metadata, generations, schemas):
    # Published and staged catalog source tables are immutable after the snapshot enters the ledger.
    survivors = await connection.fetch(
        f"SELECT schema_name FROM {control}.registry_fhir_catalog_snapshot WHERE NOT(metadata_generation=ANY($1::text[]))",
        retired_metadata,
    )
    for row in survivors:
        sources = storage_relation(row["schema_name"], "registry_fhir_catalog_source")
        referenced = await connection.fetchval(
            f"""SELECT EXISTS(SELECT FROM {sources} WHERE
            jsonb_path_exists(descriptor_json,'$.**.retained_manifest.generation_id ? (@ == $ids[*])',
                jsonb_build_object('ids',$1::bigint[])) OR
            jsonb_path_exists(descriptor_json,'$.**.schema ? (@ == $names[*])',jsonb_build_object('names',$2::text[])) OR
            jsonb_path_exists(descriptor_json,'$.**.schema_name ? (@ == $names[*])',jsonb_build_object('names',$2::text[])))""",
            generations,
            schemas,
        )
        _require(not referenced, "catalog_source_referenced")


async def _require_consumers(connection, control, generations, candidates, manifests, metadata):
    generation_strings = [str(generation) for generation in generations]
    conflict = await connection.fetchval(
        f"""SELECT CASE WHEN EXISTS(SELECT FROM {control}.network_membership_candidate c
          WHERE c.state<>'rejected' AND NOT(c.candidate_id=ANY($2::uuid[])) AND (
            c.source_generations->>'fhir_catalog'=ANY($4::text[]) OR
            (c.state<>'published' AND c.expected_head=ANY($1::bigint[])) OR
            ((c.state<>'published' OR c.source_generations->>$6::text IS DISTINCT FROM $7::text)
               AND c.source_generations->>'retained_network_source'=ANY($3::text[]))))
          THEN 'generation_referenced'
        WHEN EXISTS(SELECT FROM {control}.registry_fhir_catalog_snapshot
          WHERE published_at IS NULL AND custody_json#>>'{{preparation,previous_metadata_generation}}'=ANY($4::text[]))
          THEN 'catalog_preparation_referenced'
        WHEN EXISTS(SELECT FROM {control}.registry_publication_request q
          LEFT JOIN {control}.registry_publication_request preview ON preview.request_id::text=q.command_json->>'preview_request_id'
          WHERE q.state IN ('queued','running') AND (
            q.command_json->>'target_generation'=ANY($5::text[]) OR q.command_json->>'expected_serving_generation'=ANY($5::text[])
            OR q.command_json#>>'{{selector,retained_generation_id}}'=ANY($5::text[])
            OR preview.command_json->>'target_generation'=ANY($5::text[])
            OR q.command_json->>'metadata_generation'=ANY($4::text[]) OR q.command_json->>'expected_metadata_generation'=ANY($4::text[])))
          THEN 'request_referenced'
        WHEN EXISTS(SELECT FROM {control}.registry_site_binding WHERE NOT archived AND source_generation=ANY($1::bigint[]))
          OR EXISTS(SELECT FROM {control}.registry_approved_record r WHERE r.record_kind='site_binding'
            AND r.record_json->'archived'='false'::jsonb AND r.record_json->>'source_generation'=ANY($5::text[])
            AND (r.approved_revision=(SELECT approved_revision FROM {control}.registry_revision_control WHERE id=1)
              OR EXISTS(SELECT FROM {control}.network_membership_candidate c WHERE c.state<>'rejected'
                AND NOT(c.candidate_id=ANY($2::uuid[])) AND c.approved_custom_revision=r.approved_revision)))
          OR EXISTS(SELECT FROM {control}.registry_ptg_office_hold WHERE hold_json#>>'{{retained_serving,generation_id}}'=ANY($5::text[]))
          OR EXISTS(SELECT FROM {control}.registry_ptg_office_approval WHERE approval_json#>>'{{command,retained_generation_id}}'=ANY($5::text[]))
          THEN 'office_referenced' END""",
        generations,
        candidates,
        manifests,
        metadata,
        generation_strings,
        INDEPENDENT_STORAGE_KEY,
        INDEPENDENT_STORAGE,
    )
    _require(conflict is None, conflict)


async def _preparations(connection, schema, networks, catalogs, schemas):
    network_ids = [network["candidate_id"] for network in networks]
    catalog_ids = [UUID(catalog["schema_name"].removeprefix("fhir_catalog_")) for catalog in catalogs]
    matching = await connection.fetch(
        """SELECT preparation_id,node_id,importer_id,state FROM hp_snapshot_retention.preparation
        WHERE preparation_id=ANY($1::uuid[])""",
        network_ids + catalog_ids,
    )
    for preparation in matching:
        is_network = preparation["preparation_id"] in network_ids
        _require(
            preparation["node_id"] == schema and preparation["importer_id"] == ("network" if is_network else "catalog"),
            "preparation_referenced",
        )
        # Membership publication pairs an imported catalog after its named preparation reaches validated.
        _require(
            preparation["state"] == "published" or (not is_network and preparation["state"] == "validated"),
            "preparation_busy",
        )
    ids = [preparation["preparation_id"] for preparation in matching]
    conflict = await connection.fetchval(
        """SELECT EXISTS(SELECT FROM hp_snapshot_retention.relation WHERE schema_name=ANY($1::text[]))
        OR EXISTS(SELECT FROM hp_snapshot_retention.preparation_relation
          WHERE (schema_name=ANY($1::text[])) <> (preparation_id=ANY($2::uuid[])))
        OR EXISTS(SELECT FROM hp_snapshot_retention.preparation WHERE state<>'abandoned' AND (
          (expected_generation_id=ANY($2::uuid[]) AND NOT(preparation_id=ANY($2::uuid[]))) OR
          (node_id=$3 AND importer_id='office' AND handoff#>>'{snapshot,retained_generation_id}'=ANY($4::text[]))))""",
        schemas,
        ids,
        schema,
        [str(network["generation_id"]) for network in networks],
    )
    _require(not conflict, "preparation_referenced")
    return ids


async def _lock_retirement(connection, control, relations):
    # Both normal pin statements read these controls before their first data query. Their ACCESS SHARE
    # locks last through that transaction, so NOWAIT also protects a reader paused between those statements.
    await connection.execute(
        "SELECT set_config('lock_timeout',LEAST(CASE WHEN setting::bigint=0 THEN 1000 ELSE setting::bigint END,1000)::text,true) "
        "FROM pg_settings WHERE name='lock_timeout'"
    )
    await connection.execute(
        f"LOCK TABLE {control}.network_serving_manifest,{control}.registry_fhir_catalog_snapshot IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    controls = (
        "network_serving_control",
        "network_membership_candidate",
        "registry_publication_request",
        "registry_site_binding",
        "registry_approved_record",
        "registry_revision_control",
        "registry_ptg_office_hold",
        "registry_ptg_office_approval",
    )
    reservations = ("preparation", "preparation_relation", "preparation_claim", "relation")
    await connection.execute(
        "LOCK TABLE "
        + ",".join(
            [f"{control}.{name}" for name in controls] + [f"hp_snapshot_retention.{name}" for name in reservations]
        )
        + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    tables = ",".join("ONLY " + storage_relation(*relation) for relation in sorted(relations))
    await connection.execute("LOCK TABLE " + tables + " IN ACCESS EXCLUSIVE MODE NOWAIT")


async def _retire(connection, control, preparation_ids, candidates, metadata, relations, schemas):
    # Dropping a partitioned parent would implicitly remove its partitions even under RESTRICT.
    external_child = await connection.fetchval(
        """SELECT EXISTS(SELECT FROM pg_catalog.pg_class p JOIN pg_catalog.pg_namespace pn ON pn.oid=p.relnamespace
        CROSS JOIN LATERAL pg_catalog.pg_partition_tree(p.oid) branch
        JOIN pg_catalog.pg_class child ON child.oid=branch.relid
        JOIN pg_catalog.pg_namespace cn ON cn.oid=child.relnamespace
        WHERE p.relkind='p' AND pn.nspname=ANY($1::text[]) AND NOT(cn.nspname=ANY($1::text[])))""",
        schemas,
    )
    _require(not external_child, "external_partition")
    for table in ("preparation_relation", "preparation_claim", "preparation"):
        await connection.execute(
            f"DELETE FROM hp_snapshot_retention.{table} WHERE preparation_id=ANY($1::uuid[])", preparation_ids
        )
    await connection.execute(
        f"DELETE FROM {control}.registry_fhir_catalog_snapshot WHERE metadata_generation=ANY($1::text[])", metadata
    )
    for table in (
        "network_membership_batch",
        "network_candidate_custody",
        "network_serving_manifest",
        "network_membership_candidate",
    ):
        await connection.execute(f"DELETE FROM {control}.{table} WHERE candidate_id=ANY($1::uuid[])", candidates)
    await connection.execute(
        "DROP TABLE " + ",".join(storage_relation(*relation) for relation in sorted(relations)) + " RESTRICT"
    )
    await connection.execute("DROP SCHEMA " + ",".join(storage_identifier(schema) for schema in schemas) + " RESTRICT")


def _composition_schemas(networks):
    schemas = []
    for network in networks:
        address_schema = network["address_schema"]
        if address_schema is not None and address_schema.startswith("network_composition_"):
            _require(re.fullmatch(r"network_composition_[0-9a-f]{32}", address_schema) is not None, "schema_invalid")
            schemas.append(address_schema)
    return list(dict.fromkeys(schemas))


def _classify_optional_schema(schema_name):
    if schema_name.startswith("registry_fhir_typed_"):
        _require(_TYPED_SCHEMA_PATTERN.fullmatch(schema_name) is not None, "schema_invalid")
        return "typed"
    if schema_name.startswith("network_bootstrap_"):
        _require(_BOOTSTRAP_SCHEMA_PATTERN.fullmatch(schema_name) is not None, "schema_invalid")
        return "bootstrap"
    return None


def _address_schema_from_candidate(candidate_row):
    raw_validation = candidate_row.get("validation_json")
    validation = json.loads(raw_validation) if type(raw_validation) is str else raw_validation
    if type(validation) is not dict:
        return None
    address_source = validation.get("address_source")
    return address_source.get("schema_name") if type(address_source) is dict else None


def _record_candidate_schemas(
    candidate_by_field,
    row_schemas,
    *,
    is_retiring,
    retiring_typed,
    retiring_bootstrap,
    surviving_schemas,
):
    """Classify candidate schemas into retiring vs surviving dependency sets."""
    if not is_retiring:
        surviving_schemas.update(row_schemas)
        return
    for candidate_schema in sorted(row_schemas):
        kind = _classify_optional_schema(candidate_schema)
        if kind == "typed":
            retiring_typed.add(candidate_schema)
        elif kind == "bootstrap":
            retiring_bootstrap.add(candidate_schema)


async def _collect_candidate_schema_sets(connection, control, schema, retiring_candidates, retiring_generations):
    """Collect typed and bootstrap schemas referenced only by retiring generations."""
    retiring_typed, retiring_bootstrap, surviving_schemas = set(), set(), set()
    after = None
    while True:
        candidate_rows = await connection.fetch(_page_sql(control), after, _MAX_METADATA_BYTES)
        if not candidate_rows:
            return retiring_typed - surviving_schemas, retiring_bootstrap - surviving_schemas
        for candidate_row in candidate_rows:
            _require(candidate_row["bounded"] is True, "schema_invalid")
            candidate_by_field = dict(candidate_row)
            is_retiring = candidate_row["candidate_id"] in retiring_candidates
            dependencies = await _candidate_dependencies(
                connection,
                candidate_by_field,
                schema,
                () if is_retiring else retiring_generations,
            )
            row_schemas = {dep_schema for dep_schema, _ in dependencies}
            addr_schema = _address_schema_from_candidate(candidate_by_field)
            if type(addr_schema) is str:
                row_schemas.add(addr_schema)
            _record_candidate_schemas(
                candidate_by_field,
                row_schemas,
                is_retiring=is_retiring,
                retiring_typed=retiring_typed,
                retiring_bootstrap=retiring_bootstrap,
                surviving_schemas=surviving_schemas,
            )
        after = candidate_rows[-1]["candidate_id"]


async def _unrequested_existing_schemas(connection, control, candidate_schemas):
    """Return existing schemas that are not referenced by queued or running publication requests."""
    if not candidate_schemas:
        return []
    existing_schemas = [
        namespace_row["nspname"]
        for namespace_row in await connection.fetch(
            "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname=ANY($1::text[]) ORDER BY nspname",
            sorted(candidate_schemas),
        )
    ]
    if not existing_schemas:
        return []
    request_rows = await connection.fetch(
        f"""SELECT q.command_json::text AS cmd,preview.command_json::text AS preview_cmd
        FROM {control}.registry_publication_request q
        LEFT JOIN {control}.registry_publication_request preview
          ON preview.request_id::text=q.command_json->>'preview_request_id'
        WHERE q.state IN ('queued','running')"""
    )
    active_blobs = [
        blob.lower()
        for request_row in request_rows
        for blob in (request_row["cmd"], request_row["preview_cmd"])
        if blob
    ]
    kept_schemas = []
    for candidate_schema in existing_schemas:
        capture_uuid = str(UUID(candidate_schema.rsplit("_", 1)[1]))
        if not any(candidate_schema in blob or capture_uuid in blob for blob in active_blobs):
            kept_schemas.append(candidate_schema)
    return kept_schemas


def _typed_source_key(metadata):
    if type(metadata) is not str:
        return None
    for prefix in _TYPED_COMMENT_PREFIXES:
        if metadata.startswith(prefix):
            try:
                document = json.loads(metadata[len(prefix) :])
            except ValueError:
                return None
            break
    else:
        return None
    descriptor = document.get("catalog_descriptor") if type(document) is dict else None
    coords = descriptor.get("coordinates") if type(descriptor) is dict else None
    if type(descriptor) is not dict or type(coords) is not dict:
        return None
    key_parts = (
        coords.get("source_id"),
        coords.get("dataset_schema"),
        coords.get("dataset_id"),
        descriptor.get("dataset_sha256"),
    )
    return key_parts if all(type(part) is str for part in key_parts) else None


async def _filter_reusable_typed_schemas(connection, control, retired_metadata, typed_schemas):
    """Exclude typed schemas whose source edition still matches a surviving catalog snapshot."""
    if not typed_schemas:
        return []
    comment_rows = await connection.fetch(
        """SELECT n.nspname,
          CASE WHEN pg_catalog.octet_length(pg_catalog.obj_description(n.oid,'pg_namespace'))<=$2
               THEN pg_catalog.obj_description(n.oid,'pg_namespace') END AS metadata
        FROM pg_catalog.pg_namespace n WHERE n.nspname=ANY($1::text[]) ORDER BY n.nspname""",
        typed_schemas,
        _MAX_TYPED_COMMENT_BYTES,
    )
    key_by_schema = {comment_row["nspname"]: _typed_source_key(comment_row["metadata"]) for comment_row in comment_rows}
    valid_schemas = [schema_item for schema_item in typed_schemas if key_by_schema.get(schema_item) is not None]
    if not valid_schemas:
        return []
    survivors = await connection.fetch(
        f"SELECT schema_name FROM {control}.registry_fhir_catalog_snapshot WHERE NOT(metadata_generation=ANY($1::text[]))",
        retired_metadata,
    )
    if not survivors:
        return valid_schemas
    distinct_keys = sorted({key_by_schema[schema_item] for schema_item in valid_schemas})
    reusable_keys = set()
    for survivor in survivors:
        catalog_table = storage_relation(survivor["schema_name"], "registry_fhir_catalog_source")
        matched_entries = await connection.fetch(
            f"""SELECT k.source_id,k.dataset_schema,k.dataset_id,k.dataset_sha256
            FROM unnest($1::text[],$2::text[],$3::text[],$4::text[])
              AS k(source_id,dataset_schema,dataset_id,dataset_sha256)
            WHERE EXISTS(SELECT FROM {catalog_table} s
              WHERE s.descriptor_json#>>'{{coordinates,source_id}}'=k.source_id
                AND s.descriptor_json#>>'{{coordinates,dataset_schema}}'=k.dataset_schema
                AND s.descriptor_json#>>'{{coordinates,dataset_id}}'=k.dataset_id
                AND s.descriptor_json->>'dataset_sha256'=k.dataset_sha256)""",
            [entry[0] for entry in distinct_keys],
            [entry[1] for entry in distinct_keys],
            [entry[2] for entry in distinct_keys],
            [entry[3] for entry in distinct_keys],
        )
        for match_entry in matched_entries:
            reusable_keys.add(
                (
                    match_entry["source_id"],
                    match_entry["dataset_schema"],
                    match_entry["dataset_id"],
                    match_entry["dataset_sha256"],
                )
            )
    return [schema_item for schema_item in valid_schemas if key_by_schema[schema_item] not in reusable_keys]


async def _retirable_dependency_schemas(
    connection, control, schema, retiring_candidates, retiring_generations, metadata
):
    raw_typed, raw_bootstrap = await _collect_candidate_schema_sets(
        connection, control, schema, retiring_candidates, retiring_generations
    )
    typed_existing = await _unrequested_existing_schemas(connection, control, raw_typed)
    typed_schemas = await _filter_reusable_typed_schemas(connection, control, metadata, typed_existing)
    bootstrap_schemas = await _unrequested_existing_schemas(connection, control, raw_bootstrap)
    return typed_schemas, bootstrap_schemas


def _retirement_report(
    execute,
    generations,
    network_schemas,
    metadata,
    catalog_schemas,
    composition_schemas,
    typed_schemas=(),
    bootstrap_schemas=(),
):
    return {
        "state": "retired" if execute else "listed",
        "generation_ids": generations,
        "network_schemas": network_schemas,
        "metadata_generations": metadata,
        "catalog_schemas": catalog_schemas,
        **({"composition_schemas": composition_schemas} if composition_schemas else {}),
        **({"typed_schemas": list(typed_schemas)} if typed_schemas else {}),
        **({"bootstrap_schemas": list(bootstrap_schemas)} if bootstrap_schemas else {}),
    }


@dataclass(frozen=True)
class _RetirementTargets:
    """Resolved generation schemas and dependent artifacts for one retirement batch."""

    networks: list
    catalogs: list
    candidates: list
    manifests: list
    metadata: list
    network_schemas: list
    catalog_schemas: list
    composition_schemas: list
    typed_schemas: list
    bootstrap_schemas: list
    schemas: list


async def _retirement_targets(connection, control, schema, generations, expected_head):
    networks, catalogs = await _selection(connection, control, generations, expected_head)
    candidates = [network["candidate_id"] for network in networks]
    manifests = [network["manifest_sha256"] for network in networks]
    metadata = [catalog["metadata_generation"] for catalog in catalogs]
    network_schemas = [network["schema_name"] for network in networks]
    catalog_schemas = [catalog["schema_name"] for catalog in catalogs]
    composition_schemas = _composition_schemas(networks)
    typed_schemas, bootstrap_schemas = await _retirable_dependency_schemas(
        connection, control, schema, set(candidates), set(generations), metadata
    )
    schemas = network_schemas + catalog_schemas + composition_schemas + typed_schemas + bootstrap_schemas
    return _RetirementTargets(
        networks=networks,
        catalogs=catalogs,
        candidates=candidates,
        manifests=manifests,
        metadata=metadata,
        network_schemas=network_schemas,
        catalog_schemas=catalog_schemas,
        composition_schemas=composition_schemas,
        typed_schemas=typed_schemas,
        bootstrap_schemas=bootstrap_schemas,
        schemas=schemas,
    )


async def _verify_and_execute_retirement(connection, control, schema, generations, versions, selection, execute):
    """Verify catalog, reader, and consumer invariants, then retire selected generations."""
    _require(schema not in selection.schemas, "schema_invalid")
    relations = await _target_relations(connection, selection.schemas)
    await _walk_references(
        connection,
        relations,
        schema,
        retiring_candidates=set(selection.candidates),
        retiring_generations=set(generations),
    )
    await _require_catalog_sources(connection, control, selection.metadata, generations, selection.schemas)
    if execute:
        _require(
            await connection.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", "registry-fhir-catalog:" + schema
            ),
            "catalog_build_active",
        )
        await _lock_retirement(connection, control, relations)
    _require(await _control_versions(connection, control) == versions, "controls_changed")
    _require(await _target_relations(connection, selection.schemas) == relations, "storage_changed")
    await _require_consumers(
        connection, control, generations, selection.candidates, selection.manifests, selection.metadata
    )
    preparation_ids = await _preparations(connection, schema, selection.networks, selection.catalogs, selection.schemas)
    if execute:
        await _retire(
            connection,
            control,
            preparation_ids,
            selection.candidates,
            selection.metadata,
            relations,
            selection.schemas,
        )


async def retire_network_generations(connection, *, generation_ids, expected_head, control_schema=None, execute=False):
    """List or atomically retire an explicit older batch. The administrative caller owns authorization."""
    _require(not connection.is_in_transaction() and type(execute) is bool, "invalid_call")
    _require(type(expected_head) is int and 0 < expected_head < 2**63, "head_invalid")
    _require(
        type(generation_ids) in (tuple, list)
        and bool(generation_ids)
        and all(type(generation) is int and 0 < generation < expected_head for generation in generation_ids)
        and len(set(generation_ids)) == len(generation_ids),
        "selection_invalid",
    )
    generations = sorted(generation_ids)
    schema = registry_schema() if control_schema is None else control_schema
    control = storage_identifier(schema)
    try:
        async with connection.transaction(isolation="read_committed"):
            versions = await _control_versions(connection, control)
            selection = await _retirement_targets(connection, control, schema, generations, expected_head)
            await _verify_and_execute_retirement(connection, control, schema, generations, versions, selection, execute)
    except asyncpg.LockNotAvailableError as error:
        raise NetworkGenerationRetirementError("network_retirement_pin_or_writer_active") from error
    return _retirement_report(
        execute,
        generations,
        selection.network_schemas,
        selection.metadata,
        selection.catalog_schemas,
        selection.composition_schemas,
        selection.typed_schemas,
        selection.bootstrap_schemas,
    )


async def _main(arguments):
    from db.connection import db

    await db.connect()
    try:
        async with db.acquire_driver() as connection:
            report = await retire_network_generations(
                connection,
                generation_ids=arguments.generation,
                expected_head=arguments.expected_head,
                execute=arguments.execute,
            )
    finally:
        await db.disconnect()
    print(json.dumps(report, sort_keys=True))


def main(argv=None):
    """Run the explicit retirement command against the configured registry."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--generation", required=True, type=int, action="append", help="older generation ID; repeat for each target"
    )
    parser.add_argument("--expected-head", required=True, type=int)
    parser.add_argument("--execute", action="store_true", help="retire all selected generations in one transaction")
    asyncio.run(_main(parser.parse_args(argv)))


if __name__ == "__main__":
    main()
