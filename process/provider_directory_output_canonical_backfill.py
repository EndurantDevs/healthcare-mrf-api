# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic bounded canonical-resource and source-edge maintenance."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from contextvars import copy_context
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text

from db.plan_release_storage import storage_relation
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_journal as journal
from process import provider_directory_output_snapshot_stage as stages
from process.provider_directory_dataset_rehydrate_pages import projected_pages
from process.provider_directory_dataset_rehydrate_pin import (
    _close_reader,
    _require_reader,
    _same_database,
    _SourceReader,
)
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_output_snapshot_catalog import (
    OUTPUT_CATALOG_SQL,
    decoded,
    output_models,
    require_output,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

TARGETS = ("provider_directory_canonical_resource", "provider_directory_source_resource")
CONTROLS = ("provider_directory_api_endpoint", "provider_directory_source")
CONTROL_KEYS = {CONTROLS[0]: "endpoint_id", CONTROLS[1]: "source_id"}
# Control keys can also be referenced by native FKs; report their own constraint.
CONTROL_CATALOG_SQL = OUTPUT_CATALOG_SQL.replace(
    "WHERE k.conindid=i.indexrelid)", "WHERE k.conindid=i.indexrelid AND k.conrelid=c.oid)", 1
)
TYPED_INPUTS = {
    "InsurancePlan": "provider_directory_insurance_plan",
    "Practitioner": "provider_directory_practitioner",
    "Organization": "provider_directory_organization",
    "Location": "provider_directory_location",
    "PractitionerRole": "provider_directory_practitioner_role",
    "HealthcareService": "provider_directory_healthcare_service",
    "OrganizationAffiliation": "provider_directory_organization_affiliation",
    "Endpoint": "provider_directory_endpoint",
}


@dataclass(frozen=True)
class _CanonicalInputs:
    receipts: dict
    controls: dict
    summary: dict
    operation: dict


def _selected(directory, resources):
    """Select fixed model inputs; routing maps never provide native custody."""
    selected_resources = [name for name in directory._selected_resources(resources) if name in TYPED_INPUTS]
    required_roots = set(TARGETS) | set(CONTROLS) | {TYPED_INPUTS[name] for name in selected_resources}
    overrides = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    require_output(not required_roots.intersection(overrides), "canonical_scratch_custody_required")
    for name in selected_resources:
        require_output(
            directory.RESOURCE_MODELS_BY_TYPE[name] is output_models()[TYPED_INPUTS[name]], "canonical_model_changed"
        )
    return selected_resources


def _incoming_sql(directory, resource_type, *, edges):
    """Retain native JSON hashing, winner order and exact former INSERT values."""
    relation = storage_relation(directory._schema(), TYPED_INPUTS[resource_type])
    source = storage_relation(directory._schema(), "provider_directory_source")
    base = "COALESCE(NULLIF(src.canonical_api_base,''),NULLIF(src.api_base,''))"
    resource_literal = directory._sql_string_literal(resource_type)
    payload = "(to_jsonb(r)-'source_id'-'last_seen_run_id'-'observed_at'-'updated_at')"
    predicate = f"{base} IS NOT NULL AND NULLIF(r.source_id,'') IS NOT NULL AND NULLIF(r.resource_id,'') IS NOT NULL"
    if edges:
        return f"SELECT DISTINCT r.source_id,{base} AS canonical_api_base,{resource_literal}::varchar AS resource_type,r.resource_id,r.last_seen_run_id,r.observed_at,COALESCE(r.updated_at,r.observed_at) AS updated_at FROM {relation} r JOIN {source} src USING(source_id) WHERE {predicate}"
    return f"""SELECT DISTINCT ON ({base},r.resource_id) {base} AS canonical_api_base,
      {resource_literal}::varchar AS resource_type,r.resource_id,r.resource_url,r.fhir_meta,r.fhir_self_url,
      r.fhir_fetch_url,r.fhir_fetch_mode,md5(({payload})::text)::varchar AS payload_hash,{payload} AS payload_json,
      r.last_seen_run_id AS first_seen_run_id,r.last_seen_run_id,r.observed_at,COALESCE(r.updated_at,r.observed_at) AS updated_at
      FROM {relation} r JOIN {source} src USING(source_id) WHERE {predicate}
      ORDER BY {base},r.resource_id,r.updated_at DESC NULLS LAST,r.observed_at DESC NULLS LAST,r.source_id"""


def canonical_projection(directory, target, selected_resources):
    """Merge only the selected native keys and preserve all unrelated incumbent cells."""
    require_output(target in TARGETS and bool(selected_resources), "canonical_target_invalid")
    is_edge = target == TARGETS[1]
    incoming = " UNION ALL ".join(
        "(" + _incoming_sql(directory, name, edges=is_edge) + ")" for name in dict.fromkeys(selected_resources)
    )
    keys = tuple(str(column.name) for column in output_models()[target].__table__.primary_key.columns)
    key_sql = ",".join(keys)
    columns = [str(column.name) for column in output_models()[target].__table__.columns]
    column_expressions = []
    for name in columns:
        value = (
            "COALESCE(old.first_seen_run_id,i.first_seen_run_id)" if name == "first_seen_run_id" else 'i."' + name + '"'
        )
        column_expressions.append(value + ' AS "' + name + '"')
    incumbent = storage_relation(directory._schema(), target)
    match = " AND ".join('i."' + key + '"=old."' + key + '"' for key in keys)
    return (
        f"WITH incoming AS MATERIALIZED ({incoming}) SELECT {','.join(column_expressions)} FROM incoming i "
        f"LEFT JOIN {incumbent} old USING({key_sql}) UNION ALL SELECT "
        + ",".join('old."' + name + '"' for name in columns)
        + f" FROM {incumbent} old WHERE NOT EXISTS(SELECT FROM incoming i WHERE {match})"
    )


def count_projection(directory, selected_resources):
    """Count affected UPSERT keys, including unchanged and repeated requested kinds."""
    return " UNION ALL ".join(
        "SELECT "
        + directory._sql_string_literal(name)
        + "::text AS resource_type,"
        + f"(SELECT count(*) FROM ({_incoming_sql(directory, name, edges=False)}) c)::bigint AS canonical_rows,"
        + f"(SELECT count(*) FROM ({_incoming_sql(directory, name, edges=True)}) e)::bigint AS source_edge_rows"
        for name in dict.fromkeys(selected_resources)
    )


def _summary(selected_resources, counts):
    counts_by_type = {
        row["resource_type"]: {name: row[name] for name in ("canonical_rows", "source_edge_rows")} for row in counts
    }
    by_resource = {name: counts_by_type[name] for name in selected_resources}
    return {
        "resources": by_resource,
        "canonical_rows": sum(counts_by_type[name]["canonical_rows"] for name in selected_resources),
        "source_edge_rows": sum(counts_by_type[name]["source_edge_rows"] for name in selected_resources),
    }


async def _require_control_fence_rights(connection, schema):
    """Existing native rights, never a grant or a caller's snapshot marker."""
    for root in CONTROLS:
        allowed = await connection.fetchval(
            "SELECT to_regclass($1) IS NOT NULL AND has_table_privilege(current_user,$1,'SELECT') "
            "AND has_table_privilege(current_user,$1,'MAINTAIN,UPDATE,DELETE,TRUNCATE')",
            storage_relation(schema, root),
        )
        require_output(allowed is True, "canonical_control_cut_fence_required")


async def _control_digest(connection, schema, root):
    """Frame all complete low-volume controls in bounded native keyset pages."""
    relation, key = storage_relation(schema, root), CONTROL_KEYS[root]
    digest, cursor, count = hashlib.sha256(), None, 0
    while True:
        headers = await connection.fetch(
            f"SELECT {key},octet_length(convert_to(row_to_json(r)::text,'UTF8'))+256 AS bytes FROM {relation} r "
            f"WHERE ($1::varchar IS NULL OR {key}>$1) ORDER BY {key} LIMIT 512",
            cursor,
        )
        if not headers:
            return {"rows": count, "sha256": digest.hexdigest()}
        size, selected_headers = 21, []
        for header in headers:
            require_output(type(header["bytes"]) is int and header["bytes"] > 0, "canonical_control_size_invalid")
            if size + header["bytes"] > 16 * 1024 * 1024:
                require_output(bool(selected_headers), "canonical_control_too_large")
                break
            size += header["bytes"]
            selected_headers.append(header)
        upper = selected_headers[-1][key]
        control_rows = await connection.fetch(
            f"SELECT {key},to_jsonb(r)::text AS payload FROM {relation} r "
            f"WHERE ($1::varchar IS NULL OR {key}>$1) AND {key}<=$2 ORDER BY {key}",
            cursor,
            upper,
        )
        require_output(
            len(control_rows) == len(selected_headers) and control_rows[-1][key] == upper, "canonical_control_changed"
        )
        for control_row in control_rows:
            encoded_payload = control_row["payload"].encode("utf-8")
            digest.update(len(encoded_payload).to_bytes(8, "big"))
            digest.update(encoded_payload)
        cursor, count = upper, count + len(control_rows)


async def _control_witness(session, connection, policy):
    from process.mrf_address_publication import require_native_read_catalog

    rows = await connection.fetch(CONTROL_CATALOG_SQL, policy.schema, list(CONTROLS), 0, [])
    require_output({row["relname"] for row in rows} == set(CONTROLS), "canonical_controls_missing")
    await require_native_read_catalog(
        session, tuple(row["oid"] for row in rows), executable_roles=(policy.owner_role, policy.publisher_role)
    )
    controls_by_root = {}
    for row in rows:
        columns = decoded(row["columns"])
        keys_by_number = {column["number"]: column["name"] for column in columns}
        primary_constraints = [constraint for constraint in decoded(row["constraints"]) if constraint["type"] == "p"]
        require_output(
            len(primary_constraints) == 1
            and primary_constraints[0]["validated"]
            and not primary_constraints[0]["deferrable"]
            and [keys_by_number[number] for number in primary_constraints[0]["keys"]] == [CONTROL_KEYS[row["relname"]]],
            "canonical_control_shape_changed",
        )
        controls_by_root[row["relname"]] = {
            "catalog": {
                name: decoded(value) if name in {"topology", "columns", "constraints", "indexes", "acl"} else value
                for name, value in dict(row).items()
            },
            "content": await _control_digest(connection, policy.schema, row["relname"]),
        }
    return controls_by_root


@asynccontextmanager
async def _preservation_reader(directory, capability, input_receipts):
    """Pin all actual typed inputs and both incumbents with a genuine native Reader."""
    database = await directory.db._connect_reader()
    session, reader = database.session_factory(), None
    try:
        driver = await _require_reader(session, database, capability.policy)
        await _same_database(capability.connection, driver)
        await driver.execute(
            "LOCK TABLE "
            + ",".join(storage_relation(receipt.schema, receipt.target) for receipt in input_receipts.values())
            + " IN ACCESS SHARE MODE NOWAIT"
        )
        for receipt in input_receipts.values():
            require_output(
                await driver.fetchval(
                    "SELECT to_regclass($1)::oid::bigint", storage_relation(receipt.schema, receipt.target)
                )
                == receipt.oid,
                "canonical_input_changed",
            )
        reader = _SourceReader(session, capability.policy.schema, {})
        yield reader
    finally:
        if reader is not None:
            reader.deactivate()
        await _close_reader(session)


async def _fresh_controls(directory, expected, parent_driver):
    """Authenticate a fresh second native snapshot while the original cut locks are held."""
    async with source_transaction(directory.db, isolation="read committed") as session:
        publisher = bound_publisher()
        await _same_database(parent_driver, publisher.connection)
        actual = await _control_witness(session, publisher.connection, publisher.policy)
        require_output(actual == expected, "canonical_control_changed")


async def _join_fresh_control_task(task):
    await asyncio.gather(task, return_exceptions=True)


async def _final_fence(directory, capability, expected, factory_context, input_receipts):
    await capability.connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(capability.policy.schema, root) for root in CONTROLS)
        + " IN SHARE MODE NOWAIT"
    )
    task = asyncio.create_task(
        _fresh_controls(directory, expected, capability.connection), context=factory_context.copy()
    )
    try:
        await task
    finally:
        if not task.done():
            task.cancel()
            await drain_operation(_join_fresh_control_task(task), preserve_cancellation=True)
    require_output(
        await custody._capture(capability, {root: root for root in input_receipts}) == input_receipts,
        "canonical_input_changed",
    )


async def _prepare(directory, capability, selected_resources, expected_incumbents):
    receipts, prepared_stages = [], []
    for target in TARGETS:
        stage = await stages.prepare_output_stage(directory, capability, target)
        prepared_stages.append(stage)
        require_output(stage.incumbent == expected_incumbents[target], "canonical_incumbent_changed")
        await journal.record_output_progress(capability, stage, 0)
        projection = canonical_projection(directory, target, selected_resources)
        copied = 0
        async for rows in projected_pages(directory.db, projection, {}, output_models()[target]):
            copied += await stages.copy_output_rows(directory, capability, stage, rows)
            await journal.record_output_progress(capability, stage, copied)
        await stages.finish_output_indexes(directory, capability, stage)
        relation = storage_relation(stage.schema, stage.relation)
        parity = f"WITH expected AS MATERIALIZED ({projection}), difference AS ((SELECT to_jsonb(e) FROM expected e EXCEPT ALL SELECT to_jsonb(a) FROM {relation} a) UNION ALL (SELECT to_jsonb(a) FROM {relation} a EXCEPT ALL SELECT to_jsonb(e) FROM expected e)) SELECT NOT EXISTS(SELECT FROM difference)"
        require_output((await capability.session.execute(text(parity))).scalar_one(), "canonical_parity_failed")
        await stages.seal_output_stage(capability, stage, expected_rows=copied)
        receipts.append(
            await custody.capture_prepared_output(
                capability, target, stage.relation, expected_incumbent=stage.incumbent
            )
        )
    await journal.record_output_prepared(capability, receipts)
    return receipts


async def _capture_inputs(directory, publisher, session, selected_resources):
    from process.provider_directory_output_snapshot_provision import _capture

    policy = publisher.policy
    require_output(directory._schema() == policy.schema, "schema_changed")
    native = await _capture(session, policy)
    roots = tuple(sorted(set(TARGETS) | {TYPED_INPUTS[name] for name in selected_resources}))
    receipts_by_root = {root: custody._native_receipt(policy, native[root], root) for root in roots}
    controls = await _control_witness(session, publisher.connection, policy)
    counts = [dict(row._mapping) for row in await directory.db.all(count_projection(directory, selected_resources))]
    summary = _summary(selected_resources, counts)
    operation_by_field = dict(
        kind="canonical_backfill",
        run_id="canonical-" + uuid4().hex,
        producer="canonical-backfill-v1",
        source_pins=[],
        resource_types=list(dict.fromkeys(selected_resources)),
        source_fence_digest=custody._digest(
            {
                "controls": controls,
                "selection": selected_resources,
                "inputs": [receipt.to_payload() for receipt in receipts_by_root.values()],
                "counts": counts,
            }
        ),
    )
    return _CanonicalInputs(receipts_by_root, controls, summary, operation_by_field)


async def backfill_canonical_resources(directory, *, resources=None):
    """Prepare and atomically publish exactly the two fixed compatibility roots."""
    from process.provider_directory_artifact_bundle_preparation import apply_prepared_output_bundle

    selected_resources = _selected(directory, resources)
    if not selected_resources:
        return {"resources": {}, "canonical_rows": 0, "source_edge_rows": 0}
    require_output(
        directory.db._transaction_binding() is None and not directory.db.has_reader_session(),
        "canonical_unbound_operation_required",
    )
    factory_context = copy_context()
    async with source_transaction(directory.db, isolation="repeatable read") as session:
        publisher = bound_publisher()
        policy = publisher.policy
        await _require_control_fence_rights(publisher.connection, policy.schema)
        inputs = await _capture_inputs(directory, publisher, session, selected_resources)
        if inputs.summary["canonical_rows"] == 0 and inputs.summary["source_edge_rows"] == 0:
            return inputs.summary
        async with custody.output_snapshot_transaction(
            directory, operation=inputs.operation, targets=TARGETS
        ) as capability:
            async with _preservation_reader(directory, capability, inputs.receipts):
                receipts = await _prepare(directory, capability, selected_resources, inputs.receipts)
            await _final_fence(directory, capability, inputs.controls, factory_context, inputs.receipts)

            async def append_history(capability, retained):
                """Advance the real protected operation journal inside the same atomic cut."""
                await journal.append_output_history(capability, receipts, retained)

            await apply_prepared_output_bundle(directory, capability, receipts, after_output_publish=append_history)
        return inputs.summary
