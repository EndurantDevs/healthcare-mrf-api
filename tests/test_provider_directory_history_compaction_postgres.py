# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Online history partition compaction, NOWAIT cutover, rollback, and downstream checks."""

import json
import uuid

import asyncpg
import pytest

from db.migration_provider_directory_dataset_candidates import TABLES
from process import provider_directory_history_compaction_cutover as cutover_module
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_provision import _retained_empty_dataset_histories
from process.provider_directory_history_compaction import (
    _STAGE_COMPONENT,
    GENERIC_HISTORY_TABLES,
    HistoryCompactionError,
    _stage_name,
    plan_history_compaction,
    prepare_history_compaction,
    select_compaction_plan,
)
from process.provider_directory_history_compaction_cutover import (
    cutover_history_compaction,
    drop_retired_history,
    rollback_history_compaction,
)
from process.provider_directory_terminal_root_retirement_native import require_native_retirement_controls
from tests.formulary_fhir_twin_admission_pg_support import connect, database_url, quoted, run_migration
from tests.test_provider_dataset_candidates_postgres import (
    _candidate_migration,
    _candidate_test_scope,
    _install_retirement_child_guards,
)


class _DatabaseAdapter:
    """Minimal database facade for native terminal-retirement control verification."""

    def __init__(self, connection):
        self._connection = connection

    async def all(self, query, **params):
        sql, args = self._bind(query, params)
        return [dict(record) for record in await self._connection.fetch(sql, *args)]

    async def first(self, query, **params):
        sql, args = self._bind(query, params)
        record = await self._connection.fetchrow(sql, *args)
        return dict(record) if record is not None else None

    async def scalar(self, query, **params):
        sql, args = self._bind(query, params)
        return await self._connection.fetchval(sql, *args)

    @staticmethod
    def _bind(query, params):
        if not params:
            return query, []
        args = []
        rendered = query
        for index, (key, param_value) in enumerate(sorted(params.items(), key=lambda entry: -len(entry[0])), 1):
            token = f":{key}"
            if token in rendered:
                rendered = rendered.replace(token, f"${index}")
                args.append(param_value)
        return rendered, args


async def _insert_secondary_history_rows(connection, schema, dataset_id):
    """Populate dependent generic history tables for one synthetic dataset."""
    await connection.execute(
        f"""INSERT INTO {schema}.{TABLES[1]}(dataset_id,resource_id,payload_hash,payload_json)
        VALUES($1,$2,repeat('b',64),'{{}}')""",
        dataset_id,
        f"{dataset_id}-res-1",
    )
    await connection.execute(
        f"INSERT INTO {schema}.{TABLES[2]} VALUES($1,$2,$3)",
        dataset_id,
        f"{dataset_id}-net",
        f"{dataset_id}-res-1",
    )
    await connection.execute(
        f"INSERT INTO {schema}.{TABLES[3]} VALUES($1,$2,$3)",
        dataset_id,
        f"{dataset_id}-org",
        f"{dataset_id}-aff",
    )


async def _seed_dataset_rows(connection, schema, spec):
    """Seed one endpoint dataset and generic history rows under candidate guards."""
    dataset_id, status, is_current, endpoint_id, proof_version, metadata, resource_count = spec
    await connection.execute(
        f"""INSERT INTO {schema}.provider_directory_endpoint_dataset(
          dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json
        ) VALUES($1,$4,'building',false,$2,$3::jsonb)""",
        dataset_id,
        resource_count,
        json.dumps(metadata or {}),
        endpoint_id,
    )
    for entity_index in range(1, resource_count + 1):
        await connection.execute(
            f"""INSERT INTO {schema}.{TABLES[0]}(dataset_id,resource_type,resource_id,payload_hash,payload_json)
            VALUES($1,'InsurancePlan',$2,repeat('a',64),'{{}}')""",
            dataset_id,
            f"{dataset_id}-res-{entity_index}",
        )
    if resource_count:
        await _insert_secondary_history_rows(connection, schema, dataset_id)
    await connection.execute(
        f"""UPDATE {schema}.provider_directory_endpoint_dataset
        SET status=$2,is_current=$3,completion_proof_required_version=$4
        WHERE dataset_id=$1""",
        dataset_id,
        status,
        is_current,
        proof_version,
    )


async def _seed_history_fixture(context):
    """Seed retained and omitted datasets plus a synthetic catalog reference."""
    connection, schema = context.connection, context.schema
    await connection.execute(
        f"""INSERT INTO {schema}.provider_directory_api_endpoint(
          endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash
        ) VALUES('endpoint-pub','https://pub.example.test/fhir',repeat('1',64),repeat('2',64))"""
    )
    retained_specs = (
        ("ds-published", "published", True, "endpoint-pub", None, {"source_ids": ["payer-a"]}, 2),
        ("ds-baseline", "verification_baseline", False, "endpoint-official", None, {"source_ids": ["payer-a"]}, 2),
        ("ds-validated", "validated", False, "endpoint-official", None, {"source_ids": ["payer-a"]}, 2),
        ("ds-retired", "acquisition_retired", False, "endpoint-official", None, {"source_ids": ["payer-a"]}, 1),
        ("ds-abandoned", "acquisition_abandoned", False, "endpoint-official", 3, {"source_ids": ["payer-a"]}, 1),
        ("ds-superseded-v3", "superseded", False, "endpoint-official", 3, {"source_ids": ["payer-a"]}, 2),
        ("ds-superseded-cms", "superseded", False, "endpoint-official", None, {"source_ids": ["cms-npd"]}, 2),
        ("ds-superseded-cat", "superseded", False, "endpoint-official", None, {"source_ids": ["payer-b"]}, 2),
    )
    omitted_specs = (
        ("ds-superseded-old", "superseded", False, "endpoint-official", None, {"source_ids": ["payer-a"]}, 2),
        ("ds-failed-old", "failed", False, "endpoint-official", None, {"source_ids": ["payer-a"]}, 2),
    )
    for spec in retained_specs + omitted_specs:
        await _seed_dataset_rows(connection, schema, spec)
    cat_schema = "fhir_catalog_" + uuid.uuid4().hex
    await connection.execute(f'CREATE SCHEMA "{cat_schema}"')
    await connection.execute(f'CREATE TABLE "{cat_schema}".registry_fhir_catalog_source(descriptor_json jsonb)')
    await connection.execute(
        f'INSERT INTO "{cat_schema}".registry_fhir_catalog_source VALUES($1::jsonb)',
        json.dumps({"coordinates": {"dataset_id": "ds-superseded-cat"}}),
    )
    return cat_schema, [spec[0] for spec in retained_specs], [spec[0] for spec in omitted_specs]


async def _assert_downstream_checks(context, schema_name, roles_by_kind):
    """Verify downstream consumers, native controls, and post-compaction writes stay healthy."""
    connection, schema = context.connection, context.schema
    plan = await connection.fetchval(f"SELECT plan FROM {schema}.pd_dataset_migration_plan WHERE singleton")
    parsed_plan = json.loads(plan) if type(plan) is str else plan
    histories = ["pd_dataset_history_" + str(parsed_plan["tables"][root]["original_oid"]) for root in TABLES]
    assert await connection.fetchval(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) AND c.relkind='r'",
        schema_name,
        histories,
    ) == len(TABLES)
    assert await require_native_retirement_controls(_DatabaseAdapter(connection)) == set(TABLES[:5])
    policy = GenericSourcePolicy(
        schema=schema_name,
        owner_role=roles_by_kind["owner"],
        publisher_role=roles_by_kind["generic"],
        runtime_roles=(roles_by_kind["member"],),
    )
    async with connection.transaction():
        retained_empty = await _retained_empty_dataset_histories(connection, policy)
    assert len(retained_empty) >= 1

    await connection.execute(
        f"""INSERT INTO {schema}.provider_directory_endpoint_dataset(
          dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json
        ) VALUES('ds-post-compaction','endpoint-official','acquiring',false,1,'{{}}')"""
    )
    await connection.execute(
        f"""INSERT INTO {schema}.{TABLES[0]}(dataset_id,resource_type,resource_id,payload_hash,payload_json)
        VALUES('ds-post-compaction','InsurancePlan','post-res',repeat('c',64),'{{}}')"""
    )
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='validated' WHERE dataset_id='ds-post-compaction'"
    )


async def _exercise_prepare_and_nowait_guard(connection, context, schema_name):
    """Prepare online alongside a pinned reader, verify reuse, and assert NOWAIT refusal."""
    reader = await connect(database_url())
    try:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            before_count = await reader.fetchval(f"SELECT count(*) FROM {context.schema}.{TABLES[0]}")
            prepared = await prepare_history_compaction(connection, schema=schema_name)
            assert prepared["status"] == "prepared"
            assert all(not entry["reused"] for entry in prepared["tables"].values())
            reused = await prepare_history_compaction(connection, schema=schema_name)
            assert all(entry["reused"] for entry in reused["tables"].values())
            with pytest.raises(asyncpg.LockNotAvailableError):
                await cutover_history_compaction(connection, schema=schema_name)
    finally:
        await reader.close()
    return before_count


@pytest.mark.asyncio
async def test_online_history_compaction_cutover_rollback_and_drop(monkeypatch):
    """Exercise plan, online prepare, NOWAIT cutover, rollback, re-cutover, and drop."""
    async with _candidate_test_scope(monkeypatch) as (context, roles_by_kind):
        await _install_retirement_child_guards(context)
        cat_schema, _retained_ids, omitted_ids = await _seed_history_fixture(context)
        try:
            await run_migration(context.engine, _candidate_migration(), "upgrade")
            connection, schema_name = context.connection, context.schema_name
            planned = await plan_history_compaction(connection, schema=schema_name)
            assert planned["omitted_key_count"] == 2
            for root in GENERIC_HISTORY_TABLES:
                assert planned["tables"][root]["current_key_count"] - planned["tables"][root]["retained_key_count"] == 2

            before_count = await _exercise_prepare_and_nowait_guard(connection, context, schema_name)
            swapped = await cutover_history_compaction(connection, schema=schema_name)
            assert swapped["status"] == "cutover"
            for dataset_id in omitted_ids:
                assert (
                    await connection.fetchval(
                        f"SELECT count(*) FROM {context.schema}.{TABLES[0]} WHERE dataset_id=$1", dataset_id
                    )
                    == 0
                )
            assert await connection.fetchval(f"SELECT count(*) FROM {context.schema}.{TABLES[0]}") == before_count - 4
            await run_migration(context.engine, _candidate_migration(), "upgrade")

            rolled_back = await rollback_history_compaction(connection, schema=schema_name)
            assert rolled_back["status"] == "rolled_back"
            assert await connection.fetchval(f"SELECT count(*) FROM {context.schema}.{TABLES[0]}") == before_count

            await prepare_history_compaction(connection, schema=schema_name)
            await cutover_history_compaction(connection, schema=schema_name)
            dropped = await drop_retired_history(connection, schema=schema_name)
            assert len(dropped["dropped_relations"]) == len(GENERIC_HISTORY_TABLES)
            await _assert_downstream_checks(context, schema_name, roles_by_kind)
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        finally:
            await context.connection.execute(f'DROP SCHEMA IF EXISTS "{cat_schema}" CASCADE')


async def _probe_reader_and_writer(probe, root_ref, heap_ref):
    """Read the history root and try to write one compared heap, each with a short lock timeout."""
    async with probe.transaction():
        await probe.execute("SET LOCAL lock_timeout = '200ms'")
        await probe.fetchval(f"SELECT count(*) FROM {root_ref}")
    try:
        async with probe.transaction():
            await probe.execute("SET LOCAL lock_timeout = '200ms'")
            await probe.execute(f"DELETE FROM {heap_ref} WHERE false")
    except asyncpg.LockNotAvailableError:
        return "read ok", "write blocked"
    return "read ok", "write allowed"


def _probe_on_first_fingerprint(monkeypatch, probe, root_ref, heap_ref, probe_results):
    original_fingerprint = cutover_module._history_fingerprint

    async def probing_fingerprint(*arguments):
        if not probe_results:
            probe_results.append(await _probe_reader_and_writer(probe, root_ref, heap_ref))
        return await original_fingerprint(*arguments)

    monkeypatch.setattr(cutover_module, "_history_fingerprint", probing_fingerprint)


@pytest.mark.asyncio
async def test_cutover_and_rollback_verify_without_blocking_readers(monkeypatch):
    """Fingerprint scans hold only SHARE locks: readers proceed, writers to the compared heaps wait."""
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        await _install_retirement_child_guards(context)
        cat_schema, _retained_ids, _omitted_ids = await _seed_history_fixture(context)
        probe = await connect(database_url())
        try:
            await run_migration(context.engine, _candidate_migration(), "upgrade")
            connection, schema_name = context.connection, context.schema_name
            root_ref = f"{context.schema}.{TABLES[0]}"
            selection = await select_compaction_plan(connection, schema=schema_name)
            old_ref = f"{context.schema}.{quoted(f'pd_dataset_history_{selection.old_oid_by_table[TABLES[0]]}')}"
            await prepare_history_compaction(connection, schema=schema_name)
            cutover_probes = []
            _probe_on_first_fingerprint(monkeypatch, probe, root_ref, old_ref, cutover_probes)
            swapped = await cutover_history_compaction(connection, schema=schema_name)
            assert cutover_probes == [("read ok", "write blocked")]
            new_ref = f"{context.schema}.{quoted(f'pd_dataset_history_{swapped["tables"][TABLES[0]]["new_oid"]}')}"
            rollback_probes = []
            _probe_on_first_fingerprint(monkeypatch, probe, root_ref, new_ref, rollback_probes)
            assert (await rollback_history_compaction(connection, schema=schema_name))["status"] == "rolled_back"
            assert rollback_probes == [("read ok", "write blocked")]
        finally:
            await probe.close()
            await context.connection.execute(f'DROP SCHEMA IF EXISTS "{cat_schema}" CASCADE')


@pytest.mark.asyncio
async def test_cutover_refuses_a_stage_whose_rows_changed(monkeypatch):
    """Refuse cutover when stage rows no longer match the source, even with the prepared comment intact."""
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        await _install_retirement_child_guards(context)
        cat_schema, _retained_ids, _omitted_ids = await _seed_history_fixture(context)
        try:
            await run_migration(context.engine, _candidate_migration(), "upgrade")
            connection, schema_name = context.connection, context.schema_name
            prepared = await prepare_history_compaction(connection, schema=schema_name)
            stage_ref = f"{context.schema}.{quoted(prepared['tables'][TABLES[0]]['stage_relation'])}"
            await connection.execute(f"DELETE FROM {stage_ref} WHERE ctid = (SELECT min(ctid) FROM {stage_ref})")
            with pytest.raises(HistoryCompactionError, match="compaction_stage_changed"):
                await cutover_history_compaction(connection, schema=schema_name)
            assert await connection.fetchval(f"SELECT count(*) FROM {stage_ref}") > 0
        finally:
            await context.connection.execute(f'DROP SCHEMA IF EXISTS "{cat_schema}" CASCADE')


@pytest.mark.asyncio
async def test_online_history_compaction_handles_empty_retained_keys(monkeypatch):
    """Verify full omission (zero retained keys) detaches empty partitions cleanly."""
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        await _install_retirement_child_guards(context)
        connection, schema_name = context.connection, context.schema_name
        await connection.execute(
            f"UPDATE {context.schema}.provider_directory_endpoint_dataset SET status='failed', is_current=false"
        )
        await _seed_dataset_rows(
            connection,
            context.schema,
            ("ds-only-failed", "failed", False, "endpoint-official", None, {}, 2),
        )
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        prepared = await prepare_history_compaction(connection, schema=schema_name)
        assert prepared["omitted_key_count"] >= 1
        swapped = await cutover_history_compaction(connection, schema=schema_name)
        assert all(entry["retained_keys"] == 0 for entry in swapped["tables"].values())
        dropped = await drop_retired_history(connection, schema=schema_name)
        assert len(dropped["dropped_relations"]) == len(GENERIC_HISTORY_TABLES)
        assert await connection.fetchval(f"SELECT count(*) FROM {context.schema}.{TABLES[0]}") == 0
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await require_native_retirement_controls(_DatabaseAdapter(connection)) == set(TABLES[:5])


@pytest.mark.asyncio
async def test_prepare_history_compaction_guards_stage_slot_ownership(monkeypatch):
    """Fail closed on an unrecognized relation at the stage name while allowing prior-plan rebuilds."""
    async with _candidate_test_scope(monkeypatch) as (context, _roles_by_kind):
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        connection, schema_name = context.connection, context.schema_name
        selection = await select_compaction_plan(connection, schema=schema_name)
        stage_name = _stage_name(TABLES[0], selection.plan_sha256)
        stage_ref = f"{context.schema}.{quoted(stage_name)}"
        await connection.execute(f"CREATE TABLE {stage_ref} (marker integer)")
        await connection.execute(f"INSERT INTO {stage_ref} VALUES (1)")
        with pytest.raises(HistoryCompactionError, match="compaction_stage_occupied"):
            await prepare_history_compaction(connection, schema=schema_name)
        assert await connection.fetchval(f"SELECT count(*) FROM {stage_ref}") == 1

        receipt_by_field = {"component": _STAGE_COMPONENT, "root": TABLES[0], "plan_sha256": "f" * 64}
        comment_literal = json.dumps(receipt_by_field, sort_keys=True, separators=(",", ":")).replace("'", "''")
        await connection.execute(f"COMMENT ON TABLE {stage_ref} IS '{comment_literal}'")
        prepared = await prepare_history_compaction(connection, schema=schema_name)
        assert prepared["tables"][TABLES[0]]["stage_relation"] == stage_name
        assert prepared["tables"][TABLES[0]]["reused"] is False
        stage_comment = json.loads(
            await connection.fetchval("SELECT obj_description($1::regclass, 'pg_class')", stage_ref)
        )
        assert not {"old_oid", "stage_oid"} & set(stage_comment)
