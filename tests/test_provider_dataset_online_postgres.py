# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Committed migration retries and reader-compatible native validation locks."""

import asyncio
import json
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.util.concurrency import await_only

from db import migration_provider_dataset_online as online
from db.migration_provider_directory_dataset_candidates import TABLES
from process.provider_directory_rooted_graph_result_store import complete_provider_directory_rooted_graph_result
from tests import test_provider_directory_uhc_flex_practitioner_publication_postgres as publication_fixture
from tests import test_provider_dataset_candidates_postgres as candidate_fixture
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import insert_valid_cohort, seed_official_dataset
from tests.test_provider_dataset_candidates_postgres import _candidate_migration
from tests.test_provider_directory_rooted_graph_bulk_postgres import _claim
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope


async def _seed_references(context):
    resource = f"{context.schema}.{TABLES[0]}"
    for table, partition in (("ordinary_reference", ""), ("partitioned_reference", " PARTITION BY LIST(dataset_id)")):
        await context.connection.execute(
            f"CREATE TABLE {context.schema}.{table}(dataset_id varchar NOT NULL,resource_type varchar NOT NULL,resource_id varchar NOT NULL,CONSTRAINT source_resource_fk FOREIGN KEY(dataset_id,resource_type,resource_id) REFERENCES {resource} ON DELETE CASCADE){partition}"
        )
        if partition:
            keys = await context.connection.fetchval(
                f"SELECT string_agg(quote_literal(dataset_id),',') FROM {context.schema}.provider_directory_endpoint_dataset"
            )
            await context.connection.execute(
                f"CREATE TABLE {context.schema}.reference_leaf PARTITION OF {context.schema}.{table} FOR VALUES IN ({keys})"
            )
        await context.connection.execute(
            f"INSERT INTO {context.schema}.{table} SELECT dataset_id,resource_type,resource_id FROM {resource}"
        )


async def _assert_fenced(context):
    for table in (TABLES[0], "provider_directory_endpoint_dataset"):
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="migration_incomplete_rerun_migration"):
            await context.connection.execute(f"DELETE FROM {context.schema}.{table} WHERE false")


async def _check_validation_reader(context, backend, validation_locks):
    connection, schema = context.connection, context.schema
    rows = await connection.fetch(
        """
        SELECT relation::regclass::text AS relation,mode FROM pg_locks
         WHERE pid=$1 AND locktype='relation' AND granted
    """,
        backend,
    )
    assert not any(row["mode"] == "AccessExclusiveLock" for row in rows)
    assert any(row["mode"] == "ShareUpdateExclusiveLock" for row in rows)
    for table in (TABLES[0], "ordinary_reference", "partitioned_reference"):
        assert await asyncio.wait_for(connection.fetchval(f"SELECT count(*) FROM {schema}.{table}"), timeout=1) > 0
    await _assert_fenced(context)
    validation_locks.append(rows)


def _watch_cutover_scans(monkeypatch, old_oids):
    convert = online._convert_relation

    def inspect_cutover(op, candidate_schema, table, keys):
        before_scan = op.get_bind().scalar(
            text("SELECT sum(seq_tup_read) FROM pg_stat_xact_user_tables WHERE relid=ANY(:oids)"),
            {"oids": old_oids},
        )
        history = convert(op, candidate_schema, table, keys)
        scanned = op.get_bind().scalar(
            text("SELECT sum(seq_tup_read) FROM pg_stat_xact_user_tables WHERE relid=ANY(:oids)"),
            {"oids": old_oids},
        )
        assert scanned == before_scan
        return history

    monkeypatch.setattr(online, "_convert_relation", inspect_cutover)


@pytest.mark.asyncio
async def test_online_validation_keeps_readers_and_reuses_native_storage(monkeypatch):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        await _seed_references(context)
        connection, schema = context.connection, context.schema
        relation_names = [f"{schema}.{table}" for table in TABLES]
        old_indexes = await connection.fetch(
            "SELECT indexrelid,indrelid FROM pg_index WHERE indrelid=ANY($1::regclass[]) ORDER BY indexrelid",
            relation_names,
        )
        outbound = await connection.fetch(
            "SELECT oid,conrelid,confrelid FROM pg_constraint WHERE contype='f' AND conrelid=ANY($1::regclass[]) AND NOT confrelid=ANY($1::regclass[]) ORDER BY oid",
            relation_names,
        )
        validation_locks = []
        _watch_cutover_scans(monkeypatch, sorted({index["indrelid"] for index in old_indexes}))

        def inspect_validation(sync_connection, _cursor, statement, _parameters, _context, _executemany):
            if " VALIDATE CONSTRAINT " in statement:
                backend = sync_connection.exec_driver_sql("SELECT pg_backend_pid()").scalar()
                await_only(_check_validation_reader(context, backend, validation_locks))

        event.listen(context.engine.sync_engine, "after_cursor_execute", inspect_validation)
        try:
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        finally:
            event.remove(context.engine.sync_engine, "after_cursor_execute", inspect_validation)
        assert len(validation_locks) >= len(TABLES) + 2
        for index in old_indexes:
            assert (
                await connection.fetchrow(
                    "SELECT indexrelid,indrelid FROM pg_index WHERE indexrelid=$1", index["indexrelid"]
                )
                == index
            )
        for foreign_key in outbound:
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_constraint WHERE oid=$1)", foreign_key["oid"]
            )

        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=ANY($1::regclass[]) AND contype='f')",
            relation_names,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_after", ["preparation", "conversion", "foreign_key_validation", "reference_switch"])
async def test_online_migration_resumes_committed_phases(monkeypatch, stop_after):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        await _seed_references(context)
        connection, schema = context.connection, context.schema
        before = await connection.fetch(
            f"SELECT tableoid,* FROM {schema}.{TABLES[0]} ORDER BY dataset_id,resource_type,resource_id"
        )
        callback = (
            "_rebind_reference" if stop_after in {"foreign_key_validation", "reference_switch"} else "_convert_one"
        )
        original = getattr(online, callback)

        def interrupt(*args):
            if stop_after == "conversion" and args[2] == TABLES[0]:
                return original(*args)
            if stop_after == "reference_switch":
                original(*args)
            if stop_after == "foreign_key_validation":
                op, migration_schema, plan, reference = args
                root = online._root_reference(op.get_bind(), reference["oid"])
                replacement = "pd_dataset_fk_" + str(reference["oid"])
                with online._phase(op):
                    op.execute(
                        f"ALTER TABLE {root['relation']} ADD CONSTRAINT {online.quote(replacement)} {reference['definition']} NOT VALID"
                    )
                with online._phase(op, validation=True):
                    op.execute(f"ALTER TABLE {root['relation']} VALIDATE CONSTRAINT {online.quote(replacement)}")
            raise RuntimeError("injected_committed_phase_failure")

        monkeypatch.setattr(online, callback, interrupt)
        with pytest.raises(RuntimeError, match="injected_committed_phase_failure"):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        await _assert_fenced(context)
        assert (
            await connection.fetch(
                f"SELECT tableoid,* FROM {schema}.{TABLES[0]} ORDER BY dataset_id,resource_type,resource_id"
            )
            == before
        )
        plan = json.loads(await connection.fetchval(f"SELECT plan FROM {schema}.{online.PLAN_TABLE}"))
        assert not plan["complete"]
        monkeypatch.setattr(online, callback, original)
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert json.loads(await connection.fetchval(f"SELECT plan FROM {schema}.{online.PLAN_TABLE}"))["complete"]
        assert (
            await connection.fetch(
                f"SELECT tableoid,* FROM {schema}.{TABLES[0]} ORDER BY dataset_id,resource_type,resource_id"
            )
            == before
        )
        await connection.execute(f"DELETE FROM {schema}.{TABLES[0]} WHERE false")


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ("row_guard", "view"))
async def test_provider_unknown_dependencies_refuse_then_recover(monkeypatch, drift):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        connection, schema = context.connection, context.schema
        resource = f"{schema}.{TABLES[0]}"
        original_oid = await connection.fetchval("SELECT $1::regclass::oid", resource)
        before = await connection.fetch(
            f"SELECT tableoid,* FROM {resource} ORDER BY dataset_id,resource_type,resource_id"
        )
        index_query = (
            "SELECT indexrelid,relfilenode FROM pg_index JOIN pg_class ON oid=indexrelid "
            "WHERE indrelid=$1 ORDER BY indexrelid"
        )
        indexes = await connection.fetch(index_query, original_oid)
        dependency = f"{schema}.synthetic_dependency"
        if drift == "row_guard":
            await connection.execute(
                f"CREATE FUNCTION {dependency}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$"
            )
            await connection.execute(
                f"CREATE TRIGGER synthetic_guard BEFORE INSERT ON {resource} "
                f"FOR EACH ROW EXECUTE FUNCTION {dependency}()"
            )
            repair = f"DROP TRIGGER synthetic_guard ON {resource}; DROP FUNCTION {dependency}()"
            refusal = "provider_dataset_candidate_row_guard_drift"
        else:
            await connection.execute(f"CREATE VIEW {dependency} AS SELECT * FROM {resource}")
            repair = f"DROP VIEW {dependency}"
            refusal = "provider_dataset_candidate_dependency_drift"
        with pytest.raises(RuntimeError, match=refusal):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{online.PLAN_TABLE}") is None
        assert await connection.fetchval("SELECT $1::regclass::oid", resource) == original_oid
        await connection.execute(repair)
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        history = f"{schema}.pd_dataset_history_{original_oid}"
        assert await connection.fetchval("SELECT $1::regclass::oid", history) == original_oid
        assert await connection.fetch(index_query, original_oid) == indexes
        assert (
            await connection.fetch(f"SELECT tableoid,* FROM {resource} ORDER BY dataset_id,resource_type,resource_id")
            == before
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ("unvalidated_constraint", "invalid_index"))
async def test_provider_migration_preflight_refuses_invalid_native_storage(monkeypatch, drift):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        connection, schema = context.connection, context.schema
        resource = f"{schema}.{TABLES[0]}"
        original_oid = await connection.fetchval("SELECT $1::regclass::oid", resource)
        if drift == "unvalidated_constraint":
            constraint = await connection.fetchrow(
                "SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint "
                "WHERE conrelid=$1::regclass AND contype='f' AND conparentid=0 ORDER BY conname LIMIT 1",
                resource,
            )
            assert constraint is not None
            name = online.quote(constraint["conname"])
            await connection.execute(f"ALTER TABLE {resource} DROP CONSTRAINT {name}")
            await connection.execute(
                f"ALTER TABLE {resource} ADD CONSTRAINT {name} {constraint['definition']} NOT VALID"
            )
            repair = f"ALTER TABLE {resource} VALIDATE CONSTRAINT {name}"
        else:
            with pytest.raises(asyncpg.UniqueViolationError):
                await connection.execute(
                    f"CREATE UNIQUE INDEX CONCURRENTLY synthetic_invalid_index ON {resource} ((1))"
                )
            repair = f"DROP INDEX {schema}.synthetic_invalid_index"
        with pytest.raises(RuntimeError, match="provider_dataset_" + drift):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{online.PLAN_TABLE}") is None
        assert await connection.fetchval("SELECT $1::regclass::oid", resource) == original_oid
        await connection.execute(repair)
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert (
            await connection.fetchval("SELECT $1::regclass::oid", f"{schema}.pd_dataset_history_{original_oid}")
            == original_oid
        )


@pytest.mark.asyncio
async def test_provider_completed_retry_refuses_detached_history(monkeypatch):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        connection, schema = context.connection, context.schema
        resource = f"{schema}.{TABLES[0]}"
        original_oid = await connection.fetchval("SELECT $1::regclass::oid", resource)
        before = await connection.fetch(
            f"SELECT tableoid,* FROM {resource} ORDER BY dataset_id,resource_type,resource_id"
        )
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        history = f"{schema}.pd_dataset_history_{original_oid}"
        bound = await connection.fetchval(
            "SELECT pg_get_expr(relpartbound,oid) FROM pg_class WHERE oid=$1::regclass", history
        )
        await connection.execute(f"ALTER TABLE {resource} DETACH PARTITION {history}")
        with pytest.raises(RuntimeError, match="provider_dataset_migration_relation_drift"):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert (
            await connection.fetch(f"SELECT tableoid,* FROM {history} ORDER BY dataset_id,resource_type,resource_id")
            == before
        )
        await connection.execute(f"ALTER TABLE {resource} ATTACH PARTITION {history} {bound}")
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert (
            await connection.fetch(f"SELECT tableoid,* FROM {resource} ORDER BY dataset_id,resource_type,resource_id")
            == before
        )


@pytest.mark.asyncio
async def test_provider_migration_contention_refuses_then_recovers(monkeypatch):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        lock_key = "provider_dataset_candidates:" + context.schema_name
        await context.connection.execute("SELECT pg_advisory_lock(hashtextextended($1,0))", lock_key)
        try:
            with pytest.raises(RuntimeError, match="provider_dataset_migration_already_running"):
                await asyncio.wait_for(run_migration(context.engine, _candidate_migration(), "upgrade"), timeout=2)
            assert (
                await context.connection.fetchval("SELECT to_regclass($1)", f"{context.schema}.{online.PLAN_TABLE}")
                is None
            )
        finally:
            await context.connection.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", lock_key)
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert json.loads(await context.connection.fetchval(f"SELECT plan FROM {context.schema}.{online.PLAN_TABLE}"))[
            "complete"
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("converted", [False, True])
async def test_online_retry_rejects_replaced_relation(monkeypatch, converted):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        original = online._convert_one

        def interrupt(op, schema, table, plan):
            if converted and table == TABLES[0]:
                return original(op, schema, table, plan)
            raise RuntimeError("injected_relation_swap")

        monkeypatch.setattr(online, "_convert_one", interrupt)
        with pytest.raises(RuntimeError, match="injected_relation_swap"):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        connection, schema = context.connection, context.schema
        canonical = f"{schema}.{TABLES[0]}"
        displaced = f"{schema}.synthetic_displaced_resource"
        await connection.execute(f"ALTER TABLE {canonical} RENAME TO synthetic_displaced_resource")
        await connection.execute(f"CREATE TABLE {canonical}(LIKE {displaced}) PARTITION BY LIST(dataset_id)")
        replacement_oid = await connection.fetchval("SELECT $1::regclass::oid", canonical)
        monkeypatch.setattr(online, "_convert_one", original)
        with pytest.raises(RuntimeError, match="provider_dataset_migration_relation_drift"):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await connection.fetchval("SELECT $1::regclass::oid", canonical) == replacement_oid
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=$1)", replacement_oid
        )
        assert await connection.fetchval(f"SELECT count(*) FROM {displaced}") > 0


@pytest.mark.asyncio
async def test_online_migration_rejects_unknown_bulk_relationship(monkeypatch):
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        connection, schema = context.connection, context.schema
        resource = f"{schema}.{TABLES[0]}"
        referenced = f"{schema}.synthetic_partitioned_reference"
        await connection.execute(
            f"CREATE TABLE {referenced}(payload_hash varchar PRIMARY KEY) PARTITION BY LIST(payload_hash)"
        )
        keys = await connection.fetchval(f"SELECT string_agg(DISTINCT quote_literal(payload_hash),',') FROM {resource}")
        leaf = f"{schema}.synthetic_reference_leaf"
        await connection.execute(f"CREATE TABLE {leaf} PARTITION OF {referenced} FOR VALUES IN ({keys})")
        await connection.execute(f"INSERT INTO {referenced} SELECT DISTINCT payload_hash FROM {resource}")
        await connection.execute(
            f"ALTER TABLE {resource} ADD CONSTRAINT synthetic_partitioned_fk FOREIGN KEY(payload_hash) REFERENCES {referenced}(payload_hash)"
        )
        with pytest.raises(RuntimeError, match="provider_dataset_bulk_relationship_drift"):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await connection.fetchval("SELECT to_regclass($1)", f"{schema}.{online.PLAN_TABLE}") is None


async def _migrate_empty_history(context, monkeypatch, interrupted):
    if interrupted:
        finish = online._finish

        def interrupt_finish(*arguments):
            finish(*arguments)
            raise RuntimeError("synthetic_empty_finish_failure")

        with monkeypatch.context() as patch:
            patch.setattr(online, "_finish", interrupt_finish)
            with pytest.raises(RuntimeError, match="synthetic_empty_finish_failure"):
                await run_migration(context.engine, _candidate_migration(), "upgrade")
        await _assert_fenced(context)
    await run_migration(context.engine, _candidate_migration(), "upgrade")
    await run_migration(context.engine, _candidate_migration(), "upgrade")


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupted", (False, True))
async def test_empty_history_accepts_first_provider_publication(monkeypatch, interrupted):
    official_rows_by_table = {}

    async def seed_registry(connection, schema_name):
        await seed_official_dataset(connection, schema_name)
        for table in ("provider_directory_endpoint_dataset", TABLES[0]):
            official_rows_by_table[table] = [
                dict(snapshot_row) for snapshot_row in await connection.fetch(f'SELECT * FROM "{schema_name}".{table}')
            ]
        for table in (TABLES[0], "provider_directory_endpoint_dataset"):
            await connection.execute(f'DELETE FROM "{schema_name}".{table}')

    with monkeypatch.context() as patch:
        patch.setattr(publication_fixture, "seed_official_dataset", seed_registry)
        patch.setattr(publication_fixture, "insert_valid_cohort", AsyncMock())
        async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
            connection, schema = context.connection, context.schema
            assert await connection.fetchval(f"SELECT count(*) FROM {schema}.provider_directory_endpoint_dataset") == 0
            assert (
                await connection.fetchval(f"SELECT count(*) FROM {schema}.provider_directory_rooted_graph_acquisition")
                == 0
            )
            assert (
                await connection.fetchval(
                    f"SELECT count(*) FROM {schema}.provider_directory_uhc_flex_practitioner_acquisition"
                )
                == 0
            )
            await _migrate_empty_history(context, monkeypatch, interrupted)
            for table, table_rows in official_rows_by_table.items():
                if table == "provider_directory_endpoint_dataset":
                    table_rows = [{**snapshot_row, "status": "building"} for snapshot_row in table_rows]
                await connection.copy_records_to_table(
                    table,
                    schema_name=context.schema_name,
                    columns=list(table_rows[0]),
                    records=[tuple(snapshot_row.values()) for snapshot_row in table_rows],
                )
            for header in official_rows_by_table["provider_directory_endpoint_dataset"]:
                await connection.execute(
                    f"UPDATE {schema}.provider_directory_endpoint_dataset SET status=$2 WHERE dataset_id=$1",
                    header["dataset_id"],
                    header["status"],
                )
            await insert_valid_cohort(connection, context.schema_name)
            identity, claim, query_result = await _claim(context)
            await complete_provider_directory_rooted_graph_result(claim, query_result, database=context.database)
            assert await connection.fetchval(
                f"SELECT count(*) FROM {schema}.provider_directory_rooted_graph_resource WHERE acquisition_id=$1",
                identity.acquisition_id,
            ) == len(query_result.resources)
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE contype='f' AND conrelid=ANY($1::regclass[]) "
                "AND confrelid IN (SELECT oid FROM pg_class WHERE relnamespace=$2::regnamespace AND relname LIKE 'pd_dataset_history_%'))",
                [f"{schema}.{table}" for table in TABLES],
                schema,
            )


async def _seed_mixed_dataset_families(context, monkeypatch):
    """Retain a real practitioner publication beside a longer generic key."""
    with monkeypatch.context() as patch:
        patch.setattr(
            candidate_fixture.practitioner_store,
            "prepare_dataset_candidate",
            candidate_fixture._historical_dataset_storage,
        )
        patch.setattr(
            candidate_fixture.practitioner_materialization,
            "copy_dataset_candidate_rows",
            candidate_fixture._copy_historical_dataset,
        )
        practitioner = await candidate_fixture._publish_legacy_root(context.database)
    assert len(practitioner.dataset_id) == 55
    long_dataset_id = "synthetic-long-generic-" + "g" * 43 + "\\'é"
    assert len(long_dataset_id) == 69
    await context.connection.execute(
        f"INSERT INTO {context.schema}.provider_directory_endpoint_dataset"
        "(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) "
        "VALUES($1,'endpoint-official','building',false,1,'{}')",
        long_dataset_id,
    )
    await context.connection.execute(
        f"INSERT INTO {context.schema}.{TABLES[0]}"
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "VALUES($1,'Organization','synthetic-long-resource',repeat('a',64),'{}')",
        long_dataset_id,
    )
    return practitioner.dataset_id, long_dataset_id


async def _dataset_storage_catalog(connection, relation_oids):
    """Capture physical heap and index identities without scanning their rows."""
    heaps = await connection.fetch(
        "SELECT oid,relfilenode FROM pg_class WHERE oid=ANY($1::oid[]) ORDER BY oid", relation_oids
    )
    indexes = await connection.fetch(
        "SELECT indexrelid,indrelid,relfilenode,indisvalid,indisready "
        "FROM pg_index JOIN pg_class ON oid=indexrelid WHERE indrelid=ANY($1::oid[]) ORDER BY indexrelid",
        relation_oids,
    )
    return heaps, indexes


def _restore_legacy_shared_bounds(op, schema, plan):
    """Recreate the persisted predecessor plan before any conversion commits."""
    with online._phase(op):
        for header in (
            "provider_directory_uhc_flex_practitioner_dataset",
            "provider_directory_rooted_graph_dataset",
        ):
            op.execute(f"DROP TRIGGER {online.FENCE} ON {online._relation(schema, header)}")
        bound = "dataset_id IN (" + ",".join(online.literal(key) for key in plan["keys"]) + ")"
        for table in TABLES:
            plan["tables"][table].pop("keys")
            relation = online._relation(schema, table)
            op.execute(f"ALTER TABLE {relation} DROP CONSTRAINT pd_dataset_history_bound")
            op.execute(f"ALTER TABLE {relation} ADD CONSTRAINT pd_dataset_history_bound CHECK ({bound}) NOT VALID")
        online._save_plan(op, schema, plan)


async def _fail_legacy_provenance_attach(context, monkeypatch):
    """Commit four old conversions and reproduce the narrow-key attach error."""
    convert_one = online._convert_one

    def convert_legacy(op, schema, table, plan):
        if table == TABLES[0]:
            _restore_legacy_shared_bounds(op, schema, plan)
        return convert_one(op, schema, table, plan)

    with monkeypatch.context() as patch:
        patch.setattr(online, "_convert_one", convert_legacy)
        with pytest.raises(DBAPIError) as failure:
            await run_migration(context.engine, _candidate_migration(), "upgrade")
    assert failure.value.orig.sqlstate == "22001"
    plan = json.loads(await context.connection.fetchval(f"SELECT plan FROM {context.schema}.{online.PLAN_TABLE}"))
    assert not plan["complete"]
    assert all("keys" not in identity for identity in plan["tables"].values())
    assert [bool(plan["tables"][table]["parent_oid"]) for table in TABLES] == [True] * 4 + [False] * 2
    await _assert_fenced(context)
    return plan


async def _assert_family_history_bounds(context, practitioner_id, long_dataset_id):
    """Only a family's frozen identities may route to its preserved heap."""
    connection, schema = context.connection, context.schema
    plan = json.loads(await connection.fetchval(f"SELECT plan FROM {schema}.{online.PLAN_TABLE}"))
    assert plan["complete"]
    generic_keys = await connection.fetchval(
        f"SELECT array_agg(dataset_id ORDER BY dataset_id) FROM {schema}.provider_directory_endpoint_dataset"
    )
    assert long_dataset_id in generic_keys
    for table in TABLES:
        identity = plan["tables"][table]
        expected_keys = generic_keys if table in TABLES[:4] else [practitioner_id] if table == TABLES[4] else []
        assert identity["keys"] == expected_keys
        assert await connection.fetchval("SELECT $1::regclass::oid", f"{schema}.{table}") == identity["parent_oid"]
        assert await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=$1 AND inhparent=$2)",
            identity["original_oid"],
            identity["parent_oid"],
        ) == bool(expected_keys)
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
        "WHERE c.relnamespace=$1::regnamespace AND t.tgname=$2)",
        schema,
        online.FENCE,
    )
    return plan


async def _assert_family_headers_fenced(context):
    """Every dataset family remains write-fenced until the migration completes."""
    for header in (
        "provider_directory_endpoint_dataset",
        "provider_directory_uhc_flex_practitioner_dataset",
        "provider_directory_rooted_graph_dataset",
    ):
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="migration_incomplete_rerun_migration"):
            await context.connection.execute(f"DELETE FROM {context.schema}.{header} WHERE false")


async def _interrupt_family_conversion(context, monkeypatch, *, convert_generic=False):
    """Stop after committed preparation or all four generic cutovers."""
    convert_one = online._convert_one

    def interrupt(op, schema, table, plan):
        if convert_generic and table in TABLES[:4]:
            return convert_one(op, schema, table, plan)
        raise RuntimeError("injected_family_conversion_failure")

    with monkeypatch.context() as patch:
        patch.setattr(online, "_convert_one", interrupt)
        with pytest.raises(RuntimeError, match="injected_family_conversion_failure"):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
    await _assert_family_headers_fenced(context)
    plan = json.loads(await context.connection.fetchval(f"SELECT plan FROM {context.schema}.{online.PLAN_TABLE}"))
    assert not plan["complete"] and all("keys" in identity for identity in plan["tables"].values())
    return plan


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_partial", (False, True))
@pytest.mark.parametrize("string_setting", ("on", "off"))
async def test_dataset_family_bounds_preserve_storage(monkeypatch, legacy_partial, string_setting):
    """Fresh and retried bounds preserve wide quoted keys and narrow provenance."""
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        practitioner_id, long_dataset_id = await _seed_mixed_dataset_families(context, monkeypatch)
        connection, schema = context.connection, context.schema
        relation_oids = [await connection.fetchval("SELECT $1::regclass::oid", f"{schema}.{table}") for table in TABLES]
        original_storage = await _dataset_storage_catalog(connection, relation_oids)
        original_rows_by_table = {
            table: await connection.fetch(
                f"SELECT tableoid,* FROM {schema}.{table} ORDER BY dataset_id,resource_type,resource_id"
            )
            for table in (TABLES[0], TABLES[4], TABLES[5])
        }

        def set_literal_setting(dbapi_connection, _record, _proxy):
            cursor = dbapi_connection.cursor()
            cursor.execute(f"SET standard_conforming_strings='{string_setting}'")
            cursor.close()

        event.listen(context.engine.sync_engine, "checkout", set_literal_setting)
        _watch_cutover_scans(monkeypatch, relation_oids)
        partial = await _fail_legacy_provenance_attach(context, monkeypatch) if legacy_partial else None
        committed_parents = [partial["tables"][table]["parent_oid"] for table in TABLES[:4]] if partial else []
        committed_storage = await _dataset_storage_catalog(connection, committed_parents)
        if partial:
            repaired = await _interrupt_family_conversion(context, monkeypatch)
            assert repaired["tables"][TABLES[4]]["keys"] == [practitioner_id]
            assert repaired["tables"][TABLES[5]]["keys"] == []
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        plan = await _assert_family_history_bounds(context, practitioner_id, long_dataset_id)
        assert await _dataset_storage_catalog(connection, relation_oids) == original_storage
        assert await _dataset_storage_catalog(connection, committed_parents) == committed_storage
        if partial:
            assert [plan["tables"][table]["parent_oid"] for table in TABLES[:4]] == committed_parents
        for table, retained_rows in original_rows_by_table.items():
            assert (
                await connection.fetch(
                    f"SELECT tableoid,* FROM {schema}.{table} ORDER BY dataset_id,resource_type,resource_id"
                )
                == retained_rows
            )
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert json.loads(await connection.fetchval(f"SELECT plan FROM {schema}.{online.PLAN_TABLE}")) == plan


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ("partial_key_plan", "bound_drift"))
async def test_dataset_family_retry_refuses_drift(monkeypatch, drift):
    """An ambiguous plan or changed same-OID partition bound cannot resume."""
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False) as context:
        await _seed_mixed_dataset_families(context, monkeypatch)
        connection, schema = context.connection, context.schema
        plan = await _interrupt_family_conversion(context, monkeypatch, convert_generic=drift == "bound_drift")
        relation_oids = [identity["original_oid"] for identity in plan["tables"].values()]
        original_storage = await _dataset_storage_catalog(connection, relation_oids)
        if drift == "partial_key_plan":
            plan["tables"][TABLES[4]].pop("keys")
            await connection.execute(f"UPDATE {schema}.{online.PLAN_TABLE} SET plan=$1::jsonb", json.dumps(plan))
        else:
            identity = plan["tables"][TABLES[2]]
            history = f"{schema}.pd_dataset_history_{identity['original_oid']}"
            assert await connection.fetchval(f"SELECT count(*) FROM {history}") == 0
            assert len(identity["keys"]) > 1
            await connection.execute(f"ALTER TABLE {schema}.{TABLES[2]} DETACH PARTITION {history}")
            await connection.execute(
                f"ALTER TABLE {schema}.{TABLES[2]} ATTACH PARTITION {history} "
                f"FOR VALUES IN ({online.literal(identity['keys'][0])})"
            )
        with pytest.raises(RuntimeError, match="provider_dataset_migration_" + drift):
            await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await _dataset_storage_catalog(connection, relation_oids) == original_storage
        assert json.loads(await connection.fetchval(f"SELECT plan FROM {schema}.{online.PLAN_TABLE}")) == plan
        await _assert_family_headers_fenced(context)
