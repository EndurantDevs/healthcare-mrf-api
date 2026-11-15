# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connected shared entity keys through ordinary pinned partial preparation."""

from contextlib import asynccontextmanager
from uuid import UUID

import pytest

from process import entity_address_batch as batch
from process import entity_address_ordinary_preparation as ordinary
from process import entity_address_rows_native as rows
from process import entity_address_rows_preparation as preparation
from tests.entity_address_batch_postgres_support import database_fixture, fixtures
from tests.entity_address_partial_projection_support import AVAILABLE, _create_tables, _seed


@asynccontextmanager
async def group_database(monkeypatch):
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await _create_tables(database, schema)
        await _seed(database, schema)
        await database.status(
            f"CREATE TABLE {schema}.entity_address_unified AS SELECT 'npi'::varchar AS entity_type,"
            "npi::varchar AS entity_id,npi,address_key,first_line,second_line,city_name,"
            "COALESCE(state_name,state_code,'')::varchar AS state_name,postal_code,country_code,"
            "source_record_id::varchar AS location_key,ARRAY[source_record_id]::varchar[] AS source_record_ids,"
            "ARRAY['provider_directory_fhir']::varchar[] AS address_sources,"
            "NULL::varchar AS city_norm,NULL::varchar AS state_code,NULL::varchar AS zip5 "
            f"FROM {schema}.provider_directory_address_overlay"
        )
        await database.status(
            f"CREATE TABLE {schema}.desired_overlay AS TABLE {schema}.provider_directory_address_overlay"
        )
        await database.status(f"CREATE INDEX ON {schema}.desired_overlay(npi)")
        await database.status(f"TRUNCATE {schema}.provider_directory_address_overlay")
        preparation = sql.candidate_preparation
        pin = preparation.ProviderDirectoryAddressDatasetPin
        pins = (
            pin("selected", "endpoint-a", "dataset-a", "hash-a", "run-a"),
            pin("sibling", "endpoint-a", "dataset-a", "hash-a", "run-a"),
            pin("other", "endpoint-b", "dataset-b", "hash-b", "run-b"),
        )
        inputs = preparation.ProviderDirectoryAddressPreparationInput(
            dataset_pins=pins,
            overlay_table="desired_overlay",
            address_alias_generation=0,
            semantic_as_of="2026-01-03",
            ordinary_serving_peer_pins=(pins[2],),
        )
        token = preparation._PREPARATION.set(inputs)
        try:
            yield sql, database, schema
        finally:
            preparation._PREPARATION.reset(token)


async def assert_equal_tables(database, schema, expected, actual):
    assert not await database.scalar(
        f"SELECT EXISTS((SELECT * FROM {schema}.{expected} EXCEPT ALL SELECT * FROM {schema}.{actual}) "
        f"UNION ALL (SELECT * FROM {schema}.{actual} EXCEPT ALL SELECT * FROM {schema}.{expected}))"
    )


async def assert_partial_groups(monkeypatch):
    async with group_database(monkeypatch) as (sql, database, schema):
        source_select = sql._provider_directory_partial_overlay_source_select(
            schema, AVAILABLE, source_ids=["selected"], run_id="run-a"
        )
        await database.status(
            sql._prepare_partial_affected_groups_sql(schema, "expected", [source_select], source_ids=["selected"])
        )
        caches = []

        @batch.scope
        async def build():
            await batch.prepare_partial(sql, schema, "partial_run", AVAILABLE, source_ids=["selected"], run_id="run-a")
            cache = batch._TABLE.get()
            assert cache is not None
            caches.append(cache)
            assert await database.scalar(f"SELECT count(*) FROM {cache} WHERE native_fields IS NOT NULL") > 0
            selected = sql._provider_directory_partial_overlay_source_select(
                schema, AVAILABLE, source_ids=["selected"], run_id="run-a"
            )
            statement = sql._prepare_partial_affected_groups_sql(schema, "actual", [selected], source_ids=["selected"])
            assert "npi_address" not in statement and "entity_street_key" in statement
            await database.status(statement)
            await assert_equal_tables(database, schema, "expected", "actual")
            await database.status(
                f"INSERT INTO {schema}.entity_address_unified SELECT entity_type,entity_id,npi,address_key,"
                "'10 Main St',second_line,city_name,state_name,postal_code,country_code,"
                "'cache-miss',source_record_ids,address_sources,city_norm,state_code,zip5 "
                f"FROM {schema}.entity_address_unified WHERE location_key LIKE :pattern LIMIT 1",
                pattern="%:one",
            )
            await assert_matching_and_evidence(sql, database, schema)
            previous = cache
            await batch.prepare_raw(sql, schema, "entity_address_unified")
            assert previous in batch._TABLE.get()
            assert await database.scalar(f"SELECT count(*)=count(DISTINCT lookup_key) FROM {batch._TABLE.get()}")

        async with ordinary.scope(schema, set()):
            await build()
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=caches[0])
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema=:schema "
            "AND (table_name LIKE '%_address_batch_%' OR table_name LIKE '%_pd_projection'))",
            schema=schema,
        )


async def assert_matching_and_evidence(sql, database, schema):
    token = batch._TABLE.set(None)
    try:
        match_expected = sql._entity_address_evidence_group_match_sql("g", "r")
        evidence_expected = sql._load_multi_source_evidence_base_sql(
            schema, "entity_address_unified", "evidence_expected", evidence_shards=3
        )
    finally:
        batch._TABLE.reset(token)
    query = (
        f"SELECT r.location_key,g.entity_id,g.street_key FROM {schema}.entity_address_unified r "
        f"CROSS JOIN {schema}.expected g WHERE "
    )
    expected = await database.all(query + match_expected)
    actual = await database.all(query + sql._entity_address_evidence_group_match_sql("g", "r"))
    assert sorted(map(tuple, actual)) == sorted(map(tuple, expected))
    for name in ("evidence_expected", "evidence_actual"):
        await database.status(sql._prepare_multi_source_evidence_table_sql(schema, name))
    await database.status(evidence_expected)
    await database.status(
        sql._load_multi_source_evidence_base_sql(schema, "entity_address_unified", "evidence_actual", evidence_shards=3)
    )
    await assert_equal_tables(database, schema, "evidence_expected", "evidence_actual")


async def assert_cached_raw_collation(monkeypatch, has_missing_tuple):
    """Qualify unfiltered raw columns even when every exact tuple is already cached."""
    from process import entity_address_batch as batch

    async with group_database(monkeypatch) as (sql, database, schema):
        collation = await database.scalar(
            "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.collname) FROM pg_collation c "
            "JOIN pg_namespace n ON n.oid=c.collnamespace WHERE c.collname LIKE 'tr-%' "
            "AND c.collprovider='i' ORDER BY c.collname LIMIT 1"
        )
        if collation is None:
            pytest.skip("Turkish ICU collation is unavailable")
        for table in ("desired_overlay", "entity_address_unified"):
            await database.status(f"UPDATE {schema}.{table} SET first_line='I ROAD',city_name='I',state_name='i'")

        @batch.scope
        async def compare():
            """Build a C-qualified group cache, then read identical Turkish raw tuples."""
            await batch.prepare_partial(
                sql, schema, "collation_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
            )
            previous = batch._TABLE.get()
            assert previous is not None
            assert await database.scalar(f"SELECT bool_and(native_fields->>'entity_city_norm'='i') FROM {previous}")
            for field in batch.stage.ADDRESS_FIELDS:
                await database.status(
                    f"ALTER TABLE {schema}.entity_address_unified ALTER COLUMN {field} TYPE varchar COLLATE {collation}"
                )
            if has_missing_tuple:
                await database.status(f"UPDATE {schema}.entity_address_unified SET first_line='I OTHER' WHERE npi=123")
            await batch.prepare_raw(sql, schema, "entity_address_unified")
            assert batch._TABLE.get() is None
            normalizers = batch.raw_normalized_sql(sql)
            assert "native_fields->>" not in normalizers
            actual = await database.all(f"SELECT {normalizers} FROM {schema}.entity_address_unified r")
            assert actual and all(row[1] == "İ" and row[2] is None for row in actual)

        await compare()


async def assert_partial_failure_cleanup(monkeypatch, failure):
    """Retire committed group inputs while preserving a pre-existing colliding table."""
    import asyncio

    from process import entity_address_batch as batch

    async with group_database(monkeypatch) as (sql, database, schema):
        monkeypatch.setattr(batch, "uuid4", lambda: UUID(int=0))
        live_name = batch.scratch_name(sql, "failed_run", "_pd_live_inputs")
        if failure == "collision":
            await database.status(f"CREATE TABLE {schema}.{live_name}(sentinel int)")
            await database.status(f"INSERT INTO {schema}.{live_name} VALUES(7)")

        @batch.scope
        async def prepare():
            """Enter the actual pinned group path before the injected consumer failure."""
            await batch.prepare_partial(sql, schema, "failed_run", AVAILABLE, source_ids=["selected"], run_id="run-a")
            assert batch._TABLE.get() is not None and batch._LIVE.get() is not None
            raise asyncio.CancelledError()

        with pytest.raises(
            asyncio.CancelledError if failure == "cancel" else Exception,
            match=None if failure == "cancel" else "already exists",
        ):
            await prepare()
        assert batch._TABLE.get() is None and batch._LIVE.get() is None
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema=:schema "
            "AND (table_name LIKE '%_address_batch_%' OR table_name LIKE '%_pd_projection'))",
            schema=schema,
        )
        if failure == "collision":
            assert await database.scalar(f"SELECT sentinel FROM {schema}.{live_name}") == 7
        else:
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.{live_name}")


async def assert_source_case_parity(monkeypatch, locale, qualified):
    async with database_fixture(monkeypatch) as (sql, database, schema):
        collation = await database.scalar(
            "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.collname) FROM pg_collation c "
            "JOIN pg_namespace n ON n.oid=c.collnamespace WHERE c.collname LIKE :locale "
            "AND c.collprovider='i' ORDER BY c.collname LIMIT 1",
            locale=locale + "-%",
        )
        if collation is None:
            pytest.skip("the requested ICU collation is unavailable")
        for field in rows.TEXT_COLUMNS:
            await database.status(
                f"ALTER TABLE {schema}.source_rows ALTER COLUMN {field} TYPE varchar COLLATE {collation}"
            )
        await database.status(
            f"UPDATE {schema}.source_rows SET first_line='I ROAD',city_name='I',state_name='i',"
            "country_code='unitedstatesofamerica',telephone_number='3125550102 EXTENSION 07' WHERE entity_id='office-4'"
        )
        await database.status(f"UPDATE {schema}.source_rows SET city_name='İstanbul' WHERE entity_id='office-3'")
        source_select = fixtures.source_select_sql(schema)
        await database.status(sql._insert_raw_from_source_sql(schema, "raw_expected", source_select))
        selected_statements = []

        async def consume(statement, *, native_prepared):
            """Observe the actual qualified projection and consume its native rows."""
            assert native_prepared
            selected_statements.append(statement)
            await database.status(
                sql._insert_raw_from_source_sql(schema, "raw_actual", statement, native_prepared=True)
            )

        await preparation.run_prepared_source(sql, schema, source_select, consume)
        assert ("CASE WHEN p.native_case_safe THEN" in selected_statements[0]) is qualified
        assert ("CASE WHEN p.native_contact_safe THEN" in selected_statements[0]) is qualified
        assert not await database.scalar(
            f"SELECT EXISTS((SELECT * FROM {schema}.raw_actual EXCEPT ALL SELECT * FROM {schema}.raw_expected) UNION ALL "
            f"(SELECT * FROM {schema}.raw_expected EXCEPT ALL SELECT * FROM {schema}.raw_actual))"
        )
        await fixtures.assert_no_row_scratch(database, schema)


async def assert_projection_reader_drain(monkeypatch, failure):
    """Keep actual SQL-only projection readers alive until sibling cleanup finishes."""
    import asyncio

    async with group_database(monkeypatch) as (sql, database, schema):
        monkeypatch.setattr(sql.row_preparation, "native_preparation_module", lambda: None)
        started, stopped = asyncio.Event(), asyncio.Event()
        projection_relations, reader_tasks = [], []

        @batch.scope
        async def load_sources():
            """Exercise source gathering with committed projection and no native cache."""
            await batch.prepare_partial(sql, schema, "drain_run", AVAILABLE, source_ids=["selected"], run_id="run-a")
            assert batch._TABLE.get() is None and batch._LIVE.get() is None
            relation = batch.projection._ACTIVE.get().relation
            projection_relations.append(relation)

            async def reader():
                """Retain a real projection read through asynchronous child finalization."""
                reader_tasks.append(asyncio.current_task())
                assert await database.scalar(f"SELECT count(*) FROM {relation}") > 0
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(0)
                    assert await database.scalar("SELECT to_regclass(:name) IS NOT NULL", name=relation)
                    stopped.set()

            async def stop():
                """Fail after the projection reader starts or wait for caller cancellation."""
                await started.wait()
                if failure == "consumer":
                    raise RuntimeError("source failed")
                await asyncio.Event().wait()

            await batch.gather(reader(), stop())

        operation = asyncio.create_task(load_sources())
        ready = asyncio.create_task(started.wait())
        try:
            async with asyncio.timeout(5):
                await asyncio.wait((operation, ready), return_when=asyncio.FIRST_COMPLETED)
                if operation.done():
                    await operation
                if failure == "caller":
                    operation.cancel()
                with pytest.raises(asyncio.CancelledError if failure == "caller" else RuntimeError):
                    await operation
            assert stopped.is_set()
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=projection_relations[0])
        finally:
            for task in (operation, ready, *reader_tasks):
                if not task.done():
                    task.cancel()
            await asyncio.gather(operation, ready, *reader_tasks, return_exceptions=True)


async def assert_run_named_entity_scratch(monkeypatch):
    """Expose every committed entity input/cache to the existing logical run reaper."""
    import importlib

    fhir = importlib.import_module("process.provider_directory_fhir")
    control_run_id = "run_" + "a" * 32
    async with group_database(monkeypatch) as (sql, database, schema):
        names = []

        @batch.scope
        async def materialize(ctx):
            """Use the actual entrypoint context while preparing group and raw caches."""
            assert ctx["context"]["control_run_id"] == control_run_id
            await batch.prepare_partial(sql, schema, "owned_run", AVAILABLE, source_ids=["selected"], run_id="run-a")
            assert batch._TABLE.get() is not None and batch._LIVE.get() is not None
            await batch.prepare_raw(sql, schema, "entity_address_unified")
            rows = await database.all(
                "SELECT table_name FROM information_schema.tables WHERE table_schema=:schema", schema=schema
            )
            names.extend(row[0] for row in rows if fhir._is_address_overlay_stage_table_name(row[0]))
            assert len(names) == 5
            owner_token = fhir._address_overlay_stage_owner_token(control_run_id)
            assert all(name.rsplit("_", 1)[-1].startswith(owner_token) for name in names)

        await materialize({"context": {"control_run_id": control_run_id}})
        assert batch._OWNER.get() is None
        for name in names:
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.{name}")
