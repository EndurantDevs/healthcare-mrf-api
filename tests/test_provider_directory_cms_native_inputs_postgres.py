# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual revision triggers and short native fences; no complete address builder claim."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_native_inputs as inputs
from tests.cms_native_inputs_postgres_support import (
    create_fixed_input_models,
    enroll_native_input_custody,
    native_input_actor,
    source_input_custody,
)
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_provider_directory_cms_serving_receipt_postgres import _apply, _migration

_DATASET_INPUTS = {
    "provider_directory_dataset_network_plan": (
        "dataset_id text, network_resource_id text, insurance_plan_resource_id text, "
        "PRIMARY KEY (dataset_id,network_resource_id,insurance_plan_resource_id)",
        "('dataset-a','network-a','plan-a')",
    ),
    "provider_directory_dataset_affiliation_organization": (
        "dataset_id text, participating_organization_resource_id text, affiliation_resource_id text, "
        "PRIMARY KEY (dataset_id,participating_organization_resource_id,affiliation_resource_id)",
        "('dataset-a','organization-a','affiliation-a')",
    ),
    "provider_directory_dataset_insurance_plan": (
        "dataset_id text, resource_id text, payload_hash text, payload_json json, PRIMARY KEY (dataset_id,resource_id)",
        "('dataset-a','plan-a',repeat('a',64),'{\"status\":\"active\",\"plan_identifier\":\"plan-a\"}')",
    ),
}


async def _seed_authorities(database, schema):
    """Use real NPI migration and valid synthetic native publication pointers."""
    async with database.engine.begin() as connection:
        await connection.run_sync(lambda sync: _apply(sync, "20260914120000_npi_result_generation"))
        await connection.run_sync(lambda sync: _apply(sync, "20260930120000"))
        await connection.execute(text(_migration("20261019130000")._counter_sql(schema)))
    await database.status(f'''CREATE TABLE "{schema}".reference_family_result_generation (
        importer_id text PRIMARY KEY,local_lineage_id uuid,local_generation bigint,
        origin_lineage_id uuid,origin_generation bigint,published_at timestamptz,relation_oids bigint[],
        source_revision_tracked boolean NOT NULL DEFAULT false,
        CONSTRAINT reference_family_result_generation_shape_check CHECK (local_generation>=0))''')

    async with database.engine.begin() as connection:

        def named(sync):
            from alembic.migration import MigrationContext
            from alembic.operations import Operations

            with Operations.context(MigrationContext.configure(sync)):
                for prefix in ("20261114160000", "20261114180000"):
                    migration = _migration(prefix)
                    migration._schema = lambda: schema
                    migration.upgrade()

        await connection.run_sync(named)
        counter = _migration("20261114190000")
        check, key = counter._key_sql(schema)
        await connection.execute(text(check))
        await connection.execute(
            text(counter._function_sql(schema, names=("cms_native_input_advance", "cms_native_input_revision_guard")))
        )
        await connection.execute(text(key))
    for family in inputs._FAMILIES:
        if family == "mrf":
            continue
        namespace = "tiger" if family == "tiger" else schema
        names = inputs.reference.RELATION_NAMES_BY_IMPORTER[family]
        for name in names:
            await database.status(f'CREATE TABLE IF NOT EXISTS "{namespace}"."{name}" (value int)')
        await database.status(
            f'''INSERT INTO "{schema}".reference_family_result_generation
            (importer_id,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,serving_schema,serving_relation_names)
            VALUES (:family,:lineage,1,:lineage,1,now(),:namespace,:names)''',
            family=family,
            lineage=uuid4(),
            namespace=namespace,
            names=list(names),
        )
    await database.status(
        f'''UPDATE "{schema}".npi_result_generation SET local_generation=1,
        origin_lineage_id=local_lineage_id,origin_generation=1,published_at=now(),serving_schema=:schema,serving_relation_names=:names''',
        schema=schema,
        names=list(inputs.npi.RELATION_NAMES),
    )


@asynccontextmanager
async def _fixture(monkeypatch):
    """Create only a disposable database's UUID schema and its exclusively owned TIGER schema."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        await database.status("CREATE SCHEMA tiger")
        try:
            await create_fixed_input_models(database, schema)
            fixed_names = {"doctor_clinician_address", "geo_zip_lookup"}
            for namespace, name in inputs._relations(schema):
                if namespace == schema and name in fixed_names:
                    continue
                columns = _DATASET_INPUTS[name][0] if name in _DATASET_INPUTS else "value int"
                await database.status(f'CREATE TABLE "{namespace}"."{name}" ({columns})')
            await _seed_authorities(database, schema)
            async with source_input_custody(database, schema, monkeypatch, fixed_inputs=True) as component:
                await enroll_native_input_custody(database, schema, component)
                async with native_input_actor(database) as connection:
                    await inputs.register_native_address_inputs(connection, schema)
                yield database, schema
        finally:
            await database.status("DROP SCHEMA tiger CASCADE")


async def _capture(database, schema):
    async with native_input_actor(database, readonly=True) as connection:
        return await inputs.capture_native_address_input_fence(connection, schema)


async def _assert(database, schema, fence):
    async with native_input_actor(database) as connection:
        await inputs.assert_native_address_input_fence(connection, schema, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO {relation} VALUES (2)",
        "UPDATE {relation} SET value=2",
        "DELETE FROM {relation}",
        "TRUNCATE {relation}",
    ],
)
async def test_same_oid_mutations_invalidate_native_fence(monkeypatch, statement):
    """Every source DML event changes proof without requiring a physical replacement."""
    async with _fixture(monkeypatch) as (database, schema):
        relation = f'"{schema}".address_alias_v1'
        await database.status(f"INSERT INTO {relation} VALUES (1)")
        fence = await _capture(database, schema)
        await _assert(database, schema, fence)
        await database.status(statement.format(relation=relation))
        changed = await _capture(database, schema)
        assert changed["relations"] == fence["relations"]
        assert changed["revisions"] != fence["revisions"]
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)


@pytest.mark.asyncio
async def test_named_input_replacement_advances_counter_in_the_atomic_swap(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        async with native_input_actor(database) as session:
            await session.execute(text(f'CREATE TABLE "{schema}".incoming_nucc(value int)'))
            await session.execute(text(f'INSERT INTO "{schema}".incoming_nucc VALUES (7)'))
            prepared = await inputs.prepare_local_native_input_publication(
                session, ledger_schema=schema, relations=(("nucc_taxonomy", "incoming_nucc"),)
            )
            await session.execute(text(f'ALTER TABLE "{schema}".nucc_taxonomy RENAME TO old_nucc'))
            await session.execute(text(f'ALTER TABLE "{schema}".incoming_nucc RENAME TO nucc_taxonomy'))
            await inputs.finish_local_native_input_publication(session, prepared=prepared)
        changed = await _capture(database, schema)
        assert changed["relations"] == fence["relations"]
        before = next(row for row in fence["revisions"] if row["table_name"] == "nucc_taxonomy")
        after = next(row for row in changed["revisions"] if row["table_name"] == "nucc_taxonomy")
        assert after["revision"] == before["revision"] + 1
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)
        assert await database.scalar(f'SELECT count(*) FROM "{schema}".{inputs._TABLE}') == len(fence["revisions"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["DISABLE TRIGGER cms_native_input_mutation", "ENABLE TRIGGER cms_native_input_mutation"]
)
async def test_registration_never_repairs_tampered_existing_trigger(monkeypatch, mutation):
    """Disabled or replica-bypassable tracking fails rather than resetting its baseline."""
    async with _fixture(monkeypatch) as (database, schema):
        await database.status(f'ALTER TABLE "{schema}".nucc_taxonomy {mutation}')
        with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
            async with native_input_actor(database) as connection:
                await inputs.register_native_address_inputs(connection, schema)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement", ["UPDATE {table} SET revision=revision+1", "DELETE FROM {table}", "TRUNCATE {table}"]
)
async def test_revision_history_cannot_be_rewritten_directly(monkeypatch, statement):
    """The actual migration rejects direct history mutation and retains the valid fence."""
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        with pytest.raises(DBAPIError, match="revision_immutable"):
            await database.status(statement.format(table=f'"{schema}".{inputs._TABLE}'))
        await _assert(database, schema, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("table", ["address_archive_v2", *_DATASET_INPUTS])
async def test_source_writer_fences_cutover_and_cancellation_rolls_back(monkeypatch, table):
    """No wait or build-long locks: final contention rejects and cancelled source work vanishes."""
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        started = asyncio.Event()

        async def write_source():
            async with database.engine.begin() as connection:
                values = _DATASET_INPUTS[table][1] if table in _DATASET_INPUTS else "(7)"
                await connection.execute(text(f'INSERT INTO "{schema}"."{table}" VALUES {values}'))
                started.set()
                await asyncio.Event().wait()

        writer = asyncio.create_task(write_source())
        try:
            await asyncio.wait_for(started.wait(), 5)
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                await asyncio.wait_for(_assert(database, schema, fence), 5)
        finally:
            writer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await writer
        await _assert(database, schema, fence)
        assert await database.scalar(f'SELECT count(*) FROM "{schema}"."{table}"') == 0


@pytest.mark.asyncio
async def test_optional_creation_and_view_substitution_fail_closed(monkeypatch):
    """An absent optional input is pinned, and a view cannot hide untracked base writes."""
    async with _fixture(monkeypatch) as (database, schema):
        await database.status(f'DROP TABLE "{schema}".provider_enrichment_summary')
        fence = await _capture(database, schema)
        await database.status(f'CREATE VIEW "{schema}".provider_enrichment_summary AS SELECT 1 AS value')
        with pytest.raises(RuntimeError, match="requires_persistent_heap"):
            await _assert(database, schema, fence)


@pytest.mark.asyncio
async def test_content_registration_does_not_fabricate_native_acceptance(monkeypatch):
    """A generationless native family remains unacceptable despite valid content tracking."""
    async with _fixture(monkeypatch) as (database, schema):
        await database.status(f'''UPDATE "{schema}".reference_family_result_generation
            SET origin_lineage_id=NULL,origin_generation=NULL,published_at=NULL,serving_schema=NULL,serving_relation_names=NULL
            WHERE importer_id='geo' ''')
        with pytest.raises(RuntimeError, match="native_publication_unavailable"):
            await _capture(database, schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["function", "events", "ledger"])
async def test_changed_function_event_shape_or_ledger_guard_rejects(monkeypatch, target):
    """A same-name guard is insufficient: exact function, event set and ledger protection matter."""
    async with _fixture(monkeypatch) as (database, schema):
        if target == "function":
            await database.status(f'''CREATE OR REPLACE FUNCTION "{schema}".cms_native_input_advance()
                RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
                AS $$ BEGIN RETURN NULL; END $$''')
        elif target == "events":
            await database.status(f'DROP TRIGGER cms_native_input_mutation ON "{schema}".nucc_taxonomy')
            await database.status(f'''CREATE TRIGGER cms_native_input_mutation AFTER INSERT
                ON "{schema}".nucc_taxonomy FOR EACH STATEMENT
                EXECUTE FUNCTION "{schema}".cms_native_input_advance()''')
            await database.status(
                f'ALTER TABLE "{schema}".nucc_taxonomy ENABLE ALWAYS TRIGGER cms_native_input_mutation'
            )
        else:
            await database.status(f'''ALTER TABLE "{schema}".{inputs._TABLE}
                DISABLE TRIGGER cms_native_input_revision_no_truncate''')
        with pytest.raises(RuntimeError, match="native_revision_(guard|ledger)_unavailable"):
            await _capture(database, schema)


@pytest.mark.asyncio
async def test_registration_rollback_and_history_retention(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        with pytest.raises(asyncio.CancelledError):
            async with native_input_actor(database) as session:
                await session.execute(text(f'CREATE TABLE "{schema}".incoming_nucc(value int)'))
                prepared = await inputs.prepare_local_native_input_publication(
                    session, ledger_schema=schema, relations=(("nucc_taxonomy", "incoming_nucc"),)
                )
                await session.execute(text(f'ALTER TABLE "{schema}".nucc_taxonomy RENAME TO prior_nucc'))
                await session.execute(text(f'ALTER TABLE "{schema}".incoming_nucc RENAME TO nucc_taxonomy'))
                await inputs.finish_local_native_input_publication(session, prepared=prepared)
                raise asyncio.CancelledError
        await _assert(database, schema, fence)
        assert await database.scalar("SELECT to_regclass(:name)", name=f'"{schema}".incoming_nucc') is None
        with pytest.raises(DBAPIError, match="history_requires_retention"):
            async with database.engine.begin() as connection:
                await connection.run_sync(lambda sync: _apply(sync, "20260930120000", "downgrade"))


@pytest.mark.asyncio
async def test_npi_revision_remains_its_only_content_counter(monkeypatch):
    """Keep the existing NPI history intact while detecting its in-place input changes."""
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        await database.status(f'INSERT INTO "{schema}".npi_address VALUES (3)')
        changed = await _capture(database, schema)
        before = fence["npi"]["authority"]
        after = changed["npi"]["authority"]
        assert after["local_generation"] == before["local_generation"] + 1
        assert after["canonical_provenance"] == before["canonical_provenance"]
        assert changed["revisions"] == fence["revisions"]
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)


@pytest.mark.asyncio
async def test_replaced_npi_revision_function_is_not_content_authority(monkeypatch):
    """Existing NPI provenance cannot compensate for a same-name no-op revision function."""
    async with _fixture(monkeypatch) as (database, schema):
        await database.status(f'''CREATE OR REPLACE FUNCTION "{schema}".advance_npi_result_generation()
            RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
            AS $$ BEGIN RETURN NULL; END $$''')
        with pytest.raises(RuntimeError, match="npi_revision_guard_unavailable"):
            await _capture(database, schema)


@pytest.mark.asyncio
async def test_candidate_output_is_not_a_consumed_input_but_override_is(monkeypatch):
    """Output mutations leave the source fence valid; reviewed override changes invalidate it."""
    async with _fixture(monkeypatch) as (database, schema):
        candidate = f'"{schema}".facility_anchor_npi_candidate'
        await database.status(f"CREATE TABLE {candidate} (value int)")
        fence = await _capture(database, schema)
        assert candidate not in fence["relations"]
        await database.status(f"INSERT INTO {candidate} VALUES (1)")
        await database.status(f"TRUNCATE {candidate}")
        await _assert(database, schema, fence)
        await database.status(f'INSERT INTO "{schema}".facility_anchor_npi_override VALUES (1)')
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table,assignment",
    [
        ("provider_directory_dataset_network_plan", "network_resource_id='network-b'"),
        ("provider_directory_dataset_affiliation_organization", "affiliation_resource_id='affiliation-b'"),
        ("provider_directory_dataset_insurance_plan", "resource_id='plan-b'"),
        ("provider_directory_dataset_insurance_plan", "payload_hash=repeat('b',64)"),
        ("provider_directory_dataset_insurance_plan", 'payload_json=\'{"status":"inactive"}\'::json'),
        *((table, None) for table in _DATASET_INPUTS),
    ],
)
async def test_same_count_dataset_edge_key_and_payload_changes_invalidate_fence(monkeypatch, table, assignment):
    """Revision guards catch edits and atomic replacements even when OIDs, filenodes and counts match."""
    async with _fixture(monkeypatch) as (database, schema):
        relation = f'"{schema}"."{table}"'
        await database.status(f"INSERT INTO {relation} VALUES {_DATASET_INPUTS[table][1]}")
        fence = await _capture(database, schema)
        await _assert(database, schema, fence)
        if assignment is not None:
            await database.status(f"UPDATE {relation} SET {assignment}")
        else:
            async with database.transaction():
                await database.status(f"DELETE FROM {relation}")
                await database.status(
                    f"INSERT INTO {relation} VALUES " + _DATASET_INPUTS[table][1].replace("dataset-a", "dataset-b")
                )
        assert await database.scalar(f"SELECT count(*) FROM {relation}") == 1
        changed = await _capture(database, schema)
        assert changed["relations"] == fence["relations"]
        before = next(row for row in fence["revisions"] if row["table_name"] == table)
        after = next(row for row in changed["revisions"] if row["table_name"] == table)
        assert after["revision"] == before["revision"] + (1 if assignment is not None else 2)
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("table", _DATASET_INPUTS)
@pytest.mark.parametrize("mutation", ["drop", "replace"])
async def test_dataset_input_drop_or_replacement_cannot_reuse_captured_fence(monkeypatch, table, mutation):
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        relation = f'"{schema}"."{table}"'
        if mutation == "drop":
            await database.status(f"DROP TABLE {relation}")
        else:
            stage = table + "_next"
            async with native_input_actor(database) as session:
                await session.execute(text(f'CREATE TABLE "{schema}"."{stage}" ({_DATASET_INPUTS[table][0]})'))
                await session.execute(
                    text(
                        f'INSERT INTO "{schema}"."{stage}" VALUES '
                        + _DATASET_INPUTS[table][1].replace("dataset-a", "dataset-b")
                    )
                )
                prepared = await inputs.prepare_local_native_input_publication(
                    session, ledger_schema=schema, relations=((table, stage),)
                )
                await session.execute(text(f'ALTER TABLE {relation} RENAME TO "{table}_prior"'))
                await session.execute(text(f'ALTER TABLE "{schema}"."{stage}" RENAME TO "{table}"'))
                await inputs.finish_local_native_input_publication(session, prepared=prepared)
            changed = await _capture(database, schema)
            assert changed["relations"] == fence["relations"]
            assert len(changed["revisions"]) == len(fence["revisions"])
            assert await database.scalar(f'SELECT count(*) FROM "{schema}".{inputs._TABLE}') == len(fence["revisions"])
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("table", _DATASET_INPUTS)
@pytest.mark.parametrize("mutation", ["drop", "replace", "disable"])
async def test_dataset_guard_tampering_cannot_be_repaired_by_registration(monkeypatch, table, mutation):
    """Missing, partial-event and disabled guards reject even if missed writes retain the old counter."""
    async with _fixture(monkeypatch) as (database, schema):
        relation = f'"{schema}"."{table}"'
        await database.status(f"INSERT INTO {relation} VALUES {_DATASET_INPUTS[table][1]}")
        fence = await _capture(database, schema)
        if mutation == "disable":
            await database.status(f"ALTER TABLE {relation} DISABLE TRIGGER {inputs._TRIGGER}")
        else:
            await database.status(f"DROP TRIGGER {inputs._TRIGGER} ON {relation}")
            if mutation == "replace":
                await database.status(
                    f"CREATE TRIGGER {inputs._TRIGGER} AFTER INSERT ON {relation} "
                    f'FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".cms_native_input_advance()'
                )
                await database.status(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {inputs._TRIGGER}")
        await database.status(f"UPDATE {relation} SET dataset_id='dataset-b'")
        with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
            await _assert(database, schema, fence)
        with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
            async with native_input_actor(database) as connection:
                await inputs.register_native_address_inputs(connection, schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_native_input_actor_restores_read_mode_on_every_exit(monkeypatch, failure):
    """Actor proof uses the fixture Factory without leaking capture mode on failure."""
    from types import SimpleNamespace

    from process import provider_directory_generic_source_policy as source

    factory = SimpleNamespace()
    actual_database = object()
    events = []

    class Session:
        async def scalar(self, statement, parameters=None):
            events.append(str(statement))
            if parameters is not None:
                assert parameters == {"owner": "synthetic_owner", "publisher": "synthetic_publisher"}
                return True
            return "on" if factory.readonly else "off"

    @asynccontextmanager
    async def transaction(database, *, isolation):
        assert database is actual_database and isolation == "repeatable read"
        assert factory.readonly is True
        yield Session()

    database = SimpleNamespace(
        _native_input_component=SimpleNamespace(
            database=actual_database,
            factory=factory,
            owner="synthetic_owner",
            publisher="synthetic_publisher",
        )
    )
    monkeypatch.setattr(source, "source_transaction", transaction)
    try:
        async with native_input_actor(database, readonly=True):
            if failure is not None:
                raise failure()
    except BaseException as error:
        assert failure is not None and type(error) is failure
    assert not hasattr(factory, "readonly")
    assert "session_user=:publisher" in events[0] and "rolsuper" in events[0]
    assert events[1] == "SHOW transaction_read_only"


@pytest.mark.asyncio
async def test_native_input_replacement_enrollment_changes_only_the_exact_oid():
    """Explicit new-root setup grants no functions and never adopts during a read."""
    from types import SimpleNamespace

    statements = []

    class Connection:
        async def scalar(self, statement, parameters):
            statements.append((str(statement), parameters))
            return 42 if str(statement).startswith("SELECT to_regclass") else True

        async def execute(self, statement):
            statements.append((str(statement), None))

    @asynccontextmanager
    async def begin():
        yield Connection()

    database = SimpleNamespace(engine=SimpleNamespace(begin=begin))
    component = SimpleNamespace(owner="synthetic_owner")
    await enroll_native_input_custody(
        database,
        "synthetic_schema",
        component,
        replacement_name="nucc_taxonomy",
    )
    assert len(statements) == 3
    assert statements[0][1] == {"name": '"synthetic_schema"."nucc_taxonomy"'}
    assert statements[1] == ('ALTER TABLE "synthetic_schema"."nucc_taxonomy" OWNER TO "synthetic_owner"', None)
    assert statements[2][1] == {"oid": 42, "owner": "synthetic_owner", "name": '"synthetic_schema"."nucc_taxonomy"'}
    assert database._native_input_component is component


@pytest.mark.asyncio
async def test_fhir_input_layout_uses_named_partition_edges(monkeypatch):
    """A copied same-name partition keeps its layout proof without physical coordinates."""
    from process import provider_directory_generic_source_provision as provision
    from process.provider_directory_generic_source_policy import GenericSourcePolicy
    from process.provider_directory_projection_stage import _copy_driver

    async with _delta_database(monkeypatch) as (database, schema):
        roots = provision.FHIR_INPUT_ROOTS
        for root in roots:
            await database.status(
                f'CREATE TABLE "{schema}"."{root}" '
                "(dataset_id varchar NOT NULL, id integer NOT NULL, PRIMARY KEY(dataset_id,id)) "
                "PARTITION BY LIST(dataset_id)"
            )
        parent = f'"{schema}"."{roots[0]}"'
        leaf = f'"{schema}".fhir_input_leaf'
        await database.status(f"CREATE TABLE {leaf} PARTITION OF {parent} FOR VALUES IN ('dataset-a')")
        await database.status(f"INSERT INTO {leaf} VALUES ('dataset-a',1)")
        policy = GenericSourcePolicy(schema, "input_owner", "input_publisher", ("input_reader",))
        async with database.session() as session, session.begin():
            await session.execute(text("SELECT 1"))
            driver = await _copy_driver(session)
            before = await provision.capture_protected_fhir_input(driver, source_policy=policy)
            assert before["parents"] == [[schema, name] for name in roots]
            observed_leaf = next(
                catalog_record
                for catalog_record in before["inventory"]
                if catalog_record["relname"] == "fhir_input_leaf"
            )
            assert observed_leaf["parents"] == [[schema, roots[0]]]
            assert not {"database_oid", "owner_oid", "parent_oids", "family_sha256"} & before.keys()
            await driver.execute(f"CREATE TEMP TABLE saved_fhir_leaf AS TABLE {leaf}")
            await driver.execute(f"ALTER TABLE {parent} DETACH PARTITION {leaf}")
            await driver.execute(f"DROP TABLE {leaf}")
            await driver.execute(f"CREATE TABLE {leaf} PARTITION OF {parent} FOR VALUES IN ('dataset-a')")
            await driver.execute(f"INSERT INTO {leaf} SELECT * FROM saved_fhir_leaf")
            await driver.execute("DROP TABLE saved_fhir_leaf")
            assert await driver.fetchval(f"SELECT count(*) FROM {leaf}") == 1
            assert await provision.capture_protected_fhir_input(driver, source_policy=policy) == before
            await driver.execute(f"ALTER TABLE {parent} ADD COLUMN extra integer")
            assert await provision.capture_protected_fhir_input(driver, source_policy=policy) != before
