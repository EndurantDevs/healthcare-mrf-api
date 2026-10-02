# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual revision triggers and short native fences; no complete address builder claim."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_native_inputs as inputs
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_provider_directory_cms_serving_receipt_postgres import _apply

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
    await database.status(f'''CREATE TABLE "{schema}".reference_family_result_generation (
        importer_id text PRIMARY KEY,local_lineage_id uuid,local_generation bigint,
        origin_lineage_id uuid,origin_generation bigint,published_at timestamptz,relation_oids bigint[])''')
    for family in inputs._FAMILIES:
        if family == "mrf":
            continue
        namespace = "tiger" if family == "tiger" else schema
        names = inputs.reference.RELATION_NAMES_BY_IMPORTER[family]
        for name in names:
            await database.status(f'CREATE TABLE IF NOT EXISTS "{namespace}"."{name}" (value int)')
        oids = [
            int(await database.scalar("SELECT to_regclass(:name)::oid::bigint", name=f'"{namespace}"."{name}"'))
            for name in names
        ]
        await database.status(
            f'''INSERT INTO "{schema}".reference_family_result_generation
            VALUES (:family,:lineage,1,:lineage,1,now(),:oids)''',
            family=family,
            lineage=uuid4(),
            oids=oids,
        )
    async with database.session() as session:
        oids = list(await inputs.npi.current_npi_relation_oids(session, schema_name=schema))
    await database.status(
        f'''UPDATE "{schema}".npi_result_generation SET local_generation=1,
        origin_lineage_id=local_lineage_id,origin_generation=1,published_at=now(),relation_oids=:oids''',
        oids=oids,
    )


@asynccontextmanager
async def _fixture(monkeypatch):
    """Create only a disposable database's UUID schema and its exclusively owned TIGER schema."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        await database.status("CREATE SCHEMA tiger")
        try:
            for namespace, name in inputs._relations(schema):
                columns = _DATASET_INPUTS[name][0] if name in _DATASET_INPUTS else "value int"
                await database.status(f'CREATE TABLE "{namespace}"."{name}" ({columns})')
            await _seed_authorities(database, schema)
            async with database.engine.begin() as connection:
                await inputs.register_native_address_inputs(connection, schema)
            yield database, schema
        finally:
            await database.status("DROP SCHEMA tiger CASCADE")


async def _capture(database, schema):
    async with database.engine.begin() as connection:
        await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        return await inputs.capture_native_address_input_fence(connection, schema)


async def _assert(database, schema, fence):
    async with database.engine.begin() as connection:
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
        assert [row["relation_oid"] for row in changed["relations"].values()] == [
            row["relation_oid"] for row in fence["relations"].values()
        ]
        assert changed["revisions"] != fence["revisions"]
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)


@pytest.mark.asyncio
async def test_swapped_in_input_requires_additive_registration(monkeypatch):
    """Renamed old OIDs retain their revision, while an untracked replacement cannot sign."""
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        await database.status(f'ALTER TABLE "{schema}".nucc_taxonomy RENAME TO old_nucc')
        await database.status(f'CREATE TABLE "{schema}".nucc_taxonomy (value int)')
        with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
            await _capture(database, schema)
        async with database.engine.begin() as connection:
            await inputs.register_native_address_inputs(connection, schema)
        changed = await _capture(database, schema)
        assert changed["relations"] != fence["relations"]
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await _assert(database, schema, fence)
        assert await database.scalar(f'SELECT count(*) FROM "{schema}".{inputs._TABLE}') == len(fence["revisions"]) + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["DISABLE TRIGGER cms_native_input_mutation", "ENABLE TRIGGER cms_native_input_mutation"]
)
async def test_registration_never_repairs_tampered_existing_trigger(monkeypatch, mutation):
    """Disabled or replica-bypassable tracking fails rather than resetting its baseline."""
    async with _fixture(monkeypatch) as (database, schema):
        await database.status(f'ALTER TABLE "{schema}".nucc_taxonomy {mutation}')
        with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
            async with database.engine.begin() as connection:
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
            SET origin_lineage_id=NULL,origin_generation=NULL,published_at=NULL,relation_oids=NULL
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
    """Failed registration leaves no partial new-OID tracking; downgrade keeps committed history."""
    async with _fixture(monkeypatch) as (database, schema):
        await database.status(f'ALTER TABLE "{schema}".nucc_taxonomy RENAME TO prior_nucc')
        await database.status(f'CREATE TABLE "{schema}".nucc_taxonomy (value int)')
        with pytest.raises(asyncio.CancelledError):
            async with database.engine.begin() as connection:
                await inputs.register_native_address_inputs(connection, schema)
                raise asyncio.CancelledError
        with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
            await _capture(database, schema)
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
    """New physical OIDs need additive registration and never revive a prior input identity."""
    async with _fixture(monkeypatch) as (database, schema):
        fence = await _capture(database, schema)
        relation = f'"{schema}"."{table}"'
        if mutation == "drop":
            await database.status(f"DROP TABLE {relation}")
        else:
            await database.status(f'ALTER TABLE {relation} RENAME TO "{table}_prior"')
            await database.status(f"CREATE TABLE {relation} ({_DATASET_INPUTS[table][0]})")
            with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
                await _assert(database, schema, fence)
            async with database.engine.begin() as connection:
                await inputs.register_native_address_inputs(connection, schema)
            changed = await _capture(database, schema)
            assert changed["relations"][relation]["relation_oid"] != fence["relations"][relation]["relation_oid"]
            assert len(changed["revisions"]) == len(fence["revisions"])
            assert (
                await database.scalar(f'SELECT count(*) FROM "{schema}".{inputs._TABLE}') == len(fence["revisions"]) + 1
            )
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
            async with database.engine.begin() as connection:
                await inputs.register_native_address_inputs(connection, schema)
