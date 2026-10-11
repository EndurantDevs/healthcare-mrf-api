# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual migrated-head activation in an exactly cleaned genuine-role database."""

import json
from dataclasses import replace
from uuid import uuid4

import asyncpg
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database, db
from process import entity_address_snapshot_preparation as preparation
from tests import test_address_numeric_grid_alias_db as numeric
from tests import test_entity_address_snapshot_destination as fixture
from tests.test_entity_address_alias_guard_postgres import _admin_url, _owned_guard_database
from tests.test_provider_directory_profile_capacity_adoption_postgres import _upgrade_disposable_schema_to_head

_SCHEMA = "migrated_address"


async def _migrated_tables(resources, monkeypatch):
    database_name = await resources.admin.fetchval("SELECT current_database()")
    url = _admin_url().set(database=database_name, drivername="postgresql+asyncpg")
    engine = create_async_engine(url, pool_size=1, max_overflow=0, hide_parameters=True)
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    try:
        script = ScriptDirectory.from_config(Config("alembic.ini"))
        assert script.get_heads() == ["20261010010000_custom_import_materialization_contract"]
        assert script.get_revision(script.get_heads()[0]).down_revision == (
            "20261010000000_custom_import_admission_indexes"
        )
        await fixture._install_destination_extensions(database)
        await database.status(f'CREATE SCHEMA "{_SCHEMA}"')
        monkeypatch.setenv("DB_SCHEMA", _SCHEMA)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
        await _upgrade_disposable_schema_to_head(url.render_as_string(hide_password=False), _SCHEMA)
        assert await database.scalar(f'SELECT version_num FROM "{_SCHEMA}".alembic_version') == (
            "20261010010000_custom_import_materialization_contract"
        )
        metadata = MetaData(schema=_SCHEMA)
        for model in preparation.destination.restore._models():
            model.__table__.to_metadata(metadata, schema=_SCHEMA)
        async with engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
        await _missing_geo_inputs(database)
        await fixture._seed_geo_dependencies(database, _SCHEMA)
        await database.status(
            f'INSERT INTO "{_SCHEMA}".entity_address_unified '
            "(entity_type,entity_id,location_key,checksum,type) VALUES ('synthetic','old','old',99,'primary')"
        )
    finally:
        await database.disconnect()
    await _provision_migrated_roles(resources, database_name)


async def _missing_geo_inputs(database):
    for table, create in (
        ("entity_address_geo_assurance_state", fixture._create_geo_assurance_state_table),
        ("npi_address", fixture._create_npi_address_table),
        ("mrf_address", fixture._create_mrf_address_table),
        ("doctor_clinician_address", fixture._create_cms_address_table),
    ):
        if not await database.scalar("SELECT to_regclass(:name)", name=f"{_SCHEMA}.{table}"):
            await create(database, _SCHEMA)
    await database.status(
        f'CREATE TABLE IF NOT EXISTS "{_SCHEMA}".geo_zip_lookup '
        "(zip_code varchar PRIMARY KEY,state varchar,state_name varchar)"
    )
    await database.status("CREATE TABLE tiger.zip_state (zip varchar PRIMARY KEY,stusps varchar)")
    await database.status(
        "CREATE TABLE tiger.zcta5 (gid bigserial PRIMARY KEY,zcta5ce varchar NOT NULL,"
        "the_geom geometry(Polygon,4269) NOT NULL)"
    )


async def _provision_migrated_roles(resources, database_name):
    admin, roles = resources.admin, resources.roles
    await admin.execute(f'GRANT "{roles["writer"]}" TO "{roles["publisher"]}"')
    await admin.execute(f'GRANT CREATE ON DATABASE "{database_name}" TO "{roles["writer"]}"')
    await admin.execute(f'GRANT USAGE,CREATE ON SCHEMA "{_SCHEMA}" TO "{roles["writer"]}","{roles["owner"]}"')
    tables = await admin.fetch(
        "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relkind='r'",
        _SCHEMA,
    )
    for catalog_row in tables:
        owner = (
            roles["owner"]
            if catalog_row["relname"] in {"address_alias_v1", "address_alias_state_v1"}
            else roles["writer"]
        )
        await admin.execute(f'ALTER TABLE "{_SCHEMA}"."{catalog_row["relname"]}" OWNER TO "{owner}"')
    for function in (*preparation.alias_guard.GUARDS, *preparation._cms_guard_bodies(_SCHEMA)):
        await admin.execute(f'ALTER FUNCTION "{_SCHEMA}"."{function}"() OWNER TO "{roles["owner"]}"')
    await admin.execute(f'GRANT SELECT ON "{_SCHEMA}".address_alias_state_v1 TO "{roles["writer"]}"')
    await admin.execute(
        f"GRANT SELECT,INSERT,UPDATE ({','.join(preparation.alias_guard.REVOCATION_COLUMNS)}) "
        f'ON "{_SCHEMA}".address_alias_v1 TO "{roles["writer"]}"'
    )
    await admin.execute(f'GRANT USAGE ON SCHEMA tiger TO "{roles["writer"]}"')
    await admin.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA tiger TO "{roles["writer"]}"')


async def _seed_consumed_archive(session, owner):
    """Use the migrated canonical functions and keep every consumed key coherent."""
    source_rows = (
        (
            await session.execute(
                text(f'SELECT * FROM "{owner.schema_name}".entity_address_unified ORDER BY location_key')
            )
        )
        .mappings()
        .all()
    )
    for source_row in source_rows:
        new_key = await numeric._insert_archive_address(
            _SCHEMA,
            first_line=source_row["first_line"],
            second_line=source_row["second_line"],
            city="TEST CITY",
            state="MI",
            postal_code="48202" if source_row["entity_id"] == "none" else "48201",
            strict_source_bits=source_row["address_source_mask"],
        )
        bindings_by_name = {"new_key": new_key, "old_key": source_row["address_key"]}
        await session.execute(
            text(
                f'UPDATE "{_SCHEMA}".address_archive_v2 SET lat=42.0,long=-83.0 '
                "WHERE address_key=CAST(:new_key AS uuid)"
            ),
            bindings_by_name,
        )
        for table in ("npi_address", "mrf_address", "doctor_clinician_address"):
            await session.execute(
                text(
                    f'UPDATE "{_SCHEMA}"."{table}" SET address_key=CAST(:new_key AS uuid) '
                    "WHERE address_key=CAST(:old_key AS uuid)"
                ),
                bindings_by_name,
            )
        await session.execute(
            text(
                f'UPDATE "{owner.schema_name}".entity_address_unified AS staged SET '
                "address_key=archive.address_key,premise_key=archive.premise_key,archive_identity_version='v2',"
                "state_code='MI',state_name='MICHIGAN',postal_code=archive.zip5,zip5=archive.zip5 "
                f'FROM "{_SCHEMA}".address_archive_v2 archive '
                "WHERE archive.address_key=CAST(:new_key AS uuid) AND staged.address_key=CAST(:old_key AS uuid)"
            ),
            bindings_by_name,
        )
    await session.execute(
        text(f"UPDATE \"{_SCHEMA}\".geo_zip_lookup SET zip_code='48201',state='MI',state_name='MICHIGAN'")
    )
    await session.execute(text("UPDATE tiger.zip_state SET zip='48201',stusps='MI'"))
    await session.execute(text("UPDATE tiger.zcta5 SET zcta5ce='48201'"))
    assert await session.scalar(text(f'SELECT count(*) FROM "{_SCHEMA}".address_archive_v2')) == 5


async def _prepared_candidate():
    async with db.transaction() as session:
        owner = await preparation.destination.restore.precreate_entity_address_archive_restore(
            session, dataset_id=uuid4(), db_schema=_SCHEMA, import_date="20261003"
        )
        await fixture._seed_source_rows(session, owner.schema_name, source_generation=3)
        await _seed_consumed_archive(session, owner)
    native = preparation.destination.entity_address_unified
    async with db.transaction():
        assert (
            await native._materialize_geo_assurance(
                _SCHEMA, "entity_address_unified", force=True, context={}, run_id="", stage_rows=1
            )
            == 1
        )
        assert await db.scalar(native._activate_geo_assurance_candidate_sql(_SCHEMA)) == await db.scalar(
            "SELECT to_regclass(:name)::oid", name=f"{_SCHEMA}.entity_address_unified"
        )
    async with db.transaction() as session:
        incumbent = await preparation.serving.capture_entity_address_receive_admission(session, schema_name=_SCHEMA)
    semantic = await fixture._capture_receipt(db.session_factory, owner.schema_name, "UTC")
    aliases = await fixture._capture_alias_receipt(db.session_factory, _SCHEMA)
    async with db.session_factory() as session, session.begin():
        prepared = await preparation.prepare_private_entity_address_archive_destination(
            session,
            owner=owner,
            semantic_receipt=semantic,
            source_alias_receipt=replace(aliases, local_generation=3),
            destination={
                "db_schema": _SCHEMA,
                "import_date": "20261003",
                "source_serving_generation": fixture._source_serving_generation(),
            },
        )
    return owner, prepared.as_dict(), incumbent.as_dict()


async def _freeze_candidate(resources, owner):
    admin, roles = resources.admin, resources.roles
    await admin.execute(f'ALTER SCHEMA "{owner.schema_name}" OWNER TO "{roles["owner"]}"')
    await admin.execute(f'REVOKE ALL ON SCHEMA "{owner.schema_name}" FROM "{roles["writer"]}",PUBLIC')
    for table, _oid in owner.relation_oids:
        await admin.execute(f'ALTER TABLE "{owner.schema_name}"."{table}" OWNER TO "{roles["owner"]}"')
        await admin.execute(f'REVOKE ALL ON "{owner.schema_name}"."{table}" FROM "{roles["writer"]}",PUBLIC')
    sequence = preparation._EVIDENCE_SEQUENCE
    await admin.execute(f'REVOKE ALL ON SEQUENCE "{owner.schema_name}"."{sequence}" FROM "{roles["writer"]}",PUBLIC')
    inventory_by_field = {
        "schema_name": owner.schema_name,
        "schema_oid": owner.schema_oid,
        "relations": [{"table_name": table, "relation_oid": oid} for table, oid in owner.relation_oids],
        "sequences": [
            {
                "sequence_name": sequence,
                "sequence_oid": await admin.fetchval("SELECT to_regclass($1)::oid", f"{owner.schema_name}.{sequence}"),
                "owner_table": "entity_address_evidence",
                "owner_column": "evidence_id",
            }
        ],
    }
    return {
        "state": "frozen",
        "inventory": inventory_by_field,
        "inventory_sha256": preparation._digest(inventory_by_field),
        "builder_oid": await admin.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", roles["writer"]),
        "frozen_owner_oid": await admin.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", roles["owner"]),
    }


async def _activate(resources, stored, proof, incumbent, events, *, fail_after_publish=False):
    async def authenticate(session, supplied, validation):
        assert supplied == proof
        assert await session.scalar(text("SELECT session_user=current_user")) is True
        assert validation == proof.get("validation", {}).get("evidence")
        return proof

    if proof["state"] == "frozen":
        async with resources.publisher.session_factory() as session, session.begin():
            validation = await preparation.validate_entity_address_archive_preparation(
                session, stored=stored, preparation=proof, authenticate_preparation=authenticate
            )
        proof.update(state="validated", validation={"evidence": validation})

    async def before():
        assert await db.scalar("SELECT current_user") == resources.roles["publisher"]
        events.append("before")

    async def after():
        assert await db.scalar(f'SELECT count(*) FROM "{_SCHEMA}".entity_address_unified') == 5
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with resources.admin.transaction():
                await resources.admin.execute(
                    f'LOCK TABLE "{_SCHEMA}".provider_directory_cms_serving_receipt IN ROW EXCLUSIVE MODE NOWAIT'
                )
        events.append("after")
        if fail_after_publish:
            raise RuntimeError("synthetic late publication failure")

    async with resources.publisher.session_factory() as session, session.begin():
        publication_validation = await preparation.activate_validated_entity_address_archive_destination(
            session,
            stored=stored,
            validation=proof["validation"]["evidence"],
            preparation=proof,
            expected_incumbent=incumbent,
            authenticate_preparation=authenticate,
            callbacks=preparation.destination.adoption.EntityAddressSnapshotAdoptionCallbacks(before, after),
        )
    return publication_validation


async def _guard_refusals(resources, proof):
    schema = f'"{_SCHEMA}"'
    cases = (
        (f'ALTER FUNCTION {schema}.cms_serving_address_transition() OWNER TO "{resources.roles["writer"]}"', "guards"),
        (
            f"CREATE OR REPLACE FUNCTION {schema}.cms_serving_no_truncate() RETURNS trigger LANGUAGE plpgsql "
            "SET search_path=pg_catalog AS $$ BEGIN RETURN NULL; END $$",
            "guards",
        ),
        (
            f"ALTER TABLE {schema}.entity_address_result_generation DISABLE TRIGGER cms_serving_address_transition",
            "guards",
        ),
        (
            f"CREATE TRIGGER unexpected_guard BEFORE TRUNCATE ON {schema}.entity_address_result_generation "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {schema}.cms_serving_no_truncate()",
            "guards",
        ),
    )
    for statement, message in cases:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match=message):
            async with resources.publisher.transaction() as session:
                await session.execute(text(statement))
                await preparation._lock_publication_state(session, _SCHEMA, proof["frozen_owner_oid"])
    # A transaction-local synthetic receipt proves the nonempty branch without
    # claiming a valid composite publication or bypassing its deferred guards.
    receipt_payload = json.dumps(
        {
            "address": {"local_lineage_id": str(uuid4()), "local_generation": 0},
            "profile": {"generation_id": "synthetic-history"},
        }
    )
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="populated CMS receipt history"):
        async with resources.publisher.transaction() as session:
            await session.execute(
                text(
                    f"INSERT INTO {schema}.provider_directory_cms_serving_receipt(receipt_id,payload) "
                    "SELECT encode(sha256(convert_to(value::text,'UTF8')),'hex'),value "
                    "FROM (SELECT CAST(:payload AS jsonb) AS value) input"
                ),
                {"payload": receipt_payload},
            )
            await preparation._lock_publication_state(session, _SCHEMA, proof["frozen_owner_oid"])


async def migrated_head_sealed_activation(monkeypatch):
    async with _owned_guard_database(monkeypatch) as resources:
        await _migrated_tables(resources, monkeypatch)
        owner, stored, incumbent = await _prepared_candidate()
        proof = await _freeze_candidate(resources, owner)
        await _guard_refusals(resources, proof)
        events = []
        with pytest.raises(RuntimeError, match="synthetic late publication failure"):
            await _activate(resources, stored, proof, incumbent, events, fail_after_publish=True)
        assert events == ["before", "after"]
        async with db.transaction() as session:
            unchanged = await preparation.serving.capture_entity_address_receive_admission(session, schema_name=_SCHEMA)
            assert unchanged.as_dict() == incumbent
        async with resources.publisher.session_factory() as session, session.begin():
            assert (
                await preparation._protected_catalog(session, proof, owner)
                == proof["validation"]["evidence"]["catalog_sha256"]
            )
        events.clear()
        await _activate(resources, stored, proof, incumbent, events)
        assert events == ["before", "after"]
        assert (
            await resources.admin.fetchval(f'SELECT count(*) FROM "{_SCHEMA}".provider_directory_cms_serving_receipt')
            == 0
        )
        assert await resources.admin.fetchval("SELECT to_regnamespace($1)", owner.schema_name) is None
        for table, oid in owner.relation_oids:
            catalog = await resources.admin.fetchrow(
                "SELECT c.oid,c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=$1 AND c.relname=$2",
                _SCHEMA,
                table,
            )
            assert (catalog["oid"], catalog["relowner"]) == (oid, proof["builder_oid"])
        authority = await resources.admin.fetchrow(f'SELECT * FROM "{_SCHEMA}".entity_address_result_generation')
        assert authority["local_generation"] == 0
        assert authority["origin_generation"] == 27
        assert str(authority["origin_lineage_id"]) == fixture._source_serving_generation()["origin_lineage_id"]
