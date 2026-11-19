# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual migrated-head activation in an exactly cleaned genuine-role database."""

import json
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import asyncpg
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database, db
from process import entity_address_alias_guard as alias_guard
from process import entity_address_snapshot_preparation as preparation
from tests import test_address_numeric_grid_alias_db as numeric
from tests import test_entity_address_snapshot_destination as fixture
from tests.test_company_registry_group_assertion_migration import _assert_source_inventory_migration_head
from tests.test_entity_address_alias_guard_postgres import _admin_url, _owned_guard_database
from tests.test_provider_directory_profile_capacity_adoption_postgres import _upgrade_disposable_schema_to_head

_SCHEMA = "migrated_address"
_HEAD = "20261119090000_cms_npd_typed_release"
_ROLE_SCHEMA_REVISION = "20261019030000_provider_directory_role_schema"


def _assert_address_migration_chain(script):
    """Keep the exact accepted migration head and predecessor assertions."""
    _assert_source_inventory_migration_head(script)
    assert script.get_revision(_ROLE_SCHEMA_REVISION).down_revision == "20261019020000_registry_continuation_join"
    assert script.get_revision("20261019020000_registry_continuation_join").down_revision == (
        "20261019010000_registry_raw_object_join",
        "20261019010000_custom_import_build_continuation",
    )
    assert script.get_revision("20261019010000_custom_import_build_continuation").down_revision == (
        "20261017010000_custom_import_raw_objects"
    )
    assert script.get_revision("20261017020000_source_registry_snapshot_join").down_revision == (
        "20261016060000_uhc_official_cohort_snapshots",
        "20261017010000_registry_cms_source_notifications",
    )
    assert script.get_revision("20261017010000_registry_cms_source_notifications").down_revision == (
        "20261016070000_registry_allowed_plan_scope"
    )
    assert script.get_revision("20261016070000_registry_allowed_plan_scope").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision("20261016060000_uhc_official_cohort_snapshots").down_revision == (
        "20261016050000_acquisition_validator_argument_types"
    )
    assert script.get_revision("20261015010000_plan_release_binding_cohorts").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision("20261015020000_custom_import_query_projections").down_revision == (
        "20261013010000_registry_custom_history_events"
    )
    assert script.get_revision("20261010010000_custom_import_materialization_contract").down_revision == (
        "20261010000000_custom_import_admission_indexes"
    )
    assert (
        script.get_revision("20261010030000_registry_source_admission_receipt").down_revision
        == "20261010020000_registry_ptg_office_retention"
    )
    assert script.get_revision("20261010020000_registry_ptg_office_retention").down_revision == (
        "20261010010000_registry_ptg_office_approval"
    )
    assert script.get_revision("20261010010000_registry_ptg_office_approval").down_revision == (
        "20261009040000_registry_ptg_published_plan_scope"
    )
    assert script.get_revision("20261009040000_registry_ptg_published_plan_scope").down_revision == (
        "20261009030000_network_catalog_evidence"
    )
    assert script.get_revision("20261006010000_nucc_reference_result_generation").down_revision == (
        "20261007000000_custom_import_rejection_anti_joins"
    )
    assert script.get_revision("20261009000000_custom_import_child_presence_decode").down_revision == (
        "20261006010000_nucc_reference_result_generation"
    )
    assert script.get_revision("20261010000000_custom_import_admission_indexes").down_revision == (
        "20261009000000_custom_import_child_presence_decode"
    )
    assert script.get_revision("20261007010000_managed_network_registry").down_revision == (
        "20261010010000_custom_import_materialization_contract"
    )


async def _migrated_tables(resources, monkeypatch):
    """Migrate an isolated schema, seed inputs and provision its genuine roles."""
    database_name = await resources.admin.fetchval("SELECT current_database()")
    url = _admin_url().set(database=database_name, drivername="postgresql+asyncpg")
    engine = create_async_engine(url, pool_size=1, max_overflow=0, hide_parameters=True)
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    try:
        script = ScriptDirectory.from_config(Config("alembic.ini"))
        _assert_address_migration_chain(script)
        await fixture._install_destination_extensions(database)
        await database.status(f'CREATE SCHEMA "{_SCHEMA}"')
        monkeypatch.setenv("DB_SCHEMA", _SCHEMA)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
        await _upgrade_disposable_schema_to_head(url.render_as_string(hide_password=False), _SCHEMA)
        assert await database.scalar(f'SELECT version_num FROM "{_SCHEMA}".alembic_version') == _HEAD
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
    for function in alias_guard.GUARDS:
        await admin.execute(f'ALTER FUNCTION "{_SCHEMA}"."{function}"() OWNER TO "{roles["owner"]}"')
    await admin.execute(f'GRANT SELECT ON "{_SCHEMA}".address_alias_state_v1 TO "{roles["writer"]}"')
    await admin.execute(
        f"GRANT SELECT,INSERT,UPDATE ({','.join(alias_guard.REVOCATION_COLUMNS)}) "
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
        assert (
            await db.scalar(native._activate_geo_assurance_candidate_sql(_SCHEMA))
            == native.geo_projection.GEO_ASSURANCE_VERSION
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
    from process.cms_doctors_immutable import _freeze_named_model_relations

    async with resources.publisher.session_factory() as session, session.begin():
        await session.execute(text(f'ALTER SCHEMA "{owner.schema_name}" OWNER TO "{resources.roles["owner"]}"'))
        await _freeze_named_model_relations(session, owner.schema_name, owner.relation_names)
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{owner.schema_name}" TO "{resources.roles["writer"]}"'))
        await session.execute(
            text(f'REVOKE CREATE ON SCHEMA "{owner.schema_name}" FROM "{resources.roles["writer"]}",PUBLIC')
        )
    inventory_by_field = {
        "schema_name": owner.schema_name,
        "relations": [{"schema_name": owner.schema_name, "relation_name": name} for name in owner.relation_names],
        "sequences": [
            {
                "sequence_name": preparation._EVIDENCE_SEQUENCE,
                "owner_table": "entity_address_evidence",
                "owner_column": "evidence_id",
            }
        ],
    }
    return {
        "state": "frozen",
        "inventory": inventory_by_field,
        "inventory_sha256": preparation._digest(inventory_by_field),
    }


async def _activate(resources, stored, proof, incumbent, events, *, fail_after_publish=False):
    async def authenticate(session, supplied, validation):
        assert supplied == proof
        assert session.in_transaction()
        assert validation == proof.get("validation", {}).get("evidence")
        return proof

    if proof["state"] == "frozen":
        async with resources.publisher.session_factory() as session, session.begin():
            validation = await preparation.validate_entity_address_archive_preparation(
                session, stored=stored, preparation=proof, authenticate_preparation=authenticate
            )
        proof.update(state="validated", validation={"evidence": validation})

    async def before():
        assert db._transaction_binding().session.in_transaction()
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
    """Prove frozen data is readable while real runtime DML and DDL are denied."""
    inventory = proof["inventory"]
    for relation in inventory["relations"]:
        table = f'"{inventory["schema_name"]}"."{relation["relation_name"]}"'
        assert await db.scalar(f"SELECT count(*) FROM {table}") >= 0
        for statement in (
            f"DELETE FROM {table} WHERE FALSE",
            f"ALTER TABLE {table} ADD COLUMN forbidden_write integer",
        ):
            with pytest.raises(DBAPIError) as caught:
                async with db.transaction() as session:
                    await session.execute(text(statement))
            assert caught.value.orig.sqlstate == "42501"
    schema = f'"{_SCHEMA}"'
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
                    f"INSERT INTO {schema}.provider_directory_cms_serving_receipt(receipt_id,payload) SELECT encode(sha256(convert_to(value::text,'UTF8')),'hex'),value FROM (SELECT CAST(:payload AS jsonb) AS value) input"
                ),
                {"payload": receipt_payload},
            )
            await preparation._lock_publication_state(session, _SCHEMA)


async def _receipt_index_refusals(resources, proof):
    """Reject actual unsupported mutable-state topology rather than retired catalog seals."""
    for table, alteration in (
        ("entity_address_geo_assurance_state", "ENABLE ROW LEVEL SECURITY"),
        ("entity_address_geo_assurance_state", "FORCE ROW LEVEL SECURITY"),
        ("entity_address_geo_assurance_state", "SET UNLOGGED"),
        ("entity_address_result_generation", "ENABLE ROW LEVEL SECURITY"),
        ("entity_address_result_generation", "SET UNLOGGED"),
    ):
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="publication state layout differs"):
            async with resources.publisher.transaction() as session:
                await session.execute(text(f'ALTER TABLE "{_SCHEMA}"."{table}" {alteration}'))
                await preparation._lock_publication_state(session, _SCHEMA)


async def migrated_head_sealed_activation(monkeypatch):
    with TemporaryDirectory(prefix="migrated_address_policy_") as folder:
        async with _owned_guard_database(monkeypatch) as resources:
            policy = Path(folder) / "roles.json"
            policy.write_text(
                json.dumps(
                    {
                        "owner_role": resources.admin_role,
                        "migration_role": resources.roles["publisher"],
                        "preparation_owner_role": resources.roles["owner"],
                        "runtime_roles": [resources.roles["writer"]],
                    }
                )
            )
            monkeypatch.setenv("HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE", str(policy))
            await _migrated_tables(resources, monkeypatch)
            owner, stored, incumbent = await _prepared_candidate()
            proof = await _freeze_candidate(resources, owner)
            await _receipt_index_refusals(resources, proof)
            await _guard_refusals(resources, proof)
            events = []
            with pytest.raises(RuntimeError, match="synthetic late publication failure"):
                await _activate(resources, stored, proof, incumbent, events, fail_after_publish=True)
            assert events == ["before", "after"]
            async with db.transaction() as session:
                unchanged = await preparation.serving.capture_entity_address_receive_admission(
                    session, schema_name=_SCHEMA
                )
                assert unchanged.as_dict() == incumbent
            async with resources.publisher.session_factory() as session, session.begin():
                assert (
                    await preparation._protected_catalog(session, proof, owner, read_only=True)
                    == proof["validation"]["evidence"]["inventory"]
                )
            events.clear()
            await _activate(resources, stored, proof, incumbent, events)
            assert events == ["before", "after"]
            assert (
                await resources.admin.fetchval(
                    f'SELECT count(*) FROM "{_SCHEMA}".provider_directory_cms_serving_receipt'
                )
                == 0
            )
            assert await resources.admin.fetchval("SELECT to_regnamespace($1)", owner.schema_name) is None
            await _assert_published_runtime_denials(resources, owner.relation_names)
            authority = await resources.admin.fetchrow(f'SELECT * FROM "{_SCHEMA}".entity_address_result_generation')
            assert authority["local_generation"] == 0
            assert authority["origin_generation"] == 27
            assert str(authority["origin_lineage_id"]) == fixture._source_serving_generation()["origin_lineage_id"]


async def _assert_published_runtime_denials(resources, relation_names):
    """Read published names while genuine runtime DML remains denied."""
    for name in relation_names:
        if name == "entity_address_alias_authority":
            continue
        assert await resources.admin.fetchval("SELECT to_regclass($1) IS NOT NULL", f'{_SCHEMA}."{name}"')
        with pytest.raises(DBAPIError) as caught:
            async with db.transaction() as session:
                await session.execute(text(f'DELETE FROM "{_SCHEMA}"."{name}" WHERE FALSE'))
        assert caught.value.orig.sqlstate == "42501"
