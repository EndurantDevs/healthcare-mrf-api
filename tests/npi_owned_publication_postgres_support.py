# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real model completion and closed ownership for newly acquired NPI output."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from process import npi_owned_publication as owned
from process import npi_result_archive as archive
from process import npi_result_generation as generation
from tests.npi_native_source_support import create_native_npi_family, native_npi_runtime


@pytest.fixture
async def native_runtime():
    """Keep direct and registered collection independent of module import order."""
    from tests.test_npi_result_archive_postgres import _database_url

    async with native_npi_runtime(_database_url()) as runtime:
        yield runtime


async def _builder_output(runtime):
    """Bind new compiled heaps to an independent builder without namespace creation."""
    dataset_id = uuid4()
    runtime.schemas.add(archive.npi_stage_schema(dataset_id))
    async with runtime.administrator.begin() as connection:
        await connection.execute(text(f'GRANT "{runtime.builder}" TO "{runtime.publisher}"'))
        owner_oid = await connection.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.owner})
        builder_oid = await connection.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.builder})
    async with runtime.sessions() as session, session.begin():
        output = await owned.precreate_npi_output(session, dataset_id=dataset_id)
        assert len(output.relation_oids) == 7 and output.sequence_oids == ()
        schema = output.schema_name
        await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{runtime.owner}"'))
        await session.execute(text(f'GRANT USAGE,CREATE ON SCHEMA "{schema}" TO "{runtime.builder}"'))
        for name, _ in output.relation_oids:
            await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{runtime.builder}"'))
        await session.execute(text(f'REVOKE CREATE ON SCHEMA "{schema}" FROM "{runtime.builder}"'))
    return output, owner_oid, builder_oid


async def _fill_output(runtime, output):
    """Produce native rows and their genuine canonical contribution independently."""
    schema, address_key = output.schema_name, uuid4()
    async with runtime.builders() as session, session.begin():
        await session.execute(text(f'INSERT INTO "{schema}".npi(npi) VALUES (1000000001)'))
        await session.execute(
            text(
                f"INSERT INTO \"{schema}\".npi_address(npi,type,checksum,address_key) VALUES (1000000001,'primary',1,:key)"
            ),
            {"key": address_key},
        )
        await session.execute(
            text(
                f"INSERT INTO \"{schema}\".npi_canonical_address(address_key,identity_key,source_bits) VALUES (:key,'synthetic-output',1)"
            ),
            {"key": address_key},
        )


async def _output_manifest(session, output):
    """Observe real completed model schemas and counts, without serving-history claims."""
    for name, _ in output.relation_oids:
        await session.execute(text(f'ANALYZE "{output.schema_name}"."{name}"'))
    tables = await archive._manifest_tables(session, output.schema_name, canonical=True)
    metadata, metadata_sha = archive._source_metadata({"run_id": "synthetic-run", "source_evidence": "a" * 64})
    return archive.NpiResultManifest(
        tables,
        metadata,
        metadata_sha,
        archive._schema_digest(tables),
        "legacy-manual",
        None,
        None,
        archive.MODEL_CONTRACT,
    )


@pytest.mark.asyncio
async def test_native_output_custody(native_runtime):
    runtime = native_runtime
    output, owner_oid, builder_oid = await _builder_output(runtime)
    schema = output.schema_name
    await _fill_output(runtime, output)
    async with runtime.sessions() as session, session.begin():
        assert await owned.require_npi_output_heap_catalog(
            session,
            output,
            executable_roles=(runtime.builder, runtime.reader),
            protected_owner_oid=owner_oid,
            builder_oid=builder_oid,
        )
        await archive.complete_npi_restore(session, output)
        manifest = await _output_manifest(session, output)
        assert await owned.require_npi_output_catalog(
            session,
            output,
            executable_roles=(runtime.builder, runtime.reader),
            protected_owner_oid=owner_oid,
            expected_owner_oid=builder_oid,
        )
        for name, _ in output.relation_oids:
            await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{runtime.owner}"'))
        await session.execute(text(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{runtime.builder}"'))
        owner_oid = await session.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.owner})
        await owned._require_inert_owned_stage(session, output, owner_oid)
        validation = await archive.prepare_npi_activation(
            session,
            ownership=output,
            manifest=manifest,
            package_id=archive._manifest_digest(manifest),
            sealed_owner_oid=owner_oid,
        )
        assert await owned.require_npi_output_catalog(
            session,
            output,
            executable_roles=(runtime.builder, runtime.reader),
            protected_owner_oid=owner_oid,
            expected_owner_oid=owner_oid,
        )
        assert validation.tables == manifest.tables
        assert await session.scalar(
            text("SELECT NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=ANY(CAST(:oids AS oid[])))"),
            {"oids": [oid for _, oid in output.relation_oids]},
        )
        assert await session.scalar(
            text(
                "SELECT NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=ANY(CAST(:oids AS oid[])) AND contype='f')"
            ),
            {"oids": [oid for _, oid in output.relation_oids]},
        )
    async with runtime.builders() as session, session.begin():
        with pytest.raises(DBAPIError):
            await session.execute(text(f"UPDATE \"{schema}\".npi SET provider_first_name='late'"))
    await _assert_constructor_activation(runtime, output, manifest, validation, owner_oid)


@pytest.mark.asyncio
async def test_native_output_rejects_incomplete_canonical_closure(native_runtime):
    runtime, dataset_id = native_runtime, uuid4()
    runtime.schemas.add(archive.npi_stage_schema(dataset_id))
    async with runtime.sessions() as session, session.begin():
        output = await owned.precreate_npi_output(session, dataset_id=dataset_id)
        schema = output.schema_name
        await session.execute(text(f'INSERT INTO "{schema}".npi(npi) VALUES (1000000001)'))
        await session.execute(
            text(
                f"INSERT INTO \"{schema}\".npi_address(npi,type,checksum,address_key) VALUES (1000000001,'primary',1,:key)"
            ),
            {"key": uuid4()},
        )
        with pytest.raises(RuntimeError, match="closure is incomplete"):
            await archive.complete_npi_restore(session, output)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["owner", "enum", "index", "default", "trigger", "rule"])
async def test_native_catalog_proof_rejects_drift(native_runtime, drift):
    runtime, dataset_id = native_runtime, uuid4()
    runtime.schemas.add(archive.npi_stage_schema(dataset_id))
    async with runtime.sessions() as session, session.begin():
        output = await owned.precreate_npi_output(session, dataset_id=dataset_id)
        schema = output.schema_name
        await archive.complete_npi_restore(session, output)
        await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{runtime.owner}"'))
        for name, _ in output.relation_oids:
            await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{runtime.owner}"'))
        owner_oid = await session.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.owner})
        changes_by_name = {
            "owner": f'ALTER TABLE "{schema}".npi OWNER TO "{runtime.publisher}"',
            "enum": f'ALTER TABLE "{schema}".npi_canonical_address ALTER COLUMN geo_source TYPE text USING geo_source::text',
            "index": f'CREATE INDEX unexpected_index ON "{schema}".npi(provider_first_name)',
            "default": f"ALTER TABLE \"{schema}\".npi ALTER COLUMN provider_first_name SET DEFAULT 'unexpected'",
            "trigger": f'CREATE TRIGGER unexpected_trigger BEFORE UPDATE ON "{schema}".npi FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()',
            "rule": f'CREATE RULE unexpected_rule AS ON DELETE TO "{schema}".npi DO INSTEAD NOTHING',
        }
        await session.execute(text(changes_by_name[drift]))
        with pytest.raises((RuntimeError, archive.NpiResultArchiveError)):
            await owned.require_npi_output_catalog(
                session,
                output,
                executable_roles=(runtime.builder, runtime.reader),
                protected_owner_oid=owner_oid,
                expected_owner_oid=owner_oid,
            )


async def assert_geo_profile_read_only(session, schema):
    """An independent NPI owner may read an installed profile but cannot update it."""
    relation = f'"{schema}".entity_address_geo_assurance_state'
    if await session.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": relation}):
        assert (
            await session.scalar(
                text(
                    "SELECT has_table_privilege(current_user,:name,'SELECT') "
                    "AND NOT has_table_privilege(current_user,:name,'UPDATE') "
                    "AND NOT has_any_column_privilege(current_user,:name,'UPDATE')"
                ),
                {"name": relation},
            )
            is True
        )


@asynccontextmanager
async def independent_geo_profile(runtime, schema):
    """Create a separate profile principal; no NPI actor receives a write grant."""
    from db.connection import Database
    from tests.ptg2_serving_address_evidence_postgres_support import _create_geo_assurance_state_table

    role, is_created = "geo_profile_" + uuid4().hex, False
    relation = f'"{schema}".entity_address_geo_assurance_state'
    database = Database(engine=runtime.administrator, session_factory=async_sessionmaker(runtime.administrator))
    try:
        async with runtime.administrator.begin() as connection:
            await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER'))
            is_created = True
        await _create_geo_assurance_state_table(database, schema)
        async with runtime.administrator.begin() as connection:
            await connection.execute(text(f'ALTER TABLE {relation} OWNER TO "{role}"'))
            await connection.execute(text(f'GRANT SELECT ON {relation} TO "{runtime.owner}","{runtime.publisher}"'))
            before = await connection.scalar(text(f"SELECT to_jsonb(state) FROM {relation} state"))
        yield
        async with runtime.administrator.connect() as connection:
            assert await connection.scalar(text(f"SELECT to_jsonb(state) FROM {relation} state")) == before
    finally:
        if is_created:
            async with runtime.administrator.begin() as connection:
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


async def _constructor_destination(runtime):
    from tests.test_npi_result_archive_postgres import _bootstrap

    schema = "npi_constructor_" + uuid4().hex
    await create_native_npi_family(runtime, schema, populated=False, publication_owner=True)
    await _bootstrap(runtime.sessions, schema)
    async with runtime.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{runtime.owner}"'))
        await session.execute(
            text(
                f'CREATE TABLE "{schema}".constructor_receipts (publication_ref text PRIMARY KEY,payload jsonb NOT NULL)'
            )
        )
        await session.execute(text(f'CREATE TABLE "{schema}".activation_log (publication_ref text NOT NULL)'))
        incumbent = await archive.capture_npi_incumbent(session, schema_name=schema, canonical=True)
    return incumbent


def _constructor_authority(validation, owner_oid, schema):
    """Native component callbacks observe actual content and persist actual receipts."""

    async def verify(session, output, manifest, _incumbent, checked, _cutover):
        assert await archive._validate_stage_manifest(session, output, manifest) == checked.tables

    async def attach(session, _output, manifest, incumbent, checked):
        return await _record_observed_constructor_receipt(session, incumbent.schema_name, manifest, checked)

    async def activated(session, receipt):
        observed = await generation.read_npi_result_generation_authority(session, schema_name=schema)
        assert observed == receipt.adopted_authority
        await session.execute(
            text(f'INSERT INTO "{schema}".activation_log VALUES (:reference)'),
            {"reference": observed.canonical_provenance.publication_ref},
        )

    return owned.NpiOwnershipPublication(
        archive.NpiCutoverAuthority(validation.package_id, owner_oid, owner_oid, "manual"),
        verify,
        attach,
        activated,
    )


async def _record_observed_constructor_receipt(session, schema, manifest, checked):
    """Validate live canonical closure and derive every count from the adopted family."""
    from process.npi_canonical_publication import NpiCanonicalPublicationInput, build_npi_canonical_publication_receipt

    tables = await archive._manifest_tables(session, schema, canonical=True)
    assert tables == checked.tables == manifest.tables
    await archive.validate_canonical_closure(session, "npi", schema, lambda namespace, name: f'"{namespace}"."{name}"')
    counts_by_name = {entry.table_name: entry.row_count for entry in tables}
    current = await generation.read_npi_result_generation_authority(session, schema_name=schema)
    timestamp = (await session.scalar(text("SELECT transaction_timestamp()"))).isoformat(timespec="microseconds")
    publication_input = NpiCanonicalPublicationInput(
        "synthetic-constructor",
        "synthetic-constructor:" + "a" * 32,
        timestamp,
        "penpc1_" + "b" * 43,
        timestamp[:10],
        await generation.current_npi_relation_oids(session, schema_name=schema),
        tuple(counts_by_name[name] for name in generation.RELATION_NAMES),
    )
    receipt = build_npi_canonical_publication_receipt(
        publication_input, publication_generation=current.local_generation + 1, created_at=timestamp
    )
    await session.execute(
        text(f'INSERT INTO "{schema}".constructor_receipts VALUES (:reference,CAST(:payload AS jsonb))'),
        {"reference": receipt.publication_ref, "payload": json.dumps(receipt._asdict())},
    )
    return receipt


async def _run_constructor_activation(session, runtime, output, manifest, validation, owner_oid, incumbent):
    await session.execute(text(f'SET LOCAL ROLE "{runtime.owner}"'))
    await session.execute(text(f'SET LOCAL search_path="{incumbent.schema_name}",public,pg_catalog'))
    await assert_geo_profile_read_only(session, incumbent.schema_name)
    return await owned.activate_ownership_sealed_npi_stage(
        session,
        ownership=output,
        manifest=manifest,
        incumbent=incumbent,
        validation_receipt=validation,
        authority=_constructor_authority(validation, owner_oid, incumbent.schema_name),
    )


async def _assert_constructor_activation(runtime, output, manifest, validation, owner_oid):
    """Exercise constructor activation, not external canonical producer admission."""
    incumbent = await _constructor_destination(runtime)
    runtime.schemas.add(archive.npi_predecessor_schema(output.dataset_id))
    schema = incumbent.schema_name
    async with independent_geo_profile(runtime, schema):
        async with runtime.sessions() as session:
            previous = await generation.read_npi_result_generation_authority(session, schema_name=schema)
        with pytest.raises(RuntimeError, match="constructor rollback"):
            async with runtime.sessions() as session, session.begin():
                await _run_constructor_activation(session, runtime, output, manifest, validation, owner_oid, incumbent)
                raise RuntimeError("constructor rollback")
        async with runtime.sessions() as reader, reader.begin():
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            assert await generation.read_npi_result_generation_authority(reader, schema_name=schema) == previous
            assert await reader.scalar(text(f'SELECT count(*) FROM "{schema}".constructor_receipts')) == 0
            assert await reader.scalar(text(f'SELECT count(*) FROM "{schema}".activation_log')) == 0
            async with runtime.sessions() as session, session.begin():
                receipt = await _run_constructor_activation(
                    session, runtime, output, manifest, validation, owner_oid, incumbent
                )
            assert await generation.read_npi_result_generation_authority(reader, schema_name=schema) == previous
            assert await reader.scalar(text(f'SELECT count(*) FROM "{schema}".constructor_receipts')) == 0
        async with runtime.sessions() as session:
            current = await generation.read_npi_result_generation_authority(session, schema_name=schema)
            assert current == receipt.adopted_authority and current.local_generation == previous.local_generation + 1
            assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".activation_log')) == 1
            saved = await session.scalar(text(f'SELECT payload FROM "{schema}".constructor_receipts'))
            assert saved["row_counts"] == [
                next(entry.row_count for entry in validation.tables if entry.table_name == name)
                for name in generation.RELATION_NAMES
            ]
            assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": output.schema_name}) is None


@asynccontextmanager
async def _geo_profile_sessions(database, *, canonical_read=False):
    """Give a real profile login data SELECT and ownership of only its geo state."""
    from process.provider_directory_cms_control_privileges import _grant_npi_generation_read

    role, password, engine, is_created = "geo_reader_" + uuid4().hex, uuid4().hex, None, False
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(f"CREATE ROLE {role} LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE")
            )
            is_created = True
            for schema in ("mrf", "tiger", "held_input"):
                if await connection.scalar(text("SELECT to_regnamespace(:schema) IS NOT NULL"), {"schema": schema}):
                    await connection.execute(text(f"GRANT USAGE ON SCHEMA {schema} TO {role}"))
                    await connection.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role}"))
            await connection.execute(text(f"REVOKE ALL ON mrf.npi_result_generation FROM {role}"))
            await connection.execute(text(f"ALTER TABLE mrf.entity_address_geo_assurance_state OWNER TO {role}"))
            if canonical_read:
                driver = (await connection.get_raw_connection()).driver_connection
                await _grant_npi_generation_read(driver, SimpleNamespace(schema="mrf", owner_role=role))
        engine = create_async_engine(database.engine.url.set(username=role, password=password), poolclass=NullPool)
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        if engine is not None:
            await engine.dispose()
        if is_created:
            async with database.engine.begin() as connection:
                await connection.execute(text(f"DROP OWNED BY {role}"))
                await connection.execute(text(f"DROP ROLE {role}"))


def _geo_values_sql(*, projected):
    from api import ptg2_geo_policy as policy
    from api import ptg2_geo_projection as projection

    evidence = projection.evidence_level_from_source_id_sql(
        projection.legacy_evidence_source_id_sql("address", schema_name="mrf")
    )
    identity = policy.provider_address_identity_coherence_sql("address", schema_name="mrf", use_projection=False)
    point = policy.provider_address_point_coherence_sql("address", schema_name="mrf", use_projection=False)
    if projected:
        evidence = projection.projected_evidence_level_sql("address", schema_name="mrf", legacy_level_sql=evidence)
        identity = projection.projected_boolean_sql(
            "address", "geo_identity_coherent", schema_name="mrf", legacy_sql=identity
        )
        point = projection.projected_boolean_sql("address", "geo_point_coherent", schema_name="mrf", legacy_sql=point)
    return f"SELECT location_key,{evidence},{identity},{point} FROM mrf.entity_address_unified address ORDER BY location_key"


async def assert_legacy_geo_fallback(dsn, monkeypatch):
    """Legacy array metadata stays untouched while real live evidence supplies the API."""
    from api import provider_profile_snapshot as snapshots
    from api import ptg2_geo_projection as projection
    from process import entity_address_snapshot_serving as serving
    from process.entity_address_dependency_bindings import activate_geo_assurance_candidate_sql
    from tests.test_entity_address_snapshot_destination import _owned_native_database
    from tests.test_geo_assurance_dependency_bindings import _prepare_npi_geo_rotation

    async with _owned_native_database(dsn, monkeypatch) as database:
        await _prepare_npi_geo_rotation(database, "canonical_null", monkeypatch)
        async with _geo_profile_sessions(database, canonical_read=True) as sessions:
            await _assert_column_only_generation_read(sessions)
            reader_role = sessions.kw["bind"].url.username
            await database.status(f'GRANT SELECT ON mrf.npi_result_generation TO "{reader_role}"')
            await database.status(
                "UPDATE mrf.entity_address_unified SET geo_evidence_source_id=0,geo_identity_coherent=false,geo_point_coherent=false"
            )
            async with sessions() as session, session.begin():
                await session.execute(
                    text(
                        "UPDATE mrf.entity_address_geo_assurance_state SET "
                        "active_relation_signature=(SELECT jsonb_object_agg(key,jsonb_build_array(11,22)) FROM jsonb_each(active_relation_signature)),"
                        "candidate_relation_signature=(SELECT jsonb_object_agg(key,jsonb_build_array(11,22)) FROM jsonb_each(candidate_relation_signature))"
                    )
                )
                original = await session.scalar(
                    text("SELECT to_jsonb(s) FROM mrf.entity_address_geo_assurance_state s")
                )
                assert await session.scalar(text("SELECT " + projection.projection_state_available_sql("mrf"))) is False
                assert await session.scalar(text(activate_geo_assurance_candidate_sql("mrf"))) is None
                with pytest.raises(RuntimeError, match="geo assurance is not active"):
                    await serving._geo_assurance_state(session, schema_name="mrf")
                assert (
                    await serving._geo_assurance_state(session, schema_name="mrf", allow_unpublished_geo=True)
                    == projection.GEO_ASSURANCE_VERSION
                )
            async with sessions() as session, session.begin():
                async with snapshots._read_snapshot_scope(
                    session, "mrf", include_detail=True, table_names=snapshots._GEO_TABLES
                ):
                    assert snapshots.snapshot_relation_available("mrf.npi_result_generation") is True
                    expected = (await session.execute(text(_geo_values_sql(projected=False)))).all()
                    assert (await session.execute(text(_geo_values_sql(projected=True)))).all() == expected
                    assert next(tuple(geo_row[1:]) for geo_row in expected if geo_row[0] == "nppes") == (
                        "nppes_registry_address",
                        True,
                        True,
                    )
                    assert (
                        await session.scalar(text("SELECT to_jsonb(s) FROM mrf.entity_address_geo_assurance_state s"))
                        == original
                    )


async def _assert_column_only_generation_read(sessions):
    from api import ptg2_geo_projection as projection

    async with sessions() as session:
        privileges = await session.execute(
            text(
                "SELECT "
                "has_table_privilege(current_user,'mrf.npi_result_generation','SELECT'),"
                "has_any_column_privilege(current_user,'mrf.npi_result_generation','UPDATE'),"
                "has_column_privilege(current_user,'mrf.npi_result_generation','origin_lineage_id','SELECT'),"
                "has_column_privilege(current_user,'mrf.npi_result_generation','origin_generation','SELECT')"
            )
        )
        assert tuple(privileges.one()) == (False, False, True, True)
        signature = await session.scalar(text("SELECT " + projection.projection_relation_signature_sql("mrf")))
        assert signature["mrf.npi_address"]["generation"]["origin_generation"] > 0
        assert await session.scalar(text("SELECT " + projection.projection_state_available_sql("mrf"))) is True


async def assert_held_geo_generation_independence(dsn, monkeypatch):
    """Held profiles remain usable without canonical metadata rights or its table."""
    from tests.test_entity_address_snapshot_destination import _owned_native_database
    from tests.test_geo_assurance_dependency_bindings import _prepare_npi_geo_rotation

    async with _owned_native_database(dsn, monkeypatch) as database:
        stage = await _prepare_npi_geo_rotation(database, "held", monkeypatch)
        bindings = await database.scalar(
            "SELECT active_dependency_bindings FROM mrf.entity_address_geo_assurance_state"
        )
        await database.status("DELETE FROM mrf.npi_address")
        async with _geo_profile_sessions(database) as sessions:
            async with sessions() as session:
                assert (
                    await session.scalar(
                        text("SELECT has_any_column_privilege(current_user,'mrf.npi_result_generation','SELECT')")
                    )
                    is False
                )
            await _assert_held_profile_queries(sessions, bindings, stage)
            await database.status("DROP TABLE mrf.npi_result_generation")
            await _assert_held_profile_queries(sessions, bindings, stage)


async def _assert_held_profile_queries(sessions, bindings, stage):
    from api import provider_profile_snapshot as snapshots
    from api import ptg2_geo_projection as projection
    from process import entity_address_snapshot_serving as serving
    from process.entity_address_dependency_bindings import activate_geo_assurance_candidate_sql

    ready_sql = "SELECT " + projection.projection_state_available_sql("mrf", dependency_bindings=bindings)
    activate_sql = activate_geo_assurance_candidate_sql("mrf", dependency_bindings=bindings)
    assert "npi_result_generation" not in ready_sql + activate_sql
    await _assert_optional_generation_inventory(sessions)
    async with sessions() as session, session.begin():
        assert (
            await serving._geo_assurance_state(session, schema_name="mrf", allow_unpublished_geo=True)
            == projection.GEO_ASSURANCE_VERSION
        )
        assert await session.scalar(text(ready_sql)) is True
        assert await session.scalar(text(activate_sql)) == stage
        await session.rollback()
    async with sessions() as session, session.begin():
        async with snapshots._read_snapshot_scope(
            session, "mrf", include_detail=True, table_names=snapshots._GEO_TABLES
        ):
            assert snapshots.snapshot_relation_available("mrf.npi_result_generation") is False
            assert snapshots.snapshot_geo_dependency_bindings("mrf") == bindings
            statement = _geo_values_sql(projected=True)
            assert "npi_result_generation" not in statement
            values = (await session.execute(text(statement))).all()
            assert next(tuple(row[1:]) for row in values if row[0] == "nppes") == ("nppes_registry_address", True, True)


async def _assert_optional_generation_inventory(sessions):
    from api import provider_profile_snapshot as snapshots

    names = ("entity_address_geo_assurance_state", "npi_result_generation", "npi_address")
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        installed = await session.scalar(text("SELECT to_regclass('mrf.npi_result_generation') IS NOT NULL"))
        locked = await snapshots.lock_reader_relations(session, (("mrf", name) for name in names))
        assert locked["mrf.npi_result_generation"] is (False if installed else None)
        assert all(locked[f"mrf.{name}"] == f"mrf.{name}" for name in names if name != "npi_result_generation")
        observed = await snapshots._inventory_relation_names(session, "mrf", names, locked_names=locked)
        assert observed == {f"mrf.{name}": None if name == "npi_result_generation" else f"mrf.{name}" for name in names}
