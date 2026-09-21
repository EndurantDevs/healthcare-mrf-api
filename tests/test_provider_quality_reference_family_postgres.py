# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native provider-quality publication and reference-family archive proof."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db import models
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.provider_quality_parts import model_helpers, publish_helpers, table_helpers
from tests.test_reference_family_archive_postgres import _database_url
from tests.test_reference_family_result_generation_postgres import (
    _CENSUS_MIGRATION_PATH,
    _GEO_MIGRATION_PATH,
    _PHARMACY_MIGRATION_PATH,
    _TERMINOLOGY_MIGRATION_PATH,
    _run_migration,
    _upgrade_reference_generation_chain,
)

_QUALITY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260921000000_provider_quality_result_generation.py"
)


class _Database:
    """Minimal database facade that keeps publisher work in one real transaction."""

    def __init__(self, sessions):
        self.sessions = sessions
        self.active = None

    @asynccontextmanager
    async def transaction(self):
        assert self.active is None
        async with self.sessions() as session, session.begin():
            self.active = session
            try:
                yield session
            finally:
                self.active = None

    async def _execute(self, statement, parameters):
        statement = text(statement) if isinstance(statement, str) else statement
        if self.active is not None:
            return await self.active.execute(statement, parameters)
        async with self.sessions() as session, session.begin():
            return await session.execute(statement, parameters)

    async def status(self, statement, **parameters):
        return getattr(await self._execute(statement, parameters), "rowcount", None)

    async def first(self, statement, **parameters):
        return (await self._execute(statement, parameters)).first()

    async def all(self, statement, **parameters):
        return (await self._execute(statement, parameters)).all()

    async def scalar(self, statement, **parameters):
        return (await self._execute(statement, parameters)).scalar()


async def _upgrade_generation_chain(connection):
    await _upgrade_reference_generation_chain(connection)
    for migration_path in (
        _GEO_MIGRATION_PATH,
        _PHARMACY_MIGRATION_PATH,
        _TERMINOLOGY_MIGRATION_PATH,
        _CENSUS_MIGRATION_PATH,
        _QUALITY_MIGRATION_PATH,
    ):
        await _run_migration(connection, migration_path, "upgrade")


async def _create_table(session, model_type, schema):
    table = model_type.__table__.to_metadata(archive.MetaData(schema=schema), schema=schema)
    await session.execute(text(str(archive.CreateTable(table).compile(dialect=archive.postgresql.dialect()))))


async def _create_quality_publication(sessions, schema):
    spec = archive.reference_family_spec("provider-quality")
    async with sessions() as session, session.begin():
        await archive._create_model_family(session, spec, schema, create_indexes=False)
        await _create_table(session, models.PricingProcedureTaxonomySignal, schema)
        await _upgrade_generation_chain(await session.connection())

    for model_type in (*spec.model_types, models.PricingProcedureTaxonomySignal):
        await table_helpers._ensure_indexes(model_type, schema)

    classes = model_helpers._staging_classes("snapshot_test", schema)
    async with sessions() as session, session.begin():
        for stage_model in classes.values():
            await session.execute(
                text(str(archive.CreateTable(stage_model.__table__).compile(dialect=archive.postgresql.dialect())))
            )
    for stage_model in classes.values():
        await table_helpers._ensure_indexes(stage_model, schema)
    return classes


async def _relation_oids(session, schema, names):
    rows = (
        await session.execute(
            text(
                "SELECT relation_name,to_regclass(format('%I.%I',CAST(:schema AS text),relation_name))::oid::bigint "
                "FROM unnest(CAST(:names AS text[])) WITH ORDINALITY names(relation_name,ordinal) "
                "ORDER BY ordinal"
            ),
            {"schema": schema, "names": list(names)},
        )
    ).all()
    return tuple(int(row[1]) for row in rows)


async def _insert_quality_rows(session, schema):
    statements = (
        f'INSERT INTO "{schema}".pricing_qpp_provider (npi,year) VALUES (1000000001,2026)',
        f"INSERT INTO \"{schema}\".pricing_svi_zcta (zcta,year) VALUES ('10001',2026)",
        f'INSERT INTO "{schema}".pricing_provider_quality_measure '
        "(npi,year,benchmark_mode,measure_id,domain) VALUES (1000000001,2026,'national','cost','cost')",
        f'INSERT INTO "{schema}".pricing_provider_quality_domain '
        "(npi,year,benchmark_mode,domain) VALUES (1000000001,2026,'national','cost')",
        f'INSERT INTO "{schema}".pricing_provider_quality_score '
        "(npi,year,benchmark_mode) VALUES (1000000001,2026,'national')",
        f'INSERT INTO "{schema}".pricing_provider_quality_feature (npi,year) VALUES (1000000001,2026)',
        f'INSERT INTO "{schema}".pricing_provider_quality_procedure_lsh (npi,year,band_no) VALUES (1000000001,2026,1)',
        f'INSERT INTO "{schema}".pricing_provider_quality_peer_target '
        "(year,benchmark_mode,geography_scope,geography_value,cohort_level,specialty_key,taxonomy_code,"
        "procedure_bucket) VALUES (2026,'national','country','US','all','all','all','all')",
    )
    for statement in statements:
        await session.execute(text(statement))


async def _command(*arguments):
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
    assert process.returncode == 0, stderr.decode()
    return stdout.decode()


@asynccontextmanager
async def _quality_database(monkeypatch, schema, *additional_schemas):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = _Database(sessions)
    monkeypatch.setattr(publish_helpers, "db", database)
    monkeypatch.setattr(table_helpers, "db", database)
    try:
        yield sessions
    finally:
        async with engine.begin() as connection:
            for owned_schema in (*additional_schemas, schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{owned_schema}" CASCADE'))
                assert (
                    await connection.scalar(
                        text("SELECT to_regnamespace(:schema)"),
                        {"schema": owned_schema},
                    )
                    is None
                )
        await engine.dispose()


@pytest.mark.asyncio
async def test_quality_publish_generation_rolls_back_with_all_table_swaps(monkeypatch):
    """Generation publication and all eight table swaps share one transaction."""

    schema = "quality_publish_" + uuid4().hex
    async with _quality_database(monkeypatch, schema) as sessions:
        classes = await _create_quality_publication(sessions, schema)
        relation_names = generation.RELATION_NAMES_BY_IMPORTER["provider-quality"]
        staged_relation_names = tuple(
            classes[model_type.__name__].__tablename__
            for model_type in archive.reference_family_spec("provider-quality").model_types
        )
        async with sessions() as session, session.begin():
            live_oids = await _relation_oids(session, schema, relation_names)
            stage_oids = await _relation_oids(session, schema, staged_relation_names)

        real_publish = publish_helpers.publish_local_reference_family_generation

        async def fail_after_generation(*args, **kwargs):
            await real_publish(*args, **kwargs)
            raise RuntimeError("force rollback")

        monkeypatch.setattr(publish_helpers, "publish_local_reference_family_generation", fail_after_generation)
        with pytest.raises(RuntimeError, match="force rollback"):
            await publish_helpers._publish_by_table_rename(classes, schema)

        async with sessions() as session, session.begin():
            assert await _relation_oids(session, schema, relation_names) == live_oids
            assert await _relation_oids(session, schema, staged_relation_names) == stage_oids
            authority = await generation.read_reference_family_result_generation_authority(
                session,
                importer_id="provider-quality",
                schema_name=schema,
            )
            assert authority.local_generation == 0 and authority.relation_oids is None

        monkeypatch.setattr(publish_helpers, "publish_local_reference_family_generation", real_publish)
        await publish_helpers._publish_by_table_rename(classes, schema)
        async with sessions() as session, session.begin():
            authority = await generation.read_reference_family_result_generation_authority(
                session,
                importer_id="provider-quality",
                schema_name=schema,
            )
            assert authority.local_generation == 1
            assert authority.relation_oids == await generation.current_reference_family_relation_oids(
                session,
                importer_id="provider-quality",
                schema_name=schema,
            )
            assert len(authority.relation_oids) == 8
            assert await session.scalar(text(f"SELECT to_regclass('{schema}.procedure_taxonomy_signal')")) is not None
            assert await session.scalar(text(f"SELECT to_regclass('{schema}.pricing_quality_run')")) is None
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(await session.connection(), _QUALITY_MIGRATION_PATH, "downgrade")


async def _prepare_quality_archive(sessions, schema, dataset_id, *, create_publication=True):
    if create_publication:
        classes = await _create_quality_publication(sessions, schema)
        await publish_helpers._publish_by_table_rename(classes, schema)
    async with sessions() as session, session.begin():
        await _insert_quality_rows(session, schema)

    async def metadata(session):
        serving = await generation.capture_reference_family_serving_generation(
            session,
            importer_id="provider-quality",
            schema_name=schema,
        )
        return {"serving_generation": serving.as_dict()}

    async def retain(_session, _prepared):
        return None

    return await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="provider-quality",
        schema_name=schema,
        source_metadata=None,
        dataset_id=dataset_id,
        on_prepared=retain,
        source_metadata_factory=metadata,
    )


async def _ordinary_quality_publish(sessions, schema):
    """Publish empty staged replacements through the ordinary table-rename path."""

    classes = model_helpers._staging_classes("ordinary", schema)
    async with sessions() as session, session.begin():
        for stage_model in classes.values():
            await session.execute(
                text(str(archive.CreateTable(stage_model.__table__).compile(dialect=archive.postgresql.dialect())))
            )
    for stage_model in classes.values():
        await table_helpers._ensure_indexes(stage_model, schema)
    await publish_helpers._publish_by_table_rename(classes, schema)


async def _dump_quality_archive(sessions, prepared, dump_path, expected_names):
    dsn = _database_url().replace("+asyncpg", "")

    async def dump(capture):
        await _command(
            "pg_dump",
            "--dbname",
            dsn,
            "--format=custom",
            "--no-owner",
            "--no-acl",
            "--schema",
            capture.ownership.schema_name,
            "--snapshot",
            capture.postgres_snapshot,
            "--file",
            str(dump_path),
        )

    await archive.export_prepared_reference_family_archive(sessions, prepared=prepared, archive_copy=dump)
    listing = await _command("pg_restore", "--list", str(dump_path))
    assert all(table_name in listing for table_name in expected_names)
    assert "pricing_quality_run" not in listing
    assert "procedure_taxonomy_signal" not in listing
    return dsn


async def _restore_and_validate_quality_archive(sessions, prepared, dataset_id, dump_path, dsn):
    async with sessions() as session, session.begin():
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="provider-quality",
            dataset_id=dataset_id,
        )
    await _command(
        "pg_restore",
        "--dbname",
        dsn,
        "--data-only",
        "--no-owner",
        "--no-acl",
        "--exit-on-error",
        "--single-transaction",
        str(dump_path),
    )
    async with sessions() as session, session.begin():
        await archive.complete_reference_family_restore(session, restored)
        await archive.validate_reference_family_stage(
            session,
            ownership=restored,
            manifest=prepared.manifest.as_dict(),
        )
        indexes = (
            (
                await session.execute(
                    text("SELECT indexname FROM pg_indexes WHERE schemaname=:schema ORDER BY indexname"),
                    {"schema": restored.schema_name},
                )
            )
            .scalars()
            .all()
        )
        assert len(indexes) == 46
        assert len(indexes) == len(set(indexes))
        assert all(len(index_name) <= 63 for index_name in indexes)
        assert (
            await session.scalar(text(f'SELECT count(*) FROM "{restored.schema_name}".pricing_provider_quality_score'))
            == 1
        )
    return restored


@pytest.mark.asyncio
async def test_quality_native_archive_roundtrip_has_exact_inventory_and_indexes(monkeypatch, tmp_path):
    """The exact family survives a native archive roundtrip with producer index shape."""

    schema = "quality_archive_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    async with _quality_database(monkeypatch, schema, stage_schema) as sessions:
        prepared = await _prepare_quality_archive(sessions, schema, dataset_id)
        expected_names = archive.reference_family_spec("provider-quality").table_names
        assert tuple(table.table_name for table in prepared.manifest.tables) == expected_names
        assert all(table.row_count == 1 for table in prepared.manifest.tables)
        assert tuple(name for name, _oid in prepared.ownership.relation_oids) == tuple(sorted(expected_names))
        assert "pricing_quality_run" not in expected_names
        assert "procedure_taxonomy_signal" not in expected_names

        dump_path = tmp_path / "provider-quality.dump"
        dsn = await _dump_quality_archive(sessions, prepared, dump_path, expected_names)
        await _restore_and_validate_quality_archive(sessions, prepared, dataset_id, dump_path, dsn)


async def _quality_activation_binding(sessions, restored, manifest, schema):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session,
            importer_id="provider-quality",
            schema_name=schema,
        )
        owner_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
        receipt = await archive.prepare_reference_family_activation(
            session,
            ownership=restored,
            manifest=manifest,
            package_id="b" * 64,
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return incumbent, owner_oid, receipt


async def _activate_quality(session, restored, manifest, incumbent, owner_oid, receipt, serving_generation):
    return await archive.activate_validated_reference_family_stage(
        session,
        ownership=restored,
        manifest=manifest,
        expected_incumbent=incumbent,
        validation_receipt=receipt,
        cutover=archive.ReferenceFamilyCutoverAuthority(
            "b" * 64,
            archive.CONTRACT,
            owner_oid,
            owner_oid,
            "manual",
            serving_generation,
        ),
    )


@pytest.mark.asyncio
async def test_quality_restored_indexes_activate_after_ordinary_publish_and_rollback(monkeypatch, tmp_path):
    """Stage-owned restored indexes survive rollback and replace an ordinary published family."""

    schema = "quality_activate_" + uuid4().hex
    dataset_id = uuid4()
    second_dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    second_stage_schema = archive.reference_family_stage_schema(second_dataset_id)
    predecessor_schemas = (
        archive.reference_family_predecessor_schema(dataset_id),
        archive.reference_family_predecessor_schema(second_dataset_id),
    )
    async with _quality_database(
        monkeypatch,
        schema,
        stage_schema,
        second_stage_schema,
        *predecessor_schemas,
    ) as sessions:
        prepared = await _prepare_quality_archive(sessions, schema, dataset_id)
        dump_path = tmp_path / "provider-quality-activate.dump"
        dsn = await _dump_quality_archive(
            sessions,
            prepared,
            dump_path,
            tuple(name for name, _oid in prepared.ownership.relation_oids),
        )
        restored = await _restore_and_validate_quality_archive(sessions, prepared, dataset_id, dump_path, dsn)
        incumbent, owner_oid, receipt = await _quality_activation_binding(sessions, restored, prepared.manifest, schema)
        source_generation = prepared.manifest.source_metadata["serving_generation"]
        async with sessions() as session:
            transaction = await session.begin()
            await _activate_quality(
                session, restored, prepared.manifest, incumbent, owner_oid, receipt, source_generation
            )
            assert (
                len(await _relation_oids(session, schema, generation.RELATION_NAMES_BY_IMPORTER["provider-quality"]))
                == 8
            )
            await transaction.rollback()
        async with sessions() as session, session.begin():
            assert await _relation_oids(
                session, schema, generation.RELATION_NAMES_BY_IMPORTER["provider-quality"]
            ) == tuple(relation_oid for _name, relation_oid in incumbent.relation_oids)
            await _activate_quality(
                session, restored, prepared.manifest, incumbent, owner_oid, receipt, source_generation
            )
            assert (
                len(await _relation_oids(session, schema, generation.RELATION_NAMES_BY_IMPORTER["provider-quality"]))
                == 8
            )
        await _ordinary_quality_publish(sessions, schema)
        prepared_second = await _prepare_quality_archive(
            sessions,
            schema,
            second_dataset_id,
            create_publication=False,
        )
        second_dump_path = tmp_path / "provider-quality-second-adopt.dump"
        second_dsn = await _dump_quality_archive(
            sessions,
            prepared_second,
            second_dump_path,
            tuple(name for name, _oid in prepared_second.ownership.relation_oids),
        )
        restored_second = await _restore_and_validate_quality_archive(
            sessions,
            prepared_second,
            second_dataset_id,
            second_dump_path,
            second_dsn,
        )
        incumbent_second, owner_oid, receipt_second = await _quality_activation_binding(
            sessions,
            restored_second,
            prepared_second.manifest,
            schema,
        )
        second_generation = prepared_second.manifest.source_metadata["serving_generation"]
        async with sessions() as session:
            transaction = await session.begin()
            await _activate_quality(
                session,
                restored_second,
                prepared_second.manifest,
                incumbent_second,
                owner_oid,
                receipt_second,
                second_generation,
            )
            assert (
                len(await _relation_oids(session, schema, generation.RELATION_NAMES_BY_IMPORTER["provider-quality"]))
                == 8
            )
            await transaction.rollback()
        async with sessions() as session, session.begin():
            assert await _relation_oids(
                session, schema, generation.RELATION_NAMES_BY_IMPORTER["provider-quality"]
            ) == tuple(relation_oid for _name, relation_oid in incumbent_second.relation_oids)
            await _activate_quality(
                session,
                restored_second,
                prepared_second.manifest,
                incumbent_second,
                owner_oid,
                receipt_second,
                second_generation,
            )
            assert (
                len(await _relation_oids(session, schema, generation.RELATION_NAMES_BY_IMPORTER["provider-quality"]))
                == 8
            )
