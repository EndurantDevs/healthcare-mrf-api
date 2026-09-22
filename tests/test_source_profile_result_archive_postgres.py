# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped preparation, immutable seals and CAS preserve unrelated native rows."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import provider_profile_states as serving
from db import models
from db.connection import Database
from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import _create_family, _database_url, _drop_family, _seed


@asynccontextmanager
async def _prepared_case(importer, *, with_ancestry=False):
    """Own isolated source/destination families and their exact clone cleanup."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "profile_test_" + uuid4().hex
    destination_schema = "profile_test_" + uuid4().hex
    prepared = None
    created_schemas = []
    try:
        async with sessions() as session, session.begin():
            for schema in (source_schema, destination_schema):
                await _create_family(session, schema)
                created_schemas.append(schema)
            incoming, ancestors, incumbent, other, unrelated, owner = await _seed_case(
                session, source_schema, destination_schema, importer, with_ancestry
            )
            await _assert_reference_free_tables(session, created_schemas)
        async with sessions() as session, session.begin():
            prepared = await archive.prepare_source(
                session, importer_id=importer, schema=source_schema, run_id=incoming, dataset_id=uuid4()
            )
        activation_by_field = dict(
            prepared=prepared,
            destination_schema=destination_schema,
            expected_current_run_id=incumbent,
            package_id="a" * 64,
            sealed_owner_oid=owner,
            pin_id=uuid4(),
        )
        yield SimpleNamespace(
            engine=engine,
            sessions=sessions,
            source_schema=source_schema,
            destination_schema=destination_schema,
            importer=importer,
            incoming=incoming,
            ancestors=ancestors,
            incumbent=incumbent,
            other=other,
            unrelated=unrelated,
            prepared=prepared,
            activation_by_field=activation_by_field,
        )
    finally:
        async with sessions() as session, session.begin():
            if prepared is not None:
                await archive.cleanup_stage(session, prepared.ownership)
                await archive.release_source_pin(
                    session,
                    schema=source_schema,
                    importer_id=importer,
                    run_id=prepared.manifest["run_id"],
                    pin_id=prepared.ownership.dataset_id,
                )
            for schema in reversed(created_schemas):
                await _drop_family(session, schema)
        await engine.dispose()


async def _seed_case(session, source_schema, destination_schema, importer, with_ancestry):
    ancestors = []
    if with_ancestry:
        ancestors.append(await _seed(session, source_schema, importer))
        ancestors.insert(0, await _seed(session, source_schema, importer, parent_run_id=ancestors[0]))
    incoming = await _seed(session, source_schema, importer, parent_run_id=ancestors[0] if ancestors else None)
    incumbent = await _seed(session, destination_schema, importer)
    other = next(name for name in archive.SOURCES if name != importer)
    unrelated = await _seed(session, destination_schema, other)
    await _seed(session, source_schema, other)
    owner = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
    return incoming, ancestors, incumbent, other, unrelated, owner


async def _assert_reference_free_tables(session, schemas):
    for schema in schemas:
        for name in ("npi", "nucc_taxonomy", "import_run", "provider_profile_projection"):
            assert await session.scalar(text("SELECT to_regclass(:name)"), {"name": f"{schema}.{name}"}) is None


async def _assert_stage_refusals(case):
    with pytest.raises(archive.SourceProfileArchiveError, match="restored result differs"):
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(f"UPDATE \"{case.prepared.ownership.schema_name}\".provider_profile_fact SET display='changed'")
            )
            await archive.validate_stage(session, case.prepared.ownership, case.prepared.manifest)
    with pytest.raises(archive.SourceProfileArchiveError, match="stage ownership changed"):
        async with case.sessions() as session, session.begin():
            await archive.verify_ownership(
                session, replace(case.prepared.ownership, schema_oid=case.prepared.ownership.schema_oid + 1)
            )


async def _assert_source_guards(case):
    for schema in (case.source_schema, case.destination_schema):
        for statement in (
            f"UPDATE \"{schema}\".provider_profile_fact SET display='changed' WHERE run_id=:run",
            f'DELETE FROM "{schema}".provider_profile_fact WHERE run_id=:run',
            f'TRUNCATE "{schema}".provider_profile_fact',
            f'INSERT INTO "{schema}".provider_profile_fact SELECT * FROM "{schema}".provider_profile_fact WHERE run_id=:run',
        ):
            with pytest.raises(DBAPIError, match="retained run is pinned"):
                async with case.sessions() as session, session.begin():
                    await session.execute(text(statement), {"run": case.incoming})


async def _assert_pointer_fences(case, validation):
    with pytest.raises(archive.SourceProfileArchiveError, match="destination predecessor changed"):
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'UPDATE "{case.destination_schema}".provider_profile_source_publication SET current_run_id=:run WHERE source_key=:source'
                ),
                {"run": "f" * 32, "source": archive.SOURCES[case.importer][0]},
            )
            await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
    with pytest.raises(RuntimeError, match="synthetic recording failure"):
        async with case.sessions() as session, session.begin():
            await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
            raise RuntimeError("synthetic recording failure")
    async with case.sessions() as session, session.begin():
        assert (await archive._pointer(session, case.destination_schema, case.importer))[
            "current_run_id"
        ] == case.incumbent


async def _assert_reference_free_read(case, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(serving, "db", Database(engine=case.engine, session_factory=case.sessions))
        patch.setattr(serving.ProviderProfileSourcePublication.__table__, "schema", case.destination_schema)
        projections = await serving.fetch_additional_state_profile_projections(1234567890)
        restored = next(
            projection
            for projection in projections
            if projection["source"]["source_key"] == archive.SOURCES[case.importer][0]
        )
        assert restored["generation_id"] == case.incoming
        assert restored["categories"]["education"]["items"][0]["display"] == "Synthetic school"
        assert restored["source"]["registry_generation"] == "d" * 64


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", archive.SOURCES)
@pytest.mark.parametrize("fault", ["run_id", "agency", "jurisdiction", "fact_type"])
async def test_unservable_fact_semantics_are_rejected(importer, fault):
    async with _prepared_case(importer) as case:
        schema = case.prepared.ownership.schema_name
        with pytest.raises(archive.SourceProfileArchiveError):
            async with case.sessions() as session, session.begin():
                if fault == "fact_type":
                    await session.execute(text(f"UPDATE \"{schema}\".provider_profile_fact SET fact_type='incorrect'"))
                else:
                    await session.execute(
                        text(
                            f'UPDATE "{schema}".provider_profile_fact SET source_json='
                            "jsonb_set(source_json::jsonb,CAST(:field AS text[]),CAST(:value AS jsonb))::json"
                        ),
                        {"field": [fault], "value": json.dumps("incorrect")},
                    )
                await archive.describe_result(session, importer_id=importer, schema=schema, run_id=case.incoming)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", archive.SOURCES)
async def test_scoped_adoption_and_reference_free_read(importer, monkeypatch):
    async with _prepared_case(importer) as case:
        assert [table["row_count"] for table in case.prepared.manifest["tables"]] == [1, 1, 1, 1]
        assert case.prepared.manifest["dependencies"] == {}
        assert case.prepared.manifest["source_completed_at"] == "2026-01-02T00:00:00+00:00"
        await _assert_stage_refusals(case)
        async with case.sessions() as session, session.begin():
            unrelated_before = await archive.describe_result(
                session, importer_id=case.other, schema=case.destination_schema, run_id=case.unrelated
            )
            validation = await archive.prepare_activation(session, **case.activation_by_field)
            assert (await archive._pointer(session, case.destination_schema, importer))[
                "current_run_id"
            ] == case.incumbent
            assert await archive._run(session, case.destination_schema, case.incoming) is not None
        await _assert_source_guards(case)
        await _assert_pointer_fences(case, validation)

        async def no_content_scan(*_args, **_kwargs):
            raise AssertionError("short activation must not hash or recount content")

        with monkeypatch.context() as patch:
            patch.setattr(archive, "_projected_row_identity", no_content_scan)
            async with case.sessions() as session, session.begin():
                receipt = await archive.activate_validated_result(
                    session, validation=validation, **case.activation_by_field
                )
                assert receipt["previous_run_id"] == case.incumbent
        await _assert_reference_free_read(case, monkeypatch)
        async with case.sessions() as session, session.begin():
            assert (
                await archive.describe_result(
                    session, importer_id=case.other, schema=case.destination_schema, run_id=case.unrelated
                )
                == unrelated_before
            )
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=importer,
                    run_id=case.incoming,
                    pin_id=case.activation_by_field["pin_id"],
                )
                == "retained"
            )
            await archive.rollback_result(
                session,
                schema=case.destination_schema,
                importer_id=importer,
                expected_current_run_id=case.incoming,
                expected_previous_run_id=case.incumbent,
            )
            pointer = await archive._pointer(session, case.destination_schema, importer)
            assert (pointer["current_run_id"], pointer["previous_run_id"]) == (case.incumbent, case.incoming)
            assert (await archive._pointer(session, case.destination_schema, case.other))[
                "current_run_id"
            ] == case.unrelated


@pytest.mark.asyncio
async def test_native_source_retention_excludes_exact_pins_until_release(monkeypatch, tmp_path):
    import importlib
    from datetime import datetime

    from process import massachusetts_profile_store
    from process import provider_profile_source_store as shared

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(shared, "db", database)
    monkeypatch.setattr(importlib.import_module("process.florida_mqa_profile"), "db", database)
    importer = "massachusetts-borim-profile"
    prepared = None
    try:
        async with sessions() as session, session.begin():
            await _create_family(session, "mrf")
            incoming = await _seed(session, "mrf", importer)
        async with sessions() as session, session.begin():
            prepared = await archive.prepare_source(
                session, importer_id=importer, schema="mrf", run_id=incoming, dataset_id=uuid4()
            )
        async with sessions() as session, session.begin():
            await _seed(session, "mrf", importer)
            newest = await _seed(session, "mrf", importer)
            await session.execute(
                text("UPDATE mrf.provider_profile_import_run SET started_at=:started WHERE run_id=:run"),
                {"started": datetime(2026, 1, 3), "run": newest},
            )
        receipt = await massachusetts_profile_store._store.retain_source_history(tmp_path)
        assert incoming in receipt["protected_audit_run_ids"]
        assert incoming not in receipt["deleted_run_ids"]
        async with sessions() as session, session.begin():
            await archive.release_source_pin(
                session, schema="mrf", importer_id=importer, run_id=incoming, pin_id=prepared.ownership.dataset_id
            )
        receipt = await massachusetts_profile_store._store.retain_source_history(tmp_path)
        assert incoming in receipt["deleted_run_ids"]
        async with sessions() as session, session.begin():
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM mrf.provider_profile_fact WHERE run_id=:run"), {"run": incoming}
                )
                == 0
            )
            assert await archive._run(session, "mrf", incoming) is not None
    finally:
        async with sessions() as session, session.begin():
            if prepared is not None:
                await archive.cleanup_stage(session, prepared.ownership)
            await _drop_family(session, "mrf")
        await engine.dispose()


async def _assign_adoption_roles(case, ordinary_role, publisher_role):
    """Give an inheriting publisher stage ownership and ordinary native ownership."""
    async with case.sessions() as session, session.begin():
        for role in (ordinary_role, publisher_role):
            await session.execute(text(f'CREATE ROLE "{role}" NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE'))
        await session.execute(text(f'GRANT "{ordinary_role}" TO "{publisher_role}"'))
        for schema, owner, names in (
            (
                case.destination_schema,
                ordinary_role,
                (*archive.TABLES, "provider_profile_source_publication", archive.pins.TABLE),
            ),
            (case.prepared.ownership.schema_name, publisher_role, archive.TABLES),
        ):
            await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'))
            for name in names:
                await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{owner}"'))
        for function in ("provider_profile_pinned_run_guard", "provider_profile_pinned_truncate_guard"):
            await session.execute(
                text(f'ALTER FUNCTION "{case.destination_schema}".{function}() OWNER TO "{ordinary_role}"')
            )
        await session.execute(
            text(
                f'UPDATE "{case.destination_schema}".provider_profile_import_run '
                "SET started_at=:started WHERE run_id=:run"
            ),
            {"started": datetime(2026, 1, 3), "run": case.incumbent},
        )
        case.activation_by_field["sealed_owner_oid"] = await session.scalar(
            text("SELECT oid FROM pg_roles WHERE rolname=:role"), {"role": publisher_role}
        )


async def _exercise_inherited_adoption(case, ordinary_role, publisher_role):
    """Prepare, clean, reactivate and roll back under the role that owns the stage."""
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
        await archive.prepare_activation(session, **case.activation_by_field)
        assert [
            pin["purpose"]
            for pin in await archive._pin_group(session, case.destination_schema, case.activation_by_field["pin_id"])
        ] == ["adoption"]
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))
        assert (
            await session.scalar(
                text(
                    f'SELECT count(*) FROM "{case.destination_schema}".provider_profile_source_pin '
                    "WHERE purpose='adoption'"
                )
            )
            == 1
        )
        assert await archive._run(session, case.destination_schema, case.incoming) is not None
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
        assert (
            await archive.cleanup_adoption(
                session,
                schema=case.destination_schema,
                importer_id=case.importer,
                run_id=case.incoming,
                pin_id=case.activation_by_field["pin_id"],
            )
            == "released"
        )
        assert await archive._run(session, case.destination_schema, case.incoming) is None
        validation = await archive.prepare_activation(session, **case.activation_by_field)
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
        await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
        await archive.rollback_result(
            session,
            schema=case.destination_schema,
            importer_id=case.importer,
            expected_current_run_id=case.incoming,
            expected_previous_run_id=case.incumbent,
        )
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))
        await session.execute(
            text(
                f'UPDATE "{case.destination_schema}".provider_profile_source_publication '
                "SET previous_run_id=NULL WHERE source_key=:source"
            ),
            {"source": archive.SOURCES[case.importer][0]},
        )


async def _assert_ordinary_adoption_retention(case, ordinary_role, publisher_role, monkeypatch, tmp_path):
    """The native source GC protects a sealed run until publisher release."""
    import importlib

    from process import massachusetts_profile_store as massachusetts
    from process import provider_profile_source_store as shared

    florida = importlib.import_module("process.florida_mqa_profile")
    role_engine = create_async_engine(_database_url(), connect_args={"server_settings": {"role": ordinary_role}})
    role_sessions = async_sessionmaker(role_engine, expire_on_commit=False)
    try:
        with monkeypatch.context() as patch:
            runtime_db = Database(engine=role_engine, session_factory=role_sessions)
            patch.setattr(shared, "db", runtime_db)
            patch.setattr(florida, "db", runtime_db)
            for model in (*archive.MODELS, models.ProviderProfileSourcePublication, models.ProviderProfileSourcePin):
                patch.setattr(model.__table__, "schema", case.destination_schema)
            receipt = await massachusetts._store.retain_source_history(tmp_path)
            assert case.incoming in receipt["protected_audit_run_ids"]
            assert case.incoming not in receipt["deleted_run_ids"]
            async with case.sessions() as session, session.begin():
                await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
                assert (
                    await archive.release_source_pin(
                        session,
                        schema=case.destination_schema,
                        importer_id=case.importer,
                        run_id=case.incoming,
                        pin_id=case.activation_by_field["pin_id"],
                    )
                    == "released"
                )
            receipt = await massachusetts._store.retain_source_history(tmp_path)
            assert case.incoming in receipt["deleted_run_ids"]
    finally:
        await role_engine.dispose()


async def _drop_adoption_roles(case, ordinary_role, publisher_role):
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'REASSIGN OWNED BY "{publisher_role}","{ordinary_role}" TO CURRENT_USER'))
        await session.execute(text(f'DROP OWNED BY "{publisher_role}","{ordinary_role}"'))
        await session.execute(text(f'DROP ROLE "{publisher_role}"'))
        await session.execute(text(f'DROP ROLE "{ordinary_role}"'))


@pytest.mark.asyncio
async def test_inherited_publisher_adoption_rollback_and_ordinary_retention(monkeypatch, tmp_path):
    """Native publisher seals and ordinary retention coexist across activation."""
    async with _prepared_case("massachusetts-borim-profile") as case:
        token = uuid4().hex
        ordinary_role = "profile_owner_" + token
        publisher_role = "profile_publisher_" + token
        await _assign_adoption_roles(case, ordinary_role, publisher_role)
        try:
            await _exercise_inherited_adoption(case, ordinary_role, publisher_role)
            await _assert_ordinary_adoption_retention(case, ordinary_role, publisher_role, monkeypatch, tmp_path)
        finally:
            await _drop_adoption_roles(case, ordinary_role, publisher_role)


@pytest.mark.asyncio
async def test_unpublished_cleanup_and_active_conflict():
    async with _prepared_case("massachusetts-borim-profile") as case:
        with pytest.raises(archive.SourceProfileArchiveError, match="source is active"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(
                        f"UPDATE \"{case.destination_schema}\".provider_profile_import_run SET status='running' WHERE run_id=:run"
                    ),
                    {"run": case.incumbent},
                )
                await archive.prepare_activation(session, **case.activation_by_field)
        async with case.sessions() as session, session.begin():
            await archive.prepare_activation(session, **case.activation_by_field)
        async with case.sessions() as session, session.begin():
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=case.importer,
                    run_id=case.incoming,
                    pin_id=case.activation_by_field["pin_id"],
                )
                == "released"
            )
            assert await archive._run(session, case.destination_schema, case.incoming) is None
            assert (await archive._pointer(session, case.destination_schema, case.importer))[
                "current_run_id"
            ] == case.incumbent
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=case.importer,
                    run_id=case.incoming,
                    pin_id=case.activation_by_field["pin_id"],
                )
                == "already_released"
            )
