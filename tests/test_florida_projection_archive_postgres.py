# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native Florida projection archive on an isolated PostgreSQL database."""

import asyncio
import importlib
import json
import os
from datetime import datetime
from uuid import uuid4

import pytest
from sqlalchemy import insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db import models
from process import florida_projection_archive as archive
from process.source_profile_result_pins import statement_pin_guard_statements
from tests.source_profile_archive_support import _create_family
from tests.source_profile_archive_support import _seed_florida as _native_seed


@pytest.fixture(autouse=True)
def configured_projection_roles(monkeypatch, tmp_path):
    """Declare the isolated test login as administrator and an existing native reader role."""
    url = os.getenv("FLORIDA_SNAPSHOT_TEST_DATABASE_URL")
    if not url:
        return
    login = make_url(url).username
    assert isinstance(login, str) and login
    policy_by_field = {
        "owner_role": "synthetic_native_owner",
        "migration_role": "synthetic_native_migration",
        "preparation_owner_role": login,
        "runtime_roles": ["pg_monitor"],
    }
    filename = tmp_path / "projection-role-policy.json"
    filename.write_text(json.dumps(policy_by_field))
    from process.source_profile_result_pins import ROLE_POLICY_ENVIRONMENT

    monkeypatch.setenv(ROLE_POLICY_ENVIRONMENT, str(filename))


async def _drop_test_schemas(engine, schema_names):
    """Remove only schemas created and committed by this test."""
    if not schema_names:
        return
    async with engine.begin() as connection:
        for schema_name in schema_names:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


async def _native_family(session, schema):
    await _create_family(session, schema, projection=True)


@pytest.mark.asyncio
async def test_ordinary_florida_publication_uses_bounded_native_copy_and_model_indexes(monkeypatch):
    """Exercise actual lazy SQLAlchemy transactions and JSON binary COPY on the ordinary writer."""
    from db.connection import Database

    url = os.getenv("FLORIDA_SNAPSHOT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("isolated PostgreSQL URL not configured")
    florida = importlib.import_module("process.florida_mqa_profile")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine)
    is_created = False
    try:
        async with sessions.begin() as session:
            await _native_family(session, "mrf")
            run_id = await _native_seed(session, "mrf")
        is_created = True
        async with sessions.begin() as session:
            run = await archive._run(session, "mrf", run_id)
            projection_rows = [
                dict(projection_row)
                for projection_row in (
                    await session.execute(text("SELECT * FROM mrf.provider_profile_projection"))
                ).mappings()
            ]
            old_oid = await archive.native._relation_oid(session, "mrf", archive.PROJECTION)

        async def batches():
            yield projection_rows

        monkeypatch.setattr(florida, "db", Database(engine=engine, session_factory=sessions))
        publication, _ = await florida._publish_projection_swap(
            run_id,
            batches(),
            started_at=run["started_at"],
            completion_metrics=run["metrics"],
            allow_volume_drop=False,
            min_first_publish_providers=1,
            min_publish_ratio=0.8,
        )
        assert publication["published_rows"] == 1
        async with sessions.begin() as session:
            await archive.validate_native_result(session, "mrf", run_id)
            assert await archive.native._relation_oid(session, "mrf", archive.PROJECTION + "_old") == old_oid
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_index WHERE indrelid='mrf.provider_profile_projection'::regclass AND indisprimary AND indisvalid"
                    )
                )
                == 1
            )
    finally:
        try:
            if is_created:
                await _drop_test_schemas(engine, ("mrf",))
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_shared_native_florida_copy_publish_rollback_and_mixed_evidence():
    """One rollback-owned transaction closes all five models without external references."""
    url = os.getenv("FLORIDA_SNAPSHOT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("isolated PostgreSQL URL not configured")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine)
    schemas = ["fl_native_" + uuid4().hex for _ in range(3)]
    try:
        async with sessions() as session:
            await session.begin()
            try:
                for schema in schemas:
                    await _native_family(session, schema)
                destination = schemas[-1]
                unrelated = await _native_seed(session, destination, unrelated=True)
                installations = [
                    await _native_projection_installation(session, source_schema, destination, day=index * 3)
                    for index, source_schema in enumerate(schemas[:2])
                ]
                assert await archive.profiles._run(session, destination, unrelated) is not None
                await _assert_native_projection_rollback(session, destination, installations)
            finally:
                await session.rollback()
    finally:
        await engine.dispose()


async def _native_projection_installation(session, source_schema, destination, *, day):
    """Prepare invisible COPY/indexed heaps before publishing their exact sealed identities."""
    profiles = archive.profiles
    copier = archive.native.ReferenceFamilySourceCopy(archive.native.native_copy_projection, 1024 * 1024, 300)
    run_id = await _native_seed(session, source_schema, day=day)
    candidate = await profiles.prepare_source(
        session,
        importer_id=archive.IMPORTER_ID,
        schema=source_schema,
        run_id=run_id,
        dataset_id=uuid4(),
        contract=profiles.CONTRACT,
        source_copy=copier,
    )
    assert len(candidate.manifest["tables"]) == 5
    assert len(candidate.ownership.relation_names) == 9
    expected = await profiles._pointer(session, destination, archive.IMPORTER_ID)
    pin_id = uuid4()
    activation_by_field = dict(
        prepared=candidate,
        destination_schema=destination,
        expected_current_run_id=expected["current_run_id"],
        package_id="f" * 64,
        pin_id=pin_id,
    )
    validation = await profiles.prepare_activation(
        session,
        **activation_by_field,
        publication_request={"expected": expected, "source_copy": copier},
    )
    assert await profiles._run(session, destination, run_id) is None
    activation = await profiles.activate_validated_result(session, **activation_by_field, validation=validation)
    assert activation["current_run_id"] == run_id
    return candidate, activation, pin_id


async def _assert_native_projection_rollback(session, destination, installations):
    """Reject changed named predecessors and retain the original installation plus both pins."""
    profiles = archive.profiles
    first, activation, first_pin = installations[0]
    second, _, second_pin = installations[1]
    expected = await profiles._pointer(session, destination, archive.IMPORTER_ID)
    with pytest.raises(profiles.SourceProfileArchiveError, match="projection predecessor"):
        async with session.begin_nested():
            await profiles._admission(
                session,
                first.manifest,
                destination,
                second.manifest["run_id"],
                projection={
                    "expected": {
                        **expected,
                        "previous_relation": {"schema": destination, "table_name": "another_snapshot"},
                    }
                },
            )
    await profiles.rollback_validated_result(
        session,
        schema=destination,
        importer_id=archive.IMPORTER_ID,
        expected_current_run_id=second.manifest["run_id"],
        expected_previous_run_id=first.manifest["run_id"],
        pin_id=first_pin,
        manifest=first.manifest,
        package_id="f" * 64,
    )
    restored = await profiles._pointer(session, destination, archive.IMPORTER_ID)
    assert restored["current_relation"] == {"schema": destination, "table_name": archive.PROJECTION}
    assert restored["current_run_id"] == first.manifest["run_id"]
    assert (
        await profiles.cleanup_adoption(
            session,
            schema=destination,
            importer_id=archive.IMPORTER_ID,
            run_id=second.manifest["run_id"],
            pin_id=second_pin,
        )
        == "retained"
    )


async def _named_source_case(session):
    """Create one declared native family and two complete synthetic generations."""
    from types import SimpleNamespace

    from process import source_profile_result_archive as profiles
    from tests.source_profile_archive_support import native_source_copy

    token = uuid4().hex
    source_schema, destination_schema = "named_fl_source_" + token, "named_fl_dest_" + token
    owner = "named_fl_owner_" + token
    reader = "named_fl_reader_" + token
    worker = "named_fl_worker_" + token
    roles = SimpleNamespace(owner_role=owner, publisher_role=reader, runtime_roles=(worker,))
    for role in (owner, reader, worker):
        await session.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
    for schema in (source_schema, destination_schema):
        await _create_family(session, schema, projection=True)
    incoming = await _native_seed(session, source_schema, day=1)
    incumbent = await _native_seed(session, destination_schema)
    prepared = await profiles.prepare_source(
        session,
        importer_id=profiles.PROJECTION_IMPORTER,
        schema=source_schema,
        run_id=incoming,
        dataset_id=uuid4(),
        contract=profiles.CONTRACT,
        source_copy=native_source_copy(),
    )
    previous = await profiles.prepare_source(
        session,
        importer_id=profiles.PROJECTION_IMPORTER,
        schema=destination_schema,
        run_id=incumbent,
        dataset_id=uuid4(),
        contract=profiles.CONTRACT,
        source_copy=native_source_copy(),
    )
    for role in (reader, worker):
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{destination_schema}" TO "{role}"'))
        await session.execute(
            text(f'GRANT SELECT,UPDATE ON "{destination_schema}".provider_profile_projection TO "{role}"')
        )
        await session.execute(
            text(f'GRANT UPDATE(profile_json) ON "{destination_schema}".provider_profile_projection TO "{role}"')
        )
    return SimpleNamespace(
        schema=destination_schema,
        roles=roles,
        incoming=incoming,
        incumbent=incumbent,
        prepared=prepared,
        previous=previous,
    )


async def _switch_named_source(session, case):
    """Copy under one bound, prove outer rollback, then switch the same candidate."""
    from process import florida_projection_named_source as named
    from tests.source_profile_archive_support import native_source_copy

    expected = await named.named_publication_pointer(session, case.schema)
    cutover_by_field = await named.prepare_named_cutover(
        session,
        prepared=case.prepared,
        schema=case.schema,
        expected=expected,
        cutover_id=uuid4(),
        source_copy=native_source_copy(),
        deadline=asyncio.get_running_loop().time() + 300,
        roles=case.roles,
    )
    with pytest.raises(RuntimeError, match="synthetic outer rollback"):
        async with session.begin_nested():
            await named.publish_named_source_projection(
                session, case.schema, cutover_by_field["cutover"], expected, case.incoming, roles=case.roles
            )
            raise RuntimeError("synthetic outer rollback")
    assert await named.named_publication_pointer(session, case.schema) == expected
    await named.publish_named_source_projection(
        session, case.schema, cutover_by_field["cutover"], expected, case.incoming, roles=case.roles
    )
    pointer_by_field = await named.named_publication_pointer(session, case.schema)
    assert (pointer_by_field["current_run_id"], pointer_by_field["previous_run_id"]) == (case.incoming, case.incumbent)
    return pointer_by_field


async def _assert_projection_readonly(session, case, pointer):
    """Exercise SELECT and native denial of both table and column write privileges."""
    from sqlalchemy.exc import DBAPIError

    from process import florida_projection_named_source as named

    for role in (case.roles.publisher_role, *case.roles.runtime_roles):
        await session.execute(text(f'SET LOCAL ROLE "{role}"'))
        for relation in (pointer["current_relation"], pointer["previous_relation"]):
            table = named._table(relation["schema"], relation["table_name"])
            assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 1
            await _refuse_projection_mutations(session, table, DBAPIError)
        await session.execute(text("RESET ROLE"))


async def _refuse_projection_mutations(session, table, error_type):
    """Each failed native mutation rolls back without losing the reader transaction."""
    for mutation in (f"UPDATE {table} SET profile_json='{{}}'::json", f"TRUNCATE {table}"):
        with pytest.raises(error_type, match="permission denied|must be owner"):
            async with session.begin_nested():
                await session.execute(text(mutation))


async def _restore_named_source(session, case, pointer):
    """Restore only the retained generation with complete immutable archive parity."""
    from process import florida_projection_named_source as named

    await named.rollback_named_source_projection(session, case.previous, case.schema, pointer, roles=case.roles)
    restored = await named.named_publication_pointer(session, case.schema)
    assert (restored["current_run_id"], restored["previous_run_id"]) == (case.incumbent, case.incoming)
    assert await named.native._is_model_table_equal(
        session,
        models.ProviderProfileProjection,
        left_schema=case.previous.ownership.schema_name,
        left_name=named.PROJECTION,
        right_schema=case.schema,
        right_name=named.PROJECTION,
    )


@pytest.mark.asyncio
async def test_named_source_projection_copy_atomic_switch_permissions_and_exact_rollback():
    """The Source-profile adapter retains content and closes real native write paths."""
    url = os.getenv("FLORIDA_SNAPSHOT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("isolated PostgreSQL URL not configured")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine)
    try:
        async with sessions() as session, session.begin() as transaction:
            try:
                case = await _named_source_case(session)
                pointer = await _switch_named_source(session, case)
                await _assert_projection_readonly(session, case, pointer)
                await _restore_named_source(session, case, pointer)
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
