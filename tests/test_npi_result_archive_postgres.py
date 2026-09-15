# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from process import npi_result_archive as archive
from process import npi_result_generation as generation
from process.npi_canonical_publication import (
    NpiCanonicalPublicationInput,
    build_npi_canonical_publication_receipt,
)

_DSN_ENV = "HLTHPRT_NPI_RESULT_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE = re.compile(r"hc_npi_archive_[0-9a-f]{32}\Z")
_CI_DATABASE = "ptg2_v3_lifecycle_test_ci_runner"
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost"})
_CI_HOSTS = _LOCAL_HOSTS | {"postgres"}
_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260914120000_npi_result_generation.py"
)


def _is_owned_native_test_database(database_url) -> bool:
    """Accept only the pinned CI lifecycle DB or a UUID-owned local DB."""

    database_name = str(database_url.database or "")
    host = str(database_url.host or "")
    if not database_url.drivername.startswith("postgresql") or not database_url.username:
        return False
    if database_url.port == 5440:
        return host in _LOCAL_HOSTS and _LOCAL_DATABASE.fullmatch(database_name) is not None
    return host in _CI_HOSTS and database_url.port in (None, 5432) and database_name == _CI_DATABASE


def _database_url() -> str:
    raw_dsn = os.getenv(_DSN_ENV, "")
    if not raw_dsn:
        if os.getenv("HLTHPRT_DB_DATABASE") != _CI_DATABASE:
            pytest.skip(f"{_DSN_ENV} is not set")
        try:
            raw_dsn = URL.create(
                "postgresql",
                username=os.environ["HLTHPRT_DB_USER"],
                password=os.getenv("HLTHPRT_DB_PASSWORD") or None,
                host=os.environ["HLTHPRT_DB_HOST"],
                port=int(os.environ["HLTHPRT_DB_PORT"]),
                database=os.environ["HLTHPRT_DB_DATABASE"],
            ).render_as_string(hide_password=False)
        except KeyError, TypeError, ValueError:
            pytest.fail("pinned CI PostgreSQL configuration is invalid")
    database_url = make_url(raw_dsn)
    if not _is_owned_native_test_database(database_url):
        pytest.fail(f"{_DSN_ENV} must identify the dedicated native archive test database")
    return database_url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


def _asyncpg_database_url() -> str:
    return make_url(_database_url()).set(drivername="postgresql").render_as_string(hide_password=False)


def _native_archive_tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        pytest.fail(f"required PostgreSQL archive tool is unavailable: {name}")
    return path


def _native_archive_environment() -> dict[str, str]:
    database_url = make_url(_database_url())
    environment = os.environ.copy()
    environment["PGHOST"] = str(database_url.host or "")
    environment["PGPORT"] = str(database_url.port or 5432)
    environment["PGUSER"] = str(database_url.username or "")
    environment["PGDATABASE"] = str(database_url.database or "")
    if database_url.password is None:
        environment.pop("PGPASSWORD", None)
    else:
        environment["PGPASSWORD"] = database_url.password
    return environment


def _run_archive_tool(arguments: list[str], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=60,
    )


def _dump_npi_stage(capture: archive.NpiStageCapture, dump_path: Path) -> None:
    environment = _native_archive_environment()
    result = _run_archive_tool(
        [
            _native_archive_tool("pg_dump"),
            "--format=custom",
            "--file",
            str(dump_path),
            f"--snapshot={capture.postgres_snapshot}",
            *[f"--table={capture.ownership.schema_name}.{table_name}" for table_name in generation.RELATION_NAMES],
        ],
        environment,
    )
    assert result.returncode == 0, result.stderr


def _restore_npi_table_data(dump_path: Path, restore_list_path: Path, stage_schema: str) -> None:
    environment = _native_archive_environment()
    pg_restore = _native_archive_tool("pg_restore")
    listing = _run_archive_tool([pg_restore, "--list", str(dump_path)], environment)
    assert listing.returncode == 0, listing.stderr
    selected_entries = []
    selected_tables = set()
    for line in listing.stdout.splitlines():
        if ";" not in line:
            continue
        fields = line.split(";", 1)[1].split()
        if len(fields) >= 7 and fields[2:4] == ["TABLE", "DATA"] and fields[4] == stage_schema:
            if fields[5] in generation.RELATION_NAMES:
                selected_entries.append(line)
                selected_tables.add(fields[5])
    assert selected_tables == set(generation.RELATION_NAMES)
    restore_list_path.write_text("\n".join(selected_entries) + "\n")
    restored = _run_archive_tool(
        [
            pg_restore,
            "--exit-on-error",
            "--no-owner",
            "--no-acl",
            f"--use-list={restore_list_path}",
            "--dbname",
            environment["PGDATABASE"],
            str(dump_path),
        ],
        environment,
    )
    assert restored.returncode == 0, restored.stderr


@pytest.mark.parametrize(
    ("dsn", "expected"),
    [
        (
            "postgresql://postgres@127.0.0.1:5440/hc_npi_archive_0123456789abcdef0123456789abcdef",
            True,
        ),
        (
            "postgresql://postgres@localhost:5432/ptg2_v3_lifecycle_test_ci_runner",
            True,
        ),
        (
            "postgresql://postgres@postgres:5432/ptg2_v3_lifecycle_test_ci_runner",
            True,
        ),
        (
            "postgresql://postgres@127.0.0.1:5440/ptg2_v3_lifecycle_test_ci_runner",
            False,
        ),
        ("postgresql://postgres@localhost:5432/another_database", False),
    ],
)
def test_native_test_database_guard(dsn: str, expected: bool) -> None:
    assert _is_owned_native_test_database(make_url(dsn)) is expected


def test_database_url_uses_pinned_ci_lifecycle_configuration(monkeypatch) -> None:
    monkeypatch.delenv(_DSN_ENV, raising=False)
    monkeypatch.setenv("HLTHPRT_DB_HOST", "postgres")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5432")
    monkeypatch.setenv("HLTHPRT_DB_USER", "postgres")
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", "fixture-secret")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", _CI_DATABASE)

    database_url = make_url(_database_url())

    assert database_url.drivername == "postgresql+asyncpg"
    assert database_url.host == "postgres"
    assert database_url.port == 5432
    assert database_url.username == "postgres"
    assert database_url.password == "fixture-secret"
    assert database_url.database == _CI_DATABASE


def _migration_module():
    module_spec = importlib.util.spec_from_file_location(
        "npi_result_generation_postgres_proof",
        _MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_migration(connection, schema_name: str) -> None:
    migration = _migration_module()

    def apply(sync_connection) -> None:
        migration.op = Operations(MigrationContext.configure(sync_connection))
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
            monkeypatch.delenv("DB_SCHEMA", raising=False)
            migration.upgrade()

    await connection.run_sync(apply)


async def _ensure_model_extensions(connection) -> None:
    await connection.execute(text("CREATE EXTENSION IF NOT EXISTS intarray"))
    await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))


async def _create_family(connection, schema_name: str, *, populated: bool) -> None:
    await _ensure_model_extensions(connection)
    await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    for table_name in generation.RELATION_NAMES:
        await connection.execute(
            text(f'CREATE TABLE "{schema_name}"."{table_name}" (synthetic_id bigint PRIMARY KEY, marker text NOT NULL)')
        )
        if populated:
            await connection.execute(text(f'INSERT INTO "{schema_name}"."{table_name}" VALUES (1, \'before\')'))
    await connection.execute(
        text(
            f'CREATE TABLE "{schema_name}".npi_canonical_publication_receipt ('
            "publication_ref text PRIMARY KEY, publication_generation bigint NOT NULL, "
            "chain_ref text NOT NULL, import_date date NOT NULL, "
            + ", ".join(f"{table_name}_table_oid bigint NOT NULL" for table_name in generation.RELATION_NAMES)
            + ")"
        )
    )
    await connection.execute(
        text(f'CREATE TABLE "{schema_name}".npi_canonical_publication_receipt_seal (publication_ref text PRIMARY KEY)')
    )
    await connection.execute(
        text(
            f'CREATE FUNCTION "{schema_name}".guard_npi_canonical_publication_after_seal() '
            "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"
        )
    )
    await _run_migration(connection, schema_name)


async def _add_owned_sequence(
    connection,
    schema_name: str,
    *,
    sequence_name: str,
) -> None:
    """Add a deliberately noncanonical serial name to prove catalog binding."""

    await connection.execute(text(f'CREATE SEQUENCE "{schema_name}"."{sequence_name}" AS bigint'))
    await connection.execute(
        text(f'ALTER SEQUENCE "{schema_name}"."{sequence_name}" OWNED BY "{schema_name}".npi.synthetic_id')
    )
    await connection.execute(
        text(
            f'ALTER TABLE "{schema_name}".npi ALTER COLUMN synthetic_id '
            f'SET DEFAULT nextval(\'"{schema_name}"."{sequence_name}"\'::regclass)'
        )
    )


async def _authority(sessions, schema_name: str):
    async with sessions() as session:
        return await generation.read_npi_result_generation_authority(
            session,
            schema_name=schema_name,
        )


async def _bootstrap(sessions, schema_name: str):
    async with sessions() as session, session.begin():
        return await generation.bootstrap_npi_result_generation(
            session,
            schema_name=schema_name,
        )


async def _capture_while_coordinate_write_continues(
    sessions,
    schema_name: str,
) -> tuple[archive.NpiSourceCapture, archive.NpiStageOwnership]:
    dataset_id = uuid4()
    async with sessions() as source_session, source_session.begin():
        capture = await archive.capture_npi_source(
            source_session,
            schema_name=schema_name,
            source_metadata={"release": "synthetic"},
        )
        async with sessions() as writer, writer.begin():
            await writer.execute(text("SET LOCAL lock_timeout TO '250ms'"))
            await writer.execute(text(f"UPDATE \"{schema_name}\".npi_address SET marker='after' WHERE synthetic_id=1"))
        async with sessions() as clone_session, clone_session.begin():
            await archive._clone_source(
                clone_session,
                capture,
                archive.npi_stage_schema(dataset_id),
            )
            ownership = await archive.capture_npi_stage_ownership(
                clone_session,
                dataset_id=dataset_id,
            )
            cloned_marker = await clone_session.scalar(
                text(f'SELECT marker FROM "{ownership.schema_name}".npi_address')
            )
            assert cloned_marker == "before"
    return capture, ownership


async def _prepare_tracked_source(sessions, schema_name: str) -> archive.NpiPreparedSource:
    prepared_sources = []

    async def retain_source(_session, prepared_source) -> None:
        """Record the exact owner as a coordinator would in this transaction."""

        prepared_sources.append(prepared_source)

    prepared = await archive.prepare_npi_archive_source(
        sessions,
        schema_name=schema_name,
        source_metadata={"release": "synthetic-after-coordinate-update"},
        dataset_id=uuid4(),
        on_prepared=retain_source,
    )
    assert prepared_sources == [prepared]
    assert prepared.manifest.capture_authority == "tracked-generation"
    return prepared


async def _copy_authority_to_destination(
    sessions,
    destination_schema: str,
    source_authority,
) -> None:
    async with sessions() as session, session.begin():
        await generation.publish_adopted_npi_result_generation(
            session,
            schema_name=destination_schema,
            source_generation=source_authority.serving_generation,
            canonical_provenance=None,
        )


async def _prepare_cutover(
    sessions,
    destination_schema: str,
    prepared: archive.NpiPreparedSource,
):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_npi_incumbent(
            session,
            schema_name=destination_schema,
        )
        owner_oid = await session.scalar(text("SELECT current_user::regrole::oid"))
        validation = await archive.prepare_npi_activation(
            session,
            ownership=prepared.ownership,
            manifest=prepared.manifest,
            package_id="a" * 64,
            sealed_owner_oid=owner_oid,
        )
    return incumbent, owner_oid, validation


async def _activate(
    session,
    *,
    prepared: archive.NpiPreparedSource,
    incumbent: archive.NpiIncumbent,
    owner_oid: int,
    validation: archive.NpiValidationReceipt,
) -> archive.NpiActivationReceipt:
    async def record_activation(active_session, _receipt) -> None:
        """Persist a synthetic controller record in the caller transaction."""

        await active_session.execute(text(f'INSERT INTO "{incumbent.schema_name}".activation_log VALUES (1)'))

    return await archive.activate_validated_npi_stage(
        session,
        ownership=prepared.ownership,
        manifest=prepared.manifest,
        incumbent=incumbent,
        validation_receipt=validation,
        cutover=archive.NpiCutoverAuthority("a" * 64, owner_oid, owner_oid, "automatic"),
        on_activated=record_activation,
    )


async def _assert_cutover_rollback(
    sessions,
    *,
    prepared: archive.NpiPreparedSource,
    incumbent: archive.NpiIncumbent,
    owner_oid: int,
    validation: archive.NpiValidationReceipt,
    previous_authority,
) -> None:
    with pytest.raises(RuntimeError, match="synthetic rollback"):
        async with sessions() as session, session.begin():
            await _activate(
                session,
                prepared=prepared,
                incumbent=incumbent,
                owner_oid=owner_oid,
                validation=validation,
            )
            raise RuntimeError("synthetic rollback")
    assert await _authority(sessions, incumbent.schema_name) == previous_authority
    async with sessions() as session:
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema_name)"),
                {"schema_name": prepared.ownership.schema_name},
            )
            is not None
        )
        assert await session.scalar(text(f'SELECT count(*) FROM "{incumbent.schema_name}".activation_log')) == 0


async def _assert_trigger_scope_after_cutover(
    sessions,
    *,
    destination_schema: str,
    receipt: archive.NpiActivationReceipt,
) -> None:
    before_mutations = await _authority(sessions, destination_schema)
    predecessor = receipt.predecessor_schema_name
    assert predecessor is not None
    async with sessions() as session, session.begin():
        await session.execute(text(f'UPDATE "{predecessor}".npi_address SET marker=marker'))
    assert await _authority(sessions, destination_schema) == before_mutations
    async with sessions() as session, session.begin():
        await session.execute(text(f'UPDATE "{destination_schema}".npi_address SET marker=marker'))
    after_live_mutation = await _authority(sessions, destination_schema)
    assert after_live_mutation.local_generation == before_mutations.local_generation + 1
    assert after_live_mutation.serving_generation.origin_lineage_id == (after_live_mutation.local_lineage_id)


async def _assert_legacy_automatic_rejected(
    sessions,
    *,
    destination_schema: str,
    prepared: archive.NpiPreparedSource,
) -> None:
    incumbent, owner_oid, validation = await _prepare_cutover(
        sessions,
        destination_schema,
        prepared,
    )

    async def reject_unexpected_activation(_session, _receipt) -> None:
        """Fail if a rejected automatic cutover reaches record installation."""

        raise AssertionError("legacy automatic cutover reached installation")

    async with sessions() as session, session.begin():
        with pytest.raises(archive.NpiResultArchiveError, match="legacy incumbent"):
            await archive.activate_validated_npi_stage(
                session,
                ownership=prepared.ownership,
                manifest=prepared.manifest,
                incumbent=incumbent,
                validation_receipt=validation,
                cutover=archive.NpiCutoverAuthority(
                    "a" * 64,
                    owner_oid,
                    owner_oid,
                    "automatic",
                ),
                on_activated=reject_unexpected_activation,
            )


async def _complete_activation_proof(
    sessions,
    *,
    source_schema: str,
    destination_schema: str,
    source_initial,
    cleanup_schemas: set[str],
) -> None:
    prepared = await _prepare_tracked_source(sessions, source_schema)
    cleanup_schemas.add(prepared.ownership.schema_name)
    await _assert_legacy_automatic_rejected(
        sessions,
        destination_schema=destination_schema,
        prepared=prepared,
    )
    await _copy_authority_to_destination(sessions, destination_schema, source_initial)
    destination_initial = await _authority(sessions, destination_schema)
    incumbent, owner_oid, validation = await _prepare_cutover(
        sessions,
        destination_schema,
        prepared,
    )
    await _assert_cutover_rollback(
        sessions,
        prepared=prepared,
        incumbent=incumbent,
        owner_oid=owner_oid,
        validation=validation,
        previous_authority=destination_initial,
    )
    async with sessions() as session, session.begin():
        receipt = await _activate(
            session,
            prepared=prepared,
            incumbent=incumbent,
            owner_oid=owner_oid,
            validation=validation,
        )
    cleanup_schemas.discard(prepared.ownership.schema_name)
    cleanup_schemas.add(receipt.predecessor_schema_name)
    adopted = await _authority(sessions, destination_schema)
    source_current = await _authority(sessions, source_schema)
    assert adopted.local_generation == destination_initial.local_generation
    assert adopted.serving_generation == source_current.serving_generation
    await _assert_trigger_scope_after_cutover(
        sessions,
        destination_schema=destination_schema,
        receipt=receipt,
    )


async def _drop_schemas(engine, schema_names: set[str | None]) -> None:
    async with engine.begin() as connection:
        for schema_name in sorted(name for name in schema_names if name is not None):
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


async def _publication_receipt(connection, schema_name: str):
    relation_oids = []
    for table_name in generation.RELATION_NAMES:
        relation_oids.append(
            await connection.fetchval(
                "SELECT to_regclass($1)::oid::bigint",
                f"{schema_name}.{table_name}",
            )
        )
    return build_npi_canonical_publication_receipt(
        NpiCanonicalPublicationInput(
            "run_npi_generation",
            "run_npi_generation:" + "a" * 32,
            "2026-09-14T12:00:00.000000+00:00",
            "penpc1_" + "b" * 43,
            "2026-09-14",
            tuple(relation_oids),
            (1, 1, 1, 1, 1, 1),
        ),
        publication_generation=1,
        created_at="2026-09-14T12:01:00.000000+00:00",
    )


@pytest.mark.asyncio
async def test_ordinary_publication_generation_is_transactional() -> None:
    """Advance exact current OIDs and roll the authority back with publication."""

    engine = create_async_engine(_database_url(), poolclass=NullPool)
    schema_name = "npi_publish_" + uuid4().hex
    connection = None
    try:
        async with engine.begin() as sqlalchemy_connection:
            await _create_family(sqlalchemy_connection, schema_name, populated=True)
        connection = await asyncpg.connect(_asyncpg_database_url())
        receipt = await _publication_receipt(connection, schema_name)
        transaction = connection.transaction()
        await transaction.start()
        advanced = await generation.publish_local_npi_result_generation(
            connection,
            schema_name=schema_name,
            receipt=receipt,
        )
        assert advanced.local_generation == 1
        assert advanced.relation_oids == receipt.relation_oids
        await transaction.rollback()
        authority_row = await connection.fetchrow(f'SELECT * FROM "{schema_name}".npi_result_generation')
        assert authority_row["local_generation"] == 0
        assert authority_row["origin_generation"] is None
    finally:
        if connection is not None:
            await connection.close()
        try:
            async with engine.begin() as sqlalchemy_connection:
                await sqlalchemy_connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_npi_snapshot_generation_activation_and_rollback() -> None:
    """Prove legacy classification, MVCC capture, cutover rollback, and trigger scope."""

    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "npi_source_" + uuid4().hex
    destination_schema = "npi_destination_" + uuid4().hex
    cleanup_schemas: set[str] = {source_schema, destination_schema}
    try:
        async with engine.begin() as connection:
            await _create_family(connection, source_schema, populated=True)
            await _create_family(connection, destination_schema, populated=True)
            await connection.execute(text(f'CREATE TABLE "{destination_schema}".activation_log (id integer)'))
        async with sessions() as session, session.begin():
            legacy_capture = await archive.capture_npi_source(
                session,
                schema_name=source_schema,
                source_metadata={"release": "legacy"},
            )
        assert legacy_capture.capture_authority == "legacy-manual"
        source_initial = await _bootstrap(sessions, source_schema)
        _, old_snapshot_owner = await _capture_while_coordinate_write_continues(
            sessions,
            source_schema,
        )
        cleanup_schemas.add(old_snapshot_owner.schema_name)
        source_current = await _authority(sessions, source_schema)
        assert source_current.local_generation == source_initial.local_generation + 1
        async with sessions() as session, session.begin():
            await archive.cleanup_npi_stage(session, old_snapshot_owner)
        cleanup_schemas.discard(old_snapshot_owner.schema_name)
        await _complete_activation_proof(
            sessions,
            source_schema=source_schema,
            destination_schema=destination_schema,
            source_initial=source_initial,
            cleanup_schemas=cleanup_schemas,
        )
    finally:
        try:
            await _drop_schemas(engine, cleanup_schemas)
        finally:
            await engine.dispose()


async def _create_paired_metadata_source(
    engine,
    source_schema: str,
    sequence_name: str,
) -> None:
    """Create the canonical family and a deliberately unrelated ledger sequence."""

    async with engine.begin() as connection:
        await _create_family(connection, source_schema, populated=True)
        # The real serving schema also contains publication-ledger identity
        # sequences, which must not become part of the six-table clone.
        await connection.execute(
            text(
                f'CREATE TABLE "{source_schema}".unrelated_publication_ledger '
                "(generation bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"
            )
        )
        await _add_owned_sequence(
            connection,
            source_schema,
            sequence_name=sequence_name,
        )


async def _prepare_paired_metadata_source(
    sessions,
    *,
    source_schema: str,
    dataset_id: UUID,
) -> tuple[archive.NpiPreparedSource, list[object], list[archive.NpiPreparedSource]]:
    """Prepare one clone while proving metadata and durable callbacks share a view."""

    await _bootstrap(sessions, source_schema)
    captured_metadata_sessions: list[object] = []
    prepared_archives: list[archive.NpiPreparedSource] = []

    async def capture_metadata(session):
        captured_metadata_sessions.append(session)
        marker = await session.scalar(text(f'SELECT marker FROM "{source_schema}".npi'))
        return {"release": marker, "contract": "paired-call"}

    async def persist(_session, frozen):
        assert frozen.ownership.freeze_function_oid is not None
        assert len(frozen.ownership.freeze_trigger_oids) == 6
        assert len(frozen.ownership.freeze_catalog_versions) == 13
        json.dumps(frozen.ownership.as_dict(), sort_keys=True)
        prepared_archives.append(frozen)

    prepared = await archive.prepare_npi_archive_source(
        sessions,
        schema_name=source_schema,
        source_metadata=None,
        dataset_id=dataset_id,
        on_prepared=persist,
        source_metadata_factory=capture_metadata,
    )
    return prepared, captured_metadata_sessions, prepared_archives


async def _assert_paired_stage_sequence(
    sessions,
    prepared: archive.NpiPreparedSource,
    source_schema: str,
    sequence_name: str,
) -> None:
    """Require the cloned family to own a new sequence with a local default."""

    assert len(prepared.ownership.sequence_oids) == 1
    stage_sequence = prepared.ownership.sequence_oids[0]
    assert stage_sequence[0] == sequence_name
    assert stage_sequence[2:] == ("npi", "synthetic_id")
    async with sessions() as session, session.begin():
        source_sequence_oid = await session.scalar(
            text("SELECT to_regclass(:qualified)::oid::bigint"),
            {"qualified": f"{source_schema}.{sequence_name}"},
        )
        default_expression = await session.scalar(
            text(
                "SELECT pg_get_expr(default_value.adbin,default_value.adrelid) "
                "FROM pg_attrdef AS default_value "
                "JOIN pg_attribute AS column_value "
                "ON column_value.attrelid=default_value.adrelid "
                "AND column_value.attnum=default_value.adnum "
                "WHERE default_value.adrelid=to_regclass(:table_name) "
                "AND column_value.attname='synthetic_id'"
            ),
            {"table_name": f"{prepared.ownership.schema_name}.npi"},
        )
    assert stage_sequence[1] != source_sequence_oid
    assert prepared.ownership.schema_name in default_expression
    assert source_schema not in default_expression


async def _assert_frozen_paired_stage_rejects_write(
    sessions,
    prepared: archive.NpiPreparedSource,
) -> None:
    """Exercise the SQL guard before the clone is sent to an archive copier."""

    with pytest.raises(DBAPIError, match="npi_result_archive_is_frozen"):
        async with sessions() as session, session.begin():
            await session.execute(
                text(f"UPDATE \"{prepared.ownership.schema_name}\".npi SET marker='replacement' WHERE synthetic_id=1")
            )


async def _export_paired_archive(
    sessions,
    prepared: archive.NpiPreparedSource,
) -> list[archive.NpiStageCapture]:
    """Capture the callback payload emitted by a prepared NPI archive export."""

    archive_captures: list[archive.NpiStageCapture] = []

    async def archive_copy(capture):
        archive_captures.append(capture)

    await archive.export_prepared_npi_archive(
        sessions,
        prepared=prepared,
        archive_copy=archive_copy,
    )
    return archive_captures


def _assert_paired_archive_capture(
    prepared: archive.NpiPreparedSource,
    archive_captures: list[archive.NpiStageCapture],
) -> None:
    """Require export to preserve prepared manifest and ownership exactly."""

    assert archive_captures == [
        archive.NpiStageCapture(
            prepared.manifest,
            prepared.ownership,
            archive_captures[0].postgres_snapshot,
        )
    ]


async def _replace_frozen_paired_stage_row(
    sessions,
    prepared: archive.NpiPreparedSource,
) -> None:
    """Restore the same trigger definition after an in-place clone mutation."""

    stage_schema = prepared.ownership.schema_name
    async with sessions() as session, session.begin():
        await session.execute(text(f'DROP TRIGGER "{archive._FREEZE_WRITE_TRIGGER}" ON "{stage_schema}".npi'))
        await session.execute(text(f"UPDATE \"{stage_schema}\".npi SET marker='replacement' WHERE synthetic_id=1"))
        await session.execute(
            text(
                f'CREATE TRIGGER "{archive._FREEZE_WRITE_TRIGGER}" '
                f'BEFORE INSERT OR UPDATE OR DELETE ON "{stage_schema}".npi '
                f'FOR EACH STATEMENT EXECUTE FUNCTION "{stage_schema}".'
                f'"{archive._FREEZE_FUNCTION}"()'
            )
        )
        await session.execute(
            text(f'ALTER TABLE "{stage_schema}".npi ENABLE ALWAYS TRIGGER "{archive._FREEZE_WRITE_TRIGGER}"')
        )


async def _assert_frozen_retry_rejected(
    sessions,
    prepared: archive.NpiPreparedSource,
) -> None:
    """Reject export and cleanup when a frozen clone's catalog seal changes."""

    async def never_copy(_capture):
        pytest.fail("changed clone reached archive copier")

    with pytest.raises(archive.NpiResultArchiveError, match="stage ownership differs"):
        await archive.export_prepared_npi_archive(
            sessions,
            prepared=prepared,
            archive_copy=never_copy,
        )
    with pytest.raises(archive.NpiResultArchiveError, match="stage ownership differs"):
        async with sessions() as session, session.begin():
            await archive.cleanup_npi_stage(session, prepared.ownership)


@pytest.mark.asyncio
async def test_paired_metadata_sequence_clone_and_frozen_retry_contract() -> None:
    """Exercise the paired IC call and reject same-count clone replacement."""

    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "npi_paired_source_" + uuid4().hex
    sequence_name = "ordinary_import_owned_" + uuid4().hex[:12]
    dataset_id = uuid4()
    try:
        await _create_paired_metadata_source(engine, source_schema, sequence_name)
        prepared, captured_metadata_sessions, prepared_archives = await _prepare_paired_metadata_source(
            sessions,
            source_schema=source_schema,
            dataset_id=dataset_id,
        )
        assert captured_metadata_sessions and prepared_archives == [prepared]
        assert prepared.manifest.source_metadata == {
            "contract": "paired-call",
            "release": "before",
        }
        await _assert_paired_stage_sequence(
            sessions,
            prepared,
            source_schema,
            sequence_name,
        )
        await _assert_frozen_paired_stage_rejects_write(sessions, prepared)
        archive_captures = await _export_paired_archive(sessions, prepared)
        _assert_paired_archive_capture(prepared, archive_captures)
        await _replace_frozen_paired_stage_row(sessions, prepared)
        await _assert_frozen_retry_rejected(sessions, prepared)
    finally:
        schemas = {source_schema, archive.npi_stage_schema(dataset_id)}
        try:
            await _drop_schemas(engine, schemas)
        finally:
            await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["disable_enable", "replace_trigger", "replace_function"])
@pytest.mark.parametrize("during_preparation", [False, True])
async def test_frozen_retry_rejects_same_oid_catalog_mutation(
    mutation: str,
    during_preparation: bool,
) -> None:
    """Restoring a guard's definition does not restore its original seal."""

    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "npi_seal_source_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = archive.npi_stage_schema(dataset_id)

    async def mutate(session):
        await _mutate_frozen_catalog(session, stage_schema, mutation)

    async def persist(session, _prepared):
        if during_preparation:
            await mutate(session)

    try:
        async with engine.begin() as connection:
            await _create_family(connection, source_schema, populated=True)
        prepared = await archive.prepare_npi_archive_source(
            sessions,
            schema_name=source_schema,
            source_metadata={"contract": "catalog-seal"},
            dataset_id=dataset_id,
            on_prepared=persist,
        )
        if not during_preparation:
            async with sessions() as session, session.begin():
                await mutate(session)
        async with sessions() as session, session.begin():
            changed = await archive.capture_npi_stage_ownership(session, dataset_id=dataset_id)
            assert changed.freeze_function_oid == prepared.ownership.freeze_function_oid
            assert changed.freeze_trigger_oids == prepared.ownership.freeze_trigger_oids
            assert changed.freeze_catalog_versions != prepared.ownership.freeze_catalog_versions
            assert await archive._manifest_tables(session, stage_schema) == prepared.manifest.tables
        await _assert_frozen_retry_rejected(sessions, prepared)
    finally:
        try:
            await _drop_schemas(engine, {source_schema, stage_schema})
        finally:
            await engine.dispose()


async def _mutate_frozen_catalog(
    session,
    stage_schema: str,
    mutation: str,
) -> None:
    """Mutate and restore a guard while preserving its OID and final definition."""

    relation = f'"{stage_schema}".npi'
    trigger = f'"{archive._FREEZE_WRITE_TRIGGER}"'
    freeze_function = f'"{stage_schema}"."{archive._FREEZE_FUNCTION}"()'
    if mutation == "disable_enable":
        await session.execute(text(f"ALTER TABLE {relation} DISABLE TRIGGER {trigger}"))
    elif mutation == "replace_trigger":
        await session.execute(
            text(
                f"CREATE OR REPLACE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE "
                f"ON {relation} FOR EACH STATEMENT WHEN (false) EXECUTE FUNCTION {freeze_function}"
            )
        )
    else:
        await session.execute(
            text(
                f"CREATE OR REPLACE FUNCTION {freeze_function} RETURNS trigger LANGUAGE plpgsql "
                "SECURITY DEFINER SET search_path=pg_catalog "
                "AS $function$ BEGIN RETURN NULL; END; $function$"
            )
        )
    await session.execute(text(f"UPDATE {relation} SET marker='replacement' WHERE synthetic_id=1"))
    if mutation == "replace_trigger":
        await session.execute(
            text(
                f"CREATE OR REPLACE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE "
                f"ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {freeze_function}"
            )
        )
    elif mutation == "replace_function":
        await session.execute(
            text(
                f"CREATE OR REPLACE FUNCTION {freeze_function} RETURNS trigger LANGUAGE plpgsql "
                "SECURITY DEFINER SET search_path=pg_catalog "
                f"AS $function$ {archive._FREEZE_FUNCTION_BODY} $function$"
            )
        )
    await session.execute(text(f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER {trigger}"))


def _install_timeout_observers(
    monkeypatch,
    timeout_observations: list[tuple[str, str]],
    stage_sessions: list[object],
) -> None:
    """Record timeout scopes without changing archive export behavior."""

    original_lock = archive._lock_family
    original_validate = archive._validate_stage_manifest
    original_snapshot = archive._export_stage_snapshot

    async def observed_lock(session, *args, **kwargs):
        timeout_observations.append(("lock", await session.scalar(text("SHOW statement_timeout"))))
        return await original_lock(session, *args, **kwargs)

    async def slow_validation(session, *args, **kwargs):
        timeout_observations.append(("validation", await session.scalar(text("SHOW statement_timeout"))))
        await session.execute(text("SELECT pg_sleep(0.04)"))
        return await original_validate(session, *args, **kwargs)

    async def observed_snapshot(session):
        timeout_observations.append(("snapshot", await session.scalar(text("SHOW statement_timeout"))))
        stage_sessions.append(session)
        return await original_snapshot(session)

    monkeypatch.setattr(archive, "_CAPTURE_TIMEOUT", "20ms")
    monkeypatch.setattr(archive, "_lock_family", observed_lock)
    monkeypatch.setattr(archive, "_validate_stage_manifest", slow_validation)
    monkeypatch.setattr(archive, "_export_stage_snapshot", observed_snapshot)


@pytest.mark.asyncio
async def test_prepared_npi_export_keeps_long_validation_outside_capture_timeout(monkeypatch) -> None:
    """Keep row-count validation under the caller limit while locks stay bounded."""

    assert archive._LOCK_TIMEOUT == "500ms"
    assert archive._CAPTURE_TIMEOUT == "5s"
    engine = create_async_engine(
        _database_url(),
        poolclass=NullPool,
        connect_args={"server_settings": {"statement_timeout": "500ms"}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema_name = "npi_export_timeout_" + uuid4().hex
    prepared = None
    timeout_observations: list[tuple[str, str]] = []
    stage_sessions: list[object] = []
    try:
        async with engine.begin() as connection:
            await _create_family(connection, schema_name, populated=True)
        await _bootstrap(sessions, schema_name)
        prepared = await _prepare_tracked_source(sessions, schema_name)
        _install_timeout_observers(
            monkeypatch,
            timeout_observations,
            stage_sessions,
        )

        archive_captures: list[archive.NpiStageCapture] = []

        async def archive_copy(capture):
            archive_captures.append(capture)
            timeout_observations.append(("copy", await stage_sessions[-1].scalar(text("SHOW statement_timeout"))))

        await archive.export_prepared_npi_archive(
            sessions,
            prepared=prepared,
            archive_copy=archive_copy,
        )

        assert archive_captures and archive_captures[0].ownership == prepared.ownership
        assert timeout_observations == [
            ("lock", "20ms"),
            ("validation", "500ms"),
            ("snapshot", "20ms"),
            ("copy", "500ms"),
        ]
    finally:
        schemas = {schema_name}
        if prepared is not None:
            schemas.add(prepared.ownership.schema_name)
        try:
            await _drop_schemas(engine, schemas)
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_npi_model_restore_layout_has_exact_owned_relations() -> None:
    """Create and exactly clean the model-driven data-only restore target."""

    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    dataset_id = uuid4()
    try:
        async with sessions() as session, session.begin():
            await _ensure_model_extensions(session)
            ownership = await archive.precreate_npi_restore(
                session,
                dataset_id=dataset_id,
            )
            table_receipts = await archive._manifest_tables(
                session,
                ownership.schema_name,
            )
        assert tuple(table_name for table_name, _ in ownership.relation_oids) == tuple(
            sorted(generation.RELATION_NAMES)
        )
        assert len(table_receipts) == 6
        async with sessions() as session, session.begin():
            await archive.cleanup_npi_stage(session, ownership)
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(f'DROP SCHEMA IF EXISTS "{archive.npi_stage_schema(dataset_id)}" CASCADE')
                )
        finally:
            await engine.dispose()


async def _build_dumped_npi_archive(
    sessions,
    *,
    source_dataset_id: UUID,
    restore_dataset_id: UUID,
    dump_path: Path,
    restored_npi: int,
):
    source_schema = archive.npi_stage_schema(source_dataset_id)
    async with sessions() as session, session.begin():
        await _ensure_model_extensions(session)
        await archive.precreate_npi_restore(session, dataset_id=source_dataset_id)
        await session.execute(
            text(f'INSERT INTO "{source_schema}".npi (npi) VALUES (:npi)'),
            {"npi": restored_npi},
        )
        await _run_migration(await session.connection(), source_schema)

    async def retain_prepared(_session, _prepared) -> None:
        return None

    prepared = await archive.prepare_npi_archive_source(
        sessions,
        schema_name=source_schema,
        source_metadata={"release": "synthetic-data-only"},
        dataset_id=restore_dataset_id,
        on_prepared=retain_prepared,
    )
    assert prepared.manifest.capture_authority == "legacy-manual"
    assert len(prepared.ownership.sequence_oids) == 1

    async def dump_capture(capture) -> None:
        _dump_npi_stage(capture, dump_path)

    await archive.export_prepared_npi_archive(
        sessions,
        prepared=prepared,
        archive_copy=dump_capture,
    )
    async with sessions() as session, session.begin():
        await archive.cleanup_npi_stage(session, prepared.ownership)
    return prepared.manifest


async def _prepare_data_only_cutover(
    sessions,
    *,
    restore_dataset_id: UUID,
    destination_schema: str,
    manifest,
    dump_path: Path,
    restore_list_path: Path,
    restored_npi: int,
):
    stage_schema = archive.npi_stage_schema(restore_dataset_id)
    async with sessions() as session, session.begin():
        restored_ownership = await archive.precreate_npi_restore(session, dataset_id=restore_dataset_id)
    _restore_npi_table_data(dump_path, restore_list_path, stage_schema)
    sequence_name, _sequence_oid, owner_table, owner_column = restored_ownership.sequence_oids[0]
    assert (owner_table, owner_column) == ("npi", "npi")
    sequence_relation = f'"{stage_schema}"."{sequence_name}"'
    async with sessions() as session, session.begin():
        initial_state = await session.execute(text(f"SELECT last_value::bigint, is_called FROM {sequence_relation}"))
        assert tuple(initial_state.one()) == (1, False)
        await archive.validate_npi_stage(session, ownership=restored_ownership, manifest=manifest)
        incumbent = await archive.capture_npi_incumbent(session, schema_name=destination_schema)
        owner_oid = await session.scalar(text("SELECT current_user::regrole::oid"))
        validation = await archive.prepare_npi_activation(
            session,
            ownership=restored_ownership,
            manifest=manifest,
            package_id="b" * 64,
            sealed_owner_oid=owner_oid,
        )
        aligned_state = await session.execute(text(f"SELECT last_value::bigint, is_called FROM {sequence_relation}"))
        assert tuple(aligned_state.one()) == (restored_npi, True)
    return restored_ownership, incumbent, owner_oid, validation


async def _activate_and_assert_restored_sequence(
    sessions,
    *,
    ownership,
    manifest,
    incumbent,
    validation,
    owner_oid: int,
):
    sequence_name = ownership.sequence_oids[0][0]
    sequence_relation = f'"{ownership.schema_name}"."{sequence_name}"'
    async with sessions() as session, session.begin():
        await session.execute(text(f"SELECT pg_catalog.setval('{sequence_relation}'::regclass,1,false)"))

    async def record_activation(_session, _receipt) -> None:
        return None

    async with sessions() as session, session.begin():
        receipt = await archive.activate_validated_npi_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            incumbent=incumbent,
            validation_receipt=validation,
            cutover=archive.NpiCutoverAuthority("b" * 64, owner_oid, owner_oid, "manual"),
            on_activated=record_activation,
        )
    async with sessions() as session:
        highest_npi = await session.scalar(text(f'SELECT max(npi) FROM "{incumbent.schema_name}".npi'))
        next_npi = await session.scalar(
            text("SELECT nextval(pg_get_serial_sequence(:table_name,'npi'))"),
            {"table_name": f"{incumbent.schema_name}.npi"},
        )
    assert next_npi == highest_npi + 1
    return receipt


@pytest.mark.asyncio
async def test_data_only_restore_advances_owned_sequence_before_activation(tmp_path: Path) -> None:
    """Restore only table data, activate it, and allocate beyond the restored NPI."""

    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_dataset_id = uuid4()
    restore_dataset_id = uuid4()
    destination_schema = "npi_data_restore_" + uuid4().hex
    restored_npi = 1_999_999_999
    cleanup_schemas = {
        archive.npi_stage_schema(source_dataset_id),
        archive.npi_stage_schema(restore_dataset_id),
        destination_schema,
    }
    try:
        async with engine.begin() as connection:
            await _create_family(connection, destination_schema, populated=False)
        manifest = await _build_dumped_npi_archive(
            sessions,
            source_dataset_id=source_dataset_id,
            restore_dataset_id=restore_dataset_id,
            dump_path=tmp_path / "npi-result.dump",
            restored_npi=restored_npi,
        )
        ownership, incumbent, owner_oid, validation = await _prepare_data_only_cutover(
            sessions,
            restore_dataset_id=restore_dataset_id,
            destination_schema=destination_schema,
            manifest=manifest,
            dump_path=tmp_path / "npi-result.dump",
            restore_list_path=tmp_path / "npi-result-table-data.list",
            restored_npi=restored_npi,
        )
        receipt = await _activate_and_assert_restored_sequence(
            sessions,
            ownership=ownership,
            manifest=manifest,
            incumbent=incumbent,
            validation=validation,
            owner_oid=owner_oid,
        )
        cleanup_schemas.add(receipt.predecessor_schema_name)
    finally:
        try:
            await _drop_schemas(engine, cleanup_schemas)
        finally:
            await engine.dispose()


def _install_ordinary_staging_collaborators(
    monkeypatch,
    ordinary_npi,
    session,
    connection,
) -> None:
    """Route ordinary staging helpers through the test transaction and session."""

    async def status(statement):
        await session.execute(text(statement))

    async def create_table(table, *, checkfirst):
        await connection.run_sync(
            lambda sync_connection: table.create(
                sync_connection,
                checkfirst=checkfirst,
            )
        )

    monkeypatch.setattr(ordinary_npi.db, "status", status)
    monkeypatch.setattr(ordinary_npi.db, "create_table", create_table)


async def _create_ordinary_model_indexes(
    session,
    source_schema: str,
    staged_model,
    index_definitions,
    *,
    has_postgis: bool,
) -> None:
    """Create the ordinary staging indexes supported by the local extension set."""

    for index_definition in index_definitions:
        if archive._uses_postgis_index(index_definition) and not has_postgis:
            continue
        await session.execute(
            text(
                archive._additional_index_sql(
                    source_schema,
                    staged_model,
                    index_definition,
                )
            )
        )


def _ordinary_index_suffixes(
    model_type,
    index_definitions,
    *,
    has_postgis: bool,
) -> tuple[str, ...]:
    """Return ordinary rotation suffixes for indexes present in the test schema."""

    all_indexes = tuple(getattr(model_type, "__my_initial_indexes__", ()) or ()) + tuple(index_definitions)
    return tuple(
        definition.get("name", "_".join(definition["index_elements"]))
        for definition in all_indexes
        if not (archive._uses_postgis_index(definition) and not has_postgis)
    )


async def _rotate_ordinary_model(
    session,
    ordinary_npi,
    driver,
    *,
    source_schema: str,
    import_date: str,
    model_type,
    has_postgis: bool,
) -> None:
    """Build indexes then rotate one ordinary NPI staging model to canonical."""

    from process.ext.utils import make_class

    staged_model = make_class(model_type, import_date)
    index_definitions = tuple(getattr(model_type, "__my_additional_indexes__", ()) or ())
    await _create_ordinary_model_indexes(
        session,
        source_schema,
        staged_model,
        index_definitions,
        has_postgis=has_postgis,
    )
    await ordinary_npi._rotate_npi_canonical_table(
        driver,
        schema=source_schema,
        live_table=model_type.__tablename__,
        stage_table=staged_model.__tablename__,
        index_suffixes=_ordinary_index_suffixes(
            model_type,
            index_definitions,
            has_postgis=has_postgis,
        ),
    )


async def _prepare_and_rotate_ordinary_npi_models(
    session,
    ordinary_npi,
    driver,
    *,
    source_schema: str,
    import_date: str,
) -> None:
    """Run ordinary NPI staging and canonical publication for every model table."""

    await ordinary_npi._prepare_npi_staging(import_date, source_schema)
    has_postgis = await archive._has_postgis(session)
    for model_type in archive._MODEL_TYPES:
        await _rotate_ordinary_model(
            session,
            ordinary_npi,
            driver,
            source_schema=source_schema,
            import_date=import_date,
            model_type=model_type,
            has_postgis=has_postgis,
        )


async def _assert_primary_indexes(
    session,
    source_schema: str,
    restored_schema: str,
) -> None:
    """Require matching ordinary and model-driven primary indexes per table."""

    for table_name in generation.RELATION_NAMES:
        assert await session.scalar(
            text("SELECT to_regclass(:name) IS NOT NULL"),
            {"name": f"{source_schema}.{table_name}_idx_primary"},
        )
        assert await session.scalar(
            text("SELECT to_regclass(:name) IS NOT NULL"),
            {"name": f"{restored_schema}.{table_name}_idx_primary"},
        )


@pytest.mark.asyncio
async def test_model_restore_matches_ordinary_staging_publication_route(
    monkeypatch,
) -> None:
    """Match the physical family produced by ordinary stage creation and rotation."""

    ordinary_npi = importlib.import_module("process.npi")
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    dataset_id = uuid4()
    import_date = "20990101"
    source_schema = "mrf"
    try:
        async with sessions() as session, session.begin():
            await _ensure_model_extensions(session)
            await session.execute(text(f'CREATE SCHEMA "{source_schema}"'))
            connection = await session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            _install_ordinary_staging_collaborators(
                monkeypatch,
                ordinary_npi,
                session,
                connection,
            )
            await _prepare_and_rotate_ordinary_npi_models(
                session,
                ordinary_npi,
                driver,
                source_schema=source_schema,
                import_date=import_date,
            )
            source_receipts = await archive._manifest_tables(session, source_schema)
            ownership = await archive.precreate_npi_restore(session, dataset_id=dataset_id)
            restored_receipts = await archive._manifest_tables(session, ownership.schema_name)
            await _assert_primary_indexes(
                session,
                source_schema,
                ownership.schema_name,
            )
        assert [(receipt.model_name, receipt.table_name, receipt.schema_sha256) for receipt in restored_receipts] == [
            (receipt.model_name, receipt.table_name, receipt.schema_sha256) for receipt in source_receipts
        ]
    finally:
        try:
            await _drop_schemas(
                engine,
                {source_schema, archive.npi_stage_schema(dataset_id)},
            )
        finally:
            await engine.dispose()
