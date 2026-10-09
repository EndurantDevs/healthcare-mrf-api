# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The read fixture uses real migrations and preserves historical DDL stages."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests import custom_import_postgres_support as support


@pytest.mark.parametrize(
    "segmented,snapshots,migration_through,expected_tail",
    [
        (True, True, None, support._SNAPSHOT_MIGRATION_NAMES),
        (True, False, None, ()),
        (False, True, None, ()),
        (True, True, "20261002010000", support._SNAPSHOT_MIGRATION_NAMES[:1]),
        (True, True, "20261002030000", support._SNAPSHOT_MIGRATION_NAMES[:3]),
        (True, True, "20261002040000", support._SNAPSHOT_MIGRATION_NAMES[:4]),
        (True, True, "20261005020000", support._SNAPSHOT_MIGRATION_NAMES[:5]),
        (True, True, "20261005030000", support._SNAPSHOT_MIGRATION_NAMES[:6]),
        (True, True, "20261005040000", support._SNAPSHOT_MIGRATION_NAMES[:7]),
        (True, True, "20261005060000", support._SNAPSHOT_MIGRATION_NAMES[:9]),
    ],
)
def test_fixture_installs_snapshot_prerequisites_exactly_once(
    monkeypatch, segmented, snapshots, migration_through, expected_tail
):
    installed_migrations = []
    connection = object()
    schema = "custom_import_publication_0123456789abcdef"

    def load(path, module_name):
        assert path.is_file()
        migration = SimpleNamespace()

        def upgrade():
            assert migration._schema() == schema
            assert migration.op is connection
            installed_migrations.append(path.stem)

        migration.upgrade = upgrade
        return migration

    monkeypatch.setattr(support, "_migration", load)
    monkeypatch.setattr(support, "MigrationContext", SimpleNamespace(configure=lambda value: value))
    monkeypatch.setattr(support, "Operations", lambda value: value)
    support._install_custom_import_migrations(connection, schema, segmented, snapshots, migration_through)
    expected_migrations = [
        "20260914120000_custom_import_v1_schema",
        "20260917130000_custom_import_generation_finality",
        "20260922000000_custom_import_durable_parquet_capture",
        "20260922010000_custom_import_execution_request_identity",
        "20260923030000_custom_import_source_binding",
    ]
    if segmented:
        expected_migrations.append("20261002000000_custom_import_segmented_capture")
    assert installed_migrations == expected_migrations + list(expected_tail)
    assert len(installed_migrations) == len(set(installed_migrations))


def test_fixture_loads_the_actual_snapshot_read_and_write_functions():
    name = "20261005030000_custom_import_snapshot_storage"
    module = support._migration(support._ROOT / "alembic" / "versions" / f"{name}.py", "snapshot_fixture_contract")
    assert "resolve_custom_import_snapshot_relations" in module._READ_BINDING_BODY
    assert "IN ACCESS SHARE MODE" in module._READ_BINDING_BODY
    assert "FOR SHARE" not in module._READ_BINDING_BODY
    assert "lock_custom_import_snapshot_attempt" in module._WRITE_BINDING_BODY
    assert "FOR UPDATE" in module._WRITE_BINDING_BODY


def test_fixture_current_chain_ends_at_child_presence_decode():
    assert support._SNAPSHOT_MIGRATION_NAMES[-8:] == (
        "20261005030000_custom_import_snapshot_storage",
        "20261005040000_custom_import_bulk_snapshot_writers",
        "20261005050000_custom_import_legacy_snapshot_writers",
        "20261005060000_custom_import_snapshot_finality",
        "20261005070000_custom_import_materialization_storage",
        "20261005080000_custom_import_writer_cutover",
        "20261007000000_custom_import_rejection_anti_joins",
        "20261009000000_custom_import_child_presence_decode",
    )


@pytest.mark.parametrize(
    "segmented,snapshots,migration_through",
    [(True, True, "missing"), (False, True, "20261005030000"), (True, False, "20261005030000")],
)
def test_fixture_rejects_unknown_or_disabled_stage_before_loading(monkeypatch, segmented, snapshots, migration_through):
    load_migration = Mock()
    monkeypatch.setattr(support, "_migration", load_migration)
    with pytest.raises(ValueError, match="fixture migration stop"):
        support._install_custom_import_migrations(object(), "unused", segmented, snapshots, migration_through)
    load_migration.assert_not_called()


@pytest.mark.asyncio
async def test_fixture_historical_cleanup_does_not_require_registry():
    connection = SimpleNamespace(scalar=AsyncMock(return_value=None), scalars=AsyncMock(), execute=AsyncMock())
    await support._drop_snapshot_families(connection, "custom_import_publication_0123456789abcdef")
    connection.scalars.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_fixture_cleanup_rejects_non_registry_identity():
    connection = SimpleNamespace(
        scalar=AsyncMock(return_value=1), scalars=AsyncMock(return_value=[0]), execute=AsyncMock()
    )
    with pytest.raises(ValueError, match="positive bigint"):
        await support._drop_snapshot_families(connection, "custom_import_publication_0123456789abcdef")
    connection.execute.assert_not_awaited()


def _fake_fixture_engine(monkeypatch, connection):
    @asynccontextmanager
    async def transaction():
        yield connection

    engine = SimpleNamespace(begin=transaction, dispose=AsyncMock())
    engine.execution_options = lambda **_options: engine
    monkeypatch.setattr(support, "create_async_engine", lambda *_args, **_options: engine)
    monkeypatch.setattr(support, "async_sessionmaker", lambda *_args, **_options: object())
    monkeypatch.setattr(support, "_database_url", lambda: object())
    monkeypatch.setattr(support.uuid, "uuid4", lambda: SimpleNamespace(hex="0123456789abcdef"))
    return engine


@pytest.mark.asyncio
async def test_historical_append_fixture_restores_real_guard_before_yield(monkeypatch):
    from tests import test_custom_import_publication_postgres as native

    migration_stops = []
    connection = SimpleNamespace(run_sync=AsyncMock())
    engine = _fake_fixture_engine(monkeypatch, connection)
    case = SimpleNamespace(engine=engine, schema_name="custom_import_publication_0123456789abcdef")

    @asynccontextmanager
    async def historical_case(*, migration_through):
        migration_stops.append(migration_through)
        yield case

    monkeypatch.setattr(native, "isolated_publication_case", historical_case)
    async with native._original_append_case() as retained_case:
        assert retained_case is case
        connection.run_sync.assert_awaited_once_with(native._replace_append_plans, case.schema_name, downgrade=True)
    assert migration_stops == ["20261005030000"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", ("body", "migration", "cleanup"))
async def test_fixture_drops_recorded_leaves_before_control_and_always_disposes(monkeypatch, failure_at):
    connection = SimpleNamespace(
        scalar=AsyncMock(return_value=1),
        scalars=AsyncMock(return_value=[41, 73]),
        execute=AsyncMock(),
        run_sync=AsyncMock(),
    )
    engine = _fake_fixture_engine(monkeypatch, connection)
    if failure_at == "migration":
        connection.run_sync.side_effect = RuntimeError("migration")
    if failure_at == "cleanup":
        connection.scalars.side_effect = RuntimeError("cleanup")
    with pytest.raises(RuntimeError, match=failure_at):
        async with support.isolated_publication_case(migration_through="20261005030000"):
            if failure_at == "body":
                raise RuntimeError("body")
    engine.dispose.assert_awaited_once()
    statements = [str(call.args[0]) for call in connection.execute.await_args_list]
    control = '"custom_import_publication_0123456789abcdef"'
    assert statements[0] == f"CREATE SCHEMA {control}"
    if failure_at != "cleanup":
        assert statements[1:] == [
            'DROP SCHEMA IF EXISTS "ci_snapshot_41" CASCADE',
            'DROP SCHEMA IF EXISTS "ci_snapshot_73" CASCADE',
            f"DROP SCHEMA {control} CASCADE",
        ]
        assert (
            str(connection.scalars.await_args.args[0])
            == f"SELECT family_id FROM {control}.custom_import_snapshot_family"
        )
    else:
        assert len(statements) == 1
    assert connection.run_sync.await_args.args[-1] == "20261005030000"


@pytest.mark.asyncio
async def test_native_read_fixture_uses_complete_canonical_materialization():
    from db.models import custom_import as models
    from process.custom_import import runner_codec
    from tests import test_custom_import_snapshot_reads_postgres as native

    class ModelsOnlySession:
        def __init__(self):
            self.rows = []

        def add(self, row):
            self.rows.append(row)

        def add_all(self, rows):
            self.rows.extend(rows)

        async def flush(self):
            for ordinal, row in enumerate(self.rows, start=1):
                for column in row.__table__.primary_key.columns:
                    if getattr(row, column.name) is None:
                        setattr(row, column.name, ordinal)

    session = ModelsOnlySession()
    seed = SimpleNamespace(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_bundle_id=4,
        definition=native.fixture.definition(),
    )
    attempt = support.GenerationAttempt(
        execution_id=5, generation_id=6, fence=7, token=support.digest("synthetic-read").hex()
    )
    root, binding = await native._seed_material(session, seed, attempt)
    rows_by_model = lambda model: [row for row in session.rows if isinstance(row, model)]
    assert len(rows_by_model(models.CustomImportRootRevision)) == len(native._families()) == 8
    assert len(rows_by_model(models.CustomImportChildRevision)) == len(native.child_fixture._children()) == 11
    assert len(rows_by_model(models.CustomImportRootScalar)) == 40
    assert len(rows_by_model(models.CustomImportChildScalar)) == 33
    assert len(rows_by_model(models.CustomImportWinner)) == 11
    assert binding.canonical_value == native.native._A
    assert root.canonical_logical_key == runner_codec.root_key_document(seed.definition, native._families()[0].root)
    for revision, family in zip(rows_by_model(models.CustomImportRootRevision), native._families(), strict=True):
        assert (
            runner_codec.payload_values(seed.definition.root_fields, revision.canonical_payload, label="synthetic root")
            == family.root
        )
        assert revision.payload_sha256 == runner_codec.root_payload_hash(seed.definition, family)
