# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ACA control layout, operator grants and namespace retention boundaries."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from db.models.registry_aca_source_notification import RegistryACASourceNotification
from db.registry_aca_source_notification import CREATE_TABLE_SQL, INSERT_COLUMNS, TABLE
from process import registry_aca_source_notification_permissions as permissions
from process import registry_management_permissions as management
from process.registry_cms_source_notifications import _OPERATOR_PREFLIGHT_SQL

REVISION = "20261018040000_registry_aca_source_notifications"
FLAGS = ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions" / (REVISION + ".py")
    spec = importlib.util.spec_from_file_location(REVISION, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _owner(**changes):
    return (
        dict(
            oid=11,
            rolname="protected_owner",
            actor="operator",
            original_actor=True,
            inbox_oid=30,
            head_oid=31,
            schema_oid=32,
            can_assume_owner=True,
            schema_usage=True,
            **dict.fromkeys(FLAGS, False),
        )
        | changes
    )


class Bind:
    def __init__(self, owner=None, grantees=()):
        self.owner = _owner() if owner is None else owner
        self.grantees = grantees
        self.statements = []

    def execute(self, statement, parameters=None):
        statement = str(statement)
        self.statements.append((statement, parameters))
        value = self.owner if statement.startswith("SELECT r.*") else self.grantees
        return SimpleNamespace(
            mappings=lambda: SimpleNamespace(one_or_none=lambda: value),
            scalars=lambda: SimpleNamespace(all=lambda: list(value)),
        )


def test_migration_model_share_frozen_storage_without_row_hooks():
    migration = _migration()
    assert migration.down_revision == "20261018011000_network_candidate_custody"
    assert migration.CREATE_TABLE_SQL is CREATE_TABLE_SQL
    model = RegistryACASourceNotification
    assert model.__runtime_schema_sync__ is False
    assert tuple(model.__table__.columns.keys()) == (*INSERT_COLUMNS, "created_at")
    assert all(column.nullable is False for column in model.__table__.columns)
    assert [column.name for column in model.__table__.primary_key] == ["capture_id"]
    assert not model.__table__.foreign_keys
    assert TABLE not in management._TABLES
    assert all(token not in CREATE_TABLE_SQL.upper() for token in ("CREATE FUNCTION", "CREATE TRIGGER", "REFERENCES"))
    assert "source_schema_oid<>0" in CREATE_TABLE_SQL
    assert "source_schema='reference_family_archive_'" in CREATE_TABLE_SQL
    assert "sha256(convert_to(notification_json,'UTF8'))" in CREATE_TABLE_SQL
    with pytest.raises(RuntimeError, match="retained-data"):
        migration.downgrade()


def test_migration_uses_serving_owner_and_scrubs_default_table_and_column_grants(monkeypatch):
    migration, bind = _migration(), Bind(grantees=("old_default_reader",))
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "registry_control")
    monkeypatch.setattr(migration.op, "get_bind", lambda: bind)
    migration.upgrade()
    statements = [sql for sql, _ in bind.statements]
    assert "c.relname='network_serving_control'" in statements[0]
    assert 'ALTER TABLE "registry_control".' + TABLE + ' OWNER TO "protected_owner"' in statements
    assert any("REVOKE ALL ON TABLE" in sql and "old_default_reader" in sql for sql in statements)
    assert any("REVOKE ALL(" in sql and '"created_at"' in sql and "old_default_reader" in sql for sql in statements)
    assert not any(sql.startswith("GRANT") or "SET ROLE" in sql for sql in statements)


@pytest.mark.parametrize("flag", FLAGS)
def test_migration_bootstraps_without_runtime_grants_before_owner_enrollment(monkeypatch, flag):
    migration, bind = _migration(), Bind(owner=_owner(**{flag: True}))
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "registry_control")
    monkeypatch.setattr(migration.op, "get_bind", lambda: bind)
    migration.upgrade()
    statements = [sql for sql, _ in bind.statements]
    assert any(sql.startswith("CREATE TABLE") for sql in statements)
    assert not any(sql.startswith("GRANT") or "CREATE ROLE" in sql for sql in statements)
    assert 'ALTER TABLE "registry_control".' + TABLE + ' OWNER TO "protected_owner"' in statements


def test_migration_refuses_missing_native_serving_owner_before_ddl(monkeypatch):
    migration, bind = _migration(), Bind()
    bind.owner = None
    monkeypatch.setattr(migration.op, "get_bind", lambda: bind)
    with pytest.raises(RuntimeError, match="existing serving owner"):
        migration.upgrade()
    assert len(bind.statements) == 1


class Connection:
    def __init__(self, *, allowed=True, owner=None, active=True):
        self.allowed, self.owner, self.active = allowed, _owner() if owner is None else owner, active
        self.calls = []

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, sql, *arguments):
        self.calls.append((sql, arguments))
        return self.owner

    async def fetchval(self, sql, *arguments):
        self.calls.append((sql, arguments))
        return self.allowed

    async def execute(self, *_arguments):
        raise AssertionError("A grant plan must not execute authority SQL")


@pytest.mark.asyncio
async def test_operator_plan_preserves_exact_publisher_columns_and_reader_only_acl():
    connection = Connection()
    plan = await permissions.plan_registry_aca_source_notification_grants(
        connection,
        control_schema="registry_control",
        publisher_role="publisher",
        reader_roles=("reader",),
        runtime_roles=("publisher", "reader", "worker"),
    )
    assert permissions._PREFLIGHT_SQL == _OPERATOR_PREFLIGHT_SQL.replace("registry_cms_source_notification", TABLE)
    assert len(plan) == 6 and plan[0].startswith("LOCK TABLE")
    assert (
        plan[-1]
        == "GRANT INSERT ("
        + ",".join('"' + name + '"' for name in INSERT_COLUMNS)
        + ') ON "registry_control".'
        + TABLE
        + ' TO "publisher"'
    )
    assert '"created_at"' not in plan[-1]
    assert all("UPDATE" not in sql and "DELETE" not in sql and "TRUNCATE" not in sql for sql in plan)
    assert connection.calls[-1][1] == (
        ("publisher", "reader", "worker"),
        11,
        "registry_control",
        "publisher",
        ("reader",),
        INSERT_COLUMNS,
    )
    assert connection.calls[-2][1] == (
        "registry_control",
        "protected_owner",
        ("publisher", "reader"),
        "publisher",
        INSERT_COLUMNS,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    (
        {"rolcanlogin": True},
        {"rolsuper": True},
        {"original_actor": False},
        {"can_assume_owner": False},
        {"actor": "publisher"},
        {"rolname": "reader"},
    ),
)
async def test_operator_rejects_unsafe_owner_actor_and_authority_before_acl(changes):
    connection = Connection(owner=_owner(**changes))
    with pytest.raises(ValueError):
        await permissions.plan_registry_aca_source_notification_grants(
            connection,
            control_schema="registry_control",
            publisher_role="publisher",
            reader_roles=("reader",),
            runtime_roles=("publisher", "reader", "worker"),
        )
    assert len(connection.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("reader_roles", ((), ["reader"], ("reader", "reader"), ("z", "a"), ("publisher",)))
async def test_operator_rejects_unclosed_role_configuration(reader_roles):
    connection = Connection()
    with pytest.raises(ValueError):
        await permissions.plan_registry_aca_source_notification_grants(
            connection,
            control_schema="registry_control",
            publisher_role="publisher",
            reader_roles=reader_roles,
            runtime_roles=("publisher", "reader", "worker"),
        )
    assert not connection.calls


@pytest.mark.asyncio
async def test_operator_rejects_native_acl_bypass_without_any_mutation():
    connection = Connection(allowed=False)
    with pytest.raises(ValueError):
        await permissions.plan_registry_aca_source_notification_grants(
            connection,
            control_schema="registry_control",
            publisher_role="publisher",
            reader_roles=("reader",),
            runtime_roles=("publisher", "reader", "worker"),
        )
    assert len(connection.calls) == 2
    assert "att.attacl" in connection.calls[-1][0] and "pg_has_role" in connection.calls[-1][0]


def _control(**changes):
    return (
        dict(
            oid=30,
            relname=TABLE,
            relowner=11,
            head_owner=11,
            relkind="r",
            relpersistence="p",
            columns=(*INSERT_COLUMNS, "created_at"),
            relhastriggers=False,
            relhasrules=False,
            relrowsecurity=False,
            relforcerowsecurity=False,
            **dict.fromkeys(FLAGS, False),
        )
        | changes
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    ({"relowner": 99}, {"relkind": "v"}, {"columns": INSERT_COLUMNS}, {"relhastriggers": True}),
)
async def test_protected_control_catalog_refuses_owner_layout_and_hook_drift(changes):
    connection = Connection(owner=_control(**changes))
    with pytest.raises(management.RegistryManagementPermissionError):
        await management._aca_controls(connection, "registry_control")


@pytest.mark.asyncio
async def test_control_inventory_keeps_exact_staging_head_owner_for_operator_transfer():
    staged = _control(rolcanlogin=True)
    assert await management._aca_controls(Connection(owner=staged), "registry_control") == [staged]


@pytest.mark.asyncio
async def test_namespace_move_keeps_accepted_table_identity_without_draft_views(monkeypatch):
    statements = []
    connection = SimpleNamespace()

    async def execute(sql):
        statements.append(sql)

    async def fetchval(sql, *_arguments):
        return None if "to_regclass" in sql else 20

    async def catalog(*_arguments):
        return [], []

    connection.execute, connection.fetchval = execute, fetchval
    monkeypatch.setattr(management, "_catalog", catalog)
    await management._move_registry_objects(
        connection, "old_control", "new_control", "protected_owner", [], [], [_control()]
    )
    assert statements == [
        'CREATE SCHEMA "new_control" AUTHORIZATION "protected_owner"',
        'ALTER TABLE "old_control"."' + TABLE + '" SET SCHEMA "new_control"',
    ]
    assert all("DROP" not in sql and "GRANT" not in sql and "CREATE VIEW" not in sql for sql in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "runtime_roles",
    ([], (), ("publisher",), ("publisher", "reader", "reader"), (True,), tuple(f"r{i:03}" for i in range(129))),
)
async def test_operator_requires_complete_bounded_typed_runtime_census(runtime_roles):
    connection = Connection()
    with pytest.raises(ValueError):
        await permissions.plan_registry_aca_source_notification_grants(
            connection,
            control_schema="registry_control",
            publisher_role="publisher",
            reader_roles=("reader",),
            runtime_roles=runtime_roles,
        )
    assert not connection.calls


@pytest.mark.asyncio
async def test_native_protected_owner_exclusion_occurs_after_owner_identity_proof():
    connection = Connection()
    await permissions.plan_registry_aca_source_notification_grants(
        connection,
        control_schema="registry_control",
        publisher_role="publisher",
        reader_roles=("reader",),
        runtime_roles=("protected_owner", "publisher", "reader", "worker"),
    )
    assert connection.calls[-1][1][0] == ("publisher", "reader", "worker")
    assert "pg_has_role" in permissions._RUNTIME_SQL
    assert all(privilege in permissions._RUNTIME_SQL for privilege in ("'MEMBER'", "'USAGE'", "'SET'"))
    assert "session_replication_role" in permissions._RUNTIME_SQL


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "grant", "postcheck", "cancel"))
async def test_installer_keeps_grants_atomic_under_caller_transaction(failure):
    import asyncio
    from contextlib import asynccontextmanager

    class Installer(Connection):
        def __init__(self):
            super().__init__()
            self.statements = []
            self.rolled_back = False

        @asynccontextmanager
        async def transaction(self):
            previous_statements = list(self.statements)
            try:
                yield
            except BaseException:
                self.statements = previous_statements
                self.rolled_back = True
                raise

        async def execute(self, statement):
            if statement.startswith("GRANT INSERT"):
                if failure == "grant":
                    raise RuntimeError("grant rejected")
                if failure == "cancel":
                    raise asyncio.CancelledError
            self.statements.append(statement)

        async def is_permitted(self, sql, *arguments):
            self.calls.append((sql, arguments))
            return not (failure == "postcheck" and sql == permissions._CLOSED_SQL)

        fetchval = is_permitted

    connection = Installer()
    arguments_by_name = dict(
        control_schema="registry_control",
        publisher_role="publisher",
        reader_roles=("reader",),
        runtime_roles=("publisher", "reader", "worker"),
    )
    if failure:
        exception = (
            asyncio.CancelledError if failure == "cancel" else (RuntimeError if failure == "grant" else ValueError)
        )
        with pytest.raises(exception):
            await permissions.install_registry_aca_source_notification_permissions(connection, **arguments_by_name)
        assert connection.rolled_back and connection.statements == []
    else:
        receipt = await permissions.install_registry_aca_source_notification_permissions(
            connection, **arguments_by_name
        )
        assert receipt["table_oid"] == 30 and receipt["runtime_role_count"] == 3
        assert len(connection.statements) == 6
        assert connection.statements[0].startswith("LOCK TABLE")
        assert connection.calls[-2][0] == permissions._CLOSED_SQL
        assert connection.calls[-1][0] == permissions._RUNTIME_SQL


def _operator_cli_arguments():
    return [
        "--control-schema",
        "registry_control",
        "--publisher-role",
        "publisher",
        "--reader-role",
        "reader",
        "--runtime-role",
        "worker",
        "--runtime-role",
        "reader",
        "--runtime-role",
        "publisher",
    ]


def test_operator_cli_passes_explicit_census_inside_existing_config_transaction(monkeypatch, capsys):
    """Use the original driver and complete role census in one operator transaction."""
    from contextlib import asynccontextmanager, nullcontext

    import db.connection
    import process.custom_import.cli
    import process.registry_source_import

    events = []

    class Driver:
        @asynccontextmanager
        async def transaction(self, **options):
            assert options == {"isolation": "repeatable_read"}
            events.append("transaction")
            yield

    driver = Driver()

    class Database:
        async def connect(self):
            events.append("connect")

        @asynccontextmanager
        async def acquire_driver(self):
            yield driver

    async def install(connection, **arguments):
        assert connection is driver
        assert events == ["connect", "transaction"]
        assert arguments == dict(
            control_schema="registry_control",
            publisher_role="publisher",
            reader_roles=("reader",),
            runtime_roles=("publisher", "reader", "worker"),
        )
        return {"component": "registry_aca_source_notification_permissions", "revision": 1}

    async def disconnect(_database):
        events.append("disconnect")

    monkeypatch.setattr(db.connection, "Database", Database)
    monkeypatch.setattr(process.custom_import.cli, "_receipt_only_database_output", lambda _database: nullcontext())
    monkeypatch.setattr(process.custom_import.cli, "_set_engine_echo", lambda *_args: None)
    monkeypatch.setattr(process.registry_source_import, "_disconnect_database", disconnect)
    monkeypatch.setattr(permissions, "install_registry_aca_source_notification_permissions", install)
    assert permissions.run_command(_operator_cli_arguments()) == 0
    assert events[-1] == "disconnect"
    assert '"status":"ok"' in capsys.readouterr().out


@pytest.mark.asyncio
@pytest.mark.parametrize("bypass", ("publisher_reach", "owner_reach"))
async def test_operator_denies_runtime_role_graph_bypass_before_any_grant(bypass):
    class UnsafeRuntime(Connection):
        async def is_permitted(self, sql, *arguments):
            self.calls.append((sql, arguments))
            if sql == permissions._RUNTIME_SQL:
                assert (
                    "pg_has_role(r.oid,s.owner_oid,'SET')"
                    if bypass == "owner_reach"
                    else "(SELECT oid FROM pg_roles WHERE rolname=$4)"
                ) in sql
                return False
            return True

        fetchval = is_permitted

    connection = UnsafeRuntime()
    with pytest.raises(ValueError):
        await permissions.plan_registry_aca_source_notification_grants(
            connection,
            control_schema="registry_control",
            publisher_role="publisher",
            reader_roles=("reader",),
            runtime_roles=("publisher", "reader", "worker"),
        )
    assert len(connection.calls) == 3 and connection.calls[-1][0] == permissions._RUNTIME_SQL


@pytest.mark.asyncio
async def test_namespace_move_excludes_native_owner_from_single_publisher_acl_count():
    calls = []

    async def fetch(statement, *arguments):
        calls.append((statement, arguments))
        return [{"grantee": 12, "rolname": "publisher", "safe": True}]

    rows = await management._aca_readers(SimpleNamespace(fetch=fetch), "registry_control", [_control()])
    assert rows == [{"grantee": 12, "rolname": "publisher", "safe": True}]
    assert "privilege_type='INSERT' AND grantee<>s.owner_oid" in calls[0][0]
    assert calls[0][1][:3] == (30, 11, "registry_control")
