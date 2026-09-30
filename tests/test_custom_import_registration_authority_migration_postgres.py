# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execute registration-authority retention guards on disposable PostgreSQL."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.custom_import_postgres_support import (
    _migration,
    digest,
    isolated_publication_case,
)

_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260930030000_custom_import_registration_authority.py"
_TABLE = "custom_import_registration_authority"
_GUARD = "guard_custom_import_registration_authority"


def _apply(connection, migration, direction):
    """Run the real version through Alembic operations in the caller transaction."""

    migration.op = Operations(MigrationContext.configure(connection))
    getattr(migration, direction)()


@asynccontextmanager
async def _authority_case(monkeypatch):
    """Install the version in the existing fixture's exact disposable schema."""

    async with isolated_publication_case() as case:
        migration = _migration(_PATH, "registration_authority_postgres_migration")
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", case.schema_name)
        monkeypatch.delenv("DB_SCHEMA", raising=False)
        async with case.engine.begin() as connection:
            await connection.run_sync(_apply, migration, "upgrade")
        yield case, migration


def _table(case):
    return f'"{case.schema_name}"."{_TABLE}"'


async def _insert(connection, table, **override_by_field):
    """Insert synthetic pins or an absent-first tombstone through the real DDL."""

    value_by_field = {
        "authority_id": "synthetic_authority",
        "input_sha256": digest("synthetic registration input"),
        "token_sha256": digest("synthetic capability digest"),
        "expires_at": dt.datetime.now(dt.UTC) + dt.timedelta(hours=1),
        "revoked_at": None,
        "result_receipt": None,
    }
    value_by_field.update(override_by_field)
    return (
        (
            await connection.execute(
                text(
                    f"INSERT INTO {table} ({', '.join(value_by_field)}) "
                    f"VALUES ({', '.join(':' + field for field in value_by_field)}) RETURNING *"
                ),
                value_by_field,
            )
        )
        .mappings()
        .one()
    )


async def _row(case):
    async with case.engine.connect() as connection:
        return dict((await connection.execute(text(f"SELECT * FROM {_table(case)}"))).mappings().one())


async def _denied(case, statement, message, *, replication_role="origin"):
    """Require a guard error and a rollback rather than silently accepting DML."""

    with pytest.raises(DBAPIError, match=message) as caught:
        async with case.engine.begin() as connection:
            await connection.execute(text(f"SET LOCAL session_replication_role = '{replication_role}'"))
            await connection.execute(text(statement))
    assert caught.value.orig.sqlstate == "P0001"


@pytest.mark.asyncio
async def test_migration_creates_empty_relation_with_always_guards(monkeypatch):
    async with _authority_case(monkeypatch) as (case, _):
        table = _table(case)
        async with case.engine.begin() as connection:
            assert await connection.scalar(text(f"SELECT count(*) FROM {table}")) == 0
            triggers = (
                await connection.execute(
                    text("SELECT tgname, tgenabled::text FROM pg_trigger WHERE tgrelid = to_regclass(:table)"),
                    {"table": table},
                )
            ).all()
            assert dict(triggers) == {
                "custom_import_reg_authority_row_guard": "A",
                "custom_import_reg_authority_truncate_guard": "A",
            }
            constraints = (
                await connection.execute(
                    text(
                        "SELECT conname, contype::text FROM pg_constraint "
                        "WHERE conrelid = to_regclass(:table) AND contype <> 'n'"
                    ),
                    {"table": table},
                )
            ).all()
            assert dict(constraints) == {
                "custom_import_reg_authority_pkey": "p",
                "custom_import_reg_authority_id_check": "c",
                "custom_import_reg_authority_pins_check": "c",
                "custom_import_reg_authority_result_check": "c",
            }
            before = await connection.scalar(text("SELECT clock_timestamp()"))
            minted_row = await _insert(connection, table)
            after = await connection.scalar(text("SELECT clock_timestamp()"))
            assert before <= minted_row["created_at"] <= after
            assert minted_row["revoked_at"] is None and minted_row["result_receipt"] is None


@pytest.mark.asyncio
async def test_migration_preserves_required_identity_and_database_timestamp(monkeypatch):
    async with _authority_case(monkeypatch) as (case, _):
        async with case.engine.connect() as connection:
            columns = (
                await connection.execute(
                    text(
                        "SELECT attname, attnotnull FROM pg_attribute "
                        "WHERE attrelid = to_regclass(:table) AND attnum > 0 AND NOT attisdropped"
                    ),
                    {"table": _table(case)},
                )
            ).all()
            assert dict(columns) == {
                "authority_id": True,
                "created_at": True,
                "input_sha256": False,
                "token_sha256": False,
                "expires_at": False,
                "revoked_at": False,
                "result_receipt": False,
            }


@pytest.mark.asyncio
async def test_migration_revokes_public_table_and_function_privileges(monkeypatch):
    """Check the version's PUBLIC ACL; this is not deployed-role denial proof."""

    async with _authority_case(monkeypatch) as (case, _):
        async with case.engine.connect() as connection:
            table_grants = await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_class, "
                    "LATERAL aclexplode(coalesce(relacl, acldefault('r', relowner))) AS privilege "
                    "WHERE oid = to_regclass(:table) AND privilege.grantee = 0"
                ),
                {"table": _table(case)},
            )
            function = (
                (
                    await connection.execute(
                        text(
                            "SELECT proconfig, prosecdef, "
                            "(SELECT count(*) FROM aclexplode(coalesce(proacl, acldefault('f', proowner))) "
                            "AS privilege WHERE privilege.grantee = 0) AS public_grants "
                            "FROM pg_proc WHERE oid = to_regprocedure(:function)"
                        ),
                        {"function": f'"{case.schema_name}"."{_GUARD}"()'},
                    )
                )
                .mappings()
                .one()
            )
            assert table_grants == 0 and function["public_grants"] == 0
            assert function["proconfig"] == ["search_path=pg_catalog"]
            assert function["prosecdef"] is False


@pytest.mark.parametrize(
    ("overrides", "constraint", "sqlstate"),
    [
        ({"authority_id": None}, "authority_id", "23502"),
        ({"authority_id": ""}, "id_check", "23514"),
        ({"authority_id": "-synthetic"}, "id_check", "23514"),
        ({"authority_id": "synthetic authority"}, "id_check", "23514"),
        ({"authority_id": "é"}, "id_check", "23514"),
        ({"authority_id": "a" * 129}, "authority_id", "22001"),
        ({"input_sha256": b"x" * 31}, "pins_check", "23514"),
        ({"token_sha256": b"x" * 33}, "pins_check", "23514"),
        ({"input_sha256": None}, "pins_check", "23514"),
        ({"token_sha256": None}, "pins_check", "23514"),
        ({"expires_at": None}, "pins_check", "23514"),
        ({"input_sha256": None, "token_sha256": None, "expires_at": None}, "pins_check", "23514"),
        ({"result_receipt": "x"}, "result_check", "23514"),
        ({"result_receipt": "x" * 4097}, "result_check", "23514"),
        ({"result_receipt": "é" * 2049}, "result_check", "23514"),
        (
            {
                "input_sha256": None,
                "token_sha256": None,
                "expires_at": None,
                "revoked_at": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                "result_receipt": "{}",
            },
            "pins_check",
            "23514",
        ),
    ],
)
@pytest.mark.asyncio
async def test_migration_rejects_invalid_authority_rows(monkeypatch, overrides, constraint, sqlstate):
    async with _authority_case(monkeypatch) as (case, _):
        with pytest.raises(DBAPIError) as caught:
            async with case.engine.begin() as connection:
                await _insert(connection, _table(case), **overrides)
        assert caught.value.orig.sqlstate == sqlstate
        if sqlstate != "22001":
            assert constraint in str(caught.value)
        async with case.engine.connect() as connection:
            assert await connection.scalar(text(f"SELECT count(*) FROM {_table(case)}")) == 0


@pytest.mark.asyncio
async def test_migration_accepts_tombstone_and_byte_bounded_retained_result(monkeypatch):
    async with _authority_case(monkeypatch) as (case, _):
        async with case.engine.begin() as connection:
            await _insert(
                connection,
                _table(case),
                authority_id="synthetic:tombstone-1",
                input_sha256=None,
                token_sha256=None,
                expires_at=None,
                revoked_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
            )
            row = await _insert(
                connection,
                _table(case),
                authority_id="a" * 128,
                result_receipt="é" * 2048,
                revoked_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
            )
            assert len(row["result_receipt"].encode("utf-8")) == 4096
        with pytest.raises(DBAPIError, match="custom_import_reg_authority_pkey") as caught:
            async with case.engine.begin() as connection:
                await _insert(connection, _table(case), authority_id="a" * 128)
        assert caught.value.orig.sqlstate == "23505"


@pytest.mark.parametrize(
    "assignment",
    [
        "authority_id = 'synthetic_changed'",
        "input_sha256 = decode(repeat('ab', 32), 'hex')",
        "token_sha256 = decode(repeat('ab', 32), 'hex')",
        "expires_at = expires_at + interval '1 second'",
        "created_at = created_at + interval '1 second'",
        "input_sha256 = NULL",
        "token_sha256 = NULL",
        "expires_at = NULL",
    ],
)
@pytest.mark.asyncio
async def test_migration_forbids_pin_changes_and_removal(monkeypatch, assignment):
    async with _authority_case(monkeypatch) as (case, _):
        async with case.engine.begin() as connection:
            await _insert(connection, _table(case))
        original = await _row(case)
        await _denied(case, f"UPDATE {_table(case)} SET {assignment}", "authority_immutable")
        assert await _row(case) == original


@pytest.mark.asyncio
async def test_absent_first_tombstone_cannot_be_minted_later(monkeypatch):
    async with _authority_case(monkeypatch) as (case, _):
        async with case.engine.begin() as connection:
            await _insert(
                connection,
                _table(case),
                input_sha256=None,
                token_sha256=None,
                expires_at=None,
                revoked_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
            )
        original = await _row(case)
        await _denied(
            case,
            f"UPDATE {_table(case)} SET input_sha256 = decode(repeat('ab', 32), 'hex'), "
            "token_sha256 = decode(repeat('cd', 32), 'hex'), "
            "expires_at = clock_timestamp() + interval '1 hour', revoked_at = NULL",
            "authority_immutable",
        )
        assert await _row(case) == original


@pytest.mark.parametrize("result_first", [False, True])
@pytest.mark.asyncio
async def test_revoke_and_result_are_once_only_and_history_coexists(monkeypatch, result_first):
    async with _authority_case(monkeypatch) as (case, _):
        table = _table(case)
        changes = ["revoked_at = clock_timestamp()", "result_receipt = '{}'"]
        if result_first:
            changes.reverse()
        async with case.engine.begin() as connection:
            await _insert(connection, table)
            for assignment in changes:
                await connection.execute(text(f"UPDATE {table} SET {assignment}"))
            await connection.execute(
                text(f"UPDATE {table} SET revoked_at = revoked_at, result_receipt = result_receipt")
            )
        retained = await _row(case)
        for assignment in (
            "revoked_at = NULL",
            "revoked_at = revoked_at + interval '1 second'",
            "result_receipt = NULL",
            "result_receipt = '[]'",
        ):
            await _denied(case, f"UPDATE {table} SET {assignment}", "authority_immutable")
            assert await _row(case) == retained


@pytest.mark.parametrize("replication_role", ["origin", "replica"])
@pytest.mark.parametrize("operation", ["DELETE FROM", "TRUNCATE", "UPDATE"])
@pytest.mark.asyncio
async def test_always_guards_reject_destructive_dml_in_replica_mode(monkeypatch, replication_role, operation):
    async with _authority_case(monkeypatch) as (case, _):
        async with case.engine.begin() as connection:
            await _insert(connection, _table(case))
        original = await _row(case)
        statement = f"{operation} {_table(case)}"
        message = "authority_retained"
        if operation == "UPDATE":
            statement += " SET expires_at = expires_at + interval '1 second'"
            message = "authority_immutable"
        await _denied(case, statement, message, replication_role=replication_role)
        assert await _row(case) == original


@pytest.mark.parametrize("kind", ["mint", "tombstone", "result"])
@pytest.mark.asyncio
async def test_downgrade_rejects_every_retained_authority_kind(monkeypatch, kind):
    async with _authority_case(monkeypatch) as (case, migration):
        override_by_field = {}
        if kind == "tombstone":
            override_by_field = {
                "input_sha256": None,
                "token_sha256": None,
                "expires_at": None,
                "revoked_at": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
            }
        elif kind == "result":
            override_by_field = {"result_receipt": "{}"}
        async with case.engine.begin() as connection:
            await _insert(connection, _table(case), **override_by_field)
        original = await _row(case)
        with pytest.raises(DBAPIError, match="authority_downgrade_blocked") as caught:
            async with case.engine.begin() as connection:
                await connection.run_sync(_apply, migration, "downgrade")
        assert caught.value.orig.sqlstate == "P0001"
        assert await _row(case) == original
        await _denied(case, f"DELETE FROM {_table(case)}", "authority_retained")


@pytest.mark.asyncio
async def test_unused_downgrade_removes_relation_and_guard_and_can_reupgrade(monkeypatch):
    async with _authority_case(monkeypatch) as (case, migration):
        async with case.engine.begin() as connection:
            await connection.run_sync(_apply, migration, "downgrade")
            assert await connection.scalar(text("SELECT to_regclass(:table)"), {"table": _table(case)}) is None
            assert (
                await connection.scalar(
                    text("SELECT to_regprocedure(:function)"),
                    {"function": f'"{case.schema_name}"."{_GUARD}"()'},
                )
                is None
            )
            await connection.run_sync(_apply, migration, "upgrade")
            await _insert(connection, _table(case))
        await _denied(case, f"TRUNCATE {_table(case)}", "authority_retained")
