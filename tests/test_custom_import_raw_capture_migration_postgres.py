# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native migration, ACL and rollback proofs for original-object storage."""

from __future__ import annotations

import asyncio
import hashlib
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, insert, inspect, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import_raw_capture import MAX_RAW_OBJECT_BYTES, RAW_OBJECT_CONTRACT, CustomImportCaptureRawObject
from tests.custom_import_postgres_support import _migration, isolated_publication_case

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20261017010000_custom_import_raw_objects.py"
_TABLE = "custom_import_capture_raw_object"


def _run_raw_migration(connection, schema_name, operation):
    migration = _migration(_MIGRATION, "synthetic_raw_capture_native_migration")
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(connection))
    getattr(migration, operation)()


def _original_values(**changes):
    original = b"synthetic-original"
    return (
        dict(
            capture_bundle_id=1,
            stream_slot=1,
            dataset_id=1,
            definition_revision_id=1,
            schema_revision_id=1,
            payload_contract=RAW_OBJECT_CONTRACT,
            byte_count=len(original),
            payload=original,
            payload_sha256=hashlib.sha256(original).digest(),
        )
        | changes
    )


def _raw_catalog(connection, schema_name):
    inspector = inspect(connection)
    return dict(
        columns=inspector.get_columns(_TABLE, schema=schema_name),
        primary_key=inspector.get_pk_constraint(_TABLE, schema=schema_name),
        checks=inspector.get_check_constraints(_TABLE, schema=schema_name),
        indexes=inspector.get_indexes(_TABLE, schema=schema_name),
        foreign_keys=inspector.get_foreign_keys(_TABLE, schema=schema_name),
    )


async def _existing_catalog(connection, schema_name):
    table_names = tuple(
        await connection.scalars(
            text("SELECT tablename FROM pg_tables WHERE schemaname=:schema AND tablename<>:raw ORDER BY tablename"),
            {"schema": schema_name, "raw": _TABLE},
        )
    )
    function_ids = tuple(
        await connection.scalars(
            text(
                "SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema ORDER BY p.oid"
            ),
            {"schema": schema_name},
        )
    )
    parquet_guards = (
        await connection.execute(
            text("SELECT tgname,tgenabled,tgfoid FROM pg_trigger WHERE tgrelid=to_regclass(:relation) ORDER BY tgname"),
            {"relation": f"{schema_name}.custom_import_capture_parquet_part"},
        )
    ).all()
    return table_names, function_ids, parquet_guards


async def test_raw_upgrade_installs_native_constraints_without_changing_existing_objects():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case, case.engine.begin() as connection:
        await connection.run_sync(_run_raw_migration, case.schema_name, "downgrade")
        before = await _existing_catalog(connection, case.schema_name)
        await connection.run_sync(_run_raw_migration, case.schema_name, "upgrade")
        assert await _existing_catalog(connection, case.schema_name) == before
        catalog_by_kind = await connection.run_sync(_raw_catalog, case.schema_name)
        assert all(not column["nullable"] for column in catalog_by_kind["columns"])
        assert catalog_by_kind["primary_key"]["constrained_columns"] == ["capture_bundle_id", "stream_slot"]
        assert catalog_by_kind["foreign_keys"] == []
        assert [index["column_names"] for index in catalog_by_kind["indexes"]] == [["dataset_id"]]
        checks_by_name = {check["name"]: check["sqltext"] for check in catalog_by_kind["checks"]}
        assert len(checks_by_name) == 2
        assert str(MAX_RAW_OBJECT_BYTES) in checks_by_name["custom_import_capture_raw_object_payload_check"]
        assert "sha256(payload)" in checks_by_name["custom_import_capture_raw_object_payload_check"]
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass(:relation)"),
                {"relation": f"{case.schema_name}.{_TABLE}"},
            )
            == 0
        )


async def test_raw_native_checks_reject_small_corrupt_rows_and_accept_empty_originals():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case, case.engine.begin() as connection:
        await connection.execute(insert(CustomImportCaptureRawObject), _original_values())
        await connection.execute(
            insert(CustomImportCaptureRawObject),
            _original_values(stream_slot=2, byte_count=0, payload=b"", payload_sha256=hashlib.sha256(b"").digest()),
        )
        invalid_by_field = (
            {"dataset_id": 0},
            {"payload_contract": "synthetic-invalid-contract"},
            {"byte_count": -1},
            {"byte_count": MAX_RAW_OBJECT_BYTES + 1},
            {"byte_count": 1},
            {"payload": b"changed"},
            {"payload_sha256": hashlib.sha256(b"changed").digest()},
            {"payload_sha256": b"short"},
        )
        for changes_by_field in invalid_by_field:
            with pytest.raises(DBAPIError) as rejected:
                async with connection.begin_nested():
                    await connection.execute(
                        insert(CustomImportCaptureRawObject), _original_values(stream_slot=3, **changes_by_field)
                    )
            assert rejected.value.orig.sqlstate == "23514"
        with pytest.raises(DBAPIError) as duplicate:
            async with connection.begin_nested():
                await connection.execute(insert(CustomImportCaptureRawObject), _original_values())
        assert duplicate.value.orig.sqlstate == "23505"
        assert await connection.scalar(select(func.count()).select_from(CustomImportCaptureRawObject)) == 2


async def _assert_default_acl_probe(connection, schema_name, role):
    relation = f'"{schema_name}".synthetic_raw_acl_probe'
    await connection.execute(text(f"CREATE TABLE {relation}(id integer)"))
    grantees = set(
        await connection.scalars(
            text(
                "SELECT acl.grantee FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) acl "
                "WHERE c.oid=to_regclass(:relation) AND acl.privilege_type='INSERT'"
            ),
            {"relation": relation},
        )
    )
    role_id = await connection.scalar(text("SELECT oid FROM pg_roles WHERE rolname=:role"), {"role": role})
    assert 0 in grantees and role_id in grantees
    await connection.execute(text(f"DROP TABLE {relation}"))


@asynccontextmanager
async def _hostile_default_acl_case():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case:
        role = "synthetic_raw_acl_" + uuid.uuid4().hex[:16]
        is_role_created = False
        try:
            async with case.engine.begin() as connection:
                await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            is_role_created = True
            async with case.engine.begin() as connection:
                await connection.run_sync(_run_raw_migration, case.schema_name, "downgrade")
                await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{role}"'))
                await connection.execute(
                    text(
                        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{case.schema_name}" GRANT ALL ON TABLES TO PUBLIC,"{role}"'
                    )
                )
                await _assert_default_acl_probe(connection, case.schema_name, role)
                await connection.run_sync(_run_raw_migration, case.schema_name, "upgrade")
            yield case, role
        finally:
            if is_role_created:
                async with case.engine.begin() as connection:
                    await connection.execute(
                        text(
                            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{case.schema_name}" REVOKE ALL ON TABLES FROM PUBLIC,"{role}"'
                        )
                    )
                    await connection.execute(text(f'DROP OWNED BY "{role}"'))
                    await connection.execute(text(f'DROP ROLE "{role}"'))


async def test_raw_upgrade_revokes_inherited_acl_and_denies_actual_nonowner_writes():
    async with _hostile_default_acl_case() as (case, role), case.engine.begin() as connection:
        relation = f'"{case.schema_name}".{_TABLE}'
        assert await connection.scalar(
            text("SELECT has_schema_privilege(:role,:schema,'USAGE')"), {"role": role, "schema": case.schema_name}
        )
        assert not await connection.scalar(
            text(
                "SELECT has_table_privilege(:role,:relation,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')"
            ),
            {"role": role, "relation": relation},
        )
        assert (
            await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) acl "
                    "WHERE c.oid=to_regclass(:relation) AND acl.grantee<>c.relowner"
                ),
                {"relation": relation},
            )
            == 0
        )
        await connection.execute(text(f'SET LOCAL ROLE "{role}"'))
        for statement in (
            f"INSERT INTO {relation} DEFAULT VALUES",
            f"UPDATE {relation} SET byte_count=0",
            f"DELETE FROM {relation}",
            f"TRUNCATE {relation}",
        ):
            with pytest.raises(DBAPIError) as denied:
                async with connection.begin_nested():
                    await connection.execute(text(statement))
            assert denied.value.orig.sqlstate == "42501"
            assert await connection.scalar(text("SELECT current_user")) == role


async def test_raw_migrations_preserve_stricter_positive_lock_timeouts():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case, case.engine.begin() as connection:
        for timeout in ("1ms", "250ms"):
            await connection.execute(text("SELECT set_config('lock_timeout',:timeout,true)"), {"timeout": timeout})
            await connection.run_sync(_run_raw_migration, case.schema_name, "downgrade")
            assert await connection.scalar(text("SHOW lock_timeout")) == timeout
            await connection.run_sync(_run_raw_migration, case.schema_name, "upgrade")
            assert await connection.scalar(text("SHOW lock_timeout")) == timeout


async def test_raw_downgrade_refuses_actual_retained_originals():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case, case.engine.begin() as connection:
        await connection.execute(insert(CustomImportCaptureRawObject), _original_values())
        with pytest.raises(RuntimeError, match="custom_import_raw_objects_downgrade_blocked"):
            async with connection.begin_nested():
                await connection.run_sync(_run_raw_migration, case.schema_name, "downgrade")
        assert bytes(await connection.scalar(select(CustomImportCaptureRawObject.payload))) == b"synthetic-original"


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_raw_downgrade_refuses_non_read_committed_snapshots(isolation):
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case, case.engine.begin() as connection:
        await connection.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
        assert await connection.scalar(select(func.count()).select_from(CustomImportCaptureRawObject)) == 0
        with pytest.raises(RuntimeError, match="custom_import_raw_objects_downgrade_isolation"):
            await connection.run_sync(_run_raw_migration, case.schema_name, "downgrade")
        assert (
            await connection.scalar(text("SELECT to_regclass(:relation)"), {"relation": f"{case.schema_name}.{_TABLE}"})
            is not None
        )


async def _wait_for_raw_downgrade_lock(case):
    async with asyncio.timeout(1), case.engine.connect() as connection:
        while not await connection.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation=to_regclass(:relation) "
                "AND mode='AccessExclusiveLock' AND NOT granted)"
            ),
            {"relation": f"{case.schema_name}.{_TABLE}"},
        ):
            await asyncio.sleep(0.01)


async def _attempt_raw_downgrade(case):
    async with case.engine.begin() as connection:
        await connection.run_sync(_run_raw_migration, case.schema_name, "downgrade")


async def test_raw_downgrade_waits_for_pending_writer_and_retains_its_committed_original():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case, case.engine.connect() as writer:
        transaction = await writer.begin()
        downgrade_task = None
        try:
            await writer.execute(insert(CustomImportCaptureRawObject), _original_values())
            downgrade_task = asyncio.create_task(_attempt_raw_downgrade(case))
            await _wait_for_raw_downgrade_lock(case)
            await transaction.commit()
            with pytest.raises(RuntimeError, match="custom_import_raw_objects_downgrade_blocked"):
                await asyncio.wait_for(downgrade_task, 2)
            assert bytes(await writer.scalar(select(CustomImportCaptureRawObject.payload))) == b"synthetic-original"
        finally:
            if transaction.is_active:
                await transaction.rollback()
            if downgrade_task is not None:
                downgrade_task.cancel()
                await asyncio.gather(downgrade_task, return_exceptions=True)
