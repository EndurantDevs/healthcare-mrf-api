# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine column-Reader failure/recovery mechanisms, separate from Source authority."""

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from api.network_read_execution import direct_heap_relation_sql, execute_network_read, install_network_read_inventory
from api.provider_profile_snapshot import provider_optional_read
from process.entity_address_cutover_contract import postgres_sqlstate
from tests.cms_npd_admission_postgres_support import _owned_database
from tests.test_network_direct_heap_reads_postgres import _column_reader_case


@asynccontextmanager
async def _native_case(monkeypatch):
    dsn = os.getenv("NETWORK_SQL_RECOVERY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_SQL_RECOVERY_TEST_DSN must select an authorized local creation database")
    async with _owned_database(make_url(dsn)) as (url, _database_admin):
        for field, value in {
            "HOST": url.host or url.query.get("host", "127.0.0.1"),
            "PORT": str(url.port or url.query.get("port", 5432)),
            "USER": url.username,
            "PASSWORD": url.password or "",
            "DATABASE": url.database,
        }.items():
            monkeypatch.setenv("HLTHPRT_DB_" + field, value)
        async with _column_reader_case(monkeypatch) as held:
            case, _admin, session, _table, _parent, _child, oid = held
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            version = int(await driver.fetchval("SHOW server_version_num"))
            assert 180000 <= version < 190000
            assert await driver.fetchval("SELECT current_user=session_user AND current_user::text=$1", case.reader)
            assert await driver.fetchval("SELECT NOT has_table_privilege(current_user,$1::oid,'SELECT')", oid)
            assert await driver.fetchval("SELECT has_column_privilege(current_user,$1::oid,'id','SELECT')", oid)
            assert not await driver.fetchval(
                "SELECT EXISTS(SELECT FROM pg_catalog.pg_locks WHERE pid=pg_backend_pid() AND relation=$1::oid AND granted)",
                oid,
            )
            await install_network_read_inventory(session, (), absent_names=((case.schema, "missing_optional_probe"),))
            yield held, driver, version


def _write_evidence(case, evidence_by_field):
    if prefix := os.getenv("NETWORK_SQL_RECOVERY_EVIDENCE"):
        Path(prefix + "-" + case + ".json").write_text(json.dumps(evidence_by_field, indent=2) + "\n")


@pytest.mark.asyncio
async def test_native_drop_after_metadata_before_first_payload_is_private_503(monkeypatch):
    evidence_by_field = {"source_authority": False, "mechanism": "drop-before-first-payload"}
    try:
        with pytest.raises(ServiceUnavailable) as caught:
            async with _native_case(monkeypatch) as (held, driver, version):
                case, admin, session, _table, parent, _child, oid = held
                evidence_by_field.update(
                    server_version_num=version, reader_pid=driver.get_server_pid(), original_oid=oid
                )
                relation = direct_heap_relation_sql(session, case.schema, "direct_heap_probe")
                await admin.execute("SET lock_timeout='250ms'")
                await admin.execute("SET statement_timeout='2s'")
                await admin.execute(f"DROP TABLE {parent}")
                assert await admin.fetchval("SELECT pg_catalog.to_regclass($1)::oid", parent) is None
                evidence_by_field["exact_parent_removed_without_payload_lock"] = True
                await execute_network_read(session, f"SELECT p.id FROM {relation} p")
        assert caught.value.headers["Cache-Control"] == "private, no-store"
        assert isinstance(caught.value.__cause__, DBAPIError)
        assert postgres_sqlstate(caught.value.__cause__) == "42P01"
        evidence_by_field.update(
            private_no_store_503=True, original_sqlstate="42P01", exact_fixture_and_database_cleanup=True
        )
    finally:
        _write_evidence("drop", evidence_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["statement", "timeout"])
async def test_native_owned_rollback_revalidates_before_optional_fallback_and_later_read(monkeypatch, failure):
    evidence_by_field = {"source_authority": False, "mechanism": "owned-rollback-before-fallback", "failure": failure}
    try:
        async with _native_case(monkeypatch) as (held, driver, version):
            case, _admin, session, _table, _parent, _child, _oid = held
            original_transaction, pid = session.get_transaction(), driver.get_server_pid()
            snapshot = await driver.fetchval("SELECT pg_catalog.pg_current_snapshot()::text")
            relation = direct_heap_relation_sql(session, case.schema, "direct_heap_probe")
            if failure == "timeout":
                await session.execute(text("SET LOCAL statement_timeout='100ms'"))
                query = f"SELECT p.id FROM {relation} p CROSS JOIN pg_catalog.pg_sleep(1)"
            else:
                query = f"SELECT p.id/0 FROM {relation} p"
            failures = []
            logger = SimpleNamespace(debug=lambda _message, error: failures.append(error))
            async with provider_optional_read(session, logger):
                await execute_network_read(session, query)
            assert len(failures) == 1 and isinstance(failures[0], DBAPIError)
            expected_state = "57014" if failure == "timeout" else "22012"
            assert postgres_sqlstate(failures[0]) == expected_state
            assert session.get_transaction() is original_transaction and original_transaction.is_active
            actual_driver = (await (await session.connection()).get_raw_connection()).driver_connection
            assert actual_driver is driver and driver.get_server_pid() == pid and driver.is_in_transaction()
            assert await driver.fetchval("SELECT pg_catalog.pg_current_snapshot()::text") == snapshot
            assert (await execute_network_read(session, f"SELECT p.id FROM {relation} p")).scalars().all() == [1]
            evidence_by_field.update(
                server_version_num=version,
                reader_pid=pid,
                original_sqlstate=expected_state,
                optional_fallback_after_revalidation=True,
                original_root_driver_pid_and_snapshot=True,
                later_guarded_rows=[1],
            )
        evidence_by_field["exact_fixture_and_database_cleanup"] = True
    finally:
        _write_evidence(failure, evidence_by_field)
