# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native optional savepoint recovery preserves named reads and their pinned data."""

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from api.network_read_execution import qualified_relation_sql
from api.provider_profile_snapshot import provider_optional_read, provider_read_savepoint
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
            _case, _admin, session, _table, _parent, _child = held
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            version = int(await driver.fetchval("SHOW server_version_num"))
            assert 180000 <= version < 190000
            yield held, driver, version


def _write_evidence(case, evidence_by_field):
    if prefix := os.getenv("NETWORK_SQL_RECOVERY_EVIDENCE"):
        Path(prefix + "-" + case + ".json").write_text(json.dumps(evidence_by_field, indent=2) + "\n")


@pytest.mark.asyncio
async def test_native_missing_relation_preserves_mandatory_error(monkeypatch):
    evidence_by_field = {"mechanism": "mandatory-error-after-relation-drop"}
    try:
        async with _native_case(monkeypatch) as (held, _driver, version):
            case, admin, session, _table, parent, _child = held
            relation = "ONLY " + qualified_relation_sql(case.schema, "direct_heap_probe")
            await admin.execute("SET lock_timeout='250ms'")
            await admin.execute("SET statement_timeout='2s'")
            await admin.execute(f"DROP TABLE {parent}")
            assert await admin.fetchval("SELECT pg_catalog.to_regclass($1) IS NULL", parent)
            with pytest.raises(DBAPIError) as caught:
                async with provider_read_savepoint(session):
                    await session.execute(text(f"SELECT p.id FROM {relation} p"))
            assert postgres_sqlstate(caught.value) == "42P01"
            assert session.get_transaction().is_active
            assert (await session.execute(text("SELECT 1"))).scalar_one() == 1
            evidence_by_field.update(server_version_num=version, original_sqlstate="42P01", later_transaction_read=1)
        evidence_by_field["exact_fixture_and_database_cleanup"] = True
    finally:
        _write_evidence("drop", evidence_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["statement", "timeout"])
async def test_native_optional_rollback_preserves_pinned_rows(monkeypatch, failure):
    evidence_by_field = {"mechanism": "savepoint-before-fallback", "failure": failure}
    try:
        async with _native_case(monkeypatch) as (held, driver, version):
            case, admin, session, _table, parent, _child = held
            relation = "ONLY " + qualified_relation_sql(case.schema, "direct_heap_probe")
            assert (await session.execute(text(f"SELECT id FROM {relation} ORDER BY id"))).scalars().all() == [1]
            await admin.execute(f"INSERT INTO {parent}(id,private_payload) VALUES(2,'later')")
            assert await admin.fetchval(f"SELECT count(*) FROM {parent}") == 2
            if failure == "timeout":
                await session.execute(text("SET LOCAL statement_timeout='100ms'"))
                query = f"SELECT p.id FROM {relation} p CROSS JOIN pg_catalog.pg_sleep(1)"
            else:
                query = f"SELECT p.id/0 FROM {relation} p"
            failures = []
            logger = SimpleNamespace(debug=lambda _message, error: failures.append(error))
            async with provider_optional_read(session, logger):
                await session.execute(text(query))
            assert len(failures) == 1 and isinstance(failures[0], DBAPIError)
            expected_state = "57014" if failure == "timeout" else "22012"
            assert postgres_sqlstate(failures[0]) == expected_state
            assert session.get_transaction().is_active
            assert (await session.execute(text(f"SELECT p.id FROM {relation} p ORDER BY p.id"))).scalars().all() == [1]
            evidence_by_field.update(
                server_version_num=version,
                original_sqlstate=expected_state,
                optional_fallback_after_savepoint=True,
                later_pinned_rows=[1],
                concurrent_committed_rows=2,
            )
        evidence_by_field["exact_fixture_and_database_cleanup"] = True
    finally:
        _write_evidence(failure, evidence_by_field)
