# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The authenticated snapshot endpoint keeps the real backend read-only and bounded."""

import json
from types import SimpleNamespace

import pytest
from sanic.exceptions import SanicException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api import provider_directory_profile_capacity_preflight as snapshot_api
from tests.test_provider_directory_capacity_reservation_snapshot_postgres import _fixture

_SETTINGS = "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),current_setting('lock_timeout'),current_setting('statement_timeout'),current_setting('temp_file_limit'),current_setting('max_parallel_workers_per_gather')"


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "backend_failure", "timeout"])
async def test_endpoint_native_limits_and_cleanup_on_all_exits(monkeypatch, outcome):
    observed_rows = []
    executed_sql_statements = []

    class SnapshotBackendSession(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            executed_sql_statements.append(str(statement))
            result = await super().execute(statement, *args, **kwargs)
            if "pg_current_snapshot()" in str(statement):
                observed_rows.append((await super().execute(text(_SETTINGS))).one())
                if outcome == "timeout":
                    await super().execute(text("SELECT pg_sleep(1)"))
            return result

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    async with _fixture(monkeypatch, SnapshotBackendSession) as (fhir, engine, _tables):
        monkeypatch.setattr(snapshot_api, "fhir", fhir)
        previous_setting_values = tuple(await fhir.db.first(_SETTINGS))
        executed_sql_statements.clear()
        if outcome == "backend_failure":
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f"DROP TABLE {fhir._unscoped_qt(fhir._schema(), 'provider_directory_profile_capacity_lease_consumption')}"
                    )
                )
        elif outcome == "timeout":
            monkeypatch.setattr(snapshot_api, "_RESERVATION_SNAPSHOT_TIMEOUT_SECONDS", 0.2)
        operation = snapshot_api.control_capacity_reservation_snapshot(
            SimpleNamespace(headers={"Authorization": "Bearer synthetic-token"})
        )
        if outcome == "success":
            payload_by_field = json.loads((await operation).body)
            assert payload_by_field["capacity_complete"] is False
            assert payload_by_field["reservations"] == payload_by_field["owners"] == []
        else:
            with pytest.raises(SanicException) as error:
                await operation
            assert error.value.status_code == 503
            assert str(error.value) == "capacity reservation snapshot unavailable"
        assert [tuple(setting_row) for setting_row in observed_rows] == [
            ("repeatable read", "on", "5s", "30s", "64MB", "0")
        ]
        assert all(statement.lstrip().upper().startswith(("SET", "SELECT")) for statement in executed_sql_statements)
        assert fhir.db._transaction_binding() is None
        assert tuple(await fhir.db.first(_SETTINGS)) == previous_setting_values
        assert (
            await fhir.db.scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND query LIKE 'SELECT pg_sleep(%'"
            )
            == 0
        )
