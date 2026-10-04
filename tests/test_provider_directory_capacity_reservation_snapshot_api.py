# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authentication, exact reservation observations, and bounded control failures."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Sanic
from sanic.exceptions import Forbidden, SanicException

import api as api_package
from api import control as control_api
from api import control_wave_routes
from api import provider_directory_profile_capacity_preflight as snapshot_api
from process import provider_directory_capacity_reservation_snapshot as snapshot
from tests.test_provider_directory_capacity_reservation_snapshot import _metadata, _project

_PATH = "/control/provider-directory/profile-capacity-reservation-snapshot"


def _app(monkeypatch):
    monkeypatch.setattr(api_package.db, "init_app", lambda _app: None)
    monkeypatch.setattr(control_api, "ensure_import_run_table", AsyncMock())
    monkeypatch.setattr(control_wave_routes, "assert_nonterminal_receipt_key_coverage", AsyncMock())
    app = Sanic("reservation-snapshot-" + uuid4().hex)
    api_package.init_api(app)
    return app


@pytest.mark.asyncio
@pytest.mark.parametrize("auth", ["bearer", "explicit", "missing", "wrong", "unconfigured"])
async def test_shared_control_gate_precedes_snapshot_reads(monkeypatch, auth):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    headers_by_name = {"Authorization": "Bearer synthetic-token" if auth == "bearer" else "Bearer invalid-token"}
    if auth == "explicit":
        headers_by_name = {"X-HealthPorta-Control-Token": "synthetic-token"}
    elif auth == "missing":
        headers_by_name = {}
    elif auth == "unconfigured":
        monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    snapshot_by_field = _project()
    runner = AsyncMock(return_value=snapshot_by_field)
    monkeypatch.setattr(snapshot_api, "capacity_reservation_snapshot", runner)
    operation = snapshot_api.control_capacity_reservation_snapshot(SimpleNamespace(headers=headers_by_name))
    if auth in {"bearer", "explicit"}:
        result = await operation
        assert result.status == 200 and result.headers["Cache-Control"] == "no-store"
        assert json.loads(result.body) == snapshot_by_field
        assert json.loads(result.body)["capacity_complete"] is False
        runner.assert_awaited_once_with(snapshot_api.fhir)
    else:
        with pytest.raises(Forbidden):
            await operation
        runner.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("synthetic catalog detail"),
        OSError("synthetic backend detail"),
        ValueError("synthetic malformed envelope"),
    ],
)
async def test_snapshot_failures_return_neutral_unavailable(monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    monkeypatch.setattr(snapshot_api, "capacity_reservation_snapshot", AsyncMock(side_effect=failure))
    with pytest.raises(SanicException) as error:
        await snapshot_api.control_capacity_reservation_snapshot(
            SimpleNamespace(headers={"Authorization": "Bearer synthetic-token"})
        )
    assert error.value.status_code == 503
    assert str(error.value) == "capacity reservation snapshot unavailable"
    assert error.value.headers["Cache-Control"] == "no-store"


@pytest.mark.asyncio
async def test_total_timeout_cancels_snapshot_reader(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    monkeypatch.setattr(snapshot_api, "_RESERVATION_SNAPSHOT_TIMEOUT_SECONDS", 0.01)
    finished = asyncio.Event()

    async def read(_fhir):
        try:
            await asyncio.Future()
        finally:
            finished.set()

    monkeypatch.setattr(snapshot_api, "capacity_reservation_snapshot", read)
    with pytest.raises(SanicException) as error:
        await snapshot_api.control_capacity_reservation_snapshot(
            SimpleNamespace(headers={"Authorization": "Bearer synthetic-token"})
        )
    assert error.value.status_code == 503 and finished.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_get_registration_uses_control_error_contract(monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    snapshot_by_field = _project()
    reader = AsyncMock(
        return_value=snapshot_by_field, side_effect=RuntimeError("synthetic detail") if failure else None
    )
    monkeypatch.setattr(snapshot_api, "capacity_reservation_snapshot", reader)
    app = _app(monkeypatch)
    _, result = await app.asgi_client.get(_PATH, headers={"Authorization": "Bearer synthetic-token"})
    assert result.status == (503 if failure else 200)
    assert result.headers["Cache-Control"] == "no-store"
    if failure:
        assert result.json["error"]["code"] == "internal"
        assert result.json["error"]["message"] == "capacity reservation snapshot unavailable"
    else:
        assert result.json == snapshot_by_field
        assert result.json["contract_id"] == snapshot.CONTRACT_ID
    _, forbidden = await app.asgi_client.get(_PATH)
    assert forbidden.status == 403
    reader.assert_awaited_once_with(snapshot_api.fhir)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "backend_failure", "timeout"])
async def test_registered_snapshot_uses_real_reader_and_closes_transaction(monkeypatch, outcome):
    directory_importer = importlib.import_module("process.provider_directory_fhir")
    assert snapshot_api.fhir is directory_importer
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    monkeypatch.setattr(snapshot_api, "_RESERVATION_SNAPSHOT_TIMEOUT_SECONDS", 0.05)
    transaction_state = SimpleNamespace(is_active=False)
    statements = []

    async def execute(statement, *args):
        assert transaction_state.is_active
        sql = str(statement)
        statements.append(sql)
        if "pg_current_snapshot()" in sql:
            if outcome == "backend_failure":
                raise RuntimeError("synthetic catalog detail")
            if outcome == "timeout":
                await asyncio.Future()
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one=_metadata))
        return SimpleNamespace(mappings=lambda: [])

    @asynccontextmanager
    async def transaction():
        assert not transaction_state.is_active
        transaction_state.is_active = True
        try:
            yield SimpleNamespace(execute=execute)
        finally:
            transaction_state.is_active = False

    monkeypatch.setattr(directory_importer.db, "_transaction_binding", lambda: None)
    monkeypatch.setattr(directory_importer.db, "transaction", transaction)
    app = _app(monkeypatch)
    _, snapshot_response = await app.asgi_client.get(_PATH, headers={"Authorization": "Bearer synthetic-token"})
    assert not transaction_state.is_active
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    assert snapshot_response.headers["Cache-Control"] == "no-store"
    assert snapshot_response.status == (200 if outcome == "success" else 503)
    if outcome == "success":
        assert snapshot_response.json == _project() and len(statements) == 11
    else:
        assert snapshot_response.json["error"]["message"] == "capacity reservation snapshot unavailable"
