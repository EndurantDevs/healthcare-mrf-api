# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

serving = importlib.import_module("process.entity_address_snapshot_serving")


def _session(*, active: bool = True):
    return SimpleNamespace(
        execute=AsyncMock(),
        in_transaction=lambda: active,
    )


def _geo_signature(schema_name: str) -> tuple[tuple[str, int, int], ...]:
    names = (
        f"{schema_name}.doctor_clinician_address",
        f"{schema_name}.geo_zip_lookup",
        f"{schema_name}.mrf_address",
        f"{schema_name}.npi_address",
        "tiger.zcta5",
        "tiger.zip_state",
    )
    return tuple((name, ordinal + 20, ordinal + 30) for ordinal, name in enumerate(names))


def _install_observation_results(monkeypatch: pytest.MonkeyPatch, schema_name: str) -> None:
    relation_oids = iter(range(10, 17))

    async def relation_oid(_session, _schema_name, _table_name):
        return next(relation_oids)

    monkeypatch.setattr(serving, "_relation_oid", relation_oid)
    monkeypatch.setattr(serving, "_alias_state", AsyncMock(return_value=(2, 1, 4)))
    monkeypatch.setattr(
        serving,
        "_geo_assurance_state",
        AsyncMock(return_value=(1, 10, _geo_signature(schema_name))),
    )


@pytest.mark.asyncio
async def test_queue_capture_uses_fixed_bounds_and_writer_lock_order(monkeypatch: pytest.MonkeyPatch):
    session = _session()
    _install_observation_results(monkeypatch, "mrf")

    captured = await serving.capture_entity_address_observed_serving(session, schema_name="mrf")

    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[:4] == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET LOCAL lock_timeout TO '1s'",
        "SET LOCAL statement_timeout TO '3s'",
        serving.address_alias_sql.alias_advisory_xact_lock_sql(),
    ]
    assert len([statement for statement in statements[4:11] if " IN SHARE MODE" in statement]) == 7
    assert statements[11] == serving.geo_projection.projection_dependency_lock_sql("mrf")
    assert captured.relation_oids == tuple(range(10, 17))
    assert serving.validate_entity_address_observed_serving_capture(captured.as_dict()) == captured


@pytest.mark.asyncio
async def test_worker_observation_does_not_inherit_queue_timeouts(monkeypatch: pytest.MonkeyPatch):
    session = _session()
    _install_observation_results(monkeypatch, "mrf")

    await serving.observe_entity_address_serving(
        session,
        schema_name="mrf",
        apply_queue_bounds=False,
    )

    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert statements[1] == serving.address_alias_sql.alias_advisory_xact_lock_sql()
    assert not any("timeout" in statement for statement in statements)


@pytest.mark.asyncio
async def test_observation_requires_caller_transaction_before_sql():
    session = _session(active=False)

    with pytest.raises(ValueError, match="requires a caller transaction"):
        await serving.capture_entity_address_observed_serving(session, schema_name="mrf")

    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_observation_rejects_oversize_schema_before_sql():
    session = _session()

    with pytest.raises(ValueError, match="safe schema name"):
        await serving.capture_entity_address_observed_serving(session, schema_name="a" * 64)

    session.execute.assert_not_awaited()
