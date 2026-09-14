# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

serving = importlib.import_module("process.entity_address_snapshot_serving")
generation = importlib.import_module("process.entity_address_result_generation")


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
    monkeypatch.setattr(serving, "_result_generation_state", AsyncMock(return_value=None))


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
    assert captured.result_generation is None
    assert serving.validate_entity_address_observed_serving_capture(captured.as_dict()) == captured


@pytest.mark.asyncio
async def test_capture_exports_durable_origin_generation(monkeypatch: pytest.MonkeyPatch):
    session = _session()
    _install_observation_results(monkeypatch, "mrf")
    expected = generation.EntityAddressServingGeneration(
        origin_lineage_id="c8f27af1-56ba-4cda-82d8-0fc67650918f",
        origin_generation=12,
        published_at=datetime.datetime(2026, 9, 14, 8, 30, tzinfo=datetime.UTC),
    )
    monkeypatch.setattr(
        serving,
        "_result_generation_state",
        AsyncMock(return_value=expected),
    )

    captured = await serving.capture_entity_address_observed_serving(
        session,
        schema_name="mrf",
    )

    assert captured.result_generation == expected
    assert captured.as_dict()["result_generation"] == expected.as_dict()


def test_legacy_capture_without_generation_is_explicitly_generationless():
    capture = serving.EntityAddressObservedServingCapture(
        contract=serving.CONTRACT,
        source_schema="mrf",
        relation_oids=tuple(range(10, 17)),
        alias_schema_version=2,
        alias_ruleset_version=1,
        alias_generation=4,
        geo_assurance_version=1,
        geo_active_table_oid=10,
        geo_active_relation_signature=_geo_signature("mrf"),
    ).as_dict()
    capture.pop("result_generation")

    validated = serving.validate_entity_address_observed_serving_capture(capture)

    assert validated.result_generation is None


def test_capture_rejects_duplicate_relation_oids():
    capture = serving.EntityAddressObservedServingCapture(
        contract=serving.CONTRACT,
        source_schema="mrf",
        relation_oids=(10, 10, 12, 13, 14, 15, 16),
        alias_schema_version=2,
        alias_ruleset_version=1,
        alias_generation=4,
        geo_assurance_version=1,
        geo_active_table_oid=10,
        geo_active_relation_signature=_geo_signature("mrf"),
    ).as_dict()

    with pytest.raises(ValueError, match="relation identity"):
        serving.validate_entity_address_observed_serving_capture(capture)


@pytest.mark.asyncio
async def test_generation_state_rejects_stale_relation_identity(monkeypatch):
    authority = generation.EntityAddressResultGenerationAuthority(
        local_lineage_id="c8f27af1-56ba-4cda-82d8-0fc67650918f",
        local_generation=12,
        serving_generation=generation.EntityAddressServingGeneration(
            origin_lineage_id="c8f27af1-56ba-4cda-82d8-0fc67650918f",
            origin_generation=12,
            published_at=datetime.datetime(2026, 9, 14, 8, 30, tzinfo=datetime.UTC),
        ),
        relation_oids=tuple(range(20, 27)),
    )
    monkeypatch.setattr(
        generation,
        "read_entity_address_result_generation_authority",
        AsyncMock(return_value=authority),
    )

    with pytest.raises(RuntimeError, match="does not identify the serving relations"):
        await serving._result_generation_state(
            object(),
            schema_name="mrf",
            relation_oids=tuple(range(10, 17)),
        )


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
