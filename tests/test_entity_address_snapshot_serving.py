# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

serving = importlib.import_module("process.entity_address_snapshot_serving")
generation = importlib.import_module("process.entity_address_result_generation")


def _session(*, active: bool = True):
    session = SimpleNamespace(
        execute=AsyncMock(),
        scalar=AsyncMock(return_value="read committed"),
        in_transaction=lambda: active,
    )
    session.scalar.side_effect = lambda sql: (
        None if "active_dependency_bindings" in str(sql) else session.scalar.return_value
    )
    return session


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
    async def relation_name(_session, schema, table):
        return f"{schema}.{table}"

    monkeypatch.setattr(serving, "_relation_name", relation_name)
    monkeypatch.setattr(serving, "_alias_state", AsyncMock(return_value=(2, 1, 4)))
    monkeypatch.setattr(
        serving,
        "_geo_assurance_state",
        AsyncMock(return_value=1),
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
    assert statements[11].startswith('LOCK TABLE "mrf"."doctor_clinician_address"')
    assert statements[11].endswith(" IN ACCESS SHARE MODE")
    assert captured.relation_oids == (None,) * 7
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
    capture["contract"] = serving.LEGACY_CONTRACT
    for relation in capture["relations"]:
        relation["relation_oid"] = 17
    capture["geo_assurance"] = {"version": 1, "active_table_oid": 17, "active_relation_signature": None}
    capture.pop("result_generation")

    validated = serving.validate_entity_address_observed_serving_capture(capture)

    assert validated.result_generation is None


def test_capture_ignores_legacy_relation_oids():
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

    capture["contract"] = serving.LEGACY_CONTRACT
    capture["geo_assurance"] = {"version": 1, "active_table_oid": 10, "active_relation_signature": None}
    for relation in capture["relations"]:
        relation["relation_oid"] = 10
    assert serving.validate_entity_address_observed_serving_capture(capture).relation_oids == (None,) * 7


@pytest.mark.asyncio
async def test_generation_state_uses_portable_origin_for_named_relations(monkeypatch):
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

    names = tuple(f"mrf.{name}" for name in generation.RELATION_NAMES)
    assert (
        await serving._result_generation_state(object(), schema_name="mrf", relation_names=names)
        == authority.serving_generation
    )
    with pytest.raises(RuntimeError, match="serving relations are incomplete"):
        await serving._result_generation_state(
            object(),
            schema_name="mrf",
            relation_names=names[:-1],
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


@pytest.mark.asyncio
async def test_receive_admission_uses_read_locks_but_source_observer_stays_strict(monkeypatch):
    session = _session()
    _install_observation_results(monkeypatch, "mrf")
    captured = await serving.capture_entity_address_receive_admission(session, schema_name="mrf")
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len([statement for statement in statements[3:10] if " IN ACCESS SHARE MODE" in statement]) == 7
    assert not any("ISOLATION LEVEL" in statement for statement in statements)
    assert str(session.scalar.await_args_list[0].args[0]) == "SHOW transaction_isolation"
    assert serving._geo_assurance_state.await_args.kwargs["allow_unpublished_geo"] is True
    assert serving.validate_entity_address_observed_serving_capture(captured.as_dict()) == captured


@pytest.mark.asyncio
async def test_receive_final_comparison_uses_writer_order_and_rejects_changed_token(monkeypatch):
    session = _session()
    _install_observation_results(monkeypatch, "mrf")
    expected = (await serving.capture_entity_address_receive_admission(session, schema_name="mrf")).as_dict()
    session.execute.reset_mock()
    _install_observation_results(monkeypatch, "mrf")
    await serving.require_entity_address_receive_incumbent(session, schema_name="mrf", expected=expected)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[:3] == [
        "SET LOCAL lock_timeout TO '1s'",
        "SET LOCAL statement_timeout TO '3s'",
        serving.address_alias_sql.alias_advisory_xact_lock_sql(),
    ]
    assert len([statement for statement in statements[3:10] if " IN SHARE MODE" in statement]) == 7
    _install_observation_results(monkeypatch, "mrf")
    expected["alias_state"]["generation"] += 1
    with pytest.raises(RuntimeError, match="receive incumbent changed"):
        await serving.require_entity_address_receive_incumbent(session, schema_name="mrf", expected=expected)


def _install_empty_receive_results(monkeypatch):
    _install_observation_results(monkeypatch, "mrf")
    monkeypatch.setattr(serving, "_geo_assurance_state", AsyncMock(return_value=None))
    authority = SimpleNamespace(local_generation=0, serving_generation=None, relation_oids=None)
    monkeypatch.setattr(
        generation, "read_entity_address_result_generation_authority", AsyncMock(return_value=authority)
    )
    return authority


@pytest.mark.asyncio
async def test_empty_receive_roundtrips_null_geo_and_rechecks_before_publication(monkeypatch):
    session = _session()
    session.scalar.side_effect = lambda sql: (
        None
        if "active_dependency_bindings" in str(sql)
        else "read committed"
        if str(sql) == "SHOW transaction_isolation"
        else False
    )
    _install_empty_receive_results(monkeypatch)
    captured = await serving.capture_entity_address_receive_admission(session, schema_name="mrf")
    assert captured.as_dict()["geo_assurance"]["version"] is None
    assert set(captured.as_dict()["geo_assurance"]) == {"version", "dependencies"}
    assert serving.validate_entity_address_observed_serving_capture(captured, allow_unpublished_geo=True) == captured
    with pytest.raises(ValueError):
        serving.validate_entity_address_observed_serving_capture(captured)
    _install_empty_receive_results(monkeypatch)
    await serving.require_entity_address_receive_incumbent(session, schema_name="mrf", expected=captured)
    assert len([call for call in session.scalar.await_args_list if str(call.args[0]).startswith("SELECT EXISTS")]) == 2
    _install_empty_receive_results(monkeypatch)
    with pytest.raises(RuntimeError, match="not serving authority"):
        await serving.capture_entity_address_observed_serving(session, schema_name="mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ("generation", "populated", "alias", "relation"))
async def test_empty_receive_final_recheck_rejects_changed_state(monkeypatch, invalid):
    session = _session()
    session.scalar.side_effect = lambda sql: (
        None
        if "active_dependency_bindings" in str(sql)
        else "read committed"
        if str(sql) == "SHOW transaction_isolation"
        else False
    )
    _install_empty_receive_results(monkeypatch)
    expected = (await serving.capture_entity_address_receive_admission(session, schema_name="mrf")).as_dict()
    authority = _install_empty_receive_results(monkeypatch)
    if invalid == "generation":
        authority.local_generation = 1
    elif invalid == "populated":
        session.scalar.side_effect = lambda sql: (
            None
            if "active_dependency_bindings" in str(sql)
            else "read committed"
            if str(sql) == "SHOW transaction_isolation"
            else True
        )
    elif invalid == "alias":
        monkeypatch.setattr(serving, "_alias_state", AsyncMock(return_value=(2, 1, 5)))
    else:
        monkeypatch.setattr(serving, "_relation_name", AsyncMock(side_effect=RuntimeError("relation is unavailable")))
    with pytest.raises(RuntimeError):
        await serving.require_entity_address_receive_incumbent(session, schema_name="mrf", expected=expected)


@pytest.mark.asyncio
async def test_receive_admission_requires_transaction_before_sql():
    session = _session(active=False)
    with pytest.raises(ValueError, match="requires a caller transaction"):
        await serving.capture_entity_address_receive_admission(session, schema_name="mrf")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable", "read uncommitted"])
async def test_receive_admission_rejects_unsupported_isolation_without_changing_it(isolation):
    session = _session()
    session.scalar.return_value = isolation
    with pytest.raises(RuntimeError, match="requires READ COMMITTED isolation"):
        await serving.capture_entity_address_receive_admission(session, schema_name="mrf")
    session.execute.assert_not_awaited()


async def test_named_dependency_capture_discards_old_catalog_metadata(monkeypatch):
    session = _session()
    _install_observation_results(monkeypatch, "mrf")
    bindings_by_role = {
        name: {"schema_name": "published_inputs", "table_name": table, "relation_oid": None, "relfilenode": None}
        for name, _schema, table in serving._dependency_relations("mrf")
    }
    session.scalar.side_effect = lambda _sql: bindings_by_role
    captured = await serving.capture_entity_address_observed_serving(session, schema_name="mrf")
    document = captured.as_dict()
    assert document["contract"] == serving.CONTRACT
    assert all(set(relation) == {"model_name", "table_name"} for relation in document["relations"])
    assert all(
        set(binding) == {"schema_name", "table_name"} for binding in document["geo_assurance"]["dependencies"].values()
    )
    assert serving.validate_entity_address_observed_serving_capture(document) == captured
    assert all(
        'ONLY "published_inputs".' + '"' + table + '"' in str(session.execute.await_args_list[11].args[0])
        for _, _, table in serving._dependency_relations("mrf")
    )


@pytest.mark.parametrize("explicit", [False, True])
async def test_canonical_dependency_capture_locks_inheritance(monkeypatch, explicit):
    session = _session()
    _install_observation_results(monkeypatch, "mrf")
    bindings_by_role = {
        name: {"schema_name": schema, "table_name": table}
        for name, schema, table in serving._dependency_relations("mrf")
    }
    session.scalar.side_effect = lambda _sql: bindings_by_role if explicit else None
    await serving.capture_entity_address_observed_serving(session, schema_name="mrf")
    lock_sql = str(session.execute.await_args_list[11].args[0])
    assert "ONLY" not in lock_sql
    assert '"tiger"."zcta5"' in lock_sql and '"tiger"."zip_state"' in lock_sql
    assert lock_sql.endswith(" IN ACCESS SHARE MODE")
