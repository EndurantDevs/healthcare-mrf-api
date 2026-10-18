# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host wiring checks; actual output custody remains a separate native proof."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests import cms_output_snapshot_postgres_support as outputs


@pytest.mark.parametrize("writable", [False, True])
async def test_output_writer_closure_checks_all_roots(writable):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=writable))
    policy = SimpleNamespace(schema="fixture", publisher_role="publisher", runtime_roles=("reader", "builder"))
    if writable:
        with pytest.raises(AssertionError):
            await outputs._assert_output_writers(connection, policy)
    else:
        await outputs._assert_output_writers(connection, policy)
    connection.fetchval.assert_awaited_once()
    sql, schema, roots, roles = connection.fetchval.await_args.args
    assert schema == "fixture" and roots == list(outputs.OUTPUT_ROOTS)
    assert roles == ["reader", "builder"]
    assert policy.publisher_role not in roles
    assert "has_table_privilege" in sql and "MAINTAIN" in sql and "has_any_column_privilege" in sql


@pytest.mark.parametrize("rejected", [False, True])
async def test_native_outputs_use_existing_admin_gate(monkeypatch, rejected):
    engine, policy = object(), object()
    session = SimpleNamespace(begin=lambda: nullcontext(), execute=AsyncMock())
    component = SimpleNamespace(admin_engine=engine, policy=policy)
    driver = object()
    monkeypatch.setattr(
        outputs, "async_sessionmaker", lambda actual: (lambda: nullcontext(session)) if actual is engine else None
    )
    copy_driver = AsyncMock(return_value=driver)
    monkeypatch.setattr(outputs, "_copy_driver", copy_driver)
    census = AsyncMock(return_value=tuple((name, 0) for name in outputs.OUTPUT_ROOTS))
    monkeypatch.setattr(outputs, "_output_census", census)
    failure = ValueError("original output gate refused")
    gate = AsyncMock(
        side_effect=failure if rejected else None,
        return_value=tuple(SimpleNamespace(target=name) for name in outputs.OUTPUT_ROOTS),
    )
    monkeypatch.setattr(outputs, "provision_output_snapshot_storage", gate)
    if rejected:
        with pytest.raises(ValueError) as caught:
            await outputs.provision_native_outputs(component)
        assert caught.value is failure
    else:
        await outputs.provision_native_outputs(component)
    copy_driver.assert_awaited_once_with(session)
    gate.assert_awaited_once_with(
        session,
        policy,
        verify_role_profile=outputs.verify_native_source_profile,
        verify_writer_closure=outputs._assert_output_writers,
    )
    assert census.await_count == (1 if rejected else 2)
    assert all(call.args == (driver, policy) for call in census.await_args_list)


async def test_output_gate_precedes_source_attestation(monkeypatch, tmp_path):
    from tests import cms_registry_complete_publication_postgres_support as bootstrap

    connection = object()
    engine = SimpleNamespace(begin=lambda: nullcontext(connection))
    component = SimpleNamespace(admin_engine=engine, policy=SimpleNamespace(schema="mrf"))
    monkeypatch.setattr(bootstrap.source.fhir.db, "_transaction_binding", lambda: None)
    monkeypatch.setattr(bootstrap, "Database", lambda **kwargs: object())
    monkeypatch.setattr(bootstrap, "async_sessionmaker", lambda actual: None)
    monkeypatch.setattr(bootstrap, "_source_native_control_catalog", AsyncMock(return_value=object()))
    initialized = AsyncMock()
    monkeypatch.setattr(bootstrap, "_initialize_native_inputs", initialized)
    failure = ValueError("output custody refused")

    async def refuse(actual):
        initialized.assert_awaited_once()
        assert actual is component
        raise failure

    monkeypatch.setattr(outputs, "provision_native_outputs", refuse)
    with pytest.raises(ValueError) as caught:
        await bootstrap.prepare_source_native_inputs(component, monkeypatch, tmp_path, "synthetic-source")
    assert caught.value is failure
