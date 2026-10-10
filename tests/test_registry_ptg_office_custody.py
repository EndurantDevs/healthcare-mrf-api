# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed server configuration and native-profile refusal seams, not native proof."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_office_approval as approval
from process import registry_ptg_office_custody as custody
from process import registry_ptg_scope_runtime as runtime
from tests.test_registry_ptg_office_approval import _envelope
from tests.test_registry_ptg_published_office_capture import _fixture
from tests.test_registry_ptg_scope_engine import _service
from tests.test_registry_ptg_scope_runtime import _configured


def test_optional_server_profile_preserves_plan_configuration():
    old = _configured()
    assert runtime._configuration(old)[0] == old
    configured_by_field = {
        **old,
        "office_custody": {"owner_role": "office_owner", "publisher_role": "office_publisher"},
    }
    assert runtime._configuration(configured_by_field)[0]["office_custody"] == custody.RegistryPTGOfficeCustodyProfile(
        "office_owner", "office_publisher"
    )
    with pytest.raises(ValueError):
        runtime._configuration(
            {**configured_by_field, "office_custody": {"owner_role": old["owner_role"], "publisher_role": "p"}}
        )


@pytest.mark.asyncio
async def test_missing_office_profile_refuses_original_context_without_breaking_plan_service(monkeypatch):
    service, _ = _service()
    session, _, document = _fixture()
    monkeypatch.setattr(approval, "_approved_source_review", AsyncMock(return_value=document))
    with pytest.raises(ValueError, match="profile_unavailable"):
        await approval._context(session, service, _envelope()["command"], "synthetic_control")
    assert service.office_custody is None


@pytest.mark.asyncio
async def test_approval_uses_installed_office_owner_and_consumes_native_guard(monkeypatch):
    service, _ = _service()
    profile = custody.RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher")
    service = replace(service, office_custody=profile)
    session, _, document = _fixture()
    driver = object()
    guard = AsyncMock(return_value="office_owner")
    monkeypatch.setattr(approval, "_approved_source_review", AsyncMock(return_value=document))
    monkeypatch.setattr(approval, "_reader_roles", lambda actual: ("office_reader", "scope_approver"))
    from process import registry_ptg_office_capture as capture

    monkeypatch.setattr(capture, "_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(custody, "verify_registry_ptg_office_custody", guard)
    context = await approval._context(session, service, _envelope()["command"], "synthetic_control")
    assert context.owner_role == "office_owner" != service.store.owner_role
    assert context.office_custody is profile
    guard.assert_awaited_once_with(driver, context, publisher=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [None, "genuine", "flags", "set_closed", "readers_closed", "office_only", "publisher_writes_closed", "oversize"],
)
async def test_native_profile_refuses_foreign_owner_set_acl_and_catalog_scope(monkeypatch, failure):
    profile = custody.RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher")
    context = SimpleNamespace(
        office_custody=profile,
        owner_role="office_owner",
        reader_roles=("reader", "approver"),
        scope_store=SimpleNamespace(owner_role="store_owner"),
        source_specification=SimpleNamespace(ptg_schema_name="synthetic_ptg"),
    )
    record_by_field = {
        field: True
        for field in ("genuine", "flags", "set_closed", "readers_closed", "office_only", "publisher_writes_closed")
    }
    record_by_field.update(schema_count=0, schemas=[], owner_oid=10, publisher_oid=11)
    if failure == "oversize":
        record_by_field["schema_count"] = 129
    elif failure:
        record_by_field[failure] = False
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=record_by_field))
    if failure:
        with pytest.raises(ValueError, match="profile_unavailable"):
            await custody.verify_registry_ptg_office_custody(driver, context, publisher=True)
    else:
        assert await custody.verify_registry_ptg_office_custody(driver, context, publisher=True) == "office_owner"
    assert driver.fetchrow.await_args.args[1:] == (
        "office_owner",
        "office_publisher",
        ["reader", "approver"],
        True,
        "store_owner",
        "synthetic_ptg",
    )


def test_browser_owner_fields_refuse_at_original_closed_envelope():
    envelope = _envelope()
    envelope["command"]["owner_role"] = "browser_owner"
    with pytest.raises(ValueError):
        approval.validated_office_envelope(envelope)


@pytest.mark.asyncio
async def test_published_capture_missing_profile_refuses_before_candidate_ddl(monkeypatch):
    from process import registry_ptg_office_capture as capture
    from process.registry_ptg_published_plan_contract import RegistryPTGPublishedPlanSourceSpecification
    from tests.test_registry_ptg_office_capture import _context, _request

    context = replace(
        _context(),
        source_specification=RegistryPTGPublishedPlanSourceSpecification(
            "22222222-2222-4222-8222-222222222222", "synthetic_ptg", "snapshot", "source"
        ),
    )
    driver = SimpleNamespace(execute=AsyncMock())
    session = SimpleNamespace()
    monkeypatch.setattr(capture, "require_published_office_path", AsyncMock())
    monkeypatch.setattr(capture, "_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(capture, "_encoder", lambda: object())
    with pytest.raises(ValueError, match="profile_unavailable"):
        await capture.prepare_registry_ptg_office_capture(session, context, _request(), object())
    driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_carries_only_trusted_profile_into_original_service(monkeypatch):
    from tests.test_registry_ptg_scope_runtime import _boundaries

    boundary = _boundaries(monkeypatch)
    configured_by_field = {
        **_configured(),
        "office_custody": {"owner_role": "office_owner", "publisher_role": "office_publisher"},
    }
    owned = await runtime.build_registry_ptg_scope_runtime(configured_by_field)
    assert owned.service.office_custody == custody.RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher")
    await owned.close()
    assert all(engine.dispose.await_count == 1 for engine in boundary.engines)


@pytest.mark.asyncio
async def test_release_consumes_profile_before_owner_fence_or_store_sql(monkeypatch):
    from process import registry_ptg_office_retention as retention
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
    from process.registry_ptg_published_plan_contract import RegistryPTGPublishedPlanSourceSpecification
    from tests.test_registry_ptg_office_capture import _context

    context = replace(
        _context(),
        source_specification=RegistryPTGPublishedPlanSourceSpecification(
            "22222222-2222-4222-8222-222222222222", "synthetic_ptg", "snapshot", "source"
        ),
    )
    guard = AsyncMock(side_effect=ValueError("registry_ptg_office_profile_unavailable"))
    fence = AsyncMock()
    monkeypatch.setattr(custody, "verify_registry_ptg_office_custody", guard)
    monkeypatch.setattr(retention, "require_registry_company_approval_fence", fence)
    session = SimpleNamespace(execute=AsyncMock())
    store = RegistryPTGProducerScopeStore("store_owner", "approver", context.control_schema)
    driver = object()
    with pytest.raises(ValueError, match="profile_unavailable"):
        await retention._owner_tables(session, store, driver, context)
    guard.assert_awaited_once_with(driver, context, publisher=True)
    fence.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_configured_publisher_write_refusal_is_consumed_on_approver_connection():
    profile = custody.RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher")
    context = SimpleNamespace(
        office_custody=profile,
        owner_role="office_owner",
        reader_roles=("reader", "approver"),
        scope_store=SimpleNamespace(owner_role="store_owner"),
        source_specification=SimpleNamespace(ptg_schema_name="synthetic_ptg"),
    )
    record_by_field = {field: True for field in ("genuine", "flags", "set_closed", "readers_closed", "office_only")}
    record_by_field.update(schema_count=0, schemas=[], publisher_writes_closed=False)
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=record_by_field))
    with pytest.raises(ValueError, match="profile_unavailable"):
        await custody.verify_registry_ptg_office_custody(driver, context, publisher=False)
    assert driver.fetchrow.await_args.args[2] == "office_publisher"
    assert driver.fetchrow.await_args.args[4] is False


def test_publisher_query_reuses_native_guard_without_pin_writer_or_role_switch():
    from process.registry_ptg_published_provisioning import HEADER_TABLES, LOCK_COLUMN

    assert "current_user" not in custody._PUBLISHER_LOCK_SQL
    assert "$2::name" in custody._PUBLISHER_LOCK_SQL
    assert ":" not in custody._PUBLISHER_LOCK_SQL.replace("::", "")
    assert "SET ROLE" not in custody._PROFILE_SQL
    assert "MAINTAIN" in custody._PUBLISHER_LOCK_SQL
    assert "AND NOT(FALSE AND" in custody._PUBLISHER_LOCK_SQL
    assert LOCK_COLUMN + "=0" in custody._PUBLISHER_LOCK_SQL
    assert all("'" + table + "'" in custody._PUBLISHER_LOCK_SQL for table in HEADER_TABLES)
