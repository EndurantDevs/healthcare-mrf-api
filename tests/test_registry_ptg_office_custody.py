# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed server configuration and native-profile refusal seams, not native proof."""

import json
from copy import deepcopy
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


@pytest.mark.parametrize("mode", ["plan", "custody", "publisher"])
@pytest.mark.parametrize("encoded", [False, True])
def test_absent_intents_preserves_explicit_configuration(mode, encoded):
    """An optional durable service adds no derived fields to unrelated inputs."""
    configured = _configured()
    if mode != "plan":
        configured["office_custody"] = {"owner_role": "office_owner", "publisher_role": "office_publisher"}
    if mode == "publisher":
        configured["publisher_dsn"] = "postgresql://office_publisher@database.example/synthetic?ssl=require"
    original = deepcopy(configured)
    parsed = runtime._configuration(json.dumps(configured) if encoded else configured)[0]
    expected = deepcopy(original)
    if mode != "plan":
        expected["office_custody"] = custody.RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher")
    assert parsed == expected
    assert configured == original


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
    monkeypatch.setattr(approval, "_approved_source_review", AsyncMock(return_value=document))
    monkeypatch.setattr(approval, "_reader_roles", lambda actual: ("office_reader", "scope_approver"))
    from process import registry_ptg_office_capture as capture

    monkeypatch.setattr(capture, "_driver", AsyncMock(return_value=driver))
    context = await approval._context(session, service, _envelope()["command"], "synthetic_control")
    assert context.owner_role == "office_owner" != service.store.owner_role
    assert context.office_custody is profile
    assert custody.office_preparation_roles(context) == {
        "namespace_owner_role": "office_owner",
        "publisher_role": "office_publisher",
        "application_roles": ["office_publisher", "office_reader", "scope_approver"],
    }


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
        office_custody=None,
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


@pytest.mark.parametrize("change", ["profile", "owner", "reader_owner", "empty"])
def test_office_role_configuration_refuses_missing_or_conflicting_names(change):
    profile = custody.RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher")
    context = SimpleNamespace(
        office_custody=profile, owner_role="office_owner", reader_roles=("office_reader", "approver")
    )
    if change == "profile":
        context.office_custody = None
    elif change == "owner":
        context.owner_role = "other_owner"
    elif change == "reader_owner":
        context.reader_roles = ("office_owner",)
    else:
        context.reader_roles = ()
    with pytest.raises(ValueError, match="profile_unavailable"):
        custody.office_preparation_roles(context)


@pytest.mark.asyncio
async def test_administrator_installer_gives_only_publisher_and_approver_control_writes():
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
    from process.registry_ptg_published_provisioning import install_registry_ptg_office_preparation_permissions

    connection = SimpleNamespace(
        is_in_transaction=lambda: True, execute=AsyncMock(), fetchval=AsyncMock(return_value='"synthetic"')
    )
    profile = custody.RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher")
    store = RegistryPTGProducerScopeStore("scope_owner", "office_approver", "synthetic_control")
    await install_registry_ptg_office_preparation_permissions(
        connection, profile=profile, store=store, reader_roles=("office_reader",)
    )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    writes = [
        query
        for query in statements
        if query.startswith("GRANT SELECT,INSERT(") and "ON hp_snapshot_retention.preparation TO" in query
    ]
    assert len(writes) == 1 and writes[0].endswith('TO "office_approver","office_publisher"')
    assert all("office_reader" not in query for query in writes)
    assert "frozen_inventory" not in writes[0] and "frozen_sha256" not in writes[0]
    assert any(
        "preparation_claim" in query and "preparation_relation" in query
        for query in statements
        if query.startswith("REVOKE ") and query.endswith('FROM "office_reader"')
    )
    assert any(
        "INSERT(" in query and "frozen_inventory" in query
        for query in statements
        if query.startswith("REVOKE ") and query.endswith('FROM "office_reader"')
    )

    assert any(query.startswith("GRANT SELECT ON ") and query.endswith('TO "office_reader"') for query in statements)
    assert any(query.startswith('REVOKE "office_owner" FROM ') for query in statements)
    assert not any(query.startswith('GRANT "office_owner"') or "SET ROLE" in query for query in statements)
