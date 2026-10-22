# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Locked scope, anchored deadlines and registration rollback without external I/O."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import capture_handoff as service
from process.custom_import.runner_types import LeaseAuthorityLost
from tests.capture_handoff_test_support import NOW, fixture
from tests.test_custom_import_admission_service import Session


def _store(monkeypatch):
    state = fixture()
    state.session = Session()
    state.now = NOW
    state.retained = SimpleNamespace(
        idempotency_key=state.request.idempotency_key,
        request_identity_sha256=bytes.fromhex(state.verified.permit.request_identity_sha256),
        started_at=NOW - timedelta(seconds=1),
        capture_bundle_id=None,
    )
    state.session.get = AsyncMock(
        return_value=SimpleNamespace(
            canonical_definition=state.request.definition.canonical,
        )
    )

    async def locked(*_args, **_kwargs):
        return state.retained, "running", state.now

    async def register(*_args, **_kwargs):
        state.session.pending.append("capture")
        return SimpleNamespace(capture_bundle_id=71)

    monkeypatch.setattr(service.execution, "_lock_current_capture_binding", AsyncMock(side_effect=locked))
    monkeypatch.setattr(service.execution, "_database_now", AsyncMock(side_effect=lambda *_: state.now))
    monkeypatch.setattr(service.runner_registry, "validate_revision_identity", AsyncMock())
    monkeypatch.setattr(service, "require_raw_capture_authority", AsyncMock())
    monkeypatch.setattr(service, "register_raw_capture_bundle", AsyncMock(side_effect=register))
    monkeypatch.setattr(
        service.execution,
        "bind_execution_capture_bundle",
        AsyncMock(
            return_value=SimpleNamespace(capture_bundle_id=71),
        ),
    )
    monkeypatch.setattr(service, "load_replayable_raw_bundle", AsyncMock(return_value=state.captures))
    state.factory = lambda: state.session
    return state


@pytest.mark.asyncio
async def test_preflight_anchors_to_first_start_and_requires_registrar(monkeypatch):
    state = _store(monkeypatch)
    deadline = await service.preflight_capture(state.factory, state.verified)
    assert deadline == NOW + timedelta(seconds=4)
    service.require_raw_capture_authority.assert_awaited_once_with(state.session)
    state.now = NOW + timedelta(seconds=5)
    with pytest.raises(LeaseAuthorityLost):
        await service.preflight_capture(state.factory, state.verified)
    assert state.session.events.count("rollback") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["bind", "deadline", "content", "identity", "lease"])
async def test_failed_registration_never_commits_partial_capture(monkeypatch, failure):
    state = _store(monkeypatch)
    if failure == "bind":
        service.execution.bind_execution_capture_bundle.return_value = None
    elif failure == "deadline":

        async def late_register(*_args, **_kwargs):
            state.session.pending.append("capture")
            state.now = NOW + timedelta(seconds=5)
            return SimpleNamespace(capture_bundle_id=71)

        service.register_raw_capture_bundle.side_effect = late_register
    elif failure == "content":
        state.verified = replace(
            state.verified,
            permit=replace(
                state.verified.permit,
                expected_content_sha256="f" * 64,
            ),
        )
    elif failure == "identity":
        state.retained.idempotency_key = "different-request"
    else:
        service.execution._lock_current_capture_binding.side_effect = None
        service.execution._lock_current_capture_binding.return_value = None
    with pytest.raises((service.CaptureHandoffError, LeaseAuthorityLost)):
        await service.register_capture(state.factory, state.verified, state.captures)
    assert state.session.durable == state.session.pending == []
    assert "rollback" in state.session.events and "commit" not in state.session.events


@pytest.mark.asyncio
async def test_registration_uses_existing_store_and_commits_only_after_binding(monkeypatch):
    state = _store(monkeypatch)
    assert await service.register_capture(state.factory, state.verified, state.captures) == 71
    assert state.session.durable == ["capture"]
    options = service.register_raw_capture_bundle.await_args.kwargs
    assert options["maximum_dataset_retained_bytes"] == state.request.maximum_dataset_retained_bytes
    assert options["limits"] == state.request.limits
    assert service.execution._lock_current_capture_binding.await_count == 2
    service.execution.bind_execution_capture_bundle.assert_awaited_once()


@pytest.mark.asyncio
async def test_lost_ack_observation_after_acquisition_expiry_never_reopens_source(monkeypatch):
    state = _store(monkeypatch)
    state.retained.capture_bundle_id = 71
    state.now = NOW + timedelta(seconds=30)
    state.verified = replace(state.verified, fence=2)
    assert await service.retained_capture(state.factory, state.verified) == (71, state.captures)
    service.register_raw_capture_bundle.assert_not_awaited()
    service.require_raw_capture_authority.assert_not_awaited()
    with pytest.raises(LeaseAuthorityLost):
        await service.preflight_capture(state.factory, state.verified)


@pytest.mark.asyncio
async def test_retained_read_rechecks_lease_after_large_payload_verification(monkeypatch):
    state = _store(monkeypatch)
    state.retained.capture_bundle_id = 71
    service.execution._lock_current_capture_binding.side_effect = [(state.retained, "running", NOW), None]
    with pytest.raises(LeaseAuthorityLost):
        await service.retained_capture(state.factory, state.verified)
    service.load_replayable_raw_bundle.assert_awaited_once()
    assert "rollback" in state.session.events
