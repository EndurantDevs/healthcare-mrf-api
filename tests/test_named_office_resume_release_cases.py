# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Office intent recovery and release preserve the original candidate on failure."""

import copy
import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_office_coordinator as coordinator
from process import registry_ptg_office_preparation as preparation
from process import registry_ptg_office_retention as retention
from process.registry_ptg_office_capture import _digest
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_ptg_scope_engine import RegistryPTGScopeDeadlineExpired
from tests.test_registry_ptg_office_coordinator import IDENTITY, boundaries, envelope
from tests.test_registry_ptg_office_retention import _release_case, _verified


def _pending_fixture(monkeypatch):
    fixture = boundaries(monkeypatch)
    prepare = fixture.preparation.prepare.side_effect

    async def pending(document, **kwargs):
        reply = await prepare(document, **kwargs)
        reply["state"] = "handed_off"
        return reply

    fixture.preparation.prepare.side_effect = pending
    return fixture, prepare


@pytest.mark.asyncio
async def test_lost_pending_yield_ack_resumes_original_candidate(monkeypatch):
    fixture, prepare = _pending_fixture(monkeypatch)
    fixture.queue.lose_commit_on = 2
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as raised:
        await fixture.service.prepare(envelope())
    assert raised.value.outcome_unknown
    original = copy.deepcopy(next(iter(fixture.storage.requests_by_key.values())))
    assert original["state"] == "running" and fixture.queue.expired
    assert "finish" not in fixture.events
    fixture.queue.lose_commit_on = None
    fixture.preparation.prepare.side_effect = prepare
    resumed = await fixture.service.prepare(envelope())
    assert resumed["intent"]["request_id"] == str(original["request_id"])
    assert resumed["preparation"]["capture_id"] == json.loads(original["command_json"])["selector"]["capture_id"]
    assert resumed["intent"]["state"] == "completed"
    assert len(fixture.storage.requests_by_key) == len(fixture.prepared_by_capture) == 1


@pytest.mark.asyncio
async def test_failed_pending_yield_cas_never_reports_completion(monkeypatch):
    fixture, _ = _pending_fixture(monkeypatch)
    fetchval = fixture.queue.fetchval

    async def lost_lease(sql, *args):
        if "SET lease_expires_at=clock_timestamp()" in sql:
            return None
        return await fetchval(sql, *args)

    fixture.queue.fetchval = lost_lease
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as raised:
        await fixture.service.prepare(envelope())
    assert raised.value.outcome_unknown
    request = next(iter(fixture.storage.requests_by_key.values()))
    assert request["state"] == "running" and not fixture.queue.expired
    assert "finish" not in fixture.events and len(fixture.prepared_by_capture) == 1
    busy = await fixture.service.prepare(envelope())
    assert busy["preparation"] is None
    assert fixture.preparation.prepare.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", [False, True])
async def test_source_change_after_preparation_keeps_intent_recoverable(monkeypatch, pending):
    fixture, _ = _pending_fixture(monkeypatch) if pending else (boundaries(monkeypatch), None)
    monkeypatch.setattr(
        coordinator, "_approved_source", AsyncMock(side_effect=[IDENTITY] * 4 + [(43, "changed_source")])
    )
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as raised:
        await fixture.service.prepare(envelope())
    assert raised.value.outcome_unknown
    assert "finish" not in fixture.events and "pending_lease_yield" not in fixture.events
    assert len(fixture.storage.requests_by_key) == len(fixture.prepared_by_capture) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("reason", "Another review"), ("retained_generation_id", 99)])
async def test_pending_preparation_must_match_original_review(monkeypatch, field, value):
    fixture, _ = _pending_fixture(monkeypatch)
    pending = fixture.preparation.prepare.side_effect

    async def changed(document, **kwargs):
        reply = await pending(document, **kwargs)
        reply["command"][field] = value
        return reply

    fixture.preparation.prepare.side_effect = changed
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as raised:
        await fixture.service.prepare(envelope())
    assert raised.value.outcome_unknown
    assert "finish" not in fixture.events and "pending_lease_yield" not in fixture.events
    assert len(fixture.storage.requests_by_key) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["capture", "state", "digest", "reason", "retained_generation_id"])
async def test_completion_refuses_changed_candidate_or_review_data(monkeypatch, change):
    fixture = boundaries(monkeypatch)
    prepare = fixture.preparation.prepare.side_effect

    async def changed(document, **kwargs):
        reply = await prepare(document, **kwargs)
        if change == "capture":
            reply["capture_id"] = "22222222-2222-4222-8222-222222222222"
        elif change == "state":
            reply["state"] = "loading"
        elif change == "digest":
            reply["command_sha256"] = "e" * 64
        else:
            reply["command"][change] = "Different review" if change == "reason" else 99
            reply["command_sha256"] = _digest(reply["command"])
        return reply

    fixture.preparation.prepare.side_effect = changed
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as raised:
        await fixture.service.prepare(envelope())
    assert raised.value.outcome_unknown and "finish" not in fixture.events
    assert len(fixture.storage.requests_by_key) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["operation", "handed_off"])
async def test_completed_intent_cannot_replay_a_different_result(monkeypatch, change):
    fixture = boundaries(monkeypatch)
    completed = await fixture.service.prepare(envelope())
    if change == "operation":
        next(iter(fixture.storage.requests_by_key.values()))["result_json"]["operation"] = "different_operation"
    else:
        fixture.prepared_by_capture[completed["preparation"]["capture_id"]]["state"] = "handed_off"
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as raised:
        await fixture.service.prepare(envelope())
    assert raised.value.outcome_unknown
    assert fixture.preparation.prepare.await_count == fixture.events.count("finish") == 1
    assert len(fixture.storage.requests_by_key) == len(fixture.prepared_by_capture) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["state", "candidate_id", "tables", "snapshot"])
async def test_cancellation_must_confirm_exact_candidate_before_drop(monkeypatch, changed):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)
    cancellation_by_field = {"handoff": copy.deepcopy(case.document), "state": "abandoned"}
    if changed == "state":
        cancellation_by_field["state"] = "published"
    elif changed == "candidate_id":
        cancellation_by_field["handoff"][changed] = "22222222-2222-4222-8222-222222222222"
    else:
        cancellation_by_field["handoff"][changed] = {}
    case.callback.side_effect = None
    case.callback.return_value = cancellation_by_field
    with pytest.raises(ValueError, match="retention_owner_invalid"):
        await case.invoke()
    case.driver.execute.assert_not_awaited()
    assert case.events[-1] == "rollback"


@pytest.mark.asyncio
async def test_approval_added_during_cancellation_prevents_drop(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)

    async def new_approval(driver, document):
        case.session.execute.return_value = SimpleNamespace(scalar_one=lambda: True)
        return {"handoff": document, "state": "abandoned"}

    case.callback.side_effect = new_approval
    with pytest.raises(ValueError, match="release_unavailable"):
        await case.invoke()
    case.callback.assert_awaited_once()
    case.driver.execute.assert_not_awaited()
    assert case.events[-1] == "rollback"


@pytest.mark.asyncio
async def test_manifest_replaced_before_drop_requires_rollback(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)
    changed_handoff_by_field = {**case.document, "snapshot": {**case.document["snapshot"], "input_row_count": 99}}
    monkeypatch.setattr(
        preparation, "load_named_office_preparation", AsyncMock(side_effect=[case.document, changed_handoff_by_field])
    )
    with pytest.raises(ValueError, match="manifest_changed"):
        await case.invoke()
    statements = [call.args[0] for call in case.driver.execute.await_args_list]
    assert len(statements) == 1 and statements[0].startswith("LOCK TABLE")
    assert case.events[-1] == "rollback"


@pytest.mark.asyncio
async def test_incomplete_candidate_cancellation_does_not_drop_tables(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)
    monkeypatch.setattr(preparation, "load_named_office_preparation", AsyncMock(return_value=None))
    case.callback.side_effect = None
    case.callback.return_value = {"handoff": case.document, "state": "abandoned"}
    assert (await case.invoke())["state"] == "abandoned"
    case.callback.assert_awaited_once_with(case.driver, None)
    case.driver.execute.assert_not_awaited()
    assert case.events[-1] == "commit"


@pytest.mark.asyncio
async def test_invalidation_failure_preserves_release_body_error(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared, failure="body")
    cleanup_error = PermissionError("synthetic invalidation failure")
    case.connection.invalidate.side_effect = cleanup_error
    with pytest.raises(RuntimeError) as raised:
        await case.invoke()
    assert raised.value is case.original and raised.value.__cause__ is cleanup_error
    assert case.events[-1] == "rollback"


@pytest.mark.asyncio
async def test_release_adapter_uses_configured_named_candidate(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    release = AsyncMock()
    monkeypatch.setitem(sys.modules, "directory_catalog_runtime", SimpleNamespace(release_registry_generation=release))
    store = RegistryPTGProducerScopeStore("scope_owner", "scope_approver", prepared.context.control_schema)
    connection = object()
    result = await retention.release_registry_ptg_office_capture(
        connection, store=store, context=prepared.context, request=prepared.request, descriptor=prepared.descriptor
    )
    actual_connection, configuration, candidate_id = release.await_args.args
    assert actual_connection is connection and candidate_id == str(prepared.request.capture_id)
    assert configuration["storage_schema"] == prepared.descriptor.schema_name
    assert configuration["preparation_roles"]["publisher_role"] == "capture_publisher"
    assert result == {
        "capture_id": candidate_id,
        "manifest_sha256": prepared.descriptor.manifest_sha256,
        "state": "released",
    }
    store = RegistryPTGProducerScopeStore("scope_owner", "scope_approver", "other_control")
    with pytest.raises(ValueError, match="retention_owner_invalid"):
        await retention.release_registry_ptg_office_capture(
            connection, store=store, context=prepared.context, request=prepared.request, descriptor=prepared.descriptor
        )
    release.assert_awaited_once()
