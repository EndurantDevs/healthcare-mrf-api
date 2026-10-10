# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual approval/service dispatch with mocked physical and live-authority boundaries."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_registry_ptg_scope as routes
from process import registry_ptg_office_approval as approval
from process import registry_ptg_office_capture as capture
from process import registry_ptg_published_office_scope as published
from process import registry_ptg_scope_engine as engine
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_ptg_office_review_contract import validated_office_review_command
from tests.test_registry_ptg_office_capture import _request
from tests.test_registry_ptg_published_office_capture import _fixture
from tests.test_registry_ptg_scope_engine import _actor, _service


def _command():
    _, context, document_by_field = _fixture()
    scope_by_field = published._office_scope(document_by_field, context.graph_identity, [0])
    request = _request()
    serving = PinnedNetworkServingManifest(
        3, "33333333-3333-4333-8333-333333333333", "synthetic_serving", 1, {}, 3, "d" * 64, 42
    )
    accounting_by_field = {
        "canonical_input_sha256": request.canonical_input_sha256,
        "input_row_count": request.input_row_count,
        "retained_site_identity": {"generation_id": 3},
        "retained_site_rows_sha256": "e" * 64,
    }
    return capture.office_review_command(request, scope_by_field, serving, accounting_by_field)


def _envelope():
    return {
        "actor": _actor(),
        "session_token_sha256": "f" * 64,
        "command": _command(),
        "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 32, "maximum_coordinates": 100},
    }


@pytest.mark.parametrize(
    "change",
    [
        {"company_key": "invented"},
        {"cohort_id": "invented"},
        {"scope_id": "22222222-2222-4222-8222-222222222222"},
        {"approval_sha256": "a" * 64},
        {"selection_mode": "partial"},
    ],
)
def test_closed_office_scope_refuses_changed_namespace(change):
    command_by_field = _command()
    command_by_field["source"]["source_scope"].update(change)
    with pytest.raises(ValueError):
        validated_office_review_command(command_by_field)


def test_actual_control_body_closes_office_envelope():
    envelope = _envelope()
    request = SimpleNamespace(query_string="", args={}, body=json.dumps(envelope).encode())
    assert routes._body(request, "approve_offices") == approval.validated_office_envelope(envelope)


@pytest.mark.parametrize(
    "limits",
    [
        {"maximum_bytes": True, "maximum_pages": 32, "maximum_coordinates": 100},
        {"maximum_bytes": 0, "maximum_pages": 32, "maximum_coordinates": 100},
    ],
)
def test_read_hints_never_widen_native_budget(limits):
    envelope = _envelope()
    envelope["read_limits"] = limits
    with pytest.raises(ValueError):
        approval.validated_office_envelope(envelope)


def _bind_office_hold(monkeypatch, session, service, events):
    from process import registry_ptg_office_retention as retention

    async def retain_office(actual_session, store, descriptor, witness, approval_sha256):
        assert actual_session is session and store is service.store and approval_sha256 == "a" * 64
        events.append("office_hold")
        return "b" * 64

    monkeypatch.setattr(retention, "retain_registry_ptg_office_capture", retain_office)


def _boundaries(monkeypatch, *, physical_error=None, authority_error=None, commit_error=None):
    events = []
    service, _ = _service()
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: "original,pg_catalog")))

    @asynccontextmanager
    async def owned(sessions, *, control_schema):
        events.append("fence")
        try:
            yield session
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")
            if commit_error:
                raise commit_error

    async def physical(actual_session, *args):
        assert actual_session is session
        events.append("physical")
        if physical_error:
            raise physical_error
        return SimpleNamespace(manifest_sha256="c" * 64), SimpleNamespace(
            as_dict=lambda: {"command_sha256": capture._digest(_command())}
        )

    async def authorize(self, envelope, *, deadline):
        assert set(envelope) == {"actor", "session_token_sha256", "command"}
        events.append("authorize")
        if authority_error:
            raise authority_error
        return {"policy_revision": 1}

    async def retain(actual_session, table, document_by_field):
        assert actual_session is session and document_by_field["command"] == _command()
        events.append("retain")
        return "a" * 64

    monkeypatch.setattr(approval, "registry_company_approval_transaction", owned)
    monkeypatch.setattr(
        approval, "_protected_store", AsyncMock(return_value='"synthetic_control"."registry_ptg_office_approval"')
    )
    monkeypatch.setattr(approval, "_physical_review", physical)
    monkeypatch.setattr(approval, "_retain", retain)

    async def pin_source(actual_session, schema_name, command_by_field):
        assert actual_session is session
        events.append("source_pin")
        return command_by_field["source"]["evidence"]["source_authority"]

    monkeypatch.setattr(approval, "_pin_source", pin_source)
    _bind_office_hold(monkeypatch, session, service, events)
    monkeypatch.setattr(engine.RegistryPTGScopeAuthorityClient, "authorize", authorize)
    return service, session, events


@pytest.mark.asyncio
async def test_actual_service_orders_witness_auth_append_and_owner_commit(monkeypatch):
    service, session, events = _boundaries(monkeypatch)
    reply = await service.approve_offices(_envelope())
    assert reply["state"] == "reviewed" and events == [
        "fence",
        "physical",
        "authorize",
        "source_pin",
        "retain",
        "office_hold",
        "commit",
    ]
    assert str(session.execute.await_args_list[0].args[0]) == "SELECT pg_catalog.current_setting('search_path')"
    assert session.execute.await_args.kwargs == {} and session.execute.await_args.args[1] == {
        "original_path": "original,pg_catalog"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["physical", "authority"])
async def test_original_failure_rolls_back_before_append(monkeypatch, phase):
    failure = PermissionError("synthetic refusal")
    service, _, events = _boundaries(
        monkeypatch,
        physical_error=failure if phase == "physical" else None,
        authority_error=failure if phase == "authority" else None,
    )
    with pytest.raises(PermissionError) as caught:
        await service.approve_offices(_envelope())
    assert caught.value is failure and events[-1] == "rollback" and "retain" not in events


@pytest.mark.asyncio
async def test_cancellation_and_uncertain_commit_never_fabricate_approval(monkeypatch):
    cancelled = asyncio.CancelledError()
    service, _, events = _boundaries(monkeypatch, physical_error=cancelled)
    with pytest.raises(asyncio.CancelledError) as caught:
        await service.approve_offices(_envelope())
    assert caught.value is cancelled and events[-1] == "rollback"
    service, _, uncertain_events = _boundaries(monkeypatch, commit_error=RuntimeError("synthetic commit loss"))
    with pytest.raises(engine.RegistryPTGScopeDeadlineExpired) as caught:
        await service.approve_offices(_envelope())
    assert caught.value.outcome_unknown is True
    assert uncertain_events[-2:] == ["office_hold", "commit"]


@pytest.mark.asyncio
async def test_existing_control_route_consumes_installed_method(monkeypatch):
    service, _, events = _boundaries(monkeypatch)
    request = SimpleNamespace(
        query_string="",
        args={},
        body=json.dumps(_envelope()).encode(),
        headers={},
        app=SimpleNamespace(ctx=SimpleNamespace(registry_ptg_scope_engine=service)),
    )
    monkeypatch.setattr(routes, "require_control_auth", lambda request: None)
    result = await routes.approve_registry_ptg_offices(request)
    assert result.status == 200 and json.loads(result.body)["state"] == "reviewed"
    assert "retain" in events


@pytest.mark.asyncio
async def test_physical_reader_reuses_complete_witness_and_original_limits(monkeypatch):
    session = object()
    context = object()
    command_by_field = _command()
    descriptor = SimpleNamespace(manifest_json=json.dumps({"command": command_by_field}).encode())
    reader = AsyncMock(
        return_value=SimpleNamespace(as_dict=lambda: {"command_sha256": capture._digest(command_by_field)})
    )
    monkeypatch.setattr(approval, "_context", AsyncMock(return_value=context))
    monkeypatch.setattr(approval, "_descriptor", AsyncMock(return_value=descriptor))
    monkeypatch.setattr(approval, "verify_registry_ptg_office_witness", reader)
    limits_by_field = _envelope()["read_limits"]
    assert (await approval._physical_review(session, object(), command_by_field, limits_by_field, "synthetic_control"))[
        0
    ] is descriptor
    assert reader.await_args.args[0] is session and reader.await_args.args[1] is context
    budget = reader.await_args.kwargs["read_budget"]
    assert budget.maximum_bytes == limits_by_field["maximum_bytes"] and budget.read_bytes == 0
    descriptor.manifest_json = json.dumps({"command": {**command_by_field, "client_id": "other"}}).encode()
    with pytest.raises(ValueError, match="command_changed"):
        await approval._physical_review(session, object(), command_by_field, limits_by_field, "synthetic_control")
    assert reader.await_count == 1


@pytest.mark.asyncio
async def test_write_role_proof_precedes_original_published_reader(monkeypatch):
    from process import registry_ptg_published_plan_scope as source

    session = object()
    protected = AsyncMock(return_value='"synthetic_control"."registry_ptg_published_plan_scope"')
    reader = AsyncMock(return_value={"verified": True})
    monkeypatch.setattr(approval, "_protected_store", protected)
    monkeypatch.setattr(source, "_read_protected_published_scope", reader)
    await approval._approved_source_review(
        session, scope_id="scope", client_id="client", approval_sha256="a" * 64, store=object()
    )
    assert protected.await_args.kwargs == {"write": True, "table_name": "registry_ptg_published_plan_scope"}
    assert reader.await_args.args[:2] == (session, protected.return_value)
    protected.side_effect = PermissionError("synthetic role mismatch")
    with pytest.raises(PermissionError):
        await approval._approved_source_review(
            session, scope_id="scope", client_id="client", approval_sha256="a" * 64, store=object()
        )
    assert reader.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("row_count", [0, 1, 2])
async def test_exact_whole_approval_replay_is_all_or_conflict(row_count):
    document_by_field = {"actor": _actor(), "command": _command()}
    stored_reviews = [
        {"approval_sha256": capture._digest(document_by_field), "approval_json": document_by_field}
    ] * row_count
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[None, SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: stored_reviews))]
        )
    )
    if row_count == 1:
        assert await approval._retain(
            session, '"synthetic_control"."registry_ptg_office_approval"', document_by_field
        ) == capture._digest(document_by_field)
    else:
        with pytest.raises(ValueError, match="idempotency_conflict"):
            await approval._retain(session, '"synthetic_control"."registry_ptg_office_approval"', document_by_field)
    assert "ON CONFLICT DO NOTHING" in str(session.execute.await_args_list[0].args[0])
    assert "LIMIT 2" in str(session.execute.await_args_list[1].args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("is_changed", [False, True])
async def test_independent_office_source_pin_requires_original_identity(monkeypatch, is_changed):
    from process.ptg_parts import result_archive_published_authority as published_source
    from process.ptg_parts import result_archive_source_authority as source_writer

    command_by_field = _command()
    identity_by_field = dict(command_by_field["source"]["evidence"]["source_authority"]["identity"])
    if is_changed:
        identity_by_field["snapshot_key"] += 1
    operation_id = "registry_ptg_office_review_" + command_by_field["capture_id"].replace("-", "")
    source_pin = published_source.PtgPublishedResultSourceAuthority(operation_id, identity_by_field)
    prepared = AsyncMock(return_value=source_pin)
    committed = AsyncMock(return_value=source_pin)
    monkeypatch.setattr(published_source, "prepare_ptg_published_result_source_authority", prepared)
    monkeypatch.setattr(source_writer, "commit_ptg_result_archive_source_authority", committed)
    session = object()
    if is_changed:
        with pytest.raises(ValueError, match="source_changed"):
            await approval._pin_source(session, "synthetic_ptg", command_by_field)
        assert committed.await_count == 0
    else:
        assert await approval._pin_source(session, "synthetic_ptg", command_by_field) == source_pin.as_dict()
        assert (
            committed.await_args.args[0] is session and committed.await_args.kwargs["authority"] == source_pin.as_dict()
        )
    assert prepared.await_args.kwargs["operation_id"] == operation_id


@pytest.mark.asyncio
async def test_approval_context_reaches_real_complete_witness(monkeypatch):
    from process import registry_ptg_office_witness as witness
    from tests import test_registry_ptg_office_witness as fixture

    prepared = fixture.arrange.__wrapped__(monkeypatch)()
    context = approval.RegistryPTGOfficeApprovalContext(**vars(prepared.context))
    command_by_field = json.loads(prepared.descriptor.manifest_json)["command"]
    monkeypatch.setattr(approval, "_context", AsyncMock(return_value=context))
    monkeypatch.setattr(approval, "_descriptor", AsyncMock(return_value=prepared.descriptor))
    # Only native I/O/codec boundaries use the existing fixture. The verifier is real.
    assert approval.verify_registry_ptg_office_witness is witness.verify_registry_ptg_office_witness
    descriptor, evidence = await approval._physical_review(
        prepared.session, object(), command_by_field, _envelope()["read_limits"], "synthetic_control"
    )
    assert descriptor is prepared.descriptor
    assert evidence.as_dict()["command_sha256"] == capture._digest(command_by_field)
    assert evidence.as_dict()["accounting"]["input_row_count"] == prepared.request.input_row_count
    assert prepared.driver.calls and prepared.session.witness_calls[-1] == prepared.request.input_row_count
    with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="input_invalid"):
        capture._validated(prepared.request, context)


def test_witness_context_allowance_preserves_exact_types_and_request_guards(monkeypatch):
    from dataclasses import dataclass, replace

    from process import registry_ptg_office_witness as witness
    from tests import test_registry_ptg_office_witness as fixture

    prepared = fixture.arrange.__wrapped__(monkeypatch)()
    context = approval.RegistryPTGOfficeApprovalContext(**vars(prepared.context))
    assert witness._descriptor(prepared.descriptor, prepared.request, context)[0] == prepared.descriptor.schema_name
    assert (
        witness._descriptor(prepared.descriptor, prepared.request, prepared.context)[0]
        == prepared.descriptor.schema_name
    )

    @dataclass(frozen=True)
    class OtherApprovalContext(approval.RegistryPTGOfficeApprovalContext):
        pass

    for invalid in (OtherApprovalContext(**vars(context)), SimpleNamespace(**vars(context))):
        with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="input_invalid"):
            witness._descriptor(prepared.descriptor, prepared.request, invalid)
    with pytest.raises(capture.RegistryPTGOfficeCaptureError, match="input_invalid"):
        witness._descriptor(prepared.descriptor, replace(prepared.request, input_row_count=True), context)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_cancelled", [False, True])
async def test_hold_failure_rolls_back_original_approval_and_source_pin(monkeypatch, is_cancelled):
    from process import registry_ptg_office_retention as retention

    service, _, events = _boundaries(monkeypatch)
    original = asyncio.CancelledError() if is_cancelled else ValueError("synthetic hold refusal")

    async def fail_hold(*args):
        events.append("office_hold_failure")
        raise original

    monkeypatch.setattr(retention, "retain_registry_ptg_office_capture", fail_hold)
    with pytest.raises(type(original)) as caught:
        await service.approve_offices(_envelope())
    assert caught.value is original
    assert events[-3:] == ["retain", "office_hold_failure", "rollback"]
    assert "source_pin" in events and "commit" not in events


@pytest.mark.asyncio
async def test_committed_office_reply_reaches_the_real_recipe_decoder(monkeypatch):
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    service, _, events = _boundaries(monkeypatch)
    command_by_field = _command()
    reply = await service.approve_offices(_envelope())
    recipes = decode_registry_source_recipes(reply["source_recipes"])
    assert events[-1] == "commit" and len(recipes) == 1
    pin = recipes[0].source_pin
    assert (pin.capture_id, pin.client_id, pin.network_id, pin.retained_generation_id) == (
        command_by_field["capture_id"],
        command_by_field["client_id"],
        command_by_field["source"]["network_id"],
        command_by_field["retained_generation_id"],
    )
    assert (pin.approval_sha256, pin.hold_sha256, pin.manifest_sha256) == ("a" * 64, "b" * 64, "c" * 64)
    assert reply["command_sha256"] == capture._digest(command_by_field)
    assert reply["source_recipes_sha256"] == capture._digest(json.loads(canonical_registry_source_recipes(recipes)))


@pytest.mark.asyncio
async def test_missing_durable_hold_never_issues_recipe(monkeypatch):
    from process import registry_ptg_office_retention as retention

    service, _, events = _boundaries(monkeypatch)
    monkeypatch.setattr(retention, "retain_registry_ptg_office_capture", AsyncMock(return_value=None))
    with pytest.raises(ValueError):
        await service.approve_offices(_envelope())
    assert events[-1] == "rollback" and "commit" not in events
