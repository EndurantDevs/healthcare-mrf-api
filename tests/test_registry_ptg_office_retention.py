# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real office hold/drop control with existing bounded native-I/O fixtures."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_office_capture as capture
from process import registry_ptg_office_retention as retention
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from tests import test_registry_ptg_office_witness as fixture


async def _verified(monkeypatch):
    prepared = fixture.arrange.__wrapped__(monkeypatch)()
    witness = await fixture._verify(prepared)
    monkeypatch.setattr(retention, "resolve_network_serving_manifest", capture.resolve_network_serving_manifest)
    return prepared, witness


def _release_sessions(driver, held):
    connection = SimpleNamespace(in_transaction=lambda: False, invalidate=AsyncMock())
    raw = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(
        connection=AsyncMock(return_value=raw), execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: held))
    )
    return connection, session


async def _release_case(monkeypatch, prepared, *, held=False, failure=None):
    """Model administrative IO while the real loader validates persisted office rows."""
    from process.registry_ptg_office_preparation import load_named_office_preparation

    events = []
    driver = prepared.driver
    document = await load_named_office_preparation(
        driver, str(prepared.request.capture_id), control_schema=prepared.context.control_schema
    )
    connection, session = _release_sessions(driver, held)
    original = (
        asyncio.CancelledError() if failure in {"cancel", "cleanup_after_cancel"} else RuntimeError("synthetic failure")
    )

    @asynccontextmanager
    async def owner(sessions, actual_connection, *, control_schema, is_exclusive):
        assert actual_connection is connection and is_exclusive and control_schema == prepared.context.control_schema
        events.append("exclusive_before_rr")
        try:
            yield session
        except BaseException:
            events.append("rollback")
            if failure in {"cleanup_after_body", "cleanup_after_cancel"}:
                raise OSError("synthetic cleanup failure")
            raise
        else:
            events.append("commit")
            if failure == "commit":
                raise original

    async def cancel(actual_driver, actual_document):
        assert actual_driver is driver and actual_document == document
        events.append("cancel")
        if failure in {"body", "cancel", "cleanup_after_body", "cleanup_after_cancel"}:
            raise original
        return {"handoff": document, "state": "abandoned"}

    driver.execute = AsyncMock()
    callback = AsyncMock(side_effect=cancel)
    monkeypatch.setattr(retention, "_company_approval_snapshot", owner)
    invoke = lambda: retention.release_named_office_preparation(
        connection,
        str(prepared.request.capture_id),
        control_schema=prepared.context.control_schema,
        storage_schema=prepared.descriptor.schema_name,
        cancel_preparation=callback,
    )
    return SimpleNamespace(
        invoke=invoke,
        driver=driver,
        session=session,
        connection=connection,
        callback=callback,
        document=document,
        events=events,
        original=original,
    )


@pytest.mark.asyncio
async def test_real_witness_hold_binds_all_original_identities(monkeypatch):
    prepared, witness = await _verified(monkeypatch)
    document = retention.office_hold_document(prepared.descriptor, witness, "a" * 64)
    assert document["approval_sha256"] == "a" * 64
    assert document["manifest_sha256"] == prepared.descriptor.manifest_sha256
    assert document["witness_sha256"] == capture._digest(witness.as_dict())
    assert document["storage"] == capture.descriptor_storage(prepared.descriptor)
    assert document["retained_serving"] == witness.as_dict()["retained_serving"]
    assert prepared.session.witness_calls[-1] == prepared.request.input_row_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["capture_id", "schema_name", "manifest_sha256", "command_sha256", "storage", "retained_serving"]
)
async def test_hold_refuses_witness_substitution(monkeypatch, changed):
    prepared, witness = await _verified(monkeypatch)
    evidence = witness.as_dict()
    evidence[changed] = {} if changed in ("storage", "retained_serving") else "changed"
    substituted = type(witness)(capture._canonical(evidence))
    with pytest.raises(ValueError, match="retention_invalid"):
        retention.office_hold_document(prepared.descriptor, substituted, "a" * 64)


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_hold_replay_is_exact_and_never_commits(monkeypatch, conflict):
    prepared, witness = await _verified(monkeypatch)
    document = retention.office_hold_document(prepared.descriptor, witness, "a" * 64)
    retained_by_field = {"approval_sha256": "a" * 64, "hold_sha256": capture._digest(document), "hold_json": document}
    if conflict:
        retained_by_field["approval_sha256"] = "b" * 64
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[None, SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [retained_by_field]))]
        )
    )
    protected = AsyncMock(return_value='"synthetic_control"."registry_ptg_office_hold"')
    monkeypatch.setattr(retention, "_protected_store", protected)
    if conflict:
        with pytest.raises(ValueError, match="retention_conflict"):
            await retention.retain_registry_ptg_office_capture(
                session, object(), prepared.descriptor, witness, "a" * 64
            )
    else:
        assert await retention.retain_registry_ptg_office_capture(
            session, object(), prepared.descriptor, witness, "a" * 64
        ) == capture._digest(document)
    assert protected.await_args.kwargs == {"write": True, "table_name": retention.TABLE}
    assert "ON CONFLICT DO NOTHING" in str(session.execute.await_args_list[0].args[0])
    assert "LIMIT 2" in str(session.execute.await_args_list[1].args[0])
    assert not hasattr(session, "commit")


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [True, None, 1])
async def test_any_held_or_uncertain_obligation_refuses_release(held):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: held)))
    with pytest.raises(ValueError, match="release_unavailable"):
        await retention._require_unheld(session, ["approval", "hold"], "11111111-1111-4111-8111-111111111111")


@pytest.mark.asyncio
async def test_exact_unapproved_drop_consumes_guard_and_restrict(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)
    result = await case.invoke()
    assert result == {"preparation_id": str(prepared.request.capture_id), "state": "abandoned"}
    statements = [call.args[0] for call in case.driver.execute.await_args_list]
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in statements[0]
    assert statements[1].startswith("DROP TABLE ") and statements[1].endswith(" RESTRICT")
    assert statements[2].startswith("DROP SCHEMA ") and statements[2].endswith(" RESTRICT")
    assert all("CASCADE" not in query and "GRANT" not in query and "SET ROLE" not in query for query in statements)
    assert case.events == ["exclusive_before_rr", "cancel", "commit"]
    case.callback.assert_awaited_once()
    case.connection.invalidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_active_approval_blocks_before_family_or_ddl(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared, held=True)
    with pytest.raises(ValueError, match="release_unavailable"):
        await case.invoke()
    case.callback.assert_not_awaited()
    case.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_named_handoff_cannot_drop(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)
    case.callback.side_effect = None
    case.callback.return_value = {"handoff": {**case.document, "storage_schema": "other_capture"}, "state": "abandoned"}
    with pytest.raises(ValueError, match="owner_invalid"):
        await case.invoke()
    case.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "body", "cancel", "commit", "cleanup_after_body", "cleanup_after_cancel"])
async def test_release_owns_terminal_outcome_without_swallowing_cancel(monkeypatch, failure):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared, failure=failure)
    if failure == "commit":
        with pytest.raises(retention.RegistryPTGOfficeReleaseOutcomeUnknown) as raised:
            await case.invoke()
        assert raised.value.__cause__ is case.original
    elif failure:
        with pytest.raises(type(case.original)) as raised:
            await case.invoke()
        assert raised.value is case.original and case.events[-1] == "rollback"
    else:
        assert (await case.invoke())["state"] == "abandoned"
        assert case.events[-1] == "commit"


@pytest.mark.asyncio
async def test_public_release_uses_existing_admin_operator_and_configured_roles(monkeypatch):
    try:
        import directory_catalog_runtime
    except ModuleNotFoundError:
        prepared, _ = await _verified(monkeypatch)
        store = RegistryPTGProducerScopeStore("scope_owner", "scope_approver", prepared.context.control_schema)
        with pytest.raises(ModuleNotFoundError):
            await retention.release_registry_ptg_office_capture(
                object(),
                store=store,
                context=prepared.context,
                request=prepared.request,
                descriptor=prepared.descriptor,
            )
        return

    prepared, _ = await _verified(monkeypatch)
    operator = AsyncMock(return_value={"preparation_id": str(prepared.request.capture_id), "state": "abandoned"})
    monkeypatch.setattr(directory_catalog_runtime, "release_registry_generation", operator)
    connection = object()
    store = RegistryPTGProducerScopeStore("scope_owner", "scope_approver", prepared.context.control_schema)
    result = await retention.release_registry_ptg_office_capture(
        connection, store=store, context=prepared.context, request=prepared.request, descriptor=prepared.descriptor
    )
    assert result["state"] == "released"
    actual_connection, config, candidate_id = operator.await_args.args
    assert actual_connection is connection and candidate_id == str(prepared.request.capture_id)
    assert config["kind"] == "office" and config["storage_schema"] == prepared.descriptor.schema_name
    assert config["preparation_roles"]["publisher_role"] == "capture_publisher"


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_phase", ["ACCESS EXCLUSIVE", "DROP TABLE", "DROP SCHEMA"])
async def test_native_reader_or_dependency_refusal_preserves_original_error(monkeypatch, blocked_phase):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)
    original = PermissionError("synthetic native refusal")
    statements = []

    async def execute(statement):
        statements.append(statement)
        if blocked_phase in statement:
            raise original

    case.driver.execute.side_effect = execute
    with pytest.raises(PermissionError) as raised:
        await case.invoke()
    assert raised.value is original
    if blocked_phase != "DROP SCHEMA":
        assert not any("DROP SCHEMA" in query for query in statements)
    if blocked_phase == "ACCESS EXCLUSIVE":
        assert not any("DROP TABLE" in query for query in statements)


@pytest.mark.asyncio
async def test_wrong_retained_serving_identity_cannot_assume_drop_owner(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    case = await _release_case(monkeypatch, prepared)
    from process.network_serving_read import PinnedNetworkServingManifest

    serving = PinnedNetworkServingManifest(
        **json.loads(prepared.descriptor.manifest_json)["command"]["retained_serving"]
    )
    monkeypatch.setattr(
        retention,
        "resolve_network_serving_manifest",
        AsyncMock(
            return_value=replace(
                serving, address_relation={"schema_name": "other_serving", "table_name": "entity_address_unified"}
            )
        ),
    )
    with pytest.raises(ValueError, match="owner_invalid"):
        await case.invoke()
    case.callback.assert_not_awaited()
    case.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_hold_input_bound_precedes_json_deserialization(monkeypatch):
    prepared, witness = await _verified(monkeypatch)
    oversized = type(witness)(b"x" * (capture.MAX_INPUT_BYTES + 1))

    def refuse_decode(self):
        raise AssertionError("oversized input decoded")

    monkeypatch.setattr(type(witness), "as_dict", refuse_decode)
    with pytest.raises(ValueError, match="retention_invalid"):
        retention.office_hold_document(prepared.descriptor, oversized, "a" * 64)


@pytest.mark.asyncio
@pytest.mark.parametrize("state,active", [("queued", True), ("running", True), ("completed", False), ("unknown", None)])
async def test_release_refuses_active_or_uncertain_core_intent(state, active):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: active)))
    if active is False:
        await retention._require_idle_intent(session, "synthetic_control", "11111111-1111-4111-8111-111111111111")
    else:
        with pytest.raises(ValueError, match="release_unavailable"):
            await retention._require_idle_intent(session, "synthetic_control", "11111111-1111-4111-8111-111111111111")
    statement, parameters = session.execute.await_args.args
    assert "state IN ('queued','running')" in str(statement)
    assert parameters["operation"] == "prepare_ptg_office_capture"
