# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real office hold/drop control with existing bounded native-I/O fixtures."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_company_approval_fence as fence
from process import registry_ptg_office_capture as capture
from process import registry_ptg_office_retention as retention
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from tests import test_registry_ptg_office_witness as fixture


async def _verified(monkeypatch):
    prepared = fixture.arrange.__wrapped__(monkeypatch)()
    witness = await fixture._verify(prepared)
    monkeypatch.setattr(retention, "resolve_network_serving_manifest", capture.resolve_network_serving_manifest)
    prepared.driver.fetchval = AsyncMock(return_value='"synthetic_publisher"')
    return prepared, witness


@pytest.mark.asyncio
async def test_real_witness_hold_binds_all_original_identities(monkeypatch):
    prepared, witness = await _verified(monkeypatch)
    document = retention.office_hold_document(prepared.descriptor, witness, "a" * 64)
    assert document["approval_sha256"] == "a" * 64
    assert document["manifest_sha256"] == prepared.descriptor.manifest_sha256
    assert document["witness_sha256"] == capture._digest(witness.as_dict())
    assert document["custody"] == prepared.custody_by_field
    assert document["retained_serving"] == witness.as_dict()["retained_serving"]
    assert prepared.session.witness_calls[-1] == prepared.request.input_row_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["capture_id", "schema_name", "manifest_sha256", "command_sha256", "custody", "retained_serving"]
)
async def test_hold_refuses_witness_substitution(monkeypatch, changed):
    prepared, witness = await _verified(monkeypatch)
    evidence = witness.as_dict()
    evidence[changed] = {} if changed in ("custody", "retained_serving") else "changed"
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
    prepared.driver.execute = AsyncMock()
    prepared.session.execute = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: False))
    owner = AsyncMock(return_value=["approval", "hold"])
    monkeypatch.setattr(retention, "_owner_tables", owner)
    await retention._drop_unheld(prepared.session, object(), prepared.context, prepared.request, prepared.descriptor)
    sql_statements = [call.args[0] for call in prepared.driver.execute.await_args_list]
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in sql_statements[2]
    assert sql_statements[3].startswith("DROP TABLE ") and sql_statements[3].endswith(" RESTRICT")
    assert sql_statements[4].startswith("DROP SCHEMA ") and sql_statements[4].endswith(" RESTRICT")
    assert all("CASCADE" not in statement for statement in sql_statements)
    assert all("ptg2_snapshot_pin" not in statement for statement in sql_statements)
    assert prepared.session.execute.await_count == 2


@pytest.mark.asyncio
async def test_active_approval_blocks_before_family_or_ddl(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    prepared.driver.execute = AsyncMock()
    prepared.session.execute = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: True))
    monkeypatch.setattr(retention, "_owner_tables", AsyncMock(return_value=["approval", "hold"]))
    with pytest.raises(ValueError, match="release_unavailable"):
        await retention._drop_unheld(
            prepared.session, object(), prepared.context, prepared.request, prepared.descriptor
        )
    assert all(
        "DROP " not in str(call.args[0]) and "LOCK TABLE" not in str(call.args[0])
        for call in prepared.driver.execute.await_args_list
    )


@pytest.mark.asyncio
async def test_mismatched_native_custody_cannot_drop(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    prepared.driver.execute = AsyncMock()
    prepared.session.execute = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: False))
    monkeypatch.setattr(retention, "_owner_tables", AsyncMock(return_value=["approval", "hold"]))
    monkeypatch.setattr(capture, "_custody", AsyncMock(return_value={**prepared.custody_by_field, "table_oid": 777}))
    with pytest.raises(ValueError, match="custody_changed"):
        await retention._drop_unheld(
            prepared.session, object(), prepared.context, prepared.request, prepared.descriptor
        )
    assert all("DROP " not in str(call.args[0]) for call in prepared.driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "body", "cancel", "commit", "cleanup_after_body", "cleanup_after_cancel"])
async def test_release_owns_terminal_outcome_without_swallowing_cancel(monkeypatch, failure):
    prepared, _ = await _verified(monkeypatch)
    events = []
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: "original,pg_catalog")))
    original = (
        asyncio.CancelledError() if failure in ("cancel", "cleanup_after_cancel") else RuntimeError("synthetic failure")
    )

    @asynccontextmanager
    async def owner(sessions, *, control_schema, is_exclusive):
        assert is_exclusive is True and control_schema == prepared.context.control_schema
        events.append("exclusive_before_rr")
        try:
            yield session
        except BaseException:
            events.append("rollback")
            if failure in ("cleanup_after_body", "cleanup_after_cancel"):
                raise OSError("synthetic cleanup failure")
            raise
        else:
            events.append("commit")
            if failure == "commit":
                raise original

    async def drop(actual_session, *args):
        assert actual_session is session
        events.append("drop")
        if failure in ("body", "cancel", "cleanup_after_body", "cleanup_after_cancel"):
            raise original

    monkeypatch.setattr(retention, "registry_company_approval_transaction", owner)
    monkeypatch.setattr(retention, "_drop_unheld", drop)
    invoke = retention.release_registry_ptg_office_capture(
        object(), store=object(), context=prepared.context, request=prepared.request, descriptor=prepared.descriptor
    )
    if failure == "commit":
        with pytest.raises(retention.RegistryPTGOfficeReleaseOutcomeUnknown) as caught:
            await invoke
        assert caught.value.__cause__ is original
    elif failure:
        with pytest.raises(type(original)) as caught:
            await invoke
        assert caught.value is original and events[-1] == "rollback"
    else:
        reply = await invoke
        assert reply["state"] == "released" and events == ["exclusive_before_rr", "drop", "commit"]
        assert session.execute.await_args.args[1] == {"original_path": "original,pg_catalog"}


@pytest.mark.asyncio
async def test_cleanup_attests_original_owner_and_store_acl(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    checked = AsyncMock()
    exclusive = AsyncMock()
    monkeypatch.setattr(retention, "_check_roles", checked)
    monkeypatch.setattr(retention, "require_registry_company_approval_fence", exclusive)
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: True)))
    store = RegistryPTGProducerScopeStore(
        prepared.context.owner_role, "synthetic_approver", prepared.context.control_schema
    )
    tables = await retention._owner_tables(session, store, prepared.driver, prepared.context)
    assert len(tables) == 2 and exclusive.await_args.kwargs == {"is_exclusive": True}
    for call in session.execute.await_args_list:
        assert "r.rolname=:approval_role" in str(call.args[0])
        assert call.args[1]["approval_role"] == store.approval_role
    with pytest.raises(ValueError, match="owner_invalid"):
        await retention._owner_tables(
            session, replace(store, control_schema="other_control"), prepared.driver, prepared.context
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_phase", ["ACCESS EXCLUSIVE", "DROP TABLE", "DROP SCHEMA"])
async def test_native_reader_or_dependency_refusal_preserves_original_error(monkeypatch, blocked_phase):
    prepared, _ = await _verified(monkeypatch)
    original = PermissionError("synthetic native refusal")
    statements = []

    async def execute(statement):
        statements.append(statement)
        if blocked_phase in statement:
            raise original

    prepared.driver.execute = execute
    prepared.session.execute = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: False))
    monkeypatch.setattr(retention, "_owner_tables", AsyncMock(return_value=["approval", "hold"]))
    with pytest.raises(PermissionError) as caught:
        await retention._drop_unheld(
            prepared.session, object(), prepared.context, prepared.request, prepared.descriptor
        )
    assert caught.value is original
    if blocked_phase != "DROP SCHEMA":
        assert all("DROP SCHEMA" not in sql_statements for sql_statements in statements)
    if blocked_phase == "ACCESS EXCLUSIVE":
        assert all("DROP TABLE" not in sql_statements for sql_statements in statements)


@pytest.mark.asyncio
async def test_wrong_retained_serving_identity_cannot_assume_drop_owner(monkeypatch):
    prepared, _ = await _verified(monkeypatch)
    manifest = json.loads(prepared.descriptor.manifest_json)
    from process.network_serving_read import PinnedNetworkServingManifest

    serving = PinnedNetworkServingManifest(**manifest["command"]["retained_serving"])
    monkeypatch.setattr(
        retention, "resolve_network_serving_manifest", AsyncMock(return_value=replace(serving, address_table_oid=999))
    )
    prepared.driver.execute = AsyncMock()
    monkeypatch.setattr(retention, "_owner_tables", AsyncMock(return_value=["approval", "hold"]))
    with pytest.raises(ValueError, match="owner_invalid"):
        await retention._drop_unheld(
            prepared.session, object(), prepared.context, prepared.request, prepared.descriptor
        )
    prepared.driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_hold_input_bound_precedes_json_deserialization(monkeypatch):
    prepared, witness = await _verified(monkeypatch)
    oversized = type(witness)(b"x" * (capture.MAX_INPUT_BYTES + 1))

    def refuse_decode(self):
        raise AssertionError("oversized input decoded")

    monkeypatch.setattr(type(witness), "as_dict", refuse_decode)
    with pytest.raises(ValueError, match="retention_invalid"):
        retention.office_hold_document(prepared.descriptor, oversized, "a" * 64)
