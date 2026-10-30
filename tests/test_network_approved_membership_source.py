# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure approved-source boundaries; native custody remains independently verified."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import network_approved_membership_source as approved
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_approved_source_bindings import scope_row
from tests.test_network_raw_provider_api_identity import _manifest


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["source", "transaction"])
async def test_command_reader_requires_the_typed_pin_in_its_caller_transaction(fault):
    connection = SimpleNamespace(is_in_transaction=lambda: fault != "transaction", fetchrow=AsyncMock())
    source = approved.ApprovedMembershipSource(4, "a" * 64)
    with pytest.raises(approved.ApprovedMembershipSourceError, match="caller transaction"):
        await approved.read_approved_membership_commands(
            connection, {} if fault == "source" else source, control_schema="registry"
        )
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "revision", "isolation"])
async def test_command_seek_refuses_a_changed_approval_snapshot_before_decoding(monkeypatch, fault):
    page_by_field = {
        "approved_revision": 5 if fault == "revision" else 4,
        "isolation": "read committed" if fault == "isolation" else "repeatable read",
        "record_key": "7",
        "command_count": 0,
        "input_json": "[]",
    }
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=None if fault == "missing" else page_by_field)
    )
    partition = AsyncMock()
    monkeypatch.setattr(approved, "partition_network_membership_commands", partition)
    with pytest.raises(approved.ApprovedMembershipSourceError, match="transaction differs"):
        await approved.read_approved_membership_commands(
            connection, approved.ApprovedMembershipSource(4, "a" * 64), after_network_key="3", control_schema="registry"
        )
    assert connection.fetchrow.await_args.args[1:3] == (4, "3")
    partition.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "read_identity",
    [
        (),
        (object(), 41),
        (CompactAddressRelation("other_registry", "registry_approved_record", 42), 41),
        (CompactAddressRelation("registry", "network_membership", 42), 41),
    ],
)
async def test_retained_pin_cannot_borrow_an_unrelated_consumed_relation(read_identity):
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=scope_row()))
    with pytest.raises(approved.ApprovedMembershipSourceError, match="read identity"):
        await approved.pin_retained_approved_membership_source(
            connection, approved_revision=4, control_schema="registry", read_identity=read_identity
        )
    connection.fetchrow.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["untyped", "changed"])
async def test_api_pin_re_resolves_the_exact_manifest_before_reading_control_pointers(monkeypatch, fault):
    from process import network_candidate_custody as custody
    from process import network_serving_read as serving

    manifest = _manifest()
    resolve = AsyncMock(return_value=replace(manifest, manifest_sha256="b" * 64))
    pointers = AsyncMock()
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(custody, "read_network_control_relation_pins", pointers)
    with pytest.raises(approved.ApprovedMembershipSourceError, match="manifest"):
        await approved.pin_api_retained_approved_membership_source(
            object(), {} if fault == "untyped" else manifest, control_schema="registry"
        )
    pointers.assert_not_awaited()
    if fault == "changed":
        assert resolve.await_args.kwargs == {
            "generation_id": manifest.generation_id,
            "control_schema": "registry",
            "api_read": True,
        }
    else:
        resolve.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["physical_pointer", "source_generation"])
async def test_api_pin_rejects_consumed_heap_or_approved_generation_drift(monkeypatch, fault):
    from process import network_candidate_custody as custody
    from process import network_serving_read as serving

    manifest = replace(_manifest(), source_generations={"custom_membership": "a" * 64})
    relation = CompactAddressRelation("registry", "registry_approved_record", 42)
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    monkeypatch.setattr(custody, "read_network_control_relation_pins", AsyncMock(return_value=(relation,)))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        get_server_pid=lambda: 41,
        fetchrow=AsyncMock(
            side_effect=[
                scope_row(
                    revision=manifest.approved_custom_revision,
                    fingerprint="b" * 64 if fault == "source_generation" else "a" * 64,
                ),
                {"backend_matches": True, "relation_count": 1, "valid": fault != "physical_pointer"},
            ]
        ),
    )
    with pytest.raises(approved.ApprovedMembershipSourceError, match="prerequisites|generation differs"):
        await approved.pin_api_retained_approved_membership_source(connection, manifest, control_schema="registry")
    assert connection.fetchrow.await_count == 2
    assert connection.fetchrow.await_args.args[1:4] == (['"registry"."registry_approved_record"'], [42], 41)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [asyncpg.PostgresError("read unavailable"), ValueError("invalid catalog"), asyncio.CancelledError()]
)
async def test_api_pin_preserves_cancellation_and_closes_other_resolver_failures(monkeypatch, failure):
    from process import network_serving_read as serving

    monkeypatch.setattr(serving, "resolve_network_serving_manifest", AsyncMock(side_effect=failure))
    expected = (
        asyncio.CancelledError
        if isinstance(failure, asyncio.CancelledError)
        else approved.ApprovedMembershipSourceError
    )
    with pytest.raises(expected) as rejected:
        await approved.pin_api_retained_approved_membership_source(object(), _manifest(), control_schema="registry")
    if isinstance(failure, asyncio.CancelledError):
        assert rejected.value is failure
    else:
        assert rejected.value.__cause__ is failure


@pytest.mark.asyncio
async def test_page_reader_refuses_an_untyped_source_before_any_query():
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(approved.ApprovedMembershipSourceError, match="Exact approved"):
        await approved.read_approved_membership_batch(connection, {}, control_schema="registry")
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_page_failure_retains_its_native_cause_without_returning_partial_bytes():
    source = approved.ApprovedMembershipSource(4, "a" * 64)
    error = asyncpg.QueryCanceledError("bounded read timed out")
    scope_by_field = {**scope_row(), "command_documents": 0}
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=AsyncMock(side_effect=[scope_by_field, error])
    )
    with pytest.raises(approved.ApprovedMembershipSourceError, match="page is unavailable") as rejected:
        await approved.read_approved_membership_batch(connection, source, control_schema="registry")
    assert rejected.value.__cause__ is error and connection.fetchrow.await_count == 2
