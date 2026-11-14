# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Approved revision, bounded data and named snapshot read boundaries."""

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
@pytest.mark.parametrize("fault", ["schema", "revision", "transaction"])
async def test_retained_pin_rejects_invalid_logical_scope_before_io(fault):
    connection = SimpleNamespace(is_in_transaction=lambda: fault != "transaction", fetchrow=AsyncMock())
    with pytest.raises((approved.ApprovedMembershipSourceError, ValueError)):
        await approved.pin_retained_approved_membership_source(
            connection,
            approved_revision=-1 if fault == "revision" else 4,
            control_schema="invalid.schema" if fault == "schema" else "registry",
        )
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("valid", [False, None, 0, "true"])
async def test_retained_pin_rejects_inconsistent_published_edition_metadata(valid):
    edition_by_field = {**scope_row(), "edition_valid": valid}
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=edition_by_field))
    with pytest.raises(approved.ApprovedMembershipSourceError, match="published edition metadata"):
        await approved.pin_retained_approved_membership_source(
            connection, approved_revision=4, control_schema="registry"
        )


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
@pytest.mark.parametrize("owned", [False, True])
@pytest.mark.parametrize("fault", [None, "source_generation"])
async def test_api_pin_uses_named_relation_and_exact_approved_generation(monkeypatch, fault, owned):
    from process import network_serving_read as serving

    manifest = replace(_manifest(), source_generations={"custom_membership": "a" * 64})
    relation = CompactAddressRelation("registry", "registry_approved_record")
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(
            return_value=scope_row(
                revision=manifest.approved_custom_revision,
                fingerprint="b" * 64 if fault == "source_generation" else "a" * 64,
            )
        ),
    )

    async def pin():
        if owned:
            return await approved.resolve_api_retained_approved_membership_source(
                connection, generation_id=manifest.generation_id, control_schema="registry"
            )
        return await approved.pin_api_retained_approved_membership_source(
            connection, manifest, control_schema="registry"
        )

    if fault:
        with pytest.raises(approved.ApprovedMembershipSourceError, match="generation differs"):
            await pin()
    else:
        pinned = await pin()
        approved_source, selected_relation = pinned[-2:]
        assert approved_source == approved.ApprovedMembershipSource(manifest.approved_custom_revision, "a" * 64)
        assert selected_relation == relation
    connection.fetchrow.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [asyncpg.PostgresError("read unavailable"), ValueError("invalid catalog"), asyncio.CancelledError()]
)
@pytest.mark.parametrize("owned", [False, True])
async def test_api_pin_preserves_cancellation_and_closes_other_resolver_failures(monkeypatch, failure, owned):
    from process import network_serving_read as serving

    monkeypatch.setattr(serving, "resolve_network_serving_manifest", AsyncMock(side_effect=failure))
    expected = (
        asyncio.CancelledError
        if isinstance(failure, asyncio.CancelledError)
        else approved.ApprovedMembershipSourceError
    )
    with pytest.raises(expected) as rejected:
        if owned:
            await approved.resolve_api_retained_approved_membership_source(object(), control_schema="registry")
        else:
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


@pytest.mark.parametrize("generation", [None, 7])
async def test_owned_api_pin_resolves_once_before_approved_custody(monkeypatch, generation):
    from process import network_serving_read as serving

    connection, manifest, source, relation = object(), _manifest(), object(), object()
    resolve = AsyncMock(return_value=manifest)
    control_pin = AsyncMock(return_value=(source, relation))
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(approved, "_pin_verified_api_approved_source", control_pin)
    assert await approved.resolve_api_retained_approved_membership_source(
        connection, generation_id=generation, control_schema="registry"
    ) == (manifest, source, relation)
    resolve.assert_awaited_once_with(connection, generation_id=generation, control_schema="registry", api_read=True)
    control_pin.assert_awaited_once_with(connection, manifest, control_schema="registry")
