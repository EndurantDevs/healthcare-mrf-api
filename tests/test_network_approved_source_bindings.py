# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained binding maps keep full validation without requiring the current head."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import network_approved_source_bindings as bindings
from process import registry_published_plan_binding_refs as published
from process.network_approved_membership_source import ApprovedMembershipSource

COORDINATES = bindings.RegistryNetworkSourceCoordinates(
    "aca", "source-example", "retained_source", "dataset-example", "producer-example", "edition-one"
)


def scope_row(revision=4, fingerprint="a" * 64):
    return dict(
        approved_revision=revision,
        isolation="repeatable read",
        is_bounded=True,
        invalid_documents=0,
        unresolved_rows=0,
        generation_id=fingerprint,
        total_rows=0,
    )


def boundary(*, current_revision=7, fingerprint="a" * 64, invalid=False, published_json=None):
    calls = []

    async def fetchrow(sql, *parameters):
        calls.append((sql, parameters))
        if len(parameters) == 3:
            revision = current_revision if "registry_revision_control" in sql else parameters[0]
            return scope_row(revision, fingerprint)
        assert len(parameters) == 8 and parameters[1:7] == COORDINATES.sql_parameters
        return dict(
            invalid=invalid,
            binding_count=2,
            published_count=int(published_json is not None),
            published_json=published_json,
        )

    return SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(side_effect=fetchrow)), calls


@pytest.mark.asyncio
async def test_retained_map_uses_exact_revision_after_current_approval_advances():
    connection, calls = boundary()
    source = ApprovedMembershipSource(4, "a" * 64)
    assert (
        await bindings.require_retained_approved_network_source_bindings(
            connection, source, COORDINATES, control_schema="sample_control"
        )
        == 2
    )
    assert len(calls) == 2 and "registry_revision_control" not in calls[0][0]
    assert "SELECT $1::bigint AS approved_revision,1 AS id" in calls[0][0]
    assert "invalid_scopes" in calls[1][0] and "duplicate_digests" in calls[1][0]
    assert calls[1][1][:7] == (4, *COORDINATES.sql_parameters)
    calls.clear()
    with pytest.raises(bindings.ApprovedNetworkSourceBindingError, match="unavailable"):
        await bindings.require_approved_network_source_bindings(
            connection, source, COORDINATES, control_schema="sample_control"
        )
    assert len(calls) == 1 and "registry_revision_control" in calls[0][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [False, True])
async def test_current_and_retained_share_identical_scope_validation(retained):
    connection, calls = boundary(current_revision=4)
    verifier = (
        bindings.require_retained_approved_network_source_bindings
        if retained
        else bindings.require_approved_network_source_bindings
    )
    assert (
        await verifier(connection, ApprovedMembershipSource(4, "a" * 64), COORDINATES, control_schema="sample_control")
        == 2
    )
    assert calls[1][0] == bindings._VALIDATION_SQL.format(
        bindings=bindings.APPROVED_NETWORK_BINDINGS_SQL.format(namespace='"sample_control"'),
        namespace='"sample_control"',
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["fingerprint", "scope", "transaction", "document", "bounds", "native"])
async def test_retained_map_refuses_changed_or_incomplete_native_proof(damage):
    connection, calls = boundary(
        fingerprint="b" * 64 if damage == "fingerprint" else "a" * 64, invalid=damage == "scope"
    )
    if damage == "transaction":
        connection.is_in_transaction = lambda: False
    if damage in {"document", "bounds"}:
        fields_map = scope_row()
        fields_map["invalid_documents" if damage == "document" else "is_bounded"] = 1 if damage == "document" else False
        connection.fetchrow.side_effect = [fields_map]
    if damage == "native":
        connection.fetchrow.side_effect = asyncpg.PostgresError("native source unavailable")
    with pytest.raises(bindings.ApprovedNetworkSourceBindingError):
        await bindings.require_retained_approved_network_source_bindings(
            connection, ApprovedMembershipSource(4, "a" * 64), COORDINATES, control_schema="sample_control"
        )
    if damage == "fingerprint":
        assert len(calls) == 1
    if damage == "transaction":
        connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["none", "changed", "cancel"])
async def test_retained_map_keeps_published_reference_guard_and_cancellation(monkeypatch, failure):
    connection, calls = boundary(published_json='[{"source":"published"}]')
    store = object()
    verifier = AsyncMock()
    if failure != "none":
        verifier.side_effect = asyncio.CancelledError() if failure == "cancel" else ValueError("changed reference")
    monkeypatch.setattr(published, "require_published_plan_binding_references", verifier)
    arguments = (connection, ApprovedMembershipSource(4, "a" * 64), COORDINATES)
    options_by_name = dict(control_schema="sample_control", published_plan_store=store)
    if failure == "none":
        assert await bindings.require_retained_approved_network_source_bindings(*arguments, **options_by_name) == 2
    else:
        with pytest.raises(
            asyncio.CancelledError if failure == "cancel" else bindings.ApprovedNetworkSourceBindingError
        ):
            await bindings.require_retained_approved_network_source_bindings(*arguments, **options_by_name)
    verifier.assert_awaited_once_with(connection, '"sample_control"', b'[{"source":"published"}]', store)
    assert len(calls) == 2
