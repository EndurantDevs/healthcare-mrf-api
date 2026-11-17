# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy control observations cannot outlive their native administrator scope."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_acquisition_custody as acquisition
from process import provider_directory_source_builder_delegation as delegation
from process import provider_directory_source_cohort_custody as cohort
from tests.test_provider_directory_source_builder_delegation import BUILDER, OWNER, _access, _receipt


def _policy():
    """Declare the same independent administrator boundary as production capture."""
    return SimpleNamespace(
        schema="sample", owner_role="owner", publisher_role="publisher", runtime_roles=("builder", "reader")
    )


def _connection():
    """Provide native metadata responses without replacing authority validation."""
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(
            return_value=dict(
                session="administrator",
                current="administrator",
                backend=1,
                xid="123",
                native_admin=True,
            )
        ),
        fetch=AsyncMock(return_value=[dict(oid=901)]),
    )


@pytest.mark.asyncio
async def test_final_graph_metadata_stays_credential_free():
    connection = _connection()
    graph_only = delegation.builder_delegation(_receipt())
    await delegation.require_legacy_observation(connection, _policy(), graph_only)
    connection.fetchrow.assert_not_awaited()
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_another_admin_connection_must_obtain_its_own_fresh_observation():
    connection, other = _connection(), _connection()
    proof = await delegation.issue_legacy_delegation(connection, _policy(), _receipt())
    with pytest.raises(ValueError, match="legacy_context_changed"):
        await delegation.require_legacy_observation(other, _policy(), proof, legacy=True)
    fresh = await delegation.issue_legacy_delegation(other, _policy(), _receipt())
    await delegation.require_legacy_observation(other, _policy(), fresh, legacy=True)


@pytest.mark.asyncio
async def test_native_issuer_allows_admin_and_refuses_runtime_actor():
    connection = _connection()
    proof = await delegation.issue_legacy_delegation(connection, _policy(), _receipt())
    await delegation.require_legacy_observation(connection, _policy(), proof, legacy=True)
    delegation.require_delegated_access(
        _access(_receipt()),
        proof,
        owner=OWNER,
        builder=BUILDER,
        relation_oid=901,
        legacy_builder=True,
    )
    connection.fetchrow.return_value = dict(
        session="builder",
        current="builder",
        backend=1,
        xid="123",
        native_admin=False,
    )
    connection.fetch.reset_mock()
    with pytest.raises(ValueError, match="independent_admin_required"):
        await delegation.issue_legacy_delegation(connection, _policy(), _receipt())
    connection.fetch.assert_not_awaited()
