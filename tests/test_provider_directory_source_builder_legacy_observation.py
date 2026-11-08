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


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["acquisition", "cohort"])
@pytest.mark.parametrize(
    "fault",
    [
        "unissued",
        "graph_only",
        "connection",
        "transaction",
        "backend",
        "task",
        "locks",
        "runtime",
        "extra_oid",
        "ended",
    ],
)
async def test_direct_capture_rejects_invalid_legacy_scope_before_storage(monkeypatch, kind, fault):
    connection, receipt = _connection(), _receipt()
    proof = await delegation.issue_legacy_delegation(connection, _policy(), receipt)
    match fault:
        case "unissued":
            proof = delegation.builder_delegation(receipt, locked_relations=frozenset({901}))
        case "graph_only":
            proof = delegation.builder_delegation(receipt)
        case "connection":
            connection = _connection()
        case "transaction" | "backend" | "runtime":
            actor_by_field = dict(connection.fetchrow.return_value)
            changes_by_fault = {
                "transaction": {"xid": "124"},
                "backend": {"backend": 2},
                "runtime": {"session": "builder", "current": "builder", "native_admin": False},
            }
            actor_by_field.update(changes_by_fault[fault])
            connection.fetchrow.return_value = actor_by_field
        case "locks":
            connection.fetch.return_value = []
        case "extra_oid":
            proof = replace(proof, locked_relations=frozenset({901, 902}))
        case "ended":
            connection.is_in_transaction = lambda: False
    first_read = AsyncMock(side_effect=AssertionError("storage must not be read"))
    monkeypatch.setattr(
        acquisition, "_native_family" if kind == "acquisition" else "_capture_family_relation", first_read
    )
    capture = acquisition.capture_source_acquisition_custody if kind == "acquisition" else cohort.capture_cohort_family
    with pytest.raises(ValueError, match="legacy_issuer|legacy_context|legacy_locks|independent_admin"):
        operation = capture(connection, _policy(), receipt, legacy=True, delegation=proof)
        await asyncio.create_task(operation) if fault == "task" else await operation
    first_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_acquisition_checks_scope_after_its_last_storage_await(monkeypatch):
    connection, receipt = _connection(), _receipt()
    proof = await delegation.issue_legacy_delegation(connection, _policy(), receipt)
    monkeypatch.setattr(acquisition, "_native_family", AsyncMock(return_value=({}, [], [], {})))
    monkeypatch.setattr(cohort, "capture_cohort_family", AsyncMock(return_value=dict(relations={})))
    monkeypatch.setattr(acquisition, "_capture_family_foreign", AsyncMock(return_value=[]))

    async def expire_context(*_arguments):
        connection.fetch.return_value = []
        return {}

    monkeypatch.setattr(acquisition, "_capture_dataset_plan", expire_context)
    with pytest.raises(ValueError, match="legacy_locks_changed"):
        await acquisition.capture_source_acquisition_custody(
            connection,
            _policy(),
            receipt,
            legacy=True,
            delegation=proof,
        )
    acquisition._capture_family_foreign.assert_awaited_once()


@pytest.mark.asyncio
async def test_cohort_checks_scope_after_its_last_child_await(monkeypatch):
    connection, receipt = _connection(), _receipt()
    proof = await delegation.issue_legacy_delegation(connection, _policy(), receipt)

    async def header(*_arguments, **_options):
        return dict(catalog=dict(classes=[dict(relname=cohort.HEADER, relowner=str(BUILDER), oid=901)]))

    async def member(_connection, _policy, name, *_arguments, **_options):
        return dict(catalog=dict(classes=[dict(relname=name, relowner=str(BUILDER), oid=902)]))

    async def expire_context(*_arguments):
        connection.fetch.return_value = []
        return {}

    async def native_reads(query):
        return [dict(oid=901)] if "FROM pg_locks" in query else []

    connection.fetch.side_effect = native_reads
    monkeypatch.setattr(acquisition, "_capture_family_relation", header)
    monkeypatch.setattr(cohort, "_capture_member", member)
    monkeypatch.setattr(cohort, "_require_cohort_partition", AsyncMock())
    monkeypatch.setattr(cohort, "_capture_cohort_children", expire_context)

    async def final_children(*_arguments):
        connection.fetch.side_effect = None
        return await expire_context()

    monkeypatch.setattr(cohort, "_capture_cohort_children", final_children)
    with pytest.raises(ValueError, match="legacy_locks_changed"):
        await cohort.capture_cohort_family(connection, _policy(), receipt, legacy=True, delegation=proof)
    cohort._require_cohort_partition.assert_awaited_once()
