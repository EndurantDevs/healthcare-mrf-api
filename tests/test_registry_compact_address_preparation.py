# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Component checks for compact preparation; doubles confer no native Source authority."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_compact_address_origin as origin
from process import registry_candidate_composition as composition
from process import registry_composition_addresses as addresses
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["complete", "origin_refused", "actor_refused", "npi_refused", "cancel"])
async def test_compact_preparation_proves_reader_intent_and_all_actors_without_cloning(monkeypatch, outcome):
    """Require the complete actor census and original Reader intent without cloning addresses."""
    base = PinnedAddressSource("address_input", "entity_address_unified", "a" * 64)
    npi = PinnedAddressSource("npi_input", "npi_address", "b" * 64)
    source_descriptors = composition.RegistryCompositionAddressSources(base, npi_source=npi, compact=True)
    readers = ("api_reader", "loader")
    actors = ("api_reader", "custodian", "executor", "loader", "worker_owner")
    custody = composition.RegistryAddressArtifactCustody("custody_owner", actors, "registry", readers)
    physical = CompactAddressRelation("retained_address", "entity_address_unified", 42)
    actor_by_field = dict(login="custodian", actor="custodian", rolcanlogin=True)
    actor_by_field.update(
        dict.fromkeys(("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"), False)
    )
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=actor_by_field))
    approved = SimpleNamespace(total_rows=1, approved_revision=3, generation_id="c" * 64)
    events = []

    async def resolve(*args, **options):
        events.append("origin")
        assert args == (connection, {"source_generations": {"unified_address": base.generation_id}}, base)
        assert options == {"runtime_roles": readers, "control_schema": "registry"}
        if outcome == "origin_refused":
            raise ValueError("origin_refused")
        return {}, physical

    async def close_actors(actual, dependency, roles):
        events.append("actors")
        assert actual is connection and roles == actors
        assert dependency == PinnedAddressSource(physical.schema_name, physical.table_name, base.generation_id)
        if outcome == "actor_refused":
            raise ValueError("actor_refused")
        if outcome == "cancel":
            raise asyncio.CancelledError

    async def npi_dependency(actual, dependency, roles):
        events.append("npi")
        assert actual is connection and dependency is npi and roles == actors
        if outcome == "npi_refused":
            raise ValueError("npi_refused")

    monkeypatch.setattr(addresses, "_require_transaction", AsyncMock())
    monkeypatch.setattr(addresses, "pin_approved_membership_source", AsyncMock(return_value=approved))
    selection = AsyncMock(return_value=((), ()))
    monkeypatch.setattr(composition, "_composition_source_recipes", selection)
    monkeypatch.setattr(origin, "resolve_compact_address_origin", resolve)
    monkeypatch.setattr(addresses, "_dependency_actors_closed", close_actors)
    monkeypatch.setattr(addresses, "_artifact_dependency_identity", npi_dependency)
    clone = AsyncMock(side_effect=AssertionError("compact_preparation_must_not_clone"))
    monkeypatch.setattr(addresses, "prepare_recipe_address_source", clone)
    operation = _compact_operation(connection, source_descriptors, custody)
    if outcome == "complete":
        assert await operation is None
    else:
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else ValueError):
            await operation
    _assert_compact_events(events, outcome, selection, clone)


@pytest.mark.asyncio
async def test_compact_preparation_requires_explicit_trusted_reader_intent(monkeypatch):
    base = PinnedAddressSource("address_input", "entity_address_unified", "a" * 64)
    sources = composition.RegistryCompositionAddressSources(base, compact=True)
    custody = composition.RegistryAddressArtifactCustody("custody_owner", ("api_reader",), "registry")
    resolve = AsyncMock(side_effect=AssertionError("reader_intent_missing"))
    monkeypatch.setattr(origin, "resolve_compact_address_origin", resolve)
    with pytest.raises(composition.RegistryCompositionError):
        await composition._verify_compact_preparation_origin(object(), sources, None, custody)
    resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_aca_ptg_custody_keeps_qualified_factory_without_unsupported_preparer(monkeypatch):
    import json
    from contextlib import AsyncExitStack
    from dataclasses import fields, replace
    from unittest.mock import Mock

    from process import registry_aca_source_worker as aca
    from process import registry_publication_worker as worker
    from tests.test_registry_aca_source_worker import configuration

    configured = aca.parse_registry_aca_source_worker_config(json.dumps(configuration()))
    configured = replace(configured, custody_url=configured.execution_url.set(username="custodian"))
    sessions = Mock(return_value=object())
    verifier = AsyncMock()
    monkeypatch.setattr(worker, "_worker_sessions", sessions)
    async with AsyncExitStack() as stack:
        consumer = worker._ptg_custody_runtime(configured, stack, configured.consumer, {}, backend_verifier=verifier)
        sessions.assert_called_once_with(configured, stack, configured.custody_url, backend_verifier=verifier)
    assert callable(consumer.seal_candidate)
    assert "prepare_address_sources" not in {field.name for field in fields(consumer)}


def _assert_compact_events(events, outcome, selection, clone):
    """Preserve exact origin, actor and NPI check order and forbid cloning."""
    assert (
        events
        == {
            "complete": ["origin", "actors", "npi"],
            "origin_refused": ["origin"],
            "actor_refused": ["origin", "actors"],
            "npi_refused": ["origin", "actors", "npi"],
            "cancel": ["origin", "actors"],
        }[outcome]
    )
    assert selection.await_args.kwargs["inventory"] is True
    clone.assert_not_awaited()


def _compact_operation(connection, address_sources, custody):
    """Keep the complete compact-preparation request fixed across all refusal cases."""
    return composition.prepare_registry_composition_address_artifact(
        connection,
        request_id=UUID(int=1),
        approved_revision=3,
        address_sources=address_sources,
        source_manifest=None,
        expected_head=0,
        custody=custody,
    )


@pytest.mark.parametrize(
    "options",
    [
        {"npi_source": object()},
        {"prepared_address_artifact": object()},
        {"initial_source_recipes": [], "compact": True},
        {"replacement_source_recipes": []},
    ],
)
def test_address_source_contract_rejects_untyped_dependencies(options):
    base = PinnedAddressSource("address_input", "entity_address_unified", "a" * 64)
    with pytest.raises(addresses.RegistryCompositionError):
        composition.RegistryCompositionAddressSources(base, **options)


@pytest.mark.parametrize("readers", [("outside_reader",), ("api_reader", "outside_reader")])
def test_address_reader_intent_cannot_expand_the_fixed_runtime_census(readers):
    with pytest.raises(addresses.RegistryCompositionError, match="roles_differ"):
        composition.RegistryAddressArtifactCustody("custodian", ("api_reader",), "registry", readers)


def test_address_recipe_phase_rejects_untyped_or_mismatched_retained_manifest():
    from tests.test_network_read_execution import _manifest

    sources = composition.RegistryCompositionAddressSources(
        PinnedAddressSource("different_address", "entity_address_unified", "a" * 64)
    )
    with pytest.raises(addresses.RegistryCompositionError, match="source_invalid"):
        addresses._require_source_recipe_phase(sources, object(), 1)
    with pytest.raises(addresses.RegistryCompositionError, match="source_address_mismatch"):
        addresses._require_source_recipe_phase(sources, _manifest(), 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["inventory", "office_policy_context"])
async def test_reused_address_artifact_refuses_changed_storage_contract(monkeypatch, changed):
    stage = SimpleNamespace(
        inventory=changed == "inventory", office_policy_context={} if changed == "office_policy_context" else None
    )
    replay = AsyncMock(side_effect=AssertionError("changed contract reached office replay"))
    monkeypatch.setattr(addresses, "_composition_office_summary", replay)
    with pytest.raises(addresses.RegistryCompositionError, match="address_artifact_changed"):
        await addresses._reuse_composition_artifact(object(), object(), object(), (), stage, 1, "registry")
    replay.assert_not_awaited()


@pytest.mark.asyncio
async def test_reuse_still_replays_complete_office_context_before_return(monkeypatch):
    from tests.test_registry_ptg_address_carry import _artifact

    artifact = _artifact()
    inputs = SimpleNamespace(
        connection=object(), approved=object(), office_context=object(), retained_fhir_reads={}, source_manifest=None
    )
    stage = SimpleNamespace(
        inventory=False, office_policy_context=None, office_matches=object(), contribution_revision=2
    )
    summary_by_field = {"correspondence_sha256": "a" * 64}
    replay = AsyncMock(return_value=summary_by_field)
    monkeypatch.setattr(addresses, "_composition_office_summary", replay)
    result = await addresses._reuse_composition_artifact(inputs, object(), artifact, (), stage, 1, "registry")
    assert result == (artifact, summary_by_field)
    assert replay.await_args.args[-1] == ((), inputs.retained_fhir_reads, stage.office_matches, None, 2, 1)
    assert replay.await_args.args[2] is inputs.approved
