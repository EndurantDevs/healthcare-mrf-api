# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Final refresh fences compare complete registered recipes and live leases."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_membership_refresh_execution as refresh
from process import registry_publication_execution as publication
from process.network_address_projection import PinnedAddressSource
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_membership_refresh_execution import _require_prepared_recipes
from process.registry_membership_refresh_queue import RegistryMembershipRefreshAddressProfile
from process.registry_publication_consumer import _lease_authority
from tests.test_registry_source_recipe_store_postgres import _candidate, _cms_epoch_recipe


def recipe_for(index=0):
    recipe = _cms_epoch_recipe()
    return replace(
        recipe,
        source_pin=replace(
            recipe.source_pin, retained_epoch=replace(recipe.source_pin.retained_epoch, content_sha256=str(index) * 64)
        ),
    )


def source_inputs(index=0):
    return RegistryCompositionAddressSources(
        PinnedAddressSource("address_example", "entity_address_unified", "a" * 64),
        initial_source_recipes=(recipe_for(index),),
        compact=True,
    )


def test_final_source_fence_compares_whole_recipe_documents():
    original = source_inputs()
    _require_prepared_recipes(_candidate((recipe_for(),)), original, original)
    with pytest.raises(RegistryApprovalConflict, match="candidate_recipes_changed"):
        _require_prepared_recipes(_candidate((recipe_for(1),)), original, original)
    with pytest.raises(RegistryApprovalConflict, match="sources_changed"):
        _require_prepared_recipes(_candidate((recipe_for(),)), original, source_inputs(1))
    with pytest.raises(RegistryApprovalConflict, match="sources_changed"):
        _require_prepared_recipes(
            _candidate((recipe_for(),)),
            original,
            replace(original, base_source=replace(original.base_source, generation_id="b" * 64)),
        )


@pytest.mark.asyncio
async def test_commit_authority_never_outlives_a_lost_lease():
    stopped = asyncio.Event()
    authorize = AsyncMock(return_value={"policy_revision": 1})
    stopped.set()
    with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
        await _lease_authority(authorize, stopped, {"actor": {}, "command": {}})
    authorize.assert_not_awaited()
    stopped.clear()

    async def revoked_during_http(**_request):
        stopped.set()
        return {"policy_revision": 2}

    with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
        await _lease_authority(revoked_during_http, stopped, {"actor": {}, "command": {}})


@pytest.mark.asyncio
async def test_source_workers_recover_compact_base_without_changing_legacy_inputs(monkeypatch):
    original = source_inputs()
    connection = object()
    pin = SimpleNamespace(schema_name="retained", manifest_sha256="b" * 64, compact_layout=None)
    recover = AsyncMock(return_value=original)
    monkeypatch.setattr(publication, "_source_rollback_address_sources", recover)
    legacy = await publication.source_worker_address_sources(connection, pin, "registry_example")
    assert legacy.base_source == PinnedAddressSource("retained", "entity_address_unified", "b" * 64)
    assert legacy.compact is False
    recover.assert_not_awaited()
    pin.compact_layout = object()
    assert await publication.source_worker_address_sources(connection, pin, "registry_example") is original
    recover.assert_awaited_once_with(connection, pin, "registry_example")


def _runtime(authorize):
    return refresh.RegistryMembershipRefreshRuntime(
        authorize, RegistryMembershipRefreshAddressProfile("addresses", ("reader",)), AsyncMock(), AsyncMock()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["authority", "profile", "resolve", "pin"])
async def test_refresh_runtime_rejects_missing_host_capabilities(fault):
    from tests.test_registry_membership_refresh_authority import client

    async with client(lambda _request: pytest.fail("configuration attempted HTTP")) as authorize:
        runtime = _runtime(authorize)
        changes = {
            "authority": {"authorize": {}},
            "profile": {"address_profile": {}},
            "resolve": {"resolve_sources": None},
            "pin": {"pin_sources": None},
        }[fault]
        with pytest.raises(ValueError, match="runtime_invalid"):
            replace(runtime, **changes)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["untyped", "noncompact", "replacement_on_initial"])
async def test_fresh_sources_cannot_change_phase_or_drop_compact_custody(monkeypatch, fault):
    from tests.test_registry_membership_refresh_authority import client
    from tests.test_registry_membership_refresh_observations import published_rows

    _, record, _, _ = published_rows()
    sources = source_inputs()
    if fault == "noncompact":
        sources = replace(sources, compact=False)
    elif fault == "replacement_on_initial":
        sources = replace(sources, initial_source_recipes=(), replacement_source_recipes=sources.initial_source_recipes)
    monkeypatch.setattr(refresh, "_current_heads", AsyncMock(return_value={}))
    monkeypatch.setattr(refresh, "_require_address", AsyncMock())
    authorize = AsyncMock()
    async with client(lambda _request: pytest.fail("unexpected transport")) as configured:
        runtime = replace(
            _runtime(configured), resolve_sources=AsyncMock(return_value={} if fault == "untyped" else sources)
        )
        with pytest.raises(ValueError, match="refresh_sources_invalid|refresh_source_phase_invalid"):
            await refresh._fresh_inputs(object(), record, runtime, "registry", authorize)
    authorize.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "head", "address"])
async def test_final_candidate_must_keep_exact_approval_head_and_address_epoch(fault):
    from process.registry_candidate_composition import _target
    from tests.test_registry_membership_refresh_contract import command

    selected = command()
    target = _target(UUID(int=1), selected["expected_approved_revision"], selected["expected_serving_generation"], {})
    candidate_by_field = {
        "approved_custom_revision": selected["expected_approved_revision"],
        "expected_head": selected["expected_serving_generation"],
        "source_generations": {"network_address_origin": selected["address_artifact_sha256"]},
    }
    if fault == "head":
        candidate_by_field["expected_head"] += 1
    elif fault == "address":
        candidate_by_field["source_generations"]["network_address_origin"] = "f" * 64
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=None if fault == "missing" else candidate_by_field))
    with pytest.raises(RegistryApprovalConflict, match="candidate_changed|address_changed"):
        await refresh._candidate_scope(driver, target, selected, "registry")
    assert driver.fetchrow.await_args.args[1] == UUID(target.candidate_id)


@pytest.mark.asyncio
async def test_refresh_refuses_nested_execution_transaction_before_touching_request():
    from tests.test_registry_membership_refresh_authority import client

    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock())
    async with client(lambda _request: pytest.fail("unexpected transport")) as authorize:
        with pytest.raises(ValueError, match="runtime_invalid"):
            await refresh.complete_queued_membership_refresh(
                connection,
                {},
                runtime=_runtime(authorize),
                writer_context=None,
                authorize=AsyncMock(),
                authorize_commit=AsyncMock(),
            )
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_locked_request_stops_before_candidate_locks_or_final_grant(monkeypatch):
    events = []

    @asynccontextmanager
    async def locked(*_arguments):
        try:
            yield {"request_sha256": "b" * 64}
        finally:
            events.append("released")

    monkeypatch.setattr(refresh, "_locked_request_transaction", locked)
    controls = AsyncMock()
    monkeypatch.setattr(refresh, "_lock_publication_controls", controls)
    final_authority = AsyncMock()
    with pytest.raises(RegistryApprovalConflict, match="queue_request_changed"):
        await refresh._finish_refresh(
            object(),
            (UUID(int=1), UUID(int=2), {"request_sha256": "a" * 64}),
            ({"command": {}}, None, None),
            None,
            SimpleNamespace(control_schema="registry"),
            final_authority,
        )
    assert events == ["released"]
    controls.assert_not_awaited()
    final_authority.assert_not_awaited()
