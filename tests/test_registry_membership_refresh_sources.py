# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source vector and context contracts; protocol doubles confer no native authority."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_membership_refresh_source_fence as fences
from process import registry_membership_refresh_sources as sources
from process import registry_refresh_anchor_service as anchors
from process.network_approved_membership_source import ApprovedMembershipSource
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from tests.test_network_fhir_membership_successor import _successor
from tests.test_registry_refresh_anchor_adapters import _binding
from tests.test_registry_refresh_anchor_contract import enrolled_scope
from tests.test_registry_source_recipe_store_postgres import _cms_epoch_recipe


def test_vector_uses_physical_capture_content_and_preserves_original_recipe():
    recipe = _cms_epoch_recipe()
    result = sources._result((recipe, recipe))
    assert result.recipes == (recipe,)
    assert result.accepted_sources == (
        {
            "source_id": "cms-npd",
            "capture_generation": recipe.source_pin.generation_id,
            "bindings_sha256": recipe.source_pin.retained_epoch.content_sha256,
            "transform_version": "fhir-source-facts-v1",
        },
    )
    successor = _successor(recipe.source_pin)
    wrapped = replace(recipe, source_pin=replace(recipe.source_pin, retained_successor=successor))
    current = sources._result((wrapped,))
    assert current.recipes == (wrapped,)
    assert current.accepted_sources[0]["capture_generation"] == successor.generation_id
    assert current.accepted_sources[0]["bindings_sha256"] == successor.retained_epoch.content_sha256
    assert current.accepted_sources != result.accepted_sources


def test_source_id_collision_refuses_distinct_physical_capture_without_ranking():
    recipe = _cms_epoch_recipe()
    successor = replace(recipe, source_pin=replace(recipe.source_pin, retained_successor=_successor(recipe.source_pin)))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_id_ambiguous"):
        sources._result((recipe, successor))


@pytest.mark.asyncio
async def test_caller_constructed_session_or_config_cannot_issue_source_pin():
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_context_unavailable"):
        async with sources.pin_registry_membership_refresh_sources(object(), [], approved_revision=0, readers=object()):
            raise AssertionError("unissued context yielded")


@pytest.fixture
async def source_factories():
    engines = []

    def factory(user="source_reader", database="refresh_contract"):
        engine = create_async_engine(f"postgresql+asyncpg://{user}@localhost:1/{database}", pool_size=1, max_overflow=0)
        engines.append(engine)
        return async_sessionmaker(engine, expire_on_commit=False)

    yield factory
    for engine in engines:
        assert engine.pool.checkedout() == 0
        await engine.dispose()


@pytest.mark.parametrize(
    "changes",
    [
        {"read_budget_limits": [1024, 1, 1]},
        {"read_budget_limits": (1024, 1)},
        {"cms_reader": {}},
        {"aca_store": {}},
        {"ptg_store": {}},
        {"head_fences": {}},
    ],
)
def test_source_reader_configuration_rejects_request_supplied_capabilities(source_factories, changes):
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="read_budget_invalid|source_reader_invalid"):
        sources.RegistryMembershipRefreshSourceReaders("control", source_factories(), **changes)


@pytest.mark.parametrize("family", ["aca", "generic"])
def test_source_readers_and_head_fences_cannot_share_a_connection_pool(source_factories, family):
    sessions = source_factories()
    head_fences = fences.RegistryMembershipRefreshHeadFences(**{family + "_sessions": sessions})
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_reader_invalid"):
        sources.RegistryMembershipRefreshSourceReaders("control", sessions, head_fences=head_fences)
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_fence_unavailable"):
        fences.RegistryMembershipRefreshHeadFences(aca_sessions=sessions, generic_sessions=sessions)


def test_generic_fence_requires_a_separate_principal_even_with_separate_pools(source_factories):
    reader, head = source_factories(), source_factories()
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_reader_invalid"):
        sources.RegistryMembershipRefreshSourceReaders(
            "control", reader, head_fences=fences.RegistryMembershipRefreshHeadFences(generic_sessions=head)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["shared", "different_database"])
async def test_source_snapshot_refuses_shared_pool_or_different_database(source_factories, target):
    outer = source_factories()
    child = outer if target == "shared" else source_factories(database="other_contract")
    async with outer() as session:
        with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_fence_unavailable"):
            fences._same_database(session, child)


def test_unretained_fhir_recipe_cannot_be_promoted_to_accepted_source():
    recipe = _cms_epoch_recipe()
    recipe = replace(recipe, source_pin=replace(recipe.source_pin, retained_epoch=None))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="fhir_capture_unavailable"):
        sources._result((recipe,))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_family_unavailable"):
        sources._accepted_identity(SimpleNamespace(source_pin={}))


@pytest.mark.asyncio
async def test_missing_family_reader_refuses_before_native_derivation(monkeypatch, source_factories):
    reader = sources.RegistryMembershipRefreshSourceReaders("control", source_factories())
    monkeypatch.setattr(sources, "native_driver", AsyncMock(return_value=object()))
    for system in ("fhir", "aca"):
        with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_reader_unconfigured"):
            await sources._source_batches(
                object(),
                object(),
                (_binding(system),),
                ApprovedMembershipSource(1, "a" * 64),
                reader,
                AsyncMock(),
                AsyncMock(),
                None,
            )
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="ptg_unconfigured"):
        await sources._ptg_recipes(object(), (_binding("ptg"),), None, reader, RegistryPTGGraphReadBudget(100000))


@pytest.mark.asyncio
async def test_ptg_partial_recipe_batch_cannot_issue_an_accepted_selection(monkeypatch):
    from process import registry_ptg_office_selection, registry_ptg_refresh_anchors

    binding = _binding("ptg")
    resolved = AsyncMock(return_value={binding.binding_id: {}})
    references = AsyncMock(return_value=())
    monkeypatch.setattr(registry_ptg_refresh_anchors, "resolve_ptg_refresh_anchors", resolved)
    monkeypatch.setattr(registry_ptg_office_selection, "_read_references", references)
    monkeypatch.setattr(sources, "native_driver", AsyncMock(return_value=object()))
    readers = SimpleNamespace(ptg_store=object(), control_schema="control")
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="ptg_incomplete"):
        await sources._ptg_recipes(object(), (binding,), None, readers, RegistryPTGGraphReadBudget(100000))
    resolved.assert_awaited_once()
    references.assert_awaited_once()


@pytest.mark.asyncio
async def test_busy_alias_fence_never_yields_or_unlocks_unowned_key(monkeypatch):
    connection = SimpleNamespace(
        execution_options=AsyncMock(), scalar=AsyncMock(side_effect=[7, False]), commit=AsyncMock()
    )

    @asynccontextmanager
    async def connected(_sessions):
        yield connection

    release = AsyncMock()
    monkeypatch.setattr(fences, "_company_approval_connection", connected)
    monkeypatch.setattr(fences, "_release_connection_fence", release)
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_fence_unavailable"):
        async with fences._alias_snapshot_connection(object()):
            pytest.fail("busy alias lock yielded a snapshot")
    connection.commit.assert_awaited_once()
    release.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("same_identity", [False, True])
async def test_aca_head_fence_refuses_foreign_database_or_missing_protected_owner(monkeypatch, same_identity):
    owner = AsyncMock()
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    identity = ("database", 7, "started")
    monkeypatch.setattr(fences, "protected_publisher_owner", owner)
    monkeypatch.setattr(
        fences,
        "_native_context",
        AsyncMock(return_value=(connection, identity if same_identity else ("other", 8, "started"))),
    )
    readers = SimpleNamespace(aca_store=SimpleNamespace(control_schema="control"))
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_fence_unavailable"):
        await fences._aca_lock_authority(object(), readers, identity)
    owner.assert_awaited_once()
    assert connection.fetchrow.await_count == int(same_identity)


@pytest.mark.asyncio
async def test_required_aca_fence_needs_an_installed_native_capability():
    readers = SimpleNamespace(aca_store=None, aca_head_fence_sessions=None)
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_fence_unavailable"):
        async with fences._aca_head_fence(readers, True, ("database", 7, "started")):
            pytest.fail("unconfigured ACA fence yielded")


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [None, True, "1"])
async def test_current_approval_requires_a_native_revision_before_pin_derivation(monkeypatch, revision):
    pin = AsyncMock()
    monkeypatch.setattr(anchors, "pin_approved_membership_source", pin)
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="approved_control_unavailable"):
        await anchors._current_pin(SimpleNamespace(fetchval=AsyncMock(return_value=revision)), 1, "control")
    pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction", [False, True])
async def test_binding_read_requires_typed_approval_and_a_live_transaction(transaction):
    driver = SimpleNamespace(is_in_transaction=lambda: transaction, fetchrow=AsyncMock())
    approved = {} if transaction else ApprovedMembershipSource(1, "a" * 64)
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="approved_pin_unavailable"):
        await anchors.read_approved_refresh_bindings(driver, approved, [enrolled_scope()], control_schema="control")
    driver.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_anchor_service_refuses_unconfigured_source_reader_and_budget(source_factories):
    sessions = source_factories()
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="read_budget_invalid"):
        anchors.RegistryRefreshAnchorService(sessions, "control", read_budget=[1024, 1, 1])
    service = anchors.RegistryRefreshAnchorService(sessions, "control")
    with pytest.raises(RegistryRefreshAnchorUnavailable, match="source_reader_unconfigured"):
        await service._read_source_anchors(
            (), ApprovedMembershipSource(1, "a" * 64), (), RegistryPTGGraphReadBudget(1024)
        )
