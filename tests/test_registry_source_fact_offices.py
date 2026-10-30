# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound native fact traversal and preserve its savepoint across cancellation."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import registry_source_fact_offices as offices
from process.registry_source_contribution_artifacts import ResolvedSourceFactArtifact, SourceFactArtifactReference
from tests.test_registry_source_contributions import _descriptor, _recipe


def _scope():
    recipe = _recipe()
    artifact = ResolvedSourceFactArtifact(
        SourceFactArtifactReference(str(uuid4()), 42, "a" * 64), "synthetic_fact_owner", _descriptor(recipe)
    )
    context = offices.FactOfficeContext(SimpleNamespace(approved_revision=7), object(), ("reader",), "control")
    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    connection = SimpleNamespace(transaction=lambda: transaction, execute=AsyncMock(), terminate=Mock())
    return recipe, artifact, context, connection, transaction


@pytest.mark.asyncio
async def test_one_ordered_native_cursor_closes_before_savepoint_commit():
    recipe, artifact, context, connection, transaction = _scope()
    async with offices._pair_cursor(connection, recipe, artifact, context) as name:
        assert name.startswith("registry_fact_offices_")
        transaction.commit.assert_not_awaited()
    declaration, close = connection.execute.await_args_list
    assert declaration.args[0].startswith('DECLARE "registry_fact_offices_')
    assert " NO SCROLL CURSOR FOR " in declaration.args[0]
    assert declaration.args[0].count("SELECT DISTINCT") == 1
    assert "omission_reason' IS NULL" in declaration.args[0]
    assert "source_scope_json'=fact_json->'source_scope_json'" in declaration.args[0]
    assert "ORDER BY provider_system,provider_id,location_id" in declaration.args[0]
    assert 'COLLATE "C"' in declaration.args[0]
    assert close.args[0] == f'CLOSE "{name}"'
    transaction.start.assert_awaited_once()
    transaction.commit.assert_awaited_once()
    transaction.rollback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["declare", "body", "close"])
async def test_cursor_failure_rolls_back_only_its_owned_savepoint(phase):
    recipe, artifact, context, connection, transaction = _scope()
    if phase == "declare":
        connection.execute.side_effect = RuntimeError("native failure")
    elif phase == "close":
        connection.execute.side_effect = [None, RuntimeError("native failure")]
    with pytest.raises(RuntimeError, match="native failure"):
        async with offices._pair_cursor(connection, recipe, artifact, context):
            if phase == "body":
                raise RuntimeError("native failure")
    transaction.rollback.assert_awaited_once()
    transaction.commit.assert_not_awaited()
    connection.terminate.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["start", "commit"])
async def test_cancellation_drains_start_and_commit_before_deciding_cleanup(phase):
    recipe, artifact, context, connection, transaction = _scope()
    entered, proceed = asyncio.Event(), asyncio.Event()

    async def pending():
        entered.set()
        await proceed.wait()

    getattr(transaction, phase).side_effect = pending

    async def use():
        async with offices._pair_cursor(connection, recipe, artifact, context):
            assert transaction.start.await_count == 1

    task = asyncio.create_task(use())
    await entered.wait()
    task.cancel()
    proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    if phase == "start":
        transaction.rollback.assert_awaited_once()
        connection.execute.assert_not_awaited()
    else:
        transaction.rollback.assert_not_awaited()
        assert connection.execute.await_args.args[0].startswith("CLOSE ")


def test_pair_pages_enforce_row_and_encoded_byte_bounds(monkeypatch):
    pair_by_field = {"provider_system": "npi", "provider_id": "1000000491", "location_id": uuid4()}
    assert str(pair_by_field["location_id"]) in offices._pair_documents([pair_by_field])
    with pytest.raises(ValueError, match="pair_bound_exceeded"):
        offices._pair_documents([pair_by_field] * (offices.MAX_PAIR_ROWS + 1))
    monkeypatch.setattr(offices, "MAX_PAIR_BYTES", 10)
    with pytest.raises(ValueError, match="pair_bound_exceeded"):
        offices._pair_documents([pair_by_field])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["bad_seal", "wrong_oid", "changed_fact_hash", "missing_witness"])
async def test_fact_authentication_failure_never_starts_pair_cursor(monkeypatch, failure):
    recipe, artifact, context, connection, transaction = _scope()
    verify = AsyncMock(side_effect=ValueError(failure))
    monkeypatch.setattr(offices, "verify_anchored_source_fact_contents", verify)
    with pytest.raises(ValueError, match=failure):
        await offices.capture_fact_office_pages(connection, recipe, artifact, context)
    transaction.start.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_current_mapping_and_full_source_admission_precede_bounded_fact_pages(monkeypatch):
    from process import registry_source_recipe_composition as custody

    recipe, artifact, context, connection, transaction = _scope()
    identity = artifact.descriptor["recipe_sha256"]
    monkeypatch.setattr(offices, "verify_anchored_source_fact_contents", AsyncMock(return_value={identity: artifact}))
    mapping = AsyncMock(return_value="b" * 64)
    monkeypatch.setattr(offices, "source_mapping_sha256", mapping)
    admission = AsyncMock(return_value=object())
    monkeypatch.setattr(custody, "_require_recipe_custody", admission)
    monkeypatch.setattr(
        offices,
        "resolve_source_office_origin",
        AsyncMock(return_value=SimpleNamespace(address_identity=(1, 2, 3, "c" * 64))),
    )
    pair_by_field = {"provider_system": "npi", "provider_id": "1000000491", "location_id": uuid4()}
    connection.fetch = AsyncMock(side_effect=[[pair_by_field], [pair_by_field], []])
    matcher = AsyncMock(return_value=([{"selected_office": "d" * 64}], [("npi", "1000000491")], (1, 2, 3, "c" * 64)))
    monkeypatch.setattr(offices, "_match_pair_page", matcher)
    sink = AsyncMock()
    context = offices.FactOfficeContext(
        context.approved, context.address_source, context.runtime_roles, context.control_schema, page_sink=sink
    )
    document = await offices.capture_fact_office_pages(connection, recipe, artifact, context)
    assert (document["mode"], document["office_rows"], document["page_count"]) == ("facts", 2, 2)
    assert document["mapping_sha256"] == "b" * 64
    assert document["fact_artifact"]["fact_table_oid"] == artifact.reference.fact_table_oid
    admission.assert_awaited_once()
    mapping.assert_awaited_once()
    assert sink.await_count == matcher.await_count == 2
    assert all(call.args[0].startswith("FETCH FORWARD 1000 FROM ") for call in connection.fetch.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("conflicting", [False, True])
async def test_copy_keeps_coherent_aliases_and_rejects_conflicting_ownership(monkeypatch, conflicting):
    from process import network_initial_cms_office_bindings as cms

    copy = AsyncMock()
    monkeypatch.setattr(cms, "_copy_bindings", copy)
    target = SimpleNamespace(schema_name="candidate_offices")
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=conflicting))
    bindings = [("npi", "1000000491", uuid4(), "a" * 64, "npi", "1000000491")]
    if conflicting:
        with pytest.raises(cms.InitialCMSOfficeBindingError, match="binding_ambiguous"):
            await cms._copy_source_office_records(connection, target, bindings)
    else:
        await cms._copy_source_office_records(connection, target, bindings)
    copy.assert_awaited_once_with(connection, target, bindings)
    assert (
        "count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1"
        in connection.fetchval.await_args.args[0]
    )


@pytest.mark.asyncio
async def test_broken_cursor_rollback_terminates_connection_and_preserves_original_error():
    recipe, artifact, context, connection, transaction = _scope()
    transaction.rollback.side_effect = RuntimeError("rollback unavailable")
    original = asyncio.CancelledError("caller canceled")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with offices._pair_cursor(connection, recipe, artifact, context):
            raise original
    assert caught.value is original
    transaction.rollback.assert_awaited_once()
    connection.terminate.assert_called_once_with()
    transaction.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["context", "artifact", "capture", "missing_authenticated_fact"])
async def test_fact_page_scope_refusal_precedes_mapping_and_cursor(monkeypatch, damage):
    recipe, artifact, context, connection, transaction = _scope()
    verify = AsyncMock(return_value={})
    monkeypatch.setattr(offices, "verify_anchored_source_fact_contents", verify)
    if damage == "context":
        context = SimpleNamespace(**context.__dict__)
    elif damage == "artifact":
        artifact = SimpleNamespace(**artifact.__dict__)
    elif damage == "capture":
        artifact = replace(artifact, descriptor={**artifact.descriptor, "capture_sha256": "f" * 64})
    with pytest.raises(ValueError, match="scope_invalid|capture_changed"):
        await offices.capture_fact_office_pages(connection, recipe, artifact, context)
    assert verify.await_count == int(damage == "missing_authenticated_fact")
    transaction.start.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_fact_artifact_selection_requires_unchanged_manifest_before_owner_lookup(monkeypatch):
    from process import network_serving_read

    manifest = SimpleNamespace(generation_id=1)
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", AsyncMock(return_value=object()))
    owner_lookup = AsyncMock(side_effect=AssertionError("changed manifest reached fact owners"))
    monkeypatch.setattr(offices, "retained_source_fact_descriptors", owner_lookup)
    with pytest.raises(ValueError, match="manifest_changed"):
        await offices.fact_office_artifacts(object(), (_recipe(),), manifest, "control")
    owner_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_flat_source_without_retained_fact_entitlement_never_reads_owner_rows(monkeypatch):
    from process import network_serving_read

    manifest = SimpleNamespace(generation_id=1, source_generations={})
    monkeypatch.setattr(network_serving_read, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    owner_lookup = AsyncMock(side_effect=AssertionError("unretained source reached fact owners"))
    monkeypatch.setattr(offices, "retained_source_fact_descriptors", owner_lookup)
    assert await offices.fact_office_artifacts(object(), (_recipe(),), manifest, "control") == {}
    owner_lookup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("successor", [False, True])
async def test_retained_source_page_refuses_an_unissued_read_before_any_query(successor):
    from tests.test_registry_source_inventory_store_postgres import _capture_recipe

    _, _, context, connection, _ = _scope()
    recipe = _capture_recipe()
    if not successor:
        recipe = replace(recipe, source_pin=replace(recipe.source_pin, retained_successor=None))
    with pytest.raises(ValueError, match="read_unavailable"):
        await offices._verify_source_page(connection, recipe, context)
    connection.execute.assert_not_awaited()
