# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Office4 derives native offices once and records only the locked candidate's inventory."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import registry_initial_source_composition as offices
from process import registry_source_fact_offices as facts
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import SourceInventoryReference
from process.registry_source_inventory_summaries import office_summary_v4
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_contributions import _recipe as _unprotected_recipe
from tests.test_registry_source_inventory_preparation import _aca_recipe, _derivation, _descriptor

_APPROVED = SimpleNamespace(approved_revision=7, generation_id="b" * 64)
_ADDRESS = SimpleNamespace(generation_id="c" * 64)


class _AsyncSink:
    async def __call__(self, _document):
        pytest.fail("An asynchronous sink must never run")


def _recipe(index=0):
    recipe = _ptg_recipe()
    return replace(recipe, source_pin=replace(recipe.source_pin, capture_id=str(UUID(int=index + 1))))


def _cms_recipe():
    recipe = _unprotected_recipe()
    return replace(
        recipe,
        source_pin=replace(
            recipe.source_pin,
            source_id="cms-npd",
            custody_owner_role="synthetic_owner",
            custody_runtime_roles=("synthetic_reader",),
            custody_proof_sha256="d" * 64,
            custody_catalog_sha256="e" * 64,
        ),
        binding_coordinates=replace(recipe.binding_coordinates, source_id="cms-npd"),
    )


def _identity(recipe):
    return hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()


def _complete(recipes):
    preparation = SourceInventoryPreparation(recipes)
    derivations_by_sha = {}
    for recipe in preparation.recipes:
        descriptor = _descriptor(recipe, anchored=True) if recipe == _cms_recipe() else None
        if descriptor is not None:
            preparation.add_contribution(descriptor)
        document = _derivation(recipe, descriptor, facts=descriptor is not None)
        preparation.add_derivation(document)
        derivations_by_sha[document["recipe_sha256"]] = document
    reference, _entries = preparation.finish()
    return reference.root, derivations_by_sha


def _request(**options):
    return offices.OfficeSummaryRequest(("synthetic_reader",), "synthetic_control", revision=4, **options)


def _source_mocks(monkeypatch, derivations_by_sha):
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())

    async def raw(_connection, recipe, *_arguments):
        return derivations_by_sha[_identity(recipe)]

    source = AsyncMock(side_effect=raw)
    monkeypatch.setattr(offices, "_raw_office_derivation", source)
    return source


@pytest.mark.parametrize("count", [1, 101, 1000])
def test_inventory_recipe_bound_preserves_legacy(count):
    recipes = tuple(_recipe(index) for index in reversed(range(count)))
    actual = offices.validate_initial_source_recipes(recipes, inventory=True)
    assert actual == SourceInventoryPreparation(recipes).recipes
    if count <= 100:
        assert offices.validate_initial_source_recipes(recipes) == actual
    else:
        with pytest.raises(offices.RegistryInitialSourceError, match="recipes_invalid"):
            offices.validate_initial_source_recipes(recipes)


@pytest.mark.parametrize("inventory", [False, True])
def test_inventory_keeps_exact_pin_custody(inventory):
    accepted = (_recipe(), _aca_recipe(), _cms_recipe())
    assert len(offices.validate_initial_source_recipes(accepted, inventory=inventory)) == 3
    for invalid in (list(accepted), (_unprotected_recipe(),), (accepted[0], accepted[0])):
        with pytest.raises(offices.RegistryInitialSourceError, match="recipes_invalid"):
            offices.validate_initial_source_recipes(invalid, inventory=inventory)


def test_inventory_rejects_entry_1001():
    with pytest.raises(offices.RegistryInitialSourceError, match="recipes_invalid"):
        offices.validate_initial_source_recipes(tuple(_recipe(index) for index in range(1001)), inventory=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 101, 1000])
async def test_unknown_root_emits_once_in_sha_order(monkeypatch, count):
    recipes = tuple(_recipe(index) for index in reversed(range(count)))
    derivations_by_sha = {_identity(recipe): _derivation(recipe) for recipe in recipes}
    source = _source_mocks(monkeypatch, derivations_by_sha)
    emitted_documents = []
    actual = await offices.prepare_source_offices(
        object(), recipes, _APPROVED, _ADDRESS, _request(derivation_sink=emitted_documents.append)
    )
    assert actual is None and source.await_count == len(emitted_documents) == count
    identities = [_identity(call.args[1]) for call in source.await_args_list]
    assert identities == sorted(derivations_by_sha) == [document["recipe_sha256"] for document in emitted_documents]
    assert len({id(document) for document in emitted_documents}) == count


@pytest.mark.asyncio
@pytest.mark.parametrize("sink", [None, object(), AsyncMock(), _AsyncSink()])
async def test_unknown_root_requires_sync_sink(monkeypatch, sink):
    source = _source_mocks(monkeypatch, {})
    transaction = offices._require_transaction
    with pytest.raises(offices.RegistryInitialSourceError, match="revision_invalid"):
        await offices.prepare_source_offices(
            object(), (_recipe(),), _APPROVED, _ADDRESS, _request(derivation_sink=sink)
        )
    source.assert_not_awaited()
    transaction.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [True, "replacement", "coroutine"])
async def test_sink_cannot_replace_metadata(monkeypatch, response):
    recipe = _recipe()
    source = _source_mocks(monkeypatch, {_identity(recipe): _derivation(recipe)})
    returned = None

    async def deferred():
        pytest.fail("An asynchronous sink must never run")

    if response == "coroutine":
        returned = deferred()
    sink = Mock(return_value=returned if returned is not None else response)
    with pytest.raises(offices.RegistryInitialSourceError, match="request_invalid"):
        await offices.prepare_source_offices(object(), (recipe,), _APPROVED, _ADDRESS, _request(derivation_sink=sink))
    source.assert_awaited_once()
    if returned is not None:
        assert returned.cr_frame is None


@pytest.mark.asyncio
async def test_derivation_identity_must_match_recipe(monkeypatch):
    recipe = _recipe()
    _source_mocks(monkeypatch, {_identity(recipe): _derivation(_recipe(1))})
    sink = Mock()
    with pytest.raises(offices.RegistryInitialSourceError, match="recipes_invalid"):
        await offices.prepare_source_offices(object(), (recipe,), _APPROVED, _ADDRESS, _request(derivation_sink=sink))
    sink.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["recipe_hash", "entry_count", "derivation_count", "type"])
async def test_known_root_checks_before_source(monkeypatch, damage):
    recipe = _recipe()
    root, derivations_by_sha = _complete((recipe,))
    source = _source_mocks(monkeypatch, derivations_by_sha)
    selected = AsyncMock()
    monkeypatch.setattr(facts, "fact_office_artifacts", selected)
    changed = {
        "recipe_hash": replace(root, recipes_sha256="0" * 64),
        "entry_count": replace(root, entry_count=2),
        "derivation_count": replace(root, derivation_count=0),
        "type": SourceInventoryReference(root),
    }[damage]
    with pytest.raises(offices.RegistryInitialSourceError, match="recipes_invalid"):
        await offices.prepare_source_offices(
            object(), (recipe,), _APPROVED, _ADDRESS, _request(inventory_root=changed, source_manifest=object())
        )
    source.assert_not_awaited()
    selected.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("with_facts", [False, True])
async def test_known_root_folds_independent_sources(monkeypatch, with_facts):
    recipes = (_recipe(), _cms_recipe()) if with_facts else (_recipe(), _recipe(1))
    root, derivations_by_sha = _complete(recipes)
    raw = _source_mocks(monkeypatch, derivations_by_sha)
    artifact = object()
    cms_identity = _identity(_cms_recipe())
    selected = AsyncMock(return_value={cms_identity: artifact} if with_facts else {})
    monkeypatch.setattr(facts, "fact_office_artifacts", selected)
    capture = AsyncMock(return_value=derivations_by_sha.get(cms_identity))
    monkeypatch.setattr(facts, "capture_fact_office_pages", capture)
    sink = Mock(side_effect=AssertionError("Known-root preparation must fold its own derivations"))
    actual = await offices.prepare_source_offices(
        object(),
        recipes,
        _APPROVED,
        _ADDRESS,
        _request(inventory_root=root, derivation_sink=sink, source_manifest=object()),
    )
    assert actual == office_summary_v4(
        root, _APPROVED, _ADDRESS, sorted(derivations_by_sha.values(), key=lambda d: d["recipe_sha256"]), None
    )
    assert raw.await_count == (1 if with_facts else 2)
    assert capture.await_count == int(with_facts)
    selected.assert_awaited_once()
    sink.assert_not_called()
    if with_facts:
        assert capture.await_args.args[1:3] == (_cms_recipe(), artifact)


@pytest.mark.asyncio
async def test_raw_derivation_reuses_one_page_pass(monkeypatch):
    recipe = _recipe()
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())
    custody = AsyncMock(return_value=object())
    monkeypatch.setattr(offices, "_require_recipe_custody", custody)
    page = SimpleNamespace(source_rows=(object(),))
    read = AsyncMock(side_effect=[(page, 1, 1000), (SimpleNamespace(source_rows=()), None, 1000)])
    monkeypatch.setattr(offices, "_read_recipe_page", read)
    capture = AsyncMock(return_value=(SimpleNamespace(office_count=2), ("synthetic_binding",)))
    monkeypatch.setattr(offices, "_capture_office_page", capture)
    monkeypatch.setattr(offices, "source_office_pin_document", lambda _pin: {"synthetic_pin": "a" * 64})
    emitted_documents = []
    await offices.prepare_source_offices(
        object(), (recipe,), _APPROVED, _ADDRESS, _request(derivation_sink=emitted_documents.append)
    )
    custody.assert_awaited_once()
    assert read.await_count == 2
    capture.assert_awaited_once()
    assert (emitted_documents[0]["office_rows"], emitted_documents[0]["page_count"], emitted_documents[0]["mode"]) == (
        2,
        1,
        "raw",
    )
    assert emitted_documents[0]["physical_source_sha256"] == recipe.source_pin.generation_id


@pytest.mark.asyncio
async def test_fact_failure_has_no_raw_fallback(monkeypatch):
    recipe = _cms_recipe()
    root, derivations_by_sha = _complete((recipe,))
    raw = _source_mocks(monkeypatch, derivations_by_sha)
    monkeypatch.setattr(facts, "fact_office_artifacts", AsyncMock(return_value={_identity(recipe): object()}))
    monkeypatch.setattr(facts, "capture_fact_office_pages", AsyncMock(side_effect=ValueError("invalid_native_seal")))
    with pytest.raises(ValueError, match="invalid_native_seal"):
        await offices.prepare_source_offices(
            object(), (recipe,), _APPROVED, _ADDRESS, _request(inventory_root=root, source_manifest=object())
        )
    raw.assert_not_awaited()


@asynccontextmanager
async def _transaction():
    yield


def _record_candidate(root, receipt, *, retained=False):
    return {
        "state": "open",
        "approved_custom_revision": _APPROVED.approved_revision,
        "source_recipes_json": SourceInventoryReference(root).as_dict(),
        "source_generations": {
            RECIPE_DIGEST_KEY: root.recipes_sha256,
            "registry_source_inventory": root.entries_sha256,
            "custom_membership": _APPROVED.generation_id,
            "unified_address": _ADDRESS.generation_id,
            offices.SUMMARY_KEY: offices.initial_source_office_receipt_sha256(receipt, inventory_root=root),
        },
        "validation_json": {offices.SUMMARY_KEY: receipt} if retained else {},
    }


def _record_mocks(monkeypatch, candidate):
    connection = SimpleNamespace(transaction=_transaction, execute=AsyncMock())
    lock = AsyncMock(return_value=candidate)
    monkeypatch.setattr(offices, "_locked_candidate", lock)
    approved = AsyncMock(return_value=_APPROVED)
    monkeypatch.setattr(offices, "pin_retained_approved_membership_source", approved)
    return connection, lock, approved


@pytest.mark.asyncio
@pytest.mark.parametrize("office_request", [None, {}, object()])
async def test_office_prepare_and_record_require_typed_request_before_connection(office_request):
    with pytest.raises(offices.RegistryInitialSourceError, match="request_invalid"):
        await offices.prepare_source_offices(object(), (), object(), object(), office_request)
    with pytest.raises(offices.RegistryInitialSourceError, match="request_invalid"):
        await offices.record_source_offices(object(), object(), (), object(), office_request)


@pytest.mark.asyncio
async def test_retained_initial_read_cannot_borrow_origin_manifest_or_foreign_approval():
    request = _request(origin_manifest_sha256="a" * 64)
    with pytest.raises(offices.RegistryInitialSourceError, match="revision_invalid"):
        await offices.prepare_initial_source_offices(
            object(), (), object(), object(), request, retained_fhir_reads=object()
        )
    with pytest.raises(offices.RegistryInitialSourceError, match="request_invalid"):
        await offices.record_initial_source_offices(object(), object(), (), object(), object(), request=request)


@pytest.mark.asyncio
@pytest.mark.parametrize("generation_key", ["unified_address", offices.SUMMARY_KEY])
async def test_office_record_rechecks_candidate_commitments_before_summary_write(monkeypatch, generation_key):
    recipe = _recipe()
    root, derivations_by_sha = _complete((recipe,))
    receipt = office_summary_v4(root, _APPROVED, _ADDRESS, derivations_by_sha.values(), None)
    candidate = _record_candidate(root, receipt)
    candidate["source_generations"][generation_key] = "0" * 64
    connection, _lock, _approved = _record_mocks(monkeypatch, candidate)
    source = _source_mocks(monkeypatch, derivations_by_sha)
    with pytest.raises(offices.RegistryInitialSourceError, match="receipt_conflict"):
        await offices.record_source_offices(connection, object(), (recipe,), _ADDRESS, _request(inventory_root=root))
    source.assert_awaited_once()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [False, True])
async def test_record_uses_locked_root_and_replay(monkeypatch, retained):
    recipe = _recipe()
    root, derivations_by_sha = _complete((recipe,))
    receipt = office_summary_v4(root, _APPROVED, _ADDRESS, derivations_by_sha.values(), None)
    candidate = _record_candidate(root, receipt, retained=retained)
    connection, lock, approved = _record_mocks(monkeypatch, candidate)
    source = _source_mocks(monkeypatch, derivations_by_sha)
    target = SimpleNamespace(candidate_id=str(UUID(int=11)))
    sink = Mock(side_effect=AssertionError("Caller metadata sink has no recording authority"))
    actual = await offices.record_source_offices(
        connection, target, (recipe,), _ADDRESS, _request(inventory_root=object(), derivation_sink=sink)
    )
    assert actual == receipt
    lock.assert_awaited_once()
    approved.assert_awaited_once()
    assert source.await_args.args[-1].context.inventory_root == root
    assert source.await_args.args[-1].copy_target is target
    assert connection.execute.await_count == int(not retained)
    if not retained:
        assert json.loads(connection.execute.await_args.args[-1]) == receipt
    sink.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["closed", "revision", "recipes", "approved"])
async def test_record_guards_precede_copy(monkeypatch, failure):
    recipe = _recipe()
    root, derivations_by_sha = _complete((recipe,))
    receipt = office_summary_v4(root, _APPROVED, _ADDRESS, derivations_by_sha.values(), None)
    candidate = _record_candidate(root, receipt)
    if failure == "closed":
        candidate["state"] = "sealed"
    elif failure == "approved":
        candidate["source_generations"]["custom_membership"] = "0" * 64
    connection, _lock, approved = _record_mocks(monkeypatch, candidate)
    source = _source_mocks(monkeypatch, derivations_by_sha)
    request = (
        replace(_request(inventory_root=root), revision=3) if failure == "revision" else _request(inventory_root=root)
    )
    recipes = (_recipe(1),) if failure == "recipes" else (recipe,)
    with pytest.raises(ValueError):
        await offices.record_source_offices(connection, object(), recipes, _ADDRESS, request)
    source.assert_not_awaited()
    connection.execute.assert_not_awaited()
    if failure != "approved":
        approved.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_candidate_cannot_borrow_root(monkeypatch):
    recipe = _recipe()
    root, derivations_by_sha = _complete((recipe,))
    receipt = office_summary_v4(root, _APPROVED, _ADDRESS, derivations_by_sha.values(), None)
    candidate = _record_candidate(root, receipt)
    candidate["source_recipes_json"] = json.loads(canonical_registry_source_recipes((recipe,)))
    candidate["source_generations"].pop("registry_source_inventory")
    connection, _lock, approved = _record_mocks(monkeypatch, candidate)
    source = _source_mocks(monkeypatch, derivations_by_sha)
    with pytest.raises(offices.RegistryInitialSourceError, match="revision_invalid"):
        await offices.record_source_offices(connection, object(), (recipe,), _ADDRESS, _request(inventory_root=root))
    source.assert_not_awaited()
    approved.assert_not_awaited()
    connection.execute.assert_not_awaited()
