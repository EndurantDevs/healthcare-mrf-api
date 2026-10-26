# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject incomplete batch evidence before it can become native read authority."""

import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_legacy_membership_source as legacy
from process import registry_aca_source_recipe as producer
from process import registry_source_recipe_store as codec
from tests import test_registry_aca_source_recipe as fixtures
from tests.test_registry_source_recipe_store_postgres import _example


def _metadata():
    scopes = fixtures._batch_scopes()
    return {
        producer._BATCH_KEY: producer._compact_batch(scopes, fixtures._ROLES, fixtures._batch_coverage(scopes)),
        "aca_completed_publication": {"import_id": "import-one"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("digest", [None, "A" * 64, "a" * 63, "g" * 64])
async def test_invalid_reviewed_alias_never_reads_completion(monkeypatch, digest):
    metadata = fixtures._metadata()
    metadata["network_membership"]["reviewed_alias_sha256"] = digest
    completed = AsyncMock()
    monkeypatch.setattr(producer, "require_completed_publication", completed)
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.capture_registry_aca_source_metadata(
            object(), metadata, schema="mrf", runtime_roles=fixtures._ROLES, dataset_id=fixtures._DATASET
        )
    completed.assert_not_awaited()


@pytest.mark.parametrize("scopes", [None, (), [], [{}] * (producer.MAX_SOURCE_RECIPES + 1)])
def test_batch_requires_a_bounded_nonempty_scope_list(scopes):
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        producer._batch_scopes(scopes, fixtures._ROLES)


@pytest.mark.parametrize("damage", ["order", "physical_dataset"])
def test_batch_cannot_reorder_or_join_distinct_physical_captures(damage):
    scopes = fixtures._batch_scopes()
    if damage == "order":
        scopes.reverse()
    else:
        dataset = UUID(int=2)
        scopes[1]["network_bindings"].update(
            dataset_id=str(dataset), dataset_schema=producer.reference_family_stage_schema(dataset)
        )
        scopes.sort(key=producer._canonical_json)
    original = deepcopy(scopes)
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        producer._batch_scopes(scopes, fixtures._ROLES)
    assert scopes == original


@pytest.mark.parametrize(
    "damage",
    ["outer_extra", "batch_type", "batch_keys", "contract", "completion_type", "coverage_type", "coverage_size"],
)
def test_compact_batch_rejects_missing_or_unbounded_shared_authority(damage):
    metadata = _metadata()
    assert len(producer._batch_descriptors(metadata, fixtures._ROLES)) == 2
    batch = metadata[producer._BATCH_KEY]
    match damage:
        case "outer_extra":
            metadata["other_capture"] = {}
        case "batch_type":
            metadata[producer._BATCH_KEY] = []
        case "batch_keys":
            batch.pop("coverage")
        case "contract":
            batch["contract"] = "unrecognized-batch/v1"
        case "completion_type":
            metadata["aca_completed_publication"] = []
        case "coverage_type":
            batch["coverage"] = {}
        case "coverage_size":
            batch["coverage"] = [{}] * (producer.MAX_SOURCE_RECIPES + 1)
    original = deepcopy(metadata)
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        producer._batch_descriptors(metadata, fixtures._ROLES)
    assert metadata == original


@pytest.mark.parametrize(
    "damage",
    [
        "url_type",
        "url_extra",
        "url_missing",
        "bool_count",
        "zero_count",
        "overflow",
        "exceeds_group",
        "exceeds_evidence",
        "incomplete_union",
    ],
)
def test_url_coverage_must_reconcile_to_real_raw_and_evidence_counts(damage):
    scopes = fixtures._batch_scopes()
    coverage = fixtures._batch_coverage(scopes)
    group = coverage[0]
    entry = group["urls"][0]
    match damage:
        case "url_type":
            group["urls"][0] = None
        case "url_extra":
            entry["accepted"] = True
        case "url_missing":
            entry.pop("evidence_rows")
        case "bool_count":
            entry["raw_rows"] = True
        case "zero_count":
            entry["evidence_rows"] = 0
        case "overflow":
            entry["raw_rows"] = 2**63
        case "exceeds_group":
            entry.update(raw_rows=3, evidence_rows=3)
        case "exceeds_evidence":
            entry.update(raw_rows=2, evidence_rows=1)
        case "incomplete_union":
            group.update(raw_rows=3, covered_raw_rows=3)
    original = deepcopy(coverage)
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        producer._validate_batch_coverage(coverage, scopes)
    assert coverage == original


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [True, False])
async def test_coverage_reader_accepts_driver_json_and_refuses_nonlist_results(encoded):
    scopes = fixtures._batch_scopes()
    coverage = fixtures._batch_coverage(scopes)
    session = SimpleNamespace(scalar=AsyncMock(return_value=json.dumps(coverage) if encoded else coverage))
    assert await producer._capture_batch_coverage(session, "mrf", scopes) == coverage
    assert json.loads(session.scalar.await_args.args[1]["groups"]) == [
        {"issuer_id": 7, "year": 2026, "import_id": "import-one"}
    ]
    session.scalar.return_value = "null" if encoded else None
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer._capture_batch_coverage(session, "mrf", scopes)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["admit", "require"])
async def test_batch_stamp_requires_the_callers_original_transaction(monkeypatch, operation):
    session = SimpleNamespace(in_transaction=lambda: False, scalar=AsyncMock())
    owner = AsyncMock()
    monkeypatch.setattr(producer, "protected_publisher_owner", owner)
    function = (
        producer.admit_registry_aca_source_batch if operation == "admit" else producer.require_registry_aca_source_batch
    )
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await function(session, object(), runtime_roles=fixtures._ROLES)
    owner.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["coverage", "existing_stamp"])
async def test_admission_cannot_replace_drifted_evidence_or_a_foreign_stamp(monkeypatch, damage):
    prepared, validation = await fixtures._batch_prepared(monkeypatch)
    session, connection = fixtures._comment_session("native_facade")
    monkeypatch.setattr(producer, "protected_publisher_owner", AsyncMock(return_value=13))
    monkeypatch.setattr(producer, "prepare_reference_family_activation", AsyncMock(return_value=validation))
    native = AsyncMock()
    monkeypatch.setattr(producer, "_require_native_archive", native)
    if damage == "coverage":
        coverage = fixtures._batch_coverage()
        coverage[0].update(raw_rows=3, covered_raw_rows=3)
        for entry in coverage[0]["urls"]:
            entry.update(raw_rows=2, evidence_rows=2)
        producer._validate_batch_coverage(coverage, fixtures._batch_scopes())
        monkeypatch.setattr(producer, "_capture_batch_coverage", AsyncMock(return_value=coverage))
    else:
        session.scalar = AsyncMock(return_value="another protected capture")
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.admit_registry_aca_source_batch(session, prepared, runtime_roles=fixtures._ROLES)
    native.assert_awaited_once()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("recipes", [(), (_example(),)])
async def test_batch_stamp_cannot_claim_empty_or_other_source_recipes(monkeypatch, recipes):
    prepared, _validation = await fixtures._batch_prepared(monkeypatch)
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value=producer._BATCH_PREFIX + codec.canonical_registry_source_recipes(recipes)),
    )
    native = AsyncMock()
    monkeypatch.setattr(producer, "_require_native_archive", native)
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.require_registry_aca_source_batch(session, prepared, runtime_roles=fixtures._ROLES)
    native.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_batch_selector_cannot_borrow_another_logical_scope(monkeypatch):
    prepared, validation = await fixtures._batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, fixtures._ROLES)
    lookup = AsyncMock(return_value=recipes)
    monkeypatch.setattr(producer, "require_registry_aca_source_batch", lookup)
    coordinates = replace(recipes[0].binding_coordinates, edition_id="unselected-edition")
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.require_registry_aca_source_recipe(
            object(), prepared, runtime_roles=fixtures._ROLES, binding_coordinates=coordinates
        )
    lookup.assert_awaited_once()


@pytest.mark.asyncio
async def test_verified_read_never_omits_the_requested_scope(monkeypatch):
    connection, recipes, decode, native = await fixtures._verified_batch_fixture(monkeypatch)
    decode.return_value = (recipes[1],)
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.prepare_registry_aca_batch_read(connection, recipes[0].source_pin)
    decode.assert_awaited_once()
    native.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_read_token_cannot_be_reused_for_a_singleton(monkeypatch):
    prepared, validation = await fixtures._prepared(monkeypatch)
    source = producer._recipe(prepared, validation, fixtures._ROLES).source_pin
    connection = SimpleNamespace(fetch=AsyncMock(), fetchval=AsyncMock())
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.prepare_registry_aca_batch_read(connection, source, verified=object())
    connection.fetch.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_cached_descriptors_do_not_authorize_a_different_scope(monkeypatch):
    prepared, validation = await fixtures._batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, fixtures._ROLES)
    source = recipes[0].source_pin
    assert legacy.aca_source_descriptor(source)["network_membership"] == source.coordinates
    with pytest.raises(legacy.LegacyMembershipSourceError, match="ACA source admission coordinates differ"):
        replace(source, source_url="https://source.example.test/unclaimed")
    lookup = AsyncMock(return_value=recipes[1])
    monkeypatch.setattr(producer, "require_registry_aca_source_recipe", lookup)
    with pytest.raises(legacy.LegacyMembershipSourceError, match="ACA protected batch scope differs"):
        await legacy._require_flat_archive(object(), source)
    assert lookup.await_args.kwargs["binding_coordinates"] == recipes[0].binding_coordinates


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["prepared_type", "prepared_keys", "children_type"])
async def test_wire_batch_rejects_incomplete_physical_identity_before_decoding(monkeypatch, damage):
    prepared, validation = await fixtures._batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, fixtures._ROLES)
    documents = json.loads(codec.canonical_registry_source_recipes(recipes))
    if damage == "prepared_type":
        documents[0]["prepared"] = []
    elif damage == "prepared_keys":
        documents[0]["prepared"].pop("ownership")
    else:
        documents[0]["recipes"] = {}
    with pytest.raises(codec.RegistrySourceRecipeError, match="registry_source_recipes_invalid"):
        codec.decode_registry_source_recipes(documents)
