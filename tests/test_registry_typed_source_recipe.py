# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed metadata roundtrips are descriptions, never Source or office admission."""

import hashlib
import json
from copy import deepcopy
from dataclasses import asdict, replace
from uuid import UUID

import pytest

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_source_witness_contract import digest
from process.network_fhir_typed_capture import PREFIX, TypedGraphAnchor
from process.network_fhir_typed_contract import graph_document
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.registry_initial_source_composition import validate_initial_source_recipes
from process.registry_source_contribution_store import (
    registry_source_contribution_receipt,
    validate_source_contribution_descriptor,
)
from process.registry_source_inventory_contract import make_source_inventory_entry
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_store import _COLUMNS, source_inventory_entry, source_inventory_record
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from process.registry_typed_source_metadata import (
    COUNTERS,
    typed_contribution_summary,
    typed_office_summary,
    validate_typed_contribution,
    validate_typed_derivation,
    validate_typed_summary,
)
from tests.test_network_fhir_source_witness_contract import database_fields, origin_fields
from tests.test_registry_fhir_catalog_sources import descriptor as catalog_descriptor
from tests.test_registry_source_recipe_store_postgres import _example


def _recipe():
    catalog = catalog_descriptor()
    origin = origin_fields() | {
        "schema_name": catalog["coordinates"]["dataset_schema"],
        "source_id": catalog["coordinates"]["source_id"],
        "source_aliases": tuple(catalog["source_aliases"]),
        "header_oid": catalog["header_table_oid"],
        "resource_parent_oid": catalog["resource_table_oid"],
        "resource_leaf_oid": catalog["resource_leaf_oids"][0],
        "database_oid": catalog["database_oid"],
        "source_uri": catalog["source_uri"],
        "semantic_projection_as_of": "2026-10-01",
        "native_catalog_sha256": catalog["native_catalog_sha256"],
        "proof_sha256": catalog["admission"]["proof_sha256"],
        "admission_metadata_sha256": catalog["admission"]["metadata_sha256"],
        "resource_counts": tuple(sorted(catalog["admission"]["resource_counts"].items())),
        "resource_hashes": tuple((kind, "e" * 64) for kind in sorted(catalog["admission"]["resource_counts"])),
        "descriptor_counts": tuple(sorted(catalog["admission"]["resource_counts"].items())),
    }
    identity = database_fields() | {"database_oid": origin["database_oid"]}
    graph = graph_document(
        origin,
        identity,
        "2026-10-01",
        {
            "source_rows": sum(catalog["admission"]["resource_counts"].values()),
            "projected_rows": 1,
            "pair_rows": 1,
            "reference_rows": 1,
            "provider_only_roles": 0,
        },
    )
    anchor = TypedGraphAnchor(str(UUID(int=5)), "2026-10-01T00:00:00.000000+00:00")
    capture_by_field = {
        "component": "registry_fhir_typed_capture",
        "descriptor_version": 4,
        "capture_id": anchor.capture_id,
        "observed_at": anchor.observed_at,
        "schema_name": PREFIX + UUID(anchor.capture_id).hex,
        "owner_role": "typed_owner",
        "runtime_roles": ["typed_reader"],
        "catalog": {
            "schema_oid": 100,
            "relation_oids": {"cells": 101, "addresses": 102, "pairs": 103},
            "sha256": "a" * 64,
        },
        "content": {name: {"rows": 1, "sha256": "b" * 64} for name in ("cells", "addresses", "pairs")},
        "graph": graph,
        "catalog_descriptor": catalog,
    }
    capture_by_field["capture_sha256"] = digest(capture_by_field)
    pin = PinnedTypedFHIRMembershipSource(capture_by_field, anchor, catalog)
    return RegistrySourceMembershipRecipe(pin, RegistryNetworkSourceCoordinates(**catalog["coordinates"]))


def _metadata(recipe):
    counters = dict.fromkeys(COUNTERS, 0)
    counters.update(
        source_rows=recipe.source_pin.capture["graph"]["counts"]["source_rows"],
        projected_rows=1,
        strict_pair_rows=1,
        reference_rows=1,
        mapped_pair_rows=1,
        mapped_reference_rows=1,
        matched_offices=1,
    )
    contribution = validate_typed_contribution(
        {
            "component": "registry_fhir_typed_contribution",
            "descriptor_version": 4,
            "recipe_sha256": hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest(),
            "capture_sha256": recipe.source_pin.generation_id,
            "catalog_generation": recipe.source_pin.catalog_generation,
            "mapping_sha256": "c" * 64,
            "counters": counters,
        }
    )
    derivation = validate_typed_derivation(
        {
            "derivation_version": 3,
            "recipe_sha256": contribution["recipe_sha256"],
            "capture_sha256": contribution["capture_sha256"],
            "contribution_sha256": digest(contribution),
            "mapping_sha256": contribution["mapping_sha256"],
            "office_policy": "fhir-direct-network-office-v3",
            "canonicalizer_version": 2,
            "approved_revision": 1,
            "approved_generation_sha256": "d" * 64,
            "address_source": {
                "schema_name": "address_source",
                "table_name": "entity_address_unified",
                "generation_id": "e" * 64,
            },
            "address_identity": {"schema_oid": 200, "table_oid": 201, "owner_oid": 202, "identity_sha256": "f" * 64},
            "matched_offices": 1,
            "missing_offices": 0,
        }
    )
    return contribution, derivation


def test_typed_recipe_closed_roundtrip_and_old_bytes_unchanged():
    old = canonical_registry_source_recipes((_example(),))
    recipe = _recipe()
    canonical = canonical_registry_source_recipes((recipe,))
    assert json.loads(canonical)[0]["source_kind"] == "fhir_typed"
    assert decode_registry_source_recipes(canonical) == (recipe,)
    assert validate_initial_source_recipes((recipe,), inventory=True) == (recipe,)
    with pytest.raises(ValueError):
        validate_initial_source_recipes((recipe,), inventory=False)
    assert canonical_registry_source_recipes(decode_registry_source_recipes(old)) == old
    assert "source_witness_anchor" not in canonical and "fact_candidate_id" not in canonical
    with pytest.raises(ValueError):
        replace(recipe, binding_coordinates=replace(recipe.binding_coordinates, edition_id="other"))
    with pytest.raises(ValueError):
        replace(recipe.source_pin, anchor=replace(recipe.source_pin.anchor, capture_id=str(UUID(int=6))))


@pytest.mark.parametrize("field", ["source_witness_capture", "retained_capture", "retained_successor", "extra"])
def test_typed_pin_cannot_borrow_legacy_authority_fields(field):
    document = json.loads(canonical_registry_source_recipes((_recipe(),)))
    document[0]["source_pin"][field] = {}
    with pytest.raises(ValueError):
        decode_registry_source_recipes(document)


def test_typed_inventory_pairs_distinct_descriptors_and_indexed_scope():
    recipe = _recipe()
    contribution, derivation = _metadata(recipe)
    assert validate_source_contribution_descriptor(contribution) == contribution
    preparation = SourceInventoryPreparation((recipe,))
    preparation.add_contribution(contribution)
    preparation.add_derivation(derivation)
    reference, entries = preparation.finish()
    assert reference.root.contribution_count == reference.root.derivation_count == 1
    record = source_inventory_record(entries[0])
    assert record[-2:] == ("source_generation", recipe.source_pin.catalog_generation)
    assert source_inventory_entry(dict(zip(_COLUMNS, record, strict=True))) == entries[0]
    summary = typed_contribution_summary(reference.root, [contribution])
    assert summary["revision"] == 6 and summary["counters"]["mapped_pair_rows"] == 1
    assert validate_typed_summary(summary, reference.root) == summary
    office = typed_office_summary(reference.root, [derivation])
    assert validate_typed_summary(office, reference.root, offices=True) == office
    stage = RegistrySourceRecipeStage(
        "unused", 1, 0, "a" * 64, contribution_receipts=(contribution,), contribution_revision=4, inventory=True
    )
    assert registry_source_contribution_receipt(stage, inventory_root=reference.root) == summary
    with pytest.raises(ValueError):
        registry_source_contribution_receipt(replace(stage, contribution_revision=2), inventory_root=reference.root)


def _mutate_inventory_metadata(contribution, derivation, mutation):
    if mutation == "capture":
        contribution["capture_sha256"] = "0" * 64
        return
    if mutation == "catalog":
        contribution["catalog_generation"] = "0" * 64
        return
    if mutation == "mapping":
        derivation["mapping_sha256"] = "0" * 64
        return
    if mutation == "count":
        contribution["counters"]["mapped_pair_rows"] = 2
        return
    if mutation == "anchor":
        contribution["fact_candidate_id"] = str(UUID(int=1))
        return
    else:
        derivation["derivation_version"] = 2


@pytest.mark.parametrize("mutation", ["capture", "catalog", "mapping", "count", "anchor", "legacy"])
def test_typed_inventory_refuses_cross_recipe_or_old_protocol_metadata(mutation):
    recipe = _recipe()
    contribution, derivation = _metadata(recipe)
    _mutate_inventory_metadata(contribution, derivation, mutation)
    preparation = SourceInventoryPreparation((recipe,))
    with pytest.raises(ValueError):
        preparation.add_contribution(contribution)
        preparation.add_derivation(derivation)
        preparation.finish()
    with pytest.raises(ValueError):
        preparation.finish()


def test_typed_inventory_requires_complete_metadata_and_never_coerces_legacy_pin():
    recipe = _recipe()
    contribution, derivation = _metadata(recipe)
    preparation = SourceInventoryPreparation((recipe,))
    preparation.add_contribution(contribution)
    with pytest.raises(ValueError):
        preparation.finish()
    legacy = _example()
    contribution["recipe_sha256"] = hashlib.sha256(canonical_registry_source_recipes((legacy,)).encode()).hexdigest()
    with pytest.raises(ValueError):
        SourceInventoryPreparation((legacy,)).add_contribution(contribution)
    assert make_source_inventory_entry(1, recipe).contribution_json is None


@pytest.mark.parametrize("section", ["contribution", "derivation"])
def test_typed_metadata_is_defensive_and_rejects_boolean_versions(section):
    contribution, derivation = _metadata(_recipe())
    selected = contribution if section == "contribution" else derivation
    validate = validate_typed_contribution if section == "contribution" else validate_typed_derivation
    original = deepcopy(selected)
    checked = validate(selected)
    selected[next(iter(selected))] = None
    assert checked == original
    original["descriptor_version" if section == "contribution" else "derivation_version"] = True
    with pytest.raises(ValueError):
        validate(original)


def test_mixed_inventory_commits_each_protocol_without_coercion():
    from tests.test_registry_source_inventory_preparation import _derivation, _descriptor

    typed, legacy = _recipe(), _example()
    typed_contribution, typed_derivation = _metadata(typed)
    legacy_contribution = _descriptor(legacy, anchored=True)
    legacy_derivation = _derivation(legacy, legacy_contribution, facts=True)
    preparation = SourceInventoryPreparation((typed, legacy))
    for document in (typed_contribution, legacy_contribution):
        preparation.add_contribution(document)
    for document in (typed_derivation, legacy_derivation):
        preparation.add_derivation(document)
    reference, entries = preparation.finish()
    contributions = sorted((typed_contribution, legacy_contribution), key=lambda row: row["recipe_sha256"])
    derivations = sorted((typed_derivation, legacy_derivation), key=lambda row: row["recipe_sha256"])
    summary = typed_contribution_summary(reference.root, contributions)
    office = typed_office_summary(reference.root, derivations)
    assert summary["source_count"] == office["source_count"] == 2
    assert summary["typed_source_count"] == office["typed_source_count"] == 1
    assert summary["legacy"][0]["version"] == office["legacy"][0]["version"] == 1
    assert "fact_rows" in summary["legacy"][0]["counters"] and "fact_rows" not in summary["counters"]
    assert (
        json.loads(
            next(
                entry.contribution_json
                for entry in entries
                if json.loads(entry.contribution_json).get("descriptor_version") != 4
            )
        )
        == legacy_contribution
    )
    changed = deepcopy(summary)
    changed["legacy"] *= 2
    with pytest.raises(ValueError):
        validate_typed_summary(changed, reference.root)
    with pytest.raises(ValueError):
        typed_contribution_summary(reference.root, reversed(contributions))


def test_typed_resume_vector_cannot_drop_a_retained_source():
    from process.registry_fhir_serving_preservation import fhir_vector_succession

    typed, legacy = _recipe(), _example()
    assert fhir_vector_succession((legacy,), (legacy, typed)) == ((legacy, legacy),)
    assert fhir_vector_succession((legacy, typed), (legacy, typed)) == ((legacy, legacy), (typed, typed))
    with pytest.raises(ValueError):
        fhir_vector_succession((legacy, typed), (legacy,))
    with pytest.raises(ValueError):
        fhir_vector_succession((legacy, typed), (typed,))


def test_typed_office_summary_binds_the_registered_approval_and_address():
    from process.registry_initial_source_composition import (
        SUMMARY_KEY,
        initial_source_office_receipt_sha256,
        verify_initial_source_office_receipt,
    )
    from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY
    from process.registry_source_recipe_store import RECIPE_DIGEST_KEY

    recipe = _recipe()
    contribution, derivation = _metadata(recipe)
    preparation = SourceInventoryPreparation((recipe,))
    preparation.add_contribution(contribution)
    preparation.add_derivation(derivation)
    reference, _entries = preparation.finish()
    summary = typed_office_summary(reference.root, [derivation])
    candidate_by_field = {
        "source_recipes_json": reference.as_dict(),
        "approved_custom_revision": 1,
        "source_generations": {
            RECIPE_DIGEST_KEY: reference.root.recipes_sha256,
            INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
            "custom_membership": derivation["approved_generation_sha256"],
            "unified_address": derivation["address_source"]["generation_id"],
            SUMMARY_KEY: initial_source_office_receipt_sha256(summary, inventory_root=reference.root),
        },
        "validation_json": {SUMMARY_KEY: summary},
    }
    assert verify_initial_source_office_receipt(candidate_by_field) == summary
    candidate_by_field["approved_custom_revision"] = 2
    with pytest.raises(ValueError):
        verify_initial_source_office_receipt(candidate_by_field)


@pytest.mark.parametrize(
    "section,field,value",
    [
        ("context", "office_policy", "fhir-direct-network-office-v2"),
        ("context", "canonicalizer_version", True),
        ("address_identity", "schema_oid", True),
        ("address_identity", "owner_oid", 0),
        ("address_identity", "table_oid", 2**32),
        ("summary", "typed_source_count", 0),
    ],
)
def test_typed_office_summary_rejects_changed_policy_or_non_native_identity(section, field, value):
    from process.registry_initial_source_composition import SUMMARY_KEY
    from process.registry_source_inventory_reference import verify_source_recipe_metadata

    _, _, candidate, _ = _registered_typed_resume()
    root = verify_source_recipe_metadata(candidate).inventory.root
    summary = candidate["validation_json"][SUMMARY_KEY]
    assert validate_typed_summary(summary, root, offices=True) == summary
    damaged = deepcopy(summary)
    target = damaged if section == "summary" else damaged["context"]
    if section == "address_identity":
        target = target["address_identity"]
    target[field] = value
    with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
        validate_typed_summary(damaged, root, offices=True)


@pytest.mark.parametrize(
    "start,end,expected",
    [
        ("2026-02-30", None, False),
        ("2026-01-01T25:00:00Z", None, False),
        ("2026-01-01", "2025-12-31", False),
        ("2026-01-01", "2026-01-01", True),
    ],
)
def test_typed_membership_period_requires_real_dates_and_preserves_inclusive_boundaries(start, end, expected):
    from datetime import date

    from process.network_fhir_typed_source import _is_period_allowed

    assert _is_period_allowed(start, end, date(2026, 1, 1)) is expected


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_failed_typed_workspace_invalidates_its_capability_and_terminates_after_failed_rollback(cancelled):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from process.network_fhir_typed_source import _workspace

    transaction = SimpleNamespace(
        start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock(side_effect=RuntimeError("rollback failed"))
    )
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=lambda: transaction,
        execute=AsyncMock(),
        fetchval=AsyncMock(side_effect=[101, 102, 103, 104, "1/2"]),
        terminate=Mock(),
    )
    failure = asyncio.CancelledError() if cancelled else ValueError("consumer failed")
    with pytest.raises(type(failure)) as raised:
        async with _workspace(connection) as stage:
            assert stage.connection is connection and stage.task is asyncio.current_task()
            assert stage.virtual_xid == "1/2" and stage.oids == {
                "cells": 101,
                "addresses": 102,
                "pairs": 103,
                "checks": 104,
            }
            raise failure
    assert raised.value is failure and stage.active is False
    transaction.start.assert_awaited_once()
    transaction.rollback.assert_awaited_once()
    transaction.commit.assert_not_awaited()
    connection.terminate.assert_called_once_with()


def _registered_typed_resume(state="open"):
    from process.network_address_projection import PinnedAddressSource
    from process.registry_candidate_composition import _target
    from process.registry_initial_source_composition import SUMMARY_KEY as OFFICE_KEY
    from process.registry_initial_source_composition import initial_source_office_receipt_sha256
    from process.registry_source_contribution_store import SUMMARY_KEY as CONTRIBUTION_KEY
    from process.registry_source_contribution_store import registry_source_contribution_sha256
    from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY
    from process.registry_source_recipe_store import RECIPE_DIGEST_KEY

    recipe = _recipe()
    contribution, derivation = _metadata(recipe)
    preparation = SourceInventoryPreparation((recipe,))
    preparation.add_contribution(contribution)
    preparation.add_derivation(derivation)
    reference, _entries = preparation.finish()
    contribution = typed_contribution_summary(reference.root, [contribution])
    office = typed_office_summary(reference.root, [derivation])
    generations_by_kind = {
        RECIPE_DIGEST_KEY: reference.root.recipes_sha256,
        INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
        "custom_membership": derivation["approved_generation_sha256"],
        "unified_address": derivation["address_source"]["generation_id"],
        CONTRIBUTION_KEY: registry_source_contribution_sha256(contribution, inventory_root=reference.root),
        OFFICE_KEY: initial_source_office_receipt_sha256(office, inventory_root=reference.root),
    }
    request = UUID(int=8)
    copy_target = _target(request, 1, 0, generations_by_kind)
    candidate_by_field = {
        **{
            field: getattr(copy_target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        "bounded": True,
        "state": state,
        "expected_head": 0,
        "source_recipes_json": reference.as_dict(),
        "source_generations": generations_by_kind,
        "approved_custom_revision": 1,
        "validation_json": {
            CONTRIBUTION_KEY: contribution,
            OFFICE_KEY: office,
        },
    }
    if state != "open":
        candidate_by_field["validation_json"]["writer_closure"] = {
            "loader_roles": ["writer"],
            "reader_roles": ["reader"],
        }
    if state not in {"open", "sealed"}:
        candidate_by_field["validation_json"]["address_source"] = dict(derivation["address_source"])
    return request, copy_target, candidate_by_field, PinnedAddressSource(**derivation["address_source"])


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "sealed", "validated", "ready", "published"])
async def test_registered_typed_resume_preserves_revision_and_office_pin(state):
    """Unvalidated registration rederives offices; validated registration retains custody inputs."""
    from process import registry_composition_resume as resume
    from tests.test_registry_composition_resume import _connection

    request, copy_target, candidate, address_source = _registered_typed_resume(state)
    fresh_inputs = (address_source, ("current_reader", "current_writer"))
    expected = fresh_inputs
    if state not in {"open", "sealed"}:
        expected = (address_source, ("reader", "writer"))
    connection = _connection([candidate])
    assert await resume.resolve_registered_contribution_resume(
        connection, request, 1, 0
    ) == resume.RegisteredContributionResume(copy_target, 4)
    assert await resume.resolve_registered_office_summary_revision(connection, request, 1, 0) == 6
    assert (
        await resume.resolve_registered_verified_office_inputs(
            connection, request, 1, 0, verified_office_inputs=fresh_inputs
        )
        == expected
    )
    assert await resume.resolve_registered_source_inventory(connection, request, 1, 0) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "sealed"])
@pytest.mark.parametrize("damage", ["absent", "shape", "roles", "schema_name", "table_name", "generation_id"])
async def test_unvalidated_typed_resume_requires_current_inputs_matching_the_frozen_address(state, damage):
    from process import registry_composition_resume as resume
    from tests.test_registry_composition_resume import _connection

    request, _target, candidate, address_source = _registered_typed_resume(state)
    inputs = (address_source, ("reader", "writer"))
    if damage == "absent":
        inputs = None
    elif damage == "shape":
        inputs = (address_source,)
    elif damage == "roles":
        inputs = (address_source, ["reader"])
    else:
        inputs = (replace(address_source, **{damage: "changed"}), inputs[1])
    with pytest.raises(resume.RegistryCompositionResumeError, match="verified_offices_differ"):
        await resume.resolve_registered_verified_office_inputs(
            _connection([candidate]), request, 1, 0, verified_office_inputs=inputs
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "sealed"])
@pytest.mark.parametrize(
    "field", ["dataset_id", "schema_id", "producer_id", "approved_custom_revision", "expected_head"]
)
async def test_unvalidated_typed_resume_refuses_changed_request_identity(state, field):
    from process import registry_composition_resume as resume
    from tests.test_registry_composition_resume import _connection

    request, _target, candidate, address_source = _registered_typed_resume(state)
    candidate[field] = 2 if field in ("approved_custom_revision", "expected_head") else str(UUID(int=9))
    with pytest.raises(resume.RegistryCompositionResumeError, match="scope_changed"):
        await resume.resolve_registered_verified_office_inputs(
            _connection([candidate]), request, 1, 0, verified_office_inputs=(address_source, ("reader",))
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "sealed"])
@pytest.mark.parametrize("damage", ["address_identity", "address_source", "inventory", "contribution", "generation"])
async def test_unvalidated_typed_resume_refuses_changed_registered_commitments(state, damage):
    from process import registry_composition_resume as resume
    from process.registry_initial_source_composition import SUMMARY_KEY as OFFICE_KEY
    from process.registry_source_contribution_store import SUMMARY_KEY as CONTRIBUTION_KEY
    from tests.test_registry_composition_resume import _connection

    request, _target, candidate, address_source = _registered_typed_resume(state)
    match damage:
        case "address_identity":
            candidate["validation_json"][OFFICE_KEY]["context"]["address_identity"]["table_oid"] += 1
        case "address_source":
            candidate["validation_json"][OFFICE_KEY]["context"]["address_source"]["generation_id"] = "a" * 64
        case "inventory":
            candidate["source_recipes_json"] = []
        case "contribution":
            candidate["validation_json"][CONTRIBUTION_KEY]["revision"] = 4
        case "generation":
            candidate["source_generations"]["unified_address"] = "a" * 64
    with pytest.raises(ValueError):
        await resume.resolve_registered_verified_office_inputs(
            _connection([candidate]), request, 1, 0, verified_office_inputs=(address_source, ("reader",))
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["validated", "ready", "published"])
@pytest.mark.parametrize("damage", ["address_source", "writer_closure", "address_changed"])
async def test_validated_typed_resume_requires_retained_custody(state, damage):
    from process import registry_composition_resume as resume
    from tests.test_registry_composition_resume import _connection

    request, _target, candidate, address_source = _registered_typed_resume(state)
    report = candidate["validation_json"]
    if damage == "address_changed":
        report["address_source"]["generation_id"] = "a" * 64
    else:
        report.pop(damage)
    with pytest.raises(resume.RegistryCompositionResumeError):
        await resume.resolve_registered_verified_office_inputs(
            _connection([candidate]), request, 1, 0, verified_office_inputs=(address_source, ("reader",))
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["stage_registry_source_recipes", "stage_verified_registry_source_recipes"])
async def test_mixed_stage_dispatches_each_protocol_once_and_rolls_back_combination(monkeypatch, entrypoint):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from process import registry_source_recipe_composition as composition
    from process import registry_typed_source_stage as typed_stage
    from process import registry_verified_witness_stage as witness_stage

    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("complete")

    connection = SimpleNamespace(transaction=transaction)
    typed, legacy = _recipe(), _example()
    typed_result, legacy_result, combined = object(), object(), object()
    produce = AsyncMock(return_value=typed_result)
    verify = AsyncMock(return_value=typed_result)
    replay = AsyncMock(return_value=legacy_result)
    merge = AsyncMock(return_value=combined)
    monkeypatch.setattr(typed_stage, "stage_typed_source_recipes", produce)
    monkeypatch.setattr(typed_stage, "stage_verified_typed_source_recipes", verify)
    monkeypatch.setattr(typed_stage, "combine_typed_source_stages", merge)
    monkeypatch.setattr(witness_stage, "stage_verified_witness_inventory", replay)
    options_by_name = {
        "request_id": uuid4(),
        "control_schema": "registry",
        "inventory": True,
        "contribution_revision": 4,
        "read_context": composition.SourceRecipeReadContext(verified_office_inputs=(object(), ("reader",))),
    }
    dispatch = getattr(composition, entrypoint)
    assert await dispatch(connection, (legacy, typed), object(), **options_by_name) is combined
    selected, unused = (produce, verify) if entrypoint == "stage_registry_source_recipes" else (verify, produce)
    assert selected.await_args.args[1] == (typed,) and replay.await_args.args[1] == (legacy,)
    unused.assert_not_awaited()
    assert merge.await_args.args[1:] == (typed_result, legacy_result)
    assert events == ["begin", "complete"]
    merge.side_effect = ValueError("changed_native_binding")
    with pytest.raises(ValueError, match="changed_native_binding"):
        await dispatch(connection, (legacy, typed), object(), **options_by_name)
    assert events[-2:] == ["begin", "rollback"]


def test_invalid_native_canonical_addresses_are_omitted_before_uuid_conversion():
    from process.network_fhir_typed_source import _valid_canonical_address_records

    address_values = (None,) * 6
    pending_by_key = {name: address_values for name in ("missing", "no-key", "no-identity", "valid")}
    key = str(UUID(int=1))
    canonical_results = [
        {"address_key": None, "identity_key": None},
        {"address_key": None, "identity_key": "unused"},
        {"address_key": key, "identity_key": None},
        {"address_key": key, "identity_key": "valid-identity"},
    ]
    assert _valid_canonical_address_records(pending_by_key, canonical_results) == [
        ("valid", *address_values, UUID(key), "valid-identity")
    ]
    canonical_results[-1]["address_key"] = "malformed"
    with pytest.raises(ValueError):
        _valid_canonical_address_records(pending_by_key, canonical_results)


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", ["batch", "retained"])
async def test_valid_canonical_address_collisions_still_refuse(monkeypatch, collision):
    import sys
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process.network_fhir_typed_contract import ADDRESS_FIELDS, TypedFHIRGraphError
    from process.network_fhir_typed_source import _canonical_address_records

    key = str(UUID(int=1))
    canonical_results = [{"address_key": key, "identity_key": "first"}]
    address_rows = [{"raw_key": "first", **dict.fromkeys(ADDRESS_FIELDS)}]
    retained_rows = [{"address_key": UUID(key), "canonical_identity": "different"}]
    if collision == "batch":
        canonical_results.append({"address_key": key, "identity_key": "different"})
        address_rows.append({"raw_key": "second", **dict.fromkeys(ADDRESS_FIELDS)})
        retained_rows = []
    monkeypatch.setitem(
        sys.modules, "ptg2_address_canon", SimpleNamespace(canonicalize_batch=lambda _rows: canonical_results)
    )
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[[], retained_rows]))
    stage = SimpleNamespace(relation=lambda _kind: "pg_temp.addresses")
    with pytest.raises(TypedFHIRGraphError, match="canonical_collision"):
        await _canonical_address_records(connection, stage, address_rows, ",".join(ADDRESS_FIELDS))


def _mixed_typed_summaries():
    """Build both accounting formats through the actual inventory validators."""
    from tests.test_registry_source_inventory_preparation import _derivation, _descriptor

    typed, legacy = _recipe(), _example()
    contribution, derivation = _metadata(typed)
    legacy_contribution = _descriptor(legacy, anchored=True)
    legacy_derivation = _derivation(legacy, legacy_contribution, facts=True)
    preparation = SourceInventoryPreparation((typed, legacy))
    contributions = sorted((contribution, legacy_contribution), key=lambda row: row["recipe_sha256"])
    derivations = sorted((derivation, legacy_derivation), key=lambda row: row["recipe_sha256"])
    for document in contributions:
        preparation.add_contribution(document)
    for document in derivations:
        preparation.add_derivation(document)
    reference, _ = preparation.finish()
    return reference.root, contributions, derivations


@pytest.mark.parametrize("offices", [False, True])
@pytest.mark.parametrize("damage", ["boolean-version", "unknown-version", "empty", "count", "duplicate"])
def test_mixed_summary_refuses_legacy_version_or_census_substitution(offices, damage):
    root, contributions, derivations = _mixed_typed_summaries()
    summary = typed_office_summary(root, derivations) if offices else typed_contribution_summary(root, contributions)
    assert validate_typed_summary(summary, root, offices=offices) == summary
    changed = deepcopy(summary)
    legacy = changed["legacy"][0]
    if damage == "duplicate":
        changed["legacy"].append(deepcopy(legacy))
    elif damage in {"boolean-version", "unknown-version"}:
        legacy["version"] = True if damage == "boolean-version" else 99
    else:
        legacy["source_count"] = 0 if damage == "empty" else 2
    with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
        validate_typed_summary(changed, root, offices=offices)
    assert validate_typed_summary(summary, root, offices=offices) == summary


@pytest.mark.parametrize(
    "field,value", [("revision", True), ("inventory_entries_sha256", "0" * 64), ("source_count", 1)]
)
def test_typed_summary_cannot_rebind_its_inventory_root(field, value):
    root, contributions, _ = _mixed_typed_summaries()
    summary = typed_contribution_summary(root, contributions)
    with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
        validate_typed_summary(summary | {field: value}, root)
    for selected_root, offices in ((asdict(root), False), (root, 1)):
        with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
            validate_typed_summary(summary, selected_root, offices=offices)


def test_typed_summary_requires_complete_unique_inventory_documents():
    root, contributions, derivations = _mixed_typed_summaries()
    for summarize, documents in ((typed_contribution_summary, contributions), (typed_office_summary, derivations)):
        summarize(root, documents)
        for selected in ([], documents[:1], documents[::-1], [documents[0], documents[0]]):
            with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
                summarize(root, selected)
        with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
            summarize(asdict(root), documents)


@pytest.mark.parametrize("legacy", [{}, None, [None] * 4])
def test_typed_summary_refuses_a_noncanonical_or_unbounded_legacy_roster(legacy):
    root, contributions, _ = _mixed_typed_summaries()
    summary = typed_contribution_summary(root, contributions)
    with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
        validate_typed_summary(summary | {"legacy": legacy}, root)


def _two_typed_office_derivations():
    """Bind distinct retained captures before comparing their office context."""
    first = _recipe()
    anchor = replace(first.source_pin.anchor, capture_id=str(UUID(int=6)))
    capture = deepcopy(first.source_pin.capture)
    capture.pop("capture_sha256")
    capture.update(capture_id=anchor.capture_id, schema_name=PREFIX + UUID(anchor.capture_id).hex)
    capture["capture_sha256"] = digest(capture)
    second = replace(first, source_pin=replace(first.source_pin, capture=capture, anchor=anchor))
    preparation, documents = SourceInventoryPreparation((first, second)), []
    for recipe in (first, second):
        contribution, derivation = _metadata(recipe)
        preparation.add_contribution(contribution)
        preparation.add_derivation(derivation)
        documents.append(derivation)
    reference, _ = preparation.finish()
    return reference.root, sorted(documents, key=lambda row: row["recipe_sha256"])


@pytest.mark.parametrize(
    "field", ["approved_revision", "approved_generation_sha256", "address_source", "address_identity"]
)
def test_typed_office_inventory_requires_one_current_approval_and_native_address_context(field):
    root, documents = _two_typed_office_derivations()
    assert typed_office_summary(root, documents)["typed_source_count"] == 2
    second = documents[1]
    if field == "approved_revision":
        second[field] += 1
    elif field == "approved_generation_sha256":
        second[field] = "0" * 64
    elif field == "address_source":
        second[field]["generation_id"] = "0" * 64
    else:
        second[field]["table_oid"] += 1
    validate_typed_derivation(second)
    with pytest.raises(ValueError, match="registry_typed_metadata_invalid"):
        typed_office_summary(root, documents)


@pytest.mark.parametrize("start,end", [("2026/10/01", None), (None, "2026-10-01 trailing")])
def test_typed_period_refuses_noncanonical_dates(start, end):
    from process.network_fhir_typed_source import _is_period_allowed

    assert not _is_period_allowed(start, end, "2026-10-01")


@pytest.mark.asyncio
@pytest.mark.parametrize("oversized", [False, True])
async def test_canonicalization_bounds_pages_and_reuses_exact_existing_addresses(monkeypatch, oversized):
    import sys
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from process.network_fhir_typed_contract import ADDRESS_FIELDS, TypedFHIRGraphError
    from process.network_fhir_typed_source import _canonicalize

    native = Mock(side_effect=AssertionError("known address was recanonicalized"))
    monkeypatch.setitem(sys.modules, "ptg2_address_canon", SimpleNamespace(canonicalize_batch=native))
    address_by_field = {"resource_id": "location-one", "raw_key": "raw-one", **dict.fromkeys(ADDRESS_FIELDS)}
    sizes = [9 * 1024**2] * 11 if oversized else [9 * 1024**2, 1, 0]
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=sizes),
        fetch=AsyncMock(side_effect=[[address_by_field], [address_by_field], []]),
        copy_records_to_table=AsyncMock(),
    )
    stage = SimpleNamespace(relation=lambda kind: "pg_temp." + kind, names={"addresses": "addresses"})
    if oversized:
        with pytest.raises(TypedFHIRGraphError, match="canonical_bounds"):
            await _canonicalize(connection, stage)
        connection.fetch.assert_not_awaited()
        assert connection.fetchval.await_args.args[-1] == 1
    else:
        await _canonicalize(connection, stage)
        assert [call.args[-2:] for call in connection.fetchval.await_args_list] == [
            ("", 1000),
            ("", 500),
            ("location-one", 500),
        ]
    native.assert_not_called()
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_absent_optional_references_and_empty_projection_do_not_invent_rows():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process.network_fhir_typed_source import _project_page, _refs

    assert _refs({}, "network") == _refs({"network": None}, "network") == []
    connection = SimpleNamespace(execute=AsyncMock())
    await _project_page(connection, object(), [])
    connection.execute.assert_not_awaited()
