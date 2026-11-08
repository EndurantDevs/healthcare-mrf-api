# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Versioned office summaries preserve old bytes and bind each current derivation."""

import copy
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import registry_initial_source_composition as composition
from process import registry_source_fact_offices as facts
from process import registry_source_office_summary_v3 as summary
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)
from tests.test_registry_source_contributions import _recipe


def _derivation(identity="a" * 64, *, mode="facts"):
    return {
        "recipe_sha256": identity,
        "mode": mode,
        "mapping_sha256": "b" * 64,
        "physical_source_sha256": "c" * 64,
        "fact_artifact": {
            "fact_candidate_id": str(uuid4()),
            "fact_table_oid": 42,
            "fact_seal_sha256": "d" * 64,
            "capture_sha256": "e" * 64,
            "transform_version": "generic-fhir-source-facts-v1",
            "fact_sha256": "f" * 64,
            "fact_rows": 3,
        }
        if mode == "facts"
        else None,
        "office_rows": 2,
        "page_count": 1,
        "correspondence_sha256": "0" * 64,
    }


def _summary(derivations=None, origin=None):
    return summary.office_summary_v3(
        "a" * 64,
        SimpleNamespace(approved_revision=7, generation_id="b" * 64),
        SimpleNamespace(generation_id="c" * 64),
        [_derivation()] if derivations is None else derivations,
        origin,
    )


@pytest.mark.parametrize("revision", [1, 2])
def test_old_receipt_shape_digest_and_input_bound_are_unchanged(revision):
    receipt_by_field = {
        "component": composition.SUMMARY_KEY,
        "revision": revision,
        "recipe_sha256": "a" * 64,
        "approved_revision": 7,
        "address_generation_sha256": "b" * 64,
        "recipe_count": 1,
        "office_rows": 2,
        "page_count": 1,
        "correspondence_sha256": "c" * 64,
        **({"origin_manifest_sha256": "d" * 64} if revision == 2 else {}),
    }
    encoded = summary.canonical(receipt_by_field)
    assert composition.validate_initial_source_office_receipt(receipt_by_field) == receipt_by_field
    assert (
        composition.initial_source_office_receipt_sha256(receipt_by_field)
        == hashlib.sha256(encoded.encode()).hexdigest()
    )
    assert composition.validate_initial_source_office_receipt(encoded + " " * (4096 - len(encoded))) == receipt_by_field
    for invalid in (encoded + " " * (4097 - len(encoded)), encoded[:-1] + ',"revision":1}'):
        with pytest.raises(composition.RegistryInitialSourceError, match="receipt_invalid"):
            composition.validate_initial_source_office_receipt(invalid)


@pytest.mark.parametrize("origin", [None, "d" * 64])
def test_v3_sorted_mixed_derivations_and_larger_bound(origin):
    document = _summary([_derivation("c" * 64), _derivation("a" * 64, mode="raw")], origin)
    assert [source["recipe_sha256"] for source in document["derivations"]] == ["a" * 64, "c" * 64]
    assert (document["recipe_count"], document["office_rows"], document["page_count"]) == (2, 4, 2)
    encoded = summary.canonical(document)
    assert composition.validate_initial_source_office_receipt(encoded + " " * (8192 - len(encoded))) == document
    with pytest.raises(composition.RegistryInitialSourceError, match="receipt_invalid"):
        composition.validate_initial_source_office_receipt(encoded + " " * (summary.MAX_V3_RECEIPT_BYTES + 1))


@pytest.mark.parametrize(
    "damage",
    [
        "extra",
        "revision_bool",
        "approved_bool",
        "origin",
        "all_raw",
        "duplicate",
        "unordered",
        "aggregate",
        "hash",
        "artifact_oid",
        "artifact_uuid",
        "artifact_transform",
        "raw_anchor",
        "overflow",
    ],
)
def test_v3_rejects_unbound_or_malformed_derivations(damage):
    document = _summary([_derivation("a" * 64), _derivation("b" * 64, mode="raw")])
    fact_derivation = document["derivations"][0]
    match damage:
        case "extra":
            document["unexpected"] = None
        case "revision_bool":
            document["revision"] = True
        case "approved_bool":
            document["approved_revision"] = True
        case "origin":
            document["origin_manifest_sha256"] = "A" * 64
        case "all_raw":
            fact_derivation.update(mode="raw", fact_artifact=None)
        case "duplicate":
            document["derivations"][1]["recipe_sha256"] = fact_derivation["recipe_sha256"]
        case "unordered":
            document["derivations"].reverse()
        case "aggregate":
            document["office_rows"] += 1
        case "hash":
            document["correspondence_sha256"] = "f" * 64
        case "artifact_oid":
            fact_derivation["fact_artifact"]["fact_table_oid"] = True
        case "artifact_uuid":
            fact_derivation["fact_artifact"]["fact_candidate_id"] = "not-a-uuid"
        case "artifact_transform":
            fact_derivation["fact_artifact"]["transform_version"] = "unsupported"
        case "raw_anchor":
            document["derivations"][1]["fact_artifact"] = fact_derivation["fact_artifact"]
        case _:
            fact_derivation["office_rows"] = 2**63 - 1
    with pytest.raises(composition.RegistryInitialSourceError, match="receipt_invalid"):
        composition.validate_initial_source_office_receipt(document)


def test_v3_candidate_verification_binds_approved_generation_origin_and_recipe_inventory():
    recipe = _recipe()
    canonical = canonical_registry_source_recipes((recipe,))
    identity = registry_source_recipes_sha256(canonical)
    document = _summary([_derivation(identity)], "d" * 64)
    document["recipe_sha256"] = identity
    candidate_by_field = {
        "approved_custom_revision": 7,
        "source_recipes_json": canonical,
        "source_generations": {
            RECIPE_DIGEST_KEY: identity,
            "custom_membership": document["approved_generation_sha256"],
            "unified_address": document["address_generation_sha256"],
            "retained_network_source": "d" * 64,
            composition.SUMMARY_KEY: composition.initial_source_office_receipt_sha256(document),
        },
        "validation_json": {composition.SUMMARY_KEY: document},
    }
    assert composition.verify_initial_source_office_receipt(candidate_by_field) == document
    for field in ("custom_membership", "retained_network_source", RECIPE_DIGEST_KEY):
        damaged = copy.deepcopy(candidate_by_field)
        damaged["source_generations"][field] = "e" * 64
        with pytest.raises(composition.RegistryInitialSourceError):
            composition.verify_initial_source_office_receipt(damaged)
    damaged = copy.deepcopy(candidate_by_field)
    damaged["validation_json"][composition.SUMMARY_KEY]["derivations"][0]["recipe_sha256"] = "f" * 64
    altered = damaged["validation_json"][composition.SUMMARY_KEY]
    altered["correspondence_sha256"] = hashlib.sha256(summary.canonical(altered["derivations"]).encode()).hexdigest()
    damaged["source_generations"][composition.SUMMARY_KEY] = composition.initial_source_office_receipt_sha256(altered)
    with pytest.raises(composition.RegistryInitialSourceError):
        composition.verify_initial_source_office_receipt(damaged)


def test_v3_receipt_does_not_widen_the_parent_candidate_custody_cap():
    from process import network_candidate_custody as custody
    from tests.test_network_candidate_custody import Connection

    candidate = Connection().candidate
    candidate["validation_json"] = {composition.SUMMARY_KEY: _summary()}
    assert custody._evidence(candidate)["validation_json"] == candidate["validation_json"]
    assert composition.MAX_RECEIPT_BYTES == 4096
    assert summary.MAX_V3_RECEIPT_BYTES == 128 * 1024
    assert custody.MAX_RECEIPT_BYTES == 2 * 1024 * 1024
    candidate["validation_json"]["oversized_other_component"] = "x" * custody.MAX_RECEIPT_BYTES
    with pytest.raises(custody.NetworkCandidateCustodyError):
        custody._evidence(candidate)


@pytest.mark.asyncio
async def test_v3_keeps_changed_recipes_raw_and_exact_facts(monkeypatch):
    recipe = _recipe()
    changed = replace(recipe, source_pin=replace(recipe.source_pin, as_of="2026-10-02"))
    identity = registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,)))
    changed_identity = registry_source_recipes_sha256(canonical_registry_source_recipes((changed,)))
    monkeypatch.setattr(composition, "validate_initial_source_recipes", lambda recipes: recipes)
    monkeypatch.setattr(composition, "_require_transaction", AsyncMock())
    selected = AsyncMock(return_value={identity: object()})
    monkeypatch.setattr(facts, "fact_office_artifacts", selected)
    capture = AsyncMock(return_value=_derivation(identity))
    monkeypatch.setattr(facts, "capture_fact_office_pages", capture)
    raw = AsyncMock(return_value=_derivation(changed_identity, mode="raw"))
    monkeypatch.setattr(composition, "_raw_office_derivation", raw)
    approved = SimpleNamespace(approved_revision=7, generation_id="b" * 64)
    address = SimpleNamespace(generation_id="c" * 64)
    request = composition.OfficeSummaryRequest(("reader",), "control", source_manifest=object())
    actual = await composition.prepare_source_offices(object(), (recipe, changed), approved, address, request)
    assert actual["revision"] == 3
    capture.assert_awaited_once()
    raw.assert_awaited_once()
    assert raw.await_args.args[1] == changed
    assert {source["mode"] for source in actual["derivations"]} == {"facts", "raw"}


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [1, 2])
async def test_explicit_old_versions_never_select_fact_artifacts(monkeypatch, revision):
    recipe = _recipe()
    monkeypatch.setattr(composition, "validate_initial_source_recipes", lambda recipes: recipes)
    monkeypatch.setattr(composition, "_require_transaction", AsyncMock())
    selected = AsyncMock(side_effect=AssertionError("old version must retain raw replay"))
    monkeypatch.setattr(facts, "fact_office_artifacts", selected)
    monkeypatch.setattr(composition, "_raw_recipe_offices", AsyncMock(return_value=(2, 1)))
    request = composition.OfficeSummaryRequest(
        ("reader",),
        "control",
        origin_manifest_sha256="d" * 64 if revision == 2 else None,
        source_manifest=object(),
        revision=revision,
    )
    actual = await composition.prepare_source_offices(
        object(),
        (recipe,),
        SimpleNamespace(approved_revision=7),
        SimpleNamespace(generation_id="c" * 64),
        request,
    )
    assert actual["revision"] == revision
    selected.assert_not_awaited()


@pytest.mark.asyncio
async def test_bad_selected_fact_authority_is_terminal_without_raw_fallback(monkeypatch):
    monkeypatch.setattr(composition, "validate_initial_source_recipes", lambda recipes: recipes)
    monkeypatch.setattr(composition, "_require_transaction", AsyncMock())
    monkeypatch.setattr(facts, "fact_office_artifacts", AsyncMock(side_effect=ValueError("bad_native_seal")))
    raw = AsyncMock()
    monkeypatch.setattr(composition, "_raw_recipe_offices", raw)
    request = composition.OfficeSummaryRequest(("reader",), "control", source_manifest=object())
    with pytest.raises(ValueError, match="bad_native_seal"):
        await composition.prepare_source_offices(object(), (_recipe(),), object(), object(), request)
    raw.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("revision,origin", [(True, None), (1, "d" * 64), (2, None), (4, None)])
async def test_explicit_version_must_be_exact_and_compatible_with_its_origin(monkeypatch, revision, origin):
    monkeypatch.setattr(composition, "validate_initial_source_recipes", lambda recipes: recipes)
    monkeypatch.setattr(composition, "_require_transaction", AsyncMock())
    raw = AsyncMock()
    monkeypatch.setattr(composition, "_raw_recipe_offices", raw)
    request = composition.OfficeSummaryRequest(("reader",), "control", origin_manifest_sha256=origin, revision=revision)
    with pytest.raises(composition.RegistryInitialSourceError, match="revision_invalid"):
        await composition.prepare_source_offices(object(), (_recipe(),), object(), object(), request)
    raw.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", [None, 3])
async def test_absent_matching_artifacts_allow_only_the_new_raw_default(monkeypatch, revision):
    monkeypatch.setattr(composition, "validate_initial_source_recipes", lambda recipes: recipes)
    monkeypatch.setattr(composition, "_require_transaction", AsyncMock())
    monkeypatch.setattr(facts, "fact_office_artifacts", AsyncMock(return_value={}))
    raw = AsyncMock(return_value=(2, 1))
    monkeypatch.setattr(composition, "_raw_recipe_offices", raw)
    request = composition.OfficeSummaryRequest(("reader",), "control", source_manifest=object(), revision=revision)
    if revision == 3:
        with pytest.raises(composition.RegistryInitialSourceError, match="fact_owner_unavailable"):
            await composition.prepare_source_offices(object(), (_recipe(),), object(), object(), request)
        raw.assert_not_awaited()
    else:
        actual = await composition.prepare_source_offices(
            object(),
            (_recipe(),),
            SimpleNamespace(approved_revision=7),
            SimpleNamespace(generation_id="c" * 64),
            request,
        )
        assert actual["revision"] == 1
