# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Freeze actual optional recipe semantics and keep input reads on their caller transaction."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_receipt as rows
from process import provider_directory_cms_native_inputs as existing_inputs
from process import provider_directory_ordinary_facility_inputs as inputs


def _relations():
    return {
        name: dict(
            schema="fixture",
            name=name,
            relation_oid=ordinal,
            kind="r",
            persistence="p",
            row_security=False,
            relfilenode=ordinal,
            owner_oid=11,
            address_key=True,
            medicare_ccn=True,
            protected_owner=False,
        )
        for ordinal, name in enumerate(inputs._NAMES, 20)
    }


@pytest.mark.parametrize("inference", [False, True])
async def test_disabled_facility_preserves_existing_inference_inputs(monkeypatch, inference):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES", "false")
    monkeypatch.setattr(inputs, "_relations", AsyncMock(return_value=_relations()))
    semantics = AsyncMock(return_value={"enrollment": {}})
    monkeypatch.setattr(inputs, "_semantic_inputs", semantics)
    recipe, observed = await inputs.capture_facility_inputs(object(), "fixture", {}, inference_enabled=inference)
    assert recipe["enabled"] is False
    assert observed["inference_available"] is inference
    assert set(observed["relations"]) == (set(inputs._NPI[:3]) | {"nucc_taxonomy"} if inference else set())
    assert set(semantics.await_args.args[2]) == set(observed["relations"])


async def test_enabled_facility_binds_existing_flags_effective_options_and_limit(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES", "true")
    monkeypatch.setenv("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES", "true")
    monkeypatch.setenv("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER", "true")
    monkeypatch.setenv("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT", "17")
    monkeypatch.setattr(inputs, "_relations", AsyncMock(return_value=_relations()))
    monkeypatch.setattr(inputs, "_semantic_inputs", AsyncMock(return_value={"enrollment": {}}))
    recipe, observed = await inputs.capture_facility_inputs(object(), "fixture", {})
    assert recipe["enabled"] and recipe["include_nppes"] and recipe["include_other_identifier"]
    assert recipe["candidate_limit"] == 17 and all(recipe["options"].values())
    assert set(observed["relations"]) == set(inputs._NAMES)


@pytest.mark.parametrize("registered", [False, True])
async def test_legacy_enrollment_uses_exact_existing_revision_or_shared_content_identity(monkeypatch, registered):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[True, registered]))
    revision = AsyncMock(return_value=[{"relation_oid": 20, "revision": 3}])
    content = AsyncMock(return_value=(4, "ab" * 32))
    monkeypatch.setattr(existing_inputs, "_revision_guards", revision)
    monkeypatch.setattr(rows, "_row_identity", content)
    name = inputs._ENROLLMENT[0]
    observed = await inputs._enrollment_identity(session, "fixture", {name: _relations()[name]})
    if registered:
        assert observed[name] == {"revision": revision.return_value}
        content.assert_not_awaited()
    else:
        assert observed[name] == {"row_count": 4, "row_sha256": "ab" * 32}
        revision.assert_not_awaited()


@pytest.mark.parametrize("changed", [None, "recipe", "rows"])
async def test_generator_requires_matching_semantics_after_lock_on_same_transaction(monkeypatch, changed):
    observed_by_field = {"inference_available": False, "relations": {}, "identities": {"enrollment": {}}}
    recipe_by_field = {"enabled": False, "options": {}}
    capture = SimpleNamespace(
        affected_scope={"facility_recipe": recipe_by_field, "inference_enabled": False},
        predecessor={"native": {"facility_inputs": observed_by_field}},
    )
    session = SimpleNamespace(in_transaction=lambda: True)
    events = []

    async def lock(actual, schema, captured):
        assert actual is session and captured is capture and schema == "fixture"
        events.append("lock")

    async def observe(actual, schema, _selected, **_options):
        assert actual is session and events == ["lock"] and schema == "fixture"
        current_recipe, current = deepcopy(recipe_by_field), deepcopy(observed_by_field)
        if changed == "recipe":
            current_recipe["enabled"] = True
        if changed == "rows":
            current["identities"]["enrollment"] = {"changed": True}
        events.append("observe")
        return current_recipe, current

    monkeypatch.setattr(inputs, "_lock_inputs", lock)
    monkeypatch.setattr(inputs, "capture_facility_inputs", observe)
    if changed:
        with pytest.raises(RuntimeError, match="facility_inputs_changed"):
            await inputs.require_facility_inputs(session, "fixture", capture)
    else:
        await inputs.require_facility_inputs(session, "fixture", capture)
    assert events == ["lock", "observe"]
