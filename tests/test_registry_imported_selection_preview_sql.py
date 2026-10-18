# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete CMS aggregate keeps source lineage and avoids wide resolved spooling."""

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_imported_selection_preview as preview
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, _extraction_sql


def _recipe():
    source = PinnedFHIRMembershipSource(
        "synthetic_source",
        "cms-npd",
        "endpoint-example",
        "dataset-example",
        "a" * 64,
        "b" * 64,
        1,
        "cms-npd",
        "2026-01-01",
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir", source.source_id, source.schema_name, source.dataset_id, "producer-example", "edition-example"
    )
    return SimpleNamespace(source_pin=source, binding_coordinates=coordinates)


def test_cms_aggregate_preserves_extractor_lineage_and_full_accounting():
    recipe = _recipe()
    extracted = _extraction_sql(recipe.source_pin, '"synthetic_registry"', reviewed=True, approved_only=True)
    aggregated = preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")
    # Compare every expansion, source-witness and resolution predicate against the existing extractor.
    expected_body = extracted.split("providers AS (", 1)[1].split(", resource_edges AS (", 1)[0]
    expected_body = expected_body.replace(preview._FHIR_EXPANDED_PAGE, "\n", 1)
    expected_body = re.sub(
        r"\$(\d+)", lambda match: "$" + str(int(match[1]) - (3 if int(match[1]) > 11 else 0)), expected_body
    )
    aggregate_body = aggregated.split("providers AS (", 1)[1].split("\n      SELECT (SELECT count(*) FROM page)", 1)[0]
    assert aggregate_body.replace("resolved AS NOT MATERIALIZED", "resolved AS MATERIALIZED") == expected_body
    assert aggregated.count("resolved AS NOT MATERIALIZED") == 1
    assert "page AS MATERIALIZED" in aggregated and "expanded AS MATERIALIZED" in aggregated
    assert "LIMIT" not in aggregated and "ORDER BY resource_type,resource_id" not in aggregated
    assert "encoded AS" not in aggregated and "input_json" not in aggregated
    assert "resource_documents AS" not in aggregated and "resource_accounting AS" not in aggregated
    assert max(map(int, re.findall(r"\$(\d+)", aggregated))) == 12
    assert "count(*) FILTER(WHERE NOT unresolved AND NOT omitted) AS mapped_rows" in aggregated
    assert "count(*) FILTER(WHERE omitted) AS omitted_rows" in aggregated
    assert "count(*) FILTER(WHERE unresolved AND NOT omitted) AS unresolved_rows" in aggregated
    assert "SELECT count(*) FROM page" in aggregated and "count(*) AS expanded_rows" in aggregated
    assert preview.PREVIEW_DEADLINE_SECONDS == 2.5


@pytest.mark.parametrize("change", ["duplicate", "missing", "already_inline"])
def test_cms_resolved_shape_drift_refuses_aggregation(monkeypatch, change):
    recipe = _recipe()
    extracted = _extraction_sql(recipe.source_pin, '"synthetic_registry"', reviewed=True, approved_only=True)
    marker = ", resolved AS MATERIALIZED ("
    if change == "duplicate":
        changed = extracted + marker
    elif change == "missing":
        changed = extracted.replace(marker, ", changed_resolution AS MATERIALIZED (")
    else:
        changed = extracted.replace(marker, ", resolved AS NOT MATERIALIZED (")
    monkeypatch.setattr(preview, "fhir_extraction_sql", lambda *_args, **_options: changed)
    with pytest.raises(preview.RegistryImportedSelectionUnavailable, match="selection_unavailable"):
        preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")


def test_aca_aggregate_retains_existing_materialization():
    recipe = SimpleNamespace(
        source_pin=None,
        binding_coordinates=RegistryNetworkSourceCoordinates(
            "aca", "source-example", "synthetic_source", "dataset-example", "producer-example", "edition-example"
        ),
    )
    aggregated = preview._impact_sql(recipe, '"synthetic_registry"', '"synthetic_selection"', "synthetic_registry")
    assert aggregated.count("resolved AS MATERIALIZED") == 1
    assert "resolved AS NOT MATERIALIZED" not in aggregated


def _retained_candidate():
    from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
    from process.registry_source_recipe_store import (
        RegistrySourceMembershipRecipe,
        canonical_registry_source_recipes,
        registry_source_recipes_sha256,
    )
    from tests.test_registry_source_retention import candidate_receipt, retention_receipt

    coordinates = RegistryNetworkSourceCoordinates(
        "ptg", "source-example", "synthetic_source", "dataset-example", "producer-example", "edition-example"
    )
    recipe = RegistrySourceMembershipRecipe(
        PinnedPTGOfficeMembershipSource(
            str(UUID(int=1)), "client-example", "a" * 64, "b" * 64, "c" * 64, coordinates, 7, 1
        ),
        coordinates,
    )
    encoded = canonical_registry_source_recipes((recipe,))
    digest = registry_source_recipes_sha256(encoded)
    candidate = candidate_receipt(
        retention_receipt(source_rows=2, add_rows=0, remove_rows=1, suppressed_source_rows=1, effective_rows=1)
    )
    candidate.update(schema_name="synthetic_candidate", approved_custom_revision=3, source_recipes_json=encoded)
    candidate["source_generations"].update(
        registry_source_recipes=digest, custom_membership="d" * 64, registry_source_membership="e" * 64
    )
    candidate["validation_json"]["registry_source_selection"] = dict(
        component="registry_source_selection",
        revision=1,
        selection_policy="approved-bindings-only",
        recipe_sha256=digest,
        recipe_count=1,
        approved_revision=3,
        approved_generation_sha256="d" * 64,
        stage_generation_sha256="e" * 64,
        mapped_rows=2,
        omitted_rows=1,
    )
    return candidate


@pytest.mark.asyncio
@pytest.mark.parametrize("unresolved", [0, 1, 2])
async def test_ptg_preview_counts_retained_source_before_custom_suppression(unresolved):
    candidate = _retained_candidate()
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            side_effect=[candidate, dict(source_rows=2, unresolved_rows=unresolved, binding_changed=False)]
        )
    )
    manifest = SimpleNamespace(
        candidate_id=str(UUID(int=1)), schema_name="synthetic_candidate", approved_custom_revision=3
    )
    result = await preview._retained_source_impact(
        connection, SimpleNamespace(expected_approved_revision=4), '"selected"', manifest, "synthetic_registry"
    )
    assert result == dict(status="available", mapped_rows=2 - unresolved, omitted_rows=1, unresolved_rows=unresolved)
    query, *arguments = connection.fetchrow.await_args.args
    assert '"synthetic_candidate".network_membership_source source' in query
    assert '"synthetic_candidate".network_membership ' not in query
    assert "EXCEPT" in query and "network.record_key=source.network_id::text" in query
    assert arguments == [4, 3]
    assert connection.fetchrow.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "missing", "retention", "selection", "count", "bool", "overflow"])
async def test_ptg_retained_preview_refuses_changed_mapping_or_unproven_accounting(change):
    candidate = _retained_candidate()
    counts_by_name = dict(source_rows=2, unresolved_rows=0, binding_changed=change == "binding")
    match change:
        case "missing":
            candidate = None
        case "retention":
            candidate["validation_json"]["registry_source_retention"]["source_rows"] = 9
        case "selection":
            candidate["validation_json"]["registry_source_selection"]["recipe_sha256"] = "f" * 64
        case "count":
            counts_by_name["source_rows"] = 1
        case "bool":
            counts_by_name["unresolved_rows"] = True
        case "overflow":
            counts_by_name["source_rows"] = preview.MAX_PREVIEW_COUNT + 1
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=[candidate, counts_by_name]))
    manifest = SimpleNamespace(
        candidate_id=str(UUID(int=1)), schema_name="synthetic_candidate", approved_custom_revision=3
    )
    call = preview._retained_source_impact(
        connection, SimpleNamespace(expected_approved_revision=3), '"selected"', manifest, "synthetic_registry"
    )
    if change == "binding":
        assert await call == {"status": "unavailable"}
    else:
        with pytest.raises((ValueError, preview.RegistryImportedSelectionUnavailable)):
            await call


@pytest.mark.asyncio
async def test_changed_ptg_impact_requires_full_source_witness_and_counts_exact_offices(monkeypatch):
    from process.registry_source_recipe_store import decode_registry_source_recipes

    recipe = decode_registry_source_recipes(_retained_candidate()["source_recipes_json"])[0]
    command = SimpleNamespace(expected_approved_revision=4)
    approved = object()
    authority = (object(), object(), object())
    monkeypatch.setattr(preview, "pin_approved_membership_source", AsyncMock(return_value=approved))
    witness = AsyncMock(
        return_value=SimpleNamespace(
            context=SimpleNamespace(scope_id=UUID(int=2), scope_approval_sha256="a" * 64),
            descriptor=SimpleNamespace(schema_name="retained_offices"),
            request=SimpleNamespace(input_row_count=2),
        )
    )
    monkeypatch.setattr(preview, "require_ptg_office_recipe", witness)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=dict(source_rows=2, unresolved_rows=1, has_company_authority=True))
    )
    actual = await preview._ptg_recipe_impact(connection, recipe, (command, '"selected"', "control", authority))
    assert actual == dict(mapped_rows=1, omitted_rows=0, unresolved_rows=1)
    witness.assert_awaited_once_with(connection, recipe, approved, "control", authority)
    sql, *parameters = connection.fetchrow.await_args.args
    assert '"retained_offices".office_assertion' in sql and "prospective" in sql
    assert parameters == [4, recipe.source_pin.network_id, UUID(int=2), "a" * 64]
    assert "prospective company" in sql and "prospective links" in sql
    connection.fetchrow.return_value["source_rows"] = 1
    with pytest.raises(preview.RegistryImportedSelectionUnavailable):
        await preview._ptg_recipe_impact(connection, recipe, (command, '"selected"', "control", authority))


@pytest.mark.asyncio
async def test_complete_ptg_archive_has_known_zero_imported_impact(monkeypatch):
    from process.registry_source_recipe_store import decode_registry_source_recipes

    recipes = decode_registry_source_recipes(_retained_candidate()["source_recipes_json"])
    monkeypatch.setattr(preview, "resolve_network_serving_manifest", AsyncMock(return_value=object()))
    monkeypatch.setattr(preview, "resolve_retained_registry_source_recipes", AsyncMock(return_value=recipes))
    monkeypatch.setattr(preview, "_retained_source_impact", AsyncMock(return_value={"status": "unavailable"}))
    selection = AsyncMock(return_value=())
    monkeypatch.setattr(preview, "resolve_ptg_office_recipe_selection", selection)
    driver = SimpleNamespace(fetchval=AsyncMock(return_value="repeatable read"), execute=AsyncMock())
    command = SimpleNamespace(expected_approved_revision=4)
    authority = (object(), object(), object())
    actual = await preview._retained_impact(driver, command, '"selected"', 1, "control", authority)
    assert actual == dict(status="available", mapped_rows=0, omitted_rows=0, unresolved_rows=0)
    assert selection.await_args.kwargs["office_authority"] is authority
    assert selection.await_args.kwargs["staging"] == '"selected"'


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["company", "company_links"])
async def test_company_authority_changes_cannot_use_raw_source_fast_path(monkeypatch, kind):
    from process.registry_source_recipe_store import decode_registry_source_recipes

    recipes = decode_registry_source_recipes(_retained_candidate()["source_recipes_json"])
    monkeypatch.setattr(preview, "resolve_network_serving_manifest", AsyncMock(return_value=object()))
    monkeypatch.setattr(preview, "resolve_retained_registry_source_recipes", AsyncMock(return_value=recipes))
    monkeypatch.setattr(
        preview, "_retained_source_impact", AsyncMock(return_value=dict(status="available", mapped_rows=2))
    )
    monkeypatch.setattr(preview, "resolve_ptg_office_recipe_selection", AsyncMock(return_value=recipes))
    full_proof = AsyncMock(
        side_effect=preview.RegistryImportedSelectionUnavailable("registry_imported_selection_unavailable")
    )
    monkeypatch.setattr(preview, "_ptg_recipe_impact", full_proof)
    driver = SimpleNamespace(fetchval=AsyncMock(return_value="repeatable read"), execute=AsyncMock())
    command = SimpleNamespace(expected_approved_revision=4, selection=({"record_kind": kind},))
    with pytest.raises(preview.RegistryImportedSelectionUnavailable):
        await preview._retained_impact(driver, command, '"selected"', 1, "control", None)
    full_proof.assert_awaited_once()
