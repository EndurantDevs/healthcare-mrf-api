# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allowed evidence stays source-qualified and readiness cannot restore denied data."""

import json
import sqlite3
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_allowed_amount_policy as allowed
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_em_provider_inputs
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.test_provider_allowed_amount_policy import HeldInputs, policy_fixture, route_fixture
from tests.test_provider_group_plan_network_read import held_scope
from tests.test_registry_allowed_plan_authority import allowed_case


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "duplicate", "foreign", "not_published"])
async def test_snapshot_census_must_match_original_allowed_contributors(policy_fixture, damage):
    held = policy_fixture
    snapshots = held.batches[0]
    if damage == "missing":
        snapshots.pop()
    elif damage == "duplicate":
        snapshots.append(deepcopy(snapshots[0]))
    elif damage == "foreign":
        snapshots[0] = {**snapshots[0], "snapshot_id": "unrelated-snapshot"}
    else:
        snapshots[0] = {**snapshots[0], "status": "superseded"}
    with pytest.raises(PlanPricingProjectionUnavailable):
        await allowed.admitted_allowed_amount_bindings(held.session, held.selection)
    held.ready.assert_not_awaited()


@pytest.mark.asyncio
async def test_unready_public_contributor_does_not_reintroduce_denied_source(policy_fixture):
    held = policy_fixture
    held.ready.return_value = False
    original = held.selection.bindings
    with pytest.raises(PlanPricingProjectionUnavailable):
        await allowed.admitted_allowed_amount_bindings(held.session, held.selection)
    held.ready.assert_awaited_once_with(held.session, held.public)
    assert held.selection.bindings is original


def test_allowed_review_cannot_substitute_a_different_physical_source(monkeypatch):
    _, review, manifest, _ = allowed_case()
    command = review["document"]["command"]
    references = allowed._allowed_review_commands(json.dumps([review]))
    snapshots_by_id = {command["published_identity"]["snapshot_id"]: {"manifest": manifest}}
    monkeypatch.setattr(allowed.source_policy, "PTG2_SCHEMA", command["source"]["ptg_schema_name"])
    reviewed, eligible = allowed._mapped_allowed_units(
        [], references, snapshots_by_id, (), organization_policy=RegistryOrganizationVisibility()
    )
    assert reviewed == {
        (
            command["published_identity"]["source_key"],
            command["published_identity"]["snapshot_id"],
            command["plan_id"],
            command["plan_market_type"],
        )
    }
    assert not eligible
    monkeypatch.setattr(allowed.source_policy, "PTG2_SCHEMA", "different_physical_source")
    with pytest.raises(ValueError, match="source identity"):
        allowed._mapped_allowed_units(
            [], references, snapshots_by_id, (), organization_policy=RegistryOrganizationVisibility()
        )


@pytest.mark.asyncio
async def test_office_inputs_pin_exact_manifest_and_propagate_dependency_failure(monkeypatch):
    driver = object()
    raw = SimpleNamespace(driver_connection=driver)
    session = SimpleNamespace(
        connection=AsyncMock(return_value=SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw)))
    )
    inputs = object()
    resolver = AsyncMock(return_value=inputs)
    monkeypatch.setattr(network_em_provider_inputs, "resolve_em_provider_inputs", resolver)
    assert await allowed.allowed_office_inputs(session) is None
    session.connection.assert_not_awaited()
    with held_scope() as manifest:
        assert await allowed.allowed_office_inputs(session) is inputs
        resolver.assert_awaited_once_with(driver, manifest)
        resolver.side_effect = PlanPricingProjectionUnavailable("held dependency changed")
        with pytest.raises(PlanPricingProjectionUnavailable, match="dependency"):
            await allowed.allowed_office_inputs(session)


@pytest.mark.parametrize("damage", ["missing", "list", "foreign_source", "foreign_snapshot"])
def test_public_wire_cannot_forge_private_contributor_coordinates(policy_fixture, damage):
    held = policy_fixture
    binding = held.public
    supplied_by_kind = {
        "missing": {},
        "list": {"_managed_allowed_bindings": [binding]},
        "foreign_source": {"_managed_allowed_bindings": (replace(binding, source_key="unrelated-source"),)},
        "foreign_snapshot": {"_managed_allowed_bindings": (replace(binding, snapshot_id="unrelated-snapshot"),)},
    }
    with pytest.raises(PlanPricingProjectionUnavailable):
        allowed.allowed_contributors(held.selection, supplied_by_kind[damage])
    held.ready.assert_not_awaited()


@pytest.mark.asyncio
async def test_unmanaged_helpers_preserve_original_selection_without_authority_reads(monkeypatch):
    from tests.test_pricing_api import _allowed_only_release_selection

    monkeypatch.setattr(allowed, "current_network_address_scope", lambda: None)
    selection = _allowed_only_release_selection()
    session = SimpleNamespace(connection=AsyncMock(side_effect=AssertionError("unexpected managed authority read")))
    assert await allowed.admitted_allowed_amount_bindings(session, selection) == selection.allowed_amount_bindings
    assert allowed.allowed_contributors(selection, {}) == selection.allowed_amount_bindings
    assert allowed.unavailable_route() is None
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_combined_zip_and_radius_keep_retained_office_policy_before_paging(route_fixture):
    from tests.test_pricing_api import FakeResult, FakeSession, _build_allowed_amount_page_row, pricing_module

    session = FakeSession([FakeResult([_build_allowed_amount_page_row(total=0)])])
    with held_scope() as manifest:
        result = await pricing_module._search_ptg_allowed_amount_evidence(
            session,
            {
                "plan_release_id": route_fixture.selection.plan_release_id,
                "code": "99214",
                "zip5": "00002",
                "lat": "0",
                "long": "0",
                "radius_miles": "5",
            },
            SimpleNamespace(limit=1, offset=0, page=1),
        )
    assert result["pagination"]["total"] == 0
    statement, parameters = session.executions[0][0]
    query = str(statement)
    assert parameters["allowed_geo_lat"] == parameters["allowed_geo_long"] == 0
    assert parameters["allowed_geo_radius_miles"] == 5 and parameters["allowed_zip5"] == "00002"
    assert f'"{manifest.schema_name}".entity_address_unified' in query
    assert query.index("addr.lat IS NULL AND addr.long IS NULL") < query.index("LIMIT :limit")
    assert "_network_excluded_ids" in query and parameters["_network_excluded_ids"] == [7]


@pytest.mark.parametrize("predicate", ["TRUE", "addr.zip5='00002'"])
def test_zip_only_office_survives_missing_polygon_reference(predicate):
    with sqlite3.connect(":memory:") as connection:
        connection.create_function("BTRIM", 1, str.strip)
        connection.executescript("""ATTACH DATABASE ':memory:' AS held_inputs;
          CREATE TABLE held_inputs.geo_zip_lookup(zip_code text,state text,state_name text);
          CREATE TABLE held_inputs.tiger_zip_state(zip text,stusps text);
          CREATE TABLE held_inputs.tiger_zcta5(zcta5ce text,the_geom text);
          INSERT INTO held_inputs.geo_zip_lookup VALUES('00002','IL','Illinois');
          INSERT INTO held_inputs.tiger_zip_state VALUES('00002','IL');""")
        joins = allowed.allowed_geo_joins(HeldInputs(), "mrf")
        assert connection.execute(f"""SELECT addr.zip5,allowed_zcta_ref.zcta5ce
          FROM (SELECT '00002' AS zip5,'IL' AS state_name,'IL' AS state_code,NULL AS lat,NULL AS long) addr
          {joins} WHERE {predicate}""").fetchall() == [("00002", None)]

    exact_zip = allowed.allowed_spatial_predicate(HeldInputs(), "mrf", "addr.zip5='00002'", [])
    assert "addr.lat IS NULL AND addr.long IS NULL" in exact_zip
    radius = allowed.allowed_spatial_predicate(HeldInputs(), "mrf", None, ["distance<5"])
    assert "addr.lat IS NOT NULL" in radius and "addr.long IS NOT NULL" in radius
    assert "public.ST_Covers(" in radius and "allowed_zcta_ref.the_geom" in radius
    assert "addr.lat IS NULL AND addr.long IS NULL" not in radius
