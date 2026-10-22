# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject malformed scoped inputs before aggregation or an unscoped read."""

import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import provider_detail_network_read as detail
from api import provider_profile_scoped_resources as resources
from api import provider_profile_scoped_sql as scoped_sql
from api.endpoint import npi as provider
from api.network_address_scope import current_network_address_scope, network_address_read_scope
from api.provider_detail_network_contract import parse_managed_detail_query
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests.test_provider_profile_network_read import address_manifest
from tests.test_provider_profile_scoped_resources import NPI, _association, _graph
from tests.test_provider_profile_scoped_sql import _arguments


@pytest.mark.parametrize("field,invalid", [("practitioner_ref", 42), ("npi", True), ("accepting_medicaid", "false")])
def test_malformed_role_identity_refuses_without_changing_the_source_graph(field, invalid):
    graph = _graph()
    graph[1]["payload_json"][field] = invalid
    before = copy.deepcopy(graph)
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        resources.project_scoped_profile_resources(graph, NPI, [_association()])
    assert graph == before


@pytest.mark.parametrize("failure", ["open-association", "unlinked-office", "unlinked-network", "missing-endpoint"])
def test_unproved_association_cannot_return_partial_profile_facts(failure):
    graph_resources, association = _graph(), _association()
    if failure == "open-association":
        association["source_id"] = "another-source"
    elif failure == "unlinked-office":
        graph_resources[1]["payload_json"]["location_refs"] = ["Location/b"]
    elif failure == "unlinked-network":
        association["plan_id"] = None
        graph_resources[1]["payload_json"]["network_refs"] = ["Organization/y"]
    else:
        graph_resources = [entry for entry in graph_resources if entry["resource_type"] != "Endpoint"]
    before = copy.deepcopy((graph_resources, association))
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        resources.project_scoped_profile_resources(graph_resources, NPI, [association])
    assert (graph_resources, association) == before


def test_numeric_provider_identity_cannot_bypass_scoped_resource_validation():
    with pytest.raises(ValueError, match="profile_scoped_resources_invalid"):
        resources.project_scoped_profile_resources(_graph(), int(NPI), [_association()])


def _assert_projection_refused(arguments):
    before = copy.deepcopy(arguments)
    with pytest.raises(ValueError, match="scoped_profile_projection_invalid"):
        scoped_sql.build_scoped_profile_sql(**arguments)
    assert arguments == before


@pytest.mark.parametrize("failure", ["resource-container", "resource-shape", "resource-kind", "plan-name"])
def test_invalid_resource_never_produces_profile_sql(failure):
    arguments = _arguments()
    projection = arguments["projection"]
    bundle = projection["bundles"][0]
    if failure == "resource-container":
        projection["independent_resources"] = tuple(projection["independent_resources"])
    elif failure == "resource-shape":
        projection["independent_resources"][0]["extra"] = True
    elif failure == "resource-kind":
        projection["independent_resources"][0]["resource_type"] = "Patient"
    else:
        bundle["resources"][-1]["payload_json"]["name"] = ["another-plan"]
    _assert_projection_refused(arguments)


@pytest.mark.parametrize("failure", ["bundle-shape", "association-id"])
def test_invalid_bundle_never_produces_profile_sql(failure):
    arguments = _arguments()
    bundle = arguments["projection"]["bundles"][0]
    if failure == "bundle-shape":
        bundle["extra"] = True
    else:
        bundle["association"]["location_resource_id"] = "Location/office-a"
    _assert_projection_refused(arguments)


def test_aggregate_resource_limit_prevents_profile_sql():
    arguments = _arguments()
    arguments["projection"]["independent_resources"].extend(
        {"resource_type": "Location", "resource_id": f"extra-{index}", "payload_json": {}} for index in range(4995)
    )
    _assert_projection_refused(arguments)


@pytest.mark.parametrize("failure", ["source-identity", "row-type"])
def test_invalid_source_binding_never_produces_profile_sql(failure):
    arguments = _arguments()
    if failure == "source-identity":
        arguments["source"]["endpoint_id"] = ""
    else:
        arguments["row_type_refs"]["Practitioner"] = ["fixture", "provider_directory_practitioner"]
    _assert_projection_refused(arguments)


@pytest.fixture
def no_detail_reads(monkeypatch):
    """Only forbidden transport seams are replaced; no successful authority is supplied."""
    snapshot = Mock(side_effect=AssertionError("refused input opened a Reader"))
    native = AsyncMock(side_effect=AssertionError("refused input reached native detail"))
    fallback = AsyncMock(side_effect=AssertionError("refused input reached unscoped profile"))
    plans = AsyncMock(side_effect=AssertionError("refused input reached plan counts"))
    cache = Mock()
    cache.get.side_effect = AssertionError("refused input read the detail cache")
    monkeypatch.setattr(detail, "provider_profile_read_snapshot", snapshot)
    monkeypatch.setattr(provider, "read_npi_detail", native)
    monkeypatch.setattr(provider, "_fetch_provider_directory_profile_map", fallback)
    monkeypatch.setattr(detail, "read_registry_provider_plans", plans)
    monkeypatch.setattr(provider, "_NPI_DETAIL_RESPONSE_CACHE", cache)
    yield
    snapshot.assert_not_called()
    native.assert_not_called()
    fallback.assert_not_called()
    plans.assert_not_called()
    assert cache.mock_calls == []
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["selector-type", "selector-order", "duplicate-option", "policy-type"])
async def test_forged_detail_authority_stops_before_reader_or_cached_fallback(no_detail_reads, forgery):
    query = parse_managed_detail_query({"npi": NPI, "include": ["plans"]})
    policy_map = None
    if forgery == "selector-type":
        query = replace(query, selector=[42])
    elif forgery == "selector-order":
        query = replace(query, selector=(84, 42))
    elif forgery == "duplicate-option":
        query = replace(query, native_args=(("address_limit", "5"), ("address_limit", "5")))
    else:
        policy_map = {"excluded_company_ids": []}
    with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
        await detail.read_managed_provider_detail(
            object(), query, excluded_network_ids=(), access_scope_sha256="d" * 64, organization_policy=policy_map
        )


@pytest.mark.asyncio
async def test_shared_scope_refuses_untyped_policy_before_any_count_or_connection(no_detail_reads):
    session = SimpleNamespace(connection=AsyncMock(side_effect=AssertionError("unexpected connection")))
    with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
        async with detail.managed_provider_network_read_scope(
            session,
            selector=(42,),
            network_generation=9,
            excluded_network_ids=(),
            access_scope_sha256="d" * 64,
            organization_policy={"excluded_group_ids": []},
        ):
            pytest.fail("untyped policy installed a scope")
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["manifest-object", "exclusions"])
async def test_detail_composition_cannot_cross_the_bound_scope(no_detail_reads, change):
    manifest = address_manifest()
    session = SimpleNamespace(connection=AsyncMock(side_effect=AssertionError("unexpected connection")))
    query = parse_managed_detail_query({"npi": NPI, "include": ["plans"]})
    other_manifest = replace(manifest) if change == "manifest-object" else manifest
    exclusions = (7,) if change == "manifest-object" else (8,)
    with network_address_read_scope(manifest, None, excluded_network_ids=(7,), access_scope_sha256="d" * 64):
        with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
            await detail.compose_managed_provider_detail(
                object(), query, session, other_manifest, excluded_network_ids=exclusions
            )
    session.connection.assert_not_awaited()


def test_absence_proof_cannot_be_rebound_to_an_equal_but_different_manifest(no_detail_reads):
    manifest = address_manifest()
    with network_address_read_scope(manifest, (42,), access_scope_sha256="d" * 64):
        with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
            detail.ManagedProviderDetailAbsent(replace(manifest), NPI)
