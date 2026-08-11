# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete next-frontier derivation and registration boundaries."""

from __future__ import annotations

import pytest

import process.provider_directory_rooted_graph_frontier as frontier
from process.provider_directory_rooted_graph_query import (
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_result_contract import (
    build_provider_directory_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    API_BASE,
    claim_for_query,
)


class _Database:
    def __init__(self, row: object) -> None:
        self.row = row

    async def first(self, *_args, **_kwargs):
        return self.row


def _organization_result():
    query = build_rooted_graph_direct_read(
        api_base=API_BASE,
        resource_type="Organization",
        resource_id="organization.synthetic-1",
    )
    claim = claim_for_query(query)
    result = build_provider_directory_rooted_graph_query_result(
        claim,
        [
            {
                "resourceType": "Organization",
                "id": "organization.synthetic-1",
            }
        ],
    )
    return claim, result


def _endpoint_result():
    query = build_rooted_graph_direct_read(
        api_base=API_BASE,
        resource_type="Endpoint",
        resource_id="endpoint.synthetic-1",
    )
    claim = claim_for_query(query)
    result = build_provider_directory_rooted_graph_query_result(
        claim,
        [{"resourceType": "Endpoint", "id": "endpoint.synthetic-1"}],
    )
    return claim, result


@pytest.mark.asyncio
async def test_acquisition_api_base_requires_exact_building_row() -> None:
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await frontier._acquisition_api_base(_Database({}), "pdrga_" + "0" * 48)


def test_organization_resource_derives_affiliation_query() -> None:
    claim, result = _organization_result()
    specs = frontier._derived_work_specs(claim, result, API_BASE)
    assert len(specs) == 1
    assert specs[0].resource_type == "OrganizationAffiliation"
    assert specs[0].reference_id == "organization.synthetic-1"


def test_role_reference_derives_direct_read_and_ignores_role_resource() -> None:
    query = build_provider_directory_practitioner_role_query(
        API_BASE,
        "practitioner.synthetic-1",
    )
    claim = claim_for_query(query)
    result = build_provider_directory_rooted_graph_query_result(
        claim,
        [
            {
                "resourceType": "PractitionerRole",
                "id": "role.synthetic-1",
                "practitioner": {"reference": "Practitioner/practitioner.synthetic-1"},
                "organization": {"reference": "Organization/organization.synthetic-1"},
            }
        ],
    )

    specs = frontier._derived_work_specs(claim, result, API_BASE)
    assert len(specs) == 1
    assert specs[0].resource_type == "Organization"
    assert specs[0].reference_id == "organization.synthetic-1"


@pytest.mark.asyncio
async def test_terminal_without_reference_frontier_needs_no_database_read() -> None:
    claim, result = _endpoint_result()
    await frontier.register_rooted_graph_frontier(object(), claim, result)


@pytest.mark.asyncio
async def test_empty_defensive_derivation_skips_store_action(monkeypatch) -> None:
    claim, result = _organization_result()
    actions: list[object] = []

    async def fail_if_called(*args, **kwargs):
        actions.append((args, kwargs))

    monkeypatch.setattr(frontier, "_derived_work_specs", lambda *_args: ())
    monkeypatch.setattr(frontier, "set_store_action", fail_if_called)
    await frontier.register_rooted_graph_frontier(
        _Database({"canonical_api_base": API_BASE}),
        claim,
        result,
    )
    assert actions == []


@pytest.mark.asyncio
async def test_registration_sets_action_before_sorted_specs(monkeypatch) -> None:
    claim, result = _organization_result()
    events: list[tuple[str, object]] = []

    async def record_action(_database, action, *_args):
        events.append(("action", action))

    async def record_spec(_database, _acquisition_id, spec):
        events.append(("spec", spec.query_id))

    monkeypatch.setattr(frontier, "set_store_action", record_action)
    monkeypatch.setattr(frontier, "insert_work_spec", record_spec)
    await frontier.register_rooted_graph_frontier(
        _Database({"canonical_api_base": API_BASE}),
        claim,
        result,
    )
    assert events[0] == ("action", "derive")
    assert events[1][0] == "spec"
