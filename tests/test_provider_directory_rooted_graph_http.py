# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPError,
    fetch_provider_directory_rooted_graph_query,
    rebind_provider_directory_rooted_graph_query,
)
from process.provider_directory_rooted_graph_query import (
    build_insurance_plan_census_query,
    build_provider_directory_organization_affiliation_query,
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    API_BASE,
    FakeResponse,
    FakeSession,
    bundle,
    claim_for_query,
)


def role_claim():
    query = build_provider_directory_practitioner_role_query(
        API_BASE,
        "practitioner.synthetic-1",
    )
    return query, claim_for_query(query)


def direct_claim():
    query = build_rooted_graph_direct_read(
        api_base=API_BASE,
        resource_type="Organization",
        resource_id="organization.synthetic-1",
    )
    return query, claim_for_query(query)


def census_claim():
    query = build_insurance_plan_census_query(API_BASE)
    return query, claim_for_query(query, closure_scope="census")


def role_resource(resource_id: str = "role.synthetic-1") -> dict[str, object]:
    return {
        "resourceType": "PractitionerRole",
        "id": resource_id,
        "practitioner": {"reference": "Practitioner/practitioner.synthetic-1"},
    }


@pytest.mark.asyncio
async def test_exact_search_rebound_and_request_are_exact_and_bounded() -> None:
    query, claim = role_claim()
    session = FakeSession([FakeResponse(query.url, bundle([role_resource()]))])

    fetched_result = await fetch_provider_directory_rooted_graph_query(
        session,
        API_BASE,
        claim,
    )

    assert rebind_provider_directory_rooted_graph_query(API_BASE, claim) == query
    assert fetched_result.resources == (role_resource(),)
    assert fetched_result.advertised_total is None
    assert fetched_result.terminal_page_count == 1
    assert session.requests[0][0] == query.url
    request_options = session.requests[0][1]
    assert request_options["allow_redirects"] is False
    assert request_options["headers"] == {
        "Accept": "application/fhir+json",
        "Accept-Encoding": "identity",
    }
    assert request_options["timeout"].total == 30.0


@pytest.mark.asyncio
async def test_http_rejects_a_reference_hidden_outside_reviewed_paths() -> None:
    query, claim = direct_claim()
    hidden_reference_by_field = {
        "resourceType": "Organization",
        "id": "organization.synthetic-1",
        "endpoint": [{"reference": "Endpoint/reviewed.synthetic-1"}],
        "endpoint[0]": {"reference": "Organization/hidden.synthetic-1"},
    }

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([FakeResponse(query.url, hidden_reference_by_field)]),
            API_BASE,
            claim,
        )

    assert error_info.value.code == "response_invalid"


def test_rebound_rejects_any_claimed_identity_drift() -> None:
    _query, claim = role_claim()
    for changed_claim in (
        replace(claim, query_id="pdrgq_" + "0" * 48),
        replace(claim, query_identity_sha256="0" * 64),
    ):
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
            rebind_provider_directory_rooted_graph_query(API_BASE, changed_claim)
        assert error_info.value.code == "claim_rebound_invalid"


@pytest.mark.asyncio
async def test_affiliation_requires_exact_reference_and_match_mode() -> None:
    query = build_provider_directory_organization_affiliation_query(
        API_BASE,
        "network.synthetic-1",
    )
    claim = claim_for_query(query)
    valid_resource_by_field = {
        "resourceType": "OrganizationAffiliation",
        "id": "affiliation.synthetic-1",
        "participatingOrganization": {"reference": "Organization/network.synthetic-1"},
    }
    valid_result = await fetch_provider_directory_rooted_graph_query(
        FakeSession([FakeResponse(query.url, bundle([valid_resource_by_field]))]),
        API_BASE,
        claim,
    )
    assert valid_result.resources == (valid_resource_by_field,)

    invalid_responses = [
        bundle(
            [
                {
                    **valid_resource_by_field,
                    "participatingOrganization": {"reference": "Organization/other"},
                }
            ]
        ),
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "entry": [{"resource": valid_resource_by_field}],
        },
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "entry": [
                {
                    "search": {"mode": "include"},
                    "resource": valid_resource_by_field,
                }
            ],
        },
    ]
    for response_by_field in invalid_responses:
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
            await fetch_provider_directory_rooted_graph_query(
                FakeSession([FakeResponse(query.url, response_by_field)]),
                API_BASE,
                claim,
            )
        assert error_info.value.code == "response_invalid"


@pytest.mark.asyncio
async def test_sparse_census_continues_on_exact_collection_default_port_cursor() -> (
    None
):
    query, claim = census_claim()
    opaque_next = (
        "https://DIRECTORY.synthetic.test:443/fhir/R4/InsurancePlan?"
        "cursor=opaque%2Btoken&cursor=second"
    )
    plan_by_field = {
        "resourceType": "InsurancePlan",
        "id": "plan.synthetic-1",
    }
    session = FakeSession(
        [
            FakeResponse(query.url, bundle([], total=1, next_url=opaque_next)),
            FakeResponse(
                opaque_next,
                bundle([plan_by_field], total=1),
                response_url=(
                    "https://directory.synthetic.test/fhir/R4/InsurancePlan?"
                    "cursor=opaque%2Btoken&cursor=second"
                ),
            ),
        ]
    )

    fetched_result = await fetch_provider_directory_rooted_graph_query(
        session,
        API_BASE,
        claim,
    )

    assert fetched_result.resources == (plan_by_field,)
    assert fetched_result.advertised_total == 1
    assert fetched_result.terminal_page_count == 2
    assert [request[0] for request in session.requests] == [
        query.url,
        (
            "https://DIRECTORY.synthetic.test/fhir/R4/InsurancePlan?"
            "cursor=opaque%2Btoken&cursor=second"
        ),
    ]


@pytest.mark.parametrize(
    "unsafe_next",
    [
        "http://directory.synthetic.test/fhir/R4?cursor=a",
        "https://evil.synthetic.test/fhir/R4?cursor=a",
        "https://directory.synthetic.test:444/fhir/R4?cursor=a",
        "https://user@directory.synthetic.test/fhir/R4?cursor=a",
        "https://directory.synthetic.test/fhir/R40?cursor=a",
        "https://directory.synthetic.test/fhir/R4?cursor=a",
        "https://directory.synthetic.test/fhir/R4/Other?cursor=a",
        "https://directory.synthetic.test/fhir/R4%2FInsurancePlan?cursor=a",
        "https://directory.synthetic.test/fhir/R4/%2e%2e/secret",
        "https://directory.synthetic.test/fhir/R4\\secret",
        "https://directory.synthetic.test/fhir/R4?cursor=a#fragment",
    ],
)
@pytest.mark.asyncio
async def test_next_url_rejects_origin_path_and_encoded_traversal(
    unsafe_next: str,
) -> None:
    query, claim = census_claim()
    session = FakeSession(
        [FakeResponse(query.url, bundle([], total=0, next_url=unsafe_next))]
    )

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            session,
            API_BASE,
            claim,
        )

    assert error_info.value.code == "pagination_invalid"


@pytest.mark.asyncio
async def test_census_requires_stable_total_distinct_ids_and_exact_final_count() -> (
    None
):
    query, claim = census_claim()
    first_plan_by_field = {"resourceType": "InsurancePlan", "id": "plan.synthetic-1"}
    second_plan_by_field = {
        "resourceType": "InsurancePlan",
        "id": "plan.synthetic-2",
    }
    next_url = f"{API_BASE}/InsurancePlan?cursor=two"
    invalid_page_pairs = [
        (
            bundle([first_plan_by_field], total=2, next_url=next_url),
            bundle([second_plan_by_field], total=3),
        ),
        (
            bundle([first_plan_by_field], total=2, next_url=next_url),
            bundle([first_plan_by_field], total=2),
        ),
        (bundle([first_plan_by_field], total=2), None),
    ]
    for first_page, second_page in invalid_page_pairs:
        responses = [FakeResponse(query.url, first_page)]
        if second_page is not None:
            responses.append(FakeResponse(next_url, second_page))
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
            await fetch_provider_directory_rooted_graph_query(
                FakeSession(responses),
                API_BASE,
                claim,
            )
        assert error_info.value.code == "response_invalid"


@pytest.mark.asyncio
async def test_equivalent_host_case_default_port_cursor_cycle_is_rejected() -> None:
    query, claim = census_claim()
    equivalent_next = query.url.replace(
        "https://directory.synthetic.test",
        "https://DIRECTORY.synthetic.test:443",
    )
    session = FakeSession(
        [FakeResponse(query.url, bundle([], total=0, next_url=equivalent_next))]
    )

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            session,
            API_BASE,
            claim,
        )

    assert error_info.value.code == "pagination_invalid"
    assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_exact_search_advertised_total_must_be_stable_and_complete() -> None:
    query, claim = role_claim()
    truncated = bundle([role_resource()], total=5)
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([FakeResponse(query.url, truncated)]),
            API_BASE,
            claim,
        )
    assert error_info.value.code == "response_invalid"

    supplied_total = await fetch_provider_directory_rooted_graph_query(
        FakeSession([FakeResponse(query.url, bundle([role_resource()], total=1))]),
        API_BASE,
        claim,
    )
    assert supplied_total.advertised_total == 1

    next_url = f"{API_BASE}/PractitionerRole?cursor=two"
    late_responses = [
        FakeResponse(
            query.url,
            bundle([role_resource("role.synthetic-1")], next_url=next_url),
        ),
        FakeResponse(
            next_url,
            bundle([role_resource("role.synthetic-2")], total=2),
        ),
    ]
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession(late_responses),
            API_BASE,
            claim,
        )
    assert error_info.value.code == "response_invalid"
