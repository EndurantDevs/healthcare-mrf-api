# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded search page-size fallback without changing logical query evidence."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
import urllib.parse

import pytest

from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPBounds,
    ProviderDirectoryRootedGraphHTTPError,
    fetch_provider_directory_rooted_graph_query,
    rebind_provider_directory_rooted_graph_query,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    API_BASE,
    FakeResponse,
    FakeSession,
    bundle,
)
from tests.test_provider_directory_rooted_graph_http import (
    census_claim,
    direct_claim,
    role_claim,
    role_resource,
)


def timeout_response(
    partial_body: bytes = b"",
    failure: BaseException | None = None,
) -> FakeResponse:
    """Retain received byte accounting even if the response never finishes."""

    async def chunks(_chunk_size: int):
        if partial_body:
            yield partial_body
        raise failure if failure is not None else TimeoutError()

    response = FakeResponse(
        "__REQUEST_URL__", headers={"Content-Type": "application/fhir+json"}
    )
    response.content = SimpleNamespace(iter_chunked=chunks)
    return response


def request_sizes(session: FakeSession) -> list[int]:
    """Read only the wire page-size hints from synthetic requests."""

    return [
        int(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))["_count"])
        for url, _options in session.requests
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("claim_builder", [role_claim, census_claim])
async def test_smaller_pages_restart_and_follow_next_without_changing_identity(
    claim_builder,
) -> None:
    query, claim = claim_builder()
    resources = (
        [role_resource("role.synthetic-1"), role_resource("role.synthetic-2")]
        if claim.resource_type == "PractitionerRole"
        else [
            {"resourceType": "InsurancePlan", "id": "plan.synthetic-1"},
            {"resourceType": "InsurancePlan", "id": "plan.synthetic-2"},
        ]
    )
    next_url = f"{API_BASE}/{claim.resource_type}?cursor=second&_count=50"
    session = FakeSession(
        [
            timeout_response(),
            FakeResponse(
                "__REQUEST_URL__", bundle(resources[:1], total=2, next_url=next_url)
            ),
            FakeResponse(next_url, bundle(resources[1:], total=2)),
        ]
    )
    fetched_result = await fetch_provider_directory_rooted_graph_query(
        session, API_BASE, claim
    )
    assert request_sizes(session) == [100, 50, 50]
    assert session.requests[-1][0] == next_url
    original_by_parameter = dict(
        urllib.parse.parse_qsl(urllib.parse.urlsplit(query.url).query)
    )
    fallback_by_parameter = dict(
        urllib.parse.parse_qsl(urllib.parse.urlsplit(session.requests[1][0]).query)
    )
    assert fallback_by_parameter == {**original_by_parameter, "_count": "50"}
    assert fetched_result.query_id == claim.query_id
    assert fetched_result.resources == tuple(resources)
    assert fetched_result.advertised_total == fetched_result.terminal_page_count == 2
    assert rebind_provider_directory_rooted_graph_query(API_BASE, claim) == query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError(),
        ProviderDirectoryRootedGraphHTTPError("transport_timeout", retryable=True),
    ],
)
async def test_timeout_halving_stops_at_one_with_no_additional_request(failure) -> None:
    _query, claim = role_claim()
    session = FakeSession([timeout_response(failure=failure) for _ in range(7)])
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(session, API_BASE, claim)
    assert error_info.value.code == "transport_timeout"
    assert error_info.value.retryable is False
    assert request_sizes(session) == [100, 50, 25, 12, 6, 3, 1]


@pytest.mark.asyncio
async def test_later_page_timeout_discards_the_previous_pagination_stream() -> None:
    query, claim = role_claim()
    first = FakeResponse(
        query.url,
        bundle(
            [role_resource()],
            total=2,
            next_url=f"{API_BASE}/PractitionerRole?cursor=old",
        ),
    )
    final = FakeResponse("__REQUEST_URL__", bundle([role_resource()], total=1))
    session = FakeSession([first, timeout_response(), final])
    result = await fetch_provider_directory_rooted_graph_query(session, API_BASE, claim)
    assert len(session.requests) == 3
    assert session.requests[-1][0] == query.url.replace("_count=100", "_count=50")
    assert result.resources == (role_resource(),)
    assert result.terminal_page_count == result.advertised_total == 1
    assert result.total_bytes == len(final.content.body)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError(), asyncio.CancelledError()])
async def test_direct_read_timeout_and_cancellation_do_not_retry(failure) -> None:
    _query, claim = direct_claim()
    session = FakeSession([timeout_response(failure=failure)])
    expected = (
        asyncio.CancelledError
        if isinstance(failure, asyncio.CancelledError)
        else ProviderDirectoryRootedGraphHTTPError
    )
    with pytest.raises(expected):
        await fetch_provider_directory_rooted_graph_query(session, API_BASE, claim)
    assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_shared_request_cap_counts_timeouts_and_abandoned_pages() -> None:
    query, claim = role_claim()
    session = FakeSession(
        [
            FakeResponse(
                query.url,
                bundle([], total=0, next_url=f"{API_BASE}/PractitionerRole?cursor=old"),
            ),
            timeout_response(),
            timeout_response(),
        ]
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            session,
            API_BASE,
            claim,
            bounds=ProviderDirectoryRootedGraphHTTPBounds(max_pages=3),
        )
    assert error_info.value.code == "page_limit"
    assert len(session.requests) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("partial_bytes", [1, 32])
async def test_shared_byte_cap_counts_timed_out_body_chunks(partial_bytes: int) -> None:
    _query, claim = role_claim()
    final = FakeResponse("__REQUEST_URL__", bundle([role_resource()], total=1))
    cap = len(final.content.body)
    session = FakeSession([timeout_response(b"x" * partial_bytes), final])
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            session,
            API_BASE,
            claim,
            bounds=ProviderDirectoryRootedGraphHTTPBounds(max_query_bytes=cap),
        )
    assert error_info.value.code == "query_limit"
    assert len(session.requests) == 2


@pytest.mark.asyncio
async def test_shared_byte_cap_counts_abandoned_complete_pages() -> None:
    query, claim = role_claim()
    first = FakeResponse(
        query.url,
        bundle(
            [role_resource()],
            total=2,
            next_url=f"{API_BASE}/PractitionerRole?cursor=old",
        ),
    )
    final = FakeResponse("__REQUEST_URL__", bundle([role_resource()], total=1))
    session = FakeSession([first, timeout_response(), final])
    byte_cap = len(first.content.body) + len(final.content.body) - 1
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            session,
            API_BASE,
            claim,
            bounds=ProviderDirectoryRootedGraphHTTPBounds(max_query_bytes=byte_cap),
        )
    assert error_info.value.code == "query_limit"
    assert len(session.requests) == 3


@pytest.mark.asyncio
async def test_exhausted_byte_cap_prevents_another_request() -> None:
    _query, claim = role_claim()
    session = FakeSession([timeout_response(b"x" * 32)])
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            session,
            API_BASE,
            claim,
            bounds=ProviderDirectoryRootedGraphHTTPBounds(max_query_bytes=32),
        )
    assert error_info.value.code == "query_limit"
    assert len(session.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,status,headers,expected_code",
    [
        (bundle([role_resource()], total=2), 200, None, "response_invalid"),
        (
            bundle([role_resource(), role_resource()], total=2),
            200,
            None,
            "response_invalid",
        ),
        (
            bundle(
                [
                    {
                        **role_resource(),
                        "practitioner": {"reference": "Practitioner/other"},
                    }
                ]
            ),
            200,
            None,
            "response_invalid",
        ),
        (
            bundle(
                [],
                total=0,
                next_url="https://other.synthetic.test/fhir/R4/PractitionerRole",
            ),
            200,
            None,
            "pagination_invalid",
        ),
        (bundle([]), 200, {"Content-Type": "text/html"}, "content_type_invalid"),
        (bundle([]), 504, None, "http_transient"),
    ],
)
async def test_fallback_does_not_bypass_validation_or_retry_other_errors(
    payload,
    status,
    headers,
    expected_code,
) -> None:
    _query, claim = role_claim()
    session = FakeSession(
        [
            timeout_response(),
            FakeResponse("__REQUEST_URL__", payload, status=status, headers=headers),
        ]
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(session, API_BASE, claim)
    assert error_info.value.code == expected_code
    assert request_sizes(session) == [100, 50]
