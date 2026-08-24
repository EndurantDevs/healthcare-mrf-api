# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exhaustive strict HTTP helper and transport failure boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import math
from types import SimpleNamespace

import aiohttp
import pytest

from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPBounds,
    ProviderDirectoryRootedGraphHTTPError,
    _SearchState,
    _bundle_entries,
    _bundle_next_link,
    _query_for_claim,
    _validate_bundle,
    _validate_search_resource,
    fetch_provider_directory_rooted_graph_query,
    rebind_provider_directory_rooted_graph_query,
)
from process.provider_directory_rooted_graph_http_transport import (
    _bounded_retry_after,
    _declared_length,
    _header_value,
    _read_body,
    _request_url_identity,
    _strict_json_float,
    _url_byte_length,
    _validate_headers,
    _validated_next_url,
    rooted_graph_retry_after_seconds,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_EXACT_SEARCH,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    API_BASE,
    FakeResponse,
    FakeSession,
)
from tests.test_provider_directory_rooted_graph_http import (
    census_claim,
    direct_claim,
    role_claim,
    role_resource,
)


@pytest.mark.parametrize(
    "changes",
    (
        {"max_pages": 0},
        {"timeout_seconds": True},
    ),
)
def test_http_bounds_reject_invalid_integer_or_timeout(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="http_bounds_invalid"):
        ProviderDirectoryRootedGraphHTTPBounds(**changes)


def test_http_result_rejects_missing_witness_or_generic_envelope_drift() -> None:
    from tests.provider_directory_rooted_graph_runtime_test_support import (
        RuntimeHarness,
    )

    claim = RuntimeHarness()._claims("baseline")["direct"]
    missing_result = RuntimeHarness()._missing_result(claim)
    with pytest.raises(ValueError, match="http_result_invalid"):
        replace(missing_result, missing_response_json_text="not-json")
    with pytest.raises(ValueError, match="http_result_invalid"):
        replace(missing_result, missing_http_status=None)


def test_retry_and_header_helpers_fail_closed_at_type_boundaries() -> None:
    assert _bounded_retry_after(True) == 0
    assert _bounded_retry_after(math.inf) == 0
    assert rooted_graph_retry_after_seconds("Wed, 21 Oct 2030 07:28:00") == 60
    assert rooted_graph_retry_after_seconds("Wed, 21 Oct 2030 07:28:00 GMT") == 60
    assert _header_value(None, "Content-Type") is None
    assert _header_value({"content-type": "FHIR"}, "Content-Type") == "FHIR"


def test_url_helpers_reject_unicode_parse_and_identity_boundaries() -> None:
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as unicode_error:
        _url_byte_length("\ud800")
    assert unicode_error.value.code == "pagination_invalid"

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as parse_error:
        _validated_next_url(
            api_base=API_BASE,
            collection_url=f"{API_BASE}/InsurancePlan?_count=100",
            current_url=f"{API_BASE}/InsurancePlan?_count=100",
            next_link=(
                "https://directory.synthetic.test:bad/fhir/R4/InsurancePlan?cursor=1"
            ),
            max_url_bytes=8192,
        )
    assert parse_error.value.code == "pagination_invalid"
    assert _request_url_identity(object()) is None
    assert _request_url_identity("https://host:bad/path") is None
    assert _request_url_identity("http://directory.synthetic.test/path") is None


def test_strict_float_and_header_parameters_reject_ambiguous_values() -> None:
    with pytest.raises(ValueError):
        _strict_json_float("1e999")
    response_without_length = SimpleNamespace(
        headers={"Content-Type": "application/fhir+json"}
    )
    assert _declared_length(response_without_length) is None
    assert _validate_headers(response_without_length) is None
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as media_error:
        _validate_headers(
            SimpleNamespace(
                headers={"Content-Type": "application/fhir+json; profile=synthetic"}
            )
        )
    assert media_error.value.code == "content_type_invalid"
    assert media_error.value.retryable is True


class _Content:
    def __init__(self, chunks: tuple[object, ...]) -> None:
        self.chunks = chunks

    async def iter_chunked(self, _size: int):
        for chunk in self.chunks:
            yield chunk


@pytest.mark.parametrize(
    ("chunks", "declared", "page_limit", "query_remaining", "expected_code"),
    (
        (("not-bytes",), None, 10, 10, "body_invalid"),
        ((b"123456",), None, 5, 10, "page_limit"),
        ((b"123456",), None, 10, 5, "query_limit"),
        ((b"abc",), 2, 10, 10, "content_length_invalid"),
    ),
)
@pytest.mark.asyncio
async def test_stream_reader_rejects_dynamic_body_boundaries(
    chunks: tuple[object, ...],
    declared: int | None,
    page_limit: int,
    query_remaining: int,
    expected_code: str,
) -> None:
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await _read_body(
            SimpleNamespace(content=_Content(chunks)),
            declared_length=declared,
            page_limit=page_limit,
            query_remaining=query_remaining,
        )
    assert error_info.value.code == expected_code


def test_claim_query_and_bundle_helpers_reject_unclassified_shapes() -> None:
    invalid_claim = SimpleNamespace(
        kind=ROOTED_GRAPH_QUERY_EXACT_SEARCH,
        resource_type="Other",
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
        _query_for_claim(API_BASE, invalid_claim)
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
        rebind_provider_directory_rooted_graph_query(API_BASE, object())
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
        rebind_provider_directory_rooted_graph_query("not-a-base", role_claim()[1])

    for payload in ({"entry": {}}, {"entry": [None]}):
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
            _bundle_entries(payload)


@pytest.mark.parametrize(
    "payload",
    (
        {"link": {}},
        {"link": [None]},
        {"link": [{"relation": "self", "url": "ignored"}, None]},
        {
            "link": [
                {"relation": "next", "url": "first"},
                {"relation": "next", "url": "second"},
            ]
        },
    ),
)
def test_bundle_next_link_rejects_untrusted_shapes(payload: dict[str, object]) -> None:
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
        _bundle_next_link(payload)


def test_bundle_next_link_ignores_well_formed_non_next_relation() -> None:
    assert _bundle_next_link({"link": [{"relation": "self", "url": "ignored"}]}) is None


def test_resource_and_bundle_validation_reject_wrong_identity() -> None:
    claim = role_claim()[1]
    for resource in (
        {"resourceType": "Organization", "id": "role.synthetic-1"},
        {"resourceType": "PractitionerRole", "id": "bad id"},
    ):
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
            _validate_search_resource(resource, claim)
    for payload in (
        {"resourceType": "Bundle", "type": "history"},
        {"resourceType": "Bundle", "type": "searchset", "total": -1},
    ):
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
            _validate_bundle(payload, claim)


@pytest.mark.asyncio
async def test_direct_read_rejects_resource_id_mismatch() -> None:
    query, claim = direct_claim()
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession(
                [
                    FakeResponse(
                        query.url,
                        {
                            "resourceType": "Organization",
                            "id": "organization.different",
                        },
                    )
                ]
            ),
            API_BASE,
            claim,
        )
    assert error_info.value.code == "response_invalid"


def test_search_state_rejects_missing_or_oversized_advertised_total() -> None:
    bounds = ProviderDirectoryRootedGraphHTTPBounds(max_resources=1)
    state = _SearchState(expected_total=1)
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
        state.add_page([], None, 1, role_claim()[1], bounds)

    census_state = _SearchState()
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
        census_state.add_page([], None, 1, census_claim()[1], bounds)

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as limit_error:
        _SearchState().add_page([], 2, 1, role_claim()[1], bounds)
    assert limit_error.value.code == "resource_limit"


@pytest.mark.asyncio
async def test_public_fetch_rejects_bounds_and_preserves_cancellation() -> None:
    query, claim = role_claim()
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as bounds_error:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([]),
            API_BASE,
            claim,
            bounds=object(),
        )
    assert bounds_error.value.code == "request_invalid"

    class CancellingSession:
        def get(self, *_args, **_kwargs):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await fetch_provider_directory_rooted_graph_query(
            CancellingSession(),
            API_BASE,
            claim,
        )
    assert query.url


@pytest.mark.parametrize("failure", (aiohttp.ClientError(), ValueError("bad")))
@pytest.mark.asyncio
async def test_public_fetch_normalizes_generic_transport_or_value_failure(
    failure: BaseException,
) -> None:
    _query, claim = role_claim()

    class RaisingSession:
        def get(self, *_args, **_kwargs):
            raise failure

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            RaisingSession(),
            API_BASE,
            claim,
        )
    assert error_info.value.code in {"transport_failure", "response_invalid"}


def test_redirect_status_is_never_retried() -> None:
    from process.provider_directory_rooted_graph_http_transport import (
        _require_success_status,
    )

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        _require_success_status(
            SimpleNamespace(status=302, headers={}), role_claim()[1]
        )
    assert error_info.value.code == "redirect_forbidden"
    assert error_info.value.retryable is False
