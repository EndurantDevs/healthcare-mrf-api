# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import json

import aiohttp
import pytest

from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPBounds,
    ProviderDirectoryRootedGraphHTTPError,
    fetch_provider_directory_rooted_graph_query,
    provider_directory_rooted_graph_retry_after_seconds,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    API_BASE,
    FakeResponse,
    FakeSession,
    bundle,
    missing_outcome,
)
from tests.test_provider_directory_rooted_graph_http import (
    direct_claim,
    role_claim,
    role_resource,
)


@pytest.mark.asyncio
async def test_direct_read_preserves_exact_resource_or_missing_proof() -> None:
    query, claim = direct_claim()
    resource_by_field = {
        "resourceType": "Organization",
        "id": "organization.synthetic-1",
    }
    fetched_result = await fetch_provider_directory_rooted_graph_query(
        FakeSession([FakeResponse(query.url, resource_by_field)]),
        API_BASE,
        claim,
    )
    assert fetched_result.resources == (resource_by_field,)

    for missing_status in (404, 410):
        outcome_by_field = missing_outcome(missing_status)
        raw_body = json.dumps(outcome_by_field, separators=(",", ":")).encode()
        missing_result = await fetch_provider_directory_rooted_graph_query(
            FakeSession(
                [FakeResponse(query.url, outcome_by_field, status=missing_status)]
            ),
            API_BASE,
            claim,
        )
        assert missing_result.missing_http_status == missing_status
        assert missing_result.resources == ()
        assert missing_result.total_bytes == len(raw_body)
        assert missing_result.missing_response_json_text == raw_body.decode("utf-8")
        assert (
            missing_result.missing_response_sha256
            == hashlib.sha256(raw_body).hexdigest()
        )
        assert raw_body.decode("utf-8") not in repr(missing_result)


@pytest.mark.asyncio
async def test_direct_missing_accepts_only_closed_live_or_status_specific_shapes() -> (
    None
):
    query, claim = direct_claim()
    live_outcome_by_field = {
        "resourceType": "OperationOutcome",
        "issue": [
            {"severity": "error", "code": "processing"},
            {"severity": "information", "code": "informational"},
        ],
    }
    missing_result = await fetch_provider_directory_rooted_graph_query(
        FakeSession([FakeResponse(query.url, live_outcome_by_field, status=404)]),
        API_BASE,
        claim,
    )
    assert missing_result.missing_http_status == 404

    rejected_outcome_by_field_values = (
        {
            "resourceType": "OperationOutcome",
            "issue": [{"severity": "informational", "code": "processing"}],
        },
        {
            "resourceType": "OperationOutcome",
            "issue": [{"severity": "information", "code": "processing"}],
        },
        {
            "resourceType": "OperationOutcome",
            "issue": [
                {"severity": "error", "code": "processing"},
                {"severity": "information", "code": "informational"},
                {"severity": "warning", "code": "informational"},
            ],
        },
    )
    for rejected_outcome_by_field in rejected_outcome_by_field_values:
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
            await fetch_provider_directory_rooted_graph_query(
                FakeSession(
                    [FakeResponse(query.url, rejected_outcome_by_field, status=404)]
                ),
                API_BASE,
                claim,
            )


@pytest.mark.asyncio
async def test_direct_missing_has_an_independent_64kib_body_cap() -> None:
    query, claim = direct_claim()
    oversized = missing_outcome(404)
    oversized["text"] = {"div": "x" * (64 * 1024)}
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([FakeResponse(query.url, oversized, status=404)]),
            API_BASE,
            claim,
            bounds=ProviderDirectoryRootedGraphHTTPBounds(
                max_page_bytes=128 * 1024,
                max_query_bytes=128 * 1024,
            ),
        )
    assert error_info.value.code == "page_limit"


@pytest.mark.asyncio
async def test_direct_missing_rejects_untrusted_or_non_fhir_error_bodies() -> None:
    query, claim = direct_claim()
    invalid_responses = (
        FakeResponse(query.url, body=b"", status=404),
        FakeResponse(
            query.url,
            body=b"<html>missing</html>",
            status=404,
            headers={"Content-Type": "text/html", "Content-Length": "20"},
        ),
        FakeResponse(query.url, {"error": "missing"}, status=404),
        FakeResponse(
            query.url,
            missing_outcome(404),
            status=404,
            headers={"Content-Type": "application/json"},
        ),
    )
    for invalid_response in invalid_responses:
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError):
            await fetch_provider_directory_rooted_graph_query(
                FakeSession([invalid_response]),
                API_BASE,
                claim,
            )
    for status in (400, 409, 422):
        with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
            await fetch_provider_directory_rooted_graph_query(
                FakeSession([FakeResponse(query.url, status=status)]),
                API_BASE,
                claim,
            )
        assert error_info.value.code == "http_terminal"


@pytest.mark.parametrize("status", [408, 423, 425, 429, 500, 503, 599])
@pytest.mark.asyncio
async def test_only_bounded_transient_statuses_are_retryable(status: int) -> None:
    query, claim = role_claim()
    response = FakeResponse(
        query.url,
        status=status,
        headers={"Retry-After": "9999"},
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([response]),
            API_BASE,
            claim,
        )
    assert error_info.value.retryable is True
    assert error_info.value.retry_after_seconds == 60.0


@pytest.mark.parametrize(
    ("body", "expected_code"),
    [
        (b'[{"resourceType":"Bundle"}]', "json_invalid"),
        (b'{"resourceType":"Bundle","resourceType":"Bundle"}', "json_invalid"),
        (b'{"resourceType":"Bundle","value":NaN}', "json_invalid"),
        (b'{"resourceType":"Bundle","value":1.500}', "json_invalid"),
        (b'{"resourceType":"Bundle","value":0.123456789012345678901}', "json_invalid"),
        (b"\xff", "json_invalid"),
    ],
)
@pytest.mark.asyncio
async def test_strict_json_rejects_nonobject_duplicates_nonfinite_and_lossy_decimal(
    body: bytes,
    expected_code: str,
) -> None:
    query, claim = role_claim()
    response = FakeResponse(
        query.url,
        body=body,
        headers={
            "Content-Type": "application/fhir+json",
            "Content-Length": str(len(body)),
        },
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([response]),
            API_BASE,
            claim,
        )
    assert error_info.value.code == expected_code


@pytest.mark.asyncio
async def test_supported_decimal_is_not_rounded_before_result_reduction() -> None:
    query, claim = role_claim()
    resource_by_field = {**role_resource(), "score": 0.125}
    response = FakeResponse(query.url, bundle([resource_by_field]))
    fetched_result = await fetch_provider_directory_rooted_graph_query(
        FakeSession([response]),
        API_BASE,
        claim,
    )
    assert fetched_result.resources[0]["score"] == 0.125


@pytest.mark.parametrize(
    ("headers", "expected_code"),
    [
        ({"Content-Type": "application/json"}, "content_type_invalid"),
        (
            {
                "Content-Type": "application/fhir+json",
                "Content-Encoding": "gzip",
            },
            "content_encoding_invalid",
        ),
        (
            {
                "Content-Type": "application/fhir+json",
                "Content-Length": "-1",
            },
            "content_length_invalid",
        ),
    ],
)
@pytest.mark.asyncio
async def test_response_headers_are_fail_closed(
    headers: dict[str, str],
    expected_code: str,
) -> None:
    query, claim = role_claim()
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession(
                [FakeResponse(query.url, bundle([role_resource()]), headers=headers)]
            ),
            API_BASE,
            claim,
        )
    assert error_info.value.code == expected_code


@pytest.mark.parametrize(
    ("bounds", "response_by_field", "expected_code"),
    [
        (
            ProviderDirectoryRootedGraphHTTPBounds(max_page_bytes=10),
            bundle([]),
            "page_limit",
        ),
        (
            ProviderDirectoryRootedGraphHTTPBounds(max_resources=1),
            bundle(
                [role_resource("role.synthetic-1"), role_resource("role.synthetic-2")]
            ),
            "resource_limit",
        ),
        (
            ProviderDirectoryRootedGraphHTTPBounds(max_url_bytes=10),
            bundle([]),
            "request_invalid",
        ),
    ],
)
@pytest.mark.asyncio
async def test_page_resource_and_url_caps_are_hard(
    bounds: ProviderDirectoryRootedGraphHTTPBounds,
    response_by_field: dict[str, object],
    expected_code: str,
) -> None:
    query, claim = role_claim()
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([FakeResponse(query.url, response_by_field)]),
            API_BASE,
            claim,
            bounds=bounds,
        )
    assert error_info.value.code == expected_code


@pytest.mark.asyncio
async def test_page_count_and_aggregate_query_byte_caps_are_hard() -> None:
    query, claim = role_claim()
    next_url = f"{API_BASE}/PractitionerRole?cursor=two"
    first_response = FakeResponse(
        query.url,
        bundle([role_resource("role.synthetic-1")], next_url=next_url),
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as pages_error:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([first_response]),
            API_BASE,
            claim,
            bounds=ProviderDirectoryRootedGraphHTTPBounds(max_pages=1),
        )
    assert pages_error.value.code == "page_limit"

    first_response = FakeResponse(
        query.url,
        bundle([role_resource("role.synthetic-1")], next_url=next_url),
    )
    second_response = FakeResponse(
        next_url,
        bundle([role_resource("role.synthetic-2")]),
    )
    aggregate_limit = (
        len(first_response.content.body) + len(second_response.content.body) - 1
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as bytes_error:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([first_response, second_response]),
            API_BASE,
            claim,
            bounds=ProviderDirectoryRootedGraphHTTPBounds(
                max_query_bytes=aggregate_limit
            ),
        )
    assert bytes_error.value.code == "query_limit"


@pytest.mark.asyncio
async def test_payload_truncation_is_retryable_and_response_url_must_be_exact() -> None:
    query, claim = role_claim()
    truncated = FakeResponse(
        query.url,
        bundle([role_resource()]),
        headers={
            "Content-Type": "application/fhir+json",
            "Content-Length": "9999",
        },
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as truncation:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([truncated]),
            API_BASE,
            claim,
        )
    assert truncation.value.code == "payload_truncated"
    assert truncation.value.retryable is True

    wrong_url = FakeResponse(
        query.url,
        bundle([role_resource()]),
        response_url=f"{API_BASE}/other",
    )
    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as url_error:
        await fetch_provider_directory_rooted_graph_query(
            FakeSession([wrong_url]),
            API_BASE,
            claim,
        )
    assert url_error.value.code == "response_url_invalid"


@pytest.mark.parametrize(
    "error",
    [
        asyncio.TimeoutError(),
        aiohttp.ClientConnectionError(),
        aiohttp.ClientPayloadError(),
    ],
)
@pytest.mark.asyncio
async def test_timeout_connection_and_truncation_failures_are_retryable(
    error: BaseException,
) -> None:
    query, claim = role_claim()

    class RaisingSession:
        def get(self, *_args, **_kwargs):
            raise error

    with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
        await fetch_provider_directory_rooted_graph_query(
            RaisingSession(),
            API_BASE,
            claim,
        )
    assert error_info.value.retryable is True


def test_retry_after_parser_is_bounded_and_rejects_bad_values() -> None:
    assert provider_directory_rooted_graph_retry_after_seconds("2.5") == 2.5
    assert provider_directory_rooted_graph_retry_after_seconds("9999") == 60.0
    assert provider_directory_rooted_graph_retry_after_seconds("bad") == 0.0
    assert provider_directory_rooted_graph_retry_after_seconds(None) == 0.0
