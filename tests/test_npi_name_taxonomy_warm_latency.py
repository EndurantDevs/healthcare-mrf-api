# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in process-loopback gate for the representative name+taxonomy search."""

import json
import os
import statistics
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, build_opener

import pytest


LOOPBACK_URL_ENV = "HLTHPRT_NPI_NAME_TAXONOMY_LOOPBACK_URL"
EXPECTED_NPIS_ENV = "HLTHPRT_NPI_NAME_TAXONOMY_EXPECTED_NPIS"
TARGET_WARM_P95_MS = 40.0
HARD_WARM_REQUEST_MS = 3_000.0
WARMUP_REQUESTS = 2
WARM_REQUESTS = 7
EXPECTED_TAXONOMY_CODE = "1223E0200X"


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def _loopback_endpoint() -> str:
    endpoint = str(os.getenv(LOOPBACK_URL_ENV) or "").strip()
    if not endpoint:
        pytest.skip(
            f"set {LOOPBACK_URL_ENV} to a local /api/v1/npi/all URL"
        )
    parsed = urlsplit(endpoint)
    assert parsed.scheme == "http"
    assert parsed.hostname in {"127.0.0.1", "::1", "localhost"}
    assert parsed.path.rstrip("/").endswith("/api/v1/npi/all")
    assert not parsed.query
    assert not parsed.fragment
    return f"{endpoint}?{urlencode({'q': 'wheeler', 'codes': EXPECTED_TAXONOMY_CODE})}"


def _expected_npis() -> tuple[str, ...]:
    expected = tuple(
        sorted(
            value.strip()
            for value in str(os.getenv(EXPECTED_NPIS_ENV) or "").split(",")
            if value.strip()
        )
    )
    assert expected, f"set {EXPECTED_NPIS_ENV} to the exact parity oracle"
    assert all(value.isdigit() and len(value) == 10 for value in expected)
    return expected


def _assert_response_parity(document, expected_npis: tuple[str, ...]) -> None:
    rows = document["rows"]
    actual_npis = tuple(sorted(str(row["npi"]) for row in rows))
    assert actual_npis == expected_npis
    assert int(document["total"]) == len(expected_npis)
    assert document["total_source"] == "computed"
    assert all(
        any(
            taxonomy.get("healthcare_provider_taxonomy_code")
            == EXPECTED_TAXONOMY_CODE
            for taxonomy in row.get("taxonomy_list", [])
        )
        for row in rows
    )


def _request_once(opener, endpoint: str, expected_npis: tuple[str, ...]) -> float:
    started_at = time.perf_counter()
    with opener.open(endpoint, timeout=HARD_WARM_REQUEST_MS / 1_000.0) as response:
        body = response.read()
        assert response.status == 200
        assert response.headers.get_content_type() == "application/json"
    elapsed_ms = (time.perf_counter() - started_at) * 1_000.0
    _assert_response_parity(json.loads(body), expected_npis)
    return elapsed_ms


def test_name_taxonomy_process_loopback_warm_latency_and_parity():
    """Target a locally started API process; remote URLs are rejected above."""

    endpoint = _loopback_endpoint()
    expected_npis = _expected_npis()
    opener = build_opener(_NoRedirect)

    for _ in range(WARMUP_REQUESTS):
        _request_once(opener, endpoint, expected_npis)
    warm_samples_ms = [
        _request_once(opener, endpoint, expected_npis)
        for _ in range(WARM_REQUESTS)
    ]

    warm_p95_ms = statistics.quantiles(
        warm_samples_ms,
        n=20,
        method="inclusive",
    )[18]
    warm_max_ms = max(warm_samples_ms)
    assert warm_p95_ms < TARGET_WARM_P95_MS, warm_samples_ms
    assert warm_max_ms < HARD_WARM_REQUEST_MS, warm_samples_ms
