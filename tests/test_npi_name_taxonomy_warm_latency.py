# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in process-loopback gate for representative name+taxonomy searches."""

import json
import os
import statistics
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, build_opener

import pytest


LOOPBACK_URL_ENV = "HLTHPRT_NPI_NAME_TAXONOMY_LOOPBACK_URL"
EXPECTED_NPIS_ENV = "HLTHPRT_NPI_NAME_TAXONOMY_EXPECTED_NPIS"
SMITH_EXPECTED_NPIS_ENV = "HLTHPRT_NPI_NAME_TAXONOMY_SMITH_EXPECTED_NPIS"
SMITH_EXPECTED_TOTAL_ENV = "HLTHPRT_NPI_NAME_TAXONOMY_SMITH_EXPECTED_TOTAL"
TARGET_WARM_P95_MS = 40.0
HARD_WARM_REQUEST_MS = 3_000.0
WARMUP_REQUESTS = 3
WARM_REQUESTS = 30
EXPECTED_TAXONOMY_CODE = "1223E0200X"


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def _loopback_endpoint(query: dict[str, str]) -> str:
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
    return f"{endpoint}?{urlencode(query)}"


def _expected_npis(environment_name: str) -> tuple[str, ...]:
    expected_npis = tuple(
        value.strip()
        for value in str(os.getenv(environment_name) or "").split(",")
        if value.strip()
    )
    assert expected_npis, f"set {environment_name} to the exact parity oracle"
    assert all(value.isdigit() and len(value) == 10 for value in expected_npis)
    return expected_npis


def _assert_response_parity(
    document,
    expected_npis: tuple[str, ...],
    expected_total: int,
) -> None:
    rows = document["rows"]
    actual_npis = tuple(str(row["npi"]) for row in rows)
    assert actual_npis == expected_npis
    assert int(document["total"]) == expected_total
    assert document["total_source"] == "computed"
    assert all(
        any(
            taxonomy.get("healthcare_provider_taxonomy_code")
            == EXPECTED_TAXONOMY_CODE
            for taxonomy in row.get("taxonomy_list", [])
        )
        for row in rows
    )


def _request_once(
    opener,
    endpoint: str,
    expected_npis: tuple[str, ...],
    expected_total: int,
) -> float:
    started_at = time.perf_counter()
    with opener.open(endpoint, timeout=HARD_WARM_REQUEST_MS / 1_000.0) as response:
        body = response.read()
        assert response.status == 200
        assert response.headers.get_content_type() == "application/json"
    elapsed_ms = (time.perf_counter() - started_at) * 1_000.0
    _assert_response_parity(json.loads(body), expected_npis, expected_total)
    return elapsed_ms


def test_response_parity_requires_configured_npi_order(monkeypatch):
    monkeypatch.setenv(EXPECTED_NPIS_ENV, "1598811960,1003010604")
    expected_npis = _expected_npis(EXPECTED_NPIS_ENV)
    assert expected_npis == ("1598811960", "1003010604")
    response_document_map = {
        "rows": [
            {
                "npi": npi,
                "taxonomy_list": [
                    {"healthcare_provider_taxonomy_code": EXPECTED_TAXONOMY_CODE}
                ],
            }
            for npi in expected_npis
        ],
        "total": 2,
        "total_source": "computed",
    }

    _assert_response_parity(response_document_map, expected_npis, 2)
    response_document_map["rows"].reverse()
    with pytest.raises(AssertionError):
        _assert_response_parity(response_document_map, expected_npis, 2)


@pytest.mark.parametrize(
    ("query", "expected_npis_environment", "expected_total_environment"),
    [
        (
            {"q": "wheeler", "codes": EXPECTED_TAXONOMY_CODE},
            EXPECTED_NPIS_ENV,
            None,
        ),
        (
            {"q": "smith", "codes": EXPECTED_TAXONOMY_CODE, "limit": "10"},
            SMITH_EXPECTED_NPIS_ENV,
            SMITH_EXPECTED_TOTAL_ENV,
        ),
    ],
    ids=("wheeler", "smith-limit-10"),
)
def test_name_taxonomy_process_loopback_warm_latency_and_parity(
    query,
    expected_npis_environment,
    expected_total_environment,
):
    """Target a locally started API process; remote URLs are rejected above."""

    endpoint = _loopback_endpoint(query)
    expected_npis = _expected_npis(expected_npis_environment)
    expected_total = (
        int(os.environ[expected_total_environment])
        if expected_total_environment
        else len(expected_npis)
    )
    assert expected_total >= len(expected_npis)
    opener = build_opener(_NoRedirect)

    for _ in range(WARMUP_REQUESTS):
        _request_once(opener, endpoint, expected_npis, expected_total)
    warm_samples_ms = [
        _request_once(opener, endpoint, expected_npis, expected_total)
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
