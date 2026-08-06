# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from scripts.research.provider_directory_fhir_benchmark_contract import (
    build_tuning_report,
    parse_observation,
)


def _observation(concurrency, elapsed, *, retry=0, throttle=False, exact=True):
    expected = 100
    processed = expected if exact else expected - 1
    return parse_observation(
        {
            "concurrency": concurrency,
            "cutoff": "2026-08-06T12:00:00Z",
            "elapsed_seconds": elapsed,
            "deadline_seconds": 100,
            "proxy_route": "dev-server",
            "resources": [
                {
                    "resource_type": "Practitioner",
                    "pre_count": expected,
                    "processed_rows": processed,
                    "unique_staged_ids": processed,
                    "post_count": expected,
                    "pages": 1,
                    "elapsed_seconds": elapsed,
                    "requests": 200,
                    "transient_retries": retry,
                    "unresolved_throttling": throttle,
                    "peak_memory_bytes": 1024,
                    "peak_db_backlog_rows": 5000,
                }
            ],
        }
    )


def test_report_allows_four_only_for_safe_material_improvement():
    report = build_tuning_report(
        [
            _observation(1, 80),
            _observation(2, 60),
            _observation(4, 50),
        ]
    )

    assert report["recommended_resource_concurrency"] == 4
    assert all(item["passes_safety_gates"] for item in report["observations"][1:])


def test_report_keeps_two_when_four_is_not_materially_faster():
    report = build_tuning_report(
        [_observation(1, 80), _observation(2, 60), _observation(4, 58)]
    )

    assert report["recommended_resource_concurrency"] == 2


def test_report_fails_closed_on_inexact_or_throttled_observations():
    report = build_tuning_report(
        [
            _observation(1, 65),
            _observation(2, 50, exact=False),
            _observation(4, 40, throttle=True),
        ]
    )

    assert report["recommended_resource_concurrency"] == 1
    assert report["observations"][1]["passes_safety_gates"] is False
    assert report["observations"][2]["passes_safety_gates"] is False


def test_report_requires_dev_server_route_and_retry_rate_below_one_percent():
    observation = _observation(2, 50, retry=2)
    observation_by_field = {
        "concurrency": observation.concurrency,
        "cutoff": observation.cutoff,
        "elapsed_seconds": observation.elapsed_seconds,
        "deadline_seconds": observation.deadline_seconds,
        "proxy_route": "direct",
        "resources": [
            {
                **observation.resources[0].__dict__,
                "transient_retries": 0,
            }
        ],
    }

    report = build_tuning_report(
        [observation, parse_observation(observation_by_field)]
    )

    assert report["recommended_resource_concurrency"] == 0
    assert all(not item["passes_safety_gates"] for item in report["observations"])
