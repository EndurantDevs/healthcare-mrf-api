# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for the bounded live OpenAPI monitor."""

from scripts import monitor_openapi


def test_monitor_cases_cover_every_openapi_operation() -> None:
    openapi_spec = monitor_openapi.load_spec(monitor_openapi.DEFAULT_OPENAPI_PATH)
    cases = monitor_openapi.operation_cases(openapi_spec)
    operation_count = sum(
        1
        for path_item in openapi_spec["paths"].values()
        for method in path_item
        if method in monitor_openapi.HTTP_METHODS
    )

    assert len(cases) == operation_count
    bulk_case = next(case for case in cases if case.operation_id == "postPlanPriceBulk")
    assert bulk_case.method == "POST"
    assert bulk_case.body == {"plan_ids": ["monitoring-no-match"]}
    assert all("{" not in case.path for case in cases)


def test_monitor_cases_fail_closed_for_unclassified_mutation() -> None:
    openapi_map = {
        "paths": {
            "/unsafe": {
                "post": {
                    "operationId": "unsafeMutation",
                    "responses": {"204": {"description": "done"}},
                }
            }
        }
    }

    try:
        monitor_openapi.operation_cases(openapi_map)
    except ValueError as exc:
        assert "must set x-monitoring.safe or x-monitoring.excluded_reason" in str(exc)
    else:
        raise AssertionError("unclassified mutation was accepted")

    trace_openapi_map = {
        "paths": {
            "/trace": {
                "trace": {
                    "operationId": "trace",
                    "responses": {"200": {"description": "ok"}},
                }
            }
        }
    }
    try:
        monitor_openapi.operation_cases(trace_openapi_map)
    except ValueError as exc:
        assert "must set x-monitoring.safe" in str(exc)
    else:
        raise AssertionError("unclassified TRACE operation was accepted")


def test_documented_errors_are_not_healthy() -> None:
    openapi_map = {
        "paths": {
            "/items/{item_id}": {
                "get": {
                    "operationId": "getItem",
                    "parameters": [
                        {
                            "name": "item_id",
                            "in": "path",
                            "required": True,
                            "example": "missing",
                        }
                    ],
                    "responses": {
                        "200": {"description": "ok"},
                        "404": {"description": "missing"},
                    },
                }
            }
        }
    }

    [case] = monitor_openapi.operation_cases(openapi_map)
    assert case.expected_statuses == (200,)


def test_run_cases_fails_on_documented_server_error_and_latency(monkeypatch) -> None:
    cases = [
        monitor_openapi.ProbeCase("healthy", "GET", "/healthy", (200,)),
        monitor_openapi.ProbeCase("unready", "GET", "/ready", (200, 503)),
    ]

    def fake_execute(case, **_kwargs):
        if case.operation_id == "healthy":
            return monitor_openapi.ProbeResult("healthy", 200, 40, True)
        return monitor_openapi.ProbeResult("unready", 503, 240, False)

    monkeypatch.setattr(monitor_openapi, "execute_case", fake_execute)
    summary = monitor_openapi.run_cases(
        cases,
        base_url="http://monitor.invalid",
        api_key="",
        timeout=1,
        workers=1,
        max_p95_ms=200,
    )

    assert summary == {
        "ok": False,
        "operation_count": 2,
        "failure_count": 1,
        "p95_ms": 240,
        "max_p95_ms": 200,
        "failures": [
            {
                "operation_id": "unready",
                "status": 503,
                "elapsed_ms": 240,
                "ok": False,
                "error": None,
            }
        ],
    }


def test_kuma_push_failure_redacts_secret_url(monkeypatch) -> None:
    def fail(*_args, **_kwargs):
        raise monitor_openapi.urllib.error.URLError("opaque-secret-token")

    monkeypatch.setattr(monitor_openapi.urllib.request, "urlopen", fail)
    try:
        monitor_openapi.push_summary(
            "https://kuma.invalid/api/push/opaque-secret-token",
            {"operation_count": 1, "failure_count": 1, "p95_ms": 1, "ok": False},
        )
    except RuntimeError as exc:
        assert str(exc) == "Kuma push failed"
        assert "opaque-secret-token" not in str(exc)
    else:
        raise AssertionError("push failure must fail closed")


def test_kuma_push_rejects_template_query() -> None:
    try:
        monitor_openapi.push_summary(
            "https://kuma.invalid/api/push/token?status=up&msg=OK&ping=",
            {"operation_count": 1, "failure_count": 0, "p95_ms": 1, "ok": True},
        )
    except ValueError as exc:
        assert str(exc) == "Kuma push URL must not contain a query or fragment"
    else:
        raise AssertionError("Kuma template query must be removed")
