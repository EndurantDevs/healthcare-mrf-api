# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for the bounded live OpenAPI monitor."""

from scripts import monitor_openapi, monitor_openapi_policy


EXPECTED_MONITORED_OPERATION_IDS = frozenset(
    {
        "getClinicalClinicalAreas",
        "getClinicalConcepts",
        "getClinicalConditions",
        "getClinicalCrosswalk",
        "getClinicalRelationships",
        "getClinicalTreatments",
        "getCodes",
        "getCodesCodeSystemCode",
        "getCodesCodeSystemCodeRelated",
        "getFormularyPartdImportStatus",
        "getFormularyStatistics",
        "getGeoStateStateCities",
        "getGeoStates",
        "getGeoZipZipCode",
        "getGeoZipZipCodePlaces",
        "getHealthcheck",
        "getIssuer",
        "getIssuerStateState",
        "getLiveness",
        "getNpiIdNpi",
        "getNpiIdNpiFullTaxonomy",
        "getNpiPlansByNpiNpi",
        "getPharmacyLicenseCoverage",
        "getPharmacyLicenseImportStatus",
        "getPricingProcedureTaxonomyResolve",
        "getPricingProceduresAutocomplete",
        "getPricingProceduresResolve",
        "getPricingProviderTypesAutocomplete",
        "getPricingProviderTypesResolve",
        "getReadiness",
        "postPlanPriceBulk",
    }
)


def test_monitor_cases_cover_every_openapi_operation() -> None:
    openapi_spec = monitor_openapi.load_spec(monitor_openapi.DEFAULT_OPENAPI_PATH)
    cases = monitor_openapi.operation_cases(openapi_spec)
    operation_ids = {
        operation["operationId"]
        for path_item in openapi_spec["paths"].values()
        for method, operation in path_item.items()
        if method in monitor_openapi.HTTP_METHODS
    }

    safe_operation_ids = {case.operation_id for case in cases}
    assert safe_operation_ids | set(monitor_openapi.EXCLUDED_MONITORING_OPERATIONS) == operation_ids
    assert not safe_operation_ids & set(monitor_openapi.EXCLUDED_MONITORING_OPERATIONS)
    exclusion_reasons = set(monitor_openapi.EXCLUDED_MONITORING_OPERATIONS.values())
    assert len(exclusion_reasons) == 2
    assert all(reason.startswith("reviewed: ") for reason in exclusion_reasons)
    assert not (
        set(monitor_openapi_policy._STABLE_CANARY_REQUIRED_OPERATIONS)
        & set(monitor_openapi_policy._BOUNDED_DEFAULT_REQUIRED_OPERATIONS)
    )
    assert safe_operation_ids == EXPECTED_MONITORED_OPERATION_IDS
    bulk_case = next(case for case in cases if case.operation_id == "postPlanPriceBulk")
    assert bulk_case.method == "POST"
    assert bulk_case.body == {"plan_ids": ["monitoring-no-match"]}
    assert bulk_case.max_latency_ms == 5000
    assert {case.max_latency_ms for case in cases} == {2000, 5000}
    assert next(case for case in cases if case.operation_id == "getGeoStates").path.endswith(
        "?limit=1"
    )
    assert all("{" not in case.path for case in cases)


def test_monitor_cases_fail_closed_for_every_unclassified_operation() -> None:
    openapi_map = {
        "paths": {
            "/unsafe": {
                "get": {
                    "operationId": "newRead",
                    "responses": {"200": {"description": "done"}},
                }
            }
        }
    }

    try:
        monitor_openapi.operation_cases(openapi_map)
    except ValueError as exc:
        assert "has no explicit monitoring policy" in str(exc)
    else:
        raise AssertionError("unclassified GET was accepted")


def test_monitor_rejects_invalid_explicit_parameter_and_bounds_optional_limit() -> None:
    parameter_definitions = [
        {
            "name": "item_id",
            "in": "path",
            "required": True,
            "schema": {"type": "string", "pattern": "^item_[0-9]{3}$"},
        },
        {
            "name": "limit",
            "in": "query",
            "schema": {"type": "integer", "minimum": 1, "maximum": 100},
        },
    ]
    operation_by_name = {
        "operationId": "getItem",
        "responses": {"200": {"description": "ok"}},
    }

    try:
        monitor_openapi.build_case(
            {},
            "/items/{item_id}",
            "get",
            operation_by_name,
            parameter_definitions,
            {
                "safe": True,
                "max_latency_ms": 200,
                "parameters": {"item_id": "not-valid"},
            },
        )
    except ValueError as exc:
        assert "does not satisfy its schema" in str(exc)
    else:
        raise AssertionError("invalid monitoring parameter was accepted")

    case = monitor_openapi.build_case(
        {},
        "/items/{item_id}",
        "get",
        operation_by_name,
        parameter_definitions,
        {
            "safe": True,
            "max_latency_ms": 200,
            "parameters": {"item_id": "item_001"},
        },
    )
    assert case.path == "/items/item_001?limit=1"


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

    operation = openapi_map["paths"]["/items/{item_id}"]["get"]
    case = monitor_openapi.build_case(
        openapi_map,
        "/items/{item_id}",
        "get",
        operation,
        operation["parameters"],
        {
            "safe": True,
            "max_latency_ms": 200,
            "parameters": {"item_id": "missing"},
        },
    )
    assert case.expected_statuses == (200,)


def test_run_cases_fails_on_documented_server_error_and_latency(monkeypatch) -> None:
    cases = [
        monitor_openapi.ProbeCase("healthy", "GET", "/healthy", (200,), 200),
        monitor_openapi.ProbeCase("unready", "GET", "/ready", (200, 503), 200),
    ]

    def fake_execute(case, **_kwargs):
        if case.operation_id == "healthy":
            return monitor_openapi.ProbeResult("healthy", 200, 40, True)
        return monitor_openapi.ProbeResult("unready", 503, 240, False)

    monkeypatch.setattr(monitor_openapi, "execute_case", fake_execute)
    summary = monitor_openapi.run_cases(
        cases,
        base_url="http://monitor.invalid",
        timeout=1,
        workers=1,
    )

    assert summary == {
        "ok": False,
        "operation_count": 2,
        "failure_count": 1,
        "p95_ms": 240,
        "failures": [
            {
                "operation_id": "unready",
                "status": 503,
                "elapsed_ms": 240,
                "ok": False,
                "error": None,
                "max_latency_ms": 200,
                "reason": "response+latency",
            },
        ],
        "truncated_failure_count": 0,
    }


def test_latency_budget_is_enforced_per_operation(monkeypatch) -> None:
    cases = [
        monitor_openapi.ProbeCase(
            f"operation-{index}", "GET", f"/{index}", (200,), 50
        )
        for index in range(117)
    ]

    def fake_execute(case, **_kwargs):
        index = int(case.operation_id.removeprefix("operation-"))
        elapsed_ms = 10_000 if index >= 112 else 1
        return monitor_openapi.ProbeResult(case.operation_id, 200, elapsed_ms, True)

    monkeypatch.setattr(monitor_openapi, "execute_case", fake_execute)
    summary = monitor_openapi.run_cases(
        cases,
        base_url="http://monitor.invalid",
        timeout=1,
        workers=1,
    )

    assert summary["ok"] is False
    assert summary["p95_ms"] == 1
    assert summary["failure_count"] == 5
    assert {failure["reason"] for failure in summary["failures"]} == {"latency"}


def test_execute_case_rejects_non_json_success(monkeypatch) -> None:
    observed_requests = []

    class Response:
        status = 200
        headers = {"Content-Type": "text/html"}

        def read(self, _size):
            return b"<html>login</html>"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def open_request(request, _timeout):
        observed_requests.append(request)
        return Response()

    monkeypatch.setattr(monitor_openapi, "_open_request", open_request)
    probe_result = monitor_openapi.execute_case(
        monitor_openapi.ProbeCase("html", "GET", "/html", (200,), 200),
        base_url="https://monitor.invalid",
        timeout=1,
    )

    assert probe_result.ok is False
    assert probe_result.error == "invalid_content_type"
    assert observed_requests[0].full_url == "https://monitor.invalid/html"
    assert observed_requests[0].get_header("Authorization") is None


def test_execute_case_rejects_oversized_request_before_network(monkeypatch) -> None:
    monkeypatch.setattr(
        monitor_openapi,
        "_open_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("network request must not be attempted")
        ),
    )
    case = monitor_openapi.ProbeCase(
        "oversized",
        "POST",
        "/bulk",
        (200,),
        200,
        body={"items": ["x" * monitor_openapi.MAX_REQUEST_BYTES]},
    )

    try:
        monitor_openapi.execute_case(
            case,
            base_url="https://monitor.invalid",
            timeout=1,
        )
    except ValueError as exc:
        assert str(exc) == "monitor request body is too large"
    else:
        raise AssertionError("oversized monitor request body was accepted")


def test_response_validation_is_bounded_and_requires_expected_json() -> None:
    required_json = (("status", "OK"),)

    assert (
        monitor_openapi._response_error(b"not-json", "application/json", required_json)
        == "invalid_json"
    )
    assert (
        monitor_openapi._response_error(
            b'{"status":"NOT_OK"}', "application/json", required_json
        )
        == "invalid_json"
    )
    assert (
        monitor_openapi._response_error(
            b'{"status":"OK"}', "application/json", required_json
        )
        is None
    )
    oversized_body = b"x" * (monitor_openapi.MAX_RESPONSE_BYTES + 1)
    assert (
        monitor_openapi._response_error(
            oversized_body, "application/json", required_json
        )
        == "response_too_large"
    )


def test_monitor_rejects_redirects_and_off_origin_case_paths() -> None:
    handler = monitor_openapi.NoRedirectHandler()
    request = monitor_openapi.urllib.request.Request("https://monitor.invalid/start")

    assert (
        handler.redirect_request(
            request,
            None,
            302,
            "redirect",
            {},
            "https://elsewhere.invalid/target",
        )
        is None
    )
    try:
        monitor_openapi.request_url("https://monitor.invalid/api/v1", "//elsewhere.invalid")
    except ValueError as exc:
        assert str(exc) == "monitor case path must stay on the configured origin"
    else:
        raise AssertionError("off-origin monitoring path was accepted")


def test_kuma_push_failure_redacts_secret_url(monkeypatch) -> None:
    def fail(*_args, **_kwargs):
        raise monitor_openapi.urllib.error.URLError("opaque-secret-token")

    monkeypatch.setattr(monitor_openapi, "_open_request", fail)
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


def test_kuma_message_identifies_first_and_truncated_failures() -> None:
    failure_records = [
        {
            "operation_id": f"operation-{index}",
            "status": 500,
            "elapsed_ms": 1,
            "ok": False,
            "error": None,
            "max_latency_ms": 50,
            "reason": "response",
        }
        for index in range(20)
    ]
    summary_by_name = {
        "ok": False,
        "operation_count": 117,
        "failure_count": 21,
        "p95_ms": 1,
        "failures": failure_records,
        "truncated_failure_count": 1,
    }

    message = monitor_openapi.push_message(summary_by_name)

    assert "first=operation-0:response:500" in message
    assert "elapsed_ms=1 budget_ms=50" in message
    assert "additional_failures=20" in message
