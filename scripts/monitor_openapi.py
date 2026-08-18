#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Probe every explicitly safe OpenAPI operation and report aggregate latency."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

if __package__:
    from .monitor_openapi_policy import EXCLUDED_MONITORING_OPERATIONS
    from .monitor_openapi_schema import (
        query_text,
        smallest_pagination_value,
        validate_schema_value,
    )
else:
    from monitor_openapi_policy import EXCLUDED_MONITORING_OPERATIONS
    from monitor_openapi_schema import (
        query_text,
        smallest_pagination_value,
        validate_schema_value,
    )


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OPENAPI_PATH = ROOT / "doc" / "openapi.yaml"
HTTP_METHODS = {"delete", "get", "head", "options", "patch", "post", "put", "trace"}
PAGINATION_MINIMUM_BY_NAME = dict(
    limit=1, offset=0, page=0, page_size=1, per_page=1, results_per_page=1
)
MAX_RESPONSE_BYTES = 65_536
MAX_REQUEST_BYTES = 16_384
MAX_REPORTED_FAILURES = 20


@dataclass(frozen=True)
class ProbeCase:
    operation_id: str
    method: str
    path: str
    expected_statuses: tuple[int, ...]
    max_latency_ms: int
    body: dict[str, Any] | None = None
    required_json: tuple[tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class ProbeResult:
    operation_id: str
    status: int | None
    elapsed_ms: int
    ok: bool
    error: str | None = None


def load_spec(path: Path) -> dict[str, Any]:
    """Load the checked-in OpenAPI contract."""
    return yaml.safe_load(path.read_text())


def resolve_parameter(spec: dict[str, Any], parameter: dict[str, Any]) -> dict[str, Any]:
    """Resolve a local reusable OpenAPI parameter."""
    reference = parameter.get("$ref")
    if not reference:
        return parameter
    prefix = "#/components/parameters/"
    if not str(reference).startswith(prefix):
        raise ValueError(f"unsupported parameter reference: {reference}")
    return spec["components"]["parameters"][str(reference)[len(prefix) :]]


def operation_cases(spec: dict[str, Any]) -> list[ProbeCase]:
    """Build only explicitly reviewed cases and reject policy drift."""
    cases: list[ProbeCase] = []
    documented_operation_ids: set[str] = set()
    for path, path_item in sorted((spec.get("paths") or {}).items()):
        shared_parameters = path_item.get("parameters") or []
        for method, operation in sorted(path_item.items()):
            if method not in HTTP_METHODS:
                continue
            operation_id = str(operation.get("operationId") or "").strip()
            if not operation_id or operation_id in documented_operation_ids:
                raise ValueError(f"{method.upper()} {path} needs a unique operationId")
            documented_operation_ids.add(operation_id)
            monitoring = operation.get("x-monitoring") or {}
            if operation_id in EXCLUDED_MONITORING_OPERATIONS:
                if monitoring:
                    raise ValueError(f"{operation_id} has conflicting monitoring policies")
                if not EXCLUDED_MONITORING_OPERATIONS[operation_id].strip():
                    raise ValueError(f"{operation_id} has an empty monitoring exclusion")
                continue
            if not monitoring:
                raise ValueError(f"{method.upper()} {path} has no explicit monitoring policy")
            if monitoring.get("safe") is not True:
                raise ValueError(f"{operation_id} monitoring policy is not explicitly safe")
            cases.append(
                build_case(
                    spec,
                    path,
                    method,
                    operation,
                    [*shared_parameters, *(operation.get("parameters") or [])],
                    monitoring,
                )
            )
    stale_policy_ids = set(EXCLUDED_MONITORING_OPERATIONS) - documented_operation_ids
    if stale_policy_ids:
        raise ValueError(f"monitoring policy refers to absent operation: {min(stale_policy_ids)}")
    if not cases:
        raise ValueError("OpenAPI contract has no safe monitoring cases")
    return cases


def build_case(
    spec: dict[str, Any],
    path: str,
    method: str,
    operation: dict[str, Any],
    raw_parameters: list[dict[str, Any]],
    monitoring: dict[str, Any],
) -> ProbeCase:
    """Materialize one schema-valid bounded request."""
    rendered_path = _render_case_path(
        spec, path, method, raw_parameters, monitoring.get("parameters") or {}
    )
    documented_statuses = {
        int(status)
        for status in (operation.get("responses") or {})
        if str(status).isdigit()
    }
    expected_statuses = tuple(
        sorted(status for status in documented_statuses if 200 <= status < 300)
    )
    if not expected_statuses:
        raise ValueError(f"{method.upper()} {path} has no successful monitoring status")
    body = _validated_request_body(path, method, operation, monitoring)
    max_latency_ms = monitoring.get("max_latency_ms")
    if (
        isinstance(max_latency_ms, bool)
        or not isinstance(max_latency_ms, int)
        or max_latency_ms <= 0
    ):
        raise ValueError(f"{operation.get('operationId')} needs a positive max_latency_ms")
    required_json = monitoring.get("required_json") or {}
    if not isinstance(required_json, dict):
        raise ValueError("required_json must be an object")
    return ProbeCase(
        operation_id=str(operation.get("operationId")),
        method=method.upper(),
        path=rendered_path,
        expected_statuses=expected_statuses,
        max_latency_ms=max_latency_ms,
        body=body,
        required_json=tuple(sorted(required_json.items())),
    )


def _render_case_path(
    spec: dict[str, Any],
    path: str,
    method: str,
    raw_parameters: list[dict[str, Any]],
    configured_parameters: dict[str, Any],
) -> str:
    """Render and validate one case's path and query parameters."""
    query_pairs: list[tuple[str, str]] = []
    rendered_path = path
    if not isinstance(configured_parameters, dict):
        raise ValueError("monitoring parameters must be an object")
    declared_parameter_names: set[str] = set()
    declared_parameter_keys: set[tuple[Any, str]] = set()
    for raw_parameter in raw_parameters:
        parameter = resolve_parameter(spec, raw_parameter)
        name = str(parameter.get("name") or "")
        location = parameter.get("in")
        parameter_key = (location, name)
        if not name or parameter_key in declared_parameter_keys:
            raise ValueError(f"{method.upper()} {path} has an invalid parameter declaration")
        declared_parameter_names.add(name)
        declared_parameter_keys.add(parameter_key)
        if name in configured_parameters:
            parameter_value = configured_parameters[name]
        elif location == "query" and name.lower() in PAGINATION_MINIMUM_BY_NAME:
            parameter_value = smallest_pagination_value(
                PAGINATION_MINIMUM_BY_NAME[name.lower()], parameter.get("schema") or {}
            )
        elif location == "path" or parameter.get("required"):
            raise ValueError(
                f"{method.upper()} {path} needs an explicit monitoring value for {name}"
            )
        else:
            continue
        validate_schema_value(parameter_value, parameter.get("schema") or {}, name)
        if location == "path":
            rendered_path = rendered_path.replace(
                "{" + name + "}", urllib.parse.quote(str(parameter_value), safe="")
            )
        elif location == "query":
            query_pairs.append((name, query_text(parameter_value)))
    unknown_parameters = set(configured_parameters) - declared_parameter_names
    if unknown_parameters:
        raise ValueError(f"monitoring policy has unknown parameter: {min(unknown_parameters)}")
    if "{" in rendered_path or "}" in rendered_path:
        raise ValueError(f"unresolved path parameter for {method.upper()} {path}")
    if query_pairs:
        rendered_path = f"{rendered_path}?{urllib.parse.urlencode(query_pairs)}"
    return rendered_path


def _validated_request_body(
    path: str,
    method: str,
    operation: dict[str, Any],
    monitoring: dict[str, Any],
) -> dict[str, Any] | None:
    """Return a schema-valid JSON request body when the case declares one."""
    body = monitoring.get("request_body")
    request_body = operation.get("requestBody") or {}
    if request_body.get("required") and body is None:
        raise ValueError(f"{method.upper()} {path} needs an explicit monitoring request body")
    if body is not None:
        body_schema = (
            (request_body.get("content") or {})
            .get("application/json", {})
            .get("schema", {})
        )
        if not body_schema:
            raise ValueError(f"{method.upper()} {path} has no JSON request body schema")
        validate_schema_value(body, body_schema, "request_body")
    return body


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Return redirect responses to the caller instead of following them."""

    def redirect_request(self, *_args, **_kwargs):
        """Reject redirects so credentials cannot leave the configured origin."""
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(NoRedirectHandler())


def request_url(base_url: str, case_path: str) -> str:
    """Return a URL whose origin is exactly the configured service origin."""
    parsed_base = urllib.parse.urlsplit(base_url)
    if (
        parsed_base.scheme not in {"http", "https"}
        or not parsed_base.netloc
        or parsed_base.query
        or parsed_base.fragment
        or not case_path.startswith("/")
        or case_path.startswith("//")
    ):
        raise ValueError("monitor case path must stay on the configured origin")
    url = base_url.rstrip("/") + case_path
    parsed_url = urllib.parse.urlsplit(url)
    if (parsed_url.scheme, parsed_url.netloc) != (parsed_base.scheme, parsed_base.netloc):
        raise ValueError("monitor case path must stay on the configured origin")
    return url


def _open_request(request: urllib.request.Request, timeout: float):
    return _NO_REDIRECT_OPENER.open(request, timeout=timeout)


def _json_path_value(payload: Any, path: str) -> Any:
    value = payload
    for key in path.split("."):
        if not isinstance(value, dict) or key not in value:
            raise ValueError("missing required JSON field")
        value = value[key]
    return value


def execute_case(
    probe_case: ProbeCase,
    *,
    base_url: str,
    timeout: float,
) -> ProbeResult:
    """Execute one same-origin request and validate its bounded JSON response."""
    body = (
        None
        if probe_case.body is None
        else json.dumps(probe_case.body).encode("utf-8")
    )
    if body is not None and len(body) > MAX_REQUEST_BYTES:
        raise ValueError("monitor request body is too large")
    headers_by_name = {"Accept": "application/json", "User-Agent": "HealthPortaMonitor/1.0"}
    if body is not None:
        headers_by_name["Content-Type"] = "application/json"
    request = urllib.request.Request(
        request_url(base_url, probe_case.path),
        data=body,
        headers=headers_by_name,
        method=probe_case.method,
    )
    started = time.perf_counter()
    try:
        with _open_request(request, timeout) as response:
            status = response.status
            content_type = str(response.headers.get("Content-Type", "")).lower()
            response_body = response.read(MAX_RESPONSE_BYTES + 1)
            error = _response_error(
                response_body, content_type, probe_case.required_json
            )
    except urllib.error.HTTPError as exc:
        status = exc.code
        error = "redirect" if 300 <= status < 400 else None
        exc.close()
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        status = None
        error = type(exc).__name__
    elapsed_ms = max(0, round((time.perf_counter() - started) * 1000))
    is_healthy = status in probe_case.expected_statuses and error is None
    return ProbeResult(probe_case.operation_id, status, elapsed_ms, is_healthy, error)


def _response_error(
    response_body: bytes,
    content_type: str,
    required_json: tuple[tuple[str, Any], ...],
) -> str | None:
    """Return a bounded response-validation error, if any."""
    if "application/json" not in content_type and "+json" not in content_type:
        return "invalid_content_type"
    if len(response_body) > MAX_RESPONSE_BYTES:
        return "response_too_large"
    try:
        payload = json.loads(response_body)
        for path, expected_value in required_json:
            if _json_path_value(payload, path) != expected_value:
                raise ValueError("unexpected required JSON value")
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return "invalid_json"
    return None


def percentile_95(values: list[int]) -> int:
    """Return the nearest-rank p95 for a non-empty sample."""
    if not values:
        raise ValueError("p95 requires at least one value")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]


def run_cases(
    cases: list[ProbeCase],
    *,
    base_url: str,
    timeout: float,
    workers: int,
) -> dict[str, Any]:
    """Run all cases and return a redacted aggregate result."""
    if workers < 1 or timeout <= 0:
        raise ValueError("workers and timeout must be positive")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        probe_results = list(
            executor.map(
                lambda case: execute_case(
                    case,
                    base_url=base_url,
                    timeout=timeout,
                ),
                cases,
            )
        )
    p95_ms = percentile_95([probe_result.elapsed_ms for probe_result in probe_results])
    failures = []
    for case, probe_result in zip(cases, probe_results):
        is_response_failed = not probe_result.ok
        is_latency_failed = probe_result.elapsed_ms > case.max_latency_ms
        if not is_response_failed and not is_latency_failed:
            continue
        failure = asdict(probe_result)
        failure["max_latency_ms"] = case.max_latency_ms
        if is_response_failed and is_latency_failed:
            failure["reason"] = "response+latency"
        elif is_response_failed:
            failure["reason"] = "response"
        else:
            failure["reason"] = "latency"
        failures.append(failure)
    reported_failures = failures[:MAX_REPORTED_FAILURES]
    return {
        "ok": not failures,
        "operation_count": len(cases),
        "failure_count": len(failures),
        "p95_ms": p95_ms,
        "failures": reported_failures,
        "truncated_failure_count": len(failures) - len(reported_failures),
    }


def push_message(summary: dict[str, Any]) -> str:
    """Return a bounded message that identifies the first failing operation."""
    message = (
        f"operations={summary['operation_count']} failures={summary['failure_count']} "
        f"p95_ms={summary['p95_ms']}"
    )
    failures = summary.get("failures") or []
    if failures:
        first_failure = failures[0]
        message += (
            f" first={first_failure['operation_id']}:{first_failure['reason']}:"
            f"{first_failure['status']} elapsed_ms={first_failure['elapsed_ms']}"
            f" budget_ms={first_failure['max_latency_ms']}"
        )
        additional_failures = max(0, int(summary["failure_count"]) - 1)
        if additional_failures:
            message += f" additional_failures={additional_failures}"
    return message


def push_summary(push_url: str, summary: dict[str, Any]) -> None:
    """Publish the aggregate outcome to an Uptime Kuma Push monitor."""
    parsed_url = urllib.parse.urlsplit(push_url)
    if (
        parsed_url.scheme not in {"http", "https"}
        or not parsed_url.netloc
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise ValueError("Kuma push URL must not contain a query or fragment")
    state = "up" if summary["ok"] else "down"
    message = push_message(summary)
    separator = "&" if "?" in push_url else "?"
    url = push_url + separator + urllib.parse.urlencode(
        {"status": state, "msg": message, "ping": summary["p95_ms"]}
    )
    try:
        request = urllib.request.Request(url, method="GET")
        with _open_request(request, 10) as response:
            if response.status >= 300:
                raise RuntimeError("Kuma push failed")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        if isinstance(exc, urllib.error.HTTPError):
            exc.close()
        raise RuntimeError("Kuma push failed") from None


def parse_args() -> argparse.Namespace:
    """Parse command-line and environment configuration."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--openapi", type=Path, default=DEFAULT_OPENAPI_PATH)
    parser.add_argument("--base-url", default=os.getenv("MONITOR_BASE_URL", ""))
    parser.add_argument("--push-url", default=os.getenv("KUMA_PUSH_URL", ""))
    parser.add_argument("--timeout", type=float, default=2.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


def main() -> int:
    """Run contract validation or the bounded live probe."""
    args = parse_args()
    probe_cases = operation_cases(load_spec(args.openapi))
    if args.check:
        check_counts_by_name = {
            "excluded_operation_count": len(EXCLUDED_MONITORING_OPERATIONS),
            "safe_operation_count": len(probe_cases),
        }
        print(json.dumps(check_counts_by_name, sort_keys=True))
        return 0
    if not args.base_url:
        raise SystemExit("--base-url or MONITOR_BASE_URL is required")
    summary = run_cases(
        probe_cases,
        base_url=args.base_url,
        timeout=args.timeout,
        workers=args.workers,
    )
    print(json.dumps(summary, sort_keys=True))
    if args.push_url:
        push_summary(args.push_url, summary)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
