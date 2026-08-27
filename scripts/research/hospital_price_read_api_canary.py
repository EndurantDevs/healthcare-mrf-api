#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Gate one populated packed hospital-price page on latency and physical storage."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import sys
import time
from typing import Any, Mapping
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from api.hospital_price_request import validate_hospital_price_query
from scripts.research.hospital_price_canary_storage import CanaryError
from scripts.research.hospital_price_canary_storage import (
    capture_storage_receipt as _storage_receipt,
)


_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")
_HEADER_PATTERN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+\Z")
_VERSION_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_MAX_RESPONSE_BYTES = 2 << 20


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base-url", required=True)
    parser.add_argument("--hospital-id", required=True)
    parser.add_argument("--code-type", required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--payer-name")
    parser.add_argument("--plan-name")
    parser.add_argument("--version-id", required=True)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--minimum-scanned", type=int, default=1)
    parser.add_argument("--minimum-items", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--header-env", action="append", default=[])
    parser.add_argument("--database-url-env", default="HOSPITAL_PRICE_CANARY_DATABASE_URL")
    parser.add_argument("--database-schema", default="mrf")
    parser.add_argument("--pre-import-receipt", type=Path, required=True)
    parser.add_argument("--maximum-baseline-age-seconds", type=float, default=21_600.0)
    parser.add_argument(
        "--maximum-physical-storage-ratio", type=float, default=0.2
    )
    parser.add_argument("--maximum-packed-payload-ratio", type=float)
    parser.add_argument("--maximum-cold-ms", type=float, required=True)
    parser.add_argument("--maximum-warm-p95-ms", type=float, required=True)
    parser.add_argument("--allow-insecure-http", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _base_url(value: str, allow_insecure_http: bool) -> str:
    normalized = value.strip().rstrip("/")
    parsed = urlsplit(normalized)
    allowed = {"https"} | ({"http"} if allow_insecure_http else set())
    if (
        parsed.scheme not in allowed or not parsed.hostname
        or parsed.username or parsed.password or parsed.query or parsed.fragment
    ):
        raise CanaryError("API base URL is invalid")
    return normalized


def _load_baseline_receipt(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise CanaryError("pre-import storage receipt is unreadable") from None
    if type(payload) is not dict:
        raise CanaryError("pre-import storage receipt is invalid")
    return payload


def _headers(specifications: list[str]) -> tuple[dict[str, str], list[str]]:
    header_by_name: dict[str, str] = {}
    environment_names = []
    for specification in specifications:
        header, separator, environment_name = specification.partition("=")
        header, environment_name = header.strip(), environment_name.strip()
        value = os.getenv(environment_name)
        if (
            not separator or _HEADER_PATTERN.fullmatch(header) is None
            or _IDENTIFIER_PATTERN.fullmatch(environment_name) is None or not value
            or header.lower() in {name.lower() for name in header_by_name}
        ):
            raise CanaryError("header inputs must be unique HTTP-Header=ENV_NAME pairs")
        header_by_name[header] = value
        environment_names.append(environment_name)
    return header_by_name, environment_names


def _query_parameters(args: argparse.Namespace) -> dict[str, str]:
    try:
        query = validate_hospital_price_query(
            args.hospital_id,
            code_type=args.code_type,
            code=args.code,
            payer_name=args.payer_name,
            plan_name=args.plan_name,
            version_id=args.version_id,
            limit=str(args.limit),
        )
    except RuntimeError:
        raise CanaryError("hospital-price query is invalid") from None
    parameters_by_name = {
        "code_type": query.code_type,
        "code": query.code,
        "limit": str(query.limit),
    }
    if query.payer_name is not None:
        parameters_by_name.update(
            payer_name=query.payer_name,
            plan_name=query.plan_name,
        )
    if query.version_id is not None:
        parameters_by_name["version_id"] = query.version_id
    return parameters_by_name


def _validate_response_item(
    response_item_by_field: object,
    args: argparse.Namespace,
    has_payer_request: bool,
) -> None:
    """Require one exact-code charge and its requested nested price facts."""

    service_by_field = (
        response_item_by_field.get("service")
        if type(response_item_by_field) is dict else None
    )
    charge_by_field = (
        response_item_by_field.get("charge")
        if type(response_item_by_field) is dict else None
    )
    codes_by_field = (
        service_by_field.get("codes")
        if type(service_by_field) is dict else None
    )
    facts_by_field = (
        response_item_by_field.get("negotiated_prices")
        if type(response_item_by_field) is dict else None
    )
    if (
        type(charge_by_field) is not dict or type(codes_by_field) is not list
        or not any(
            type(code_by_field) is dict
            and code_by_field.get("code_type") == args.code_type
            and code_by_field.get("code") == args.code
            for code_by_field in codes_by_field
        )
        or type(facts_by_field) is not list
        or (has_payer_request and not facts_by_field)
        or (not has_payer_request and facts_by_field)
        or any(
            type(fact_by_field) is not dict
            or fact_by_field.get("payer_name") != args.payer_name
            or fact_by_field.get("plan_name") != args.plan_name
            for fact_by_field in facts_by_field
        )
    ):
        raise CanaryError("hospital-price nested fact binding is invalid")


def _response_values(
    response_payload_by_field: object,
    *,
    args: argparse.Namespace,
) -> tuple[str, int, int]:
    """Validate response/query/version binding and return stable sample counts."""

    if type(response_payload_by_field) is not dict:
        raise CanaryError("hospital-price response is not an object")
    version = response_payload_by_field.get("version")
    query = response_payload_by_field.get("query")
    pagination = response_payload_by_field.get("pagination")
    response_items = response_payload_by_field.get("items")
    if not all(
        type(metadata_section) is dict
        for metadata_section in (version, query, pagination)
    ):
        raise CanaryError("hospital-price response metadata is invalid")
    version_id = version.get("version_id")
    has_payer_request = args.payer_name is not None
    if (
        response_payload_by_field.get("hospital_id") != args.hospital_id
        or type(version_id) is not str
        or _VERSION_PATTERN.fullmatch(version_id) is None
        or (args.version_id is not None and version_id != args.version_id)
        or query.get("code_type") != args.code_type
        or query.get("code") != args.code
        or query.get("payer_name") != args.payer_name
        or query.get("plan_name") != args.plan_name
        or query.get("negotiated_prices_requested") is not has_payer_request
        or pagination.get("unit") != "charges"
        or pagination.get("limit") != args.limit
        or type(pagination.get("scanned")) is not int
        or not 0 <= pagination["scanned"] <= args.limit
        or type(response_items) is not list
        or len(response_items) > pagination["scanned"]
    ):
        raise CanaryError("hospital-price response binding is invalid")
    for response_item_by_field in response_items:
        _validate_response_item(response_item_by_field, args, has_payer_request)
    return version_id, pagination["scanned"], len(response_items)


def _http_sample(
    url: str,
    headers: Mapping[str, str],
    timeout_seconds: float,
) -> tuple[float, bytes, Mapping[str, str]]:
    request = Request(url, headers=dict(headers), method="GET")
    started = time.perf_counter_ns()
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read(_MAX_RESPONSE_BYTES + 1)
        status = response.status
        response_header_by_name = dict(response.headers.items())
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    if status != 200 or len(body) > _MAX_RESPONSE_BYTES:
        raise CanaryError("hospital-price HTTP response is invalid")
    return elapsed_ms, body, response_header_by_name


def _validated_http_sample(
    args: argparse.Namespace,
    url: str,
    headers: Mapping[str, str],
) -> tuple[float, str, str, int, int]:
    """Return one response-bound latency sample and its stable identity."""

    elapsed_ms, body, response_header_by_name = _http_sample(
        url, headers, args.timeout_seconds
    )
    cache_control = response_header_by_name.get("Cache-Control", "").lower()
    if "private" not in cache_control or "no-store" not in cache_control:
        raise CanaryError("hospital-price response cache policy is invalid")
    try:
        response_payload_by_field = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise CanaryError("hospital-price response JSON is invalid") from None
    version_id, scanned, item_count = _response_values(
        response_payload_by_field,
        args=args,
    )
    return (
        elapsed_ms,
        hashlib.sha256(body).hexdigest(),
        version_id,
        scanned,
        item_count,
    )


def _latency_receipt(
    args: argparse.Namespace,
    url: str,
    headers: Mapping[str, str],
) -> dict[str, object]:
    cold_sample = _validated_http_sample(args, url, headers)
    stable_identity = cold_sample[1:]
    warm_latencies = []
    for ordinal in range(args.warmups + args.samples):
        sample = _validated_http_sample(args, url, headers)
        if sample[1:] != stable_identity:
            raise CanaryError("hospital-price populated sample is not stable")
        if ordinal >= args.warmups:
            warm_latencies.append(sample[0])
    _digest, version_id, scanned, item_count = stable_identity
    if scanned < args.minimum_scanned or item_count < args.minimum_items:
        raise CanaryError("hospital-price populated sample is not stable")
    ordered = sorted(warm_latencies)
    return {
        "version_id": version_id,
        "response_sha256": _digest,
        "cold_ms": round(cold_sample[0], 3),
        "samples": len(warm_latencies),
        "minimum_scanned_charges": scanned,
        "minimum_returned_charges": item_count,
        "median_ms": round(statistics.median(warm_latencies), 3),
        "p95_ms": round(ordered[math.ceil(0.95 * len(ordered)) - 1], 3),
        "maximum_ms": round(max(warm_latencies), 3),
    }


def _canary_gates(
    args: argparse.Namespace,
    latency: Mapping[str, object],
    storage: Mapping[str, object],
) -> dict[str, object]:
    packed_payload_ratio = (
        int(storage["packed_payload_bytes"])
        / int(storage["source_content_bytes"])
    )
    physical_ratio = (
        int(storage["physical_growth_bytes"])
        / int(storage["unique_downloaded_source_bytes"])
    )
    return {
        "maximum_physical_storage_ratio": args.maximum_physical_storage_ratio,
        "physical_storage_ratio_passed": (
            physical_ratio <= args.maximum_physical_storage_ratio
        ),
        "maximum_cold_ms": args.maximum_cold_ms,
        "cold_latency_passed": float(latency["cold_ms"]) <= args.maximum_cold_ms,
        "maximum_warm_p95_ms": args.maximum_warm_p95_ms,
        "warm_p95_latency_passed": (
            float(latency["p95_ms"]) <= args.maximum_warm_p95_ms
        ),
        "maximum_packed_payload_ratio": args.maximum_packed_payload_ratio,
        "packed_payload_ratio_diagnostic_passed": (
            None if args.maximum_packed_payload_ratio is None
            else packed_payload_ratio <= args.maximum_packed_payload_ratio
        ),
    }


def _canary_result(
    args: argparse.Namespace,
    header_environment_names: list[str],
    parameters_by_name: Mapping[str, str],
    latency: Mapping[str, object],
    storage: Mapping[str, object],
    gates: Mapping[str, object],
) -> dict[str, object]:
    is_passed = all(
        gates[name] is True
        for name in (
            "physical_storage_ratio_passed",
            "cold_latency_passed",
            "warm_p95_latency_passed",
        )
    )
    return {
        "schema_version": 2,
        "status": "passed" if is_passed else "gate_failed",
        "captured_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "contract": {
            "pagination_unit": "charges",
            "payer_omission": "charge_metadata_only",
            "payer_pair": "all_or_none_exact_nested_facts",
            "storage_measurement": (
                "quiescent_relation_delta_including_heap_toast_and_indexes"
            ),
            "header_environment_names": header_environment_names,
        },
        "query": {"hospital_id": args.hospital_id, **parameters_by_name},
        "latency": dict(latency),
        "storage": dict(storage),
        "gates": dict(gates),
    }


def capture_canary_receipt(args: argparse.Namespace) -> dict[str, object]:
    """Run source-bound latency and quiescent physical-storage gates."""

    if (
        args.warmups < 0 or args.samples < 1
        or args.minimum_scanned < 1 or args.minimum_items < 1
        or args.timeout_seconds <= 0
        or args.version_id is None
        or args.maximum_baseline_age_seconds <= 0
        or not 0 < args.maximum_physical_storage_ratio <= 1
        or args.maximum_cold_ms <= 0
        or args.maximum_warm_p95_ms <= 0
        or _IDENTIFIER_PATTERN.fullmatch(args.database_schema) is None
        or (
            args.maximum_packed_payload_ratio is not None
            and not 0 < args.maximum_packed_payload_ratio <= 1
        )
    ):
        raise CanaryError("canary bounds are invalid")
    base_url = _base_url(args.api_base_url, args.allow_insecure_http)
    header_by_name, header_environment_names = _headers(args.header_env)
    parameters_by_name = _query_parameters(args)
    url = (
        f"{base_url}/api/v1/hospital-prices/facilities/"
        f"{args.hospital_id}/prices?{urlencode(parameters_by_name)}"
    )
    latency = _latency_receipt(args, url, header_by_name)
    dsn = os.getenv(args.database_url_env)
    if not dsn:
        raise CanaryError(f"database URL environment is unset: {args.database_url_env}")
    baseline = _load_baseline_receipt(args.pre_import_receipt)
    storage = asyncio.run(
        _storage_receipt(
            dsn,
            args.database_schema,
            str(latency["version_id"]),
            baseline,
            args.timeout_seconds,
            args.maximum_baseline_age_seconds,
        )
    )
    gates = _canary_gates(args, latency, storage)
    return _canary_result(
        args, header_environment_names, parameters_by_name, latency, storage, gates
    )


def main(argv: list[str] | None = None) -> int:
    """Capture and persist one read-only hospital-price canary receipt."""

    args = _parser().parse_args(argv)
    try:
        receipt = capture_canary_receipt(args)
    except (CanaryError, ValueError) as error:
        raise SystemExit(str(error)) from None
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(receipt, sort_keys=True))
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
