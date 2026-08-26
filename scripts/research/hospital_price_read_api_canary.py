#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Measure one populated packed hospital-price page and its payload ratio."""

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


_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")
_HEADER_PATTERN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+\Z")
_VERSION_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_MAX_RESPONSE_BYTES = 2 << 20


class CanaryError(RuntimeError):
    """Reject an unsafe canary configuration or invalid observation."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base-url", required=True)
    parser.add_argument("--hospital-id", required=True)
    parser.add_argument("--code-type", required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--payer-name")
    parser.add_argument("--plan-name")
    parser.add_argument("--version-id")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--minimum-scanned", type=int, default=1)
    parser.add_argument("--minimum-items", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--header-env", action="append", default=[])
    parser.add_argument("--database-url-env", default="HOSPITAL_PRICE_CANARY_DATABASE_URL")
    parser.add_argument("--database-schema", default="mrf")
    parser.add_argument("--maximum-packed-payload-ratio", type=float)
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


def _latency_receipt(
    args: argparse.Namespace,
    url: str,
    headers: Mapping[str, str],
) -> dict[str, object]:
    latencies = []
    digests = set()
    version_ids = set()
    scanned_values = set()
    item_counts = set()
    for ordinal in range(args.warmups + args.samples):
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
        if ordinal >= args.warmups:
            latencies.append(elapsed_ms)
            digests.add(hashlib.sha256(body).hexdigest())
            version_ids.add(version_id)
            scanned_values.add(scanned)
            item_counts.add(item_count)
    if (
        len(digests) != 1 or len(version_ids) != 1
        or min(scanned_values) < args.minimum_scanned
        or min(item_counts) < args.minimum_items
    ):
        raise CanaryError("hospital-price populated sample is not stable")
    ordered = sorted(latencies)
    return {
        "version_id": next(iter(version_ids)),
        "response_sha256": next(iter(digests)),
        "samples": len(latencies),
        "minimum_scanned_charges": min(scanned_values),
        "minimum_returned_charges": min(item_counts),
        "median_ms": round(statistics.median(latencies), 3),
        "p95_ms": round(ordered[math.ceil(0.95 * len(ordered)) - 1], 3),
        "maximum_ms": round(max(latencies), 3),
    }


async def _storage_receipt(
    dsn: str,
    schema: str,
    version_id: str,
) -> dict[str, object]:
    import asyncpg

    normalized_dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    normalized_dsn = normalized_dsn.replace("postgresql+psycopg://", "postgresql://", 1)
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    connection = await asyncpg.connect(normalized_dsn)
    try:
        version = await connection.fetchrow(
            f"""SELECT content.byte_count, root.service_count,
                       root.charge_count, root.fact_count
                  FROM {quoted_schema}.hospital_price_version version
                  JOIN {quoted_schema}.hospital_price_content content
                    ON content.content_sha256=version.content_sha256
                  JOIN {quoted_schema}.hospital_price_packed_root root
                    ON root.version_id=version.version_id
                 WHERE version.version_id=$1""",
            version_id,
        )
        blocks = await connection.fetch(
            f"""SELECT block_kind, count(*)::bigint AS block_count,
                       sum(octet_length(payload))::bigint AS payload_bytes
                  FROM {quoted_schema}.hospital_price_data_block
                 WHERE version_id=$1 GROUP BY block_kind ORDER BY block_kind""",
            version_id,
        )
    finally:
        await connection.close()
    if version is None or not blocks:
        raise CanaryError("packed storage evidence is unavailable")
    source_bytes = int(version["byte_count"])
    packed_payload_bytes = sum(
        int(block_row["payload_bytes"])
        for block_row in blocks
    )
    if source_bytes <= 0 or packed_payload_bytes <= 0:
        raise CanaryError("packed storage byte counts are invalid")
    return {
        "measurement": "version_scoped_native_block_payloads_excluding_heap_and_index_overhead",
        "source_content_bytes": source_bytes,
        "packed_payload_bytes": packed_payload_bytes,
        "packed_payload_ratio_to_source": round(packed_payload_bytes / source_bytes, 6),
        "service_count": int(version["service_count"]),
        "charge_count": int(version["charge_count"]),
        "fact_count": int(version["fact_count"]),
        "blocks": [
            {
                "block_kind": int(block_row["block_kind"]),
                "block_count": int(block_row["block_count"]),
                "payload_bytes": int(block_row["payload_bytes"]),
            }
            for block_row in blocks
        ],
    }


def capture_canary_receipt(args: argparse.Namespace) -> dict[str, object]:
    """Run the read-only latency and version-scoped storage observations."""

    if (
        args.warmups < 0 or args.samples < 1
        or args.minimum_scanned < 1 or args.minimum_items < 1
        or args.timeout_seconds <= 0
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
    storage = asyncio.run(
        _storage_receipt(dsn, args.database_schema, str(latency["version_id"]))
    )
    ratio = (
        int(storage["packed_payload_bytes"])
        / int(storage["source_content_bytes"])
    )
    is_ratio_passed = (
        None if args.maximum_packed_payload_ratio is None
        else ratio <= args.maximum_packed_payload_ratio
    )
    return {
        "schema_version": 1,
        "status": "passed" if is_ratio_passed is not False else "gate_failed",
        "captured_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "contract": {
            "pagination_unit": "charges",
            "payer_omission": "charge_metadata_only",
            "payer_pair": "all_or_none_exact_nested_facts",
            "header_environment_names": header_environment_names,
        },
        "query": {
            "hospital_id": args.hospital_id,
            **parameters_by_name,
        },
        "latency": latency,
        "storage": storage,
        "gates": {
            "maximum_packed_payload_ratio": args.maximum_packed_payload_ratio,
            "packed_payload_ratio_passed": is_ratio_passed,
        },
    }


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
