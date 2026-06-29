#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build a PTG price-address assurance report.

Examples:

  rtk venv/bin/python scripts/research/ptg_address_assurance_report.py \
    --api-payload /tmp/ptg-response.json \
    --raw-artifact /work/ptg2-artifacts/raw/source.json.gz

  curl -sS "$PTG_URL" | rtk venv/bin/python scripts/research/ptg_address_assurance_report.py --api-payload -

  rtk venv/bin/python scripts/research/ptg_address_assurance_report.py \
    --api-url "$PTG_URL" \
    --api-key "$HEALTHPORTA_API_KEY" \
    --require-network-bound-address \
    --strict
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from process.ptg_parts.address_assurance import (
    build_ptg_address_assurance_report,
    source_file_version_ids_from_ptg_payload,
)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-payload",
        help="API response JSON path, or '-' to read from stdin. Omit to only audit raw artifacts.",
    )
    parser.add_argument(
        "--api-url",
        help="Fetch API response JSON directly from this URL. Mutually exclusive with --api-payload.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("HEALTHPORTA_API_KEY"),
        help="Bearer token for --api-url. Defaults to HEALTHPORTA_API_KEY.",
    )
    parser.add_argument("--timeout", type=float, default=60.0, help="Seconds to wait for --api-url.")
    parser.add_argument(
        "--raw-artifact",
        action="append",
        default=[],
        help="Raw TiC artifact path to scan for direct provider address/phone-like fields. Can be repeated.",
    )
    parser.add_argument(
        "--source-file-version-id",
        action="append",
        default=[],
        help=(
            "PTG source_file_version_id to resolve to a retained raw artifact through "
            "mrf.ptg2_source_file_version. Can be repeated."
        ),
    )
    parser.add_argument(
        "--resolve-raw-artifacts-from-db",
        action="store_true",
        help=(
            "Resolve source_file_version_id values found in API source_trace, plus any explicit "
            "--source-file-version-id values, through the PTG source-file table and audit local file:// raw artifacts."
        ),
    )
    parser.add_argument(
        "--require-resolved-raw-artifacts",
        action="store_true",
        help=(
            "Fail when source_file_version_id values cannot be resolved to existing local raw artifacts. "
            "Use with --resolve-raw-artifacts-from-db for strict live/deployed assurance."
        ),
    )
    parser.add_argument("--db-host", default=os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1")
    parser.add_argument("--db-port", type=int, default=int(os.getenv("HLTHPRT_DB_PORT") or "5432"))
    parser.add_argument("--database", default=os.getenv("HLTHPRT_DB_DATABASE") or "healthporta")
    parser.add_argument("--db-user", default=os.getenv("HLTHPRT_DB_USER") or "postgres")
    parser.add_argument("--db-password", default=os.getenv("HLTHPRT_DB_PASSWORD") or "")
    parser.add_argument("--db-schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--max-samples", type=int, default=5, help="Maximum raw direct-location samples per artifact.")
    parser.add_argument(
        "--allow-missing-displayed-address",
        action="store_true",
        help=(
            "Accept rows that explicitly set displayed_address_present=false and expose no usable address. "
            "Use this for schema/provenance checks, not for member-facing displayed-address assurance."
        ),
    )
    parser.add_argument(
        "--require-network-names",
        action="store_true",
        help="Fail when any PTG row in the API payload lacks retained network_names.",
    )
    parser.add_argument(
        "--require-source-file-version-id",
        action="store_true",
        help="Fail when any PTG row in the API payload lacks source_trace.source_file_version_id.",
    )
    parser.add_argument(
        "--require-network-bound-address",
        action="store_true",
        help=(
            "Fail when a displayed PTG address is only inferred from provider identity. "
            "Use this for member-facing exact-office assurance; it requires payer-confirmed "
            "location evidence or payer Provider Directory plan/network corroboration, plus retained "
            "network_names and source_trace.source_file_version_id."
        ),
    )
    parser.add_argument("--strict", action="store_true", help="Exit 1 when the assurance report is not ok.")
    return parser.parse_args()


def _fetch_api_payload(url: str, api_key: str | None, timeout: float) -> Any:
    headers = {"Accept": "application/json", "User-Agent": "healthporta-ptg-address-assurance/1.0"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=max(float(timeout), 1.0)) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return json.loads(response.read().decode(charset))


def _load_api_payload(path: str | None, api_url: str | None, api_key: str | None, timeout: float) -> Any | None:
    if path and api_url:
        raise ValueError("--api-payload and --api-url are mutually exclusive")
    if api_url:
        return _fetch_api_payload(api_url, api_key, timeout)
    if not path:
        return None
    if path == "-":
        return json.load(sys.stdin)
    with Path(path).open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def _file_uri_to_path(uri: str | None) -> str | None:
    parsed = urlparse(str(uri or ""))
    if parsed.scheme != "file":
        return None
    return unquote(parsed.path)


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return f'"{value}"'


async def _resolve_raw_artifacts_from_db(
    source_file_version_ids: list[str],
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    schema: str,
) -> list[dict[str, Any]]:
    if not source_file_version_ids:
        return []
    try:
        import asyncpg
    except ImportError as exc:  # pragma: no cover - depends on runtime image.
        raise RuntimeError("asyncpg is required for --resolve-raw-artifacts-from-db") from exc

    table = f"{_quote_identifier(schema)}.ptg2_source_file_version"
    conn = await asyncpg.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password or None,
    )
    try:
        rows = await conn.fetch(
            f"""
            SELECT source_file_version_id, raw_storage_uri, raw_sha256, content_length
              FROM {table}
             WHERE source_file_version_id = ANY($1::text[])
             ORDER BY source_file_version_id
            """,
            source_file_version_ids,
        )
    finally:
        await conn.close()

    rows_by_id = {str(row["source_file_version_id"]): row for row in rows}
    resolutions: list[dict[str, Any]] = []
    for source_file_version_id in source_file_version_ids:
        row = rows_by_id.get(source_file_version_id)
        if row is None:
            resolutions.append(
                {
                    "source_file_version_id": source_file_version_id,
                    "status": "not_found",
                }
            )
            continue
        raw_storage_uri = str(row["raw_storage_uri"] or "")
        path = _file_uri_to_path(raw_storage_uri)
        status = "resolved"
        if path is None:
            status = "non_file_uri"
        elif not Path(path).exists():
            status = "missing_file"
        resolutions.append(
            {
                "source_file_version_id": source_file_version_id,
                "raw_storage_uri": raw_storage_uri,
                "raw_artifact_path": path,
                "raw_sha256": row["raw_sha256"],
                "content_length": row["content_length"],
                "status": status,
            }
        )
    return resolutions


def _raw_artifact_resolution_issues(
    source_file_version_ids: list[str],
    raw_artifact_resolution: list[dict[str, Any]],
    *,
    resolve_attempted: bool,
) -> list[dict[str, Any]]:
    if not source_file_version_ids:
        return [
            {
                "severity": "error",
                "message": (
                    "raw artifact resolution was required but no source_file_version_id values were available"
                ),
            }
        ]
    if not resolve_attempted:
        return [
            {
                "severity": "error",
                "message": (
                    "raw artifact resolution was required but --resolve-raw-artifacts-from-db was not used"
                ),
            }
        ]
    by_id = {
        str(resolution.get("source_file_version_id") or ""): resolution
        for resolution in raw_artifact_resolution
        if str(resolution.get("source_file_version_id") or "").strip()
    }
    issues: list[dict[str, Any]] = []
    for source_file_version_id in source_file_version_ids:
        resolution = by_id.get(source_file_version_id)
        if not resolution:
            issues.append(
                {
                    "severity": "error",
                    "source_file_version_id": source_file_version_id,
                    "message": (
                        "source_file_version_id was not returned by raw artifact resolution: "
                        f"{source_file_version_id}"
                    ),
                }
            )
            continue
        status = str(resolution.get("status") or "").strip()
        if status != "resolved":
            issues.append(
                {
                    "severity": "error",
                    "source_file_version_id": source_file_version_id,
                    "message": (
                        "source_file_version_id did not resolve to an existing local raw artifact: "
                        f"{source_file_version_id} ({status or 'unknown'})"
                    ),
                }
            )
    return issues


def main() -> int:
    args = parse_args()
    api_payload = _load_api_payload(args.api_payload, args.api_url, args.api_key, args.timeout)
    source_file_version_ids = _dedupe(
        [*source_file_version_ids_from_ptg_payload(api_payload), *args.source_file_version_id]
        if api_payload is not None
        else list(args.source_file_version_id)
    )
    raw_artifact_paths = list(args.raw_artifact)
    raw_artifact_resolution: list[dict[str, Any]] = []
    raw_artifact_source_file_version_ids_by_path: dict[str, list[str]] = {}
    if args.resolve_raw_artifacts_from_db:
        raw_artifact_resolution = asyncio.run(
            _resolve_raw_artifacts_from_db(
                source_file_version_ids,
                host=args.db_host,
                port=args.db_port,
                database=args.database,
                user=args.db_user,
                password=args.db_password,
                schema=args.db_schema,
            )
        )
        raw_artifact_paths.extend(
            str(resolution["raw_artifact_path"])
            for resolution in raw_artifact_resolution
            if resolution.get("status") == "resolved" and resolution.get("raw_artifact_path")
        )
        for resolution in raw_artifact_resolution:
            if resolution.get("status") != "resolved" or not resolution.get("raw_artifact_path"):
                continue
            path = str(resolution["raw_artifact_path"])
            source_file_version_id = str(resolution.get("source_file_version_id") or "").strip()
            if source_file_version_id:
                raw_artifact_source_file_version_ids_by_path.setdefault(path, []).append(source_file_version_id)
        raw_artifact_paths = _dedupe(raw_artifact_paths)
    report = build_ptg_address_assurance_report(
        api_payload=api_payload,
        raw_artifact_paths=raw_artifact_paths,
        raw_artifact_source_file_version_ids_by_path=raw_artifact_source_file_version_ids_by_path,
        max_samples=args.max_samples,
        require_displayed_address=not args.allow_missing_displayed_address,
        require_network_names=bool(args.require_network_names),
        require_source_file_version_id=bool(args.require_source_file_version_id),
        require_network_bound_address=bool(args.require_network_bound_address),
    )
    report["requested_source_file_version_ids"] = source_file_version_ids
    report["raw_artifact_resolution"] = raw_artifact_resolution
    if args.require_resolved_raw_artifacts:
        report["issues"].extend(
            _raw_artifact_resolution_issues(
                source_file_version_ids,
                raw_artifact_resolution,
                resolve_attempted=bool(args.resolve_raw_artifacts_from_db),
            )
        )
        report["ok"] = bool(
            report["ok"] and not any(issue.get("severity") == "error" for issue in report["issues"])
        )
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    if args.strict and not report["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
