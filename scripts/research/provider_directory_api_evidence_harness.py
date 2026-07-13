#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify current Provider Directory evidence through the client-facing api-layer contract."""
from __future__ import annotations
import argparse
import asyncio
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.research.provider_directory_endpoint_acquisition_harness import (
    DEFAULT_MANIFEST,
    load_manifest,
)

DEFAULT_API_BASE_URL_ENV = "PROVIDER_DIRECTORY_API_BASE_URL"
DEFAULT_API_BEARER_TOKEN_ENV = "PROVIDER_DIRECTORY_API_BEARER_TOKEN"
DEFAULT_API_KEY_ENV = "PROVIDER_DIRECTORY_API_KEY"
SOURCE_ID_RE = re.compile(r"^pdfhir_[0-9a-f]{24}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
REQUIRED_CLASSIFICATIONS = frozenset({"acquisition", "bulk_acquisition", "external"})
SENSITIVE_FIELD_PARTS = (
    "authorization",
    "token",
    "secret",
    "password",
    "api_key",
    "credential",
    "headers",
)


@dataclass(frozen=True)
class SourceSelection:
    entry_id: str
    source_id: str
    classification: str
    required: bool


@dataclass(frozen=True)
class OverlaySample:
    source_id: str
    npi: int
    phone: str | None


@dataclass(frozen=True)
class ApiConfig:
    """API settings whose credentials remain in memory only."""

    base_url: str | None
    bearer_token: str | None
    api_key: str | None
    api_key_header: str
    timeout_seconds: float
    data_only: bool = False

    @property
    def is_enabled(self) -> bool:
        """Return whether authenticated API calls are allowed."""
        return bool(
            self.base_url and (self.bearer_token or self.api_key) and not self.data_only
        )


@dataclass(frozen=True)
class HttpResult:
    status_code: int | None
    latency_ms: float
    payload: Mapping[str, Any] | None
    error: str | None = None


def _utc_now() -> str:
    return (
        dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _quote_identifier(value: str, *, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"invalid {label}")
    return f'"{value}"'


def _normalized_ids(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(value.strip() for value in values if value and value.strip())
    )


def resolve_source_selection(
    manifest: Mapping[str, Any],
    *,
    requested_entry_ids: Iterable[str] = (),
    requested_source_ids: Iterable[str] = (),
    max_sources: int = 100,
) -> list[SourceSelection]:
    """Resolve all selected maintained sources or reject a cap that would truncate them."""
    if max_sources < 1:
        raise ValueError("max_sources must be at least one")
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ValueError("manifest entries are unavailable")
    requested_entries = _normalized_ids(requested_entry_ids)
    requested_sources = _normalized_ids(requested_source_ids)
    entries_by_id = {
        str(entry.get("entry_id") or ""): entry
        for entry in entries
        if isinstance(entry, Mapping)
    }
    sources_by_id = {
        str(source_id): entry_id
        for entry_id, entry in entries_by_id.items()
        for source_id in entry.get("source_ids") or []
    }
    if set(requested_entries) - set(entries_by_id):
        raise ValueError("unknown manifest entry selector")
    if any(not SOURCE_ID_RE.fullmatch(source_id) for source_id in requested_sources):
        raise ValueError("unknown manifest source selector")
    if set(requested_sources) - set(sources_by_id):
        raise ValueError("unknown manifest source selector")
    selections: list[SourceSelection] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        entry_id = str(entry.get("entry_id") or "")
        classification = str(entry.get("classification") or "")
        for source_id_value in entry.get("source_ids") or []:
            source_id = str(source_id_value)
            if requested_entries or requested_sources:
                if (
                    entry_id not in requested_entries
                    and source_id not in requested_sources
                ):
                    continue
            selections.append(
                SourceSelection(
                    entry_id,
                    source_id,
                    classification,
                    classification in REQUIRED_CLASSIFICATIONS,
                )
            )
    if len(selections) > max_sources:
        raise ValueError("selected maintained sources exceed max_sources")
    return selections


def overlay_sample_sql(schema: str) -> str:
    """Return a deterministic, source-indexed, current dataset/overlay probe."""
    quoted_schema = _quote_identifier(schema, label="database schema")
    return f"""
        WITH requested_sources AS MATERIALIZED (
            SELECT DISTINCT source_id FROM unnest($1::varchar[]) AS requested(source_id)
        ), current_sources AS MATERIALIZED (
            SELECT requested.source_id, dataset.dataset_id, COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id
              FROM requested_sources AS requested JOIN {quoted_schema}.provider_directory_source AS source ON source.source_id = requested.source_id
              JOIN {quoted_schema}.provider_directory_endpoint_dataset AS dataset ON dataset.endpoint_id = source.endpoint_id
             WHERE dataset.is_current IS TRUE AND dataset.status = 'published'
               AND dataset.published_at IS NOT NULL AND dataset.superseded_at IS NULL
               AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) IS NOT NULL
        )
        SELECT current_source.source_id, sampled.npi, sampled.phone_number
          FROM current_sources AS current_source
          CROSS JOIN LATERAL (
              SELECT DISTINCT ON (overlay.npi) overlay.npi, overlay.phone_number
                FROM {quoted_schema}.provider_directory_address_overlay AS overlay
                JOIN {quoted_schema}.provider_directory_dataset_resource AS resource ON resource.dataset_id = current_source.dataset_id
                 AND resource.resource_type = overlay.resource_type AND resource.resource_id = overlay.resource_id
               WHERE overlay.source_id = current_source.source_id AND overlay.last_seen_run_id = current_source.run_id
                 AND overlay.npi IS NOT NULL
               ORDER BY overlay.npi,
                        (overlay.address_key IS NOT NULL) DESC,
                        (NULLIF(overlay.phone_number, '') IS NOT NULL) DESC,
                        overlay.source_record_id
               LIMIT $2
          ) AS sampled
         ORDER BY current_source.source_id, sampled.npi;
    """


async def fetch_overlay_samples(
    conn: Any,
    *,
    schema: str,
    selections: Iterable[SourceSelection],
    samples_per_source: int,
) -> dict[str, list[OverlaySample]]:
    """Fetch no more than five deterministic, de-duplicated samples per source."""
    if not 1 <= samples_per_source <= 5:
        raise ValueError("samples_per_source must be between one and five")
    source_ids = [selection.source_id for selection in selections]
    samples_by_source = {source_id: [] for source_id in source_ids}
    if not source_ids:
        return samples_by_source
    rows = await conn.fetch(overlay_sample_sql(schema), source_ids, samples_per_source)
    for row in rows:
        row_map = getattr(row, "_mapping", row)
        source_id = str(row_map.get("source_id") or "")
        npi_value = row_map.get("npi")
        if source_id not in samples_by_source or npi_value is None:
            continue
        samples_by_source[source_id].append(
            OverlaySample(
                source_id,
                int(npi_value),
                _normalized_phone(row_map.get("phone_number")),
            )
        )
    return samples_by_source


def _normalized_phone(value: Any) -> str | None:
    digits = "".join(character for character in str(value or "") if character.isdigit())
    return digits if len(digits) >= 7 else None


def _masked_phone(phone: str | None) -> str | None:
    return f"***-***-{phone[-4:]}" if phone else None


def _validated_api_base_url(base_url: str | None) -> str | None:
    candidate = str(base_url or "").strip().rstrip("/")
    if not candidate:
        return None
    parsed = urllib.parse.urlsplit(candidate)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("API base URL must be credential-free HTTP(S) URL")
    return candidate


class ProviderDirectoryApiClient:
    """Credential-safe api-layer JSON client with explicit request timeouts."""

    def __init__(
        self, config: ApiConfig, opener: Callable[..., Any] = urllib.request.urlopen
    ):
        self.config = config
        self.opener = opener
        self.base_url = _validated_api_base_url(config.base_url)
        if config.timeout_seconds <= 0:
            raise ValueError("API timeout must be greater than zero")
        if not re.fullmatch(r"[A-Za-z0-9-]+", config.api_key_header):
            raise ValueError("API key header is invalid")

    def get_json(self, path: str, params: Mapping[str, str]) -> HttpResult:
        """Request one api-layer object without retaining response text on errors."""
        if not self.config.is_enabled or not self.base_url:
            raise RuntimeError("API client is disabled")
        request = urllib.request.Request(
            f"{self.base_url}/{self._api_path(path)}?{urllib.parse.urlencode(params)}",
            headers=self._headers(),
            method="GET",
        )
        started = time.monotonic()
        try:
            with self.opener(request, timeout=self.config.timeout_seconds) as response:
                status_code = int(
                    getattr(response, "status", None) or response.getcode()
                )
                decoded = json.loads(response.read(2 * 1024 * 1024).decode("utf-8"))
            payload = decoded if isinstance(decoded, Mapping) else None
            error = None if payload is not None else "non_object_json"
            return HttpResult(status_code, _elapsed_ms(started), payload, error)
        except urllib.error.HTTPError as exc:
            return HttpResult(int(exc.code), _elapsed_ms(started), None, "http_error")
        except urllib.error.URLError:
            return HttpResult(None, _elapsed_ms(started), None, "network_error")
        except TimeoutError:
            return HttpResult(None, _elapsed_ms(started), None, "timeout")
        except (UnicodeDecodeError, json.JSONDecodeError):
            return HttpResult(None, _elapsed_ms(started), None, "invalid_json")
        except OSError:
            return HttpResult(None, _elapsed_ms(started), None, "network_error")

    def _api_path(self, path: str) -> str:
        return path if self.base_url.endswith("/api/v1") else f"api/v1/{path}"

    def _headers(self) -> dict[str, str]:
        header_map = {
            "Accept": "application/json",
            "User-Agent": "healthporta-provider-directory-evidence/1.0",
        }
        if self.config.bearer_token:
            header_map["Authorization"] = f"Bearer {self.config.bearer_token}"
        if self.config.api_key:
            header_map[self.config.api_key_header] = self.config.api_key
        return header_map


def _elapsed_ms(started: float) -> float:
    return round((time.monotonic() - started) * 1000.0, 2)


def _envelope_rows(payload: Mapping[str, Any] | None, field: str) -> list[Any]:
    data_map = payload.get("data") if isinstance(payload, Mapping) else None
    if field == "address_list":
        data_map = data_map.get("npi") if isinstance(data_map, Mapping) else None
    rows = data_map.get(field) if isinstance(data_map, Mapping) else None
    return rows if isinstance(rows, list) else []


def has_row_source_provenance(row: Any, source_id: str) -> bool:
    """Return whether one api-layer row exposes the requested FHIR source."""
    if not isinstance(row, Mapping):
        return False
    summaries = row.get("provider_directory_sources")
    if not isinstance(summaries, list):
        return False
    for summary in summaries:
        if not isinstance(summary, Mapping):
            continue
        if summary.get("source") != "provider_directory_fhir":
            continue
        if summary.get("catalog_aliases_verified") is not False:
            continue
        source_ids = summary.get("source_ids")
        if isinstance(source_ids, list) and source_id in {
            str(value) for value in source_ids
        }:
            return True
        aliases = summary.get("catalog_aliases")
        if isinstance(aliases, list) and any(
            isinstance(alias, Mapping)
            and str(alias.get("source_id") or "") == source_id
            for alias in aliases
        ):
            return True
    return False


def _has_detail_source(payload: Mapping[str, Any] | None, source_id: str) -> bool:
    return any(
        has_row_source_provenance(row, source_id)
        for row in _envelope_rows(payload, "address_list")
    )


def _has_phone_candidate_source(
    payload: Mapping[str, Any] | None, source_id: str, npi: int
) -> bool:
    return any(
        isinstance(row, Mapping)
        and int(row.get("npi") or 0) == npi
        and has_row_source_provenance(row, source_id)
        for row in _envelope_rows(payload, "candidates")
    )


def _http_summary(http_result: HttpResult) -> dict[str, Any]:
    summary_map: dict[str, Any] = {
        "status_code": http_result.status_code,
        "latency_ms": http_result.latency_ms,
    }
    if http_result.error:
        summary_map["error"] = http_result.error
    return summary_map


def _within_latency_slo(http_result: HttpResult, latency_slo_ms: float) -> bool:
    """Return whether a request meets the enabled client-facing latency SLO."""
    return latency_slo_ms == 0 or http_result.latency_ms <= latency_slo_ms


def evaluate_source(
    selection: SourceSelection,
    samples: list[OverlaySample],
    api_client: ProviderDirectoryApiClient | None,
    *,
    candidate_limit: int,
    api_latency_slo_ms: float,
    api_skip_reason: str | None,
) -> dict[str, Any]:
    """Verify bounded api-layer detail and phone evidence for one manifest source."""
    source_result_map: dict[str, Any] = {
        "entry_id": selection.entry_id,
        "source_id": selection.source_id,
        "classification": selection.classification,
        "required": selection.required,
        "samples": [
            {"npi": sample.npi, "phone": _masked_phone(sample.phone)}
            for sample in samples
        ],
    }
    if not samples:
        source_result_map["status"] = "fail" if selection.required else "skip"
        source_result_map["reason"] = (
            "required_current_overlay_dataset_evidence_not_found"
            if selection.required
            else "probe_only_current_overlay_dataset_evidence_not_found"
        )
        return source_result_map
    if api_client is None:
        source_result_map["status"] = "skip"
        source_result_map["reason"] = api_skip_reason or "api_credentials_unavailable"
        return source_result_map
    checks: list[dict[str, Any]] = []
    for sample in samples:
        detail = api_client.get_json(
            f"providers/{sample.npi}",
            {"include_sources": "true", "include_evidence": "true"},
        )
        check_map: dict[str, Any] = {"npi": sample.npi, "detail": _http_summary(detail)}
        check_map["detail_source_present"] = (
            detail.status_code == 200
            and _has_detail_source(detail.payload, selection.source_id)
        )
        check_map["detail_within_latency_slo"] = _within_latency_slo(
            detail, api_latency_slo_ms
        )
        if sample.phone:
            phone_result = api_client.get_json(
                "providers/match-candidates",
                {
                    "phone": sample.phone,
                    "limit": str(candidate_limit),
                    "include_sources": "true",
                    "include_evidence": "true",
                },
            )
            check_map["phone_match_candidates"] = _http_summary(phone_result)
            check_map["phone_source_present"] = (
                phone_result.status_code == 200
                and _has_phone_candidate_source(
                    phone_result.payload, selection.source_id, sample.npi
                )
            )
            check_map["phone_within_latency_slo"] = _within_latency_slo(
                phone_result, api_latency_slo_ms
            )
        checks.append(check_map)
    source_result_map["checks"] = checks
    source_result_map["status"] = (
        "pass"
        if all(
            check["detail_source_present"]
            and check["detail_within_latency_slo"]
            and check.get("phone_source_present", True)
            and check.get("phone_within_latency_slo", True)
            for check in checks
        )
        else "fail"
    )
    return source_result_map


def redact_sensitive(value: Any) -> Any:
    """Strip sensitive keyed values before report serialization."""
    if isinstance(value, Mapping):
        return {
            str(key): redact_sensitive(nested)
            for key, nested in value.items()
            if not any(
                part in str(key).lower().replace("-", "_")
                for part in SENSITIVE_FIELD_PARTS
            )
        }
    if isinstance(value, list):
        return [redact_sensitive(item) for item in value]
    return value


async def build_report(
    config: Any,
    conn: Any,
    api_config: ApiConfig,
    api_client: ProviderDirectoryApiClient | None = None,
) -> dict[str, Any]:
    """Build a credential-safe report, failing required sources without current evidence."""
    if not 1 <= config.candidate_limit <= 20:
        raise ValueError("candidate_limit must be between one and twenty")
    if config.api_latency_slo_ms < 0:
        raise ValueError("api_latency_slo_ms must be zero or greater")
    manifest_map = load_manifest(config.manifest_path)
    selections = resolve_source_selection(
        manifest_map,
        requested_entry_ids=config.entry_ids,
        requested_source_ids=config.source_ids,
        max_sources=config.max_sources,
    )
    if not selections:
        raise ValueError("no maintained sources selected")
    try:
        samples_by_source = await fetch_overlay_samples(
            conn,
            schema=config.schema,
            selections=selections,
            samples_per_source=config.samples_per_source,
        )
        db_error = None
    except Exception as exc:
        samples_by_source = {selection.source_id: [] for selection in selections}
        db_error = type(exc).__name__
    api_skip_reason = None
    active_client = api_client if api_config.is_enabled else None
    if db_error:
        api_skip_reason = "database_probe_failed"
    elif active_client is None and api_config.is_enabled:
        active_client = ProviderDirectoryApiClient(api_config)
    elif not api_config.is_enabled:
        api_skip_reason = (
            "data_only_mode" if api_config.data_only else "api_credentials_unavailable"
        )
    source_results = [
        (
            _database_failure_result(selection)
            if db_error
            else evaluate_source(
                selection,
                samples_by_source.get(selection.source_id, []),
                active_client,
                candidate_limit=config.candidate_limit,
                api_latency_slo_ms=config.api_latency_slo_ms,
                api_skip_reason=api_skip_reason,
            )
        )
        for selection in selections
    ]
    required_failures = sum(
        source_result_map["status"] == "fail" and source_result_map["required"]
        for source_result_map in source_results
    )
    report_map = {
        "schema_version": 1,
        "generated_at": _utc_now(),
        "mode": "api" if active_client is not None and not db_error else "data_only",
        "selection": {
            "entry_ids": list(config.entry_ids),
            "source_ids": list(config.source_ids),
            "selected_source_count": len(selections),
            "max_sources": config.max_sources,
            "samples_per_source": config.samples_per_source,
            "phone_candidate_limit": config.candidate_limit,
            "api_latency_slo_ms": config.api_latency_slo_ms,
        },
        "summary": {
            "pass": sum(item["status"] == "pass" for item in source_results),
            "fail": sum(item["status"] == "fail" for item in source_results),
            "skip": sum(item["status"] == "skip" for item in source_results),
            "required_sources_failed": required_failures,
        },
        "sources": source_results,
    }
    return redact_sensitive(report_map)


def _database_failure_result(selection: SourceSelection) -> dict[str, Any]:
    status = "fail" if selection.required else "skip"
    return {
        "entry_id": selection.entry_id,
        "source_id": selection.source_id,
        "classification": selection.classification,
        "required": selection.required,
        "status": status,
        "reason": "database_probe_failed",
        "samples": [],
    }


async def _connect(args: argparse.Namespace) -> Any:
    try:
        import asyncpg
    except ImportError as exc:  # pragma: no cover - runtime dependency.
        raise RuntimeError("asyncpg is required for database probes") from exc
    return await asyncpg.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password or None,
        database=args.db_database,
        timeout=args.db_timeout_seconds,
        server_settings={
            "statement_timeout": str(int(args.statement_timeout_seconds * 1000))
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse explicit database/API settings without printing their secret values."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--entry-id",
        action="append",
        default=[],
        help="Exact manifest entry_id; repeatable.",
    )
    parser.add_argument(
        "--source-id",
        action="append",
        default=[],
        help="Exact manifest source_id; repeatable.",
    )
    parser.add_argument("--max-sources", type=int, default=100)
    parser.add_argument("--samples-per-source", type=int, default=1)
    parser.add_argument("--phone-candidate-limit", type=int, default=5)
    parser.add_argument(
        "--db-host", default=os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1"
    )
    parser.add_argument(
        "--db-port", type=int, default=int(os.getenv("HLTHPRT_DB_PORT") or "5432")
    )
    parser.add_argument(
        "--db-database", default=os.getenv("HLTHPRT_DB_DATABASE") or "healthporta"
    )
    parser.add_argument("--db-user", default=os.getenv("HLTHPRT_DB_USER") or "postgres")
    parser.add_argument("--db-password", default=os.getenv("HLTHPRT_DB_PASSWORD") or "")
    parser.add_argument("--db-schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--db-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--statement-timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--api-base-url", default=os.getenv(DEFAULT_API_BASE_URL_ENV) or ""
    )
    parser.add_argument(
        "--api-bearer-token", default=os.getenv(DEFAULT_API_BEARER_TOKEN_ENV) or ""
    )
    parser.add_argument("--api-key", default=os.getenv(DEFAULT_API_KEY_ENV) or "")
    parser.add_argument("--api-key-header", default="X-API-Key")
    parser.add_argument("--api-timeout-seconds", type=float, default=15.0)
    parser.add_argument(
        "--api-latency-slo-ms",
        type=float,
        default=40.0,
        help="Maximum API latency per check in milliseconds; zero disables the SLO.",
    )
    parser.add_argument(
        "--data-only",
        action="store_true",
        help="Inspect current DB evidence without API calls.",
    )
    return parser.parse_args(argv)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Connect, probe current evidence, and close the database session."""
    if args.db_timeout_seconds <= 0 or args.statement_timeout_seconds <= 0:
        raise ValueError("database timeouts must be greater than zero")
    api_config = ApiConfig(
        args.api_base_url,
        args.api_bearer_token or None,
        args.api_key or None,
        args.api_key_header,
        args.api_timeout_seconds,
        args.data_only,
    )
    args.entry_ids = _normalized_ids(args.entry_id)
    args.source_ids = _normalized_ids(args.source_id)
    args.schema = args.db_schema
    args.candidate_limit = args.phone_candidate_limit
    conn = await _connect(args)
    try:
        return await build_report(args, conn, api_config)
    finally:
        await conn.close()


def main(argv: list[str] | None = None) -> int:
    """Print the safe JSON report and return nonzero for required source failures."""
    try:
        report = asyncio.run(run(parse_args(argv)))
        print(json.dumps(redact_sensitive(report), indent=2, sort_keys=True))
        return 1 if report["summary"]["required_sources_failed"] else 0
    except Exception as exc:  # Never serialize command arguments or exception text.
        print(
            json.dumps({"status": "error", "error": type(exc).__name__}, sort_keys=True)
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
