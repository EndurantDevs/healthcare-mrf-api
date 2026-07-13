"""Credential-safe client checks for Provider Directory API evidence."""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Mapping


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
    """One exact manifest source and its API-readiness requirement."""

    entry_id: str
    source_id: str
    classification: str
    required: bool


@dataclass(frozen=True)
class OverlaySample:
    """One bounded current overlay sample used for API verification."""

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
    """A bounded HTTP outcome with no response text or credentials."""

    status_code: int | None
    latency_ms: float
    payload: Mapping[str, Any] | None
    error: str | None = None


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
            str(source_value) for source_value in source_ids
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


def is_within_latency_slo(http_result: HttpResult, latency_slo_ms: float) -> bool:
    """Return whether a request meets the enabled client-facing latency SLO."""
    return latency_slo_ms == 0 or http_result.latency_ms <= latency_slo_ms


def _masked_phone(phone: str | None) -> str | None:
    return f"***-***-{phone[-4:]}" if phone else None


def _evaluate_sample(
    sample: OverlaySample,
    source_id: str,
    api_client: ProviderDirectoryApiClient,
    candidate_limit: int,
    api_latency_slo_ms: float,
) -> dict[str, Any]:
    detail_result = api_client.get_json(
        f"providers/{sample.npi}",
        {"include_sources": "true", "include_evidence": "true"},
    )
    source_check_map: dict[str, Any] = {
        "npi": sample.npi,
        "detail": _http_summary(detail_result),
        "detail_source_present": detail_result.status_code == 200
        and _has_detail_source(detail_result.payload, source_id),
        "detail_within_latency_slo": is_within_latency_slo(
            detail_result, api_latency_slo_ms
        ),
    }
    if not sample.phone:
        return source_check_map
    phone_result = api_client.get_json(
        "providers/match-candidates",
        {
            "phone": sample.phone,
            "limit": str(candidate_limit),
            "include_sources": "true",
            "include_evidence": "true",
        },
    )
    source_check_map.update(
        {
            "phone_match_candidates": _http_summary(phone_result),
            "phone_source_present": phone_result.status_code == 200
            and _has_phone_candidate_source(
                phone_result.payload, source_id, sample.npi
            ),
            "phone_within_latency_slo": is_within_latency_slo(
                phone_result, api_latency_slo_ms
            ),
        }
    )
    return source_check_map


def _is_source_check_passing(source_check: Mapping[str, Any]) -> bool:
    return bool(
        source_check["detail_source_present"]
        and source_check["detail_within_latency_slo"]
        and source_check.get("phone_source_present", True)
        and source_check.get("phone_within_latency_slo", True)
    )


def _base_source_result(
    selection: SourceSelection, samples: list[OverlaySample]
) -> dict[str, Any]:
    return {
        "entry_id": selection.entry_id,
        "source_id": selection.source_id,
        "classification": selection.classification,
        "required": selection.required,
        "samples": [
            {"npi": sample.npi, "phone": _masked_phone(sample.phone)}
            for sample in samples
        ],
    }


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
    source_result = _base_source_result(selection, samples)
    if not samples:
        source_result["status"] = "fail" if selection.required else "skip"
        source_result["reason"] = (
            "required_current_overlay_dataset_evidence_not_found"
            if selection.required
            else "probe_only_current_overlay_dataset_evidence_not_found"
        )
        return source_result
    if api_client is None:
        source_result["status"] = "skip"
        source_result["reason"] = api_skip_reason or "api_credentials_unavailable"
        return source_result
    source_checks = [
        _evaluate_sample(
            sample,
            selection.source_id,
            api_client,
            candidate_limit,
            api_latency_slo_ms,
        )
        for sample in samples
    ]
    source_result["checks"] = source_checks
    source_result["status"] = (
        "pass" if all(map(_is_source_check_passing, source_checks)) else "fail"
    )
    return source_result


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
        return [redact_sensitive(nested) for nested in value]
    return value
