# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validation and deterministic result helpers for numeric-grid alias jobs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STATE_RE = re.compile(r"^[A-Z]{2}$")
_ZIP_PREFIX_RE = re.compile(r"^[0-9]{1,5}$")
_TIMEOUT_RE = re.compile(r"^[1-9][0-9]*(?:ms|s|min|h)$")


@dataclass(frozen=True)
class NumericGridAliasResult:
    run_id: str | None
    mode: str
    status: str
    candidate_digest: str | None
    source_count: int
    candidate_sources: int
    candidate_rows: int
    no_candidate: int
    active_skipped: int
    eligible: int
    ambiguous: int
    insufficient_provenance: int
    promoted: int
    generation: int
    sample_rows: list[dict[str, Any]]
    alias_kind: str = "numeric_grid_direction_v1"


@dataclass(frozen=True)
class NumericGridAliasRequest:
    mode: str = "off"
    schema: str | None = None
    state_code: str | None = None
    zip_prefix: str | None = None
    alias_run_id: str | None = None
    expected_candidate_sha256: str | None = None
    reviewed_by: str | None = None
    sample_limit: int = 20
    timeout: str = "10min"
    cancel_check: Callable[[], Awaitable[None]] | None = None
    alias_kind: str = "numeric_grid_direction_v1"


def _quote_ident(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return f'"{value}"'


def _relation(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _normalize_scope(
    state_code: str | None,
    zip_prefix: str | None,
) -> tuple[str | None, str | None]:
    normalized_state = str(state_code or "").strip().upper() or None
    normalized_zip = str(zip_prefix or "").strip() or None
    if normalized_state and not _STATE_RE.fullmatch(normalized_state):
        raise ValueError("state_code must be a two-letter uppercase state code")
    if normalized_zip and not _ZIP_PREFIX_RE.fullmatch(normalized_zip):
        raise ValueError("zip_prefix must contain one to five digits")
    return normalized_state, normalized_zip


def _reviewed_digest(value: str | None) -> str:
    digest = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(digest):
        raise ValueError("apply requires the exact 64-character candidate SHA-256")
    return digest


def _reviewer(value: str | None) -> str:
    reviewer = str(value or "").strip()
    if not reviewer:
        raise ValueError("apply requires reviewed_by")
    return reviewer[:256]


def _statement_timeout(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _TIMEOUT_RE.fullmatch(normalized):
        raise ValueError("timeout must be a positive PostgreSQL duration")
    return normalized


def _statement_timeout_seconds(value: str) -> float:
    """Convert one already-supported PostgreSQL duration to wall seconds."""
    normalized = _statement_timeout(value)
    for suffix, multiplier in (("min", 60.0), ("ms", 0.001), ("h", 3600.0), ("s", 1.0)):
        if normalized.endswith(suffix):
            return int(normalized[: -len(suffix)]) * multiplier
    raise AssertionError("validated timeout has no supported suffix")


def _candidate_digest(rows: list[Any]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        payload = dict(row._mapping)
        digest.update(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _candidate_sample(rows: list[Any], sample_limit: int) -> list[dict[str, Any]]:
    bounded_limit = max(0, min(int(sample_limit), 100))
    return [dict(row._mapping) for row in rows[:bounded_limit]]
