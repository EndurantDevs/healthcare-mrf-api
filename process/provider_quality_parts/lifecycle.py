# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run identity and manifest helpers for provider-quality imports."""

from __future__ import annotations

import datetime
import hashlib
import json
import secrets
from pathlib import Path
from typing import Any

from process.provider_quality_parts.config import POSTGRES_IDENTIFIER_MAX_LENGTH, PROVIDER_QUALITY_WORKDIR


def _normalize_run_id(run_id: str | None) -> str:
    if run_id:
        normalized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(run_id))
        normalized = normalized.strip("_")
        if normalized:
            return normalized
    token = secrets.token_hex(4)
    return f"{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{token}"


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    return normalized or datetime.date.today().strftime("%Y%m%d")


def _format_duration_compact(total_seconds: float) -> str:
    seconds = max(int(total_seconds), 0)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes > 0:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _materialize_shard_job_id(run_id: str, phase: str, year: int, shard_id: int) -> str:
    return f"provider_quality_materialize_{phase}_{run_id}_{year}_{shard_id}"


def _npi_shard_predicate(expr: str) -> str:
    return f"MOD(({expr})::bigint, :shard_count) = :shard_id"


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _run_dir(import_id: str, run_id: str) -> Path:
    return Path(PROVIDER_QUALITY_WORKDIR) / import_id / run_id


def _manifest_path(work_dir: Path) -> Path:
    return work_dir / "manifest.json"


def _read_manifest(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
