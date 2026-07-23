"""Bounded read-only HTTP and local evidence-file helpers."""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import httpx

from scripts.ptg_v4_dev_canary_support import (
    CanaryConfigurationError,
    parse_graph_metrics,
)
from scripts.ptg_v4_dev_canary_metrics import parse_runtime_identity_metric
from api.runtime_identity import (
    IMAGE_IDENTITY_HEADER,
    PROCESS_IDENTITY_HEADER,
    PROCESS_STARTED_AT_HEADER,
)


class CanaryInfrastructureError(RuntimeError):
    """Stable error whose message cannot reveal transport credentials."""


async def get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Mapping[str, str],
    parameters: Mapping[str, str] | None,
    maximum_bytes: int,
) -> tuple[dict[str, Any], float]:
    """GET one bounded JSON object and return it with client-observed latency."""

    started_at = time.perf_counter()
    try:
        async with client.stream(
            "GET",
            url,
            headers=dict(headers),
            params=dict(parameters or {}),
        ) as response:
            if response.status_code != 200:
                raise CanaryInfrastructureError("read-only HTTP probe returned non-200")
            body = await _bounded_body(response, maximum_bytes)
    except httpx.HTTPError as exc:
        raise CanaryInfrastructureError("read-only HTTP probe failed") from exc
    elapsed_ms = (time.perf_counter() - started_at) * 1_000
    try:
        document_by_field = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanaryInfrastructureError("HTTP probe returned invalid JSON") from exc
    if not isinstance(document_by_field, dict):
        raise CanaryInfrastructureError("HTTP probe returned a non-object document")
    return document_by_field, elapsed_ms


async def get_identity_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Mapping[str, str],
    parameters: Mapping[str, str] | None,
    maximum_bytes: int,
) -> tuple[dict[str, Any], float, dict[str, str]]:
    """GET JSON and bind it to the exact API process/image response headers."""

    started_at = time.perf_counter()
    try:
        async with client.stream(
            "GET",
            url,
            headers=dict(headers),
            params=dict(parameters or {}),
        ) as response:
            if response.status_code != 200:
                raise CanaryInfrastructureError("read-only HTTP probe returned non-200")
            body = await _bounded_body(response, maximum_bytes)
            runtime_identity_by_field = {
                "process_identity": str(
                    response.headers.get(PROCESS_IDENTITY_HEADER) or ""
                ).strip(),
                "process_started_at": str(
                    response.headers.get(PROCESS_STARTED_AT_HEADER) or ""
                ).strip(),
                "image_identity": str(
                    response.headers.get(IMAGE_IDENTITY_HEADER) or ""
                ).strip(),
            }
    except httpx.HTTPError as exc:
        raise CanaryInfrastructureError("read-only HTTP probe failed") from exc
    elapsed_ms = (time.perf_counter() - started_at) * 1_000
    try:
        document_by_field = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanaryInfrastructureError("HTTP probe returned invalid JSON") from exc
    if not isinstance(document_by_field, dict) or not all(
        runtime_identity_by_field.values()
    ):
        raise CanaryInfrastructureError(
            "HTTP probe lacks JSON or exact runtime identity"
        )
    return document_by_field, elapsed_ms, runtime_identity_by_field


async def get_metrics(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: Mapping[str, str],
    maximum_bytes: int,
) -> dict[str, Any]:
    """GET and parse the bounded authenticated Prometheus evidence surface."""

    try:
        async with client.stream("GET", url, headers=dict(headers)) as response:
            if response.status_code != 200:
                raise CanaryInfrastructureError("metrics probe returned non-200")
            body = await _bounded_body(response, maximum_bytes)
        metric_text = body.decode("ascii")
        return {
            **parse_graph_metrics(metric_text),
            "runtime_identity": parse_runtime_identity_metric(metric_text),
        }
    except (httpx.HTTPError, UnicodeDecodeError, ValueError) as exc:
        raise CanaryInfrastructureError("authenticated metrics probe failed") from exc


async def _bounded_body(response: httpx.Response, maximum_bytes: int) -> bytes:
    body = bytearray()
    async for chunk in response.aiter_bytes():
        body.extend(chunk)
        if len(body) > maximum_bytes:
            raise CanaryInfrastructureError("HTTP response exceeded byte limit")
    return bytes(body)


def required_environment(name: str) -> str:
    """Return a required environment value without exposing it in errors."""

    if not name or not os.environ.get(name):
        raise CanaryConfigurationError(
            f"required environment variable is unset: {name or 'missing'}"
        )
    return os.environ[name]


def load_json(path: Path) -> dict[str, Any]:
    """Load a required local evidence object with a filename-only error."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanaryConfigurationError(f"evidence file is unreadable: {path.name}") from exc
    if not isinstance(payload, dict):
        raise CanaryConfigurationError(f"evidence file is not an object: {path.name}")
    return payload


def load_optional_json(path: Path) -> dict[str, Any] | None:
    """Load an evidence object when present, otherwise return no evidence."""

    return load_json(path) if path.exists() else None


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically persist a local canary evidence object."""

    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as temporary:
        temporary.write(serialized)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def parse_datetime(value: Any) -> datetime:
    """Parse one required progress timestamp as an aware UTC datetime."""

    if not isinstance(value, str) or not value.strip():
        raise CanaryConfigurationError("progress evidence lacks import timestamps")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


def utc_now_text() -> str:
    """Return the current UTC time in the evidence contract format."""

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
