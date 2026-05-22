# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 serving index artifact and in-memory cache helpers."""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from api.ptg2_types import PTG2ServingIndex

PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX = "snapshot_index"
PTG2_INDEX_CACHE_TTL_SECONDS = max(float(os.getenv("HLTHPRT_PTG2_INDEX_CACHE_TTL_SECONDS", "300")), 0.0)
PTG2_RESPONSE_CACHE_TTL_SECONDS = max(float(os.getenv("HLTHPRT_PTG2_RESPONSE_CACHE_TTL_SECONDS", "300")), 0.0)
PTG2_RESPONSE_CACHE_MAX_KEYS = max(int(os.getenv("HLTHPRT_PTG2_RESPONSE_CACHE_MAX_KEYS", "512")), 0)

_PTG2_INDEX_CACHE: dict[str, tuple[float, PTG2ServingIndex]] = {}
_PTG2_RESPONSE_CACHE: OrderedDict[str, tuple[float, dict[str, Any] | None]] = OrderedDict()
_CACHE_MISS = object()


def clear_ptg2_index_cache() -> None:
    _PTG2_INDEX_CACHE.clear()
    _PTG2_RESPONSE_CACHE.clear()


def _ptg2_response_cache_key(snapshot_id: str, args: dict[str, Any], pagination: Any) -> str:
    normalized_args = {
        str(key): value
        for key, value in args.items()
        if value not in (None, "", [], {})
    }
    payload = {
        "snapshot_id": snapshot_id,
        "limit": int(getattr(pagination, "limit", 25) or 25),
        "offset": int(getattr(pagination, "offset", 0) or 0),
        "args": normalized_args,
    }
    return json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))


def _ptg2_response_cache_get(cache_key: str) -> dict[str, Any] | None | object:
    if PTG2_RESPONSE_CACHE_TTL_SECONDS <= 0 or PTG2_RESPONSE_CACHE_MAX_KEYS <= 0:
        return _CACHE_MISS
    entry = _PTG2_RESPONSE_CACHE.get(cache_key)
    if entry is None:
        return _CACHE_MISS
    cached_at, payload = entry
    if (time.monotonic() - cached_at) > PTG2_RESPONSE_CACHE_TTL_SECONDS:
        _PTG2_RESPONSE_CACHE.pop(cache_key, None)
        return _CACHE_MISS
    _PTG2_RESPONSE_CACHE.move_to_end(cache_key)
    return deepcopy(payload)


def _ptg2_response_cache_set(cache_key: str, payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if PTG2_RESPONSE_CACHE_TTL_SECONDS <= 0 or PTG2_RESPONSE_CACHE_MAX_KEYS <= 0:
        return payload
    _PTG2_RESPONSE_CACHE[cache_key] = (time.monotonic(), deepcopy(payload))
    _PTG2_RESPONSE_CACHE.move_to_end(cache_key)
    while len(_PTG2_RESPONSE_CACHE) > PTG2_RESPONSE_CACHE_MAX_KEYS:
        _PTG2_RESPONSE_CACHE.popitem(last=False)
    return payload


def _artifact_root() -> Path:
    configured = os.getenv("HLTHPRT_PTG2_ARTIFACT_DIR")
    return Path(configured) if configured else Path(tempfile.gettempdir()) / "healthporta-ptg2-artifacts"


def _path_from_uri(uri: str) -> Path:
    if uri.startswith("file://"):
        return Path(unquote(urlsplit(uri).path))
    return Path(uri)


def load_ptg2_index_from_path(path: str | Path) -> PTG2ServingIndex:
    artifact_path = Path(path)
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    return PTG2ServingIndex.from_payload(payload, source_uri=artifact_path.resolve().as_uri())
