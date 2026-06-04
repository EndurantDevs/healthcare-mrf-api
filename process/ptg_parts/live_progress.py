# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG compatibility facade for generic live import progress."""

from __future__ import annotations

from typing import Any

from process.live_progress import (
    IMPORT_LIVE_PROGRESS_STALE_SECONDS,
    IMPORT_LIVE_PROGRESS_TTL_SECONDS,
    current_live_progress_context,
    estimate_payload_from_live,
    live_progress_key,
    progress_payload_from_live,
    read_live_progress,
    reset_live_progress_context,
    set_live_progress_context,
)
from process.live_progress import write_live_progress as _write_live_progress

PTG_LIVE_PROGRESS_TTL_SECONDS = IMPORT_LIVE_PROGRESS_TTL_SECONDS
PTG_LIVE_PROGRESS_STALE_SECONDS = IMPORT_LIVE_PROGRESS_STALE_SECONDS


def write_live_progress(**payload: Any) -> None:
    payload.setdefault("importer", "ptg")
    payload.setdefault("source", "ptg-live-progress")
    _write_live_progress(**payload)
