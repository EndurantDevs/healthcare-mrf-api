# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 clock and progress-reporting helpers."""

from __future__ import annotations

import datetime
import logging
import time
from pathlib import Path
from typing import Any

from process.ptg_parts.config import (
    PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV,
    PTG2_PROGRESS_INTERVAL_SECONDS_ENV,
    _env_int,
)
from process.ptg_parts.live_progress import write_live_progress
from process.ptg_parts.screen import _emit_screen_line

logger = logging.getLogger(__name__)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def _artifact_progress_position(stream: Any) -> int | None:
    raw_stream = getattr(getattr(stream, "raw", None), "fileobj", None)
    if raw_stream is None:
        raw_stream = getattr(stream, "fileobj", None)
    if raw_stream is not None and hasattr(raw_stream, "tell"):
        try:
            return int(raw_stream.tell())
        except (OSError, TypeError, ValueError):
            pass
    if hasattr(stream, "tell"):
        try:
            return int(stream.tell())
        except (OSError, TypeError, ValueError):
            return None
    return None


def _format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "unknown"
    seconds_int = int(seconds)
    hours, remainder = divmod(seconds_int, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _scale_stage_progress_pct(
    phase_pct: Any,
    start_pct: Any,
    end_pct: Any,
) -> float | None:
    try:
        phase = float(phase_pct)
        start = float(start_pct)
        end = float(end_pct)
    except (TypeError, ValueError):
        return None
    if end <= start:
        return None
    phase = max(0.0, min(100.0, phase))
    return max(0.0, min(100.0, start + ((end - start) * (phase / 100.0))))


def _maybe_log_artifact_progress(
    path: str | Path,
    stream: Any,
    state: dict[str, Any],
    label: str,
    *,
    ref_count: int = 0,
    item_count: int = 0,
) -> None:
    interval = max(_env_int(PTG2_PROGRESS_INTERVAL_SECONDS_ENV, 30), 1)
    now = time.monotonic()
    if state.get("last_log") and now - state["last_log"] < interval:
        return
    position = _artifact_progress_position(stream)
    if position is None:
        return
    try:
        total = Path(path).stat().st_size
    except OSError:
        total = 0
    if total <= 0:
        return
    position = min(position, total)
    elapsed = now - state.setdefault("started_at", now)
    rate = position / elapsed if elapsed > 0 else None
    compressed_eta = (total - position) / rate if rate and rate > 0 else None
    compressed_pct = (position / total) * 100
    expected_items = max(_env_int(PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV, 0), 0)
    item_parts = ""
    if item_count > 0:
        item_started_at = state.setdefault("first_item_at", state.get("started_at", now))
        item_elapsed = max(now - item_started_at, 1e-6)
        item_rate = item_count / item_elapsed
        item_parts = f", item_rate={item_rate:.2f}/s"
        if expected_items > 0:
            remaining_items = max(expected_items - item_count, 0)
            item_eta = remaining_items / item_rate if item_rate > 0 else None
            item_pct = min((item_count / expected_items) * 100, 100)
            item_parts += (
                f", item_progress={item_pct:.2f}% "
                f"({item_count}/{expected_items}), item_eta={_format_duration(item_eta)}"
            )
    message = (
        f"PTG2 progress {label}: compressed_read={compressed_pct:.2f}% "
        f"({position / (1024 ** 3):.2f}/{total / (1024 ** 3):.2f} GiB), "
        f"provider_refs={ref_count}, in_network_items={item_count}, "
        f"elapsed={_format_duration(elapsed)}, compressed_eta={_format_duration(compressed_eta)}"
        f"{item_parts}"
    )
    _emit_screen_line(message)
    logger.info(message)
    live_pct = item_pct if item_count > 0 and expected_items > 0 else compressed_pct
    live_eta = item_eta if item_count > 0 and expected_items > 0 else compressed_eta
    write_live_progress(
        phase=label,
        unit="items" if item_count > 0 else "compressed_bytes",
        done=item_count if item_count > 0 else position,
        total=expected_items if item_count > 0 and expected_items > 0 else total,
        pct=live_pct,
        eta_seconds=live_eta,
        message=message[:512],
        ref_count=ref_count,
        item_count=item_count,
        compressed_read_bytes=position,
        compressed_total_bytes=total,
    )
    state["last_log"] = now
