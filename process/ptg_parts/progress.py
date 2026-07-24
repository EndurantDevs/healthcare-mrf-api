# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 clock and progress-reporting helpers."""

from __future__ import annotations

import datetime
import logging
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from process.ptg_parts.config import (
    PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV,
    PTG2_PROGRESS_INTERVAL_SECONDS_ENV,
    _env_int,
)
from process.ptg_parts.live_progress import write_live_progress
from process.ptg_parts.screen import _emit_screen_line

logger = logging.getLogger(__name__)


class PTGFileProgressCoordinator:
    """Aggregate concurrent file progress using exact compressed input weights."""

    def __init__(
        self,
        file_weights: Sequence[int],
        file_labels: Sequence[str],
        *,
        stage_start_pct: float,
        stage_end_pct: float,
        stage_id: str = "processing_files",
        stage_ordinal: int = 3,
    ) -> None:
        if len(file_weights) != len(file_labels) or not file_weights:
            raise ValueError("PTG progress files require matching nonempty weights and labels")
        weights = tuple(max(int(weight), 0) for weight in file_weights)
        if sum(weights) <= 0:
            raise ValueError("PTG progress files require positive aggregate weight")
        if stage_end_pct <= stage_start_pct:
            raise ValueError("PTG progress stage end must be greater than its start")
        self._weights = weights
        self._labels = tuple(_safe_progress_file_label(label) for label in file_labels)
        self._stage_start_pct = float(stage_start_pct)
        self._stage_end_pct = float(stage_end_pct)
        self._stage_id = stage_id
        self._stage_ordinal = int(stage_ordinal)
        self._total_weight = sum(weights)
        self._dominant_index = max(range(len(weights)), key=weights.__getitem__)
        self._fraction_by_index = [0.0 for _ in weights]
        self._counters_by_index: list[dict[str, Any]] = [
            {} for _ in weights
        ]
        self._completed_indices: set[int] = set()
        self._lock = threading.Lock()

    def observer(self, file_index: int):
        """Return a scanner callback bound to one zero-based file index."""

        self._validate_index(file_index)

        def observe(payload: dict[str, Any]) -> None:
            """Forward one scanner observation to its bound file slot."""

            self.observe(file_index, payload)

        return observe

    def observe(self, file_index: int, payload: Mapping[str, Any]) -> None:
        """Publish one monotonic weighted scanner observation."""

        self._validate_index(file_index)
        with self._lock:
            counters = payload.get("counters")
            if isinstance(counters, Mapping):
                _merge_file_progress_counters(
                    self._counters_by_index[file_index],
                    counters,
                )
            candidate = _file_progress_fraction(payload)
            if candidate is not None:
                self._fraction_by_index[file_index] = max(
                    self._fraction_by_index[file_index],
                    min(candidate, 0.999),
                )
            self._publish_locked(file_index, payload)

    def complete(self, file_index: int, *, message: str | None = None) -> None:
        """Advance one file to 100 percent only after successful processing."""

        self._validate_index(file_index)
        with self._lock:
            self._fraction_by_index[file_index] = 1.0
            self._completed_indices.add(file_index)
            self._publish_locked(
                file_index,
                {
                    "phase": "processing files",
                    "phase_pct": 100.0,
                    "message": message or "file processing complete",
                    "source": "ptg-file-progress-coordinator",
                    "confidence": "measured",
                },
            )

    def _publish_locked(
        self,
        file_index: int,
        scanner_observation: Mapping[str, Any],
    ) -> None:
        """Publish the aggregate run position while holding the coordinator lock."""

        weighted_done = sum(
            weight * fraction
            for weight, fraction in zip(
                self._weights,
                self._fraction_by_index,
                strict=True,
            )
        )
        stage_fraction = weighted_done / self._total_weight
        run_pct = self._stage_start_pct + (
            (self._stage_end_pct - self._stage_start_pct) * stage_fraction
        )
        file_weight_pct = self._weights[file_index] * 100.0 / self._total_weight
        counters = _aggregate_file_progress_counters(self._counters_by_index)
        counters.update(
            {
                "files_completed": len(self._completed_indices),
                "files_total": len(self._weights),
                "active_file_weight_pct": file_weight_pct,
                "dominant_file_weight_pct": (
                    self._weights[self._dominant_index] * 100.0
                    / self._total_weight
                ),
            }
        )
        original_message = str(
            scanner_observation.get("message")
            or scanner_observation.get("phase")
            or "processing"
        )
        is_indeterminate = (
            file_index not in self._completed_indices
            and _is_progress_denominator_indeterminate(scanner_observation)
        )
        if is_indeterminate:
            semantic_work_done = scanner_observation.get("work_done")
            if semantic_work_done is None:
                semantic_work_done = scanner_observation.get("done")
            progress_position_by_field = {
                "stage_pct": None,
                "phase_pct": None,
                "unit": (
                    scanner_observation.get("unit")
                    or scanner_observation.get("basis")
                    or "items"
                ),
                "basis": (
                    scanner_observation.get("basis")
                    or scanner_observation.get("unit")
                    or "items"
                ),
                "denominator_state": "unknown",
                "done": scanner_observation.get("done"),
                "total": None,
                "work_done": semantic_work_done,
                "work_total": None,
                "pct": None,
                "weighted_compressed_input_bytes_lower_bound": int(
                    weighted_done
                ),
                "stage_pct_lower_bound": stage_fraction * 100.0,
                "pct_lower_bound": min(run_pct, self._stage_end_pct),
                "eta_seconds": None,
            }
        else:
            progress_position_by_field = {
                "stage_pct": stage_fraction * 100.0,
                "phase_pct": scanner_observation.get("phase_pct"),
                "unit": "compressed_input_bytes",
                "basis": "weighted_compressed_input_bytes",
                "denominator_state": "known",
                "done": int(weighted_done),
                "total": self._total_weight,
                "work_done": int(weighted_done),
                "work_total": self._total_weight,
                "pct": min(run_pct, self._stage_end_pct),
            }
        weight_label = f"{file_weight_pct:.1f}% of compressed input"
        if file_index == self._dominant_index:
            weight_label += "; largest file"
        progress_by_field = {
            **dict(scanner_observation),
            "stage_id": self._stage_id,
            "stage_ordinal": self._stage_ordinal,
            **progress_position_by_field,
            "file_index": file_index + 1,
            "file_count": len(self._weights),
            "file_name": self._labels[file_index],
            "file_weight": self._weights[file_index],
            "file_weight_pct": file_weight_pct,
            "dominant_file": file_index == self._dominant_index,
            "counters": counters,
            "message": (
                f"File {file_index + 1}/{len(self._weights)} "
                f"({weight_label}): {original_message}"
            )[:512],
        }
        write_live_progress(**progress_by_field)

    def _validate_index(self, file_index: int) -> None:
        if file_index < 0 or file_index >= len(self._weights):
            raise IndexError("PTG progress file index is out of range")


def _file_progress_fraction(payload: Mapping[str, Any]) -> float | None:
    if _is_progress_denominator_indeterminate(payload):
        return None
    for field_name in ("phase_pct", "stage_pct"):
        try:
            value = float(payload.get(field_name))
        except (TypeError, ValueError):
            continue
        if value == value:
            return max(0.0, min(value / 100.0, 1.0))
    try:
        done = float(payload.get("done"))
        total = float(payload.get("total"))
    except (TypeError, ValueError):
        return None
    if total <= 0 or done != done or total != total:
        return None
    return max(0.0, min(done / total, 1.0))


def _is_progress_denominator_indeterminate(payload: Mapping[str, Any]) -> bool:
    """Return whether a progress payload exposes only lower-bound work."""

    progress_basis = str(
        payload.get("basis") or payload.get("unit") or ""
    ).strip().lower()
    denominator_state = str(
        payload.get("denominator_state") or ""
    ).strip().lower()
    return (
        progress_basis == "semantic_work"
        and denominator_state in {"unknown", "lower_bound"}
    )


def _merge_file_progress_counters(
    retained: dict[str, Any],
    incoming: Mapping[str, Any],
) -> None:
    """Retain each file's latest monotonic scanner counters."""

    for raw_name, value in incoming.items():
        name = str(raw_name)
        previous = retained.get(name)
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and isinstance(previous, (int, float))
            and not isinstance(previous, bool)
        ):
            retained[name] = max(previous, value)
        else:
            retained[name] = value


def _aggregate_file_progress_counters(
    counters_by_index: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Sum numeric per-file counters while retaining simple shared metadata."""

    counters_by_name: dict[str, Any] = {}
    for file_counters in counters_by_index:
        for name, value in file_counters.items():
            if isinstance(value, bool):
                counters_by_name[name] = bool(
                    counters_by_name.get(name, False) or value
                )
            elif isinstance(value, (int, float)):
                counters_by_name[name] = counters_by_name.get(name, 0) + value
            elif name not in counters_by_name:
                counters_by_name[name] = value
    return counters_by_name


def _safe_progress_file_label(value: str) -> str:
    raw = str(value or "").strip()
    parsed = urlsplit(raw)
    if parsed.scheme and parsed.netloc:
        return parsed.path.rsplit("/", 1)[-1] or parsed.netloc
    return Path(raw).name or raw[:256] or "PTG file"


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def _artifact_progress_position(stream: Any) -> int | None:
    raw_stream = getattr(getattr(stream, "raw", None), "fileobj", None)
    if raw_stream is None:
        raw_stream = getattr(stream, "fileobj", None)
    if raw_stream is not None and hasattr(raw_stream, "tell"):
        with suppress(OSError, TypeError, ValueError):
            return int(raw_stream.tell())
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
    """Log bounded artifact progress when the configured interval elapses."""
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
