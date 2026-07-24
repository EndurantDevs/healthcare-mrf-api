"""Durable progress-timeline evaluation for the PTG V4 canary."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from scripts.ptg_v4_dev_canary_support import ElapsedBudget


PROGRESS_EVIDENCE_CONTRACT = "ptg_v4_progress_timeline_v1"


def evaluate_progress_timeline(
    timeline_by_field: Mapping[str, Any],
    *,
    budget: ElapsedBudget,
    maximum_update_gap_seconds: float,
    minimum_intermediate_updates: int = 2,
) -> dict[str, Any]:
    """Fail closed unless captured progress moved monotonically to 100 percent."""

    failures: list[str] = []
    if timeline_by_field.get("contract") != PROGRESS_EVIDENCE_CONTRACT:
        failures.append("progress timeline contract is missing or unsupported")
    poll_interval = _optional_float(timeline_by_field.get("poll_interval_seconds"))
    if poll_interval is None or not 0 < poll_interval <= 5:
        failures.append("progress monitor interval was not within five seconds")
    run = _mapping(timeline_by_field.get("run"))
    status = str(run.get("status") or "").strip().lower()
    if status != "succeeded":
        failures.append(f"control run status is {status or 'missing'}, not succeeded")
    elapsed_seconds = _elapsed_seconds(
        run.get("started_at"),
        run.get("finished_at"),
        failures,
    )
    if elapsed_seconds is not None and elapsed_seconds > budget.ceiling_seconds:
        failures.append("import elapsed time exceeds its size-derived ceiling")
    raw_samples = timeline_by_field.get("samples")
    terminal_progress = _terminal_progress(raw_samples, failures)
    movement_samples = _progress_updates(raw_samples, failures)
    heartbeat_samples = _heartbeat_updates(raw_samples, failures)
    _validate_progress_updates(
        movement_samples,
        maximum_update_gap_seconds=maximum_update_gap_seconds,
        minimum_intermediate_updates=minimum_intermediate_updates,
        started_at=run.get("started_at"),
        finished_at=run.get("finished_at"),
        failures=failures,
    )
    _validate_heartbeat_updates(
        heartbeat_samples,
        maximum_gap_seconds=maximum_update_gap_seconds,
        failures=failures,
    )
    run_summary_by_field = {
        "status": status or None,
        "elapsed_seconds": (
            round(elapsed_seconds, 3) if elapsed_seconds is not None else None
        ),
        "poll_interval_seconds": poll_interval,
        "captured_sample_count": len(raw_samples or []),
    }
    return _progress_report(
        failures,
        run_summary_by_field=run_summary_by_field,
        budget=budget,
        movement_samples=movement_samples,
        heartbeat_samples=heartbeat_samples,
        terminal_progress=terminal_progress,
    )


def _progress_report(
    failures: Sequence[str],
    *,
    run_summary_by_field: Mapping[str, Any],
    budget: ElapsedBudget,
    movement_samples: Sequence[Mapping[str, Any]],
    heartbeat_samples: Sequence[Mapping[str, Any]],
    terminal_progress: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Shape the durable progress acceptance report after validation."""

    return {
        "passed": not failures,
        "failures": list(failures),
        **dict(run_summary_by_field),
        "elapsed_budget": budget.report(),
        "distinct_progress_update_count": len(movement_samples),
        "heartbeat_update_count": len(heartbeat_samples),
        "intermediate_update_count": sum(
            1 for sample in movement_samples if _is_intermediate_progress(sample)
        ),
        "maximum_observed_update_gap_seconds": _maximum_update_gap(
            movement_samples
        ),
        "maximum_observed_heartbeat_gap_seconds": _maximum_update_gap(
            heartbeat_samples,
            timestamp_field="observed_at",
        ),
        "final_progress": terminal_progress,
    }


def _terminal_progress(raw_samples: Any, failures: list[str]) -> dict[str, Any] | None:
    if not isinstance(raw_samples, list) or not raw_samples:
        failures.append("progress timeline lacks a terminal sample")
        return None
    terminal_sample = _mapping(raw_samples[-1])
    terminal_status = str(terminal_sample.get("status") or "").strip().lower()
    terminal_progress = _mapping(terminal_sample.get("progress"))
    if terminal_status != "succeeded":
        failures.append("progress timeline does not end with succeeded status")
    if _optional_float(terminal_progress.get("pct")) != 100:
        failures.append("terminal control progress did not reach 100 percent")
    return terminal_progress or None


def _progress_updates(raw_samples: Any, failures: list[str]) -> list[dict[str, Any]]:
    if not isinstance(raw_samples, list):
        failures.append("progress timeline samples are missing")
        return []
    updates: list[dict[str, Any]] = []
    previous_progress_sequence = -1
    for raw_sample in raw_samples:
        sample = _mapping(raw_sample)
        if str(sample.get("status") or "").strip().lower() in {
            "succeeded",
            "failed",
            "canceled",
            "cancelled",
            "dead_letter",
        }:
            continue
        progress = _mapping(sample.get("progress"))
        if not progress:
            continue
        update_by_field = {
            "observed_at": sample.get("observed_at"),
            "updated_at": progress.get("updated_at"),
            "progressed_at": progress.get("progressed_at"),
            "pct": _optional_float(progress.get("pct")),
            "done": _optional_int(progress.get("done")),
            "total": _optional_int(progress.get("total")),
            "stage_id": progress.get("stage_id"),
            "stage_ordinal": _optional_int(progress.get("stage_ordinal")),
            "compiler_phase": progress.get("compiler_phase") or progress.get("phase"),
            "unit": progress.get("unit"),
            "basis": progress.get("basis") or progress.get("scanner_progress_basis"),
            "denominator_state": progress.get("denominator_state"),
            "counters": _mapping(progress.get("counters")),
            "event_seq": _optional_int(progress.get("event_seq")),
            "progress_seq": _optional_int(progress.get("progress_seq")),
        }
        progress_sequence = update_by_field["progress_seq"]
        if progress_sequence is None:
            continue
        if progress_sequence < previous_progress_sequence:
            failures.append("progress timeline progress_seq moved backward")
            continue
        if progress_sequence == previous_progress_sequence:
            continue
        previous_progress_sequence = progress_sequence
        updates.append(update_by_field)
    return updates


def _heartbeat_updates(raw_samples: Any, failures: list[str]) -> list[dict[str, Any]]:
    if not isinstance(raw_samples, list):
        return []
    heartbeats: list[dict[str, Any]] = []
    previous_event_sequence = -1
    for raw_sample in raw_samples:
        sample = _mapping(raw_sample)
        if str(sample.get("status") or "").strip().lower() in {
            "succeeded",
            "failed",
            "canceled",
            "cancelled",
            "dead_letter",
        }:
            continue
        progress = _mapping(sample.get("progress"))
        event_sequence = _optional_int(progress.get("event_seq"))
        if event_sequence is None:
            continue
        if event_sequence < previous_event_sequence:
            failures.append("progress timeline event_seq moved backward")
            continue
        if event_sequence == previous_event_sequence:
            continue
        previous_event_sequence = event_sequence
        heartbeats.append(
            {
                "event_seq": event_sequence,
                "observed_at": progress.get("observed_at") or sample.get("observed_at"),
            }
        )
    return heartbeats


def _validate_progress_updates(
    samples: Sequence[Mapping[str, Any]],
    *,
    maximum_update_gap_seconds: float,
    minimum_intermediate_updates: int,
    started_at: Any,
    finished_at: Any,
    failures: list[str],
) -> None:
    if len(samples) < minimum_intermediate_updates:
        failures.append("progress timeline has too few distinct updates")
        return
    required_fields = (
        "stage_ordinal",
        "event_seq",
        "progress_seq",
    )
    if any(
        sample.get(field_name) is None
        for sample in samples
        for field_name in required_fields
    ):
        failures.append("progress timeline contains incomplete numeric evidence")
        return
    for sample in samples:
        _validate_progress_shape(sample, failures)
    if (
        sum(1 for sample in samples if _is_intermediate_progress(sample))
        < minimum_intermediate_updates
    ):
        failures.append("progress timeline lacks two intermediate updates")
    _validate_boundary_gap(
        started_at,
        samples[0].get("progressed_at"),
        maximum_update_gap_seconds,
        label="initial progress movement",
        failures=failures,
    )
    for previous, current in zip(samples, samples[1:]):
        _validate_progress_pair(
            previous,
            current,
            maximum_update_gap_seconds,
            failures,
        )
    _validate_boundary_gap(
        samples[-1].get("progressed_at"),
        finished_at,
        maximum_update_gap_seconds,
        label="terminal progress movement",
        failures=failures,
    )


def _validate_boundary_gap(
    previous_timestamp: Any,
    current_timestamp: Any,
    maximum_gap_seconds: float,
    *,
    label: str,
    failures: list[str],
) -> None:
    gap_seconds = _timestamp_gap(previous_timestamp, current_timestamp)
    if gap_seconds is None or gap_seconds < 0:
        failures.append(f"{label} timestamps are invalid")
    elif gap_seconds > maximum_gap_seconds:
        failures.append(f"{label} gap exceeds the configured maximum")


def _validate_progress_pair(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
    maximum_gap_seconds: float,
    failures: list[str],
) -> None:
    for field_name in ("stage_ordinal", "event_seq", "progress_seq"):
        if float(current[field_name]) < float(previous[field_name]):
            failures.append(f"progress timeline {field_name} moved backward")
    previous_pct = _optional_float(previous.get("pct"))
    current_pct = _optional_float(current.get("pct"))
    if (
        previous_pct is not None
        and current_pct is not None
        and current_pct < previous_pct
    ):
        failures.append("progress timeline pct moved backward")
    is_same_work_context = _work_counter_context(previous) == _work_counter_context(
        current
    )
    previous_done = _optional_int(previous.get("done"))
    current_done = _optional_int(current.get("done"))
    if (
        is_same_work_context
        and previous_done is not None
        and current_done is not None
        and current_done < previous_done
    ):
        failures.append("progress timeline done moved backward within one phase")
    are_both_indeterminate = (
        is_same_work_context
        and _is_progress_indeterminate(previous)
        and _is_progress_indeterminate(current)
    )
    has_counter_advanced = (
        _has_counter_advanced(previous, current, failures)
        if are_both_indeterminate
        else False
    )
    if (
        are_both_indeterminate
        and not has_counter_advanced
        and not (
            previous_done is not None
            and current_done is not None
            and current_done > previous_done
        )
    ):
        failures.append("indeterminate progress counters did not advance")
    gap_seconds = _timestamp_gap(
        previous.get("progressed_at"),
        current.get("progressed_at"),
    )
    if gap_seconds is None or gap_seconds <= 0:
        failures.append("progressed_at timestamps are invalid or non-increasing")
    elif gap_seconds > maximum_gap_seconds:
        failures.append("progress movement gap exceeds the configured maximum")


def _validate_progress_shape(sample: Mapping[str, Any], failures: list[str]) -> None:
    """Require either exact bounded work or truthful indeterminate work."""

    if _is_progress_indeterminate(sample):
        if _optional_float(sample.get("pct")) is not None:
            failures.append(
                "indeterminate progress falsely reports a determinate percent"
            )
        if _optional_int(sample.get("total")) is not None:
            failures.append("indeterminate progress falsely reports a known total")
        if (
            _optional_int(sample.get("done")) is None
            and not _numeric_counters(sample)
        ):
            failures.append("indeterminate progress lacks advancing work counters")
        return
    if any(
        _optional_float(sample.get(field_name)) is None
        for field_name in ("pct", "done", "total")
    ):
        failures.append("progress timeline contains incomplete numeric evidence")
        return
    if float(sample["total"]) <= 0:
        failures.append("determinate progress total is not positive")


def _is_intermediate_progress(sample: Mapping[str, Any]) -> bool:
    progress_pct = _optional_float(sample.get("pct"))
    return progress_pct is None or progress_pct < 100


def _is_progress_indeterminate(sample: Mapping[str, Any]) -> bool:
    denominator_state = str(sample.get("denominator_state") or "").strip().lower()
    return (
        denominator_state in {"unknown", "lower_bound"}
        or _optional_int(sample.get("total")) is None
    )


def _numeric_counters(sample: Mapping[str, Any]) -> dict[str, float]:
    return {
        str(name): float(value)
        for name, value in _mapping(sample.get("counters")).items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }


def _has_counter_advanced(
    previous: Mapping[str, Any],
    current: Mapping[str, Any],
    failures: list[str],
) -> bool:
    previous_counters = _numeric_counters(previous)
    current_counters = _numeric_counters(current)
    has_advanced = False
    for name, previous_value in previous_counters.items():
        current_value = current_counters.get(name)
        if current_value is None:
            continue
        if current_value < previous_value:
            failures.append(f"progress timeline counter {name} moved backward")
        elif current_value > previous_value:
            has_advanced = True
    return has_advanced


def _validate_heartbeat_updates(
    samples: Sequence[Mapping[str, Any]],
    *,
    maximum_gap_seconds: float,
    failures: list[str],
) -> None:
    if len(samples) < 2:
        failures.append("progress timeline lacks heartbeat liveness evidence")
        return
    for previous, current in zip(samples, samples[1:]):
        gap_seconds = _timestamp_gap(
            previous.get("observed_at"),
            current.get("observed_at"),
        )
        if gap_seconds is None or gap_seconds <= 0:
            failures.append("heartbeat timestamps are invalid or non-increasing")
        elif gap_seconds > maximum_gap_seconds:
            failures.append("heartbeat liveness gap exceeds the configured maximum")


def _work_counter_context(sample: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        sample.get("stage_id"),
        sample.get("compiler_phase"),
        sample.get("unit"),
        sample.get("total"),
    )


def _elapsed_seconds(
    started_value: Any,
    finished_value: Any,
    failures: list[str],
) -> float | None:
    try:
        started_at = _parse_datetime(started_value)
        finished_at = _parse_datetime(finished_value)
    except ValueError:
        failures.append("control run timestamps are missing or invalid")
        return None
    elapsed_seconds = (finished_at - started_at).total_seconds()
    if elapsed_seconds < 0:
        failures.append("control run elapsed time is negative")
        return None
    return elapsed_seconds


def _maximum_update_gap(
    samples: Sequence[Mapping[str, Any]],
    *,
    timestamp_field: str = "progressed_at",
) -> float | None:
    gaps = [
        gap
        for previous, current in zip(samples, samples[1:])
        if (gap := _timestamp_gap(
            previous.get(timestamp_field),
            current.get(timestamp_field),
        ))
        is not None
    ]
    return round(max(gaps), 3) if gaps else None


def _timestamp_gap(previous: Any, current: Any) -> float | None:
    try:
        return (_parse_datetime(current) - _parse_datetime(previous)).total_seconds()
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    else:
        raise ValueError("missing timestamp")
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
