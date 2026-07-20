# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Low-level UTC boundary and fingerprint primitives for FHIR partitioning."""

from __future__ import annotations

import copy
import datetime
import hashlib
import json
from typing import Any, Iterable, Mapping


class PartitionPlanError(ValueError):
    """Raised when a plan operation or checkpoint is invalid."""


def validate_utc_instant(
    instant: datetime.datetime,
    label: str = "instant",
) -> datetime.datetime:
    """Validate and normalize an explicitly UTC-aware datetime."""
    if not isinstance(instant, datetime.datetime):
        raise PartitionPlanError(f"{label} must be a datetime")
    if instant.tzinfo is None or instant.utcoffset() != datetime.timedelta(0):
        raise PartitionPlanError(f"{label} must be explicitly UTC")
    return instant.astimezone(datetime.UTC)


def format_utc_instant(instant: datetime.datetime) -> str:
    """Format a validated UTC instant with deterministic microseconds."""
    normalized = validate_utc_instant(instant)
    return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc_instant(text: str) -> datetime.datetime:
    """Parse an ISO instant and reject naive or non-UTC offsets."""
    if not isinstance(text, str):
        raise PartitionPlanError("UTC instant must be a string")
    try:
        instant = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PartitionPlanError("invalid UTC instant") from exc
    return validate_utc_instant(instant)


def align_partition_boundary(
    instant: datetime.datetime,
    precision: datetime.timedelta,
    *,
    round_up: bool,
) -> datetime.datetime:
    """Align an instant to a UTC epoch-based precision boundary."""
    normalized = validate_utc_instant(instant)
    precision_microseconds = timedelta_microseconds(precision)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
    offset_microseconds = timedelta_microseconds(normalized - epoch)
    boundary_units, remainder = divmod(offset_microseconds, precision_microseconds)
    if round_up and remainder:
        boundary_units += 1
    return epoch + datetime.timedelta(
        microseconds=boundary_units * precision_microseconds
    )


def is_partition_boundary_aligned(
    instant: datetime.datetime,
    precision: datetime.timedelta,
) -> bool:
    """Return whether an instant falls exactly on a UTC precision boundary."""
    normalized = validate_utc_instant(instant)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
    return (
        timedelta_microseconds(normalized - epoch)
        % timedelta_microseconds(precision)
        == 0
    )


def fingerprint_resource(
    resource: Mapping[str, Any],
    volatile_paths: Iterable[str] = (),
) -> str:
    """Hash canonical resource JSON after removing configured JSON Pointers."""
    canonical = copy.deepcopy(dict(resource))
    decoded_paths = sorted(
        (decode_json_pointer(path) for path in volatile_paths),
        reverse=True,
    )
    for path_parts in decoded_paths:
        _remove_path(canonical, path_parts)
    encoded = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def decode_json_pointer(path: str) -> tuple[str, ...]:
    """Decode a non-root JSON Pointer used for volatile metadata removal."""
    if not isinstance(path, str) or not path.startswith("/") or path == "/":
        raise PartitionPlanError(
            "volatile metadata paths must be non-root JSON Pointers"
        )
    return tuple(
        part.replace("~1", "/").replace("~0", "~")
        for part in path[1:].split("/")
    )


def _remove_path(container: Any, path_parts: tuple[str, ...]) -> None:
    current = container
    for part in path_parts[:-1]:
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return
    final = path_parts[-1]
    if isinstance(current, dict):
        current.pop(final, None)
    elif isinstance(current, list) and final.isdigit() and int(final) < len(current):
        current.pop(int(final))


def timedelta_microseconds(duration: datetime.timedelta) -> int:
    """Return an exact integer microsecond count without float conversion."""
    return (
        duration.days * 86_400_000_000
        + duration.seconds * 1_000_000
        + duration.microseconds
    )
