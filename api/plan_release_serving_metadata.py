# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small metadata normalizers for immutable plan-release selection."""

from __future__ import annotations

import datetime as dt
from typing import Any, Iterable, Mapping


def single_text_value(
    rows: Iterable[Mapping[str, Any]],
    field: str,
) -> str | None:
    """Return one shared non-empty text value across release rows."""

    values = {str(row.get(field) or "").strip() for row in rows}
    if len(values) != 1:
        return None
    value = values.pop()
    return value or None


def canonical_published_at(value: Any) -> str | None:
    """Return a timezone-bound serving publication as canonical UTC text."""

    if isinstance(value, dt.datetime):
        published_at = value
    elif isinstance(value, str) and value == value.strip() and value:
        try:
            published_at = dt.datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )
        except ValueError:
            return None
    else:
        return None
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        return None
    return published_at.astimezone(dt.UTC).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def serving_revision_published_at(
    release_rows: Iterable[Mapping[str, Any]],
) -> str | None:
    """Return one canonical shared publication timestamp when available."""

    canonical_values = {
        canonical_published_at(row.get("serving_revision_published_at"))
        for row in release_rows
    }
    if len(canonical_values) != 1 or None in canonical_values:
        return None
    return next(iter(canonical_values))


def has_expected_binding_count(
    release_rows: list[dict[str, Any]],
) -> bool:
    """Return whether every row declares the complete binding count."""

    try:
        expected_counts = {
            int(release_row.get("expected_binding_count"))
            for release_row in release_rows
        }
    except (TypeError, ValueError):
        return False
    return expected_counts == {len(release_rows)}
