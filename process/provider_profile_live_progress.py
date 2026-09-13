# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Report measured work within long provider-profile import stages."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable
from time import monotonic
from typing import Any, TypeVar

_Row = TypeVar("_Row")
PROGRESS_INTERVAL_SECONDS = 10.0


async def normalization_rows(
    rows: Iterable[_Row],
    *,
    title: str,
    file_name: str,
    file_index: int,
    file_count: int,
    report: Callable[..., Any],
) -> AsyncIterator[_Row]:
    """Announce each source and count consumed rows without inventing a total."""
    progress_by_field = {
        "phase": "normalizing",
        "pct": 36 + int(49 * (file_index - 1) / file_count),
        "message": f"Normalizing {title}",
        "file_name": file_name,
        "file_index": file_index,
        "file_count": file_count,
    }
    report(**progress_by_field, counters={"file_rows_processed": 0})
    await asyncio.sleep(0)
    last_report = monotonic()
    for processed, row in enumerate(rows, start=1):
        yield row
        now = monotonic()
        if now - last_report >= PROGRESS_INTERVAL_SECONDS:
            report(**progress_by_field, counters={"file_rows_processed": processed})
            await asyncio.sleep(0)
            last_report = now


def projection_batches(
    inserted_count: int,
    *,
    report: Callable[..., Any],
) -> None:
    """Report staged providers only after their database write completes."""
    report(
        phase="publishing",
        pct=94,
        message="Building provider profile generation",
        counters={"staged_providers": inserted_count},
    )


def normalization_completed(
    source: Any,
    file_index: int,
    file_count: int,
    metrics: dict[str, Any],
    report: Callable[..., Any],
) -> None:
    """Report completed source counters with the existing physical semantics."""
    report(
        phase="normalizing",
        pct=36 + int(49 * file_index / file_count),
        message=f"Normalized {source.title}",
        file_index=file_index,
        file_count=file_count,
        file_name=source.filename,
        counters={key: value for key, value in metrics.items() if key not in {"artifacts", "source_metrics"}},
    )
