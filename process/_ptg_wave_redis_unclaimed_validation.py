"""Pure ARQ membership checks for all-unclaimed Redis evidence."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from process._ptg_wave_redis_manifest import attest_arq_job_bytes
from process._ptg_wave_redis_models import (
    PTGSmallWaveAttestationError,
    PTGSmallWaveManifest,
    as_optional_bytes,
)


def queued_ordinals(
    manifest: PTGSmallWaveManifest,
    queue_entries: Any,
) -> tuple[int, ...]:
    """Return the exact queue subset, rejecting foreign or altered members."""

    if (
        not isinstance(queue_entries, Sequence)
        or isinstance(queue_entries, (str, bytes, bytearray))
    ):
        raise PTGSmallWaveAttestationError(
            "all-unclaimed queue read returned an invalid shape"
        )
    expected_by_id = {job.job_id: job for job in manifest.jobs}
    ordinals: list[int] = []
    seen_job_ids: set[str] = set()
    for entry in queue_entries:
        if (
            not isinstance(entry, Sequence)
            or isinstance(entry, (str, bytes, bytearray))
            or len(entry) != 2
        ):
            raise PTGSmallWaveAttestationError(
                "all-unclaimed queue entry has an invalid shape"
            )
        job_id = _queue_member_text(entry[0])
        score = entry[1]
        if job_id in seen_job_ids or job_id not in expected_by_id:
            raise PTGSmallWaveAttestationError(
                "all-unclaimed queue has a foreign or repeated member"
            )
        job = expected_by_id[job_id]
        if (
            not isinstance(score, (int, float))
            or isinstance(score, bool)
            or int(score) != score
            or int(score) != job.score_ms
        ):
            raise PTGSmallWaveAttestationError(
                "all-unclaimed queue score does not match the immutable manifest"
            )
        seen_job_ids.add(job_id)
        ordinals.append(job.ordinal)
    return tuple(sorted(ordinals))


def _queue_member_text(candidate: Any) -> str:
    raw = as_optional_bytes(candidate)
    if raw is None:
        raise PTGSmallWaveAttestationError(
            "all-unclaimed queue contains a missing member"
        )
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PTGSmallWaveAttestationError(
            "all-unclaimed queue contains a non-UTF-8 member"
        ) from exc


def verified_job_ordinals(
    manifest: PTGSmallWaveManifest,
    scalars: Any,
) -> tuple[int, ...]:
    """Validate every retained ARQ payload against the immutable bytes."""

    scalar_values = scalar_sequence(
        scalars,
        expected_count=len(manifest.jobs),
        label="job",
    )
    ordinals: list[int] = []
    for job, scalar in zip(manifest.jobs, scalar_values):
        stored = as_optional_bytes(scalar)
        if stored is None:
            continue
        attest_arq_job_bytes(job, stored)
        ordinals.append(job.ordinal)
    return tuple(ordinals)


def present_ordinals(
    scalars: Any,
    *,
    expected_count: int,
    label: str,
) -> tuple[int, ...]:
    """Record exactly which ordinal keys exist after type validation."""

    scalar_values = scalar_sequence(
        scalars,
        expected_count=expected_count,
        label=label,
    )
    return tuple(
        ordinal
        for ordinal, scalar in enumerate(scalar_values)
        if as_optional_bytes(scalar) is not None
    )


def scalar_sequence(
    scalars: Any,
    *,
    expected_count: int,
    label: str,
) -> Sequence[Any]:
    """Require one exact ordered Redis scalar response sequence."""

    if (
        not isinstance(scalars, Sequence)
        or isinstance(scalars, (str, bytes, bytearray))
        or len(scalars) != expected_count
    ):
        raise PTGSmallWaveAttestationError(
            f"all-unclaimed {label} key read returned an invalid shape"
        )
    return scalars


def validate_released_partition(
    manifest: PTGSmallWaveManifest,
    queued_ordinal_values: tuple[int, ...],
    job_ordinal_values: tuple[int, ...],
    result_ordinal_values: tuple[int, ...],
) -> None:
    """Require each released ordinal to be pending or result-retained."""

    queued_ordinal_set = set(queued_ordinal_values)
    job_ordinal_set = set(job_ordinal_values)
    result_ordinal_set = set(result_ordinal_values)
    expected_ordinal_set = set(range(len(manifest.jobs)))
    if queued_ordinal_set != job_ordinal_set:
        raise PTGSmallWaveAttestationError(
            "released all-unclaimed queue and job payload subsets differ"
        )
    if (
        queued_ordinal_set & result_ordinal_set
        or queued_ordinal_set | result_ordinal_set != expected_ordinal_set
    ):
        raise PTGSmallWaveAttestationError(
            "released all-unclaimed ARQ state is not a complete stable partition"
        )
