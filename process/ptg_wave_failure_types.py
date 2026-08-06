"""Shared immutable types and pure helpers for exact-wave failure proof."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)


FAILURE_SCHEMA = "healthporta.ptg-wave.unclaimed-failure.v1"
CLAIMED_PRESTART_FAILURE_SCHEMA = (
    "healthporta.ptg-wave.claimed-prestart-failure.v1"
)
CLAIMED_PRESTART_FAILURE_REASON = "claimed_prestart_failure"
FAILURE_REASONS = frozenset(
    {"kubernetes_post_absent", "redis_release_absent", "pre_claim_failure"}
)
_HEX_DIGEST_LENGTH = 64


class PTGWaveFailureConflict(PTGWaveStateConflict):
    """A fail-closed recovery or all-unclaimed failure proof is invalid."""


@dataclass(frozen=True)
class PTGWaveReadOnlyRecovery:
    """One unresolved ticket and the sole GET-only observation it permits."""

    operation: str
    ticket: str
    required_observation: str
    mutation_permitted: bool = False


def _digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _HEX_DIGEST_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PTGWaveFailureConflict(f"{name} must be a SHA-256 digest")
    return value


def _rows_by_ordinal(rows: Iterable[Any]) -> list[Any]:
    ordered_rows = sorted(rows, key=lambda row: int(row.ordinal))
    if [row.ordinal for row in ordered_rows] != list(range(len(ordered_rows))):
        raise PTGWaveFailureConflict(
            "exact-wave ordinals are not complete and contiguous"
        )
    return ordered_rows


def _unclaimed_ordinals_digest(wave: Any) -> str:
    return sha256_digest(
        canonical_json(
            {
                "schema_version": 1,
                "wave_id": wave.wave_id,
                "ordinals": list(range(wave.intent_count)),
            }
        )
    )


def _claimed_ordinals_digest(
    wave: Any, ordinals: Iterable[int]
) -> str:
    return sha256_digest(
        canonical_json(
            {
                "schema_version": 1,
                "wave_id": wave.wave_id,
                "wave_digest": wave.wave_digest,
                "claimed_ordinals": list(ordinals),
            }
        )
    )


def is_claimed_prestart_failure_receipt(receipt: object) -> bool:
    """Return whether a persisted failure uses the narrow prestart schema."""

    return (
        isinstance(receipt, dict)
        and receipt.get("schema_version") == CLAIMED_PRESTART_FAILURE_SCHEMA
    )


def _require_mapping(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PTGWaveFailureConflict(f"{name} must be an object")
    return value


def _dead_letter_record(intent: Any) -> dict[str, Any]:
    return {
        "ordinal": intent.ordinal,
        "run_id": intent.run_id,
        "job_id": intent.job_id,
        "source_file_import_id": intent.source_file_import_id,
        "content_version": intent.content_version,
        "status": "dead_letter",
        "snapshot_id": None,
        "import_id": None,
    }


def _single_outcome_digest(record: dict[str, Any]) -> str:
    return sha256_digest(
        canonical_json({"schema_version": 1, "outcome": record})
    )


def _outcomes_digest(records: Iterable[dict[str, Any]]) -> str:
    return sha256_digest(
        canonical_json(
            {
                "domain": "healthporta.ptg-wave.outcomes.v1",
                "records": list(records),
            }
        )
    )
