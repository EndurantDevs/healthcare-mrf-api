"""Pure exact-wave outcome and linkage contract helpers."""

from __future__ import annotations

import hashlib
import hmac
import os
import re
from typing import Any, Iterable

from process.ptg_wave_state import (
    PTGWaveStateConflict,
    canonical_json,
    sha256_digest,
)


_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_CLAIM_FAILURE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_TERMINAL_RUN_STATES = frozenset(
    {"succeeded", "failed", "canceled", "dead_letter"}
)
_LINKAGE_VERSION = "healthporta.ptg-wave-linkage-ack.v1"
_LINKAGE_DOMAIN = b"healthporta.ptg-wave-linkage-ack.v1\0"


class PTGWaveOutcomeConflict(PTGWaveStateConflict):
    """Terminal records, linkage, or cleanup proof does not match the wave."""


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or not _HEX_64.fullmatch(value):
        raise PTGWaveOutcomeConflict(f"{name} must be a SHA-256 digest")
    return value


def _linkage_key(explicit_key: str | bytes | None) -> bytes:
    value = explicit_key
    if value is None:
        value = str(os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if isinstance(value, str):
        value = value.encode("utf-8")
    if not isinstance(value, bytes) or not value:
        raise PTGWaveOutcomeConflict("linkage acknowledgement key is required")
    return value


def sign_linkage_ack(unsigned_ack: dict[str, Any], *, key: str | bytes) -> str:
    """Sign an upstream linkage acknowledgement with the node token."""

    payload = _LINKAGE_DOMAIN + canonical_json(unsigned_ack)
    return hmac.new(_linkage_key(key), payload, hashlib.sha256).hexdigest()


def _outcome_record(intent: Any, run: Any) -> dict[str, Any]:
    status = str(run.status or "")
    if status not in _TERMINAL_RUN_STATES:
        raise PTGWaveOutcomeConflict(
            "all exact-wave ImportRuns must be terminal before linkage"
        )
    snapshot_id = run.snapshot_id
    import_id = run.import_id
    if status == "succeeded":
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise PTGWaveOutcomeConflict(
                "successful exact-wave ImportRun lacks snapshot evidence"
            )
        if import_id != intent.source_file_import_id:
            raise PTGWaveOutcomeConflict(
                "successful exact-wave ImportRun lacks source evidence"
            )
    return {
        "ordinal": intent.ordinal,
        "run_id": intent.run_id,
        "job_id": intent.job_id,
        "source_file_import_id": intent.source_file_import_id,
        "content_version": intent.content_version,
        "status": status,
        "snapshot_id": snapshot_id,
        "import_id": import_id,
    }


def _record_digest(record: dict[str, Any]) -> str:
    return sha256_digest(
        canonical_json({"schema_version": 1, "outcome": record})
    )


def _collection_digest(domain: str, records: Iterable[dict[str, Any]]) -> str:
    return sha256_digest(
        canonical_json({"domain": domain, "records": list(records)})
    )


def _rows_by_ordinal(
    rows: Iterable[Any], *, require_zero_based: bool = True
) -> list[Any]:
    ordered_rows = sorted(rows, key=lambda row: int(row.ordinal))
    if require_zero_based:
        expected_ordinals = list(range(len(ordered_rows)))
    elif ordered_rows:
        first_ordinal = int(ordered_rows[0].ordinal)
        expected_ordinals = list(
            range(first_ordinal, first_ordinal + len(ordered_rows))
        )
    else:
        expected_ordinals = []
    if [row.ordinal for row in ordered_rows] != expected_ordinals:
        raise PTGWaveOutcomeConflict(
            "exact-wave ordinals are not complete and contiguous"
        )
    return ordered_rows


def _outcome_ordinal(outcome: Any) -> int:
    value = outcome["ordinal"] if isinstance(outcome, dict) else outcome.ordinal
    return int(value)


def _validate_claim_outcomes(
    claims: Iterable[Any], outcomes: Iterable[Any]
) -> list[int]:
    """Require each immutable claim disposition to agree with its outcome."""

    ordered_claims = _rows_by_ordinal(claims)
    try:
        ordered_outcomes = sorted(outcomes, key=_outcome_ordinal)
        outcome_ordinals = [
            _outcome_ordinal(terminal_outcome)
            for terminal_outcome in ordered_outcomes
        ]
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise PTGWaveOutcomeConflict(
            "terminal outcomes have invalid ordinals"
        ) from exc
    if [claim.ordinal for claim in ordered_claims] != outcome_ordinals:
        raise PTGWaveOutcomeConflict(
            "worker claims and terminal outcomes cover different ordinals"
        )
    rejected_ordinals: list[int] = []
    for claim, outcome in zip(ordered_claims, ordered_outcomes):
        claim_status = getattr(claim, "claim_status", None)
        failure_code = getattr(claim, "failure_code", None)
        if claim_status == "started" and failure_code is None:
            continue
        outcome_status = (
            outcome["status"] if isinstance(outcome, dict) else outcome.status
        )
        if (
            claim_status == "rejected"
            and isinstance(failure_code, str)
            and _CLAIM_FAILURE_CODE.fullmatch(failure_code)
            and outcome_status == "failed"
        ):
            rejected_ordinals.append(int(claim.ordinal))
            continue
        raise PTGWaveOutcomeConflict(
            "worker claim disposition does not match its terminal outcome"
        )
    return rejected_ordinals


def _rejected_ordinals_summary(
    wave: Any, rejected: Iterable[int]
) -> tuple[list[int], str]:
    ordinals = list(rejected)
    digest = sha256_digest(
        canonical_json(
            {
                "schema_version": 1,
                "wave_id": wave.wave_id,
                "wave_digest": wave.wave_digest,
                "rejected_ordinals": ordinals,
            }
        )
    )
    return ordinals, digest


def _mapping_records(outcomes: Iterable[Any]) -> list[dict[str, Any]]:
    return [
        {
            "ordinal": row.ordinal,
            "run_id": row.run_id,
            "job_id": row.job_id,
            "source_file_import_id": row.source_file_import_id,
            "content_version": row.content_version,
            "status": row.status,
            "snapshot_id": row.snapshot_id,
            "import_id": row.import_id,
        }
        for row in _rows_by_ordinal(outcomes, require_zero_based=False)
    ]


def linkage_mapping_digest(outcomes: Iterable[Any]) -> str:
    """Digest the complete stable outcome-to-source linkage mapping."""

    return _collection_digest(
        "healthporta.ptg-wave.linkage-map.v1", _mapping_records(outcomes)
    )


def _validate_linkage_ack(
    wave: Any,
    outcomes: list[Any],
    ack: object,
    key: str | bytes | None,
) -> tuple[dict[str, Any], str]:
    if not isinstance(ack, dict):
        raise PTGWaveOutcomeConflict(
            "linkage acknowledgement must be an object"
        )
    expected_fields = {
        "schema_version",
        "wave_id",
        "wave_digest",
        "intent_count",
        "mapping_digest",
        "outcomes_digest",
        "signature",
    }
    if set(ack) != expected_fields or ack["schema_version"] != _LINKAGE_VERSION:
        raise PTGWaveOutcomeConflict(
            "linkage acknowledgement fields are not exact"
        )
    signature = _digest(ack["signature"], "linkage acknowledgement signature")
    unsigned_ack_map = {
        name: field_value
        for name, field_value in ack.items()
        if name != "signature"
    }
    expected_signature = sign_linkage_ack(
        unsigned_ack_map,
        key=_linkage_key(key),
    )
    if not hmac.compare_digest(signature, expected_signature):
        raise PTGWaveOutcomeConflict(
            "linkage acknowledgement signature is invalid"
        )
    if (
        ack["wave_id"] != wave.wave_id
        or ack["wave_digest"] != wave.wave_digest
        or ack["intent_count"] != wave.intent_count
        or ack["outcomes_digest"] != wave.outcomes_digest
    ):
        raise PTGWaveOutcomeConflict(
            "linkage acknowledgement does not bind this exact wave"
        )
    mapping_digest = linkage_mapping_digest(outcomes)
    if ack["mapping_digest"] != mapping_digest:
        raise PTGWaveOutcomeConflict(
            "linkage acknowledgement does not cover every exact outcome"
        )
    return ack, sha256_digest(canonical_json(ack))
