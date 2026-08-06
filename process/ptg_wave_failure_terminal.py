"""Pure terminal eligibility reducers for exact-wave failures."""

from __future__ import annotations

from typing import Any, Iterable

from process.ptg_wave_failure_kubernetes import _verify_failure_kubernetes
from process.ptg_wave_failure_receipts import (
    _require_claimed_prestart_failure_receipt,
    _require_unclaimed_failure_receipt,
)
from process.ptg_wave_failure_snapshots import _started_claim_ordinals
from process.ptg_wave_failure_types import (
    PTGWaveFailureConflict,
    _outcomes_digest,
    _require_mapping,
    _rows_by_ordinal,
)
from process.ptg_wave_failure_validation import (
    _verify_failure_redis,
    _verify_linkage,
)
from process.ptg_wave_state import canonical_json, sha256_digest


def verify_unclaimed_dead_letter_terminal_eligibility(
    wave: Any,
    intents: Iterable[Any],
    claims: Iterable[Any],
    outcomes: Iterable[Any],
    terminal_receipt: object,
    *,
    key: str | bytes | None = None,
) -> dict[str, Any]:
    """Reduce a zero-claim dead-letter wave to cleanup-eligible evidence."""

    ordered_intents = _rows_by_ordinal(intents)
    ordered_outcomes = _rows_by_ordinal(outcomes)
    if list(claims):
        raise PTGWaveFailureConflict(
            "failure terminal proof requires zero worker claims"
        )
    _require_exact_coverage(
        wave,
        ordered_intents,
        ordered_outcomes,
        "failure terminal proof must cover every admitted ordinal",
    )
    terminal_outcome_records = _dead_letter_records(
        ordered_intents,
        ordered_outcomes,
        "failure terminal outcome is not an exact dead letter",
    )
    if _outcomes_digest(terminal_outcome_records) != wave.outcomes_digest:
        raise PTGWaveFailureConflict(
            "persisted failure outcomes digest is corrupt"
        )
    failure_receipt = _require_unclaimed_failure_receipt(
        wave, wave.failure_receipt, require_origin_state=False
    )
    failure_digest = sha256_digest(canonical_json(failure_receipt))
    if failure_digest != wave.failure_receipt_digest:
        raise PTGWaveFailureConflict(
            "persisted failure receipt digest is corrupt"
        )
    _verify_linkage(wave, ordered_outcomes, key=key)
    kubernetes, redis = _terminal_receipt_evidence(
        wave,
        failure_receipt,
        terminal_receipt,
        receipt_name="failure terminal receipt",
        fields_error="failure terminal receipt fields are not exact",
    )
    return {
        "schema_version": "healthporta.ptg-wave.terminal.v1",
        "mode": "unclaimed_failure",
        "wave_digest": wave.wave_digest,
        "outcomes_digest": wave.outcomes_digest,
        "failure_receipt_digest": failure_digest,
        "linkage_ack_digest": wave.linkage_ack_digest,
        "kubernetes": kubernetes,
        "redis_pre_cleanup": redis,
    }


def verify_claimed_prestart_terminal_eligibility(
    wave: Any,
    intents: Iterable[Any],
    claims: Iterable[Any],
    outcomes: Iterable[Any],
    terminal_receipt: object,
    *,
    key: str | bytes | None = None,
) -> dict[str, Any]:
    """Reduce the claim-commit/import-start crash into cleanup proof."""

    ordered_intents = _rows_by_ordinal(intents)
    ordered_outcomes = _rows_by_ordinal(outcomes)
    claim_rows = list(claims)
    _require_exact_coverage(
        wave,
        ordered_intents,
        ordered_outcomes,
        "claimed-prestart terminal proof must cover every admitted ordinal",
    )
    claimed_ordinals = _started_claim_ordinals(
        wave, ordered_intents, claim_rows
    )
    terminal_outcome_records = _dead_letter_records(
        ordered_intents,
        ordered_outcomes,
        "claimed-prestart terminal outcome is not an exact dead letter",
    )
    if _outcomes_digest(terminal_outcome_records) != wave.outcomes_digest:
        raise PTGWaveFailureConflict(
            "persisted claimed-prestart outcomes digest is corrupt"
        )
    failure_receipt, failure_digest = _claimed_failure_receipt(
        wave, claimed_ordinals
    )
    _verify_linkage(wave, ordered_outcomes, key=key)
    kubernetes, redis = _terminal_receipt_evidence(
        wave,
        failure_receipt,
        terminal_receipt,
        receipt_name="claimed-prestart terminal receipt",
        fields_error="claimed-prestart terminal receipt fields are not exact",
    )
    return {
        "schema_version": "healthporta.ptg-wave.terminal.v1",
        "mode": "claimed_prestart_failure",
        "wave_digest": wave.wave_digest,
        "outcomes_digest": wave.outcomes_digest,
        "failure_receipt_digest": failure_digest,
        "claimed_ordinals": claimed_ordinals,
        "claimed_ordinals_digest": failure_receipt["claimed_ordinals_digest"],
        "linkage_ack_digest": wave.linkage_ack_digest,
        "kubernetes": kubernetes,
        "redis_pre_cleanup": redis,
    }


def _require_exact_coverage(
    wave: Any,
    intents: list[Any],
    outcomes: list[Any],
    error_message: str,
) -> None:
    if len(intents) != wave.intent_count or len(outcomes) != wave.intent_count:
        raise PTGWaveFailureConflict(error_message)


def _dead_letter_records(
    intents: list[Any],
    outcomes: list[Any],
    error_message: str,
) -> list[dict[str, Any]]:
    terminal_outcome_records: list[dict[str, Any]] = []
    for intent, outcome in zip(intents, outcomes):
        if (
            outcome.ordinal != intent.ordinal
            or outcome.run_id != intent.run_id
            or outcome.job_id != intent.job_id
            or outcome.source_file_import_id != intent.source_file_import_id
            or outcome.content_version != intent.content_version
            or outcome.status != "dead_letter"
            or outcome.snapshot_id is not None
            or outcome.import_id is not None
        ):
            raise PTGWaveFailureConflict(error_message)
        terminal_outcome_records.append(
            {
                "ordinal": outcome.ordinal,
                "run_id": outcome.run_id,
                "job_id": outcome.job_id,
                "source_file_import_id": outcome.source_file_import_id,
                "content_version": outcome.content_version,
                "status": outcome.status,
                "snapshot_id": outcome.snapshot_id,
                "import_id": outcome.import_id,
            }
        )
    return terminal_outcome_records


def _claimed_failure_receipt(
    wave: Any, claimed_ordinals: list[int]
) -> tuple[dict[str, Any], str]:
    failure_receipt = _require_claimed_prestart_failure_receipt(
        wave, wave.failure_receipt, require_origin_state=False
    )
    if claimed_ordinals != failure_receipt["claimed_ordinals"]:
        raise PTGWaveFailureConflict(
            "claimed-prestart claims differ from the immutable failure receipt"
        )
    failure_digest = sha256_digest(canonical_json(failure_receipt))
    if failure_digest != wave.failure_receipt_digest:
        raise PTGWaveFailureConflict(
            "persisted claimed-prestart failure receipt digest is corrupt"
        )
    return failure_receipt, failure_digest


def _terminal_receipt_evidence(
    wave: Any,
    failure_receipt: dict[str, Any],
    terminal_receipt: object,
    *,
    receipt_name: str,
    fields_error: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    receipt_map = _require_mapping(terminal_receipt, receipt_name)
    if set(receipt_map) != {"kubernetes", "redis"}:
        raise PTGWaveFailureConflict(fields_error)
    kubernetes_evidence = _verify_failure_kubernetes(
        wave, failure_receipt, receipt_map["kubernetes"]
    )
    redis_evidence = _verify_failure_redis(
        wave, failure_receipt, receipt_map["redis"]
    )
    return kubernetes_evidence, redis_evidence


verify_claimed_prestart_dead_letter_terminal_eligibility = (
    verify_claimed_prestart_terminal_eligibility
)
