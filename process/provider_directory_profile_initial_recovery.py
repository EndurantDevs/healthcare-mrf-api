# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Revalidate authentic initial targets restored by an existing complete recovery."""

import json
from collections.abc import Mapping

from process import provider_directory_profile_initial_contract as contract


def _require(condition):
    if not condition:
        raise RuntimeError("provider_directory_profile_initial_recovery_changed")


def _reference(witness):
    _require(
        isinstance(witness, Mapping)
        and set(witness) == {"initial_targets", "initial_receipt", "recovery_id", "recovered_event_sha256"}
    )
    original = witness["initial_targets"]
    _require(isinstance(original, Mapping) and original.get("contract_id") == contract.TARGET_CONTRACT)
    original = contract.validated_target_state(original)
    return contract.validated_recovered_publication(
        {
            "initial_targets": original,
            "initial_receipt": witness["initial_receipt"],
            "recovery_id": witness["recovery_id"],
            "recovered_event_sha256": witness["recovered_event_sha256"],
            "original_target_state_sha256": contract.target_state_sha256(original),
        }
    )


async def require_recovered_targets(fhir, schema):
    """The existing journal provider must prove the full current inverse before admission."""
    from process.provider_directory_serving_recovery import recovered_initial_targets

    witness = await recovered_initial_targets(fhir, schema)
    if witness is None:
        raise RuntimeError("provider_directory_profile_initial_receipt_exists")
    _reference(witness)
    return json.loads(contract.canonical_json(witness))


def recovered_target_state(current, witness):
    """Keep original v1 history while honestly recording retained receipts and current sizes."""
    reference = _reference(witness)
    original = reference["initial_targets"]
    _require(
        {key: value for key, value in current.items() if key not in {"evidence_target_bytes", "profile_target_bytes"}}
        == {
            key: value
            for key, value in original.items()
            if key not in {"evidence_target_bytes", "profile_target_bytes"}
        }
    )
    return contract.validated_target_state(
        {
            **current,
            "contract_id": contract.RECOVERED_TARGET_CONTRACT,
            "initial_commit_receipt_absent": False,
            "recovered_initial_publication": reference,
        }
    )


def assert_recovered_binding(admitted_targets, witness):
    """Recheck the same terminal inverse at the existing locked cutover boundary."""
    target = contract.validated_target_state(admitted_targets)
    if target["contract_id"] == contract.TARGET_CONTRACT:
        _require(witness is None)
    else:
        _require(witness is not None and target["recovered_initial_publication"] == _reference(witness))


async def capture_original_history(fhir, schema, profile_ref, evidence_ref, witness):
    """Read the exact original terminal run; a newer rolled-back publication cannot replace it."""
    original = _reference(witness)["initial_targets"]
    history = original["historical_publication"]
    if history is None:
        _require(
            not await fhir.db.scalar(
                f"SELECT EXISTS (SELECT 1 FROM {profile_ref}) OR EXISTS (SELECT 1 FROM {evidence_ref})"
            )
        )
        return None, 0, 0
    history_row = await fhir.db.first(
        f"SELECT run_id,finished_at,metrics::jsonb -> :key AS result FROM {fhir._unscoped_qt(schema, fhir.ImportRun.__tablename__)} "
        "WHERE run_id=:run AND importer='provider-directory-fhir' AND status='succeeded'",
        key=fhir.PROFILE_SELECTION_RESULT_METRIC,
        run=history["run_id"],
    )
    _require(history_row is not None)
    history_by_field = fhir._pagination_checkpoint_row_mapping(history_row)
    _require(history_by_field["run_id"] == history["run_id"] and history_by_field.get("finished_at") is not None)
    result_by_field = history_by_field["result"]
    if isinstance(result_by_field, str):
        result_by_field = json.loads(result_by_field)
    result_by_field = contract.validated_legacy_result(result_by_field)
    _require(
        result_by_field == history["result"] and contract.result_sha256(result_by_field) == history["result_sha256"]
    )
    await fhir._profile_adoption_targets(
        schema,
        generation_id=result_by_field["profile_generation_id"],
        profile_rows=original["profile_rows"],
        evidence_rows=original["evidence_rows"],
    )
    _require(
        not await fhir.db.scalar(
            f"SELECT EXISTS (SELECT 1 FROM {profile_ref} WHERE generation_id <> :generation)",
            generation=result_by_field["profile_generation_id"],
        )
    )
    return history, original["evidence_rows"], original["profile_rows"]
