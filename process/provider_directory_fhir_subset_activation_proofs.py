# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-root proof binding helpers for reviewed subset activation."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_fhir_subset_activation_contract import (
    ReviewedSubsetActivationError,
    _text,
)


def _root_neutral_proof(verification_by_field: object) -> dict | None:
    if not isinstance(verification_by_field, Mapping):
        return None
    proof_by_field = verification_by_field.get("proof")
    if not isinstance(proof_by_field, Mapping):
        return None
    return {
        field_name: field_value
        for field_name, field_value in proof_by_field.items()
        if field_name != "acquisition_root_run_id"
    }


def validate_candidate_baseline_binding(
    baseline: Mapping[str, Any],
    baseline_metadata: Mapping[str, Any],
    candidate_metadata: Mapping[str, Any],
) -> None:
    """Require candidate and baseline to carry one neutral twin proof."""

    baseline_verification = baseline_metadata.get("twin_root_verification_v1")
    candidate_verification = candidate_metadata.get("twin_root_verification_v1")
    baseline_dataset_id = _text(baseline.get("dataset_id"))
    baseline_root_run_id = _text(baseline.get("acquisition_root_run_id"))
    if (
        not isinstance(candidate_verification, Mapping)
        or baseline_dataset_id is None
        or baseline_root_run_id is None
        or candidate_metadata.get("verification_baseline_dataset_id")
        != baseline_dataset_id
        or candidate_verification.get("baseline_dataset_id")
        != baseline_dataset_id
        or candidate_verification.get("baseline_acquisition_root_run_id")
        != baseline_root_run_id
        or _root_neutral_proof(baseline_verification) is None
        or _root_neutral_proof(baseline_verification)
        != _root_neutral_proof(candidate_verification)
    ):
        raise ReviewedSubsetActivationError("evidence")
