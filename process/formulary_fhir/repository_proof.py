# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-policy additions to immutable formulary membership proofs."""

from __future__ import annotations

import hashlib

from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.types import AlternativeCorrection
from process.formulary_fhir.types import MedicationRecord


CORRECTED_VARIANT_DOMAIN = "fhir-formulary-corrected-variant-v1"


def source_medication_variant_hash(
    medication: MedicationRecord,
    correction: AlternativeCorrection | None,
) -> str:
    """Bind optional source correction policy without changing legacy hashes."""

    base_variant_hash = medication_variant_hash(medication)
    if correction is None:
        return base_variant_hash
    if type(correction) is not AlternativeCorrection:
        raise ValueError("FHIR formulary alternative correction is invalid")
    proof_by_field = {
        "alternative_correction": {
            "prefix": correction.prefix,
            "rule_version": correction.rule_version,
        },
        "medication_variant_hash": base_variant_hash,
    }
    digest = hashlib.sha256()
    digest.update(CORRECTED_VARIANT_DOMAIN.encode("ascii"))
    digest.update(b"\n")
    digest.update(json_text(proof_by_field).encode("utf-8"))
    return digest.hexdigest()


__all__ = ("source_medication_variant_hash",)
