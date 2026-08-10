# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Restart-safe identities shared by Practitioner acquisition and admission."""

from __future__ import annotations

from datetime import date
import hashlib
import re

from process.uhc_flex_practitioner_store_contract import COHORT_PATTERN
from process.uhc_flex_practitioner_store_contract import HASH_PATTERN
from process.uhc_flex_practitioner_store_contract import INTENT_PATTERN


UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-twin-attempt.v1"
)
UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-matched-admission.v1"
)
UHC_FLEX_PRACTITIONER_DATASET_INTENT_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-intent.v1"
)
UHC_FLEX_PRACTITIONER_RUN_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-acquisition-run.v1"
)

ATTEMPT_PATTERN = re.compile(r"pdufpta_[0-9a-f]{48}\Z")
ADMISSION_PATTERN = re.compile(r"pdufpad_[0-9a-f]{48}\Z")


def digest_identifier(prefix: str, identity_fields: tuple[object, ...]) -> str:
    """Hash one ordered identity tuple into a bounded storage namespace."""

    digest = hashlib.sha256(
        "\x1f".join(str(field) for field in identity_fields).encode("utf-8")
    ).hexdigest()
    return prefix + digest[:48]


def canonical_semantic_projection_as_of(candidate: object) -> str:
    """Require one finite canonical date shared by semantic materialization."""

    if (
        type(candidate) is not str
        or len(candidate) != 10
        or candidate != candidate.strip()
    ):
        raise ValueError("Flex Practitioner semantic projection date is invalid")
    try:
        projection_date = date.fromisoformat(candidate)
    except ValueError:
        raise ValueError(
            "Flex Practitioner semantic projection date is invalid"
        ) from None
    if projection_date.isoformat() != candidate:
        raise ValueError("Flex Practitioner semantic projection date is invalid")
    return candidate


def practitioner_dataset_intent_id(
    cohort_id: str,
    semantic_projection_as_of: str,
    operation_key: str,
) -> str:
    """Derive the restart-safe dataset intent for one dated operation."""

    if type(cohort_id) is not str or COHORT_PATTERN.fullmatch(cohort_id) is None:
        raise ValueError("Flex Practitioner cohort ID is invalid")
    projection_date = canonical_semantic_projection_as_of(
        semantic_projection_as_of
    )
    if type(operation_key) is not str or HASH_PATTERN.fullmatch(operation_key) is None:
        raise ValueError("Flex Practitioner operation key is invalid")
    return digest_identifier(
        "pdufdi_",
        (
            UHC_FLEX_PRACTITIONER_DATASET_INTENT_DOMAIN,
            cohort_id,
            projection_date,
            operation_key,
        ),
    )


build_uhc_flex_practitioner_dataset_intent_id = practitioner_dataset_intent_id


def build_uhc_flex_practitioner_run_id(
    dataset_intent_id: str,
    acquisition_role: str,
) -> str:
    """Derive one role-specific run ID from an immutable dataset intent."""

    if (
        type(dataset_intent_id) is not str
        or INTENT_PATTERN.fullmatch(dataset_intent_id) is None
        or acquisition_role not in {"baseline", "candidate"}
    ):
        raise ValueError("Flex Practitioner run identity is invalid")
    return digest_identifier(
        "pdufpr_",
        (
            UHC_FLEX_PRACTITIONER_RUN_DOMAIN,
            dataset_intent_id,
            acquisition_role,
        ),
    )


__all__ = (
    "build_uhc_flex_practitioner_dataset_intent_id",
    "build_uhc_flex_practitioner_run_id",
    "canonical_semantic_projection_as_of",
    "ADMISSION_PATTERN",
    "ATTEMPT_PATTERN",
    "UHC_FLEX_PRACTITIONER_DATASET_INTENT_DOMAIN",
    "UHC_FLEX_PRACTITIONER_RUN_DOMAIN",
    "UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID",
)
