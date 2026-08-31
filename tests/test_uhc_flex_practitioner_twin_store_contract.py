# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import fields
from datetime import datetime
from datetime import timezone
import hashlib

import pytest

from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_store_contract import (
    UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    build_uhc_flex_practitioner_dataset_intent_id,
    build_uhc_flex_practitioner_run_id,
    build_uhc_flex_practitioner_twin_admission,
    build_uhc_flex_practitioner_twin_attempt,
    canonical_semantic_projection_as_of,
    uhc_flex_practitioner_twin_admission_id,
    uhc_flex_practitioner_twin_attempt_id,
    UHCFlexPractitionerSealedRoot,
    UHCFlexPractitionerTwinAdmission,
    UHCFlexPractitionerTwinAttempt,
    UHCFlexPractitionerTwinStoreError,
    UHC_FLEX_PRACTITIONER_DATASET_INTENT_DOMAIN,
    UHC_FLEX_PRACTITIONER_RUN_DOMAIN,
)


COHORT_ID = "pdufc_" + "1" * 48
PROJECTION_DATE = "2026-08-10"
OPERATION_KEY = "2" * 64
TIMESTAMP = datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc)


def _root(
    role: str,
    *,
    terminal_set_sha256: str = "3" * 64,
    resource_count: int = 2,
    cohort_id: str = COHORT_ID,
    error_count: int = 0,
    cohort_complete: bool = True,
) -> UHCFlexPractitionerSealedRoot:
    intent_id = build_uhc_flex_practitioner_dataset_intent_id(
        COHORT_ID,
        PROJECTION_DATE,
        OPERATION_KEY,
    )
    return UHCFlexPractitionerSealedRoot(
        acquisition_id=("pdufpa_" + ("4" if role == "baseline" else "5") * 48),
        cohort_id=cohort_id,
        acquisition_role=role,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        connector_id=UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
        query_contract_id=UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
        storage_contract_id=UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
        run_id=build_uhc_flex_practitioner_run_id(intent_id, role),
        dataset_intent_id=intent_id,
        expected_npi_count=2,
        resource_count=resource_count,
        terminal_set_sha256=terminal_set_sha256,
        error_count=error_count,
        cohort_complete=cohort_complete,
    )


def test_sealed_root_defaults_exact_and_carries_reviewed_partial_state() -> None:
    exact = _root("candidate")
    partial = _root(
        "candidate",
        error_count=1,
        cohort_complete=False,
    )

    assert (exact.error_count, exact.cohort_complete) == (0, True)
    assert (partial.error_count, partial.cohort_complete) == (1, False)
    with pytest.raises(ValueError):
        _root("candidate", error_count=1)
    with pytest.raises(ValueError):
        _root("candidate", error_count=3, cohort_complete=False)


def test_twin_attempt_rejects_partial_roots() -> None:
    with pytest.raises(UHCFlexPractitionerTwinStoreError) as error:
        build_uhc_flex_practitioner_twin_attempt(
            _root("baseline"),
            _root("candidate", error_count=1, cohort_complete=False),
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key=OPERATION_KEY,
            attempted_at=TIMESTAMP,
        )

    assert error.value.code == "state"


def test_restart_safe_intent_and_role_run_ids_match_contract_bytes() -> None:
    intent_id = build_uhc_flex_practitioner_dataset_intent_id(
        COHORT_ID,
        PROJECTION_DATE,
        OPERATION_KEY,
    )
    expected_intent = hashlib.sha256(
        "\x1f".join(
            (
                UHC_FLEX_PRACTITIONER_DATASET_INTENT_DOMAIN,
                COHORT_ID,
                PROJECTION_DATE,
                OPERATION_KEY,
            )
        ).encode("utf-8")
    ).hexdigest()
    assert intent_id == "pdufdi_" + expected_intent[:48]
    assert build_uhc_flex_practitioner_run_id(intent_id, "baseline") == (
        "pdufpr_"
        + hashlib.sha256(
            "\x1f".join(
                (UHC_FLEX_PRACTITIONER_RUN_DOMAIN, intent_id, "baseline")
            ).encode("utf-8")
        ).hexdigest()[:48]
    )
    assert build_uhc_flex_practitioner_run_id(
        intent_id,
        "baseline",
    ) != build_uhc_flex_practitioner_run_id(intent_id, "candidate")


@pytest.mark.parametrize(
    "projection_date",
    ("2026-8-10", "2026-02-30", "infinity", " 2026-08-10"),
)
def test_projection_date_is_canonical_and_finite(projection_date: str) -> None:
    with pytest.raises(ValueError):
        canonical_semantic_projection_as_of(projection_date)


def test_twin_and_admission_ids_bind_date_operation_and_exact_roots() -> None:
    baseline = _root("baseline")
    candidate = _root("candidate")
    attempt = build_uhc_flex_practitioner_twin_attempt(
        baseline,
        candidate,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        attempted_at=TIMESTAMP,
    )
    admission = build_uhc_flex_practitioner_twin_admission(
        attempt,
        admitted_at=TIMESTAMP,
    )

    assert attempt.matched is True
    assert attempt.attempt_id == uhc_flex_practitioner_twin_attempt_id(
        baseline,
        candidate,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
    )
    assert admission.admission_id == uhc_flex_practitioner_twin_admission_id(
        attempt
    )
    assert admission.candidate_acquisition_id == candidate.acquisition_id
    assert admission.publication_authority is True
    assert "npi" not in {field.name for field in fields(type(attempt))}
    assert "payload" not in {field.name for field in fields(type(admission))}


@pytest.mark.parametrize(
    ("terminal_set_sha256", "resource_count"),
    (("6" * 64, 2), ("3" * 64, 1)),
)
def test_mismatch_is_terminal_root_or_resource_count_inequality(
    terminal_set_sha256: str,
    resource_count: int,
) -> None:
    attempt = build_uhc_flex_practitioner_twin_attempt(
        _root("baseline"),
        _root(
            "candidate",
            terminal_set_sha256=terminal_set_sha256,
            resource_count=resource_count,
        ),
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        attempted_at=TIMESTAMP,
    )
    assert attempt.matched is False
    with pytest.raises(UHCFlexPractitionerTwinStoreError) as error:
        build_uhc_flex_practitioner_twin_admission(
            attempt,
            admitted_at=TIMESTAMP,
        )
    assert error.value.code == "mismatch"


def test_pair_rejects_wrong_role_identity_and_projection_binding() -> None:
    with pytest.raises(UHCFlexPractitionerTwinStoreError) as role_error:
        build_uhc_flex_practitioner_twin_attempt(
            _root("candidate"),
            _root("baseline"),
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key=OPERATION_KEY,
            attempted_at=TIMESTAMP,
        )
    assert role_error.value.code == "identity"

    with pytest.raises(UHCFlexPractitionerTwinStoreError) as date_error:
        build_uhc_flex_practitioner_twin_attempt(
            _root("baseline"),
            _root("candidate"),
            semantic_projection_as_of="2026-08-11",
            operation_key=OPERATION_KEY,
            attempted_at=TIMESTAMP,
        )
    assert date_error.value.code == "identity"


def test_bounded_result_types_reject_forged_match_and_authority() -> None:
    attempt = build_uhc_flex_practitioner_twin_attempt(
        _root("baseline"),
        _root("candidate"),
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        attempted_at=TIMESTAMP,
    )
    attempt_by_field = {
        field.name: getattr(attempt, field.name) for field in fields(type(attempt))
    }
    attempt_by_field["matched"] = False
    with pytest.raises(ValueError):
        UHCFlexPractitionerTwinAttempt(**attempt_by_field)

    admission = build_uhc_flex_practitioner_twin_admission(
        attempt,
        admitted_at=TIMESTAMP,
    )
    admission_by_field = {
        field.name: getattr(admission, field.name)
        for field in fields(type(admission))
    }
    admission_by_field["publication_authority"] = False
    with pytest.raises(ValueError):
        UHCFlexPractitionerTwinAdmission(**admission_by_field)
