# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Partial-census receipt and persistence boundaries for one Flex root."""

from __future__ import annotations

from dataclasses import replace
import json

import pytest

from process import uhc_flex_practitioner_twin_store as twin_store
from process import uhc_flex_practitioner_twin_store_contract as twin_contract
from process.uhc_flex_practitioner_acquisition_contract import (
    UHCFlexPractitionerRootReceipt,
)
from process.uhc_flex_practitioner_single_root_contract import (
    build_single_root_admission,
    UHCFlexPractitionerSingleRootReceipt,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import cohort_fixture
from tests.test_uhc_flex_practitioner_twin_boundaries import (
    _Database,
    _single_root,
)
from tests.test_uhc_flex_practitioner_twin_store_contract import (
    OPERATION_KEY,
    PROJECTION_DATE,
    TIMESTAMP,
)


def test_reviewed_single_root_admits_and_receipts_partial_census() -> None:
    cohort = cohort_fixture()
    candidate = replace(
        _single_root(cohort.cohort_id, cohort.npi_count),
        error_count=1,
        cohort_complete=False,
    )
    admission = build_single_root_admission(
        candidate,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        admitted_at=TIMESTAMP,
    )
    candidate_receipt = UHCFlexPractitionerRootReceipt(
        acquisition_role="candidate",
        acquisition_id=candidate.acquisition_id,
        run_id=candidate.run_id,
        matched_count=cohort.npi_count - 1,
        unmatched_count=0,
        resource_count=candidate.resource_count,
        terminal_set_sha256=candidate.terminal_set_sha256,
        elapsed_seconds=1.0,
        error_count=1,
        cohort_complete=False,
    )
    receipt = UHCFlexPractitionerSingleRootReceipt(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of=PROJECTION_DATE,
        source_id=candidate.source_id,
        endpoint_id="0" * 64,
        cohort_id=cohort.cohort_id,
        official_dataset_id=cohort.official_dataset_id,
        official_dataset_hash=cohort.official_dataset_hash,
        official_content_proof_sha256=cohort.official_content_proof_sha256,
        dataset_intent_id=candidate.dataset_intent_id,
        expected_npi_count=cohort.npi_count,
        candidate=candidate_receipt,
        admission_id=admission.admission_id,
        reviewed_root_policy_json=admission.reviewed_root_policy_json,
        elapsed_seconds=1.0,
    )

    assert receipt.candidate.error_count == 1
    assert receipt.candidate.cohort_complete is False
    with pytest.raises(ValueError):
        replace(
            receipt,
            candidate=replace(
                candidate_receipt,
                error_count=0,
                cohort_complete=True,
            ),
        )


@pytest.mark.asyncio
async def test_single_root_store_normalizes_contract_value_errors(monkeypatch):
    cohort = cohort_fixture()
    candidate = _single_root(cohort.cohort_id, cohort.npi_count)
    admission = build_single_root_admission(
        candidate,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        admitted_at=TIMESTAMP,
    )

    async def lock_single_root(*_args, **_kwargs):
        return candidate

    monkeypatch.setattr(twin_store, "_lock_single_root", lock_single_root)
    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError) as admit_error:
        await twin_store.admit_uhc_flex_practitioner_single_root(
            candidate.acquisition_id,
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key="invalid",
            database=_Database(),
        )
    assert admit_error.value.code == "identity"

    with pytest.raises(twin_contract.UHCFlexPractitionerTwinStoreError) as read_error:
        await twin_store._rebuild_single_root_admission(
            _Database(), admission, PROJECTION_DATE, "invalid"
        )
    assert read_error.value.code == "identity"


@pytest.mark.asyncio
async def test_single_root_admission_serializes_jsonb_policy() -> None:
    cohort = cohort_fixture()
    admission = build_single_root_admission(
        _single_root(cohort.cohort_id, cohort.npi_count),
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY,
        admitted_at=TIMESTAMP,
    )
    database = _Database()

    await twin_store._insert_admission(database, admission)

    statement, parameters = database.status_calls[0]
    assert "CAST(:reviewed_root_policy_json AS jsonb)" in statement
    assert json.loads(parameters["reviewed_root_policy_json"]) == (
        admission.reviewed_root_policy_json
    )
