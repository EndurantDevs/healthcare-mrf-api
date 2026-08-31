# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Partial and complete publication receipts for the Flex operator."""

from __future__ import annotations

import json
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from process import uhc_flex_practitioner_operator as operator
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from tests.test_uhc_flex_practitioner_operator import (
    CANDIDATE_ACQUISITION_ID,
    OPERATION_KEY,
    _disable_all_gates,
)


def _publication_phase_module() -> tuple[ModuleType, type, list[tuple]]:
    phase_module = ModuleType("process.uhc_flex_practitioner_publication")
    publication_calls: list[tuple] = []

    class PublicationResult:
        def __init__(self) -> None:
            self.replayed = True
            self.readiness = SimpleNamespace(
                admission_id="pdufpad_" + "1" * 48,
                candidate_acquisition_id=CANDIDATE_ACQUISITION_ID,
                acquisition_root_run_id="pdufpar_" + "6" * 48,
                cohort_complete=False,
                cohort_id="pdufc_" + "2" * 48,
                dataset_hash="3" * 64,
                dataset_id="pdufpd_" + "4" * 48,
                dataset_intent_id="pdufdi_" + "5" * 48,
                endpoint_collection_complete=False,
                endpoint_complete=False,
                operation_key=OPERATION_KEY,
                previous_dataset_id=None,
                resource_count=8,
                retry_exhausted_count=1,
                semantic_projection_as_of="2026-08-10",
                source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
            )

    async def publish(candidate_acquisition_id, **keyword_arguments):
        publication_calls.append((candidate_acquisition_id, keyword_arguments))
        return PublicationResult()

    phase_module.UHCFlexPractitionerPublicationResult = PublicationResult
    phase_module.publish_uhc_flex_practitioner_dataset = publish
    return phase_module, PublicationResult, publication_calls


def _assert_external_profile_followup(receipt_by_field: dict[str, Any]) -> None:
    """Prove the receipt remains dormant and matches the closed controller shape."""

    dispatch_by_field = receipt_by_field["profile_delta_dispatch"]
    assert dispatch_by_field["operator_command_available"] is False
    assert dispatch_by_field["required_external_global_dispatch"] is True
    assert dispatch_by_field["status"] == "not_dispatched"
    assert dispatch_by_field["external_followup_contract_id"] == (
        "healthporta.provider-directory.global-profile-followup.v1"
    )
    followup_by_field = dispatch_by_field["external_followup"]
    assert followup_by_field["source_id"] == UHC_FLEX_PRACTITIONER_SOURCE_ID
    assert followup_by_field["dataset_id"] == "pdufpd_" + "4" * 48
    assert followup_by_field["parent_run_id"] == "pdufpar_" + "6" * 48
    assert set(followup_by_field) == {
        "status",
        "kind",
        "intent",
        "importer",
        "source_id",
        "dataset_id",
        "parent_run_id",
        "idempotency_key",
        "triggered_by",
        "params",
    }
    assert followup_by_field["params"]["source_ids"] == []
    assert followup_by_field["params"]["require_complete_global_profile_fence"] is True


@pytest.mark.asyncio
async def test_publication_is_explicit_and_requires_profile_dispatch(
    monkeypatch,
) -> None:
    """Dataset publication remains separate from the unavailable dispatcher."""

    _disable_all_gates(monkeypatch)
    monkeypatch.setenv(operator.PUBLICATION_ENABLED_ENV, "true")
    phase_module, publication_result_type, publication_calls = (
        _publication_phase_module()
    )
    monkeypatch.setitem(__import__("sys").modules, phase_module.__name__, phase_module)
    database = object()

    rendered_receipt = await operator.publish_admitted_uhc_flex_practitioner_operation(
        candidate_acquisition_id=CANDIDATE_ACQUISITION_ID,
        batch_size=250,
        database=database,
    )
    receipt_by_field = json.loads(rendered_receipt)

    assert publication_calls == [
        (
            CANDIDATE_ACQUISITION_ID,
            {"batch_size": 250, "database": database},
        )
    ]
    assert receipt_by_field["status"] == "published"
    assert receipt_by_field["replayed"] is True
    assert receipt_by_field["retry_exhausted_count"] == 1
    assert receipt_by_field["cohort_complete"] is False
    _assert_external_profile_followup(receipt_by_field)

    exact = publication_result_type()
    exact.readiness.cohort_complete = True
    exact.readiness.retry_exhausted_count = 0
    exact_receipt = json.loads(operator._publication_result_json(exact))
    assert "retry_exhausted_count" not in exact_receipt
    _assert_external_profile_followup(exact_receipt)
