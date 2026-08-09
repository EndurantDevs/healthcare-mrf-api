# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit reviewed-root policy verification tests."""

from __future__ import annotations

import dataclasses
import importlib

import pytest

from tests import test_provider_directory_twin_root_verification as shared
from tests.provider_directory_fhir_subset_completion_support import (
    build_subset_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _single_root_candidate() -> importer.EndpointDatasetCandidate:
    contract = build_subset_contract()
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id="dataset_candidate",
        acquisition_root_run_id="root_candidate",
        source_ids=(contract.source_id,),
        selected_resources=contract.resources,
        expected_resources=contract.resources,
        import_run_id="run_candidate",
        previous_dataset_id=None,
        requires_twin_root_verification=False,
        reviewed_root_policy=importer.ReviewedRootPolicy(1),
        verification_campaign_id=contract.campaign_id,
        verification_source_scope_hash="a" * 64,
        completion_proof_required_version=3,
        subset_contract=contract,
    )


def test_single_root_policy_validates_without_twin_evidence():
    candidate = _single_root_candidate()

    importer._assert_candidate_reviewed_root_policy(candidate)
    candidate_metadata_by_field = (
        importer._endpoint_dataset_candidate_metadata(candidate)
    )
    assert all(
        field_name not in candidate_metadata_by_field
        for field_name in (
            importer.TWIN_ROOT_VERIFICATION_ROLE_KEY,
            importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY,
            importer.TWIN_ROOT_VERIFICATION_METADATA_KEY,
        )
    )
    assert importer._twin_root_verification_decision(
        candidate,
        shared._content_proof(),
        None,
    ) == (importer.ENDPOINT_DATASET_VALIDATED, {}, [])


@pytest.mark.parametrize(
    "candidate",
    (
        dataclasses.replace(
            _single_root_candidate(),
            requires_twin_root_verification=True,
        ),
        dataclasses.replace(
            _single_root_candidate(),
            acquisition_root_run_id=None,
        ),
        dataclasses.replace(
            _single_root_candidate(),
            verification_role=importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
        ),
        dataclasses.replace(
            _single_root_candidate(),
            completion_proof_required_version=None,
        ),
    ),
)
def test_single_root_policy_rejects_candidate_contract_drift(candidate):
    with pytest.raises(RuntimeError, match="reviewed_root_policy"):
        importer._assert_candidate_reviewed_root_policy(candidate)


def test_single_root_policy_rejects_twin_decision_inputs():
    candidate = _single_root_candidate()

    with pytest.raises(RuntimeError, match="reviewed_root_policy"):
        importer._twin_root_verification_decision(
            candidate,
            shared._content_proof(),
            shared._baseline_map(shared._candidate()),
        )
