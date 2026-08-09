# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused boundary coverage for reviewed-root policy contracts."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
import dataclasses
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_contract as contract
from process import provider_directory_fhir_subset_activation_evidence as evidence_api
from process import provider_directory_fhir_subset_activation_selection as selection
from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
    ReviewedRootPolicy,
    reviewed_root_policy_for_status,
)
from tests import test_provider_directory_reviewed_root_verification as reviewed
from tests import test_provider_directory_twin_root_verification as twin
from tests.provider_directory_fhir_subset_activation_support import (
    activation_inputs,
    single_root_activation_inputs,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _policy_two_candidate():
    return dataclasses.replace(
        twin._candidate(
            verification_role=importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
        ),
        reviewed_root_policy=ReviewedRootPolicy(2),
        completion_proof_required_version=3,
    )


def test_policy_status_and_source_profile_boundaries():
    policy_document = ReviewedRootPolicy(1).document()
    with pytest.raises(ValueError, match="policy_required"):
        reviewed_root_policy_for_status({}, POLICY_PENDING_STATUS)
    with pytest.raises(ValueError, match="status_invalid"):
        reviewed_root_policy_for_status({}, "unreviewed-status")

    with pytest.raises(RuntimeError, match="status_required"):
        importer._reviewed_source_profile_key(
            {"metadata_json": {REVIEWED_ROOT_POLICY_METADATA_KEY: policy_document}}
        )
    with pytest.raises(RuntimeError, match="scope_invalid"):
        importer._reviewed_source_profile_key(
            {
                "metadata_json": {
                    "provider_directory_candidate_status": (
                        importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING
                    ),
                    importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY: (
                        "campaign-synthetic"
                    ),
                    REVIEWED_ROOT_POLICY_METADATA_KEY: policy_document,
                }
            }
        )


def test_artifact_policy_sql_and_subset_identity_boundaries():
    with pytest.raises(ValueError, match="root_policy_ambiguous"):
        importer._artifact_source_policy_sql("source", "dataset", 1, True)
    assert importer._artifact_source_policy_sql(
        "source", "dataset", None, False
    ) == ""
    with pytest.raises(ValueError, match="root_policy_ambiguous"):
        importer._artifact_baseline_policy_sql("baseline", 2, True)

    source_record, _dataset_rows, _evidence = single_root_activation_inputs()
    metadata_keys = importer._source_contract_metadata_keys(
        source_record["metadata_json"], artifact=False
    )
    assert REVIEWED_ROOT_POLICY_METADATA_KEY in metadata_keys


def test_artifact_source_and_dataset_policy_parsing():
    policy_one = ReviewedRootPolicy(1).document()
    policy_two = ReviewedRootPolicy(2).document()
    source_by_field = {
        "metadata_json": {REVIEWED_ROOT_POLICY_METADATA_KEY: policy_one}
    }

    with pytest.raises(RuntimeError, match="policy_changed"):
        importer._artifact_reviewed_root_policy(source_by_field, {}, "dataset-a")
    with pytest.raises(RuntimeError, match="policy_invalid"):
        importer._artifact_reviewed_root_policy(
            {"metadata_json": {REVIEWED_ROOT_POLICY_METADATA_KEY: {}}},
            {REVIEWED_ROOT_POLICY_METADATA_KEY: {}},
            "dataset-a",
        )
    assert importer._artifact_reviewed_root_policy(
        source_by_field,
        {REVIEWED_ROOT_POLICY_METADATA_KEY: policy_one},
        "dataset-a",
    ) == ReviewedRootPolicy(1)
    with pytest.raises(RuntimeError, match="policy_changed"):
        importer._artifact_reviewed_root_policy(
            source_by_field,
            {REVIEWED_ROOT_POLICY_METADATA_KEY: policy_two},
            "dataset-a",
        )

    assert importer._artifact_dataset_root_policy({}, "dataset-a") is None
    assert importer._artifact_dataset_root_policy(
        {REVIEWED_ROOT_POLICY_METADATA_KEY: policy_one}, "dataset-a"
    ) == ReviewedRootPolicy(1)
    with pytest.raises(RuntimeError, match="policy_invalid"):
        importer._artifact_dataset_root_policy(
            {REVIEWED_ROOT_POLICY_METADATA_KEY: {}}, "dataset-a"
        )


def test_candidate_policy_metadata_branches():
    single_candidate = reviewed._single_root_candidate()
    policy_two_candidate = _policy_two_candidate()

    single_orphan_metadata = importer._empty_orphan_expected_metadata(
        single_candidate
    )
    assert importer.TWIN_ROOT_VERIFICATION_ROLE_KEY not in single_orphan_metadata
    twin_orphan_metadata = importer._empty_orphan_expected_metadata(
        policy_two_candidate
    )
    assert importer.TWIN_ROOT_VERIFICATION_ROLE_KEY in twin_orphan_metadata
    assert importer.TWIN_ROOT_VERIFICATION_ROLE_KEY in (
        importer._endpoint_dataset_candidate_metadata(policy_two_candidate)
    )


def test_single_root_orphan_rejects_forbidden_twin_fields():
    candidate = reviewed._single_root_candidate()
    metadata_by_field = importer._empty_orphan_expected_metadata(candidate)
    metadata_by_field[importer.RESOURCE_HASH_CONTRACT_METADATA_KEY] = (
        candidate.resource_hash_contract
    )
    existing_candidate_by_field = {
        "endpoint_id": candidate.endpoint_id,
        "status": importer.ENDPOINT_DATASET_ACQUIRING,
        "publication_metadata_json": metadata_by_field,
    }
    importer._assert_empty_orphan_candidate_identity(
        existing_candidate_by_field, candidate
    )
    metadata_by_field[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY] = None
    with pytest.raises(RuntimeError, match="orphan_identity_mismatch"):
        importer._assert_empty_orphan_candidate_identity(
            existing_candidate_by_field, candidate
        )


def test_persisted_policy_and_finalized_replay_boundaries():
    policy_one = ReviewedRootPolicy(1)
    with pytest.raises(RuntimeError, match="policy_invalid"):
        importer._assert_persisted_reviewed_root_policy(
            {REVIEWED_ROOT_POLICY_METADATA_KEY: None}, policy_one
        )
    with pytest.raises(RuntimeError, match="profile_mismatch"):
        importer._assert_persisted_reviewed_root_policy(
            {REVIEWED_ROOT_POLICY_METADATA_KEY: ReviewedRootPolicy(2).document()},
            policy_one,
        )

    single_candidate = reviewed._single_root_candidate()
    assert importer._finalized_replay_identity(
        single_candidate, {}, "validated"
    )["requires_twin_root_verification"] is False
    assert importer._policy_finalized_replay_identity(
        twin._candidate(), {}, "validated"
    ) == {}
    with pytest.raises(RuntimeError, match="identity_mismatch"):
        importer._policy_finalized_replay_identity(
            single_candidate,
            {importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: None},
            "validated",
        )
    assert importer.TWIN_ROOT_VERIFICATION_ROLE_KEY in (
        importer._policy_finalized_replay_identity(
            _policy_two_candidate(), {}, "validated"
        )
    )


@pytest.mark.asyncio
async def test_command_injects_policy_and_rejects_wrong_scope(monkeypatch):
    monkeypatch.setattr(
        importer, "_apply_provider_directory_refresh_preset", lambda fields: fields
    )
    monkeypatch.setattr(
        importer, "validate_uhc_official_file_admission", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(importer, "current_version_census_request", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(importer, "startup", AsyncMock())
    monkeypatch.setattr(importer, "shutdown", AsyncMock())
    process_data = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr(importer, "process_data", process_data)

    assert await importer.run_provider_directory_fhir_command(
        provider_directory_acquisition_strategy="server-issued-traversal-subset",
        provider_directory_reviewed_root_count=1,
    ) == {"ok": True}
    task_by_field = process_data.await_args.args[1]
    assert task_by_field["provider_directory_reviewed_root_policy"] == (
        ReviewedRootPolicy(1).document()
    )
    with pytest.raises(ValueError, match="policy_scope_invalid"):
        await importer.run_provider_directory_fhir_command(
            provider_directory_reviewed_root_count=1
        )


@pytest.mark.asyncio
async def test_process_rejects_policy_scope_and_control_run_drift(monkeypatch):
    monkeypatch.setattr(
        importer, "_apply_provider_directory_refresh_preset", lambda fields: fields
    )
    monkeypatch.setattr(
        importer, "validate_uhc_official_file_admission", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(importer, "current_version_census_request", lambda *_args, **_kwargs: None)
    ensure_database = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", ensure_database)

    with pytest.raises(ValueError, match="task_control_run_id_mismatch"):
        await importer.process_provider_directory_fhir_data(
            {"context": {"test_mode": True}, "control_run_id": "control-run"},
            {"test_mode": True, "run_id": "task-run"},
        )
    with pytest.raises(ValueError, match="policy_scope_invalid"):
        await importer.process_provider_directory_fhir_data(
            {"context": {"test_mode": True}},
            {
                "test_mode": True,
                "provider_directory_reviewed_root_policy": (
                    ReviewedRootPolicy(1).document()
                ),
            },
        )
    ensure_database.assert_not_awaited()


def test_activation_v2_marker_and_manifest_boundaries():
    source_record, dataset_rows, evidence = single_root_activation_inputs()
    selected = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    policy_two_selection = dataclasses.replace(
        selected,
        root_policy=ReviewedRootPolicy(2),
        baseline_dataset_id="dataset-baseline",
        baseline_root_run_id="root-baseline",
        baseline_replay_evidence_sha256="4" * 64,
        baseline_coverage_sha256="5" * 64,
    )
    assert "baseline" in policy_two_selection.metadata_marker()
    with pytest.raises(activation.ReviewedSubsetActivationError):
        contract._validated_manifest_document([])


def test_single_root_evidence_and_manifest_paths():
    source_record, dataset_rows, expected_evidence = single_root_activation_inputs()
    observed_evidence = evidence_api._completion_activation_evidence(
        source_record, dataset_rows, ReviewedRootPolicy(1)
    )
    assert observed_evidence == expected_evidence
    rendered_manifest = evidence_api.reviewed_subset_activation_verified_manifest_json(
        observed_evidence
    )
    assert '"root_policy"' in rendered_manifest
    predicate_sql, params_by_name = evidence_api._scope_policy_sql(
        ReviewedRootPolicy(1)
    )
    assert "root_policy" in predicate_sql
    assert params_by_name["root_policy_key"] == REVIEWED_ROOT_POLICY_METADATA_KEY


def test_completion_evidence_rejects_missing_single_or_twin_root(monkeypatch):
    source_record, dataset_rows, _evidence = single_root_activation_inputs()
    monkeypatch.setattr(
        importer, "_validated_parent_subset_completion_pair", lambda _row: None
    )
    with pytest.raises(activation.ReviewedSubsetActivationError):
        evidence_api._completion_activation_evidence(
            source_record, dataset_rows, ReviewedRootPolicy(1)
        )

    twin_source, twin_rows, _twin_evidence = activation_inputs()
    monkeypatch.setattr(selection, "_activation_roots", lambda *_args: (None, twin_rows[1]))
    with pytest.raises(activation.ReviewedSubsetActivationError):
        evidence_api._completion_activation_evidence(twin_source, twin_rows, None)


def test_derived_evidence_rejects_non_object_source_metadata():
    _source_record, dataset_rows, _evidence = activation_inputs()
    with pytest.raises(activation.ReviewedSubsetActivationError):
        evidence_api._derived_activation_evidence(
            [{"metadata_json": None}], dataset_rows, "synthetic-source"
        )


@asynccontextmanager
async def _read_only_transaction():
    yield


@pytest.mark.asyncio
@pytest.mark.parametrize("source_rows", ((), ({"metadata_json": None},)))
async def test_evidence_reread_rejects_source_cardinality_and_shape(
    monkeypatch, source_rows
):
    database = SimpleNamespace(
        transaction=lambda: _read_only_transaction(),
        status=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        evidence_api,
        "_initial_evidence_identity",
        AsyncMock(return_value=("endpoint-a", "campaign-a")),
    )
    monkeypatch.setattr(
        evidence_api,
        "_evidence_source_rows",
        AsyncMock(return_value=source_rows),
    )
    with pytest.raises(activation.ReviewedSubsetActivationError):
        await evidence_api._read_activation_evidence(database, "synthetic-source")


@pytest.mark.parametrize(
    "source_metadata",
    (
        {"provider_directory_candidate_status": "unexpected"},
        {
            "provider_directory_candidate_status": POLICY_PENDING_STATUS,
            REVIEWED_ROOT_POLICY_METADATA_KEY: {},
        },
    ),
)
def test_activation_selection_rejects_invalid_policy_shape(source_metadata):
    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._reviewed_root_policy(source_metadata)


def test_activation_source_rejects_evidence_policy_mismatch():
    source_record, _dataset_rows, evidence = single_root_activation_inputs()
    mismatched_evidence = dataclasses.replace(
        evidence, root_policy=ReviewedRootPolicy(2)
    )
    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._activation_source(
            [source_record], source_record["source_id"], mismatched_evidence
        )


@pytest.mark.parametrize("drift_kind", ("count", "lifecycle", "identity"))
def test_single_root_selection_rejects_root_shape_drift(drift_kind):
    _source_record, dataset_rows, _evidence = single_root_activation_inputs()
    drifted_rows = deepcopy(dataset_rows)
    if drift_kind == "count":
        drifted_rows = []
    elif drift_kind == "lifecycle":
        drifted_rows[0]["validated_at"] = None
    else:
        drifted_rows[0]["dataset_id"] = None
    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._activation_roots(drifted_rows, ReviewedRootPolicy(1))


def test_single_and_twin_root_proof_policy_boundaries():
    _source_record, single_rows, single_evidence = single_root_activation_inputs()
    single_candidate = deepcopy(single_rows[0])
    single_candidate["publication_metadata_json"][
        "verification_role"
    ] = None
    with pytest.raises(ValueError, match="single root proof"):
        selection._validated_single_root_proof(
            importer,
            single_candidate,
            single_evidence,
            ReviewedRootPolicy(1),
        )

    _twin_source, twin_rows, twin_evidence = activation_inputs()
    with pytest.raises(ValueError, match="root policy"):
        selection._validated_twin_root_proofs(
            importer,
            twin_rows[0],
            twin_rows[1],
            twin_evidence,
            ReviewedRootPolicy(2),
        )
    with pytest.raises(activation.ReviewedSubsetActivationError):
        selection._validated_root_proofs(
            None, twin_rows[1], twin_evidence, None
        )
