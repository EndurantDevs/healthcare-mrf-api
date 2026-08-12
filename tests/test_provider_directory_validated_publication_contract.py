import datetime as dt
import importlib
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from api import control_imports
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS,
    AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
    AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS,
    VALIDATED_PUBLICATION_NON_PROFILE_TARGETS,
    ValidatedPublicationCandidate,
    validated_publication_candidate_from_params,
    validated_publication_source_status,
)
from process.provider_directory_fhir_subset_completion import (
    SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
)
from tests.test_provider_directory_source_outcomes import (
    DATASET_HASH as CURRENT_DATASET_HASH,
    DATASET_ID as CURRENT_DATASET_ID,
    ENDPOINT_ID,
    ROOT_RUN_ID as CURRENT_ROOT_RUN_ID,
)


importer = importlib.import_module("process.provider_directory_fhir")


SOURCE_ID = "source-a"
CANDIDATE_ID = "dataset-candidate"
CANDIDATE_HASH = "b" * 64
CANDIDATE_ROOT = "root-candidate"
PROOF_HASH = "c" * 64
SCOPE_HASH = "d" * 64
CAMPAIGN_ID = "campaign-v1"
VALIDATED_AT = "2026-08-11T00:00:00+00:00"


def _candidate_payload(
    *,
    completion_proof_required_version=SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
    completion_proof_sha256=PROOF_HASH,
    first_publication=False,
):
    return {
        "source_id": SOURCE_ID,
        "endpoint_id": ENDPOINT_ID,
        "dataset_id": CANDIDATE_ID,
        "dataset_hash": CANDIDATE_HASH,
        "acquisition_root_run_id": CANDIDATE_ROOT,
        "validated_at": VALIDATED_AT,
        "automatic_publication_policy": AUTOMATIC_VALIDATED_PUBLICATION_POLICY,
        "completion_proof_required_version": completion_proof_required_version,
        "completion_proof_sha256": completion_proof_sha256,
        "verification_campaign_id": CAMPAIGN_ID,
        "verification_source_scope_sha256": SCOPE_HASH,
        "expected_current": (
            None
            if first_publication
            else {
                "endpoint_id": ENDPOINT_ID,
                "dataset_id": CURRENT_DATASET_ID,
                "dataset_hash": CURRENT_DATASET_HASH,
                "acquisition_root_run_id": CURRENT_ROOT_RUN_ID,
            }
        ),
    }


def _publication_params(**candidate_overrides):
    return {
        "source_ids": [SOURCE_ID],
        "publish_artifacts_only": True,
        "publish_corroboration": False,
        "publish_artifacts_targets": list(
            VALIDATED_PUBLICATION_NON_PROFILE_TARGETS
        ),
        "validated_publication_candidate": _candidate_payload(
            **candidate_overrides
        ),
    }


@pytest.mark.parametrize(
    ("proof_version", "proof_sha256", "expected_status"),
    (
        (
            SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
            PROOF_HASH,
            AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS,
        ),
        (None, None, AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS),
    ),
)
def test_policy_status_is_derived_from_exact_proof_pair(
    proof_version,
    proof_sha256,
    expected_status,
):
    payload = _candidate_payload(
        completion_proof_required_version=proof_version,
        completion_proof_sha256=proof_sha256,
    )

    candidate = ValidatedPublicationCandidate.from_payload(payload)

    assert candidate.to_payload() == payload
    assert validated_publication_source_status(candidate) == expected_status


@pytest.mark.parametrize(
    ("proof_version", "proof_sha256"),
    (
        (SERVER_ISSUED_SUBSET_REQUIRED_VERSION, None),
        (None, PROOF_HASH),
        (2, PROOF_HASH),
        (SERVER_ISSUED_SUBSET_REQUIRED_VERSION, "C" * 64),
    ),
)
def test_policy_rejects_every_other_proof_pair(
    proof_version,
    proof_sha256,
):
    with pytest.raises(ValueError, match="completion_proof_pair_invalid"):
        ValidatedPublicationCandidate.from_payload(
            _candidate_payload(
                completion_proof_required_version=proof_version,
                completion_proof_sha256=proof_sha256,
            )
        )


def test_request_contract_is_closed_and_first_publication_is_explicit():
    params = _publication_params()

    assert set(VALIDATED_PUBLICATION_NON_PROFILE_TARGETS) == (
        set(importer.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS)
        - {"corroboration", "profile"}
    )
    assert validated_publication_candidate_from_params(params).to_payload() == (
        _candidate_payload()
    )
    first_publication = _candidate_payload(first_publication=True)
    assert ValidatedPublicationCandidate.from_payload(
        first_publication
    ).to_payload() == first_publication

    invalid_payload = deepcopy(first_publication)
    invalid_payload["expected_current"] = {}
    with pytest.raises(ValueError, match="expected_current_schema_invalid"):
        ValidatedPublicationCandidate.from_payload(invalid_payload)
    invalid_payload = deepcopy(first_publication)
    invalid_payload.pop("expected_current")
    with pytest.raises(ValueError, match="schema_invalid"):
        ValidatedPublicationCandidate.from_payload(invalid_payload)

    params["validated_publication_candidate"]["unexpected"] = True
    with pytest.raises(ValueError, match="schema_invalid"):
        validated_publication_candidate_from_params(params)

    params = _publication_params()
    params["publish_artifacts_targets"].pop()
    with pytest.raises(ValueError, match="target_set_invalid"):
        validated_publication_candidate_from_params(params)

    params = _publication_params()
    params["source_id"] = SOURCE_ID
    with pytest.raises(ValueError, match="alias_invalid"):
        validated_publication_candidate_from_params(params)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    (
        ("source_id", " source-a"),
        ("dataset_hash", "B" * 64),
        ("validated_at", ""),
        ("validated_at", "not-a-timestamp"),
        ("validated_at", None),
        ("validated_at", dt.datetime(2026, 8, 11)),
    ),
)
def test_candidate_rejects_noncanonical_identity(
    field_name,
    field_value,
):
    payload = _candidate_payload()
    payload[field_name] = field_value

    with pytest.raises(ValueError, match="identity_invalid"):
        ValidatedPublicationCandidate.from_payload(payload)


@pytest.mark.parametrize(
    ("updates", "reason"),
    (
        ({"publish_artifacts_targets": ""}, "target_set_invalid"),
        ({"publish_artifacts_targets": None}, "target_set_invalid"),
        ({"source_ids": []}, "source_scope_invalid"),
        ({"publish_artifacts_only": False}, "publication_mode_invalid"),
        ({"publish_corroboration": True}, "corroboration_mode_invalid"),
        (
            {"provider_directory_profile_generation": 1},
            "profile_mode_invalid",
        ),
        ({"retry_of_run_id": "run-parent"}, "acquisition_mode_invalid"),
        ({"full_refresh": True}, "incompatible_mode"),
        ({"refresh_preset": "monthly-full"}, "preset_mode_invalid"),
    ),
)
def test_request_contract_rejects_incompatible_modes(updates, reason):
    params = _publication_params()
    params.update(updates)

    with pytest.raises(ValueError, match=reason):
        validated_publication_candidate_from_params(params)


@pytest.mark.asyncio
async def test_control_admission_rejects_a_mixed_proof_pair():
    with pytest.raises(ValueError, match="completion_proof_pair_invalid"):
        await control_imports.create_import_run(
            {
                "run_id": "run-invalid-candidate",
                "importer": "provider-directory-fhir",
                "params": _publication_params(
                    completion_proof_required_version=None,
                    completion_proof_sha256=PROOF_HASH,
                ),
            }
        )


def _validated_publication_fence(
    proof_version,
    proof_sha256,
    *,
    first_publication=False,
):
    candidate = ValidatedPublicationCandidate.from_payload(
        _candidate_payload(
            completion_proof_required_version=proof_version,
            completion_proof_sha256=proof_sha256,
            first_publication=first_publication,
        )
    )
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id=SOURCE_ID,
        endpoint_id=ENDPOINT_ID,
        dataset_id=CANDIDATE_ID,
        evidence_run_id=CANDIDATE_ROOT,
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        previous_dataset_id=(
            None if first_publication else CURRENT_DATASET_ID
        ),
        expected_incumbent_dataset_id=(
            None if first_publication else CURRENT_DATASET_ID
        ),
        promote_on_cutover=True,
        dataset_hash=CANDIDATE_HASH,
        validated_at=VALIDATED_AT,
        completion_proof_required_version=proof_version,
        completion_proof_sha256=proof_sha256,
        verification_source_status=validated_publication_source_status(
            candidate
        ),
        verification_campaign_id=CAMPAIGN_ID,
        verification_source_scope_hash=SCOPE_HASH,
        verification_source_ids=(SOURCE_ID,),
    )
    return candidate, importer.ProviderDirectoryArtifactDatasetFence(
        (dataset,),
        should_select_validated_candidates=True,
        validated_publication_candidate=candidate,
    )


@pytest.mark.parametrize(
    ("proof_version", "proof_sha256"),
    (
        (SERVER_ISSUED_SUBSET_REQUIRED_VERSION, PROOF_HASH),
        (None, None),
    ),
)
def test_locked_fence_rechecks_candidate_and_incumbent(
    proof_version,
    proof_sha256,
):
    _, fence = _validated_publication_fence(
        proof_version,
        proof_sha256,
    )
    candidate_row_map = {
        "dataset_id": CANDIDATE_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": CANDIDATE_ROOT,
        "status": importer.ENDPOINT_DATASET_VALIDATED,
        "is_current": False,
        "previous_dataset_id": CURRENT_DATASET_ID,
        "dataset_hash": CANDIDATE_HASH,
        "validated_at": VALIDATED_AT,
        "superseded_at": None,
        "completion_proof_required_version": proof_version,
        "completion_proof_sha256": proof_sha256,
        "locked_current_dataset_ids": [CURRENT_DATASET_ID],
    }
    incumbent_row_map = {
        "dataset_id": CURRENT_DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "dataset_hash": CURRENT_DATASET_HASH,
        "acquisition_root_run_id": CURRENT_ROOT_RUN_ID,
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "published_at": dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        "superseded_at": None,
        "locked_current_dataset_ids": [CURRENT_DATASET_ID],
    }

    importer._assert_locked_artifact_fence_datasets(
        fence,
        [candidate_row_map, incumbent_row_map],
        {ENDPOINT_ID: [CANDIDATE_ID]},
    )
    for field_name, drifted_value in (
        ("dataset_hash", "f" * 64),
        ("acquisition_root_run_id", "other-root"),
        ("status", "superseded"),
        ("is_current", False),
        ("published_at", None),
    ):
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="validated_publication_fence_changed",
        ):
            importer._assert_locked_artifact_fence_datasets(
                fence,
                [
                    candidate_row_map,
                    {**incumbent_row_map, field_name: drifted_value},
                ],
                {ENDPOINT_ID: [CANDIDATE_ID]},
            )


def test_first_publication_fence_rejects_any_current_dataset():
    _, fence = _validated_publication_fence(
        None,
        None,
        first_publication=True,
    )
    candidate_row_map = {
        "dataset_id": CANDIDATE_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": CANDIDATE_ROOT,
        "status": importer.ENDPOINT_DATASET_VALIDATED,
        "is_current": False,
        "previous_dataset_id": None,
        "dataset_hash": CANDIDATE_HASH,
        "validated_at": VALIDATED_AT,
        "superseded_at": None,
        "completion_proof_required_version": None,
        "completion_proof_sha256": None,
    }
    current_row_map = {
        "dataset_id": CURRENT_DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "superseded_at": None,
    }

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="endpoint_dataset_current_changed",
    ):
        importer._assert_locked_artifact_fence_datasets(
            fence,
            [candidate_row_map, current_row_map],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="endpoint_dataset_current_changed",
    ):
        importer._assert_locked_artifact_fence_datasets(
            fence,
            [
                {
                    **candidate_row_map,
                    "locked_current_dataset_ids": [CURRENT_DATASET_ID],
                }
            ],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )


@pytest.mark.asyncio
async def test_binder_verifies_the_bound_fence(monkeypatch):
    candidate, bound_fence = _validated_publication_fence(None, None)
    unbound_fence = importer.ProviderDirectoryArtifactDatasetFence(
        bound_fence.datasets,
        should_select_validated_candidates=True,
    )
    verify = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_verify_provider_directory_artifact_dataset_fence",
        verify,
    )

    result = await importer._bind_validated_publication_candidate(
        unbound_fence,
        candidate,
    )

    assert result.validated_publication_candidate is candidate
    verify.assert_awaited_once_with(result)


@pytest.mark.asyncio
async def test_worker_forwards_the_closed_publication_fence(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_tables",
        AsyncMock(),
    )
    publish = AsyncMock(return_value={"published": True})
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_dataset_artifacts",
        publish,
    )
    monkeypatch.setattr(
        importer,
        "_source_local_dataset_followup_if_current",
        AsyncMock(return_value={"status": "required"}),
    )
    params = _publication_params(
        completion_proof_required_version=None,
        completion_proof_sha256=None,
    )

    assert await importer.process_data(
        {"context": {"test_mode": True}},
        params,
    ) == {
        "published": True,
        "dataset_followup": {"status": "required"},
    }
    assert publish.await_args.kwargs[
        "validated_publication_candidate"
    ].to_payload() == _candidate_payload(
        completion_proof_required_version=None,
        completion_proof_sha256=None,
    )
