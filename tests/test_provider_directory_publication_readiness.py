# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy

import pytest

from scripts import generate_provider_directory_support_docs as generator


def test_rendered_live_proof_keeps_artifact_and_api_readiness_separate():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    snapshot["entries"]["idaho"]["publication_readiness"] = {
        "dataset_id": "pdds_fc4faaf27524be41f181cc4ccfa81bf5911d9a534c0114b0ecbe6a114685b94f",
        "derived_artifact_state": "promoted",
        "unified_api_state": "pending_verification",
        "observed_at": "2026-07-12T00:00:00Z",
        "evidence": {
            "counts": {"source_rows": 12, "address_keys": 10},
            "signals": {"address_overlay": "not_checked"},
        },
    }

    rendered_document = generator.render_markdown(
        manifest,
        verification_snapshot=snapshot,
    )

    assert "| Idaho (`idaho`) | Current | Succeeded | Complete | Promoted | Pending Verification | 2026-07-12T00:00:00Z |" in rendered_document
    assert (
        "Source rows: 12<br>Address keys: 10<br>Address Overlay: Not Checked"
        in rendered_document
    )


def test_verification_snapshot_rejects_invalid_publication_readiness_signal():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    snapshot["entries"]["idaho"]["publication_readiness"] = {
        "dataset_id": "pdds_fc4faaf27524be41f181cc4ccfa81bf5911d9a534c0114b0ecbe6a114685b94f",
        "derived_artifact_state": "promoted",
        "unified_api_state": "ready",
        "observed_at": "2026-07-12T00:00:00Z",
        "evidence": {"signals": {"phones": "maybe"}},
    }

    with pytest.raises(generator.SupportDocumentationError, match="readiness signals"):
        generator.validate_verification_snapshot(snapshot, manifest)


@pytest.mark.parametrize("dataset_id", ["not-a-dataset-id", None])
def test_verification_snapshot_rejects_malformed_nonready_dataset_id(dataset_id):
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    snapshot["entries"]["idaho"]["publication_readiness"] = {
        "dataset_id": dataset_id,
        "derived_artifact_state": "promoted",
        "unified_api_state": "pending_verification",
        "observed_at": "2026-07-12T00:00:00Z",
    }

    with pytest.raises(generator.SupportDocumentationError, match="dataset_id is invalid"):
        generator.validate_verification_snapshot(snapshot, manifest)


@pytest.mark.parametrize(
    "missing_fields",
    [("observed_at",), ("run_status", "state_status")],
)
def test_verification_snapshot_rejects_incomplete_current_observation(missing_fields):
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    observation = snapshot["entries"]["aetna-commercial-medicare"][
        "current_observation"
    ]
    for field_name in missing_fields:
        observation.pop(field_name)

    with pytest.raises(
        generator.SupportDocumentationError,
        match="current observation requires observed_at and status",
    ):
        generator.validate_verification_snapshot(snapshot, manifest)
