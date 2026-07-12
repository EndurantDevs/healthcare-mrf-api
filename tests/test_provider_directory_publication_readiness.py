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
        "derived_artifact_state": "promoted",
        "unified_api_state": "ready",
        "observed_at": "2026-07-12T00:00:00Z",
        "evidence": {"signals": {"phones": "maybe"}},
    }

    with pytest.raises(generator.SupportDocumentationError, match="readiness signals"):
        generator.validate_verification_snapshot(snapshot, manifest)
