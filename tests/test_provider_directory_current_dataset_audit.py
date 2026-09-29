import copy

import pytest

from scripts import generate_provider_directory_support_docs as generator
from tests.provider_directory_endpoint_acquisition_test_support import (
    synthetic_current_dataset_audit, synthetic_verification_snapshot,
)


def test_public_audit_explicitly_records_no_operational_evidence():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    assert audit == {"schema_version": 1, "as_of": None, "records": []}
    rendered = generator.render_markdown(manifest, current_dataset_audit=audit)
    assert "Current dataset evidence is not recorded" in rendered
    assert "Audit as of" not in rendered
    assert "Current published (`pdds_" not in rendered


@pytest.mark.parametrize("audit", [
    {"schema_version": 1, "as_of": "2026-08-26", "records": []},
    {"schema_version": 1, "as_of": None, "records": [{}]},
])
def test_audit_absence_cannot_hide_partial_recorded_evidence(audit):
    with pytest.raises(generator.SupportDocumentationError):
        generator.render_markdown(generator.load_manifest(generator.DEFAULT_MANIFEST), current_dataset_audit=audit)


def test_current_dataset_audit_renders_separately_from_acquisition_support():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)

    rendered_document = generator.render_markdown(
        manifest,
        current_dataset_audit=audit,
        verification_snapshot=synthetic_verification_snapshot(manifest),
    )

    assert "## Current Published Dataset Audit" in rendered_document
    assert f"Audit as of `{audit['as_of']}`" in rendered_document
    assert "Idaho (`idaho`) | Current published (`pdds_aaaaaaaaaaaa...`) | 3 | Snapshot-ready" in rendered_document
    assert "CareSource (`caresource`) | Current published (`pdds_aaaaaaaaaaaa...`) | 3 | Not proven" in rendered_document
    assert "Synthetic dataset observation." in rendered_document
    assert "Cigna (`cigna`) | Acquisition-configured" in rendered_document


def test_current_dataset_audit_rejects_unknown_manifest_entry():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    audit["records"][0]["entry_id"] = "unknown-entry"

    with pytest.raises(generator.SupportDocumentationError, match="invalid audit entry_id"):
        generator.render_markdown(
            manifest,
            current_dataset_audit=audit,
        )


def test_current_dataset_audit_requires_every_manifest_entry():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    audit["records"] = [
        record for record in audit["records"] if record.get("entry_id") != "aetna-commercial-medicare"
    ]

    with pytest.raises(generator.SupportDocumentationError, match="misses manifest entry_ids"):
        generator.render_markdown(
            manifest, current_dataset_audit=audit,
            verification_snapshot=synthetic_verification_snapshot(manifest),
        )


def test_snapshot_ready_requires_promoted_ready_verification():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    snapshot = synthetic_verification_snapshot(manifest)
    snapshot["entries"]["idaho"]["publication_readiness"]["unified_api_state"] = "not_ready"

    with pytest.raises(generator.SupportDocumentationError, match="lacks ready verification"):
        generator.render_markdown(
            manifest,
            verification_snapshot=snapshot,
            current_dataset_audit=audit,
        )


def test_not_proven_rejects_stale_ready_verification():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    snapshot = synthetic_verification_snapshot(manifest)
    snapshot["entries"]["caresource"]["publication_readiness"] = {
        "dataset_id": "pdds_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "derived_artifact_state": "promoted",
        "evidence": {"counts": {"source_rows": 3}},
        "unified_api_state": "ready",
        "observed_at": "2026-08-26T00:00:00Z",
        "proof_state": "current",
    }

    with pytest.raises(generator.SupportDocumentationError, match="retains ready verification"):
        generator.render_markdown(
            manifest,
            verification_snapshot=snapshot,
            current_dataset_audit=audit,
        )


def test_not_proven_preserves_superseded_ready_receipt():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    snapshot = synthetic_verification_snapshot(manifest)
    snapshot["entries"]["caresource"]["publication_readiness"] = {
        "dataset_id": "pdds_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "derived_artifact_state": "promoted",
        "evidence": {"counts": {"source_rows": 1}},
        "unified_api_state": "ready",
        "observed_at": "2026-08-26T00:00:00Z",
        "proof_state": "superseded",
    }

    rendered = generator.render_markdown(
        manifest,
        verification_snapshot=snapshot,
        current_dataset_audit=audit,
    )

    assert "Superseded (Promoted)" in rendered


def test_snapshot_ready_rejects_readiness_for_another_dataset():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    snapshot = synthetic_verification_snapshot(manifest)
    snapshot["entries"]["idaho"]["publication_readiness"]["dataset_id"] = (
        "pdds_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )

    with pytest.raises(generator.SupportDocumentationError, match="does not match the audited dataset"):
        generator.render_markdown(
            manifest,
            verification_snapshot=snapshot,
            current_dataset_audit=audit,
        )


def test_grouped_non_ready_audit_does_not_skip_ready_alias():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    snapshot = synthetic_verification_snapshot(manifest)
    snapshot["entries"]["amerihealth-de"]["publication_readiness"] = {
        "dataset_id": "pdds_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "derived_artifact_state": "promoted",
        "evidence": {"counts": {"source_rows": 1}},
        "unified_api_state": "ready",
        "observed_at": "2026-08-26T00:00:00Z",
    }

    with pytest.raises(generator.SupportDocumentationError, match="does not match the audited dataset"):
        generator.render_markdown(
            manifest,
            verification_snapshot=snapshot,
            current_dataset_audit=audit,
        )


def test_null_publication_readiness_is_not_ready():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = synthetic_current_dataset_audit(manifest)
    snapshot = synthetic_verification_snapshot(manifest)
    snapshot["entries"]["caresource"]["publication_readiness"] = None

    generator.render_markdown(
        manifest,
        verification_snapshot=snapshot,
        current_dataset_audit=audit,
    )
