import copy

import pytest

from scripts import generate_provider_directory_support_docs as generator


def test_current_dataset_audit_renders_separately_from_acquisition_support():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = generator.load_current_dataset_audit(
        generator.DEFAULT_CURRENT_DATASET_AUDIT
    )

    rendered_document = generator.render_markdown(
        manifest,
        current_dataset_audit=audit,
    )

    assert "## Current Published Dataset Audit" in rendered_document
    assert f"Audit as of `{audit['as_of']}`" in rendered_document
    assert "Molina (`molina`) | Acquisition active | - | Not proven" in rendered_document
    assert "Texas TMHP (`texas-tmhp`) | Current published (`pdds_87113f97f3f9...`) | 782,642 | Not proven" in rendered_document
    assert "CareSource (`caresource`) | Current published (`pdds_811a33b01e0f...`) | 4,647,573 | Not proven" in rendered_document
    assert "An exhaustive acquisition produced the current published dataset" in rendered_document
    assert "No product membership is inferred" in rendered_document
    assert "Humana Carrier Directory (`humana`) | Current published (`pdds_eec2e2cae2e9...`) | 16,827,223 | Not proven" in rendered_document
    assert "ALOHR (`alohr`) | Current published (`pdds_085b7d2da6de...`) | 319,384 | Contract/live mismatch" in rendered_document
    assert "fresh four-resource acquisition" in rendered_document
    assert "Aetna Commercial and Medicare (`aetna-commercial-medicare`)" in rendered_document
    assert "Devoted Health (`devoted-health`) | Current published (`pdds_fc4167c03b85...`) | 288,056 | Not proven" in rendered_document
    assert "San Bernardino County DBH (`san-bernardino-county-dbh`) | Current published (`pdds_f4d01cad21a5...`) | 8,749 | Not proven" in rendered_document
    assert "Current Profile evidence binds this dataset" in rendered_document


def test_current_dataset_audit_rejects_unknown_manifest_entry():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = copy.deepcopy(
        generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    )
    audit["records"][0]["entry_id"] = "unknown-entry"

    with pytest.raises(generator.SupportDocumentationError, match="invalid audit entry_id"):
        generator.render_markdown(
            manifest,
            current_dataset_audit=audit,
        )


def test_current_dataset_audit_requires_every_manifest_entry():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = copy.deepcopy(
        generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    )
    audit["records"] = [
        record for record in audit["records"] if record.get("entry_id") != "aetna-commercial-medicare"
    ]

    with pytest.raises(generator.SupportDocumentationError, match="misses manifest entry_ids"):
        generator.render_markdown(manifest, current_dataset_audit=audit)


def test_snapshot_ready_requires_promoted_ready_verification():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    snapshot["entries"]["idaho"]["publication_readiness"]["unified_api_state"] = "not_ready"

    with pytest.raises(generator.SupportDocumentationError, match="lacks ready verification"):
        generator.render_markdown(
            manifest,
            verification_snapshot=snapshot,
            current_dataset_audit=audit,
        )


def test_not_proven_rejects_stale_ready_verification():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    snapshot["entries"]["caresource"]["publication_readiness"] = {
        "dataset_id": "pdds_811a33b01e0f242994b38b0d3d524997fa2a2cbe708d76e4c9add2652c18c8e4",
        "derived_artifact_state": "promoted",
        "evidence": {"counts": {"source_rows": 4647573}},
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
    audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
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
    audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
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
    audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
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
    audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    snapshot["entries"]["caresource"]["publication_readiness"] = None

    generator.render_markdown(
        manifest,
        verification_snapshot=snapshot,
        current_dataset_audit=audit,
    )
