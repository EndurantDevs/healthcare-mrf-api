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
    assert "Audit as of `2026-07-20`" in rendered_document
    assert "Molina (`molina`) | No current dataset | - | Not applicable" in rendered_document
    assert "Texas TMHP (`texas-tmhp`) | Current published (`pdds_87113f97f3f9...`) | 782,642 | Not proven" in rendered_document
    assert "CareSource (`caresource`) | No current dataset | - | Not proven" in rendered_document
    assert "exhaustive acquisition and downstream proof are pending" in rendered_document
    assert "No product membership is inferred" in rendered_document
    assert "Humana Carrier Directory (`humana`) | Current published (`pdds_97a8b36ca361...`) | 16,140,342 | Not proven" in rendered_document
    assert "ALOHR (`alohr`) | Current published (`pdds_085b7d2da6de...`) | 319,384 | Contract/live mismatch" in rendered_document
    assert "fresh four-resource acquisition" in rendered_document
    assert "Aetna Commercial and Medicare (`aetna-commercial-medicare`)" in rendered_document
    assert "Devoted Health (`devoted-health`) | Current published (`pdds_fc4167c03b85...`) | 288,056 | Not proven" in rendered_document
    assert "San Bernardino County DBH (`san-bernardino-county-dbh`) | Current published (`pdds_f4d01cad21a5...`) | 8,749 | Not proven" in rendered_document
    assert "pre-release artifact pass excluded this source from Profile" in rendered_document


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
