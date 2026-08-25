# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import datetime as dt
import json
import re
from pathlib import Path

import pytest

from scripts import generate_provider_directory_support_docs as generator
from scripts.research import provider_directory_endpoint_acquisition_harness as harness

def test_validate_manifest_rejects_unusable_catalog_confirmation():
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    manifest["catalog_confirmation"]["checked_at"] = "not-a-date"

    with pytest.raises(generator.SupportDocumentationError, match="ISO-8601"):
        generator.validate_manifest(manifest)


def test_freshness_validation_rejects_expired_catalog_source_and_proof():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    blockers = generator.validate_blocker_registry(
        generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY)
    )
    snapshot = generator.load_verification_snapshot(
        generator.DEFAULT_VERIFICATION_SNAPSHOT
    )

    with pytest.raises(generator.SupportDocumentationError, match="catalog confirmation expired") as error:
        generator.validate_support_freshness(
            manifest,
            blockers,
            snapshot,
            dt.date(2026, 8, 26),
        )

    assert "idaho terminal proof expired" in str(error.value)


def test_freshness_validation_accepts_current_reviews():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    blockers = generator.validate_blocker_registry(
        generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY)
    )
    snapshot = generator.load_verification_snapshot(
        generator.DEFAULT_VERIFICATION_SNAPSHOT
    )

    generator.validate_support_freshness(
        manifest,
        blockers,
        snapshot,
        dt.date(2026, 7, 11),
    )


@pytest.mark.parametrize(
    "entry_id, expected_detail",
    [
        ("idaho", "api-ida-prd.safhir.io cursor continuations with checkpoints"),
        ("molina", "checkpointed 497,700 Location rows"),
        ("michigan", "Synthetic _getpagesoffset continuation is not equivalent"),
        ("cigna", "both _count=100 and _count=75 returned populated search sets"),
        ("aetna-commercial-medicare", "OAuth2 client credentials and Bulk"),
        ("humana", "catalog product aliases are neutralized"),
        ("iehp", "Normalizes portal and resource paths"),
        ("arkansas", "synthetic _skip pagination with stable _id sorting"),
        ("hap", "throttles requests to 20 seconds"),
        ("washington", "regressed from 50,800 to 16,800"),
        ("wyoming", "PractitionerRole pagination was revalidated"),
        ("amerihealth-caritas-carrier", "clears plan_name"),
        ("texas-tmhp", "stable _id sorting and offset pagination"),
        ("nebraska", "Endpoint is excluded because it returns HTTP 404"),
        ("uhc", "server ignores the :missing modifier"),
        ("maine", "Five collections are anonymously readable with ct cursor pagination"),
        ("horizon-nj", "approved Provider Directory API-product subscription"),
        ("missouri", "Practitioner response exceeds the 20 MiB cap"),
        ("scan", "1,000-result search ceiling"),
        ("centene", "Location requires at least one resource search parameter"),
        ("contra-costa", "Seven public collections follow opaque next-link pagination"),
        ("alohr", "A fresh four-resource GraphQL acquisition"),
    ],
)
def test_support_metadata_retains_audited_source_details(entry_id, expected_detail):
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    limitation = manifest["support_documentation"]["entry_support"][entry_id]["limitation"]

    assert expected_detail in limitation


def test_reviewed_manual_subset_support_never_claims_exhaustive():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    manual_entries = [
        entry
        for entry in manifest["entries"]
        if entry["classification"] == "manual_acquisition"
    ]
    assert len(manual_entries) == 1
    entry_id = manual_entries[0]["entry_id"]
    limitation = manifest["support_documentation"]["entry_support"][
        entry_id
    ]["limitation"]
    current_audit = generator.load_current_dataset_audit(
        generator.DEFAULT_CURRENT_DATASET_AUDIT
    )
    audit_notes = [
        entry["note"]
        for entry in current_audit["records"]
        if entry.get("entry_id") == entry_id
    ]
    assert len(audit_notes) == 1
    generated_support = generator.DEFAULT_OUTPUT.read_text(encoding="utf-8")

    for support_text in (limitation, audit_notes[0]):
        normalized_text = support_text.lower()
        for required_text in (
            "server-issued traversal subset",
            "advertised",
            "returned",
            "deficit",
            "absence",
            "unknown",
            "root-neutral subset proof",
        ):
            assert required_text in normalized_text
        assert "exhaustive" not in normalized_text
        assert "current-version census" not in normalized_text
        assert "census acquisition" not in normalized_text
        assert support_text in generated_support


def test_amerihealth_uses_one_carrier_acquisition_and_five_probe_aliases():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    support_by_entry = manifest["support_documentation"]["entry_support"]
    entries_by_id = {
        entry["entry_id"]: entry
        for entry in manifest["entries"]
        if entry["entry_id"].startswith("amerihealth-")
    }
    carrier = entries_by_id.pop("amerihealth-caritas-carrier")

    assert carrier["classification"] == "acquisition"
    assert carrier["resource_profile"] == "A6"
    assert carrier["canonical_base"].endswith("/0900/provider-api")
    assert carrier["source_ids"] == ["pdfhir_3e8f8d73e9f63b41f4f3fca5"]
    assert set(entries_by_id) == {
        "amerihealth-de", "amerihealth-la", "amerihealth-nc",
        "amerihealth-dc", "amerihealth-pa",
    }
    assert all(entry["classification"] == "probe_only" for entry in entries_by_id.values())
    assert all(entry["resources"] == [] for entry in entries_by_id.values())
    support = support_by_entry[carrier["entry_id"]]
    assert support["support_level"] == "acquisition-configured"
    assert support["method"] == "rest"
    assert "Exhaustive equivalence" in support["limitation"]
    assert "no resource evidence is fanned out" in support["limitation"]
    assert "No terminal full-acquisition" in support["limitation"]


def test_documentation_metadata_does_not_change_entry_execution_fingerprints():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    fingerprints_by_entry = {
        entry["entry_id"]: harness._entry_fingerprint(manifest, entry)
        for entry in manifest["entries"]
    }
    changed = copy.deepcopy(manifest)
    changed["support_documentation"]["entry_support"]["idaho"]["limitation"] = "Documentation-only wording."

    assert {
        entry["entry_id"]: harness._entry_fingerprint(changed, entry)
        for entry in changed["entries"]
    } == fingerprints_by_entry


def test_check_reports_generated_documentation_drift(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    output_path = tmp_path / "support.md"
    manifest_path.write_text(json.dumps(generator.load_manifest(generator.DEFAULT_MANIFEST)), encoding="utf-8")

    assert generator.main(["--manifest", str(manifest_path), "--output", str(output_path)]) == 0
    assert generator.main([
        "--manifest", str(manifest_path),
        "--output", str(output_path),
        "--check", "--as-of", "2026-07-11",
    ]) == 0
    output_path.write_text("stale\n", encoding="utf-8")

    assert generator.main([
        "--manifest", str(manifest_path),
        "--output", str(output_path),
        "--check", "--as-of", "2026-07-11",
    ]) == 1


def test_provider_directory_guide_local_links_resolve():
    root = Path(__file__).resolve().parents[1]
    guide_path = root / "docs/imports/provider-directory-fhir.md"
    guide = guide_path.read_text(encoding="utf-8")
    links = re.findall(r"\[[^\]]+\]\(([^)]+)\)", guide)

    assert links
    for target in links:
        assert not target.startswith(("http://", "https://", "#"))
        assert (guide_path.parent / target).resolve().is_file(), target


def test_provider_directory_guide_documents_the_full_lifecycle():
    root = Path(__file__).resolve().parents[1]
    guide = (root / "docs/imports/provider-directory-fhir.md").read_text(encoding="utf-8")
    expected_links = {
        "../../specs/provider_directory_endpoint_acquisition_manifest.json",
        "provider-directory-endpoint-support.md",
        "../../specs/provider_directory_blocker_registry.json",
        "../../specs/provider_directory_endpoint_verification.json",
        "../../.github/workflows/ci.yml",
    }
    actual_links = set(re.findall(r"\[[^\]]+\]\(([^)]+)\)", guide))

    assert expected_links <= actual_links
    for command in (
        "scripts/research/provider_directory_endpoint_acquisition_cli.py",
        "--validate-only",
        "--apply",
        "scripts/update_provider_directory_verification.py",
        "scripts/generate_provider_directory_support_docs.py",
        "--check",
        "openaddresses_geocode",
        "archive coordinates are never replaced",
        "Resource completion",
        "CI rejects expired evidence",
        "stores the fingerprint of its manifest entry",
        "publication_readiness",
        "Canonical endpoint identity is transport identity",
    ):
        assert command in guide
    assert "Never hand-edit the generated" in guide


def test_verification_snapshot_rejects_terminal_record_without_timestamp():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    snapshot["entries"]["idaho"] = {
        "terminal_status": "succeeded",
        "run_id": "run_idaho",
        "access_verification": "verified",
        "checked_at": None,
    }

    with pytest.raises(generator.SupportDocumentationError, match="terminal entries need"):
        generator.validate_verification_snapshot(
            snapshot,
            manifest,
        )


def test_verification_snapshot_rejects_current_proof_for_changed_entry():
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    snapshot = copy.deepcopy(
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    )
    idaho_entry = next(
        entry for entry in manifest["entries"] if entry["entry_id"] == "idaho"
    )
    idaho_entry["canonical_base"] = "https://changed.example.test/fhir"

    with pytest.raises(generator.SupportDocumentationError, match="current manifest entry"):
        generator.validate_verification_snapshot(snapshot, manifest)
