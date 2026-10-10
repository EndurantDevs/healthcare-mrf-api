# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import datetime as dt
import json
import re
from pathlib import Path

import pytest

from scripts import generate_provider_directory_support_docs as generator
from scripts.research import provider_directory_endpoint_acquisition_harness as harness
from tests.provider_directory_endpoint_acquisition_test_support import (
    synthetic_support_manifest,
    synthetic_verification_snapshot,
)


def test_validate_manifest_rejects_unusable_catalog_confirmation():
    manifest = synthetic_support_manifest()
    manifest["catalog_confirmation"]["checked_at"] = "not-a-date"

    with pytest.raises(generator.SupportDocumentationError, match="ISO-8601"):
        generator.validate_manifest(manifest)


def test_freshness_validation_rejects_expired_catalog_source_and_proof():
    manifest = synthetic_support_manifest()
    blockers = generator.validate_blocker_registry(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    snapshot = synthetic_verification_snapshot(manifest)

    with pytest.raises(generator.SupportDocumentationError, match="catalog confirmation expired") as error:
        generator.validate_support_freshness(
            manifest,
            blockers,
            snapshot,
            dt.date(2026, 10, 11),
        )

    assert "idaho terminal proof expired" in str(error.value)


def test_freshness_validation_accepts_current_reviews():
    manifest = synthetic_support_manifest()
    blockers = generator.validate_blocker_registry(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    snapshot = synthetic_verification_snapshot(manifest)

    generator.validate_support_freshness(
        manifest,
        blockers,
        snapshot,
        dt.date(2026, 9, 25),
    )


def test_freshness_validation_rejects_stale_active_observation():
    manifest = synthetic_support_manifest()
    blockers = generator.validate_blocker_registry(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    snapshot = synthetic_verification_snapshot(manifest)
    snapshot["entries"]["aetna-commercial-medicare"]["current_observation"] = {
        "run_id": "run_" + "2" * 32,
        "state_status": "observed",
        "run_status": "running",
        "observed_at": "2026-07-12T00:00:00Z",
    }

    with pytest.raises(generator.SupportDocumentationError, match="active observation expired"):
        generator.validate_support_freshness(
            manifest,
            blockers,
            snapshot,
            dt.date(2026, 9, 25),
        )


@pytest.mark.parametrize(
    "entry_id, expected_detail",
    [
        ("san-bernardino-county-dbh", "_offset page index by one after each full page"),
        ("san-mateo-county-bhrs", "_offset page index by one after each full page"),
        ("devoted-health", "PractitionerRole uses adaptive _lastUpdated partitions"),
        ("idaho", "official public cursor continuations with checkpoints"),
        ("molina", "fails closed with pagination_resume_required"),
        ("michigan", "Synthetic _getpagesoffset continuation is not equivalent"),
        ("cigna", "supports configured _count=100 and _count=75 searches"),
        ("aetna-commercial-medicare", "OAuth2 client credentials and Bulk"),
        ("humana", "catalog product aliases are neutralized"),
        ("iehp", "Normalizes portal and resource paths"),
        ("arkansas", "synthetic _skip pagination with stable _id sorting"),
        ("hap", "throttles requests to 20 seconds"),
        ("washington", "provider_directory_pagination_resume_required"),
        ("wyoming", "PractitionerRole pagination requires complete traversal proof"),
        ("amerihealth-caritas-carrier", "clears plan_name"),
        ("texas-tmhp", "stable _id sorting and offset pagination"),
        ("nebraska", "Historical probes returned HTTP 404 for Endpoint, which remains excluded"),
        ("uhc", "ignored :missing modifier"),
        ("maine", "Five collections are configured with ct cursor pagination"),
        ("horizon-nj", "approved Provider Directory API-product subscription"),
        ("missouri", "Practitioner response exceeds the 20 MiB cap"),
        ("scan", "1,000-result search ceiling"),
        ("centene", "Location requires at least one resource search parameter"),
        ("contra-costa", "Seven public collections are configured with opaque next-link pagination"),
        ("alohr", "A fresh four-resource GraphQL acquisition"),
    ],
)
def test_support_metadata_retains_audited_source_details(entry_id, expected_detail):
    manifest = synthetic_support_manifest()
    limitation = manifest["support_documentation"]["entry_support"][entry_id]["limitation"]

    assert expected_detail in limitation


def test_reviewed_manual_subset_support_never_claims_exhaustive():
    manifest = synthetic_support_manifest()
    manual_entries = [entry for entry in manifest["entries"] if entry["classification"] == "manual_acquisition"]
    assert len(manual_entries) == 1
    entry_id = manual_entries[0]["entry_id"]
    limitation = manifest["support_documentation"]["entry_support"][entry_id]["limitation"]
    current_audit = generator.load_current_dataset_audit(generator.DEFAULT_CURRENT_DATASET_AUDIT)
    assert current_audit == {"schema_version": 1, "as_of": None, "records": []}
    generated_support = generator.DEFAULT_OUTPUT.read_text(encoding="utf-8")

    for support_text in (limitation,):
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
    manifest = synthetic_support_manifest()
    support_by_entry = manifest["support_documentation"]["entry_support"]
    entries_by_id = {
        entry["entry_id"]: entry for entry in manifest["entries"] if entry["entry_id"].startswith("amerihealth-")
    }
    carrier = entries_by_id.pop("amerihealth-caritas-carrier")

    assert carrier["classification"] == "acquisition"
    assert carrier["resource_profile"] == "A6"
    assert carrier["canonical_base"].endswith("/0900/provider-api")
    assert carrier["source_ids"] == ["pdfhir_3e8f8d73e9f63b41f4f3fca5"]
    assert set(entries_by_id) == {
        "amerihealth-de",
        "amerihealth-la",
        "amerihealth-nc",
        "amerihealth-dc",
        "amerihealth-pa",
    }
    assert all(entry["classification"] == "probe_only" for entry in entries_by_id.values())
    assert all(entry["resources"] == [] for entry in entries_by_id.values())
    support = support_by_entry[carrier["entry_id"]]
    assert support["support_level"] == "acquisition-configured"
    assert support["method"] == "rest"
    assert "Exhaustive equivalence" in support["limitation"]
    assert "no resource evidence is fanned out" in support["limitation"]
    assert "Terminal six-collection acquisition evidence" in support["limitation"]


def test_documentation_metadata_does_not_change_entry_execution_fingerprints():
    manifest = synthetic_support_manifest()
    fingerprints_by_entry = {
        entry["entry_id"]: harness._entry_fingerprint(manifest, entry) for entry in manifest["entries"]
    }
    changed = copy.deepcopy(manifest)
    changed["support_documentation"]["entry_support"]["idaho"]["limitation"] = "Documentation-only wording."

    assert {
        entry["entry_id"]: harness._entry_fingerprint(changed, entry) for entry in changed["entries"]
    } == fingerprints_by_entry


def test_check_reports_generated_documentation_drift(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    output_path = tmp_path / "support.md"
    manifest_path.write_text(json.dumps(generator.load_manifest(generator.DEFAULT_MANIFEST)), encoding="utf-8")

    assert generator.main(["--manifest", str(manifest_path), "--output", str(output_path)]) == 0
    assert (
        generator.main(
            [
                "--manifest",
                str(manifest_path),
                "--output",
                str(output_path),
                "--check",
                "--as-of",
                "2026-07-11",
            ]
        )
        == 0
    )
    output_path.write_text("stale\n", encoding="utf-8")

    assert (
        generator.main(
            [
                "--manifest",
                str(manifest_path),
                "--output",
                str(output_path),
                "--check",
                "--as-of",
                "2026-07-11",
            ]
        )
        == 1
    )


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
        "--operator-input",
        "--verification-report",
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
    manifest = synthetic_support_manifest()
    snapshot = synthetic_verification_snapshot(manifest)
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
    manifest = synthetic_support_manifest()
    snapshot = synthetic_verification_snapshot(manifest)
    idaho_entry = next(entry for entry in manifest["entries"] if entry["entry_id"] == "idaho")
    idaho_entry["canonical_base"] = "https://changed.example.test/fhir"

    with pytest.raises(generator.SupportDocumentationError, match="current manifest entry"):
        generator.validate_verification_snapshot(snapshot, manifest)
