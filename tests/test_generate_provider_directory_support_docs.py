import copy
import json

import pytest

from scripts import generate_provider_directory_support_docs as generator
from scripts.research import provider_directory_endpoint_acquisition_harness as harness


def test_rendered_support_matrix_represents_each_manifest_entry_once():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)

    rendered = generator.render_markdown(manifest)
    rows = [line for line in rendered.splitlines() if line.startswith("| ")][2:]
    entry_ids = [entry["entry_id"] for entry in manifest["entries"]]

    assert len(entry_ids) == 28
    assert len(rows) == len(entry_ids)
    assert all(row.count(f"`{entry_id}`") == 1 for row, entry_id in zip(rows, entry_ids))
    assert "OAuth2 client credentials | Bulk" in rendered
    assert "Cigna (`cigna`) | Acquisition-configured | None | REST" in rendered
    assert "Sequential REST pagination preserves Plan-Net network extensions; no Bulk." in rendered
    assert "ALOHR (`alohr`) | Supported | Private connector | GraphQL | Practitioner, Organization, Location, PractitionerRole, OrganizationAffiliation" in rendered
    assert "Horizon NJ (`horizon-nj`) | Probe-only | Unknown | Probe | None configured" in rendered
    assert "does not claim that a live probe succeeded" in rendered
    assert "`reports/provider-directory-endpoint-acquisition/report.json`" in rendered
    assert "selected `--report` path; the report is not tracked" in rendered
    assert "[campaign report]" not in rendered


@pytest.mark.parametrize(
    "field_name, value, message",
    [
        ("support_level", "live", "invalid support level"),
        ("access_requirement", "api-key", "invalid access requirement"),
        ("method", "ftp", "invalid method"),
    ],
)
def test_validate_manifest_rejects_uncontrolled_metadata_values(field_name, value, message):
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    manifest["support_documentation"]["entry_support"]["idaho"][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_manifest(manifest)


def test_validate_manifest_rejects_missing_or_extra_entry_metadata():
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    entry_support = manifest["support_documentation"]["entry_support"]
    entry_support.pop("idaho")
    entry_support["not-a-manifest-entry"] = copy.deepcopy(entry_support["molina"])

    with pytest.raises(generator.SupportDocumentationError, match="missing metadata.*metadata without"):
        generator.validate_manifest(manifest)


@pytest.mark.parametrize(
    "entry_id, expected_detail",
    [
        ("idaho", "api-ida-prd.safhir.io cursor continuations with checkpoints"),
        ("molina", "molina.sapphirethreesixtyfive.com cursor continuations"),
        ("michigan", "PractitionerRole pages are capped at 25"),
        ("cigna", "Sequential REST pagination preserves Plan-Net network extensions; no Bulk"),
        ("aetna-commercial-medicare", "OAuth2 client credentials and Bulk"),
        ("humana", "Overrides portal or stale paths to the public FHIR base"),
        ("iehp", "Normalizes portal and resource paths"),
        ("arkansas", "synthetic _skip pagination with stable _id sorting"),
        ("hap", "throttles requests to 20 seconds"),
        ("washington", "Location pages are capped at 25"),
        ("wyoming", "PractitionerRole pages are capped at 25"),
        ("amerihealth-nh", "Plan code 0900; full-refresh pages target 250 rows"),
        ("texas-tmhp", "Offset pagination is supported, but this campaign only probes"),
        ("nebraska", "Offset pagination is supported, but this campaign only probes"),
        ("uhc", "Search partitions and role reverse lookup exist, but this campaign only probes"),
        ("scan", "Role reverse lookup and 100-page cap exist, but this campaign only probes"),
        ("centene", "CloudFront or WAF access can block a runtime probe"),
        ("contra-costa", "Official catalog can return 403, so the fallback base is retained"),
        ("alohr", "FHIR REST reads are auth-gated; the maintained GraphQL connector uses tenant alohr"),
    ],
)
def test_support_metadata_retains_audited_source_details(entry_id, expected_detail):
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    limitation = manifest["support_documentation"]["entry_support"][entry_id]["limitation"]

    assert expected_detail in limitation


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
    assert generator.main(["--manifest", str(manifest_path), "--output", str(output_path), "--check"]) == 0
    output_path.write_text("stale\n", encoding="utf-8")

    assert generator.main(["--manifest", str(manifest_path), "--output", str(output_path), "--check"]) == 1
