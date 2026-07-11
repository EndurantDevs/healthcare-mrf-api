import copy
import datetime as dt
import json
import re
from pathlib import Path

import pytest

from scripts import generate_provider_directory_support_docs as generator
from scripts.research import provider_directory_endpoint_acquisition_harness as harness


def test_rendered_support_matrix_represents_each_manifest_entry_once():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)

    rendered_document = generator.render_markdown(manifest)
    configured_table = rendered_document.split(
        "| Source | Configured support |", 1
    )[1].split("## Known Not Importable", 1)[0]
    configured_rows = [
        line for line in configured_table.splitlines() if line.startswith("| ")
    ][1:]
    entry_ids = [entry["entry_id"] for entry in manifest["entries"]]

    assert len(configured_rows) == len(entry_ids)
    assert all(
        configured_row.count(f"`{entry_id}`") == 1
        for configured_row, entry_id in zip(configured_rows, entry_ids)
    )
    assert "OAuth2 client credentials | Bulk" in rendered_document
    assert "Cigna (`cigna`) | Acquisition-configured | None | REST" in rendered_document
    assert "_count=100 preserves Plan-Net network extensions; _count=75 returns false-empty search sets" in rendered_document
    assert "ALOHR (`alohr`) | Externally supported | Private connector | GraphQL | Practitioner, Organization, Location, PractitionerRole, OrganizationAffiliation" in rendered_document
    assert "Horizon NJ (`horizon-nj`) | Probe-only | None | Probe | None configured" in rendered_document
    assert "does not claim that a live probe succeeded" in rendered_document
    assert "`reports/provider-directory-endpoint-acquisition/report.json`" in rendered_document
    assert "selected `--report` path; the report is not tracked" in rendered_document
    assert "Catalog inventory was last confirmed in `healthporta-dev`" in rendered_document
    assert "tracked verification snapshot is the authority for terminal per-endpoint live status" in rendered_document
    assert "CI rejects expired evidence" in rendered_document
    assert "## Inventory Summary" in rendered_document
    assert "| Acquisition-configured | 23 |" in rendered_document
    assert "| Externally supported | 1 |" in rendered_document
    assert "| Probe-only | 4 |" in rendered_document
    assert "| Known not importable | 3 |" in rendered_document
    assert "| Total tracked | 31 |" in rendered_document
    assert "### Credentialed Or Registered Access" in rendered_document
    assert "Aetna Commercial/Medicare (`aetna-commercial-medicare`) | Acquisition-configured | OAuth2 client credentials | Required" in rendered_document
    assert "ALOHR (`alohr`) | Externally supported | Private connector | Required" in rendered_document
    assert "First Medical Health Plan, Inc. (`provider-directory-blocked-first-medical-pr`) | Not supported | User token | Required" in rendered_document
    assert "| Registration | Reviewed at | Review valid through |" in rendered_document
    assert "Aetna Commercial/Medicare (`aetna-commercial-medicare`)" in rendered_document
    assert "Required | 2026-07-11 | 2026-08-25 | OAuth2 client credentials and Bulk" in rendered_document
    assert "Cigna (`cigna`)" in rendered_document
    assert "Not required | 2026-07-10 | 2026-08-24 | Sequential REST pagination" in rendered_document
    assert "## Observed Live Verification" in rendered_document
    assert "| Terminal status | Resource completion |" in rendered_document
    assert "| ALOHR (`alohr`) | Current | External Completed | Complete |" in rendered_document
    assert "| Idaho (`idaho`) | Current | Succeeded | Not recorded |" in rendered_document
    assert "scripts/update_provider_directory_verification.py" in rendered_document
    assert "| Idaho (`idaho`) | Current | Succeeded | Not recorded | run_" in rendered_document
    assert "## Known Not Importable" in rendered_document
    assert "Chorus Community Health Plans" in rendered_document
    assert "First Medical Health Plan, Inc." in rendered_document
    assert "Territory of Puerto Rico" in rendered_document
    assert "User token | Required" in rendered_document
    assert "[campaign report]" not in rendered_document


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
    ("field_name", "value", "message"),
    [
        ("requires_registration", "no", "requires_registration must be boolean"),
        ("reviewed_at", "10 July 2026", "reviewed_at must be an ISO date"),
    ],
)
def test_validate_manifest_rejects_uncontrolled_access_review_metadata(
    field_name,
    value,
    message,
):
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    manifest["support_documentation"]["entry_support"]["idaho"][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_manifest(manifest)


def test_validate_manifest_rejects_registration_access_contradictions():
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    entry_support = manifest["support_documentation"]["entry_support"]
    entry_support["idaho"]["requires_registration"] = True

    with pytest.raises(generator.SupportDocumentationError, match="public access"):
        generator.validate_manifest(manifest)

    entry_support["idaho"]["requires_registration"] = False
    entry_support["aetna-commercial-medicare"]["requires_registration"] = False

    with pytest.raises(generator.SupportDocumentationError, match="requires registration"):
        generator.validate_manifest(manifest)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("canonical_base", "http://example.test/fhir", "credential-free HTTPS URL"),
        ("source_ids", ["short-id"], "full pdfhir IDs"),
        ("resources", ["Doctor"], "unique known resource types"),
    ],
)
def test_validate_manifest_rejects_invalid_endpoint_contract(
    field_name,
    value,
    message,
):
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    manifest["entries"][0][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_manifest(manifest)


def test_validate_manifest_rejects_external_support_without_documented_resources():
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    support = manifest["support_documentation"]["entry_support"]["alohr"]
    support.pop("documented_resources")

    with pytest.raises(generator.SupportDocumentationError, match="requires documented_resources"):
        generator.validate_manifest(manifest)


def test_validate_manifest_rejects_external_method_mismatch():
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    support = manifest["support_documentation"]["entry_support"]["alohr"]
    support["method"] = "rest"

    with pytest.raises(generator.SupportDocumentationError, match="external classification"):
        generator.validate_manifest(manifest)


def test_blocker_registry_is_complete_and_shared_with_generated_docs():
    registry = generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY)

    entries = generator.validate_blocker_registry(registry)
    rendered = generator.render_markdown(
        generator.load_manifest(generator.DEFAULT_MANIFEST),
        registry,
    )

    assert len(entries) == 3
    assert all(entry["id"] in rendered for entry in entries)
    assert "Operational state" in rendered
    assert "| Method | Resources | Canonical base |" in rendered
    assert "Not importable | None confirmed | None confirmed" in rendered
    assert "Practitioner, PractitionerRole, Location | None confirmed" in rendered
    assert "| Live verification |" in rendered
    assert "2026-07-10" in rendered


def test_validate_blocker_registry_rejects_unknown_access_requirement():
    registry = copy.deepcopy(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    registry["entries"][0]["access_requirement"] = "portal-maybe"

    with pytest.raises(generator.SupportDocumentationError, match="invalid access requirement"):
        generator.validate_blocker_registry(registry)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("acquisition_method", {}, "invalid acquisition method"),
        ("access_requirement", [], "invalid access requirement"),
        ("operational_status", {}, "invalid operational status"),
    ],
)
def test_validate_blocker_registry_rejects_non_string_control_values(
    field_name,
    value,
    message,
):
    registry = copy.deepcopy(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    registry["entries"][0][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_blocker_registry(registry)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("acquisition_method", "rest", "invalid acquisition method"),
        ("documented_resources", ["Practitioner", "Practitioner"], "unique known resource types"),
        ("documented_resources", [{"type": "Practitioner"}], "unique known resource types"),
        ("canonical_base", "https://example.test/fhir", "canonical_base must be null"),
        (
            "live_verification",
            {"status": "succeeded", "checked_at": "2026-07-10T00:00:00Z"},
            "live_verification must remain not recorded",
        ),
    ],
)
def test_validate_blocker_registry_rejects_false_importability(
    field_name,
    value,
    message,
):
    registry = copy.deepcopy(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    registry["entries"][0][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_blocker_registry(registry)


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("operational_status", "maybe", "invalid operational status"),
        ("reviewed_at", "10 July", "reviewed_at must be an ISO date"),
    ],
)
def test_validate_blocker_registry_rejects_uncontrolled_freshness(field_name, value, message):
    registry = copy.deepcopy(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    registry["entries"][0][field_name] = value

    with pytest.raises(generator.SupportDocumentationError, match=message):
        generator.validate_blocker_registry(registry)


def test_validate_blocker_registry_rejects_legacy_schema_shape():
    registry = copy.deepcopy(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    registry["schema_version"] = 1

    with pytest.raises(generator.SupportDocumentationError, match="schema_version must be 2"):
        generator.validate_blocker_registry(registry)


def test_combined_validation_rejects_blocker_manifest_identity_overlap():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    registry = copy.deepcopy(generator.load_blocker_registry(generator.DEFAULT_BLOCKER_REGISTRY))
    snapshot = generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    registry["entries"][0]["id"] = "idaho"

    with pytest.raises(generator.SupportDocumentationError, match="overlap runnable"):
        generator.render_markdown(manifest, registry, snapshot)


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
        ("molina", "molina.sapphirethreesixtyfive.com cursor continuations"),
        ("michigan", "canonical HAPI next-page link returns HTTP 403"),
        ("cigna", "_count=75 returns false-empty search sets"),
        ("aetna-commercial-medicare", "OAuth2 client credentials and Bulk"),
        ("humana", "Overrides portal or stale paths to the public FHIR base"),
        ("iehp", "Normalizes portal and resource paths"),
        ("arkansas", "synthetic _skip pagination with stable _id sorting"),
        ("hap", "throttles requests to 20 seconds"),
        ("washington", "HealthcareService preflight timed out"),
        ("wyoming", "PractitionerRole pagination was revalidated"),
        ("amerihealth-nh", "Plan code 0900; full-refresh pages target 250 rows"),
        ("texas-tmhp", "stable _id sorting and offset pagination"),
        ("nebraska", "Endpoint is excluded because it returns HTTP 404"),
        ("uhc", "full acquisition currently fails closed"),
        ("maine", "Five collections are anonymously readable with ct cursor pagination"),
        ("horizon-nj", "core resource searches return HAPI HTTP 403"),
        ("missouri", "Location continuation pages changed page size and total semantics"),
        ("scan", "every shard has a 100-page cap"),
        ("centene", "CloudFront HTTP 403 from dev egress"),
        ("contra-costa", "Seven public collections follow opaque next-link pagination"),
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
        "--control-url \"$HLTHPRT_IMPORT_CONTROL_URL\"",
        "--apply",
        "scripts/update_provider_directory_verification.py",
        "scripts/generate_provider_directory_support_docs.py",
        "--check",
        "openaddresses_geocode",
        "archive coordinates are never replaced",
        "Resource completion",
        "CI rejects expired evidence",
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
            [entry["entry_id"] for entry in manifest["entries"]],
            manifest["campaign_id"],
        )
