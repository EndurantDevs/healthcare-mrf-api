import copy
import datetime as dt
import json
import re
from pathlib import Path

import pytest
from scripts import generate_provider_directory_support_docs as generator
from scripts.research import provider_directory_endpoint_acquisition_harness as harness


def test_rendered_support_matrix_represents_each_manifest_entry_once():
    """The generated inventory and live-proof tables cover every tracked source."""
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
    assert "CareSource (`caresource`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint" in rendered_document
    assert "processed and unique candidate resource IDs equal unchanged post-scan census" in rendered_document
    assert "No product membership is inferred from the catalog row" in rendered_document
    assert "_count=100 preserves Plan-Net network extensions; _count=75 returns false-empty search sets" in rendered_document
    assert "ALOHR (`alohr`) | Acquisition-configured | Private connector | GraphQL | Practitioner, Organization, Location, PractitionerRole | https://" in rendered_document
    assert "Horizon NJ (`horizon-nj`) | Probe-only | OAuth2 client credentials | Probe | None configured" in rendered_document
    assert "AmeriHealth Caritas Carrier Directory (`amerihealth-caritas-carrier`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole" in rendered_document
    assert "AmeriHealth Caritas DC (`amerihealth-dc`) | Probe-only" in rendered_document
    assert "clears plan_name and does not claim NH product membership" in rendered_document
    assert "Exhaustive equivalence with plan-code bases" in rendered_document
    assert "## Inventory Summary" in rendered_document
    assert "| Acquisition-configured | 17 |" in rendered_document
    assert "| Externally supported | 0 |" in rendered_document
    assert "| Probe-only | 20 |" in rendered_document
    assert "| Known not importable | 3 |" in rendered_document
    assert "| Total tracked | 40 |" in rendered_document
    assert "### Credentialed Or Registered Access" in rendered_document
    assert "Aetna Commercial/Medicare (`aetna-commercial-medicare`) | Acquisition-configured | OAuth2 client credentials | Required" in rendered_document
    assert "Horizon NJ (`horizon-nj`) | Probe-only | OAuth2 client credentials | Required" in rendered_document
    assert "ALOHR (`alohr`) | Acquisition-configured | Private connector | Required" in rendered_document
    assert "First Medical Health Plan, Inc. (`provider-directory-blocked-first-medical-pr`) | Not supported | User token | Required" in rendered_document
    assert "| Registration | Reviewed at | Review valid through |" in rendered_document
    assert "Aetna Commercial/Medicare (`aetna-commercial-medicare`)" in rendered_document
    assert "Required | 2026-07-11 | 2026-08-25 | OAuth2 client credentials and Bulk" in rendered_document
    assert "Cigna (`cigna`)" in rendered_document
    assert "Not required | 2026-07-10 | 2026-08-24 | Sequential REST pagination" in rendered_document
    assert "## Observed Live Verification" in rendered_document
    assert "| Terminal status | Resource completion | Derived artifacts | Unified/API readiness | Readiness observed at |" in rendered_document
    assert "| ALOHR (`alohr`) | Not recorded | Not recorded | Not recorded | Not Promoted | Not Ready |" in rendered_document
    assert "| Idaho (`idaho`) | Current | Succeeded | Complete | Promoted | Ready |" in rendered_document
    assert "scripts/update_provider_directory_verification.py" in rendered_document
    assert "## Known Not Importable" in rendered_document
    assert "Chorus Community Health Plans" in rendered_document
    assert "First Medical Health Plan, Inc." in rendered_document
    assert "Territory of Puerto Rico" in rendered_document
    assert "User token | Required" in rendered_document
    assert "[campaign report]" not in rendered_document


def test_caresource_manifest_entry_is_public_carrier_level_r8_rest_support():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    entry = next(item for item in manifest["entries"] if item["entry_id"] == "caresource")
    support = manifest["support_documentation"]["entry_support"]["caresource"]

    assert entry["owner_id"] == "caresource-provider-directory"
    assert entry["source_ids"] == ["pdfhir_b627b38e07cae99151baa4b7"]
    assert entry["canonical_base"] == (
        "https://orchestrateserver.caresource.careevolution.com/"
        "api/fhir/provider-directory"
    )
    assert entry["classification"] == "acquisition"
    assert entry["resource_profile"] == "R8"
    assert entry["resources"] == [
        "InsurancePlan",
        "PractitionerRole",
        "Practitioner",
        "Organization",
        "Location",
        "HealthcareService",
        "OrganizationAffiliation",
        "Endpoint",
    ]
    assert support["access_requirement"] == "none"
    assert support["requires_registration"] is False
    assert support["method"] == "rest"
    assert "No product membership is inferred" in support["limitation"]


def test_alohr_manifest_requires_fresh_four_resource_graphql_proof():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    alohr_entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["entry_id"] == "alohr"
    )
    alohr_support = manifest["support_documentation"]["entry_support"]["alohr"]
    alohr_verification = generator.load_verification_snapshot(
        generator.DEFAULT_VERIFICATION_SNAPSHOT
    )["entries"]["alohr"]

    assert alohr_entry["classification"] == "acquisition"
    assert alohr_entry["launch_mode"] == "create"
    assert alohr_entry["resource_profile"] == "G4"
    assert alohr_entry["resources"] == [
        "Practitioner",
        "Organization",
        "Location",
        "PractitionerRole",
    ]
    assert "external_run_id" not in alohr_entry
    assert alohr_support["support_level"] == "acquisition-configured"
    assert alohr_support["method"] == "graphql"
    assert "documented_resources" not in alohr_support
    assert alohr_verification["access_verification"] == "not_recorded"
    assert alohr_verification["proof_state"] == "not_recorded"
    assert alohr_verification["terminal_status"] is None
    assert alohr_verification["publication_readiness"][
        "derived_artifact_state"
    ] == (
        "not_promoted"
    )
    assert alohr_verification["publication_readiness"]["unified_api_state"] == (
        "not_ready"
    )
    assert "current_observation" not in alohr_verification
    assert "terminal_evidence" not in alohr_verification
    assert "OrganizationAffiliation" not in json.dumps(alohr_verification)
    assert "357802" not in json.dumps(alohr_verification)


def test_rendered_live_proof_summarizes_resource_rows():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    rendered_document = generator.render_markdown(manifest)

    assert "| Rows by resource |" in rendered_document
    assert "| Idaho (`idaho`) | Current | Succeeded | Complete | Promoted | Ready |" in rendered_document
    assert "| Cigna (`cigna`) | Current | Succeeded | Complete | Promoted | Ready |" in rendered_document
    assert "HealthcareService: 1,108,600" in rendered_document
    assert "Location: 280,847" in rendered_document


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
    entry = next(item for item in manifest["entries"] if item["entry_id"] == "alohr")
    support = manifest["support_documentation"]["entry_support"]["alohr"]
    entry.update(
        {
            "classification": "external",
            "launch_mode": "external_completed",
            "external_run_id": "run_17baae4934f54639bd748d50554a9cbd",
            "resource_profile": "NONE",
            "resources": [],
        }
    )
    support.update(
        {
            "support_level": "externally-supported",
            "method": "graphql",
            "documented_resources": [
                "Practitioner",
                "Organization",
                "Location",
                "PractitionerRole",
            ],
        }
    )
    support.pop("documented_resources")

    with pytest.raises(generator.SupportDocumentationError, match="requires documented_resources"):
        generator.validate_manifest(manifest)


def test_validate_manifest_rejects_external_method_mismatch():
    manifest = copy.deepcopy(generator.load_manifest(generator.DEFAULT_MANIFEST))
    entry = next(item for item in manifest["entries"] if item["entry_id"] == "alohr")
    support = manifest["support_documentation"]["entry_support"]["alohr"]
    entry.update(
        {
            "classification": "external",
            "launch_mode": "external_completed",
            "external_run_id": "run_17baae4934f54639bd748d50554a9cbd",
            "resource_profile": "NONE",
            "resources": [],
        }
    )
    support.update(
        {
            "support_level": "externally-supported",
            "method": "rest",
            "documented_resources": [
                "Practitioner",
                "Organization",
                "Location",
                "PractitionerRole",
            ],
        }
    )

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
        ("molina", "checkpointed 497,700 Location rows"),
        ("michigan", "Synthetic _getpagesoffset continuation is not equivalent"),
        ("cigna", "_count=75 returns false-empty search sets"),
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
        ("uhc", "plan_graph_complete never satisfies endpoint publication"),
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
