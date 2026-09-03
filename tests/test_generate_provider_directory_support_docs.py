import copy
import datetime as dt
import json
import re
from pathlib import Path

import pytest
from scripts import generate_provider_directory_support_docs as generator


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
    assert "Michigan (`michigan`) | Acquisition-configured | None | REST | Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole" in rendered_document
    assert "CareSource (`caresource`) | Acquisition-configured | None | REST | InsurancePlan, PractitionerRole, Practitioner, Organization, Location, HealthcareService, OrganizationAffiliation, Endpoint" in rendered_document
    assert "processed and unique candidate resource IDs equal unchanged post-scan census" in rendered_document
    assert "No product membership is inferred from the catalog row" in rendered_document
    assert "both _count=100 and _count=75 returned populated search sets" in rendered_document
    assert "ALOHR (`alohr`) | Acquisition-configured | Private connector | GraphQL | Practitioner, Organization, Location, PractitionerRole | https://" in rendered_document
    assert "UnitedHealthcare Official Provider Files (`uhc-provider-files`) | Acquisition-configured | None | Official files | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole" in rendered_document
    assert "Horizon NJ (`horizon-nj`) | Probe-only | OAuth2 client credentials | Probe | None configured" in rendered_document
    assert "AmeriHealth Caritas Carrier Directory (`amerihealth-caritas-carrier`) | Acquisition-configured | None | REST | InsurancePlan, Location, Organization, OrganizationAffiliation, Practitioner, PractitionerRole" in rendered_document
    assert "AmeriHealth Caritas DC (`amerihealth-dc`) | Probe-only" in rendered_document
    assert "clears plan_name and does not claim NH product membership" in rendered_document
    assert "Exhaustive equivalence with plan-code bases" in rendered_document
    assert "## Inventory Summary" in rendered_document
    assert "| Acquisition-configured | 26 |" in rendered_document
    assert "| Externally supported | 0 |" in rendered_document
    assert "| Probe-only | 13 |" in rendered_document
    assert "| Known not importable | 3 |" in rendered_document
    assert "| Total tracked | 42 |" in rendered_document
    assert "### Credentialed Or Registered Access" in rendered_document
    assert "Aetna Commercial/Medicare (`aetna-commercial-medicare`) | Acquisition-configured | OAuth2 client credentials | Required" in rendered_document
    assert "Horizon NJ (`horizon-nj`) | Probe-only | OAuth2 client credentials | Required" in rendered_document
    assert "ALOHR (`alohr`) | Acquisition-configured | Private connector | Required" in rendered_document
    assert "First Medical Health Plan, Inc. (`provider-directory-blocked-first-medical-pr`) | Not supported | User token | Required" in rendered_document
    assert "| Registration | Reviewed at | Review valid through |" in rendered_document
    assert "Aetna Commercial/Medicare (`aetna-commercial-medicare`)" in rendered_document
    assert "Required | 2026-08-26 | 2026-10-10 | OAuth2 client credentials and Bulk" in rendered_document
    assert "Cigna (`cigna`)" in rendered_document
    assert "Not required | 2026-08-26 | 2026-10-10 | Sequential REST pagination" in rendered_document
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
    assert "| Cigna (`cigna`) | Current | Succeeded | Complete | Superseded (Promoted) | Superseded (Ready) |" in rendered_document
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
    assert "2026-08-25" in rendered


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
