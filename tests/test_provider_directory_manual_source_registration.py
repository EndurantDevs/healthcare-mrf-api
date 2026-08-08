# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

from api.provider_directory_sources import provider_directory_source_catalog
from scripts import generate_provider_directory_support_docs as generator
from scripts.research import (
    provider_directory_endpoint_acquisition_harness as harness,
)


def _manual_manifest_entry(manifest):
    manual_entries = [
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["classification"] == "manual_acquisition"
    ]
    assert len(manual_entries) == 1
    return manual_entries[0]


def test_reviewed_manual_source_manifest_contract():
    """Keep the reviewed manual source fully bound without persisting a cutoff."""
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    manual_entry = _manual_manifest_entry(manifest)
    manual_contract = manual_entry["manual_current_version_census"]
    assert manual_entry["launch_mode"] == "manual"
    assert manual_entry["resource_profile"] == "A7"
    assert len(manual_entry["resources"]) == 7
    assert set(manual_contract) == {
        "canonicalization_version",
        "completion_scopes",
        "contract_version",
        "semantics",
        "strategy_version",
        "traversal_version",
        "plan_name",
        "seed_source",
        "continuation_strategy",
        "expected_nonempty_resources",
        "page_count",
        "verification_campaign_id",
        "start_urls",
    }
    assert manual_contract["contract_version"] == 3
    assert manual_contract["semantics"] == "server-issued-traversal-subset"
    assert manual_contract["continuation_strategy"] == (
        "smile-opaque-logical-offset-v3"
    )
    assert manual_contract["expected_nonempty_resources"] == manual_entry["resources"]
    assert manual_contract["page_count"] == 250
    assert set(manual_contract["start_urls"]) == set(manual_entry["resources"])
    assert all(
        start_url == f"{manual_entry['canonical_base']}/{resource_type}"
        for resource_type, start_url in manual_contract["start_urls"].items()
    )
    assert "cutoff" not in json.dumps(manual_contract).lower()

    support = manifest["support_documentation"]["entry_support"][
        manual_entry["entry_id"]
    ]
    assert support["support_level"] == "acquisition-configured"
    assert support["method"] == "rest"
    assert support["access_requirement"] == "none"


def test_reviewed_manual_source_has_no_current_proof():
    """Keep acquisition, current-dataset, and downstream proof states separate."""
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    manual_entry = _manual_manifest_entry(manifest)
    verification_record = generator.load_verification_snapshot(
        generator.DEFAULT_VERIFICATION_SNAPSHOT
    )["entries"][manual_entry["entry_id"]]
    assert verification_record == {
        "access_verification": "not_recorded",
        "checked_at": None,
        "proof_state": "not_recorded",
        "run_id": None,
        "terminal_status": None,
    }
    audit = generator.load_current_dataset_audit(
        generator.DEFAULT_CURRENT_DATASET_AUDIT
    )
    audit_record = next(
        candidate_record
        for candidate_record in audit["records"]
        if candidate_record.get("entry_id") == manual_entry["entry_id"]
    )
    assert audit_record["dataset_state"] == "no-current-dataset"
    assert audit_record["downstream_evidence"] == "not-proven"


def test_manual_source_is_excluded_from_generic_harness():
    """Emit no generic run parameters or generic terminal-proof acceptance."""
    harness_manifest = harness.load_manifest(harness.DEFAULT_MANIFEST)
    harness_entry = _manual_manifest_entry(harness_manifest)
    assert harness.entry_params(harness_manifest, harness_entry) == {}
    plan = harness.build_operator_plan(
        harness_manifest, frozenset({harness_entry["entry_id"]})
    )
    assert len(plan["entries"]) == 1
    plan_entry = plan["entries"][0]
    assert plan_entry["entry_id"] == harness_entry["entry_id"]
    assert plan_entry["owner_id"] == harness_entry["owner_id"]
    assert plan_entry["canonical_base"] == harness_entry["canonical_base"]
    assert plan_entry["classification"] == "manual_acquisition"
    assert plan_entry["spec_sha256"] == harness._entry_fingerprint(
        harness_manifest, harness_entry
    )
    assert plan_entry["params"] == {}
    terminal_errors = harness.terminal_metric_errors(
        harness_manifest,
        harness_entry,
        {
            "importer": harness_manifest["importer"],
            "params": {},
            "metrics": {
                "stale_cleanup": False,
                "publish_artifacts": False,
                "publish_after_acquisition": False,
                "publish_corroboration": False,
                "pagination_resume_required": False,
            },
        },
    )
    assert terminal_errors == [
        "manual acquisition requires dedicated current-version census proof"
    ]


def test_manual_source_is_nonrunnable_and_outside_profile():
    """Expose reviewed resources without enabling control or Profile execution."""
    source_catalog = provider_directory_source_catalog()
    manual_catalog_entries = [
        catalog_entry
        for catalog_entry in source_catalog["items"]
        if catalog_entry["classification"] == "manual_acquisition"
    ]

    assert len(manual_catalog_entries) == 1
    catalog_entry = manual_catalog_entries[0]
    assert catalog_entry["runnable"] is False
    assert catalog_entry["profile_enabled"] is False
    assert len(catalog_entry["resources"]) == 7
    assert catalog_entry["supported_resources"] == catalog_entry["resources"]
