"""Shared fixtures for Provider Directory endpoint acquisition harness tests."""

from typing import Any

from scripts.research import (
    provider_directory_endpoint_acquisition_cli as acquisition_cli,
)
from scripts import generate_provider_directory_support_docs as generator


def synthetic_catalog_confirmation() -> dict[str, Any]:
    """Return a small internally consistent catalog observation for tests."""
    statuses = dict.fromkeys((
        "auth_required", "valid", "valid_non_fhir", "dns_failure", "timeout",
        "no_api", "server_error", "unreachable",
    ), 0)
    statuses["valid"] = 2
    return {
        "environment": "test", "checked_at": "2026-08-26T00:00:00Z",
        "relation": "test.provider_directory_source", "source_count": 3,
        "probe_status_counts": statuses, "never_probed_source_count": 1,
        "valid_canonical_base_count": 1, "represented_valid_canonical_base_count": 1,
        "unrepresented_valid_canonical_base_count": 0, "collapsed_valid_alias_source_count": 1,
        "coverage_note": "Synthetic catalog aliases share one canonical base.",
    }


def synthetic_support_manifest() -> dict[str, Any]:
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    manifest["catalog_confirmation"] = synthetic_catalog_confirmation()
    return manifest


def synthetic_verification_snapshot(manifest: dict[str, Any]) -> dict[str, Any]:
    snapshot = generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    for entry_id in ("idaho", "cigna"):
        entry = next(item for item in manifest["entries"] if item["entry_id"] == entry_id)
        snapshot["entries"][entry_id] = {
            "terminal_status": "succeeded", "run_id": "run_" + "1" * 32,
            "access_verification": "verified", "checked_at": "2026-08-26T00:00:00Z",
            "proof_state": "current", "entry_spec_sha256": generator.provider_directory_entry_sha256(entry),
            "terminal_evidence": {"resource_outcomes": {
                resource: {"sources_attempted": 1, "sources_completed": 1,
                           "sources_failed": 0, "sources_bounded": 0,
                           "collection_complete_sources": 1, "rows_fetched": 3}
                for resource in entry["resources"]
            }},
            "publication_readiness": {
                "dataset_id": "pdds_" + "a" * 64, "derived_artifact_state": "promoted",
                "unified_api_state": "ready", "observed_at": "2026-08-26T00:00:00Z",
                "proof_state": "current" if entry_id == "idaho" else "superseded",
                "evidence": {"counts": {"source_rows": 3}},
            },
        }
    return snapshot


def synthetic_current_dataset_audit(manifest: dict[str, Any]) -> dict[str, Any]:
    records = [
        {"source": entry["display_name"], "entry_id": entry["entry_id"],
         "dataset_state": "no-current-dataset", "downstream_evidence": "not-proven",
         "note": "Synthetic dataset observation."}
        for entry in manifest["entries"]
    ]
    for record in records:
        if record["entry_id"] in {"idaho", "caresource"}:
            record.update(dataset_state="current-published", dataset_id="pdds_" + "a" * 64,
                          resource_count=3, observed_at="2026-08-26T00:00:00Z")
        if record["entry_id"] == "idaho":
            record["downstream_evidence"] = "snapshot-ready"
    return {"schema_version": 1, "as_of": "2026-08-26", "records": records}


def manifest_with_attached_entry(
    harness_module: Any, entry_id: str, run_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a manifest copy with one create entry converted to attach mode."""

    manifest = harness_module.load_manifest()
    entry = next(item for item in manifest["entries"] if item["entry_id"] == entry_id)
    entry.update(launch_mode="attach", attached_run_id=run_id)
    return manifest, entry


def successful_operator_input(
    manifest: dict[str, Any], entry: dict[str, Any]
) -> dict[str, Any]:
    """Return one exact source-bound successful operator observation."""
    source_ids = list(entry["source_ids"])
    operator_plan = acquisition_cli.harness.build_operator_plan(
        manifest, frozenset({entry["entry_id"]})
    )
    entry_plan = operator_plan["entries"][0]
    return {
        "schema_version": 1,
        "campaign_id": operator_plan["campaign_id"],
        "manifest_sha256": operator_plan["manifest_sha256"],
        "observed_at": acquisition_cli.harness._utc_now(),
        "observation_method": "operator-attested-read-only-export",
        "environment": manifest["catalog_confirmation"]["environment"],
        "results": {
            entry["entry_id"]: {
                "spec_sha256": entry_plan["spec_sha256"],
                "run_id": "run_0123456789abcdef0123456789abcdef",
                "status": "succeeded",
                "importer": manifest["importer"],
                "params": acquisition_cli.harness.entry_params(manifest, entry),
                "metrics": {
                    "source_ids": source_ids,
                    "source_import_sources_selected": len(source_ids),
                    "source_import_groups_attempted": 1,
                    "resource_fetch_completed_source_ids": dict.fromkeys(
                        entry["resources"], source_ids
                    ),
                    "resource_fetch_stats": {
                        resource_type: {
                            "sources_completed": 1,
                            "sources_bounded": 0,
                            "sources_failed": 0,
                        }
                        for resource_type in entry["resources"]
                    },
                    "stale_cleanup": False,
                    "publish_artifacts": False,
                    "publish_after_acquisition": False,
                    "publish_corroboration": False,
                },
            }
        },
    }
