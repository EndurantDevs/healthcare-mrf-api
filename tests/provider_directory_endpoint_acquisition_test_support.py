"""Shared fixtures for Provider Directory endpoint acquisition harness tests."""

from typing import Any

from scripts.research import (
    provider_directory_endpoint_acquisition_cli as acquisition_cli,
)


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
