import json
import subprocess
import sys

import pytest

from scripts import update_provider_directory_verification as verification_updater
from scripts import generate_provider_directory_support_docs as generator
from scripts.research import (
    provider_directory_endpoint_acquisition_cli as acquisition_cli,
)


def _successful_operator_input(manifest, entry):
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
                    "resource_fetch_completed_source_ids": {
                        resource_type: source_ids
                        for resource_type in entry["resources"]
                    },
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


def test_acquisition_cli_defaults_to_local_inputs():
    args = acquisition_cli.parse_acquisition_arguments([])

    assert args.manifest == acquisition_cli.harness.DEFAULT_MANIFEST
    assert args.entry == []
    assert args.operator_input is None
    assert args.output is None
    assert args.validate_only is False
    assert args.verification_report is False


def test_acquisition_cli_emits_selected_operator_plan(capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = manifest["entries"][0]

    exit_code = acquisition_cli.run_acquisition_cli(["--entry", entry["entry_id"]])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [item["entry_id"] for item in payload["entries"]] == [entry["entry_id"]]
    assert payload["entries"][0]["params"] == (
        acquisition_cli.harness.entry_params(manifest, entry)
    )


def test_acquisition_cli_verifies_local_operator_input(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["resources"]
    )
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(
        json.dumps(_successful_operator_input(manifest, entry)),
        encoding="utf-8",
    )
    report_path = tmp_path / "report.json"

    exit_code = acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(report_path),
        ]
    )
    cli_report = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert cli_report["ok"] is True
    assert cli_report["entries"][entry["entry_id"]]["status"] == "passed"
    assert json.loads(report_path.read_text(encoding="utf-8")) == cli_report


def test_acquisition_cli_emits_updater_compatible_report(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["resources"]
    )
    operator_input = _successful_operator_input(manifest, entry)
    first_resource = entry["resources"][0]
    operator_input["results"][entry["entry_id"]]["metrics"][
        "resource_fetch_stats"
    ][first_resource]["internal_debug_url"] = "https://internal.invalid/debug"
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(
        json.dumps(operator_input), encoding="utf-8"
    )
    verification_report_path = tmp_path / "verification-report.json"
    exit_code = acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(verification_report_path),
            "--verification-report",
        ]
    )
    verification_report = json.loads(capsys.readouterr().out)
    updated_snapshot = verification_updater.update_verification_snapshot(
        manifest,
        verification_report,
        verification_updater._empty_snapshot(manifest),
        manifest["catalog_confirmation"]["environment"],
    )
    updated_existing_snapshot = verification_updater.update_verification_snapshot(
        manifest,
        verification_report,
        generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT),
        manifest["catalog_confirmation"]["environment"],
    )

    assert exit_code == 0
    assert verification_report["mode"] == "dry-run"
    assert verification_report["verification_update"]["eligible"] is True
    assert verification_report["verification_update"]["argv"][2:4] == [
        "--manifest",
        str(acquisition_cli.harness.DEFAULT_MANIFEST),
    ]
    assert "internal_debug_url" not in json.dumps(verification_report)
    updated_entry = updated_snapshot["entries"][entry["entry_id"]]
    assert updated_entry["terminal_status"] == "succeeded"
    assert updated_entry["run_id"] == "run_0123456789abcdef0123456789abcdef"
    assert updated_entry["access_verification"] == "not_verified"
    assert "current_observation" not in updated_entry
    assert generator._observation_display(updated_entry).startswith("Succeeded")
    assert updated_existing_snapshot["entries"][entry["entry_id"]].get(
        "publication_readiness"
    ) == generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)[
        "entries"
    ][entry["entry_id"]].get("publication_readiness")
    generator.render_markdown(
        manifest,
        verification_snapshot=updated_existing_snapshot,
        current_dataset_audit=generator.load_current_dataset_audit(
            generator.DEFAULT_CURRENT_DATASET_AUDIT
        ),
    )


def test_acquisition_cli_records_terminal_metric_failure(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["resources"]
    )
    operator_input = _successful_operator_input(manifest, entry)
    operator_input["results"][entry["entry_id"]]["metrics"][
        "publish_artifacts"
    ] = True
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(json.dumps(operator_input), encoding="utf-8")
    report_path = tmp_path / "verification-report.json"

    exit_code = acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(report_path),
            "--verification-report",
        ]
    )
    report = json.loads(capsys.readouterr().out)
    report_entry = report["entries"][entry["entry_id"]]
    snapshot = verification_updater.update_verification_snapshot(
        manifest,
        report,
        verification_updater._empty_snapshot(manifest),
        manifest["catalog_confirmation"]["environment"],
    )

    assert exit_code == 0
    assert report_entry["status"] == "metric_validation_failed"
    assert report_entry["metric_errors"] == [
        "metrics.publish_artifacts must be false"
    ]
    assert snapshot["entries"][entry["entry_id"]]["current_observation"][
        "run_status"
    ] == "succeeded"


def test_acquisition_cli_records_failed_run_as_observation(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    operator_input = _successful_operator_input(manifest, entry)
    observed_result = operator_input["results"][entry["entry_id"]]
    observed_result["status"] = "failed"
    observed_result["params"]["concurrency"] += 1
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(json.dumps(operator_input), encoding="utf-8")
    report_path = tmp_path / "verification-report.json"

    acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(report_path),
            "--verification-report",
        ]
    )
    report = json.loads(capsys.readouterr().out)
    snapshot = verification_updater.update_verification_snapshot(
        manifest,
        report,
        verification_updater._empty_snapshot(manifest),
        manifest["catalog_confirmation"]["environment"],
    )

    assert report["entries"][entry["entry_id"]]["status"] == "observed"
    assert report["entries"][entry["entry_id"]]["plan_bound"] is False
    assert report["verification_update"]["eligible"] is False
    assert snapshot["entries"][entry["entry_id"]]["terminal_status"] is None
    assert snapshot["entries"][entry["entry_id"]]["current_observation"]["run_status"] == "failed"


def test_acquisition_cli_does_not_accept_unplanned_params(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    operator_input = _successful_operator_input(manifest, entry)
    operator_input["results"][entry["entry_id"]]["params"]["unplanned_mutation"] = True
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(json.dumps(operator_input), encoding="utf-8")
    report_path = tmp_path / "verification-report.json"

    acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(report_path),
            "--verification-report",
        ]
    )
    report_entry = json.loads(capsys.readouterr().out)["entries"][entry["entry_id"]]

    assert report_entry["status"] == "metric_validation_failed"
    assert report_entry["metric_errors"] == [
        "result params are not controlled: unplanned_mutation"
    ]


def test_acquisition_report_sanitizes_scalar_and_count_fields():
    summary = acquisition_cli._run_summary(
        {
            "run_id": "run_0123456789abcdef0123456789abcdef",
            "status": {},
            "created_at": "Bearer very-secret-value",
            "error": {
                "code": "import_failed",
                "message": "https://internal.invalid/users/Alice",
            },
            "metrics": {
                "source_ids": ["pdfhir_0123456789abcdef01234567"],
                "sources_probed": -1,
                "resource_fetch_stats": {
                    "Location": {
                        "rows_fetched": float("nan"),
                        "sources_completed": True,
                    }
                },
                "resource_fetch_completed_source_ids": {"Location": [{}]},
                "bulk_export_mode": {"effective": 1, "requested": False},
            },
        }
    )

    assert "very-secret" not in json.dumps(summary)
    assert "internal.invalid" not in json.dumps(summary)
    assert summary["terminal_error"] == {"code": "import_failed"}
    assert "created_at" not in summary
    assert "sources_probed" not in summary
    assert "status" not in summary
    assert summary["resource_outcomes"]["Location"] == {}
    assert "effective_acquisition" not in summary


def test_operator_attestation_is_environment_bound(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    operator_input = _successful_operator_input(manifest, entry)
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(json.dumps(operator_input), encoding="utf-8")
    report_path = tmp_path / "verification-report.json"
    acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(report_path),
            "--verification-report",
        ]
    )
    report = json.loads(capsys.readouterr().out)

    report_without_metadata = dict(report)
    report_without_metadata.pop("verification_update")
    with pytest.raises(
        verification_updater.VerificationUpdateError,
        match="require verification update metadata",
    ):
        verification_updater.update_verification_snapshot(
            manifest,
            report_without_metadata,
            verification_updater._empty_snapshot(manifest),
            manifest["catalog_confirmation"]["environment"],
        )

    with pytest.raises(
        verification_updater.VerificationUpdateError,
        match="environment does not match",
    ):
        verification_updater.update_verification_snapshot(
            manifest,
            report,
            verification_updater._empty_snapshot(manifest),
            "another-environment",
        )


def test_operator_input_environment_must_match_manifest(tmp_path):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    operator_input = _successful_operator_input(manifest, entry)
    operator_input["environment"] = "production"
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(json.dumps(operator_input), encoding="utf-8")

    with pytest.raises(acquisition_cli.harness.ManifestError, match="environment does not match"):
        acquisition_cli.run_acquisition_cli(
            [
                "--entry",
                entry["entry_id"],
                "--operator-input",
                str(operator_input_path),
                "--output",
                str(tmp_path / "report.json"),
                "--verification-report",
            ]
        )


def test_failed_observation_requires_exact_source_identity(tmp_path):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    operator_input = _successful_operator_input(manifest, entry)
    result = operator_input["results"][entry["entry_id"]]
    result["status"] = "failed"
    result["importer"] = "another-importer"
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(json.dumps(operator_input), encoding="utf-8")

    with pytest.raises(acquisition_cli.harness.ManifestError, match="run identity is invalid"):
        acquisition_cli.run_acquisition_cli(
            [
                "--entry",
                entry["entry_id"],
                "--operator-input",
                str(operator_input_path),
                "--output",
                str(tmp_path / "report.json"),
                "--verification-report",
            ]
        )


def test_failed_successor_restores_prior_terminal_proof():
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    prior = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(entry),
        "proof_state": "superseded",
        "superseded_reason": "newer_active_run",
        "current_observation": {
            "run_id": "run_11111111111111111111111111111111",
            "state_status": "observed",
            "run_status": "running",
            "observed_at": "2026-08-25T01:00:00Z",
        },
    }
    report_entry = {
        "status": "observed",
        "current_run_id": "run_11111111111111111111111111111111",
        "last_run": {
            "run_id": "run_11111111111111111111111111111111",
            "status": "failed",
            "source_ids": entry["source_ids"],
        },
        "plan_bound": True,
    }

    merged = verification_updater._merge_nonterminal_observation(
        entry["entry_id"], entry, report_entry, prior, "2026-08-26T00:00:00Z"
    )

    assert merged["proof_state"] == "current"
    assert "superseded_reason" not in merged


def test_unbound_active_observation_does_not_supersede_terminal_proof():
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    prior = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(entry),
        "proof_state": "current",
    }
    report_entry = {
        "status": "observed",
        "current_run_id": "run_22222222222222222222222222222222",
        "last_run": {
            "run_id": "run_22222222222222222222222222222222",
            "status": "running",
            "source_ids": entry["source_ids"],
        },
        "plan_bound": False,
    }

    merged = verification_updater._merge_nonterminal_observation(
        entry["entry_id"], entry, report_entry, prior, "2026-08-26T00:00:00Z"
    )

    assert merged["proof_state"] == "current"
    assert "superseded_reason" not in merged


@pytest.mark.parametrize(
    ("plan_bound", "run_status", "run_id"),
    [
        (False, "running", "run_22222222222222222222222222222222"),
        (True, "failed", "run_33333333333333333333333333333333"),
    ],
)
def test_unrelated_observation_preserves_bound_active_supersession(
    plan_bound, run_status, run_id
):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    prior = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(entry),
        "proof_state": "superseded",
        "superseded_reason": "newer_active_run",
        "current_observation": {
            "run_id": "run_11111111111111111111111111111111",
            "state_status": "observed",
            "run_status": "running",
            "observed_at": "2026-08-25T01:00:00Z",
        },
    }
    report_entry = {
        "status": "observed",
        "current_run_id": run_id,
        "last_run": {
            "run_id": run_id,
            "status": run_status,
            "source_ids": entry["source_ids"],
        },
        "plan_bound": plan_bound,
    }

    merged = verification_updater._merge_nonterminal_observation(
        entry["entry_id"], entry, report_entry, prior, "2026-08-26T00:00:00Z"
    )

    assert merged == prior


def test_same_bound_active_successor_refreshes_observation():
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(item for item in manifest["entries"] if item["resources"])
    active_run_id = "run_11111111111111111111111111111111"
    prior = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(entry),
        "proof_state": "superseded",
        "superseded_reason": "newer_active_run",
        "current_observation": {
            "run_id": active_run_id,
            "state_status": "observed",
            "run_status": "running",
            "observed_at": "2026-08-25T01:00:00Z",
        },
    }
    report_entry = {
        "status": "observed",
        "current_run_id": active_run_id,
        "last_run": {
            "run_id": active_run_id,
            "status": "running",
            "source_ids": entry["source_ids"],
        },
        "plan_bound": True,
    }

    merged = verification_updater._merge_nonterminal_observation(
        entry["entry_id"], entry, report_entry, prior, "2026-08-26T00:00:00Z"
    )

    assert merged["proof_state"] == "superseded"
    assert merged["superseded_reason"] == "newer_active_run"
    assert merged["current_observation"]["observed_at"] == "2026-08-26T00:00:00Z"


def test_updater_drops_free_form_terminal_error_text():
    assert verification_updater._safe_terminal_error(
        {
            "terminal_error": {
                "code": "import_failed",
                "reason": "user Alice",
                "message": "https://internal.invalid/debug",
            }
        }
    ) == {"code": "import_failed"}


def test_acquisition_cli_refuses_invalid_updater_report(tmp_path):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["resources"]
    )
    invalid_input = _successful_operator_input(manifest, entry)
    invalid_input["results"][entry["entry_id"]]["spec_sha256"] = "0" * 64
    invalid_input_path = tmp_path / "invalid-operator-input.json"
    invalid_input_path.write_text(json.dumps(invalid_input), encoding="utf-8")
    invalid_report_path = tmp_path / "invalid-verification-report.json"

    with pytest.raises(acquisition_cli.harness.ManifestError, match="planned entry"):
        acquisition_cli.run_acquisition_cli(
            [
                "--entry",
                entry["entry_id"],
                "--operator-input",
                str(invalid_input_path),
                "--output",
                str(invalid_report_path),
                "--verification-report",
            ]
        )
    assert not invalid_report_path.exists()

    invalid_input["results"][entry["entry_id"]]["spec_sha256"] = (
        acquisition_cli.harness.build_operator_plan(
            manifest, frozenset({entry["entry_id"]})
        )["entries"][0]["spec_sha256"]
    )
    invalid_input["observed_at"] = "2000-01-01T00:00:00Z"
    invalid_input_path.write_text(json.dumps(invalid_input), encoding="utf-8")
    with pytest.raises(acquisition_cli.harness.ManifestError, match="not current"):
        acquisition_cli.run_acquisition_cli(
            [
                "--entry",
                entry["entry_id"],
                "--operator-input",
                str(invalid_input_path),
                "--output",
                str(invalid_report_path),
                "--verification-report",
            ]
        )
    assert not invalid_report_path.exists()


def test_acquisition_cli_requires_exact_selected_results(tmp_path):
    manifest = acquisition_cli.harness.load_manifest()
    selected_entry = next(item for item in manifest["entries"] if item["resources"])
    extra_entry = next(
        item
        for item in manifest["entries"]
        if item["entry_id"] != selected_entry["entry_id"]
    )
    operator_input = _successful_operator_input(manifest, selected_entry)
    operator_input["results"][extra_entry["entry_id"]] = dict(
        operator_input["results"][selected_entry["entry_id"]]
    )
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(json.dumps(operator_input), encoding="utf-8")

    with pytest.raises(acquisition_cli.harness.ManifestError, match="exactly match"):
        acquisition_cli.run_acquisition_cli(
            [
                "--entry",
                selected_entry["entry_id"],
                "--operator-input",
                str(operator_input_path),
                "--output",
                str(tmp_path / "report.json"),
                "--verification-report",
            ]
        )

def test_acquisition_cli_is_directly_executable():
    result = subprocess.run(
        [sys.executable, acquisition_cli.__file__, "--validate-only"],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["valid"] is True
    assert payload["entries"] > 0
