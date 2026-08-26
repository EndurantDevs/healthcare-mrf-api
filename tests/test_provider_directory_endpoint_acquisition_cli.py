import json
import subprocess
import sys

import pytest

from scripts import update_provider_directory_verification as verification_updater
from scripts import generate_provider_directory_support_docs as generator
from scripts.research import (
    provider_directory_endpoint_acquisition_cli as acquisition_cli,
    provider_directory_endpoint_acquisition_reporting as acquisition_reporting,
    provider_directory_endpoint_acquisition_support as acquisition_support,
)
from tests.provider_directory_endpoint_acquisition_test_support import (
    successful_operator_input,
)


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
        json.dumps(successful_operator_input(manifest, entry)),
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


def _assert_existing_readiness_survives(manifest, verification_report, entry):
    prior_snapshot = generator.load_verification_snapshot(
        generator.DEFAULT_VERIFICATION_SNAPSHOT
    )
    updated_snapshot = verification_updater.update_verification_snapshot(
        manifest,
        verification_report,
        prior_snapshot,
        manifest["catalog_confirmation"]["environment"],
    )
    assert updated_snapshot["entries"][entry["entry_id"]].get(
        "publication_readiness"
    ) == prior_snapshot["entries"][entry["entry_id"]].get("publication_readiness")
    generator.render_markdown(
        manifest,
        verification_snapshot=updated_snapshot,
        current_dataset_audit=generator.load_current_dataset_audit(
            generator.DEFAULT_CURRENT_DATASET_AUDIT
        ),
    )


def test_acquisition_cli_emits_updater_compatible_report(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["resources"]
    )
    operator_input = successful_operator_input(manifest, entry)
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
    _assert_existing_readiness_survives(manifest, verification_report, entry)


def test_acquisition_cli_records_terminal_metric_failure(tmp_path, capsys):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["resources"]
    )
    operator_input = successful_operator_input(manifest, entry)
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
    entry = next(manifest_entry for manifest_entry in manifest["entries"] if manifest_entry["resources"])
    operator_input = successful_operator_input(manifest, entry)
    observed_result = operator_input["results"][entry["entry_id"]]
    observed_result["status"] = "failed"
    observed_result["params"]["provider_directory_pagination_attempt"] = 0
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
    entry = next(manifest_entry for manifest_entry in manifest["entries"] if manifest_entry["resources"])
    operator_input = successful_operator_input(manifest, entry)
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
    summary = acquisition_cli.run_summary(
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
    entry = next(manifest_entry for manifest_entry in manifest["entries"] if manifest_entry["resources"])
    operator_input = successful_operator_input(manifest, entry)
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

    report_without_metadata_by_field = dict(report)
    report_without_metadata_by_field.pop("verification_update")
    with pytest.raises(
        verification_updater.VerificationUpdateError,
        match="require verification update metadata",
    ):
        verification_updater.update_verification_snapshot(
            manifest,
            report_without_metadata_by_field,
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
    entry = next(manifest_entry for manifest_entry in manifest["entries"] if manifest_entry["resources"])
    operator_input = successful_operator_input(manifest, entry)
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
    entry = next(manifest_entry for manifest_entry in manifest["entries"] if manifest_entry["resources"])
    operator_input = successful_operator_input(manifest, entry)
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


def test_acquisition_cli_refuses_invalid_updater_report(tmp_path):
    manifest = acquisition_cli.harness.load_manifest()
    entry = next(
        manifest_entry
        for manifest_entry in manifest["entries"]
        if manifest_entry["resources"]
    )
    invalid_input = successful_operator_input(manifest, entry)
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
    selected_entry = next(manifest_entry for manifest_entry in manifest["entries"] if manifest_entry["resources"])
    extra_entry = next(
        item
        for item in manifest["entries"]
        if item["entry_id"] != selected_entry["entry_id"]
    )
    operator_input = successful_operator_input(manifest, selected_entry)
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


def test_acquisition_modules_share_run_id_pattern():
    assert acquisition_cli.harness.RUN_ID_PATTERN is acquisition_support.RUN_ID_PATTERN
    assert acquisition_reporting.RUN_ID_PATTERN is acquisition_support.RUN_ID_PATTERN
