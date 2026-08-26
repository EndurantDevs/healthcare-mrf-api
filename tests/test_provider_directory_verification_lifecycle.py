import json

import pytest

from scripts import update_provider_directory_verification as verification_updater
from scripts.research import (
    provider_directory_endpoint_acquisition_cli as acquisition_cli,
)
from tests.provider_directory_endpoint_acquisition_test_support import (
    successful_operator_input,
)


def _resource_manifest_entry():
    manifest = acquisition_cli.harness.load_manifest()
    manifest_entry = next(
        candidate_entry
        for candidate_entry in manifest["entries"]
        if candidate_entry["resources"]
    )
    return manifest_entry


def test_failed_successor_restores_prior_terminal_proof():
    manifest_entry = _resource_manifest_entry()
    prior_record_by_field = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(
            manifest_entry
        ),
        "proof_state": "superseded",
        "superseded_reason": "newer_active_run",
        "current_observation": {
            "run_id": "run_11111111111111111111111111111111",
            "state_status": "observed",
            "run_status": "running",
            "observed_at": "2026-08-25T01:00:00Z",
        },
    }
    report_entry_by_field = {
        "status": "observed",
        "current_run_id": "run_11111111111111111111111111111111",
        "last_run": {
            "run_id": "run_11111111111111111111111111111111",
            "status": "failed",
            "source_ids": manifest_entry["source_ids"],
        },
        "plan_bound": True,
    }

    merged = verification_updater._merge_nonterminal_observation(
        manifest_entry["entry_id"],
        manifest_entry,
        report_entry_by_field,
        prior_record_by_field,
        "2026-08-26T00:00:00Z",
    )

    assert merged["proof_state"] == "current"
    assert "superseded_reason" not in merged


def test_unbound_active_observation_does_not_supersede_terminal_proof():
    manifest_entry = _resource_manifest_entry()
    prior_record_by_field = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(
            manifest_entry
        ),
        "proof_state": "current",
    }
    report_entry_by_field = {
        "status": "observed",
        "current_run_id": "run_22222222222222222222222222222222",
        "last_run": {
            "run_id": "run_22222222222222222222222222222222",
            "status": "running",
            "source_ids": manifest_entry["source_ids"],
        },
        "plan_bound": False,
    }

    merged = verification_updater._merge_nonterminal_observation(
        manifest_entry["entry_id"],
        manifest_entry,
        report_entry_by_field,
        prior_record_by_field,
        "2026-08-26T00:00:00Z",
    )

    assert merged["proof_state"] == "current"
    assert "superseded_reason" not in merged


@pytest.mark.parametrize(
    ("is_plan_bound", "run_status", "prior_active_run_id", "run_id"),
    [
        (
            False,
            "running",
            "run_11111111111111111111111111111111",
            "run_22222222222222222222222222222222",
        ),
        (
            True,
            "failed",
            "run_11111111111111111111111111111111",
            "run_33333333333333333333333333333333",
        ),
        (True, "failed", None, None),
    ],
)
def test_unrelated_observation_preserves_bound_active_supersession(
    is_plan_bound, run_status, prior_active_run_id, run_id
):
    manifest_entry = _resource_manifest_entry()
    prior_record_by_field = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(
            manifest_entry
        ),
        "proof_state": "superseded",
        "superseded_reason": "newer_active_run",
        "current_observation": {
            **({"run_id": prior_active_run_id} if prior_active_run_id else {}),
            "state_status": "observed",
            "run_status": "running",
            "observed_at": "2026-08-25T01:00:00Z",
        },
    }
    report_entry_by_field = {
        "status": "observed",
        "current_run_id": run_id,
        "last_run": {
            "run_id": run_id,
            "status": run_status,
            "source_ids": manifest_entry["source_ids"],
        },
        "plan_bound": is_plan_bound,
    }

    merged = verification_updater._merge_nonterminal_observation(
        manifest_entry["entry_id"],
        manifest_entry,
        report_entry_by_field,
        prior_record_by_field,
        "2026-08-26T00:00:00Z",
    )

    assert merged == prior_record_by_field


def test_same_bound_active_successor_refreshes_observation():
    manifest_entry = _resource_manifest_entry()
    active_run_id = "run_11111111111111111111111111111111"
    prior_record_by_field = {
        "terminal_status": "succeeded",
        "run_id": "run_0123456789abcdef0123456789abcdef",
        "access_verification": "verified",
        "checked_at": "2026-08-25T00:00:00Z",
        "entry_spec_sha256": verification_updater.provider_directory_entry_sha256(
            manifest_entry
        ),
        "proof_state": "superseded",
        "superseded_reason": "newer_active_run",
        "current_observation": {
            "run_id": active_run_id,
            "state_status": "observed",
            "run_status": "running",
            "observed_at": "2026-08-25T01:00:00Z",
        },
    }
    report_entry_by_field = {
        "status": "observed",
        "current_run_id": active_run_id,
        "last_run": {
            "run_id": active_run_id,
            "status": "running",
            "source_ids": manifest_entry["source_ids"],
        },
        "plan_bound": True,
    }

    merged = verification_updater._merge_nonterminal_observation(
        manifest_entry["entry_id"],
        manifest_entry,
        report_entry_by_field,
        prior_record_by_field,
        "2026-08-26T00:00:00Z",
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


@pytest.mark.parametrize("run_status", ["failed", "succeeded"])
def test_manual_observation_remains_unbound_without_source_params(
    tmp_path, capsys, run_status
):
    """Manual run labels cannot replace dedicated census evidence."""
    manifest = acquisition_cli.harness.load_manifest()
    manifest_entry = next(
        candidate_entry
        for candidate_entry in manifest["entries"]
        if candidate_entry["classification"] == "manual_acquisition"
    )
    operator_input_by_field = successful_operator_input(manifest, manifest_entry)
    operator_result_by_field = operator_input_by_field["results"][
        manifest_entry["entry_id"]
    ]
    operator_result_by_field["status"] = run_status
    operator_result_by_field["metrics"] = {}
    operator_input_path = tmp_path / "operator-input.json"
    operator_input_path.write_text(
        json.dumps(operator_input_by_field), encoding="utf-8"
    )

    acquisition_cli.run_acquisition_cli(
        [
            "--entry",
            manifest_entry["entry_id"],
            "--operator-input",
            str(operator_input_path),
            "--output",
            str(tmp_path / "report.json"),
            "--verification-report",
        ]
    )
    report_entry_by_field = json.loads(capsys.readouterr().out)["entries"][
        manifest_entry["entry_id"]
    ]

    assert report_entry_by_field["status"] == "observed"
    assert report_entry_by_field["plan_bound"] is False

    operator_result_by_field["params"] = {
        "source_ids": ["pdfhir_000000000000000000000000"]
    }
    operator_input_path.write_text(
        json.dumps(operator_input_by_field), encoding="utf-8"
    )
    with pytest.raises(acquisition_cli.harness.ManifestError, match="run identity"):
        acquisition_cli.run_acquisition_cli(
            [
                "--entry",
                manifest_entry["entry_id"],
                "--operator-input",
                str(operator_input_path),
                "--output",
                str(tmp_path / "report.json"),
                "--verification-report",
            ]
        )
