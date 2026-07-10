import copy
import json
import subprocess
import sys

import pytest

from scripts import generate_provider_directory_support_docs as generator
from scripts import update_provider_directory_verification as updater
from scripts.research import provider_directory_endpoint_acquisition_harness as harness


def _manifest_and_snapshot():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    snapshot = generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    return manifest, snapshot


def _report(manifest, entry_id="idaho", status="succeeded", run_id="run_idaho"):
    """Build a credential-bearing harness-shaped report for updater boundary tests."""
    manifest_entry = next((entry for entry in manifest["entries"] if entry["entry_id"] == entry_id), None)
    source_ids = list(manifest_entry["source_ids"]) if manifest_entry else []
    resources = list(manifest_entry["resources"]) if manifest_entry else []
    run_status = status if status in harness.ACTIVE_STATUSES else "succeeded"
    run_summary = harness._run_summary(
        {
            "run_id": run_id,
            "status": run_status,
            "params": {},
            "metrics": {
                "source_ids": source_ids,
                "sources_probed": len(source_ids),
                "source_import_sources_selected": len(source_ids),
                "source_import_groups_attempted": 1,
                "resource_fetch_completed_source_ids": {
                    resource: source_ids for resource in resources
                },
                "resource_fetch_stats": {
                    resource: {
                        "sources_attempted": len(source_ids),
                        "sources_completed": len(source_ids),
                        "sources_bounded": 0,
                        "sources_failed": 0,
                    }
                    for resource in resources
                },
            },
            "error": {"message": "safe terminal detail", "authorization": "SECRET"},
        }
    )
    entry_state_by_id = {
        entry_id: {
            "status": status,
            "current_run_id": run_id,
            "last_run": run_summary,
            "error": {"token": "SECRET_ENTRY_ERROR"},
        }
    }
    campaign_state_dict = {
        "updated_at": "2026-07-10T18:00:00Z",
        "manifest_sha256": updater._manifest_sha256(manifest),
        "entries": entry_state_by_id,
    }
    is_terminal = status in harness.VERIFICATION_TERMINAL_STATUSES
    report_dict = {
        "schema_version": 1, "generated_at": campaign_state_dict["updated_at"], "mode": "apply",
        "campaign_id": manifest["campaign_id"], "manifest_sha256": campaign_state_dict["manifest_sha256"],
        "entries": campaign_state_dict["entries"],
        "verification_update": {
            "eligible": is_terminal, "selected_entry_ids": [entry_id],
            "terminal_entry_ids": [entry_id] if is_terminal else [],
            "nonterminal_entry_ids": [] if is_terminal else [entry_id],
            "argv": ["python", "scripts/update_provider_directory_verification.py", "--report", "report.json", "--environment", "<environment>"],
        },
    }
    report_dict["credentials"] = "SHOULD NOT BE COPIED"
    return report_dict


def _persist_harness_report(tmp_path, monkeypatch, manifest, report_dict):
    """Write a report through the harness persistence boundary for lifecycle tests."""
    report_path = tmp_path / "report.json"
    campaign = object.__new__(harness.AcquisitionHarness)
    campaign.manifest = manifest
    campaign.config = harness.HarnessConfig(
        tmp_path / "state.json", report_path, apply=True,
        selected_entry_ids=frozenset(report_dict["entries"]),
    )
    campaign.state = {
        "schema_version": 1,
        "manifest_sha256": report_dict["manifest_sha256"],
        "entries": copy.deepcopy(report_dict["entries"]),
    }
    for entry_state_dict in campaign.state["entries"].values():
        entry_state_dict.pop("error", None)
    monkeypatch.setattr(harness, "_utc_now", lambda: report_dict["generated_at"])
    campaign._persist()
    persisted_report_dict = json.loads(report_path.read_text(encoding="utf-8"))
    persisted_report_dict["credentials"] = "SHOULD NOT BE COPIED"
    report_path.write_text(json.dumps(persisted_report_dict), encoding="utf-8")
    return report_path, persisted_report_dict


def test_unknown_campaign_is_rejected():
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest)
    report["campaign_id"] = "different-campaign"

    with pytest.raises(updater.VerificationUpdateError, match="campaign_id"):
        updater.update_verification_snapshot(manifest, report, snapshot, "healthporta-dev")


def test_unknown_entry_is_rejected():
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest, entry_id="not-a-manifest-entry")

    with pytest.raises(updater.VerificationUpdateError, match="unknown entries"):
        updater.update_verification_snapshot(manifest, report, snapshot, "healthporta-dev")


def test_unknown_manifest_hash_is_rejected():
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest)
    report["manifest_sha256"] = "0" * 64

    with pytest.raises(updater.VerificationUpdateError, match="manifest_sha256"):
        updater.update_verification_snapshot(manifest, report, snapshot, "healthporta-dev")


def test_successful_update_records_safe_terminal_fields_and_regenerates_docs(tmp_path, monkeypatch):
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest)
    report_path, report = _persist_harness_report(tmp_path, monkeypatch, manifest, report)
    snapshot_path = tmp_path / "verification.json"
    output_path = tmp_path / "support.md"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    updated = updater.update_files(
        report_path=report_path,
        snapshot_path=snapshot_path,
        output_path=output_path,
        environment="healthporta-dev",
    )

    assert updated["environment"] == "healthporta-dev"
    assert updated["checked_at"] == "2026-07-10T18:00:00Z"
    idaho_record = updated["entries"]["idaho"]
    assert {
        field_name: idaho_record[field_name]
        for field_name in ("terminal_status", "run_id", "access_verification", "checked_at")
    } == {
        "terminal_status": "succeeded",
        "run_id": "run_idaho",
        "access_verification": "verified",
        "checked_at": "2026-07-10T18:00:00Z",
    }
    assert idaho_record["proof_state"] == "current"
    assert idaho_record["terminal_evidence"]["source_ids"] == next(
        entry["source_ids"] for entry in manifest["entries"] if entry["entry_id"] == "idaho"
    )
    assert "InsurancePlan" in idaho_record["terminal_evidence"]["resource_outcomes"]
    assert updated["report_identity"]["mode"] == "apply"
    assert report["verification_update"]["eligible"] is True
    assert report["verification_update"]["argv"][1] == "scripts/update_provider_directory_verification.py"
    serialized_snapshot = snapshot_path.read_text(encoding="utf-8")
    serialized_docs = output_path.read_text(encoding="utf-8")
    assert "SECRET" not in serialized_snapshot
    assert "SECRET" not in serialized_docs
    assert '"error"' not in serialized_snapshot
    assert "| Idaho (`idaho`) | Current | Succeeded | run_idaho | Succeeded (`run_idaho`)" in serialized_docs
    assert '"InsurancePlan": {' in serialized_docs
    assert '"sources_bounded": 0' in serialized_docs
    assert "| Molina (`molina`) | Not recorded | Not recorded | Not recorded | Not recorded" in serialized_docs


def test_bounded_terminal_evidence_preserves_safe_structured_details():
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest, status="bounded", run_id="run_bounded")
    report_entry = report["entries"]["idaho"]
    report_entry["metric_errors"] = ["InsurancePlan was bounded or failed"]
    report_entry["last_run"]["resource_outcomes"]["InsurancePlan"]["sources_bounded"] = 1

    updated = updater.update_verification_snapshot(
        manifest, report, snapshot, "healthporta-dev"
    )
    evidence = updated["entries"]["idaho"]["terminal_evidence"]

    assert evidence["bounded_reasons"] == ["InsurancePlan was bounded or failed"]
    assert evidence["terminal_error"] == {"message": "safe terminal detail"}
    assert evidence["effective_acquisition"]["selected_sources"] == 1
    assert evidence["resource_outcomes"]["InsurancePlan"]["sources_bounded"] == 1


def test_newer_nonterminal_report_marks_prior_terminal_proof_superseded():
    manifest, snapshot = _manifest_and_snapshot()
    first_report = _report(manifest)
    prior = updater.update_verification_snapshot(
        manifest, first_report, copy.deepcopy(snapshot), "healthporta-dev"
    )
    report = _report(manifest, status="running", run_id="run_idaho_new")
    report["generated_at"] = "2026-07-10T19:00:00Z"

    updated = updater.update_verification_snapshot(manifest, report, prior, "healthporta-dev")
    rendered = generator.render_markdown(manifest, verification_snapshot=updated)

    idaho_verification = updated["entries"]["idaho"]
    assert idaho_verification["run_id"] == "run_idaho"
    assert idaho_verification["terminal_status"] == "succeeded"
    assert idaho_verification["proof_state"] == "superseded"
    assert idaho_verification["current_observation"]["run_id"] == "run_idaho_new"
    assert "| Idaho (`idaho`) | Superseded | Succeeded | run_idaho | Running (`run_idaho_new`)" in rendered


def test_terminal_label_backed_by_active_run_is_rejected():
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest)
    report["entries"]["idaho"]["last_run"]["status"] = "running"

    with pytest.raises(updater.VerificationUpdateError, match="cannot be recorded as terminal"):
        updater.update_verification_snapshot(manifest, report, snapshot, "healthporta-dev")


def test_older_report_is_rejected():
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest)
    report["generated_at"] = "2026-07-10T12:00:00Z"

    with pytest.raises(updater.VerificationUpdateError, match="older than"):
        updater.update_verification_snapshot(manifest, report, snapshot, "healthporta-dev")


def test_generator_failure_leaves_snapshot_and_docs_unchanged(tmp_path, monkeypatch):
    manifest, snapshot = _manifest_and_snapshot()
    report_path = tmp_path / "report.json"
    snapshot_path = tmp_path / "verification.json"
    output_path = tmp_path / "support.md"
    report_path.write_text(json.dumps(_report(manifest)), encoding="utf-8")
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    output_path.write_text("prior docs\n", encoding="utf-8")
    prior_snapshot = snapshot_path.read_bytes()
    prior_docs = output_path.read_bytes()
    monkeypatch.setattr(generator, "render_markdown", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("render failed")))

    with pytest.raises(RuntimeError, match="render failed"):
        updater.update_files(
            report_path=report_path,
            snapshot_path=snapshot_path,
            output_path=output_path,
            environment="healthporta-dev",
        )

    assert snapshot_path.read_bytes() == prior_snapshot
    assert output_path.read_bytes() == prior_docs


def test_updater_direct_script_imports_from_its_own_directory():
    result = subprocess.run(
        [sys.executable, str(updater.ROOT / "scripts/update_provider_directory_verification.py"), "--help"],
        cwd=updater.ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "--environment" in result.stdout
