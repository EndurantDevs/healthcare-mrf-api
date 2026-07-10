import copy
import json
import subprocess
import sys

import pytest

from scripts import generate_provider_directory_support_docs as generator
from scripts import update_provider_directory_verification as updater


def _manifest_and_snapshot():
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    snapshot = generator.load_verification_snapshot(generator.DEFAULT_VERIFICATION_SNAPSHOT)
    return manifest, snapshot


def _report(manifest, entry_id="idaho", status="succeeded", run_id="run_idaho"):
    return {
        "schema_version": 1,
        "generated_at": "2026-07-10T12:00:00Z",
        "campaign_id": manifest["campaign_id"],
        "manifest_sha256": updater._manifest_sha256(manifest),
        "credentials": "SHOULD NOT BE COPIED",
        "entries": {
            entry_id: {
                "status": status,
                "current_run_id": run_id,
                "last_run": {
                    "run_id": run_id,
                    "status": "succeeded",
                    "error": {"authorization": "SECRET_ERROR_PAYLOAD"},
                },
                "error": {"token": "SECRET_ENTRY_ERROR"},
            }
        },
    }


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


def test_successful_update_records_safe_terminal_fields_and_regenerates_docs(tmp_path):
    manifest, snapshot = _manifest_and_snapshot()
    report_path = tmp_path / "report.json"
    snapshot_path = tmp_path / "verification.json"
    output_path = tmp_path / "support.md"
    report_path.write_text(json.dumps(_report(manifest)), encoding="utf-8")
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    updated = updater.update_files(
        report_path=report_path,
        snapshot_path=snapshot_path,
        output_path=output_path,
        environment="healthporta-dev",
    )

    assert updated["environment"] == "healthporta-dev"
    assert updated["checked_at"] == "2026-07-10T12:00:00Z"
    assert updated["entries"]["idaho"] == {
        "terminal_status": "succeeded",
        "run_id": "run_idaho",
        "access_verification": "verified",
        "checked_at": "2026-07-10T12:00:00Z",
    }
    serialized_snapshot = snapshot_path.read_text(encoding="utf-8")
    serialized_docs = output_path.read_text(encoding="utf-8")
    assert "SECRET" not in serialized_snapshot
    assert "SECRET" not in serialized_docs
    assert '"error"' not in serialized_snapshot
    assert "| Idaho (`idaho`) | Succeeded | run_idaho | Verified | 2026-07-10T12:00:00Z |" in serialized_docs
    assert "| Molina (`molina`) | Not recorded | Not recorded | Not recorded | Not recorded |" in serialized_docs


def test_nonterminal_report_does_not_replace_unverified_display():
    manifest, snapshot = _manifest_and_snapshot()
    report = _report(manifest, status="running")

    updated = updater.update_verification_snapshot(manifest, report, copy.deepcopy(snapshot), "healthporta-dev")
    rendered = generator.render_markdown(manifest, verification_snapshot=updated)

    assert updated["entries"]["idaho"] == snapshot["entries"]["idaho"]
    assert "| Idaho (`idaho`) | Not recorded | Not recorded | Not recorded | Not recorded |" in rendered


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
