import copy
import json

import pytest

from scripts.research import provider_directory_endpoint_acquisition_harness as harness


class FakeImportControl:
    def __init__(self, *, runs=None, create_responses=None, transitions=None):
        self.runs_by_id = {run["run_id"]: copy.deepcopy(run) for run in runs or []}
        self.create_responses = [copy.deepcopy(run) for run in create_responses or []]
        self.transitions_by_id = {
            run_id: [copy.deepcopy(run) for run in sequence]
            for run_id, sequence in (transitions or {}).items()
        }
        self.created_requests = []

    def list_runs(self):
        return [copy.deepcopy(run) for run in self.runs_by_id.values()]

    def get_run(self, run_id):
        sequence = self.transitions_by_id.get(run_id, [])
        if sequence:
            self.runs_by_id[run_id] = copy.deepcopy(sequence.pop(0))
        return copy.deepcopy(self.runs_by_id[run_id])

    def create_run(self, request):
        self.created_requests.append(copy.deepcopy(request))
        run = self.create_responses.pop(0)
        self.runs_by_id[run["run_id"]] = copy.deepcopy(run)
        return copy.deepcopy(run)


def _entry(manifest):
    return next(entry for entry in manifest["entries"] if entry["entry_id"] == "idaho")


def _metrics(entry):
    return {
        "source_ids": list(entry["source_ids"]),
        "sources_probed": 1,
        "resource_rows": {},
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "source_import_sources_selected": 1,
        "source_import_groups_attempted": 1,
        "resource_fetch_completed_source_ids": {resource: list(entry["source_ids"]) for resource in entry["resources"]},
        "resource_fetch_stats": {
            resource: {"sources_completed": 1, "sources_bounded": 0, "sources_failed": 0}
            for resource in entry["resources"]
        },
    }


def _run(manifest, entry, run_id, status="succeeded", metrics=None):
    return {
        "run_id": run_id,
        "importer": manifest["importer"],
        "status": status,
        "params": harness.entry_params(manifest, entry),
        "metrics": metrics or _metrics(entry),
        "error": None,
    }


def _seed_state(tmp_path, manifest, entry, run, status):
    state_dict = {
        "schema_version": 1,
        "entries": {
            "idaho": {
                "status": status,
                "run_ids": [run["run_id"]],
                "root_run_id": run["run_id"],
                "current_run_id": run["run_id"],
                "spec_sha256": harness._entry_fingerprint(manifest, entry),
            }
        },
    }
    (tmp_path / "state.json").write_text(json.dumps(state_dict), encoding="utf-8")


def _execute(tmp_path, manifest, control, *, apply=True, entry_ids=("idaho",)):
    config = harness.HarnessConfig(
        tmp_path / "state.json",
        tmp_path / "report.json",
        apply,
        frozenset(entry_ids),
        0,
        0,
        frozenset(["idaho"]),
    )
    return harness.AcquisitionHarness(manifest, control, config, sleeper=lambda _: None).execute_campaign()


def test_restart_entry_starts_fresh_root_and_persists_prior_lineage(tmp_path):
    manifest = harness.load_manifest()
    entry = _entry(manifest)
    failed_metrics = _metrics(entry)
    failed_metrics["resource_fetch_stats"]["InsurancePlan"]["sources_completed"] = 0
    prior_run = _run(manifest, entry, "run_idaho_metric_failed", metrics=failed_metrics)
    fresh_run = _run(manifest, entry, "run_idaho_fresh")
    _seed_state(tmp_path, manifest, entry, prior_run, "metric_validation_failed")
    control = FakeImportControl(
        runs=[prior_run],
        create_responses=[fresh_run],
        transitions={fresh_run["run_id"]: [fresh_run]},
    )

    state = _execute(tmp_path, manifest, control, entry_ids=())
    entry_state = state["entries"]["idaho"]
    assert control.created_requests[0]["idempotency_key"] != harness.create_run_payload(manifest, entry)["idempotency_key"]
    assert control.created_requests[0]["params"].get("retry_of_run_id") is None
    assert entry_state["status"] == "succeeded"
    assert entry_state["root_run_id"] == entry_state["current_run_id"] == fresh_run["run_id"]
    assert entry_state["run_ids"] == [prior_run["run_id"], fresh_run["run_id"]]
    audit = entry_state["restart_history"][0]
    assert audit["prior_lineage"] == {
        "root_run_id": prior_run["run_id"],
        "current_run_id": prior_run["run_id"],
        "run_ids": [prior_run["run_id"]],
    }
    assert audit["prior_last_run"] == harness._run_summary(prior_run)
    assert json.loads((tmp_path / "state.json").read_text())["entries"]["idaho"]["restart_history"] == entry_state["restart_history"]


def test_restart_entry_refuses_active_current_run(tmp_path):
    manifest = harness.load_manifest()
    entry = _entry(manifest)
    active_run = _run(manifest, entry, "run_idaho_active", "running")
    _seed_state(tmp_path, manifest, entry, active_run, "failed")
    control = FakeImportControl(runs=[active_run])
    with pytest.raises(harness.HarnessConflict, match="is active"):
        _execute(tmp_path, manifest, control)
    assert control.created_requests == []


@pytest.mark.parametrize("status", ["succeeded", "external_completed"])
def test_restart_entry_refuses_successful_or_external_state(tmp_path, status):
    manifest = harness.load_manifest()
    entry = _entry(manifest)
    completed_run = _run(manifest, entry, "run_idaho_done")
    _seed_state(tmp_path, manifest, entry, completed_run, status)
    control = FakeImportControl(runs=[completed_run])
    with pytest.raises(harness.HarnessConflict, match="not a retryable terminal failure"):
        _execute(tmp_path, manifest, control)
    assert control.created_requests == []


def test_restart_entry_refuses_dry_run(tmp_path):
    manifest = harness.load_manifest()
    control = FakeImportControl()
    with pytest.raises(harness.HarnessConflict, match="requires --apply"):
        _execute(tmp_path, manifest, control, apply=False)
    with pytest.raises(SystemExit, match="requires --apply"):
        harness.main(["--restart-entry", "idaho"])
    assert control.created_requests == []
