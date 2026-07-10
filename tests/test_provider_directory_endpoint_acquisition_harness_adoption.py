import copy

import pytest

from scripts.research import provider_directory_endpoint_acquisition_harness as harness
from tests.test_provider_directory_endpoint_acquisition_harness import (
    FakeImportControl,
    _manifest_entry,
    _run_record,
)


def _adoption_config(tmp_path, run_id):
    return harness.HarnessConfig(
        tmp_path / "state.json",
        tmp_path / "report.json",
        apply=True,
        selected_entry_ids=frozenset({"idaho"}),
        poll_interval_seconds=0,
        retry_wait_seconds=0,
        adopt_run_ids=(("idaho", run_id),),
    )


def test_adopt_run_attaches_exact_manual_retry_tip(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "idaho")
    root_run = _run_record(manifest, entry, "run_root", "failed")
    adopted_running = _run_record(
        manifest,
        entry,
        "run_adopted",
        "running",
        extra_params={
            "retry_of_run_id": "run_manual_parent",
            "provider_directory_pagination_root_run_id": "run_root",
        },
    )
    adopted_succeeded = copy.deepcopy(adopted_running)
    adopted_succeeded["status"] = "succeeded"
    control = FakeImportControl(
        runs=[root_run, adopted_running],
        transitions={"run_adopted": [adopted_running, adopted_succeeded]},
    )

    state = harness.AcquisitionHarness(
        manifest,
        control,
        _adoption_config(tmp_path, "run_adopted"),
        sleeper=lambda _seconds: None,
    ).execute_campaign()

    entry_state = state["entries"]["idaho"]
    assert control.created_requests == []
    assert entry_state["action"] == "adopt"
    assert entry_state["root_run_id"] == "run_root"
    assert entry_state["current_run_id"] == "run_adopted"
    assert entry_state["run_ids"] == ["run_root", "run_adopted"]
    assert entry_state["status"] == "succeeded"


def test_adopt_run_rejects_manifest_mismatch(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "idaho")
    mismatched_run = _run_record(manifest, entry, "run_wrong", "succeeded")
    mismatched_run["params"]["source_ids"] = ["pdfhir_111111111111111111111111"]
    control = FakeImportControl(runs=[mismatched_run])

    with pytest.raises(harness.HarnessConflict, match="adopted run does not match"):
        harness.AcquisitionHarness(
            manifest,
            control,
            _adoption_config(tmp_path, "run_wrong"),
        ).execute_campaign()


def test_adopt_run_rejects_non_tip(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "idaho")
    root_run = _run_record(manifest, entry, "run_root", "failed")
    adopted_run = _run_record(
        manifest,
        entry,
        "run_adopted",
        "failed",
        extra_params={
            "retry_of_run_id": "run_root",
            "provider_directory_pagination_root_run_id": "run_root",
        },
    )
    child_run = _run_record(
        manifest,
        entry,
        "run_child",
        "running",
        extra_params={
            "retry_of_run_id": "run_adopted",
            "provider_directory_pagination_root_run_id": "run_root",
        },
    )
    control = FakeImportControl(runs=[root_run, adopted_run, child_run])

    with pytest.raises(harness.HarnessConflict, match="not the lineage tip"):
        harness.AcquisitionHarness(
            manifest,
            control,
            _adoption_config(tmp_path, "run_adopted"),
        ).execute_campaign()


@pytest.mark.parametrize(
    ("raw_values", "message"),
    [
        (["idaho"], "ENTRY=RUN_ID"),
        (["Idaho=run_1"], "ENTRY=RUN_ID"),
        (["idaho=wrong_1"], "ENTRY=RUN_ID"),
        (["idaho=run_1", "idaho=run_2"], "duplicate"),
    ],
)
def test_parse_adopt_runs_rejects_ambiguous_values(raw_values, message):
    with pytest.raises(SystemExit, match=message):
        harness.parse_adopt_runs(raw_values, harness.SLUG_PATTERN)
