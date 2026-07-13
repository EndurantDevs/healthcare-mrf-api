import copy

import pytest

from scripts.research import provider_directory_endpoint_acquisition_harness as harness
from tests.test_provider_directory_endpoint_acquisition_harness import (
    FakeImportControl,
    _manifest_entry,
    _run_record,
)


def _adoption_config(tmp_path, run_id, entry_id="idaho"):
    return harness.HarnessConfig(
        tmp_path / "state.json",
        tmp_path / "report.json",
        apply=True,
        selected_entry_ids=frozenset({entry_id}),
        poll_interval_seconds=0,
        retry_wait_seconds=0,
        adopt_run_ids=((entry_id, run_id),),
    )


def _monthly_schedule_run(manifest, entry, run_id="run_nebraska", status="succeeded"):
    run_record = _run_record(manifest, entry, run_id, status)
    run_record["params"].update(
        {
            "refresh_preset": "monthly-full",
            "concurrency": 12,
            "open_only": False,
            "include_auth_required": True,
            "linked_resource_deadline_seconds": 1800,
        }
    )
    for omitted_param in ("probe", "resources", "retest_results_url"):
        run_record["params"].pop(omitted_param)
    return run_record


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


def test_adopt_run_accepts_exact_nonpublishing_monthly_schedule_profile(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "nebraska")
    monthly_run = _monthly_schedule_run(manifest, entry)
    control = FakeImportControl(runs=[monthly_run])

    state = harness.AcquisitionHarness(
        manifest,
        control,
        _adoption_config(tmp_path, "run_nebraska", "nebraska"),
        sleeper=lambda _seconds: None,
    ).execute_campaign()

    assert state["entries"]["nebraska"]["action"] == "adopt"
    assert state["entries"]["nebraska"]["status"] == "succeeded"


def test_monthly_preset_adoption_accepts_importer_alias_and_explicit_resource_list():
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "nebraska")
    monthly_run = _monthly_schedule_run(manifest, entry)
    monthly_run["params"].update(
        {
            "refresh_preset": "monthly_full",
            "concurrency": "12",
            "resources": list(entry["resources"]),
        }
    )

    assert harness._run_param_errors(manifest, entry, monthly_run) == []


def test_monthly_adoption_without_resources_still_requires_exact_terminal_completion():
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "nebraska")
    monthly_run = _monthly_schedule_run(manifest, entry)
    monthly_run["metrics"]["resource_fetch_completed_source_ids"].pop(
        "PractitionerRole"
    )

    errors = harness.terminal_metric_errors(manifest, entry, monthly_run)

    assert "PractitionerRole did not complete for the exact source" in errors


def test_monthly_preset_adoption_rejects_unknown_preset_and_missing_safety_value():
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "nebraska")
    unknown_preset_run = _monthly_schedule_run(manifest, entry, "run_unknown")
    unknown_preset_run["params"]["refresh_preset"] = "weekly"

    assert harness._run_param_errors(manifest, entry, unknown_preset_run) == [
        "params.refresh_preset is not the audited monthly-full preset"
    ]

    unsafe_monthly_run = _monthly_schedule_run(manifest, entry, "run_unsafe")
    unsafe_monthly_run["params"]["stale_cleanup"] = None

    assert any(
        "params.stale_cleanup" in error
        for error in harness._run_param_errors(manifest, entry, unsafe_monthly_run)
    )


@pytest.mark.parametrize(
    ("param_name", "bad_value"),
    [
        ("source_ids", ["pdfhir_111111111111111111111111"]),
        ("provider_directory_endpoint_scope", "https://example.invalid/fhir"),
        ("resources", ["InsurancePlan", "Practitioner"]),
        ("retest_results_url", "https://example.invalid/retest.json"),
        ("concurrency", 1),
        ("open_only", True),
        ("include_auth_required", False),
        ("linked_resource_deadline_seconds", 0),
        ("page_limit", 1),
        ("resource_limit", 1),
        ("source_concurrency", 2),
        ("bulk_export", True),
        ("stale_cleanup", True),
        ("publish_artifacts", True),
        ("publish_after_acquisition", True),
        ("publish_corroboration", True),
        ("seed_only", True),
    ],
)
def test_monthly_preset_adoption_keeps_audited_scope_profile_and_safety_strict(
    param_name,
    bad_value,
):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "nebraska")
    monthly_run = _monthly_schedule_run(manifest, entry, "run_monthly")
    monthly_run["params"][param_name] = bad_value

    errors = harness._run_param_errors(manifest, entry, monthly_run)

    assert any(param_name in error for error in errors)


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
