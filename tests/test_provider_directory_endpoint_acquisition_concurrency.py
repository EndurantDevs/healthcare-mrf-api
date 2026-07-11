"""Concurrency coverage for Provider Directory endpoint acquisition campaigns."""

import pytest

from scripts.research import provider_directory_endpoint_acquisition_harness as harness
from scripts.research.provider_directory_endpoint_acquisition_concurrency import (
    campaign_state_lock,
)
from tests.test_provider_directory_endpoint_acquisition_harness import (
    FakeImportControl,
    _execute_case,
    _manifest_entry,
    _run_record,
)


def test_disjoint_active_run_allows_next_endpoint(tmp_path):
    manifest = harness.load_manifest()
    molina = _manifest_entry(manifest, "molina")
    active_run = _run_record(manifest, molina, "run_other", "running")
    active_run["params"].pop("provider_directory_endpoint_scope")
    active_run["metrics"]["active_source_groups"] = [
        {"api_base": molina["canonical_base"]}
    ]
    idaho = _manifest_entry(manifest, "idaho")
    completed_run = _run_record(manifest, idaho, "run_idaho")
    control = FakeImportControl(
        runs=[active_run],
        create_responses=[completed_run],
        transitions={"run_idaho": [completed_run]},
    )

    state = _execute_case(tmp_path, manifest, control, ["idaho"])

    assert len(control.created_requests) == 1
    assert state["entries"]["idaho"]["status"] == "succeeded"


@pytest.mark.parametrize("overlap_kind", ["source", "endpoint"])
def test_overlapping_active_run_still_blocks(tmp_path, overlap_kind):
    manifest = harness.load_manifest()
    idaho = _manifest_entry(manifest, "idaho")
    molina = _manifest_entry(manifest, "molina")
    active_run = _run_record(manifest, molina, "run_other", "running")
    if overlap_kind == "source":
        active_run["params"]["source_ids"] = list(idaho["source_ids"])
    else:
        active_run["params"]["provider_directory_endpoint_scope"] = idaho[
            "canonical_base"
        ]
    control = FakeImportControl(runs=[active_run])

    state = _execute_case(tmp_path, manifest, control, ["idaho"])

    assert control.created_requests == []
    assert state["entries"]["idaho"]["status"] == "active_conflict"
    assert "run_other" in state["entries"]["idaho"]["message"]


def test_campaign_state_lock_rejects_second_writer(tmp_path):
    state_path = tmp_path / "state.json"

    with campaign_state_lock(state_path):
        with pytest.raises(harness.HarnessConflict, match="already locked"):
            with campaign_state_lock(state_path):
                raise AssertionError("second campaign unexpectedly acquired the state lock")


def test_mismatched_campaign_client_tag_does_not_attach(tmp_path):
    manifest = harness.load_manifest()
    idaho = _manifest_entry(manifest, "idaho")
    cigna = _manifest_entry(manifest, "cigna")
    contaminated_cigna_run = _run_record(manifest, cigna, "run_cigna", "running")
    contaminated_cigna_run["metrics"]["client_ids"] = [
        f"{manifest['campaign_id']}:idaho"
    ]
    completed_idaho_run = _run_record(manifest, idaho, "run_idaho")
    control = FakeImportControl(
        runs=[contaminated_cigna_run],
        create_responses=[completed_idaho_run],
        transitions={"run_idaho": [completed_idaho_run]},
    )

    state = _execute_case(tmp_path, manifest, control, ["idaho"])

    assert len(control.created_requests) == 1
    assert state["entries"]["idaho"]["current_run_id"] == "run_idaho"
    assert state["entries"]["idaho"]["status"] == "succeeded"
