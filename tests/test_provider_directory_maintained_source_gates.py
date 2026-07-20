# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import json

import pytest

from scripts.smoke import provider_directory_maintained_source_gates as gates


def _maintained_sources():
    return gates.load_maintained_sources()


def _observation_payload_by_kind():
    acquisition_list = []
    artifact_list = []
    unified_list = []
    api_list = []
    latency_list = []
    for maintained_source in _maintained_sources():
        identity_by_field = {
            "entry_id": maintained_source["entry_id"],
            "source_id": maintained_source["source_id"],
        }
        dataset_id = f"pdds-{maintained_source['entry_id']}"
        acquisition_list.append({**identity_by_field, "dataset_id": dataset_id, "is_current": True, "terminal_state": "succeeded"})
        artifact_list.append({**identity_by_field, "dataset_id": dataset_id, "mapped_artifact_count": 1})
        unified_list.append(
            {
                **identity_by_field,
                "dataset_id": dataset_id,
                "source_evidence_count": 1,
                "phone_evidence_count": 1,
                "geo_evidence_count": 1,
            }
        )
        api_list.append({**identity_by_field, "dataset_id": dataset_id, "profile_status": "ready", "evidence_status": "ready"})
        latency_list.append({**identity_by_field, "dataset_id": dataset_id, "search_latency_ms": 40.0, "detail_latency_ms": 40.0})
    return {
        "acquisition": acquisition_list,
        "artifacts": artifact_list,
        "unified": unified_list,
        "api": api_list,
        "latency": latency_list,
    }


def _observation_by_entry_by_kind(observation_payload_by_kind):
    return {
        label: {observation["entry_id"]: observation for observation in observation_list}
        for label, observation_list in observation_payload_by_kind.items()
    }


def _report(observation_payload_by_kind):
    observation_maps = _observation_by_entry_by_kind(observation_payload_by_kind)
    return gates.build_report(
        maintained_sources=_maintained_sources(),
        acquisition_by_entry=observation_maps["acquisition"],
        artifacts_by_entry=observation_maps["artifacts"],
        unified_by_entry=observation_maps["unified"],
        api_by_entry=observation_maps["api"],
        latency_by_entry=observation_maps["latency"],
        latency_slo_ms=40.0,
    )


def _write_observations(tmp_path, observation_payload_by_kind):
    path_by_label = {}
    for label, observation_list in observation_payload_by_kind.items():
        path = tmp_path / f"{label}.json"
        path.write_text(json.dumps({"observations": observation_list}), encoding="utf-8")
        path_by_label[label] = path
    return path_by_label


def test_all_21_maintained_sources_can_produce_ready_rows_and_table():
    report = _report(_observation_payload_by_kind())

    assert report["maintained_source_count"] == 21
    assert report["ready"] is True
    assert len(report["sources"]) == 21
    assert {row["status"] for row in report["sources"]} == {"ready"}
    table = gates.format_table(report)
    assert table.count("\n") == 22
    assert "aetna-commercial-medicare" in table
    assert "current_terminal_acquisition" not in table


def test_aetna_active_and_humana_failed_are_not_ready():
    observation_payload_by_kind = _observation_payload_by_kind()
    by_entry = _observation_by_entry_by_kind(observation_payload_by_kind)
    by_entry["acquisition"]["aetna-commercial-medicare"]["terminal_state"] = "active"
    by_entry["acquisition"]["humana"]["terminal_state"] = "failed"

    row_by_entry = {row["entry_id"]: row for row in _report(observation_payload_by_kind)["sources"]}

    assert row_by_entry["aetna-commercial-medicare"]["statuses"]["current_terminal_acquisition"] == "not_ready"
    assert row_by_entry["humana"]["statuses"]["current_terminal_acquisition"] == "failed"


def test_missing_profile_remains_unknown_without_static_inference():
    observation_payload_by_kind = _observation_payload_by_kind()
    _observation_by_entry_by_kind(observation_payload_by_kind)["api"]["cigna"].pop("profile_status")

    row_by_entry = {row["entry_id"]: row for row in _report(observation_payload_by_kind)["sources"]}

    assert row_by_entry["cigna"]["statuses"]["npi_profile_evidence_api"] == "unknown"
    assert row_by_entry["cigna"]["status"] == "unknown"


def test_rejects_source_and_dataset_mismatches(tmp_path):
    observation_payload_by_kind = _observation_payload_by_kind()
    by_entry = _observation_by_entry_by_kind(observation_payload_by_kind)
    by_entry["artifacts"]["cigna"]["dataset_id"] = "pdds-stale-cigna"

    with pytest.raises(gates.ObservationValidationError, match="artifacts dataset_id mismatch for cigna"):
        _report(observation_payload_by_kind)

    by_entry["artifacts"]["cigna"]["dataset_id"] = "pdds-cigna"
    by_entry["artifacts"]["cigna"]["source_id"] = by_entry["artifacts"]["humana"]["source_id"]
    artifact_path = tmp_path / "artifacts.json"
    artifact_path.write_text(json.dumps(observation_payload_by_kind["artifacts"]), encoding="utf-8")
    with pytest.raises(gates.ObservationValidationError, match="source_id mismatch for cigna"):
        gates.load_source_observations(
            artifact_path,
            label="artifacts",
            maintained_sources=_maintained_sources(),
        )


def test_observation_loader_rejects_duplicate_missing_and_wrong_source_ids(tmp_path):
    observation_payload_by_kind = _observation_payload_by_kind()
    artifact_list = observation_payload_by_kind["artifacts"]
    artifact_list.append(copy.deepcopy(artifact_list[0]))
    artifact_path = tmp_path / "artifacts.json"
    artifact_path.write_text(json.dumps(artifact_list), encoding="utf-8")

    with pytest.raises(gates.ObservationValidationError, match="duplicate observation"):
        gates.load_source_observations(artifact_path, label="artifacts", maintained_sources=_maintained_sources())

    artifact_list.pop()
    artifact_list.pop()
    artifact_path.write_text(json.dumps(artifact_list), encoding="utf-8")
    with pytest.raises(gates.ObservationValidationError, match="misses maintained sources"):
        gates.load_source_observations(artifact_path, label="artifacts", maintained_sources=_maintained_sources())

    artifact_list.append(copy.deepcopy(_observation_payload_by_kind()["artifacts"][-1]))
    artifact_list[0]["source_id"] = artifact_list[1]["source_id"]
    artifact_path.write_text(json.dumps(artifact_list), encoding="utf-8")
    with pytest.raises(gates.ObservationValidationError, match="source_id mismatch"):
        gates.load_source_observations(artifact_path, label="artifacts", maintained_sources=_maintained_sources())


def test_exact_40ms_latency_boundary_is_ready():
    report = _report(_observation_payload_by_kind())

    assert report["sources"][0]["statuses"]["search_detail_latency"] == "ready"
    assert report["sources"][0]["latency_metrics"] == {"detail_latency_ms": 40.0, "search_latency_ms": 40.0}


def test_cli_strict_mode_fails_for_active_aetna_and_emits_deterministic_json(tmp_path, capsys):
    observation_payload_by_kind = _observation_payload_by_kind()
    _observation_by_entry_by_kind(observation_payload_by_kind)["acquisition"]["aetna-commercial-medicare"]["terminal_state"] = "active"
    path_by_label = _write_observations(tmp_path, observation_payload_by_kind)

    exit_code = gates.run_gate(
        [
            "--acquisition-observations", str(path_by_label["acquisition"]),
            "--artifact-observations", str(path_by_label["artifacts"]),
            "--unified-observations", str(path_by_label["unified"]),
            "--api-observations", str(path_by_label["api"]),
            "--latency-observations", str(path_by_label["latency"]),
            "--format", "json",
            "--strict",
        ]
    )

    assert exit_code == 1
    rendered = capsys.readouterr().out
    assert json.loads(rendered)["ready"] is False
    assert rendered == json.dumps(json.loads(rendered), indent=2, sort_keys=True) + "\n"
