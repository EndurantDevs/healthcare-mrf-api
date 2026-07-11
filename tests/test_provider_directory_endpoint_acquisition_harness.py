import copy
import json
from pathlib import Path

import pytest

from scripts.research import provider_directory_endpoint_acquisition_harness as harness
from tests.provider_directory_endpoint_acquisition_test_support import manifest_with_attached_entry


class FakeImportControl:
    def __init__(self, *, runs=None, create_responses=None, transitions=None):
        self.runs_by_id = {run_record["run_id"]: copy.deepcopy(run_record) for run_record in (runs or [])}
        self.create_responses = [copy.deepcopy(run_record) for run_record in (create_responses or [])]
        self.transitions_by_id = {
            run_id: [copy.deepcopy(run_record) for run_record in run_sequence]
            for run_id, run_sequence in (transitions or {}).items()
        }
        self.created_requests = []
        self.events = []

    def list_runs(self):
        """Return all fake mirror rows."""
        self.events.append("list")
        return [copy.deepcopy(run_record) for run_record in self.runs_by_id.values()]

    def get_run(self, run_id):
        """Advance and return one fake run."""
        run_sequence = self.transitions_by_id.get(run_id, [])
        if run_sequence:
            run_record = run_sequence.pop(0)
            self.runs_by_id[run_id] = copy.deepcopy(run_record)
        run_record = self.runs_by_id[run_id]
        self.events.append(f"get:{run_id}:{run_record['status']}")
        return copy.deepcopy(run_record)

    def create_run(self, request_body):
        """Record one POST and return its configured run."""
        self.created_requests.append(copy.deepcopy(request_body))
        run_record = self.create_responses.pop(0)
        self.runs_by_id[run_record["run_id"]] = copy.deepcopy(run_record)
        self.events.append(f"create:{run_record['run_id']}")
        return copy.deepcopy(run_record)


def _manifest_entry(manifest, entry_id):
    return next(entry for entry in manifest["entries"] if entry["entry_id"] == entry_id)


def _success_metrics(entry):
    metrics_by_name = {
        "source_ids": list(entry["source_ids"]),
        "sources_probed": len(entry["source_ids"]),
        "resource_rows": {},
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
    }
    if entry["classification"] not in harness.ACQUISITION_CLASSIFICATIONS:
        return metrics_by_name
    metrics_by_name.update(
        {
            "source_import_sources_selected": len(entry["source_ids"]),
            "source_import_groups_attempted": 1,
            "resource_fetch_completed_source_ids": {
                resource_type: list(entry["source_ids"])
                for resource_type in entry["resources"]
            },
            "resource_fetch_stats": {
                resource_type: {
                    "sources_attempted": 1,
                    "sources_completed": 1,
                    "sources_bounded": 0,
                    "sources_failed": 0,
                }
                for resource_type in entry["resources"]
            },
        }
    )
    return metrics_by_name


def _run_record(manifest, entry, run_id, status="succeeded", metrics=None, extra_params=None):
    params_by_name = harness.entry_params(manifest, entry)
    params_by_name["provider_directory_endpoint_scope"] = entry["canonical_base"]
    params_by_name.update(extra_params or {})
    return {
        "run_id": run_id,
        "importer": "provider-directory-fhir",
        "status": status,
        "params": params_by_name,
        "metrics": copy.deepcopy(metrics if metrics is not None else _success_metrics(entry)),
        "error": None,
    }


def _case_config(tmp_path, entry_ids, *, apply=True, retry_wait=0, restart_entry_ids=()):
    return harness.HarnessConfig(
        tmp_path / "state.json",
        tmp_path / "report.json",
        apply,
        frozenset(entry_ids),
        0,
        retry_wait,
        frozenset(restart_entry_ids),
    )


def _execute_case(tmp_path, manifest, control, entry_ids, *, apply=True, retry_wait=0, restart_entry_ids=()):
    config = _case_config(tmp_path, entry_ids, apply=apply, retry_wait=retry_wait, restart_entry_ids=restart_entry_ids)
    campaign = harness.AcquisitionHarness(manifest, control, config, sleeper=lambda _seconds: None)
    return campaign.execute_campaign()


def test_dry_run_never_posts(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "idaho")
    control = FakeImportControl()

    state = _execute_case(tmp_path, manifest, control, ["idaho"], apply=False)

    assert control.created_requests == []
    assert state["entries"]["idaho"]["status"] == "planned"
    assert state["entries"]["idaho"]["request"]["params"] == harness.entry_params(manifest, entry)
    assert json.loads((tmp_path / "report.json").read_text())["mode"] == "dry-run"
def test_aetna_bulk_acquisition_payload_is_exact_and_audited():
    manifest = harness.load_manifest()
    aetna_entry = _manifest_entry(manifest, "aetna-commercial-medicare")

    assert len(manifest["entries"]) == 28
    assert aetna_entry == {
        "entry_id": "aetna-commercial-medicare",
        "display_name": "Aetna Commercial/Medicare",
        "owner_id": "aetna-commercial-medicare",
        "source_ids": ["pdfhir_d68a896335981928bdbbb80e"],
        "canonical_base": "https://apif1.aetna.com/fhir/v1/providerdirectorydata",
        "classification": "bulk_acquisition",
        "launch_mode": "create",
        "resource_profile": "A7",
        "resources": ["InsurancePlan", "PractitionerRole", "Practitioner", "Organization", "Location", "HealthcareService", "OrganizationAffiliation"],
    }
    expected_params_by_name = {
        "probe": True,
        "timeout": 60,
        "concurrency": 1,
        "include_supplemental_catalogs": True,
        "open_only": False,
        "include_auth_required": True,
        "import_resources": True,
        "full_refresh": True,
        "resource_limit": 0,
        "resource_deadline_seconds": 0,
        "linked_resource_limit": 0,
        "linked_resource_deadline_seconds": 0,
        "page_limit": 0,
        "page_count": 100,
        "stream_batch_size": 5000,
        "bulk_export": True,
        "source_concurrency": 1,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "retest_results_url": harness.RETEST_RESULTS_URL,
        "source_ids": ["pdfhir_d68a896335981928bdbbb80e"],
        "resources": "InsurancePlan,PractitionerRole,Practitioner,Organization,Location,HealthcareService,OrganizationAffiliation",
    }

    assert harness.entry_params(manifest, aetna_entry) == expected_params_by_name
    assert harness.create_run_payload(manifest, aetna_entry) == {
        "importer": "provider-directory-fhir",
        "engine": "healthcare-mrf-api",
        "params": expected_params_by_name,
        "provider_directory_endpoint_scope": "https://apif1.aetna.com/fhir/v1/providerdirectorydata",
        "idempotency_key": "1374e1a4c9ee0440892ed944c80d2039830d70c09bbd8757",
        "client_id": "provider-directory-canonical-acquisition-2026-07-10-v2:aetna-commercial-medicare",
        "triggered_by": "endpoint_acquisition",
    }
@pytest.mark.parametrize("output_name", ["state.json", "report.json"])
def test_state_and_report_reject_credentials(tmp_path, output_name):
    with pytest.raises(harness.ManifestError, match="credentials"):
        harness._atomic_write_json(tmp_path / output_name, {"credentials": "prohibited"})


def test_aetna_bulk_terminal_validation_requires_effective_bulk_per_resource(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "aetna-commercial-medicare")
    metrics_by_name = _success_metrics(entry)
    for resource_stats in metrics_by_name["resource_fetch_stats"].values():
        resource_stats["bulk_export_sources"] = 1
    metrics_by_name["bulk_export_mode"] = {
        "requested": True,
        "effective": True,
        "effective_resource_fetches": len(entry["resources"]),
    }
    successful_run = _run_record(manifest, entry, "run_aetna", metrics=metrics_by_name)
    control = FakeImportControl(
        create_responses=[successful_run],
        transitions={"run_aetna": [successful_run]},
    )

    state = _execute_case(tmp_path, manifest, control, [entry["entry_id"]])

    assert state["entries"][entry["entry_id"]]["status"] == "succeeded"

    metrics_by_name["resource_fetch_stats"]["Location"]["bulk_export_sources"] = 0
    failed_run = _run_record(manifest, entry, "run_aetna_failed", metrics=metrics_by_name)
    errors = harness.terminal_metric_errors(manifest, entry, failed_run)

    assert "Location was not effectively acquired through bulk export" in errors


@pytest.mark.parametrize(
    "mutation",
    [
        lambda manifest: manifest["entries"][1].update(source_ids=manifest["entries"][0]["source_ids"]),
        lambda manifest: manifest["entries"][0].update(source_ids=["pdfhir_b6fdc036"]),
        lambda manifest: manifest["entries"][0].update(resources=["InsurancePlan"]),
        lambda manifest: manifest["entries"][3].update(launch_mode="attach"),
    ],
)
def test_manifest_rejects_unsafe_entries(tmp_path, mutation):
    manifest = copy.deepcopy(harness.load_manifest())
    mutation(manifest)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(harness.ManifestError):
        harness.load_manifest(manifest_path)


def test_runs_are_strictly_serial(tmp_path):
    manifest = harness.load_manifest()
    idaho = _manifest_entry(manifest, "idaho")
    molina = _manifest_entry(manifest, "molina")
    idaho_queued = _run_record(manifest, idaho, "run_idaho", "queued")
    molina_queued = _run_record(manifest, molina, "run_molina", "queued")
    control = FakeImportControl(
        create_responses=[idaho_queued, molina_queued],
        transitions={
            "run_idaho": [
                _run_record(manifest, idaho, "run_idaho", "running"),
                _run_record(manifest, idaho, "run_idaho"),
            ],
            "run_molina": [_run_record(manifest, molina, "run_molina")],
        },
    )

    state = _execute_case(tmp_path, manifest, control, ["idaho", "molina"])

    assert state["entries"]["idaho"]["status"] == "succeeded"
    assert state["entries"]["molina"]["status"] == "succeeded"
    assert control.events.index("get:run_idaho:succeeded") < control.events.index("create:run_molina")


def test_late_attach_precedes_create(tmp_path):
    manifest, cigna = manifest_with_attached_entry(harness, "cigna", "run_cigna_attached")
    idaho = _manifest_entry(manifest, "idaho")
    cigna_active = _run_record(manifest, cigna, cigna["attached_run_id"], "running")
    idaho_queued = _run_record(manifest, idaho, "run_idaho", "queued")
    control = FakeImportControl(
        runs=[cigna_active],
        create_responses=[idaho_queued],
        transitions={
            cigna["attached_run_id"]: [
                _run_record(manifest, cigna, cigna["attached_run_id"], "running"),
                _run_record(manifest, cigna, cigna["attached_run_id"]),
            ],
            "run_idaho": [_run_record(manifest, idaho, "run_idaho")],
        },
    )

    state = _execute_case(tmp_path, manifest, control, ["idaho", "cigna"])

    assert state["entries"]["cigna"]["status"] == "succeeded"
    assert state["entries"]["idaho"]["status"] == "succeeded"
    assert control.events.index(f"get:{cigna['attached_run_id']}:succeeded") < control.events.index("create:run_idaho")


def test_attach_ignores_canceled_alias_state(tmp_path):
    manifest, cigna = manifest_with_attached_entry(harness, "cigna", "run_cigna_attached")
    attached_run = _run_record(manifest, cigna, cigna["attached_run_id"])
    alias_run_dict = _run_record(manifest, cigna, "run_canceled_cigna_alias", "canceled")
    alias_run_dict["params"]["source_ids"] = ["pdfhir_6442cb085b35d73b4fed36e7"]
    prior_state_dict = {
        "schema_version": 1,
        "entries": {
            "cigna": {
                "status": "canceled", "run_ids": [alias_run_dict["run_id"]],
                "root_run_id": alias_run_dict["run_id"], "current_run_id": alias_run_dict["run_id"],
                "spec_sha256": harness._entry_fingerprint(manifest, cigna),
            }
        },
    }
    (tmp_path / "state.json").write_text(json.dumps(prior_state_dict), encoding="utf-8")
    control = FakeImportControl(runs=[alias_run_dict, attached_run])

    state = _execute_case(tmp_path, manifest, control, ["cigna"])

    assert control.created_requests == []
    assert not any(alias_run_dict["run_id"] in event for event in control.events if event.startswith("get:"))
    assert state["entries"]["cigna"]["root_run_id"] == cigna["attached_run_id"]
    assert state["entries"]["cigna"]["current_run_id"] == cigna["attached_run_id"]
    assert state["entries"]["cigna"]["status"] == "succeeded"


def test_restart_skips_completed_entry(tmp_path):
    manifest = harness.load_manifest()
    idaho = _manifest_entry(manifest, "idaho")
    molina = _manifest_entry(manifest, "molina")
    prior_state_dict = {
        "schema_version": 1,
        "entries": {
            "idaho": {
                "status": "succeeded",
                "run_ids": ["run_idaho_done"],
                "root_run_id": "run_idaho_done",
                "current_run_id": "run_idaho_done",
                "spec_sha256": harness._entry_fingerprint(manifest, idaho),
            }
        },
    }
    (tmp_path / "state.json").write_text(json.dumps(prior_state_dict), encoding="utf-8")
    control = FakeImportControl(
        create_responses=[_run_record(manifest, molina, "run_molina", "queued")],
        transitions={"run_molina": [_run_record(manifest, molina, "run_molina")]},
    )

    state = _execute_case(tmp_path, manifest, control, ["idaho", "molina"])

    assert len(control.created_requests) == 1
    assert control.created_requests[0]["params"]["source_ids"] == molina["source_ids"]
    assert state["entries"]["idaho"]["current_run_id"] == "run_idaho_done"


def test_poll_rejects_run_that_changes_source_identity(tmp_path):
    manifest = harness.load_manifest()
    idaho = _manifest_entry(manifest, "idaho")
    cigna = _manifest_entry(manifest, "cigna")
    initial_run = _run_record(manifest, idaho, "run_shared", "running")
    wrong_polled_run = _run_record(manifest, cigna, "run_shared", "running")
    control = FakeImportControl(create_responses=[initial_run], transitions={"run_shared": [wrong_polled_run]})
    state = _execute_case(tmp_path, manifest, control, ["idaho"])

    idaho_state = state["entries"]["idaho"]
    assert idaho_state["status"] == "active_conflict"
    assert "polled run changed identity" in idaho_state["message"]
    assert "params.source_ids does not match" in idaho_state["message"]


def test_retry_lineage_is_followed(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "idaho")
    failed_metrics = _success_metrics(entry)
    failed_metrics["pagination_resume_required"] = ["InsurancePlan:pdfhir"]
    root_run = _run_record(manifest, entry, "run_root", "failed", failed_metrics)
    root_run["error"] = {"message": "provider_directory_pagination_resume_required"}
    child_run = _run_record(
        manifest,
        entry,
        "run_child",
        extra_params={
            "retry_of_run_id": "run_root",
            "provider_directory_pagination_root_run_id": "run_root",
        },
    )
    state_seed_dict = {
        "schema_version": 1,
        "entries": {
            "idaho": {
                "status": "running",
                "run_ids": ["run_root"],
                "root_run_id": "run_root",
                "current_run_id": "run_root",
                "spec_sha256": harness._entry_fingerprint(manifest, entry),
            }
        },
    }
    (tmp_path / "state.json").write_text(json.dumps(state_seed_dict), encoding="utf-8")
    control = FakeImportControl(runs=[root_run, child_run])

    state = _execute_case(tmp_path, manifest, control, ["idaho"])

    assert control.created_requests == []
    assert state["entries"]["idaho"]["run_ids"] == ["run_root", "run_child"]
    assert state["entries"]["idaho"]["status"] == "succeeded"


def test_resume_required_is_persisted(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "idaho")
    resume_metrics = _success_metrics(entry)
    resume_metrics["pagination_resume_required"] = ["InsurancePlan:pdfhir"]
    root_run = _run_record(manifest, entry, "run_root", "failed", resume_metrics)
    root_run["error"] = {"message": "provider_directory_pagination_resume_required"}
    control = FakeImportControl(runs=[root_run])
    control.transitions_by_id["run_root"] = [root_run]
    state_seed_dict = {
        "schema_version": 1,
        "entries": {
            "idaho": {
                "status": "running", "run_ids": ["run_root"],
                "root_run_id": "run_root", "current_run_id": "run_root",
                "spec_sha256": harness._entry_fingerprint(manifest, entry),
            }
        },
    }
    (tmp_path / "state.json").write_text(json.dumps(state_seed_dict), encoding="utf-8")

    state = _execute_case(tmp_path, manifest, control, ["idaho"])

    assert state["entries"]["idaho"]["status"] == "resume_required"
    assert json.loads((tmp_path / "report.json").read_text())["entries"]["idaho"]["status"] == "resume_required"


def test_bounded_metrics_fail_gate(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "idaho")
    bounded_metrics = _success_metrics(entry)
    bounded_metrics["resource_fetch_stats"]["InsurancePlan"]["sources_bounded"] = 1
    bounded_run = _run_record(manifest, entry, "run_bounded", "succeeded", bounded_metrics)
    control = FakeImportControl(
        create_responses=[bounded_run],
        transitions={"run_bounded": [bounded_run]},
    )

    state = _execute_case(tmp_path, manifest, control, ["idaho"])

    assert state["entries"]["idaho"]["status"] == "bounded"
    assert "InsurancePlan was bounded or failed" in state["entries"]["idaho"]["metric_errors"]


def test_probe_payload_omits_import_fields(tmp_path):
    manifest = harness.load_manifest()
    entry = _manifest_entry(manifest, "horizon-nj")
    probe_run = _run_record(manifest, entry, "run_probe", "succeeded")
    control = FakeImportControl(
        create_responses=[probe_run],
        transitions={"run_probe": [probe_run]},
    )

    state = _execute_case(tmp_path, manifest, control, ["horizon-nj"])
    posted_params = control.created_requests[0]["params"]

    assert state["entries"]["horizon-nj"]["status"] == "succeeded"
    assert posted_params["import_resources"] is False
    assert posted_params["full_refresh"] is False
    assert harness.PROBE_OMITTED_KEYS.isdisjoint(posted_params)


def test_attach_and_external_never_post(tmp_path):
    manifest, cigna = manifest_with_attached_entry(harness, "cigna", "run_cigna_attached")
    alohr = _manifest_entry(manifest, "alohr")
    cigna_run = _run_record(manifest, cigna, cigna["attached_run_id"], "running")
    alohr_run_dict = {
        "run_id": alohr["external_run_id"],
        "importer": "provider-directory-fhir",
        "status": "succeeded",
        "params": {"source_ids": alohr["source_ids"]},
        "metrics": {
            **_success_metrics(alohr),
            "source_import_sources_selected": 1,
            "source_import_groups_attempted": 1,
            "resource_rows": {"Practitioner": 1},
            "resource_fetch_stats": {
                "Practitioner": {
                    "sources_completed": 1,
                    "sources_bounded": 0,
                    "sources_failed": 0,
                }
            },
        },
    }
    control = FakeImportControl(runs=[cigna_run, alohr_run_dict])

    state = _execute_case(tmp_path, manifest, control, ["cigna", "alohr"], apply=False)

    assert control.created_requests == []
    assert state["entries"]["cigna"]["action"] == "attach"
    assert state["entries"]["alohr"]["status"] == "external_completed"


def test_external_completion_rejects_unbound_success(tmp_path):
    manifest = harness.load_manifest()
    alohr = _manifest_entry(manifest, "alohr")
    unbound_run_dict = {
        "run_id": alohr["external_run_id"],
        "importer": "provider-directory-fhir",
        "status": "succeeded",
        "params": {"source_ids": ["pdfhir_ffffffffffffffffffffffff"]},
        "metrics": _success_metrics(alohr),
    }
    control = FakeImportControl(runs=[unbound_run_dict])

    state = _execute_case(tmp_path, manifest, control, ["alohr"], apply=False)

    assert state["entries"]["alohr"]["status"] == "external_validation_failed"
    assert "external params.source_ids does not match the endpoint" in state["entries"]["alohr"]["metric_errors"]
