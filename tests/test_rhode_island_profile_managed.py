# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import rhode_island_profile as worker
from process import rhode_island_profile_acquisition as acquisition
from process.massachusetts_profile_acquisition import write_new_json
from process.rhode_island_profile_store import store
from tests.test_rhode_island_profile_acquisition import ProfileResponse, _page, synthetic_schema_fingerprint
from tests.test_rhode_island_profile_cohort import Session, responses
from tests.test_rhode_island_profile_registry import snapshot
from tests.test_rhode_island_profile_rows import occurrence


def run_row():
    return {
        "run_id": "a" * 64,
        "source_key": worker.SOURCE_KEY,
        "jurisdiction": "RI",
        "schema_version": worker.SCHEMA_VERSION,
        "source_manifest": worker._source_manifest({"run_id": "managed"}, snapshot(), None),
    }


class GetSession:
    def __init__(self, license_number):
        self.values = [
            ProfileResponse(_page(license_number)),
            ProfileResponse(json.dumps(occurrence(license_number) * 2).encode()),
        ]

    def get(self, url, *, allow_redirects):
        response = self.values.pop(0)
        response.url = url
        return response


def mock_source_transport(monkeypatch):
    from process import rhode_island_profile_cohort as cohort_source

    session = Session(responses())
    monkeypatch.setattr(cohort_source.aiohttp, "ClientSession", lambda **kwargs: session)

    async def profile(license_number, destination, *, run_id):
        destination.mkdir()
        return await acquisition._acquire_pair(GetSession(license_number), license_number, destination, run_id)

    monkeypatch.setattr(acquisition, "acquire_profile", profile)


async def acquired(tmp_path, monkeypatch):
    mock_source_transport(monkeypatch)
    monkeypatch.setattr(worker, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(worker, "_progress", lambda *args: None)
    cohort, profiles, metrics = await worker._acquire({}, {"run_id": "managed"}, run_row(), tmp_path)
    return cohort, profiles, metrics, worker._artifact(run_row(), cohort, profiles, metrics)


@pytest.fixture
def managed_storage(monkeypatch):
    """Record transaction commits and rollbacks at the worker boundary."""
    retained = SimpleNamespace(
        rows=[],
        staged=None,
        events=[],
        progress=[],
        cancel_at=None,
        fail_at=None,
        incumbent="b" * 64,
        publication="b" * 64,
    )

    async def upsert(model, records, identity):
        if model is worker.ProviderProfileFact and retained.fail_at == "facts":
            raise RuntimeError("fact_write_failed")
        destination = retained.rows if retained.staged is None else retained.staged
        destination.extend((model, copy.deepcopy(record)) for record in records)

    @asynccontextmanager
    async def transaction():
        assert retained.staged is None
        retained.staged = []
        try:
            yield
        except BaseException:
            retained.events.append("rollback")
            raise
        else:
            retained.rows.extend(retained.staged)
            retained.events.append("commit")
        finally:
            retained.staged = None

    monkeypatch.setattr(worker, "_upsert_rows", upsert)
    monkeypatch.setattr(worker.db, "transaction", transaction)
    return retained


@pytest.fixture
def managed_worker(tmp_path, monkeypatch, managed_storage, synthetic_schema_fingerprint):
    retained = managed_storage
    mock_source_transport(monkeypatch)
    monkeypatch.setenv("HLTHPRT_RI_DOH_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setattr(worker, "ensure_tables", AsyncMock())
    monkeypatch.setattr(worker, "enqueue_live_progress", lambda **progress: retained.progress.append(progress))

    async def cancelled(ctx, task):
        if retained.cancel_at == "after_records" and retained.staged:
            raise asyncio.CancelledError()
        if retained.cancel_at == "after_retaining" and any(p["phase"] == "retaining" for p in retained.progress):
            raise asyncio.CancelledError()

    async def capture(schema, progress):
        await progress(1, 1)
        return snapshot()

    async def complete(ctx, task, run, metrics):
        assert store.claim_run.await_count == 1
        assert store.update_run.await_args.args == (run["run_id"], {"status": "validating", "metrics": metrics})
        assert len([row for model, row in retained.rows if model is worker.ProviderProfileSourceRecord]) == 2
        if retained.fail_at == "completion":
            raise RuntimeError("publication_refused")
        retained.publication = run["run_id"]
        retained.events.append("published")
        return {"published": True, "retained_source_records": 2}

    monkeypatch.setattr(worker, "raise_if_cancelled", cancelled)
    monkeypatch.setattr(worker.registry, "capture_registry_snapshot", capture)
    monkeypatch.setattr(type(store), "read_publication", AsyncMock(return_value={"current_run_id": retained.incumbent}))
    for name in ("claim_run", "update_run", "mark_run_failed", "retain_source_history"):
        monkeypatch.setattr(type(store), name, AsyncMock())
    monkeypatch.setattr(type(worker.completion), "reconcile_failed_control_runs", AsyncMock())
    monkeypatch.setattr(type(worker.completion), "complete_run", AsyncMock(side_effect=complete))
    return retained


@pytest.mark.parametrize("retention_error", [False, True])
async def test_managed_completion_survives_cleanup_error(tmp_path, managed_worker, retention_error, caplog):
    retained = managed_worker
    if retention_error:
        store.retain_source_history.side_effect = OSError("history_cleanup_failed")
    result = await worker.import_profiles({}, {"run_id": "managed"})
    assert result == {"published": True, "retained_source_records": 2}
    assert retained.events == ["commit", "published"]
    assert retained.publication != retained.incumbent
    directory = tmp_path / "artifacts" / retained.publication
    assert json.loads((directory / "snapshot.json").read_text()) == snapshot()
    manifest = json.loads((directory / "manifest.json").read_text())
    assert manifest["source_manifest"]["expected_current_run_id"] == retained.incumbent
    assert set(manifest["profiles"]) == {"MD00001", "DO00001"}
    assert [progress["phase"] for progress in retained.progress] == [
        "registry snapshot",
        "acquiring",
        "acquiring",
        "retaining",
    ]
    assert all(progress["run_id"] == "managed" and progress["total"] > 0 for progress in retained.progress)
    store.mark_run_failed.assert_not_awaited()
    store.retain_source_history.assert_awaited_once_with(tmp_path / "artifacts")
    assert ("history_cleanup_failed" in caplog.text) is retention_error


@pytest.mark.parametrize("checkpoint", ["after_records", "after_retaining"])
async def test_managed_cancellation_preserves_publication(managed_worker, checkpoint):
    retained = managed_worker
    retained.cancel_at = checkpoint
    with pytest.raises(asyncio.CancelledError):
        await worker.import_profiles({}, {"run_id": "managed"})
    assert retained.publication == retained.incumbent and retained.staged is None
    rows = [row for model, row in retained.rows if model is not worker.ProviderProfileArtifact]
    if checkpoint == "after_records":
        assert rows == [] and retained.events == ["rollback"]
    else:
        assert rows and retained.events == ["commit"]
    store.mark_run_failed.assert_awaited_once()
    assert isinstance(store.mark_run_failed.await_args.args[1], asyncio.CancelledError)
    worker.completion.complete_run.assert_not_awaited()
    store.retain_source_history.assert_not_awaited()


@pytest.mark.parametrize("failure", ["facts", "completion"])
async def test_managed_failure_preserves_publication(managed_worker, failure):
    retained = managed_worker
    retained.fail_at = failure
    message = "fact_write_failed" if failure == "facts" else "publication_refused"
    with pytest.raises(RuntimeError, match=message):
        await worker.import_profiles({}, {"run_id": "managed"})
    assert retained.publication == retained.incumbent and retained.staged is None
    rows = [row for model, row in retained.rows if model is not worker.ProviderProfileArtifact]
    if failure == "facts":
        assert rows == [] and retained.events == ["rollback"]
        worker.completion.complete_run.assert_not_awaited()
    else:
        assert rows and retained.events == ["commit"]
        worker.completion.complete_run.assert_awaited_once()
    store.mark_run_failed.assert_awaited_once()
    assert str(store.mark_run_failed.await_args.args[1]) == message
    store.retain_source_history.assert_not_awaited()


@pytest.mark.parametrize(
    "fault,error",
    [
        ("incomplete", "receipt_incomplete"),
        ("identity", "capture_identity_changed"),
        ("descriptor", "capture_changed"),
        ("receipt_size", "receipt_too_large"),
        ("total_size", "capture_too_large"),
    ],
)
async def test_managed_capture_refusal_preserves_publication(managed_worker, monkeypatch, fault, error):
    retained = managed_worker
    original_profile, original_acquire = acquisition.acquire_profile, worker._acquire

    async def changed_profile(license_number, destination, *, run_id):
        result = await original_profile(license_number, destination, run_id=run_id)
        path = destination / "record.json"
        receipt = json.loads(path.read_text())
        if fault == "incomplete":
            receipt["eof"] = False
        elif fault == "identity":
            receipt["source_url"] = receipt["source_url"].replace("MD00001", "DO00001")
            receipt["response_url"] = receipt["source_url"]
        elif fault == "receipt_size":
            monkeypatch.setattr(acquisition, "MAX_RESPONSE_BYTES", 16)
        path.write_text(json.dumps(receipt))
        return result

    async def changed_acquire(*args):
        assert store.claim_run.await_count == 1
        cohort, profiles, metrics = await original_acquire(*args)
        if fault == "descriptor":
            profiles["MD00001"]["downloaded_at"] = "2026-01-01T00:00:00+00:00"
        return cohort, profiles, metrics

    monkeypatch.setattr(acquisition, "acquire_profile", changed_profile)
    monkeypatch.setattr(worker, "_acquire", changed_acquire)
    if fault == "total_size":
        monkeypatch.setattr(worker, "MAX_CAPTURE_BYTES", 0)
    with pytest.raises(ValueError, match=error):
        await worker.import_profiles({}, {"run_id": "managed"})
    assert retained.publication == retained.incumbent
    assert all(model is worker.ProviderProfileArtifact for model, _ in retained.rows)
    store.mark_run_failed.assert_awaited_once()
    worker.completion.complete_run.assert_not_awaited()
    store.retain_source_history.assert_not_awaited()


@pytest.mark.parametrize("fault", ["artifact_root", "registry_schema"])
async def test_managed_configuration_refused_before_claim(managed_worker, monkeypatch, fault):
    if fault == "artifact_root":
        monkeypatch.setenv("HLTHPRT_RI_DOH_ARTIFACT_ROOT", " ")
        error = "artifact_root_invalid"
    else:
        monkeypatch.setattr(worker.NPIData.__table__, "schema", "other_schema")
        error = "registry_schema_mismatch"
    with pytest.raises(ValueError, match=error):
        await worker.import_profiles({}, {"run_id": "managed"})
    assert managed_worker.publication == managed_worker.incumbent
    assert managed_worker.rows == [] and managed_worker.progress == []
    store.claim_run.assert_not_awaited()
    store.mark_run_failed.assert_not_awaited()
    worker.completion.complete_run.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_providers", 1),
        ("sources", ["MD"]),
        ("professions", []),
        ("license_types", ["DO"]),
        ("resume_from", "abc"),
        ("run_id", ""),
    ],
)
def test_scope_cannot_be_reduced(field, value):
    with pytest.raises(ValueError):
        worker._parameters({"run_id": "managed", field: value})


async def test_full_capture_binding_retains_unmatched_records_originals_and_fact_provenance(
    tmp_path, monkeypatch, synthetic_schema_fingerprint
):
    cohort, profiles, metrics, artifact = await acquired(tmp_path, monkeypatch)
    write_new_json(tmp_path / "snapshot.json", snapshot())
    rows = []

    async def upsert(model, records, identity):
        rows.extend((model, copy.deepcopy(record)) for record in records)

    @asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(worker, "_upsert_rows", upsert)
    monkeypatch.setattr(worker.db, "transaction", transaction)
    await worker._persist({}, {"run_id": "managed"}, run_row(), cohort, profiles, tmp_path, artifact, "mrf")
    records = [row for model, row in rows if model == worker.ProviderProfileSourceRecord]
    facts = [row for model, row in rows if model == worker.ProviderProfileFact]
    assert len(records) == 2 and len(facts) == metrics["facts"]
    assert records[0]["matched_npi"] == 1003000126 and records[1]["matched_npi"] is None
    assert records[0]["raw_payload"]["roster_occurrences"] == cohort["roots"][0]["originals"]
    assert all(row["source_json"]["schema_page"]["content_sha256"] for row in facts)
    assert all(len(row["source_json"]["raw_occurrences"]) == 2 for row in facts)
    assert all(row["source_json"]["artifact_id"] == artifact["artifact_id"] for row in facts)
    assert store._bundle_counts(run_row(), [artifact])["invalid_bundle_artifacts"] == 0


@pytest.mark.parametrize("field", ["page", "record"])
async def test_changed_retained_capture_is_rejected(tmp_path, monkeypatch, synthetic_schema_fingerprint, field):
    cohort, profiles, metrics, artifact = await acquired(tmp_path, monkeypatch)
    path = tmp_path / "profiles" / "MD00001" / f"{field}.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="receipt_changed"):
        list(worker._profile_inputs(cohort, profiles, tmp_path, artifact))


async def test_roster_identity_change_refuses_retention(tmp_path, monkeypatch, synthetic_schema_fingerprint):
    cohort, profiles, metrics, artifact = await acquired(tmp_path, monkeypatch)
    write_new_json(tmp_path / "snapshot.json", snapshot())
    bound = worker.registry.bind_snapshot_profiles(
        worker._profile_inputs(cohort, profiles, tmp_path, artifact),
        snapshot_path=tmp_path / "snapshot.json",
        snapshot_sha256=run_row()["source_manifest"]["snapshot_sha256"],
    )
    record, _ = next(bound)
    cohort["roots"][0]["originals"][0]["raw_payload"]["Last"] = "Different"
    with pytest.raises(ValueError, match="roster_identity_changed"):
        worker._retain_roster(record, cohort["roots"][0], artifact)


@pytest.mark.parametrize(
    ("field", "roster_name", "profile_name", "matches"),
    [(field, "O\\'Example", "O'Example", True) for field in ("First", "Middle", "Last")]
    + [
        ("Last", "O\\'Example", "O'Other", False),
        ("Last", "O\\'Example", "OExample", False),
        ("Last", "O\\\\'Example", "O'Example", False),
        ("Last", "O'Example", "O\\'Example", False),
    ],
)
async def test_roster_apostrophe_escape_comparison_preserves_originals(
    tmp_path, monkeypatch, synthetic_schema_fingerprint, field, roster_name, profile_name, matches
):
    cohort, profiles, metrics, artifact = await acquired(tmp_path, monkeypatch)
    license_number, body, evidence = next(worker._profile_inputs(cohort, profiles, tmp_path, artifact))
    record, _ = acquisition.parse_profile(body, license_number=license_number, evidence=evidence)
    root = cohort["roots"][0]
    root["originals"][0]["raw_payload"][field] = roster_name
    record["raw_payload"]["values"][worker.PROFILE_FIELDS.index(field + "_Name")] = profile_name
    original = copy.deepcopy(record)
    originals = copy.deepcopy(root["originals"])
    if matches:
        worker._retain_roster(record, root, artifact)
        assert record["raw_payload"]["roster_occurrences"] == originals
    else:
        with pytest.raises(ValueError, match="roster_identity_changed"):
            worker._retain_roster(record, root, artifact)
        assert record == original
    assert root["originals"] == originals
    assert record["raw_payload"]["values"] == original["raw_payload"]["values"]
    assert record["matched_npi"] is None and record["match_evidence"] == original["match_evidence"]


@pytest.mark.parametrize("failure", [ValueError("source_failed"), __import__("asyncio").CancelledError()])
async def test_claim_precedes_http_and_failure_never_completes(tmp_path, monkeypatch, failure):
    monkeypatch.setenv("HLTHPRT_RI_DOH_ARTIFACT_ROOT", str(tmp_path))
    for name in ("raise_if_cancelled", "ensure_tables"):
        monkeypatch.setattr(worker, name, AsyncMock())
    for name in ("reconcile_failed_control_runs", "complete_run"):
        monkeypatch.setattr(type(worker.completion), name, AsyncMock())
    monkeypatch.setattr(type(store), "read_publication", AsyncMock(return_value=None))
    claim, failed = AsyncMock(), AsyncMock()
    monkeypatch.setattr(type(store), "claim_run", claim)
    monkeypatch.setattr(type(store), "mark_run_failed", failed)
    monkeypatch.setattr(worker.registry, "capture_registry_snapshot", AsyncMock(return_value=snapshot()))

    async def claimed(*args):
        assert claim.await_count == 1
        raise failure

    monkeypatch.setattr(worker, "_run_claimed", claimed)
    with pytest.raises(type(failure)):
        await worker.import_profiles({}, {"run_id": "managed"})
    failed.assert_awaited_once()
    worker.completion.complete_run.assert_not_called()


def counts(metrics):
    return {
        "bundle_metrics": metrics,
        "retained_source_records": metrics["responses"],
        "received_profiles": metrics["responses"],
        "md_source_records": 1,
        "do_source_records": 1,
        "retained_facts": metrics["facts"],
        **dict.fromkeys(
            (
                "invalid_source_records",
                "invalid_facts",
                "foreign_artifacts",
                "invalid_bundle_artifacts",
                "invalid_bundle_records",
                "invalid_capture_facts",
                "duplicate_licenses",
            ),
            0,
        ),
    }


async def test_completion_requires_exact_bundle_retention_and_full_inventory(
    tmp_path, monkeypatch, synthetic_schema_fingerprint
):
    _, _, metrics, artifact = await acquired(tmp_path, monkeypatch)
    assert store._completion_metrics(run_row(), metrics, counts(metrics))["requested_licenses"] == 2
    for field in ("invalid_capture_facts", "invalid_bundle_records", "duplicate_licenses", "invalid_facts"):
        invalid = counts(metrics) | {field: 1}
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            store._completion_metrics(run_row(), metrics, invalid)
    for field in (
        "received_profiles",
        "retained_source_records",
        "retained_facts",
        "md_source_records",
        "do_source_records",
    ):
        invalid = counts(metrics)
        invalid[field] -= 1
        with pytest.raises(RuntimeError, match="retained_count_mismatch"):
            store._completion_metrics(run_row(), metrics, invalid)
    invalid = copy.deepcopy(artifact)
    del invalid["metadata_json"]["profiles"]["DO00001"]
    invalid["content_sha256"] = worker._hash(invalid["metadata_json"])
    invalid["content_bytes"] = len(worker.encoded_json(invalid["metadata_json"]))
    assert store._bundle_counts(run_row(), [invalid])["invalid_bundle_artifacts"] == 1


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("unexpected", None, "manifest_invalid"),
        ("license_types", ["MD"], "complete_pair_required"),
        ("max_providers", 1, "complete_pair_required"),
        ("snapshot_sha256", "changed", "snapshot_invalid"),
        ("snapshot_row_count", True, "snapshot_invalid"),
        ("control_run_id", " ", "managed_run_required"),
    ],
)
def test_retained_manifest_refuses_changed_scope_or_identity(field, value, error):
    run = run_row()
    run["source_manifest"][field] = value
    with pytest.raises(ValueError, match=error):
        store._manifest(run)


def test_retained_manifest_preserves_prior_pointer_and_registry_generation():
    run = run_row()
    manifest = run["source_manifest"]
    manifest["expected_current_run_id"] = "b" * 64
    assert store._manifest(run) == manifest
    manifest["source"]["registry_generation"] = "c" * 64
    with pytest.raises(ValueError, match="manifest_source_invalid"):
        store._manifest(run)


@pytest.mark.parametrize("artifact_count", [0, 2])
async def test_retained_bundle_requires_one_manifest(
    tmp_path, monkeypatch, synthetic_schema_fingerprint, artifact_count
):
    _, _, _, artifact = await acquired(tmp_path, monkeypatch)
    assert store._bundle_counts(run_row(), [artifact])["invalid_bundle_artifacts"] == 0
    assert store._bundle_counts(run_row(), [artifact] * artifact_count) == {
        "invalid_bundle_artifacts": 1,
        "bundle_metrics": None,
    }


@pytest.mark.parametrize(
    "path,value",
    [
        (("run_id",), "b" * 64),
        (("cohort", "rosters"), {}),
        (("cohort", "rosters", "MD", "preview", "content_sha256"), "changed"),
        (("profiles", "MD00001", "source_url"), "https://example.org/changed"),
        (("profiles", "MD00001", "schema_page", "downloaded_at"), "not-a-timestamp"),
        (("acquisition", "responses"), 3),
        (("acquisition", "cohort_sha256"), "c" * 64),
    ],
)
async def test_rehashed_bundle_refuses_changed_retained_evidence(
    tmp_path, monkeypatch, synthetic_schema_fingerprint, path, value
):
    _, _, _, artifact = await acquired(tmp_path, monkeypatch)
    assert store._bundle_counts(run_row(), [artifact])["invalid_bundle_artifacts"] == 0
    invalid = copy.deepcopy(artifact)
    parent = invalid["metadata_json"]
    for field in path[:-1]:
        parent = parent[field]
    parent[path[-1]] = value
    invalid["content_sha256"] = worker._hash(invalid["metadata_json"])
    invalid["content_bytes"] = len(worker.encoded_json(invalid["metadata_json"]))
    assert store._bundle_counts(run_row(), [invalid]) == {
        "invalid_bundle_artifacts": 1,
        "bundle_metrics": None,
    }
    assert store._bundle_counts(run_row(), [artifact])["invalid_bundle_artifacts"] == 0


def test_publication_accepts_complete_refresh_at_volume_boundary():
    incumbent_metrics_by_field = {
        "md_source_records": 7500,
        "do_source_records": 1000,
        "matched_public_providers": 100,
        "received_profiles": 8500,
    }
    store._publication_volume(incumbent_metrics_by_field, incumbent_metrics_by_field)
    current_metrics_by_field = {key: value * 4 // 5 for key, value in incumbent_metrics_by_field.items()}
    store._publication_volume(current_metrics_by_field, incumbent_metrics_by_field)
    for key in incumbent_metrics_by_field:
        with pytest.raises(RuntimeError, match="publication_volume_drop:" + key):
            store._publication_volume(
                current_metrics_by_field | {key: current_metrics_by_field[key] - 1}, incumbent_metrics_by_field
            )


def test_publication_refuses_preview_or_source_volume_drop():
    metrics_by_field = {
        "md_source_records": 6447,
        "do_source_records": 712,
        "matched_public_providers": 100,
        "received_profiles": 7159,
    }
    store._publication_volume(metrics_by_field, None)
    for key in metrics_by_field:
        smaller = metrics_by_field | {key: 0}
        with pytest.raises(RuntimeError):
            store._publication_volume(smaller, metrics_by_field)


def test_managed_registry_entrypoint_and_terminal_commit():
    from click.testing import CliRunner

    import process
    from api import control_imports, control_workers
    from process import control_lifecycle

    entry = next(entry for entry in control_imports.importer_registry() if entry["name"] == worker.IMPORTER)
    assert entry["family"] == "provider" and entry["depends_on"] == ["npi"] and entry["cancelable"]
    assert entry["params_schema"] == []
    adapter = control_imports._SINGLE_JOB_ADAPTERS[worker.IMPORTER]
    assert (
        adapter["target_module"] == "process.rhode_island_profile" and adapter["target_function"] == "import_profiles"
    )
    spec = next(spec for spec in control_workers._WORKERS if worker.IMPORTER in spec.importers)
    assert spec.queue == adapter["queue"] == process.RhodeIslandDOHProfile.queue_name
    assert process.RhodeIslandDOHProfile.max_jobs == process.RhodeIslandDOHProfile.functions[0].max_tries == 1
    cli = CliRunner().invoke(process.process_group, [worker.IMPORTER])
    assert cli.exit_code == 2 and "managed import API" in cli.output
    committed_by_field = {"published": True, "retained_source_records": 2}
    assert (
        control_lifecycle._committed_target_result(
            {"context": {"control_run_terminal_committed": True, "_control_committed_result": committed_by_field}},
            target_module="process.rhode_island_profile",
        )
        is committed_by_field
    )


def test_worker_capacity_and_override(monkeypatch):
    from api import control_workers

    spec = control_workers.WorkerSpec("arq:RhodeIslandDOHProfile", "process.RhodeIslandDOHProfile", (worker.IMPORTER,))
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", raising=False)
    assert control_workers._worker_job_resources(spec) == {
        "requests": {"cpu": "500m", "memory": "512Mi"},
        "limits": {"cpu": "4", "memory": "4Gi"},
    }
    resources_by_kind = {"requests": {"memory": "2Gi"}, "limits": {"memory": "6Gi"}}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", json.dumps({spec.worker_class: resources_by_kind}))
    assert control_workers._worker_job_resources(spec) == resources_by_kind


async def test_actual_managed_manifest_and_bound_facts_project(tmp_path, monkeypatch, synthetic_schema_fingerprint):
    from datetime import datetime

    from api import provider_profile as profile_api
    from api import provider_profile_states as state_api

    cohort, profiles, _, artifact = await acquired(tmp_path, monkeypatch)
    write_new_json(tmp_path / "snapshot.json", snapshot())
    bound = worker.registry.bind_snapshot_profiles(
        worker._profile_inputs(cohort, profiles, tmp_path, artifact),
        snapshot_path=tmp_path / "snapshot.json",
        snapshot_sha256=run_row()["source_manifest"]["snapshot_sha256"],
    )
    source_record, facts = next(bound)
    fact_rows = [
        {
            **fact,
            "published_at": datetime(2026, 9, 11),
            "publication_source_key": worker.SOURCE_KEY,
            "generation_id": run_row()["run_id"],
            "source_published_at": datetime(2026, 9, 11),
            "run_status": "completed",
            "run_schema_version": worker.SCHEMA_VERSION,
            "run_jurisdiction": "RI",
            "source_manifest": run_row()["source_manifest"],
        }
        for fact in facts
    ]
    projection = state_api._state_projection(source_record["matched_npi"], fact_rows, source_key=worker.SOURCE_KEY)
    envelope = state_api.merge_state_profile_projection(source_record["matched_npi"], None, projection)
    profile = profile_api.compose_provider_profile(
        source_record["matched_npi"], state_projection=envelope, fhir_profile=None
    )
    assert profile["source_generations"] == {worker.SOURCE_KEY: run_row()["run_id"]}
    for fact in facts:
        group = profile["categories"][fact["category"]]
        assert any(
            assertion["value"] == fact["value_json"]
            for profile_item in group["items"]
            for assertion in profile_item["assertions"]
        )
    evidence = profile_api.compose_provider_profile_evidence(
        state_projection=envelope, fhir_evidence=None, provider_profile=profile
    )
    assert len(evidence["sources"][worker.SOURCE_KEY]["records"]) == len(facts)
