# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exercise orchestration with retained public bytes and synthetic I/O boundaries."""

import asyncio
import copy
import hashlib
import importlib
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import live_progress
from process.control_cancel import ImportCancelledError
from tests.test_kentucky_profile_rows import CANDIDATE, FIELDS, response_html

worker = importlib.import_module("process.kentucky_profile")
PREDECESSOR = "a" * 64


def _cohort(licenses=("00042", "C0007", "C0008")):
    return worker.acquisition.build_cohort([
        {**CANDIDATE, "license_number": license_number, "npi": 1000000004, "taxonomy": "207Q00000X"}
        for license_number in licenses
    ], {"mrf.npi": 101, "mrf.npi_taxonomy": 102, "mrf.nucc_taxonomy": 103})


def _response(license_number, *, html=None, empty=False):
    fields = [] if empty else [(label, license_number if label == "License" else value) for label, value in FIELDS]
    if not empty:
        fields.extend([("*Area of Practice", ""), ("Type of Practice", "")])
    body = (response_html(fields, license_number=license_number) if html is None else html).encode("utf-8")
    return {
        "schema_version": worker.acquisition.RESPONSE_SCHEMA, "source_key": worker.SOURCE_KEY,
        "license_number": license_number, "source_url": worker.acquisition.source_url(license_number),
        "downloaded_at": "2026-09-08T12:00:00+00:00", "status": 200,
        "content_type": "text/html; charset=utf-8", "body_text": body.decode(),
        "content_sha256": hashlib.sha256(body).hexdigest(),
    }


class ImportHarness:
    def __init__(self, monkeypatch, tmp_path):
        self.artifact_root = tmp_path / "artifacts"
        self.cohort = _cohort()
        self.responses_by_license = {root["license_number"]: _response(root["license_number"]) for root in self.cohort["roots"]}
        self.writes = []
        self.requests = []
        self.redis = SimpleNamespace(get=AsyncMock(return_value=None))
        self.ctx = {"redis": self.redis}
        self.task = {"run_id": "synthetic-control-run"}
        self.fail_model = None
        self.cancel_model = None
        self.store_by_name = {}
        for name in ("ensure_tables", "claim_run", "update_run", "mark_run_failed", "retain_source_history", "publish_run", "finish_unpublished_run"):
            self.store_by_name[name] = AsyncMock()
            monkeypatch.setattr(worker.store, name, self.store_by_name[name])
        monkeypatch.setattr(worker.store, "read_publication", AsyncMock(return_value={"current_run_id": PREDECESSOR}))
        self.reconcile = AsyncMock(return_value=[])
        monkeypatch.setattr(worker, "reconcile_failed_control_runs", self.reconcile)
        monkeypatch.setattr(worker.acquisition, "capture_registry_cohort", AsyncMock(return_value=self.cohort))
        monkeypatch.setattr(worker.acquisition, "fetch_profile", self.fetch)
        monkeypatch.setattr(worker.acquisition.aiohttp, "ClientSession", self.session)
        monkeypatch.setattr(worker.acquisition, "REQUEST_INTERVAL_SECONDS", 0)
        monkeypatch.setattr(worker, "db", SimpleNamespace(transaction=self.transaction))
        monkeypatch.setattr(worker, "_upsert_rows", self.upsert)
        monkeypatch.setattr(worker, "enqueue_live_progress", Mock())
        self.finish = AsyncMock(side_effect=lambda _ctx, _task, run, metrics: {"run_id": run["run_id"], **metrics})
        monkeypatch.setattr(worker, "_finish_run", self.finish)
        monkeypatch.setenv("HLTHPRT_KY_KBML_ARTIFACT_ROOT", str(self.artifact_root))

    @asynccontextmanager
    async def session(self, **_options):
        yield SimpleNamespace(_retry_connection=True)

    @asynccontextmanager
    async def transaction(self):
        checkpoint = len(self.writes)
        try:
            yield
        except BaseException:
            del self.writes[checkpoint:]
            raise

    async def fetch(self, _session, license_number):
        assert _session._retry_connection is False
        self.requests.append(license_number)
        response = self.responses_by_license[license_number]
        if isinstance(response, BaseException):
            raise response
        worker.acquisition.decoded_profile(response)
        return copy.deepcopy(response)

    async def upsert(self, model, rows, key):
        if model is self.fail_model:
            raise RuntimeError("synthetic persistence failure")
        self.writes.append((model, copy.deepcopy(rows), key))
        if model is self.cancel_model:
            self.redis.get.return_value = b"1"

    def rows_for(self, model):
        return [source_row for written_model, source_rows, _key in self.writes if written_model is model for source_row in source_rows]


@pytest.fixture
def harness(monkeypatch, tmp_path):
    return ImportHarness(monkeypatch, tmp_path)


@pytest.mark.parametrize("limit", [None, 2, 10])
async def test_selected_scope_is_frozen_before_claim_and_retained(harness, limit):
    await worker.import_profiles(harness.ctx, {**harness.task, "max_providers": limit})
    claimed_run = harness.store_by_name["claim_run"].call_args.args[0]
    manifest = claimed_run["source_manifest"]
    expected = 3 if limit is None else min(limit, 3)
    assert manifest["full_cohort_licenses"] == 3
    assert manifest["requested_licenses"] == expected
    assert manifest["max_providers"] == limit
    assert manifest["expected_current_run_id"] == PREDECESSOR
    assert manifest["cohort_sha256"] == worker._hash(harness.cohort)
    assert manifest["source"]["registry_generation"] == harness.cohort["registry_generation"]
    assert manifest["categories"] == list(worker.PROFILE_CATEGORIES)
    assert manifest["source"]["agency"] == "Kentucky Board of Medical Licensure"
    assert manifest["source"]["jurisdiction"] == "KY" and manifest["source"]["source_key"] == worker.SOURCE_KEY
    assert len(harness.requests) == len(set(harness.requests)) == expected
    assert len(harness.rows_for(worker.ProviderProfileSourceRecord)) == expected
    assert len(harness.rows_for(worker.ProviderProfileFact)) == expected
    assert all(fact["npi"] == 1000000004 and fact["category"] == "education"
               and fact["fact_type"] == "education_history" for fact in harness.rows_for(worker.ProviderProfileFact))
    assert worker.acquisition.read_cohort(harness.artifact_root / claimed_run["run_id"] / "cohort.json") == harness.cohort
    harness.finish.assert_awaited_once()
    assert harness.finish.call_args.args[3]["responses"] == expected
    harness.store_by_name["mark_run_failed"].assert_not_called()


async def test_frozen_claim_precedes_artifact_creation_and_run_ids_are_source_bound(harness):
    async def claim_before_directory(run):
        worker.acquisition.capture_registry_cohort.assert_awaited_once()
        assert not harness.artifact_root.exists()
        assert run["run_id"] == worker._hash([worker.SOURCE_KEY, harness.task["run_id"]])
        assert run["run_id"] != worker._hash(["massachusetts-borim", harness.task["run_id"]])
        assert run["source_manifest"]["cohort_sha256"] == worker._hash(harness.cohort)

    harness.store_by_name["claim_run"].side_effect = claim_before_directory
    await worker.import_profiles(harness.ctx, harness.task)
    harness.store_by_name["claim_run"].assert_awaited_once()


async def test_first_publication_freezes_an_absent_predecessor(harness):
    worker.store.read_publication.return_value = None
    await worker.import_profiles(harness.ctx, harness.task)
    claimed_run = harness.store_by_name["claim_run"].call_args.args[0]
    assert claimed_run["source_manifest"]["expected_current_run_id"] is None
    harness.finish.assert_awaited_once()


async def test_invalid_license_root_fails_before_claim_or_artifact_paths(harness):
    harness.cohort["roots"][-1]["license_number"] = "../outside"
    with pytest.raises(ValueError, match="cohort_licenses_invalid"):
        await worker.import_profiles(harness.ctx, harness.task)
    harness.store_by_name["claim_run"].assert_not_awaited()
    assert not harness.artifact_root.exists() and harness.requests == []


def test_bounded_selection_is_order_independent_and_preserves_roots():
    cohort = _cohort()
    original = copy.deepcopy(cohort)
    selected = worker._selected_roots(cohort, 2)
    shuffled_by_field = {**cohort, "roots": list(reversed(cohort["roots"]))}
    assert worker._selected_roots(shuffled_by_field, 2) == selected
    assert len(selected) == 2
    assert cohort == original
    assert worker._selected_roots(cohort, None) == cohort["roots"]


def test_persisted_sampling_versions_keep_literal_historical_order():
    cohort = _cohort(("0", "00000", "00042", "C0007", "C0009", "10", "2"))
    legacy = worker._selected_roots(cohort, 7, strategy=worker.LEGACY_SAMPLING_STRATEGY)
    assert [root["license_number"] for root in legacy] == ["C0007", "00000", "10", "0", "2", "00042", "C0009"]
    current = worker._selected_roots(cohort, 4)
    assert [root["license_number"] for root in current] == ["0", "C0007", "00000", "10"]
    assert worker._selected_roots(cohort, None) is cohort["roots"]


def test_unknown_sampling_version_is_rejected():
    with pytest.raises(ValueError, match="sampling_strategy_invalid"):
        worker._selected_roots(_cohort(), 1, strategy="unknown/v1")


@pytest.mark.parametrize("limit", [1, 2, 100, 300])
def test_bounded_sample_includes_lexical_first_then_existing_hash_order(limit):
    cohort = _cohort(tuple(str(number) for number in range(201)))
    selected = worker._selected_roots(cohort, limit)
    original_sample = worker._hash_selected_roots(cohort, len(cohort["roots"]))
    assert selected[0]["license_number"] == "0"
    assert selected[1:] == [root for root in original_sample if root["license_number"] != "0"][:limit - 1]
    assert len(selected) == len({root["license_number"] for root in selected}) == min(limit, 201)
    assert worker._selected_roots(cohort, None) is cohort["roots"]
    assert worker._selected_roots(cohort, limit, strategy=worker.LEGACY_SAMPLING_STRATEGY) == original_sample[:limit]


async def test_artifact_accounts_for_not_found_without_inventing_facts(harness):
    harness.responses_by_license["C0007"] = _response("C0007", empty=True)
    await worker.import_profiles(harness.ctx, harness.task)
    artifact = harness.rows_for(worker.ProviderProfileArtifact)[0]
    path = harness.artifact_root / artifact["run_id"] / artifact["file_name"]
    assert artifact["content_bytes"] == path.stat().st_size
    assert artifact["content_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    metrics = json.loads(path.read_text())["acquisition"]
    expected_hash = hashlib.sha256()
    for license_number in harness.requests:
        response = harness.responses_by_license[license_number]
        expected_hash.update(worker.acquisition.encoded_json(response))
    assert metrics["responses_sha256"] == expected_hash.hexdigest()
    assert metrics["response_bytes"] == sum(len(response["body_text"].encode()) for response in harness.responses_by_license.values())
    assert metrics["responses"] == 3 and metrics["reused_responses"] == 0
    assert metrics["acquisition_complete"] is True and metrics["transport_failures"] == 0
    missing = [source_row for source_row in harness.rows_for(worker.ProviderProfileSourceRecord) if source_row["license_number"] == "C0007"][0]
    assert missing["match_status"] == "not_found" and missing["matched_npi"] is None
    assert missing["raw_payload"] == {"html": harness.responses_by_license["C0007"]["body_text"], "profiles": []}
    assert len(harness.rows_for(worker.ProviderProfileFact)) == 2


@pytest.mark.parametrize("failure, attempts", [(ValueError("synthetic transport failure"), 1),
                                             (asyncio.CancelledError("synthetic transport failure"), 1),
                                             (worker.acquisition.ProfileHTTPStatusError(500), 3)])
async def test_acquisition_failure_keeps_checkpoint_without_finishing(harness, failure, attempts):
    harness.responses_by_license["C0007"] = failure
    with pytest.raises(type(failure), match=str(failure)):
        await worker.import_profiles(harness.ctx, harness.task)
    run_id = harness.store_by_name["claim_run"].call_args.args[0]["run_id"]
    assert harness.requests == ["00042", *["C0007"] * attempts]
    assert worker.acquisition.read_response(harness.artifact_root / run_id / "profiles" / "00042.json", "00042") == harness.responses_by_license["00042"]
    assert not (harness.artifact_root / run_id / "manifest.json").exists()
    assert harness.writes == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()
    assert harness.store_by_name["mark_run_failed"].call_args.args[0] == run_id
    harness.store_by_name["publish_run"].assert_not_called()
    harness.store_by_name["retain_source_history"].assert_not_called()


async def test_persistence_failure_rolls_back_batch_and_preserves_pointer(harness):
    harness.fail_model = worker.ProviderProfileFact
    with pytest.raises(RuntimeError, match="synthetic persistence failure"):
        await worker.import_profiles(harness.ctx, harness.task)
    assert len(harness.rows_for(worker.ProviderProfileArtifact)) == 1
    assert harness.rows_for(worker.ProviderProfileSourceRecord) == []
    assert harness.rows_for(worker.ProviderProfileFact) == []
    harness.finish.assert_not_called()
    harness.store_by_name["publish_run"].assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_error_response_fails_before_any_profile_batch_is_stored(harness):
    harness.responses_by_license["C0007"] = _response("C0007", html="<html><body>Upstream unavailable</body></html>")
    with pytest.raises(ValueError, match="result_envelope_invalid"):
        await worker.import_profiles(harness.ctx, harness.task)
    assert harness.rows_for(worker.ProviderProfileSourceRecord) == []
    assert harness.rows_for(worker.ProviderProfileFact) == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_fresh_path_collision_never_overwrites_existing_artifacts(harness):
    run_id = worker._hash([worker.SOURCE_KEY, harness.task["run_id"]])
    existing = harness.artifact_root / run_id
    existing.mkdir(parents=True)
    retained = existing / "cohort.json"
    retained.write_text("existing evidence")
    with pytest.raises(FileExistsError):
        await worker.import_profiles(harness.ctx, harness.task)
    assert retained.read_text() == "existing evidence"
    assert harness.requests == [] and harness.writes == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_failed_claim_does_not_fail_someone_elses_run(harness):
    harness.store_by_name["claim_run"].side_effect = RuntimeError("synthetic occupied scope")
    with pytest.raises(RuntimeError, match="synthetic occupied scope"):
        await worker.import_profiles(harness.ctx, harness.task)
    assert not harness.artifact_root.exists()
    harness.store_by_name["mark_run_failed"].assert_not_called()


@pytest.mark.parametrize("task", [
    {"max_providers": True}, {"max_providers": 0}, {"max_providers": "2"},
    {"resume_from": "../previous"}, {"resume_from": 1},
])
async def test_invalid_task_is_rejected_before_side_effects(harness, task):
    with pytest.raises(ValueError, match="kentucky_profile_"):
        await worker.import_profiles(harness.ctx, task)
    harness.store_by_name["ensure_tables"].assert_not_called()
    harness.store_by_name["claim_run"].assert_not_called()
    assert not harness.artifact_root.exists()


async def test_preflight_cancellation_claims_nothing(harness):
    harness.redis.get.return_value = b"1"
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles(harness.ctx, harness.task)
    harness.store_by_name["ensure_tables"].assert_not_called()
    harness.store_by_name["claim_run"].assert_not_called()
    assert harness.requests == []


async def test_acquisition_cancellation_preserves_first_checkpoint(harness, monkeypatch):
    original_fetch = harness.fetch

    async def fetch_then_cancel(session, license_number):
        response = await original_fetch(session, license_number)
        harness.redis.get.return_value = b"1"
        return response

    monkeypatch.setattr(worker.acquisition, "fetch_profile", fetch_then_cancel)
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles(harness.ctx, harness.task)
    run_id = harness.store_by_name["claim_run"].call_args.args[0]["run_id"]
    assert harness.requests == ["00042"]
    assert (harness.artifact_root / run_id / "profiles" / "00042.json").exists()
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_persistence_cancellation_stops_before_source_rows(harness):
    harness.cancel_model = worker.ProviderProfileArtifact
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles(harness.ctx, harness.task)
    assert len(harness.requests) == 3
    assert len(harness.rows_for(worker.ProviderProfileArtifact)) == 1
    assert harness.rows_for(worker.ProviderProfileSourceRecord) == []
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


async def test_persistence_cancellation_between_record_and_fact_writes_rolls_back(harness):
    harness.cancel_model = worker.ProviderProfileSourceRecord
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles(harness.ctx, harness.task)
    assert len(harness.rows_for(worker.ProviderProfileArtifact)) == 1
    assert harness.rows_for(worker.ProviderProfileSourceRecord) == harness.rows_for(worker.ProviderProfileFact) == []
    harness.finish.assert_not_awaited()
    harness.store_by_name["publish_run"].assert_not_awaited()


async def test_cancellation_during_cohort_capture_prevents_a_claim(harness, monkeypatch):
    async def capture_then_cancel(_schema):
        harness.redis.get.return_value = b"1"
        return harness.cohort

    monkeypatch.setattr(worker.acquisition, "capture_registry_cohort", capture_then_cancel)
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles(harness.ctx, harness.task)
    harness.store_by_name["claim_run"].assert_not_awaited()
    assert not harness.artifact_root.exists() and harness.requests == []


async def test_cancellation_after_claim_prevents_the_run_directory(harness):
    async def claim_then_cancel(_run):
        harness.redis.get.return_value = b"1"

    harness.store_by_name["claim_run"].side_effect = claim_then_cancel
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles(harness.ctx, harness.task)
    assert list(harness.artifact_root.iterdir()) == [] and harness.requests == []
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


@pytest.mark.parametrize("changed_field", ["body_text", "downloaded_at", "response_bytes"])
async def test_retention_requires_acquisition_digest_and_byte_count(harness, changed_field):
    """Rehashing a changed response cannot bypass the manifest's aggregate digest."""
    async def rewrite_after_acquisition(run_id, _fields):
        if changed_field == "response_bytes":
            _fields["metrics"]["response_bytes"] += 1
            return
        path = harness.artifact_root / run_id / "profiles" / "C0007.json"
        response = json.loads(path.read_text())
        if changed_field == "body_text":
            response["body_text"] = response["body_text"].replace("Synthetic &amp; Medical School", "Different Medical School")
            response["content_sha256"] = hashlib.sha256(response["body_text"].encode()).hexdigest()
        else:
            response["downloaded_at"] = "2026-09-08T13:00:00+00:00"
        path.write_bytes(worker.acquisition.encoded_json(response))

    harness.store_by_name["update_run"].side_effect = rewrite_after_acquisition
    with pytest.raises(ValueError, match="retained_responses_changed"):
        await worker.import_profiles(harness.ctx, harness.task)
    harness.finish.assert_not_awaited()
    harness.store_by_name["publish_run"].assert_not_awaited()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()
    run_id = harness.store_by_name["claim_run"].call_args.args[0]["run_id"]
    manifest = json.loads((harness.artifact_root / run_id / "manifest.json").read_text())
    assert manifest["acquisition"] == harness.store_by_name["update_run"].call_args.args[1]["metrics"]


async def test_retention_failure_does_not_downgrade_completed_import(harness):
    harness.store_by_name["retain_source_history"].side_effect = RuntimeError("synthetic cleanup error")
    receipt = await worker.import_profiles(harness.ctx, harness.task)
    assert receipt["responses"] == 3
    harness.finish.assert_awaited_once()
    harness.store_by_name["mark_run_failed"].assert_not_called()


def _retained_run(harness, *, limit=2, strategy=worker.SAMPLING_STRATEGY):
    run_id = "b" * 64
    directory = harness.artifact_root / run_id
    (directory / "profiles").mkdir(parents=True)
    worker.acquisition.write_new_json(directory / "cohort.json", harness.cohort)
    selected = worker._selected_roots(harness.cohort, limit, strategy=strategy)
    for root in selected:
        worker.acquisition.write_new_json(directory / "profiles" / f"{root['license_number']}.json", harness.responses_by_license[root["license_number"]])
    manifest = worker._source_manifest({"max_providers": limit}, harness.cohort, PREDECESSOR, sampling_strategy=strategy)
    return {"run_id": run_id, "source_manifest": manifest}, directory


async def test_historical_resume_preserves_old_hash_sample(harness, monkeypatch):
    previous, directory = _retained_run(harness, strategy=worker.LEGACY_SAMPLING_STRATEGY)
    previous["source_manifest"].pop("sampling_strategy")
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous))
    await worker.import_profiles(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous["run_id"]})
    claimed = harness.store_by_name["claim_run"].call_args.args[0]
    assert claimed["source_manifest"]["sampling_strategy"] == worker.LEGACY_SAMPLING_STRATEGY
    assert harness.requests == []
    assert {path.name for path in (directory / "profiles").iterdir()} == {
        path.name for path in (harness.artifact_root / claimed["run_id"] / "profiles").iterdir()}


async def test_legacy_uncheckpointed_oversize_is_not_repeated_on_resume(harness, monkeypatch):
    previous, _directory = _retained_run(harness)
    previous["source_manifest"].pop("acquisition_strategy")
    previous["error"] = "ValueError: kentucky_profile_response_too_large"
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous))
    with pytest.raises(ValueError, match="legacy_oversized_checkpoint_incomplete"):
        await worker.import_profiles(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous["run_id"]})
    assert harness.requests == []
    harness.store_by_name["claim_run"].assert_not_called()


async def test_resume_uses_frozen_cohort_and_exact_bytes_in_new_directory(harness, monkeypatch):
    previous_run, previous_directory = _retained_run(harness)
    bytes_by_name = {path.name: path.read_bytes() for path in (previous_directory / "profiles").iterdir()}

    async def recovered_resume(*_args, **_kwargs):
        harness.reconcile.assert_awaited_once()
        return previous_run

    monkeypatch.setattr(worker.store, "read_resume_run", recovered_resume)
    worker.acquisition.capture_registry_cohort.side_effect = AssertionError("Resume must not read the current registry")
    await worker.import_profiles(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous_run["run_id"]})
    run_row = harness.store_by_name["claim_run"].call_args.args[0]
    new_directory = harness.artifact_root / run_row["run_id"]
    assert new_directory != previous_directory and harness.requests == []
    assert {path.name: path.read_bytes() for path in (new_directory / "profiles").iterdir()} == bytes_by_name
    assert {path.name: path.read_bytes() for path in (previous_directory / "profiles").iterdir()} == bytes_by_name
    assert run_row["source_manifest"]["cohort_sha256"] == previous_run["source_manifest"]["cohort_sha256"]
    assert run_row["source_manifest"]["expected_current_run_id"] == PREDECESSOR
    assert harness.finish.call_args.args[3]["reused_responses"] == 2
    artifact = harness.rows_for(worker.ProviderProfileArtifact)[0]
    for root in worker._selected_roots(harness.cohort, 2):
        old_artifact_by_field = {**artifact, "run_id": previous_run["run_id"], "artifact_id": "c" * 64}
        _, old_facts = worker._source_rows(root, harness.responses_by_license[root["license_number"]], old_artifact_by_field, 1)
        _, new_facts = worker._source_rows(root, harness.responses_by_license[root["license_number"]], artifact, 1)
        assert old_facts[0]["logical_fact_key"] == new_facts[0]["logical_fact_key"]
        assert old_facts[0]["fact_id"] != new_facts[0]["fact_id"]


async def test_resume_reports_retained_validation_before_acquisition(harness, monkeypatch):
    previous_run, retained_directory = _retained_run(harness, limit=None)
    missing_license = harness.cohort["roots"][-1]["license_number"]
    (retained_directory / "profiles" / f"{missing_license}.json").unlink()
    bytes_by_name = {path.name: path.read_bytes() for path in (retained_directory / "profiles").iterdir()}
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous_run))
    observations = []
    merged_events = []

    def observe(**event):
        merged_event_by_field = dict(event)
        live_progress._merge_previous_progress(merged_event_by_field, merged_events[-1] if merged_events else {}, now=datetime.now(UTC))
        assert (merged_event_by_field["phase"], merged_event_by_field["done"]) == (event["phase"], event["done"])
        merged_events.append(merged_event_by_field)
        run_id = event["metrics"]["provider_profile_run_id"]
        destination = harness.artifact_root / run_id / "profiles"
        observations.append((event["phase"], event["done"], event["total"], len(list(destination.glob("*.json")))))
        if event["phase"] == "checking_retained":
            assert harness.requests == [] and not destination.exists()
        elif event["phase"] == "acquiring":
            assert event["done"] == len(list(destination.glob("*.json")))

    worker.enqueue_live_progress.side_effect = observe
    await worker.import_profiles(harness.ctx, {**harness.task, "resume_from": previous_run["run_id"]})

    phases = [phase for phase, *_ in observations]
    acquisition_start = phases.index("acquiring")
    assert all(phase == "checking_retained" for phase in phases[:acquisition_start])
    assert observations[acquisition_start - 1] == ("checking_retained", 3, 3, 0)
    assert observations[acquisition_start] == ("acquiring", 0, 3, 0)
    assert [observation for observation in observations if observation[0] == "acquiring"][-1] == ("acquiring", 3, 3, 3)
    assert harness.requests == [missing_license]
    assert {path.name: path.read_bytes() for path in (retained_directory / "profiles").iterdir()} == bytes_by_name
    metrics = harness.finish.call_args.args[3]
    assert metrics["responses"] == 3 and metrics["reused_responses"] == 2
    delayed_validation_by_field = dict(merged_events[acquisition_start - 1])
    live_progress._merge_previous_progress(delayed_validation_by_field, merged_events[-1], now=datetime.now(UTC))
    assert (delayed_validation_by_field["phase"], delayed_validation_by_field["done"]) == ("retaining", 3)


async def test_changed_resume_cohort_is_refused_before_new_claim(harness, monkeypatch):
    previous_run, previous_directory = _retained_run(harness)
    changed = _cohort(("00042", "C0007", "C0009"))
    (previous_directory / "cohort.json").write_bytes(worker.acquisition.encoded_json(changed))
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous_run))
    with pytest.raises(ValueError, match="resume_cohort_changed"):
        await worker.import_profiles(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous_run["run_id"]})
    harness.store_by_name["claim_run"].assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_not_called()
    assert harness.requests == []


async def test_changed_retained_response_is_never_replaced_by_network(harness, monkeypatch):
    previous_run, previous_directory = _retained_run(harness)
    path = next((previous_directory / "profiles").iterdir())
    response = json.loads(path.read_text())
    response["body_text"] += "changed"
    path.write_bytes(worker.acquisition.encoded_json(response))
    monkeypatch.setattr(worker.store, "read_resume_run", AsyncMock(return_value=previous_run))
    with pytest.raises(ValueError, match="response_changed"):
        await worker.import_profiles(harness.ctx, {**harness.task, "max_providers": 2, "resume_from": previous_run["run_id"]})
    assert harness.requests == []
    assert json.loads(path.read_text()) == response
    harness.finish.assert_not_called()
    harness.store_by_name["mark_run_failed"].assert_awaited_once()


def test_registry_adapter_and_worker_agree_without_unmanaged_cli(monkeypatch):
    from click.testing import CliRunner
    from api import control_imports, control_workers
    import process

    importer = "kentucky-kbml-profile"
    registration = next(entry for entry in control_imports.importer_registry() if entry["name"] == importer)
    assert registration["family"] == "provider" and registration["depends_on"] == ["npi"]
    assert registration["cancelable"] is True and registration["enqueue_adapter"] == "arq_single_job"
    assert {parameter["name"] for parameter in registration["params_schema"]} == {"max_providers", "resume_from"}
    adapter = control_imports._SINGLE_JOB_ADAPTERS[importer]
    dispatch_payload = control_imports._adapter_payload(adapter, {
        "run_id": "synthetic-control", "importer": importer, "family": "provider",
    }, {"max_providers": 2, "resume_from": "b" * 64})
    assert dispatch_payload["target_module"] == "process.kentucky_profile"
    assert dispatch_payload["target_function"] == "import_profiles" and dispatch_payload["call_style"] == "ctx_task"
    assert dispatch_payload["task"]["max_providers"] == 2 and dispatch_payload["task"]["resume_from"] == "b" * 64
    spec = next(entry for entry in control_workers.worker_registry() if importer in entry["importers"] and entry["role"] == "start")
    assert spec["worker_class"] == "process.KentuckyKBMLProfile"
    assert spec["queue"] == adapter["queue"] == registration["queue"] == process.KentuckyKBMLProfile.queue_name
    assert process.KentuckyKBMLProfile.max_jobs == 1
    assert process.KentuckyKBMLProfile.functions[0].name == adapter["function"]
    assert process.KentuckyKBMLProfile.functions[0].max_tries == 1
    assert process.process_group.commands[importer] is worker.kentucky_kbml_profile
    cli_help = CliRunner().invoke(process.process_group, [importer, "--help"])
    assert cli_help.exit_code == 0
    assert "--max-providers" in cli_help.output and "--resume-from" in cli_help.output
    import_profiles = AsyncMock()
    monkeypatch.setattr(worker, "import_profiles", import_profiles)
    cli_run = CliRunner().invoke(process.process_group, [importer, "--max-providers", "2"])
    assert cli_run.exit_code == 2 and "managed import API" in cli_run.output
    import_profiles.assert_not_awaited()


@pytest.mark.parametrize("limit", [None, 2])
async def test_finish_delegates_reviewed_completion_after_validating(monkeypatch, limit):
    ctx_by_field = {"context": {"_control_attempt_id": "synthetic-attempt"}}
    task_by_field = {"run_id": "synthetic-control"}
    run_by_field = {"run_id": "c" * 64, "source_manifest": {"max_providers": limit}}
    metrics_by_field = {"responses": 2, "acquisition_complete": True}
    update = AsyncMock()
    cancel = AsyncMock()
    completed_by_field = {"run_id": run_by_field["run_id"], "published": limit is None, "terminal_progress": {"pct": 100}}

    async def complete(ctx, task, run, metrics):
        cancel.assert_awaited_once_with(ctx_by_field, task_by_field)
        update.assert_awaited_once_with(run_by_field["run_id"], {"status": "validating", "metrics": metrics_by_field})
        assert (ctx, task, run, metrics) == (ctx_by_field, task_by_field, run_by_field, metrics_by_field)
        return completed_by_field

    monkeypatch.setattr(worker, "raise_if_cancelled", cancel)
    monkeypatch.setattr(worker.store, "update_run", update)
    monkeypatch.setattr(worker, "complete_run", complete)
    assert await worker._finish_run(ctx_by_field, task_by_field, run_by_field, metrics_by_field) is completed_by_field


async def test_cancelled_finish_does_not_enter_completion(monkeypatch):
    update = AsyncMock()
    complete = AsyncMock()
    monkeypatch.setattr(worker, "raise_if_cancelled", AsyncMock(side_effect=ImportCancelledError("synthetic cancel")))
    monkeypatch.setattr(worker.store, "update_run", update)
    monkeypatch.setattr(worker, "complete_run", complete)
    with pytest.raises(ImportCancelledError):
        await worker._finish_run({}, {}, {"run_id": "c" * 64}, {})
    update.assert_not_called()
    complete.assert_not_called()


@pytest.mark.parametrize("task", [{}, {"run_id": None}, {"run_id": ""}, {"run_id": " "}, {"run_id": 123}])
async def test_unmanaged_run_cannot_claim_source_or_start_acquisition(harness, task):
    with pytest.raises(ValueError, match="managed_run_required"):
        await worker.import_profiles(harness.ctx, task)
    harness.store_by_name["ensure_tables"].assert_not_awaited()
    harness.store_by_name["claim_run"].assert_not_awaited()
    worker.acquisition.capture_registry_cohort.assert_not_awaited()
    assert harness.requests == [] and harness.writes == []
    assert not harness.artifact_root.exists()


@pytest.mark.parametrize("component", ["run", "profiles"])
def test_retained_resume_rejects_symlink_directories(tmp_path, component):
    actual = tmp_path / "actual"
    actual.mkdir()
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    run_id = "b" * 64
    run_directory = artifact_root / run_id
    if component == "run":
        run_directory.symlink_to(actual, target_is_directory=True)
    else:
        run_directory.mkdir()
        (run_directory / "profiles").symlink_to(actual, target_is_directory=True)
    with pytest.raises(ValueError, match="artifact_symlink"):
        worker._retained_directory(artifact_root, run_id)
    assert actual.is_dir() and list(actual.iterdir()) == []


def test_artifact_root_uses_pvc_default_and_explicit_override(monkeypatch, tmp_path):
    from pathlib import Path

    monkeypatch.delenv("HLTHPRT_KY_KBML_ARTIFACT_ROOT", raising=False)
    assert worker._artifact_root() == Path("/work/kentucky-kbml")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HLTHPRT_KY_KBML_ARTIFACT_ROOT", "relative-artifacts")
    assert worker._artifact_root() == tmp_path / "relative-artifacts"
    assert not (tmp_path / "relative-artifacts").exists()


@pytest.mark.parametrize("setting", ["", " "])
def test_artifact_root_rejects_blank_override(monkeypatch, setting):
    monkeypatch.setenv("HLTHPRT_KY_KBML_ARTIFACT_ROOT", setting)
    with pytest.raises(ValueError, match="artifact_root_invalid"):
        worker._artifact_root()


async def test_symlink_artifact_root_is_rejected_before_cohort_and_claim(harness, monkeypatch, tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(actual, target_is_directory=True)
    monkeypatch.setenv("HLTHPRT_KY_KBML_ARTIFACT_ROOT", str(linked))
    with pytest.raises(ValueError, match="artifact_symlink"):
        await worker.import_profiles(harness.ctx, harness.task)
    worker.acquisition.capture_registry_cohort.assert_not_awaited()
    harness.store_by_name["claim_run"].assert_not_awaited()
    assert list(actual.iterdir()) == [] and harness.requests == []


@pytest.mark.parametrize("run_id", ["../other", "", None])
def test_retained_directory_validates_run_id_before_deriving_path(tmp_path, run_id):
    with pytest.raises(ValueError, match="run_id_invalid"):
        worker._retained_directory(tmp_path, run_id)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("profiles", ["", '{"process.PTGHuge":{"limits":{"memory":"64Gi"}}}'])
def test_ky_resources_fall_back_without_changing_other_workers(monkeypatch, profiles):
    from api import control_workers

    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", profiles)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "2")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_MEMORY_REQUEST", "4Gi")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_CPU_LIMIT", "16")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "64Gi")
    ky_spec = control_workers.WorkerSpec("arq:KentuckyKBMLProfile", "process.KentuckyKBMLProfile", ("kentucky-kbml-profile",))
    other_spec = control_workers.WorkerSpec("arq:CMSDoctors", "process.CMSDoctors", ("cms-doctors",))
    assert control_workers._worker_job_resources(ky_spec) == {
        "requests": {"cpu": "500m", "memory": "512Mi"}, "limits": {"cpu": "4", "memory": "4Gi"},
    }
    assert control_workers._worker_job_resources(other_spec) == {
        "requests": {"cpu": "2", "memory": "4Gi"}, "limits": {"cpu": "16", "memory": "64Gi"},
    }


@pytest.mark.parametrize("selector", ["process.KentuckyKBMLProfile", "arq:KentuckyKBMLProfile"])
def test_explicit_ky_resource_profile_precedes_default(monkeypatch, selector):
    from api import control_workers

    resources_by_kind = {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "2", "memory": "2Gi"}}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", json.dumps({selector: resources_by_kind}))
    spec = control_workers.WorkerSpec("arq:KentuckyKBMLProfile", "process.KentuckyKBMLProfile", ("kentucky-kbml-profile",))
    assert control_workers._worker_job_resources(spec) == resources_by_kind
