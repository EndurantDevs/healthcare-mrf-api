# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
import hashlib
import json
import socket
from collections import deque
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import new_york_profile as worker
from process import new_york_profile_store as profile_store
from process.control_cancel import ImportCancelledError
from tests.test_live_progress import _merge_nested_profile_snapshots
from tests.test_new_york_nysed_profile import (
    PUBLIC_HEADER,
    _profile_body,
)
from tests.test_new_york_nysed_profile import (
    SourceResponse as NysedResponse,
)
from tests.test_new_york_nysed_profile import (
    SourceSession as NysedSession,
)
from tests.test_new_york_profile_acquisition import SourceResponse, SourceSession, _education, _search
from tests.test_new_york_profile_binding import _candidate, _snapshot

TASK = {"run_id": "synthetic-control-run"}


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    def deny(*args, **kwargs):
        raise AssertionError("Managed profile checks forbid network access")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket.socket, "connect_ex", deny)
    monkeypatch.setattr(socket, "create_connection", deny)


def _cases():
    return [
        {"license": "111111", "middle": "Morgan", "transposed": True},
        {"license": "222222", "nysed_held": True},
        {"license": "333333", "nysed_held": True, "registry_changes": {"first_name": "Other"}},
        {"license": "444444", "total": 0},
        {"license": "555555", "middle": "Morgan", "transposed": True, "collision": True},
        {"license": "666666", "total": 2},
    ]


def _registry(cases):
    candidates = []
    for case in cases:
        candidates.append(_candidate(license_number=case["license"], **case.get("registry_changes", {})))
        if case.get("collision"):
            candidates.append(_candidate(license_number=case["license"], taxonomy_grouping="Nursing Service Providers"))
    return _snapshot(candidates)


def _sessions(cases, directory):
    sessions = []
    for case in cases:
        license_number = case["license"]
        middle = case.get("middle", "")
        names = {"physicianFirstName": "Example", "physicianLastName": "Alex"} if case.get("transposed") else {}
        replies = [SourceResponse(_search(case.get("total", 1), **names))]
        if case.get("total", 1) == 1:
            replies.append(SourceResponse(_education(license_number, middleName=middle, nationalProviderId="")))
        sessions.append(SourceSession(replies, directory / "profiles" / license_number))
        if case.get("total", 1) != 1:
            continue
        response = (
            NysedResponse(raw=b"", status=204)
            if case.get("nysed_held")
            else NysedResponse(_profile_body(license_number, name=("EXAMPLE ALEX " + middle).strip()))
        )
        sessions.append(NysedSession(response))
    return sessions


def _install_store(monkeypatch, state):
    async def claim(run):
        profile_store.store._manifest(run)
        state.run = copy.deepcopy(run)
        state.claimed = True

    async def upsert(model, rows, identifier):
        assert state.claimed
        assert len(rows) <= worker.BATCH_SIZE
        state.writes.append((model, copy.deepcopy(rows), identifier))

    async def complete(ctx, task, run, metrics):
        assert state.claimed and not state.failed
        artifacts = [row for model, rows, _ in state.writes if model == worker.ProviderProfileArtifact for row in rows]
        bundle = profile_store.store._bundle(run, artifacts)
        assert bundle["acquisition"] == metrics
        state.published = run["run_id"]
        return {"published": True, **metrics}

    @asynccontextmanager
    async def transaction():
        yield

    async def failed(run_id, error):
        state.failed.append((run_id, type(error).__name__))

    monkeypatch.setattr(
        worker,
        "store",
        SimpleNamespace(
            read_publication=AsyncMock(return_value=None),
            claim_run=claim,
            update_run=AsyncMock(),
            mark_run_failed=failed,
            retain_source_history=AsyncMock(),
        ),
    )
    monkeypatch.setattr(
        worker, "completion", SimpleNamespace(reconcile_failed_control_runs=AsyncMock(), complete_run=complete)
    )
    monkeypatch.setattr(worker, "_upsert_rows", upsert)
    monkeypatch.setattr(worker.db, "transaction", transaction)


@pytest.fixture
def managed_case(tmp_path, monkeypatch):
    artifact_root = tmp_path.resolve() / "artifacts"
    monkeypatch.setenv("HLTHPRT_NYPP_ARTIFACT_ROOT", str(artifact_root))
    monkeypatch.setenv("HLTHPRT_NYSED_PUBLIC_API_KEY", PUBLIC_HEADER)
    monkeypatch.setenv("HLTHPRT_NYPP_MAX_RETAINED_BYTES", str(1024**3))
    monkeypatch.setenv("HLTHPRT_NYPP_DEADLINE_SECONDS", "120")
    monkeypatch.setenv("HLTHPRT_NYPP_MAX_BUNDLE_BYTES", str(512 * 1024 * 1024))
    monkeypatch.setattr(worker, "ensure_tables", AsyncMock())
    monkeypatch.setattr(worker, "enqueue_live_progress", lambda **kwargs: None)
    monkeypatch.setattr(worker.acquisition, "REQUEST_INTERVAL_SECONDS", 0)
    state = SimpleNamespace(claimed=False, failed=[], writes=[], published=None, cancel=False, run=None)

    async def cancellation(ctx, task):
        if state.cancel:
            raise ImportCancelledError("Synthetic managed cancellation")

    monkeypatch.setattr(worker, "raise_if_cancelled", cancellation)
    _install_store(monkeypatch, state)
    state.directory = artifact_root / worker._hash([worker.SOURCE_KEY, TASK["run_id"]])

    def install(cases):
        state.sessions = _sessions(cases, state.directory)
        pending = deque(state.sessions)

        def session(**options):
            assert state.claimed, "Source requests must follow the source claim"
            return pending.popleft()

        monkeypatch.setattr(worker.acquisition.aiohttp, "ClientSession", session)
        monkeypatch.setattr(worker.registry, "capture_registry_snapshot", AsyncMock(return_value=_registry(cases)))
        state.pending = pending
        return state

    return install


async def test_full_mixed_cohort_preserves_sources_holds_and_support(managed_case):
    state = managed_case(_cases())
    result = await worker.import_profiles({}, TASK)
    assert result["published"] is True and result["responses"] == 6
    assert result["acquired_profiles"] == 4 and result["held_attempts"] == 2 and result["facts"] == 4
    assert not state.pending and all(session.closed for session in state.sessions)
    records = [row for model, rows, _ in state.writes if model == worker.ProviderProfileSourceRecord for row in rows]
    by_license = {row["license_number"]: row for row in records}
    assert set(by_license) == {"111111", "222222", "333333", "555555"}
    assert by_license["111111"]["matched_npi"] == by_license["222222"]["matched_npi"] == 1000000004
    assert by_license["333333"]["matched_npi"] is None and by_license["555555"]["matched_npi"] is None
    assert by_license["111111"]["normalized_payload"]["quality_flags"] == ["search_header_name_disagreement"]
    assert by_license["222222"]["match_evidence"]["registry_binding"]["method"] == "exact_ny_license_name_components"
    bundle = json.loads((state.directory / "manifest.json").read_bytes())
    assert set(bundle["profiles"]) == {case["license"] for case in _cases()}
    support_by_license = bundle["acquisition"]["nysed_support"]
    assert set(support_by_license) == set(by_license)
    for license_number, support in support_by_license.items():
        assert set(support["file_sha256"]) == set(worker.NYSED_FILES)
        assert support["capture_manifest"]["run_id"] == state.run["run_id"]
        assert (support["source_identity"] is None) == (license_number in {"222222", "333333"})
    for descriptor in bundle["profiles"].values():
        assert descriptor["capture_manifest"]["run_id"] == state.run["run_id"]
    assert not state.failed and state.published == state.run["run_id"]
    validating = worker.store.update_run.await_args.args[1]["metrics"]
    assert "nysed_support" not in validating and validating["responses"] == 6
    assert all(PUBLIC_HEADER.encode() not in path.read_bytes() for path in state.directory.rglob("*.json"))


async def test_complete_cohort_is_not_limited_to_one_hundred(managed_case):
    cases = [{"license": str(100000 + index), **({"total": 0} if index else {})} for index in range(102)]
    state = managed_case(cases)
    result = await worker.import_profiles({}, TASK)
    assert result["responses"] == 102 and result["held_attempts"] == 101
    assert state.run["source_manifest"]["full_cohort_licenses"] == 102
    assert not state.pending


async def test_null_empty_search_retains_hold_without_nysed_or_provider_rows(managed_case):
    state = managed_case([{"license": "111111", "total": 0}, {"license": "222222", "nysed_held": True}])
    response = _search(0)
    response["data"]["physicians"] = None
    state.sessions[0].responses[0] = SourceResponse(response)
    result = await worker.import_profiles({}, TASK)
    assert result["responses"] == 2 and result["held_attempts"] == result["acquired_profiles"] == 1
    assert not state.pending and len(state.sessions[0].requests) == 1
    assert not (state.directory / "nysed" / "111111").exists()
    bundle = json.loads((state.directory / "manifest.json").read_bytes())
    descriptor = bundle["profiles"]["111111"]
    assert descriptor["acquisition_outcome"] == "held" and descriptor["reported_total"] == 0
    assert descriptor["record_id"] is None and descriptor["facts"] == {}
    assert set(bundle["acquisition"]["nysed_support"]) == {"222222"}
    records = [row for model, rows, _ in state.writes if model == worker.ProviderProfileSourceRecord for row in rows]
    assert len(records) == 1 and records[0]["license_number"] == "222222"
    facts = [row for model, rows, _ in state.writes if model == worker.ProviderProfileFact for row in rows]
    assert len(facts) == 1 and facts[0]["source_record_id"] == records[0]["record_id"]
    captured = json.loads((state.directory / "profiles" / "111111" / "search.response.json").read_bytes())
    assert captured["content_sha256"] == hashlib.sha256(worker.encoded_json(response)).hexdigest()


async def test_first_search_failure_resets_registry_progress_to_unprocessed_licenses(managed_case, monkeypatch):
    state = managed_case([{"license": "111111"}])
    state.sessions[0].responses[0] = SourceResponse(_search(), status=500)
    observations = []
    monkeypatch.setattr(worker, "enqueue_live_progress", lambda **event: observations.append(event))
    worker._progress(TASK, "registry snapshot", 2, 2)
    with pytest.raises(ValueError, match="http_failure"):
        await worker.import_profiles({}, TASK)
    assert len(observations) == 2 and observations[0]["pct"] == 100
    merged = _merge_nested_profile_snapshots(TASK["run_id"], observations)
    assert merged[-1]["phase"] == "retaining" and merged[-1]["unit"] == "license"
    assert merged[-1]["done"] == merged[-1]["pct"] == 0 and merged[-1]["total"] == 1
    assert state.failed and not state.published and not state.writes


async def test_budget_counts_all_retained_files(managed_case, monkeypatch):
    state = managed_case(_cases())
    original = worker._parameters
    budgets = []

    def parameters(task):
        prepared = original(task)
        budgets.append(prepared[2])
        return prepared

    monkeypatch.setattr(worker, "_parameters", parameters)
    await worker.import_profiles({}, TASK)
    assert budgets[0]["retained_bytes"] == sum(
        path.stat().st_size for path in state.directory.rglob("*") if path.is_file()
    )


async def test_fact_writes_are_bounded_and_cancelable(managed_case, monkeypatch):
    state = managed_case(_cases()[:1])
    body = _education("111111", middleName="Morgan", nationalProviderId="")
    body["data"]["medSchools"] = [
        {"schoolName": f"Example Medical School {index}", "gradDate": "2001"} for index in range(301)
    ]
    state.sessions[0].responses[1] = SourceResponse(body)
    original = worker._upsert_rows

    async def upsert(model, rows, identifier):
        await original(model, rows, identifier)
        if model == worker.ProviderProfileFact:
            state.cancel = True

    monkeypatch.setattr(worker, "_upsert_rows", upsert)
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles({}, TASK)
    fact_writes = [rows for model, rows, _ in state.writes if model == worker.ProviderProfileFact]
    assert len(fact_writes) == 1 and len(fact_writes[0]) == 250
    assert state.failed and not state.published


@pytest.mark.parametrize("field", ["max_providers", "resume_from", "sources", "professions", "license_types"])
async def test_partial_scope_never_claims_or_requests(managed_case, field):
    state = managed_case(_cases())
    with pytest.raises(ValueError, match="complete_cohort_required"):
        await worker.import_profiles({}, {**TASK, field: 1})
    assert not state.claimed and all(not session.requests for session in state.sessions)


@pytest.mark.parametrize(
    "environment",
    [
        "HLTHPRT_NYPP_MAX_RETAINED_BYTES",
        "HLTHPRT_NYPP_MAX_BUNDLE_BYTES",
        "HLTHPRT_NYPP_DEADLINE_SECONDS",
        "HLTHPRT_NYSED_PUBLIC_API_KEY",
    ],
)
@pytest.mark.parametrize("configured", ["", "0", "-1", "not valid"])
async def test_resource_and_header_validation_precedes_claim(managed_case, monkeypatch, environment, configured):
    state = managed_case(_cases())
    if environment.endswith("API_KEY") and configured in {"0", "-1"}:
        configured = "\n"
    monkeypatch.setenv(environment, configured)
    with pytest.raises(ValueError):
        await worker.import_profiles({}, TASK)
    assert not state.claimed and not state.published


async def test_cancel_between_sources_preserves_captured_files(managed_case, monkeypatch):
    state = managed_case(_cases())
    original = worker.acquisition.acquire_license

    async def cancel_after(*args, **kwargs):
        captured = await original(*args, **kwargs)
        state.cancel = True
        return captured

    monkeypatch.setattr(worker.acquisition, "acquire_license", cancel_after)
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles({}, TASK)
    assert state.failed and not state.published and not state.writes
    assert (state.directory / "profiles" / "111111" / "education.response.json").exists()
    assert not (state.directory / "nysed" / "111111").exists()


async def test_deadline_interrupts_waiting_source(managed_case, monkeypatch):
    state = managed_case(_cases())
    original = worker._parameters

    def parameters(task):
        root, api_key, budget = original(task)
        budget["deadline"] = worker.time.monotonic() + 0.15
        return root, api_key, budget

    async def blocked(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(worker, "_parameters", parameters)
    monkeypatch.setattr(worker.acquisition, "acquire_license", blocked)
    with pytest.raises(TimeoutError):
        await worker.import_profiles({}, TASK)
    assert state.failed and not state.published


@pytest.mark.parametrize("failure", ["transport", "support_transport", "database", "bundle_write"])
async def test_uncertainty_or_write_failure_never_publishes(managed_case, monkeypatch, failure):
    state = managed_case(_cases())
    if failure == "transport":
        state.sessions[0].responses = [OSError("Synthetic transport failure")]
    elif failure == "support_transport":
        state.sessions[1].response = OSError("Synthetic support failure")
    elif failure == "database":
        monkeypatch.setattr(worker, "_upsert_rows", AsyncMock(side_effect=OSError("Synthetic database failure")))
    else:
        original = worker._write_json

        def write(path, content_by_field, budget, **options):
            if path == state.directory / "manifest.json":
                raise OSError("Synthetic bundle write failure")
            return original(path, content_by_field, budget, **options)

        monkeypatch.setattr(worker, "_write_json", write)
    with pytest.raises((OSError, ValueError)):
        await worker.import_profiles({}, TASK)
    assert state.claimed and state.failed and not state.published
    assert state.directory.is_dir()


@pytest.mark.parametrize("resource", ["bytes", "disk", "inodes"])
async def test_resource_failure_after_capture_stops_before_next_source(managed_case, monkeypatch, resource):
    state = managed_case(_cases())
    original = worker._account_capture

    def account(directory, budget):
        original(directory, budget)
        if resource == "bytes":
            budget["max_bytes"] = budget["retained_bytes"] - 1
        else:
            space = SimpleNamespace(
                f_bavail=0 if resource == "disk" else 10**12,
                f_frsize=4096,
                f_files=100,
                f_favail=0 if resource == "inodes" else 100,
            )
            monkeypatch.setattr(worker.os, "statvfs", lambda path: space)

    monkeypatch.setattr(worker, "_account_capture", account)
    with pytest.raises(ValueError, match="(budget|reserve)"):
        await worker.import_profiles({}, TASK)
    assert state.failed and not state.published
    assert not (state.directory / "nysed" / "111111").exists()


async def test_preclaim_failure_removes_only_own_staging_directory(managed_case, monkeypatch):
    state = managed_case(_cases())
    sibling = state.directory.parent / "existing"
    sibling.mkdir(parents=True)
    (sibling / "keep.txt").write_text("preserve")
    monkeypatch.setattr(
        worker.registry, "capture_registry_snapshot", AsyncMock(side_effect=OSError("Synthetic capture failure"))
    )
    with pytest.raises(OSError):
        await worker.import_profiles({}, TASK)
    assert not state.directory.exists() and (sibling / "keep.txt").read_text() == "preserve"
    assert not state.claimed and not state.failed


def _budget(directory):
    return {
        "path": directory,
        "deadline": worker.time.monotonic() + 120,
        "retained_bytes": 0,
        "max_bytes": 16 * 1024 * 1024,
    }


def test_streamed_json_matches_canonical_bytes_and_digest(tmp_path):
    directory = tmp_path.resolve()
    content_by_field = {"z": [{"school": "École Example", "year": 2001}] * 30000, "a": [None, False, True]}
    expected = worker.encoded_json(content_by_field)
    budget = _budget(directory)
    path = directory / "bundle.json"
    digest, size = worker._write_json(path, content_by_field, budget, max_bytes=len(expected))
    assert path.read_bytes() == expected and size == len(expected) == budget["retained_bytes"]
    assert digest == hashlib.sha256(expected).hexdigest()
    assert list(directory.iterdir()) == [path]


@pytest.mark.parametrize("failure", ["size", "encoding", "link"])
def test_streamed_json_failure_removes_temporary_and_preserves_existing(tmp_path, monkeypatch, failure):
    directory = tmp_path.resolve()
    budget = _budget(directory)
    existing = directory / "existing.json"
    existing.write_text("preserve")
    content_by_field = {"rows": [{"name": "Example"}] * 100000}
    max_bytes = None
    if failure == "size":
        max_bytes = 1100000
        monkeypatch.setattr(
            worker, "encoded_json", lambda content_by_field: pytest.fail("Full serialization is forbidden")
        )
    elif failure == "encoding":
        content_by_field["z"] = float("nan")
    else:
        monkeypatch.setattr(worker.os, "link", lambda *args: (_ for _ in ()).throw(OSError("Synthetic link failure")))
    with pytest.raises((OSError, ValueError)):
        worker._write_json(directory / "bundle.json", content_by_field, budget, max_bytes=max_bytes)
    assert list(directory.iterdir()) == [existing] and existing.read_text() == "preserve"
    assert budget["retained_bytes"] == 0


async def test_bundle_budget_holds_before_adding_metadata_or_publishing(managed_case, monkeypatch):
    state = managed_case(_cases())
    original = worker._append_profile

    def append(profiles, metrics, license_number, descriptor, support, budget):
        budget["max_bundle_bytes"] = budget["bundle_bytes"] + 5
        try:
            return original(profiles, metrics, license_number, descriptor, support, budget)
        finally:
            assert profiles == {} and metrics["nysed_support"] == {} and metrics["responses"] == 0

    monkeypatch.setattr(worker, "_append_profile", append)
    with pytest.raises(ValueError, match="bundle_byte_budget_exceeded"):
        await worker.import_profiles({}, TASK)
    assert state.failed and not state.published and not state.writes
    assert not (state.directory / "manifest.json").exists()
