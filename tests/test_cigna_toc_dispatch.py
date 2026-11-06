# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed proof transport, recovery and complete targeted collection parsing."""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports
from process import ptg_control
from process.ptg_parts import source_download
from process.ptg_parts.cigna_toc_collection import COLLECTION_PROOF_PARAM
from process.ptg_parts.cigna_toc_dispatch import restore_collection_retry_proof, validated_collection_dispatch
from tests.ptg_v4_import_orchestration_support import install_import_boundary
from tests.test_cigna_toc_collection import _SIGNED_URL, _documents, _subject

ptg = importlib.import_module("process.ptg")


def _request(proof):
    return {
        COLLECTION_PROOF_PARAM: copy.deepcopy(proof),
        "toc_urls": [_SIGNED_URL],
        "import_id": "source-attempt",
        "source_file_import_id": "source-attempt",
        "source_key": "synthetic-source",
        "import_month": "2026-11",
        "plan_ids": ["123456789"],
        "file_url_contains": ["core-0.json.gz"],
        "max_files": 1,
    }


def test_api_retains_private_proof_for_execution_and_hides_it_from_response(tmp_path):
    params = _request(_subject(tmp_path).proof)
    views = control_imports._import_param_views("ptg", params, run_id="run-parent")
    assert views.persisted_by_name[COLLECTION_PROOF_PARAM] == params[COLLECTION_PROOF_PARAM]
    assert views.enqueue_by_name[COLLECTION_PROOF_PARAM] == params[COLLECTION_PROOF_PARAM]
    assert views.persisted_by_name[COLLECTION_PROOF_PARAM] is not params[COLLECTION_PROOF_PARAM]
    public = control_imports._params_for_import_run_response("ptg", views.persisted_by_name)
    assert COLLECTION_PROOF_PARAM not in public
    assert COLLECTION_PROOF_PARAM in views.persisted_by_name


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "wrong_month",
        "wrong_attempt",
        "wrong_url",
        "two_urls",
        "no_plans",
        "no_file",
        "two_files",
        "boolean_limit",
        "direct",
        "other_importer",
    ],
)
def test_admission_rejects_unbound_or_competing_collection_requests(tmp_path, mutation):
    params = _request(_subject(tmp_path).proof)
    updates_by_mutation = {
        "wrong_month": {"import_month": "2026-10"},
        "wrong_attempt": {"source_file_import_id": "other-attempt"},
        "wrong_url": {"toc_urls": [_SIGNED_URL.replace("2026-11", "2026-10")]},
        "two_urls": {"toc_urls": [_SIGNED_URL, _SIGNED_URL]},
        "no_plans": {"plan_ids": []},
        "no_file": {"file_url_contains": []},
        "two_files": {"file_url_contains": ["one", "two"]},
        "boolean_limit": {"max_files": True},
        "direct": {"in_network_url": "https://rates.example.test/one.json.gz"},
    }
    params.update(updates_by_mutation.get(mutation, {}))
    if mutation == "missing":
        params.pop(COLLECTION_PROOF_PARAM)
    importer = "nucc" if mutation == "other_importer" else "ptg"
    with pytest.raises(ValueError):
        control_imports._import_param_views(importer, params, run_id="run-parent")


@pytest.mark.asyncio
async def test_retry_restores_durable_proof_but_cannot_replace_it(tmp_path, monkeypatch):
    params = _request(_subject(tmp_path).proof)
    public_run_map = {"importer": "ptg", "params": control_imports._params_for_import_run_response("ptg", params)}
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=public_run_map))
    monkeypatch.setattr(
        control_imports.npi_execution, "read_control_run", AsyncMock(return_value=SimpleNamespace(params=params))
    )
    monkeypatch.setattr(control_imports, "require_not_wave_owned_run", AsyncMock())
    create = AsyncMock(return_value=({"status": "queued"}, True))
    monkeypatch.setattr(control_imports, "create_import_run", create)
    await control_imports.retry_import_run("run-parent", {})
    retried = create.await_args.args[0]["params"]
    assert retried[COLLECTION_PROOF_PARAM] == params[COLLECTION_PROOF_PARAM]
    assert COLLECTION_PROOF_PARAM not in public_run_map["params"]
    changed = copy.deepcopy(params[COLLECTION_PROOF_PARAM])
    changed["collection"]["archive_sha256"] = "a" * 64
    with pytest.raises(ValueError, match="cannot replace"):
        await control_imports.retry_import_run("run-parent", {"retry_params": {COLLECTION_PROOF_PARAM: changed}})


@pytest.mark.asyncio
async def test_ordinary_retry_does_not_read_private_collection_evidence():
    current_run_map = {"importer": "ptg", "params": {"toc_urls": ["https://catalog.example.test/one.json"]}}
    loader = AsyncMock()
    assert await restore_collection_retry_proof(current_run_map, loader) is current_run_map
    loader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("original", [None, SimpleNamespace(params=None), SimpleNamespace(params=[])])
async def test_collection_retry_requires_retained_private_params(tmp_path, original):
    params = _request(_subject(tmp_path).proof)
    public_params = control_imports._params_for_import_run_response("ptg", params)
    current_run_map = {"importer": "ptg", "params": public_params}
    before = copy.deepcopy(current_run_map)
    loader = AsyncMock(return_value=original)
    with pytest.raises(ValueError, match="retry lacks retained archive proof"):
        await restore_collection_retry_proof(current_run_map, loader)
    loader.assert_awaited_once()
    assert current_run_map == before and COLLECTION_PROOF_PARAM not in current_run_map["params"]


@pytest.mark.parametrize("source_key", [None, "", " ", 42])
def test_collection_dispatch_requires_an_exact_source_key(tmp_path, source_key):
    params = _request(_subject(tmp_path).proof)
    params["source_key"] = source_key
    with pytest.raises(ValueError, match="source key is incomplete"):
        validated_collection_dispatch("ptg", params)


@pytest.mark.asyncio
async def test_main_rejects_unproven_collection_before_creating_artifact_lease(monkeypatch):
    lease = Mock(side_effect=AssertionError("lease must not be created"))
    monkeypatch.setattr(ptg, "artifact_lease_context", lease)
    with pytest.raises(ValueError, match="requires expected archive proof"):
        await ptg.main(toc_urls=[_SIGNED_URL])
    lease.assert_not_called()


@pytest.mark.asyncio
async def test_ptg_command_forwards_the_closed_internal_proof_keyword(tmp_path, monkeypatch):
    params = _request(_subject(tmp_path).proof)
    params["cigna_toc_collection_proof"] = params.pop(COLLECTION_PROOF_PARAM)
    guard = AsyncMock(return_value={"status": "component"})
    monkeypatch.setattr(ptg, "_guard_ptg_main_artifact_lease", guard)
    assert await ptg.main(**params) == {"status": "component"}
    forwarded = guard.await_args.args[0]
    assert forwarded["cigna_toc_collection_proof"] == params["cigna_toc_collection_proof"]
    assert forwarded["source_file_import_id"] == params["source_file_import_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["published", "validated"])
@pytest.mark.parametrize("changed_field", ["archive_sha256", "member_sha256", "expected_size"])
async def test_terminal_snapshot_reuse_requires_the_same_complete_collection_proof(
    tmp_path, monkeypatch, status, changed_field
):
    params = _request(_subject(tmp_path).proof)
    proof = params.pop(COLLECTION_PROOF_PARAM)
    boundary = install_import_boundary(monkeypatch, ptg)
    monkeypatch.setattr(ptg, "insert_or_compare_frozen_binding_transaction", AsyncMock())
    stored_snapshot_ids = []
    stored_options = []

    async def claim(rows, _model, **options):
        snapshot_id = rows[0]["snapshot_id"]
        stored_options.append(options["initial_import_run_by_field"]["options"])
        if stored_snapshot_ids and snapshot_id != stored_snapshot_ids[0]:
            raise RuntimeError("fresh snapshot required")
        stored_snapshot_ids.append(snapshot_id)
        return {"snapshot_id": snapshot_id, "status": status}

    monkeypatch.setattr(ptg, "_push_ptg2_objects", AsyncMock(side_effect=claim))
    monkeypatch.setattr(ptg, "_should_auto_activate_ptg2_candidates", lambda: False)
    reconcile = AsyncMock(return_value={"status": "unchanged"})
    resume = AsyncMock(return_value={"status": "reused"})
    monkeypatch.setattr(ptg, "_reconcile_already_published_snapshot", reconcile)
    monkeypatch.setattr(
        ptg, "_already_published_result", lambda **_options: {"status": "reused", "message": "already published"}
    )
    monkeypatch.setattr(ptg, "_resume_validated_candidate", resume)
    monkeypatch.setattr(ptg, "_finalize_resumed_terminal_attempt", AsyncMock())
    assert (await ptg._main_with_artifact_lease(**params, cigna_toc_collection_proof=proof))["status"] == "reused"
    reordered_proof_by_field = dict(reversed(list(proof.items())))
    assert (await ptg._main_with_artifact_lease(**params, cigna_toc_collection_proof=reordered_proof_by_field))[
        "status"
    ] == "reused"
    assert len(set(stored_snapshot_ids)) == 1
    assert reconcile.await_count == (2 if status == "published" else 0)
    assert resume.await_count == (2 if status == "validated" else 0)
    reconcile.reset_mock()
    resume.reset_mock()
    changed = copy.deepcopy(proof)
    if changed_field == "archive_sha256":
        changed["collection"][changed_field] = "a" * 64
    elif changed_field == "member_sha256":
        changed["collection"]["members"][0]["sha256"] = "b" * 64
    else:
        changed[changed_field] += 1
    with pytest.raises(RuntimeError, match="fresh snapshot required"):
        await ptg._main_with_artifact_lease(**params, cigna_toc_collection_proof=changed)
    assert (
        stored_options[0]["cigna_toc_collection_proof_sha256"]
        != stored_options[-1]["cigna_toc_collection_proof_sha256"]
    )
    assert "cigna_toc_collection_proof" not in stored_options[0]
    reconcile.assert_not_awaited()
    resume.assert_not_awaited()
    assert boundary.lock.enter_count == boundary.lock.exit_count == 3


@pytest.mark.asyncio
async def test_malformed_collection_proof_is_rejected_before_snapshot_lookup(tmp_path, monkeypatch):
    params = _request(_subject(tmp_path).proof)
    proof = params.pop(COLLECTION_PROOF_PARAM)
    proof["collection"]["archive_sha256"] = "invalid"
    database = AsyncMock()
    monkeypatch.setattr(ptg, "ensure_database", database)
    with pytest.raises(ValueError, match="digest is missing"):
        await ptg._main_with_artifact_lease(**params, cigna_toc_collection_proof=proof)
    database.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_forwards_same_proof_and_source_attempt_before_ptg_io(tmp_path, monkeypatch):
    params = _request(_subject(tmp_path).proof)
    monkeypatch.setattr(ptg_control, "guard_ptg_worker_start", AsyncMock(return_value=None))
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", AsyncMock(return_value=None))
    monkeypatch.setattr(ptg_control, "mark_control_run", AsyncMock(return_value=True))
    binding_read = AsyncMock(return_value=None)
    monkeypatch.setattr("process.ptg_frozen_control.recheck_frozen_binding", binding_read)
    main = AsyncMock(return_value={})
    monkeypatch.setattr(ptg_control, "ptg_main", main)
    result = await ptg_control.ptg_control_start({}, {"params": params})
    assert result["status"] == "succeeded"
    binding_read.assert_awaited_once_with(params)
    assert main.await_args.kwargs["cigna_toc_collection_proof"] == params[COLLECTION_PROOF_PARAM]
    assert main.await_args.kwargs["source_file_import_id"] == params["source_file_import_id"]


def _rate_documents():
    documents = _documents()
    plans = [
        {
            "plan_name": f"Synthetic Scope {index}",
            "plan_id": "123456789",
            "plan_id_type": "ein",
            "plan_market_type": "group",
        }
        for index in range(3)
    ]
    for index, (name, content) in enumerate(documents.items()):
        document = json.loads(content)
        document["reporting_entity_type"] = "health insurance issuer"
        members = (
            [(f"core-{part}.json.gz", [plan]) for part, plan in enumerate(plans)]
            if index == 0
            else ([(f"shared-{index - 1}.json.gz", plans)] if index < 4 else [])
        )
        document["reporting_structure"] = [
            {"reporting_plans": scope, "in_network_files": [{"location": "https://rates.example.test/" + file_name}]}
            for file_name, scope in members
        ]
        documents[name] = json.dumps(document).encode()
    return documents


def _capture_persistence(monkeypatch, tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(ptg, "ptg2_temp_parent", lambda: str(scratch))
    monkeypatch.setattr(ptg, "push_objects", AsyncMock())
    monkeypatch.setattr(ptg, "flush_error_log", AsyncMock())
    captured_members = []

    async def record(**options):
        artifact = options["logical_artifact"]
        content = Path(artifact.logical_path).read_bytes()
        captured_members.append((artifact.member_name, artifact.logical_sha256, hashlib.sha256(content).hexdigest()))

    recorder = AsyncMock(side_effect=record)
    monkeypatch.setattr(ptg, "_record_source_version", recorder)
    return captured_members, recorder


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["core-0", "core-1", "core-2", "shared-0", "shared-1", "shared-2"])
async def test_complete_collection_selects_each_physical_file_and_every_matching_plan(tmp_path, monkeypatch, target):
    subject = _subject(tmp_path, _rate_documents())
    download = AsyncMock(return_value=subject.raw)
    monkeypatch.setattr(source_download, "download_raw_artifact", download)
    captured_members, recorder = _capture_persistence(monkeypatch, tmp_path)
    jobs = await ptg._process_table_of_contents(
        _SIGNED_URL,
        {"PTGFile": object, "ImportLog": object},
        False,
        plan_ids=["123456789"],
        file_url_contains=[target + ".json.gz"],
        max_files=1,
        import_run_id="source-attempt",
        raise_on_error=True,
        cigna_toc_collection_proof=subject.proof,
    )
    assert len(jobs) == 1
    assert jobs[0]["url"] == "https://rates.example.test/" + target + ".json.gz"
    expected_scopes = (
        {f"Synthetic Scope {target[-1]}"}
        if target.startswith("core")
        else {f"Synthetic Scope {index}" for index in range(3)}
    )
    assert {plan["plan_name"] for plan in jobs[0]["plan_info"]} == expected_scopes
    assert len(captured_members) == recorder.await_count == 5
    assert all(recorded == actual for _, recorded, actual in captured_members)
    download.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["last_reporter", "ambiguous"])
async def test_collection_failure_writes_no_versions_catalog_or_file_rows(tmp_path, monkeypatch, failure):
    documents = _rate_documents()
    name = sorted(documents)[-1] if failure == "last_reporter" else next(iter(documents))
    document = json.loads(documents[name])
    if failure == "last_reporter":
        document["reporting_entity_name"] = "Different Reporter"
    else:
        duplicate = copy.deepcopy(document["reporting_structure"][0])
        duplicate["in_network_files"][0]["location"] = "https://other.example.test/core-0.json.gz"
        document["reporting_structure"].append(duplicate)
    documents[name] = json.dumps(document).encode()
    subject = _subject(tmp_path, documents)
    monkeypatch.setattr(source_download, "download_raw_artifact", AsyncMock(return_value=subject.raw))
    _, recorder = _capture_persistence(monkeypatch, tmp_path)
    with pytest.raises((ValueError, RuntimeError)):
        await ptg._process_table_of_contents(
            _SIGNED_URL,
            {"PTGFile": object, "ImportLog": object},
            False,
            plan_ids=["123456789"],
            file_url_contains=["core-0.json.gz"],
            max_files=1,
            import_run_id="source-attempt",
            raise_on_error=True,
            cigna_toc_collection_proof=subject.proof,
        )
    recorder.assert_not_awaited()
    ptg.push_objects.assert_not_awaited()
