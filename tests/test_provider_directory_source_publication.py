# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A completed native acquisition can publish without replaying its queue job."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from api import control_workers as workers
from api import provider_directory_source_publication as publication
from api import provider_directory_source_worker as source
from process import provider_directory_generic_source_storage as storage
from process.control_lifecycle import _terminal_metrics_from_result
from tests.test_provider_directory_source_worker_launch import _change_job_bindings

RUN_ID = "run_" + "a" * 32
JOB_ID = "provider_directory_start_" + RUN_ID
SPEC = workers._BY_WORKER_CLASS[source._WORKER_CLASS]
OLD_IMAGE = "example/source-worker@sha256:" + "a" * 64
IMAGE = "example/source-worker@sha256:" + "b" * 64


def _handoff():
    return {
        "contract": storage.ISOLATED_CONTRACT,
        "identity": {
            "dataset_id": "dataset",
            "endpoint_id": "endpoint",
            "acquisition_root_run_id": RUN_ID,
            "metadata": {"source_ids": ["example-source"]},
        },
        "phase": "ready_for_freeze",
        "ordinal": 1,
        "resets": {},
        "tables": {
            root: {
                "name": "pdgs_" + "c" * 32 + "_" + storage._SUFFIXES[root],
                "oid": index + 100,
                "owner": 10,
                "layout_sha256": "d" * 64,
            }
            for index, root in enumerate(sorted(set(storage.RELATIONS) | set(storage.HANDOFF_CONTROL_ROOTS)))
        },
        "auxiliary": None,
        "relationship_sha256": None,
        "storage": {
            "catalog_schema": "mrf",
            "storage_schema": "synthetic_source",
            "database_oid": 1,
            "catalog_schema_oid": 2,
            "storage_schema_oid": 3,
            "owner_oid": 10,
            "namespace_owner_oid": 11,
            "enrollment_sha256": "e" * 64,
            "producer": ["a" * 40, "sha256:" + "a" * 64],
        },
        "admission": {
            "dataset_hash": "f" * 64,
            "resource_count": 1,
            "previous_dataset_id": None,
            "completion_proof_required_version": None,
            "completion_proof_sha256": None,
            "content_proof_admission_sha256": "c" * 64,
        },
    }


def _receipt(document):
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    return {"document": document, "sha256": hashlib.sha256(encoded).hexdigest()}


def _recorded():
    return {
        "run_id": RUN_ID,
        "importer": source._IMPORTER,
        "status": "succeeded",
        "no_error": True,
        "finished_at": datetime(2026, 1, 1, 1, tzinfo=UTC),
        "retry_of_run_id": None,
        "params": {
            "source_ids": ["example-source"],
            "provider_directory_endpoint_scope": "https://example.test/fhir",
            "import_resources": True,
            "defer_typed_materialization": True,
            "publish_artifacts": False,
            "publish_after_acquisition": False,
        },
        "progress": {"attempt_id": RUN_ID + ":" + "b" * 32, "attempt_started_at": "2026-01-01T00:00:00Z"},
        "metrics": _terminal_metrics_from_result(
            {
                "source_publication": {"state": "incomplete"},
                "source_candidate_handoffs": {"dataset": _receipt(_handoff())},
            }
        ),
    }


def _request(**overrides):
    return {
        "importer": source._IMPORTER,
        "run_id": RUN_ID,
        "role": publication.ROLE,
        "worker_class": SPEC.worker_class,
        "queue": SPEC.queue,
        "job_id": JOB_ID,
        **overrides,
    }


@pytest.fixture
def configured_publication(monkeypatch):
    """Keep old and publication-only images and credentials in separate routes."""
    recorded = _recorded()
    route_by_field = {
        "root_run_id": RUN_ID,
        "source_id": "example-source",
        "endpoint_scope": "https://example.test/fhir",
        "image": OLD_IMAGE,
        "secret_name": "synthetic-original",
    }
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([route_by_field]))
    monkeypatch.setenv(
        publication.ROUTES_ENV, json.dumps([{**route_by_field, "image": IMAGE, "secret_name": "synthetic-publisher"}])
    )
    monkeypatch.setenv(source.IMAGE_ENV, OLD_IMAGE)
    monkeypatch.delenv(source.PROXY_POLICY_ENV, raising=False)
    selection_by_field = {
        "workerClasses": [SPEC.worker_class],
        "purposes": [source.PURPOSE],
        "importers": [source._IMPORTER],
    }
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON",
        json.dumps(
            [
                {**selection_by_field, "name": "PUBLISHER_DSN", "secretName": secret, "key": "dsn"}
                for secret in ("synthetic-original", "synthetic-publisher")
            ]
        ),
    )
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    **selection_by_field,
                    "name": secret,
                    "secretName": secret,
                    "mountPath": "/etc/source-policy",
                    "readOnly": True,
                }
                for secret in ("synthetic-original", "synthetic-publisher")
            ]
        ),
    )
    original_request = workers._AdmittedWorkerRequest(_request(role="start"))
    queued_by_field = {**recorded, "status": "queued", "metrics": publication.original_job_identity(recorded)}
    workers._bind_admitted_job(original_request, queued_by_field)
    original_request.fhir_source_launch = source.admit_fhir_source_worker_launch(queued_by_field)
    original = workers._worker_job_manifest(SPEC, original_request, OLD_IMAGE)
    original["status"] = {"succeeded": 1, "active": 0, "conditions": [{"type": "Complete", "status": "True"}]}
    return recorded, original


@pytest.fixture
def runtime(monkeypatch, configured_publication):
    """Observe the native run and real launcher calls while holding one run fence."""
    recorded, original = configured_publication
    native = SimpleNamespace(all=AsyncMock(side_effect=lambda *args, **kwargs: [deepcopy(recorded)]))
    fence_state_by_name = {"value": False}
    lock = asyncio.Lock()

    @asynccontextmanager
    async def connection():
        async with lock:
            fence_state_by_name["value"] = True
            try:
                yield native
            finally:
                fence_state_by_name["value"] = False

    jobs, calls = [], []

    def kubernetes(method, path, body=None):
        assert fence_state_by_name["value"], "the exact run action fence must survive threaded launch"
        calls.append((method, path, deepcopy(body)))
        if method == "GET":
            selector = parse_qs(urlsplit(path).query)["labelSelector"][0]
            if "healthporta.com/role=start" in selector:
                return {"items": [{"status": {"phase": "Succeeded"}}] if "/pods?" in path else [original]}
            return {"items": jobs}
        if method == "POST":
            jobs.append({**deepcopy(body), "status": {"active": 1}})
            jobs[-1]["metadata"]["uid"] = f"publication-job-{len(calls)}"
        elif method == "DELETE":
            assert path.rsplit("/", 1)[-1] != original["metadata"]["name"]
            job = next(job for job in jobs if path.endswith("/" + job["metadata"]["name"]))
            assert body["preconditions"] == {"uid": job["metadata"]["uid"]}
            jobs.remove(job)
        return {}

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", AsyncMock(return_value=recorded))
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_request", kubernetes)
    return SimpleNamespace(recorded=recorded, native=native, original=original, jobs=jobs, calls=calls)


@pytest.mark.parametrize("proxy_enabled", [False, True])
async def test_publication_creates_one_distinct_job_and_preserves_original(runtime, monkeypatch, proxy_enabled):
    if proxy_enabled:
        monkeypatch.setenv(
            source.PROXY_POLICY_ENV,
            json.dumps(
                {
                    "version": 1,
                    "revision": "synthetic-policy",
                    "sources": [
                        {
                            "source_id": "example-source",
                            "endpoint_scope": "https://example.test/fhir",
                            "use_proxy": True,
                        }
                    ],
                }
            ),
        )
    original = deepcopy(runtime.original)
    acquisition = deepcopy(runtime.recorded)
    assert not {"queue", "function", "job_id"}.intersection(runtime.recorded["metrics"])
    launch_results = await asyncio.gather(*(workers.guarded_ensure_worker(_request()) for _ in range(3)))
    assert [launch_result["status"] for launch_result in launch_results] == [
        "started",
        "already_running",
        "already_running",
    ]
    assert runtime.original == original
    assert runtime.recorded == acquisition
    assert len(runtime.jobs) == 1
    assert not any(method == "DELETE" for method, *_ in runtime.calls)
    job = runtime.jobs[0]
    assert job["metadata"]["name"] != original["metadata"]["name"]
    assert job["metadata"]["labels"]["healthporta.com/role"] == publication.ROLE
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["command"] == source.COMMAND and container["args"] == ["publish"]
    assert container["image"] == IMAGE
    environment_by_name = {entry["name"]: entry for entry in container["env"]}
    assert environment_by_name["HLTHPRT_CONTROL_RUN_ID"]["value"] == RUN_ID
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == JOB_ID
    assert environment_by_name[publication.ATTEMPT_ENV]["value"] == runtime.recorded["progress"]["attempt_id"]
    assert (
        environment_by_name[publication.HANDOFF_ENV]["value"]
        == runtime.recorded["metrics"]["source_candidate_handoffs"]["dataset"]["sha256"]
    )
    assert environment_by_name["PUBLISHER_DSN"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-publisher"
    assert workers._BY_WORKER_CLASS[source._WORKER_CLASS] is SPEC and SPEC.role == "start"
    assert not {
        source.PROXY_POOL_ENV,
        source.PROXY_POLICY_ENV,
        source.PROXY_BINDING_ENV,
        source.PROXY_COOLDOWNS_ENV,
    }.intersection(environment_by_name)


async def test_complete_exact_native_job_metadata_remains_supported(runtime):
    original = publication.original_job_identity(runtime.recorded)
    runtime.recorded["metrics"].update(original)
    before = deepcopy(runtime.recorded)
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "started"
    assert runtime.recorded == before


@pytest.mark.parametrize(
    "present",
    [("queue",), ("function",), ("job_id",), ("queue", "function"), ("queue", "job_id"), ("function", "job_id")],
)
async def test_partial_native_job_coordinates_are_refused(runtime, present):
    canonical = publication.original_job_identity(runtime.recorded)
    runtime.recorded["metrics"].update({name: canonical[name] for name in present})
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "failed"
    assert not runtime.calls


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("queue", "arq:Other"),
        ("function", "other"),
        ("job_id", "another_job"),
        ("queue", None),
        ("function", None),
        ("job_id", None),
    ],
)
async def test_conflicting_or_null_native_job_coordinates_are_refused(runtime, field, replacement):
    canonical = publication.original_job_identity(runtime.recorded)
    runtime.recorded["metrics"].update({**canonical, field: replacement})
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "failed"
    assert not runtime.calls


async def test_publication_follows_the_exact_retained_retry_root(runtime, monkeypatch):
    root_id = "run_" + "d" * 32
    run = runtime.recorded
    run["retry_of_run_id"] = root_id
    run["params"].update(retry_of_run_id=root_id, provider_directory_pagination_root_run_id=root_id)
    document = run["metrics"]["source_candidate_handoffs"]["dataset"]["document"]
    document["identity"]["acquisition_root_run_id"] = root_id
    run["metrics"]["source_candidate_handoffs"]["dataset"] = _receipt(document)
    root_by_field = {**deepcopy(run), "run_id": root_id, "status": "failed", "retry_of_run_id": None}
    for name in ("retry_of_run_id", "provider_directory_pagination_root_run_id"):
        root_by_field["params"].pop(name)
    runtime.native.all.side_effect = lambda query, **kwargs: (
        [deepcopy(root_by_field), deepcopy(run)] if "source_http_proxy_binding" in str(query) else [deepcopy(run)]
    )
    routes = json.loads(workers.os.environ[publication.ROUTES_ENV])
    routes[0]["root_run_id"] = root_id
    monkeypatch.setenv(publication.ROUTES_ENV, json.dumps(routes))
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "started"
    assert runtime.jobs[0]["metadata"]["annotations"]["healthporta.com/source-root-run-id"] == root_id
    assert not any(method == "DELETE" for method, *_ in runtime.calls)


async def test_publication_replaces_only_its_own_terminal_job(runtime):
    original = deepcopy(runtime.original)
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "started"
    name = runtime.jobs[0]["metadata"]["name"]
    uid = runtime.jobs[0]["metadata"]["uid"]
    runtime.jobs[0]["status"] = {"failed": 1, "conditions": [{"type": "Failed", "status": "True"}]}
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "started"
    deleted_names = [path.rsplit("/", 1)[-1] for method, path, _ in runtime.calls if method == "DELETE"]
    assert deleted_names == [name] and runtime.original == original
    assert len(runtime.jobs) == 1 and runtime.jobs[0]["metadata"]["uid"] != uid


def test_publication_requires_an_in_process_native_capability(runtime):
    result = workers.ensure_worker(_request())
    assert result["status"] == "failed" and not runtime.calls


async def test_normal_terminal_ensure_remains_refused(runtime):
    result = await workers.guarded_ensure_worker(_request(role="start"))
    assert result["status"] == "failed" and not runtime.calls


@pytest.mark.parametrize("state", ["running", "pod-running", "unavailable"])
async def test_publication_requires_original_to_be_stopped(runtime, monkeypatch, state):
    if state == "running":
        runtime.original["status"] = {"active": 1}
    elif state == "pod-running":
        monkeypatch.setattr(workers, "exact_worker_presence", lambda _: {"enabled": True, "stopped": False})
    else:
        monkeypatch.setattr(
            workers, "exact_worker_presence", lambda _: (_ for _ in ()).throw(RuntimeError("unavailable"))
        )
    result = await workers.guarded_ensure_worker(_request())
    assert result["status"] == "failed" and not runtime.jobs
    assert all(method == "GET" for method, *_ in runtime.calls)


@pytest.mark.parametrize(
    "field_path,replacement",
    [
        (("status",), "running"),
        (("no_error",), False),
        (("finished_at",), None),
        (("metrics", "source_publication", "state"), "published"),
        (("progress", "attempt_id"), "run_" + "d" * 32 + ":" + "b" * 32),
        (("metrics", "queue"), "arq:Other"),
    ],
)
async def test_publication_rejects_changed_native_proof(runtime, field_path, replacement):
    current = runtime.recorded
    for name in field_path[:-1]:
        current = current[name]
    current[field_path[-1]] = replacement
    result = await workers.guarded_ensure_worker(_request())
    assert result["status"] == "failed" and not runtime.calls


@pytest.mark.parametrize("fault", ["hash", "root", "source", "tables", "producer"])
async def test_publication_rejects_changed_handoff(runtime, fault):
    receipt = runtime.recorded["metrics"]["source_candidate_handoffs"]["dataset"]
    document = receipt["document"]
    if fault == "hash":
        receipt["sha256"] = "0" * 64
    if fault == "root":
        document["identity"]["acquisition_root_run_id"] = "run_" + "d" * 32
    if fault == "source":
        document["identity"]["metadata"]["source_ids"] = ["other-source"]
    if fault == "tables":
        document["tables"] = {}
    if fault == "producer":
        document["storage"]["producer"] = ["caller", "tag"]
    if fault != "hash":
        runtime.recorded["metrics"]["source_candidate_handoffs"]["dataset"] = _receipt(document)
    result = await workers.guarded_ensure_worker(_request())
    assert result["status"] == "failed" and not runtime.calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("image", OLD_IMAGE),
        ("secret_name", "synthetic-original"),
        ("worker_class", "process.Geo"),
        ("queue", "arq:Other"),
    ],
)
async def test_publication_cannot_accept_caller_runtime_overrides(runtime, field, value):
    result = await workers.guarded_ensure_worker(_request(**{field: value}))
    assert result["status"] in {"failed", "unsupported"} and not runtime.calls


@pytest.mark.parametrize(
    "drift", ["command", "args", "handoff", "target", "mount-path", "secret-source", "volume-device"]
)
async def test_publication_reuse_refuses_bootstrap_or_mount_drift(runtime, drift):
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "started"
    job = runtime.jobs[0]
    container = job["spec"]["template"]["spec"]["containers"][0]
    if drift == "command":
        container["command"] = ["python", "-c", "pass"]
    elif drift == "args":
        container["args"] = []
    elif drift in {"handoff", "target"}:
        name = publication.HANDOFF_ENV if drift == "handoff" else "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"
        next(entry for entry in container["env"] if entry["name"] == name)["value"] = "changed"
    elif drift == "secret-source":
        job["spec"]["template"]["spec"]["volumes"][0]["secret"]["secretName"] = "foreign-policy"
    else:
        _change_job_bindings(job, drift)
    before = len(runtime.calls)
    result = await workers.guarded_ensure_worker(_request())
    assert result["status"] == "failed" and result["items"][0]["job_status"] == "purpose_mismatch"
    assert all(method == "GET" for method, *_ in runtime.calls[before:])


async def test_publication_is_disabled_without_an_exact_server_route(runtime, monkeypatch):
    monkeypatch.delenv(publication.ROUTES_ENV)
    result = await workers.guarded_ensure_worker(_request())
    assert result["status"] == "failed" and not runtime.calls


async def test_publication_reads_native_json_columns_without_changing_original(runtime):
    expected = deepcopy(runtime.recorded)
    for name in ("params", "progress", "metrics"):
        runtime.recorded[name] = json.dumps(runtime.recorded[name])
    readback = await publication.read_completed_run(runtime.native, RUN_ID)
    assert readback == expected
    query = str(runtime.native.all.await_args.args[0])
    assert "error::jsonb" in query and "octet_length(metrics::text)<=262144" in query


@pytest.mark.parametrize("run_id", [None, "run_invalid"])
async def test_publication_refuses_invalid_run_identity_before_native_read(runtime, run_id):
    with pytest.raises(ValueError, match="^fhir_source_publication_run_invalid$"):
        await publication.read_completed_run(runtime.native, run_id)
    runtime.native.all.assert_not_awaited()
    assert not runtime.calls


@pytest.mark.parametrize("row_count", [0, 2])
async def test_publication_requires_one_unambiguous_native_run(runtime, row_count):
    runtime.native.all.side_effect = lambda *_args, **_kwargs: [deepcopy(runtime.recorded) for _ in range(row_count)]
    with pytest.raises(ValueError, match="^fhir_source_publication_run_unavailable$"):
        await publication.read_completed_run(runtime.native, RUN_ID)
    assert runtime.native.all.await_count == 1 and runtime.native.all.await_args.kwargs == {"run_id": RUN_ID}
    assert not runtime.calls


@pytest.mark.parametrize(
    "field_path,replacement,reason",
    [
        (("progress", "attempt_started_at"), "2026-01-01T02:00:00Z", "attempt_invalid"),
        (("metrics", "source_candidate_handoffs"), None, "handoff_required"),
        (("metrics", "source_candidate_handoffs"), {"dataset": {}, "other-dataset": {}}, "handoff_required"),
        (("metrics", "source_candidate_handoffs", "dataset"), None, "handoff_invalid"),
        (("metrics", "source_candidate_handoffs", "dataset"), {"document": {}}, "handoff_invalid"),
    ],
)
async def test_publication_refuses_unbound_completed_attempt_or_handoff(runtime, field_path, replacement, reason):
    """An impossible attempt or ambiguous native handoff cannot authorize a Job."""
    current = runtime.recorded
    for name in field_path[:-1]:
        current = current[name]
    current[field_path[-1]] = deepcopy(replacement)
    before = deepcopy(runtime.recorded)
    with pytest.raises(ValueError, match=f"^fhir_source_publication_{reason}$"):
        publication._completed_proof(runtime.recorded)
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "failed"
    assert runtime.recorded == before and not runtime.calls


async def test_publication_cannot_borrow_a_wildcard_acquisition_route(runtime, monkeypatch):
    routes = json.loads(workers.os.environ[publication.ROUTES_ENV])
    routes[0]["root_run_id"] = None
    monkeypatch.setenv(publication.ROUTES_ENV, json.dumps(routes))
    request = workers._AdmittedWorkerRequest(_request())
    before = deepcopy(runtime.recorded)
    with pytest.raises(ValueError, match="^fhir_source_publication_route_invalid$"):
        await publication.admit_launch(runtime.native, request)
    assert (await workers.guarded_ensure_worker(_request()))["status"] == "failed"
    assert runtime.recorded == before and not runtime.calls
