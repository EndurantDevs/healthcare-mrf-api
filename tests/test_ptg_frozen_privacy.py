# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public, log, and progress privacy boundaries for frozen multipart input."""

from __future__ import annotations

import importlib
import json
import types
from contextlib import asynccontextmanager

import pytest

from api import control
from api import control_imports
from api.control_imports import _retry_child_params, normalize_run
from process import import_status_events
from process.ptg_parts.domain import PTG2DownloadedJob
from process.ptg_parts import source_download
from process.ptg_parts.frozen_rate_files import (
    build_frozen_rate_jobs,
    normalize_frozen_rate_file_set,
)
from process.ptg_parts.live_progress import (
    reset_live_progress_context,
    set_live_progress_context,
)
from tests.ptg_frozen_test_support import (
    frozen_artifacts,
    frozen_rate_file_set,
    protected_control_payload,
)

process_ptg = importlib.import_module("process.ptg")


def _normalized_frozen_jobs():
    descriptors, set_digest = frozen_rate_file_set(2)
    normalized, _ = normalize_frozen_rate_file_set(
        descriptors,
        set_digest,
    )
    return build_frozen_rate_jobs(
        normalized,
        plan_info=[{"plan_id": "plan-a"}],
        source_network_names=["Network A"],
    )


def test_frozen_download_progress_uses_only_ordinal_opaque_label(
    monkeypatch,
):
    """Screen, logger, and live progress never render private coordinates."""

    frozen_job = _normalized_frozen_jobs()[0]
    descriptor = frozen_job["_frozen_rate_file"]
    screen_lines: list[str] = []
    log_lines: list[str] = []
    live_payloads: list[dict[str, object]] = []
    monkeypatch.setattr(
        source_download,
        "_emit_screen_line",
        lambda line, **_kwargs: screen_lines.append(line),
    )
    monkeypatch.setattr(
        source_download.logger,
        "info",
        lambda line, *_args: log_lines.append(line),
    )
    monkeypatch.setattr(
        source_download,
        "write_live_progress",
        lambda **payload: live_payloads.append(payload),
    )
    token = set_live_progress_context(
        private_source=True,
        file_name=frozen_job["_ptg_progress_label"],
        file_index=1,
        file_count=2,
    )
    try:
        source_download._emit_download_progress(
            url=descriptor["canonical_url"],
            bytes_read=50,
            total_bytes=descriptor["content_length"],
            started_at=0.0,
            done=False,
        )
    finally:
        reset_live_progress_context(token)

    rendered = json.dumps(
        [screen_lines, log_lines, live_payloads],
        sort_keys=True,
    )
    assert frozen_job["_ptg_progress_label"] in rendered
    assert "frozen-part-001-of-002-" in rendered
    assert str(descriptor["raw_sha256"])[:12] not in rendered
    for private_value in (
        descriptor["canonical_url"],
        descriptor["etag"],
        descriptor["raw_sha256"],
        descriptor["engine_source_file_version_id"],
    ):
            assert str(private_value) not in rendered


def test_frozen_duplicate_screen_line_omits_artifact_identities(tmp_path):
    """Scan-stage duplicate output keeps protected file evidence opaque."""

    frozen_job = _normalized_frozen_jobs()[0]
    descriptor = frozen_job["_frozen_rate_file"]
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor,
        tmp_path,
    )

    rendered = process_ptg._raw_job_dedupe_screen_line(
        frozen_job,
        PTG2DownloadedJob(
            frozen_job,
            raw_artifact,
            logical_artifact,
        ),
    )

    assert frozen_job["_ptg_progress_label"] in rendered
    for private_value in (
        frozen_job["url"],
        descriptor["raw_sha256"],
        descriptor["logical_sha256"],
        descriptor["engine_source_file_version_id"],
    ):
        assert str(private_value) not in rendered


@pytest.mark.asyncio
async def test_frozen_download_error_does_not_reflect_private_exception(
    monkeypatch,
):
    """An acquisition exception is classified without echoing its payload."""

    frozen_job = _normalized_frozen_jobs()[0]
    descriptor = frozen_job["_frozen_rate_file"]
    private_error = (
        f"{descriptor['canonical_url']} {descriptor['etag']} "
        f"{descriptor['raw_sha256']}"
    )

    async def fail_download(*_args, **_kwargs):
        raise RuntimeError(private_error)

    monkeypatch.setattr(
        source_download,
        "download_raw_artifact",
        fail_download,
    )

    downloaded = await source_download._download_ptg_job_artifact(
        frozen_job,
        reuse_raw_artifacts=False,
        max_bytes=None,
        keep_partial_artifacts=False,
    )

    assert downloaded.error is not None
    assert frozen_job["_ptg_progress_label"] in downloaded.error
    assert "RuntimeError" in downloaded.error
    assert descriptor["canonical_url"] not in downloaded.error
    assert descriptor["etag"] not in downloaded.error
    assert descriptor["raw_sha256"] not in downloaded.error


def test_public_import_run_recursively_redacts_frozen_descriptor_evidence():
    """List/detail responses expose counts, not private acquisition evidence."""

    control_payload = protected_control_payload()
    descriptors = control_payload["params"]["frozen_rate_files"]
    first_descriptor = descriptors[0]
    normalized = normalize_run(
        {
            "run_id": "run-protected",
            "importer": "ptg",
            "status": "failed",
            "params": control_payload["params"],
            "progress": {
                "message": (
                    f"failed {first_descriptor['canonical_url']} "
                    f"{first_descriptor['etag']}"
                )
            },
            "metrics": {
                "source_file_versions": descriptors,
                "successful_files": [
                    {
                        "url": first_descriptor["canonical_url"],
                        "raw_sha256": first_descriptor["raw_sha256"],
                    }
                ],
                "frozen_rate_file_count": 2,
            },
            "error": {
                "message": (
                    f"failed {first_descriptor['canonical_url']} "
                    f"{first_descriptor['engine_source_file_version_id']}"
                )
            },
        }
    )

    assert normalized["params"]["frozen_rate_file_set_protected"] is True
    assert normalized["params"]["frozen_rate_file_count"] == 2
    assert "frozen_rate_files" not in normalized["params"]
    assert "source_file_versions" not in normalized["metrics"]
    assert "successful_files" not in normalized["metrics"]
    rendered = json.dumps(normalized, sort_keys=True)
    for descriptor in descriptors:
        for field_name in (
            "canonical_url",
            "etag",
            "last_modified",
            "raw_sha256",
            "logical_sha256",
            "engine_source_identity_hash",
            "engine_source_file_version_id",
        ):
            private_value = descriptor.get(field_name)
            if private_value:
                assert str(private_value) not in rendered


def test_public_retry_cannot_reconstruct_a_protected_frozen_run():
    """The redacted response cannot become a lossy multipart retry request."""

    with pytest.raises(
        ValueError,
        match="cannot be retried through the public API",
    ):
        _retry_child_params(
            {
                "importer": "ptg",
                "params": {
                    "frozen_rate_file_set_protected": True,
                    "frozen_rate_file_count": 2,
                },
            },
            "run-protected",
            {},
        )


class _CreateConnection:
    async def scalar(self, *_args, **_kwargs):
        return 1

    async def all(self, *_args, **_kwargs):
        return []

    async def status(self, *_args, **_kwargs):
        return "INSERT 0 1"


class _CreateDatabase:
    @asynccontextmanager
    async def acquire(self):
        yield _CreateConnection()

    async def execute(self, _statement):
        return None


def _install_create_route_harness(
    monkeypatch,
    descriptors,
    status_events,
    live_updates,
) -> None:
    """Install a create-route harness that preserves private worker params."""

    async def compare_binding(_connection, params):
        assert params["frozen_rate_files"] == descriptors

    async def enqueue(source_row):
        assert source_row["params"]["frozen_rate_files"] == descriptors
        return {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": source_row["heartbeat_at"],
            "progress": {"message": "queued"},
            "metrics": {
                "frozen_rate_files": descriptors,
                "source_file_versions": descriptors,
                "frozen_rate_file_count": len(descriptors),
            },
            "error": None,
        }

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(control_imports, "db", _CreateDatabase())
    monkeypatch.setattr(
        control_imports,
        "insert_or_compare_frozen_binding",
        compare_binding,
    )
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    monkeypatch.setattr(
        control_imports,
        "enqueue_status_event",
        status_events.append,
    )
    monkeypatch.setattr(
        control_imports,
        "_write_run_live_progress",
        lambda payload, **_kwargs: live_updates.append(payload),
    )


@pytest.mark.asyncio
async def test_create_route_and_first_event_return_only_opaque_marker(
    monkeypatch,
):
    """The actual create path keeps worker params private at both boundaries."""

    control_payload = protected_control_payload()
    control_payload["run_id"] = "run-private-create"
    descriptors = control_payload["params"]["frozen_rate_files"]
    status_events = []
    live_updates = []

    _install_create_route_harness(
        monkeypatch,
        descriptors,
        status_events,
        live_updates,
    )

    response = await control.control_create_import(
        types.SimpleNamespace(
            headers={"Authorization": "Bearer secret"},
            json=control_payload,
        )
    )
    response_payload = json.loads(response.body)

    assert response.status == 201
    assert response_payload == status_events[0]
    assert response_payload == live_updates[0]
    assert response_payload["params"]["frozen_rate_file_count"] == 2
    assert (
        response_payload["params"]["frozen_rate_file_set_protected"]
        is True
    )
    rendered = json.dumps(response_payload, sort_keys=True)
    for descriptor in descriptors:
        assert descriptor["canonical_url"] not in rendered
        assert descriptor["raw_sha256"] not in rendered
        assert descriptor["engine_source_file_version_id"] not in rendered


class _DuplicateCreateConnection(_CreateConnection):
    def __init__(self, existing_run):
        self.existing_run = existing_run

    async def all(self, *_args, **_kwargs):
        return [self.existing_run]


class _DuplicateCreateDatabase(_CreateDatabase):
    def __init__(self, existing_run):
        self.existing_run = existing_run

    @asynccontextmanager
    async def acquire(self):
        yield _DuplicateCreateConnection(self.existing_run)


@pytest.mark.asyncio
async def test_duplicate_create_route_projects_existing_frozen_run(
    monkeypatch,
):
    """A 409 replay cannot expose the existing run's protected evidence."""

    control_payload = protected_control_payload()
    control_payload["run_id"] = "run-private-replay"
    descriptors = control_payload["params"]["frozen_rate_files"]
    existing_run_by_field = {
        "run_id": "run-existing-private",
        "importer": "ptg",
        "status": "running",
        "params": control_payload["params"],
        "metrics": {
            "source_file_versions": descriptors,
            "frozen_rate_file_count": len(descriptors),
        },
        "progress": {
            "message": f"reading {descriptors[0]['canonical_url']}",
        },
    }

    async def compare_binding(_connection, params):
        assert params["frozen_rate_files"] == descriptors

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(
        control_imports,
        "db",
        _DuplicateCreateDatabase(existing_run_by_field),
    )
    monkeypatch.setattr(
        control_imports,
        "insert_or_compare_frozen_binding",
        compare_binding,
    )

    response = await control.control_create_import(
        types.SimpleNamespace(
            headers={"Authorization": "Bearer secret"},
            json=control_payload,
        )
    )
    response_payload = json.loads(response.body)

    assert response.status == 409
    assert response_payload["run_id"] == "run-existing-private"
    assert response_payload["params"]["frozen_rate_file_count"] == 2
    assert (
        response_payload["params"]["frozen_rate_file_set_protected"]
        is True
    )
    assert "frozen_rate_files" not in response_payload["params"]
    rendered = json.dumps(response_payload, sort_keys=True)
    for descriptor in descriptors:
        assert descriptor["canonical_url"] not in rendered
        assert descriptor["raw_sha256"] not in rendered


def test_terminal_status_event_sanitizes_manifest_evidence(monkeypatch):
    """The publisher itself strips terminal manifest-derived coordinates."""

    control_payload = protected_control_payload()
    descriptors = control_payload["params"]["frozen_rate_files"]
    monkeypatch.setenv(
        "HLTHPRT_IMPORT_STATUS_EVENT_URL",
        "https://events.example.test/imports",
    )
    with import_status_events._publisher_state.lock:
        import_status_events._publisher_state.loop = None
        import_status_events._publisher_state.pending.clear()

    import_status_events.enqueue_status_event(
        {
            "run_id": "run-private-terminal",
            "status": "succeeded",
            "metrics": {
                "frozen_rate_files": descriptors,
                "frozen_rate_file_proof": descriptors,
                "source_file_versions": descriptors,
                "frozen_rate_file_count": 2,
            },
        }
    )

    with import_status_events._publisher_state.lock:
        event_by_field = dict(
            import_status_events._publisher_state.pending[-1]
        )
        import_status_events._publisher_state.pending.clear()
    assert event_by_field["metrics"] == {
        "frozen_rate_file_count": 2,
        "frozen_rate_file_set_protected": True,
    }
    rendered = json.dumps(event_by_field, sort_keys=True)
    for descriptor in descriptors:
        assert descriptor["canonical_url"] not in rendered
        assert descriptor["raw_sha256"] not in rendered
