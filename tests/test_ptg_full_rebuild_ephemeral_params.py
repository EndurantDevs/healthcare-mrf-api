# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
from types import SimpleNamespace
import uuid

import pytest
from sanic.exceptions import BadRequest

from api import control, control_imports


_REBUILD_TOKEN = "123e4567-e89b-42d3-a456-426614174000"
_SECOND_REBUILD_TOKEN = "223e4567-e89b-42d3-a456-426614174000"


class _RecordingDatabase:
    def __init__(self):
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)


class _RecordingRedis:
    def __init__(self):
        self.enqueue_calls = []

    async def enqueue_job(self, *args, **kwargs):
        self.enqueue_calls.append((args, kwargs))
        return SimpleNamespace(job_id="job_ptg_ephemeral")


@pytest.fixture
def control_observations(monkeypatch):
    database = _RecordingDatabase()
    redis = _RecordingRedis()
    status_events = []
    live_runs = []

    async def create_recording_pool(*_args, **_kwargs):
        return redis

    async def no_active_run(*_args, **_kwargs):
        return None

    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "create_pool", create_recording_pool)
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        no_active_run,
    )
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        no_active_run,
    )
    monkeypatch.setattr(
        control_imports,
        "enqueue_status_event",
        lambda run_by_name: status_events.append(dict(run_by_name)),
    )
    monkeypatch.setattr(
        control_imports,
        "_write_run_live_progress",
        lambda run_by_name, **_kwargs: live_runs.append(dict(run_by_name)),
    )
    return SimpleNamespace(
        database=database,
        redis=redis,
        status_events=status_events,
        live_runs=live_runs,
    )


def _full_rebuild_create_request():
    return {
        "run_id": "run_ephemeral",
        "importer": "ptg",
        "params": {
            "source_key": "test_source",
            "_full_rebuild_token": _REBUILD_TOKEN,
        },
    }


def _expected_scope_digest(run_id, token=_REBUILD_TOKEN):
    run_id_bytes = run_id.encode("utf-8")
    return hashlib.sha256(
        control_imports._PTG_FULL_REBUILD_SCOPE_DIGEST_DOMAIN
        + uuid.UUID(token).bytes
        + len(run_id_bytes).to_bytes(4, byteorder="big")
        + run_id_bytes
    ).hexdigest()


@pytest.mark.asyncio
async def test_ptg_rebuild_scope_is_ephemeral(
    control_observations,
):
    """Convert the raw token into a one-shot digest before Redis or storage."""

    created_run_by_name, was_created = await control_imports.create_import_run(
        _full_rebuild_create_request()
    )
    recorded = control_observations
    insert_params_by_name = recorded.database.statements[0].compile().params
    enqueue_args, _enqueue_kwargs = recorded.redis.enqueue_calls[0]
    arq_task_by_name = enqueue_args[1]
    expected_scope_digest = _expected_scope_digest("run_ephemeral")
    expected_persisted_params_by_name = {
        "source_key": "test_source",
        "full_rebuild_requested": True,
    }

    assert was_created is True
    assert insert_params_by_name["params"] == expected_persisted_params_by_name
    assert len(recorded.redis.enqueue_calls) == 1
    assert arq_task_by_name["params"] == {
        "source_key": "test_source",
        "_full_rebuild_scope_digest": expected_scope_digest,
    }
    assert created_run_by_name["params"] == expected_persisted_params_by_name
    assert recorded.status_events[0]["params"] == expected_persisted_params_by_name
    assert recorded.live_runs[0]["params"] == expected_persisted_params_by_name
    assert _REBUILD_TOKEN not in repr(insert_params_by_name)
    assert _REBUILD_TOKEN not in repr(arq_task_by_name)
    assert _REBUILD_TOKEN not in repr(created_run_by_name)
    assert _REBUILD_TOKEN not in repr(recorded.status_events)
    assert expected_scope_digest not in repr(insert_params_by_name)
    assert expected_scope_digest not in repr(recorded.status_events)


def test_ptg_rebuild_scope_is_bound_to_the_new_run_id():
    """Prevent the same request token from selecting a prior run's layout."""

    first_scope_digest = control_imports._ptg_full_rebuild_scope_digest(
        _REBUILD_TOKEN,
        run_id="run_first",
    )
    second_scope_digest = control_imports._ptg_full_rebuild_scope_digest(
        _REBUILD_TOKEN,
        run_id="run_second",
    )

    assert first_scope_digest == _expected_scope_digest("run_first")
    assert second_scope_digest == _expected_scope_digest("run_second")
    assert first_scope_digest != second_scope_digest


def test_ptg_rebuild_scope_requires_a_control_run_id():
    with pytest.raises(ValueError, match="requires a control run id"):
        control_imports._ptg_full_rebuild_scope_digest(
            _REBUILD_TOKEN,
            run_id="",
        )


@pytest.mark.parametrize(
    "internal_param_name",
    [
        control_imports._PTG_FULL_REBUILD_SCOPE_PARAM,
        control_imports._PTG_FULL_REBUILD_MARKER_PARAM,
    ],
)
def test_ptg_param_views_reject_caller_injected_rebuild_state(
    internal_param_name,
):
    with pytest.raises(ValueError, match="internal and cannot be supplied"):
        control_imports._import_param_views(
            "ptg",
            {internal_param_name: "caller-controlled"},
            run_id="run_injected",
        )


def test_live_progress_overlay_omits_absent_estimate_and_phase(monkeypatch):
    monkeypatch.setattr(
        control_imports,
        "read_live_progress",
        lambda _run_id: {"pct": 25, "message": "reading"},
    )
    monkeypatch.setattr(
        control_imports,
        "estimate_payload_from_live",
        lambda _live: {},
    )

    observed = control_imports._overlay_live_progress(
        {
            "run_id": "run_without_estimate",
            "status": "running",
            "progress": {},
        }
    )

    assert observed["progress"]["pct"] == 25
    assert "estimate" not in observed
    assert "phase_detail" not in observed


def test_normalize_run_scrubs_only_ptg_ephemeral_params():
    """Defensively hide old PTG tokens without changing another importer."""

    ptg_run_by_name = {
        "importer": "ptg",
        "params": {
            "source_key": "test_source",
            "_full_rebuild_token": _REBUILD_TOKEN,
            "_full_rebuild_scope_digest": "ab" * 32,
            "full_rebuild_requested": True,
        },
    }
    normalized_ptg_run_by_name = control_imports.normalize_run(ptg_run_by_name)
    normalized_npi_run_by_name = control_imports.normalize_run(
        {
            "importer": "npi",
            "params": {
                "_full_rebuild_token": _REBUILD_TOKEN,
                "_full_rebuild_scope_digest": "ab" * 32,
            },
        }
    )

    assert normalized_ptg_run_by_name["params"] == {
        "source_key": "test_source",
        "full_rebuild_requested": True,
    }
    assert _REBUILD_TOKEN not in repr(normalized_ptg_run_by_name)
    assert ptg_run_by_name["params"]["_full_rebuild_token"] == _REBUILD_TOKEN
    assert normalized_npi_run_by_name["params"] == {
        "_full_rebuild_token": _REBUILD_TOKEN,
        "_full_rebuild_scope_digest": "ab" * 32,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_params_by_name", "retry_params_by_name"),
    [
        (
            {"source_key": "test_source", "full_rebuild_requested": True},
            {},
        ),
        (
            {"source_key": "test_source"},
            {"_full_rebuild_token": _SECOND_REBUILD_TOKEN},
        ),
        (
            {"source_key": "test_source"},
            {"_full_rebuild_scope_digest": "ab" * 32},
        ),
    ],
)
async def test_ptg_retry_rejects_rebuild_control(
    monkeypatch,
    current_params_by_name,
    retry_params_by_name,
):
    """Require a new controlled attempt instead of downgrading a rebuild retry."""

    create_requests = []

    async def get_prior_run(_run_id):
        return {
            "run_id": "run_parent",
            "importer": "ptg",
            "params": current_params_by_name,
        }

    async def capture_create(request_by_name):
        create_requests.append(request_by_name)
        return request_by_name, True

    monkeypatch.setattr(control_imports, "get_import_run", get_prior_run)
    monkeypatch.setattr(control_imports, "create_import_run", capture_create)

    with pytest.raises(
        ValueError,
        match="create a new controlled rebuild attempt",
    ):
        await control_imports.retry_import_run(
            "run_parent",
            {"retry_params": retry_params_by_name},
        )

    assert create_requests == []


@pytest.mark.asyncio
async def test_retry_endpoint_reports_rebuild_refusal_as_bad_request(
    monkeypatch,
):
    """Expose the controlled-attempt instruction as a client error."""

    async def reject_retry(_run_id, _payload):
        raise ValueError(
            "full rebuild runs cannot be retried; create a new controlled "
            "rebuild attempt"
        )

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(control, "retry_import_run", reject_retry)
    request = SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json={},
    )

    with pytest.raises(
        BadRequest,
        match="create a new controlled rebuild attempt",
    ):
        await control.control_retry_import(request, "run_parent")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reserved_params_by_name",
    [
        {"_full_rebuild_scope_digest": "ab" * 32},
        {"full_rebuild_requested": True},
    ],
)
async def test_create_rejects_rebuild_internals(
    reserved_params_by_name,
    control_observations,
):
    """Accept only the raw UUID capability at the authenticated API boundary."""

    with pytest.raises(ValueError, match="internal and cannot be supplied"):
        await control_imports.create_import_run(
            {
                "run_id": "run_untrusted",
                "importer": "ptg",
                "params": reserved_params_by_name,
            }
        )

    recorded = control_observations
    assert recorded.database.statements == []
    assert recorded.redis.enqueue_calls == []
    assert recorded.status_events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_token",
    [
        None,
        "",
        "not-a-uuid",
        _REBUILD_TOKEN.upper(),
        f" {_REBUILD_TOKEN}",
        123,
    ],
)
async def test_create_rejects_noncanonical_rebuild_token(
    invalid_token,
    control_observations,
):
    """Reject malformed capabilities before persisting or enqueueing a run."""

    with pytest.raises(
        ValueError,
        match="private PTG full rebuild token must be a valid UUID",
    ) as exc_info:
        await control_imports.create_import_run(
            {
                "run_id": "run_untrusted",
                "importer": "ptg",
                "params": {"_full_rebuild_token": invalid_token},
            }
        )

    recorded = control_observations
    assert recorded.database.statements == []
    assert recorded.redis.enqueue_calls == []
    assert recorded.status_events == []
    if invalid_token:
        assert str(invalid_token) not in str(exc_info.value)


@pytest.mark.asyncio
async def test_ptg_enqueue_failure_status_hides_ephemeral_token(
    monkeypatch,
    control_observations,
):
    """Use a generic persisted error even if the enqueue exception reflects args."""

    async def fail_pool(*_args, **_kwargs):
        raise RuntimeError(f"enqueue rejected {_REBUILD_TOKEN}")

    monkeypatch.setattr(control_imports, "create_pool", fail_pool)
    created_run_by_name, was_created = await control_imports.create_import_run(
        _full_rebuild_create_request()
    )

    assert was_created is True
    assert created_run_by_name["error"] == {
        "code": "enqueue_failed",
        "message": "import job enqueue failed",
    }
    assert _REBUILD_TOKEN not in repr(created_run_by_name)
    assert _REBUILD_TOKEN not in repr(
        control_observations.status_events
    )
    assert _expected_scope_digest("run_ephemeral") not in repr(
        control_observations.status_events
    )


@pytest.mark.asyncio
async def test_ptg_job_id_fallback_never_reflects_ephemeral_payload(monkeypatch):
    """Omit an unsafe fallback when a queue result lacks an explicit job id."""

    class ReflectingJob:
        def __str__(self):
            return f"queued {_REBUILD_TOKEN}"

    class ReflectingRedis:
        async def enqueue_job(self, *_args, **_kwargs):
            return ReflectingJob()

    async def create_reflecting_pool(*_args, **_kwargs):
        return ReflectingRedis()

    monkeypatch.setattr(control_imports, "create_pool", create_reflecting_pool)
    enqueue_state_by_name = await control_imports._enqueue_import_start(
        {
            "run_id": "run_ephemeral",
            "importer": "ptg",
            "params": {
                "source_key": "test_source",
                "_full_rebuild_scope_digest": _expected_scope_digest(
                    "run_ephemeral"
                ),
            },
        }
    )

    assert enqueue_state_by_name["metrics"]["job_id"] == "ptg_start_run_ephemeral"
    assert _REBUILD_TOKEN not in repr(enqueue_state_by_name)
