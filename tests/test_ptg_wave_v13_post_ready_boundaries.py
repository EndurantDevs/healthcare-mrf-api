"""Runtime and API boundaries for V13 post-ready abandonment."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, NotFound, SanicException

from api import control_import_wave_v13_abandonment as persistence
from api import control_wave_routes as routes
from process import ptg_wave_v13_post_ready_abandonment_runtime as runtime
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_receipt_authority import PTGWaveReceiptAuthorityError
from process.ptg_wave_receipt_contract import PTGWaveReceiptContractError
from tests.ptg_wave_v12_pristine_abandonment_support import (
    boundary,
    keyring,
    request as v12_request,
)
from tests.ptg_wave_v13_post_ready_boundary_support import (
    RuntimeResult,
    RuntimeSession,
    observation_boundary,
    route_request,
    runtime_session,
    stored_v13_quarantine,
)
from tests.test_ptg_wave_v13_post_ready_abandonment import (
    _proof,
    _request,
)


@pytest.mark.asyncio
async def test_locked_runtime_observes_all_sources_and_builds_proof(monkeypatch):
    """The locked orchestration reads DB, Kubernetes, and Redis without mutation."""

    observation, admission, redis = await observation_boundary()
    wave = observation.predecessor_wave
    session = runtime_session(observation)
    monkeypatch.setattr(runtime, "get_wave_job", lambda _wave_id: observation.actual_job)
    monkeypatch.setattr(runtime, "list_wave_pods", lambda _wave_id: observation.actual_pods)

    proof = await runtime.attest_locked_v13_abandonment(
        session,
        wave.wave_id,
        _request(admission),
        redis=redis,
    )

    assert proof["operation_id"] == wave.wave_id
    assert proof["database"]["state"] == "slots_waiting"
    assert session.remaining == 0


@pytest.mark.asyncio
async def test_locked_runtime_rejects_missing_job_and_wave(monkeypatch):
    """A vanished external Job or durable wave cannot be converted into proof."""

    observation, admission, redis = await observation_boundary()
    monkeypatch.setattr(runtime, "get_wave_job", lambda _wave_id: None)
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="Job is unavailable"):
        await runtime.attest_locked_v13_abandonment(
            runtime_session(observation),
            admission["wave_id"],
            _request(admission),
            redis=redis,
        )

    missing_wave_session = RuntimeSession(
        [RuntimeResult(), RuntimeResult(scalar=None), RuntimeResult(scalar=None)]
    )
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="missing"):
        await runtime._locked_wave(missing_wave_session, admission["wave_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wave_id", "redis", "side_effect", "message"),
    [
        pytest.param("wave", None, None, "Redis observer", id="redis-required"),
        pytest.param("", object(), None, "wave ID", id="wave-id"),
        pytest.param(
            "wave",
            object(),
            PTGWaveMaterializedPreclaimConflict("boundary"),
            "boundary",
            id="boundary-conflict",
        ),
        pytest.param(
            "wave",
            object(),
            PTGWaveReceiptContractError("request"),
            "request",
            id="request-contract",
        ),
        pytest.param(
            "wave",
            object(),
            RuntimeError("transport"),
            "observation failed",
            id="unexpected-observer-error",
        ),
    ],
)
async def test_locked_runtime_normalizes_failures(
    monkeypatch,
    wave_id,
    redis,
    side_effect,
    message,
):
    """The facade preserves conflicts and closes lower-level error families."""

    observer = AsyncMock(side_effect=side_effect)
    monkeypatch.setattr(runtime, "_attest_locked_observation", observer)

    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match=message):
        await runtime.attest_locked_v13_abandonment(
            object(),
            wave_id,
            {},
            redis=redis,
        )


@pytest.mark.asyncio
async def test_v13_get_distinguishes_absence_from_other_recovery(monkeypatch):
    """GET returns absence but refuses to reinterpret another quarantine family."""

    monkeypatch.setattr(
        persistence.db,
        "execute",
        AsyncMock(return_value=RuntimeResult(scalar=None)),
    )
    assert (
        await persistence.get_v13_post_ready_abandonment(
            "wave",
            receipt_keyring=object(),
        )
        is None
    )

    monkeypatch.setattr(
        persistence.db,
        "execute",
        AsyncMock(
            return_value=RuntimeResult(
                scalar=SimpleNamespace(reason="another-recovery")
            )
        ),
    )
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="another recovery"):
        await persistence.get_v13_post_ready_abandonment(
            "wave",
            receipt_keyring=object(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda stored: setattr(stored, "reason", "another-recovery"),
            id="family",
        ),
        pytest.param(
            lambda stored: setattr(stored, "recovery_evidence_sha256", "0" * 64),
            id="evidence-metadata",
        ),
        pytest.param(
            lambda stored: setattr(
                stored,
                "abandonment_receipt_payload_digest",
                "0" * 64,
            ),
            id="receipt-metadata",
        ),
    ],
)
async def test_v13_replay_rejects_stored_metadata_drift(monkeypatch, mutate):
    """Replay validates the stored proof and signed envelope before returning it."""

    proof, admission = await _proof()
    request = _request(admission)
    signer = keyring(monkeypatch)
    stored = stored_v13_quarantine(proof, request, signer)
    mutate(stored)

    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        persistence.existing_v13_response(
            stored,
            request=request,
            receipt_keyring=signer,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        pytest.param(
            lambda request: request.update(schema="unsupported"),
            "unsupported",
            id="schema",
        ),
        pytest.param(
            lambda request: request.pop("admission"),
            "fields",
            id="fields",
        ),
        pytest.param(
            lambda request: request.update(key_id=""),
            "key ID",
            id="key-id",
        ),
        pytest.param(
            lambda request: request.update(operation_id="0" * 64),
            "identity",
            id="identity",
        ),
    ],
)
async def test_v13_request_normalization_rejects_bad_coordinates(mutate, message):
    """Persistence accepts only one exact V13 request coordinate."""

    _proof_value, admission = await _proof()
    request = _request(admission)
    mutate(request)

    with pytest.raises(PTGWaveReceiptContractError, match=message):
        persistence.normalize_abandonment_request(admission["wave_id"], request)


def test_v12_request_normalization_retains_exact_field_guard():
    """The V13 extension must not weaken the existing V12 request contract."""

    _wave, _intents, _runs, admission = boundary()
    request = v12_request(admission)
    request.pop("admission")

    with pytest.raises(PTGWaveReceiptContractError, match="V12.*fields"):
        persistence.normalize_abandonment_request(admission["wave_id"], request)


@pytest.mark.asyncio
async def test_v13_routes_reject_bad_requests_and_missing_receipts(monkeypatch):
    """Routes map malformed inputs and absence to stable HTTP errors."""

    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    _proof_value, admission = await _proof()
    request_body = _request(admission)
    request_body["schema"] = "unsupported"
    with pytest.raises(BadRequest, match="unsupported"):
        await routes.control_abandon_materialized_preclaim_wave(
            route_request(body=request_body),
            admission["wave_id"],
        )

    with pytest.raises(BadRequest, match="query fields"):
        await routes.control_get_v13_abandonment(
            route_request(args={"unexpected": "value"}),
            admission["wave_id"],
        )

    monkeypatch.setattr(
        routes,
        "get_v13_post_ready_abandonment",
        AsyncMock(return_value=None),
    )
    with pytest.raises(NotFound, match="receipt not found"):
        await routes.control_get_v13_abandonment(
            route_request(),
            admission["wave_id"],
        )

    with pytest.raises(BadRequest, match="ordinary terminal fields"):
        await routes.control_issue_ordinary_terminal_receipt(
            route_request(body={}),
            admission["wave_id"],
        )

    terminal_by_field = {
        "schema": "unsupported",
        "key_id": "key",
        "operation_id": admission["wave_id"],
        "member_ordinal": 0,
        "source_file_import_id": 1,
        "run_id": "run",
    }
    with pytest.raises(BadRequest, match="schema is unsupported"):
        await routes.control_issue_ordinary_terminal_receipt(
            route_request(body=terminal_by_field),
            admission["wave_id"],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        pytest.param(PTGWaveReceiptAuthorityError("authority"), 503, id="authority"),
        pytest.param(
            PTGWaveMaterializedPreclaimConflict("conflict"),
            409,
            id="conflict",
        ),
        pytest.param(ValueError("invalid"), 400, id="invalid"),
    ],
)
async def test_v13_get_route_maps_service_failures(
    monkeypatch,
    failure,
    expected_status,
):
    """GET exposes stable availability, conflict, and input status."""

    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_v13_post_ready_abandonment",
        AsyncMock(side_effect=failure),
    )

    with pytest.raises(SanicException) as raised:
        await routes.control_get_v13_abandonment(route_request(), "wave")
    assert raised.value.status_code == expected_status
