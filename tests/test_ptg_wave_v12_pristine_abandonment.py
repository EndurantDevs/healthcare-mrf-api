"""Fresh-v6 pristine abandonment proof and signed persistence tests."""

from __future__ import annotations

import copy
import datetime as dt
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_import_wave_abandonment as abandonment
from api import control_wave_routes as routes
from api.control_import_waves import (
    _new_wave_record,
    _prepare_wave_intents,
    validate_import_wave_payload,
)
from api.ptg_wave_kubernetes import build_ptg_wave_job
from process import ptg_wave_materialized_preclaim_supersession_runtime as runtime
from process.ptg_wave_materialized_preclaim_supersession import (
    PTGWaveMaterializedPreclaimObservation,
)
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_receipt_authority import (
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    ABANDONMENT_RECEIPT_SCHEMA,
    PTGWaveReceiptKeyring,
    RETAINED_PRIVATE_KEY_FILES_ENV,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_REQUEST_SCHEMA,
    V12_QUARANTINE_REASON,
    admission_receipt_mapping,
    ordinary_cutover_id,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v12_pristine_abandonment import (
    abandonment_receipt_payload,
    attest_v12_pristine_materialized_abandonment,
    validate_v12_pristine_abandonment_proof,
)
from tests.test_control_import_waves import (
    _KEY,
    _v6_payload,
)
from tests.test_ptg_wave_preclaim_supersession import (
    _BARRIER_FACTORY,
    _IMAGE,
    _RUNTIME_IMAGE,
    _actual_job,
    _empty_redis_attestation,
)
from tests.test_ptg_wave_receipt_authority import _new_key
from tests.ptg_wave_receipt_test_keys import (
    EPHEMERAL_RECEIPT_PRIVATE_KEY,
    EPHEMERAL_RECEIPT_PUBLIC_MODULUS,
)


from tests.ptg_wave_v12_pristine_abandonment_support import (
    FIXED_KEY,
    Result as _Result,
    Session as _Session,
    Transaction as _Transaction,
    boundary as _boundary,
    keyring as _keyring,
    proof as _proof,
    request as _request,
)


def test_fresh_proof_binds_exact_admission_database_job_and_redis():
    proof, admission = _proof()

    assert proof["schema_version"].endswith("abandonment-proof.v1")
    assert proof["recovery_basis"] == V12_QUARANTINE_REASON
    assert proof["admission"] == admission
    assert proof["database"]["intent_count"] == 2
    assert proof["database"]["unassigned_run_count"] == 2
    assert proof["database"]["claim_count"] == 0
    assert proof["kubernetes"]["failed"] == 12
    assert proof["redis"]["release_present"] is False
    assert validate_v12_pristine_abandonment_proof(proof) == proof
    receipt_payload = abandonment_receipt_payload(proof)
    assert receipt_payload["recovery_evidence_sha256"] == (
        proof["proof_digest"]
    )
    assert receipt_payload["admission"] == admission


@pytest.mark.parametrize(
    "changes,message",
    (
        ({"claims": (object(),)}, "zero claims"),
        ({"outcomes": (object(),)}, "zero outcomes"),
        ({"worker_events": (0,)}, "zero worker start"),
        ({"logical": object()}, "legacy recovery"),
        ({"rollback": object()}, "legacy recovery"),
    ),
)
def test_fresh_proof_rejects_work_and_cross_family_recovery(changes, message):
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match=message):
        _proof(**changes)


def test_fresh_proof_rejects_run_job_redis_and_receipt_drift():
    wave, intents, runs, admission = _boundary()
    runs[0].node_id = "assigned"
    observation = PTGWaveMaterializedPreclaimObservation(
        predecessor_wave=wave,
        intents=intents,
        runs=runs,
        claims=(),
        outcomes=(),
        worker_start_event_ordinals=(),
        logical_supersession=None,
        admission_rollback=None,
        actual_job=_actual_job(wave.kubernetes_manifest),
        redis_unclaimed_attestation=_empty_redis_attestation(wave),
    )
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        attest_v12_pristine_materialized_abandonment(
            observation,
            cutover_id=ordinary_cutover_id(wave.wave_id),
            admission=admission,
        )

    active_job = _actual_job(wave.kubernetes_manifest)
    active_job["status"]["active"] = 1
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        _proof(actual_job=active_job)

    redis = _empty_redis_attestation(wave)
    redis["release_present"] = True
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        _proof(redis=redis)


@pytest.mark.parametrize(
    "mutate",
    (
        lambda proof: proof.update(extra="forbidden"),
        lambda proof: proof.update(schema_version="legacy"),
        lambda proof: proof["admission"].update(
            attestation_schema="healthporta.ptg-import-wave-attestation.v4"
        ),
        lambda proof: proof["database"].update(claim_count=1),
        lambda proof: proof["kubernetes"].update(failed=11),
        lambda proof: proof["redis"].update(release_present=True),
        lambda proof: proof.update(proof_digest="0" * 64),
    ),
)
def test_fresh_proof_rejects_extra_downgrade_and_forgery(mutate):
    proof, _admission = _proof()
    mutate(proof)
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        validate_v12_pristine_abandonment_proof(proof)


@pytest.mark.asyncio
async def test_v12_abandonment_persists_and_replays_identical_receipt(
    monkeypatch,
):
    """Prove V12 abandonment persists and replays an identical receipt."""
    proof, admission = _proof()
    request = _request(admission)
    keyring = _keyring(monkeypatch)
    session = _Session()
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        abandonment,
        "acquire_ptg_admission_lock",
        AsyncMock(),
    )
    observer = AsyncMock(return_value=proof)
    monkeypatch.setattr(
        abandonment,
        "attest_locked_v12_abandonment",
        observer,
    )

    receipt, created = await abandonment.abandon_materialized_preclaim_wave(
        admission["wave_id"],
        request,
        redis="redis",
        receipt_keyring=keyring,
        receipt_issued_at="2026-08-10T12:34:57.654321Z",
    )
    stored = _assert_new_abandonment_receipt(
        receipt,
        created,
        proof,
        session,
        observer,
    )

    replay_session = _Session(stored)
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(replay_session),
    )
    replay, replay_created = (
        await abandonment.abandon_materialized_preclaim_wave(
            admission["wave_id"],
            copy.deepcopy(request),
            redis=object(),
            receipt_keyring=keyring,
        )
    )
    assert replay_created is False
    assert canonical_json(replay) == canonical_json(receipt)
    assert replay_session.added == []


def _assert_new_abandonment_receipt(
    receipt,
    created,
    proof,
    session,
    observer,
):
    """Assert the first signed abandonment and its persisted quarantine."""
    assert created is True
    assert receipt["schema"] == ABANDONMENT_RECEIPT_SCHEMA
    assert receipt["payload"] == abandonment_receipt_payload(proof)
    assert set(receipt) == {
        "schema", "key_id", "issued_at", "payload", "payload_digest", "signature",
    }
    stored = session.added[0]
    assert stored.reason == V12_QUARANTINE_REASON
    assert stored.recovery_evidence_sha256 == proof["proof_digest"]
    assert stored.abandonment_receipt == receipt
    observer.assert_awaited_once()
    return stored


@pytest.mark.asyncio
async def test_control_token_rotation_does_not_wedge_v12_abandonment(
    monkeypatch,
):
    wave, intents, runs, admission = _boundary()
    proof, expected_admission = _proof()
    assert admission == expected_admission
    request = _request(admission)
    session = _Session()
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        abandonment,
        "acquire_ptg_admission_lock",
        AsyncMock(),
    )
    snapshot = runtime._MaterializedDatabaseSnapshot(
        wave=wave,
        intents=intents,
        runs=runs,
        claims=(),
        outcomes=(),
        worker_start_event_ordinals=(),
        logical_supersession=None,
        admission_rollback=None,
    )
    monkeypatch.setattr(
        runtime,
        "_load_snapshot",
        AsyncMock(return_value=snapshot),
    )
    monkeypatch.setattr(
        runtime,
        "_observe_v12",
        AsyncMock(return_value=proof),
    )
    monkeypatch.setenv(
        "HLTHPRT_CONTROL_API_TOKEN",
        "token-b-after-v6-admission",
    )

    receipt, created = await abandonment.abandon_materialized_preclaim_wave(
        admission["wave_id"],
        request,
        redis=object(),
        receipt_keyring=_keyring(monkeypatch),
        receipt_issued_at="2026-08-10T12:34:57.654321Z",
    )

    assert created is True
    assert receipt["payload"]["admission"][
        "cohort_signature_digest"
    ] == wave.cohort_signature_digest


@pytest.mark.asyncio
async def test_v12_route_returns_direct_receipt_with_first_and_replay_status(
    monkeypatch,
):
    proof, admission = _proof()
    request_body = _request(admission)
    response_receipt_by_field = {
        "schema": ABANDONMENT_RECEIPT_SCHEMA,
        "key_id": request_body["key_id"],
        "issued_at": "2026-08-10T12:34:57.654321Z",
        "payload": abandonment_receipt_payload(proof),
        "payload_digest": "1" * 64,
        "signature": "2" * 512,
    }
    service = AsyncMock(
        side_effect=((response_receipt_by_field, True), (response_receipt_by_field, False)),
    )
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "abandon_materialized_preclaim_wave",
        service,
    )
    process_keyring = object()
    request = SimpleNamespace(
        json=request_body,
        app=SimpleNamespace(
            ctx=SimpleNamespace(
                ptg_wave_redis="redis",
                ptg_wave_receipt_keyring=process_keyring,
            )
        ),
    )

    first = await routes.control_abandon_materialized_preclaim_wave(
        request,
        admission["wave_id"],
    )
    replay = await routes.control_abandon_materialized_preclaim_wave(
        request,
        admission["wave_id"],
    )

    assert first.status == 201
    assert replay.status == 200
    assert json.loads(first.body) == response_receipt_by_field
    assert json.loads(replay.body) == response_receipt_by_field
    assert service.await_args_list[0].kwargs == {
        "redis": "redis",
        "receipt_keyring": process_keyring,
    }


@pytest.mark.asyncio
async def test_v12_abandonment_rejects_wrong_key_and_cutover_before_observe(
    monkeypatch,
):
    _proof_value, admission = _proof()
    request = _request(admission)
    monkeypatch.setattr(
        abandonment,
        "attest_locked_v12_abandonment",
        AsyncMock(side_effect=AssertionError("invalid request reached observer")),
    )

    wrong_key = copy.deepcopy(request)
    wrong_key["key_id"] = "other-key"
    with pytest.raises(Exception, match="identity|conflicts|key"):
        await abandonment.abandon_materialized_preclaim_wave(
            admission["wave_id"],
            wrong_key,
            redis=object(),
        )

    wrong_cutover = copy.deepcopy(request)
    wrong_cutover["cutover_id"] = "0" * 64
    with pytest.raises(Exception, match="identity|cutover"):
        await abandonment.abandon_materialized_preclaim_wave(
            admission["wave_id"],
            wrong_cutover,
            redis=object(),
        )


@pytest.mark.asyncio
async def test_rotation_uses_retained_epoch_for_pinned_abandonment(
    monkeypatch,
    tmp_path,
):
    proof, admission = _proof()
    request = _request(admission)
    new_key = _new_key(tmp_path / "new-active.pem")
    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, "receipt-new")
    monkeypatch.setenv(ACTIVE_PRIVATE_KEY_FILE_ENV, str(new_key.resolve()))
    monkeypatch.setenv(
        RETAINED_PRIVATE_KEY_FILES_ENV,
        json.dumps({"receipt-active": str(FIXED_KEY.resolve())}),
    )
    rotating = PTGWaveReceiptKeyring.from_environment()
    session = _Session()
    monkeypatch.setattr(
        abandonment.db,
        "transaction",
        lambda: _Transaction(session),
    )
    monkeypatch.setattr(
        abandonment,
        "acquire_ptg_admission_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        abandonment,
        "attest_locked_v12_abandonment",
        AsyncMock(return_value=proof),
    )

    receipt, created = await abandonment.abandon_materialized_preclaim_wave(
        admission["wave_id"],
        request,
        redis=object(),
        receipt_keyring=rotating,
        receipt_issued_at="2026-08-10T12:34:57.654321Z",
    )

    assert created is True
    assert rotating.active_key_id == "receipt-new"
    assert receipt["key_id"] == "receipt-active"
    assert rotating.validate_stored_receipt(
        receipt,
        schema=ABANDONMENT_RECEIPT_SCHEMA,
        key_id="receipt-active",
        expected_payload=receipt["payload"],
    ) == receipt
