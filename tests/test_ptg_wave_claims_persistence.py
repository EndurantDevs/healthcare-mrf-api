"""Direct fail-closed worker-claim persistence contracts."""

from __future__ import annotations

import types
from unittest.mock import AsyncMock

import pytest

from process import ptg_wave_claims as claims


_DIGEST = "1" * 64
_RUNTIME = "sha256:" + "2" * 64
_IMAGE = "registry.example/engine@sha256:" + "3" * 64


class _Result:
    def __init__(self, *, scalar=None, rowcount=1):
        self._scalar = scalar
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self._scalar


class _Session:
    def __init__(self, *results, on_execute=None):
        self.results = list(results)
        self.on_execute = on_execute
        self.added = []
        self.flush_count = 0

    async def execute(self, statement):
        if self.on_execute is not None:
            self.on_execute(statement)
        assert self.results, "unexpected database execute"
        return self.results.pop(0)

    def add(self, value):
        self.added.append(value)

    async def flush(self):
        self.flush_count += 1


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _install_transaction(monkeypatch, session):
    monkeypatch.setattr(claims.db, "transaction", lambda: _Transaction(session))


def _input(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "run_id": "run-unit",
        "job_id": "job-unit",
        "slot": 3,
        "pod_uid": "pod-3",
        "pinned_image_reference": _IMAGE,
        "pinned_image_digest": "3" * 64,
        "runtime_image_identity": _RUNTIME,
        "config_identity": "4" * 64,
        "manifest_identity": "5" * 64,
        "claim_attempt_token": "6" * 32,
    }
    fields_by_field.update(overrides)
    return claims.PTGWaveClaimInput(**fields_by_field)


def _normalized(**overrides):
    values = claims._claim_values(_input())
    values.update(overrides)
    return values


def _wave(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "state": "released",
        "state_version": 4,
        "kubernetes_manifest_identity": "5" * 64,
        "pinned_image_reference": _IMAGE,
        "pinned_image_digest": "3" * 64,
        "runtime_image_identity": _RUNTIME,
        "kubernetes_config_identity": "4" * 64,
        "kubernetes_job_uid": "kube-job",
        "kubernetes_ready_attestation": {
            "job_uid": "kube-job",
            "slots": [
                {
                    "slot": slot,
                    "pod_uid": f"pod-{slot}",
                    "runtime_image_identity": _RUNTIME,
                }
                for slot in range(12)
            ],
        },
        "redis_release_ticket": "release-ticket",
        "redis_release_started_at": object(),
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _intent(**overrides):
    fields_by_field = {
        "ordinal": 7,
        "wave_id": "wave-unit",
        "run_id": "run-unit",
        "job_id": "job-unit",
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _matching_claim(wave, normalized, *, status="started", token=None):
    return types.SimpleNamespace(
        wave_id=normalized["wave_id"],
        ordinal=7,
        run_id=normalized["run_id"],
        job_id=normalized["job_id"],
        slot=normalized["slot"],
        pod_uid=normalized["pod_uid"],
        kubernetes_job_uid=wave.kubernetes_job_uid,
        pinned_image_reference=normalized["pinned_image_reference"],
        pinned_image_digest=normalized["pinned_image_digest"],
        runtime_image_identity=normalized["runtime_image_identity"],
        config_identity=normalized["config_identity"],
        manifest_identity=normalized["manifest_identity"],
        claim_status=status,
        failure_code=(None if status == "started" else "ptg_exact_wave_claim_rejected"),
        claim_attempt_token=(
            normalized["claim_attempt_token"] if token is None else token
        ),
    )


def test_claim_scalar_normalizers_accept_only_canonical_identity():
    assert claims._digest(_DIGEST, "digest") == _DIGEST
    assert claims._text("value", "text", 5) == "value"
    assert claims._slot(11) == 11
    assert claims._runtime_identity(_RUNTIME) == _RUNTIME
    assert claims._failure_code("claim_rejected") == "claim_rejected"
    assert claims._attempt_token("a" * 32) == "a" * 32

    cases = [
        (lambda: claims._digest(None, "digest"), "SHA-256"),
        (lambda: claims._text(" value ", "text", 10), "bounded string"),
        (lambda: claims._text("long", "text", 3), "bounded string"),
        (lambda: claims._slot(True), "0 through 11"),
        (lambda: claims._slot(12), "0 through 11"),
        (lambda: claims._runtime_identity("2" * 64), "container sha256"),
        (lambda: claims._failure_code("Bad-Code"), "failure code"),
        (lambda: claims._attempt_token("a" * 31), "attempt token"),
    ]
    for operation, message in cases:
        with pytest.raises(claims.PTGWaveClaimConflict, match=message):
            operation()


def test_claim_input_coercion_and_normalization_are_exact():
    claim_input = _input()
    assert claims._coerce_claim_input(claim_input, {}) is claim_input
    assert claims._claim_values(claim_input) == _normalized()
    with pytest.raises(claims.PTGWaveClaimConflict, match="cannot be combined"):
        claims._coerce_claim_input(claim_input, {"wave_id": "other"})
    with pytest.raises(claims.PTGWaveClaimConflict, match="fields are not exact"):
        claims._coerce_claim_input(None, {"wave_id": "only-one-field"})
    assert claims._coerce_claim_input(None, vars(claim_input)) == claim_input


def test_ready_slot_requires_persisted_complete_matching_attestation():
    wave = _wave()
    claims._exact_ready_slot(
        wave,
        slot=3,
        pod_uid="pod-3",
        runtime_image_identity=_RUNTIME,
    )
    invalid_waves = [
        (_wave(kubernetes_ready_attestation=None), "no persisted"),
        (_wave(kubernetes_job_uid=""), "no persisted"),
        (_wave(kubernetes_ready_attestation={"job_uid": "other", "slots": []}), "no persisted"),
        (_wave(kubernetes_ready_attestation={"job_uid": "kube-job", "slots": []}), "exact 12-slot"),
        (_wave(kubernetes_ready_attestation={"job_uid": "kube-job", "slots": [{}] * 12}), "exact 12-slot"),
    ]
    for invalid, message in invalid_waves:
        with pytest.raises(claims.PTGWaveClaimConflict, match=message):
            claims._exact_ready_slot(
                invalid,
                slot=3,
                pod_uid="pod-3",
                runtime_image_identity=_RUNTIME,
            )
    with pytest.raises(claims.PTGWaveClaimConflict, match="Pod UID"):
        claims._exact_ready_slot(
            wave,
            slot=3,
            pod_uid="other",
            runtime_image_identity=_RUNTIME,
        )


@pytest.mark.asyncio
async def test_locked_claim_identity_revalidates_wave_intent_and_every_pin():
    normalized = _normalized()
    wave = _wave()
    intent = _intent()
    assert await claims._locked_claim_identity(
        _Session(_Result(scalar=wave), _Result(scalar=intent)),
        normalized,
        allow_states=frozenset({"released"}),
    ) == (wave, intent)

    cases = [
        (_Session(_Result(scalar=None)), "not admitted"),
        (_Session(_Result(scalar=_wave(state="cleaning"))), "not allowed"),
        (_Session(_Result(scalar=_wave(pinned_image_digest="f" * 64))), "differs"),
    ]
    for session, message in cases:
        with pytest.raises(claims.PTGWaveClaimConflict, match=message):
            await claims._locked_claim_identity(
                session,
                normalized,
                allow_states=frozenset({"released"}),
            )

    with pytest.raises(claims.PTGWaveClaimConflict, match="job/run pair"):
        await claims._locked_claim_identity(
            _Session(_Result(scalar=wave), _Result(scalar=None)),
            normalized,
            allow_states=frozenset({"released"}),
        )


@pytest.mark.asyncio
async def test_rejected_claim_advances_released_wave_with_optimistic_sync():
    executing = _wave(state="executing")
    await claims._advance_released_wave_for_rejection(_Session(), executing)

    wave = _wave()
    session = _Session(
        _Result(rowcount=1),
        on_execute=lambda _statement: (
            setattr(wave, "state", "executing"),
            setattr(wave, "state_version", 5),
        ),
    )
    await claims._advance_released_wave_for_rejection(session, wave)

    with pytest.raises(claims.PTGWaveClaimConflict, match="state changed"):
        await claims._advance_released_wave_for_rejection(
            _Session(_Result(rowcount=0)),
            _wave(),
        )
    with pytest.raises(claims.PTGWaveClaimConflict, match="synchronization failed"):
        await claims._advance_released_wave_for_rejection(
            _Session(_Result(rowcount=1)),
            _wave(),
        )


def test_matching_claim_binds_every_persisted_identity():
    normalized = _normalized()
    wave = _wave()
    claim = _matching_claim(wave, normalized)
    assert claims._has_matching_claim(claim, normalized, wave=wave, ordinal=7)
    claim.pod_uid = "other"
    assert not claims._has_matching_claim(claim, normalized, wave=wave, ordinal=7)


@pytest.mark.asyncio
async def test_start_claim_state_advancement_covers_release_window_and_execution():
    normalized = _normalized()
    wave = _wave()
    await claims._advance_wave_for_start_claim(
        _Session(_Result(scalar=wave), _Result(rowcount=1)),
        normalized,
    )
    with pytest.raises(claims.PTGWaveClaimConflict, match="not admitted"):
        await claims._advance_wave_for_start_claim(
            _Session(_Result(scalar=None)),
            normalized,
        )
    with pytest.raises(claims.PTGWaveClaimConflict, match="state changed"):
        await claims._advance_wave_for_start_claim(
            _Session(_Result(scalar=_wave()), _Result(rowcount=0)),
            normalized,
        )

    for redis_wave in (
        _wave(state="redis_releasing", redis_release_ticket=None),
        _wave(state="redis_releasing", redis_release_started_at=None),
    ):
        with pytest.raises(claims.PTGWaveClaimConflict, match="persisted Redis"):
            await claims._advance_wave_for_start_claim(
                _Session(_Result(scalar=redis_wave)),
                normalized,
            )
    await claims._advance_wave_for_start_claim(
        _Session(_Result(scalar=_wave(state="redis_releasing"))),
        normalized,
    )
    await claims._advance_wave_for_start_claim(
        _Session(_Result(scalar=_wave(state="executing"))),
        normalized,
    )
    with pytest.raises(claims.PTGWaveClaimConflict, match="not allowed"):
        await claims._advance_wave_for_start_claim(
            _Session(_Result(scalar=_wave(state="cleaning"))),
            normalized,
        )


@pytest.mark.asyncio
async def test_duplicate_intent_claim_is_refused():
    normalized = _normalized()
    await claims._require_unclaimed_intent_claim(
        _Session(_Result(scalar=None)),
        normalized,
        ordinal=7,
    )
    with pytest.raises(claims.PTGWaveClaimConflict, match="already claimed"):
        await claims._require_unclaimed_intent_claim(
            _Session(_Result(scalar=7)),
            normalized,
            ordinal=7,
        )


@pytest.mark.asyncio
async def test_public_start_claim_inserts_one_immutable_row(monkeypatch):
    wave = _wave(state="executing")
    intent = _intent()
    session = _Session()
    _install_transaction(monkeypatch, session)
    monkeypatch.setattr(claims, "_advance_wave_for_start_claim", AsyncMock())
    monkeypatch.setattr(
        claims,
        "_locked_claim_identity",
        AsyncMock(return_value=(wave, intent)),
    )
    monkeypatch.setattr(claims, "_require_unclaimed_intent_claim", AsyncMock())
    await claims.claim_wave_job_start(_input())
    assert len(session.added) == 1
    added = session.added[0]
    assert added.ordinal == 7
    assert added.claim_status == "started"
    assert added.kubernetes_job_uid == "kube-job"
    assert session.flush_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing", "expected_status", "same_attempt"),
    [
        (None, None, None),
        ("started", "claimed", True),
        ("rejected", "rejected", False),
    ],
)
async def test_existing_claim_resolution_handles_absence_started_and_rejected(
    existing, expected_status, same_attempt,
):
    normalized = _normalized()
    wave = _wave(state="executing")
    intent = _intent()
    row = None
    if existing is not None:
        token = None if same_attempt else "7" * 32
        row = _matching_claim(wave, normalized, status=existing, token=token)
    resolution = await claims._existing_claim_resolution(
        _Session(_Result(scalar=row)),
        wave,
        intent,
        normalized,
    )
    if existing is None:
        assert resolution is None
    else:
        assert resolution.status == expected_status
        assert resolution.same_attempt is same_attempt


@pytest.mark.asyncio
async def test_existing_claim_resolution_rejects_identity_and_status_drift():
    normalized = _normalized()
    wave = _wave(state="executing")
    intent = _intent()
    mismatched = _matching_claim(wave, normalized)
    mismatched.job_id = "other"
    with pytest.raises(claims.PTGWaveClaimConflict, match="differs"):
        await claims._existing_claim_resolution(
            _Session(_Result(scalar=mismatched)),
            wave,
            intent,
            normalized,
        )
    invalid_status = _matching_claim(wave, normalized)
    invalid_status.claim_status = "unknown"
    with pytest.raises(claims.PTGWaveClaimConflict, match="invalid status"):
        await claims._existing_claim_resolution(
            _Session(_Result(scalar=invalid_status)),
            wave,
            intent,
            normalized,
        )


@pytest.mark.asyncio
async def test_rejected_claim_marks_only_launchable_import_run_failed():
    normalized = _normalized()
    wave = _wave(state="executing")
    intent = _intent()
    with pytest.raises(claims.PTGWaveClaimConflict, match="lacks its ImportRun"):
        await claims._persist_rejected_claim_and_run(
            _Session(_Result(scalar=None)),
            wave,
            intent,
            normalized,
            failure_code="claim_rejected",
        )
    with pytest.raises(claims.PTGWaveClaimConflict, match="non-launchable"):
        await claims._persist_rejected_claim_and_run(
            _Session(_Result(scalar=types.SimpleNamespace(status="succeeded"))),
            wave,
            intent,
            normalized,
            failure_code="claim_rejected",
        )

    run = types.SimpleNamespace(status="running")
    session = _Session(_Result(scalar=run))
    resolution = await claims._persist_rejected_claim_and_run(
        session,
        wave,
        intent,
        normalized,
        failure_code="claim_rejected",
    )
    assert resolution.status == "rejected"
    assert resolution.same_attempt is True
    assert run.status == "failed"
    assert run.error == {"code": "claim_rejected", "retryable": False}
    assert session.added[0].claim_status == "rejected"
    assert session.flush_count == 1


@pytest.mark.asyncio
async def test_public_exception_reconciliation_replays_or_persists(monkeypatch):
    wave = _wave(state="executing")
    intent = _intent()
    session = _Session()
    _install_transaction(monkeypatch, session)
    monkeypatch.setattr(
        claims,
        "_locked_claim_identity",
        AsyncMock(return_value=(wave, intent)),
    )
    advance = AsyncMock()
    monkeypatch.setattr(claims, "_advance_released_wave_for_rejection", advance)
    existing = claims.PTGWaveClaimResolution("claimed", 7, "started", True)
    monkeypatch.setattr(
        claims,
        "_existing_claim_resolution",
        AsyncMock(return_value=existing),
    )
    persist = AsyncMock()
    monkeypatch.setattr(claims, "_persist_rejected_claim_and_run", persist)
    assert await claims.reconcile_wave_claim_exception(_input()) == existing
    persist.assert_not_awaited()

    claims._existing_claim_resolution.return_value = None
    persisted = claims.PTGWaveClaimResolution("rejected", 7, "rejected", True)
    persist.return_value = persisted
    assert await claims.reconcile_wave_claim_exception(
        _input(),
        failure_code="claim_rejected",
    ) == persisted
    persist.assert_awaited_once()
