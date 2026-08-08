"""Direct persistence and defensive-branch tests for exact-wave state."""

from __future__ import annotations

import copy
import types
from unittest.mock import AsyncMock, Mock

import pytest

from process import ptg_wave_state as state


_WAVE = "1" * 64
_IMAGE_DIGEST = "2" * 64
_RUNTIME = "sha256:" + "3" * 64
_CONFIG = "4" * 64
_MANIFEST_IDENTITY = "5" * 64
_IMAGE = "registry.example/engine@sha256:" + _IMAGE_DIGEST


class _Result:
    def __init__(self, *, scalar=None, rowcount=1):
        self._scalar = scalar
        self.rowcount = rowcount

    def scalar_one_or_none(self):
        return self._scalar

    def scalar(self):
        return self._scalar


class _Session:
    def __init__(self, *results):
        self.results = list(results)
        self.flush_count = 0

    async def execute(self, _statement):
        assert self.results, "unexpected database execute"
        return self.results.pop(0)

    async def flush(self):
        self.flush_count += 1


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _wave(**overrides):
    manifest_by_field = {"apiVersion": "batch/v1", "kind": "Job"}
    manifest_bytes = state.canonical_json(manifest_by_field)
    fields_by_field = {
        "wave_id": "wave-unit",
        "wave_digest": _WAVE,
        "state": "admitted",
        "state_version": 1,
        "uncertainty_resume_state": None,
        "intent_count": 2,
        "kubernetes_manifest": manifest_by_field,
        "kubernetes_manifest_bytes": manifest_bytes,
        "kubernetes_manifest_sha256": state.sha256_digest(manifest_bytes),
        "kubernetes_manifest_identity": _MANIFEST_IDENTITY,
        "pinned_image_reference": _IMAGE,
        "pinned_image_digest": _IMAGE_DIGEST,
        "runtime_image_identity": _RUNTIME,
        "kubernetes_config_identity": _CONFIG,
        "k8s_post_ticket": None,
        "k8s_post_started_at": object(),
        "kubernetes_job_uid": "job-uid",
        "kubernetes_job_receipt": None,
        "kubernetes_job_receipt_digest": "6" * 64,
        "kubernetes_ready_attestation": None,
        "kubernetes_ready_attestation_digest": "7" * 64,
        "redis_release_ticket": None,
        "redis_release_started_at": object(),
        "redis_release_attestation": None,
        "redis_release_attestation_digest": None,
        "release_queue": f"arq:PTGSmall:wave:{_WAVE}",
        "manifest_digest": "8" * 64,
        "jobs_digest": "9" * 64,
        "protocol_identity": "protocol-v1",
        "serializer_identity": "serializer-v1",
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _install_wave(monkeypatch, wave, session=None):
    session = session or _Session()
    monkeypatch.setattr(state.db, "transaction", lambda: _Transaction(session))
    monkeypatch.setattr(state, "_locked_wave", AsyncMock(return_value=wave))
    return session


def _job_receipt(wave):
    return {
        "wave_digest": wave.wave_digest,
        "job_uid": "job-uid",
        "manifest_identity": wave.kubernetes_manifest_identity,
        "config_identity": wave.kubernetes_config_identity,
        "pinned_image_reference": wave.pinned_image_reference,
        "pinned_image_digest": wave.pinned_image_digest,
        "runtime_image_identity": wave.runtime_image_identity,
    }


def _ready_receipt(wave):
    return {
        **_job_receipt(wave),
        "slots": [
            {
                "slot": slot,
                "pod_uid": f"pod-{slot}",
                "runtime_image_identity": wave.runtime_image_identity,
            }
            for slot in range(12)
        ],
    }


def test_scalar_validators_and_canonical_json_are_fail_closed():
    assert state._digest("a" * 64, "digest") == "a" * 64
    assert state._runtime_image_identity(_RUNTIME) == _RUNTIME
    assert state._ticket("ticket:1") == "ticket:1"
    for value, function, message in (
        (None, lambda value: state._digest(value, "digest"), "SHA-256"),
        ("A" * 64, lambda value: state._digest(value, "digest"), "SHA-256"),
        ("3" * 64, state._runtime_image_identity, "container sha256"),
        (" bad ", state._ticket, "bounded canonical"),
        ("x" * 129, state._ticket, "bounded canonical"),
    ):
        with pytest.raises(state.PTGWaveStateConflict, match=message):
            function(value)
    for value in ({"bad": object()}, {"nan": float("nan")}):
        with pytest.raises(state.PTGWaveStateConflict, match="canonical JSON"):
            state.canonical_json(value)


def test_transition_table_rejects_unknown_and_admitted_uncertainty():
    with pytest.raises(state.PTGWaveStateConflict, match="invalid"):
        state.assert_transition("unknown", "released")
    with pytest.raises(state.PTGWaveStateConflict, match="only resume"):
        state.assert_transition(
            "uncertain",
            "released",
            resume_state="slots_waiting",
        )
    original = state._NEXT_STATES["admitted"]
    state._NEXT_STATES["admitted"] = frozenset({"uncertain"})
    try:
        with pytest.raises(state.PTGWaveStateConflict, match="admitted wave"):
            state.assert_transition("admitted", "uncertain")
    finally:
        state._NEXT_STATES["admitted"] = original


@pytest.mark.asyncio
async def test_locked_wave_and_optimistic_transition_contract():
    wave = _wave(state="admitted")
    session = _Session(_Result(scalar=wave), _Result(rowcount=1))
    assert await state._locked_wave(session, wave.wave_id) is wave
    await state._transition(session, wave, "materializing")
    assert session.results == []

    with pytest.raises(state.PTGWaveStateConflict, match="not admitted"):
        await state._locked_wave(_Session(_Result(scalar=None)), wave.wave_id)

    with pytest.raises(state.PTGWaveStateConflict, match="concurrently"):
        await state._transition(
            _Session(_Result(rowcount=0)),
            wave,
            "materializing",
            values={"extra": "value"},
        )


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (({}, b"{}", _IMAGE, _IMAGE_DIGEST, _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "non-empty object"),
        (({"kind": "Job"}, b"", _IMAGE, _IMAGE_DIGEST, _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "bytes are required"),
        (({"kind": "Job"}, b"\xff", _IMAGE, _IMAGE_DIGEST, _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "must be JSON"),
        (({"kind": "Job"}, b"not-json", _IMAGE, _IMAGE_DIGEST, _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "must be JSON"),
        (({"kind": "Job"}, b'{"kind":"Other"}', _IMAGE, _IMAGE_DIGEST, _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "bytes differ"),
        (({"kind": "Job"}, b'{"kind":"Job"}', _IMAGE, "bad", _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "pinned image digest"),
        (({"kind": "Job"}, b'{"kind":"Job"}', _IMAGE, _IMAGE_DIGEST, "bad", _CONFIG, _MANIFEST_IDENTITY), "runtime image"),
        (({"kind": "Job"}, b'{"kind":"Job"}', _IMAGE, _IMAGE_DIGEST, _RUNTIME, "bad", _MANIFEST_IDENTITY), "config identity"),
        (({"kind": "Job"}, b'{"kind":"Job"}', None, _IMAGE_DIGEST, _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "not pinned"),
        (({"kind": "Job"}, b'{"kind":"Job"}', "registry/x@sha256:" + "f" * 64, _IMAGE_DIGEST, _RUNTIME, _CONFIG, _MANIFEST_IDENTITY), "not pinned"),
        (({"kind": "Job"}, b'{"kind":"Job"}', _IMAGE, _IMAGE_DIGEST, _RUNTIME, _CONFIG, "bad"), "manifest identity"),
    ],
)
def test_materialization_validation_rejects_every_identity_drift(args, message):
    with pytest.raises(state.PTGWaveStateConflict, match=message):
        state._validate_materialization(*args)


@pytest.mark.asyncio
async def test_persist_materialization_passes_exact_values_to_transition(monkeypatch):
    wave = _wave(state="admitted")
    _install_wave(monkeypatch, wave)
    transition = AsyncMock()
    monkeypatch.setattr(state, "_transition", transition)
    digest = await state.persist_materialization(
        wave.wave_id,
        manifest=wave.kubernetes_manifest,
        manifest_bytes=wave.kubernetes_manifest_bytes,
        image_reference=wave.pinned_image_reference,
        image_digest=wave.pinned_image_digest,
        runtime_image_identity=wave.runtime_image_identity,
        config_identity=wave.kubernetes_config_identity,
        manifest_identity=wave.kubernetes_manifest_identity,
    )
    assert digest == wave.kubernetes_manifest_sha256
    assert transition.await_args.args[2] == "materializing"
    assert transition.await_args.kwargs["values"]["pinned_image_reference"] == _IMAGE


def test_require_materialization_detects_missing_and_corrupt_state():
    state._require_materialization(_wave())
    with pytest.raises(state.PTGWaveStateConflict, match="not been persisted"):
        state._require_materialization(_wave(kubernetes_manifest=None))
    with pytest.raises(state.PTGWaveStateConflict, match="bytes are corrupt"):
        state._require_materialization(_wave(kubernetes_manifest_sha256="0" * 64))


@pytest.mark.asyncio
async def test_post_marker_has_one_owner_and_returns_only_persisted_materialization(monkeypatch):
    wave = _wave(state="materializing", k8s_post_ticket="existing")
    _install_wave(monkeypatch, wave)
    assert await state.mark_kubernetes_post_started(
        wave.wave_id,
        operation_ticket="candidate",
    ) == {"owner": False}

    wave.k8s_post_ticket = None
    transition = AsyncMock()
    monkeypatch.setattr(state, "_transition", transition)
    receipt = await state.mark_kubernetes_post_started(
        wave.wave_id,
        operation_ticket="candidate",
    )
    assert receipt["owner"] is True
    assert receipt["manifest_bytes"] == wave.kubernetes_manifest_bytes
    assert receipt["runtime_image_identity"] == wave.runtime_image_identity
    assert transition.await_args.args[2] == "slots_waiting"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda receipt: [], "must be an object"),
        (lambda receipt: {**receipt, "extra": True}, "fields are not exact"),
        (lambda receipt: {**receipt, "wave_digest": "other"}, "does not bind"),
        (lambda receipt: {**receipt, "job_uid": ""}, "no UID"),
    ],
)
def test_job_receipt_requires_exact_shape_and_identity(mutate, message):
    wave = _wave()
    with pytest.raises(state.PTGWaveStateConflict, match=message):
        state._validate_job_receipt(wave, mutate(_job_receipt(wave)))


@pytest.mark.asyncio
async def test_job_receipt_first_write_replay_and_conflict(monkeypatch):
    wave = _wave(
        state="slots_waiting",
        kubernetes_job_uid=None,
        kubernetes_job_receipt_digest=None,
    )
    session = _install_wave(monkeypatch, wave)
    receipt = _job_receipt(wave)
    digest = await state.record_kubernetes_job_created(wave.wave_id, receipt)
    assert wave.kubernetes_job_uid == "job-uid"
    assert wave.kubernetes_job_receipt_digest == digest
    assert session.flush_count == 1
    assert await state.record_kubernetes_job_created(wave.wave_id, receipt) == digest
    wave.kubernetes_job_receipt_digest = "f" * 64
    with pytest.raises(state.PTGWaveStateConflict, match="conflicts"):
        await state.record_kubernetes_job_created(wave.wave_id, receipt)
    wave.state = "released"
    with pytest.raises(state.PTGWaveStateConflict, match="not expected"):
        await state.record_kubernetes_job_created(wave.wave_id, receipt)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda receipt: [], "must be an object"),
        (lambda receipt: {**receipt, "extra": True}, "fields are not exact"),
        (lambda receipt: {**receipt, "config_identity": "other"}, "does not match"),
        (lambda receipt: {**receipt, "job_uid": ""}, "no Job UID"),
        (lambda receipt: {**receipt, "slots": {}}, "exactly 12"),
        (lambda receipt: {**receipt, "slots": [{"slot": 0}] * 12}, "fields are not exact"),
        (lambda receipt: {**receipt, "slots": [{**slot, "slot": True} if slot["slot"] == 0 else slot for slot in receipt["slots"]]}, "unique indexes"),
        (lambda receipt: {**receipt, "slots": [{**slot, "slot": 0} if slot["slot"] == 1 else slot for slot in receipt["slots"]]}, "unique indexes"),
        (lambda receipt: {**receipt, "slots": [{**slot, "slot": 12} if slot["slot"] == 0 else slot for slot in receipt["slots"]]}, "unique indexes"),
        (lambda receipt: {**receipt, "slots": [{**slot, "pod_uid": ""} if slot["slot"] == 0 else slot for slot in receipt["slots"]]}, "no Pod UID"),
        (lambda receipt: {**receipt, "slots": [{**slot, "runtime_image_identity": "other"} if slot["slot"] == 0 else slot for slot in receipt["slots"]]}, "image differs"),
    ],
)
def test_ready_receipt_rejects_shape_identity_and_slot_drift(mutate, message):
    wave = _wave()
    receipt = _ready_receipt(wave)
    with pytest.raises(state.PTGWaveStateConflict, match=message):
        state._validate_ready_receipt(wave, mutate(receipt))


def test_ready_receipt_iteration_must_still_cover_all_slots():
    class ShortIterationList(list):
        def __iter__(self):
            return iter(list.__getitem__(self, slice(0, 11)))

    wave = _wave()
    receipt = _ready_receipt(wave)
    receipt["slots"] = ShortIterationList(receipt["slots"])
    with pytest.raises(state.PTGWaveStateConflict, match="cover indexes"):
        state._validate_ready_receipt(wave, receipt)


@pytest.mark.asyncio
async def test_ready_receipt_first_write_replay_and_all_preconditions(monkeypatch):
    wave = _wave(
        state="slots_waiting",
        kubernetes_ready_attestation_digest=None,
    )
    session = _install_wave(monkeypatch, wave)
    receipt = _ready_receipt(wave)
    digest = await state.record_kubernetes_ready(wave.wave_id, receipt)
    assert wave.kubernetes_ready_attestation_digest == digest
    assert session.flush_count == 1
    assert await state.record_kubernetes_ready(wave.wave_id, receipt) == digest
    wave.kubernetes_ready_attestation_digest = "f" * 64
    with pytest.raises(state.PTGWaveStateConflict, match="conflicts"):
        await state.record_kubernetes_ready(wave.wave_id, receipt)

    cases = [
        (dict(state="released"), "not expected"),
        (dict(kubernetes_ready_attestation_digest=None, runtime_image_identity="sha256:" + "e" * 64), "differs from"),
        (dict(kubernetes_ready_attestation_digest=None, kubernetes_job_uid=None), "must precede"),
        (dict(kubernetes_ready_attestation_digest=None, kubernetes_job_receipt_digest=None), "must precede"),
        (dict(kubernetes_ready_attestation_digest=None, kubernetes_job_uid="other"), "UID differs"),
    ]
    for overrides, message in cases:
        candidate = _wave(**{"state": "slots_waiting", **overrides})
        _install_wave(monkeypatch, candidate)
        with pytest.raises(state.PTGWaveStateConflict, match=message):
            await state.record_kubernetes_ready(candidate.wave_id, receipt)

    candidate = _wave(state="slots_waiting", kubernetes_ready_attestation_digest=None)
    _install_wave(monkeypatch, candidate)
    bad_runtime_by_field = dict(receipt, runtime_image_identity="invalid")
    bad_runtime_by_field["slots"] = [
        dict(slot, runtime_image_identity="invalid") for slot in receipt["slots"]
    ]
    with pytest.raises(state.PTGWaveStateConflict, match="runtime image identity"):
        await state.record_kubernetes_ready(candidate.wave_id, bad_runtime_by_field)


@pytest.mark.asyncio
async def test_redis_release_marker_owner_and_preconditions(monkeypatch):
    wave = _wave(state="slots_waiting", redis_release_ticket="existing")
    _install_wave(monkeypatch, wave)
    assert not await state.has_started_redis_release(
        wave.wave_id,
        operation_ticket="candidate",
    )

    wave.redis_release_ticket = None
    wave.kubernetes_ready_attestation_digest = None
    with pytest.raises(state.PTGWaveStateConflict, match="12-slot"):
        await state.has_started_redis_release(wave.wave_id, operation_ticket="candidate")

    wave.kubernetes_ready_attestation_digest = "7" * 64
    transition = AsyncMock()
    monkeypatch.setattr(state, "_transition", transition)
    assert await state.has_started_redis_release(wave.wave_id, operation_ticket="candidate")
    assert transition.await_args.args[2] == "redis_releasing"


@pytest.mark.asyncio
@pytest.mark.parametrize("has_claim", [False, True])
async def test_redis_release_first_write_chooses_claim_aware_state(
    monkeypatch, has_claim,
):
    wave = _wave(state="redis_releasing", redis_release_attestation_digest=None)
    session = _install_wave(
        monkeypatch,
        wave,
        _Session(_Result(scalar=0 if has_claim else None)),
    )
    transition = AsyncMock()
    monkeypatch.setattr(state, "_transition", transition)
    monkeypatch.setattr(state, "_validate_release_receipt", Mock(return_value={"exact": True}))
    digest = await state.record_redis_release(wave.wave_id, {"input": True})
    assert digest == state.sha256_digest(state.canonical_json({"exact": True}))
    assert transition.await_args.args[2] == ("executing" if has_claim else "released")
    assert session.results == []


def test_release_validator_wrapper_uses_state_conflict_contract(monkeypatch):
    validator = Mock(return_value={"exact": True})
    monkeypatch.setattr(state, "validate_release_receipt", validator)
    wave = _wave()
    assert state._validate_release_receipt(wave, {"input": True}) == {"exact": True}
    assert validator.call_args.kwargs["conflict_type"] is state.PTGWaveStateConflict
    assert validator.call_args.kwargs["digest_validator"] is state._digest


@pytest.mark.asyncio
async def test_redis_release_replay_conflict_and_wrong_state(monkeypatch):
    receipt_by_field = {"exact": True}
    digest = state.sha256_digest(state.canonical_json(receipt_by_field))
    wave = _wave(
        state="redis_releasing",
        redis_release_attestation_digest=digest,
    )
    _install_wave(monkeypatch, wave)
    monkeypatch.setattr(state, "_validate_release_receipt", Mock(return_value=receipt_by_field))
    assert await state.record_redis_release(wave.wave_id, receipt_by_field) == digest
    wave.redis_release_attestation_digest = "f" * 64
    with pytest.raises(state.PTGWaveStateConflict, match="conflicts"):
        await state.record_redis_release(wave.wave_id, receipt_by_field)
    wave.state = "released"
    with pytest.raises(state.PTGWaveStateConflict, match="not expected"):
        await state.record_redis_release(wave.wave_id, receipt_by_field)


@pytest.mark.asyncio
async def test_uncertainty_mark_and_resolution_are_state_bound(monkeypatch):
    wave = _wave(state="slots_waiting")
    _install_wave(monkeypatch, wave)
    with pytest.raises(state.PTGWaveStateConflict, match="changed before"):
        await state.mark_uncertain(wave.wave_id, expected_state="materializing")
    transition = AsyncMock()
    monkeypatch.setattr(state, "_transition", transition)
    await state.mark_uncertain(wave.wave_id, expected_state="slots_waiting")
    assert transition.await_args.args[2] == "uncertain"

    with pytest.raises(state.PTGWaveStateConflict, match="not uncertain"):
        await state.resolve_uncertainty(wave.wave_id, reconciled_state="slots_waiting")
    wave.state = "uncertain"
    await state.resolve_uncertainty(wave.wave_id, reconciled_state="slots_waiting")
    assert transition.await_args.args[2] == "slots_waiting"


@pytest.mark.asyncio
async def test_get_wave_receipts_is_read_only_and_handles_absence(monkeypatch):
    results = [
        _Result(scalar=None),
        _Result(scalar=_wave()),
        _Result(scalar=0),
    ]

    async def execute(_statement):
        return results.pop(0)

    monkeypatch.setattr(state.db, "execute", execute)
    assert await state.get_wave_receipts("missing") is None
    projection = Mock(return_value={"exact": True})
    monkeypatch.setattr(state, "wave_receipt_mapping", projection)
    assert await state.get_wave_receipts("wave-unit") == {"exact": True}
    projection.assert_called_once()
