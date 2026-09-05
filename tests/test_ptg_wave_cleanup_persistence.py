

"""Direct terminal-proof and cleanup persistence contracts."""


from __future__ import annotations


import copy


import types


from unittest.mock import AsyncMock, Mock


import pytest


from process import ptg_wave_cleanup as cleanup


_WAVE = "1" * 64


_MANIFEST = "2" * 64


_JOBS = "3" * 64


_IMAGE = "registry.example/engine@sha256:" + "4" * 64


class _Result:
    def __init__(self, *, rows=()):
        self._rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


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
    fields_by_field = {
        "wave_id": "wave-unit",
        "wave_digest": _WAVE,
        "state": "cleaning",
        "intent_count": 2,
        "release_queue": f"arq:PTGSmall:wave:{_WAVE}",
        "manifest_digest": _MANIFEST,
        "jobs_digest": _JOBS,
        "pinned_image_reference": _IMAGE,
        "redis_release_attestation": {"release_digest": "5" * 64},
        "outcomes_digest": "6" * 64,
        "linkage_ack_digest": "7" * 64,
        "failure_receipt": None,
        "failure_receipt_digest": None,
        "terminal_evidence_digest": "8" * 64,
        "terminal_summary": None,
        "redis_cleanup_ticket": None,
        "redis_cleanup_started_at": None,
        "redis_cleanup_evidence": None,
        "redis_cleanup_evidence_digest": None,
        "kubernetes_delete_ticket": None,
        "kubernetes_delete_started_at": None,
        "kubernetes_delete_evidence": None,
        "kubernetes_delete_evidence_digest": None,
        "kubernetes_job_uid": "job-uid",
        "kubernetes_job_receipt_digest": "9" * 64,
        "k8s_post_started_at": object(),
        "kubernetes_manifest_identity": "a" * 64,
        "kubernetes_manifest": {"metadata": {"name": "ptg-wave-unit"}},
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _install_wave(monkeypatch, wave, session=None):
    session = session or _Session()
    monkeypatch.setattr(cleanup.db, "transaction", lambda: _Transaction(session))
    monkeypatch.setattr(cleanup, "_locked_wave", AsyncMock(return_value=wave))
    return session


def _pre_cleanup(wave):
    unsigned_by_field = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "queue_name": wave.release_queue,
        "manifest_digest": wave.manifest_digest,
        "jobs_digest": wave.jobs_digest,
        "job_count": wave.intent_count,
        "image_identity": wave.pinned_image_reference,
        "release_digest": wave.redis_release_attestation["release_digest"],
        "target_key_count": 4 + 4 * wave.intent_count,
        "queue_entry_count": 0,
        "job_payload_count": 0,
        "result_count": wave.intent_count,
        "retry_count": 0,
        "in_progress_count": 0,
        "health_check_count": 1,
        "result_presence_digest": "b" * 64,
    }
    return {
        **unsigned_by_field,
        "attestation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }


def _post_cleanup(wave):
    unsigned_by_field = {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "absent_target_count": 4 + 4 * wave.intent_count,
    }
    return {
        **unsigned_by_field,
        "attestation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }


def _cleanup_operation(wave, pre):
    return {
        "schema_version": 1,
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "deleted_key_count": 3,
        "pre_cleanup_attestation_digest": pre["attestation_digest"],
        "pre_cleanup": pre,
    }


def _cleanup_evidence(wave, *, mode="executed"):
    pre = _pre_cleanup(wave)
    wave.terminal_summary = {"redis_pre_cleanup": pre}
    return {
        "schema_version": "healthporta.ptg-wave.redis-cleanup.v1",
        "operation_ticket": wave.redis_cleanup_ticket,
        "mode": mode,
        "pre_cleanup": pre,
        "operation_receipt": None if mode == "get_only_reconciled" else _cleanup_operation(wave, pre),
        "post_cleanup": _post_cleanup(wave),
    }


def _kubernetes_evidence(wave):
    unsigned_by_field = {
        "schema_version": "healthporta.ptg-wave.kubernetes-absence.v1",
        "operation_ticket": wave.kubernetes_delete_ticket,
        "wave_digest": wave.wave_digest,
        "job_name": wave.kubernetes_manifest["metadata"]["name"],
        "job_uid": wave.kubernetes_job_uid,
        "manifest_identity": wave.kubernetes_manifest_identity,
        "delete_permitted": wave.kubernetes_job_uid is not None,
        "job_absent": True,
        "pod_count": 0,
        "pods_absent": True,
    }
    return {
        **unsigned_by_field,
        "observation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }


@pytest.mark.asyncio
async def test_begin_terminalizing_requires_both_linkage_witnesses(monkeypatch):
    wave = _wave(state="awaiting_linkage", outcomes_digest=None)
    _install_wave(monkeypatch, wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="outcomes and linkage"):
        await cleanup.begin_terminalizing(wave.wave_id)
    wave.outcomes_digest = "6" * 64
    wave.linkage_ack_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="outcomes and linkage"):
        await cleanup.begin_terminalizing(wave.wave_id)
    wave.linkage_ack_digest = "7" * 64
    transition = AsyncMock()
    monkeypatch.setattr(cleanup, "_transition", transition)
    await cleanup.begin_terminalizing(wave.wave_id)
    assert transition.await_args.args[2] == "terminalizing"

@pytest.mark.asyncio
@pytest.mark.parametrize("verification_mode", ["normal", "unclaimed", "claimed"])
async def test_terminal_evidence_selects_the_exact_verifier(
    monkeypatch, verification_mode,
):
    wave = _wave(state="terminalizing")
    if verification_mode != "normal":
        wave.failure_receipt_digest = "f" * 64
    if verification_mode == "claimed":
        wave.failure_receipt = {"reason": "claimed_prestart_failure"}
    session = _install_wave(
        monkeypatch,
        wave,
        _Session(_Result(rows=["intent"]), _Result(rows=["claim"]), _Result(rows=["outcome"])),
    )
    monkeypatch.setattr(
        cleanup,
        "is_claimed_prestart_failure_receipt",
        Mock(return_value=verification_mode == "claimed"),
    )
    verifiers_by_field = {
        "normal": "verify_terminal_eligibility",
        "unclaimed": "verify_unclaimed_dead_letter_terminal_eligibility",
        "claimed": "verify_claimed_prestart_dead_letter_terminal_eligibility",
    }
    verifier = Mock(return_value={"verified": verification_mode})
    monkeypatch.setattr(cleanup, verifiers_by_field[verification_mode], verifier)
    transition = AsyncMock()
    monkeypatch.setattr(cleanup, "_transition", transition)

    digest = await cleanup.persist_terminal_evidence(wave.wave_id, {"external": True})
    assert digest == cleanup.sha256_digest(
        cleanup.canonical_json({"verified": verification_mode})
    )
    verifier.assert_called_once_with(
        wave,
        ["intent"],
        ["claim"],
        ["outcome"],
        {"external": True},
    )
    assert transition.await_args.args[2] == "cleaning"
    assert session.results == []

@pytest.mark.asyncio
async def test_terminal_evidence_rejects_wrong_state_and_missing_linkage(monkeypatch):
    wave = _wave(state="cleaning")
    _install_wave(monkeypatch, wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.persist_terminal_evidence(wave.wave_id, {})
    wave.state = "terminalizing"
    wave.outcomes_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="lacks stable"):
        await cleanup.persist_terminal_evidence(wave.wave_id, {})

@pytest.mark.asyncio
async def test_redis_cleanup_marker_is_one_shot(monkeypatch):
    wave = _wave(redis_cleanup_ticket="existing")
    session = _install_wave(monkeypatch, wave)
    assert await cleanup.mark_redis_cleanup_started(
        wave.wave_id,
        operation_ticket="candidate",
    ) == {"owner": False}

    wave.redis_cleanup_ticket = None
    wave.state = "terminalizing"
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.mark_redis_cleanup_started(wave.wave_id, operation_ticket="candidate")
    wave.state = "cleaning"
    wave.terminal_evidence_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.mark_redis_cleanup_started(wave.wave_id, operation_ticket="candidate")
    wave.terminal_evidence_digest = "8" * 64
    wave.redis_cleanup_started_at = object()
    with pytest.raises(cleanup.PTGWaveStateConflict, match="GET only"):
        await cleanup.mark_redis_cleanup_started(wave.wave_id, operation_ticket="candidate")

    wave.redis_cleanup_started_at = None
    receipt = await cleanup.mark_redis_cleanup_started(
        wave.wave_id,
        operation_ticket="candidate",
    )
    assert receipt["owner"] is True
    assert receipt["release_digest"] == "5" * 64
    assert session.flush_count == 1

@pytest.mark.asyncio
async def test_redis_cleanup_evidence_first_write_replay_and_conflict(monkeypatch):
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        await cleanup.record_redis_cleanup_absent("wave-unit", [])

    wave = _wave(redis_cleanup_ticket="ticket", redis_cleanup_started_at=object())
    session = _install_wave(monkeypatch, wave)
    evidence = _cleanup_evidence(wave)
    digest = await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence)
    assert wave.redis_cleanup_evidence_digest == digest
    assert session.flush_count == 1
    assert await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence) == digest
    wave.redis_cleanup_evidence_digest = "f" * 64
    with pytest.raises(cleanup.PTGWaveStateConflict, match="conflicts"):
        await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence)
    wave.state = "terminalizing"
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.record_redis_cleanup_absent(wave.wave_id, evidence)

@pytest.mark.asyncio
async def test_kubernetes_delete_marker_replays_and_requires_redis_absence(monkeypatch):
    wave = _wave(kubernetes_delete_ticket="existing")
    _install_wave(monkeypatch, wave)
    assert await cleanup.mark_kubernetes_delete_started(
        wave.wave_id,
        operation_ticket="candidate",
    ) == {"owner": False}

    wave.kubernetes_delete_ticket = None
    wave.redis_cleanup_evidence_digest = None
    with pytest.raises(cleanup.PTGWaveStateConflict, match="Redis absence"):
        await cleanup.mark_kubernetes_delete_started(wave.wave_id, operation_ticket="candidate")
    wave.redis_cleanup_evidence_digest = "c" * 64
    wave.kubernetes_delete_started_at = object()
    with pytest.raises(cleanup.PTGWaveStateConflict, match="GET only"):
        await cleanup.mark_kubernetes_delete_started(wave.wave_id, operation_ticket="candidate")


@pytest.mark.asyncio
async def test_kubernetes_delete_marker_starts_normal_deletion(monkeypatch):
    wave = _wave(redis_cleanup_evidence_digest="c" * 64)
    session = _install_wave(monkeypatch, wave)

    wave.kubernetes_delete_started_at = None
    receipt = await cleanup.mark_kubernetes_delete_started(
        wave.wave_id,
        operation_ticket="candidate",
    )
    assert receipt["delete_permitted"] is True
    assert session.flush_count == 1


@pytest.mark.asyncio
async def test_kubernetes_delete_marker_allows_never_created_job(monkeypatch):
    never_created = _wave(
        kubernetes_job_uid=None,
        kubernetes_job_receipt_digest=None,
        redis_cleanup_evidence_digest="c" * 64,
    )
    _install_wave(monkeypatch, never_created)
    receipt = await cleanup.mark_kubernetes_delete_started(
        never_created.wave_id,
        operation_ticket="never-created",
    )
    assert receipt["delete_permitted"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides_by_field",
    (
        {"k8s_post_started_at": None},
        {"kubernetes_job_receipt_digest": "9" * 64},
    ),
)
async def test_kubernetes_delete_marker_rejects_never_created_job_with_post_state(
    monkeypatch,
    overrides_by_field,
):
    invalid_wave = _wave(
        kubernetes_job_uid=None,
        redis_cleanup_evidence_digest="c" * 64,
        **overrides_by_field,
    )
    _install_wave(monkeypatch, invalid_wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="persisted POST"):
        await cleanup.mark_kubernetes_delete_started(
            invalid_wave.wave_id,
            operation_ticket="never-created",
        )


@pytest.mark.asyncio
async def test_kubernetes_delete_marker_allows_early_failure(monkeypatch):
    early = _wave(
        state="terminalizing",
        failure_receipt={"reason": "redis_release_absent"},
        linkage_ack_digest="7" * 64,
    )
    _install_wave(monkeypatch, early)
    assert (await cleanup.mark_kubernetes_delete_started(
        early.wave_id,
        operation_ticket="early",
    ))["owner"] is True

@pytest.mark.asyncio
async def test_kubernetes_absence_first_write_replay_and_early_failure(monkeypatch):
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        await cleanup.record_kubernetes_delete_absent("wave-unit", [])

    wave = _wave(kubernetes_delete_ticket="ticket", kubernetes_delete_started_at=object())
    session = _install_wave(monkeypatch, wave)
    evidence = _kubernetes_evidence(wave)
    digest = await cleanup.record_kubernetes_delete_absent(wave.wave_id, evidence)
    assert wave.kubernetes_delete_evidence_digest == digest
    assert session.flush_count == 1
    assert await cleanup.record_kubernetes_delete_absent(wave.wave_id, evidence) == digest
    wave.kubernetes_delete_evidence_digest = "f" * 64
    with pytest.raises(cleanup.PTGWaveStateConflict, match="conflicts"):
        await cleanup.record_kubernetes_delete_absent(wave.wave_id, evidence)

    wrong = _wave(state="terminalizing", kubernetes_delete_started_at=None)
    _install_wave(monkeypatch, wrong)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not expected"):
        await cleanup.record_kubernetes_delete_absent(wrong.wave_id, evidence)

    early = _wave(
        state="terminalizing",
        failure_receipt={"reason": "redis_release_absent"},
        kubernetes_delete_ticket="ticket",
        kubernetes_delete_started_at=object(),
    )
    _install_wave(monkeypatch, early)
    early_evidence = _kubernetes_evidence(early)
    assert await cleanup.record_kubernetes_delete_absent(
        early.wave_id,
        early_evidence,
    )

@pytest.mark.asyncio
async def test_final_cleanup_requires_all_exact_receipts_and_derives_state(monkeypatch):
    wave = _wave(state="terminalizing")
    _install_wave(monkeypatch, wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="before terminal evidence"):
        await cleanup.persist_cleanup_and_terminal(wave.wave_id)

    wave.state = "cleaning"
    wave.redis_cleanup_started_at = object()
    with pytest.raises(cleanup.PTGWaveStateConflict, match="markers"):
        await cleanup.persist_cleanup_and_terminal(wave.wave_id)

    wave.redis_cleanup_ticket = "redis-ticket"
    wave.redis_cleanup_evidence = _cleanup_evidence(wave)
    wave.redis_cleanup_evidence_digest = cleanup.sha256_digest(
        cleanup.canonical_json(wave.redis_cleanup_evidence)
    )
    wave.kubernetes_delete_ticket = "delete-ticket"
    wave.kubernetes_delete_started_at = object()
    wave.kubernetes_delete_evidence = _kubernetes_evidence(wave)
    wave.kubernetes_delete_evidence_digest = "corrupt"
    with pytest.raises(cleanup.PTGWaveStateConflict, match="digest is corrupt"):
        await cleanup.persist_cleanup_and_terminal(wave.wave_id)

    wave.kubernetes_delete_evidence_digest = cleanup.sha256_digest(
        cleanup.canonical_json(wave.kubernetes_delete_evidence)
    )
    session = _install_wave(monkeypatch, wave, _Session(_Result(rows=["outcome"])))
    monkeypatch.setattr(cleanup, "_derive_terminal_state", Mock(return_value="succeeded"))
    transition = AsyncMock()
    monkeypatch.setattr(cleanup, "_transition", transition)
    digest = await cleanup.persist_cleanup_and_terminal(wave.wave_id)
    assert len(digest) == 64
    assert transition.await_args.args[2] == "succeeded"
    assert transition.await_args.kwargs["values"]["cleanup_summary"]["terminal_state"] == "succeeded"
    assert session.results == []

def test_normal_cleanup_validators_accept_executed_and_get_only_receipts():
    wave = _wave(redis_cleanup_ticket="ticket")
    executed = _cleanup_evidence(wave)
    assert cleanup._validate_redis_cleanup_evidence(wave, executed) == executed
    get_only = _cleanup_evidence(wave, mode="get_only_reconciled")
    assert cleanup._validate_redis_cleanup_evidence(wave, get_only) == get_only
    invented = copy.deepcopy(get_only)
    invented["operation_receipt"] = {}
    with pytest.raises(cleanup.PTGWaveStateConflict, match="cannot invent"):
        cleanup._validate_redis_cleanup_evidence(wave, invented)

def test_failure_cleanup_dispatches_to_failure_specific_validators(monkeypatch):
    wave = _wave(
        redis_cleanup_ticket="ticket",
        failure_receipt={"reason": "unclaimed"},
        failure_receipt_digest="f" * 64,
    )
    pre_by_field = {"attestation_digest": "b" * 64}
    wave.terminal_summary = {"redis_pre_cleanup": pre_by_field}
    evidence_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-cleanup.v1",
        "operation_ticket": "ticket",
        "mode": "executed",
        "pre_cleanup": pre_by_field,
        "operation_receipt": {"operation": True},
        "post_cleanup": {"post": True},
    }
    verify = Mock()
    validate_post = Mock()
    validate_operation = Mock()
    monkeypatch.setattr(cleanup, "_verify_failure_redis", verify)
    monkeypatch.setattr(cleanup, "_validate_unclaimed_redis_post_cleanup", validate_post)
    monkeypatch.setattr(cleanup, "_validate_unclaimed_redis_cleanup_operation", validate_operation)
    assert cleanup._validate_redis_cleanup_evidence(wave, evidence_by_field) == evidence_by_field
    verify.assert_called_once()
    validate_post.assert_called_once()
    validate_operation.assert_called_once()

@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda evidence: [], "must be an object"),
        (lambda evidence: {**evidence, "mode": "retry"}, "one-shot operation"),
        (lambda evidence: {**evidence, "pre_cleanup": {}}, "lost its terminal"),
    ],
)
def test_cleanup_envelope_rejects_shape_operation_and_witness_drift(mutate, message):
    wave = _wave(redis_cleanup_ticket="ticket")
    evidence = _cleanup_evidence(wave)
    with pytest.raises(cleanup.PTGWaveStateConflict, match=message):
        cleanup._validate_redis_cleanup_evidence(wave, mutate(evidence))

@pytest.mark.parametrize(
    ("builder", "validator", "message"),
    [
        (_post_cleanup, cleanup._validate_redis_post_cleanup_evidence, "cleanup evidence"),
        (_pre_cleanup, cleanup._validate_redis_pre_cleanup_evidence, "pre-cleanup evidence"),
    ],
)
def test_normal_redis_attestation_validators_reject_nonobjects_and_drift(
    builder, validator, message,
):
    wave = _wave()
    valid = builder(wave)
    assert validator(wave, valid) == valid
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        validator(wave, [])
    invalid = copy.deepcopy(valid)
    invalid["attestation_digest"] = "f" * 64
    with pytest.raises(cleanup.PTGWaveStateConflict, match=message):
        validator(wave, invalid)

@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("result_count", True),
        ("result_count", -1),
        ("result_count", 3),
        ("health_check_count", 2),
        ("queue_entry_count", 1),
    ],
)
def test_pre_cleanup_attestation_rejects_nonidle_counts(field, value):
    wave = _wave()
    evidence = _pre_cleanup(wave)
    evidence[field] = value
    with pytest.raises(cleanup.PTGWaveStateConflict, match="idleness"):
        cleanup._validate_redis_pre_cleanup_evidence(wave, evidence)

def test_cleanup_operation_validators_cover_normal_and_unclaimed_shapes():
    wave = _wave()
    pre = _pre_cleanup(wave)
    normal = _cleanup_operation(wave, pre)
    assert cleanup._validate_redis_cleanup_operation(wave, normal, pre) == normal
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_redis_cleanup_operation(wave, [], pre)
    invalid_by_field = dict(normal, deleted_key_count=True)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not exact"):
        cleanup._validate_redis_cleanup_operation(wave, invalid_by_field, pre)

    unclaimed_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-cleanup.v1",
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "deleted_key_count": 1,
        "expected_attestation_digest": pre["attestation_digest"],
        "attestation": pre,
    }
    assert cleanup._validate_unclaimed_redis_cleanup_operation(
        wave,
        unclaimed_by_field,
        pre,
    ) == unclaimed_by_field
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_unclaimed_redis_cleanup_operation(wave, [], pre)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="not exact"):
        cleanup._validate_unclaimed_redis_cleanup_operation(
            wave,
            dict(unclaimed_by_field, deleted_key_count=-1),
            pre,
        )

def test_unclaimed_post_cleanup_and_digest_validation():
    wave = _wave()
    pre = _pre_cleanup(wave)
    unsigned_by_field = {
        "schema_version": "healthporta.ptg-wave.redis-unclaimed-post-cleanup.v1",
        "wave_id": wave.wave_digest,
        "manifest_digest": wave.manifest_digest,
        "target_key_count": 4 + 4 * wave.intent_count,
        "absent_target_count": 4 + 4 * wave.intent_count,
        "expected_attestation_digest": pre["attestation_digest"],
    }
    post_by_field = {
        **unsigned_by_field,
        "attestation_digest": cleanup.sha256_digest(cleanup.canonical_json(unsigned_by_field)),
    }
    assert cleanup._validate_unclaimed_redis_post_cleanup(wave, post_by_field, pre) == post_by_field
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_unclaimed_redis_post_cleanup(wave, [], pre)
    with pytest.raises(cleanup.PTGWaveStateConflict, match="did not prove"):
        cleanup._validate_unclaimed_redis_post_cleanup(
            wave,
            dict(post_by_field, absent_target_count=0),
            pre,
        )

    assert cleanup._digest_like("a" * 64, "digest") == "a" * 64
    for invalid in (None, "a" * 63, "g" * 64):
        with pytest.raises(cleanup.PTGWaveStateConflict, match="SHA-256"):
            cleanup._digest_like(invalid, "digest")

def test_kubernetes_absence_validator_binds_job_and_never_created_job():
    wave = _wave(kubernetes_delete_ticket="ticket")
    evidence = _kubernetes_evidence(wave)
    assert cleanup._validate_kubernetes_absence_evidence(wave, evidence) == evidence
    with pytest.raises(cleanup.PTGWaveStateConflict, match="must be an object"):
        cleanup._validate_kubernetes_absence_evidence(wave, [])
    with pytest.raises(cleanup.PTGWaveStateConflict, match="does not prove"):
        cleanup._validate_kubernetes_absence_evidence(
            wave,
            dict(evidence, pod_count=1),
        )

    never_created = _wave(
        kubernetes_delete_ticket="ticket",
        kubernetes_job_uid=None,
        kubernetes_job_receipt_digest=None,
    )
    never_created_evidence = _kubernetes_evidence(never_created)
    assert cleanup._validate_kubernetes_absence_evidence(
        never_created,
        never_created_evidence,
    ) == never_created_evidence
