"""Direct admission persistence and defensive payload branch contracts."""

from __future__ import annotations

import copy
import datetime as dt
import types
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_import_waves as waves
from tests.test_control_import_waves import _KEY, _payload


class _Result:
    def __init__(self, *, rows=(), scalar=None, rowcount=1):
        self.rows = list(rows)
        self._scalar = scalar
        self.rowcount = rowcount

    def scalars(self):
        return iter(self.rows)

    def all(self):
        return list(self.rows)

    def scalar_one_or_none(self):
        return self._scalar


class _Session:
    def __init__(self, *results):
        self.results = list(results)
        self.added = []
        self.flush_count = 0
        self.scalar_result = None

    async def execute(self, _statement, _parameters=None):
        assert self.results, "unexpected database execute"
        return self.results.pop(0)

    async def scalar(self, _statement, _parameters=None):
        return self.scalar_result

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
    monkeypatch.setattr(waves.db, "transaction", lambda: _Transaction(session))


def _request():
    return {
        "wave_id": "wave-unit",
        "idempotency_key": "wave-key",
        "request_digest": "1" * 64,
        "attestation": {"signed": True},
        "attestation_digest": "2" * 64,
        "signature_digest": "3" * 64,
        "wave_digest": "4" * 64,
        "release_queue": "arq:PTGSmall:wave:" + "4" * 64,
        "partition": {
            "physical_coordinate_count": 1,
            "physical_coordinate_digest": "5" * 64,
            "partition_digest": "6" * 64,
            "imported_coordinate_count": 1,
            "imported_coordinate_digest": "7" * 64,
            "reused_coordinate_count": 0,
            "reused_coordinate_digest": "8" * 64,
        },
        "intents": [
            {
                "run_id": "run-unit",
                "source_file_import_id": "source-unit",
                "content_version": "v1",
                "params": {"source_file_import_id": "source-unit"},
            },
        ],
    }


def _prepared():
    return {
        "ordinal": 0,
        "run_id": "run-unit",
        "source_id": "source-unit",
        "content_version": "v1",
        "job_id": "job-unit",
        "run_key": "run-key",
        "persisted_params": {"source_file_import_id": "source-unit"},
        "job_payload": {"run_id": "run-unit"},
        "serialized_job": b"job",
        "serialized_job_digest": "9" * 64,
        "run_values": {
            "run_id": "run-unit",
            "engine": "healthcare-mrf-api",
            "importer": "ptg",
            "family": "pricing",
            "status": "queued",
            "params": {},
            "idempotency_key": "run-key",
            "triggered_by": "api",
            "source_file_import_id": "source-unit",
        },
    }


def _wave_record(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "request_digest": "1" * 64,
        "cohort_attestation_digest": "2" * 64,
        "physical_coordinate_count": 1,
        "physical_coordinate_digest": "5" * 64,
        "imported_coordinate_count": 1,
        "imported_coordinate_digest": "7" * 64,
        "reused_coordinate_count": 0,
        "reused_coordinate_digest": "8" * 64,
        "partition_digest": "6" * 64,
        "intent_count": 1,
        "jobs_digest": "a" * 64,
        "manifest_digest": "b" * 64,
        "wave_digest": "4" * 64,
        "enqueue_time_ms": 1234,
        "state": "admitted",
        "state_version": 1,
        "queue": waves.QUEUE,
        "release_queue": "arq:PTGSmall:wave:" + "4" * 64,
        "worker_class": waves.WORKER_CLASS,
        "resource_class": waves.RESOURCE_CLASS,
        "worker_limit": waves.WORKER_LIMIT,
        "protocol_identity": waves.PROTOCOL_IDENTITY,
        "serializer_identity": waves.SERIALIZER_IDENTITY,
        "kubernetes_job_uid": None,
        "kubernetes_job_receipt_digest": None,
        "kubernetes_ready_attestation_digest": None,
        "redis_release_attestation_digest": None,
        "outcomes_digest": None,
        "linkage_ack_digest": None,
        "terminal_evidence_digest": None,
        "redis_cleanup_evidence_digest": None,
        "kubernetes_delete_evidence_digest": None,
        "cleanup_evidence_digest": None,
        "resolved_at": None,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def test_job_and_run_identifiers_are_stable_and_domain_separated():
    first_job = waves._job_id("wave", "1" * 64, 0, "run")
    assert first_job.startswith("ptg_start_")
    assert first_job != waves._job_id("wave", "1" * 64, 1, "run")
    assert waves._run_key("wave", "1" * 64, 0).startswith("ptg-wave:")
    assert waves._now().tzinfo is None


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"b": [2, 1], "a": {"z": True, "n": None}}, {"a": {"n": None, "z": True}, "b": [2, 1]}),
        (1.25, 1.25),
        ("text", "text"),
    ],
)
def test_canonical_job_payload_recurses_and_sorts(payload, expected):
    assert waves._canonical_job_payload(payload) == expected


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({1: "value"}, "keys must be strings"),
        (float("nan"), "non-finite"),
        (float("inf"), "non-finite"),
        (float("-inf"), "non-finite"),
        (object(), "unsupported value"),
    ],
)
def test_canonical_job_payload_rejects_ambiguous_values(payload, message):
    with pytest.raises(ValueError, match=message):
        waves._canonical_job_payload(payload)


def test_validated_intent_payload_requires_source_identity_after_normalization(monkeypatch):
    intent_by_field = {
        "source_file_import_id": "source-unit",
        "params": {"source_file_import_id": "source-unit"},
    }
    monkeypatch.setattr(
        waves.direct_wave,
        "normalized_wave_params",
        Mock(return_value=intent_by_field["params"]),
    )
    monkeypatch.setattr(waves, "_assert_ptg_rebuild_request_params", Mock())
    monkeypatch.setattr(
        waves,
        "validated_control_import_payload",
        Mock(return_value={"params": intent_by_field["params"], "source_file_import_id": "source-unit"}),
    )
    source = Mock(return_value="source-unit")
    monkeypatch.setattr(waves, "source_file_import_id_from_payload", source)
    assert waves._validated_intent_payload(
        intent_by_field,
        run_id="run-unit",
        run_key="run-key",
    )[0] == "source-unit"
    source.return_value = "other"
    with pytest.raises(ValueError, match="does not match"):
        waves._validated_intent_payload(intent_by_field, run_id="run-unit", run_key="run-key")


def test_prepare_intent_binds_persisted_and_enqueue_views(monkeypatch):
    intent_by_field = {
        "run_id": "run-unit",
        "source_file_import_id": "source-unit",
        "content_version": "v1",
        "params": {},
    }
    monkeypatch.setattr(
        waves,
        "_validated_intent_payload",
        Mock(return_value=("source-unit", {"params": {}, "triggered_by": "api"})),
    )
    monkeypatch.setattr(
        waves,
        "_import_param_views",
        Mock(return_value=types.SimpleNamespace(
            persisted_by_name={"persisted": True},
            enqueue_by_name={"enqueue": True},
        )),
    )
    monkeypatch.setattr(waves, "_normalize_triggered_by", Mock(return_value="api"))
    monkeypatch.setattr(waves, "arq_serialize_job", Mock(return_value=b"serialized"))
    prepared_intent_result = waves._prepare_intent(
        intent_by_field,
        wave_id="wave-unit",
        request_digest="1" * 64,
        wave_digest="2" * 64,
        release_queue="queue",
        ordinal=0,
        enqueue_time_ms=1234,
        now=dt.datetime(2026, 1, 1),
    )
    assert prepared_intent_result["persisted_params"]["persisted"] is True
    assert prepared_intent_result["job_payload"]["params"]["enqueue"] is True
    assert prepared_intent_result["serialized_job"] == b"serialized"
    assert len(prepared_intent_result["serialized_job_digest"]) == 64


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (None, "between 1"),
        ([], "between 1"),
        ([{}], "fields are not exact"),
        ([{"ordinal": 1, "run_id": "r", "source_file_import_id": "s", "content_version": "v", "params": {}}], "contiguous"),
        ([{"ordinal": 0, "run_id": "r", "source_file_import_id": "s", "content_version": "v", "params": []}], "params must be an object"),
    ],
)
def test_signed_intents_require_exact_shape_order_and_params(raw, message):
    with pytest.raises(ValueError, match=message):
        waves._validate_signed_intents(raw)


def test_signed_intents_require_unique_run_and_source_ids():
    base_by_field = {
        "ordinal": 0,
        "run_id": "run",
        "source_file_import_id": "source",
        "content_version": "v1",
        "params": {},
    }
    for duplicate_field in ("run_id", "source_file_import_id"):
        second_by_field = dict(base_by_field, ordinal=1, run_id="run-2", source_file_import_id="source-2")
        second_by_field[duplicate_field] = base_by_field[duplicate_field]
        with pytest.raises(ValueError, match="must be unique"):
            waves._validate_signed_intents([base_by_field, second_by_field])
    assert waves._validate_signed_intents([base_by_field])[0]["run_id"] == "run"


def _resign(payload):
    unsigned_by_field = {
        key: value
        for key, value in payload["cohort_attestation"].items()
        if key != "signature"
    }
    payload["cohort_attestation"]["signature"] = waves.sign_cohort_attestation(
        unsigned_by_field,
        key=_KEY,
    )


def test_wave_payload_requires_closed_envelope_count_and_digest():
    with pytest.raises(ValueError, match="only cohort_attestation"):
        waves.validate_import_wave_payload({}, attestation_key=_KEY)

    count = _payload()
    count["cohort_attestation"]["partition"]["imported_coordinate_count"] = 1
    count["cohort_attestation"]["partition"]["reused_coordinate_count"] = 1
    _resign(count)
    with pytest.raises(ValueError, match="must equal signed intent count"):
        waves.validate_import_wave_payload(count, attestation_key=_KEY)

    digest = _payload()
    digest["cohort_attestation"]["partition"]["imported_coordinate_digest"] = "0" * 64
    _resign(digest)
    with pytest.raises(ValueError, match="does not match"):
        waves.validate_import_wave_payload(digest, attestation_key=_KEY)

    validated = waves.validate_import_wave_payload(_payload(), attestation_key=_KEY)
    assert validated["wave_id"] == "wave-unit"
    assert validated["release_queue"].endswith(validated["wave_digest"])
    assert (
        validated["attestation"]["schema_version"]
        == "healthporta.ptg-import-wave-attestation.v2"
    )
    assert waves.PROTOCOL_IDENTITY == "healthporta.ptg-small.exact-wave.v1"


@pytest.mark.asyncio
async def test_session_executor_keeps_all_database_calls_in_one_transaction():
    session = _Session(
        _Result(rows=[("row",)]),
        _Result(rows=[("all",)]),
        _Result(rowcount=3),
    )
    session.scalar_result = 7
    executor = waves._SessionExecutor(session)
    assert (await executor.execute("statement", {"x": 1})).all() == [("row",)]
    assert await executor.scalar("scalar", {"a": 1}, b=2) == 7
    assert await executor.all("all", value=1) == [("all",)]
    assert await executor.status("status", value=1) == 3


def test_manifest_digests_and_prepare_wave_intents_are_deterministic(monkeypatch):
    prepared_values = [_prepared()]
    first = waves._manifest_digests(prepared_values, wave_digest="4" * 64, enqueue_time_ms=1234)
    assert all(len(value) == 64 for value in first)
    prepare = Mock(return_value=prepared_values[0])
    monkeypatch.setattr(waves, "_prepare_intent", prepare)
    result, jobs_digest, manifest_digest = waves._prepare_wave_intents(
        _request(),
        now=dt.datetime(2026, 1, 1),
        enqueue_time_ms=1234,
    )
    assert result == prepared_values
    assert (jobs_digest, manifest_digest) == first


def test_new_wave_record_and_response_bind_capacity_and_terminal_flags():
    request = waves.validate_import_wave_payload(
        _payload(count=1),
        attestation_key=_KEY,
    )
    wave = waves._new_wave_record(
        request,
        [_prepared()],
        jobs_digest="a" * 64,
        manifest_digest="b" * 64,
        enqueue_time_ms=1234,
        now=dt.datetime(2026, 1, 1),
    )
    assert wave.state == "admitted"
    assert wave.worker_limit == 12
    assert wave.cohort_attestation["schema_version"] == waves.ATTESTATION_VERSION
    assert wave.protocol_identity == "healthporta.ptg-small.exact-wave.v1"
    response = waves._wave_response(_wave_record())
    assert response["capacity_owning"] is True
    assert response["terminal"] is False
    terminal = waves._wave_response(_wave_record(state="succeeded"))
    assert terminal["capacity_owning"] is False
    assert terminal["terminal"] is True


@pytest.mark.asyncio
async def test_persist_wave_intents_adds_binding_run_and_immutable_intent(monkeypatch):
    session = _Session()
    executor = waves._SessionExecutor(session)
    wave = types.SimpleNamespace(wave_id="wave-unit")
    binding = AsyncMock()
    monkeypatch.setattr(waves, "insert_or_compare_frozen_binding", binding)
    await waves._persist_wave_intents(session, executor, wave, [_prepared()])
    assert session.added[0] is wave
    assert len(session.added) == 3
    binding.assert_awaited_once()


def _install_admission_dependencies(monkeypatch, session, *, prepared=None):
    request = _request()
    prepared = prepared or [_prepared()]
    wave = _wave_record()
    _install_transaction(monkeypatch, session)
    monkeypatch.setattr(waves, "validate_import_wave_payload", Mock(return_value=request))
    monkeypatch.setattr(waves, "_prepare_wave_intents", Mock(return_value=(prepared, "a" * 64, "b" * 64)))
    monkeypatch.setattr(waves, "require_source_attempt_capabilities", AsyncMock())
    monkeypatch.setattr(waves, "guard_source_attempt", AsyncMock())
    monkeypatch.setattr(waves, "acquire_ptg_admission_lock", AsyncMock())
    monkeypatch.setattr(waves, "require_wave_admission_capacity", AsyncMock())
    monkeypatch.setattr(waves, "_new_wave_record", Mock(return_value=wave))
    monkeypatch.setattr(waves, "_persist_wave_intents", AsyncMock())
    monkeypatch.setattr(waves, "record_source_attempt_event", AsyncMock())
    monkeypatch.setattr(waves, "_wave_response", Mock(side_effect=lambda value: {"wave_id": value.wave_id}))
    return request, wave


@pytest.mark.asyncio
async def test_admission_persists_new_wave_after_guard_and_capacity(monkeypatch):
    session = _Session(_Result(rows=[]))
    _request_map, wave = _install_admission_dependencies(monkeypatch, session)
    response, created = await waves.admit_import_wave({"signed": True})
    assert created is True
    assert response == {"wave_id": "wave-unit"}
    waves.guard_source_attempt.assert_awaited_once()
    waves.require_wave_admission_capacity.assert_awaited_once()
    waves._persist_wave_intents.assert_awaited_once()
    waves.record_source_attempt_event.assert_awaited_once()
    assert session.flush_count == 1
    assert waves._wave_response.call_args.args[0] is wave


@pytest.mark.asyncio
async def test_recovery_admission_revalidates_and_persists_supersession_first(
    monkeypatch,
):
    session = _Session(_Result(rows=[]))
    request, _wave = _install_admission_dependencies(monkeypatch, session)
    proof = {
        "predecessor": {"wave_id": "predecessor-wave"},
        "proof_digest": "d" * 64,
    }
    request["supersession"] = proof
    witness = types.SimpleNamespace(
        as_mapping=lambda: {"bound": True},
        evidence_mapping=lambda: {"bound": True},
        proof_digest="d" * 64,
    )
    attest = AsyncMock(return_value=witness)
    monkeypatch.setattr(
        waves,
        "attest_locked_logical_preclaim_supersession",
        attest,
    )

    _response, created = await waves.admit_import_wave(
        {"signed": True},
        redis="synthetic-redis",
    )

    assert created is True
    attest.assert_awaited_once_with(
        session,
        "predecessor-wave",
        "wave-unit",
        proof,
        redis="synthetic-redis",
    )
    supersession = session.added[0]
    assert isinstance(supersession, waves.PTGImportWaveSupersession)
    assert supersession.predecessor_wave_id == "predecessor-wave"
    assert supersession.successor_wave_id == "wave-unit"
    assert supersession.recovery_evidence == {"bound": True}
    assert supersession.recovery_evidence_canonical == b'{"bound":true}'
    assert supersession.recovery_evidence_sha256 == "d" * 64
    assert session.flush_count == 2
    waves.require_wave_admission_capacity.assert_awaited_once()


@pytest.mark.asyncio
async def test_admission_replay_and_conflicts_are_immutable(monkeypatch):
    existing = _wave_record()
    session = _Session(_Result(rows=[existing]))
    _install_admission_dependencies(monkeypatch, session)
    response, created = await waves.admit_import_wave({"signed": True})
    assert response == {"wave_id": "wave-unit"}
    assert created is False
    waves.require_wave_admission_capacity.assert_not_awaited()

    existing.request_digest = "f" * 64
    session.results = [_Result(rows=[existing])]
    with pytest.raises(waves.ImportWaveConflict, match="immutable request"):
        await waves.admit_import_wave({"signed": True})

    session.results = [_Result(rows=[existing, _wave_record(wave_id="other")])]
    with pytest.raises(waves.ImportWaveConflict, match="different immutable waves"):
        await waves.admit_import_wave({"signed": True})


@pytest.mark.asyncio
async def test_get_wave_handles_found_and_absent(monkeypatch):
    results = [_Result(scalar=None), _Result(scalar=_wave_record())]

    async def execute(_statement):
        return results.pop(0)

    monkeypatch.setattr(waves.db, "execute", execute)
    assert await waves.get_import_wave("wave-unit") is None
    assert (await waves.get_import_wave("wave-unit"))["wave_id"] == "wave-unit"
