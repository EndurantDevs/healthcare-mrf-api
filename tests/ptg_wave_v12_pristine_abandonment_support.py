"""Synthetic boundary support for pristine V12 abandonment tests."""

from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace

from api.control_import_waves import (
    _new_wave_record,
    _prepare_wave_intents,
    validate_import_wave_payload,
)
from api.ptg_wave_kubernetes import build_ptg_wave_job
from process.ptg_wave_materialized_preclaim_supersession import (
    PTGWaveMaterializedPreclaimObservation,
)
from process.ptg_wave_receipt_authority import (
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    PTGWaveReceiptKeyring,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_REQUEST_SCHEMA,
    admission_receipt_mapping,
    ordinary_cutover_id,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v12_pristine_abandonment import (
    attest_v12_pristine_materialized_abandonment,
)
from tests.ptg_wave_receipt_test_keys import (
    EPHEMERAL_RECEIPT_PRIVATE_KEY,
    EPHEMERAL_RECEIPT_PUBLIC_MODULUS,
)
from tests.control_import_waves_test_support import (
    KEY as _KEY,
    v6_payload as _v6_payload,
)
from tests.test_ptg_wave_preclaim_supersession import (
    _BARRIER_FACTORY,
    _IMAGE,
    _RUNTIME_IMAGE,
    _actual_job,
    _empty_redis_attestation,
)


FIXED_KEY = EPHEMERAL_RECEIPT_PRIVATE_KEY


class Result:
    """Minimal scalar result for abandonment persistence tests."""

    def __init__(self, value=None):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class Session:
    """Minimal receipt persistence session."""

    def __init__(self, existing=None):
        self.existing = existing
        self.added = []
        self.flush_count = 0

    async def execute(self, _statement):
        return Result(self.existing)

    def add(self, row):
        self.added.append(row)

    async def flush(self):
        self.flush_count += 1


class Transaction:
    """Async context adapter for one synthetic session."""

    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def keyring(monkeypatch) -> PTGWaveReceiptKeyring:
    """Load the ephemeral signer for abandonment tests."""
    monkeypatch.setenv(ACTIVE_KEY_ID_ENV, "receipt-active")
    monkeypatch.setenv(ACTIVE_PRIVATE_KEY_FILE_ENV, str(FIXED_KEY.resolve()))
    return PTGWaveReceiptKeyring.from_environment()


def boundary():
    """Build one exact pristine V6 abandonment boundary."""
    wave, intents, runs, now = _prepared_v6_boundary()
    manifest, manifest_bytes, receipt_field_mapping = _abandonment_manifest(wave)
    _apply_abandonment_boundary(
        wave,
        now,
        manifest,
        manifest_bytes,
        receipt_field_mapping,
    )
    return wave, intents, runs, admission_receipt_mapping(wave, intents)


def _prepared_v6_boundary():
    request = validate_import_wave_payload(
        _v6_payload(modulus=EPHEMERAL_RECEIPT_PUBLIC_MODULUS),
        attestation_key=_KEY,
    )
    now = dt.datetime(2026, 8, 10, 12, 0, 0)
    enqueue_time_ms = int(now.replace(tzinfo=dt.UTC).timestamp() * 1000)
    prepared, jobs_digest, manifest_digest = _prepare_wave_intents(
        request,
        now=now,
        enqueue_time_ms=enqueue_time_ms,
    )
    wave = _new_wave_record(
        request,
        prepared,
        jobs_digest=jobs_digest,
        manifest_digest=manifest_digest,
        enqueue_time_ms=enqueue_time_ms,
        now=now,
    )
    intents = tuple(
        SimpleNamespace(
            wave_id=wave.wave_id,
            ordinal=prepared_intent["ordinal"],
            run_id=prepared_intent["run_id"],
            source_file_import_id=prepared_intent["source_id"],
            content_version=prepared_intent["content_version"],
            run_idempotency_key=prepared_intent["run_key"],
            job_id=prepared_intent["job_id"],
            params=prepared_intent["persisted_params"],
            job_payload=prepared_intent["job_payload"],
            serialized_job=prepared_intent["serialized_job"],
            serialized_job_digest=prepared_intent["serialized_job_digest"],
        )
        for prepared_intent in prepared
    )
    runs = tuple(
        SimpleNamespace(
            **prepared_intent["run_values"],
            started_at=None,
            finished_at=None,
        )
        for prepared_intent in prepared
    )
    return wave, intents, runs, now


def _abandonment_manifest(wave):
    manifest = build_ptg_wave_job(
        wave_digest=wave.wave_digest,
        manifest_digest=wave.manifest_digest,
        jobs_digest=wave.jobs_digest,
        job_count=wave.intent_count,
        image=_IMAGE,
        runtime_image_identity=_RUNTIME_IMAGE,
        barrier_factory=_BARRIER_FACTORY,
    )
    manifest_bytes = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    annotations = manifest["metadata"]["annotations"]
    receipt_field_mapping = {
        "wave_digest": wave.wave_digest,
        "job_uid": "synthetic-job-uid",
        "manifest_identity": annotations[
            "healthporta.com/ptg-wave-manifest-identity"
        ],
        "config_identity": annotations[
            "healthporta.com/ptg-wave-config-identity"
        ],
        "pinned_image_reference": _IMAGE,
        "pinned_image_digest": "d" * 64,
        "runtime_image_identity": _RUNTIME_IMAGE,
    }
    return manifest, manifest_bytes, receipt_field_mapping


def _apply_abandonment_boundary(
    wave,
    now,
    manifest,
    manifest_bytes,
    receipt_field_mapping,
) -> None:
    wave.state = "slots_waiting"
    wave.uncertainty_resume_state = None
    wave.k8s_post_ticket = "post:v12-synthetic"
    wave.k8s_post_started_at = now
    wave.kubernetes_job_uid = receipt_field_mapping["job_uid"]
    wave.kubernetes_job_receipt = receipt_field_mapping
    wave.kubernetes_job_receipt_digest = sha256_digest(
        canonical_json(receipt_field_mapping)
    )
    wave.kubernetes_manifest = manifest
    wave.kubernetes_manifest_bytes = manifest_bytes
    wave.kubernetes_manifest_sha256 = sha256_digest(manifest_bytes)
    wave.kubernetes_manifest_identity = receipt_field_mapping[
        "manifest_identity"
    ]
    wave.kubernetes_config_identity = receipt_field_mapping[
        "config_identity"
    ]
    wave.pinned_image_reference = _IMAGE
    wave.pinned_image_digest = "d" * 64
    wave.runtime_image_identity = _RUNTIME_IMAGE


def proof(**changes):
    """Build the exact pristine proof with optional negative-state changes."""
    wave, intents, runs, admission = boundary()
    observation = PTGWaveMaterializedPreclaimObservation(
        predecessor_wave=wave,
        intents=intents,
        runs=runs,
        claims=changes.get("claims", ()),
        outcomes=changes.get("outcomes", ()),
        worker_start_event_ordinals=changes.get("worker_events", ()),
        logical_supersession=changes.get("logical"),
        admission_rollback=changes.get("rollback"),
        actual_job=changes.get("actual_job", _actual_job(wave.kubernetes_manifest)),
        redis_unclaimed_attestation=changes.get(
            "redis", _empty_redis_attestation(wave)
        ),
    )
    cutover_id = ordinary_cutover_id(wave.wave_id)
    return (
        attest_v12_pristine_materialized_abandonment(
            observation,
            cutover_id=cutover_id,
            admission=admission,
        ),
        admission,
    )


def request(admission: dict) -> dict:
    """Build one exact V12 abandonment request."""
    operation_id = admission["wave_id"]
    return {
        "schema": ABANDONMENT_REQUEST_SCHEMA,
        "key_id": admission["receipt_key_id"],
        "operation_id": operation_id,
        "cutover_id": ordinary_cutover_id(operation_id),
        "admission": admission,
    }
