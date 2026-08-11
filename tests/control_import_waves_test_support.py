"""Shared synthetic contracts for control import-wave tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import types
from pathlib import Path

from api import control_import_waves as waves
from api.control_import_wave_attestation import (
    ATTESTATION_VERSION,
    AUTHORIZATION_BASIS,
    LEGACY_ATTESTATION_VERSION,
    RECEIPT_ATTESTATION_VERSION,
)
from tests.ptg_wave_supersession_fixtures import recovery_proofs


KEY = "test-control-key"


def _attestation_snapshot(schema_version: str) -> dict:
    snapshot_by_field = {
        "snapshot_digest": "a" * 64,
        "membership_digest": "b" * 64,
        "inventory_digest": "c" * 64,
        "subscription_coverage_digest": "d" * 64,
        "entitlement_coverage_count": 2,
        "entitlement_coverage_digest": "8" * 64,
        "catalog_generation": "9" * 64,
    }
    if schema_version != LEGACY_ATTESTATION_VERSION:
        snapshot_by_field.update(
            authorization_basis=AUTHORIZATION_BASIS,
            authorization_digest="7" * 64,
        )
    return snapshot_by_field


def _attestation_partition(count: int) -> dict:
    imported_digest = hashlib.sha256(
        "\0".join(
            f"coordinate-unit-{ordinal}\0v1" for ordinal in range(count)
        ).encode("utf-8")
    ).hexdigest()
    return {
        "complete": True,
        "physical_coordinate_count": count,
        "physical_coordinate_digest": "e" * 64,
        "imported_coordinate_count": count,
        "imported_coordinate_digest": imported_digest,
        "reused_coordinate_count": 0,
        "reused_coordinate_digest": "0" * 64,
        "partition_digest": "f" * 64,
    }


def unsigned_attestation(
    count: int = 2,
    *,
    schema_version: str = ATTESTATION_VERSION,
) -> dict:
    """Build one unsigned exact-wave attestation."""
    unsigned_attestation_map = {
        "schema_version": schema_version,
        "wave_id": "wave-unit",
        "idempotency_key": "wave-unit-key",
        "snapshot": _attestation_snapshot(schema_version),
        "partition": _attestation_partition(count),
        "intents": [
            {
                "ordinal": ordinal,
                "run_id": f"run-unit-{ordinal}",
                "source_file_import_id": f"coordinate-unit-{ordinal}",
                "content_version": "v1",
                "params": {
                    "source_file_import_id": f"coordinate-unit-{ordinal}",
                    "import_id": f"coordinate-unit-{ordinal}",
                },
            }
            for ordinal in range(count)
        ],
    }
    unsigned_attestation_map.update(
        recovery_proofs(
            schema_version=schema_version,
            successor_wave_id=unsigned_attestation_map["wave_id"],
            intent_count=count,
        )
    )
    return unsigned_attestation_map


def signed_payload(
    count: int = 2,
    *,
    schema_version: str = ATTESTATION_VERSION,
) -> dict:
    """Sign one exact-wave attestation with the synthetic control key."""
    unsigned = unsigned_attestation(count, schema_version=schema_version)
    return {
        "cohort_attestation": {
            **unsigned,
            "signature": waves.sign_cohort_attestation(unsigned, key=KEY),
        }
    }


def receipt_test_modulus() -> str:
    """Read the public-only shared receipt fixture modulus."""
    fixture_path = Path(__file__).parent / "fixtures" / (
        "ptg_wave_receipts_v2.json"
    )
    return json.loads(fixture_path.read_text(encoding="utf-8"))["key_epoch"][
        "rsa_modulus"
    ]


def v6_payload(
    *,
    key_id="receipt-active",
    modulus: str | None = None,
    exponent: int = 65537,
) -> dict:
    """Build one signed V6 admission payload."""
    wave_id = "6" * 64
    unsigned = unsigned_attestation(
        2,
        schema_version=RECEIPT_ATTESTATION_VERSION,
    )
    unsigned["wave_id"] = wave_id
    unsigned["idempotency_key"] = wave_id
    unsigned["receipt_key_id"] = key_id
    unsigned["receipt_public_modulus_hex"] = modulus or receipt_test_modulus()
    unsigned["receipt_public_exponent"] = exponent
    return {
        "cohort_attestation": {
            **unsigned,
            "signature": waves.sign_cohort_attestation(unsigned, key=KEY),
        }
    }


class QueryResult:
    """Minimal persistence result adapter."""

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


class Session:
    """Queue persistence results and writes."""

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


class Transaction:
    """Async context adapter for one synthetic persistence session."""

    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def install_transaction(monkeypatch, session):
    """Install one synthetic persistence transaction."""
    monkeypatch.setattr(waves.db, "transaction", lambda: Transaction(session))


def request():
    """Build a normalized admission request."""
    return {
        "wave_id": "wave-unit",
        "idempotency_key": "wave-key",
        "request_digest": "1" * 64,
        "attestation": {"signed": True},
        "attestation_digest": "2" * 64,
        "signature_digest": "3" * 64,
        "receipt_key_id": None,
        "receipt_public_modulus_hex": None,
        "receipt_public_exponent": None,
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


def prepared_intent():
    """Build one normalized persisted intent."""
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


def wave_record(**overrides):
    """Build one persisted wave record."""
    fields_by_field = {
        "wave_id": "wave-unit",
        "request_digest": "1" * 64,
        "cohort_attestation": {"signed": True},
        "cohort_attestation_digest": "2" * 64,
        "cohort_signature_digest": "3" * 64,
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
        "receipt_key_id": None,
        "receipt_public_modulus_hex": None,
        "receipt_public_exponent": None,
        "linkage_receipt": None,
        "linkage_receipt_payload_digest": None,
        "terminal_evidence_digest": None,
        "redis_cleanup_evidence_digest": None,
        "kubernetes_delete_evidence_digest": None,
        "cleanup_evidence_digest": None,
        "resolved_at": None,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def installed_v6_replay_session(monkeypatch):
    """Install one persisted V6 operation and its replay transaction."""
    wave_request_by_field = v6_payload()
    normalized_request = waves.validate_import_wave_payload(
        wave_request_by_field,
        attestation_key=KEY,
    )
    now = dt.datetime(2026, 8, 10, 12, 0, 0)
    enqueue_time_ms = int(now.replace(tzinfo=dt.UTC).timestamp() * 1000)
    prepared, jobs_digest, manifest_digest = waves._prepare_wave_intents(
        normalized_request,
        now=now,
        enqueue_time_ms=enqueue_time_ms,
    )
    existing = waves._new_wave_record(
        normalized_request,
        prepared,
        jobs_digest=jobs_digest,
        manifest_digest=manifest_digest,
        enqueue_time_ms=enqueue_time_ms,
        now=now,
    )
    session = Session(QueryResult(rows=[existing]))
    install_transaction(monkeypatch, session)
    return wave_request_by_field, existing, session
