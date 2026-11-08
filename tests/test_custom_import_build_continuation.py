# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained continuation cannot rewrite provenance or reuse a consumed launch."""

import datetime as dt
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models.custom_import import CustomImportDefinitionRevision, CustomImportGenerationSeal
from process.custom_import import build_continuation as continuation
from process.custom_import.execution import IdempotencyConflict
from tests.test_custom_import_build_source import _request


class _Result:
    def __init__(self, value):
        self.value = value

    def one_or_none(self):
        return self.value


class _Session:
    def __init__(self, execution, build, snapshot, capture, revision):
        self.execution, self.build, self.snapshot = execution, build, snapshot
        self.capture, self.revision = capture, revision
        self.row = None
        self.added = []

    async def scalars(self, statement):
        name = statement.get_final_froms()[0].name
        return _Result(
            {
                "custom_import_execution": self.execution,
                "custom_import_build_attempt": self.build,
                "custom_import_build_continuation": self.row,
            }[name]
        )

    async def get(self, model, _identity, **_kwargs):
        return {
            "custom_import_snapshot_family": self.snapshot,
            "custom_import_capture_bundle": self.capture,
            "custom_import_definition_revision": self.revision,
            "custom_import_generation_seal": None,
            "custom_import_build_continuation": self.row,
        }.get(model.__tablename__)

    def add(self, row):
        self.added.append(row)
        self.row = row

    async def flush(self):
        if self.row is not None:
            self.row.continuation_id = 41


def _execution():
    return SimpleNamespace(
        dataset_id=7,
        definition_revision_id=11,
        schema_revision_id=13,
        execution_id=17,
        capture_bundle_id=19,
        source_binding_revision_id=23,
        idempotency_key="synthetic-run",
        request_identity_sha256=b"r" * 32,
        state="running",
    )


def _retained_rows(request, now, producer):
    """Create one synthetic admitted graph and its unchanged snapshot identity."""

    execution = _execution()
    build = SimpleNamespace(
        **{
            key: getattr(execution, key)
            for key in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "capture_bundle_id",
                "request_identity_sha256",
            )
        },
        build_id=29,
        producing_fence=3,
        producing_token_sha256=producer,
        base_generation_id=None,
        base_pointer_version=0,
        source_frozen_at=now - dt.timedelta(hours=2),
        plan_stage="complete",
        plan_complete_at=now - dt.timedelta(hours=1),
        candidate_error_count=0,
        phase="graph",
        generation_id=None,
        build_deadline_at=request.build_deadline_at,
        complete_scope=True,
        selected_family_count=10,
        completed_family_count=6,
        page_row_limit=request.page_row_limit,
        page_byte_limit=request.page_byte_limit,
        statement_timeout_ms=request.statement_timeout_ms,
    )
    snapshot = SimpleNamespace(
        **{
            key: getattr(build, key)
            for key in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "capture_bundle_id",
                "producing_fence",
                "producing_token_sha256",
                "generation_id",
            )
        },
        family_id=31,
        frozen_at=None,
    )
    return execution, build, snapshot


def _test_authorization(producer, nonce, now):
    return {
        "contract": continuation.CONTRACT,
        "authorization_id": "a" * 64,
        "engine_execution_id": "17",
        "build_id": 29,
        "snapshot_family_id": 31,
        "capture_bundle_id": 19,
        "producer_fence": 3,
        "producer_token_sha256": producer.hex(),
        "request_identity_sha256": (b"r" * 32).hex(),
        "expected_authority_id": None,
        "base_generation_id": None,
        "base_pointer_version": 0,
        "worker_identity_sha256": "b" * 64,
        "predecessor_identity_sha256": "c" * 64,
        "nonce_sha256": hashlib.sha256(nonce).hexdigest(),
        "processing_deadline_removed": True,
        "launch_expires_at": (now + dt.timedelta(minutes=10)).isoformat(),
    }


@pytest.fixture
def retained(monkeypatch):
    request = _request()
    now = request.build_deadline_at + dt.timedelta(days=1)
    nonce = b"n" * 32
    producer = continuation.lease_token_sha256(request.lease_token)
    execution, build, snapshot = _retained_rows(request, now, producer)
    lease = SimpleNamespace(
        execution_id=17,
        fence=3,
        token_sha256=producer,
        continuation_id=None,
        continuation_started_at=None,
        expires_at=now - dt.timedelta(seconds=1),
    )
    capture = SimpleNamespace(capture_state="sealed", sealed_at=now - dt.timedelta(days=1), producing_execution_id=17)
    revision = CustomImportDefinitionRevision(
        dataset_id=7,
        schema_revision_id=13,
        canonical_definition=request.definition.canonical,
        definition_sha256=bytes.fromhex(request.definition.digest),
    )
    session = _Session(execution, build, snapshot, capture, revision)
    monkeypatch.setattr(continuation, "lock_dataset", AsyncMock())
    monkeypatch.setattr(continuation, "lock_lease", AsyncMock(return_value=lease))
    monkeypatch.setattr(continuation, "database_now", AsyncMock(return_value=now))
    monkeypatch.setattr(continuation, "load_current_pointer", AsyncMock(return_value=None))
    authorization = _test_authorization(producer, nonce, now)
    pins_by_field = {
        key: getattr(execution, key)
        for key in (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "source_binding_revision_id",
            "idempotency_key",
            "request_identity_sha256",
        )
    }
    return SimpleNamespace(
        request=request,
        now=now,
        nonce=nonce,
        execution=execution,
        build=build,
        snapshot=snapshot,
        lease=lease,
        capture=capture,
        revision=revision,
        session=session,
        authorization=authorization,
        pins=pins_by_field,
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("extra", 1),
        ("build_id", True),
        ("engine_execution_id", "017"),
        ("engine_execution_id", str(2**63)),
        ("nonce_sha256", "A" * 64),
        ("base_pointer_version", 1),
        ("expected_authority_id", False),
        ("processing_deadline_removed", 1),
        ("launch_expires_at", "2026-01-01T00:00:00+01:00"),
    ],
)
def test_closed_authorization_rejects_ambiguous_or_foreign_inputs(retained, field, value):
    document_by_field = dict(retained.authorization, **{field: value})
    with pytest.raises(ValueError):
        continuation.parse_build_continuation_authorization(document_by_field)


async def test_authorize_fences_legacy_nonce_without_altering_build_or_starting_lease(retained):
    retained = retained
    before = vars(retained.build).copy()
    receipt = await continuation.authorize_build_continuation(retained.session, retained.authorization, **retained.pins)
    assert receipt["continuation_id"] == 41 and receipt["processing_deadline_removed"] is True
    assert retained.lease.fence == retained.build.producing_fence == 3
    assert (
        retained.lease.token_sha256 == hashlib.sha256(retained.nonce).digest() != retained.build.producing_token_sha256
    )
    assert retained.lease.continuation_started_at is None and retained.lease.expires_at < retained.now
    assert vars(retained.build) == before
    assert retained.session.row.original_deadline_at == retained.build.build_deadline_at
    assert (
        receipt["authorization_sha256"]
        == hashlib.sha256(continuation.authorization_bytes(retained.authorization)).hexdigest()
    )
    assert (
        await continuation.authorize_build_continuation(retained.session, retained.authorization, **retained.pins)
        == receipt
    )
    assert len(retained.session.added) == 1


@pytest.mark.parametrize(
    "target,field,value",
    [
        ("execution", "state", "canceling"),
        ("execution", "dataset_id", 8),
        ("build", "phase", "admission"),
        ("build", "plan_stage", "source"),
        ("build", "candidate_error_count", 1),
        ("snapshot", "producing_token_sha256", b"x" * 32),
        ("lease", "token_sha256", b"x" * 32),
        ("capture", "capture_state", "pending"),
    ],
)
async def test_transfer_refuses_wrong_or_unsafe_retained_state(retained, target, field, value):
    setattr(getattr(retained, target), field, value)
    with pytest.raises(IdempotencyConflict):
        await continuation.authorize_build_continuation(retained.session, retained.authorization, **retained.pins)
    assert not retained.session.added


async def test_live_old_lease_cannot_be_stolen(retained):
    retained.lease.expires_at = retained.now + dt.timedelta(seconds=1)
    with pytest.raises(IdempotencyConflict):
        await continuation.authorize_build_continuation(retained.session, retained.authorization, **retained.pins)


async def test_start_uses_retained_bounds_and_original_producer_but_fresh_nonce(retained):
    retained = retained
    await continuation.authorize_build_continuation(retained.session, retained.authorization, **retained.pins)
    request = await continuation.start_build_continuation(
        retained.session, retained.authorization, retained.nonce, lease_seconds=3600
    )
    assert (
        request.execution_id == 17
        and request.continuation.build_id == 29
        and request.continuation.continuation_id == 41
    )
    assert request.lease_token == retained.nonce and request.fence == 3
    assert request.continuation.producer_token_sha256 == retained.build.producing_token_sha256
    assert request.build_deadline_at == retained.build.build_deadline_at < retained.now
    assert (
        retained.lease.continuation_started_at == retained.now
        and retained.lease.expires_at == retained.now + dt.timedelta(hours=1)
    )
    with pytest.raises(IdempotencyConflict):
        await continuation.start_build_continuation(
            retained.session, retained.authorization, retained.nonce, lease_seconds=3600
        )


@pytest.mark.parametrize("kind", ["wrong_nonce", "expired_launch", "expired_started", "stale_epoch", "canceling"])
async def test_start_refuses_replay_expiry_and_lost_authority(retained, kind):
    retained = retained
    await continuation.authorize_build_continuation(retained.session, retained.authorization, **retained.pins)
    nonce = retained.nonce
    if kind == "wrong_nonce":
        nonce = b"x" * 32
    elif kind == "expired_launch":
        retained.session.row.launch_expires_at = retained.now
    elif kind == "expired_started":
        retained.lease.continuation_started_at = retained.now - dt.timedelta(hours=1)
    elif kind == "stale_epoch":
        retained.lease.continuation_id = 42
    else:
        retained.execution.state = "canceling"
    with pytest.raises(IdempotencyConflict):
        await continuation.start_build_continuation(retained.session, retained.authorization, nonce, lease_seconds=3600)


def test_canonical_digest_has_no_nonce_or_runtime_secret_material(retained):
    encoded = continuation.authorization_bytes(retained.authorization)
    assert encoded == json.dumps(retained.authorization, sort_keys=True, separators=(",", ":")).encode()
    assert retained.nonce not in encoded
    assert str(retained.nonce) not in repr(
        continuation._authorization_row(retained.authorization, retained.execution, retained.build)
    )


def test_sealed_status_is_explicit_not_inferred_from_execution_state(retained):
    retained = retained
    status = continuation._status(
        retained.execution, retained.lease, retained.build, retained.snapshot, None, is_sealed=False
    )
    assert status["current"]["sealed"] is False and status["current"]["processing_deadline_removed"] is False
    assert status["retained"]["producer_token_sha256"] == retained.authorization["producer_token_sha256"]
    assert "nonce_sha256" not in json.dumps(status)
    assert status["retained"]["original_deadline_at"].endswith("Z")
    assert status["current"]["lease_expires_at"].endswith("Z")
