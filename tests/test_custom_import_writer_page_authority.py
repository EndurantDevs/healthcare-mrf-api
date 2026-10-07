# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real shared page authority hooks and committed HTTP receipt boundaries."""

import json
from dataclasses import asdict, replace
from datetime import timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import DBAPIError

from api import control_admission_batch as service
from process.custom_import import admission_sql as admission
from process.custom_import import build_source as staging
from process.custom_import.source_authorization import SourcePermit
from tests.test_custom_import_admission_service import NOW, ORIGIN, TOKEN, Session, _wire
from tests.test_custom_import_build_source import _request as existing_request


def _request(**changes):
    return replace(
        existing_request(),
        **(
            {
                "dataset_id": 11,
                "definition_revision_id": 12,
                "schema_revision_id": 13,
                "execution_id": 40,
                "fence": 7,
                "lease_token": TOKEN,
                "build_deadline_at": NOW + timedelta(hours=1),
            }
            | changes
        ),
    )


def _page(monkeypatch, *, final_now=NOW, commit_error=None):
    session = Session(commit_error=commit_error)
    build = SimpleNamespace(build_id=51, page_row_limit=2)
    monkeypatch.setattr(staging, "_lock_page", AsyncMock(return_value=build))
    monkeypatch.setattr(staging, "_flush_page", AsyncMock())
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(staging, "lock_execution", AsyncMock())
    monkeypatch.setattr(staging, "lock_lease", AsyncMock())

    async def verify(*args):
        session.events.append("final_authority")
        return final_now

    monkeypatch.setattr(staging, "verify_live_attempt", verify)
    return session, build


@pytest.mark.parametrize(
    "expiry", [NOW.replace(tzinfo=None), True, "2030-01-01T00:15:00Z", NOW.astimezone(timezone(timedelta(hours=1)))]
)
def test_optional_authorization_expiry_requires_utc(expiry):
    with pytest.raises(ValueError, match="UTC aware"):
        _request(authorization_expires_at=expiry)


def test_authorization_expiry_never_changes_build_identity_or_request_equality():
    original = _request()
    bounded = replace(original, authorization_expires_at=NOW + timedelta(minutes=15))
    assert original == bounded
    assert bounded.build_deadline_at == original.build_deadline_at
    identity_by_name = {
        name: getattr(original, name)
        for name in (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "complete_scope",
            "page_row_limit",
            "page_byte_limit",
            "statement_timeout_ms",
            "build_deadline_at",
        )
    }
    build = SimpleNamespace(
        **identity_by_name,
        capture_bundle_id=60,
        request_identity_sha256=b"r" * 32,
        producing_fence=7,
        producing_token_sha256=staging.lease_token_sha256(TOKEN),
        base_generation_id=None,
        base_pointer_version=0,
        refresh_mode=original.definition.refresh_mode,
    )
    execution = SimpleNamespace(capture_bundle_id=60, request_identity_sha256=b"r" * 32)
    staging._assert_build_identity(build, bounded, execution)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization,build_seconds,expected",
    [
        (None, 3600, 1800),
        (600, 3600, 600),
        (2400, 900, 900),
    ],
)
async def test_initial_and_renewed_windows_use_earliest_deadline(monkeypatch, authorization, build_seconds, expected):
    expiry = None if authorization is None else NOW + timedelta(seconds=authorization)
    request = _request(authorization_expires_at=expiry, build_deadline_at=NOW + timedelta(seconds=build_seconds))
    initial = Session([(NOW + timedelta(seconds=1800), NOW, "read committed")])
    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())
    monkeypatch.setattr(staging.time, "monotonic", lambda: 100.0)
    await staging._initial_page_window(initial, request)
    assert initial.info[staging._WINDOW].deadline == 100 + expected
    renewed = Session([NOW])
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(
        staging,
        "heartbeat_execution",
        AsyncMock(
            return_value=SimpleNamespace(
                state="running",
                expires_at=NOW + timedelta(seconds=1800),
            )
        ),
    )
    await staging._renew_page_window(renewed, request)
    assert renewed.info[staging._WINDOW].deadline == 100 + expected
    assert request.authorization_expires_at == expiry


@pytest.mark.asyncio
async def test_initial_expiry_excludes_lock_acquisition_and_candidate_work(monkeypatch):
    session = Session([(NOW + timedelta(hours=1), NOW, "read committed")])
    request = _request(authorization_expires_at=NOW)
    monkeypatch.setattr(staging, "_set_timeout", AsyncMock())
    monkeypatch.setattr(staging.time, "monotonic", lambda: 100.0)
    await staging._initial_page_window(session, request)
    with pytest.raises(staging.LeaseAuthorityLost, match="deadline elapsed"):
        await staging._prepare_statement(session)
    assert len(session.statements) == 1 and not session.pending


@pytest.mark.asyncio
@pytest.mark.parametrize("offset,commits", [(-1, True), (0, False), (1, False)])
async def test_fresh_database_clock_exclusively_gates_commit(monkeypatch, offset, commits):
    deadline = NOW + timedelta(minutes=15)
    session, _ = _page(monkeypatch, final_now=deadline + timedelta(microseconds=offset))
    request = _request(authorization_expires_at=deadline)

    async def page():
        async with staging._page_session(lambda: session, request, 51):
            session.pending.append("candidate-and-cursor")

    if commits:
        await page()
    else:
        with pytest.raises(staging.LeaseAuthorityLost, match="authorization expired"):
            await page()
    assert session.durable == (["candidate-and-cursor"] if commits else [])
    assert session.events[-3:] == ["final_authority", "commit" if commits else "rollback", "close"]


@pytest.mark.asyncio
async def test_none_preserves_existing_caller_commit(monkeypatch):
    session, _ = _page(monkeypatch)
    async with staging._page_session(lambda: session, _request(), 51):
        session.pending.append("legacy")
    assert session.durable == ["legacy"]


@pytest.mark.asyncio
async def test_fixed_locked_query_binds_every_permit_identity_and_owner(monkeypatch):
    _, verified, _ = _wire()
    session = Session([40])
    monkeypatch.setattr(admission, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(admission, "_has_admission_owner", AsyncMock(return_value=True))
    await admission._verify_admission_permit(
        session,
        _request(authorization_expires_at=verified.permit.expires_at),
        verified.permit,
    )
    compiled = session.statements[0].compile(dialect=dialect())
    assert set(compiled.params.values()) >= {11, 12, 13, 14, 40, "synthetic-run", b"\xaa" * 32}
    query = str(compiled)
    for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "source_binding_revision_id"):
        assert f"custom_import_source_binding_revision.{name} =" in query
    assert "custom_import_execution.idempotency_key =" in query
    admission._has_admission_owner.assert_awaited_once_with(session)


@pytest.mark.asyncio
@pytest.mark.parametrize("matched,owner", [(None, True), (99, True), (40, False)])
async def test_missing_locked_binding_or_actual_owner_fails_before_decision(monkeypatch, matched, owner):
    _, verified, _ = _wire()
    session, _ = _page(monkeypatch)
    session.values = [matched]
    monkeypatch.setattr(admission, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(admission, "_has_admission_owner", AsyncMock(return_value=owner))
    decision = AsyncMock()
    monkeypatch.setattr(admission, "_admit_locked", decision)
    with pytest.raises(admission.AdmissionError):
        await admission.admit_source_batch(
            lambda: session,
            _request(authorization_expires_at=verified.permit.expires_at),
            51,
            0,
            admission_permit=verified.permit,
        )
    decision.assert_not_awaited()
    assert "rollback" in session.events and not session.durable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"dataset_id": 99},
        {"definition_revision_id": 99},
        {"schema_revision_id": 99},
        {"authorization_expires_at": None},
        {"authorization_expires_at": NOW + timedelta(hours=1)},
    ],
)
async def test_request_cannot_substitute_scope_or_extend_signed_expiry(monkeypatch, changes):
    _, verified, _ = _wire()
    session = Session()
    request = _request(**{"authorization_expires_at": verified.permit.expires_at, **changes})
    with pytest.raises(admission.AdmissionError):
        await admission._verify_admission_permit(session, request, verified.permit)
    assert session.statements == []


@pytest.mark.asyncio
async def test_timeout_fallback_rechecks_same_permit_owner_request_and_cursor(monkeypatch):
    _, verified, _ = _wire(cursor=9)
    session, _ = _page(monkeypatch)
    request = _request(authorization_expires_at=verified.permit.expires_at)
    original = SimpleNamespace(sqlstate="57014")
    timeout = DBAPIError(None, None, original)
    timeout._custom_import_admission_cursor = 9
    gate = AsyncMock()
    monkeypatch.setattr(admission, "_verify_admission_permit", gate)
    decision = AsyncMock(side_effect=[timeout, admission.AdmissionResult("admission", 10, 1, 0)])
    monkeypatch.setattr(admission, "_admit_locked", decision)
    result = await admission.admit_source_batch(lambda: session, request, 51, 9, admission_permit=verified.permit)
    assert result.after_occurrence_id == 10
    assert [call.args for call in gate.await_args_list] == [(session, request, verified.permit)] * 2
    assert [call.args[2] for call in decision.await_args_list] == [9, 9]
    assert decision.await_args_list[1].kwargs == {"physical_row_cap": 2}
    assert session.events.count("rollback") == 1 and session.events.count("commit") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("matches,owners", [([40, 40], [True, False]), ([40, None], [True])])
async def test_fallback_rejects_lost_owner_or_changed_retained_binding(monkeypatch, matches, owners):
    _, verified, _ = _wire()
    session, _ = _page(monkeypatch)
    session.values = matches
    error = DBAPIError(None, None, SimpleNamespace(sqlstate="57014"))
    error._custom_import_admission_cursor = 0
    monkeypatch.setattr(admission, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(admission, "_has_admission_owner", AsyncMock(side_effect=owners))
    decision = AsyncMock(side_effect=error)
    monkeypatch.setattr(admission, "_admit_locked", decision)
    with pytest.raises(admission.AdmissionError):
        await admission.admit_source_batch(
            lambda: session,
            _request(authorization_expires_at=verified.permit.expires_at),
            51,
            0,
            admission_permit=verified.permit,
        )
    decision.assert_awaited_once()
    assert not session.durable and session.events.count("rollback") == 2


@pytest.mark.asyncio
async def test_timeout_fallback_cannot_commit_after_original_authorization_expiry(monkeypatch):
    _, verified, _ = _wire()
    session, _ = _page(monkeypatch, final_now=verified.permit.expires_at)
    error = DBAPIError(None, None, SimpleNamespace(sqlstate="57014"))
    error._custom_import_admission_cursor = 0
    monkeypatch.setattr(admission, "_verify_admission_permit", AsyncMock())

    async def decide(*args, **kwargs):
        if "rollback" not in session.events:
            raise error
        session.pending.append("candidate-and-cursor")
        return admission.AdmissionResult("graph", 1, 1, 0)

    monkeypatch.setattr(admission, "_admit_locked", decide)
    with pytest.raises(staging.LeaseAuthorityLost):
        await admission.admit_source_batch(
            lambda: session,
            _request(authorization_expires_at=verified.permit.expires_at),
            51,
            0,
            admission_permit=verified.permit,
        )
    assert not session.durable and session.events.count("rollback") == 2


@pytest.mark.asyncio
async def test_receipt_uses_only_committed_result_without_a_post_commit_read(monkeypatch):
    wire, verified, ring = _wire()
    monkeypatch.setattr(
        service,
        "_retained_request",
        AsyncMock(return_value=_request(authorization_expires_at=verified.permit.expires_at)),
    )
    session, _ = _page(monkeypatch)
    monkeypatch.setattr(admission, "_verify_admission_permit", AsyncMock())

    async def decide(*args):
        session.pending.append("batch-80")
        return admission.AdmissionResult("admission", 80, 80, 0)

    monkeypatch.setattr(admission, "_admit_locked", decide)
    reply = await service.serve_admission_batch(
        wire,
        lambda: session,
        keyring=ring,
        expected_origin=ORIGIN,
        trusted_now=NOW,
    )
    assert reply.status == 200 and session.durable == ["batch-80"]
    assert json.loads(reply.body)["after_occurrence_id"] == 80
    assert session.events[-1] == "close" and session.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "commit_error",
    [
        RuntimeError("DO-NOT-LOG commit acknowledgement lost"),
        DBAPIError(None, None, SimpleNamespace(sqlstate="08006"), connection_invalidated=True),
        DBAPIError(None, None, SimpleNamespace(sqlstate="57014")),
    ],
)
async def test_uncertain_commit_is_unavailable_without_retry_or_rollback_claim(monkeypatch, caplog, commit_error):
    wire, verified, ring = _wire()
    monkeypatch.setattr(
        service,
        "_retained_request",
        AsyncMock(return_value=_request(authorization_expires_at=verified.permit.expires_at)),
    )
    session, _ = _page(monkeypatch, commit_error=commit_error)
    monkeypatch.setattr(admission, "_verify_admission_permit", AsyncMock())

    async def decide(*args):
        session.pending.append("possibly-committed")
        return admission.AdmissionResult("graph", 1, 1, 0)

    decision = AsyncMock(side_effect=decide)
    monkeypatch.setattr(admission, "_admit_locked", decision)
    reply = await service.serve_admission_batch(
        wire,
        lambda: session,
        keyring=ring,
        expected_origin=ORIGIN,
        trusted_now=NOW,
    )
    assert reply.status == 503 and json.loads(reply.body) == {"error": "admission_unavailable"}
    assert reply.headers["Cache-Control"] == "no-store" and session.durable == ["possibly-committed"]
    decision.assert_awaited_once()
    assert b"DO-NOT-LOG" not in reply.body and caplog.records == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        None,
        {"contract": "custom-import-source-permit/v1"},
        {"path": "/control/v1/custom-import/source-batch"},
        {"method": "GET"},
        {"issuer": "other"},
        {"audience": "other"},
    ],
)
async def test_wrong_purpose_permit_fails_before_query_even_with_admission_type(monkeypatch, changes):
    _, verified, _ = _wire()
    session = Session()
    owner = AsyncMock()
    prepare = AsyncMock()
    monkeypatch.setattr(admission, "_has_admission_owner", owner)
    monkeypatch.setattr(admission, "_prepare_statement", prepare)
    impostor = SourcePermit(**asdict(verified.permit)) if changes is None else replace(verified.permit, **changes)
    with pytest.raises(admission.AdmissionError):
        await admission._verify_admission_permit(
            session, _request(authorization_expires_at=verified.permit.expires_at), impostor
        )
    assert session.statements == []
    owner.assert_not_awaited()
    prepare.assert_not_awaited()
