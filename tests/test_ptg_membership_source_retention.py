# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Commit source admission before long builds and recover only unattached pins."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg_membership_storage as storage
from api import ptg_provider_membership_build as build
from process.ptg_parts.ptg2_lifecycle_lock import PTG2LifecycleLockDeferred
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg_membership_projection import PARAMS


def _worker_database(monkeypatch, *, lost_ack=False):
    events, sessions = [], []
    engine = object()

    @asynccontextmanager
    async def factory():
        index = len(sessions)

        @asynccontextmanager
        async def begin():
            events.append((index, "begin"))
            try:
                yield
            except BaseException:
                events.append((index, "rollback"))
                raise
            else:
                events.append((index, "commit"))
                if lost_ack and index == 1:
                    raise RuntimeError("commit acknowledgement lost")

        session = SimpleNamespace(begin=begin, get_bind=lambda: SimpleNamespace(engine=engine))
        sessions.append(session)
        yield session

    monkeypatch.setattr(build.db, "session_factory", factory)
    monkeypatch.setattr(build.db, "engine", SimpleNamespace(sync_engine=engine))
    monkeypatch.setattr(build.db, "has_reader_session", lambda: False)
    return events, sessions


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("build failed"), asyncio.CancelledError()])
async def test_real_transactions_commit_retention_before_build_and_reconcile_after_rollback(monkeypatch, failure):
    events, sessions = _worker_database(monkeypatch)
    prepare = AsyncMock(return_value=PARAMS)

    async def candidate(session, **kwargs):
        assert events[:3] == [(0, "begin"), (0, "commit"), (1, "begin")]
        assert session is sessions[1]
        assert kwargs == PARAMS | {"retained_identity": PARAMS}
        if failure is not None:
            raise failure
        return {"state": "ready"}

    async def release(session, identity):
        assert session is sessions[2]
        assert identity == PARAMS
        assert events[3:5] == [(1, "rollback"), (2, "begin")]

    monkeypatch.setattr(build, "prepare_source_retention", prepare)
    monkeypatch.setattr(build, "build_in_session", candidate)
    monkeypatch.setattr(build, "release_unattached_source_retention", release)
    if failure is None:
        assert await build.build_provider_membership_projection(**PARAMS) == {"state": "ready"}
        assert events[-1] == (1, "commit")
    else:
        with pytest.raises(type(failure)) as captured:
            await build.build_provider_membership_projection(**PARAMS)
        assert captured.value is failure
        assert events[-1] == (2, "commit")
    prepare.assert_awaited_once_with(sessions[0], **PARAMS)


@pytest.mark.asyncio
async def test_unknown_publication_commit_keeps_ready_pin_and_original_error(monkeypatch):
    events, sessions = _worker_database(monkeypatch, lost_ack=True)
    monkeypatch.setattr(build, "prepare_source_retention", AsyncMock(return_value=PARAMS))
    monkeypatch.setattr(build, "build_in_session", AsyncMock(return_value={"state": "ready"}))
    release = AsyncMock(return_value=False)
    monkeypatch.setattr(build, "release_unattached_source_retention", release)
    with pytest.raises(RuntimeError, match="acknowledgement lost"):
        await build.build_provider_membership_projection(**PARAMS)
    release.assert_awaited_once_with(sessions[2], PARAMS)
    assert events[-1] == (2, "commit")


@pytest.mark.asyncio
async def test_cancellation_drains_cleanup_and_preserves_exact_recovery_receipt_on_busy(monkeypatch):
    _worker_database(monkeypatch)
    entered, finish = asyncio.Event(), asyncio.Event()
    original = asyncio.CancelledError()
    monkeypatch.setattr(build, "prepare_source_retention", AsyncMock(return_value=PARAMS))
    monkeypatch.setattr(build, "build_in_session", AsyncMock(side_effect=original))

    async def cleanup(_identity):
        entered.set()
        await finish.wait()
        raise PTG2LifecycleLockDeferred("busy")

    monkeypatch.setattr(build, "_release_failed_retention", cleanup)
    worker = asyncio.create_task(build.build_provider_membership_projection(**PARAMS))
    await entered.wait()
    worker.cancel()
    await asyncio.sleep(0)
    assert not worker.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError) as captured:
        await worker
    assert captured.value is original
    assert any(storage.digest(PARAMS) in note for note in original.__notes__)
    assert any(build.encoded(PARAMS) in note for note in original.__notes__)


@pytest.mark.asyncio
async def test_reader_context_cannot_open_independent_publisher(monkeypatch):
    factory = Mock()
    monkeypatch.setattr(build.db, "has_reader_session", lambda: True)
    monkeypatch.setattr(build.db, "session_factory", factory)
    with pytest.raises(RuntimeError, match="Reader context"):
        await build.build_provider_membership_projection(**PARAMS)
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_borrowed_transaction_cannot_be_replaced_with_independent_publisher(monkeypatch):
    factory = Mock()
    monkeypatch.setattr(build.db, "has_reader_session", lambda: False)
    monkeypatch.setattr(build.db, "_transaction_binding", lambda: object())
    monkeypatch.setattr(build.db, "session_factory", factory)
    with pytest.raises(RuntimeError, match="independent transactions"):
        await build.build_provider_membership_projection(**PARAMS)
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_independent_factory_must_use_exact_configured_engine(monkeypatch):
    _worker_database(monkeypatch)
    monkeypatch.setattr(build.db, "engine", SimpleNamespace(sync_engine=object()))
    with pytest.raises(RuntimeError, match="database binding changed"):
        await build.build_provider_membership_projection(**PARAMS)


def _retention_context(monkeypatch, *, rows=(), current=None):
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=True))
    monkeypatch.setattr(storage.native, "protected_publisher_owner", AsyncMock())
    fence = AsyncMock(return_value=PARAMS["source_key"])
    monkeypatch.setattr(storage, "acquire_snapshot_reference_lifetime", fence)
    monkeypatch.setattr(storage, "source_identity", AsyncMock(return_value=(object(), object(), current or PARAMS)))
    pins = AsyncMock(return_value=rows)
    monkeypatch.setattr(storage, "_source_pin_rows", pins)
    return session, pins, fence


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
async def test_prepare_authenticates_source_and_reuses_exact_admission_without_waiting_for_builder(
    monkeypatch, existing
):
    exact_rows = [(PARAMS["snapshot_id"], storage._SOURCE_PIN_REASON)]
    session, rows, fence = _retention_context(monkeypatch, rows=exact_rows if existing else [])
    if not existing:
        rows.side_effect = [[], exact_rows]
    assert await storage.prepare_source_retention(session, **PARAMS) == PARAMS
    fence.assert_awaited_once()
    storage.source_identity.assert_awaited_once_with(session, **PARAMS)
    assert session.scalar.await_count == int(not existing)
    if not existing:
        assert "pg_try_advisory_xact_lock" in str(session.scalar.call_args.args[0])
        assert "INSERT INTO" in str(session.execute.call_args.args[0])
    else:
        session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["source", "reason", "extra_pin"])
async def test_prepare_refuses_drift_without_modifying_pin_metadata(monkeypatch, drift):
    rows = [(PARAMS["snapshot_id"], storage._SOURCE_PIN_REASON)]
    if drift == "reason":
        rows[0] = (PARAMS["snapshot_id"], "other owner metadata")
    elif drift == "extra_pin":
        rows.append(("other-snapshot", storage._SOURCE_PIN_REASON))
    session, _, fence = _retention_context(monkeypatch, rows=rows)
    if drift == "source":
        fence.return_value = "other-source"
    with pytest.raises(PTG2ManifestArtifactError, match="differs"):
        await storage.prepare_source_retention(session, **PARAMS)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["orphan", "absent", "attached", "busy", "drift", "metadata"])
async def test_orphan_reconciliation_never_deletes_active_attached_or_different_pins(monkeypatch, state):
    exact_rows = [(PARAMS["snapshot_id"], storage._SOURCE_PIN_REASON)]
    rows = [] if state == "absent" else exact_rows
    if state == "metadata":
        rows = [(PARAMS["snapshot_id"], "different metadata")]
    session, pins, _ = _retention_context(
        monkeypatch, rows=rows, current=PARAMS | {"code": "99214"} if state == "drift" else None
    )
    monkeypatch.setattr(storage, "locate", AsyncMock(return_value=object() if state == "attached" else None))
    if state == "busy":
        session.scalar.return_value = False
    if state in {"busy", "drift", "metadata"}:
        expected = PTG2LifecycleLockDeferred if state == "busy" else PTG2ManifestArtifactError
        with pytest.raises(expected):
            await storage.release_unattached_source_retention(session, PARAMS)
    else:
        assert await storage.release_unattached_source_retention(session, PARAMS) == int(state == "orphan")
    if state == "orphan":
        sql, parameters = session.execute.call_args.args
        assert "DELETE FROM" in str(sql)
        assert parameters == storage._pin(PARAMS) | {"reason": storage._SOURCE_PIN_REASON}
    else:
        session.execute.assert_not_awaited()
    if state in {"busy", "attached", "drift"}:
        pins.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_refuses_unretained_identity_before_any_copy(monkeypatch):
    monkeypatch.setattr(build.native, "protected_publisher_owner", AsyncMock())
    monkeypatch.setattr(build, "lock_identity", AsyncMock())
    monkeypatch.setattr(build, "lock_retained_source", AsyncMock(side_effect=PTG2ManifestArtifactError("pin missing")))
    copy = AsyncMock()
    monkeypatch.setattr(build, "_authenticate_root", copy)
    with pytest.raises(PTG2ManifestArtifactError, match="pin missing"):
        await build.build_in_session(object(), **PARAMS, retained_identity=PARAMS)
    copy.assert_not_awaited()
