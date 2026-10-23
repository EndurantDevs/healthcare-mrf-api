# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source handoff, retained obligations and exact interruption reconciliation."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_scope_runtime as runtime
from process import registry_ptg_source_retention as retention
from process.ptg_parts import result_archive_published_authority as archive
from tests.test_registry_ptg_published_plan import identity
from tests.test_registry_ptg_scope_runtime import _configured


def receipt(*, office=False):
    prefix = "registry_ptg_office_review_" if office else "registry_ptg_published_review_"
    return archive.PtgPublishedResultSourceAuthority(
        prefix + UUID(int=7).hex, {**identity(), "import_month": "2026-01-01"}
    ).as_dict()


@pytest.mark.parametrize("role", ["scope_reader", "scope_approver", "scope_owner"])
def test_retention_configuration_refuses_existing_runtime_authority(role):
    configured_by_field = {
        **_configured(),
        "source_retention_dsn": f"postgresql://{role}@database.example/synthetic?ssl=require",
    }
    with pytest.raises(ValueError, match="configuration_invalid"):
        runtime._configuration(configured_by_field)


def test_retention_configuration_requires_same_database_and_separate_principal():
    configured_by_field = {
        **_configured(),
        "source_retention_dsn": "postgresql://retention_publisher@database.example/synthetic?ssl=require",
    }
    parsed, *_ = runtime._configuration(configured_by_field)
    assert parsed["source_retention_url"].username == "retention_publisher"
    assert "source_retention_url" not in configured_by_field
    configured_by_field["source_retention_dsn"] = configured_by_field["source_retention_dsn"].replace(
        "/synthetic?", "/different?"
    )
    with pytest.raises(ValueError, match="configuration_invalid"):
        runtime._configuration(configured_by_field)


def test_provisioning_only_adds_the_dedicated_pin_and_constant_lock_rights():
    statements = retention.source_retention_provisioning_statements("synthetic_ptg", "retention_publisher")
    assert len(statements) == 7
    assert all("UPDATE(registry_ptg_read_lock)" in statement for statement in statements[:-1])
    assert statements[-1] == (
        "GRANT INSERT(owner_type,owner_id,snapshot_id,reason,created_at), DELETE "
        'ON "synthetic_ptg".ptg2_snapshot_pin TO "retention_publisher"'
    )
    assert not any("SET ROLE" in statement or "CREATE" in statement for statement in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["prepare_commit", "approval", "cancel"])
async def test_interruption_reconciles_the_exact_committed_or_unknown_source(monkeypatch, phase):
    expected = receipt()
    original = asyncio.CancelledError() if phase == "cancel" else RuntimeError("synthetic lost acknowledgement")
    events = []

    async def prepare(service, envelope, deadline, state):
        state["authority"] = expected
        events.append("pin_commit_attempt")
        if phase == "prepare_commit":
            raise original
        return expected

    reconcile = AsyncMock(side_effect=lambda *_: events.append("reconciled"))
    monkeypatch.setattr(retention, "retain_published_review_source", prepare)
    monkeypatch.setattr(retention, "reconcile_unapproved_review_source", reconcile)
    with pytest.raises(type(original)) as caught:
        async with retention.retained_review_source(object(), {}, 100) as observed:
            assert observed == expected and events == ["pin_commit_attempt"]
            raise original
    assert caught.value is original
    reconcile.assert_awaited_once_with(reconcile.await_args.args[0], expected)
    assert events == ["pin_commit_attempt", "reconciled"]


@pytest.mark.asyncio
async def test_cleanup_refusal_preserves_original_error_and_retention(monkeypatch):
    expected = receipt()

    async def prepare(_service, _envelope, _deadline, state):
        state["authority"] = expected
        return expected

    monkeypatch.setattr(retention, "retain_published_review_source", prepare)
    monkeypatch.setattr(retention, "reconcile_unapproved_review_source", AsyncMock(side_effect=PermissionError("busy")))
    original = RuntimeError("original approval refusal")
    with pytest.raises(RuntimeError) as caught:
        async with retention.retained_review_source(object(), {}, 100):
            raise original
    assert caught.value is original


@pytest.mark.asyncio
@pytest.mark.parametrize("office", [False, True])
@pytest.mark.parametrize("held", [False, True, None])
async def test_reconciliation_takes_exclusive_fence_and_preserves_every_obligation(monkeypatch, office, held):
    expected = receipt(office=office)
    events = []
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[held, held]))

    @asynccontextmanager
    async def transaction(service, *, release=False):
        assert release is True
        events.append("exclusive_before_snapshot")
        yield session
        events.append("commit")

    monkeypatch.setattr(retention, "source_retention_transaction", transaction)
    monkeypatch.setattr(
        retention,
        "_protected_store",
        AsyncMock(side_effect=lambda *_, table_name, **__: '"control"."' + table_name + '"'),
    )
    released = AsyncMock(return_value="already_released")
    monkeypatch.setattr(archive, "reconcile_ptg_published_result_source_release", released)
    outcome = await retention.reconcile_unapproved_review_source(
        SimpleNamespace(store=object(), schema_name="synthetic_ptg"), expected
    )
    assert events == ["exclusive_before_snapshot", "commit"]
    if held is False:
        assert outcome == "already_released"
        released.assert_awaited_once_with(session, schema_name="synthetic_ptg", authority=expected)
        assert session.scalar.await_count == (2 if office else 1)
    else:
        assert outcome == "retained"
        released.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_retention_configuration_cannot_promote_an_approver():
    with pytest.raises(RuntimeError, match="retention_unconfigured"):
        retention.retention_sessions(SimpleNamespace(source_retention_sessions=None))


@pytest.mark.asyncio
@pytest.mark.parametrize("native_proof", [True, False, None])
async def test_native_principal_refusal_precedes_source_access(monkeypatch, native_proof):
    session = SimpleNamespace(scalar=AsyncMock(return_value=native_proof))
    service = SimpleNamespace(
        source_retention_sessions=SimpleNamespace(
            kw={"bind": SimpleNamespace(url=SimpleNamespace(username="retention_publisher"))}
        ),
        schema_name="synthetic_ptg",
    )
    source_guard = AsyncMock()
    monkeypatch.setattr(retention, "require_published_lock_privileges", source_guard)
    if native_proof is True:
        await retention.verify_source_retention_publisher(session, service)
        source_guard.assert_awaited_once_with(session, "synthetic_ptg", pin_writer=True, retention_publisher=True)
    else:
        with pytest.raises(RuntimeError, match="retention_unprotected"):
            await retention.verify_source_retention_publisher(session, service)
        source_guard.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [None, "review", "identity", "source_key"])
async def test_office_prepare_authorizes_before_short_commit_and_requires_exact_source(monkeypatch, mutation):
    from copy import deepcopy

    from process import registry_ptg_published_plan_scope as published
    from process.ptg_parts import result_archive_source_authority as writer
    from tests.test_registry_ptg_office_approval import _envelope

    envelope = _envelope()
    command = envelope["command"]
    expected = command["source"]["evidence"]["source_authority"]
    identity_by_field = deepcopy(expected["identity"])
    if mutation == "identity":
        identity_by_field["snapshot_key"] += 1
    if mutation == "source_key":
        identity_by_field["source_key"] = "different_source"
    prepared = archive.PtgPublishedResultSourceAuthority(
        "registry_ptg_office_review_" + UUID(command["capture_id"]).hex, identity_by_field
    )
    reviewed_authority = {} if mutation == "review" else expected
    events = []
    session = object()

    @asynccontextmanager
    async def transaction(_service):
        events.append("publisher_begin")
        yield session
        events.append("publisher_commit")

    service = SimpleNamespace(
        schema_name="synthetic_ptg",
        store=object(),
        authority=SimpleNamespace(authorize=AsyncMock(side_effect=lambda *_, **__: events.append("authorize"))),
    )
    monkeypatch.setattr(retention, "source_retention_transaction", transaction)
    source_review = AsyncMock(return_value={"evidence": {"source_authority": reviewed_authority}})
    monkeypatch.setattr(retention, "retention_sessions", lambda _service: object())
    monkeypatch.setattr(published, "read_registry_ptg_published_plan_scope", source_review)
    monkeypatch.setattr(archive, "prepare_ptg_published_result_source_authority", AsyncMock(return_value=prepared))
    pin = AsyncMock(side_effect=lambda *_, **__: events.append("pin_write"))
    monkeypatch.setattr(writer, "commit_ptg_result_archive_source_authority", pin)
    state_by_field = {}
    if mutation is not None:
        with pytest.raises(ValueError, match="source_changed"):
            await retention.retain_office_review_source(service, envelope, 10**20, state_by_field)
        assert events == ["authorize", "publisher_begin"]
        assert state_by_field == {}
        pin.assert_not_awaited()
    else:
        observed = await retention.retain_office_review_source(service, envelope, 10**20, state_by_field)
        assert events == ["authorize", "publisher_begin", "pin_write", "publisher_commit"]
        assert observed == prepared.as_dict() == state_by_field["authority"]
        pin.assert_awaited_once_with(session, schema_name="synthetic_ptg", authority=observed)
    assert source_review.await_args.kwargs["retention_publisher"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authority",
    [
        None,
        {},
        {"operation_id": 7},
        {"operation_id": "not_a_review"},
        {"operation_id": "registry_ptg_published_review_" + "0" * 32},
    ],
)
async def test_unrelated_or_incomplete_operation_cannot_release_any_pin(monkeypatch, authority):
    transaction = AsyncMock()
    monkeypatch.setattr(retention, "source_retention_transaction", transaction)
    with pytest.raises(ValueError):
        await retention.reconcile_unapproved_review_source(object(), authority)
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_missing_publisher_refuses_before_authority_or_database():
    authority = AsyncMock()
    service = SimpleNamespace(source_retention_sessions=None, authority=SimpleNamespace(authorize=authority))
    with pytest.raises(RuntimeError, match="retention_unconfigured"):
        await retention.retain_published_review_source(service, {}, 10**20, {})
    authority.assert_not_awaited()


@pytest.mark.asyncio
async def test_busy_reader_does_not_reserve_the_independent_retention_publisher(monkeypatch):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    publisher = create_async_engine(
        "postgresql+asyncpg://retention_publisher@localhost/synthetic", pool_size=1, max_overflow=0
    )
    reader = create_async_engine("postgresql+asyncpg://scope_reader@localhost/synthetic", pool_size=1, max_overflow=0)
    try:
        sessions = async_sessionmaker(publisher)
        service = SimpleNamespace(
            source_retention_sessions=sessions,
            reader_sessions=async_sessionmaker(reader),
            store=SimpleNamespace(owner_role="scope_owner", approval_role="scope_approver"),
            office_custody=None,
        )
        monkeypatch.setattr(reader.sync_engine.pool, "checkedout", lambda: 1)
        assert retention.retention_sessions(service) is sessions
    finally:
        await publisher.dispose()
        await reader.dispose()
