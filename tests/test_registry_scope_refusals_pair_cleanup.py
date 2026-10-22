# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained capture cleanup preserves committed evidence and its exact source namespace."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_cms_registry_source_pair as retained
from process import network_registry_cms_prepared_pair as prepared
from tests.test_cms_registry_source_pair_slot import _prepared
from tests.test_network_cms_registry_source_pair import _retained_pair, _witness_pair


def _cleanup_case(monkeypatch, comment):
    """Model only external catalog/receipt IO; execute the actual cleanup decision."""
    events = []

    @asynccontextmanager
    async def begin():
        events.append("begin")
        try:
            yield
        finally:
            events.append("end")

    session = SimpleNamespace(begin=begin, scalar=AsyncMock(return_value=comment))

    @asynccontextmanager
    async def sessions():
        events.append("open")
        try:
            yield session
        finally:
            events.append("close")

    cleanup = AsyncMock()
    receipts = AsyncMock(return_value=False)
    committed = AsyncMock(return_value=None)
    monkeypatch.setattr(retained, "cleanup_entity_address_archive_stage", cleanup)
    monkeypatch.setattr(retained.serving, "has_registry_source_pair_receipt", receipts)
    monkeypatch.setattr(prepared, "read_committed_registry_cms_pair", committed)
    return SimpleNamespace(
        sessions=sessions, session=session, events=events, cleanup=cleanup, receipts=receipts, committed=committed
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_source", [False, True])
async def test_unstamped_capture_requires_explicit_source_before_cleanup(monkeypatch, explicit_source):
    pair = _prepared()
    case = _cleanup_case(monkeypatch, None)
    arguments_by_name = {"source_schema": "actual_source"} if explicit_source else {}
    if explicit_source:
        await retained._cleanup_capture(case.sessions, pair.address_ownership, **arguments_by_name)
        case.receipts.assert_awaited_once_with(case.session, "actual_source", pair.address_ownership.dataset_id)
        case.cleanup.assert_awaited_once_with(case.session, owner=pair.address_ownership)
    else:
        with pytest.raises(ValueError, match="cleanup_scope_required"):
            await retained._cleanup_capture(case.sessions, pair.address_ownership, **arguments_by_name)
        case.receipts.assert_not_awaited()
        case.cleanup.assert_not_awaited()
    assert case.session.scalar.await_args.args[1] == {"oid": pair.address_ownership.schema_oid}
    assert case.events == ["open", "begin", "end", "close"]


@pytest.mark.asyncio
async def test_committed_receipt_refuses_unstamped_capture_deletion(monkeypatch):
    pair = _prepared()
    case = _cleanup_case(monkeypatch, None)
    case.receipts.return_value = True
    with pytest.raises(ValueError, match="registry_cms_source_pair_invalid"):
        await retained._cleanup_capture(case.sessions, pair.address_ownership, source_schema="actual_source")
    case.cleanup.assert_not_awaited()
    case.receipts.assert_awaited_once_with(case.session, "actual_source", pair.address_ownership.dataset_id)
    assert case.events == ["open", "begin", "end", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix,make_pair", [(retained._COMMENT, _retained_pair), (retained._COMMENT_V2, _witness_pair)]
)
async def test_committed_pair_stamp_survives_lost_ack_cleanup(monkeypatch, prefix, make_pair):
    pair = make_pair()
    case = _cleanup_case(monkeypatch, prefix + retained._json(pair.as_dict()))
    await retained._cleanup_capture(case.sessions, pair.address_ownership)
    case.committed.assert_not_awaited()
    case.receipts.assert_not_awaited()
    case.cleanup.assert_not_awaited()
    assert case.events == ["open", "begin", "end", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("committed,receipt_exists", [(True, False), (False, False), (False, True)])
async def test_prepared_capture_binds_cleanup_to_original_source(monkeypatch, committed, receipt_exists):
    pair = _prepared()
    comment = prepared._PREPARED_COMMENT + retained._json(pair.as_dict())
    case = _cleanup_case(monkeypatch, comment)
    case.committed.return_value = object() if committed else None
    case.receipts.return_value = receipt_exists
    if receipt_exists:
        with pytest.raises(ValueError, match="registry_cms_source_pair_invalid"):
            await retained._cleanup_capture(case.sessions, pair.address_ownership, source_schema="foreign_source")
    else:
        await retained._cleanup_capture(case.sessions, pair.address_ownership, source_schema="foreign_source")
    case.committed.assert_awaited_once_with(case.session, pair)
    if committed:
        case.receipts.assert_not_awaited()
    else:
        case.receipts.assert_awaited_once_with(
            case.session, pair.request.source_pin.schema_name, pair.address_ownership.dataset_id
        )
    assert case.cleanup.await_count == int(not committed and not receipt_exists)
    if case.cleanup.await_count:
        case.cleanup.assert_awaited_once_with(case.session, owner=pair.address_ownership)
    assert case.events == ["open", "begin", "end", "close"]


@pytest.mark.asyncio
async def test_malformed_prepared_stamp_never_falls_back_to_supplied_namespace(monkeypatch):
    pair = _prepared()
    case = _cleanup_case(monkeypatch, prepared._PREPARED_COMMENT + "{}")
    with pytest.raises(ValueError, match="registry_cms_prepared_capture_invalid"):
        await retained._cleanup_capture(case.sessions, pair.address_ownership, source_schema="foreign_source")
    case.committed.assert_not_awaited()
    case.receipts.assert_not_awaited()
    case.cleanup.assert_not_awaited()
    assert case.events == ["open", "begin", "end", "close"]
