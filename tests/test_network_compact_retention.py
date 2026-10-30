# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mandatory compact pins supplement both existing local cleanup callbacks."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_ownership as ownership
from process import entity_address_snapshot_preparation as protected
from process import network_cms_registry_address_capture as capture
from process import network_compact_retention as retention
from tests.test_entity_address_archive_ownership_guards import _set_owner


@pytest.mark.parametrize("blocked_call", [1, 2])
async def test_cleanup_requires_both_compact_reference_checks(monkeypatch, blocked_call):
    owner = _set_owner()
    connection = object()
    monkeypatch.setattr(protected, "_publisher_authority", AsyncMock(return_value=31))
    monkeypatch.setattr(ownership, "_validated_publication_owner", lambda *_: ("example", [], {"relation_oid": 8}))
    monkeypatch.setattr(ownership, "_require_retained_catalog", lambda *_: None)
    monkeypatch.setattr(
        ownership, "_retained_relation_catalog", AsyncMock(return_value=[{"nspname": "example", "relname": "retained"}])
    )
    monkeypatch.setattr(ownership, "_schema_oid", AsyncMock(return_value=owner.schema_oid))
    monkeypatch.setattr(ownership, "_assert_namespace_is_owned", AsyncMock())
    monkeypatch.setattr(capture, "native_driver", AsyncMock(return_value=connection))
    guard_outcomes = [retention.NetworkCompactRetentionError("retained")]
    if blocked_call == 2:
        guard_outcomes.insert(0, None)
    guard = AsyncMock(side_effect=guard_outcomes)
    monkeypatch.setattr(retention, "require_network_compact_artifacts_unreferenced", guard)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    old_hook = AsyncMock(return_value=True)
    with pytest.raises(retention.NetworkCompactRetentionError, match="retained"):
        await ownership.cleanup_entity_address_archive_publication(
            session, owner=owner, publication={}, assert_unreferenced=old_hook
        )
    assert guard.await_count == old_hook.await_count == blocked_call
    assert guard.await_args.args == (connection, [8])
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == blocked_call - 1
    assert all("ACCESS EXCLUSIVE" in statement for statement in statements)
    assert not any("DROP" in statement for statement in statements)
