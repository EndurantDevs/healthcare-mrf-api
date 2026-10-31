# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private preparation precedes the short, nonrecursive attachment lock."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_completion as completion


def _case(monkeypatch):
    events = []
    connection = SimpleNamespace(execute=AsyncMock(side_effect=lambda sql: events.append(sql)))
    policy = SimpleNamespace(schema="snapshots")
    binding_by_field = {
        "identity": {"dataset_id": "new-dataset"},
        "tables": {root: {"name": "stage_" + str(index)} for index, root in enumerate(completion.storage.RELATIONS)},
    }
    monkeypatch.setattr(
        completion.storage, "_close_grants", AsyncMock(side_effect=lambda *args, **kwargs: events.append("grants"))
    )
    return connection, policy, binding_by_field, events


@pytest.mark.asyncio
async def test_all_private_checks_and_grants_finish_before_sorted_only_parent_locks(monkeypatch):
    connection, policy, binding, events = _case(monkeypatch)
    await completion._attach_final_tables(connection, policy, binding)
    lock_index = next(index for index, event in enumerate(events) if event.startswith("LOCK TABLE"))
    roots = sorted(storage_relation(policy.schema, root) for root in completion.storage.RELATIONS)
    assert events[lock_index] == (
        "LOCK TABLE " + ",".join("ONLY " + root for root in roots) + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
    )
    assert sum("ADD CHECK" in event for event in events[:lock_index]) == len(roots)
    assert sum("RESET (autovacuum_enabled)" in event for event in events[:lock_index]) == len(roots)
    assert events[:lock_index].count("grants") == len(roots)
    attachments = events[lock_index + 1 :]
    assert len(attachments) == len(roots)
    assert all(" ATTACH PARTITION " in event for event in attachments)
    assert all(
        event.startswith("ALTER TABLE " + root + " ATTACH") for root, event in zip(roots, attachments, strict=True)
    )


@pytest.mark.asyncio
async def test_failed_private_preparation_never_locks_a_shared_parent(monkeypatch):
    connection, policy, binding, events = _case(monkeypatch)
    completion.storage._close_grants.side_effect = ValueError("private preparation failed")
    with pytest.raises(ValueError, match="private preparation failed"):
        await completion._attach_final_tables(connection, policy, binding)
    assert not any(event.startswith("LOCK TABLE") or "ATTACH PARTITION" in event for event in events)


@pytest.mark.asyncio
async def test_parent_lock_conflict_never_starts_attachment(monkeypatch):
    connection, policy, binding, events = _case(monkeypatch)

    def execute(sql):
        events.append(sql)
        if sql.startswith("LOCK TABLE"):
            raise ValueError("parent busy")

    connection.execute.side_effect = execute
    with pytest.raises(ValueError, match="parent busy"):
        await completion._attach_final_tables(connection, policy, binding)
    assert not any("ATTACH PARTITION" in event for event in events)
