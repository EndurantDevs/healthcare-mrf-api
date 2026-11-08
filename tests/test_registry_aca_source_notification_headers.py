# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite receipt scans; native qualification is doubled for cursor protocol tests."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_aca_source_notifications as notices
from tests.test_registry_aca_source_notifications import _store


def _header(number):
    return {"capture_id": str(UUID(int=number)), "recipe_sha256": "a" * 64, "notification_sha256": "b" * 64}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "after,upper,numbers,complete",
    [(None, None, [], True), (None, 3, [1, 2], False), (1, 3, [2, 3], True), (3, 3, [], True)],
)
async def test_scan_resumes_with_fixed_upper_bound(monkeypatch, after, upper, numbers, complete):
    identity = object()
    qualification = AsyncMock(return_value=identity)
    monkeypatch.setattr(notices.RegistryACASourceNotificationStore, "qualify", qualification)
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=str(UUID(int=upper)) if upper else None),
        fetch=AsyncMock(return_value=[_header(number) for number in numbers]),
    )
    cursor = str(UUID(int=after)) if after else None
    boundary = str(UUID(int=upper)) if after else None
    receipt = await _store().headers(connection, identity, after=cursor, upper=boundary, limit=2)
    assert receipt == {
        "headers": [_header(number) for number in numbers],
        "upper": str(UUID(int=upper)) if upper else None,
        "after": str(UUID(int=numbers[-1])) if numbers else cursor,
        "complete": complete,
    }
    assert qualification.await_count == 2
    if after:
        connection.fetchval.assert_not_awaited()
    else:
        connection.fetchval.assert_awaited_once()
    if upper:
        assert connection.fetch.await_args.args[1:] == (UUID(cursor) if cursor else None, UUID(int=upper), 2)
    else:
        connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["reversed", "duplicate", "outside", "over_limit", "qualification"])
async def test_scan_refuses_inconsistent_rows_or_changed_native_qualification(monkeypatch, damage):
    identity = object()
    qualification = AsyncMock(side_effect=[identity, object() if damage == "qualification" else identity])
    monkeypatch.setattr(notices.RegistryACASourceNotificationStore, "qualify", qualification)
    numbers_by_damage = {
        "reversed": [2, 1],
        "duplicate": [2, 2],
        "outside": [4],
        "over_limit": [1, 2, 3],
        "qualification": [1, 2],
    }
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[_header(number) for number in numbers_by_damage[damage]])
    )
    with pytest.raises(ValueError):
        await _store().headers(connection, identity, upper=str(UUID(int=3)), limit=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{"limit": True}, {"limit": 0}, {"limit": 26}, {"after": str(UUID(int=1))}])
async def test_invalid_scan_bounds_refuse_before_database_access(monkeypatch, options):
    qualification = AsyncMock(side_effect=AssertionError("unexpected qualification"))
    monkeypatch.setattr(notices.RegistryACASourceNotificationStore, "qualify", qualification)
    with pytest.raises(ValueError):
        await _store().headers(object(), object(), **options)
    qualification.assert_not_awaited()
