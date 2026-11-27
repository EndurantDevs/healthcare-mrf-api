# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh database-clock deadline checks without database resources."""

import importlib
from types import SimpleNamespace

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


class DeadlineDatabase:
    def __init__(self, remaining):
        self.remaining = iter(remaining)
        self.calls = 0

    async def first(self, statement, **params):
        assert params.keys() == {"max_build_deadline"}
        assert "clock_timestamp()" in statement
        assert "has_parameter_privilege" not in statement and "pg_proc" not in statement
        self.calls += 1
        return {"remaining_ms": next(self.remaining)}


async def _remaining(monkeypatch, database):
    monkeypatch.setattr(fhir, "db", database)
    admission = SimpleNamespace(lease=SimpleNamespace(max_build_deadline="2030-01-01T00:00:00Z"))
    return await fhir._profile_capacity_remaining_ms(admission)


@pytest.mark.asyncio
async def test_deadline_is_fresh_on_every_call_without_privilege_probe(monkeypatch):
    database = DeadlineDatabase([9000, 8000])
    assert await _remaining(monkeypatch, database) == 9000
    assert await _remaining(monkeypatch, database) == 8000
    assert database.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [-1, 0, 1])
async def test_commit_reserve_boundary_is_unchanged(monkeypatch, offset):
    remaining = fhir.PROVIDER_DIRECTORY_PROFILE_DEADLINE_COMMIT_RESERVE_MS + offset
    database = DeadlineDatabase([remaining])
    if offset <= 0:
        with pytest.raises(fhir.ProviderDirectoryCapacityLeaseError, match="deadline_reached"):
            await _remaining(monkeypatch, database)
    else:
        assert await _remaining(monkeypatch, database) == remaining
    assert database.calls == 1
