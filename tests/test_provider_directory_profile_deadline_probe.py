# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh deadline and strict fallback checks without database resources."""

import importlib
from types import SimpleNamespace

import pytest

from tests.test_provider_directory_profile_temp_limit import _helper_catalog_entry

fhir = importlib.import_module("process.provider_directory_fhir")


class DeadlineDatabase:
    def __init__(self, probes, *, helper=None, remaining=9000):
        self.probes = iter(probes)
        self.helper = [_helper_catalog_entry()] if helper is None else helper
        self.remaining = remaining
        self.calls = []

    async def first(self, statement, **params):
        assert params.keys() == {"max_build_deadline"}
        if "WITH capability AS MATERIALIZED" in statement:
            self.calls.append("probe")
            assert "CASE WHEN can_set_temp IS TRUE THEN" in statement
            return next(self.probes)
        self.calls.append("fresh_deadline")
        assert "clock_timestamp()" in statement
        assert self.calls[-2] == "helper"
        return {"remaining_ms": self.remaining}

    async def all(self, statement):
        self.calls.append("helper")
        assert "pg_catalog.pg_proc" in statement
        return self.helper

    async def scalar(self, *args, **kwargs):
        raise AssertionError("fallback must not repeat direct privilege query")


async def _remaining(monkeypatch, database):
    monkeypatch.setattr(fhir, "db", database)
    admission = SimpleNamespace(lease=SimpleNamespace(max_build_deadline="2030-01-01T00:00:00Z"))
    return await fhir._profile_capacity_remaining_ms(admission)


@pytest.mark.asyncio
async def test_direct_probe_is_fresh_on_every_call(monkeypatch):
    database = DeadlineDatabase(
        [
            {"can_set_temp": True, "remaining_ms": 9000},
            {"can_set_temp": True, "remaining_ms": 8000},
        ]
    )
    assert await _remaining(monkeypatch, database) == 9000
    assert await _remaining(monkeypatch, database) == 8000
    assert database.calls == ["probe", "probe"]


@pytest.mark.asyncio
@pytest.mark.parametrize("probe", [None, {}, {"can_set_temp": False, "remaining_ms": 999999}])
async def test_fallback_validates_then_reads_fresh_deadline(monkeypatch, probe):
    database = DeadlineDatabase([probe])
    assert await _remaining(monkeypatch, database) == 9000
    assert database.calls == ["probe", "helper", "fresh_deadline"]


@pytest.mark.asyncio
async def test_revoked_direct_privilege_is_not_cached(monkeypatch):
    database = DeadlineDatabase(
        [
            {"can_set_temp": True, "remaining_ms": 9000},
            {"can_set_temp": False, "remaining_ms": None},
        ],
        helper=[],
    )
    assert await _remaining(monkeypatch, database) == 9000
    with pytest.raises(RuntimeError, match="temp_file_limit_privilege_missing"):
        await _remaining(monkeypatch, database)
    assert database.calls == ["probe", "probe", "helper"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("signature_matches", False),
        ("owner_restricted", False),
        ("execute_acl_restricted", False),
        ("function_body", "changed"),
        ("resolved_function_oid", 20002),
        ("function_oid", True),
        ("caller_can_assume_owner", True),
    ],
)
async def test_combined_false_branch_keeps_strict_helper_refusals(monkeypatch, field, value):
    helper = _helper_catalog_entry()
    helper[field] = value
    database = DeadlineDatabase([{"can_set_temp": False}], helper=[helper])
    with pytest.raises(RuntimeError, match="temp_file_limit_privilege_missing"):
        await _remaining(monkeypatch, database)
    assert database.calls == ["probe", "helper"]


@pytest.mark.asyncio
@pytest.mark.parametrize("direct", [True, False])
@pytest.mark.parametrize("offset", [-1, 0, 1])
async def test_commit_reserve_boundary_is_unchanged(monkeypatch, direct, offset):
    remaining = fhir.PROVIDER_DIRECTORY_PROFILE_DEADLINE_COMMIT_RESERVE_MS + offset
    database = DeadlineDatabase([{"can_set_temp": direct, "remaining_ms": remaining}], remaining=remaining)
    if offset <= 0:
        with pytest.raises(fhir.ProviderDirectoryCapacityLeaseError, match="deadline_reached"):
            await _remaining(monkeypatch, database)
    else:
        assert await _remaining(monkeypatch, database) == remaining
    assert database.calls == (["probe"] if direct else ["probe", "helper", "fresh_deadline"])
