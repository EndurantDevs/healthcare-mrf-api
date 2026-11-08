# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Trust boundaries for the closed transaction temp-limit capability."""

from types import SimpleNamespace

import pytest

from process import provider_directory_profile_temp_limit as temp_limit

_UNSET = object()


def _helper_catalog_entry():
    return {
        "function_oid": 20001,
        "resolved_function_oid": 20001,
        "function_body": temp_limit.TEMP_FILE_LIMIT_BODY,
        "function_settings": ["search_path=pg_catalog"],
        "language": "plpgsql",
        "security_definer": True,
        "signature_matches": True,
        "owner_restricted": True,
        "owner_can_set": True,
        "caller_can_execute": True,
        "execute_acl_restricted": True,
        "caller_can_use_schema": True,
        "caller_can_create": False,
        "caller_can_assume_owner": False,
        "caller_can_alter_owner": False,
    }


class TempLimitDatabase:
    def __init__(self, *, can_set=False, helper_entries=None, observed_bytes=_UNSET, returned_bytes=_UNSET):
        self.can_set = can_set
        self.helper_entries = (
            [SimpleNamespace(_mapping=_helper_catalog_entry())] if helper_entries is None else helper_entries
        )
        self.observed_bytes = observed_bytes
        self.returned_bytes = returned_bytes
        self.setting_bytes = 4096
        self.statements = []
        self.mutations = []

    async def all(self, statement):
        self.statements.append(statement)
        return self.helper_entries

    async def status(self, statement):
        self.statements.append(statement)
        self.mutations.append(statement)
        self.setting_bytes = int(statement.split("'")[1][:-2]) * 1024

    async def scalar(self, statement, **params):
        self.statements.append(statement)
        if "has_parameter_privilege" in statement:
            return self.can_set
        if temp_limit.TEMP_FILE_LIMIT_FUNCTION in statement:
            assert "CAST(:limit_bytes AS bigint)" in statement
            assert params.keys() == {"limit_bytes"}
            self.mutations.append((statement, params))
            self.setting_bytes = params["limit_bytes"]
            return self.setting_bytes if self.returned_bytes is _UNSET else self.returned_bytes
        assert "pg_size_bytes(pg_catalog.current_setting('temp_file_limit'))" in statement
        return self.setting_bytes if self.observed_bytes is _UNSET else self.observed_bytes


@pytest.mark.asyncio
async def test_direct_capability_does_not_require_helper():
    database = TempLimitDatabase(can_set=True, helper_entries=[])
    assert await temp_limit.require_temp_file_limit_capability(database) == "direct"
    assert len(database.statements) == 1
    assert not database.mutations


@pytest.mark.asyncio
async def test_helper_capability_checks_without_setting():
    database = TempLimitDatabase()
    assert await temp_limit.require_temp_file_limit_capability(database) == "bounded"
    assert len(database.statements) == 2
    assert not database.mutations


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_bytes", [0, 1024, temp_limit.TEMP_FILE_LIMIT_MAX_BYTES + 1024])
async def test_direct_set_preserves_values_above_helper_cap(limit_bytes):
    database = TempLimitDatabase(can_set=True, helper_entries=[])
    await temp_limit.apply_temp_file_limit(database, limit_bytes)
    assert database.mutations == [f"SET LOCAL temp_file_limit = '{limit_bytes // 1024}kB';"]
    assert (
        database.statements[-1]
        == "SELECT pg_catalog.pg_size_bytes(pg_catalog.current_setting('temp_file_limit'))::bigint;"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_bytes", [0, 1024, temp_limit.TEMP_FILE_LIMIT_MAX_BYTES])
async def test_bounded_fallback_applies_exact_local_setting(limit_bytes):
    database = TempLimitDatabase()
    await temp_limit.apply_temp_file_limit(database, limit_bytes)
    assert len(database.mutations) == 1
    assert database.mutations[0][1] == {"limit_bytes": limit_bytes}
    assert database.setting_bytes == limit_bytes
    assert "current_setting('temp_file_limit')" in database.statements[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_bytes", [None, True, False, -1024, 1, 1025, 1024.0, "1024"])
async def test_invalid_limits_fail_before_database_access(limit_bytes):
    database = TempLimitDatabase()
    with pytest.raises(ValueError, match="temp_file_limit_invalid"):
        await temp_limit.apply_temp_file_limit(database, limit_bytes)
    assert not database.statements
    assert not database.mutations


@pytest.mark.asyncio
async def test_helper_limit_cannot_exceed_bound():
    database = TempLimitDatabase()
    with pytest.raises(ValueError, match="temp_file_limit_invalid"):
        await temp_limit.apply_temp_file_limit(database, temp_limit.TEMP_FILE_LIMIT_MAX_BYTES + 1024)
    assert not database.mutations


@pytest.mark.asyncio
@pytest.mark.parametrize("helper_count", [0, 2])
async def test_missing_or_overloaded_helper_fails_closed(helper_count):
    database = TempLimitDatabase(helper_entries=[_helper_catalog_entry() for _ in range(helper_count)])
    with pytest.raises(RuntimeError, match="temp_file_limit_privilege_missing"):
        await temp_limit.apply_temp_file_limit(database, 1024)
    assert not database.mutations


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "changed_entry"),
    [
        ("function_oid", 0),
        ("function_oid", True),
        ("resolved_function_oid", 20002),
        ("resolved_function_oid", True),
        ("function_body", "BEGIN RETURN '0'; END;"),
        ("function_settings", None),
        ("function_settings", ["search_path=pg_catalog", "temp_file_limit=-1"]),
        ("language", "sql"),
        ("security_definer", False),
        ("security_definer", 1),
        ("signature_matches", False),
        ("owner_restricted", False),
        ("owner_can_set", False),
        ("caller_can_execute", False),
        ("execute_acl_restricted", False),
        ("execute_acl_restricted", None),
        ("execute_acl_restricted", 1),
        ("execute_acl_restricted", "true"),
        ("caller_can_use_schema", False),
        ("caller_can_create", True),
        ("caller_can_assume_owner", True),
        ("caller_can_alter_owner", True),
        ("caller_can_alter_owner", 0),
    ],
)
async def test_execute_grant_does_not_accept_drift(field_name, changed_entry):
    helper_by_field = _helper_catalog_entry()
    helper_by_field[field_name] = changed_entry
    database = TempLimitDatabase(helper_entries=[helper_by_field])
    for operation in (
        lambda: temp_limit.require_temp_file_limit_capability(database),
        lambda: temp_limit.apply_temp_file_limit(database, 1024),
    ):
        with pytest.raises(RuntimeError, match="temp_file_limit_privilege_missing"):
            await operation()
    assert not database.mutations


@pytest.mark.asyncio
async def test_missing_acl_observation_fails_closed():
    helper_by_field = _helper_catalog_entry()
    helper_by_field.pop("execute_acl_restricted")
    database = TempLimitDatabase(helper_entries=[helper_by_field])
    with pytest.raises(RuntimeError, match="temp_file_limit_privilege_missing"):
        await temp_limit.apply_temp_file_limit(database, 1024)
    assert not database.mutations


@pytest.mark.asyncio
@pytest.mark.parametrize("can_set", [True, False])
@pytest.mark.parametrize("limit_bytes", [0, 1024])
@pytest.mark.parametrize("observed_bytes", [None, False, -1, 2048])
async def test_exact_setting_readback_is_required(can_set, limit_bytes, observed_bytes):
    database = TempLimitDatabase(can_set=can_set, observed_bytes=observed_bytes)
    with pytest.raises(RuntimeError, match="temp_file_limit_setting_mismatch"):
        await temp_limit.apply_temp_file_limit(database, limit_bytes)
    assert len(database.mutations) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_bytes", [None, False, -1, 2048])
async def test_fallback_return_cannot_disagree(returned_bytes):
    database = TempLimitDatabase(returned_bytes=returned_bytes)
    with pytest.raises(RuntimeError, match="temp_file_limit_setting_mismatch"):
        await temp_limit.apply_temp_file_limit(database, 1024)
    assert len(database.mutations) == 1
