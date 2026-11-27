# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction temp-limit modes, exact readback and PostgreSQL refusals."""

import pytest

from process import provider_directory_profile_temp_limit as temp_limit

_UNSET = object()


class TempLimitDatabase:
    def __init__(self, *, can_set=False, observed_bytes=_UNSET, returned_bytes=_UNSET, helper_error=None):
        self.can_set = can_set
        self.helper_error = helper_error
        self.observed_bytes = observed_bytes
        self.returned_bytes = returned_bytes
        self.setting_bytes = 4096
        self.statements = []
        self.mutations = []

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
            if self.helper_error is not None:
                raise self.helper_error
            self.mutations.append((statement, params))
            self.setting_bytes = params["limit_bytes"]
            return self.setting_bytes if self.returned_bytes is _UNSET else self.returned_bytes
        assert "pg_size_bytes(pg_catalog.current_setting('temp_file_limit'))" in statement
        return self.setting_bytes if self.observed_bytes is _UNSET else self.observed_bytes


@pytest.mark.asyncio
async def test_direct_capability_does_not_require_helper():
    database = TempLimitDatabase(can_set=True)
    assert await temp_limit.require_temp_file_limit_capability(database) == "direct"
    assert len(database.statements) == 1
    assert not database.mutations


@pytest.mark.asyncio
async def test_helper_mode_reads_no_function_catalog():
    database = TempLimitDatabase()
    assert await temp_limit.require_temp_file_limit_capability(database) == "bounded"
    assert len(database.statements) == 1
    assert "pg_proc" not in database.statements[0]
    assert not database.mutations


@pytest.mark.asyncio
async def test_postgres_refusal_of_the_helper_propagates():
    refusal = PermissionError("permission denied for function set_provider_directory_profile_temp_file_limit")
    database = TempLimitDatabase(helper_error=refusal)
    with pytest.raises(PermissionError, match="permission denied"):
        await temp_limit.apply_temp_file_limit(database, 1024)
    assert not database.mutations
    assert not any("pg_proc" in statement for statement in database.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_bytes", [0, 1024, temp_limit.TEMP_FILE_LIMIT_MAX_BYTES + 1024])
async def test_direct_set_preserves_values_above_helper_cap(limit_bytes):
    database = TempLimitDatabase(can_set=True)
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
