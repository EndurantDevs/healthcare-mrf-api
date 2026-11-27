# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Apply exact transaction temp limits through direct SET or one bounded helper."""

TEMP_FILE_LIMIT_SCHEMA = "healthporta_guardrails"
TEMP_FILE_LIMIT_FUNCTION = "set_provider_directory_profile_temp_file_limit"
TEMP_FILE_LIMIT_MAX_BYTES = 48 * 1024**3
TEMP_FILE_LIMIT_BODY = """BEGIN
    IF limit_bytes IS NULL OR limit_bytes < 0 OR limit_bytes > 51539607552
        OR limit_bytes % 1024 <> 0 THEN
        RAISE EXCEPTION 'provider_directory_profile_temp_file_limit_invalid';
    END IF;
    RETURN pg_catalog.set_config('temp_file_limit', (limit_bytes / 1024)::text || 'kB', true);
END;"""


async def require_temp_file_limit_capability(database):
    """Use direct SET when granted, otherwise the helper; PostgreSQL refuses a caller without EXECUTE."""
    if (
        await database.scalar("SELECT pg_catalog.has_parameter_privilege(current_user, 'temp_file_limit', 'SET');")
        is True
    ):
        return "direct"
    return "bounded"


async def apply_temp_file_limit(database, limit_bytes):
    """Apply a transaction-local byte limit and verify the effective exact value."""
    if type(limit_bytes) is not int or limit_bytes < 0 or limit_bytes % 1024:
        raise ValueError("provider_directory_profile_temp_file_limit_invalid")
    if await require_temp_file_limit_capability(database) == "direct":
        await database.status(f"SET LOCAL temp_file_limit = '{limit_bytes // 1024}kB';")
    else:
        if limit_bytes > TEMP_FILE_LIMIT_MAX_BYTES:
            raise ValueError("provider_directory_profile_temp_file_limit_invalid")
        returned_bytes = await database.scalar(
            "SELECT pg_catalog.pg_size_bytes("
            "healthporta_guardrails.set_provider_directory_profile_temp_file_limit(CAST(:limit_bytes AS bigint))"
            ")::bigint;",
            limit_bytes=limit_bytes,
        )
        if type(returned_bytes) is not int or returned_bytes != limit_bytes:
            raise RuntimeError("provider_directory_profile_temp_file_limit_setting_mismatch")
    observed_bytes = await database.scalar(
        "SELECT pg_catalog.pg_size_bytes(pg_catalog.current_setting('temp_file_limit'))::bigint;"
    )
    if type(observed_bytes) is not int or observed_bytes != limit_bytes:
        raise RuntimeError("provider_directory_profile_temp_file_limit_setting_mismatch")
