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

_HELPER_CATALOG_SQL = """
SELECT p.oid::bigint AS function_oid,
       pg_catalog.to_regprocedure(
           'healthporta_guardrails.set_provider_directory_profile_temp_file_limit(bigint)'
       )::pg_catalog.oid::bigint AS resolved_function_oid,
       p.prosrc AS function_body, p.proconfig AS function_settings,
       l.lanname AS language, p.prosecdef AS security_definer,
       (p.prokind='f' AND p.pronargs=1 AND p.proargtypes[0]='bigint'::pg_catalog.regtype
           AND p.proargnames=ARRAY['limit_bytes']::pg_catalog.text[] AND p.proargmodes IS NULL
           AND p.pronargdefaults=0 AND p.provariadic=0 AND p.prorettype='pg_catalog.text'::pg_catalog.regtype
           AND NOT p.proretset AND NOT p.proisstrict AND NOT p.proleakproof
           AND p.provolatile='v' AND p.proparallel='u') AS signature_matches,
       (NOT owner_role.rolcanlogin AND NOT owner_role.rolsuper AND NOT owner_role.rolcreaterole
           AND NOT owner_role.rolcreatedb AND NOT owner_role.rolreplication AND NOT owner_role.rolbypassrls
           AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members membership
               WHERE membership.member=owner_role.oid OR membership.roleid=owner_role.oid)) AS owner_restricted,
       pg_catalog.has_parameter_privilege(owner_role.oid, 'temp_file_limit', 'SET') AS owner_can_set,
       pg_catalog.has_function_privilege(current_user, p.oid, 'EXECUTE') AS caller_can_execute,
       (
           SELECT pg_catalog.count(*)=1 AND COALESCE(pg_catalog.bool_and(CASE WHEN permission.grantee=0 THEN false
               ELSE NOT permission.is_grantable
                   AND pg_catalog.pg_has_role(current_user, permission.grantee, 'USAGE') END), false)
           FROM pg_catalog.aclexplode(COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) permission
           WHERE permission.privilege_type='EXECUTE' AND permission.grantee<>p.proowner
       ) AS execute_acl_restricted,
       pg_catalog.has_schema_privilege(current_user, n.oid, 'USAGE') AS caller_can_use_schema,
       (pg_catalog.has_schema_privilege(current_user, n.oid, 'CREATE')
           OR pg_catalog.has_schema_privilege(session_user, n.oid, 'CREATE')
           OR pg_catalog.pg_has_role(current_user, n.nspowner, 'MEMBER')
           OR pg_catalog.pg_has_role(current_user, n.nspowner, 'SET')
           OR pg_catalog.pg_has_role(session_user, n.nspowner, 'MEMBER')
           OR pg_catalog.pg_has_role(session_user, n.nspowner, 'SET')) AS caller_can_create,
       (pg_catalog.pg_has_role(current_user, owner_role.oid, 'MEMBER')
           OR pg_catalog.pg_has_role(current_user, owner_role.oid, 'SET')
           OR pg_catalog.pg_has_role(session_user, owner_role.oid, 'MEMBER')
           OR pg_catalog.pg_has_role(session_user, owner_role.oid, 'SET')) AS caller_can_assume_owner,
       (caller_role.rolsuper OR caller_role.rolcreaterole
           OR session_role.rolsuper OR session_role.rolcreaterole) AS caller_can_alter_owner
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
JOIN pg_catalog.pg_language l ON l.oid=p.prolang
JOIN pg_catalog.pg_roles owner_role ON owner_role.oid=p.proowner
JOIN pg_catalog.pg_roles caller_role ON caller_role.rolname=current_user
JOIN pg_catalog.pg_roles session_role ON session_role.rolname=session_user
WHERE n.nspname='healthporta_guardrails'
  AND p.proname='set_provider_directory_profile_temp_file_limit'
"""


async def require_temp_file_limit_capability(database):
    """Return the direct or bounded mode after verifying its required capability."""
    if (
        await database.scalar("SELECT pg_catalog.has_parameter_privilege(current_user, 'temp_file_limit', 'SET');")
        is True
    ):
        return "direct"
    return await _require_bounded_temp_file_limit_capability(database)


async def _require_bounded_temp_file_limit_capability(database):
    """Verify the exact existing helper after a native privilege probe fails."""
    helper_entries = await database.all(_HELPER_CATALOG_SQL)
    helper_by_field = (
        dict(getattr(helper_entries[0], "_mapping", helper_entries[0])) if len(helper_entries) == 1 else {}
    )
    required_flags = (
        "security_definer",
        "signature_matches",
        "owner_restricted",
        "owner_can_set",
        "caller_can_execute",
        "execute_acl_restricted",
        "caller_can_use_schema",
    )
    forbidden_flags = ("caller_can_create", "caller_can_assume_owner", "caller_can_alter_owner")
    if (
        type(helper_by_field.get("function_oid")) is not int
        or helper_by_field["function_oid"] <= 0
        or type(helper_by_field.get("resolved_function_oid")) is not int
        or helper_by_field["function_oid"] != helper_by_field["resolved_function_oid"]
        or helper_by_field.get("function_body") != TEMP_FILE_LIMIT_BODY
        or helper_by_field.get("function_settings") != ["search_path=pg_catalog"]
        or helper_by_field.get("language") != "plpgsql"
        or any(helper_by_field.get(flag) is not True for flag in required_flags)
        or any(helper_by_field.get(flag) is not False for flag in forbidden_flags)
    ):
        raise RuntimeError("provider_directory_profile_capacity_temp_file_limit_privilege_missing")
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
