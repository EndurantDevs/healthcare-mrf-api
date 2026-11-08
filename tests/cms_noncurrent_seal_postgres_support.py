# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real header-guard component bootstrap; not an enrolled serving fixture.

The five metadata bodies and both lifecycle/admission guards are source exact.
The complete deferred serving, writer closure and runtime transition stay outside
this fixture; none of their guards is represented by a permissive stub.
"""

from db import migration_cms_noncurrent_seal as migration
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_source_callable_custody as custody
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS


async def _install_metadata_functions(connection, schema, owner_role, renderers):
    metadata_names = {signature.split("(", 1)[0] for signature in METADATA_FUNCTIONS}
    for render in renderers:
        sql = render(schema)
        name = sql.split("FUNCTION ", 1)[1].split("(", 1)[0]
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_catalog.pg_proc WHERE pronamespace=$1::regnamespace AND proname=$2)",
            schema,
            name.rsplit(".", 1)[1].strip('"'),
        )
        await connection.execute(sql)
        signature = await connection.fetchval(
            "SELECT oid::regprocedure::text FROM pg_catalog.pg_proc WHERE pronamespace=$1::regnamespace AND proname=$2",
            schema,
            name.rsplit(".", 1)[1].strip('"'),
        )
        await connection.execute(f"ALTER FUNCTION {signature} OWNER TO {storage_identifier(owner_role)}")
        await connection.execute(f"ALTER FUNCTION {signature} SECURITY DEFINER")
        await connection.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        body = await connection.fetchval("SELECT prosrc FROM pg_catalog.pg_proc WHERE oid=$1::regprocedure", signature)
        function_name = name.rsplit(".", 1)[1].strip('"')
        if function_name in metadata_names:
            expected = next(spec for key, spec in custody._CONTRACTS.items() if key.split("(", 1)[0] == function_name)
            assert custody._body_digest(body, schema) == expected[-1]


async def install_native_header_guards(connection, schema, *, owner_role):
    """Install authentic functions/hooks on an otherwise unguarded test header.

    Existing functions/triggers are refused rather than replaced or disabled.
    No production custody or full CMS producer acceptance is implied.
    """
    compact = migration._migration("20260818020000_provider_directory_terminal_publication_compact_guard.py")
    subset, admission = compact._terminal()._subset(), compact._admission()
    tail = compact._load_sibling(
        "20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance.py", "_cms_seal_native_tail"
    )
    renderers = (
        subset._canonical_json_function_sql,
        subset._canonical_sha256_function_sql,
        subset._payload_canonical_json_function_sql,
        subset._payload_sha256_function_sql,
        subset._proof_pair_valid_function_sql,
        lambda namespace: tail._proof_shape_valid_function_sql(namespace, tail_tolerant=True),
        lambda namespace: subset._content_proof_valid_function_sql(namespace, replace_existing=True),
        admission._digest_function_sql,
    )
    await _install_metadata_functions(connection, schema, owner_role, renderers)
    old = migration.lifecycle_guard_bodies(schema)[0]
    sql = compact._endpoint_guard_sql(schema, compact=True)
    await connection.execute(sql.replace(compact._function_body(sql), old, 1))
    guard = storage_relation(schema, migration.GUARD) + "()"
    await connection.execute(f"ALTER FUNCTION {guard} OWNER TO {storage_identifier(owner_role)}")
    await connection.execute(f"REVOKE ALL ON FUNCTION {guard} FROM PUBLIC")
    table = storage_relation(schema, "provider_directory_endpoint_dataset")
    await connection.execute(
        f"CREATE TRIGGER tin_npi_connector_endpoint_dataset_guard BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {table} FOR EACH ROW EXECUTE FUNCTION {guard}"
    )
    await connection.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER tin_npi_connector_endpoint_dataset_guard")
    sql = admission._guard_function_sql(schema)
    repairs = migration._migration("20261016050000_acquisition_validator_argument_types.py")
    for caller, old_call, new_call, count in repairs._calls(schema):
        if caller == admission._GUARD_FUNCTION:
            assert sql.count(old_call) == count
            sql = sql.replace(old_call, new_call)
    await connection.execute(sql)
    await connection.execute(admission._truncate_function_sql(schema))
    for sql in admission._install_triggers_sql(schema):
        await connection.execute(sql)
    return dict(
        await connection.fetchrow(
            "SELECT oid::bigint,proowner::bigint AS owner_oid FROM pg_catalog.pg_proc WHERE oid=$1::regprocedure",
            guard,
        )
    )
