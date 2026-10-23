# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare protected registry DDL separately from ordinary migration credentials."""

from sqlalchemy import text

from db.migration_registry_preparation import registry_migration, validate_prepared_registry_table
from process.network_address_projection import _identifier
from process.registry_candidate_custody_preparation import prepare_candidate_custody_table
from process.registry_company_approval_fence import _fence_key
from process.registry_management_permissions import assume_registry_source_owner
from process.registry_source_admission_receipt import _TABLES as _SOURCE_TABLES

_CHANGES = (
    ("20261016070000_registry_allowed_plan_scope", "registry_ptg_published_plan_scope"),
    ("20261017010000_registry_cms_source_notifications", "registry_cms_source_notification"),
    ("20261017030000_registry_ptg_office_recipe_selection", "registry_network_binding"),
    ("20261018011000_network_candidate_custody", "network_candidate_custody"),
    ("20261018040000_registry_aca_source_notifications", "registry_aca_source_notification"),
)
_EXISTING = ("registry_ptg_published_plan_scope", "registry_network_binding")


def _acl_manifest(connection, schema):
    return [
        tuple(row)
        for row in connection.execute(
            text("""SELECT c.oid,c.relname,c.relowner,c.relacl::text,
    ARRAY(SELECT ROW(a.attname,a.attacl::text)::text FROM pg_attribute a WHERE a.attrelid=c.oid
      AND a.attnum>0 AND NOT a.attisdropped AND a.attname<>'office_recipe_json' ORDER BY a.attnum)
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname=:schema AND c.relname IN ('registry_ptg_published_plan_scope','registry_network_binding')
    ORDER BY c.relname"""),
            {"schema": schema},
        )
    ]


def _prepare(connection, schema):
    """Apply the five protected changes, preserving existing grants and Alembic versions."""
    before = _acl_manifest(connection, schema)
    for revision, table in (
        ("20261009040000_registry_ptg_published_plan_scope", _EXISTING[0]),
        ("20261007130000_registry_network_bindings", _EXISTING[1]),
    ):
        migration = registry_migration(revision)
        count = 2 if table == "registry_network_binding" else 1
        validate_prepared_registry_table(connection, schema, table, lambda name: migration._ddl(name)[:count])
    for revision, table in _CHANGES:
        if table == "network_candidate_custody":
            prepare_candidate_custody_table(connection, schema)
            continue
        migration = registry_migration(revision)
        if table == "registry_aca_source_notification":
            migration._create(connection, schema)
        else:
            arguments = {"include_allowed": True} if table == "registry_ptg_published_plan_scope" else {}
            for statement in migration._ddl(schema, **arguments):
                connection.exec_driver_sql(statement)
        validate_prepared_registry_table(connection, schema, table, migration._postimage)
    if _acl_manifest(connection, schema) != before:
        raise RuntimeError("Registry preparation changed existing identities or privileges")
    return {"component": "registry_migration_preparation", "prepared_revisions": [row[0] for row in _CHANGES]}


async def prepare_registry_migrations(connection, *, owner_role, control_schema):
    """Require external caller admission/drain, then fail fast on remaining native work."""
    if not connection.in_transaction():
        raise ValueError("Registry preparation requires a caller transaction")
    namespace = _identifier(control_schema)
    async with connection.begin_nested():
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        caller = await driver.fetchval("SELECT quote_ident(current_user)")
        await assume_registry_source_owner(driver, owner_role, control_schema=control_schema)
        if not await driver.fetchval("SELECT pg_try_advisory_xact_lock($1)", _fence_key(control_schema)):
            raise RuntimeError("Registry preparation company approval is active")
        # Preserve the existing company-fence-before-revision-row ordering.
        await driver.fetchval(f"SELECT id FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE NOWAIT")
        for table in ("registry_publication_request", *_SOURCE_TABLES):
            await driver.execute(f"LOCK TABLE {namespace}.{_identifier(table)} IN SHARE MODE NOWAIT")
        if await driver.fetchval(
            f"SELECT EXISTS(SELECT FROM {namespace}.registry_publication_request WHERE state IN ('queued','running'))"
        ):
            raise RuntimeError("Registry preparation requires drained publication and office requests")
        for table in _EXISTING:
            await driver.execute(f"LOCK TABLE {namespace}.{_identifier(table)} IN ACCESS EXCLUSIVE MODE NOWAIT")
        receipt = await connection.run_sync(_prepare, control_schema)
        await driver.execute(f"SET LOCAL ROLE {caller}")
        return receipt


def run_command(arguments=None):
    """Prepare exact storage using existing operator credentials after scoped writer drain."""
    import asyncio
    import json
    import sys

    from db.connection import Database
    from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
    from process.registry_source_import import _ReceiptArgumentParser

    parser = _ReceiptArgumentParser(description=run_command.__doc__, allow_abbrev=False)
    parser.add_argument("--control-schema", required=True)
    parser.add_argument("--owner-role", required=True)
    parsed = parser.parse_args(arguments)

    async def prepare():
        """Use existing operator configuration and release the connection on every outcome."""
        database = Database()
        with _receipt_only_database_output(database):
            try:
                await database.connect()
                _set_engine_echo(database, False)
                async with database.engine.begin() as connection:
                    await connection.execute(text("SET LOCAL statement_timeout='3s'"))
                    await connection.execute(text("SET LOCAL lock_timeout='500ms'"))
                    return await prepare_registry_migrations(connection, **vars(parsed))
            finally:
                await database.disconnect()

    try:
        receipt = asyncio.run(prepare())
    except Exception:
        print('{"status":"error","code":"registry_preparation_failed"}', file=sys.stderr)
        return 1
    print(json.dumps({"status": "ok", **receipt}, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
