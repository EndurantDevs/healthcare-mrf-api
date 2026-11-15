# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Apply actual named counter helpers to intentionally bounded native fixtures."""

from sqlalchemy import text

from tests.test_provider_directory_cms_serving_receipt_postgres import _migration


async def upgrade_named_native_counter(connection, schema, *, full_serving=False):
    """Preserve real rows/guards; partial fixtures do not claim full matcher qualification."""
    physical = await connection.scalar(
        text(
            "SELECT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass(:name) AND attname='relation_oid' AND NOT attisdropped)"
        ),
        {"name": f'"{schema}".cms_native_input_revision'},
    )
    if not physical:
        return
    mutation = _migration("20261019130000")
    await connection.execute(text(mutation._counter_sql(schema)))
    matcher = await connection.scalar(
        text("SELECT to_regprocedure(:name) IS NOT NULL"),
        {"name": f'"{schema}".cms_serving_archive_result_matches(jsonb)'},
    )
    if matcher:
        await connection.execute(text(mutation._repair_sql(schema)))
    named = _migration("20261114190000")
    check, key = named._key_sql(schema)
    await connection.execute(text(check))
    names = ("cms_native_input_advance", "cms_native_input_revision_guard")
    if matcher:
        names += ("cms_serving_archive_result_matches",)
    await connection.execute(text(named._function_sql(schema, names=None if full_serving else names)))
    await connection.execute(text(key))
