# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Repair older overlay schemas without blocking readers of a complete serving table."""


async def ensure_address_overlay_table(directory, schema):
    """Emit only missing compatibility DDL, retaining the declared live index set."""
    table = directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
    columns, indexes = await directory.db.first(
        """SELECT ARRAY(SELECT attname::text FROM pg_catalog.pg_attribute
                         WHERE attrelid=to_regclass(:relation) AND attnum>0 AND NOT attisdropped),
                  ARRAY(SELECT c.relname::text FROM pg_catalog.pg_index i
                         JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
                         WHERE i.indrelid=to_regclass(:relation))""",
        relation=directory._qt(schema, table),
    )
    column_names = set(columns)
    if not column_names:
        await directory.db.status(directory.provider_directory_address_overlay_table_sql(schema))
        column_names = set(directory._provider_directory_address_overlay_columns())
    for required, statement in (
        ({"lat", "long"}, directory.address_overlay_coordinate_columns_sql),
        (
            {"formatted_address", "formatted_address_version", "formatted_address_source"},
            directory.address_overlay_formatted_address_columns_sql,
        ),
        ({"premise_key"}, directory.address_overlay_premise_key_column_sql),
    ):
        if not required <= column_names:
            await directory.db.status(statement(schema))
    expected_indexes = {
        directory._address_overlay_index_name(table, suffix)
        for suffix in directory.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_INDEX_SUFFIXES
        if suffix not in {"npi_premise_key_idx", "source_run_resource_idx"}
    }
    if not expected_indexes <= set(indexes):
        await directory._create_provider_directory_address_overlay_indexes(schema, table, include_premise_index=False)
