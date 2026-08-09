# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Catalog assertions for reviewed subset abandonment adoption."""

from __future__ import annotations

import asyncpg

EXPECTED_ABANDONMENT_FUNCTIONS = {
    "provider_directory_subset_abandonment_valid",
    "guard_provider_directory_subset_abandonment_dataset",
    "guard_provider_directory_subset_abandonment_child",
    "guard_provider_directory_subset_abandonment_checkpoint",
}
EXPECTED_ABANDONMENT_TRIGGERS = {
    "pd_subset_abandonment_dataset_guard",
    "pd_subset_abandonment_dataset_consistency_guard",
    "pd_subset_abandonment_dataset_truncate_guard",
    "pd_subset_abandonment_resource_insert_guard",
    "pd_subset_abandonment_resource_update_guard",
    "pd_subset_abandonment_resource_delete_guard",
    "pd_subset_abandonment_proof_insert_guard",
    "pd_subset_abandonment_proof_update_guard",
    "pd_subset_abandonment_proof_delete_guard",
    "pd_subset_abandonment_proof_truncate_guard",
    "pd_subset_abandonment_bulk_insert_guard",
    "pd_subset_abandonment_bulk_update_guard",
    "pd_subset_abandonment_bulk_delete_guard",
    "pd_subset_abandonment_bulk_truncate_guard",
    "pd_subset_abandonment_checkpoint_guard",
    "provider_directory_subset_abandonment_checkpoint_guard",
    "pd_subset_abandonment_checkpoint_truncate_guard",
}


async def _connect(database_url):
    return await asyncpg.connect(
        host=str(database_url.host),
        port=int(database_url.port or 5432),
        user=str(database_url.username),
        password=str(database_url.password or ""),
        database=str(database_url.database),
    )


async def _abandonment_object_shape_records(
    database_url,
    schema_name: str,
) -> tuple[list, list]:
    connection = await _connect(database_url)
    try:
        function_records = await connection.fetch(
            """
            SELECT function_row.proname, function_row.prosecdef,
                   function_row.proconfig
              FROM pg_catalog.pg_proc AS function_row
              JOIN pg_catalog.pg_namespace AS function_namespace
                ON function_namespace.oid = function_row.pronamespace
             WHERE function_namespace.nspname = $1
               AND function_row.proname = ANY($2::text[])
            """,
            schema_name,
            sorted(EXPECTED_ABANDONMENT_FUNCTIONS),
        )
        trigger_records = await connection.fetch(
            """
            SELECT trigger_row.tgname,
                   trigger_row.tgenabled::text AS tgenabled
              FROM pg_catalog.pg_trigger AS trigger_row
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid = trigger_row.tgrelid
              JOIN pg_catalog.pg_namespace AS relation_namespace
                ON relation_namespace.oid = relation.relnamespace
             WHERE relation_namespace.nspname = $1
               AND trigger_row.tgname = ANY($2::text[])
               AND trigger_row.tgisinternal IS FALSE
            """,
            schema_name,
            sorted(EXPECTED_ABANDONMENT_TRIGGERS),
        )
    finally:
        await connection.close()
    return function_records, trigger_records


async def assert_abandonment_object_shapes(
    database_url,
    schema_name: str,
) -> None:
    """Require all abandonment objects to remain hardened and ALWAYS."""

    function_records, trigger_records = await _abandonment_object_shape_records(
        database_url,
        schema_name,
    )
    assert {
        function_record["proname"] for function_record in function_records
    } == EXPECTED_ABANDONMENT_FUNCTIONS
    assert all(
        function_record["prosecdef"] is True for function_record in function_records
    )
    assert all(
        function_record["proconfig"] == ["search_path=pg_catalog"]
        for function_record in function_records
    )
    assert {
        trigger_record["tgname"] for trigger_record in trigger_records
    } == EXPECTED_ABANDONMENT_TRIGGERS
    assert all(trigger_record["tgenabled"] == "A" for trigger_record in trigger_records)
