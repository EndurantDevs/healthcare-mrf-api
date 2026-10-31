# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-local selection of the existing CMS snapshot allocator branch.

This grants no authority and creates no objects. The original CMS admin must
separately authenticate its claim/profile and own the complete atomic cut.
"""

from contextlib import asynccontextmanager

from db.migration_cms_snapshot_allocator import _ADMIN_SQL, SELECTOR, _require, require_cms_snapshot_allocator
from process.network_cms_registry_address_capture import NativeReceiptSession
from process.provider_directory_generic_source_policy import _savepoint


@asynccontextmanager
async def cms_snapshot_allocation(connection, dataset_id, *, catalog_schema, expected_allocator):
    """Select only one dataset on an unchanged original-admin transaction."""
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    _require(type(dataset_id) is str and 0 < len(dataset_id) <= 96 and "\0" not in dataset_id, "dataset_required")
    _require(await NativeReceiptSession(connection).scalar(_ADMIN_SQL) is True, "original_admin_required")
    expected_by_field = dict(expected_allocator)
    async with _savepoint(connection):
        await require_cms_snapshot_allocator(connection, catalog_schema, expected=expected_by_field)
        setting = await connection.fetchval("SELECT pg_catalog.current_setting($1,true)", SELECTOR)
        _require(setting in (None, ""), "selector_already_set")
        await connection.fetchval("SELECT pg_catalog.set_config($1,$2,true)", SELECTOR, dataset_id)
        yield
        _require(
            await connection.fetchval("SELECT pg_catalog.current_setting($1,true)", SELECTOR) == dataset_id
            and expected_allocator == expected_by_field,
            "selector_changed",
        )
        _require(await NativeReceiptSession(connection).scalar(_ADMIN_SQL) is True, "original_admin_required")
        await connection.fetchval("SELECT pg_catalog.set_config($1,'',true)", SELECTOR)
