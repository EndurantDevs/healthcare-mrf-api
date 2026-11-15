# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fence selected generic publication heads using an existing native metadata actor."""

from contextlib import asynccontextmanager

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.network_cms_registry_address_capture import native_driver
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable

_TABLES = {
    "provider_directory_api_endpoint": ("endpoint_id",),
    "provider_directory_source": ("source_id", "endpoint_id"),
    "provider_directory_endpoint_dataset": ("dataset_id", "endpoint_id", "status", "is_current"),
}


def _require(condition):
    if not condition:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_head_fence_unavailable")


async def _lock_rows(driver, schema, endpoints):
    namespace = _identifier(schema)
    endpoint_ids = sorted({endpoint[2] for endpoint in endpoints})
    source_by_id = {endpoint[1]: endpoint[2] for endpoint in endpoints}
    _require(len(source_by_id) == len(endpoints))
    actual_endpoints = await driver.fetch(
        f"SELECT endpoint_id FROM {namespace}.provider_directory_api_endpoint WHERE endpoint_id=ANY($1::text[]) "
        'ORDER BY endpoint_id COLLATE "C" FOR SHARE NOWAIT',
        endpoint_ids,
    )
    _require([record["endpoint_id"] for record in actual_endpoints] == endpoint_ids)
    actual_sources = await driver.fetch(
        f"SELECT source_id,endpoint_id FROM {namespace}.provider_directory_source WHERE source_id=ANY($1::text[]) "
        'ORDER BY source_id COLLATE "C" FOR SHARE NOWAIT',
        sorted(source_by_id),
    )
    _require({record["source_id"]: record["endpoint_id"] for record in actual_sources} == source_by_id)
    actual_heads = await driver.fetch(
        f"SELECT endpoint_id,dataset_id FROM {namespace}.provider_directory_endpoint_dataset "
        "WHERE endpoint_id=ANY($1::text[]) AND is_current IS TRUE AND status='published' "
        'ORDER BY endpoint_id COLLATE "C" FOR SHARE NOWAIT',
        endpoint_ids,
    )
    _require([record["endpoint_id"] for record in actual_heads] == endpoint_ids)


@asynccontextmanager
async def generic_refresh_head_fence(readers, endpoints):
    """Hold the existing advisory and exact row locks using ordinary configured PostgreSQL access."""
    if not endpoints:
        yield
        return
    from process.registry_company_approval_fence import _company_approval_connection

    sessions = readers.head_fences.generic_sessions
    _require(sessions is not None and type(endpoints) is tuple and endpoints == tuple(sorted(set(endpoints))))
    async with _company_approval_connection(sessions) as connection:
        async with sessions(bind=connection) as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE"))
            await session.execute(text("SET LOCAL lock_timeout='250ms'"))
            await session.execute(text("SET LOCAL statement_timeout='3s'"))
            driver = await native_driver(session)
            for endpoint in sorted({selected[2] for selected in endpoints}):
                acquired = await driver.fetchval(
                    "SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))", endpoint
                )
                _require(acquired is True)
            for schema in sorted({selected[0] for selected in endpoints}):
                await _lock_rows(driver, schema, tuple(selected for selected in endpoints if selected[0] == schema))
            yield
