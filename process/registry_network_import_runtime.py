# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install native Source discovery and prepare the coupled catalogue candidate."""

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace

import asyncpg
from sqlalchemy.exc import DBAPIError


@dataclass(frozen=True, repr=False)
class NativeNetworkImportRuntime:
    """Worker-owned Source inventory and same-lifetime publisher capabilities."""

    inventory: object
    freeze_preparation: object

    @property
    def catalog_export(self):
        """Use the genuine export bound to the existing Source owner."""
        return self.inventory.catalog_export

    @property
    def config(self):
        """Expose the fixed server-owned inventory profile."""
        return self.inventory.config


FREEZE_ATTEMPTS = 40
FREEZE_RETRY_SECONDS = 30
_CONTENTION_SQLSTATES = frozenset({"55P03", "57014", "40P01"})  # lock not available, statement timeout, deadlock


def _is_lock_contention(error):
    if isinstance(error, TimeoutError):
        return True
    cause = error.orig if isinstance(error, DBAPIError) else error
    codes = {getattr(cause, "sqlstate", None), getattr(cause.__cause__, "sqlstate", None)}
    return not _CONTENTION_SQLSTATES.isdisjoint(codes)


async def _retried_freeze(freeze_preparation, handoff, preparation_roles):
    # The freeze takes short exclusive locks on freshly loaded tables that autovacuum may hold for minutes.
    # A failed attempt rolls back and a committed freeze returns its state, so retrying is safe.
    for attempt in range(1, FREEZE_ATTEMPTS + 1):
        try:
            return await freeze_preparation(handoff, preparation_roles)
        except (TimeoutError, DBAPIError, asyncpg.PostgresError) as error:
            if attempt == FREEZE_ATTEMPTS or not _is_lock_contention(error):
                raise
            await asyncio.sleep(FREEZE_RETRY_SECONDS)


async def freeze_network_import_preparation(freeze_preparation, handoff, preparation_roles):
    """Freeze only the exact own catalog or network handoff using the existing publisher."""
    if (
        not callable(freeze_preparation)
        or type(handoff) is not dict
        or handoff.get("kind") not in {"catalog", "network"}
    ):
        raise ValueError("registry_imported_freeze_capability_invalid")
    receipt = await _retried_freeze(freeze_preparation, handoff, preparation_roles)
    if (
        type(receipt) is not dict
        or set(receipt) != {"preparation_id", "state"}
        or receipt["preparation_id"] != handoff["candidate_id"]
        or receipt["state"] not in {"frozen", "validated", "published"}
    ):
        raise ValueError("registry_imported_freeze_result_invalid")
    return receipt


@asynccontextmanager
async def native_network_import_runtime(worker_config, cleanup):
    """Open genuine factories once and keep their lifetime inside the admitted run."""
    from directory_catalog_runtime import network_import_source_runtime

    from process.registry_fhir_catalog_worker import CONFIG_ENV as CATALOG_CONFIG_ENV
    from process.registry_fhir_catalog_worker import parse_fhir_catalog_sync_config
    from process.registry_imported_source_inventory import (
        ImportedSourceInventoryRuntime,
        imported_source_inventory_runtime,
    )
    from process.registry_membership_refresh_runtime import CONFIG_ENV, _settings
    from process.registry_publication_worker import _worker_sessions

    settings = _settings(os.environ.get(CONFIG_ENV))
    if settings.inventory_sources is None or settings.app_origin is not None:
        raise ValueError("registry_imported_native_inventory_required")
    capabilities = await cleanup.enter_async_context(network_import_source_runtime())
    if (
        type(capabilities) is not dict
        or set(capabilities) != {"catalog_export", "freeze_preparation"}
        or not all(callable(capability) for capability in capabilities.values())
    ):
        raise ValueError("registry_imported_freeze_capability_invalid")
    factory = capabilities["catalog_export"]
    catalog = parse_fhir_catalog_sync_config(os.environ.get(CATALOG_CONFIG_ENV), source_export=factory)
    if catalog.source_endpoint_ids is not None or catalog.consumer.control_schema != settings.control_schema:
        raise ValueError("registry_imported_catalog_configuration_invalid")
    runtime = ImportedSourceInventoryRuntime(
        settings.inventory_sources,
        _worker_sessions(worker_config, cleanup, settings.source_reader_url),
        factory,
        control_schema=settings.control_schema,
    )
    with imported_source_inventory_runtime(runtime):
        yield catalog, NativeNetworkImportRuntime(runtime, capabilities["freeze_preparation"])


async def prepare_network_import_catalog(catalog, runtime, cleanup):
    """Reuse the isolated catalogue builder; publish its pointer only with memberships."""
    from process.registry_imported_catalog_preparation import prepare_imported_catalog_snapshot
    from process.registry_imported_source_inventory import current_imported_source_inventory

    inventory = await current_imported_source_inventory()
    pools = []
    for url, maximum in ((catalog.source_url, 1), (catalog.loader_url, 1), (catalog.sealer_url, 2)):
        pools.append(
            None
            if url is None
            else await cleanup.enter_async_context(
                asyncpg.create_pool(
                    url.set(drivername="postgresql").render_as_string(hide_password=False),
                    min_size=1,
                    max_size=maximum,
                )
            )
        )
    source_pool, loader_pool, sealer_pool = pools
    async with loader_pool.acquire() as loader:
        key = "registry-fhir-catalog:" + catalog.consumer.control_schema
        if not await loader.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", key):
            raise ValueError("registry_fhir_catalog_sync_already_running")
        try:
            prepared = await prepare_imported_catalog_snapshot(
                catalog,
                loader,
                source_pool,
                sealer_pool,
                source_export=runtime.catalog_export,
                inventory=inventory,
            )
            return await _finish_import_catalog(catalog, runtime, prepared, sealer_pool)
        finally:
            await loader.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", key)


async def _finish_import_catalog(catalog, runtime, prepared, sealer_pool):
    from process.registry_fhir_catalog_custody import FHIRCatalogTarget
    from process.registry_fhir_catalog_store import (
        catalog_snapshot_preparation,
        named_catalog_preparation,
        named_catalog_preparation_roles,
    )
    from process.registry_fhir_catalog_sync import _seal_catalog_snapshot

    if prepared.accounting.get("preparation_state") != "handed_off":
        return prepared
    handoff = named_catalog_preparation(prepared.snapshot, catalog.consumer.control_schema)
    await freeze_network_import_preparation(
        runtime.freeze_preparation, handoff, named_catalog_preparation_roles(catalog)
    )
    target = FHIRCatalogTarget(**catalog_snapshot_preparation(prepared.snapshot)["target"])
    state = await _seal_catalog_snapshot(catalog, target, prepared.snapshot, sealer_pool)
    if state != "validated":
        raise ValueError("registry_imported_catalog_preparation_incomplete")
    return replace(prepared, accounting={**prepared.accounting, "preparation_state": state})
