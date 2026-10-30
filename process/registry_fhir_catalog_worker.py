# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One control-owned catalog reconciliation using server-owned role configuration."""

import asyncio
import json
import os
from contextlib import AsyncExitStack
from dataclasses import dataclass

import click

from db.plan_release_storage import storage_identifier
from process.ext.utils import db_startup
from process.registry_fhir_catalog_grant import parse_fhir_catalog_standing_grant
from process.registry_fhir_catalog_reporting import fhir_catalog_run_reporting
from process.registry_ptg_scope_runtime import parse_registry_database_url
from process.registry_publication_worker import (
    parse_publication_consumer_config,
    unique_json_fields,
)

CONFIG_ENV = "HLTHPRT_REGISTRY_FHIR_CATALOG_SYNC"
_DSNS = ("source_reader_dsn", "loader_dsn", "sealer_dsn", "lifecycle_dsn", "execution_dsn")
_FIELDS = frozenset(_DSNS) | {"consumer", "standing_grant", "loader_role", "reader_roles", "operation_seconds"}
_SOURCE_RUNTIME_CONTEXT_KEY = "_registry_fhir_catalog_source_runtime"


@dataclass(frozen=True, repr=False)
class RegistryFHIRCatalogSyncConfig:
    source_url: object
    loader_url: object
    sealer_url: object
    lifecycle_url: object
    execution_url: object
    consumer: object
    grant: object
    loader_role: str
    reader_roles: tuple[str, ...]
    operation_seconds: int = 4200


@dataclass(repr=False)
class _OwnedCatalogSourceRuntime:
    """Source resources created by this worker for this exact context."""

    resources: AsyncExitStack
    owner_context: dict
    factory: object = None


def _invalid():
    raise ValueError("registry_fhir_catalog_sync_configuration_invalid")


def parse_fhir_catalog_sync_config(encoded, *, source_export=None):
    """Keep connections, policies and roles out of caller-controlled job parameters."""
    if source_export is not None and not callable(source_export):
        _invalid()
    config = _validated_fhir_catalog_sync_config(encoded)
    if config.source_url is None and source_export is None:
        _invalid()
    return config


def _validated_fhir_catalog_sync_config(encoded):
    """Validate server-owned configuration before any owned connection is opened."""
    try:
        if type(encoded) is not str or not 0 < len(encoded.encode()) <= 1048576:
            _invalid()
        document = json.loads(encoded, object_pairs_hook=unique_json_fields, parse_constant=lambda _value: _invalid())
        if type(document) is not dict or set(document) != _FIELDS:
            _invalid()
        urls = tuple(
            None
            if field == "source_reader_dsn" and document[field] is None
            else parse_registry_database_url(document[field])
            for field in _DSNS
        )
        connected_urls = tuple(url for url in urls if url is not None)
        consumer = parse_publication_consumer_config(document["consumer"])
        grant = parse_fhir_catalog_standing_grant(document["standing_grant"])
        loader = document["loader_role"]
        readers = document["reader_roles"]
        if type(readers) is not list or not 1 <= len(readers) <= 32 or tuple(sorted(set(readers))) != tuple(readers):
            _invalid()
        for role in (loader, *readers):
            storage_identifier(role)
        if (urls[1].username, urls[3].username, urls[4].username) != (
            loader,
            consumer.lifecycle_role,
            consumer.execution_role,
        ) or grant.control_schema != consumer.control_schema:
            _invalid()
        if (
            len({url.username for url in connected_urls}) != len(connected_urls)
            or len({url.database for url in connected_urls}) != 1
        ):
            _invalid()
        seconds = document["operation_seconds"]
        if type(seconds) is not int or not 60 <= seconds <= 86400:
            _invalid()
        return RegistryFHIRCatalogSyncConfig(*urls, consumer, grant, loader, tuple(readers), seconds)
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        _invalid()


def validate_fhir_catalog_sync_params(params):
    """Require an empty parameter object for the standing-policy catalog sync."""
    if type(params) is not dict or params:
        raise ValueError("registry_fhir_catalog_sync_parameters_invalid")


async def run_fhir_catalog_sync(config, *, source_export=None):
    """Load the connected batch pipeline only after configuration is validated."""
    from process.registry_fhir_catalog_sync import reconcile_fhir_catalog

    if type(config) is not RegistryFHIRCatalogSyncConfig:
        _invalid()
    with fhir_catalog_run_reporting() as report:
        async with asyncio.timeout(config.operation_seconds):
            if source_export is None:
                result = await reconcile_fhir_catalog(config)
            else:
                result = await reconcile_fhir_catalog(config, source_export=source_export)
        report.finish(result["state"])
        return {**result, "audit": report.audit}


async def control_start(ctx, task):
    """Reuse normal control cancellation and terminal accounting for one metadata job."""
    from process.control_cancel import raise_if_cancelled
    from process.registry_cms_source_worker import watch_registry_control_cancel

    if type(task) is not dict or set(task) != {"run_id"} or type(task["run_id"]) is not str:
        raise ValueError("registry_fhir_catalog_sync_parameters_invalid")
    if not task["run_id"] or task["run_id"].strip() != task["run_id"] or len(task["run_id"]) > 128:
        raise ValueError("registry_fhir_catalog_sync_parameters_invalid")
    source_export = ctx.get("registry_fhir_catalog_source_export")
    producer_options = {"source_export": source_export} if source_export is not None else {}
    config = parse_fhir_catalog_sync_config(os.environ.get(CONFIG_ENV), **producer_options)
    await raise_if_cancelled(ctx, task)
    with fhir_catalog_run_reporting(ctx.setdefault("context", {})):
        work = asyncio.create_task(run_fhir_catalog_sync(config, **producer_options))
        watcher = asyncio.create_task(watch_registry_control_cancel(ctx, task))
        try:
            await asyncio.wait((work, watcher), return_when=asyncio.FIRST_COMPLETED)
            if work.done():
                return await work
            await watcher
        finally:
            for owned in (work, watcher):
                if not owned.done():
                    owned.cancel()
            await asyncio.gather(work, watcher, return_exceptions=True)


async def fhir_catalog_sync_startup(ctx):
    """Bind Source-owned resources only for the validated null-reader configuration."""
    encoded = os.environ.get(CONFIG_ENV)
    config = _validated_fhir_catalog_sync_config(encoded)
    if config.source_url is not None:
        await db_startup(ctx)
        return
    if _SOURCE_RUNTIME_CONTEXT_KEY in ctx or "registry_fhir_catalog_source_export" in ctx:
        _invalid()
    from directory_catalog_runtime import catalog_source_export_runtime

    owned = _OwnedCatalogSourceRuntime(AsyncExitStack(), ctx)
    ctx[_SOURCE_RUNTIME_CONTEXT_KEY] = owned
    try:
        factory = await owned.resources.enter_async_context(catalog_source_export_runtime())
        parse_fhir_catalog_sync_config(encoded, source_export=factory)
        owned.factory = factory
        ctx["registry_fhir_catalog_source_export"] = factory
        await db_startup(ctx)
    except BaseException as error:
        await _close_fhir_catalog_source_runtime(ctx, owned, error)
        raise


async def _close_fhir_catalog_source_runtime(ctx, owned, error=None):
    if type(owned) is not _OwnedCatalogSourceRuntime or owned.owner_context is not ctx:
        return
    if ctx.get(_SOURCE_RUNTIME_CONTEXT_KEY) is owned:
        del ctx[_SOURCE_RUNTIME_CONTEXT_KEY]
    if owned.factory is not None and ctx.get("registry_fhir_catalog_source_export") is owned.factory:
        del ctx["registry_fhir_catalog_source_export"]
    if error is None:
        await owned.resources.aclose()
        return
    try:
        await owned.resources.__aexit__(type(error), error, error.__traceback__)
    except BaseException:
        error.add_note("Catalog Source startup cleanup failed")


async def fhir_catalog_sync_shutdown(ctx):
    """Drain the Source-owned runtime installed by this worker's startup hook."""
    await _close_fhir_catalog_source_runtime(ctx, ctx.get(_SOURCE_RUNTIME_CONTEXT_KEY))


class _CatalogCommand(click.Command):
    def parse_args(self, context, arguments):
        """Accept only the parameter-free sync command or its help request."""
        if arguments and arguments != ["--help"]:
            context.exit(2)
        return super().parse_args(context, arguments)


@click.command(name="registry-fhir-catalog-sync", cls=_CatalogCommand)
def registry_fhir_catalog_sync_command():
    """Reconcile all admitted catalog metadata; never acquire FHIR resources."""
    from process import run_import

    config = parse_fhir_catalog_sync_config(os.environ.get(CONFIG_ENV))
    print(json.dumps(run_import(run_fhir_catalog_sync(config)), sort_keys=True))
