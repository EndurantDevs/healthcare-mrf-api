# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One control-owned catalog reconciliation using server-owned role configuration."""

import asyncio
import json
import os
from dataclasses import dataclass

import click

from db.plan_release_storage import storage_identifier
from process.registry_fhir_catalog_grant import parse_fhir_catalog_standing_grant
from process.registry_ptg_scope_runtime import parse_registry_database_url
from process.registry_publication_worker import (
    parse_publication_consumer_config,
    unique_json_fields,
)

CONFIG_ENV = "HLTHPRT_REGISTRY_FHIR_CATALOG_SYNC"
_DSNS = ("source_reader_dsn", "loader_dsn", "sealer_dsn", "lifecycle_dsn", "execution_dsn")
_FIELDS = frozenset(_DSNS) | {"consumer", "standing_grant", "loader_role", "reader_roles", "operation_seconds"}


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


def _invalid():
    raise ValueError("registry_fhir_catalog_sync_configuration_invalid")


def parse_fhir_catalog_sync_config(encoded):
    """Keep connections, policies and roles out of caller-controlled job parameters."""
    try:
        if type(encoded) is not str or not 0 < len(encoded.encode()) <= 1048576:
            _invalid()
        document = json.loads(encoded, object_pairs_hook=unique_json_fields, parse_constant=lambda _value: _invalid())
        if type(document) is not dict or set(document) != _FIELDS:
            _invalid()
        urls = tuple(parse_registry_database_url(document[field]) for field in _DSNS)
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
        if len({url.username for url in urls}) != len(urls) or len({url.database for url in urls}) != 1:
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


async def run_fhir_catalog_sync(config):
    """Load the connected batch pipeline only after configuration is validated."""
    from process.registry_fhir_catalog_sync import reconcile_fhir_catalog

    if type(config) is not RegistryFHIRCatalogSyncConfig:
        _invalid()
    async with asyncio.timeout(config.operation_seconds):
        return await reconcile_fhir_catalog(config)


async def control_start(ctx, task):
    """Reuse normal control cancellation and terminal accounting for one metadata job."""
    from process.control_cancel import raise_if_cancelled
    from process.registry_cms_source_worker import watch_registry_control_cancel

    if type(task) is not dict or set(task) != {"run_id"} or type(task["run_id"]) is not str:
        raise ValueError("registry_fhir_catalog_sync_parameters_invalid")
    if not task["run_id"] or task["run_id"].strip() != task["run_id"] or len(task["run_id"]) > 128:
        raise ValueError("registry_fhir_catalog_sync_parameters_invalid")
    config = parse_fhir_catalog_sync_config(os.environ.get(CONFIG_ENV))
    await raise_if_cancelled(ctx, task)
    work = asyncio.create_task(run_fhir_catalog_sync(config))
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
