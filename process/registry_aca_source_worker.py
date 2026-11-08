# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run one accepted ACA refresh inside the existing genuine Source bootstrap."""

import asyncio
import json
import os
import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace

import asyncpg
import click
from sqlalchemy.exc import ArgumentError

from db.connection import db
from process import provider_directory_generic_source_policy as source_policy
from process.network_address_projection import _identifier
from process.registry_aca_source_consumer import (
    RegistryACASourceConsumerConfig,
    consume_registry_aca_source_notification_once,
    source_authority_roles,
)
from process.registry_aca_source_notifications import (
    RegistryACASourceNotificationStore,
    validate_registry_aca_source_event,
)
from process.registry_cms_source_worker import (
    _IDENTITY_SQL,
    _endpoint,
    _object,
    _roles,
    _unique,
    _validate_ptg,
    _watch_control_cancel,
)
from process.registry_ptg_scope_runtime import _database_url
from process.registry_publication_ptg_runtime import RegistryPublicationSQLLimits
from process.registry_publication_worker import (
    _BoundedPool,
    _close_pool,
    _custody_config_url,
    _initial_sources,
    _pool_options,
    _ptg_config,
    _qualified_worker_runtime,
    _sql_limits,
    _verify_custody_config,
    _worker_backend_qualification,
)

CONFIG_ENV = "HLTHPRT_REGISTRY_ACA_SOURCE_WORKER"
_FIELDS = frozenset({"capture_id", "recipe_sha256", "notification_sha256"})
_STATES = {"completed", "replayed", "busy", "review_required", "conflict", "stale_source"}


def _invalid(*_args):
    raise ValueError("registry_aca_source_worker_config_invalid")


@dataclass(frozen=True, repr=False)
class RegistryACASourceWorkerConfig:
    execution_url: object
    notification_url: object
    consumer: RegistryACASourceConsumerConfig
    notification_store: RegistryACASourceNotificationStore
    reader_role: str
    ptg: object = field(repr=False)
    sql_limits: RegistryPublicationSQLLimits = field(default_factory=RegistryPublicationSQLLimits)
    event: dict | None = field(default=None, repr=False)
    custody_url: object | None = field(default=None, repr=False)

    def __post_init__(self):
        if (
            type(self.consumer) is not RegistryACASourceConsumerConfig
            or self.consumer.composition_runtime is not None
            or type(self.notification_store) is not RegistryACASourceNotificationStore
            or type(self.sql_limits) is not RegistryPublicationSQLLimits
        ):
            _invalid()
        for url in (self.execution_url, self.notification_url):
            _database_url(url.render_as_string(hide_password=False))
        if (
            _endpoint(self.execution_url) != _endpoint(self.notification_url)
            or self.execution_url.username != self.consumer.execution_role
            or self.notification_url.username != self.reader_role
            or self.reader_role == self.consumer.execution_role
            or self.notification_store.control_schema != self.consumer.control_schema
            or self.reader_role not in self.notification_store.reader_roles
            or self.notification_store.publisher_role in {self.reader_role, self.consumer.execution_role}
        ):
            _invalid()
        _identifier(self.reader_role)
        _validate_ptg(self.ptg, self.consumer.control_schema)
        if self.reader_role == self.consumer.owner_role:
            _invalid()
        if self.ptg is None or self.ptg.store.control_schema != self.consumer.control_schema:
            _invalid()
        _verify_custody_config(self)
        if self.event is not None:
            validate_registry_aca_source_event(self.event)

    @property
    def source_authority_roles(self):
        """Expose only the runtime-derived genuine Source census to final custody."""
        return self.consumer.source_authority_roles


def parse_registry_aca_source_worker_config(encoded):
    """The event and JSON cannot carry a source factory, callback or authority-role census."""
    try:
        if type(encoded) is not str or not 0 < len(encoded.encode()) <= 16384:
            _invalid()
        document = json.loads(encoded, object_pairs_hook=_unique, parse_constant=_invalid)
        _object(
            document,
            {"execution_dsn", "notification_dsn", "consumer", "notification_store", "reader_role", "ptg"},
            {"sql_limits", "event", "custody_dsn"},
        )
        consumer = _consumer_config(document["consumer"])
        store_fields_map = dict(document["notification_store"])
        _object(store_fields_map, {"control_schema", "publisher_role", "reader_roles"})
        store_fields_map["reader_roles"] = _roles(store_fields_map["reader_roles"])
        store = RegistryACASourceNotificationStore(**store_fields_map)
        urls = [_database_url(document[name]) for name in ("execution_dsn", "notification_dsn")]
        return RegistryACASourceWorkerConfig(
            *urls,
            consumer,
            store,
            document["reader_role"],
            _ptg_config(document["ptg"], consumer.control_schema),
            _sql_limits(document["sql_limits"]) if "sql_limits" in document else RegistryPublicationSQLLimits(),
            document.get("event"),
            _custody_config_url(document["custody_dsn"]) if "custody_dsn" in document else None,
        )
    except ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError, ArgumentError:
        _invalid()


def _consumer_config(document):
    """Select one closed logical-source, initial-batch or global-epoch worker variant."""
    consumer_fields_map = dict(document)
    required_names = {"control_schema", "execution_role", "owner_role"}
    if "initial_batch" in consumer_fields_map:
        required_names.add("initial_batch")
        if consumer_fields_map["initial_batch"] is not True:
            _invalid()
    elif "global_epoch" in consumer_fields_map:
        required_names.add("global_epoch")
        if consumer_fields_map["global_epoch"] is not True:
            _invalid()
    else:
        required_names.update({"source_id", "producer_id", "issuer_id", "year"})
    _object(
        consumer_fields_map,
        required_names,
        {"loader_roles", "reader_roles", "max_attempts", "operation_seconds", "initial_address_sources"},
    )
    for name in ("loader_roles", "reader_roles"):
        consumer_fields_map[name] = _roles(consumer_fields_map.get(name, []))
    consumer_fields_map["initial_address_sources"] = _initial_sources(
        consumer_fields_map.get("initial_address_sources")
    )
    return RegistryACASourceConsumerConfig(**consumer_fields_map)


async def _identity(connection, config, role):
    statement = _IDENTITY_SQL.replace("registry_cms_source_notification", "registry_aca_source_notification")
    native = await connection.fetchrow(
        statement, config.consumer.control_schema, config.notification_store.control_schema
    )
    if native is None:
        _invalid()
    identity_map = dict(native)
    if (
        identity_map.pop("safe_login") is not True
        or identity_map.pop("current_role") != role
        or identity_map.pop("session_role") != role
        or identity_map["control_oid"] is None
        or identity_map["inbox_oid"] is None
        or connection.is_in_transaction()
    ):
        _invalid()
    return identity_map


async def _source_config(config, execution_identity):
    """Qualify the original Source factory without keeping source fact locks during the build."""
    async with source_policy.source_transaction(db, isolation="repeatable read") as session:
        publisher = source_policy.bound_publisher()
        roles = source_authority_roles(publisher.policy)
        if config.source_authority_roles and config.source_authority_roles != roles:
            _invalid()
        from process.network_cms_registry_address_capture import native_driver

        source = await native_driver(session)
        identity_map = dict(
            await source.fetchrow(
                "SELECT current_database() AS database_name,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,inet_server_addr()::text AS server_address,inet_server_port() AS server_port,pg_postmaster_start_time() AS server_started"
            )
        )
        if any(identity_map[name] != execution_identity[name] for name in identity_map):
            _invalid()
        source_policy.source_producer_identity()
    return replace(config, consumer=replace(config.consumer, source_authority_roles=roles))


async def run_registry_aca_source_worker(config, *, event=None):
    """Use bounded pools and the genuine Source factory; refuse an unbound direct launch."""
    if type(config) is not RegistryACASourceWorkerConfig:
        _invalid()
    source_policy.source_transaction_available()
    header = dict(config.event) if event is None and config.event is not None else event
    validate_registry_aca_source_event(header)
    async with AsyncExitStack() as stack:
        reader = await asyncpg.create_pool(**_pool_options(config.notification_url, 1, config.sql_limits))
        stack.push_async_callback(_close_pool, reader)
        reader = _BoundedPool(reader, config.sql_limits.connect_timeout_seconds)
        async with reader.acquire() as notification:
            qualification = await _worker_backend_qualification(config, notification, config.reader_role)
        execution_verifier = qualification.verifier(config.consumer.execution_role)
        execution = await asyncpg.create_pool(
            **_pool_options(config.execution_url, 1, config.sql_limits, backend_verifier=execution_verifier)
        )
        stack.push_async_callback(_close_pool, execution)
        execution = _BoundedPool(execution, config.sql_limits.connect_timeout_seconds)
        async with execution.acquire() as connection:
            execution_identity = await _identity(connection, config, config.consumer.execution_role)
            config = await _source_config(config, execution_identity)
            async with reader.acquire() as notification:
                if await _identity(notification, config, config.reader_role) != execution_identity:
                    _invalid()
                async with notification.transaction(isolation="repeatable_read", readonly=True):
                    identity = await config.notification_store.qualify(notification, writing=False)
            consumer = await _qualified_worker_runtime(config, stack, qualification, connection)
            if consumer.seal_candidate is None:
                _invalid()
            await consumer.composition_runtime.qualify()
            outcome = await consume_registry_aca_source_notification_once(
                connection=connection,
                notification_pool=reader,
                store=config.notification_store,
                identity=identity,
                event=header,
                config=consumer,
            )
    if type(outcome) is not dict or set(outcome) != {"state"} or outcome["state"] not in _STATES:
        _invalid()
    return dict(outcome)


def _error(code):
    print(json.dumps({"state": "error", "code": code}, separators=(",", ":")), file=sys.stderr)


def run_command():
    """Load closed operator configuration and return the existing sanitized worker exit status."""
    try:
        config = parse_registry_aca_source_worker_config(os.environ.get(CONFIG_ENV))
    except ValueError:
        _error("invalid_configuration")
        return 2
    try:
        result = asyncio.run(run_registry_aca_source_worker(config))
    except KeyboardInterrupt, asyncio.CancelledError:
        _error("canceled")
        return 130
    except Exception:
        _error("worker_failed")
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 0


class _WorkerCommand(click.Command):
    def parse_args(self, context, arguments):
        """Reject unsupported positional arguments before starting the worker."""
        if arguments and arguments != ["--help"]:
            _error("invalid_arguments")
            context.exit(2)
        return super().parse_args(context, arguments)


@click.command(name="registry-aca-source-worker", cls=_WorkerCommand, help="Consume one accepted ACA event and exit.")
def registry_aca_source_worker_command():
    """Expose the one-shot accepted-source worker through the existing command group."""
    raise click.exceptions.Exit(run_command())


async def control_start(ctx, task):
    """Run one registered source task while retaining cancellation and exact task cleanup."""
    from process.control_cancel import ImportCancelledError, raise_if_cancelled

    try:
        _object(task, _FIELDS | {"run_id"})
        if (
            type(task["run_id"]) is not str
            or not 0 < len(task["run_id"]) <= 128
            or task["run_id"] != task["run_id"].strip()
        ):
            _invalid()
        event_by_name = {name: task[name] for name in _FIELDS}
        validate_registry_aca_source_event(event_by_name)
        config = parse_registry_aca_source_worker_config(os.environ.get(CONFIG_ENV))
        work = watcher = None
        try:
            await raise_if_cancelled(ctx, task)
            work = asyncio.create_task(run_registry_aca_source_worker(config, event=event_by_name))
            watcher = asyncio.create_task(_watch_control_cancel(ctx, task))
            await asyncio.wait((work, watcher), return_when=asyncio.FIRST_COMPLETED)
            if work.done():
                return await work
            await watcher
        finally:
            owned_tasks = tuple(operation_task for operation_task in (work, watcher) if operation_task is not None)
            for operation_task in owned_tasks:
                if not operation_task.done():
                    operation_task.cancel()
            await asyncio.gather(*owned_tasks, return_exceptions=True)
    except ImportCancelledError:
        raise
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except Exception:
        raise RuntimeError("registry_aca_source_worker_failed") from None
