# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run an exact accepted PTG event with trusted roles and owned bounded connections."""

import asyncio
import json
import os
import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass, field

import asyncpg
import click
from sqlalchemy.exc import ArgumentError

from process.network_address_projection import _identifier
from process.registry_cms_source_worker import _IDENTITY_SQL, _endpoint, _object, _roles, _unique, _watch_control_cancel
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_ptg_scope_runtime import _database_url
from process.registry_ptg_source_consumer import (
    RegistryPTGSourceConsumerConfig,
    consume_registry_ptg_source_notification_once,
)
from process.registry_ptg_source_notifications import (
    qualify_registry_ptg_source_notifications,
    validate_registry_ptg_source_event,
)
from process.registry_publication_ptg_runtime import RegistryPublicationSQLLimits
from process.registry_publication_worker import (
    _BoundedPool,
    _close_pool,
    _custody_config_url,
    _pool_options,
    _ptg_config,
    _qualified_worker_runtime,
    _sql_limits,
    _verify_custody_config,
    _worker_backend_qualification,
)

CONFIG_ENV = "HLTHPRT_REGISTRY_PTG_SOURCE_WORKER"
_FIELDS = frozenset({"capture_id", "approval_sha256", "hold_sha256"})
_STATES = {"completed", "replayed", "busy", "review_required", "conflict"}


def _invalid(*_args):
    raise ValueError("registry_ptg_source_worker_config_invalid")


@dataclass(frozen=True, repr=False)
class RegistryPTGSourceWorkerConfig:
    execution_url: object
    notification_url: object
    consumer: RegistryPTGSourceConsumerConfig
    notification_store: RegistryPTGProducerScopeStore
    reader_role: str
    ptg: object = field(repr=False)
    sql_limits: RegistryPublicationSQLLimits = field(default_factory=RegistryPublicationSQLLimits)
    event: dict | None = field(default=None, repr=False)
    custody_url: object | None = field(default=None, repr=False)

    def __post_init__(self):
        if (
            type(self.consumer) is not RegistryPTGSourceConsumerConfig
            or self.consumer.composition_runtime is not None
            or type(self.notification_store) is not RegistryPTGProducerScopeStore
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
            or self.reader_role
            in {self.consumer.owner_role, self.notification_store.owner_role, self.notification_store.approval_role}
        ):
            _invalid()
        from process.registry_cms_source_worker import _validate_ptg

        _validate_ptg(self.ptg, self.consumer.control_schema)
        _identifier(self.reader_role)
        _verify_custody_config(self)
        if self.consumer.execution_role in {self.notification_store.owner_role, self.notification_store.approval_role}:
            _invalid()
        if self.ptg is None or self.ptg.store != self.notification_store:
            _invalid()
        if self.event is not None:
            validate_registry_ptg_source_event(self.event)


def parse_registry_ptg_source_worker_config(encoded):
    """No event can override this closed operator-owned configuration."""
    try:
        if type(encoded) is not str or not 0 < len(encoded.encode()) <= 16384:
            _invalid()
        document = json.loads(encoded, object_pairs_hook=_unique, parse_constant=_invalid)
        _object(
            document,
            {"execution_dsn", "notification_dsn", "consumer", "ptg", "reader_role"},
            {"sql_limits", "event", "custody_dsn"},
        )
        consumer_by_name = document["consumer"]
        _object(
            consumer_by_name,
            {"control_schema", "execution_role", "owner_role"},
            {"loader_roles", "reader_roles", "max_attempts", "operation_seconds"},
        )
        consumer_by_name = dict(consumer_by_name)
        for key in ("loader_roles", "reader_roles"):
            consumer_by_name[key] = _roles(consumer_by_name.get(key, []))
        consumer_config = RegistryPTGSourceConsumerConfig(**consumer_by_name)
        urls = []
        for key in ("execution_dsn", "notification_dsn"):
            dsn_value = document[key]
            if type(dsn_value) is not str or not 0 < len(dsn_value) <= 4096:
                _invalid()
            urls.append(_database_url(dsn_value))
        ptg = _ptg_config(document["ptg"], consumer_config.control_schema)
        return RegistryPTGSourceWorkerConfig(
            *urls,
            consumer_config,
            ptg.store,
            document["reader_role"],
            ptg,
            _sql_limits(document["sql_limits"]) if "sql_limits" in document else RegistryPublicationSQLLimits(),
            document.get("event"),
            custody_url=_custody_config_url(document["custody_dsn"]) if "custody_dsn" in document else None,
        )
    except ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError, ArgumentError:
        _invalid()


async def _identity(connection, config, role):
    identity_by_name = dict(
        await connection.fetchrow(
            _IDENTITY_SQL, config.consumer.control_schema, config.notification_store.control_schema
        )
    )
    if (
        identity_by_name.pop("safe_login") is not True
        or identity_by_name.pop("current_role") != role
        or identity_by_name.pop("session_role") != role
        or identity_by_name["control_oid"] is None
        or identity_by_name["inbox_oid"] is None
        or connection.is_in_transaction()
    ):
        _invalid()
    return identity_by_name


async def run_registry_ptg_source_worker(config, *, event=None):
    """Consume one accepted event using independently qualified bounded execution and reader pools."""
    if type(config) is not RegistryPTGSourceWorkerConfig:
        _invalid()
    header = dict(config.event) if event is None and config.event is not None else event
    validate_registry_ptg_source_event(header)
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
            native_identity = await _identity(connection, config, config.consumer.execution_role)
            async with reader.acquire() as notification_connection:
                if await _identity(notification_connection, config, config.reader_role) != native_identity:
                    _invalid()
                async with notification_connection.transaction(isolation="repeatable_read", readonly=True):
                    identity = await qualify_registry_ptg_source_notifications(
                        notification_connection, config.notification_store, reader_role=config.reader_role
                    )
            consumer = await _qualified_worker_runtime(config, stack, qualification, connection)
            await consumer.composition_runtime.qualify()
            notification_result = await consume_registry_ptg_source_notification_once(
                connection=connection,
                notification_pool=reader,
                store=config.notification_store,
                reader_role=config.reader_role,
                identity=identity,
                event=header,
                config=consumer,
            )
    if (
        type(notification_result) is not dict
        or set(notification_result) != {"state"}
        or notification_result["state"] not in _STATES
    ):
        _invalid()
    return dict(notification_result)


def _error(code):
    print(json.dumps({"state": "error", "code": code}, separators=(",", ":")), file=sys.stderr)


def run_command():
    """Load closed operator configuration and return the existing sanitized worker exit status."""
    try:
        config = parse_registry_ptg_source_worker_config(os.environ.get(CONFIG_ENV))
    except ValueError:
        _error("invalid_configuration")
        return 2
    try:
        result = asyncio.run(run_registry_ptg_source_worker(config))
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


@click.command(name="registry-ptg-source-worker", cls=_WorkerCommand, help="Consume one accepted PTG event and exit.")
def registry_ptg_source_worker_command():
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
        validate_registry_ptg_source_event(event_by_name)
        config = parse_registry_ptg_source_worker_config(os.environ.get(CONFIG_ENV))
        work = watcher = None
        try:
            await raise_if_cancelled(ctx, task)
            work = asyncio.create_task(run_registry_ptg_source_worker(config, event=event_by_name))
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
        raise RuntimeError("registry_ptg_source_worker_failed") from None
