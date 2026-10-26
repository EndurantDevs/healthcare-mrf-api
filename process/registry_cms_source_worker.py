# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consume one operator-selected immutable CMS notification with owned connections."""

import asyncio
import json
import os
import re
import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass, field

import asyncpg
import click
from sqlalchemy import event
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.registry_cms_source_consumer import (
    RegistryCMSSourceConsumerConfig,
    consume_registry_cms_source_notification_once,
)
from process.registry_cms_source_notifications import RegistryCMSSourceNotificationStore
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_ptg_scope_runtime import _database_url
from process.registry_publication_ptg_runtime import RegistryPublicationSQLLimits
from process.registry_publication_worker import (
    _BoundedPool,
    _close_pool,
    _custody_config_url,
    _pool_options,
    _ptg_config,
    _PTGWorkerConfig,
    _qualified_worker_runtime,
    _sql_limits,
    _verify_custody_config,
    _worker_backend_qualification,
)

CONFIG_ENV = "HLTHPRT_REGISTRY_CMS_SOURCE_WORKER"
MAX_CONFIG_BYTES = 16 * 1024
_STATES = {"completed", "replayed", "busy", "review_required", "stale_source", "conflict"}
_IDENTITY_SQL = """SELECT current_database()::text AS database_name,
 (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
 inet_server_addr()::text AS server_address,inet_server_port() AS server_port,
 pg_postmaster_start_time() AS server_started,to_regnamespace($1)::oid::bigint AS control_oid,
 to_regnamespace($2)::oid::bigint AS inbox_oid,
 current_user::text AS current_role,session_user::text AS session_role,
 EXISTS(SELECT 1 FROM pg_roles actor WHERE actor.rolname=current_user AND actor.rolcanlogin
   AND NOT (actor.rolsuper OR actor.rolcreatedb OR actor.rolcreaterole OR actor.rolreplication OR actor.rolbypassrls)
   AND NOT EXISTS(SELECT 1 FROM pg_roles elevated
     WHERE (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole OR elevated.rolreplication
       OR elevated.rolbypassrls OR elevated.rolname IN ('pg_read_server_files','pg_write_server_files',
       'pg_execute_server_program','pg_read_all_data','pg_write_all_data','pg_signal_backend',
       'pg_checkpoint','pg_create_subscription')) AND pg_has_role(actor.oid,elevated.oid,'MEMBER'))
 ) AS safe_login"""
_COORDINATES = {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}


def _invalid():
    raise ValueError("registry_cms_source_worker_config_invalid")


def _object(document, required_fields, optional=frozenset()):
    if (
        type(document) is not dict
        or not required_fields <= document.keys()
        or document.keys() - required_fields - optional
    ):
        _invalid()
    return document


def _roles(value):
    if type(value) is not list or len(value) > 32 or any(type(role) is not str for role in value):
        _invalid()
    if len(set(value)) != len(value):
        _invalid()
    for role in value:
        _identifier(role)
    return tuple(value)


def _unique(pairs):
    configured_by_name = {}
    for name, value in pairs:
        if name in configured_by_name:
            _invalid()
        configured_by_name[name] = value
    return configured_by_name


def _consumer(document):
    required_fields = {"control_schema", "execution_role", "owner_role", "coordinates", "replaced_coordinates"}
    _object(document, required_fields, {"loader_roles", "reader_roles", "max_attempts", "operation_seconds"})
    configured_by_name = dict(document)
    for name in ("coordinates", "replaced_coordinates"):
        _object(configured_by_name[name], _COORDINATES)
        configured_by_name[name] = RegistryNetworkSourceCoordinates(**configured_by_name[name])
    for name in ("loader_roles", "reader_roles"):
        configured_by_name[name] = _roles(configured_by_name.get(name, []))
    return RegistryCMSSourceConsumerConfig(**configured_by_name)


def _store(document):
    _object(document, {"control_schema", "owner_role", "publisher_role", "reader_roles", "source_attempt"})
    return RegistryCMSSourceNotificationStore(
        document["control_schema"],
        document["owner_role"],
        document["publisher_role"],
        RegistryCMSSourceAttempt.from_dict(document["source_attempt"]),
        _roles(document["reader_roles"]),
    )


def _endpoint(url):
    return url.host.lower(), url.port or 5432, url.database, dict(url.query)


@dataclass(frozen=True, repr=False)
class RegistryCMSSourceWorkerConfig:
    """Trusted operator inputs; source attempt evidence never confers authority."""

    execution_url: object
    notification_url: object
    consumer: RegistryCMSSourceConsumerConfig
    notification_store: RegistryCMSSourceNotificationStore
    source_schema: str
    receipt_id: str
    notification_sha256: str
    sql_limits: RegistryPublicationSQLLimits = field(default_factory=RegistryPublicationSQLLimits)
    source_endpoint_id: str | None = None
    ptg: _PTGWorkerConfig | None = field(default=None, repr=False)
    custody_url: object | None = field(default=None, repr=False)

    def __post_init__(self):
        if (
            type(self.consumer) is not RegistryCMSSourceConsumerConfig
            or self.consumer.composition_runtime is not None
            or type(self.notification_store) is not RegistryCMSSourceNotificationStore
            or type(self.sql_limits) is not RegistryPublicationSQLLimits
        ):
            _invalid()
        _validate_ptg(self.ptg, self.consumer.control_schema)
        for url in (self.execution_url, self.notification_url):
            _database_url(url.render_as_string(hide_password=False))
        if (
            _endpoint(self.execution_url) != _endpoint(self.notification_url)
            or self.execution_url.username != self.consumer.execution_role
            or self.notification_url.username not in self.notification_store.reader_roles
            or self.execution_url.username == self.notification_url.username
            or self.execution_url.username in {self.consumer.owner_role, self.notification_store.owner_role}
        ):
            _invalid()
        _verify_custody_config(self)
        _identifier(self.source_schema)
        if self.source_endpoint_id is not None:
            from process.registry_cms_source_consumer import _endpoint_id

            _endpoint_id(self.source_endpoint_id)
        for value in (self.receipt_id, self.notification_sha256):
            if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                _invalid()


def _validate_ptg(ptg, control_schema):
    if ptg is None:
        return
    if (
        type(ptg) is not _PTGWorkerConfig
        or type(ptg.store) is not RegistryPTGProducerScopeStore
        or type(ptg.read_budget) is not RegistryPTGGraphReadBudget
        or ptg.store.control_schema != control_schema
    ):
        _invalid()
    _ptg_config(
        {
            "owner_role": ptg.store.owner_role,
            "approval_role": ptg.store.approval_role,
            "read_limits": {
                name: getattr(ptg.read_budget, name)
                for name in ("maximum_bytes", "maximum_pages", "maximum_coordinates")
            },
        },
        control_schema,
    )


def parse_registry_cms_source_worker_config(encoded):
    """Decode one closed, byte-bounded environment document before opening resources."""
    try:
        if type(encoded) is not str or not 0 < len(encoded.encode("utf-8")) <= MAX_CONFIG_BYTES:
            _invalid()

        def constant(_value):
            """Reject non-finite JSON constants without echoing their spelling."""
            _invalid()

        document = json.loads(encoded, object_pairs_hook=_unique, parse_constant=constant)
        required_fields = {
            "execution_dsn",
            "notification_dsn",
            "consumer",
            "notification_store",
            "source_schema",
            "receipt_id",
            "notification_sha256",
        }
        _object(document, required_fields, {"sql_limits", "source_endpoint_id", "ptg", "custody_dsn"})
        urls = []
        for name in ("execution_dsn", "notification_dsn"):
            dsn = document[name]
            if type(dsn) is not str or not 0 < len(dsn) <= 4096:
                _invalid()
            urls.append(_database_url(dsn))
        consumer = _consumer(document["consumer"])
        return RegistryCMSSourceWorkerConfig(
            *urls,
            consumer,
            _store(document["notification_store"]),
            document["source_schema"],
            document["receipt_id"],
            document["notification_sha256"],
            _sql_limits(document["sql_limits"]) if "sql_limits" in document else RegistryPublicationSQLLimits(),
            document.get("source_endpoint_id"),
            _ptg_config(document["ptg"], consumer.control_schema) if "ptg" in document else None,
            custody_url=_custody_config_url(document["custody_dsn"]) if "custody_dsn" in document else None,
        )
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ArgumentError, AttributeError:
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
        raise ValueError("registry_cms_source_worker_connection_invalid")
    return identity_by_name


async def _notification_reader_identity(config, connection):
    """Require finite limits, a direct reader and read-only repeatable-read settings."""
    await config.sql_limits.verify(connection)
    identity = await _identity(connection, config, config.notification_url.username)
    if (
        await connection.fetchval("SHOW transaction_read_only") != "on"
        or await connection.fetchval("SHOW transaction_isolation") != "repeatable read"
    ):
        raise ValueError("registry_cms_source_worker_connection_invalid")
    return identity


async def _notification_sessions(config, stack):
    settings = config.sql_limits.server_settings() | {
        "default_transaction_read_only": "on",
        "default_transaction_isolation": "repeatable read",
    }
    engine = create_async_engine(
        config.notification_url,
        isolation_level="REPEATABLE READ",
        hide_parameters=True,
        connect_args={
            "password": config.notification_url.password or "",
            "timeout": config.sql_limits.connect_timeout_seconds,
            "command_timeout": config.sql_limits.statement_timeout_ms / 1000
            + config.sql_limits.connect_timeout_seconds,
            "server_settings": settings,
        },
        pool_size=1,
        max_overflow=0,
        pool_timeout=config.sql_limits.connect_timeout_seconds,
    )
    stack.push_async_callback(engine.dispose)
    async with engine.connect() as reader:
        raw = await reader.get_raw_connection()
        expected_identity = await _notification_reader_identity(config, raw.driver_connection)
        qualification = await _worker_backend_qualification(
            config, raw.driver_connection, config.notification_url.username
        )

    async def verify(connection):
        """Verify actual reader limits, direct role and trusted database on each checkout."""
        if await _notification_reader_identity(config, connection) != expected_identity:
            raise ValueError("registry_cms_source_worker_connection_invalid")

    def checkout(connection, _record, _proxy):
        """Run native verification through the genuine adapted checkout."""
        connection.run_async(verify)

    event.listen(engine.sync_engine, "checkout", checkout)
    return async_sessionmaker(engine), expected_identity, qualification


async def run_registry_cms_source_worker(config, *, event=None):
    """Drain the connected consumer before releasing its connection and disposing readers."""
    if type(config) is not RegistryCMSSourceWorkerConfig:
        _invalid()
    if event is not None:
        validate_registry_cms_source_event(event)
        if config.source_endpoint_id is None or event["source_schema"] != config.source_schema:
            _invalid()
    selected_event = (
        {name: getattr(config, name) for name in REGISTRY_CMS_EVENT_FIELDS} if event is None else dict(event)
    )
    async with AsyncExitStack() as stack:
        sessions, identity, qualification = await _notification_sessions(config, stack)
        execution_verifier = qualification.verifier(config.consumer.execution_role)
        pool = await asyncpg.create_pool(
            **_pool_options(config.execution_url, 1, config.sql_limits, backend_verifier=execution_verifier)
        )
        stack.push_async_callback(_close_pool, pool)
        async with _BoundedPool(pool, config.sql_limits.connect_timeout_seconds).acquire() as connection:
            if await _identity(connection, config, config.consumer.execution_role) != identity:
                raise ValueError("registry_cms_source_worker_connection_invalid")
            consumer = await _qualified_worker_runtime(config, stack, qualification, connection)
            if consumer.composition_runtime is not None:
                await consumer.composition_runtime.qualify()
            result_by_name = await consume_registry_cms_source_notification_once(
                connection=connection,
                notification_sessions=sessions,
                notification_store=config.notification_store,
                **selected_event,
                config=consumer,
                **({"source_endpoint_id": config.source_endpoint_id} if event is not None else {}),
            )
    if (
        type(result_by_name) is not dict
        or type(result_by_name.get("state")) is not str
        or result_by_name["state"] not in _STATES
    ):
        raise ValueError("registry_cms_source_worker_result_invalid")
    return {"state": result_by_name["state"]}


def _error(code):
    print(json.dumps({"state": "error", "code": code}, separators=(",", ":")), file=sys.stderr)


def run_command():
    """Print constant states/errors, never configuration, manifest or exception bodies."""
    try:
        config = parse_registry_cms_source_worker_config(os.environ.get(CONFIG_ENV))
    except ValueError:
        _error("invalid_configuration")
        return 2
    try:
        result = asyncio.run(run_registry_cms_source_worker(config))
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
        """Redact unsupported argument values before Click diagnostics."""
        if arguments and arguments != ["--help"]:
            _error("invalid_arguments")
            context.exit(2)
        return super().parse_args(context, arguments)


@click.command(
    name="registry-cms-source-worker", cls=_WorkerCommand, help="Consume one exact source notification and exit."
)
def registry_cms_source_worker_command():
    """Run the explicitly configured one-shot source notification worker."""
    raise click.exceptions.Exit(run_command())


REGISTRY_CMS_EVENT_FIELDS = frozenset({"source_schema", "receipt_id", "notification_sha256"})


def validate_registry_cms_source_event(params):
    """Accept only the exact immutable event identity, never runtime configuration."""
    _object(params, REGISTRY_CMS_EVENT_FIELDS)
    _identifier(params["source_schema"])
    for name in ("receipt_id", "notification_sha256"):
        if type(params[name]) is not str or re.fullmatch(r"[0-9a-f]{64}", params[name]) is None:
            _invalid()


async def _watch_control_cancel(ctx, task):
    """Observe the existing control cancellation flag while the consumer owns its work."""
    from process.control_cancel import raise_if_cancelled

    while True:
        await raise_if_cancelled(ctx, task)
        await asyncio.sleep(1)


async def control_start(ctx, task):
    """Bind a control request to trusted configuration and drain before terminal projection."""
    from process.control_cancel import ImportCancelledError, raise_if_cancelled

    _object(task, REGISTRY_CMS_EVENT_FIELDS | {"run_id"})
    run_id = task["run_id"]
    if type(run_id) is not str or not 0 < len(run_id) <= 128 or run_id != run_id.strip():
        _invalid()
    params_by_name = {name: task[name] for name in REGISTRY_CMS_EVENT_FIELDS}
    validate_registry_cms_source_event(params_by_name)
    config = parse_registry_cms_source_worker_config(os.environ.get(CONFIG_ENV))
    if params_by_name["source_schema"] != config.source_schema or config.source_endpoint_id is None:
        _invalid()
    work = watcher = None
    try:
        await raise_if_cancelled(ctx, task)
        work = asyncio.create_task(run_registry_cms_source_worker(config, event=params_by_name))
        watcher = asyncio.create_task(_watch_control_cancel(ctx, task))
        await asyncio.wait((work, watcher), return_when=asyncio.FIRST_COMPLETED)
        if work.done():
            return await work
        await watcher
    except ImportCancelledError:
        raise
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except Exception:
        raise RuntimeError("registry_cms_source_worker_failed") from None
    finally:
        owned_tasks = tuple(owned_task for owned_task in (work, watcher) if owned_task is not None)
        for owned_task in owned_tasks:
            if not owned_task.done():
                owned_task.cancel()
        await asyncio.gather(*owned_tasks, return_exceptions=True)
