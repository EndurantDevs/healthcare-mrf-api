# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run one durable publication request with dedicated owned connections."""

import asyncio
import json
import os
import re
import sys
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from uuid import UUID

import asyncpg
import click
from sqlalchemy import event
from sqlalchemy.engine import URL
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_em_provider_inputs import EMProviderInputRelation, EMProviderInputs
from process.registry_benefit_plan_metadata import RegistryBenefitPlanMetadataClient
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_ptg_scope_runtime import _database_url
from process.registry_publication_authority import RegistryPublicationAuthorityClient, _origin
from process.registry_publication_consumer import RegistryPublicationConsumerConfig, consume_registry_publication_once
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime, RegistryPublicationSQLLimits
from process.registry_source_recipe_store import decode_registry_source_recipes

CONFIG_ENV = "HLTHPRT_REGISTRY_PUBLICATION_WORKER"
MAX_CONFIG_BYTES = 16 * 1024
_REQUIRED = {"lifecycle_dsn", "execution_dsn", "app_origin", "service_token", "consumer"}
_ROLES = {"lifecycle_role", "execution_role", "api_role", "queue_owner_role", "physical_owner_role"}


@dataclass(frozen=True, repr=False)
class _PTGWorkerConfig:
    store: RegistryPTGProducerScopeStore
    read_budget: RegistryPTGGraphReadBudget


@dataclass(frozen=True, repr=False)
class RegistryPublicationWorkerConfig:
    lifecycle_url: object = field(repr=False)
    execution_url: object = field(repr=False)
    app_origin: str = field(repr=False)
    service_token: str = field(repr=False)
    consumer: RegistryPublicationConsumerConfig
    initial_address_sources: RegistryCompositionAddressSources | None = None
    ptg: _PTGWorkerConfig | None = field(default=None, repr=False)
    operation_seconds: int = 3600
    sql_limits: RegistryPublicationSQLLimits = field(default_factory=RegistryPublicationSQLLimits)
    custody_url: object | None = field(default=None, repr=False)

    def __post_init__(self):
        """Validate immutable SQL limits and deadline for direct and parsed callers."""
        if type(self.sql_limits) is not RegistryPublicationSQLLimits:
            _invalid()
        if type(self.operation_seconds) is not int or not 1 <= self.operation_seconds <= 86400:
            _invalid()
        _verify_custody_config(self)


def _invalid(_value=None):
    raise ValueError("registry_publication_worker_config_invalid")


def _unique_fields(pairs):
    configured_by_name = {}
    for name, configured in pairs:
        if name in configured_by_name:
            _invalid()
        configured_by_name[name] = configured
    return configured_by_name


def _consumer_config(configured):
    required = _ROLES | {"control_schema"}
    if (
        type(configured) is not dict
        or not required <= set(configured)
        or set(configured) - required - {"loader_roles", "reader_roles"}
    ):
        _invalid()
    roles_by_name = dict(configured)
    for name in ("loader_roles", "reader_roles"):
        roles = configured.get(name, [])
        if (
            type(roles) is not list
            or len(roles) > 32
            or any(type(role) is not str for role in roles)
            or len(set(roles)) != len(roles)
        ):
            _invalid()
        roles_by_name[name] = tuple(roles)
    consumer = RegistryPublicationConsumerConfig(**roles_by_name)
    if len({getattr(consumer, name) for name in _ROLES}) != len(_ROLES):
        _invalid()
    return consumer


def _address_pin(configured):
    if type(configured) is not dict or set(configured) != {"schema_name", "table_name", "generation_id"}:
        _invalid()
    return PinnedAddressSource(**configured)


def _initial_sources(configured):
    if configured is None:
        return None
    if (
        type(configured) is not dict
        or "base_source" not in configured
        or set(configured)
        - {"base_source", "npi_source", "em_provider_inputs", "initial_source_recipes", "replacement_source_recipes"}
    ):
        _invalid()
    npi = configured.get("npi_source")
    provider_inputs = configured.get("em_provider_inputs")
    if provider_inputs is not None:
        if type(provider_inputs) is not list or any(type(item) is not dict for item in provider_inputs):
            _invalid()
        provider_inputs = EMProviderInputs(tuple(EMProviderInputRelation(**item) for item in provider_inputs))
    recipes_by_phase = {}
    for phase in ("initial_source_recipes", "replacement_source_recipes"):
        recipes = configured.get(phase, [])
        if type(recipes) is not list:
            _invalid()
        recipes_by_phase[phase] = decode_registry_source_recipes(recipes)
    return RegistryCompositionAddressSources(
        _address_pin(configured["base_source"]),
        None if npi is None else _address_pin(npi),
        em_provider_inputs=provider_inputs,
        **recipes_by_phase,
    )


def _ptg_config(configured, control_schema):
    if type(configured) is not dict or set(configured) != {"owner_role", "approval_role", "read_limits"}:
        _invalid()
    for name in ("owner_role", "approval_role"):
        _identifier(configured[name])
    if configured["owner_role"] == configured["approval_role"]:
        _invalid()
    limits_by_name = configured["read_limits"]
    if type(limits_by_name) is not dict or set(limits_by_name) != {
        "maximum_bytes",
        "maximum_pages",
        "maximum_coordinates",
    }:
        _invalid()
    return _PTGWorkerConfig(
        RegistryPTGProducerScopeStore(configured["owner_role"], configured["approval_role"], control_schema),
        RegistryPTGGraphReadBudget(**limits_by_name),
    )


def _sql_limits(configured):
    if type(configured) is not dict or set(configured) != {
        "work_mem_bytes",
        "maintenance_work_mem_bytes",
        "temp_file_limit_bytes",
        "statement_timeout_ms",
        "lock_timeout_ms",
        "connect_timeout_seconds",
    }:
        _invalid()
    return RegistryPublicationSQLLimits(**configured)


def parse_registry_publication_worker_config(encoded):
    """Reject malformed or ambiguous configuration before opening any resource."""
    try:
        if (
            type(encoded) is not str
            or not 0 < len(encoded) <= MAX_CONFIG_BYTES
            or len(encoded.encode("utf-8")) > MAX_CONFIG_BYTES
        ):
            _invalid()
        configured_by_name = json.loads(encoded, object_pairs_hook=_unique_fields, parse_constant=_invalid)
        if (
            type(configured_by_name) is not dict
            or not _REQUIRED <= set(configured_by_name)
            or set(configured_by_name)
            - _REQUIRED
            - {"initial_address_sources", "ptg", "operation_seconds", "sql_limits", "custody_dsn"}
        ):
            _invalid()
        consumer = _consumer_config(configured_by_name["consumer"])
        urls = []
        for name in ("lifecycle_dsn", "execution_dsn"):
            dsn = configured_by_name[name]
            if type(dsn) is not str or not 0 < len(dsn) <= 4096:
                _invalid()
            urls.append(_database_url(dsn))
        lifecycle, execution = urls
        identity = lambda url: (url.host.lower(), url.port or 5432, url.database, dict(url.query))
        if (
            identity(lifecycle) != identity(execution)
            or lifecycle.username != consumer.lifecycle_role
            or execution.username != consumer.execution_role
        ):
            _invalid()
        token = configured_by_name["service_token"]
        if type(token) is not str or not 0 < len(token) <= 4096 or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", token) is None:
            _invalid()
        return RegistryPublicationWorkerConfig(
            lifecycle,
            execution,
            _origin(configured_by_name["app_origin"]),
            token,
            consumer,
            _initial_sources(configured_by_name.get("initial_address_sources")),
            _ptg_config(configured_by_name["ptg"], consumer.control_schema) if "ptg" in configured_by_name else None,
            configured_by_name.get("operation_seconds", 3600),
            _sql_limits(configured_by_name["sql_limits"])
            if "sql_limits" in configured_by_name
            else RegistryPublicationSQLLimits(),
            custody_url=_custody_config_url(configured_by_name["custody_dsn"])
            if "custody_dsn" in configured_by_name
            else None,
        )
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ArgumentError:
        _invalid()


def _pool_options(url, maximum, sql_limits=None):
    options_by_name = dict(
        host=url.host,
        port=url.port or 5432,
        user=url.username,
        password=url.password or "",
        database=url.database,
        ssl=url.query.get("ssl", "prefer"),
        min_size=1,
        max_size=maximum,
    )
    if sql_limits is not None:
        options_by_name.update(
            timeout=sql_limits.connect_timeout_seconds,
            command_timeout=sql_limits.statement_timeout_ms / 1000 + sql_limits.connect_timeout_seconds,
            server_settings=sql_limits.server_settings(),
        )
        if maximum == 1:
            options_by_name["setup"] = sql_limits.verify
    return options_by_name


class _BoundedPool:
    """Keep every existing consumer checkout within the trusted connection budget."""

    def __init__(self, pool, timeout):
        self.pool, self.timeout = pool, timeout

    def acquire(self, *, timeout=None):
        """Bound existing consumer checkouts, preserving stricter explicit waits."""
        return self.pool.acquire(timeout=self.timeout if timeout is None else min(timeout, self.timeout))

    def __getattr__(self, name):
        return getattr(self.pool, name)


async def _close_pool(pool):
    try:
        await pool.close()
    except BaseException:
        pool.terminate()
        raise


def _summary(result_by_name):
    if result_by_name is None:
        return {"state": "no_work"}
    if (
        type(result_by_name) is not dict
        or set(result_by_name) != {"request_id", "state", "result"}
        or result_by_name["state"] != "completed"
    ):
        raise ValueError("registry_publication_worker_result_invalid")
    identity = result_by_name["request_id"]
    if type(identity) is not str or len(identity) != 36 or str(UUID(identity)) != identity or not UUID(identity).int:
        raise ValueError("registry_publication_worker_result_invalid")
    return {"request_id": identity, "state": "completed"}


def _ptg_runtime(config, stack):
    if config.ptg is None:
        return config.consumer
    engine = create_async_engine(
        config.execution_url,
        connect_args={
            "password": config.execution_url.password or "",
            "timeout": config.sql_limits.connect_timeout_seconds,
            "command_timeout": config.sql_limits.statement_timeout_ms / 1000
            + config.sql_limits.connect_timeout_seconds,
            "server_settings": config.sql_limits.server_settings(),
        },
        pool_timeout=config.sql_limits.connect_timeout_seconds,
        pool_size=1,
        max_overflow=0,
        hide_parameters=True,
    )
    stack.push_async_callback(engine.dispose)

    def verify_checkout(connection, _record, _proxy):
        """Inspect the actual adapted asyncpg backend on every genuine checkout."""
        connection.run_async(config.sql_limits.verify)

    event.listen(engine.sync_engine, "checkout", verify_checkout)
    sessions = async_sessionmaker(engine, class_=AsyncSession)
    runtime = RegistryPublicationPTGRuntime(sessions, config.ptg.store, config.ptg.read_budget, config.sql_limits)
    return replace(config.consumer, composition_runtime=runtime)


def _custody_config_url(value):
    if type(value) is not str or not 0 < len(value) <= 4096:
        _invalid()
    return _database_url(value)


def _custody_roles(config):
    consumer = config.consumer
    roles = {
        getattr(consumer, name, None)
        for name in ("lifecycle_role", "execution_role", "api_role", "physical_owner_role", "owner_role")
    }
    roles.update(consumer.loader_roles)
    roles.update(consumer.reader_roles)
    roles.update(getattr(config, "source_authority_roles", ()))
    store = getattr(config, "notification_store", None)
    if store is not None:
        roles.add(getattr(store, "owner_role", None))
        roles.update(getattr(store, "reader_roles", ()))
        roles.add(getattr(store, "approval_role", None))
        roles.add(getattr(store, "publisher_role", None))
    roles.add(getattr(config, "reader_role", None))
    if config.ptg is not None:
        roles.update((config.ptg.store.owner_role, config.ptg.store.approval_role))
    roles.discard(None)
    if len(roles) > 128:
        _invalid()
    return tuple(sorted(roles))


def _verify_custody_config(config):
    if config.custody_url is None:
        return
    if type(config.custody_url) is not URL:
        _invalid()
    url = _database_url(config.custody_url.render_as_string(hide_password=False))
    execution = config.execution_url
    endpoint = lambda item: (item.host.lower(), item.port or 5432, item.database, dict(item.query))
    forbidden_roles = {*_custody_roles(config), getattr(config.consumer, "queue_owner_role", None)}
    if endpoint(url) != endpoint(execution) or url.username in forbidden_roles:
        _invalid()


async def _custody_identity(connection, schema, role):
    # Reuse the existing direct-login and native server identity contract.
    from process.registry_cms_source_worker import _IDENTITY_SQL

    identity_sql = (
        "SELECT native.*,owner.oid::bigint AS custody_owner_oid,owner.rolname::text AS custody_owner_role "
        "FROM (" + _IDENTITY_SQL + ") native JOIN pg_namespace n ON n.oid::bigint=native.control_oid "
        "JOIN pg_roles owner ON owner.oid=n.nspowner"
    )
    record = await connection.fetchrow(identity_sql, schema, schema)
    if record is None:
        raise ValueError("registry_candidate_custody_connection_invalid")
    identity_by_name = dict(record)
    if (
        identity_by_name.pop("safe_login") is not True
        or identity_by_name.pop("current_role") != role
        or identity_by_name.pop("session_role") != role
        or identity_by_name.get("control_oid") is None
    ):
        raise ValueError("registry_candidate_custody_connection_invalid")
    return identity_by_name


async def _custody_runtime(config, stack, consumer, execution_connection):
    if config.custody_url is None:
        return consumer
    from process.network_candidate_custody import seal_network_candidate_custody, verify_network_candidate_custody

    _verify_custody_config(config)
    schema = consumer.control_schema
    identity = await _custody_identity(execution_connection, schema, consumer.execution_role)
    pool = await asyncpg.create_pool(**_pool_options(config.custody_url, 1, config.sql_limits))
    stack.push_async_callback(_close_pool, pool)
    pool = _BoundedPool(pool, config.sql_limits.connect_timeout_seconds)
    role = config.custody_url.username

    async def seal_candidate(copy_target):
        """Seal this candidate through the separately qualified operator pool."""
        async with pool.acquire() as custodian:

            async def is_authorized(connection, target):
                """Require the same original operator and exact candidate for every custody phase."""
                return (
                    connection is custodian
                    and target == copy_target
                    and await _custody_identity(connection, schema, role) == identity
                )

            # The native protected owner may also own the pricing authority store.
            runtime_roles = tuple(name for name in _custody_roles(config) if name != identity["custody_owner_role"])
            sealed = await seal_network_candidate_custody(
                custodian, copy_target, control_schema=schema, runtime_roles=runtime_roles, authorize=is_authorized
            )
            async with custodian.transaction(isolation="repeatable_read", readonly=True):
                verified = await verify_network_candidate_custody(
                    custodian, copy_target, control_schema=schema, required=True
                )
            if verified != sealed:
                raise ValueError("registry_candidate_custody_connection_invalid")

    return replace(consumer, seal_candidate=seal_candidate)


async def run_registry_publication_worker(config):
    """Own both bounded pools and authority client through success, failure or cancellation."""
    if type(config) is not RegistryPublicationWorkerConfig:
        _invalid()
    async with AsyncExitStack() as stack:
        authorize = await stack.enter_async_context(
            RegistryPublicationAuthorityClient(config.app_origin, config.service_token)
        )
        plan_metadata_client = RegistryBenefitPlanMetadataClient.from_config(os.environ)
        if plan_metadata_client is not None:
            await stack.enter_async_context(plan_metadata_client)
        lifecycle = await asyncpg.create_pool(**_pool_options(config.lifecycle_url, 2, config.sql_limits))
        stack.push_async_callback(_close_pool, lifecycle)
        execution = await asyncpg.create_pool(**_pool_options(config.execution_url, 1, config.sql_limits))
        stack.push_async_callback(_close_pool, execution)
        lifecycle = _BoundedPool(lifecycle, config.sql_limits.connect_timeout_seconds)
        execution = _BoundedPool(execution, config.sql_limits.connect_timeout_seconds)
        consumer = _ptg_runtime(config, stack)
        async with execution.acquire() as connection:
            consumer = await _custody_runtime(config, stack, consumer, connection)
            if consumer.composition_runtime is not None:
                await consumer.composition_runtime.qualify()
        async with asyncio.timeout(config.operation_seconds):
            result_by_name = await consume_registry_publication_once(
                lifecycle_pool=lifecycle,
                execution_pool=execution,
                authorize=authorize,
                config=consumer,
                initial_address_sources=config.initial_address_sources,
                plan_metadata_client=plan_metadata_client,
            )
    return _summary(result_by_name)


def _error(code):
    print(json.dumps({"state": "error", "code": code}, separators=(",", ":")), file=sys.stderr)


def run_command():
    """Emit only bounded metadata or constant errors, never environment or exception text."""
    try:
        config = parse_registry_publication_worker_config(os.environ.get(CONFIG_ENV))
    except ValueError:
        _error("invalid_configuration")
        return 2
    try:
        receipt = asyncio.run(run_registry_publication_worker(config))
    except KeyboardInterrupt, asyncio.CancelledError:
        _error("canceled")
        return 130
    except Exception:
        _error("worker_failed")
        return 1
    print(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    return 0


class _WorkerCommand(click.Command):
    def parse_args(self, context, arguments):
        """Keep unsupported argument values out of Click diagnostics."""
        if arguments and arguments != ["--help"]:
            _error("invalid_arguments")
            context.exit(2)
        return super().parse_args(context, arguments)


@click.command(
    name="registry-publication-worker", cls=_WorkerCommand, help="Consume one durable publication request and exit."
)
def registry_publication_worker_command():
    """Run the environment-configured worker once through the existing process group."""
    raise click.exceptions.Exit(run_command())
