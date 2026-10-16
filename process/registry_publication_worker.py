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
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime

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
        or set(configured) - {"base_source", "npi_source", "em_provider_inputs"}
    ):
        _invalid()
    npi = configured.get("npi_source")
    provider_inputs = configured.get("em_provider_inputs")
    if provider_inputs is not None:
        if type(provider_inputs) is not list or any(type(item) is not dict for item in provider_inputs):
            _invalid()
        provider_inputs = EMProviderInputs(tuple(EMProviderInputRelation(**item) for item in provider_inputs))
    return RegistryCompositionAddressSources(
        _address_pin(configured["base_source"]),
        None if npi is None else _address_pin(npi),
        em_provider_inputs=provider_inputs,
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
            or set(configured_by_name) - _REQUIRED - {"initial_address_sources", "ptg"}
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
        )
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ArgumentError:
        _invalid()


def _pool_options(url, maximum):
    return dict(
        host=url.host,
        port=url.port or 5432,
        user=url.username,
        password=url.password or "",
        database=url.database,
        ssl=url.query.get("ssl", "prefer"),
        min_size=1,
        max_size=maximum,
    )


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
        connect_args={"password": config.execution_url.password or ""},
        pool_size=1,
        max_overflow=0,
        hide_parameters=True,
    )
    stack.push_async_callback(engine.dispose)
    sessions = async_sessionmaker(engine, class_=AsyncSession)
    runtime = RegistryPublicationPTGRuntime(sessions, config.ptg.store, config.ptg.read_budget)
    return replace(config.consumer, composition_runtime=runtime)


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
        lifecycle = await asyncpg.create_pool(**_pool_options(config.lifecycle_url, 2))
        stack.push_async_callback(_close_pool, lifecycle)
        execution = await asyncpg.create_pool(**_pool_options(config.execution_url, 1))
        stack.push_async_callback(_close_pool, execution)
        consumer = _ptg_runtime(config, stack)
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
