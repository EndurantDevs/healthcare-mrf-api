# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run one durable publication request with dedicated owned connections."""

import asyncio
import json
import os
import re
import sys
import traceback
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
from process.network_em_provider_inputs import EMProviderInputs, em_provider_input_relation
from process.registry_benefit_plan_metadata import RegistryBenefitPlanMetadataClient
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_ptg_scope_runtime import _database_url
from process.registry_publication_authority import (
    RegistryPublicationAuthorityClient,
    _origin,
)
from process.registry_publication_consumer import (
    RegistryPublicationConsumerConfig,
    consume_registry_publication_once,
)
from process.registry_publication_ptg_runtime import (
    RegistryPublicationPTGRuntime,
    RegistryPublicationSQLLimits,
)
from process.registry_source_recipe_store import decode_registry_source_recipes
from process.registry_source_witness_runtime import (
    RegistrySourceWitnessConfig,
    install_source_witness_runtime,
    parse_source_witness_config,
)

CONFIG_ENV = "HLTHPRT_REGISTRY_PUBLICATION_WORKER"
MAX_CONFIG_BYTES = 16 * 1024
_REQUIRED = {
    "lifecycle_dsn",
    "execution_dsn",
    "app_origin",
    "service_token",
    "consumer",
}
_ROLES = {
    "lifecycle_role",
    "execution_role",
    "api_role",
    "queue_owner_role",
    "physical_owner_role",
}


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
    operation_seconds: int | None = 3600
    sql_limits: RegistryPublicationSQLLimits = field(default_factory=RegistryPublicationSQLLimits)
    custody_url: object | None = field(default=None, repr=False)
    source_witness: RegistrySourceWitnessConfig | None = field(default=None, repr=False)
    preparation_roles: dict | None = field(default=None, repr=False)

    def __post_init__(self):
        """Validate immutable SQL limits and deadline for direct and parsed callers."""
        if type(self.sql_limits) is not RegistryPublicationSQLLimits:
            _invalid()
        if self.operation_seconds is not None and (
            type(self.operation_seconds) is not int or not 1 <= self.operation_seconds <= 86400
        ):
            _invalid()
        if self.source_witness is not None:
            if (
                type(self.source_witness) is not RegistrySourceWitnessConfig
                or type(self.initial_address_sources) is not RegistryCompositionAddressSources
                or not self.initial_address_sources.compact
                or self.initial_address_sources.source_witness_runtime is not None
            ):
                _invalid()
            self.source_witness.__post_init__()
        object.__setattr__(self, "preparation_roles", _preparation_role_profile(self))
        _validate_legacy_preparation_url(self)


def _invalid(_value=None):
    raise ValueError("registry_publication_worker_config_invalid")


def unique_json_fields(pairs):
    """Decode configuration fields while rejecting duplicate names."""
    configured_by_name = {}
    for name, configured in pairs:
        if name in configured_by_name:
            _invalid()
        configured_by_name[name] = configured
    return configured_by_name


_unique_fields = unique_json_fields


def parse_publication_consumer_config(configured):
    """Validate distinct publication roles and bounded optional reader and loader identities."""
    required = _ROLES | {"control_schema"}
    if (
        type(configured) is not dict
        or not required <= set(configured)
        or set(configured) - required - {"loader_roles", "reader_roles", "only_request_id"}
    ):
        _invalid()
    roles_by_name = dict(configured)
    if "only_request_id" in configured:
        request_id_text = configured["only_request_id"]
        if type(request_id_text) is not str:
            _invalid()
        request_id = UUID(request_id_text)
        if not request_id.int or str(request_id) != request_id_text:
            _invalid()
        roles_by_name["only_request_id"] = request_id
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


_consumer_config = parse_publication_consumer_config


def _address_pin(configured):
    if type(configured) is not dict or set(configured) != {
        "schema_name",
        "table_name",
        "generation_id",
    }:
        _invalid()
    return PinnedAddressSource(**configured)


def _initial_sources(configured):
    from process.registry_source_recipe_additions import _decode_added_recipes

    if configured is None:
        return None
    if (
        type(configured) is not dict
        or "base_source" not in configured
        or set(configured)
        - {
            "base_source",
            "npi_source",
            "em_provider_inputs",
            "initial_source_recipes",
            "replacement_source_recipes",
            "source_recipe_additions",
            "compact",
        }
    ):
        _invalid()
    npi = configured.get("npi_source")
    provider_inputs = configured.get("em_provider_inputs")
    if provider_inputs is not None:
        if type(provider_inputs) is not list or any(
            type(provider_input) is not dict for provider_input in provider_inputs
        ):
            _invalid()
        provider_inputs = EMProviderInputs(
            tuple(em_provider_input_relation(provider_input) for provider_input in provider_inputs)
        )
    recipes_by_phase = {}
    for phase in ("initial_source_recipes", "replacement_source_recipes"):
        recipes = configured.get(phase, [])
        if type(recipes) is not list:
            _invalid()
        recipes_by_phase[phase] = (
            _decode_added_recipes(recipes)
            if phase == "replacement_source_recipes"
            else decode_registry_source_recipes(recipes)
        )
    return RegistryCompositionAddressSources(
        _address_pin(configured["base_source"]),
        None if npi is None else _address_pin(npi),
        em_provider_inputs=provider_inputs,
        compact=configured.get("compact", False),
        source_recipe_additions=_source_recipe_additions(configured),
        **recipes_by_phase,
    )


def _source_recipe_additions(configured):
    from process.registry_source_recipe_additions import decode_source_recipe_additions

    return (
        decode_source_recipe_additions(configured["source_recipe_additions"])
        if "source_recipe_additions" in configured
        else None
    )


def _ptg_config(configured, control_schema):
    if type(configured) is not dict or set(configured) != {
        "owner_role",
        "approval_role",
        "read_limits",
    }:
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
            - {
                "initial_address_sources",
                "ptg",
                "operation_seconds",
                "sql_limits",
                "custody_dsn",
                "source_witness",
                "preparation_roles",
            }
        ):
            _invalid()
        consumer = _consumer_config(configured_by_name["consumer"])
        lifecycle, execution = _worker_database_urls(configured_by_name, consumer)
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
            source_witness=parse_source_witness_config(configured_by_name["source_witness"])
            if "source_witness" in configured_by_name
            else None,
            preparation_roles=configured_by_name.get("preparation_roles"),
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
    verifier = sql_limits.verify if sql_limits is not None and maximum == 1 else None
    if verifier is not None:
        options_by_name["init"] = verifier
        options_by_name["setup"] = verifier
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
        or result_by_name["state"] not in {"completed", "waiting_admin_freeze"}
    ):
        raise ValueError("registry_publication_worker_result_invalid")
    identity = result_by_name["request_id"]
    if type(identity) is not str or len(identity) != 36 or str(UUID(identity)) != identity or not UUID(identity).int:
        raise ValueError("registry_publication_worker_result_invalid")
    summary_by_field = {"request_id": identity, "state": result_by_name["state"]}
    if summary_by_field["state"] == "waiting_admin_freeze":
        candidate_id = result_by_name["result"].get("candidate_id")
        if type(candidate_id) is not str or str(UUID(candidate_id)) != candidate_id or not UUID(candidate_id).int:
            raise ValueError("registry_publication_worker_result_invalid")
        summary_by_field["candidate_id"] = candidate_id
    return summary_by_field


def _worker_sessions(config, stack, url):
    """Own one qualified genuine session factory for the selected existing login."""
    engine = create_async_engine(
        url,
        connect_args={
            "password": url.password or "",
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
    return async_sessionmaker(engine, class_=AsyncSession)


def _ptg_runtime(config, stack):
    if config.ptg is None:
        return config.consumer
    sessions = _worker_sessions(config, stack, config.execution_url)
    runtime = RegistryPublicationPTGRuntime(sessions, config.ptg.store, config.ptg.read_budget, config.sql_limits)
    return replace(config.consumer, composition_runtime=runtime)


def _custody_config_url(value):
    if type(value) is not str or not 0 < len(value) <= 4096:
        _invalid()
    return _database_url(value)


def _validate_legacy_preparation_url(config):
    """Keep the existing optional URL input parseable during configuration migration."""
    if config.custody_url is not None:
        if type(config.custody_url) is not URL:
            _invalid()
        _database_url(config.custody_url.render_as_string(hide_password=False))


def _application_role_names(config, consumer=None):
    """Collect configured application and group roles for the administrator grants."""
    consumer = config.consumer if consumer is None else consumer
    roles = set()
    for current in (config.consumer, consumer):
        roles.update(
            getattr(current, name, None)
            for name in (
                "lifecycle_role",
                "execution_role",
                "api_role",
                "physical_owner_role",
                "owner_role",
                "queue_owner_role",
            )
        )
        roles.update(current.loader_roles)
        roles.update(current.reader_roles)
        roles.update(getattr(current, "source_authority_roles", ()))
        store = getattr(getattr(current, "composition_runtime", None), "store", None)
        roles.add(getattr(store, "approval_role", None))
    roles.update(getattr(config, "source_authority_roles", ()))
    store = getattr(config, "notification_store", None)
    if store is not None:
        roles.update(
            (
                getattr(store, "owner_role", None),
                getattr(store, "publisher_role", None),
                getattr(store, "approval_role", None),
            )
        )
        roles.update(getattr(store, "reader_roles", ()))
    roles.add(getattr(config, "reader_role", None))
    roles.add(getattr(getattr(config.ptg, "store", None), "approval_role", None))
    roles.discard(None)
    if len(roles) > 128:
        _invalid()
    return tuple(sorted(roles))


def _preparation_role_profile(config):
    from process.network_membership_publication import named_network_preparation_roles

    configured = getattr(config, "preparation_roles", None)
    if configured is None:
        return None
    profile = named_network_preparation_roles(configured)
    required_roles = set(_application_role_names(config))
    source_settings = getattr(config, "source_witness", None)
    if source_settings is not None:
        required_roles.update(source_settings.runtime_roles)
    if (
        not required_roles <= set(profile["application_roles"])
        or profile["publisher_role"] != config.consumer.execution_role
    ):
        _invalid()
    return profile


async def _configured_worker_runtime(config, stack):
    """Install the existing named preparation lifecycle on bounded application connections."""
    from process.network_membership_publication import (
        named_network_preparation_roles,
        validate_named_network_preparation,
    )

    consumer = _ptg_runtime(config, stack)
    if getattr(consumer, "metadata_only", False):
        return consumer
    preparation_pool = await asyncpg.create_pool(**_pool_options(config.execution_url, 1, config.sql_limits))
    stack.push_async_callback(_close_pool, preparation_pool)
    preparation_pool = _BoundedPool(preparation_pool, config.sql_limits.connect_timeout_seconds)
    roles = named_network_preparation_roles(getattr(config, "preparation_roles", None))
    source_runtime = getattr(getattr(config, "initial_address_sources", None), "source_witness_runtime", None)

    async def seal_candidate(copy_target, *, pre_enrolled_only=False):
        """Checkpoint, validate and resume one candidate through the shared administrator handoff."""
        async with preparation_pool.acquire() as connection:
            return await validate_named_network_preparation(
                connection,
                copy_target,
                control_schema=consumer.control_schema,
                roles=roles,
                source_witness_runtime=source_runtime,
                validation_snapshot=(
                    consumer.composition_runtime.validation_snapshot
                    if consumer.composition_runtime is not None
                    else None
                ),
            )

    return replace(consumer, seal_candidate=seal_candidate, preparation_roles=roles)


def validate_membership_refresh_worker_params(params):
    """Admit one exact request selector; neither the operation label nor UUID grants authority."""
    if (
        type(params) is not dict
        or set(params) != {"request_id", "operation"}
        or params["operation"] != "refresh_memberships"
        or type(params["request_id"]) is not str
    ):
        raise ValueError("registry_membership_refresh_worker_params_invalid")
    try:
        identity = UUID(params["request_id"])
    except ValueError:
        raise ValueError("registry_membership_refresh_worker_params_invalid") from None
    if not identity.int or str(identity) != params["request_id"]:
        raise ValueError("registry_membership_refresh_worker_params_invalid")
    return identity


async def _membership_refresh_config(config, cleanup, request_id):
    """Install only the fixed server-owned native runtime, never a job-selected factory."""
    from process.registry_membership_refresh_contract import (
        RegistryMembershipRefreshUnavailable,
    )
    from process.registry_membership_refresh_execution import (
        RegistryMembershipRefreshRuntime,
    )
    from process.registry_membership_refresh_runtime import (
        membership_refresh_worker_runtime,
    )

    if type(request_id) is not UUID or not request_id.int:
        raise ValueError("registry_membership_refresh_worker_params_invalid")
    runtime = await membership_refresh_worker_runtime(config, cleanup)
    if type(runtime) is not RegistryMembershipRefreshRuntime:
        raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_runtime_unavailable")
    consumer = replace(
        config.consumer,
        only_request_id=request_id,
        membership_refresh_only=True,
        membership_refresh_runtime=runtime,
    )
    return replace(config, consumer=consumer)


async def _require_membership_refresh_request(connection, consumer, request_id):
    """Read the actual protected queue identity before the exact family-filtered claim."""
    from process.registry_membership_refresh_contract import (
        RegistryMembershipRefreshUnavailable,
    )
    from process.registry_membership_refresh_queue import (
        validated_membership_refresh_request_record,
    )
    from process.registry_source_observation_store import _namespace

    if consumer.only_request_id != request_id or consumer.membership_refresh_only is not True:
        raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_request_unavailable")
    record = await connection.fetchrow(
        f"SELECT * FROM {_namespace(consumer.control_schema)}.registry_publication_request WHERE request_id=$1",
        request_id,
    )
    if record is None or record["request_id"] != request_id:
        raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_request_unavailable")
    validated_membership_refresh_request_record(record)


async def control_start(ctx, task):
    """Use the ordinary single-job lifecycle with exact pins and owned cancellation cleanup."""
    from process.control_cancel import ImportCancelledError, raise_if_cancelled
    from process.registry_cms_source_worker import _watch_control_cancel

    if (
        type(task) is not dict
        or set(task) != {"run_id", "request_id", "operation"}
        or type(task["run_id"]) is not str
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", task["run_id"]) is None
    ):
        raise ValueError("registry_membership_refresh_worker_params_invalid")
    request_id = validate_membership_refresh_worker_params({name: task[name] for name in ("request_id", "operation")})
    config = parse_registry_publication_worker_config(os.environ.get(CONFIG_ENV))
    await raise_if_cancelled(ctx, task)
    work = asyncio.create_task(run_registry_publication_worker(config, membership_refresh_request_id=request_id))
    watcher = asyncio.create_task(_watch_control_cancel(ctx, task))
    try:
        await asyncio.wait((work, watcher), return_when=asyncio.FIRST_COMPLETED)
        if work.done():
            return await work
        await watcher
    except asyncio.CancelledError, ImportCancelledError:
        raise
    except Exception:
        raise RuntimeError("registry_membership_refresh_worker_failed") from None
    finally:
        for owned in (work, watcher):
            if not owned.done():
                owned.cancel()
        await asyncio.gather(work, watcher, return_exceptions=True)


async def run_registry_publication_worker(config, *, membership_refresh_request_id=None):
    """Own both bounded pools and authority client through success, failure or cancellation."""
    if type(config) is not RegistryPublicationWorkerConfig:
        _invalid()
    async with AsyncExitStack() as stack:
        config = await install_source_witness_runtime(
            config, stack, membership_refresh_request_id=membership_refresh_request_id
        )
        if membership_refresh_request_id is None:
            authorize = await stack.enter_async_context(
                RegistryPublicationAuthorityClient(config.app_origin, config.service_token)
            )
        else:
            config = await _membership_refresh_config(config, stack, membership_refresh_request_id)
            authorize = config.consumer.membership_refresh_runtime.authorize
        plan_metadata_client = RegistryBenefitPlanMetadataClient.from_config(os.environ)
        if plan_metadata_client is not None:
            await stack.enter_async_context(plan_metadata_client)
        lifecycle = await asyncpg.create_pool(**_pool_options(config.lifecycle_url, 2, config.sql_limits))
        stack.push_async_callback(_close_pool, lifecycle)
        lifecycle = _BoundedPool(lifecycle, config.sql_limits.connect_timeout_seconds)
        execution = await asyncpg.create_pool(
            **_pool_options(
                config.execution_url,
                1,
                config.sql_limits,
            )
        )
        stack.push_async_callback(_close_pool, execution)
        execution = _BoundedPool(execution, config.sql_limits.connect_timeout_seconds)
        consumer = await _configured_worker_runtime(config, stack)
        if consumer.composition_runtime is not None:
            await consumer.composition_runtime.qualify()
        async with asyncio.timeout(config.operation_seconds):
            if membership_refresh_request_id is not None:
                async with lifecycle.acquire() as connection:
                    await _require_membership_refresh_request(connection, consumer, membership_refresh_request_id)
            result_by_name = await consume_registry_publication_once(
                lifecycle_pool=lifecycle,
                execution_pool=execution,
                authorize=authorize,
                config=consumer,
                initial_address_sources=config.initial_address_sources,
                plan_metadata_client=plan_metadata_client,
            )
    return _summary(result_by_name)


# Constant codes always contain an underscore, so hex or base64 secrets never qualify.
_FAILURE_CODE = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)+")
_FAILURE_CLASS = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,79}")


def _failure_causes(error):
    """Describe the exception chain by class, constant code, SQLSTATE and innermost frame only.

    Suppressed contexts (``raise ... from None``) are followed on purpose: they hold the cause that the
    consumer's constant wrapper code hides from the operator.
    """
    causes, seen_ids = [], set()
    while error is not None and id(error) not in seen_ids and len(causes) < 5:
        seen_ids.add(id(error))
        cause_by_name = {"class": type(error).__name__ if _FAILURE_CLASS.fullmatch(type(error).__name__) else "unnamed"}
        # Lookup errors carry data keys rather than codes.
        message = error.args[0] if len(error.args) == 1 and not isinstance(error, LookupError) else None
        if type(message) is str and len(message) <= 128 and _FAILURE_CODE.fullmatch(message):
            cause_by_name["code"] = message
        sqlstate = getattr(error, "sqlstate", None)
        if type(sqlstate) is str and re.fullmatch(r"[0-9A-Z]{5}", sqlstate):
            cause_by_name["sqlstate"] = sqlstate
        frames = traceback.extract_tb(error.__traceback__)
        if frames:
            cause_by_name["where"] = f"{os.path.basename(frames[-1].filename)}:{frames[-1].lineno}"
        causes.append(cause_by_name)
        error = error.__cause__ or error.__context__
    return causes


def _error(code, **fields):
    print(
        json.dumps({"state": "error", "code": code, **fields}, separators=(",", ":")),
        file=sys.stderr,
    )


def run_command():
    """Emit only bounded metadata, constant codes and code locations, never environment or exception text."""
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
    except Exception as error:
        _error("worker_failed", causes=_failure_causes(error))
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
    name="registry-publication-worker",
    cls=_WorkerCommand,
    help="Consume one durable publication request and exit.",
)
def registry_publication_worker_command():
    """Run the environment-configured worker once through the existing process group."""
    raise click.exceptions.Exit(run_command())


def _worker_database_urls(configured_by_name, consumer):
    urls = []
    for name in ("lifecycle_dsn", "execution_dsn"):
        dsn = configured_by_name[name]
        if type(dsn) is not str or not 0 < len(dsn) <= 4096:
            _invalid()
        urls.append(_database_url(dsn))
    lifecycle, execution = urls
    identity = lambda url: (
        url.host.lower(),
        url.port or 5432,
        url.database,
        dict(url.query),
    )
    if (
        identity(lifecycle) != identity(execution)
        or lifecycle.username != consumer.lifecycle_role
        or execution.username != consumer.execution_role
    ):
        _invalid()
    return lifecycle, execution


async def _registered_address_sources(custodian, request_scope, address_sources, schema):
    from process.registry_composition_resume import resolve_registered_compact_format

    request_id, approved_revision, expected_head = request_scope
    registered_compact = await resolve_registered_compact_format(
        custodian,
        UUID(str(request_id)),
        approved_revision,
        expected_head,
        control_schema=schema,
    )
    if registered_compact is True and not address_sources.compact:
        raise ValueError("registry_composition_registered_compact_format_required")
    if registered_compact is False and address_sources.compact:
        address_sources = replace(address_sources, compact=False)
    return address_sources
