# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run one durable publication request with dedicated owned connections."""

import asyncio
import json
import os
import re
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime
from uuid import UUID

import asyncpg
import click
from sqlalchemy import event, text
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
    operation_seconds: int = 3600
    sql_limits: RegistryPublicationSQLLimits = field(default_factory=RegistryPublicationSQLLimits)
    custody_url: object | None = field(default=None, repr=False)
    source_witness: RegistrySourceWitnessConfig | None = field(default=None, repr=False)

    def __post_init__(self):
        """Validate immutable SQL limits and deadline for direct and parsed callers."""
        if type(self.sql_limits) is not RegistryPublicationSQLLimits:
            _invalid()
        if type(self.operation_seconds) is not int or not 1 <= self.operation_seconds <= 86400:
            _invalid()
        if self.source_witness is not None:
            if (
                type(self.source_witness) is not RegistrySourceWitnessConfig
                or self.custody_url is None
                or type(self.initial_address_sources) is not RegistryCompositionAddressSources
                or not self.initial_address_sources.compact
                or self.initial_address_sources.source_witness_runtime is not None
            ):
                _invalid()
            self.source_witness.__post_init__()
        _verify_custody_config(self)


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
            tuple(EMProviderInputRelation(**provider_input) for provider_input in provider_inputs)
        )
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
        compact=configured.get("compact", False),
        **recipes_by_phase,
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
        )
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ArgumentError:
        _invalid()


_BACKEND_IDENTITY_FIELDS = (
    "database_name",
    "database_oid",
    "server_address",
    "server_port",
    "server_started",
    "control_oid",
    "inbox_oid",
)


async def _backend_observation(connection, schema, roles):
    """Read the actual backend and native identity without assuming another role."""
    from process.registry_cms_source_worker import _IDENTITY_SQL

    if connection.is_in_transaction():
        raise ValueError("registry_publication_worker_backend_invalid")
    identity_sql = (
        "SELECT native.*,pg_catalog.clock_timestamp() AS observed_at,backend.pid,backend.backend_start,"
        "backend.backend_type,backend.datid::bigint,backend.usesysid::bigint,"
        "pg_catalog.current_setting('role') AS assumed_role,"
        "(SELECT pg_catalog.jsonb_object_agg(rolname,oid::bigint) FROM pg_catalog.pg_roles "
        "WHERE rolname=ANY($3::text[]))::text AS role_oids "
        "FROM (" + _IDENTITY_SQL + ") native JOIN pg_catalog.pg_stat_activity backend "
        "ON backend.pid=pg_catalog.pg_backend_pid()"
    )
    observed = await connection.fetchrow(identity_sql, schema, schema, roles)
    if observed is None:
        raise ValueError("registry_publication_worker_backend_invalid")
    return dict(observed)


def _backend_actor(connection, observed, role, roles):
    """Require the protocol backend to belong to the configured direct login."""
    role_oids = json.loads(observed["role_oids"] or "{}")
    if (
        set(role_oids) != set(roles)
        or any(type(oid) is not int or oid <= 0 for oid in role_oids.values())
        or observed["safe_login"] is not True
        or observed["current_role"] != role
        or observed["session_role"] != role
        or observed["assumed_role"] != "none"
        or observed["backend_type"] != "client backend"
        or observed["pid"] != connection.get_server_pid()
        or observed["datid"] != observed["database_oid"]
        or observed["usesysid"] != role_oids.get(role)
        or observed["control_oid"] is None
        or any(not isinstance(observed[name], datetime) for name in ("observed_at", "backend_start"))
        or observed["backend_start"] > observed["observed_at"]
    ):
        raise ValueError("registry_publication_worker_backend_invalid")
    return tuple(sorted(role_oids.items()))


@dataclass(frozen=True, repr=False)
class _FreshBackendQualification:
    schema: str
    not_before: datetime
    identity: tuple
    role_oids: tuple

    def verifier(self, role):
        """Bind one actor to every new connection and checkout, including reconnects."""
        if role not in dict(self.role_oids):
            raise ValueError("registry_publication_worker_backend_invalid")

        async def verify(connection):
            """Reject a stale physical backend or any change in its authenticated origin."""
            observed = await _backend_observation(connection, self.schema, tuple(dict(self.role_oids)))
            actual_oids = _backend_actor(connection, observed, role, tuple(dict(self.role_oids)))
            if (
                actual_oids != self.role_oids
                or tuple(observed[name] for name in _BACKEND_IDENTITY_FIELDS) != self.identity
                or observed["backend_start"] < self.not_before
            ):
                raise ValueError("registry_publication_worker_backend_invalid")

        return verify


async def _qualify_fresh_backends(connection, *, schema, observer_role, execution_roles):
    """Anchor provisioned roles to server time before opening their physical backends."""
    roles = tuple(sorted({observer_role, *execution_roles}))
    observed = await _backend_observation(connection, schema, roles)
    role_oids = _backend_actor(connection, observed, observer_role, roles)
    return _FreshBackendQualification(
        schema,
        observed["observed_at"],
        tuple(observed[name] for name in _BACKEND_IDENTITY_FIELDS),
        role_oids,
    )


async def _worker_backend_qualification(config, connection, observer_role):
    """Qualify execution and custody from the worker's existing independent observer."""
    execution_roles = (config.consumer.execution_role,)
    if config.custody_url is not None:
        execution_roles += (config.custody_url.username,)
    return await _qualify_fresh_backends(
        connection,
        schema=config.consumer.control_schema,
        observer_role=observer_role,
        execution_roles=execution_roles,
    )


def _connection_verifier(sql_limits, backend_verifier):
    if backend_verifier is None:
        return sql_limits.verify if sql_limits is not None else None

    async def verify(connection):
        """Keep finite SQL limits and backend qualification on the same connection."""
        try:
            if sql_limits is not None:
                await sql_limits.verify(connection)
            await backend_verifier(connection)
        except ValueError:
            raise RuntimeError("registry_publication_consumer_unavailable") from None

    return verify


def _pool_options(url, maximum, sql_limits=None, *, backend_verifier=None):
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
    verifier = _connection_verifier(sql_limits if maximum == 1 else None, backend_verifier)
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
        or result_by_name["state"] != "completed"
    ):
        raise ValueError("registry_publication_worker_result_invalid")
    identity = result_by_name["request_id"]
    if type(identity) is not str or len(identity) != 36 or str(UUID(identity)) != identity or not UUID(identity).int:
        raise ValueError("registry_publication_worker_result_invalid")
    return {"request_id": identity, "state": "completed"}


def _worker_sessions(config, stack, url, *, backend_verifier=None):
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
        connection.run_async(_connection_verifier(config.sql_limits, backend_verifier))

    event.listen(engine.sync_engine, "checkout", verify_checkout)
    return async_sessionmaker(engine, class_=AsyncSession)


def _ptg_runtime(config, stack, *, backend_verifier=None):
    if config.ptg is None:
        return config.consumer
    sessions = _worker_sessions(config, stack, config.execution_url, backend_verifier=backend_verifier)
    runtime = RegistryPublicationPTGRuntime(sessions, config.ptg.store, config.ptg.read_budget, config.sql_limits)
    return replace(config.consumer, composition_runtime=runtime)


def _custody_config_url(value):
    if type(value) is not str or not 0 < len(value) <= 4096:
        _invalid()
    return _database_url(value)


def _custody_roles(config):
    consumer = config.consumer
    if type(consumer) is RegistryPublicationConsumerConfig:
        roles = set(consumer.custody_roles)
    else:
        roles = {
            getattr(consumer, name, None)
            for name in (
                "lifecycle_role",
                "execution_role",
                "api_role",
                "physical_owner_role",
                "owner_role",
            )
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


def _custody_actor_roles(config, consumer=None):
    """Retain original and effective actors independently of protected ownership metadata."""
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


def _custody_runtime_roles(config, identity, consumer=None):
    """Exclude only authenticated owners that have no runtime actor occurrence."""
    consumer = config.consumer if consumer is None else consumer
    actors = set(_custody_actor_roles(config, consumer))
    roles = set(_custody_roles(config)) | actors
    for current in (config.consumer, consumer):
        store = getattr(getattr(current, "composition_runtime", None), "store", None)
        roles.add(getattr(store, "owner_role", None))
    roles.discard(None)
    roles -= {identity["schema_owner_role"], identity["custody_owner_role"]} - actors
    if len(roles) > 128:
        _invalid()
    return tuple(sorted(roles))


def _verify_custody_config(config, consumer=None):
    if config.custody_url is None:
        return
    if type(config.custody_url) is not URL:
        _invalid()
    url = _database_url(config.custody_url.render_as_string(hide_password=False))
    execution = config.execution_url
    endpoint = lambda item: (
        item.host.lower(),
        item.port or 5432,
        item.database,
        dict(item.query),
    )
    consumer = config.consumer if consumer is None else consumer
    forbidden_roles = {
        *_custody_roles(config),
        *_custody_actor_roles(config, consumer),
        getattr(config.consumer, "queue_owner_role", None),
        getattr(consumer, "queue_owner_role", None),
    }
    for current in (config.consumer, consumer):
        store = getattr(getattr(current, "composition_runtime", None), "store", None)
        forbidden_roles.add(getattr(store, "owner_role", None))
    if endpoint(url) != endpoint(execution) or url.username in forbidden_roles:
        _invalid()


async def _custody_identity(connection, schema, role):
    # Reuse the existing direct-login and native server identity contract.
    from process.registry_cms_source_worker import _IDENTITY_SQL

    identity_sql = (
        "SELECT native.*,owner.oid::bigint AS custody_owner_oid,owner.rolname::text AS custody_owner_role,"
        "n.nspowner::bigint AS schema_owner_oid,schema_owner.rolname::text AS schema_owner_role,"
        "candidate.oid::bigint AS candidate_table_oid,candidate.relowner::bigint AS candidate_owner_oid,"
        "ledger.oid::bigint AS seal_table_oid,"
        "(pg_has_role(current_user,n.nspowner,'SET') OR pg_has_role(current_user,n.nspowner,'USAGE') "
        "OR pg_has_role(current_user,n.nspowner,'MEMBER')) "
        "AS can_assume_schema_owner "
        "FROM (" + _IDENTITY_SQL + ") native JOIN pg_namespace n ON n.oid::bigint=native.control_oid "
        "JOIN pg_roles schema_owner ON schema_owner.oid=n.nspowner "
        "JOIN pg_class candidate ON candidate.relnamespace=n.oid AND candidate.relname='network_membership_candidate' "
        "JOIN pg_class ledger ON ledger.relnamespace=n.oid AND ledger.relname='network_candidate_custody' "
        "JOIN pg_roles owner ON owner.oid=ledger.relowner"
    )
    identity_record = await connection.fetchrow(identity_sql, schema, schema)
    if identity_record is None:
        raise ValueError("registry_candidate_custody_connection_invalid")
    identity_by_name = dict(identity_record)
    can_assume_schema_owner = identity_by_name.pop("can_assume_schema_owner")
    if (
        identity_by_name.pop("safe_login") is not True
        or identity_by_name.pop("current_role") != role
        or identity_by_name.pop("session_role") != role
        or identity_by_name.get("control_oid") is None
        or identity_by_name["candidate_owner_oid"] != identity_by_name["schema_owner_oid"]
        or (
            identity_by_name["custody_owner_oid"] != identity_by_name["schema_owner_oid"]
            and can_assume_schema_owner is not False
        )
    ):
        raise ValueError("registry_candidate_custody_connection_invalid")
    return identity_by_name


@asynccontextmanager
async def _address_preparation_snapshot(pool, sessions, consumer):
    """Keep PTG's genuine approval fence on the independent preparation backend."""
    if sessions is None:
        async with (
            pool.acquire() as custodian,
            custodian.transaction(isolation="repeatable_read"),
        ):
            yield custodian, None
        return
    from process.registry_company_approval_fence import (
        registry_company_approval_transaction,
        require_registry_company_approval_fence,
    )

    async with registry_company_approval_transaction(sessions, control_schema=consumer.control_schema) as session:
        await require_registry_company_approval_fence(session, consumer.control_schema)
        custodian = (await (await session.connection()).get_raw_connection()).driver_connection
        runtime = consumer.composition_runtime
        yield custodian, (session, runtime.store, runtime.read_budget)


def _address_preparer(config, consumer, pool, identity, role, *, sessions=None):
    """Use only the existing independent custodian before execution opens its snapshot."""
    from process.network_candidate_custody import _final_roles, _q_reach_closed
    from process.registry_candidate_composition import (
        RegistryAddressArtifactCustody,
        prepare_registry_composition_address_artifact,
    )

    async def prepare(
        *,
        request_id,
        approved_revision,
        source_manifest,
        expected_head,
        address_sources,
    ):
        """Return only a committed independently sealed artifact or the verified retained-only inputs."""
        runtime_roles = _custody_runtime_roles(config, identity, consumer)
        source_runtime = _configured_source_witness_runtime(config, consumer, identity)
        _final_roles(identity["custody_owner_role"], runtime_roles)
        schema = consumer.control_schema
        async with _address_preparation_snapshot(pool, sessions, consumer) as (
            custodian,
            office_authority,
        ):
            if await _custody_identity(custodian, schema, role) != identity:
                raise ValueError("registry_candidate_custody_connection_invalid")
            await _q_reach_closed(custodian, identity, runtime_roles)
            address_sources = await _registered_address_sources(
                custodian, (request_id, approved_revision, expected_head), address_sources, schema
            )
            if source_runtime is not None:
                address_sources = replace(address_sources, source_witness_runtime=source_runtime)
            await _require_execution_address_reads(custodian, address_sources, consumer)
            artifact = await prepare_registry_composition_address_artifact(
                custodian,
                request_id=UUID(str(request_id)),
                approved_revision=approved_revision,
                address_sources=address_sources,
                source_manifest=source_manifest,
                expected_head=expected_head,
                custody=RegistryAddressArtifactCustody(
                    identity["custody_owner_role"],
                    runtime_roles,
                    schema,
                    address_reader_roles=tuple(sorted(set(consumer.loader_roles + consumer.reader_roles)))
                    if address_sources.compact
                    else None,
                ),
                office_authority=office_authority,
            )
            if await _custody_identity(custodian, schema, role) != identity:
                raise ValueError("registry_candidate_custody_connection_invalid")
            await _q_reach_closed(custodian, identity, runtime_roles)
        return address_sources if artifact is None else replace(address_sources, prepared_address_artifact=artifact)

    return prepare


def _configured_source_witness_runtime(config, consumer, identity):
    """Read only the operator's unserialized capability; no recipe selects factory or actors."""
    from process.registry_cms_source_consumer import RegistryCMSSourceConsumerConfig
    from process.registry_ptg_source_consumer import RegistryPTGSourceConsumerConfig
    from process.registry_source_recipe_composition import SourceWitnessRuntime

    runtime = getattr(getattr(config, "initial_address_sources", None), "source_witness_runtime", None)
    if runtime is None:
        return None
    if type(runtime) is not SourceWitnessRuntime:
        raise ValueError("registry_source_witness_runtime_invalid")
    runtime.__post_init__()
    required = set(_custody_actor_roles(config, consumer)) | {
        config.custody_url.username,
        identity["custody_owner_role"],
    }
    for current in (config.consumer, consumer):
        if type(current) is RegistryPublicationConsumerConfig:
            required.add(current.queue_owner_role)
        elif type(current) in {RegistryCMSSourceConsumerConfig, RegistryPTGSourceConsumerConfig}:
            required.add(current.owner_role)
        else:
            raise ValueError("registry_source_witness_worker_unsupported")
    if not required <= set(runtime.runtime_roles):
        raise ValueError("registry_source_witness_runtime_census_differs")
    return runtime


async def _custody_runtime(config, stack, consumer, execution_connection, *, backend_verifier=None):
    """Install only the qualified independent preparation and sealing callbacks."""
    if config.custody_url is None:
        return consumer
    from process.network_candidate_custody import (
        _q_reach_closed,
        seal_network_candidate_custody,
        verify_network_candidate_custody,
    )

    _verify_custody_config(config, consumer)
    schema = consumer.control_schema
    identity = await _custody_identity(execution_connection, schema, consumer.execution_role)
    source_runtime = _configured_source_witness_runtime(config, consumer, identity)
    await _q_reach_closed(
        execution_connection,
        identity,
        _custody_runtime_roles(config, identity, consumer),
    )
    if config.ptg is not None:
        return _ptg_custody_runtime(config, stack, consumer, identity, backend_verifier=backend_verifier)
    pool = await asyncpg.create_pool(
        **_pool_options(config.custody_url, 1, config.sql_limits, backend_verifier=backend_verifier)
    )
    stack.push_async_callback(_close_pool, pool)
    pool = _BoundedPool(pool, config.sql_limits.connect_timeout_seconds)
    role = config.custody_url.username

    async def seal_candidate(copy_target, *, pre_enrolled_only=False):
        """Seal this candidate through the separately qualified operator pool."""
        async with pool.acquire() as custodian:

            async def is_authorized(connection, target):
                """Require the same original operator and exact candidate for every custody phase."""
                return (
                    connection is custodian
                    and target == copy_target
                    and await _custody_identity(connection, schema, role) == identity
                )

            runtime_roles = _custody_runtime_roles(config, identity, consumer)
            sealed = await seal_network_candidate_custody(
                custodian,
                copy_target,
                control_schema=schema,
                runtime_roles=runtime_roles,
                authorize=is_authorized,
                pre_enrolled_only=pre_enrolled_only,
                **({"source_witness_runtime": source_runtime} if source_runtime is not None else {}),
            )
            async with custodian.transaction(isolation="repeatable_read", readonly=True):
                verified = await verify_network_candidate_custody(
                    custodian, copy_target, control_schema=schema, required=True
                )
            if verified != sealed:
                raise ValueError("registry_candidate_custody_connection_invalid")

    return _with_address_preparation(config, stack, consumer, pool, identity, seal_candidate, backend_verifier)


def _with_address_preparation(
    config,
    stack,
    consumer,
    pool,
    identity,
    seal_candidate,
    backend_verifier,
    *,
    sessions=None,
):
    """Keep supported source consumers on the same committed preparation route."""
    from process.registry_cms_source_consumer import RegistryCMSSourceConsumerConfig
    from process.registry_ptg_source_consumer import RegistryPTGSourceConsumerConfig

    if type(consumer) in {
        RegistryPublicationConsumerConfig,
        RegistryCMSSourceConsumerConfig,
        RegistryPTGSourceConsumerConfig,
    }:
        if sessions is None and consumer.composition_runtime is not None:
            sessions = _worker_sessions(config, stack, config.custody_url, backend_verifier=backend_verifier)
        return replace(
            consumer,
            seal_candidate=seal_candidate,
            prepare_address_sources=_address_preparer(
                config,
                consumer,
                pool,
                identity,
                config.custody_url.username,
                sessions=sessions,
            ),
        )
    return replace(consumer, seal_candidate=seal_candidate)


async def _qualified_worker_runtime(config, stack, qualification, connection):
    """Apply one observer proof to both execution factories and the custody pool."""
    consumer = _ptg_runtime(
        config,
        stack,
        backend_verifier=qualification.verifier(config.consumer.execution_role),
    )
    return await _custody_runtime(
        config,
        stack,
        consumer,
        connection,
        backend_verifier=qualification.verifier(config.custody_url.username)
        if config.custody_url is not None
        else None,
    )


def _ptg_custody_runtime(config, stack, consumer, identity, *, backend_verifier=None):
    """Own an independent operator session, never the producer execution factory."""
    from process.network_candidate_custody import (
        seal_network_candidate_custody_in_session,
        verify_network_candidate_custody,
    )
    from process.registry_company_approval_fence import (
        registry_company_approval_transaction,
    )

    source_witness_runtime = _configured_source_witness_runtime(config, consumer, identity)
    sessions = _worker_sessions(config, stack, config.custody_url, backend_verifier=backend_verifier)
    schema, role = consumer.control_schema, config.custody_url.username

    async def seal_candidate(copy_target, *, pre_enrolled_only=False):
        """Seal through the genuine independent native office-capable session."""
        async with registry_company_approval_transaction(sessions, control_schema=schema) as session:
            custodian = (await (await session.connection()).get_raw_connection()).driver_connection

            async def is_authorized(connection, target):
                """Recheck this exact custodian connection and candidate before transfer."""
                return (
                    connection is custodian
                    and target == copy_target
                    and await _custody_identity(connection, schema, role) == identity
                )

            sealed = await seal_network_candidate_custody_in_session(
                session,
                copy_target,
                control_schema=schema,
                runtime_roles=_custody_runtime_roles(config, identity, consumer),
                authorize=is_authorized,
                office_store=config.ptg.store,
                office_read_budget=config.ptg.read_budget,
                pre_enrolled_only=pre_enrolled_only,
                **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
            )
        async with sessions() as session, session.begin():
            await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            await session.execute(text("SET TRANSACTION READ ONLY"))
            custodian = (await (await session.connection()).get_raw_connection()).driver_connection
            verified = await verify_network_candidate_custody(
                custodian, copy_target, control_schema=schema, required=True
            )
        if verified != sealed:
            raise ValueError("registry_candidate_custody_connection_invalid")

    return _with_address_preparation(
        config,
        stack,
        consumer,
        None,
        identity,
        seal_candidate,
        backend_verifier,
        sessions=sessions,
    )


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
        async with lifecycle.acquire() as connection:
            qualification = await _worker_backend_qualification(config, connection, config.consumer.lifecycle_role)
        execution_verifier = qualification.verifier(config.consumer.execution_role)
        execution = await asyncpg.create_pool(
            **_pool_options(
                config.execution_url,
                1,
                config.sql_limits,
                backend_verifier=execution_verifier,
            )
        )
        stack.push_async_callback(_close_pool, execution)
        execution = _BoundedPool(execution, config.sql_limits.connect_timeout_seconds)
        async with execution.acquire() as connection:
            consumer = await _qualified_worker_runtime(config, stack, qualification, connection)
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


def _error(code):
    print(
        json.dumps({"state": "error", "code": code}, separators=(",", ":")),
        file=sys.stderr,
    )


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


async def _require_execution_address_reads(custodian, address_sources, consumer):
    for input_pin in (address_sources.base_source, address_sources.npi_source):
        if input_pin is not None and not await custodian.fetchval(
            "SELECT has_schema_privilege($1::name,to_regnamespace($2)::oid,'USAGE') "
            "AND has_table_privilege($1::name,to_regclass($3)::oid,'SELECT')",
            consumer.execution_role,
            input_pin.schema_name,
            f"{_identifier(input_pin.schema_name)}.{_identifier(input_pin.table_name)}",
        ):
            raise ValueError("registry_composition_execution_input_reader_unavailable")


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
