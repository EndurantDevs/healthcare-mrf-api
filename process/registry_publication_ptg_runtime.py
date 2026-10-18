# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Trusted genuine-session composition adapter for retained exact PTG offices."""

from dataclasses import dataclass, field, replace

from sqlalchemy import text

from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_company_approval_fence import _source_engine
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_source_observation_store import _namespace

_IDENTITY_SQL = """SELECT current_database()::text AS database_name,current_user::text AS current_role,
 session_user::text AS session_role,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
 inet_server_addr()::text AS server_address,inet_server_port() AS server_port,
 pg_postmaster_start_time() AS server_started,to_regnamespace(:schema)::oid::bigint AS control_oid"""
_LIMIT_FIELDS = ("maximum_bytes", "maximum_pages", "maximum_coordinates")


_SQL_LIMIT_SQL = """SELECT
 pg_size_bytes(current_setting('work_mem'))::bigint AS work_mem_bytes,
 pg_size_bytes(current_setting('maintenance_work_mem'))::bigint AS maintenance_work_mem_bytes,
 pg_size_bytes(current_setting('temp_file_limit'))::bigint AS temp_file_limit_bytes,
 (SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout') AS statement_timeout_ms,
 (SELECT setting::bigint FROM pg_settings WHERE name='lock_timeout') AS lock_timeout_ms,
 current_setting('max_parallel_workers_per_gather')::bigint AS parallel_query,
 current_setting('max_parallel_maintenance_workers')::bigint AS parallel_maintenance"""


@dataclass(frozen=True)
class RegistryPublicationSQLLimits:
    """Trusted finite per-backend limits; physical-host quotas remain deployment policy."""

    work_mem_bytes: int = 16 * 1024 * 1024
    maintenance_work_mem_bytes: int = 64 * 1024 * 1024
    temp_file_limit_bytes: int = 256 * 1024 * 1024
    statement_timeout_ms: int = 120000
    lock_timeout_ms: int = 5000
    connect_timeout_seconds: int = 5

    def __post_init__(self):
        for name, minimum, maximum, unit in (
            ("work_mem_bytes", 64 * 1024, (2**31 - 1) * 1024, 1024),
            ("maintenance_work_mem_bytes", 1024 * 1024, (2**31 - 1) * 1024, 1024),
            ("temp_file_limit_bytes", 1024, (2**31 - 1) * 1024, 1024),
            ("statement_timeout_ms", 1, 2**31 - 1, 1),
            ("lock_timeout_ms", 1, 2**31 - 1, 1),
            ("connect_timeout_seconds", 1, 60, 1),
        ):
            value = getattr(self, name)
            if type(value) is not int or not minimum <= value <= maximum or value % unit:
                raise ValueError("registry_publication_worker_sql_limits_invalid")

    def server_settings(self):
        """Only USERSET settings; the operator must supply the finite temp default."""
        return dict(
            work_mem=f"{self.work_mem_bytes // 1024}kB",
            maintenance_work_mem=f"{self.maintenance_work_mem_bytes // 1024}kB",
            statement_timeout=f"{self.statement_timeout_ms}ms",
            lock_timeout=f"{self.lock_timeout_ms}ms",
            max_parallel_workers_per_gather="0",
            max_parallel_maintenance_workers="0",
        )

    async def verify(self, connection):
        """Refuse unsafe effective settings on the actual native executing backend."""
        observed = await connection.fetchrow(_SQL_LIMIT_SQL)
        for name in (
            "work_mem_bytes",
            "maintenance_work_mem_bytes",
            "temp_file_limit_bytes",
            "statement_timeout_ms",
            "lock_timeout_ms",
        ):
            value = observed[name]
            if type(value) is not int or not 0 < value <= getattr(self, name):
                raise RuntimeError("registry_publication_worker_sql_limits_unavailable")
        if observed["parallel_query"] != 0 or observed["parallel_maintenance"] != 0:
            raise RuntimeError("registry_publication_worker_sql_limits_unavailable")


@dataclass(frozen=True)
class RegistryPublicationPTGRuntime:
    """Host-owned single execution factory, protected scope store and bounded limits."""

    sessions: object
    store: RegistryPTGProducerScopeStore
    read_budget: RegistryPTGGraphReadBudget
    sql_limits: RegistryPublicationSQLLimits | None = None
    _limits: tuple = field(init=False, repr=False)

    def __post_init__(self):
        _source_engine(self.sessions)
        if self.sql_limits is not None and type(self.sql_limits) is not RegistryPublicationSQLLimits:
            raise ValueError("registry_publication_ptg_runtime_invalid")
        if (
            type(self.store) is not RegistryPTGProducerScopeStore
            or type(self.read_budget) is not RegistryPTGGraphReadBudget
        ):
            raise ValueError("registry_publication_ptg_runtime_invalid")
        _namespace(self.store.control_schema)
        limits = tuple(getattr(self.read_budget, name) for name in _LIMIT_FIELDS)
        RegistryPTGGraphReadBudget(**dict(zip(_LIMIT_FIELDS, limits, strict=True)))
        object.__setattr__(self, "_limits", limits)

    async def qualify(self):
        """Check the owned genuine execution backend before the worker claims work."""
        if self.sql_limits is not None:
            engine = _source_engine(self.sessions)
            async with engine.connect() as connection:
                raw = await connection.get_raw_connection()
                await self.sql_limits.verify(raw.driver_connection)

    async def _same_execution_database(self, connection, control_schema):
        engine = _source_engine(self.sessions)
        raw_sql = _IDENTITY_SQL.replace(":schema", "$1")
        existing_by_name = dict(await connection.fetchrow(raw_sql, control_schema))
        async with engine.connect() as owned:
            if self.sql_limits is not None:
                raw = await owned.get_raw_connection()
                await self.sql_limits.verify(raw.driver_connection)
            actual_by_name = dict(
                (await owned.execute(text(_IDENTITY_SQL), {"schema": control_schema})).mappings().one()
            )
        if (
            existing_by_name != actual_by_name
            or actual_by_name["current_role"] != actual_by_name["session_role"]
            or actual_by_name["control_oid"] is None
        ):
            raise ValueError("registry_publication_ptg_execution_changed")

    async def compose(self, connection, **composition_by_name):
        """Use the actual pre-snapshot company fence and retain its driver through sealing."""
        control_schema = _namespace(composition_by_name.get("control_schema"))[1:-1]
        if (self.store.control_schema or control_schema) != control_schema or connection.is_in_transaction():
            raise ValueError("registry_publication_ptg_runtime_invalid")
        address_sources = composition_by_name.get("address_sources")
        if type(address_sources) is not RegistryCompositionAddressSources:
            raise ValueError("registry_publication_ptg_runtime_invalid")
        await self._same_execution_database(connection, control_schema)
        read_budget = RegistryPTGGraphReadBudget(**dict(zip(_LIMIT_FIELDS, self._limits, strict=True)))
        composition_by_name = {
            **composition_by_name,
            "address_sources": replace(address_sources, office_scope_store=self.store, office_read_budget=read_budget),
        }
        return await compose_registry_membership_candidate(self.sessions, **composition_by_name)
