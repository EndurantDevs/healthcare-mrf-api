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


@dataclass(frozen=True)
class RegistryPublicationPTGRuntime:
    """Host-owned single execution factory, protected scope store and bounded limits."""

    sessions: object
    store: RegistryPTGProducerScopeStore
    read_budget: RegistryPTGGraphReadBudget
    _limits: tuple = field(init=False, repr=False)

    def __post_init__(self):
        _source_engine(self.sessions)
        if (
            type(self.store) is not RegistryPTGProducerScopeStore
            or type(self.read_budget) is not RegistryPTGGraphReadBudget
        ):
            raise ValueError("registry_publication_ptg_runtime_invalid")
        _namespace(self.store.control_schema)
        limits = tuple(getattr(self.read_budget, name) for name in _LIMIT_FIELDS)
        RegistryPTGGraphReadBudget(**dict(zip(_LIMIT_FIELDS, limits, strict=True)))
        object.__setattr__(self, "_limits", limits)

    async def _same_execution_database(self, connection, control_schema):
        engine = _source_engine(self.sessions)
        raw_sql = _IDENTITY_SQL.replace(":schema", "$1")
        existing_by_name = dict(await connection.fetchrow(raw_sql, control_schema))
        async with engine.connect() as owned:
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
