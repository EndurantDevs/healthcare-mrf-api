# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded source-local serving identity for queued address archive exports."""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from api import ptg2_geo_projection as geo_projection
from process.ext import address_alias_sql

entity_address_unified = importlib.import_module("process.entity_address_unified")
CONTRACT = "entity_address_observed_serving.postgres.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_POSTGRES_OID_MAX = (1 << 32) - 1
_RELATIONS = tuple(
    (model.__name__, model.__tablename__)
    for model in (
        entity_address_unified.EntityAddressUnified,
        *entity_address_unified.SUPPORT_TABLE_MODELS,
    )
)
_LOCAL_GEO_DEPENDENCIES = (
    "npi_address",
    "mrf_address",
    "doctor_clinician_address",
    "geo_zip_lookup",
)
_SHARED_GEO_DEPENDENCIES = ("tiger.zip_state", "tiger.zcta5")


@dataclass(frozen=True)
class EntityAddressObservedServingCapture:
    """Source-local identity observed after one address family is published."""

    contract: str
    source_schema: str
    relation_oids: tuple[int, ...]
    alias_schema_version: int
    alias_ruleset_version: int
    alias_generation: int
    geo_assurance_version: int
    geo_active_table_oid: int
    geo_active_relation_signature: tuple[tuple[str, int, int], ...]

    def as_dict(self) -> dict[str, Any]:
        """Return the bounded queue form; every identity remains source-local."""

        return {
            "contract": self.contract,
            "source_schema": self.source_schema,
            "relations": [
                {
                    "model_name": model_name,
                    "table_name": table_name,
                    "relation_oid": relation_oid,
                }
                for (model_name, table_name), relation_oid in zip(
                    _RELATIONS,
                    self.relation_oids,
                    strict=True,
                )
            ],
            "alias_state": {
                "schema_version": self.alias_schema_version,
                "active_ruleset_version": self.alias_ruleset_version,
                "generation": self.alias_generation,
            },
            "geo_assurance": {
                "version": self.geo_assurance_version,
                "active_table_oid": self.geo_active_table_oid,
                "active_relation_signature": {
                    relation_name: [relation_oid, relation_filenode]
                    for relation_name, relation_oid, relation_filenode in self.geo_active_relation_signature
                },
            },
        }


def _schema_name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("entity-address observed serving requires a schema name")
    normalized = entity_address_unified._validate_schema_name(value)
    if _IDENTIFIER.fullmatch(normalized) is None:
        raise ValueError("entity-address observed serving requires a safe schema name")
    return normalized


def _quoted(value: str) -> str:
    return f'"{value}"'


def _positive_oid(value: object, *, field_name: str) -> int:
    if type(value) is not int or not 0 < value <= _POSTGRES_OID_MAX:
        raise ValueError(f"entity-address observed serving {field_name} is invalid")
    return value


def _signature_tuple(value: object, *, schema_name: str) -> tuple[tuple[str, int, int], ...]:
    expected_names = {
        *(f"{schema_name}.{table_name}" for table_name in _LOCAL_GEO_DEPENDENCIES),
        *_SHARED_GEO_DEPENDENCIES,
    }
    if not isinstance(value, Mapping) or set(value) != expected_names:
        raise ValueError("entity-address observed serving geo signature is invalid")
    entries = []
    for relation_name, identity in value.items():
        if (
            not isinstance(relation_name, str)
            or len(relation_name.encode("utf-8")) > 127
            or not isinstance(identity, (list, tuple))
            or len(identity) != 2
        ):
            raise ValueError("entity-address observed serving geo signature is invalid")
        relation_oid = _positive_oid(identity[0], field_name="geo relation OID")
        relation_filenode = _positive_oid(identity[1], field_name="geo relation filenode")
        entries.append((relation_name, relation_oid, relation_filenode))
    return tuple(sorted(entries))


def validate_entity_address_observed_serving_capture(
    capture_value: Mapping[str, Any] | EntityAddressObservedServingCapture,
    *,
    schema_name: str | None = None,
) -> EntityAddressObservedServingCapture:
    """Validate one queued source-local identity without treating it as portable."""

    value = capture_value.as_dict() if isinstance(capture_value, EntityAddressObservedServingCapture) else capture_value
    if not isinstance(value, Mapping):
        raise ValueError("entity-address observed serving capture is invalid")
    schema = _schema_name(schema_name if schema_name is not None else value.get("source_schema"))
    if set(value) != {
        "contract",
        "source_schema",
        "relations",
        "alias_state",
        "geo_assurance",
    }:
        raise ValueError("entity-address observed serving capture is invalid")
    relation_values = value["relations"]
    if not isinstance(relation_values, (list, tuple)) or len(relation_values) != len(_RELATIONS):
        raise ValueError("entity-address observed serving relation identity is invalid")
    relation_oids = []
    for expected_relation, relation_value in zip(_RELATIONS, relation_values, strict=True):
        if (
            not isinstance(relation_value, Mapping)
            or set(relation_value) != {"model_name", "table_name", "relation_oid"}
            or (relation_value["model_name"], relation_value["table_name"]) != expected_relation
        ):
            raise ValueError("entity-address observed serving relation identity is invalid")
        relation_oids.append(_positive_oid(relation_value["relation_oid"], field_name="relation OID"))
    alias_state = value["alias_state"]
    if (
        not isinstance(alias_state, Mapping)
        or set(alias_state) != {"schema_version", "active_ruleset_version", "generation"}
        or type(alias_state["schema_version"]) is not int
        or alias_state["schema_version"] != address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION
        or type(alias_state["active_ruleset_version"]) is not int
        or alias_state["active_ruleset_version"] != address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION
        or type(alias_state["generation"]) is not int
        or alias_state["generation"] < 0
    ):
        raise ValueError("entity-address observed serving alias state is invalid")
    geo_assurance = value["geo_assurance"]
    if (
        not isinstance(geo_assurance, Mapping)
        or set(geo_assurance) != {"version", "active_table_oid", "active_relation_signature"}
        or type(geo_assurance["version"]) is not int
        or geo_assurance["version"] != geo_projection.GEO_ASSURANCE_VERSION
        or value["contract"] != CONTRACT
        or value["source_schema"] != schema
    ):
        raise ValueError("entity-address observed serving capture is invalid")
    geo_active_table_oid = _positive_oid(geo_assurance["active_table_oid"], field_name="geo active table OID")
    if geo_active_table_oid != relation_oids[0]:
        raise ValueError("entity-address observed serving geo table identity is invalid")
    return EntityAddressObservedServingCapture(
        contract=CONTRACT,
        source_schema=schema,
        relation_oids=tuple(relation_oids),
        alias_schema_version=alias_state["schema_version"],
        alias_ruleset_version=alias_state["active_ruleset_version"],
        alias_generation=alias_state["generation"],
        geo_assurance_version=geo_assurance["version"],
        geo_active_table_oid=geo_active_table_oid,
        geo_active_relation_signature=_signature_tuple(
            geo_assurance["active_relation_signature"],
            schema_name=schema,
        ),
    )


async def _relation_oid(session, schema_name: str, table_name: str) -> int:
    row = (
        (
            await session.execute(
                text(
                    "SELECT relation.oid::bigint AS relation_oid, relation.relkind::text AS relkind, "
                    "relation.relpersistence::text AS relpersistence, relation.relrowsecurity, "
                    "relation.relforcerowsecurity FROM pg_catalog.pg_class AS relation "
                    "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                    "WHERE namespace.nspname = :schema_name AND relation.relname = :table_name"
                ),
                {"schema_name": schema_name, "table_name": table_name},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        row is None
        or row["relkind"] != "r"
        or row["relpersistence"] != "p"
        or row["relrowsecurity"]
        or row["relforcerowsecurity"]
    ):
        raise RuntimeError("entity-address observed serving relation is unavailable")
    return _positive_oid(row["relation_oid"], field_name="relation OID")


async def _alias_state(session, schema_name: str) -> tuple[int, int, int]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT singleton, schema_version, active_ruleset_version, generation "
                    f"FROM {_quoted(schema_name)}.{_quoted(address_alias_sql.ADDRESS_ALIAS_STATE_TABLE)} "
                    "ORDER BY singleton"
                )
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != 1 or rows[0]["singleton"] is not True:
        raise RuntimeError("entity-address observed serving alias state is invalid")
    state = rows[0]
    if (
        type(state["schema_version"]) is not int
        or state["schema_version"] != address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION
        or type(state["active_ruleset_version"]) is not int
        or state["active_ruleset_version"] != address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION
        or type(state["generation"]) is not int
        or state["generation"] < 0
    ):
        raise RuntimeError("entity-address observed serving alias state is unsupported")
    return state["schema_version"], state["active_ruleset_version"], state["generation"]


async def _geo_assurance_state(
    session,
    *,
    schema_name: str,
    live_table_oid: int,
) -> tuple[int, int, tuple[tuple[str, int, int], ...]]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT singleton, active_geo_assurance_version, active_table_oid::bigint, "
                    f"active_relation_signature, {geo_projection.projection_relation_signature_sql(schema_name)} "
                    f"AS current_relation_signature FROM {_quoted(schema_name)}."
                    f"{_quoted(geo_projection.GEO_ASSURANCE_STATE_TABLE)} ORDER BY singleton"
                )
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != 1 or rows[0]["singleton"] is not True:
        raise RuntimeError("entity-address observed serving geo assurance state is invalid")
    state = rows[0]
    try:
        active_signature = _signature_tuple(state["active_relation_signature"], schema_name=schema_name)
        current_signature = _signature_tuple(state["current_relation_signature"], schema_name=schema_name)
        active_table_oid = _positive_oid(state["active_table_oid"], field_name="geo active table OID")
    except ValueError as error:
        raise RuntimeError("entity-address observed serving geo assurance state is invalid") from error
    if (
        type(state["active_geo_assurance_version"]) is not int
        or state["active_geo_assurance_version"] != geo_projection.GEO_ASSURANCE_VERSION
        or active_table_oid != live_table_oid
        or active_signature != current_signature
    ):
        raise RuntimeError("entity-address observed serving geo assurance is not active")
    return state["active_geo_assurance_version"], active_table_oid, active_signature


async def observe_entity_address_serving(
    session,
    *,
    schema_name: str,
    apply_queue_bounds: bool,
) -> EntityAddressObservedServingCapture:
    """Lock in writer order and observe the exact active source-local identity."""

    schema = _schema_name(schema_name)
    # READ COMMITTED intentionally takes its first data snapshot only after a
    # contended advisory lock is granted.  REPEATABLE READ would freeze a stale
    # pre-wait view in the advisory-lock SELECT itself.
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    if apply_queue_bounds:
        await session.execute(text("SET LOCAL lock_timeout TO '1s'"))
        await session.execute(text("SET LOCAL statement_timeout TO '3s'"))
    await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
    for _model_name, table_name in _RELATIONS:
        await session.execute(text(f"LOCK TABLE {_quoted(schema)}.{_quoted(table_name)} IN SHARE MODE"))
    await session.execute(text(geo_projection.projection_dependency_lock_sql(schema)))
    relation_oids = tuple([await _relation_oid(session, schema, table_name) for _, table_name in _RELATIONS])
    alias_schema_version, alias_ruleset_version, alias_generation = await _alias_state(session, schema)
    geo_assurance_version, geo_active_table_oid, geo_active_relation_signature = await _geo_assurance_state(
        session,
        schema_name=schema,
        live_table_oid=relation_oids[0],
    )
    return EntityAddressObservedServingCapture(
        contract=CONTRACT,
        source_schema=schema,
        relation_oids=relation_oids,
        alias_schema_version=alias_schema_version,
        alias_ruleset_version=alias_ruleset_version,
        alias_generation=alias_generation,
        geo_assurance_version=geo_assurance_version,
        geo_active_table_oid=geo_active_table_oid,
        geo_active_relation_signature=geo_active_relation_signature,
    )


async def capture_entity_address_observed_serving(
    session,
    *,
    schema_name: str,
) -> EntityAddressObservedServingCapture:
    """Capture current serving identity under fixed queue-path query bounds."""

    return await observe_entity_address_serving(
        session,
        schema_name=schema_name,
        apply_queue_bounds=True,
    )


__all__ = [
    "CONTRACT",
    "EntityAddressObservedServingCapture",
    "capture_entity_address_observed_serving",
    "observe_entity_address_serving",
    "validate_entity_address_observed_serving_capture",
]
