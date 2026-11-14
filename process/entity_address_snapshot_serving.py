# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded logical serving generations for queued address archive exports."""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from api import ptg2_geo_projection as geo_projection
from process import entity_address_result_generation as result_generation
from process.ext import address_alias_sql

entity_address_unified = importlib.import_module("process.entity_address_unified")
LEGACY_CONTRACT = "entity_address_observed_serving.postgres.v1"
CONTRACT = "entity_address_observed_serving.postgres.v2"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RELATIONS = tuple((model.__name__, model.__tablename__) for model in result_generation.ENTITY_ADDRESS_RESULT_MODELS)


@dataclass(frozen=True)
class EntityAddressObservedServingCapture:
    """Named serving family and its portable generation; legacy ID slots are empty."""

    contract: str
    source_schema: str
    relation_oids: tuple[None, ...]
    alias_schema_version: int
    alias_ruleset_version: int
    alias_generation: int
    geo_assurance_version: int | None
    geo_active_table_oid: None
    geo_active_relation_signature: None
    result_generation: result_generation.EntityAddressServingGeneration | None = None
    geo_dependency_relations: tuple[tuple[str, str, str], ...] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Write the named snapshot contract without physical identity fields."""

        return {
            "contract": CONTRACT,
            "source_schema": self.source_schema,
            "relations": [
                {
                    "model_name": model_name,
                    "table_name": table_name,
                }
                for model_name, table_name in _RELATIONS
            ],
            "alias_state": {
                "schema_version": self.alias_schema_version,
                "active_ruleset_version": self.alias_ruleset_version,
                "generation": self.alias_generation,
            },
            "geo_assurance": {
                "version": self.geo_assurance_version,
                "dependencies": {
                    name: {"schema_name": schema, "table_name": table}
                    for name, schema, table in (
                        self.geo_dependency_relations or _dependency_relations(self.source_schema)
                    )
                },
            },
            "result_generation": (None if self.result_generation is None else self.result_generation.as_dict()),
        }


def _schema_name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("entity-address observed serving requires a schema name")
    normalized = entity_address_unified._validate_schema_name(value)
    if _IDENTIFIER.fullmatch(normalized) is None or len(normalized.encode("utf-8")) > 63:
        raise ValueError("entity-address observed serving requires a safe schema name")
    return normalized


def _quoted(value: str) -> str:
    return f'"{value}"'


def _validated_relation_names(relation_entries: object, *, legacy: bool) -> None:
    """Require the fixed logical model names; ignore legacy catalog numbers."""
    if not isinstance(relation_entries, (list, tuple)) or len(relation_entries) != len(_RELATIONS):
        raise ValueError("entity-address observed serving relations are invalid")
    for expected_relation, relation_entry in zip(_RELATIONS, relation_entries, strict=True):
        if (
            not isinstance(relation_entry, Mapping)
            or set(relation_entry)
            != ({"model_name", "table_name", "relation_oid"} if legacy else {"model_name", "table_name"})
            or (relation_entry["model_name"], relation_entry["table_name"]) != expected_relation
        ):
            raise ValueError("entity-address observed serving relations are invalid")


def _validated_alias_state(alias_state: object) -> tuple[int, int, int]:
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
    return alias_state["schema_version"], alias_state["active_ruleset_version"], alias_state["generation"]


def _dependency_relations(schema, bindings=None):
    """Normalize the fixed six logical roles, discarding legacy catalog fields."""
    dependency_names_by_role = {
        f"{source_schema or schema}.{table}": (source_schema or schema, table)
        for source_schema, table in geo_projection._PROJECTION_DEPENDENCIES
    }
    if bindings is None:
        return tuple((name, *dependency_names_by_role[name]) for name in sorted(dependency_names_by_role))
    if not isinstance(bindings, Mapping) or set(bindings) != set(dependency_names_by_role):
        raise ValueError("entity-address observed serving dependencies are invalid")
    relations = []
    for name, binding in sorted(bindings.items()):
        if not isinstance(binding, Mapping) or set(binding) not in (
            {"schema_name", "table_name"},
            {"schema_name", "table_name", "relation_oid", "relfilenode"},
        ):
            raise ValueError("entity-address observed serving dependencies are invalid")
        relations.append((name, _schema_name(binding["schema_name"]), _schema_name(binding["table_name"])))
    if len({relation[1:] for relation in relations}) != len(dependency_names_by_role):
        raise ValueError("entity-address observed serving dependencies are invalid")
    return tuple(relations)


def _validated_geo_assurance(geo_assurance: object, *, legacy=False, allow_unpublished_geo=False) -> int | None:
    fields = {"version", "active_table_oid", "active_relation_signature"} if legacy else {"version", "dependencies"}
    if not isinstance(geo_assurance, Mapping) or set(geo_assurance) != fields:
        raise ValueError("entity-address observed serving capture is invalid")
    version = geo_assurance["version"]
    if allow_unpublished_geo and version is None:
        return None
    if type(version) is not int or version != geo_projection.GEO_ASSURANCE_VERSION:
        raise ValueError("entity-address observed serving capture is invalid")
    return version


def validate_entity_address_observed_serving_capture(
    capture_value: Mapping[str, Any] | EntityAddressObservedServingCapture,
    *,
    schema_name: str | None = None,
    allow_unpublished_geo: bool = False,
) -> EntityAddressObservedServingCapture:
    """Validate logical queue fields and normalize obsolete physical metadata."""

    capture_mapping = (
        capture_value.as_dict() if isinstance(capture_value, EntityAddressObservedServingCapture) else capture_value
    )
    if not isinstance(capture_mapping, Mapping):
        raise ValueError("entity-address observed serving capture is invalid")
    schema = _schema_name(schema_name if schema_name is not None else capture_mapping.get("source_schema"))
    legacy_fields = {
        "contract",
        "source_schema",
        "relations",
        "alias_state",
        "geo_assurance",
    }
    if set(capture_mapping) not in (legacy_fields, legacy_fields | {"result_generation"}):
        raise ValueError("entity-address observed serving capture is invalid")
    if capture_mapping["contract"] not in {LEGACY_CONTRACT, CONTRACT} or capture_mapping["source_schema"] != schema:
        raise ValueError("entity-address observed serving capture is invalid")
    is_legacy = capture_mapping["contract"] == LEGACY_CONTRACT
    _validated_relation_names(capture_mapping["relations"], legacy=is_legacy)
    alias_schema_version, alias_ruleset_version, alias_generation = _validated_alias_state(
        capture_mapping["alias_state"]
    )
    geo_assurance_version = _validated_geo_assurance(
        capture_mapping["geo_assurance"],
        legacy=is_legacy,
        allow_unpublished_geo=allow_unpublished_geo,
    )
    dependencies = _dependency_relations(
        schema, None if is_legacy else capture_mapping["geo_assurance"]["dependencies"]
    )
    generation_value = capture_mapping.get("result_generation")
    if geo_assurance_version is None and ("result_generation" not in capture_mapping or generation_value is not None):
        raise ValueError("entity-address unpublished receive generation is invalid")
    serving_generation = (
        None
        if generation_value is None
        else result_generation.validate_entity_address_serving_generation(generation_value)
    )
    return EntityAddressObservedServingCapture(
        contract=CONTRACT,
        source_schema=schema,
        relation_oids=(None,) * len(_RELATIONS),
        alias_schema_version=alias_schema_version,
        alias_ruleset_version=alias_ruleset_version,
        alias_generation=alias_generation,
        geo_assurance_version=geo_assurance_version,
        geo_active_table_oid=None,
        geo_active_relation_signature=None,
        result_generation=serving_generation,
        geo_dependency_relations=dependencies,
    )


def _validated_observed_alias_state(alias_state: Mapping[str, Any]) -> tuple[int, int, int]:
    if (
        type(alias_state["schema_version"]) is not int
        or alias_state["schema_version"] != address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION
        or type(alias_state["active_ruleset_version"]) is not int
        or alias_state["active_ruleset_version"] != address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION
        or type(alias_state["generation"]) is not int
        or alias_state["generation"] < 0
    ):
        raise RuntimeError("entity-address observed serving alias state is unsupported")
    return alias_state["schema_version"], alias_state["active_ruleset_version"], alias_state["generation"]


async def _relation_name(session, schema_name: str, table_name: str) -> str:
    row = (
        (
            await session.execute(
                text(
                    "SELECT relation.relkind::text AS relkind, "
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
    return f"{schema_name}.{table_name}"


async def _alias_state(session, schema_name: str) -> tuple[int, int, int]:
    alias_state_rows = (
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
    if len(alias_state_rows) != 1 or alias_state_rows[0]["singleton"] is not True:
        raise RuntimeError("entity-address observed serving alias state is invalid")
    return _validated_observed_alias_state(alias_state_rows[0])


async def _geo_assurance_state(
    session, *, schema_name: str, allow_unpublished_geo=False, dependencies=None
) -> int | None:
    """Require fresh source evidence while allowing a stale incumbent to be replaced."""
    bindings = (
        None
        if dependencies is None
        else {name: {"schema_name": schema, "table_name": table} for name, schema, table in dependencies}
    )
    generation_matches = (
        "TRUE"
        if allow_unpublished_geo
        else geo_projection.projection_npi_generation_match_sql(schema_name, "active", dependency_bindings=bindings)
    )
    geo_state_rows = (
        (
            await session.execute(
                text(
                    f"SELECT singleton, active_geo_assurance_version, {generation_matches} AS npi_generation_matches "
                    f"FROM {_quoted(schema_name)}."
                    f"{_quoted(geo_projection.GEO_ASSURANCE_STATE_TABLE)} ORDER BY singleton"
                )
            )
        )
        .mappings()
        .all()
    )
    if len(geo_state_rows) != 1 or geo_state_rows[0]["singleton"] is not True:
        raise RuntimeError("entity-address observed serving geo assurance state is invalid")
    version = geo_state_rows[0]["active_geo_assurance_version"]
    if allow_unpublished_geo and version is None:
        return None
    if (
        type(version) is not int
        or version != geo_projection.GEO_ASSURANCE_VERSION
        or geo_state_rows[0]["npi_generation_matches"] is not True
    ):
        raise RuntimeError("entity-address observed serving geo assurance is not active")
    return version


async def _result_generation_state(
    session: Any,
    *,
    schema_name: str,
    relation_names: tuple[str, ...],
) -> result_generation.EntityAddressServingGeneration | None:
    expected_names = tuple(f"{schema_name}.{name}" for name in result_generation.RELATION_NAMES)
    if relation_names != expected_names:
        raise RuntimeError("entity-address result generation serving relations are incomplete")
    authority = await result_generation.read_entity_address_result_generation_authority(
        session,
        schema_name=schema_name,
    )
    return authority.serving_generation


async def observe_entity_address_serving(
    session,
    *,
    schema_name: str,
    apply_queue_bounds: bool,
) -> EntityAddressObservedServingCapture:
    """Lock named snapshots in writer order and observe their logical generation."""

    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ValueError("entity-address observed serving requires a caller transaction")
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
    dependencies = await _lock_geo_dependencies(session, schema)
    return await _read_observed_serving(session, schema, dependencies=dependencies)


async def _lock_geo_dependencies(session, schema):
    bindings = await session.scalar(
        text(
            f"SELECT active_dependency_bindings FROM {_quoted(schema)}."
            f"{_quoted(geo_projection.GEO_ASSURANCE_STATE_TABLE)} WHERE singleton IS TRUE"
        )
    )
    dependencies = _dependency_relations(schema, bindings)
    names = ", ".join(
        ("" if f"{target_schema}.{table}" == role else "ONLY ") + f"{_quoted(target_schema)}.{_quoted(table)}"
        for role, target_schema, table in dependencies
    )
    await session.execute(text(f"LOCK TABLE {names} IN ACCESS SHARE MODE"))
    return dependencies


async def _read_observed_serving(session, schema, *, allow_unpublished_geo=False, dependencies=None):
    """Read portable generation and semantic state after locking named snapshots."""
    relation_names = tuple([await _relation_name(session, schema, table_name) for _, table_name in _RELATIONS])
    alias_schema_version, alias_ruleset_version, alias_generation = await _alias_state(session, schema)
    geo_assurance_version = await _geo_assurance_state(
        session, schema_name=schema, allow_unpublished_geo=allow_unpublished_geo, dependencies=dependencies
    )
    serving_generation = await _result_generation_state(
        session,
        schema_name=schema,
        relation_names=relation_names,
    )
    if geo_assurance_version is None:
        if not allow_unpublished_geo or serving_generation is not None:
            raise RuntimeError("entity-address unpublished receive is not serving authority")
        await _require_empty_receive_bootstrap(session, schema)
    return EntityAddressObservedServingCapture(
        contract=CONTRACT,
        source_schema=schema,
        relation_oids=(None,) * len(_RELATIONS),
        alias_schema_version=alias_schema_version,
        alias_ruleset_version=alias_ruleset_version,
        alias_generation=alias_generation,
        geo_assurance_version=geo_assurance_version,
        geo_active_table_oid=None,
        geo_active_relation_signature=None,
        result_generation=serving_generation,
        geo_dependency_relations=dependencies or _dependency_relations(schema),
    )


async def _require_empty_receive_bootstrap(session, schema):
    """A receive may observe truly unpublished empty heaps, never cleared legacy data."""
    authority = await result_generation.read_entity_address_result_generation_authority(session, schema_name=schema)
    if authority.local_generation != 0 or authority.serving_generation is not None:
        raise RuntimeError("entity-address unpublished receive generation is invalid")
    populated = " OR ".join(
        f"EXISTS (SELECT 1 FROM {_quoted(schema)}.{_quoted(name)} LIMIT 1)" for _model, name in _RELATIONS
    )
    if await session.scalar(text("SELECT " + populated)) is not False:
        raise RuntimeError("entity-address unpublished receive requires empty models")


async def require_entity_address_receive_isolation(session):
    """Reject stale-snapshot callers without changing an already-used transaction."""
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ValueError("entity-address receive observation requires a caller transaction")
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise RuntimeError("entity-address receive observation requires READ COMMITTED isolation")


async def _observe_receive_destination(session, *, schema_name, lock_mode):
    """Observe destination state without treating it as transferable source authority."""
    schema = _schema_name(schema_name)
    if lock_mode not in {"ACCESS SHARE", "SHARE"}:
        raise ValueError("entity-address receive lock mode is invalid")
    await require_entity_address_receive_isolation(session)
    await session.execute(text("SET LOCAL lock_timeout TO '1s'"))
    await session.execute(text("SET LOCAL statement_timeout TO '3s'"))
    await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
    for _model_name, table_name in _RELATIONS:
        await session.execute(text(f"LOCK TABLE {_quoted(schema)}.{_quoted(table_name)} IN {lock_mode} MODE"))
    dependencies = await _lock_geo_dependencies(session, schema)
    return await _read_observed_serving(session, schema, allow_unpublished_geo=True, dependencies=dependencies)


async def capture_entity_address_receive_admission(session, *, schema_name: str):
    """Capture an optimistic receive token with ordinary SELECT authority only."""
    return await _observe_receive_destination(session, schema_name=schema_name, lock_mode="ACCESS SHARE")


async def require_entity_address_receive_incumbent(session, *, schema_name: str, expected):
    """Recheck the entire destination token under publisher writer-order locks."""
    expected_capture = validate_entity_address_observed_serving_capture(expected, allow_unpublished_geo=True)
    current = await _observe_receive_destination(session, schema_name=schema_name, lock_mode="SHARE")
    if current != expected_capture:
        raise RuntimeError("entity-address receive incumbent changed")


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
    "capture_entity_address_receive_admission",
    "require_entity_address_receive_incumbent",
    "require_entity_address_receive_isolation",
    "observe_entity_address_serving",
    "validate_entity_address_observed_serving_capture",
]
