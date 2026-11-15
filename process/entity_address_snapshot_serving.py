# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable named snapshots and logical generations for queued address archive exports."""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass, replace
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import text

from api import ptg2_geo_projection as geo_projection
from process import entity_address_result_generation as result_generation
from process.entity_address_dependency_bindings import lock_active_dependencies, read_active_bindings
from process.ext import address_alias_sql

entity_address_unified = importlib.import_module("process.entity_address_unified")
CONTRACT = "entity_address_observed_serving.postgres.v1"
NAMED_CONTRACT = "entity_address_observed_serving.postgres.v2"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_POSTGRES_OID_MAX = (1 << 32) - 1
_RELATIONS = tuple((model.__name__, model.__tablename__) for model in result_generation.ENTITY_ADDRESS_RESULT_MODELS)
_LOCAL_GEO_DEPENDENCIES = (
    "npi_address",
    "mrf_address",
    "doctor_clinician_address",
    "geo_zip_lookup",
)
_SHARED_GEO_DEPENDENCIES = ("tiger.zip_state", "tiger.zcta5")


@dataclass(frozen=True)
class EntityAddressObservedServingCapture:
    """Logical state of a published named address family, with decode-only legacy records."""

    contract: str
    source_schema: str
    relation_oids: tuple[Any, ...]
    alias_schema_version: int
    alias_ruleset_version: int
    alias_generation: int
    geo_assurance_version: int | None
    geo_active_table_oid: int | None
    geo_active_relation_signature: geo_projection.ProjectionRelationSignature | None
    result_generation: result_generation.EntityAddressServingGeneration | None = None

    def as_dict(self) -> dict[str, Any]:
        """Emit the current named form or preserve the saved legacy edition."""

        if self.contract == NAMED_CONTRACT:
            return _named_capture_dict(self)
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
                "active_relation_signature": (
                    None
                    if self.geo_active_relation_signature is None
                    else {
                        relation_name: [first_component, second_component]
                        for relation_name, first_component, second_component in self.geo_active_relation_signature
                    }
                ),
            },
            "result_generation": (None if self.result_generation is None else self.result_generation.as_dict()),
        }


def _named_capture_dict(capture):
    """Emit only declared names and portable logical generation fields."""
    return {
        "contract": NAMED_CONTRACT,
        "source_schema": capture.source_schema,
        "relations": [
            {"model_name": model, "schema_name": capture.source_schema, "table_name": name}
            for model, name in _RELATIONS
        ],
        "alias_state": {
            "schema_version": capture.alias_schema_version,
            "active_ruleset_version": capture.alias_ruleset_version,
            "generation": capture.alias_generation,
        },
        "geo_assurance": {
            "version": capture.geo_assurance_version,
            "active_relation_signature": None
            if capture.geo_active_relation_signature is None
            else {name: [namespace, table] for name, namespace, table in capture.geo_active_relation_signature},
        },
        "result_generation": None if capture.result_generation is None else capture.result_generation.as_dict(),
    }


def _named_signature(schema, value, bindings=None):
    """Normalize the shared logical declaration without resolving old storage tokens."""
    return geo_projection.projection_named_relation_signature(schema, value, bindings)


def logical_entity_address_serving_capture(value, *, schema_name=None, allow_unpublished_geo=False):
    """Compare logical state without rewriting the original saved capture edition."""
    capture = validate_entity_address_observed_serving_capture(
        value, schema_name=schema_name, allow_unpublished_geo=allow_unpublished_geo
    )
    signature = capture.geo_active_relation_signature
    named_signature = (
        None
        if signature is None
        else _named_signature(capture.source_schema, {name: [first, second] for name, first, second in signature})
    )
    return replace(
        capture,
        contract=NAMED_CONTRACT,
        relation_oids=(),
        geo_active_table_oid=None,
        geo_active_relation_signature=named_signature,
    ).as_dict()


def _validate_named_capture(capture, schema, *, allow_unpublished_geo):
    """Validate the closed named edition while keeping unpublished state explicit."""
    fields = {"contract", "source_schema", "relations", "alias_state", "geo_assurance", "result_generation"}
    if set(capture) != fields or capture["source_schema"] != schema:
        raise ValueError("entity-address observed serving capture is invalid")
    expected_relations = [
        {"model_name": model, "schema_name": schema, "table_name": name} for model, name in _RELATIONS
    ]
    if capture["relations"] != expected_relations:
        raise ValueError("entity-address observed serving relation declaration is invalid")
    alias = _validated_alias_state(capture["alias_state"])
    geo = capture["geo_assurance"]
    if not isinstance(geo, Mapping) or set(geo) != {"version", "active_relation_signature"}:
        raise ValueError("entity-address observed serving capture is invalid")
    is_unpublished = geo["version"] is None and geo["active_relation_signature"] is None
    if is_unpublished:
        if not allow_unpublished_geo or capture["result_generation"] is not None:
            raise ValueError("entity-address unpublished receive generation is invalid")
        signature = None
    else:
        if type(geo["version"]) is not int or geo["version"] != geo_projection.GEO_ASSURANCE_VERSION:
            raise ValueError("entity-address observed serving capture is invalid")
        signature = _signature_tuple(geo["active_relation_signature"], schema_name=schema)
        if not all(
            type(namespace) is str and type(table) is str
            for name, namespace, table in signature
            if name != f"{schema}.npi_result_generation"
        ):
            raise ValueError("entity-address observed serving named signature is invalid")
    generation_value = capture["result_generation"]
    return EntityAddressObservedServingCapture(
        contract=NAMED_CONTRACT,
        source_schema=schema,
        relation_oids=(),
        alias_schema_version=alias[0],
        alias_ruleset_version=alias[1],
        alias_generation=alias[2],
        geo_assurance_version=geo["version"],
        geo_active_table_oid=None,
        geo_active_relation_signature=signature,
        result_generation=None
        if generation_value is None
        else result_generation.validate_entity_address_serving_generation(generation_value),
    )


def _schema_name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("entity-address observed serving requires a schema name")
    normalized = entity_address_unified._validate_schema_name(value)
    if _IDENTIFIER.fullmatch(normalized) is None or len(normalized.encode("utf-8")) > 63:
        raise ValueError("entity-address observed serving requires a safe schema name")
    return normalized


def _quoted(value: str) -> str:
    return f'"{value}"'


def _positive_oid(value: object, *, field_name: str) -> int:
    if type(value) is not int or not 0 < value <= _POSTGRES_OID_MAX:
        raise ValueError(f"entity-address observed serving {field_name} is invalid")
    return value


def _signature_tuple(value: object, *, schema_name: str) -> geo_projection.ProjectionRelationSignature:
    """Decode either saved signature edition without interpreting numeric compatibility tokens."""
    try:
        return geo_projection.projection_relation_signature_tuple(schema_name, value)
    except ValueError as error:
        raise ValueError("entity-address observed serving geo signature is invalid") from error


def _validated_relation_oids(relation_entries: object) -> tuple[int, ...]:
    if not isinstance(relation_entries, (list, tuple)) or len(relation_entries) != len(_RELATIONS):
        raise ValueError("entity-address observed serving relation identity is invalid")
    relation_oids = []
    for expected_relation, relation_entry in zip(_RELATIONS, relation_entries, strict=True):
        if (
            not isinstance(relation_entry, Mapping)
            or set(relation_entry) != {"model_name", "table_name", "relation_oid"}
            or (relation_entry["model_name"], relation_entry["table_name"]) != expected_relation
        ):
            raise ValueError("entity-address observed serving relation identity is invalid")
        relation_oids.append(relation_entry["relation_oid"])
    return tuple(relation_oids)


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


def _validated_geo_assurance(
    geo_assurance: object,
    *,
    schema_name: str,
    live_table_oid: int,
    allow_unpublished_geo: bool = False,
) -> tuple[int | None, int | None, geo_projection.ProjectionRelationSignature | None]:
    fields = {"version", "active_table_oid", "active_relation_signature"}
    if allow_unpublished_geo and isinstance(geo_assurance, Mapping) and set(geo_assurance) == fields:
        if all(geo_assurance[field] is None for field in fields):
            return None, None, None
    if (
        not isinstance(geo_assurance, Mapping)
        or set(geo_assurance) != {"version", "active_table_oid", "active_relation_signature"}
        or type(geo_assurance["version"]) is not int
        or geo_assurance["version"] != geo_projection.GEO_ASSURANCE_VERSION
    ):
        raise ValueError("entity-address observed serving capture is invalid")
    geo_active_table_oid = geo_assurance["active_table_oid"]
    signature = geo_projection.projection_relation_signature_tuple(
        schema_name, geo_assurance["active_relation_signature"], allow_opaque_legacy=True
    )
    return geo_assurance["version"], geo_active_table_oid, signature


def validate_entity_address_observed_serving_capture(
    capture_value: Mapping[str, Any] | EntityAddressObservedServingCapture,
    *,
    schema_name: str | None = None,
    allow_unpublished_geo: bool = False,
) -> EntityAddressObservedServingCapture:
    """Validate a closed serving edition without interpreting old numeric tokens."""

    capture_mapping = (
        capture_value.as_dict() if isinstance(capture_value, EntityAddressObservedServingCapture) else capture_value
    )
    if not isinstance(capture_mapping, Mapping):
        raise ValueError("entity-address observed serving capture is invalid")
    schema = _schema_name(schema_name if schema_name is not None else capture_mapping.get("source_schema"))
    if capture_mapping.get("contract") == NAMED_CONTRACT:
        return _validate_named_capture(capture_mapping, schema, allow_unpublished_geo=allow_unpublished_geo)
    legacy_fields = {
        "contract",
        "source_schema",
        "relations",
        "alias_state",
        "geo_assurance",
    }
    if set(capture_mapping) not in (legacy_fields, legacy_fields | {"result_generation"}):
        raise ValueError("entity-address observed serving capture is invalid")
    if capture_mapping["contract"] != CONTRACT or capture_mapping["source_schema"] != schema:
        raise ValueError("entity-address observed serving capture is invalid")
    relation_oids = _validated_relation_oids(capture_mapping["relations"])
    alias_schema_version, alias_ruleset_version, alias_generation = _validated_alias_state(
        capture_mapping["alias_state"]
    )
    geo_assurance_version, geo_active_table_oid, geo_signature = _validated_geo_assurance(
        capture_mapping["geo_assurance"],
        schema_name=schema,
        live_table_oid=relation_oids[0],
        allow_unpublished_geo=allow_unpublished_geo,
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
        relation_oids=relation_oids,
        alias_schema_version=alias_schema_version,
        alias_ruleset_version=alias_ruleset_version,
        alias_generation=alias_generation,
        geo_assurance_version=geo_assurance_version,
        geo_active_table_oid=geo_active_table_oid,
        geo_active_relation_signature=geo_signature,
        result_generation=serving_generation,
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
    session,
    *,
    schema_name: str,
    live_table_oid: int,
    require_current_dependencies: bool = True,
    allow_unpublished_geo: bool = False,
) -> tuple[int | None, int | None, geo_projection.ProjectionRelationSignature | None]:
    """Observe legacy receiving state, but require the captured origin for source export."""
    freshness = "TRUE"
    if require_current_dependencies and not allow_unpublished_geo:
        selected = await read_active_bindings(session, schema_name)
        freshness = geo_projection.projection_npi_generation_match_sql(
            schema_name, "active", dependency_bindings=selected
        )
    signature, bindings_match = geo_projection.projection_stored_bindings_sql(schema_name, "active_dependency_bindings")
    geo_state_result = await session.execute(
        text(
            f"SELECT singleton, active_geo_assurance_version, active_relation_signature, active_dependency_bindings, "
            f"{signature} AS current_relation_signature, {bindings_match} AS dependency_bindings_match, "
            f"{freshness} AS npi_generation_match FROM {_quoted(schema_name)}."
            f"{_quoted(geo_projection.GEO_ASSURANCE_STATE_TABLE)} ORDER BY singleton"
        )
    )
    geo_state_rows = geo_state_result.mappings().all()
    if len(geo_state_rows) != 1 or geo_state_rows[0]["singleton"] is not True:
        raise RuntimeError("entity-address observed serving geo assurance state is invalid")
    geo_state = geo_state_rows[0]
    if allow_unpublished_geo and all(
        geo_state[field] is None
        for field in ("active_geo_assurance_version", "active_relation_signature", "active_dependency_bindings")
    ):
        return None, None, None
    try:
        active_signature = _named_signature(
            schema_name, geo_state["active_relation_signature"], geo_state.get("active_dependency_bindings")
        )
        current_signature = (
            _named_signature(schema_name, geo_state["current_relation_signature"])
            if require_current_dependencies
            else active_signature
        )
    except ValueError as error:
        raise RuntimeError("entity-address observed serving geo assurance state is invalid") from error
    if (
        type(geo_state["active_geo_assurance_version"]) is not int
        or geo_state["active_geo_assurance_version"] != geo_projection.GEO_ASSURANCE_VERSION
        or (
            require_current_dependencies
            and tuple(entry for entry in active_signature if entry[0] != f"{schema_name}.npi_result_generation")
            != current_signature
        )
        or geo_state.get("npi_generation_match") is not True
        or (require_current_dependencies and geo_state.get("dependency_bindings_match") is not True)
    ):
        raise RuntimeError("entity-address observed serving geo assurance is not active")
    return geo_state["active_geo_assurance_version"], None, active_signature


async def _logical_result_state(session, schema_name):
    """Read only the migration's logical counter and portable origin, never old storage tokens."""
    generation_row = (
        (
            await session.execute(
                text(
                    "SELECT local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at "
                    f"FROM {_quoted(schema_name)}.{_quoted(result_generation.TABLE_NAME)} WHERE singleton IS TRUE"
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        generation_row is None
        or type(generation_row["local_generation"]) is not int
        or not 0 <= generation_row["local_generation"] < 2**63
    ):
        raise RuntimeError("entity-address logical result generation is unavailable")
    UUID(str(generation_row["local_lineage_id"]))
    origin_by_field = {
        name: generation_row[name] for name in ("origin_lineage_id", "origin_generation", "published_at")
    }
    if all(origin_value is None for origin_value in origin_by_field.values()):
        return generation_row["local_generation"], None
    if any(origin_value is None for origin_value in origin_by_field.values()):
        raise RuntimeError("entity-address logical result generation is incomplete")
    return generation_row["local_generation"], result_generation.validate_entity_address_serving_generation(
        origin_by_field
    )


async def _result_generation_state(
    session: Any, *, schema_name: str, relation_oids=()
) -> result_generation.EntityAddressServingGeneration | None:
    """Return the portable origin; the old optional argument is ignored compatibility data."""
    _local_generation, origin = await _logical_result_state(session, schema_name)
    return origin


async def observe_entity_address_serving(
    session,
    *,
    schema_name: str,
    apply_queue_bounds: bool,
) -> EntityAddressObservedServingCapture:
    """Lock in writer order and observe the exact active source-local identity."""

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
        await session.execute(text(f"LOCK TABLE {_quoted(schema)}.{_quoted(table_name)} IN ACCESS SHARE MODE"))
    await lock_active_dependencies(session, schema)
    return await _read_observed_serving(session, schema, require_current_dependencies=True)


async def _require_declared_family(session, schema):
    """Validate the complete named heap family after native SELECT locks are acquired."""
    declared_names = [name for _, name in _RELATIONS]
    relation_records = (
        (
            await session.execute(
                text(
                    "SELECT requested.name, relation.relkind='r' AND relation.relpersistence='p' "
                    "AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity AS accepted "
                    "FROM unnest(CAST(:names AS text[])) WITH ORDINALITY requested(name,position) "
                    "LEFT JOIN pg_namespace namespace ON namespace.nspname=:schema "
                    "LEFT JOIN pg_class relation ON relation.relnamespace=namespace.oid AND relation.relname=requested.name "
                    "ORDER BY requested.position"
                ),
                {"names": declared_names, "schema": schema},
            )
        )
        .mappings()
        .all()
    )
    if [record["name"] for record in relation_records] != declared_names or any(
        record["accepted"] is not True for record in relation_records
    ):
        raise RuntimeError("entity-address observed serving relation is unavailable")


async def _read_observed_serving(session, schema, *, require_current_dependencies, allow_unpublished_geo=False):
    """Read the named family, alias counters, geo selection and portable origin."""
    await _require_declared_family(session, schema)
    relation_oids = ()
    alias_schema_version, alias_ruleset_version, alias_generation = await _alias_state(session, schema)
    geo_assurance_version, geo_active_table_oid, geo_active_relation_signature = await _geo_assurance_state(
        session,
        schema_name=schema,
        live_table_oid=None,
        **({} if require_current_dependencies else {"require_current_dependencies": False}),
        **({"allow_unpublished_geo": True} if allow_unpublished_geo else {}),
    )
    if geo_active_relation_signature is not None:
        geo_active_relation_signature = _named_signature(
            schema, {name: [first, second] for name, first, second in geo_active_relation_signature}
        )
    serving_generation = await _result_generation_state(
        session,
        schema_name=schema,
        relation_oids=relation_oids,
    )
    if geo_assurance_version is None:
        if not allow_unpublished_geo or serving_generation is not None:
            raise RuntimeError("entity-address unpublished receive is not serving authority")
        await _require_empty_receive_bootstrap(session, schema)
    return EntityAddressObservedServingCapture(
        contract=NAMED_CONTRACT,
        source_schema=schema,
        relation_oids=relation_oids,
        alias_schema_version=alias_schema_version,
        alias_ruleset_version=alias_ruleset_version,
        alias_generation=alias_generation,
        geo_assurance_version=geo_assurance_version,
        geo_active_table_oid=None,
        geo_active_relation_signature=geo_active_relation_signature,
        result_generation=serving_generation,
    )


async def _require_empty_receive_bootstrap(session, schema):
    """A receive may observe truly unpublished empty heaps, never cleared legacy data."""
    local_generation, origin = await _logical_result_state(session, schema)
    if local_generation != 0 or origin is not None:
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
    await lock_active_dependencies(session, schema, receiving=True)
    # A grouped replacement may already have swapped its dependencies. Its
    # separate dependency fence selects the new package identities; the exact
    # stored incumbent signature must still match the queued destination token.
    return await _read_observed_serving(session, schema, require_current_dependencies=False, allow_unpublished_geo=True)


async def capture_entity_address_receive_admission(session, *, schema_name: str):
    """Capture an optimistic receive token with ordinary SELECT authority only."""
    return await _observe_receive_destination(session, schema_name=schema_name, lock_mode="ACCESS SHARE")


async def require_entity_address_receive_incumbent(session, *, schema_name: str, expected):
    """Recheck the entire destination token under publisher writer-order locks."""
    expected_capture = validate_entity_address_observed_serving_capture(expected, allow_unpublished_geo=True)
    current = await _observe_receive_destination(session, schema_name=schema_name, lock_mode="ACCESS SHARE")
    if logical_entity_address_serving_capture(
        current, allow_unpublished_geo=True
    ) != logical_entity_address_serving_capture(expected_capture, allow_unpublished_geo=True):
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
    "NAMED_CONTRACT",
    "logical_entity_address_serving_capture",
    "EntityAddressObservedServingCapture",
    "capture_entity_address_observed_serving",
    "capture_entity_address_receive_admission",
    "require_entity_address_receive_incumbent",
    "require_entity_address_receive_isolation",
    "observe_entity_address_serving",
    "validate_entity_address_observed_serving_capture",
]
