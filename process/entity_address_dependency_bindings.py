# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Local held dependency locks, projection state, and closed physical relocation."""

import json
from collections.abc import Mapping
from contextlib import asynccontextmanager

from sqlalchemy import text

from api import ptg2_geo_projection as projection
from db.models import EntityAddressUnified

PUBLISHER_SELECTED_INPUTS_CONTRACT = "entity-address-publisher-selected-inputs.v1"


def validate_publisher_selected_inputs(schema_name, value):
    """Decode trusted local selection metadata, never confer custody from JSON."""
    if (
        not isinstance(value, Mapping)
        or set(value) != {"contract", "dependency_bindings"}
        or value["contract"] != PUBLISHER_SELECTED_INPUTS_CONTRACT
    ):
        raise ValueError("entity-address publisher selected inputs are invalid")
    return {
        "contract": PUBLISHER_SELECTED_INPUTS_CONTRACT,
        "dependency_bindings": projection.validate_projection_dependency_bindings(
            schema_name, value["dependency_bindings"]
        ),
    }


async def lock_publisher_selected_inputs(session, schema_name, value):
    """Independently fence the exact locally authenticated six selected heaps."""
    if not session.in_transaction():
        raise RuntimeError("entity-address publisher selected inputs require caller transaction")
    selected = validate_publisher_selected_inputs(schema_name, value)
    bindings = selected["dependency_bindings"]
    statement = projection.projection_dependency_lock_sql(schema_name, dependency_bindings=bindings)
    await session.execute(text(statement.rstrip().removesuffix(";") + " NOWAIT"))
    if not await session.scalar(
        text("SELECT " + projection.projection_dependency_bindings_match_sql(schema_name, bindings))
    ):
        raise RuntimeError("entity-address publisher selected inputs changed")
    return selected


def validate_schema_name(schema: str) -> str:
    """Trim and validate the native unquoted schema or table identifier form."""
    cleaned = (schema or "").strip()
    if not cleaned or not (cleaned[0].isalpha() or cleaned[0] == "_"):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not all(ch.isalnum() or ch == "_" for ch in cleaned):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return cleaned


def record_geo_assurance_candidate_sql(
    db_schema: str,
    stage_table: str,
    projected_rows: int,
    *,
    dependency_bindings=None,
    require_canonical_publication=False,
) -> str:
    """Bind the candidate to read heaps; prepared native inputs still require canonical apply."""
    db_schema = validate_schema_name(db_schema)
    stage_table = validate_schema_name(stage_table)
    state_table = projection.GEO_ASSURANCE_STATE_TABLE
    stage_relation = f"{db_schema}.{stage_table}"
    signature_sql = projection.projection_relation_signature_sql(
        db_schema, **({} if dependency_bindings is None else {"dependency_bindings": dependency_bindings})
    )
    bindings_sql = (
        "NULL"
        if dependency_bindings is None or require_canonical_publication
        else "'"
        + json.dumps(
            projection.validate_projection_dependency_bindings(db_schema, dependency_bindings), sort_keys=True
        ).replace("'", "''")
        + "'::jsonb"
    )
    return f"""
    INSERT INTO {db_schema}.{state_table} (
        singleton,
        candidate_geo_assurance_version,
        candidate_table_oid,
        candidate_relation_signature,
        candidate_dependency_bindings,
        candidate_projected_rows
    )
    SELECT
        true,
        {projection.GEO_ASSURANCE_VERSION},
        to_regclass('{stage_relation}')::oid,
        {signature_sql},
        {bindings_sql},
        {int(projected_rows)}::bigint
     WHERE to_regclass('{stage_relation}') IS NOT NULL
    ON CONFLICT (singleton) DO UPDATE SET
        candidate_geo_assurance_version = EXCLUDED.candidate_geo_assurance_version,
        candidate_table_oid = EXCLUDED.candidate_table_oid,
        candidate_relation_signature = EXCLUDED.candidate_relation_signature,
        candidate_dependency_bindings = EXCLUDED.candidate_dependency_bindings,
        candidate_projected_rows = EXCLUDED.candidate_projected_rows
    RETURNING candidate_table_oid::bigint;
    """


def activate_geo_assurance_candidate_sql(db_schema: str) -> str:
    """Activate only the recorded candidate heap with its still-valid dependency map."""
    db_schema = validate_schema_name(db_schema)
    state_table = projection.GEO_ASSURANCE_STATE_TABLE
    live_relation = f"{db_schema}.{EntityAddressUnified.__main_table__}"
    signature, bindings_match = projection.projection_stored_bindings_sql(db_schema, "candidate_dependency_bindings")
    return f"""
    UPDATE {db_schema}.{state_table}
       SET active_geo_assurance_version = candidate_geo_assurance_version,
           active_table_oid = candidate_table_oid,
           active_relation_signature = candidate_relation_signature,
           active_dependency_bindings = candidate_dependency_bindings,
           candidate_geo_assurance_version = NULL,
           candidate_table_oid = NULL,
           candidate_relation_signature = NULL,
           candidate_dependency_bindings = NULL,
           candidate_projected_rows = NULL
     WHERE singleton IS TRUE
       AND candidate_geo_assurance_version = {projection.GEO_ASSURANCE_VERSION}
       AND candidate_table_oid = to_regclass('{live_relation}')::oid
       AND {bindings_match}
       AND candidate_relation_signature = ({signature})
    RETURNING active_table_oid::bigint;
    """


@asynccontextmanager
async def selected_publication_dependencies(database, schema_name, *, control_context=None):
    """Hold complete protected input families through the ordinary native finalizer."""
    from process.tiger_held_inputs import selected_captured_inventories, selected_tiger_inventory

    schema = projection._sql_identifier(schema_name, field_name="address schema")
    async with database.session_factory() as session, session.begin():
        node_options_dict = {}
        if control_context is not None and control_context.get("control_run_id"):
            from process.entity_address_native_publication import controlled_dependency_node

            node_options_dict["node_id"] = await controlled_dependency_node(session, schema, control_context)
        captured = await selected_captured_inventories(session, **node_options_dict)
        inventory = captured.get("tiger") or await selected_tiger_inventory(session, **node_options_dict)
        if inventory is None:
            if await session.scalar(
                text("""SELECT EXISTS(SELECT 1 FROM pg_inherits
                WHERE inhparent IN (to_regclass('tiger.zip_state'),to_regclass('tiger.zcta5')))""")
            ):
                raise RuntimeError("entity-address inherited TIGER publication requires protected captured inputs")
            if not captured:
                yield None
                return
        by_name = {
            relation["relation_name"]: relation for family in captured.values() for relation in family["relations"]
        }
        if inventory is not None:
            by_name.update({relation["relation_name"]: relation for relation in inventory["relations"]})
        bindings_by_name = {}
        for namespace, table in projection._PROJECTION_DEPENDENCIES:
            canonical = f"{namespace or schema}.{table}"
            physical_schema = by_name[table]["schema_name"] if table in by_name else namespace or schema
            relation = (
                (
                    await session.execute(
                        text("""SELECT oid::bigint AS relation_oid,
                pg_relation_filenode(oid)::bigint AS relfilenode FROM pg_class WHERE oid=to_regclass(:name)"""),
                        {"name": f'"{physical_schema}"."{table}"'},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if relation is None:
                raise RuntimeError("entity-address publication dependency is missing")
            bindings_by_name[canonical] = {"schema_name": physical_schema, "table_name": table, **dict(relation)}
        bindings = projection.validate_projection_dependency_bindings(schema, bindings_by_name)
        await session.execute(text(projection.projection_dependency_lock_sql(schema, dependency_bindings=bindings)))
        if not await session.scalar(
            text("SELECT " + projection.projection_dependency_bindings_match_sql(schema, bindings))
        ):
            raise RuntimeError("entity-address publication dependency changed")
        yield bindings


async def read_active_bindings(session, schema_name):
    """Read only local publication state, never bindings from a portable package."""
    schema = projection._sql_identifier(schema_name, field_name="address schema")
    bindings = await session.scalar(
        text(
            f'SELECT active_dependency_bindings FROM "{schema}".{projection.GEO_ASSURANCE_STATE_TABLE} '
            "WHERE singleton IS TRUE"
        )
    )
    return None if bindings is None else projection.validate_projection_dependency_bindings(schema, bindings)


async def lock_active_dependencies(session, schema_name, *, receiving=False):
    """Call after the alias/publication fence; lock the actual six input heaps."""
    bindings = await read_active_bindings(session, schema_name)
    if receiving and bindings is not None:
        # A grouped receive may already have relocated its predecessor heaps.
        # Lock those exact old OIDs, but never rewrite or export their evidence.
        for binding in bindings.values():
            relation_identity = (
                (
                    await session.execute(
                        text(
                            "SELECT n.nspname AS schema_name,c.relname AS table_name FROM pg_class c "
                            "JOIN pg_namespace n ON n.oid=c.relnamespace "
                            "WHERE c.oid=:oid AND pg_relation_filenode(c.oid)=:filenode"
                        ),
                        {"oid": binding["relation_oid"], "filenode": binding["relfilenode"]},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if relation_identity is None:
                raise RuntimeError("entity-address active dependency binding changed")
            binding.update(dict(relation_identity))
        bindings = projection.validate_projection_dependency_bindings(schema_name, bindings)
    await session.execute(text(projection.projection_dependency_lock_sql(schema_name, dependency_bindings=bindings)))
    if bindings is not None and not await session.scalar(
        text("SELECT " + projection.projection_dependency_bindings_match_sql(schema_name, bindings))
    ):
        raise RuntimeError("entity-address active dependency binding changed")
    return bindings


async def resolve_prepared_bindings(session, schema_name, bindings):
    """Accept only the original held name or its same-OID canonical relocation."""
    if bindings is None:
        return None
    resolved = projection.validate_projection_dependency_bindings(schema_name, bindings)
    for name, binding in resolved.items():
        relation_identity = (
            (
                await session.execute(
                    text(
                        "SELECT namespace.nspname AS schema_name,relation.relname AS table_name "
                        "FROM pg_catalog.pg_class relation JOIN pg_catalog.pg_namespace namespace "
                        "ON namespace.oid=relation.relnamespace WHERE relation.oid=:oid "
                        "AND pg_relation_filenode(relation.oid)=:filenode"
                    ),
                    {"oid": binding["relation_oid"], "filenode": binding["relfilenode"]},
                )
            )
            .mappings()
            .one_or_none()
        )
        if relation_identity is None or (relation_identity["schema_name"], relation_identity["table_name"]) not in {
            (binding["schema_name"], binding["table_name"]),
            tuple(name.split(".", 1)),
        }:
            raise RuntimeError("entity-address prepared dependency binding changed")
        binding.update(dict(relation_identity))
    await session.execute(text(projection.projection_dependency_lock_sql(schema_name, dependency_bindings=resolved)))
    if not await session.scalar(
        text("SELECT " + projection.projection_dependency_bindings_match_sql(schema_name, resolved))
    ):
        raise RuntimeError("entity-address prepared dependency binding changed")
    return resolved
