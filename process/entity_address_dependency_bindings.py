# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named dependency selection, native reader locks, and logical projection state."""

import json
from collections.abc import Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy import text

from api import ptg2_geo_projection as projection
from db.models import EntityAddressUnified

PUBLISHER_SELECTED_INPUTS_CONTRACT = "entity-address-publisher-selected-inputs.v1"


@dataclass(frozen=True)
class _HeldPublicationDependencies:
    schema: str
    session: object
    relation_names: tuple


_HELD_PUBLICATION_DEPENDENCIES = ContextVar("held_address_publication_dependencies", default=None)


def validate_publisher_selected_inputs(schema_name, value):
    """Decode trusted local selection metadata, never confer custody from JSON."""
    if (
        not isinstance(value, Mapping)
        or set(value) != {"contract", "dependency_bindings"}
        or value["contract"] != PUBLISHER_SELECTED_INPUTS_CONTRACT
    ):
        raise ValueError("entity-address publisher selected inputs are invalid")
    validator = (
        projection.validate_projection_dependency_qnames
        if projection._is_named_binding_family(value["dependency_bindings"])
        else projection.validate_projection_dependency_bindings
    )
    return {
        "contract": PUBLISHER_SELECTED_INPUTS_CONTRACT,
        "dependency_bindings": validator(schema_name, value["dependency_bindings"]),
    }


def publisher_selected_input_names(schema_name, value):
    """Project a validated local selection to portable names without rewriting retained input metadata."""
    selected = validate_publisher_selected_inputs(schema_name, value)
    bindings_by_name = {
        name: {field: binding[field] for field in ("schema_name", "table_name")}
        for name, binding in selected["dependency_bindings"].items()
    }
    return {
        "contract": PUBLISHER_SELECTED_INPUTS_CONTRACT,
        "dependency_bindings": projection.validate_projection_dependency_qnames(schema_name, bindings_by_name),
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
    return publisher_selected_input_names(schema_name, selected)


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
        db_schema,
        **(
            {}
            if dependency_bindings is None or require_canonical_publication
            else {"dependency_bindings": dependency_bindings}
        ),
    )
    selected_by_name = None
    if dependency_bindings is not None and not require_canonical_publication:
        selected_by_name = {
            name: {field: entry[field] for field in ("schema_name", "table_name")}
            for name, entry in dependency_bindings.items()
        }
    bindings_sql = (
        "NULL"
        if selected_by_name is None
        else "'" + json.dumps(selected_by_name, sort_keys=True).replace("'", "''") + "'::jsonb"
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
        NULL,
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
    RETURNING candidate_geo_assurance_version;
    """


def activate_geo_assurance_candidate_sql(db_schema: str, *, dependency_bindings=None) -> str:
    """Activate only the recorded candidate heap with its still-valid dependency map."""
    db_schema = validate_schema_name(db_schema)
    state_table = projection.GEO_ASSURANCE_STATE_TABLE
    signature, bindings_match = projection.projection_stored_bindings_sql(db_schema, "candidate_dependency_bindings")
    generation_matches = projection.projection_npi_generation_match_sql(
        db_schema, "candidate", dependency_bindings=dependency_bindings
    )
    return f"""
    UPDATE {db_schema}.{state_table}
       SET active_geo_assurance_version = candidate_geo_assurance_version,
           active_table_oid = NULL,
           active_relation_signature = candidate_relation_signature,
           active_dependency_bindings = candidate_dependency_bindings,
           candidate_geo_assurance_version = NULL,
           candidate_table_oid = NULL,
           candidate_relation_signature = NULL,
           candidate_dependency_bindings = NULL,
           candidate_projected_rows = NULL
     WHERE singleton IS TRUE
       AND candidate_geo_assurance_version = {projection.GEO_ASSURANCE_VERSION}
       AND {bindings_match}
       AND candidate_relation_signature - '{db_schema}.npi_result_generation' = ({signature})
       AND {generation_matches}
       AND EXISTS(SELECT 1 FROM pg_class live WHERE live.oid=to_regclass('{db_schema}.entity_address_unified')
           AND live.relkind='r' AND live.relpersistence='p' AND NOT live.relrowsecurity AND NOT live.relforcerowsecurity)
    RETURNING active_geo_assurance_version;
    """


async def preflight_publication_dependencies(database, schema_name, ctx):
    """Fail before staging; publication independently reacquires these input locks."""
    from process.entity_address_native_publication import _native, controlled_publication_enabled

    native = _native()
    live_models = (
        (EntityAddressUnified,)
        if ctx["context"]["serving_only_refresh"]
        else (EntityAddressUnified, *native.SUPPORT_TABLE_MODELS)
    )
    await native._assert_cutover_has_no_dependent_views(schema_name, [model.__main_table__ for model in live_models])
    if not native.candidate_preparation.geo_dependency_options():
        async with native.selected_publication_dependencies(
            database, schema_name, **({"control_context": ctx} if controlled_publication_enabled(ctx) else {})
        ):
            return


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
                raise RuntimeError(
                    "entity-address inherited TIGER publication requires protected captured inputs; "
                    "capture and select a protected TIGER source before retrying"
                )
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
            bindings_by_name[canonical] = {"schema_name": physical_schema, "table_name": table}
        bindings = projection.validate_projection_dependency_qnames(schema, bindings_by_name)
        await session.execute(text(projection.projection_dependency_lock_sql(schema, dependency_bindings=bindings)))
        if not await session.scalar(
            text("SELECT " + projection.projection_dependency_bindings_match_sql(schema, bindings))
        ):
            raise RuntimeError("entity-address publication dependency changed")
        async with _hold_publication_dependencies(database, schema, session, by_name, bindings):
            yield bindings


@asynccontextmanager
async def _hold_publication_dependencies(database, schema, session, captured_relations_by_name, bindings):
    """Retain the original lock owner while independent backends prepare its publication."""
    await session.execute(
        text(
            "SELECT set_config('idle_in_transaction_session_timeout', '0', true), "
            "CASE WHEN current_setting('transaction_timeout', true) IS NOT NULL "
            "THEN set_config('transaction_timeout', '0', true) END"
        )
    )
    relation_names = set()
    for relation in [*captured_relations_by_name.values(), *bindings.values()]:
        namespace = projection._sql_identifier(relation["schema_name"], field_name="dependency schema")
        name = projection._sql_identifier(
            relation.get("table_name", relation.get("relation_name")), field_name="dependency table"
        )
        relation_names.add(f'"{namespace}"."{name}"')
    held = _HeldPublicationDependencies(schema, session, tuple(sorted(relation_names)))
    token = _HELD_PUBLICATION_DEPENDENCIES.set(held)
    try:
        yield
    finally:
        _HELD_PUBLICATION_DEPENDENCIES.reset(token)


async def lock_selected_publication_dependencies(database, schema_name):
    """Acquire the selected logical input names on the native cutover transaction."""
    held = _HELD_PUBLICATION_DEPENDENCIES.get()
    if held is None:
        return
    binding = database._transaction_binding()
    if held.schema != schema_name or binding is None:
        raise RuntimeError("entity-address publication dependency scope changed")
    session = binding.session
    # Overlap the preparation's read locks before its context releases them.
    await session.execute(
        text("LOCK TABLE " + ", ".join("ONLY " + name for name in held.relation_names) + " IN ACCESS SHARE MODE NOWAIT")
    )


async def read_active_bindings(session, schema_name):
    """Read only local publication state, never bindings from a portable package."""
    schema = projection._sql_identifier(schema_name, field_name="address schema")
    bindings = await session.scalar(
        text(
            f'SELECT active_dependency_bindings FROM "{schema}".{projection.GEO_ASSURANCE_STATE_TABLE} '
            "WHERE singleton IS TRUE"
        )
    )
    if bindings is None:
        return None
    validator = (
        projection.validate_projection_dependency_qnames
        if projection._is_named_binding_family(bindings)
        else projection.validate_projection_dependency_bindings
    )
    return validator(schema, bindings)


async def lock_active_dependencies(session, schema_name, *, receiving=False):
    """Call after the alias/publication fence; lock the actual six input heaps."""
    bindings = await read_active_bindings(session, schema_name)
    await session.execute(text(projection.projection_dependency_lock_sql(schema_name, dependency_bindings=bindings)))
    if bindings is not None and not await session.scalar(
        text("SELECT " + projection.projection_dependency_bindings_match_sql(schema_name, bindings))
    ):
        raise RuntimeError("entity-address active dependency binding changed")
    return bindings


async def resolve_prepared_bindings(session, schema_name, bindings):
    """Use declared names; copied snapshots never relocate by numeric tokens."""
    if bindings is None:
        return None
    validator = (
        projection.validate_projection_dependency_qnames
        if projection._is_named_binding_family(bindings)
        else projection.validate_projection_dependency_bindings
    )
    resolved = validator(schema_name, bindings)
    await session.execute(text(projection.projection_dependency_lock_sql(schema_name, dependency_bindings=resolved)))
    if not await session.scalar(
        text("SELECT " + projection.projection_dependency_bindings_match_sql(schema_name, resolved))
    ):
        raise RuntimeError("entity-address prepared dependency binding changed")
    return resolved


async def activate_geo_assurance_candidate(database, schema_name):
    """Publish only the candidate whose locked named inputs still match its generation."""
    from process.entity_address_preparation_admission import read_scalar

    schema_name = validate_schema_name(schema_name)
    bindings = await read_scalar(
        database,
        f"SELECT candidate_dependency_bindings FROM {schema_name}.{projection.GEO_ASSURANCE_STATE_TABLE} "
        "WHERE singleton IS TRUE",
    )
    statement = activate_geo_assurance_candidate_sql(schema_name, dependency_bindings=bindings)
    return await read_scalar(database, statement)
