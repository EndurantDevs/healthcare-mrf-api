# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Local held dependency locks, projection state, and closed physical relocation."""

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
    database: object
    schema: str
    session: object
    transaction: object
    backend_id: int
    relations_by_name: dict


_HELD_PUBLICATION_DEPENDENCIES = ContextVar("held_address_publication_dependencies", default=None)


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


async def capture_selected_publication_bindings(session, schema_name, *, node_id=None):
    """Read the existing protected source selection on the caller's actual transaction."""
    from process.tiger_held_inputs import selected_captured_inventories, selected_tiger_inventory

    schema = projection._sql_identifier(schema_name, field_name="address schema")
    node_options_dict = {} if node_id is None else {"node_id": node_id}
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
            return None
    by_name = {relation["relation_name"]: relation for family in captured.values() for relation in family["relations"]}
    if inventory is not None:
        by_name.update({relation["relation_name"]: relation for relation in inventory["relations"]})
    return by_name, await capture_publication_dependency_bindings(session, schema, by_name)


async def capture_publication_dependency_bindings(session, schema_name, selected_relations):
    """Resolve only the same six fixed canonical or already-authenticated selected inputs."""
    schema = projection._sql_identifier(schema_name, field_name="address schema")
    by_name = selected_relations
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
    return projection.validate_projection_dependency_bindings(schema, bindings_by_name)


async def capture_inference_inputs(session, schema_name, selected_relations):
    """Observe the existing inference prerequisites and the NUCC generation actually used."""
    schema = projection._sql_identifier(schema_name, field_name="address schema")
    prerequisites_by_name = {}
    for name in ("npi", "npi_address", "npi_taxonomy", "nucc_taxonomy"):
        namespace = selected_relations.get(name, {}).get("schema_name", schema)
        observed = (
            (
                await session.execute(
                    text(
                        "SELECT c.oid::bigint AS relation_oid,c.relkind::text AS kind,"
                        "pg_relation_filenode(c.oid)::bigint AS relfilenode,"
                        "c.relowner=(SELECT nspowner FROM pg_namespace WHERE nspname='hp_snapshot_retention') "
                        "AS protected_owner FROM pg_class c WHERE c.oid=to_regclass(:name)"
                    ),
                    {"name": f'"{namespace}"."{name}"'},
                )
            )
            .mappings()
            .one_or_none()
        )
        prerequisites_by_name[name] = None if observed is None else {"schema": namespace, **dict(observed)}
        if name in selected_relations:
            expected = selected_relations[name]
            if observed is None or any(observed[key] != expected[key] for key in ("relation_oid", "relfilenode")):
                raise RuntimeError("entity-address inference selected input changed")
    nucc = (
        await _capture_nucc_generation(session, prerequisites_by_name["nucc_taxonomy"])
        if all(prerequisites_by_name.values())
        else None
    )
    return {"prerequisites": prerequisites_by_name, "nucc": nucc}


async def _capture_nucc_generation(session, relation):
    """Reuse native or historical NUCC custody; no registration or new reader authority."""
    from process import reference_family_result_generation as reference

    if relation["kind"] == "p":
        from process.provider_directory_cms_native_inputs import capture_protected_nucc_input

        observed = await capture_protected_nucc_input(session, schema=relation["schema"])
        if observed["physical"]["parent_oid"] != relation["relation_oid"]:
            raise RuntimeError("entity-address inference NUCC input changed")
        return observed
    if relation["kind"] != "r":
        raise RuntimeError("entity-address inference NUCC input changed")
    authority = await reference.read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name=relation["schema"]
    )
    if authority.serving_generation is None or authority.relation_oids != (relation["relation_oid"],):
        raise RuntimeError("entity-address inference NUCC generation changed")
    if relation["protected_owner"]:
        await reference.require_nucc_native_predecessor_storage(
            session, schema_name=relation["schema"], expected_relation_oid=relation["relation_oid"]
        )
    else:
        await reference.capture_reference_family_serving_generation(
            session, importer_id="nucc", schema_name=relation["schema"]
        )
    return {"generation": authority.as_dict()}


@asynccontextmanager
async def selected_publication_dependencies(database, schema_name, *, control_context=None):
    """Hold complete protected input families through the ordinary native finalizer."""
    schema = projection._sql_identifier(schema_name, field_name="address schema")
    async with database.session_factory() as session, session.begin():
        node_options_dict = {}
        if control_context is not None and control_context.get("control_run_id"):
            from process.entity_address_native_publication import controlled_dependency_node

            node_options_dict["node_id"] = await controlled_dependency_node(session, schema, control_context)
        selected = await capture_selected_publication_bindings(session, schema, **node_options_dict)
        if selected is None:
            yield None
            return
        by_name, bindings = selected
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
    relations_by_name = {}
    for relation in [*captured_relations_by_name.values(), *bindings.values()]:
        namespace = projection._sql_identifier(relation["schema_name"], field_name="dependency schema")
        name = projection._sql_identifier(
            relation.get("table_name", relation.get("relation_name")), field_name="dependency table"
        )
        relations_by_name[f'"{namespace}"."{name}"'] = (relation["relation_oid"], relation["relfilenode"])
    held = _HeldPublicationDependencies(
        database,
        schema,
        session,
        session.get_transaction(),
        await session.scalar(text("SELECT pg_backend_pid()")),
        relations_by_name,
    )
    token = _HELD_PUBLICATION_DEPENDENCIES.set(held)
    try:
        yield
    finally:
        _HELD_PUBLICATION_DEPENDENCIES.reset(token)


async def lock_selected_publication_dependencies(database, schema_name):
    """Overlap the build's physical locks with cutover before checking its original owner."""
    held = _HELD_PUBLICATION_DEPENDENCIES.get()
    if held is None:
        return
    binding = database._transaction_binding()
    if held.database is not database or held.schema != schema_name or binding is None:
        raise RuntimeError("entity-address publication dependency transaction changed")
    session = binding.session
    names = sorted(held.relations_by_name)
    await session.execute(
        text("LOCK TABLE " + ", ".join("ONLY " + name for name in names) + " IN ACCESS SHARE MODE NOWAIT")
    )
    observed = (
        await session.execute(
            text(
                "SELECT name, to_regclass(name)::oid::bigint, "
                "pg_relation_filenode(to_regclass(name))::bigint "
                "FROM unnest(CAST(:names AS text[])) AS name"
            ),
            {"names": names},
        )
    ).all()
    if {name: (oid, filenode) for name, oid, filenode in observed} != held.relations_by_name:
        raise RuntimeError("entity-address publication dependency changed")
    if (
        held.session.get_transaction() is not held.transaction
        or not held.session.in_transaction()
        or await held.session.scalar(text("SELECT pg_backend_pid()")) != held.backend_id
    ):
        raise RuntimeError("entity-address publication dependency lock owner changed")


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
