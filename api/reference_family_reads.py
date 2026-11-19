# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction-pinned pricing reads across native family and shared code slices."""

import json

from sanic.exceptions import InvalidUsage
from sqlalchemy import and_, bindparam, column, exists, func, or_, select, table, text, union, union_all
from sqlalchemy.dialects import postgresql

from process.scoped_catalog_layout import CONTROL_SCHEMA, DATA_SCHEMA, catalog_schema

_PRESENT_SQL = """SELECT array_agg(c.relname::text) FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='mrf' AND c.relname=ANY(CAST(:names AS text[]))"""
# Lengths are not compared: migrated live dictionaries keep varchar(64) where the models declare 128.
# The snapshot must already see each locked table under its name: a stage moved in after the snapshot
# began shows its earlier name there, and its rows loaded after the snapshot would read as empty.
_LOCKED_HEAPS_SQL = """SELECT count(*) FROM unnest(CAST(:names AS text[])) requested(name)
 JOIN pg_catalog.pg_class c ON c.oid=pg_catalog.to_regclass(pg_catalog.format('%I.%I','mrf',requested.name))
 WHERE c.relname=requested.name AND c.relnamespace=pg_catalog.to_regnamespace('mrf')
 AND c.relkind='r' AND c.relpersistence='p'
 AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
 AND NOT (c.relrowsecurity OR c.relforcerowsecurity OR c.relispartition)
 AND NOT pg_catalog.row_security_active(c.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid)
 AND NOT EXISTS(SELECT FROM jsonb_each_text(CAST(:columns AS jsonb)->c.relname::text) required WHERE NOT EXISTS(
  SELECT FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attname=required.key AND a.attnum>0
  AND NOT a.attisdropped AND a.atttypid=pg_catalog.to_regtype(required.value)))"""


async def pin_claims_reader(session):
    """Share-lock every installed physician/Rx family by name before the request's first data read.

    The share locks last until the read ends, so row security or rules cannot be added meanwhile. pg_class is
    read through the snapshot, which can predate the locks; row_security_active reads the current catalog.
    """
    from db.models import CodeCatalog, CodeCrosswalk
    from process.reference_family_archive import (
        TERMINAL_CAPABILITIES,
        _lock_family,
        reference_family_receive_spec,
        reference_family_spec,
    )

    modes = await session.execute(
        text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')")
    )
    if tuple(modes.one()) not in (("repeatable read", "on"), ("serializable", "on")):
        raise RuntimeError("claims reader requires a read-only repeatable-read snapshot")
    receive_names = sorted(
        {name for importer in TERMINAL_CAPABILITIES for name in reference_family_receive_spec(importer).table_names}
    )
    present_names = set(await session.scalar(text(_PRESENT_SQL), {"names": receive_names}) or ())
    installed_importers = []
    for importer, capability in TERMINAL_CAPABILITIES.items():
        scoped_names = {model.__tablename__ for model in (*capability.dictionary_models, *capability.effect_models)}
        if not present_names & scoped_names:
            continue
        if not set(reference_family_receive_spec(importer).table_names) <= present_names:
            raise RuntimeError("installed claims reader family is incomplete")
        installed_importers.append(importer)
    if not installed_importers:
        return None  # Ordinary, uninstalled pricing continues through its existing entrypoint.
    importers = tuple(installed_importers)
    family_names = [name for importer in importers for name in reference_family_spec(importer).table_names]
    names = (*family_names, "code_catalog", "code_crosswalk")
    await _lock_family(session, "mrf", names, "ACCESS SHARE")
    mirrors = (model for importer in importers for model in TERMINAL_CAPABILITIES[importer].dictionary_models)
    columns_by_table = {
        model.__tablename__: {
            column.name: column.type.compile(dialect=postgresql.dialect()) for column in model.__table__.columns
        }
        for model in (CodeCatalog, CodeCrosswalk, *mirrors)
    }
    admitted = await session.scalar(
        text(_LOCKED_HEAPS_SQL),
        {"names": list(names), "columns": json.dumps(columns_by_table)},
    )
    if admitted != len(names):
        raise RuntimeError("installed claims reader table is refused")
    await _require_dictionary_overlap(session, importers)
    session.info["snapshot_claims_dictionary_tables"] = _claims_dictionary_overlay(importers)
    return importers


def claims_filter_crosswalk_query(session):
    """Use the admitted native dictionaries without changing ordinary provider filters."""
    sql = text(
        """
        SELECT DISTINCT to_code
          FROM mrf.code_crosswalk
         WHERE UPPER(from_system) = :from_system
           AND UPPER(from_code) = ANY(:input_codes)
           AND UPPER(to_system) = :target_system
        UNION
        SELECT DISTINCT from_code
          FROM mrf.code_crosswalk
        WHERE UPPER(to_system) = :from_system
           AND UPPER(to_code) = ANY(:input_codes)
           AND UPPER(from_system) = :target_system
        """
    )
    if session is not None and "snapshot_claims_dictionary_tables" in getattr(session, "info", {}):
        _catalog, crosswalk = claims_dictionary_tables(session)
        sql = union(
            select(crosswalk.c.to_code).where(
                func.upper(crosswalk.c.from_system) == bindparam("from_system"),
                func.upper(crosswalk.c.from_code).in_(bindparam("input_codes", expanding=True)),
                func.upper(crosswalk.c.to_system) == bindparam("target_system"),
            ),
            select(crosswalk.c.from_code).where(
                func.upper(crosswalk.c.to_system) == bindparam("from_system"),
                func.upper(crosswalk.c.to_code).in_(bindparam("input_codes", expanding=True)),
                func.upper(crosswalk.c.from_system) == bindparam("target_system"),
            ),
        )
    return sql


def claims_dictionary_tables(session):
    """Use pinned scoped rows when admitted; preserve ordinary dictionary entrypoints otherwise."""
    from db.models import CodeCatalog, CodeCrosswalk

    return getattr(session, "info", {}).get(
        "snapshot_claims_dictionary_tables", (CodeCatalog.__table__, CodeCrosswalk.__table__)
    )


def claims_catalog_table(session):
    """Compose scoped data only for callers that read catalog rows, not crosswalk-only reads."""
    from db.models import CodeCatalog

    return catalog_read_table(CodeCatalog, claims_dictionary_tables(session)[0])


def catalog_read_table(model, shared=None, *, schema=None):
    """Merge the protected catalog with unrelated legacy rows under the existing read snapshot."""
    source = model.__table__
    schema = schema or source.schema or CONTROL_SCHEMA
    active = catalog_schema(schema)
    if shared is None:
        shared = source if schema == source.schema else _catalog_columns(source, schema)
    if active == schema:
        return shared
    from process import scoped_catalog_publication as publication

    owned_sources = publication.catalog_sources(model)
    if not owned_sources:
        raise ValueError("protected catalog reader model differs")
    protected = _catalog_columns(source, active)
    shared_rows = select(*shared.c).where(
        or_(shared.c.source.is_(None), shared.c.source.not_in(owned_sources)),
        ~exists(
            select(1)
            .where(and_(*(shared.c[key.name] == protected.c[key.name] for key in source.primary_key.columns)))
            .correlate(shared)
        ),
    )
    return union_all(shared_rows, select(*protected.c)).subquery(source.name)


def _catalog_columns(source, schema):
    """Model the read columns without copying index and constraint metadata."""
    return table(source.name, *(column(field.name, field.type) for field in source.c), schema=schema)


def catalog_read_relation_sql(model, schema, legacy_relation, *, keys):
    """Use the same compiled catalog projection in the fixed SQL procedure lookups."""
    protected_systems = {"POS", "RC", "MODIFIER", "MS_DRG", "ICD10CM", "ICD10PCS"}
    if catalog_schema(schema) == schema or not any(system in protected_systems for system, _ in keys):
        return legacy_relation
    table = catalog_read_table(model, schema=schema)
    query = table.element.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    return f"({query}) AS scoped_reference_rows"


def uses_scoped_catalog(request, *, collection_kind=None):
    """Select named locks only for request surfaces that can read the scoped catalog."""
    if catalog_schema(CONTROL_SCHEMA) != DATA_SCHEMA:
        return False
    if collection_kind is not None:
        return collection_kind == "by-service"
    path = request.path.rstrip("/")
    if path == "/api/v1/extensions/custom-import/providers/by-service":
        return True
    if path == "/api/v1/codes" or path.startswith("/api/v1/codes/"):
        return not path.endswith("/related")
    if path.startswith("/api/v1/clinical/"):
        parts = path.split("/")
        return parts[4] in {"concepts", "conditions", "treatments", "relationships"} or (
            parts[4] == "clinical-areas" and len(parts) == 7 and parts[-1] in {"conditions", "treatments"}
        )
    if not path.startswith("/api/v1/pricing/"):
        return False
    from api.endpoint.pricing import _parse_bool
    from api.ptg2_response import _is_request_flag_enabled

    args = getattr(request, "args", {})
    if path.endswith(("/estimated-cost-level", "/by-procedure", "/search-by-procedure", "/by-service")) or (
        _is_request_flag_enabled(args.get("include_code_details"))
    ):
        return True
    parts = path.removeprefix("/api/v1/pricing/").split("/")
    if len(parts) == 3 and (parts[0], parts[2]) in {("providers", "procedures"), ("physicians", "services")}:
        if any(
            str(args.get(name) or "").strip()
            for name in ("plan_id", "plan_external_id", "source_key", "snapshot_id", "plan_release_id")
        ):
            return True
    try:
        return _parse_bool(args.get("expand_codes"), "expand_codes")
    except InvalidUsage:
        return False


async def lock_scoped_catalog_reader(session):
    """Keep each selected catalog name stable before a verified Reader takes its snapshot."""
    if catalog_schema(CONTROL_SCHEMA) != DATA_SCHEMA or session.info.get("scoped_catalog_reader_pinned"):
        return
    from process.reference_family_archive import _lock_family
    from process.scoped_catalog_named_storage import MODELS

    await _lock_family(session, DATA_SCHEMA, tuple(model.__tablename__ for model in MODELS), "ACCESS SHARE")
    session.info["scoped_catalog_reader_pinned"] = True


async def _query_catalog_neighbors(session, pairs: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Return same-name neighbors from the request's pinned code catalog."""
    code_catalog_table = claims_catalog_table(session)
    if not pairs:
        return set()

    named_pairs = set()
    for system, code in pairs:
        name_result = await session.execute(
            select(code_catalog_table.c.display_name)
            .where(
                and_(
                    func.upper(code_catalog_table.c.code_system) == system,
                    func.upper(code_catalog_table.c.code) == code,
                )
            )
            .limit(1),
        )
        display_name = name_result.scalar()
        if not display_name:
            continue
        named_pairs.add((system, code))
        neighbors = await session.execute(
            select(code_catalog_table.c.code_system, code_catalog_table.c.code).where(
                func.lower(code_catalog_table.c.display_name) == str(display_name).strip().lower()
            ),
        )
        for neighbor_row in neighbors:
            pair = (str(neighbor_row[0]).upper(), str(neighbor_row[1]).upper())
            named_pairs.add(pair)
    return named_pairs


def _claims_dictionary_overlay(importers=("claims-pricing",)):
    """Pinned native keys win; unrelated shared rows remain available through indexed anti-joins."""
    from db.models import CodeCatalog, CodeCrosswalk
    from process.reference_family_archive import _CLAIMS_DICTIONARY_SOURCE, TERMINAL_CAPABILITIES

    tables = []
    for position, model in enumerate((CodeCatalog, CodeCrosswalk)):
        live = model.__table__
        mirrors = [TERMINAL_CAPABILITIES[importer].dictionary_models[position].__table__ for importer in importers]
        selectors = []
        for source in (live, *mirrors):
            exclusions = mirrors if source is live else mirrors[: mirrors.index(source)]
            selector = select(*source.c)
            if source is live and "claims-pricing" in importers:
                selector = selector.where(live.c.source.is_distinct_from(_CLAIMS_DICTIONARY_SOURCE))
            for mirror in exclusions:
                collision = and_(
                    *(source.c[column.name] == mirror.c[column.name] for column in live.primary_key.columns)
                )
                selector = selector.where(~exists(select(1).where(collision).correlate(source)))
            selectors.append(selector)
        tables.append(union_all(*selectors).subquery(live.name))
    return tuple(tables)


async def _require_dictionary_overlap(session, importers):
    """Shared immutable keys cannot silently select different installed payloads."""
    from process.reference_family_archive import TERMINAL_CAPABILITIES, _dictionary_key_join

    if len(importers) < 2:
        return
    for left, right in zip(
        TERMINAL_CAPABILITIES[importers[0]].dictionary_models,
        TERMINAL_CAPABILITIES[importers[1]].dictionary_models,
        strict=True,
    ):
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM mrf.{left.__tablename__} left_row JOIN mrf.{right.__tablename__} right_row ON {_dictionary_key_join(left, 'left_row', 'right_row')} WHERE to_jsonb(left_row) IS DISTINCT FROM to_jsonb(right_row))"
            )
        ):
            raise RuntimeError("installed claims dictionary payloads conflict")
