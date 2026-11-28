# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local MRF address coverage and immutable content receipt."""

import asyncio
import math
from dataclasses import field
from uuid import UUID

from sqlalchemy import MetaData, Table, text
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import SchemaType

from db.models import AddressArchiveV2
from process import entity_address_snapshot_receipt as catalog_identity
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.ext.address_canon import CURRENT_ADDRESS_IDENTITY_VERSION, address_key_v1, archive_table_name
from process.ext.address_format import ADDRESS_FORMAT_SOURCE, ADDRESS_FORMAT_VERSION
from process.reference_family_result_generation import RELATION_NAMES_BY_IMPORTER

STAGE_TABLE = "mrf_canonical_address"
REFERENCES = ("mrf_address", "mrf_address_evidence")
MODEL_CLOSURE_CONTRACT = "canonical-address-model-closure.v1"
ACCOUNTED_MODEL_CLOSURE_CONTRACT = "canonical-address-model-closure.v2"
MODEL_CLOSURE_CONTRACTS = (MODEL_CLOSURE_CONTRACT, ACCOUNTED_MODEL_CLOSURE_CONTRACT)
ADDRESS_COVERAGE_CONTRACT = "mrf-address-coverage.indexed-set.v3"
MERGE_CONTRACT = "canonical-address-model-publication.v1"
CANONICAL_POLICIES = {"npi": ("npi_canonical_address", 1, 0), "mrf": (STAGE_TABLE, 16, 5)}
MAX_REDIRECT_DEPTH = 64
IDENTITY_COLUMNS = (
    "identity_key",
    "identity_version",
    "precision",
    "premise_key",
    "line1_norm",
    "unit_norm",
    "city_norm",
    "state_code",
    "zip5",
    "zip4",
    "country_code",
)
DISPLAY_COLUMNS = (
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "formatted_address",
    "formatted_address_version",
    "formatted_address_source",
)


def _clone_model_table(source: Table, metadata: MetaData, *, schema: str | None, name: str | None = None) -> Table:
    """Relocate heaps without relocating declared shared native types or their DDL policy."""
    table = source.to_metadata(metadata, schema=schema, name=name)
    for column in table.columns:
        declared_type = source.c[column.key].type
        if isinstance(declared_type, SchemaType) and getattr(declared_type, "schema", None) is not None:
            column.type.schema = declared_type.schema
            if hasattr(declared_type, "create_type"):
                column.type.create_type = declared_type.create_type
    return table


def canonical_contribution_model(importer_id):
    """Compile the same complete canonical model under one fixed contribution name."""
    name, _source_bit, _priority = CANONICAL_POLICIES[importer_id]
    table = _clone_model_table(
        AddressArchiveV2.__table__, MetaData(), schema=AddressArchiveV2.__table__.schema, name=name
    )
    for index in table.indexes:
        index.name = index.name.replace(AddressArchiveV2.__tablename__, name)
    for constraint in table.constraints:
        if constraint.name:
            constraint.name = constraint.name.replace(AddressArchiveV2.__tablename__, name)
    return type(
        "NpiCanonicalAddress" if importer_id == "npi" else "MrfCanonicalAddress",
        (),
        {"__tablename__": name, "__table__": table},
    )


async def canonical_spatial_index(session, schema, name, qualified):
    """Preserve the existing optional native PostGIS index without adding executable hooks."""
    if (
        await session.scalar(
            text(
                "SELECT to_regtype('public.geography') IS NOT NULL "
                "AND to_regprocedure('public.st_makepoint(double precision,double precision)') IS NOT NULL"
            )
        )
        is True
    ):
        await session.execute(
            text(
                f'CREATE INDEX "{name}_geo_idx" ON {qualified(schema, name)} USING gist '
                "(public.Geography(public.ST_MakePoint((long)::double precision,(lat)::double precision))) "
                "WHERE lat IS NOT NULL AND long IS NOT NULL"
            )
        )


def normalized_canonical_catalog(columns, constraints, indexes):
    """Normalize physical column order and the replaced self edge, never payload rows."""
    column_names_by_number = {entry["attnum"]: entry["attname"] for entry in columns}
    normalized_columns = sorted(
        ({key: field for key, field in entry.items() if key != "attnum"} for entry in columns),
        key=lambda entry: entry["attname"],
    )
    normalized_constraints = []
    for entry in constraints:
        kind = entry["contype"]
        if kind in ("f", b"f"):
            if (
                entry["key_columns"]
                != "{"
                + str(next(number for number, name in column_names_by_number.items() if name == "merged_into"))
                + "}"
                or entry["referenced_table"] != "address_archive_v2"
                or entry["referenced_in_archive_schema"] is not True
            ):
                raise RuntimeError("canonical source relationship catalog is unsupported")
            continue
        normalized_by_field = dict(entry)
        normalized_by_field["key_columns"] = [
            column_names_by_number[int(number)] for number in entry["key_columns"].strip("{}").split(",") if number
        ]
        if kind in ("c", b"c"):
            normalized_by_field["convalidated"] = True  # Native candidate CHECKs validate the complete copied set.
        normalized_constraints.append(normalized_by_field)
    normalized_indexes = []
    for entry in indexes:
        normalized_by_field = dict(entry)
        normalized_by_field["keys"] = [
            column_names_by_number.get(int(number), "expression") for number in entry["keys"].split()
        ]
        normalized_by_field["key_attributes"] = [
            {**attribute, "attribute_number": column_names_by_number.get(attribute["attribute_number"], "expression")}
            for attribute in entry["key_attributes"]
        ]
        normalized_indexes.append(normalized_by_field)
    return {
        "columns": normalized_columns,
        "constraints": sorted(normalized_constraints, key=catalog_identity._canonical_digest),
        "indexes": sorted(normalized_indexes, key=catalog_identity._canonical_digest),
    }


async def canonical_schema_identity(session, relation_oid, schema, *, omit_index=None):
    """Authenticate full native types/constraints/index shape with physical-order equivalence."""
    from process.entity_address_native_publication import _catalog_search_path

    async with _catalog_search_path(session):
        await session.execute(text("SET LOCAL search_path=pg_catalog,public,pg_temp"))
        columns = await catalog_identity._catalog_columns(session, relation_oid)
        constraints = await catalog_identity._catalog_constraints(session, relation_oid, schema)
        indexes = await catalog_identity._catalog_indexes(session, relation_oid)
    catalog_identity._reject_schema_qualified_expressions(schema, columns, constraints, indexes)
    catalog = normalized_canonical_catalog(columns, constraints, indexes)
    if omit_index is not None:
        catalog["indexes"] = [index for index in catalog["indexes"] if not omit_index(index)]
    return catalog_identity._canonical_digest(catalog)


async def require_canonical_source_model(session, schema, qualified):
    """Attest the existing native model before any source payload projection.

    The caller already holds its source SREX fence. Use the existing native
    catalog compiler, not reconstructed rows or a second copying mechanism.
    """
    archive = qualified(schema, archive_table_name())
    oid = await session.scalar(text("SELECT to_regclass(:archive)::oid"), {"archive": archive})
    if type(oid) is not int or oid <= 0:
        raise RuntimeError("canonical source model is unavailable")
    enum_oid = await _canonical_enum_oid(session)
    await require_native_read_catalog(session, (oid,), allowed_type_oids=(enum_oid,))
    if (
        await session.scalar(
            text(
                "SELECT (SELECT count(*) FROM pg_catalog.pg_constraint "
                "WHERE conrelid=CAST(:oid AS oid) AND contype='f')<>1 OR "
                "EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conrelid=CAST(:oid AS oid) "
                "AND c.contype='f' AND (c.confrelid<>c.conrelid OR c.condeferrable OR c.condeferred "
                "OR c.confupdtype<>'a' OR c.confdeltype<>'a' OR c.confmatchtype<>'s' "
                "OR c.conkey<>ARRAY[(SELECT attnum FROM pg_catalog.pg_attribute "
                "WHERE attrelid=c.conrelid AND attname='merged_into')]::smallint[] "
                "OR c.confkey<>ARRAY[(SELECT attnum FROM pg_catalog.pg_attribute "
                "WHERE attrelid=c.conrelid AND attname='address_key')]::smallint[]))"
            ),
            {"oid": oid},
        )
        is not False
    ):
        raise RuntimeError("canonical source relationship differs")
    await _require_canonical_shape(session, schema, oid, qualified)


async def _require_canonical_shape(session, schema, oid, qualified):
    """Compare the complete native model independently of live-source relationship policy."""
    from process import entity_address_native_publication as native
    from process import entity_address_snapshot_restore as restore

    model = canonical_contribution_model("mrf")
    temporary = native._model_catalog_table(model)
    async with native._catalog_search_path(session):
        await session.execute(text("SET LOCAL search_path=pg_catalog,public,pg_temp"))
        await session.execute(CreateTable(temporary))
        try:
            await native._create_catalog_indexes(session, model, temporary, restore)
            await canonical_spatial_index(session, "pg_temp", temporary.name, qualified)
            expected_oid = await session.scalar(
                text("SELECT to_regclass(:relation)::oid"), {"relation": qualified("pg_temp", temporary.name)}
            )
            if await canonical_schema_identity(session, oid, schema) != await canonical_schema_identity(
                session, expected_oid, "pg_temp"
            ):
                raise RuntimeError("canonical source native catalog differs")
        except BaseException as failure:
            try:
                await session.execute(text(f"DROP TABLE {qualified('pg_temp', temporary.name)}"))
            except Exception:
                raise failure from None
            raise
        else:
            await session.execute(text(f"DROP TABLE {qualified('pg_temp', temporary.name)}"))


async def _canonical_enum_oid(session):
    """Only the migration-created native enum is permitted in a canonical model."""
    enum = (
        (
            await session.execute(
                text(
                    "SELECT t.oid,t.typtype='e' AND array_agg(e.enumlabel ORDER BY e.enumsortorder)="
                    "ARRAY['mapbox','google','tiger','manual','openaddresses']::name[] AS accepted "
                    "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_enum e ON e.enumtypid=t.oid "
                    "WHERE t.oid=pg_catalog.to_regtype('mrf.address_archive_geo_source') GROUP BY t.oid"
                )
            )
        )
        .mappings()
        .first()
    )
    if enum is None or enum["accepted"] is not True:
        raise RuntimeError("canonical source enum differs")
    return enum["oid"]


async def require_native_read_catalog(session, oids, *, allowed_type_oids=(), executable_roles=None):
    """Validate native layout, types and supported expressions before SOURCE payload reads."""
    _native_execution_subjects(executable_roles)
    await _require_native_read_structure(session, oids, allowed_type_oids)


async def _require_native_read_structure(session, oids, allowed_type_oids):
    """Resolve transient local handles to qualified names and validate native layout."""
    from process import entity_address_native_publication as native

    if not oids or any(type(oid) is not int or oid <= 0 for oid in oids):
        raise RuntimeError("native SOURCE catalog is incomplete")
    rows = (
        (
            await session.execute(
                text(
                    "SELECT n.nspname AS schema_name,c.relname AS relation_name,c.relkind='r' AND c.relpersistence='p' "
                    "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i "
                    "WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) AS supported FROM pg_catalog.pg_class c "
                    "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=ANY(CAST(:oids AS oid[]))"
                ),
                {"oids": list(oids)},
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != len(oids) or any(row["supported"] is not True for row in rows):
        raise RuntimeError("native SOURCE heap topology differs")
    await native._require_named_execution_catalog(
        session,
        tuple((row["schema_name"], row["relation_name"]) for row in rows),
        allowed_type_oids=allowed_type_oids,
        allowed_extensions=("postgis", "intarray", "btree_gin", "btree_gist", "pg_trgm"),
    )


def _native_execution_subjects(executable_roles):
    if executable_roles is None:
        return (None,)
    from db.plan_release_storage import storage_identifier

    if (
        type(executable_roles) is not tuple
        or len(executable_roles) != 2
        or any(type(role) is not str for role in executable_roles)
        or len(set(executable_roles)) != 2
    ):
        raise RuntimeError("native SOURCE executable roles are invalid")
    for role in executable_roles:
        storage_identifier(role)
    return executable_roles


def canonical_reference_filter(importer_id, schema, qualified, *, key="canonical.address_key", canonical_name=None):
    """Select the full canonical model through the importer's indexed address references."""
    references = ("npi_address",) if importer_id == "npi" else REFERENCES
    if importer_id not in CANONICAL_POLICIES:
        raise RuntimeError("canonical contribution importer is unsupported")
    archive = qualified(schema, canonical_name or archive_table_name())
    direct = " UNION ".join(
        f"SELECT address_key FROM {qualified(schema, name)} WHERE address_key IS NOT NULL" for name in references
    )
    return (
        f"WHERE {key} IN (WITH RECURSIVE selected_keys(address_key) AS ({direct} UNION "
        f"SELECT row_value.merged_into FROM {archive} row_value JOIN selected_keys USING(address_key) "
        "WHERE row_value.merged_into IS NOT NULL) SELECT address_key FROM selected_keys)"
    )


def _direct_reference_predicate(importer_id, stage, key):
    """Only genuine raw references carry the fixed importer's contribution authority."""
    references = ("npi_address",) if importer_id == "npi" else REFERENCES
    schema = stage.split(".", 1)[0]  # Trusted fixed qualified model name, never peer SQL.
    return (
        "("
        + " OR ".join(
            f'EXISTS(SELECT 1 FROM {schema}."{name}" referenced WHERE referenced.address_key={key})'
            for name in references
        )
        + ")"
    )


async def validate_canonical_closure(
    session, importer_id, schema, qualified, *, address_coverage=None, npi_alias_authority=None
):
    """Check complete key closure and declared attribution only after all model indexes exist."""
    if npi_alias_authority is not None:
        from process.npi_alias_witness import validate_npi_alias_contribution

        if importer_id != "npi" or address_coverage is not None:
            raise RuntimeError("NPI alias profile differs")
        await validate_npi_alias_contribution(session, schema, npi_alias_authority)
        return
    table_name, source_bit, _priority = CANONICAL_POLICIES[importer_id]
    stage = qualified(schema, table_name)
    references = ("npi_address",) if importer_id == "npi" else REFERENCES
    if address_coverage is not None:
        if importer_id != "mrf" or address_coverage.get("contract") != ADDRESS_COVERAGE_CONTRACT:
            raise RuntimeError("canonical contribution address accounting differs")
        require_address_coverage(address_coverage)
        observed = await _capture_raw_address_counts(session, schema, qualified, stage)
        if observed != address_coverage["tables"]:
            raise RuntimeError("canonical contribution address accounting differs")
        references = ()
    for name in references:
        invalid = await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {qualified(schema, name)} referenced "
                f"WHERE {'referenced.address_key IS NULL OR ' if importer_id == 'mrf' else ''}"
                f"(referenced.address_key IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {stage} canonical "
                "WHERE canonical.address_key=referenced.address_key)))"
            )
        )
        if invalid is not False:
            raise RuntimeError("canonical contribution address closure is incomplete")
    selected = canonical_reference_filter(importer_id, schema, qualified, canonical_name=table_name)
    direct = _direct_reference_predicate(importer_id, stage, "canonical.address_key")
    invalid = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {stage} canonical WHERE "
            f"({direct} AND (source_bits & {source_bit}) <> {source_bit}) OR NOT ("
            + selected.removeprefix("WHERE ")
            + "))"
        )
    )
    if invalid is not False:
        raise RuntimeError("canonical contribution address scope differs")
    await _validate_redirect_closure(session, stage)


async def _validate_redirect_closure(session, stage):
    """Replace the isolated self edge with bounded, indexed whole-set graph validation."""
    if (
        await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {stage} source WHERE source.merged_into IS NOT NULL "
                f"AND NOT EXISTS(SELECT 1 FROM {stage} target WHERE target.address_key=source.merged_into))"
            )
        )
        is not False
    ):
        raise RuntimeError("canonical redirect target is missing")
    invalid = await session.scalar(
        text(
            "WITH RECURSIVE paths(address_key,depth,path,cycle) AS ("
            f"SELECT address_key,0,ARRAY[address_key],false FROM {stage} WHERE merged_into IS NOT NULL UNION ALL "
            "SELECT target.address_key,paths.depth+1,paths.path || target.address_key,target.address_key=ANY(paths.path) "
            f"FROM paths JOIN {stage} source USING(address_key) JOIN {stage} target ON target.address_key=source.merged_into "
            f"WHERE NOT paths.cycle AND paths.depth<{MAX_REDIRECT_DEPTH}) "
            f"SELECT EXISTS(SELECT 1 FROM paths JOIN {stage} source USING(address_key) "
            f"WHERE cycle OR (depth={MAX_REDIRECT_DEPTH} AND source.merged_into IS NOT NULL))"
        )
    )
    if invalid is not False:
        raise RuntimeError("canonical redirect cycle or depth differs")


async def validate_canonical_merge(session, stage, archive):
    """Reject key/identity ambiguity; never pick an identity by contribution order."""
    left = ",".join(f"contribution.{name}" for name in IDENTITY_COLUMNS)
    right = ",".join(f"incumbent.{name}" for name in IDENTITY_COLUMNS)
    conflict = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {stage} contribution JOIN {archive} incumbent "
            "ON incumbent.address_key=contribution.address_key OR incumbent.identity_key=contribution.identity_key "
            "WHERE incumbent.address_key IS DISTINCT FROM contribution.address_key "
            f"OR ROW({left}) IS DISTINCT FROM ROW({right}) "
            "OR incumbent.merged_into IS DISTINCT FROM contribution.merged_into)"
        )
    )
    if conflict is not False:
        raise RuntimeError("canonical destination identity conflicts")


async def validate_canonical_contributions(session, contributions, archive, *, npi_alias_authority=None):
    """Authenticate every identity pair before publishing either selected contribution."""
    if not contributions or not set(contributions) <= set(CANONICAL_POLICIES):
        raise RuntimeError("canonical contribution scope differs")
    if npi_alias_authority is not None:
        from process.npi_alias_witness import validate_npi_alias_contribution

        if "npi" not in contributions:
            raise RuntimeError("NPI alias contribution is unavailable")
        schema = contributions["npi"].split(".", 1)[0].strip('"')
        await validate_npi_alias_contribution(session, schema, npi_alias_authority)
    for importer_id, stage in sorted(contributions.items()):
        await validate_canonical_merge(session, stage, archive)
        for other_id, other_stage in sorted(contributions.items()):
            if importer_id < other_id:
                await validate_canonical_merge(session, stage, other_stage)


def canonical_archive_projection(contributions, archive, *, npi_alias_authority=None):
    """Project the serial fixed-priority merge, including unchanged and inserted rows."""
    if not contributions or not set(contributions) <= set(CANONICAL_POLICIES):
        raise RuntimeError("canonical contribution scope differs")
    if npi_alias_authority is not None:
        from process.npi_alias_witness import npi_alias_archive_projection

        return npi_alias_archive_projection(contributions, archive, npi_alias_authority)
    columns = tuple(column.name for column in AddressArchiveV2.__table__.columns)
    selected = sorted(contributions, key=lambda importer: CANONICAL_POLICIES[importer][2])
    joins = " ".join(
        f"LEFT JOIN {contributions[importer]} {importer} ON {importer}.address_key=incumbent.address_key"
        for importer in selected
    )
    incumbent = ",".join(_canonical_projection_column(name, contributions, selected, "incumbent") for name in columns)
    statements = [f"SELECT {incumbent} FROM {archive} incumbent {joins}"]
    for index, importer in enumerate(selected):
        peers = [peer for peer in selected if peer != importer]
        joins = " ".join(
            f"LEFT JOIN {contributions[peer]} {peer} ON {peer}.address_key={importer}.address_key" for peer in peers
        )
        projection = ",".join(_canonical_projection_column(name, contributions, selected, importer) for name in columns)
        excluded_relations = [archive, *(contributions[earlier] for earlier in selected[:index])]
        condition = " AND ".join(
            f"NOT EXISTS(SELECT 1 FROM {relation} existing WHERE existing.address_key={importer}.address_key)"
            for relation in excluded_relations
        )
        statements.append(f"SELECT {projection} FROM {contributions[importer]} {importer} {joins} WHERE {condition}")
    return " UNION ALL ".join(statements)


def canonical_resolution_projection(importer_id, *, archive, deduplicated, strict, formatted, observed_at=None):
    """Project whole ordinary output, preserving strict raw keys and effective alias targets."""
    _table, bit, priority = CANONICAL_POLICIES[importer_id]
    changed = (
        f"(incoming.address_key IS NOT NULL AND ((incumbent.source_bits & {bit})=0 "
        f"OR {priority}<incumbent.display_priority))"
    )
    columns = [
        f"{_resolution_column(column.name, bit, priority, changed, observed_at=observed_at)} AS {column.name}"
        for column in AddressArchiveV2.__table__.columns
    ]
    return (
        "SELECT "
        + ",".join(columns)
        + f" FROM {archive} incumbent FULL JOIN {deduplicated} incoming USING(address_key) "
        f"LEFT JOIN {strict} strict ON strict.address_key=COALESCE(incumbent.address_key,incoming.address_key) "
        "AND strict.identity_key=COALESCE(incumbent.identity_key,incoming.identity_key) "
        f"LEFT JOIN {formatted} formatted ON formatted.address_key=COALESCE(incumbent.address_key,incoming.address_key)"
    )


def _resolution_column(name, bit, priority, changed, *, observed_at=None):
    """Mirror ordinary no-op timestamp, display priority and fixed source attribution semantics."""
    observed = "strict.address_key IS NOT NULL"
    timestamp = "transaction_timestamp()"
    if observed_at is not None:
        from process.npi_alias_witness import observation_time

        timestamp = "TIMESTAMPTZ '" + observation_time(observed_at).isoformat(timespec="microseconds") + "'"
    if name == "source_bits":
        return f"COALESCE(incumbent.source_bits,0) | CASE WHEN incoming.address_key IS NOT NULL OR {observed} THEN {bit} ELSE 0 END"
    if name == "strict_source_bits":
        return f"COALESCE(incumbent.strict_source_bits,0) | CASE WHEN {observed} THEN {bit} ELSE 0 END"
    if name == "last_seen_at":
        return (
            f"CASE WHEN incumbent.address_key IS NULL OR {changed} OR ({observed} AND "
            f"((incumbent.source_bits & {bit})=0 OR (incumbent.strict_source_bits & {bit})=0)) "
            f"THEN {timestamp} ELSE incumbent.last_seen_at END"
        )
    if name == "display_priority":
        return (
            f"CASE WHEN incumbent.address_key IS NULL THEN {priority} WHEN incoming.address_key IS NOT NULL "
            f"THEN least(incumbent.display_priority,{priority}) ELSE incumbent.display_priority END::smallint"
        )
    if name in DISPLAY_COLUMNS:
        if name.startswith("formatted_address"):
            fresh = {
                "formatted_address": "formatted.formatted_address",
                "formatted_address_version": f"{ADDRESS_FORMAT_VERSION}::smallint",
                "formatted_address_source": f"'{ADDRESS_FORMAT_SOURCE}'::varchar(32)",
            }[name]
            return f"CASE WHEN incumbent.address_key IS NULL OR {changed} THEN {fresh} ELSE incumbent.{name} END"
        return (
            f"CASE WHEN incumbent.address_key IS NULL OR (incoming.address_key IS NOT NULL "
            f"AND {priority}<incumbent.display_priority) THEN incoming.{name} ELSE incumbent.{name} END"
        )
    fresh = {
        "address_key": "incoming.address_key",
        "identity_key": "incoming.identity_key",
        "identity_version": f"{CURRENT_ADDRESS_IDENTITY_VERSION}::smallint",
        "precision": "CASE WHEN split_part(incoming.identity_key,'|',8)='city_zip' THEN 'city_zip' ELSE 'street' END",
        "unit_norm": "COALESCE(incoming.unit_norm,'')",
        "first_seen_at": timestamp,
    }.get(name, f"incoming.{name}" if name in IDENTITY_COLUMNS else "NULL")
    return f"CASE WHEN incumbent.address_key IS NULL THEN {fresh} ELSE incumbent.{name} END"


def _canonical_projection_column(name, contributions, selected, base):
    """Native expressions mirror fixed-bit attribution and incumbent display precedence."""
    if name == "last_seen_at":
        return _canonical_seen_projection(contributions, selected, base)
    if name in ("source_bits", "strict_source_bits"):
        bits = [f"{base}.{name}"] if base == "incumbent" else ["0"]
        bits.extend(
            f"CASE WHEN {importer}.address_key IS NULL OR NOT "
            f"{_direct_reference_predicate(importer, contributions[importer], importer + '.address_key')} THEN 0 ELSE "
            + (
                str(CANONICAL_POLICIES[importer][1])
                if name == "source_bits"
                else f"{importer}.strict_source_bits & {CANONICAL_POLICIES[importer][1]}"
            )
            + " END"
            for importer in selected
        )
        return "(" + " | ".join(bits) + f") AS {name}"
    if name == "display_priority":
        priorities = [f"{base}.display_priority"] if base == "incumbent" else ["9"]
        priorities.extend(
            f"CASE WHEN {importer}.address_key IS NULL OR NOT "
            f"{_direct_reference_predicate(importer, contributions[importer], importer + '.address_key')} "
            f"THEN 32767 ELSE {CANONICAL_POLICIES[importer][2]} END"
            for importer in selected
        )
        return "least(" + ",".join(priorities) + ")::smallint AS display_priority"
    if name in DISPLAY_COLUMNS:
        priority = (
            f"{base}.display_priority"
            if base == "incumbent"
            else (
                f"CASE WHEN {_direct_reference_predicate(base, contributions[base], base + '.address_key')} "
                f"THEN {CANONICAL_POLICIES[base][2]} ELSE 9 END"
            )
        )
        choices = " ".join(
            f"WHEN {importer}.address_key IS NOT NULL AND "
            f"{_direct_reference_predicate(importer, contributions[importer], importer + '.address_key')} "
            f"AND {CANONICAL_POLICIES[importer][2]}<({priority}) "
            f"THEN {importer}.{name}"
            for importer in selected
        )
        return f"CASE {choices} ELSE {base}.{name} END AS {name}"
    return f"{base}.{name}"


def _canonical_seen_projection(contributions, selected, base):
    """A same-transaction candidate keeps no-op timestamps and stamps only true merges."""
    if base != "incumbent":
        return "transaction_timestamp() AS last_seen_at"
    changes = []
    for importer in selected:
        _table, bit, priority = CANONICAL_POLICIES[importer]
        direct = _direct_reference_predicate(importer, contributions[importer], importer + ".address_key")
        changes.append(
            f"({importer}.address_key IS NOT NULL AND {direct} AND "
            f"((incumbent.source_bits & {bit}) <> {bit} OR "
            f"(incumbent.strict_source_bits | ({importer}.strict_source_bits & {bit})) "
            f"IS DISTINCT FROM incumbent.strict_source_bits OR {priority}<incumbent.display_priority))"
        )
    return (
        "CASE WHEN "
        + " OR ".join(changes)
        + " THEN transaction_timestamp() ELSE incumbent.last_seen_at END AS last_seen_at"
    )


def canonical_output_spec():
    """Describe the whole canonical output, never a contribution or importer enrollment."""
    from process.reference_family_archive import ReferenceFamilySpec

    return ReferenceFamilySpec(
        "address-canonical",
        (AddressArchiveV2,),
        relationships=((AddressArchiveV2, "merged_into", AddressArchiveV2, "address_key", False, True),),
    )


async def _require_canonical_output_layout(session, ownership):
    """Pin the declared whole-model heap and validate its current native layout."""
    from process import entity_address_native_publication as native
    from process import reference_family_archive as family

    family._require_transaction(session)
    relation_name = AddressArchiveV2.__tablename__
    await session.execute(
        text(f"LOCK TABLE {_qualified(ownership.schema_name, relation_name)} IN ACCESS SHARE MODE NOWAIT")
    )
    observed = await family._capture_model_family_ownership(session, canonical_output_spec(), ownership.dataset_id)
    if (
        observed.importer_id != ownership.importer_id
        or observed.schema_name != ownership.schema_name
        or observed.relation_names != ownership.relation_names
        or observed.sequence_names != ownership.sequence_names
    ):
        raise RuntimeError("canonical output inventory changed")
    await native._stage_inventory(session, ownership.schema_name, {relation_name: relation_name})
    relation_by_field = {"relation_name": relation_name, "schema_name": ownership.schema_name}
    await family._require_selected_mrf_catalog(session, [relation_by_field])
    enum_oid = await _canonical_enum_oid(session)
    relation_oid = dict(observed.relation_oids)[relation_name]
    await require_native_read_catalog(session, (relation_oid,), allowed_type_oids=(enum_oid,))
    return relation_oid


async def require_canonical_output_base(session, archive, evidence):
    """Validate the selected retained base by name and complete native model shape.

    The existing caller authenticates its logical generation and content before
    selecting this base. Legacy coordinate fields remain opaque retained data.
    """
    from process import reference_family_archive as family

    if not isinstance(evidence, dict) or evidence.get("contract") != "canonical-retained-base.v1":
        raise RuntimeError("canonical retained base authority differs")
    inventory = evidence.get("inventory")
    relations = inventory.get("relations") if isinstance(inventory, dict) else None
    if not isinstance(relations, list) or len(relations) != 1 or not isinstance(relations[0], dict):
        raise RuntimeError("canonical retained base inventory differs")
    relation_by_field = {name: relations[0].get(name) for name in ("schema_name", "relation_name")}
    if relation_by_field["relation_name"] != AddressArchiveV2.__tablename__ or archive != _qualified(
        relation_by_field["schema_name"], AddressArchiveV2.__tablename__
    ):
        raise RuntimeError("canonical retained base scope differs")
    ownership = await family._capture_model_family_ownership(
        session, canonical_output_spec(), UUID(evidence["dataset_id"])
    )
    if relation_by_field["schema_name"] != ownership.schema_name:
        raise RuntimeError("canonical retained base inventory differs")
    relation_oid = await _require_canonical_output_layout(session, ownership)
    if (
        await canonical_schema_identity(session, relation_oid, relation_by_field["schema_name"])
        != evidence["catalog_sha256"]
    ):
        raise RuntimeError("canonical retained base catalog differs")
    await _require_canonical_shape(session, relation_by_field["schema_name"], relation_oid, _qualified)


def _qualified(schema, name):
    """Use the existing native identifier compiler for locally derived model names."""
    from process.reference_family_archive import _quoted

    return f"{_quoted(schema)}.{_quoted(name)}"


async def canonical_merge_receipt(session, archive, stage, source_bit, priority):
    """Observe the exact five-field canonical merge result after native validation."""
    return {
        "contract": MERGE_CONTRACT,
        "source_bit": source_bit,
        "display_priority": priority,
        "archive_oid": await session.scalar(text("SELECT to_regclass(:archive)::oid"), {"archive": archive}),
        "contribution_oid": await session.scalar(text("SELECT to_regclass(:stage)::oid"), {"stage": stage}),
    }


async def merge_canonical_contribution(session, importer_id, stage, archive, *, npi_alias_authority=None):
    """One application-owned set merge in the actual final attachment transaction."""
    if npi_alias_authority is not None:
        from process.npi_alias_witness import merge_npi_alias_contribution

        return await merge_npi_alias_contribution(session, stage, archive, npi_alias_authority, importer_id=importer_id)
    _name, source_bit, priority = CANONICAL_POLICIES[importer_id]
    await session.execute(text(f"LOCK TABLE {archive} IN SHARE ROW EXCLUSIVE MODE NOWAIT"))
    await require_canonical_publication_capability(session, archive)
    await require_canonical_publication_catalog(session, archive)
    await validate_canonical_merge(session, stage, archive)
    direct = _direct_reference_predicate(importer_id, stage, "contribution.address_key")
    columns = tuple(column.name for column in AddressArchiveV2.__table__.columns)
    replacements_by_column = {
        "source_bits": f"CASE WHEN {direct} THEN {source_bit} ELSE 0 END",
        "strict_source_bits": f"CASE WHEN {direct} THEN contribution.strict_source_bits & {source_bit} ELSE 0 END",
        "display_priority": f"CASE WHEN {direct} THEN {priority} ELSE 9 END",
        "last_seen_at": "transaction_timestamp()",
    }
    projection = ",".join(replacements_by_column.get(name, f"contribution.{name}") for name in columns)
    await session.execute(
        text(
            f"INSERT INTO {archive} ({','.join(columns)}) SELECT {projection} FROM {stage} contribution "
            f"WHERE NOT EXISTS(SELECT 1 FROM {archive} incumbent WHERE incumbent.address_key=contribution.address_key)"
        )
    )
    display_updates = ",".join(
        f"{name}=CASE WHEN {priority}<incumbent.display_priority THEN contribution.{name} ELSE incumbent.{name} END"
        for name in DISPLAY_COLUMNS
    )
    await session.execute(
        text(
            f"UPDATE {archive} incumbent SET source_bits=incumbent.source_bits | {source_bit},"
            f"strict_source_bits=incumbent.strict_source_bits | (contribution.strict_source_bits & {source_bit}),"
            f"display_priority=least(incumbent.display_priority,{priority}),last_seen_at=transaction_timestamp(),"
            f"{display_updates} FROM {stage} contribution WHERE incumbent.address_key=contribution.address_key "
            f"AND {direct} "
            f"AND ((incumbent.source_bits & {source_bit}) <> {source_bit} "
            f"OR (incumbent.strict_source_bits | (contribution.strict_source_bits & {source_bit})) "
            f"IS DISTINCT FROM incumbent.strict_source_bits OR {priority}<incumbent.display_priority)"
        )
    )
    if (
        await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {stage} contribution WHERE NOT EXISTS(SELECT 1 FROM {archive} incumbent "
                f"WHERE incumbent.address_key=contribution.address_key AND (NOT {direct} OR "
                f"((incumbent.source_bits & {source_bit})={source_bit} "
                f"AND (incumbent.strict_source_bits & contribution.strict_source_bits & {source_bit})="
                f"(contribution.strict_source_bits & {source_bit})))))"
            )
        )
        is not False
    ):
        raise RuntimeError("canonical contribution publication differs")
    return await canonical_merge_receipt(session, archive, stage, source_bit, priority)


async def require_canonical_publication_capability(session, archive):
    """Require actual fixed-relation authority; existing alias maintenance grants are insufficient."""
    columns = (*DISPLAY_COLUMNS, "source_bits", "strict_source_bits", "display_priority", "last_seen_at")
    allowed = await session.scalar(
        text(
            "SELECT pg_catalog.has_table_privilege(current_user,CAST(:archive AS regclass),'SELECT') "
            "AND pg_catalog.has_table_privilege(current_user,CAST(:archive AS regclass),'INSERT') "
            "AND pg_catalog.has_table_privilege(current_user,CAST(:archive AS regclass),'MAINTAIN') "
            "AND NOT EXISTS(SELECT 1 FROM unnest(CAST(:columns AS text[])) name "
            "WHERE NOT pg_catalog.has_column_privilege(current_user,CAST(:archive AS regclass),name,'UPDATE'))"
        ),
        {"archive": archive, "columns": list(columns)},
    )
    if allowed is not True:
        raise RuntimeError("canonical publication capability is unavailable")


async def require_canonical_publication_catalog(session, archive):
    """The caller holds the exact canonical SREX fence before any payload query."""
    schema, name = archive.split(".", 1)
    if name != '"address_archive_v2"' or not schema.startswith('"') or not schema.endswith('"'):
        raise RuntimeError("canonical publication relation differs")
    schema = schema[1:-1]
    if not schema or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in schema):
        raise RuntimeError("canonical publication schema differs")
    await require_canonical_source_model(session, schema, lambda s, n: f'"{s}"."{n}"')


def referenced_address_filter(schema, qualified, *, key="row_value.address_key"):
    """Return the SQL predicate selecting canonical rows referenced by MRF tables."""

    return "WHERE " + " OR ".join(
        f"EXISTS (SELECT 1 FROM {qualified(schema, name)} AS source WHERE source.address_key={key})"
        for name in REFERENCES
    )


async def lock_publication_family(session, schema, qualified):
    """Hold the complete MRF serving family stable during publication capture."""

    names = ", ".join(qualified(schema, name) for name in RELATION_NAMES_BY_IMPORTER["mrf"])
    await session.execute(text(f"LOCK TABLE {names} IN SHARE MODE"))


async def capture_address_content(session, schema, qualified, *, native_set=False):
    """Caller holds family locks; pin only referenced archive content, not unrelated rows."""
    archive_name = archive_table_name()
    archive = qualified(schema, archive_name)
    archive_oid = await session.scalar(text("SELECT to_regclass(:name)::oid"), {"name": archive})
    if archive_oid is not None:
        # SHARE blocks all archive writers; use immutable contributions if capture contention becomes material.
        await session.execute(text(f"LOCK TABLE {archive} IN SHARE MODE"))
    address_content_map = {"archive_name": archive_name, "archive_oid": archive_oid, "tables": {}}
    if native_set:
        address_content_map["contract"] = ADDRESS_COVERAGE_CONTRACT
        address_content_map["tables"] = await _capture_raw_address_counts(
            session, schema, qualified, archive if archive_oid is not None else None
        )
        return address_content_map
    for name in REFERENCES:
        table = qualified(schema, name)
        if archive_oid is None:
            missing = await session.scalar(text(f"SELECT count(*) FROM {table}"))
            projection = "to_jsonb(row_value)"
        else:
            missing = await session.scalar(
                text(f"""
                SELECT count(*) FROM {table} AS source
                WHERE source.address_key IS NULL OR NOT EXISTS (
                    SELECT 1 FROM {archive} AS canonical
                    WHERE canonical.address_key=source.address_key
                      AND canonical.merged_into IS NULL
                      AND (canonical.source_bits & 16) = 16)
            """)
            )
            projection = f"""jsonb_build_array(to_jsonb(row_value),
                (SELECT to_jsonb(canonical) FROM {archive} AS canonical
                 WHERE canonical.address_key=row_value.address_key))"""
        count, digest = await _projected_row_identity(session, schema, name, row_json_sql=projection)
        address_content_map["tables"][name] = {"rows": count, "sha256": digest, "uncovered": int(missing)}
    return address_content_map


async def _count_unkeyable_null_addresses(session, table, expected_nulls):
    """Classify only NULL-key exceptions with trusted application code, never schema helpers."""
    from process.network_cms_registry_address_capture import native_driver

    driver = await native_driver(session)
    # Reuse the capture statement budget as an overall bound across cursor batches.
    milliseconds = await driver.fetchval(
        "SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name='statement_timeout'"
    )
    nulls = unkeyable = 0
    async with asyncio.timeout(milliseconds / 1000 if milliseconds else 5):
        async for row in driver.cursor(
            f"SELECT first_line,second_line,city_name,state_name,postal_code,country_code "
            f"FROM {table} WHERE address_key IS NULL",
            prefetch=512,
        ):
            nulls += 1
            unkeyable += address_key_v1(*row) is None
    if nulls != expected_nulls:
        raise RuntimeError("MRF raw address accounting changed")
    return unkeyable


async def _capture_raw_address_counts(session, schema, qualified, archive):
    """Partition every retained raw row; canonicalize only the stored NULL-key exceptions."""
    counts_by_table = {}
    for name in REFERENCES:
        table = qualified(schema, name)
        absent = (
            "NOT EXISTS (SELECT 1 FROM " + archive + " canonical "
            "WHERE canonical.address_key=source.address_key AND canonical.merged_into IS NULL "
            "AND (canonical.source_bits & 16)=16)"
            if archive is not None
            else "TRUE"
        )
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT count(*) AS rows,count(*) FILTER (WHERE address_key IS NULL) AS null_keys,"
                        f"count(*) FILTER (WHERE address_key IS NOT NULL AND {absent}) AS uncovered_nonnull "
                        f"FROM {table} source"
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        unkeyable = (
            await _count_unkeyable_null_addresses(session, table, counts["null_keys"]) if counts["null_keys"] else 0
        )
        counts_by_table[name] = {
            "rows": counts["rows"],
            "covered": counts["rows"] - counts["null_keys"] - counts["uncovered_nonnull"],
            "unkeyable_null": unkeyable,
            "keyable_null": counts["null_keys"] - unkeyable,
            "uncovered_nonnull": counts["uncovered_nonnull"],
        }
    return counts_by_table


def require_address_coverage(content):
    """Keep legacy coverage strict; v3 explicitly accounts for every retained raw row."""

    if content.get("contract") == ADDRESS_COVERAGE_CONTRACT:
        if (
            set(content) != {"contract", "archive_name", "archive_oid", "tables"}
            or content["archive_name"] != "address_archive_v2"
            or type(content["archive_oid"]) is not int
            or not 0 < content["archive_oid"] < 2**32
            or type(content["tables"]) is not dict
            or set(content["tables"]) != set(REFERENCES)
        ):
            raise RuntimeError("MRF publication address accounting is invalid")
        for counts in content["tables"].values():
            if (
                type(counts) is not dict
                or set(counts) != {"rows", "covered", "unkeyable_null", "keyable_null", "uncovered_nonnull"}
                or any(type(count) is not int or not 0 <= count < 2**63 for count in counts.values())
                or counts["rows"] != sum(count for name, count in counts.items() if name != "rows")
            ):
                raise RuntimeError("MRF publication address accounting is invalid")
            if counts["keyable_null"] or counts["uncovered_nonnull"]:
                raise RuntimeError("MRF publication address coverage is incomplete")
        return
    if content.get("contract") not in (None, "mrf-address-coverage.indexed-set.v2") or any(
        set(table) - {"rows", "sha256", "uncovered"} for table in content["tables"].values()
    ):
        raise RuntimeError("MRF publication address accounting is invalid")
    if content["archive_oid"] is None or any(table["uncovered"] for table in content["tables"].values()):
        raise RuntimeError("MRF publication address coverage is incomplete")
