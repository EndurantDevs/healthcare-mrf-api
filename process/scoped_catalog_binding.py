# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate current shared heaps separately from immutable source identities."""

from __future__ import annotations

from dataclasses import replace

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from db.models import CodeCatalog, CodeCrosswalk, CodeRelationship, CodeSynonym


def model_ordered_columns(columns, model):
    """Keep complete native column records in the same order as named COPY projections."""
    columns_by_name = {column[0]: column for column in columns}
    names = tuple(model.__table__.columns.keys())
    if len(columns_by_name) != len(columns) or set(columns_by_name) != set(names):
        raise ValueError("catalog model columns differ")
    return tuple(columns_by_name[name] for name in names)


def named_catalog_schema(columns, constraints, indexes):
    """Compare physical layouts by column names without relaxing constraint state."""
    from process.entity_address_snapshot_receipt import _canonical_digest
    from process.mrf_address_publication import normalized_canonical_catalog

    # The address helper's constraint rewrites do not apply to shared catalogs.
    normalized = normalized_canonical_catalog(columns, (), indexes)
    names_by_number = {column["attnum"]: column["attname"] for column in columns}
    normalized["constraints"] = sorted(
        (
            {
                **constraint,
                "key_columns": [
                    names_by_number[int(number)]
                    for number in (constraint["key_columns"] or "{}").strip("{}").split(",")
                    if number
                ],
            }
            for constraint in constraints
        ),
        key=_canonical_digest,
    )
    return normalized


async def lock_catalog_binding(session, schema, models, *, read_only=False):
    """Keep the complete fixed family and native execution boundary in this transaction."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    native._schema_name(schema)
    if models not in (
        (CodeCatalog,),
        (CodeCatalog, CodeSynonym, CodeRelationship),
        (CodeCatalog, CodeCrosswalk),
    ):
        raise native.ReferenceFamilyArchiveError("shared catalog family differs")
    spec = native.ReferenceFamilySpec("scoped-catalog", models)
    before = await native._incumbent_pairs(session, spec, schema)
    if any(type(oid) is not int or oid <= 0 for _, oid in before):
        raise native.ReferenceFamilyArchiveError("shared catalog family is incomplete")
    mode = "ACCESS SHARE" if read_only else "SHARE"
    await native._lock_family(session, schema, tuple(sorted(spec.table_names)), mode, nowait=True)
    if await native._incumbent_pairs(session, spec, schema) != before:
        raise native.ReferenceFamilyArchiveError("shared catalog family changed")
    await native.require_native_read_catalog(session, tuple(oid for _, oid in before))
    for model, (_name, oid) in zip(models, before, strict=True):
        await _require_model_columns(session, oid, model)
    if read_only:
        await require_closed_catalog_read_binding(session, schema, before)
    return before


async def pin_catalog_source(session, schema, models, generation_table):
    """Use read pins only for closed storage; mutable legacy sources keep writer locks."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    is_read_only = (
        await session.scalar(
            text(
                "SELECT serving.nspowner=protected.nspowner FROM pg_catalog.pg_namespace serving "
                "JOIN pg_catalog.pg_namespace protected ON protected.nspname='hp_snapshot_retention' "
                "WHERE serving.nspname=:schema"
            ),
            {"schema": schema},
        )
        is True
    )
    await lock_catalog_binding(session, schema, models, read_only=is_read_only)
    await _lock_generation(session, schema, generation_table, required=True, read_only=is_read_only)
    return is_read_only


async def require_closed_catalog_read_binding(session, schema, pairs):
    """Authenticate immutable storage without conferring publisher authority on its reader."""
    from types import SimpleNamespace

    from process import reference_family_archive as native
    from process.ptg_parts.ptg2_physical_binding import _require_closed_local_custody

    owner_oid = await session.scalar(
        text(
            "SELECT owner.oid FROM pg_catalog.pg_namespace namespace "
            "JOIN pg_catalog.pg_roles owner ON owner.oid=namespace.nspowner "
            "WHERE namespace.nspname='hp_snapshot_retention' AND NOT owner.rolcanlogin "
            "AND NOT owner.rolsuper AND NOT owner.rolcreaterole AND NOT owner.rolcreatedb "
            "AND NOT owner.rolreplication AND NOT owner.rolbypassrls"
        )
    )
    if type(owner_oid) is not int or owner_oid <= 0:
        raise native.ReferenceFamilyArchiveError("catalog protected read owner is unavailable")
    ownership = SimpleNamespace(
        schema_oid=await native._schema_oid(session, schema), relation_oids=pairs, sequence_oids=()
    )
    await _require_closed_local_custody(session, ownership, owner_oid)
    return owner_oid


async def _require_model_columns(session, oid, model):
    """Reject extra/generated/defaulted fields and require the complete native model key."""
    from process import reference_family_archive as native
    from process.ms_drg_result_generation import _table_shape

    columns = (
        await session.execute(
            text(
                "SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,"
                "a.attgenerated::text,a.attidentity::text,d.oid IS NOT NULL FROM pg_catalog.pg_attribute a "
                "LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                "WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
            ),
            {"oid": oid},
        )
    ).all()
    try:
        columns = model_ordered_columns(columns, model)
    except ValueError as error:
        raise native.ReferenceFamilyArchiveError("shared catalog model columns differ") from error
    for observed, column in zip(columns, model.__table__.columns, strict=True):
        name, type_name, not_null, generated, identity, has_default = observed
        declared = str(column.type.compile(dialect=postgresql.dialect())).upper().replace(" ", "")
        actual = type_name.upper().replace("CHARACTER VARYING", "VARCHAR").replace(" ", "")
        is_normalized_text = model is CodeCatalog and name in {"display_name", "short_description"} and actual == "TEXT"
        if (
            name != column.name
            or actual != declared
            and not is_normalized_text
            or not_null is not (not column.nullable)
            or generated
            or identity
            or has_default
        ):
            raise native.ReferenceFamilyArchiveError("shared catalog model columns differ")
    await _table_shape(session, oid, model)


async def match_catalog_text_columns(session, source_schema, target_schema, target_name):
    """Preserve the ordinary importer's reviewed TEXT normalization in isolated heaps."""
    from process import reference_family_archive as native

    native._require_transaction(session)
    for name in (source_schema, target_schema, target_name):
        native._schema_name(name)
    if source_schema == target_schema or target_name not in {"code_catalog", "code_catalog_predecessor"}:
        raise native.ReferenceFamilyArchiveError("catalog model clone scope differs")
    await native._lock_family(session, source_schema, ("code_catalog",), "ACCESS SHARE")
    oid = await native._relation_oid(session, source_schema, "code_catalog")
    if type(oid) is not int or oid <= 0:
        raise native.ReferenceFamilyArchiveError("catalog source relation is unavailable")
    await _require_model_columns(session, oid, CodeCatalog)
    columns = (
        (
            await session.execute(
                text(
                    "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid=:oid AND attname IN ('display_name','short_description') AND atttypid='pg_catalog.text'::regtype AND NOT attisdropped ORDER BY attname"
                ),
                {"oid": oid},
            )
        )
        .scalars()
        .all()
    )
    for name in columns:
        await session.execute(
            text(
                f"ALTER TABLE {native._quoted(target_schema)}.{native._quoted(target_name)} ALTER COLUMN {native._quoted(name)} TYPE TEXT"
            )
        )


def same_code_sets_origin(observed, installed):
    """Compare logical identity only; callers must separately authenticate the live binding."""
    from process.code_sets_result_archive import CodeSetsGeneration

    return (
        isinstance(observed, CodeSetsGeneration)
        and isinstance(installed, CodeSetsGeneration)
        and observed.origin_lineage_id is not None
        and observed.local_generation >= installed.local_generation
        and replace(observed, local_generation=installed.local_generation, code_catalog_oid=installed.code_catalog_oid)
        == installed
    )


def same_ms_drg_origin(observed, installed):
    """Keep complete source/schema receipts while separating only their physical OIDs."""
    from process.ms_drg_result_archive import _content

    fields = ("local_lineage_id", "origin_lineage_id", "origin_generation", "published_at", "include_relationships")
    return (
        observed["origin_lineage_id"] is not None
        and observed["local_generation"] >= installed["local_generation"]
        and all(observed[key] == installed[key] for key in fields)
        and observed["receipt"]["contract"] == installed["receipt"]["contract"]
        and observed["receipt"]["content_sha256"] == installed["receipt"]["content_sha256"]
        and _content(observed["receipt"]) == _content(installed["receipt"])
    )


async def require_code_sets_binding(session, schema, installed, *, schema_sha256):
    """Resolve an immutable installation against the authenticated current physical receipt."""
    from process import code_sets_result_archive as codes

    pairs = await lock_catalog_binding(session, schema, (CodeCatalog,))
    generation_oid = await _lock_generation(session, schema, codes.TABLE, required=True)
    observed = await codes.read_generation(session, schema)
    if (
        not same_code_sets_origin(observed, installed)
        or await codes.scope_receipt(session, schema)
        != (observed.row_count, observed.row_sha256, observed.code_catalog_oid)
        or observed.code_catalog_oid != pairs[0][1]
        or codes._schema_digest(await codes._column_signature(session, observed.code_catalog_oid)) != schema_sha256
    ):
        raise codes.CodeSetsArchiveError("code-set current binding differs")
    if observed.code_catalog_oid != installed.code_catalog_oid:
        await require_closed_catalog_binding(session, schema, pairs + ((codes.TABLE, generation_oid),))
    return observed


async def require_ms_drg_binding(session, schema, installed):
    """Resolve all three immutable source receipts against one locked current family."""
    from process.ms_drg_result_generation import TABLE, read_current_generation

    pairs = await lock_catalog_binding(session, schema, (CodeCatalog, CodeSynonym, CodeRelationship))
    generation_oid = await _lock_generation(session, schema, TABLE, required=True)
    observed = await read_current_generation(session, schema)
    if (
        not same_ms_drg_origin(observed, installed)
        or tuple((receipt["table"], receipt["relation_oid"]) for receipt in observed["receipt"]["tables"]) != pairs
    ):
        raise RuntimeError("MS-DRG current binding differs")
    if observed["receipt"] != installed["receipt"]:
        await require_closed_catalog_binding(session, schema, pairs + ((TABLE, generation_oid),))
    return observed


async def require_closed_catalog_binding(session, schema, pairs):
    """A relocated heap needs protected native custody, not just matching source rows."""
    from types import SimpleNamespace

    from process import reference_family_archive as native
    from process.ptg_parts.ptg2_physical_binding import _require_closed_local_custody

    owner = await native.protected_publisher_owner(session)
    ownership = SimpleNamespace(
        schema_oid=await native._schema_oid(session, schema), relation_oids=pairs, sequence_oids=()
    )
    await _require_closed_local_custody(session, ownership, owner)
    return owner


async def _lock_generation(session, schema, table, *, required=False, read_only=False):
    """Authenticate native control storage before executing a generation query."""
    from process import reference_family_archive as native

    oid = await native._relation_oid(session, schema, table)
    if oid is None and not required:
        return None
    if type(oid) is not int or oid <= 0:
        raise native.ReferenceFamilyArchiveError("shared catalog generation is unavailable")
    await native._lock_family(session, schema, (table,), "ACCESS SHARE" if read_only else "SHARE", nowait=True)
    if await native._relation_oid(session, schema, table) != oid:
        raise native.ReferenceFamilyArchiveError("shared catalog generation identity changed")
    await native.require_native_read_catalog(session, (oid,))
    if read_only:
        await require_closed_catalog_read_binding(session, schema, ((table, oid),))
    return oid


async def require_current_catalog_rebinding(session, schema, catalog_oid):
    """Only a reverified native source publication can supersede an effect's catalog OID.

    This does not admit crosswalk replacement or relax any key/image check. The
    caller must already authenticate the frozen effect family and destination.
    """
    from process import code_sets_result_archive as codes
    from process import reference_family_archive as native
    from process.ms_drg_result_generation import TABLE as drg_table
    from process.ms_drg_result_generation import read_current_generation

    pairs = await lock_catalog_binding(session, schema, (CodeCatalog, CodeCrosswalk))
    if pairs[0][1] != catalog_oid:
        raise native.ReferenceFamilyArchiveError("shared catalog current identity changed")
    await require_closed_catalog_binding(session, schema, pairs)
    has_current_authority = False
    for table in (codes.TABLE, drg_table):
        oid = await _lock_generation(session, schema, table)
        if oid is None:
            continue
        active = await session.scalar(
            text(
                f"SELECT origin_generation IS NOT NULL FROM {native._quoted(schema)}.{native._quoted(table)} WHERE id=1"
            )
        )
        if active is not True:
            continue
        await require_closed_catalog_binding(session, schema, pairs + ((table, oid),))
        if table == codes.TABLE:
            observed = await codes.read_generation(session, schema)
            current = await codes.scope_receipt(session, schema)
            is_valid_binding = current == (observed.row_count, observed.row_sha256, observed.code_catalog_oid)
            is_valid_binding = is_valid_binding and observed.code_catalog_oid == catalog_oid
        else:
            drg_pairs = await lock_catalog_binding(session, schema, (CodeCatalog, CodeSynonym, CodeRelationship))
            await require_closed_catalog_binding(session, schema, drg_pairs + ((table, oid),))
            observed = await read_current_generation(session, schema)
            is_valid_binding = observed["receipt"]["tables"][0]["relation_oid"] == catalog_oid
        if not is_valid_binding:
            raise native.ReferenceFamilyArchiveError("shared catalog generation content changed")
        has_current_authority = True
    if not has_current_authority:
        raise native.ReferenceFamilyArchiveError("shared catalog replacement has no current source authority")


async def rebind_code_sets_generation(session, schema, expected):
    """Rebind unchanged source rows after a verified caller-held swap, without advancing origin."""
    from process import code_sets_result_archive as codes

    pairs = await lock_catalog_binding(session, schema, (CodeCatalog,))
    await _lock_generation(session, schema, codes.TABLE, required=True)
    observed = await codes.read_generation(session, schema, lock=True)
    if observed != expected:
        raise codes.CodeSetsArchiveError("code-set generation changed before rebind")
    if expected.origin_lineage_id is None:
        return observed
    await require_closed_catalog_binding(session, schema, pairs)
    count, digest, oid = await codes.scope_receipt(session, schema)
    if (count, digest, oid) != (expected.row_count, expected.row_sha256, pairs[0][1]):
        raise codes.CodeSetsArchiveError("code-set source changed during rebind")
    await session.execute(
        text(f"UPDATE {codes._schema(schema)}.{codes.TABLE} SET code_catalog_oid=:oid WHERE id=1"), {"oid": oid}
    )
    rebound = await codes.read_generation(session, schema)
    if rebound != replace(expected, code_catalog_oid=oid):
        raise codes.CodeSetsArchiveError("code-set rebound generation differs")
    return rebound


async def rebind_ms_drg_generation(session, schema, expected):
    """Update only physical receipt fields for exactly unchanged MS-DRG source content."""
    import json

    from process import ms_drg_result_archive as drg
    from process.ms_drg_result_generation import TABLE, _quoted, capture_result, read_current_generation

    pairs = await lock_catalog_binding(session, schema, (CodeCatalog, CodeSynonym, CodeRelationship))
    await _lock_generation(session, schema, TABLE, required=True)
    generation_row = (
        (
            await session.execute(
                text(
                    f"SELECT local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,include_relationships,receipt FROM {_quoted(schema)}.{TABLE} WHERE id=1 FOR UPDATE"
                )
            )
        )
        .mappings()
        .one()
    )
    if dict(generation_row) != expected:
        raise RuntimeError("MS-DRG generation changed before rebind")
    if expected["origin_lineage_id"] is None:
        return dict(generation_row)
    await require_closed_catalog_binding(session, schema, pairs)
    current = await capture_result(session, schema)
    if (
        drg._content(current) != drg._content(expected["receipt"])
        or current["content_sha256"] != expected["receipt"]["content_sha256"]
    ):
        raise RuntimeError("MS-DRG source changed during rebind")
    await session.execute(
        text(f"UPDATE {_quoted(schema)}.{TABLE} SET receipt=CAST(:receipt AS jsonb) WHERE id=1"),
        {"receipt": json.dumps(current, sort_keys=True, separators=(",", ":"))},
    )
    rebound = await read_current_generation(session, schema)
    if rebound != {**expected, "receipt": current}:
        raise RuntimeError("MS-DRG rebound generation differs")
    return rebound
