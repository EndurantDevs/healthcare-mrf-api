# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named catalog candidates using existing model, COPY and set-validation primitives."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import text

from db.models import CodeCatalog, CodeRelationship, CodeSynonym
from process import reference_family_archive as native
from process import scoped_catalog_binding as binding
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.mrf_address_publication import _require_native_read_structure

MODELS = (CodeCatalog, CodeSynonym, CodeRelationship)


def qualified(schema, table):
    """Quote schema and relation names independently for native SQL."""
    return native._quoted(schema) + "." + native._quoted(table)


async def require_tables(session, schema, models, *, lock="ACCESS SHARE"):
    """Lock names and validate native heap/model structure without owner or ACL acceptance."""
    native._require_transaction(session)
    names = tuple(sorted(model.__tablename__ for model in models))
    await native._lock_family(session, schema, names, lock, nowait=True)
    pairs = tuple([(name, await native._relation_oid(session, schema, name)) for name in names])
    if any(type(oid) is not int or oid <= 0 for _, oid in pairs):
        raise RuntimeError("code-set catalog relation is unavailable")
    safe = await session.scalar(
        text(
            "SELECT count(*)=:count AND bool_and(NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) "
            "AND NOT EXISTS(SELECT FROM pg_catalog.pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) "
            "AND NOT EXISTS(SELECT FROM pg_catalog.pg_constraint WHERE conrelid=c.oid AND contype='f')) "
            "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:names AS text[]))"
        ),
        {"schema": schema, "names": list(names), "count": len(names)},
    )
    if safe is not True:
        raise RuntimeError("code-set catalog executable structure differs")
    await _require_native_read_structure(session, tuple(oid for _, oid in pairs), ())
    model_by_name = {model.__tablename__: model for model in models}
    for name, oid in pairs:
        await binding._require_model_columns(session, oid, model_by_name[name])
    return pairs


async def current_models(session, schema):
    """Require either the code-only catalog or the complete installed shared family."""
    present_list = [await native._relation_oid(session, schema, model.__tablename__) for model in MODELS[1:]]
    if any(present_list) and not all(present_list):
        raise RuntimeError("shared code-set family is incomplete")
    return MODELS if all(present_list) else MODELS[:1]


async def require_inventory(session, schema, names):
    """Private candidates contain exactly the compiled heap set and its native indexes."""
    actual_list = list(
        (
            await session.execute(
                text(
                    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relkind<>'i' ORDER BY c.relname"
                ),
                {"schema": schema},
            )
        ).scalars()
    )
    functions = await session.scalar(
        text(
            "SELECT EXISTS(SELECT FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema)"
        ),
        {"schema": schema},
    )
    if actual_list != sorted(names) or functions is not False:
        raise RuntimeError("catalog candidate object inventory differs")


async def create_stage(session, destination, models, dataset_id):
    """Create a unique candidate with the existing model DDL; namespace collisions refuse."""
    schema = native.reference_family_stage_schema(UUID(str(dataset_id)))
    await session.execute(text(f"CREATE SCHEMA {native._quoted(schema)}"))
    spec = native.ReferenceFamilySpec("scoped-catalog", models)
    await native._create_model_heaps(session, spec, schema, create_indexes=False, ordinary_heaps=True)
    await binding.match_catalog_text_columns(session, destination, schema, "code_catalog")
    return schema


async def complete_stage(session, schema, models):
    """Build native model indexes and constraints before accepting a candidate."""
    await native._create_model_indexes(
        session, native.ReferenceFamilySpec("scoped-catalog", models), schema, create_constraints=True
    )
    return await require_tables(session, schema, models, lock="ACCESS EXCLUSIVE")


async def table_receipts(session, schema, models):
    """Record data and logical model shape, never database-local relation identities."""
    pairs_by_field = dict(await require_tables(session, schema, models))
    receipts = []
    for model in models:
        name = model.__tablename__
        count, digest = await _projected_row_identity(
            session, schema, name, row_json_sql="pg_catalog.to_jsonb(row_value)"
        )
        receipts.append(
            {
                "table": name,
                "row_count": count,
                "row_sha256": digest,
                "schema_sha256": await native._family_schema_identity(
                    session, "scoped-catalog", pairs_by_field[name], schema, name
                ),
            }
        )
    return receipts


async def _acl_grantees(session, schema, names, *, namespace=False):
    """Enumerate native REVOKE destinations; this is no catalog acceptance or receipt."""
    if namespace:
        grant_source_sql = "pg_namespace object"
        acl = "object.nspacl"
        predicate = "object.nspname=:schema"
    else:
        grant_source_sql = "pg_class object JOIN pg_namespace home ON home.oid=object.relnamespace"
        acl = "object.relacl"
        predicate = "home.nspname=:schema AND object.relname=ANY(CAST(:names AS text[]))"
    grantees = (
        "SELECT grant_row.grantee FROM "
        + grant_source_sql
        + " CROSS JOIN LATERAL aclexplode("
        + acl
        + ") grant_row WHERE "
        + predicate
    )
    if not namespace:
        grantees += (
            " UNION SELECT grant_row.grantee FROM pg_attribute attribute JOIN pg_class object ON object.oid=attribute.attrelid "
            "JOIN pg_namespace home ON home.oid=object.relnamespace CROSS JOIN LATERAL aclexplode(attribute.attacl) grant_row "
            "WHERE home.nspname=:schema AND object.relname=ANY(CAST(:names AS text[]))"
        )
    statement = (
        "SELECT DISTINCT principal.rolname FROM ("
        + grantees
        + ") grants JOIN pg_roles principal ON principal.oid=grants.grantee ORDER BY 1 LIMIT 2049"
    )
    principals = list((await session.execute(text(statement), {"schema": schema, "names": list(names)})).scalars())
    if len(principals) > 2048:
        raise RuntimeError("code-set native privilege destinations exceed the bound")
    return principals


async def protect_tables(session, schema, models, settings, *, candidate=False, nowait=True):
    """Apply actual native ownership and read-only grants without comparing custody metadata."""
    names = tuple(model.__tablename__ for model in models)
    await native._lock_family(session, schema, tuple(sorted(names)), "ACCESS EXCLUSIVE", nowait=nowait)
    owner = native._quoted(settings.owner_role)
    if candidate:
        principals = await _acl_grantees(session, schema, (), namespace=True)
        await session.execute(text(f"ALTER SCHEMA {native._quoted(schema)} OWNER TO {owner}"))
        for principal in ("PUBLIC", *principals):
            if principal != settings.owner_role:
                await session.execute(
                    text(
                        f"REVOKE ALL ON SCHEMA {native._quoted(schema)} FROM {'PUBLIC' if principal == 'PUBLIC' else native._quoted(principal)}"
                    )
                )
    principals = await _acl_grantees(session, schema, names)
    for name in names:
        relation = qualified(schema, name)
        await session.execute(text(f"ALTER TABLE {relation} OWNER TO {owner}"))
        for principal in ("PUBLIC", *principals):
            if principal != settings.owner_role:
                await session.execute(
                    text(
                        f"REVOKE ALL ON TABLE {relation} FROM {'PUBLIC' if principal == 'PUBLIC' else native._quoted(principal)}"
                    )
                )
                columns = list(
                    (
                        await session.execute(
                            text(
                                "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(:relation) "
                                "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
                            ),
                            {"relation": relation},
                        )
                    ).scalars()
                )
                column_sql = ",".join(native._quoted(column) for column in columns)
                await session.execute(
                    text(
                        f"REVOKE ALL ({column_sql}) ON TABLE {relation} FROM {'PUBLIC' if principal == 'PUBLIC' else native._quoted(principal)}"
                    )
                )
        for reader in settings.reader_roles:
            await session.execute(text(f"GRANT SELECT ON TABLE {relation} TO {native._quoted(reader)}"))
    for reader in settings.reader_roles:
        await session.execute(text(f"GRANT USAGE ON SCHEMA {native._quoted(schema)} TO {native._quoted(reader)}"))
