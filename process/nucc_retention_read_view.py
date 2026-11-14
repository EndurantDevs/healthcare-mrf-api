# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read all retained NUCC claims using their recorded schema-qualified names."""

from sqlalchemy import text

from db.plan_release_storage import storage_identifier

VIEW_NAME = "nucc_retained_relation_claims"
VIEW = '"mrf"."' + VIEW_NAME + '"'
CLAIMS_SQL = """SELECT relation.schema_name,relation.relation_name
FROM hp_snapshot_retention.relation relation
UNION ALL
SELECT preparation_relation.schema_name,preparation_relation.relation_name
FROM hp_snapshot_retention.preparation_relation preparation_relation
UNION ALL
SELECT preparation.stage_schema AS schema_name,relation.table_name AS relation_name
FROM hp_snapshot_retention.reference_preparation_relation relation
JOIN hp_snapshot_retention.reference_preparation preparation USING(operation_id)
UNION ALL
SELECT entry->>'schema_name' AS schema_name,entry->>'relation_name' AS relation_name
FROM hp_snapshot_retention.legacy_archive archive
CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
 CASE WHEN pg_catalog.jsonb_typeof(archive.receipt#>'{inventory,relations}')='array'
      THEN CASE WHEN pg_catalog.jsonb_array_length(archive.receipt#>'{inventory,relations}')>0
                THEN archive.receipt#>'{inventory,relations}' ELSE '[null]'::jsonb END
      ELSE '[null]'::jsonb END) entry"""
CREATE_VIEW_SQL = f"CREATE VIEW {VIEW} WITH (security_barrier=true,security_invoker=false) AS {CLAIMS_SQL}"


class NuccRetentionReadError(RuntimeError):
    """A retained relation is claimed or its logical claim state is incomplete."""


def _require(condition, code):
    if not condition:
        raise NuccRetentionReadError("NUCC retention " + code)


async def require_unretained_nucc_relations(session, *, schema_name, relation_names):
    """Read fresh all-state claims after the caller locks its named rotation targets."""
    _require(session.in_transaction(), "transaction required")
    _require(
        type(relation_names) in (list, tuple)
        and 1 <= len(relation_names) <= 2
        and all(type(name) is str for name in relation_names),
        "relation scope differs",
    )
    for name in (schema_name, *relation_names):
        try:
            storage_identifier(name)
        except (TypeError, ValueError) as error:
            raise NuccRetentionReadError("NUCC retention relation scope differs") from error
    _require(
        await session.scalar(text("SHOW transaction_isolation")) == "read committed",
        "fresh claim read required",
    )
    installed = await session.scalar(text("SELECT pg_catalog.to_regnamespace('hp_snapshot_retention') IS NOT NULL"))
    if installed is False:
        return
    _require(installed is True, "catalogue state is uncertain")
    claimed = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT FROM {VIEW} WHERE schema_name IS NULL OR relation_name IS NULL "
            "OR schema_name='' OR relation_name='' "
            "OR (schema_name=:schema AND relation_name=ANY(CAST(:relations AS pg_catalog.text[]))))"
        ),
        {"schema": schema_name, "relations": sorted(set(relation_names))},
    )
    _require(claimed is False, "retained claim exists or is uncertain")


async def require_nucc_unretained_rotation(session, *, schema_name):
    """Legacy rotation cannot erase a named retained serving or predecessor table."""
    from process import reference_family_archive as archive

    archive._require_transaction(session)
    schema = archive._schema_name(schema_name)
    if await archive.is_nucc_native_handoff_required(session, schema_name=schema):
        raise archive.ReferenceFamilyArchiveError("NUCC protected custody requires publisher rotation")
    relation_names = []
    for table in (archive.models.NUCCTaxonomy.__tablename__, archive.models.NUCCTaxonomy.__tablename__ + "_old"):
        exists = await session.scalar(
            text("SELECT pg_catalog.to_regclass(:relation) IS NOT NULL"),
            {"relation": f"{archive._quoted(schema)}.{archive._quoted(table)}"},
        )
        if exists is False:
            continue
        if exists is not True:
            raise archive.ReferenceFamilyArchiveError("NUCC rotation relation lookup is uncertain")
        await session.execute(
            text(f"LOCK TABLE ONLY {archive._quoted(schema)}.{archive._quoted(table)} IN ACCESS EXCLUSIVE MODE NOWAIT")
        )
        relation_names.append(table)
    if not relation_names:
        return
    try:
        await require_unretained_nucc_relations(session, schema_name=schema, relation_names=relation_names)
    except NuccRetentionReadError as error:
        raise archive.ReferenceFamilyArchiveError(str(error)) from error
