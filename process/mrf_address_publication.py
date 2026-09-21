# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local MRF address coverage and immutable content receipt."""

from sqlalchemy import text

from process.entity_address_snapshot_receipt import _projected_row_identity
from process.ext.address_canon import archive_table_name
from process.reference_family_result_generation import RELATION_NAMES_BY_IMPORTER

STAGE_TABLE = "mrf_canonical_address"
REFERENCES = ("mrf_address", "mrf_address_evidence")


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


async def capture_address_content(session, schema, qualified):
    """Caller holds family locks; pin only referenced archive content, not unrelated rows."""
    archive_name = archive_table_name()
    archive = qualified(schema, archive_name)
    archive_oid = await session.scalar(text("SELECT to_regclass(:name)::oid"), {"name": archive})
    if archive_oid is not None:
        # SHARE blocks all archive writers; use immutable contributions if capture contention becomes material.
        await session.execute(text(f"LOCK TABLE {archive} IN SHARE MODE"))
    address_content_map = {"archive_name": archive_name, "archive_oid": archive_oid, "tables": {}}
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
        count, digest = await _projected_row_identity(
            session,
            schema,
            name,
            row_json_sql=projection,
        )
        address_content_map["tables"][name] = {"rows": count, "sha256": digest, "uncovered": int(missing)}
    return address_content_map


def require_address_coverage(content):
    """Reject publications without a live canonical row for every MRF address."""

    if content["archive_oid"] is None or any(table["uncovered"] for table in content["tables"].values()):
        raise RuntimeError("MRF publication address coverage is incomplete")
