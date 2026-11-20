# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based checks, publication and cleanup for one typed CMS NPD release schema.

The release table keeps exactly one ``current`` and at most one ``previous`` row; publication moves
both pointers in one transaction, and the schema that falls out is dropped only after commit.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re

from process.cms_npd_typed_schema import (
    FINISH_STATEMENTS,
    RELEASE_TABLE,
    RESOURCE_FILES,
    TABLE_STATEMENTS,
    release_table_statements,
)
from process.cms_npd_typed_stage import CmsNpdTypedImportError

MAX_ROW_DROP = 0.20  # fail when a table shrinks by more than this fraction against the current release
MAINTENANCE_WORK_MEM = "1GB"
MAINTENANCE_WORKERS = 4
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")
RESOURCE_TABLES = tuple(main_table for _, _, main_table, _ in RESOURCE_FILES)

# Unresolved references, counted (not failed) on the closed candidate.
UNRESOLVED_QUERIES = {
    "role.practitioner": """SELECT count(*) FROM practitioner_role r WHERE r.practitioner_npi IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM practitioner p WHERE p.npi = r.practitioner_npi)""",
    "role.organization": """SELECT count(*) FROM practitioner_role r WHERE r.organization_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM organization o WHERE o.id = r.organization_id)""",
    "role.location": """SELECT count(*) FROM (SELECT unnest(location_ids) AS id FROM practitioner_role) r
        WHERE NOT EXISTS (SELECT 1 FROM location l WHERE l.id = r.id)""",
    "role.endpoint": """SELECT count(*) FROM (SELECT unnest(endpoint_ids) AS id FROM practitioner_role) r
        WHERE NOT EXISTS (SELECT 1 FROM endpoint e WHERE e.id = r.id)""",
    "location.organization": """SELECT count(*) FROM location l WHERE l.organization_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM organization o WHERE o.id = l.organization_id)""",
    "endpoint.organization": """SELECT count(*) FROM endpoint e WHERE e.organization_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM organization o WHERE o.id = e.organization_id)""",
    "affiliation.participant": """SELECT count(*) FROM organization_affiliation a
        WHERE NOT EXISTS (SELECT 1 FROM organization o WHERE o.id = a.participating_organization_id)""",
    "affiliation.organization": """SELECT count(*) FROM organization_affiliation a
        WHERE NOT EXISTS (SELECT 1 FROM organization o WHERE o.id = a.organization_id)""",
    "organization.part_of": """SELECT count(*) FROM organization c WHERE c.part_of IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM organization o WHERE o.id = c.part_of)""",
}


# The same grantee read as the geo search projection; a NULL rolname is PUBLIC.
_ADDRESS_READERS = """SELECT DISTINCT r.rolname
     FROM pg_class AS c
    CROSS JOIN LATERAL aclexplode(c.relacl) AS a
     LEFT JOIN pg_roles AS r ON r.oid = a.grantee
    WHERE c.oid = to_regclass($1) AND a.privilege_type = 'SELECT' AND a.grantee <> c.relowner
    ORDER BY 1"""


def quoted(identifier):
    """A validated, double-quoted SQL identifier."""
    if not _IDENTIFIER.fullmatch(identifier):
        raise CmsNpdTypedImportError("cms_npd_typed_identifier_invalid")
    return f'"{identifier}"'


def control_schema():
    """The schema holding the release table; the same setting the API readers use."""
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


async def ensure_release_table(connection, control):
    """Create the release table where the deployment has not migrated it yet; the API reader may read it."""
    for statement in release_table_statements(control):
        await connection.execute(statement)
    if readers := await api_readers(connection, control):
        await connection.execute(f"GRANT SELECT ON {quoted(control)}.{RELEASE_TABLE} TO {', '.join(readers)}")


async def api_readers(connection, control):
    """Quoted SELECT grantees of the address table the API already reads, plus ``HLTHPRT_DB_READER_USER``."""
    rows = await connection.fetch(_ADDRESS_READERS, f"{quoted(control)}.entity_address_unified")
    names = [grant["rolname"] for grant in rows]
    # Catalog role names are trusted but may be any identifier; only the configured name is validated.
    readers = ["PUBLIC" if name is None else '"' + name.replace('"', '""') + '"' for name in names]
    configured = os.getenv("HLTHPRT_DB_READER_USER", "").strip()
    if configured and configured not in names:
        readers.append(quoted(configured))
    return readers


async def release_rows(connection, control):
    """Recorded releases keyed by status ("current", "previous")."""
    rows = await connection.fetch(
        f"SELECT release_id, schema_name, status, row_counts FROM {quoted(control)}.{RELEASE_TABLE}"
    )
    return {row["status"]: {**dict(row), "row_counts": json.loads(row["row_counts"])} for row in rows}


async def create_release_schema(connection, schema):
    """(Re)create the empty candidate schema and its tables before COPY."""
    async with connection.transaction():
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted(schema)} CASCADE")
        await connection.execute(f"CREATE SCHEMA {quoted(schema)}")
        await connection.execute(f"SET LOCAL search_path TO {quoted(schema)}")
        for statement in TABLE_STATEMENTS:
            await connection.execute(statement)


async def finish_release_schema(connection, schema):
    """Keys, indexes, the derived network table and statistics, with session-local maintenance memory."""
    async with connection.transaction():
        await connection.execute(f"SET LOCAL search_path TO {quoted(schema)}")
        await connection.execute(f"SET LOCAL maintenance_work_mem = '{MAINTENANCE_WORK_MEM}'")
        await connection.execute(f"SET LOCAL max_parallel_maintenance_workers = {MAINTENANCE_WORKERS}")
        for statement in FINISH_STATEMENTS:
            await connection.execute(statement)


async def unresolved_counts(connection, schema):
    """Count each kind of unresolved reference, and the derived networks, on the closed candidate."""
    async with connection.transaction(readonly=True):
        await connection.execute(f"SET LOCAL search_path TO {quoted(schema)}")
        counts_by_name = {name: await connection.fetchval(sql) for name, sql in UNRESOLVED_QUERIES.items()}
        network_rows = await connection.fetchval("SELECT count(*) FROM network")
    return counts_by_name, network_rows


def empty_tables(row_counts):
    """Resource tables without any row."""
    return [table for table in RESOURCE_TABLES if not row_counts.get(table)]


def shrunk_tables(row_counts, current_counts, max_drop=MAX_ROW_DROP):
    """Tables whose rows fell by more than ``max_drop`` against the current release."""
    return sorted(
        table
        for table, before in (current_counts or {}).items()
        # Only source tables: the derived network set is small and may change freely.
        if table in RESOURCE_TABLES and row_counts.get(table, 0) < before * (1 - max_drop)
    )


def assert_publishable(row_counts, current):
    """Refuse an empty resource table or a large shrink against the current release."""
    if empty := empty_tables(row_counts):
        raise CmsNpdTypedImportError("cms_npd_typed_table_empty", ",".join(empty))
    if current is not None and (shrunk := shrunk_tables(row_counts, current["row_counts"])):
        raise CmsNpdTypedImportError("cms_npd_typed_table_shrunk", ",".join(shrunk))


async def grant_api_read(connection, control, schema):
    """Read-only access for the API readers (see ``api_readers``); idempotent, so it also repairs a release.

    The importer itself owns the schema; no other role gets any privilege on it.
    """
    if not (readers := await api_readers(connection, control)):
        return
    await connection.execute(f"GRANT USAGE ON SCHEMA {quoted(schema)} TO {', '.join(readers)}")
    await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {quoted(schema)} TO {', '.join(readers)}")


async def publish(connection, control, release):
    """Move ``current`` to ``previous`` and record the new release in one transaction.

    Returns the release that fell out (the old ``previous``), whose schema the caller drops after commit.
    """
    table = f"{quoted(control)}.{RELEASE_TABLE}"
    async with connection.transaction():
        dropped = await connection.fetchrow(
            f"DELETE FROM {table} WHERE status = 'previous' RETURNING release_id, schema_name"
        )
        await connection.execute(f"UPDATE {table} SET status = 'previous' WHERE status = 'current'")
        await connection.execute(
            f"""INSERT INTO {table} (release_id, schema_name, status, generated_on, published_at, run_id,
                row_counts, unresolved_counts, files)
            VALUES ($1, $2, 'current', $3, now(), $4, $5::jsonb, $6::jsonb, $7::jsonb)""",
            release["release_id"],
            release["schema"],
            dt.date.fromisoformat(release["generated_at"]),
            release["run_id"],
            json.dumps(release["row_counts"], sort_keys=True),
            json.dumps(release["unresolved_counts"], sort_keys=True),
            json.dumps(release["files"], sort_keys=True),
        )
    return None if dropped is None else dict(dropped)


async def drop_schema(connection, schema):
    """Drop one release schema and everything in it."""
    await connection.execute(f"DROP SCHEMA IF EXISTS {quoted(schema)} CASCADE")
