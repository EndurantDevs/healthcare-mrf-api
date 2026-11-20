# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One fixed-name provider address table built from the current typed CMS NPD release.

stage (set-based, one row per NPI + normalized address) -> address keys and archive -> indexes -> swap.
The swap renames the stage into place in one transaction, keeps one ``_old`` copy, and records the row
count on the release row; a current release without that count (or without the table) still owes its
addresses. A second key records that the entity-address refresh for this release was admitted; until it is
set, every run retries only the enqueue.
"""

from __future__ import annotations

from process.cms_npd_typed_release import api_readers, quoted
from process.cms_npd_typed_schema import RELEASE_TABLE

ADDRESS_TABLE = "cms_npd_address"
STAGE_TABLE = ADDRESS_TABLE + "_stage"
OLD_TABLE = ADDRESS_TABLE + "_old"
ROW_COUNT_KEY = "provider_address"  # key on the release row's row_counts once the table is built
BUILD_KEY = "provider_address_build"  # BUILD_VERSION the table was built with, recorded next to the count
BUILD_VERSION = 2  # bump when the staged rows change shape or content: an older table is rebuilt once
REFRESH_KEY = "provider_address_refresh"  # set to 1 once the refresh run for this release was admitted
ARCHIVE_SOURCE = "cms_npd"  # name in HLTHPRT_ADDRESS_CANON_SOURCES
ARCHIVE_SOURCE_BIT = 256
ARCHIVE_PRIORITY = 7
SWAP_LOCK_TIMEOUT = "60s"  # fail (and retry next run) rather than queue readers behind a long reader
ADDRESS_FIELDS = {
    "first_line": "first_line",
    "second_line": "second_line",
    "city": "city_name",
    "state": "state_name",
    "zip": "postal_code",
    "country": "country_code",
}
_INDEX_SUFFIXES = ("pkey", "address_key")

CREATE_STAGE = """CREATE TABLE {stage} (
    npi              bigint NOT NULL,
    source_kind      text NOT NULL,       -- practitioner | role_location | organization
    first_line       text NOT NULL,
    second_line      text,
    city_name        text,
    state_name       text,
    postal_code      text,
    country_code     text NOT NULL,
    telephone_number text,
    fax_number       text,
    latitude         double precision,
    longitude        double precision,
    updated_at       timestamptz,
    source_record_id text NOT NULL,       -- cms_npd:<npi>:<md5 of the normalized address>
    address_key      uuid
)"""

# Work/physical addresses of practitioners and NPI organizations, and the locations of roles that are
# not inactive or ended. A practitioner's or organization's phone and fax belong to the provider, not to an
# address, so they are kept only when that NPI has exactly one such address (blank streets do not count).
_CANDIDATES = """SELECT a.npi, 'practitioner' AS source_kind, 1 AS kind_rank, a.line1, a.line2, a.city, a.state,
       a.postal_code, a.country,
       CASE WHEN count(*) OVER (PARTITION BY a.npi) = 1 THEN p.phones[1] END AS phone,
       CASE WHEN count(*) OVER (PARTITION BY a.npi) = 1 THEN p.faxes[1] END AS fax,
       NULL::float8 AS latitude, NULL::float8 AS longitude, p.updated_at
FROM {release}.practitioner_address a JOIN {release}.practitioner p ON p.npi = a.npi
WHERE a.use = 'work' AND a.type IN ('physical', 'both') AND btrim(coalesce(a.line1, '')) <> ''
UNION ALL
SELECT r.practitioner_npi, 'role_location', 0, l.line1, l.line2, l.city, l.state, l.postal_code, l.country,
       l.phones[1], l.faxes[1], l.latitude, l.longitude, l.updated_at
FROM {release}.practitioner_role r CROSS JOIN LATERAL unnest(r.location_ids) AS role_location(id)
JOIN {release}.location l ON l.id = role_location.id
WHERE r.practitioner_npi IS NOT NULL AND r.active IS NOT FALSE
  AND (r.period_end IS NULL OR r.period_end >= CURRENT_DATE)
UNION ALL
SELECT o.npi, 'organization', 2, a.line1, a.line2, a.city, a.state, a.postal_code, a.country,
       CASE WHEN count(*) OVER (PARTITION BY o.npi) = 1 THEN o.phones[1] END,
       CASE WHEN count(*) OVER (PARTITION BY o.npi) = 1 THEN o.faxes[1] END, NULL, NULL, o.updated_at
FROM {release}.organization_address a JOIN {release}.organization o ON o.id = a.organization_id
WHERE o.npi IS NOT NULL AND a.use = 'work' AND a.type IN ('physical', 'both')
  AND btrim(coalesce(a.line1, '')) <> ''"""


def _norm(column):
    return f"btrim(regexp_replace(lower(coalesce({column}, '')), '[^a-z0-9]+', ' ', 'g'))"


COUNTRY_CODE = """CASE WHEN upper(btrim(coalesce(c.country, ''))) IN ('', 'US', 'USA', 'UNITED STATES') THEN 'US'
                     ELSE upper(btrim(c.country)) END"""

# One row per NPI + normalized address; a row with coordinates wins, then role > practitioner > organization.
INSERT_STAGE = f"""INSERT INTO {{stage}}
SELECT DISTINCT ON (npi, address_hash) npi, source_kind, btrim(line1), nullif(btrim(line2), ''),
       nullif(btrim(city), ''), nullif(btrim(state), ''), nullif(btrim(postal_code), ''), country_code,
       nullif(btrim(phone), ''), nullif(btrim(fax), ''), latitude, longitude, updated_at,
       'cms_npd:' || npi || ':' || address_hash, NULL
FROM (
    SELECT c.*, {COUNTRY_CODE} AS country_code,
           md5(concat_ws('|', {_norm("c.line1")}, {_norm("c.line2")}, {_norm("c.city")}, {_norm("c.state")},
                         left(regexp_replace(coalesce(c.postal_code, ''), '[^0-9]', '', 'g'), 5),
                         {COUNTRY_CODE})) AS address_hash
    FROM ({_CANDIDATES}) c
    WHERE btrim(coalesce(c.line1, '')) <> ''
) n
ORDER BY npi, address_hash, (latitude IS NULL OR longitude IS NULL), kind_rank, updated_at DESC NULLS LAST,
         line1, line2, city, state, postal_code"""

INDEX_STAGE = (
    "ALTER TABLE {stage} ADD CONSTRAINT {stage_name}_pkey PRIMARY KEY (npi, source_record_id)",
    "CREATE INDEX {stage_name}_address_key ON {stage} (address_key) WHERE address_key IS NOT NULL",
    "ANALYZE {stage}",
)
RECORD_COUNT = f"""UPDATE {{control}}.{RELEASE_TABLE}
SET row_counts = (row_counts - '{REFRESH_KEY}') || jsonb_build_object('{ROW_COUNT_KEY}', $1::bigint, '{BUILD_KEY}', {BUILD_VERSION})
WHERE status = 'current' AND release_id = $2"""
RECORD_REFRESH = f"""UPDATE {{control}}.{RELEASE_TABLE}
SET row_counts = row_counts || jsonb_build_object('{REFRESH_KEY}', 1)
WHERE status = 'current' AND release_id = $1"""


def has_pending_addresses(release):
    """A recorded release whose address table has not been built from it yet, or was built by an older build."""
    counts = release.get("row_counts") or {}
    return ROW_COUNT_KEY not in counts or counts.get(BUILD_KEY) != BUILD_VERSION


def has_pending_refresh(release):
    """A recorded release whose entity-address refresh has not been admitted yet."""
    return REFRESH_KEY not in (release.get("row_counts") or {})


async def table_exists(connection, control):
    """A recorded count does not prove the table is still there."""
    return await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", f"{quoted(control)}.{ADDRESS_TABLE}")


async def grant_api_read(connection, control):
    """The API readers may read the live address table; idempotent, so it also repairs an older build."""
    if readers := await api_readers(connection, control):
        await connection.execute(f"GRANT SELECT ON {quoted(control)}.{ADDRESS_TABLE} TO {', '.join(readers)}")


async def resolve_archive(stage_table, schema):
    """Stamp canonical keys and resolve into the address archive, exactly as CMS Doctors does."""
    from process.ext.address_canon import resolve_into_archive, source_enabled, stamp_address_keys

    if not source_enabled(ARCHIVE_SOURCE):
        return None
    await stamp_address_keys(stage_table, ADDRESS_FIELDS, schema=schema)
    return await resolve_into_archive(
        stage_table, ADDRESS_FIELDS, source_bit=ARCHIVE_SOURCE_BIT, priority=ARCHIVE_PRIORITY, schema=schema
    )


def swap_statements(control):
    """Drop the old copy, move live to ``_old`` and the stage to live, indexes renamed with their table."""
    schema = quoted(control)
    statements = [f"SET LOCAL lock_timeout = '{SWAP_LOCK_TIMEOUT}'", f"DROP TABLE IF EXISTS {schema}.{OLD_TABLE}"]
    for source, target in ((ADDRESS_TABLE, OLD_TABLE), (STAGE_TABLE, ADDRESS_TABLE)):
        statements.append(f"ALTER TABLE IF EXISTS {schema}.{source} RENAME TO {target}")
        statements += [
            f"ALTER INDEX IF EXISTS {schema}.{source}_{index} RENAME TO {target}_{index}" for index in _INDEX_SUFFIXES
        ]
    return statements


async def build(connection, control, release_schema, release_id, *, resolve=resolve_archive):
    """Rebuild the address table from ``release_schema`` and mark the release; return the row count."""
    schema, stage = quoted(control), f"{quoted(control)}.{STAGE_TABLE}"
    async with connection.transaction():
        await connection.execute(f"DROP TABLE IF EXISTS {stage}")
        await connection.execute(CREATE_STAGE.format(stage=stage))
        await connection.execute(INSERT_STAGE.format(stage=stage, release=quoted(release_schema)))
    await resolve(STAGE_TABLE, control)  # its own pooled connection: the stage is committed first
    for statement in INDEX_STAGE:
        await connection.execute(statement.format(stage=stage, stage_name=STAGE_TABLE))
    rows = await connection.fetchval(f"SELECT count(*) FROM {stage}")
    async with connection.transaction():
        for statement in swap_statements(control):
            await connection.execute(statement)
        await grant_api_read(connection, control)
        await connection.execute(RECORD_COUNT.format(control=schema), rows, release_id)
    return rows


def refresh_request(release_id):
    """One full entity-address refresh, keyed to the release so a retry never queues a second one."""
    return {
        "run_id": None,
        "importer": "entity-address-unified",
        "params": {"refresh_mode": "full", "trigger_source_key": "cms-npd", "trigger_snapshot_id": release_id},
        "idempotency_key": f"entity-address-unified:cms-npd:{release_id}",
        "triggered_by": "cms_npd_import",
        "schedule_id": None,
        "subscription_id": None,
        "import_id": f"entity-address-unified:cms-npd:{release_id[:16]}",
    }


async def enqueue_refresh(connection, control, release_id, *, create_run=None):
    """Queue the refresh and record its admission on the release row.

    An enqueue failure is reported, never raised (the table is already published). The admission control plane
    coalesces by importer, so it may hand back an unrelated active run that started before the swap; that
    is ``deferred`` and, like a failure, stays unrecorded so the next run queues the refresh again.
    """
    request = refresh_request(release_id)
    try:
        if create_run is None:
            from api.control_imports import create_import_run as create_run
        run, created = await create_run(request)
    except Exception as error:
        return {"status": "enqueue_failed", "error": str(error), "idempotency_key": request["idempotency_key"]}
    if not created and run.get("idempotency_key") != request["idempotency_key"]:
        return {"status": "deferred", "run_id": run.get("run_id")}
    await connection.execute(RECORD_REFRESH.format(control=quoted(control)), release_id)
    return {"status": "queued" if created else "existing", "run_id": run.get("run_id")}
