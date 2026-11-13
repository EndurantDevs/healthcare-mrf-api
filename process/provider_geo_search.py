# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish the all-address geo search projection used by nearby reads without a network scope.

Each build writes a new generation table, indexes and analyzes it, and only then publishes it by switching the
current row of the state table. The previous generation stays for rollback. Every generation records the address
and NPI generations it was built from, and readers use it only while those are still the published ones.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass

import asyncpg

from db.plan_release_storage import storage_identifier, storage_relation

logger = logging.getLogger(__name__)

STATE_TABLE = "provider_geo_search_state"
TABLE_PREFIX = "provider_geo_search_g"
TABLE_NAME_PATTERN = re.compile(r"provider_geo_search_g[1-9][0-9]{0,17}\Z")
SEARCH_COLUMNS = frozenset(
    {
        "npi",
        "address_key",
        "location_key",
        "lat",
        "long",
        "type",
        "address_precision",
        "state_name",
        "taxonomy_array",
        "primary_taxonomy_codes",
        "geo",
    }
)
# Same switch as api.endpoint.npi.GEO_SERVICE_LOCATIONS_ENV; a test keeps them equal.
GEO_SERVICE_LOCATIONS_ENV = "HLTHPRT_GEO_INCLUDE_SERVICE_LOCATIONS"
_SERVICE_LOCATION_TYPES = ("primary", "secondary", "practice", "site")
_TYPE_RANK = (
    "CASE a.type WHEN 'primary' THEN 0 WHEN 'practice' THEN 1 WHEN 'site' THEN 2 WHEN 'secondary' THEN 3 ELSE 9 END"
)
# Bump when the projection columns change: readers ignore generations built with another layout.
LAYOUT = 3
# A generation without published origin (for example after a manual restore) has no usable identity.
_GENERATION_KEY = (
    "CASE WHEN origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL THEN "
    "jsonb_build_array(local_lineage_id, local_generation, origin_lineage_id, origin_generation) END"
)
_BUILD_LOCK_ID = 7_310_427_651_118_003_201
# Readers hold it shared for their transaction; retiring a table takes it exclusively first.
RETIRE_LOCK_ID = 7_310_427_651_118_003_202
_BUILD_ATTEMPTS = 3
_BATCH_START_BLOCKS = 256
_BATCH_MAX_BLOCKS = 65_536
_BATCH_LOCK_TIMEOUT = "1s"
_BATCH_LOCK_TIMEOUT_SECONDS = 1
_BATCH_LOCK_ATTEMPTS = 30


class ProviderGeoSearchError(RuntimeError):
    """The projection could not be built or published; readers keep the previous state."""


@dataclass(frozen=True, slots=True)
class CopyPace:
    """How a copy shares a live relation with its publisher."""

    statement_timeout_ms: int
    gap_seconds: float
    publisher_key: str | None = None


def address_copy_pace(schema: str) -> CopyPace:
    """Pace address copies so the address publisher's NOWAIT cutover always finds a free retry window."""
    # The address publisher takes its cutover key and NOWAIT locks, retrying after 25, 50 and 100 ms. A batch that
    # holds its locks for under 25 ms and is followed by a longer gap leaves every second retry a free window, and
    # sharing the publisher's key keeps batches out of a cutover that is already running.
    return CopyPace(statement_timeout_ms=15, gap_seconds=0.04, publisher_key=f"entity-address-unified:{schema}")


# NPI publication waits for its locks, so a short cap alone keeps batches out of its way.
NPI_COPY_PACE = CopyPace(statement_timeout_ms=40, gap_seconds=0)


def include_service_locations() -> bool:
    """Read the deployment switch that adds practice and site addresses to geo searches."""
    return os.getenv(GEO_SERVICE_LOCATIONS_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def input_keys_sql(schema: str) -> tuple[str, str]:
    """Return scalar subqueries for the published address and NPI generation keys."""
    return (
        f"SELECT {_GENERATION_KEY} FROM {storage_relation(schema, 'entity_address_result_generation')}",
        f"SELECT {_GENERATION_KEY} FROM {storage_relation(schema, 'npi_result_generation')}",
    )


def _state(schema: str) -> str:
    return storage_relation(schema, STATE_TABLE)


_STAGE_COLUMNS = {
    "addresses": "npi bigint NOT NULL, address_key uuid NOT NULL, location_key text NOT NULL, "
    "lat numeric(11,8) NOT NULL, long numeric(11,8) NOT NULL, state_name text, type text NOT NULL, "
    "address_precision text, taxonomy_array integer[]",
    "npis": "npi bigint NOT NULL",
    "primary": "npi bigint NOT NULL, code text NOT NULL",
}


def _stage_tables(load_table: str) -> dict[str, str]:
    return {stage: f"{load_table}_{stage}" for stage in _STAGE_COLUMNS}


def _copy_plans(schema: str, load_table: str, *, uses_service_locations: bool) -> list[tuple[str, str, str, CopyPace]]:
    """Return (live relation, alias, INSERT with a {block_range} placeholder, pace) for each copied input."""
    types = _SERVICE_LOCATION_TYPES if uses_service_locations else _SERVICE_LOCATION_TYPES[:2]
    type_list = ", ".join(f"'{address_type}'" for address_type in types)
    relation_by_stage = {stage: storage_relation(schema, name) for stage, name in _stage_tables(load_table).items()}
    return [
        (
            "entity_address_unified",
            "a",
            f"""INSERT INTO {relation_by_stage["addresses"]}
SELECT a.npi, a.address_key, a.location_key, a.lat, a.long, a.state_name, a.type, a.address_precision,
       a.taxonomy_array
  FROM {storage_relation(schema, "entity_address_unified")} AS a
 WHERE {{block_range}}
   AND a.npi IS NOT NULL
   AND a.address_key IS NOT NULL
   AND a.location_key IS NOT NULL
   AND a.lat IS NOT NULL
   AND a.long IS NOT NULL
   AND COALESCE(a.address_precision, '') <> 'city_zip'
   AND a.type IN ({type_list})""",
            address_copy_pace(schema),
        ),
        (
            "npi",
            "d",
            f"INSERT INTO {relation_by_stage['npis']} SELECT d.npi FROM {storage_relation(schema, 'npi')} AS d "
            "WHERE {block_range}",
            NPI_COPY_PACE,
        ),
        (
            "npi_taxonomy",
            "t",
            f"""INSERT INTO {relation_by_stage["primary"]}
SELECT t.npi, CAST(t.healthcare_provider_taxonomy_code AS text)
  FROM {storage_relation(schema, "npi_taxonomy")} AS t
 WHERE {{block_range}}
   AND UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'
   AND t.healthcare_provider_taxonomy_code IS NOT NULL""",
            NPI_COPY_PACE,
        ),
    ]


def _block_range(alias: str, lower: int, upper: int) -> str:
    return f"{alias}.ctid >= '({int(lower)},0)'::tid AND {alias}.ctid < '({int(upper)},0)'::tid"


def _projection_sql(schema: str, load_table: str, table: str) -> str:
    """Build the projection from the staged copies only; no live relation is read."""
    relation_by_stage = {stage: storage_relation(schema, name) for stage, name in _stage_tables(load_table).items()}
    return f"""CREATE TABLE {storage_relation(schema, table)} AS
WITH eligible AS (
    SELECT a.*,
           row_number() OVER (
               PARTITION BY a.npi, a.address_key, a.lat, a.long, a.state_name, a.taxonomy_array
               ORDER BY {_TYPE_RANK}, a.location_key
           ) AS group_rank
      FROM {relation_by_stage["addresses"]} AS a
     WHERE EXISTS (SELECT 1 FROM {relation_by_stage["npis"]} AS d WHERE d.npi = a.npi)
), primary_taxonomy AS (
    SELECT t.npi, array_agg(DISTINCT t.code ORDER BY t.code) AS codes
      FROM {relation_by_stage["primary"]} AS t
     GROUP BY t.npi
), placed AS (
    -- One row per provider address, point and taxonomy set keeps the address row the live query would return;
    -- only addresses with several such rows need a distinct count.
    SELECT e.npi, e.address_key, e.location_key, e.lat, e.long, e.state_name, e.type, e.address_precision,
           e.taxonomy_array,
           primary_taxonomy.codes AS primary_taxonomy_codes,
           public.Geography(public.ST_MakePoint(e.long::double precision, e.lat::double precision)) AS geo,
           count(*) OVER (PARTITION BY e.npi, e.address_key) > 1 AS shares_identity
      FROM eligible AS e
      LEFT JOIN primary_taxonomy ON primary_taxonomy.npi = e.npi
     WHERE e.group_rank = 1
)
SELECT * FROM placed
 ORDER BY public.ST_GeoHash(public.ST_MakePoint(long::double precision, lat::double precision), 9)"""


async def _ensure_state_table(connection, schema: str) -> None:
    state = _state(schema)
    await connection.execute(
        f"""CREATE TABLE IF NOT EXISTS {state} (
            generation bigint PRIMARY KEY CHECK (generation > 0),
            table_name text NOT NULL UNIQUE,
            address_input jsonb NOT NULL,
            npi_input jsonb NOT NULL,
            include_service_locations boolean NOT NULL,
            row_count bigint NOT NULL CHECK (row_count > 0),
            identity_unique boolean NOT NULL,
            built_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            published_at timestamptz,
            is_current boolean NOT NULL DEFAULT false
        )"""
    )
    has_layout = await connection.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = 'layout')", state
    )
    if not has_layout:
        # Only a table created before layouts takes this exclusive lock, and never waits long for readers.
        async with connection.transaction():
            await connection.execute("SET LOCAL lock_timeout = '5s'")
            await connection.execute(f"ALTER TABLE {state} ADD COLUMN layout integer NOT NULL DEFAULT 1")
    await connection.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {STATE_TABLE}_current ON {state} (is_current) WHERE is_current"
    )


async def _grant_address_readers(connection, schema: str, relation: str) -> None:
    """Give a new relation the same SELECT grantees as the address table it is derived from."""
    grantees = await connection.fetch(
        """SELECT DISTINCT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS grantee
             FROM pg_class AS c
            CROSS JOIN LATERAL aclexplode(c.relacl) AS a
             LEFT JOIN pg_roles AS r ON r.oid = a.grantee
            WHERE c.oid = to_regclass($1) AND a.privilege_type = 'SELECT' AND a.grantee <> c.relowner
            ORDER BY 1""",
        storage_relation(schema, "entity_address_unified"),
    )
    if grantees:
        await connection.execute(f"GRANT SELECT ON {relation} TO {', '.join(row['grantee'] for row in grantees)}")


async def _is_current(connection, schema: str, *, uses_service_locations: bool) -> bool:
    address_sql, npi_sql = input_keys_sql(schema)
    return bool(
        await connection.fetchval(
            f"""SELECT EXISTS (SELECT 1 FROM {_state(schema)} AS s
                 WHERE s.is_current AND s.layout = $2 AND s.include_service_locations = $1
                   AND s.address_input = ({address_sql}) AND s.npi_input = ({npi_sql}))""",
            uses_service_locations,
            LAYOUT,
        )
    )


async def _captured_inputs(connection, schema: str) -> tuple[str | None, str | None]:
    address_sql, npi_sql = input_keys_sql(schema)
    keys = await connection.fetchrow(f"SELECT ({address_sql})::text AS address_input, ({npi_sql})::text AS npi_input")
    return keys["address_input"], keys["npi_input"]


class _PublisherActive(Exception):
    """The live relation's publisher holds its cutover key; the batch waits instead of colliding."""


async def _load_batch(connection, sql: str, pace: CopyPace) -> None:
    """Run one statement capped by the pace, never inside a running cutover."""
    for attempt in range(_BATCH_LOCK_ATTEMPTS):
        try:
            async with connection.transaction():
                await connection.execute(f"SET LOCAL lock_timeout = '{_BATCH_LOCK_TIMEOUT}'")
                if pace.publisher_key is not None and not await connection.fetchval(
                    "SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1, 0))", pace.publisher_key
                ):
                    raise _PublisherActive
                await connection.execute(f"SET LOCAL statement_timeout = '{pace.statement_timeout_ms}ms'")
                await connection.execute(sql)
                # A timeout firing as the insert ends must not land on COMMIT instead.
                await connection.execute("SET LOCAL statement_timeout = 0")
            return
        except asyncpg.LockNotAvailableError, _PublisherActive:
            await _end_aborted_transaction(connection)
            if attempt + 1 == _BATCH_LOCK_ATTEMPTS:
                raise
            await asyncio.sleep(_BATCH_LOCK_TIMEOUT_SECONDS)
        except BaseException:
            await _end_aborted_transaction(connection)
            raise


async def _end_aborted_transaction(connection) -> None:
    """Roll back a transaction the driver no longer tracks, so the next batch or cleanup can run."""
    if connection.is_in_transaction():
        await connection.execute("ROLLBACK")


async def _copy_in_block_ranges(connection, template: str, alias: str, blocks: int, pace: CopyPace) -> None:
    """Copy physical block ranges, halving a range that overruns the cap; one 8 kB block always fits."""
    lower, width = 0, _BATCH_START_BLOCKS
    while lower < blocks:
        upper = min(lower + width, blocks)
        started = time.monotonic()
        try:
            await _load_batch(connection, template.format(block_range=_block_range(alias, lower, upper)), pace)
        except asyncpg.QueryCanceledError:
            if upper - lower == 1:
                raise
            width = (upper - lower) // 2
            # A timed-out batch held its locks too; pausing here keeps consecutive halvings from chaining.
            if pace.gap_seconds:
                await asyncio.sleep(pace.gap_seconds)
            continue
        if (time.monotonic() - started) * 1000 < pace.statement_timeout_ms / 2:
            width = min(width * 2, _BATCH_MAX_BLOCKS)
        lower = upper
        if pace.gap_seconds:
            await asyncio.sleep(pace.gap_seconds)


async def _load_rows(connection, schema: str, load_table: str, *, uses_service_locations: bool) -> None:
    """Copy the eligible live rows into unlogged stage tables in short statements."""
    for stage, name in _stage_tables(load_table).items():
        relation = storage_relation(schema, name)
        await connection.execute(f"DROP TABLE IF EXISTS {relation}")
        await connection.execute(f"CREATE UNLOGGED TABLE {relation} ({_STAGE_COLUMNS[stage]})")
    for source, alias, template, pace in _copy_plans(schema, load_table, uses_service_locations=uses_service_locations):
        blocks = await connection.fetchval(
            "SELECT pg_relation_size(to_regclass($1)) / current_setting('block_size')::bigint",
            storage_relation(schema, source),
        )
        await _copy_in_block_ranges(connection, template, alias, int(blocks or 0), pace)


async def _finish_generation(connection, schema: str, load_table: str, table: str) -> dict[str, int]:
    """Build, order, index, analyze and validate the unpublished table; no live relation is read here."""
    relation = storage_relation(schema, table)
    await connection.execute(f"DROP TABLE IF EXISTS {relation}")
    await connection.execute(_projection_sql(schema, load_table, table))
    for name in _stage_tables(load_table).values():
        await connection.execute(f"DROP TABLE {storage_relation(schema, name)}")
    await connection.execute(f"CREATE INDEX {table}_geo ON {relation} USING gist (geo)")
    await connection.execute(
        f"CREATE INDEX {table}_geo_taxonomy ON {relation} USING gist (geo, taxonomy_array public.gist__intbig_ops)"
    )
    await connection.execute(f"ANALYZE {relation}")
    counts = await connection.fetchrow(
        f"SELECT count(*) AS row_count, count(DISTINCT (npi, address_key)) AS identity_count, "
        f"count(DISTINCT location_key) AS location_count, count(*) FILTER (WHERE NOT shares_identity) "
        f"+ count(DISTINCT (npi, address_key)) FILTER (WHERE shares_identity) AS split_identity_count "
        f"FROM {relation}"
    )
    if counts["row_count"] == 0 or counts["location_count"] != counts["row_count"]:
        raise ProviderGeoSearchError("projection rows must be non-empty with one row per location")
    if counts["split_identity_count"] != counts["identity_count"]:
        raise ProviderGeoSearchError("projection shared-identity flags do not match its rows")
    await _grant_address_readers(connection, schema, relation)
    return dict(counts)


async def _build_generation(connection, schema: str, *, uses_service_locations: bool) -> tuple[int, str] | None:
    """Load from consistent inputs in short batches, then prepare the unpublished table without live locks.

    Returns None when an input was published during the load; the caller builds again.
    """
    state = _state(schema)
    generation = await connection.fetchval(f"SELECT COALESCE(max(generation), 0) + 1 FROM {state}")
    table, load_table = f"{TABLE_PREFIX}{generation}", f"{TABLE_PREFIX}{generation}_load"
    await connection.execute("SET statement_timeout = 0")
    await connection.execute("SET maintenance_work_mem = '2GB'")
    await connection.execute("SET work_mem = '256MB'")
    try:
        captured = await _captured_inputs(connection, schema)
        if None in captured:
            raise ProviderGeoSearchError("address and NPI generations must be published before the projection")
        await _load_rows(connection, schema, load_table, uses_service_locations=uses_service_locations)
        if await _captured_inputs(connection, schema) != captured:
            await _drop_unpublished(connection, schema, tuple(_stage_tables(load_table).values()))
            return None
        counts = await _finish_generation(connection, schema, load_table, table)
        await connection.execute(
            f"""INSERT INTO {state} (generation, table_name, address_input, npi_input, include_service_locations,
                                     row_count, identity_unique, layout)
                VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, $7, $8)""",
            generation,
            table,
            *captured,
            uses_service_locations,
            counts["row_count"],
            counts["identity_count"] == counts["row_count"],
            LAYOUT,
        )
    except BaseException:
        await _drop_unpublished(connection, schema, (*_stage_tables(load_table).values(), table))
        raise
    finally:
        await connection.execute("RESET statement_timeout; RESET maintenance_work_mem; RESET work_mem")
    return generation, table


async def _drop_unpublished(connection, schema: str, tables: tuple[str, ...]) -> None:
    try:
        for name in tables:
            await connection.execute(f"DROP TABLE IF EXISTS {storage_relation(schema, name)}")
    except Exception as error:  # The original failure matters more than cleanup; the next build drops these.
        logger.warning("Could not drop unpublished provider geo search tables %s: %s", tables, error)


async def _has_published(connection, schema: str, generation: int) -> bool:
    """Switch the current pointer if the generation's inputs are still the published ones."""
    state = _state(schema)
    address_sql, npi_sql = input_keys_sql(schema)
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout = '10s'")
        is_fresh = await connection.fetchval(
            f"""SELECT s.address_input = ({address_sql}) AND s.npi_input = ({npi_sql})
                  FROM {state} AS s WHERE s.generation = $1 FOR UPDATE""",
            generation,
        )
        if not is_fresh:
            return False
        await connection.execute(f"UPDATE {state} SET is_current = false WHERE is_current")
        await connection.execute(
            f"UPDATE {state} SET is_current = true, published_at = clock_timestamp() WHERE generation = $1",
            generation,
        )
    return True


async def _drop_generation(connection, schema: str, generation: int, table: str) -> None:
    """Remove one non-serving generation; a busy table is left for the next build."""
    if not TABLE_NAME_PATTERN.fullmatch(table):
        raise ProviderGeoSearchError("projection table name is invalid")
    try:
        async with connection.transaction():
            await connection.execute("SET LOCAL lock_timeout = '2s'")
            await connection.execute("SELECT pg_advisory_xact_lock($1)", RETIRE_LOCK_ID)
            await connection.execute(f"DROP TABLE IF EXISTS {storage_relation(schema, table)}")
            await connection.execute(
                f"DELETE FROM {_state(schema)} WHERE generation = $1 AND NOT is_current", generation
            )
    except Exception as error:  # Retention must never fail a published rebuild.
        logger.warning("Could not drop provider geo search generation %s: %s", generation, error)


async def _drop_retired(connection, schema: str) -> None:
    """Keep the current generation and the newest earlier published one for rollback."""
    state = _state(schema)
    retired = await connection.fetch(
        f"""SELECT generation, table_name FROM {state}
             WHERE NOT is_current
               AND generation IS DISTINCT FROM (
                   SELECT max(generation) FROM {state} WHERE published_at IS NOT NULL AND NOT is_current)
             ORDER BY generation"""
    )
    for row in retired:
        await _drop_generation(connection, schema, row["generation"], row["table_name"])


async def rebuild_provider_geo_search(
    connection,
    *,
    schema: str = "mrf",
    uses_service_locations: bool | None = None,
    force: bool = False,
    waits_for_build: bool = True,
) -> dict[str, object]:
    """Build and publish generations until the current one matches the published inputs."""
    storage_identifier(schema)
    if uses_service_locations is None:
        uses_service_locations = include_service_locations()
    outcome_by_field = {"status": "busy", "include_service_locations": uses_service_locations}
    should_wait, needs_build = waits_for_build, force
    for _round in range(_BUILD_ATTEMPTS):
        if should_wait:
            await connection.execute("SELECT pg_advisory_lock($1)", _BUILD_LOCK_ID)
        elif not await connection.fetchval("SELECT pg_try_advisory_lock($1)", _BUILD_LOCK_ID):
            return outcome_by_field
        try:
            outcome_by_field = await _rebuild_locked(
                connection, schema, uses_service_locations=uses_service_locations, needs_build=needs_build
            )
        finally:
            await connection.execute("SELECT pg_advisory_unlock($1)", _BUILD_LOCK_ID)
        # A caller refused as busy while this build finished relies on this check after the unlock.
        if await _is_current(connection, schema, uses_service_locations=uses_service_locations):
            return outcome_by_field
        should_wait = needs_build = False
    raise ProviderGeoSearchError("projection inputs kept changing during the build")


async def _rebuild_locked(connection, schema: str, *, uses_service_locations: bool, needs_build: bool):
    await _ensure_state_table(connection, schema)
    await _grant_address_readers(connection, schema, _state(schema))
    # Retired here, never right after a publish: a read that picked a generation just before the switch
    # still finds its table, since anything dropped stopped serving at least one publish earlier.
    await _drop_retired(connection, schema)
    outcome_by_field = {"status": "current", "include_service_locations": uses_service_locations}
    for _attempt in range(_BUILD_ATTEMPTS):
        # Rechecked after each publish: an input may have been published while this build ran.
        if not needs_build and await _is_current(connection, schema, uses_service_locations=uses_service_locations):
            return outcome_by_field
        needs_build = False
        built = await _build_generation(connection, schema, uses_service_locations=uses_service_locations)
        if built is None:
            logger.info("Provider geo search inputs were published during the load; loading again")
            continue
        generation, table = built
        if await _has_published(connection, schema, generation):
            outcome_by_field = {
                "status": "published",
                "generation": generation,
                "table": f"{schema}.{table}",
                "include_service_locations": uses_service_locations,
            }
        else:
            logger.info("Provider geo search inputs changed while building generation %s", generation)
            await _drop_generation(connection, schema, generation, table)
    if await _is_current(connection, schema, uses_service_locations=uses_service_locations):
        return outcome_by_field
    raise ProviderGeoSearchError("projection inputs kept changing during the build")


async def rollback_provider_geo_search(connection, *, schema: str = "mrf") -> dict[str, object]:
    """Point readers back at the newest earlier published generation that still exists."""
    state = _state(schema)
    await connection.execute("SELECT pg_advisory_lock($1)", _BUILD_LOCK_ID)
    try:
        async with connection.transaction():
            current = await connection.fetchval(f"SELECT generation FROM {state} WHERE is_current FOR UPDATE")
            previous = await connection.fetchval(
                f"""SELECT max(generation) FROM {state}
                     WHERE published_at IS NOT NULL AND NOT is_current
                       AND to_regclass(format('%I.%I', $1::text, table_name)) IS NOT NULL""",
                schema,
            )
            if current is None or previous is None:
                raise ProviderGeoSearchError("no earlier provider geo search generation to roll back to")
            await connection.execute(f"UPDATE {state} SET is_current = false WHERE is_current")
            await connection.execute(f"UPDATE {state} SET is_current = true WHERE generation = $1", previous)
        return {"status": "rolled_back", "from": current, "to": previous}
    finally:
        await connection.execute("SELECT pg_advisory_unlock($1)", _BUILD_LOCK_ID)


async def run_provider_geo_search(*, rollback: bool = False, force: bool = False) -> dict[str, object]:
    """Run a rebuild (or rollback) on the configured database for the CLI and importer hooks."""
    from db.connection import db
    from process.ext.utils import ensure_database

    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await ensure_database(False)
    async with db.acquire_driver(detached=True) as connection:
        if rollback:
            return await rollback_provider_geo_search(connection, schema=schema)
        return await rebuild_provider_geo_search(connection, schema=schema, force=force)


async def refresh_after_publication(source: str, *, test_mode: bool = False) -> None:
    """Rebuild after an address or NPI publication committed; a failure leaves readers on the address table."""
    if test_mode:
        return
    from db.connection import db

    try:
        async with db.acquire_driver(detached=True) as connection:
            result = await rebuild_provider_geo_search(
                connection, schema=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", waits_for_build=False
            )
        logger.info("Provider geo search after %s publication: %s", source, result)
    except Exception:  # The input publication already committed and must stay successful.
        logger.exception("Provider geo search rebuild after %s publication failed; reads use addresses", source)
