# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded address lookup shared by one independently prepared mapping batch."""

from contextlib import asynccontextmanager
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_fhir_typed_contract import require

_REQUEST_PAGE_ROWS = 5000


def office_lookup_names(office_name):
    """Keep lookup companions under the owning mapping workspace's exact names."""
    return {"office_keys": office_name + "_keys", "office_matches": office_name + "_matches"}


async def create_office_lookup_tables(connection, office_name):
    """Create companions before a caller enters a read-only snapshot."""
    names_by_kind = office_lookup_names(office_name)
    requested = "pg_temp." + _identifier(names_by_kind["office_keys"])
    matches = "pg_temp." + _identifier(names_by_kind["office_matches"])
    await connection.execute(
        f"CREATE TEMP TABLE {requested}(provider_id text NOT NULL,address_key uuid NOT NULL,PRIMARY KEY(provider_id,address_key));"
        f"CREATE TEMP TABLE {matches}(provider_id text NOT NULL,address_key uuid NOT NULL,location_key text NOT NULL,PRIMARY KEY(provider_id,address_key,location_key))"
    )


async def _requested_keys(connection, offices, requested):
    for relation in offices:
        after_keys = "", UUID(int=0)
        while True:
            upper = await connection.fetchrow(
                f"SELECT provider_id,location_id FROM (SELECT provider_id,location_id FROM {relation} "
                "WHERE (provider_id,location_id)>($1::text,$2::uuid) ORDER BY provider_id,location_id LIMIT 5000) page "
                "ORDER BY provider_id DESC,location_id DESC LIMIT 1",
                *after_keys,
            )
            if upper is None:
                break
            await connection.execute(
                f"INSERT INTO {requested} SELECT provider_id,address_key FROM {relation} "
                "WHERE (provider_id,location_id)>($1::text,$2::uuid) "
                "AND (provider_id,location_id)<=($3::text,$4::uuid) ON CONFLICT DO NOTHING",
                *after_keys,
                *upper,
            )
            after_keys = tuple(upper)


async def _address_matches(connection, address, requested, matches):
    """Probe only requested NPI/address pairs, retaining all candidates for ambiguity checks."""
    after = "", UUID(int=0)
    while True:
        page = await connection.fetchrow(
            f"""WITH page AS MATERIALIZED (
                SELECT provider_id,address_key FROM {requested}
                WHERE (provider_id,address_key)>($1::text,$2::uuid)
                ORDER BY provider_id,address_key LIMIT {_REQUEST_PAGE_ROWS}
            ), inserted AS (
                INSERT INTO {matches}
                SELECT page.provider_id,page.address_key,a.location_key FROM page
                JOIN LATERAL (SELECT location_key FROM {address} a
                    WHERE a.address_key=page.address_key AND a.npi=page.provider_id::bigint
                        AND a.entity_type='npi' AND a.entity_id=page.provider_id
                        AND a.type='practice' AND a.address_precision='street' AND a.inferred_npi IS NULL
                    OFFSET 0) a ON true
                RETURNING 1
            ) SELECT (SELECT count(*) FROM page) AS scanned,
                (SELECT provider_id FROM page ORDER BY provider_id DESC,address_key DESC LIMIT 1) AS last_provider,
                (SELECT address_key FROM page ORDER BY provider_id DESC,address_key DESC LIMIT 1) AS last_address,
                (SELECT count(*) FROM inserted) AS matched""",
            *after,
        )
        require(0 <= page["scanned"] <= _REQUEST_PAGE_ROWS and page["matched"] >= 0, "office_lookup_counts")
        if not page["scanned"]:
            require(page["last_provider"] is None and page["last_address"] is None, "office_lookup_cursor")
            return
        following = page["last_provider"], page["last_address"]
        require(
            type(following[0]) is str and isinstance(following[1], UUID) and following > after, "office_lookup_cursor"
        )
        after = following


@asynccontextmanager
async def typed_office_lookup(connection, office_names, address_source):
    """Reuse empty mapping companions; success clears rows and rollback preserves their empty state."""
    names_by_kind = office_lookup_names(office_names[0])
    requested = "pg_temp." + _identifier(names_by_kind["office_keys"])
    matches = "pg_temp." + _identifier(names_by_kind["office_matches"])
    offices = ["pg_temp." + _identifier(name) for name in office_names]
    address = _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name)
    async with connection.transaction():
        require(
            await connection.fetchval(
                f"SELECT NOT EXISTS(SELECT FROM {requested}) AND NOT EXISTS(SELECT FROM {matches})"
            ),
            "office_lookup_not_empty",
        )
        await _requested_keys(connection, offices, requested)
        await connection.execute(f"ANALYZE {requested}")
        await _address_matches(connection, address, requested, matches)
        await connection.execute(f"ANALYZE {matches}")
        yield matches
        await connection.execute(f"DELETE FROM {matches}; DELETE FROM {requested}")
