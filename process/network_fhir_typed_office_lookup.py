# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded address lookup shared by one independently prepared mapping batch."""

from contextlib import asynccontextmanager
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_fhir_typed_contract import require

_ADDRESS_PAGE_ROWS = 50000


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
    """Advance over raw primary-key pages even when a complete page has no requested office."""
    if not await connection.fetchval(f"SELECT EXISTS(SELECT FROM {requested})"):
        return
    after = None
    while True:
        lower, arguments = ("TRUE", ()) if after is None else ("location_key>$1::text", (after,))
        page = await connection.fetchrow(
            f"""WITH page AS MATERIALIZED (
                SELECT location_key,entity_type,entity_id,npi,address_key,type,address_precision,inferred_npi
                FROM {address} WHERE {lower} ORDER BY location_key LIMIT {_ADDRESS_PAGE_ROWS}
            ), inserted AS (
                INSERT INTO {matches}
                SELECT requested.provider_id,page.address_key,page.location_key FROM page
                JOIN LATERAL (SELECT provider_id FROM {requested} requested
                    WHERE requested.provider_id=page.entity_id AND requested.address_key=page.address_key
                    OFFSET 0) requested ON true
                WHERE page.entity_type='npi' AND page.npi=requested.provider_id::bigint
                    AND page.type='practice' AND page.address_precision='street' AND page.inferred_npi IS NULL
                RETURNING 1
            ) SELECT (SELECT count(*) FROM page) AS scanned,
                (SELECT max(location_key) FROM page) AS last_key,(SELECT count(*) FROM inserted) AS matched""",
            *arguments,
        )
        require(0 <= page["matched"] <= page["scanned"] <= _ADDRESS_PAGE_ROWS, "office_lookup_counts")
        if not page["scanned"]:
            require(page["last_key"] is None, "office_lookup_cursor")
            return
        require(type(page["last_key"]) is str and page["last_key"] != after, "office_lookup_cursor")
        after = page["last_key"]


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
