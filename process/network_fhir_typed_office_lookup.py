# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded address lookup shared by one independently prepared mapping batch."""

from contextlib import asynccontextmanager, nullcontext
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_fhir_typed_contract import require

_REQUEST_PAGE_ROWS = 20000


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
        await connection.execute(
            f"INSERT INTO {requested} SELECT provider_id,address_key FROM {relation} "
            "WHERE (provider_id,location_id)>($1::text,$2::uuid) ON CONFLICT DO NOTHING",
            "",
            UUID(int=0),
        )


async def _address_matches(connection, address, requested, matches):
    """Keep at most two exact candidates per key, enough to distinguish a unique match."""
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
                    LIMIT 2) a ON true
                RETURNING 1
            ) SELECT scanned,last.provider_id AS last_provider,last.address_key AS last_address,
                (SELECT count(*) FROM inserted) AS matched
            FROM (SELECT count(*) AS scanned FROM page) totals
            LEFT JOIN LATERAL (SELECT provider_id,address_key FROM page
                ORDER BY provider_id DESC,address_key DESC LIMIT 1) last ON true""",
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
async def typed_office_lookup(connection, office_names, address_source, *, owned_workspace=False):
    """Reuse empty lookup relations, with rollback owned by the caller for fresh batch workspaces."""
    names_by_kind = office_lookup_names(office_names[0])
    requested = "pg_temp." + _identifier(names_by_kind["office_keys"])
    matches = "pg_temp." + _identifier(names_by_kind["office_matches"])
    offices = ["pg_temp." + _identifier(name) for name in office_names]
    address = _identifier(address_source.schema_name) + "." + _identifier(address_source.table_name)
    async with nullcontext() if owned_workspace else connection.transaction():
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
        if owned_workspace:
            await connection.execute(f"TRUNCATE {matches},{requested}")
        else:
            await connection.execute(f"DELETE FROM {matches}; DELETE FROM {requested}")
