# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded source observations, never a copy of accumulated archive state."""

from __future__ import annotations

import json

from sqlalchemy import text

from process.entity_address_snapshot_alias import capture_entity_address_alias_semantic_receipt
from process.ext.address_canon import _qtable, current_canon_version

CONTRACT = "facility_address_contribution.v1"
METADATA_KEY = "00000000-0000-0000-0000-000000000000"
MAX_ROWS = 2_000_000
MAX_PAYLOAD_BYTES = 16_384
CANONICAL_COLUMNS = (
    "identity_key",
    "premise_key",
    "line1_norm",
    "unit_norm",
    "city_norm",
    "state_code",
    "zip5",
    "zip4",
    "country_code",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
)


async def require_local_family_publication(session, *, schema: str) -> None:
    """Fence protected adoption until ordinary address and family publication commits."""
    for name in ("facility_anchor", "facility_address_contribution"):
        relation = _qtable(schema, name)
        if await session.scalar(text("SELECT to_regclass(:relation) IS NOT NULL"), {"relation": relation}):
            await session.execute(text(f"LOCK TABLE {relation} IN SHARE ROW EXCLUSIVE MODE"))
    seeded = await session.scalar(
        text(
            f"SELECT importer_id FROM {_qtable(schema, 'reference_family_result_generation')} "
            "WHERE importer_id='facility-anchors' FOR UPDATE"
        )
    )
    if seeded != "facility-anchors":
        raise RuntimeError("Facility publication generation migration is required")
    allowed = await session.scalar(
        text("""
        SELECT bool_and(CASE WHEN relation.oid IS NULL THEN true
          ELSE pg_has_role(current_user,relation.relowner,'USAGE') END)
        FROM unnest(ARRAY['facility_anchor','facility_anchor_old',
                         'facility_address_contribution','facility_address_contribution_old']) wanted(name)
        LEFT JOIN pg_namespace namespace ON namespace.nspname=:schema
        LEFT JOIN pg_class relation ON relation.relnamespace=namespace.oid AND relation.relname=wanted.name
    """),
        {"schema": schema},
    )
    generation_access = await session.scalar(
        text(
            "SELECT has_table_privilege(current_user,:table_name,'SELECT') "
            "AND has_table_privilege(current_user,:table_name,'UPDATE')"
        ),
        {"table_name": _qtable(schema, "reference_family_result_generation")},
    )
    if allowed is not True or generation_access is not True:
        raise RuntimeError("Facility publication requires a protected native publication bridge")


async def require_capture_bounds(session, *, schema: str, contribution_table: str) -> None:
    """Abort publication rather than emit an unbounded contribution artifact."""
    table = _qtable(schema, contribution_table)
    excessive = (
        await session.execute(
            text(
                f"SELECT EXISTS (SELECT 1 FROM {table} GROUP BY kind HAVING count(*) > :max_rows) "
                f"OR EXISTS (SELECT 1 FROM {table} WHERE octet_length(payload::text) > :max_bytes)"
            ),
            {"max_rows": MAX_ROWS, "max_bytes": MAX_PAYLOAD_BYTES},
        )
    ).scalar()
    if excessive:
        raise RuntimeError("Facility address contribution exceeds capture bounds")


async def capture_metadata(session, *, schema: str, contribution_table: str, enabled: bool) -> None:
    """Freeze semantic identity under the alias writer lock when enabled."""
    alias = None
    if enabled:
        alias = (
            await capture_entity_address_alias_semantic_receipt(
                session,
                schema_name=schema,
            )
        ).portable_identity()
    payload = {
        "contract": CONTRACT,
        "enabled": enabled,
        "canon_version": current_canon_version(),
        "alias_semantics": alias,
        "source_bit": 8,
        "priority": 4,
    }
    await session.execute(
        text(
            f"INSERT INTO {_qtable(schema, contribution_table)} (kind, address_key, payload) "
            "VALUES ('metadata', CAST(:key AS uuid), CAST(:payload AS jsonb))"
        ),
        {"key": METADATA_KEY, "payload": json.dumps(payload)},
    )


async def capture_canonical_observations(
    session,
    *,
    schema: str,
    contribution_table: str,
    dedup_sql: str,
) -> None:
    """Capture validated pre-alias inputs using the resolver's own deduplication."""
    await capture_metadata(session, schema=schema, contribution_table=contribution_table, enabled=True)
    fields = ", ".join(f"'{name}', source.{name}" for name in CANONICAL_COLUMNS)
    await session.execute(
        text(
            f"INSERT INTO {_qtable(schema, contribution_table)} (kind, address_key, payload) "
            f"SELECT 'canonical', source.address_key, jsonb_build_object({fields}) "
            f"FROM ({dedup_sql}) source LIMIT {MAX_ROWS + 1}"
        )
    )
    await require_capture_bounds(session, schema=schema, contribution_table=contribution_table)


def captured_geocode_cte(*, schema: str, contribution_table: str, winner_sql: str) -> str:
    """Persist and apply exactly one materialization of the native tie winners."""
    return (
        f"facility_winners AS MATERIALIZED ({winner_sql} LIMIT {MAX_ROWS + 1}), "
        f"captured_geocodes AS (INSERT INTO {_qtable(schema, contribution_table)} "
        "(kind, address_key, payload) SELECT 'geocode', address_key, "
        "jsonb_build_object('lat', lat, 'long', long) FROM facility_winners "
        "RETURNING address_key, payload), "
        "facility_geocodes AS (SELECT address_key, (payload->>'lat')::numeric(11,8) AS lat, "
        "(payload->>'long')::numeric(11,8) AS long FROM captured_geocodes)"
    )
