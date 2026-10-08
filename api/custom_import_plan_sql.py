# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Count and page complete native pricing groups in transaction-local SQL."""

from __future__ import annotations

from sqlalchemy import text

from api.plan_pricing_projection_contract import COST_ORDER_FIELDS

IMPORTED = "custom_import_plan_imported"
CANDIDATES = "custom_import_plan_candidates"
MEMBERSHIPS = "custom_import_plan_memberships"
OCCURRENCES = "custom_import_plan_occurrences"
PRICES = "custom_import_plan_prices"
SELECTED = "custom_import_plan_selected"
GROUP_COLUMNS = (
    "reported_system",
    "reported_code",
    "arrangement",
    "version_present",
    "version_text",
    "name_present",
    "name_text",
    "description_present",
    "description_text",
    "network_names",
    "source_id",
)
_GROUP_DECLARATIONS = """
    reported_system text COLLATE "C" NOT NULL,
    reported_code text COLLATE "C" NOT NULL,
    arrangement text COLLATE "C" NOT NULL,
    version_present boolean NOT NULL,
    version_text text COLLATE "C" NOT NULL,
    name_present boolean NOT NULL,
    name_text text COLLATE "C" NOT NULL,
    description_present boolean NOT NULL,
    description_text text COLLATE "C" NOT NULL,
    network_names text[] COLLATE "C" NOT NULL,
    source_id text COLLATE "C" NOT NULL
"""
OCCURRENCE_COPY_COLUMNS = (
    "binding_ordinal",
    "occurrence_id",
    "provider_set_key",
    "price_key",
    *GROUP_COLUMNS,
    "serving_data",
)
SELECTED_COPY_COLUMNS = ("page_ordinal", "binding_ordinal", "npi", "location_key", *GROUP_COLUMNS)


async def prepare_plan_query_tables(session):
    """Allocate only TEMP tables before the request becomes read-only."""

    statements = (
        f"""CREATE TEMP TABLE {IMPORTED} (
            npi bigint PRIMARY KEY, import_rank bigint
        ) ON COMMIT DROP""",
        f"""CREATE TEMP TABLE {CANDIDATES} (
            binding_ordinal integer NOT NULL, npi bigint NOT NULL,
            import_missing boolean NOT NULL, import_rank bigint,
            distance_miles double precision, location_hash text COLLATE "C",
            PRIMARY KEY (binding_ordinal, npi)
        ) ON COMMIT DROP""",
        f"""CREATE TEMP TABLE {MEMBERSHIPS} (
            binding_ordinal integer NOT NULL, npi bigint NOT NULL,
            provider_set_key bigint NOT NULL, membership_ordinal integer NOT NULL,
            PRIMARY KEY (binding_ordinal, npi, provider_set_key)
        ) ON COMMIT DROP""",
        f"CREATE INDEX ON {MEMBERSHIPS} (binding_ordinal, provider_set_key, npi)",
        f"""CREATE TEMP TABLE {OCCURRENCES} (
            binding_ordinal integer NOT NULL, occurrence_id bigint NOT NULL,
            provider_set_key bigint NOT NULL, price_key bigint NOT NULL,
            {_GROUP_DECLARATIONS}, serving_data jsonb NOT NULL,
            PRIMARY KEY (binding_ordinal, occurrence_id)
        ) ON COMMIT DROP""",
        f"CREATE INDEX ON {OCCURRENCES} (binding_ordinal, provider_set_key)",
        f"CREATE INDEX ON {OCCURRENCES} (binding_ordinal, price_key)",
        f"""CREATE TEMP TABLE {PRICES} (
            binding_ordinal integer NOT NULL, price_key bigint NOT NULL,
            eligible boolean NOT NULL, minimum_rate numeric,
            PRIMARY KEY (binding_ordinal, price_key)
        ) ON COMMIT DROP""",
        f"""CREATE TEMP TABLE {SELECTED} (
            page_ordinal integer PRIMARY KEY, binding_ordinal integer NOT NULL,
            npi bigint NOT NULL, location_key text COLLATE "C" NOT NULL,
            {_GROUP_DECLARATIONS}
        ) ON COMMIT DROP""",
    )
    for statement in statements:
        await session.execute(text(statement))


def _group_source_sql():
    group_columns = ", ".join(f"occurrence.{name}" for name in GROUP_COLUMNS)
    candidate_columns = "candidate.binding_ordinal, candidate.npi, candidate.import_missing, candidate.import_rank"
    location = "COALESCE(NULLIF(candidate.location_hash, ''), '||||')"
    return f"""
        SELECT {candidate_columns}, candidate.distance_miles, candidate.location_hash,
               {location} AS location_key, {group_columns}, MIN(price.minimum_rate) AS minimum_rate
          FROM pg_temp.{CANDIDATES} candidate
          JOIN pg_temp.{MEMBERSHIPS} member
            ON member.binding_ordinal = candidate.binding_ordinal AND member.npi = candidate.npi
          JOIN pg_temp.{OCCURRENCES} occurrence
            ON occurrence.binding_ordinal = member.binding_ordinal
           AND occurrence.provider_set_key = member.provider_set_key
          LEFT JOIN pg_temp.{PRICES} price
            ON price.binding_ordinal = occurrence.binding_ordinal AND price.price_key = occurrence.price_key
         WHERE NOT :needs_prices OR price.eligible
         GROUP BY {candidate_columns}, candidate.distance_miles, candidate.location_hash, {group_columns}
    """


def _group_order(import_context, args):
    if import_context.prepared.normalized_order_terms:
        order_clauses = ["import_missing", "import_rank ASC NULLS LAST"]
    else:
        order_clauses = []
        order_by = str(args.get("order_by") or "total_allowed_amount")
        column = (
            "minimum_rate"
            if order_by in COST_ORDER_FIELDS
            else ("distance_miles" if order_by in {"distance", "distance_miles"} else None)
        )
        if column:
            direction = "DESC" if args.get("order") == "desc" else "ASC"
            order_clauses.append(f"{column} {direction} NULLS LAST")
    return ", ".join([*order_clauses, "npi", "binding_ordinal", 'location_key COLLATE "C"', *GROUP_COLUMNS])


def plan_page_statement(import_context, args):
    """Report an exact group count even when the requested page is empty."""

    return text(f"""
        WITH native_groups AS MATERIALIZED ({_group_source_sql()}),
             total AS (SELECT COUNT(*) AS total FROM native_groups),
             page AS (
                 SELECT * FROM native_groups ORDER BY {_group_order(import_context, args)}
                 LIMIT :page_limit OFFSET :page_offset
             )
        SELECT total.total, page.* FROM total LEFT JOIN page ON TRUE
         ORDER BY {_group_order(import_context, args)}
    """)


def plan_analyze_statement(table_name):
    """Collect selector statistics without analyzing unused occurrence payloads."""

    columns = ("binding_ordinal", "provider_set_key", "price_key", *GROUP_COLUMNS)
    column_sql = f" ({', '.join(columns)})" if table_name == OCCURRENCES else ""
    return text(f"ANALYZE pg_temp.{table_name}{column_sql}")


def plan_completion_statement():
    """Read every eligible occurrence belonging to the selected native groups."""

    group_match = " AND ".join(f"occurrence.{name} = selected.{name}" for name in GROUP_COLUMNS)
    return text(f"""
        SELECT selected.page_ordinal, occurrence.serving_data
          FROM pg_temp.{SELECTED} selected
          JOIN pg_temp.{MEMBERSHIPS} member
            ON member.binding_ordinal = selected.binding_ordinal AND member.npi = selected.npi
          JOIN pg_temp.{OCCURRENCES} occurrence
            ON occurrence.binding_ordinal = member.binding_ordinal
           AND occurrence.provider_set_key = member.provider_set_key AND {group_match}
          LEFT JOIN pg_temp.{PRICES} price
            ON price.binding_ordinal = occurrence.binding_ordinal AND price.price_key = occurrence.price_key
         WHERE NOT :needs_prices OR price.eligible
         ORDER BY selected.page_ordinal, member.membership_ordinal, occurrence.occurrence_id
    """)


def native_group_key(group_by_field):
    """Recreate the existing Python identity without changing presence bits."""

    return (
        str(group_by_field["npi"]),
        group_by_field["location_key"],
        group_by_field["reported_system"],
        group_by_field["reported_code"],
        group_by_field["arrangement"],
        (group_by_field["version_present"], group_by_field["version_text"]),
        (group_by_field["name_present"], group_by_field["name_text"]),
        (group_by_field["description_present"], group_by_field["description_text"]),
        tuple(group_by_field["network_names"]),
        group_by_field["source_id"],
    )
