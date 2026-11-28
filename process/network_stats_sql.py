# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based PostgreSQL DDL and HashAggregate projections for precomputed network statistics."""

from __future__ import annotations

from process.network_compact_layout import compact_identifier
from process.network_stats_contract import (
    DIM_TABLE,
    SPECIALTY_TABLE,
    STATE_SPECIALTY_TABLE,
    STATE_TABLE,
    SUMMARY_TABLE,
)

TAX_MAP_STAGE = "_stats_tax_map"
PO_NPI_STAGE = "_stats_provider_only_npis"
NPI_STAGE = "_stats_npi_stage"
OFFICE_STAGE = "_stats_office_stage"
BATCH_NETWORKS_STAGE = "_stats_batch_networks"
BATCH_OFFICES_STAGE = "_stats_batch_offices"
BATCH_STATES_STAGE = "_stats_batch_states"

STAGE_TABLES = (
    TAX_MAP_STAGE,
    PO_NPI_STAGE,
    NPI_STAGE,
    OFFICE_STAGE,
    BATCH_NETWORKS_STAGE,
    BATCH_OFFICES_STAGE,
    BATCH_STATES_STAGE,
)

_TABLE_DDL_TEMPLATES = (
    (
        DIM_TABLE,
        """CREATE TABLE {rel} (
            specialty_id integer NOT NULL CHECK (specialty_id > 0),
            specialty_kind text COLLATE "C" NOT NULL CHECK (specialty_kind IN ('taxonomy', 'classification', 'rollup')),
            specialty_key text COLLATE "C" NOT NULL CHECK (btrim(specialty_key) <> '' AND octet_length(specialty_key) <= 256),
            display_name text NOT NULL CHECK (btrim(display_name) <> '' AND octet_length(display_name) <= 512),
            taxonomy_int_code integer
        )""",
    ),
    (
        SUMMARY_TABLE,
        """CREATE TABLE {rel} (
            network_id integer NOT NULL CHECK (network_id > 0),
            total_npi_count integer NOT NULL CHECK (total_npi_count >= 0),
            office_npi_count integer NOT NULL CHECK (office_npi_count >= 0 AND office_npi_count <= total_npi_count),
            provider_only_npi_count integer NOT NULL CHECK (
                provider_only_npi_count >= 0 AND office_npi_count + provider_only_npi_count = total_npi_count
            ),
            individual_npi_count integer NOT NULL CHECK (
                individual_npi_count >= 0 AND individual_npi_count <= total_npi_count
            ),
            organization_npi_count integer NOT NULL CHECK (
                organization_npi_count >= 0 AND individual_npi_count + organization_npi_count <= total_npi_count
            ),
            unique_address_count integer NOT NULL CHECK (unique_address_count >= 0),
            unique_location_count integer NOT NULL CHECK (
                unique_location_count >= 0 AND unique_address_count <= unique_location_count
            ),
            state_count smallint NOT NULL CHECK (
                state_count >= 0 AND state_count <= 128 AND state_count = cardinality(states)
            ),
            states text[] COLLATE "C" NOT NULL CHECK (array_position(states, NULL) IS NULL),
            core_state_count smallint NOT NULL CHECK (
                core_state_count >= 0 AND core_state_count <= state_count AND core_state_count = cardinality(core_states)
            ),
            core_states text[] COLLATE "C" NOT NULL CHECK (array_position(core_states, NULL) IS NULL),
            county_count integer NOT NULL CHECK (county_count >= 0),
            zip5_count integer NOT NULL CHECK (zip5_count >= 0)
        )""",
    ),
    (
        STATE_TABLE,
        """CREATE TABLE {rel} (
            network_id integer NOT NULL CHECK (network_id > 0),
            state_code text COLLATE "C" NOT NULL CHECK (btrim(state_code) <> '' AND octet_length(state_code) <= 8),
            is_core_state boolean NOT NULL,
            office_npi_count integer NOT NULL CHECK (office_npi_count >= 0),
            individual_npi_count integer NOT NULL CHECK (
                individual_npi_count >= 0 AND individual_npi_count <= office_npi_count
            ),
            organization_npi_count integer NOT NULL CHECK (
                organization_npi_count >= 0 AND individual_npi_count + organization_npi_count <= office_npi_count
            ),
            unique_address_count integer NOT NULL CHECK (unique_address_count >= 0),
            unique_location_count integer NOT NULL CHECK (
                unique_location_count > 0 AND unique_address_count <= unique_location_count
            ),
            county_count integer NOT NULL CHECK (county_count >= 0),
            zip5_count integer NOT NULL CHECK (zip5_count >= 0)
        )""",
    ),
    (
        SPECIALTY_TABLE,
        """CREATE TABLE {rel} (
            network_id integer NOT NULL CHECK (network_id > 0),
            specialty_id integer NOT NULL CHECK (specialty_id > 0),
            total_npi_count integer NOT NULL CHECK (
                total_npi_count >= 0 AND (total_npi_count > 0 OR unique_location_count > 0)
            ),
            office_npi_count integer NOT NULL CHECK (office_npi_count >= 0 AND office_npi_count <= total_npi_count),
            primary_office_npi_count integer NOT NULL CHECK (
                primary_office_npi_count >= 0 AND primary_office_npi_count <= office_npi_count
            ),
            unique_address_count integer NOT NULL CHECK (unique_address_count >= 0),
            unique_location_count integer NOT NULL CHECK (
                unique_location_count >= 0 AND unique_address_count <= unique_location_count
            ),
            state_count smallint NOT NULL CHECK (state_count >= 0)
        )""",
    ),
    (
        STATE_SPECIALTY_TABLE,
        """CREATE TABLE {rel} (
            network_id integer NOT NULL CHECK (network_id > 0),
            state_code text COLLATE "C" NOT NULL CHECK (btrim(state_code) <> '' AND octet_length(state_code) <= 8),
            specialty_id integer NOT NULL CHECK (specialty_id > 0),
            office_npi_count integer NOT NULL CHECK (office_npi_count >= 0),
            primary_office_npi_count integer NOT NULL CHECK (
                primary_office_npi_count >= 0 AND primary_office_npi_count <= office_npi_count
            ),
            unique_address_count integer NOT NULL CHECK (unique_address_count >= 0),
            unique_location_count integer NOT NULL CHECK (
                unique_location_count > 0 AND unique_address_count <= unique_location_count
            )
        )""",
    ),
)


def _utf8_octet_prefix_sql(expr: str, max_octets: int) -> str:
    """Truncate a trimmed SQL text expression so its UTF-8 octet_length never exceeds max_octets."""
    return (
        f"(CASE WHEN octet_length({expr}) <= {max_octets} THEN {expr} "
        f"ELSE COALESCE(NULLIF(btrim((SELECT left({expr}, max(n)) "
        f"FROM generate_series(1, {max_octets}) AS g(n) "
        f"WHERE octet_length(left({expr}, g.n)) <= {max_octets})), ''), left({expr}, 1)) END)"
    )


_CLS_OCTET_EXPR = _utf8_octet_prefix_sql("btrim(classification)", 256)
_CLS_COALESCE_OCTET_EXPR = _utf8_octet_prefix_sql("btrim(COALESCE(n.classification, ''))", 256)
_TAX_KEY_OCTET_EXPR = _utf8_octet_prefix_sql("btrim(code)", 256)
_TAX_MAP_KEY_OCTET_EXPR = _utf8_octet_prefix_sql("btrim(n.code)", 256)
_TAX_LABEL_OCTET_EXPR = _utf8_octet_prefix_sql("label_text", 512)

_NUCC_INSERT_TAX_SQL = f"""INSERT INTO {{dim}} (specialty_id, specialty_kind, specialty_key, display_name, taxonomy_int_code)
    SELECT row_number() OVER (ORDER BY code_key COLLATE "C")::integer, 'taxonomy', code_key, {_TAX_LABEL_OCTET_EXPR}, int_code
    FROM (
        SELECT DISTINCT ON ({_TAX_KEY_OCTET_EXPR} COLLATE "C")
            {_TAX_KEY_OCTET_EXPR} AS code_key,
            COALESCE(
                NULLIF(btrim(display_name), ''),
                NULLIF(btrim(specialization), ''),
                NULLIF(btrim(classification), ''),
                btrim(code)
            ) AS label_text,
            int_code
        FROM ONLY {{nucc}}
        WHERE code IS NOT NULL AND btrim(code) <> ''
        ORDER BY {_TAX_KEY_OCTET_EXPR} COLLATE "C", int_code NULLS LAST
    ) dedup_codes"""

_NUCC_INSERT_CLS_SQL = f"""INSERT INTO {{dim}} (specialty_id, specialty_kind, specialty_key, display_name, taxonomy_int_code)
    SELECT (10000 + row_number() OVER (ORDER BY cls_key COLLATE "C"))::integer,
           'classification', cls_key, cls_key, NULL::integer
    FROM (
        SELECT DISTINCT {_CLS_OCTET_EXPR} AS cls_key
        FROM ONLY {{nucc}}
        WHERE classification IS NOT NULL AND btrim(classification) <> ''
    ) dedup_cls"""

_NUCC_POPULATE_MAP_SQL = f"""INSERT INTO {{tax_map}} (int_code, code_key, specialty_ids)
    SELECT n.int_code,
           {_TAX_MAP_KEY_OCTET_EXPR} AS code_key,
           ARRAY(
               SELECT DISTINCT sid FROM unnest(ARRAY[
                   td.specialty_id,
                   cd.specialty_id,
                   CASE WHEN btrim(n.code) = ANY($1::text[]) THEN 20001 END,
                   CASE WHEN btrim(COALESCE(n.grouping, '')) = 'Behavioral Health & Social Service Providers'
                          OR btrim(COALESCE(n.classification, '')) ILIKE 'Psychiatry%' THEN 20002 END,
                   CASE WHEN btrim(COALESCE(n.grouping, '')) = 'Dental Providers' THEN 20003 END,
                   CASE WHEN btrim(COALESCE(n.grouping, '')) = 'Eye and Vision Services Providers'
                          OR btrim(n.code) LIKE '207W%' THEN 20004 END,
                   CASE WHEN btrim(COALESCE(n.grouping, '')) = 'Pharmacy Service Providers' THEN 20005 END,
                   CASE WHEN btrim(COALESCE(n.grouping, '')) = 'Hospitals' THEN 20006 END,
                   CASE WHEN btrim(n.code) = '261QU0200X' THEN 20007 END,
                   CASE WHEN btrim(COALESCE(n.grouping, '')) = 'Laboratories'
                          OR btrim(n.code) = '291U00000X' THEN 20008 END
               ]::integer[]) AS u(sid)
               WHERE sid IS NOT NULL
               ORDER BY sid
           ) AS specialty_ids
    FROM (
        SELECT DISTINCT ON ({_TAX_KEY_OCTET_EXPR} COLLATE "C") int_code, code, grouping, classification
        FROM ONLY {{nucc}}
        WHERE code IS NOT NULL AND btrim(code) <> ''
        ORDER BY {_TAX_KEY_OCTET_EXPR} COLLATE "C", int_code NULLS LAST
    ) n
    JOIN {{dim}} td ON td.specialty_kind = 'taxonomy' AND td.specialty_key = {_TAX_MAP_KEY_OCTET_EXPR}
    LEFT JOIN {{dim}} cd ON cd.specialty_kind = 'classification'
        AND cd.specialty_key = {_CLS_COALESCE_OCTET_EXPR}"""

_BATCH_COMPUTE_STATES_SQL = """WITH state_locs AS (
        SELECT network_id, state_code, count(*)::integer AS unique_location_count
        FROM {batch_offices} WHERE state_code IS NOT NULL GROUP BY network_id, state_code
    ),
    state_npis AS (
        SELECT network_id, state_code,
               count(*)::integer AS office_npi_count,
               (count(*) FILTER (WHERE entity_type = 1))::integer AS individual_npi_count,
               (count(*) FILTER (WHERE entity_type = 2))::integer AS organization_npi_count
        FROM (
            SELECT network_id, state_code, effective_npi, max(entity_type) AS entity_type
            FROM {batch_offices} WHERE state_code IS NOT NULL AND effective_npi IS NOT NULL
            GROUP BY network_id, state_code, effective_npi
        ) dedup_state_npis GROUP BY network_id, state_code
    ),
    net_office_npis AS (
        SELECT network_id, count(*)::integer AS net_npi_count
        FROM (
            SELECT network_id, effective_npi FROM {batch_offices}
            WHERE effective_npi IS NOT NULL GROUP BY network_id, effective_npi
        ) dedup_net_npis GROUP BY network_id
    ),
    state_addrs AS (
        SELECT network_id, state_code, count(*)::integer AS unique_address_count
        FROM (
            SELECT network_id, state_code, address_key FROM {batch_offices}
            WHERE state_code IS NOT NULL AND address_key IS NOT NULL
            GROUP BY network_id, state_code, address_key
        ) dedup_addrs GROUP BY network_id, state_code
    ),
    state_counties AS (
        SELECT network_id, state_code, count(*)::integer AS county_count
        FROM (
            SELECT network_id, state_code, county_fips FROM {batch_offices}
            WHERE state_code IS NOT NULL AND county_fips IS NOT NULL
            GROUP BY network_id, state_code, county_fips
        ) dedup_counties GROUP BY network_id, state_code
    ),
    state_zips AS (
        SELECT network_id, state_code, count(*)::integer AS zip5_count
        FROM (
            SELECT network_id, state_code, zip5 FROM {batch_offices}
            WHERE state_code IS NOT NULL AND zip5 IS NOT NULL
            GROUP BY network_id, state_code, zip5
        ) dedup_zips GROUP BY network_id, state_code
    )
    INSERT INTO {batch_states} (
        network_id, state_code, is_core_state, office_npi_count,
        individual_npi_count, organization_npi_count,
        unique_address_count, unique_location_count, county_count, zip5_count
    )
    SELECT
        l.network_id,
        l.state_code,
        CASE
            WHEN COALESCE(n.office_npi_count, 0) <= 0 THEN false
            ELSE COALESCE(n.office_npi_count, 0) >= LEAST(
                     100, GREATEST(10, CEIL(COALESCE(nn.net_npi_count, 0) * 0.01)::integer)
                 )
              OR COALESCE(n.office_npi_count, 0) >= CEIL(COALESCE(nn.net_npi_count, 0) * 0.20)::integer
        END AS is_core_state,
        COALESCE(n.office_npi_count, 0),
        COALESCE(n.individual_npi_count, 0),
        COALESCE(n.organization_npi_count, 0),
        COALESCE(a.unique_address_count, 0),
        l.unique_location_count,
        COALESCE(c.county_count, 0),
        COALESCE(z.zip5_count, 0)
    FROM state_locs l
    LEFT JOIN state_npis n USING (network_id, state_code)
    LEFT JOIN net_office_npis nn USING (network_id)
    LEFT JOIN state_addrs a USING (network_id, state_code)
    LEFT JOIN state_counties c USING (network_id, state_code)
    LEFT JOIN state_zips z USING (network_id, state_code)"""

_BATCH_SUMMARY_SQL = """WITH all_npis AS (
        SELECT network_id, npi, max(entity_type) AS entity_type, bool_or(has_office) AS has_office
        FROM (
            SELECT network_id, effective_npi AS npi, max(entity_type) AS entity_type, true AS has_office
            FROM {batch_offices} WHERE effective_npi IS NOT NULL GROUP BY network_id, effective_npi
            {po_arm}
        ) combined_npis GROUP BY network_id, npi
    ),
    npi_rollups AS (
        SELECT network_id,
               count(*)::integer AS total_npi_count,
               (count(*) FILTER (WHERE has_office))::integer AS office_npi_count,
               (count(*) FILTER (WHERE NOT has_office))::integer AS provider_only_npi_count,
               (count(*) FILTER (WHERE entity_type = 1))::integer AS individual_npi_count,
               (count(*) FILTER (WHERE entity_type = 2))::integer AS organization_npi_count
        FROM all_npis GROUP BY network_id
    ),
    loc_rollups AS (SELECT network_id, count(*)::integer AS unique_location_count FROM {batch_offices} GROUP BY network_id),
    addr_rollups AS (
        SELECT network_id, count(*)::integer AS unique_address_count
        FROM (SELECT network_id, address_key FROM {batch_offices} WHERE address_key IS NOT NULL GROUP BY network_id, address_key) d
        GROUP BY network_id
    ),
    county_rollups AS (
        SELECT network_id, count(*)::integer AS county_count
        FROM (SELECT network_id, county_fips FROM {batch_offices} WHERE county_fips IS NOT NULL GROUP BY network_id, county_fips) d
        GROUP BY network_id
    ),
    zip_rollups AS (
        SELECT network_id, count(*)::integer AS zip5_count
        FROM (SELECT network_id, zip5 FROM {batch_offices} WHERE zip5 IS NOT NULL GROUP BY network_id, zip5) d
        GROUP BY network_id
    ),
    state_rollups AS (
        SELECT network_id,
               count(*)::smallint AS state_count,
               array_agg(state_code ORDER BY office_npi_count DESC, state_code COLLATE "C" ASC) AS states,
               (count(*) FILTER (WHERE is_core_state))::smallint AS core_state_count,
               COALESCE(
                   array_agg(state_code ORDER BY office_npi_count DESC, state_code COLLATE "C" ASC) FILTER (WHERE is_core_state),
                   '{{}}'::text[]
               ) AS core_states
        FROM {batch_states} GROUP BY network_id
    )
    INSERT INTO {summary_table} (
        network_id, total_npi_count, office_npi_count, provider_only_npi_count,
        individual_npi_count, organization_npi_count, unique_address_count, unique_location_count,
        state_count, states, core_state_count, core_states, county_count, zip5_count
    )
    SELECT
        bn.network_id,
        COALESCE(nr.total_npi_count, 0), COALESCE(nr.office_npi_count, 0), COALESCE(nr.provider_only_npi_count, 0),
        COALESCE(nr.individual_npi_count, 0), COALESCE(nr.organization_npi_count, 0),
        COALESCE(ar.unique_address_count, 0), COALESCE(lr.unique_location_count, 0),
        COALESCE(sr.state_count, 0::smallint), COALESCE(sr.states, '{{}}'::text[]),
        COALESCE(sr.core_state_count, 0::smallint), COALESCE(sr.core_states, '{{}}'::text[]),
        COALESCE(cr.county_count, 0), COALESCE(zr.zip5_count, 0)
    FROM {batch_nets} bn
    LEFT JOIN npi_rollups nr USING (network_id)
    LEFT JOIN loc_rollups lr USING (network_id)
    LEFT JOIN addr_rollups ar USING (network_id)
    LEFT JOIN state_rollups sr USING (network_id)
    LEFT JOIN county_rollups cr USING (network_id)
    LEFT JOIN zip_rollups zr USING (network_id)"""

_INSERT_STATE_SPECIALTY_SQL = """WITH loc_groups AS (
        SELECT network_id, state_code, any_specialty_ids, count(*)::integer AS loc_count
        FROM {batch_offices} WHERE state_code IS NOT NULL AND cardinality(any_specialty_ids) > 0
        GROUP BY network_id, state_code, any_specialty_ids
    ),
    state_spec_locs AS (
        SELECT lg.network_id, lg.state_code, s.specialty_id, sum(lg.loc_count)::integer AS unique_location_count
        FROM loc_groups lg CROSS JOIN LATERAL unnest(lg.any_specialty_ids) AS s(specialty_id)
        GROUP BY lg.network_id, lg.state_code, s.specialty_id
    ),
    state_spec_addrs AS (
        SELECT network_id, state_code, specialty_id, count(*)::integer AS unique_address_count
        FROM (
            SELECT ag.network_id, ag.state_code, ag.address_key, s.specialty_id
            FROM (
                SELECT network_id, state_code, address_key, any_specialty_ids FROM {batch_offices}
                WHERE state_code IS NOT NULL AND address_key IS NOT NULL AND cardinality(any_specialty_ids) > 0
                GROUP BY network_id, state_code, address_key, any_specialty_ids
            ) ag CROSS JOIN LATERAL unnest(ag.any_specialty_ids) AS s(specialty_id)
            GROUP BY ag.network_id, ag.state_code, ag.address_key, s.specialty_id
        ) d GROUP BY network_id, state_code, specialty_id
    ),
    state_spec_npis AS (
        SELECT network_id, state_code, specialty_id,
               count(*)::integer AS office_npi_count,
               (count(*) FILTER (WHERE is_primary))::integer AS primary_office_npi_count
        FROM (
            SELECT ng.network_id, ng.state_code, ng.effective_npi, s.specialty_id,
                   bool_or(s.specialty_id = ANY(ng.primary_specialty_ids)) AS is_primary
            FROM (
                SELECT network_id, state_code, effective_npi, any_specialty_ids, primary_specialty_ids FROM {batch_offices}
                WHERE state_code IS NOT NULL AND effective_npi IS NOT NULL AND cardinality(any_specialty_ids) > 0
                GROUP BY network_id, state_code, effective_npi, any_specialty_ids, primary_specialty_ids
            ) ng CROSS JOIN LATERAL unnest(ng.any_specialty_ids) AS s(specialty_id)
            GROUP BY ng.network_id, ng.state_code, ng.effective_npi, s.specialty_id
        ) d GROUP BY network_id, state_code, specialty_id
    )
    INSERT INTO {state_spec_table} (
        network_id, state_code, specialty_id, office_npi_count, primary_office_npi_count,
        unique_address_count, unique_location_count
    )
    SELECT l.network_id, l.state_code, l.specialty_id,
           COALESCE(n.office_npi_count, 0), COALESCE(n.primary_office_npi_count, 0),
           COALESCE(a.unique_address_count, 0), l.unique_location_count
    FROM state_spec_locs l
    LEFT JOIN state_spec_npis n USING (network_id, state_code, specialty_id)
    LEFT JOIN state_spec_addrs a USING (network_id, state_code, specialty_id)"""

_INSERT_NET_SPECIALTY_SQL = """WITH loc_by_state AS (
        SELECT lg.network_id, lg.state_code, s.specialty_id, sum(lg.loc_count)::integer AS state_loc_count
        FROM (
            SELECT network_id, state_code, any_specialty_ids, count(*)::integer AS loc_count
            FROM {batch_offices} WHERE cardinality(any_specialty_ids) > 0
            GROUP BY network_id, state_code, any_specialty_ids
        ) lg CROSS JOIN LATERAL unnest(lg.any_specialty_ids) AS s(specialty_id)
        GROUP BY lg.network_id, lg.state_code, s.specialty_id
    ),
    net_spec_locs AS (
        SELECT network_id, specialty_id,
               sum(state_loc_count)::integer AS unique_location_count,
               (count(*) FILTER (WHERE state_code IS NOT NULL))::smallint AS state_count
        FROM loc_by_state GROUP BY network_id, specialty_id
    ),
    net_spec_addrs AS (
        SELECT network_id, specialty_id, count(*)::integer AS unique_address_count
        FROM (
            SELECT ag.network_id, ag.address_key, s.specialty_id
            FROM (
                SELECT network_id, address_key, any_specialty_ids FROM {batch_offices}
                WHERE address_key IS NOT NULL AND cardinality(any_specialty_ids) > 0
                GROUP BY network_id, address_key, any_specialty_ids
            ) ag CROSS JOIN LATERAL unnest(ag.any_specialty_ids) AS s(specialty_id)
            GROUP BY ag.network_id, ag.address_key, s.specialty_id
        ) d GROUP BY network_id, specialty_id
    ),
    dedup_net_spec_npis AS (
        SELECT ng.network_id, ng.effective_npi, s.specialty_id,
               bool_or(s.specialty_id = ANY(ng.primary_specialty_ids)) AS is_primary
        FROM (
            SELECT network_id, effective_npi, any_specialty_ids, primary_specialty_ids FROM {batch_offices}
            WHERE effective_npi IS NOT NULL AND cardinality(any_specialty_ids) > 0
            GROUP BY network_id, effective_npi, any_specialty_ids, primary_specialty_ids
        ) ng CROSS JOIN LATERAL unnest(ng.any_specialty_ids) AS s(specialty_id)
        GROUP BY ng.network_id, ng.effective_npi, s.specialty_id
    ),
    net_spec_office_npis AS (
        SELECT network_id, specialty_id,
               count(*)::integer AS office_npi_count,
               (count(*) FILTER (WHERE is_primary))::integer AS primary_office_npi_count
        FROM dedup_net_spec_npis GROUP BY network_id, specialty_id
    ){po_spec_cte}
    INSERT INTO {spec_table} (
        network_id, specialty_id, total_npi_count, office_npi_count, primary_office_npi_count,
        unique_address_count, unique_location_count, state_count
    )
    SELECT COALESCE(l.network_id, tn.network_id), COALESCE(l.specialty_id, tn.specialty_id),
           COALESCE(tn.total_npi_count, 0), COALESCE(onp.office_npi_count, 0),
           COALESCE(onp.primary_office_npi_count, 0), COALESCE(a.unique_address_count, 0),
           COALESCE(l.unique_location_count, 0), COALESCE(l.state_count, 0::smallint)
    FROM net_spec_locs l
    FULL OUTER JOIN total_spec_npis tn USING (network_id, specialty_id)
    LEFT JOIN net_spec_office_npis onp
        ON onp.network_id = COALESCE(l.network_id, tn.network_id)
       AND onp.specialty_id = COALESCE(l.specialty_id, tn.specialty_id)
    LEFT JOIN net_spec_addrs a
        ON a.network_id = COALESCE(l.network_id, tn.network_id)
       AND a.specialty_id = COALESCE(l.specialty_id, tn.specialty_id)
    WHERE COALESCE(tn.total_npi_count, 0) > 0 OR COALESCE(l.unique_location_count, 0) > 0"""


def create_stats_tables_sql(schema_name: str) -> tuple[str, ...]:
    """DDL statements creating the five unindexed serving tables in candidate storage."""
    ns = compact_identifier(schema_name)
    return tuple(template.format(rel=f"{ns}.{compact_identifier(name)}") for name, template in _TABLE_DDL_TEMPLATES)


def create_stats_indexes_sql(schema_name: str) -> tuple[str, ...]:
    """DDL statements adding primary keys and secondary btree indexes after bulk population."""
    ns = compact_identifier(schema_name)
    dim = f"{ns}.{compact_identifier(DIM_TABLE)}"
    summary = f"{ns}.{compact_identifier(SUMMARY_TABLE)}"
    state = f"{ns}.{compact_identifier(STATE_TABLE)}"
    spec = f"{ns}.{compact_identifier(SPECIALTY_TABLE)}"
    state_spec = f"{ns}.{compact_identifier(STATE_SPECIALTY_TABLE)}"
    return (
        f"ALTER TABLE {dim} ADD PRIMARY KEY (specialty_id)",
        f"CREATE UNIQUE INDEX {DIM_TABLE}_key_idx ON {dim} (specialty_kind, specialty_key)",
        f"ALTER TABLE {summary} ADD PRIMARY KEY (network_id)",
        f"ALTER TABLE {state} ADD PRIMARY KEY (network_id, state_code)",
        f"CREATE INDEX {STATE_TABLE}_code_idx ON {state} (state_code, network_id)",
        f"ALTER TABLE {spec} ADD PRIMARY KEY (network_id, specialty_id)",
        f"CREATE INDEX {SPECIALTY_TABLE}_id_idx ON {spec} (specialty_id, network_id)",
        f"ALTER TABLE {state_spec} ADD PRIMARY KEY (network_id, state_code, specialty_id)",
        f"CREATE INDEX {STATE_SPECIALTY_TABLE}_lookup_idx ON {state_spec} (state_code, specialty_id, network_id)",
    )


def nucc_dim_and_map_sql(schema_name: str, nucc_relation: str) -> tuple[str, str, str]:
    """SQL statements populating dimension entries and taxonomy-to-specialty array map from NUCC."""
    ns = compact_identifier(schema_name)
    dim = f"{ns}.{compact_identifier(DIM_TABLE)}"
    tax_map = f"{ns}.{compact_identifier(TAX_MAP_STAGE)}"
    return (
        _NUCC_INSERT_TAX_SQL.format(dim=dim, nucc=nucc_relation),
        _NUCC_INSERT_CLS_SQL.format(dim=dim, nucc=nucc_relation),
        _NUCC_POPULATE_MAP_SQL.format(dim=dim, tax_map=tax_map, nucc=nucc_relation),
    )


def fallback_dim_and_map_sql(schema_name: str, geo_table: str) -> tuple[str, str]:
    """Fallback dimension population from integer taxonomy codes when nucc_taxonomy is absent."""
    ns = compact_identifier(schema_name)
    dim = f"{ns}.{compact_identifier(DIM_TABLE)}"
    tax_map = f"{ns}.{compact_identifier(TAX_MAP_STAGE)}"
    insert_fallback = f"""INSERT INTO {dim} (specialty_id, specialty_kind, specialty_key, display_name, taxonomy_int_code)
        SELECT (COALESCE((SELECT max(specialty_id) FROM {dim} WHERE specialty_kind = 'taxonomy'), 0)
                + row_number() OVER (ORDER BY code_int))::integer,
               'taxonomy', code_int::text, 'Taxonomy ' || code_int::text, code_int
        FROM (
            SELECT DISTINCT code_int FROM ONLY {geo_table} g
            CROSS JOIN LATERAL unnest(g.taxonomy_array) AS u(code_int)
            WHERE code_int IS NOT NULL AND code_int <> 0
              AND NOT EXISTS (
                  SELECT 1 FROM {dim} existing
                  WHERE existing.specialty_kind = 'taxonomy' AND existing.taxonomy_int_code = code_int
              )
        ) missing_codes"""
    insert_fallback_map = f"""INSERT INTO {tax_map} (int_code, code_key, specialty_ids)
        SELECT d.taxonomy_int_code, d.specialty_key, ARRAY[d.specialty_id]::integer[]
        FROM {dim} d
        WHERE d.specialty_kind = 'taxonomy' AND d.taxonomy_int_code IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM {tax_map} m WHERE m.int_code = d.taxonomy_int_code)"""
    return insert_fallback, insert_fallback_map


def office_stage_sql(schema_name: str, geo_table: str, county_expr: str, county_join: str) -> str:
    """SQL populating unlogged _stats_office_stage with deduplicated specialty ID arrays."""
    ns = compact_identifier(schema_name)
    office_stage = f"{ns}.{compact_identifier(OFFICE_STAGE)}"
    npi_stage = f"{ns}.{compact_identifier(NPI_STAGE)}"
    tax_map = f"{ns}.{compact_identifier(TAX_MAP_STAGE)}"
    return f"""CREATE UNLOGGED TABLE {office_stage} AS
        SELECT
            g.location_key COLLATE "C" AS location_key,
            COALESCE(g.npi, g.inferred_npi) AS effective_npi,
            g.address_key,
            CASE
                WHEN g.state_name IS NOT NULL AND upper(btrim(g.state_name)) ~ '^[A-Z0-9]{{2,8}}$'
                THEN upper(btrim(g.state_name)) COLLATE "C"
            END AS state_code,
            CASE
                WHEN COALESCE(NULLIF(btrim(g.zip5), ''), left(COALESCE(btrim(g.postal_code), ''), 5)) ~ '^[0-9]{{5}}$'
                THEN COALESCE(NULLIF(btrim(g.zip5), ''), left(COALESCE(btrim(g.postal_code), ''), 5)) COLLATE "C"
            END AS zip5,
            {county_expr} AS county_fips,
            COALESCE(ns.entity_type, 0::smallint) AS entity_type,
            COALESCE(spec.any_specialty_ids, '{{}}'::integer[]) AS any_specialty_ids,
            COALESCE(ns.primary_specialty_ids, '{{}}'::integer[]) AS primary_specialty_ids
        FROM ONLY {geo_table} g
        {county_join}
        LEFT JOIN {npi_stage} ns ON ns.npi = COALESCE(g.npi, g.inferred_npi)
        LEFT JOIN LATERAL (
            SELECT array_agg(DISTINCT sid ORDER BY sid) AS any_specialty_ids
            FROM unnest(g.taxonomy_array) AS raw_code(code_int)
            JOIN {tax_map} tm ON tm.int_code = raw_code.code_int
            CROSS JOIN LATERAL unnest(tm.specialty_ids) AS mapped(sid)
            WHERE raw_code.code_int IS NOT NULL AND raw_code.code_int <> 0
        ) spec ON TRUE
        WHERE g.type IN ('primary', 'secondary', 'practice', 'site')"""


def batch_state_insert_sql(schema_name: str) -> tuple[str, str]:
    """SQL populating _stats_batch_states with pure HashAggregate plans and copying to serving."""
    ns = compact_identifier(schema_name)
    batch_offices = f"{ns}.{compact_identifier(BATCH_OFFICES_STAGE)}"
    batch_states = f"{ns}.{compact_identifier(BATCH_STATES_STAGE)}"
    state_table = f"{ns}.{compact_identifier(STATE_TABLE)}"
    return (
        _BATCH_COMPUTE_STATES_SQL.format(batch_offices=batch_offices, batch_states=batch_states),
        f"INSERT INTO {state_table} SELECT * FROM {batch_states}",
    )


def batch_summary_insert_sql(schema_name: str, *, has_provider_only: bool) -> str:
    """SQL populating network_stats_summary for one bounded network ID batch."""
    ns = compact_identifier(schema_name)
    po_stage = f"{ns}.{compact_identifier(PO_NPI_STAGE)}"
    npi_stage = f"{ns}.{compact_identifier(NPI_STAGE)}"
    po_arm = (
        f"""UNION ALL
            SELECT p.network_id, p.npi, COALESCE(ns.entity_type, 0::smallint) AS entity_type, false AS has_office
            FROM {po_stage} p LEFT JOIN {npi_stage} ns USING (npi)
            WHERE p.network_id BETWEEN $1::integer AND $2::integer"""
        if has_provider_only
        else ""
    )
    return _BATCH_SUMMARY_SQL.format(
        batch_nets=f"{ns}.{compact_identifier(BATCH_NETWORKS_STAGE)}",
        batch_offices=f"{ns}.{compact_identifier(BATCH_OFFICES_STAGE)}",
        batch_states=f"{ns}.{compact_identifier(BATCH_STATES_STAGE)}",
        summary_table=f"{ns}.{compact_identifier(SUMMARY_TABLE)}",
        po_arm=po_arm,
    )


def batch_specialty_inserts_sql(schema_name: str, *, has_provider_only: bool) -> tuple[str, str]:
    """SQL populating network_stats_state_specialty and network_stats_specialty via HashAggregate."""
    ns = compact_identifier(schema_name)
    batch_offices = f"{ns}.{compact_identifier(BATCH_OFFICES_STAGE)}"
    po_stage = f"{ns}.{compact_identifier(PO_NPI_STAGE)}"
    npi_stage = f"{ns}.{compact_identifier(NPI_STAGE)}"
    po_spec_cte = (
        f""", po_spec_npis AS (
            SELECT p.network_id, p.npi, s.specialty_id
            FROM {po_stage} p JOIN {npi_stage} ns USING (npi)
            CROSS JOIN LATERAL unnest(ns.npi_specialty_ids) AS s(specialty_id)
            WHERE p.network_id BETWEEN $1::integer AND $2::integer AND cardinality(ns.npi_specialty_ids) > 0
            GROUP BY p.network_id, p.npi, s.specialty_id
        ),
        total_spec_npis AS (
            SELECT network_id, specialty_id, count(*)::integer AS total_npi_count
            FROM (
                SELECT network_id, effective_npi AS npi, specialty_id FROM dedup_net_spec_npis
                UNION SELECT network_id, npi, specialty_id FROM po_spec_npis
            ) u GROUP BY network_id, specialty_id
        )"""
        if has_provider_only
        else """, total_spec_npis AS (
            SELECT network_id, specialty_id, office_npi_count AS total_npi_count FROM net_spec_office_npis
        )"""
    )
    return (
        _INSERT_STATE_SPECIALTY_SQL.format(
            batch_offices=batch_offices,
            state_spec_table=f"{ns}.{compact_identifier(STATE_SPECIALTY_TABLE)}",
        ),
        _INSERT_NET_SPECIALTY_SQL.format(
            batch_offices=batch_offices,
            spec_table=f"{ns}.{compact_identifier(SPECIALTY_TABLE)}",
            po_spec_cte=po_spec_cte,
        ),
    )


def invariant_anti_join_sql(schema_name: str) -> str:
    """Set-based anti-join and consistency verification SQL across all five stats tables."""
    ns = compact_identifier(schema_name)
    dim = f"{ns}.{compact_identifier(DIM_TABLE)}"
    summary = f"{ns}.{compact_identifier(SUMMARY_TABLE)}"
    state = f"{ns}.{compact_identifier(STATE_TABLE)}"
    spec = f"{ns}.{compact_identifier(SPECIALTY_TABLE)}"
    state_spec = f"{ns}.{compact_identifier(STATE_SPECIALTY_TABLE)}"
    return f"""SELECT jsonb_build_object(
        'orphan_specialty_rows', EXISTS (
            SELECT 1 FROM {spec} s LEFT JOIN {dim} d USING (specialty_id) WHERE d.specialty_id IS NULL
        ),
        'orphan_state_specialty_rows', EXISTS (
            SELECT 1 FROM {state_spec} ss LEFT JOIN {dim} d USING (specialty_id) WHERE d.specialty_id IS NULL
        ),
        'orphan_state_networks', EXISTS (
            SELECT 1 FROM {state} st LEFT JOIN {summary} sm USING (network_id) WHERE sm.network_id IS NULL
        ),
        'orphan_specialty_networks', EXISTS (
            SELECT 1 FROM {spec} sp LEFT JOIN {summary} sm USING (network_id) WHERE sm.network_id IS NULL
        ),
        'orphan_state_specialty_states', EXISTS (
            SELECT 1 FROM {state_spec} ss LEFT JOIN {state} st USING (network_id, state_code) WHERE st.network_id IS NULL
        ),
        'state_count_mismatch', EXISTS (
            SELECT 1 FROM {summary} sm LEFT JOIN (
                SELECT network_id, count(*)::smallint AS actual_states,
                       (count(*) FILTER (WHERE is_core_state))::smallint AS actual_core
                FROM {state} GROUP BY network_id
            ) agg USING (network_id)
            WHERE sm.state_count <> COALESCE(agg.actual_states, 0::smallint)
               OR sm.core_state_count <> COALESCE(agg.actual_core, 0::smallint)
        ),
        'leftover_stage_tables', EXISTS (
            SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = $1::text AND starts_with(tablename, '_stats_')
        ),
        'specialty_dim_rows', (SELECT count(*)::bigint FROM {dim}),
        'summary_rows', (SELECT count(*)::bigint FROM {summary}),
        'state_rows', (SELECT count(*)::bigint FROM {state}),
        'specialty_rows', (SELECT count(*)::bigint FROM {spec}),
        'state_specialty_rows', (SELECT count(*)::bigint FROM {state_spec})
    )::text"""
