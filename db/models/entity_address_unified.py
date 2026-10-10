# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unified location addresses and their native serving index definitions."""

import os

from sqlalchemy import (
    ARRAY,
    DATE,
    SMALLINT,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from db.connection import Base
from db.json_mixin import JSONOutputMixin

ENTITY_ADDRESS_UNIFIED_SERVING_STAGE_INDEXES = {
    "npi",
    "primary_npi",
    "coalesced_npi",
    "primary_state_city_npi",
    "primary_zip5_npi",
    "serving_zip5_npi",
    "serving_zip5_taxonomy",
    "primary_phone_npi",
    "service_phone_lookup_npi",
    "service_phone_digits_npi",
    "service_phone_number_npi",
    "service_address_key_npi",
    "service_premise_key_npi",
    "address_sources",
    # The phone fallback filters "address_key = ANY(..) OR premise_key = ANY(..)";
    # without a premise_key index the OR scans the serving table.
    "premise_key",
    "taxonomy_plans_network",
    "service_plans_network_array",
    "canonical_network_ids",
    "procedures_array",
    "medications_array",
    "geo_idx",
    "geo_taxonomy",
    "geo_bbox",
    "address_key",
}


class EntityAddressUnified(Base, JSONOutputMixin):
    __tablename__ = "entity_address_unified"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("location_key"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["location_key"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi",), "name": "npi"},
        {"index_elements": ("npi",), "name": "primary_npi", "where": "type='primary'"},
        {"index_elements": ("inferred_npi",), "name": "inferred_npi", "where": "inferred_npi IS NOT NULL"},
        {"index_elements": ("coalesce(npi, inferred_npi)",), "name": "coalesced_npi"},
        {"index_elements": ("entity_type", "coalesce(npi, inferred_npi)"), "name": "entity_type_coalesced_npi"},
        {
            "index_elements": ("state_name", "city_name", "npi"),
            "name": "primary_state_city_npi",
            "where": "type='primary'",
        },
        {"index_elements": ("zip5", "npi"), "name": "primary_zip5_npi", "where": "type='primary'"},
        # Serving-type ZIP lookup for group-plan provider enumeration: the
        # expression must match the query's zip5 fallback exactly so the
        # planner can use it, and the WHERE mirrors the serving address types.
        {
            "index_elements": (
                "(COALESCE(zip5, LEFT(COALESCE(postal_code, ''), 5)))",
                "npi",
            ),
            "name": "serving_zip5_npi",
            "where": "type IN ('practice','site','primary','secondary')",
        },
        {
            "index_elements": (
                "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
                "npi",
            ),
            "name": "primary_phone_digits_npi",
            "where": "type='primary'",
        },
        {
            "index_elements": ("telephone_number", "npi"),
            "name": "primary_phone_npi",
            "where": "type='primary' AND telephone_number IS NOT NULL AND telephone_number <> ''",
        },
        {
            "index_elements": ("phone_number", "npi"),
            "name": "primary_phone_number_npi",
            "where": "type='primary' AND phone_number IS NOT NULL AND phone_number <> ''",
        },
        {
            "index_elements": (
                "COALESCE(NULLIF(phone_number, ''), regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g'))",
                "npi",
            ),
            "name": "service_phone_lookup_npi",
            "where": (
                "type IN ('primary', 'secondary', 'practice', 'site') "
                "AND COALESCE(NULLIF(phone_number, ''), regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')) <> ''"
            ),
        },
        {
            "index_elements": (
                "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
                "npi",
            ),
            "name": "service_phone_digits_npi",
            "where": (
                "type IN ('primary', 'secondary', 'practice', 'site') "
                "AND regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g') <> ''"
            ),
        },
        {
            "index_elements": ("phone_number", "npi"),
            "name": "service_phone_number_npi",
            "where": (
                "type IN ('primary', 'secondary', 'practice', 'site') "
                "AND phone_number IS NOT NULL AND phone_number <> ''"
            ),
        },
        {
            "index_elements": ("address_key", "npi"),
            "name": "service_address_key_npi",
            "where": "type IN ('primary', 'secondary', 'practice', 'site') AND address_key IS NOT NULL",
        },
        {
            "index_elements": ("premise_key", "npi"),
            "name": "service_premise_key_npi",
            "where": "type IN ('primary', 'secondary', 'practice', 'site') AND premise_key IS NOT NULL",
        },
        {"index_elements": ("address_sources",), "using": "gin", "name": "address_sources"},
        {"index_elements": ("row_origin",), "name": "row_origin"},
        {"index_elements": ("address_precision",), "name": "address_precision"},
        {"index_elements": ("archive_identity_version",), "name": "archive_identity_version"},
        {"index_elements": ("premise_key",), "name": "premise_key"},
        {"index_elements": ("zip5",), "name": "zip5"},
        {"index_elements": ("state_code", "city_norm"), "name": "state_city_norm"},
        {"index_elements": ("ptg_plan_array",), "using": "gin", "name": "ptg_plan_array"},
        {"index_elements": ("ptg_source_array",), "using": "gin", "name": "ptg_source_array"},
        {"index_elements": ("group_plan_array",), "using": "gin", "name": "group_plan_array"},
        {
            "index_elements": ("canonical_network_ids gin__int_ops",),
            "using": "gin",
            "name": "canonical_network_ids",
        },
        {
            "index_elements": ("taxonomy_array gin__int_ops", "plans_network_array gin__int_ops"),
            "using": "gin",
            "name": "taxonomy_plans_network",
            "where": "type='primary'",
        },
        {
            "index_elements": ("plans_network_array gin__int_ops",),
            "using": "gin",
            "name": "service_plans_network_array",
            "where": "type IN ('primary', 'secondary', 'practice', 'site')",
        },
        # ZIP+taxonomy (btree_gin) keeps both predicates in one Index Cond; standalone taxonomy GIN scans broadly.
        {
            "index_elements": (
                "(COALESCE(zip5, LEFT(COALESCE(postal_code, ''), 5)))",
                "taxonomy_array gin__int_ops",
            ),
            "using": "gin",
            "name": "serving_zip5_taxonomy",
            "where": "type IN ('practice','site','primary','secondary')",
        },
        {
            "index_elements": ("procedures_array gin__int_ops",),
            "using": "gin",
            "name": "procedures_array",
            "where": "type='primary'",
        },
        {
            "index_elements": ("medications_array gin__int_ops",),
            "using": "gin",
            "name": "medications_array",
            "where": "type='primary'",
        },
        {
            # Geo GiST index over every geocoded SERVICE location, not just the
            # NPPES primary/secondary -- so a radius search finds providers at their
            # TiC/PTG and ACA practice/site locations too. city_zip-precision rows
            # have no exact point and are intentionally excluded. The query-side type
            # filter (api/endpoint/npi.py geo path) MUST match this predicate for the
            # index to be used; widening the geo query is gated on a rebuild that
            # recreates this partial index (CREATE INDEX IF NOT EXISTS on a fresh
            # stage table, _create_stage_indexes), so the index lands before the
            # query that depends on it is activated.
            "index_elements": ("Geography(ST_MakePoint((long)::double precision, (lat)::double precision))",),
            "using": "gist",
            "name": "geo_idx",
            "where": (
                "type IN ('primary', 'secondary', 'practice', 'site') "
                "AND COALESCE(address_precision, '') <> 'city_zip' "
                "AND lat IS NOT NULL AND long IS NOT NULL"
            ),
        },
        {
            # Let radius+taxonomy searches apply both selective predicates in
            # one GiST scan instead of filtering every address in the radius.
            "index_elements": (
                "public.Geography(public.ST_MakePoint((long)::double precision, (lat)::double precision))",
                "taxonomy_array public.gist__intbig_ops",
            ),
            "using": "gist",
            "name": "geo_taxonomy",
            "where": (
                "type IN ('primary', 'secondary', 'practice', 'site') "
                "AND COALESCE(address_precision, '') <> 'city_zip' "
                "AND lat IS NOT NULL AND long IS NOT NULL"
            ),
        },
        {
            # Cheaper serving index for /npi/near/. The query applies a lat/long
            # bounding box before exact ST_DWithin distance filtering, so this
            # preserves exact results while avoiding the full GiST build on each
            # serving-only refresh. The GiST geo_idx stays available in the full
            # index profile for workloads that need expression-index radius scans.
            "index_elements": ("lat", "long"),
            "include": ("npi", "address_key"),
            "name": "geo_bbox",
            "where": (
                "type IN ('primary', 'secondary', 'practice', 'site') "
                "AND COALESCE(address_precision, '') <> 'city_zip' "
                "AND lat IS NOT NULL AND long IS NOT NULL"
            ),
        },
        {"index_elements": ("address_key",), "name": "address_key"},
    ]

    entity_type = Column(String(64), nullable=False)
    entity_id = Column(String(128), nullable=False)
    npi = Column(BigInteger)
    inferred_npi = Column(BigInteger)
    inference_confidence = Column(Float)
    inference_method = Column(String(64))
    entity_name = Column(String(256))
    entity_subtype = Column(String(64))
    location_key = Column(String(64), nullable=False)
    row_origin = Column(String(32), nullable=False, server_default="base")
    archive_identity_version = Column(String(16), nullable=False, server_default="v2")
    address_precision = Column(String(32), nullable=False, server_default="unknown")
    premise_key = Column(PG_UUID(as_uuid=True))
    zip5 = Column(String(5))
    state_code = Column(String(2))
    city_norm = Column(String)
    county_fips = Column(String(5))
    source_mask = Column(BigInteger, nullable=False, server_default="0")
    address_source_mask = Column(BigInteger, nullable=False, server_default="0")
    geo_evidence_source_id = Column(SMALLINT)
    geo_identity_coherent = Column(Boolean)
    geo_point_coherent = Column(Boolean)
    geo_assurance_version = Column(SMALLINT)
    source_count = Column(Integer, nullable=False, server_default="0")
    independent_source_count = Column(Integer, nullable=False, server_default="0")
    multi_source_confirmed = Column(Boolean, nullable=False, server_default="false")
    location_confidence_id = Column(SMALLINT, nullable=False, server_default="0")
    confidence_score = Column(SMALLINT)
    freshness_score = Column(SMALLINT)
    address_sources = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    source_record_ids = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    aca_plan_array = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    aca_network_array = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    ptg_plan_array = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    ptg_source_array = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    group_plan_array = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    base_address_version = Column(String(64))

    checksum = Column(BigInteger, nullable=False)
    type = Column(String(32), nullable=False)
    taxonomy_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    canonical_network_ids = Column(ARRAY(Integer), nullable=False, server_default=text("'{}'::integer[]"))
    plans_network_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    procedures_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    medications_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")

    first_line = Column(String)
    second_line = Column(String)
    city_name = Column(String)
    state_name = Column(String)
    postal_code = Column(String)
    country_code = Column(String)
    telephone_number = Column(String)
    phone_number = Column(String(15))
    phone_extension = Column(String(16))
    fax_number = Column(String)
    fax_number_digits = Column(String(15))
    fax_extension = Column(String(16))
    formatted_address = Column(String)
    formatted_address_version = Column(SMALLINT)
    formatted_address_source = Column(String(32))
    lat = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    long = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    date_added = Column(DATE)
    place_id = Column(String)
    updated_at = Column(DateTime)
    last_seen_at = Column(DateTime)
    address_key = Column(PG_UUID(as_uuid=True))
