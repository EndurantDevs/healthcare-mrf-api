"""Core SQLAlchemy table definitions used during migration."""

import os

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    TIMESTAMP,
)
from sqlalchemy.dialects.postgresql import ARRAY


_METADATA = MetaData()
DEFAULT_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
TIGER_SCHEMA = os.getenv("HLTHPRT_TIGER_SCHEMA", "tiger")

issuer_table = Table(
    "issuer",
    _METADATA,
    Column("issuer_id", Integer),
    Column("state", String(2)),
    Column("issuer_name", String),
    Column("issuer_marketing_name", String),
    Column("mrf_url", String),
    Column("data_contact_email", String),
    schema=DEFAULT_SCHEMA,
)

zip_state_table = Table(
    "zip_state",
    _METADATA,
    Column("zip", String),
    Column("stusps", String(2)),
    Column("statefp", Integer),
    schema=TIGER_SCHEMA,
)

zip_zcta5_table = Table(
    "zcta5",
    _METADATA,
    Column("zcta5ce", String),
    Column("intptlat", String),
    Column("intptlon", String),
    Column("statefp", String),
    schema=TIGER_SCHEMA,
)

plan_table = Table(
    "plan",
    _METADATA,
    Column("plan_id", String(14)),
    Column("year", Integer),
    Column("issuer_id", Integer),
    Column("state", String(2)),
    Column("plan_id_type", String),
    Column("marketing_name", String),
    Column("summary_url", String),
    Column("marketing_url", String),
    Column("formulary_url", String),
    Column("plan_contact", String),
    Column("network", ARRAY(String)),
    Column("benefits", ARRAY(JSON)),
    Column("last_updated_on", TIMESTAMP),
    Column("checksum", Integer),
    schema=DEFAULT_SCHEMA,
)

plan_attributes_table = Table(
    "plan_attributes",
    _METADATA,
    Column("plan_id", String(14)),
    Column("full_plan_id", String(17)),
    Column("year", Integer),
    Column("attr_name", String),
    Column("attr_value", String),
    schema=DEFAULT_SCHEMA,
)

plan_benefits_table = Table(
    "plan_benefits",
    _METADATA,
    Column("plan_id", String(14)),
    Column("full_plan_id", String(17)),
    Column("year", Integer),
    Column("benefit_name", String),
    Column("copay_inn_tier1", String),
    Column("copay_inn_tier2", String),
    Column("copay_outof_net", String),
    Column("coins_inn_tier1", String),
    Column("coins_inn_tier2", String),
    Column("coins_outof_net", String),
    Column("is_ehb", Boolean),
    Column("is_covered", Boolean),
    Column("quant_limit_on_svc", Boolean),
    Column("limit_qty", Float),
    Column("limit_unit", String),
    Column("exclusions", String),
    Column("explanation", String),
    Column("ehb_var_reason", String),
    Column("is_excl_from_inn_mo", Boolean),
    Column("is_excl_from_oon_mo", Boolean),
    schema=DEFAULT_SCHEMA,
)

plan_prices_table = Table(
    "plan_prices",
    _METADATA,
    Column("plan_id", String(14)),
    Column("year", Integer),
    Column("state", String(2)),
    Column("checksum", Integer),
    Column("rating_area_id", String),
    Column("min_age", Integer),
    Column("max_age", Integer),
    Column("couple", Boolean),
    Column("individual_rate", Float),
    Column("individual_rate_0_14", Float),
    Column("individual_rate_15_20", Float),
    Column("individual_rate_21", Float),
    Column("individual_rate_27", Float),
    Column("individual_rate_30", Float),
    Column("individual_rate_40", Float),
    Column("individual_rate_50", Float),
    Column("individual_rate_64", Float),
    schema=DEFAULT_SCHEMA,
)

plan_npi_table = Table(
    "plan_npi",
    _METADATA,
    Column("plan_id", String(14)),
    Column("year", Integer),
    Column("issuer_id", Integer),
    Column("checksum_network", Integer),
    schema=DEFAULT_SCHEMA,
)

plan_formulary_table = Table(
    "plan_formulary",
    _METADATA,
    Column("plan_id", String(14)),
    Column("year", Integer),
    Column("drug_tier", String),
    Column("mail_order", Boolean),
    Column("pharmacy_type", String),
    Column("copay_amount", Float),
    Column("copay_opt", String),
    Column("coinsurance_rate", Float),
    Column("coinsurance_opt", String),
    schema=DEFAULT_SCHEMA,
)

plan_network_tier_table = Table(
    "plan_networktier",
    _METADATA,
    Column("plan_id", String(14)),
    Column("network_tier", String),
    Column("issuer_id", Integer),
    Column("year", Integer),
    Column("checksum_network", Integer),
    schema=DEFAULT_SCHEMA,
)

import_log_table = Table(
    "log",
    _METADATA,
    Column("issuer_id", Integer),
    Column("checksum", Integer),
    Column("type", String(4)),
    Column("text", String),
    Column("url", String),
    Column("source", String),
    Column("level", String),
    schema=DEFAULT_SCHEMA,
)
