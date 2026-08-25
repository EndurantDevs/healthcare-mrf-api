# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL variants for the hospital-price PostgreSQL schema bakeoff."""

from __future__ import annotations

from pathlib import Path

from scripts.research.hospital_hpt_corpus import PriceFact

CANDIDATES = ("typed", "dictionary", "blocks")
FACT_COLUMNS = tuple(PriceFact.__dataclass_fields__)
COMMON_DDL = """
CREATE TABLE {schema}.facility_anchor (
    id text PRIMARY KEY, name text NOT NULL
);
CREATE TABLE {schema}.hospital_registry (
    hospital_id text PRIMARY KEY,
    facility_anchor_id text NOT NULL REFERENCES {schema}.facility_anchor(id)
);
CREATE TABLE {schema}.hospital_mrf_metadata (
    hospital_id text PRIMARY KEY REFERENCES {schema}.hospital_registry(hospital_id),
    financial_aid_policy text,
    source_sha256 char(64) NOT NULL
);
CREATE TABLE {schema}.hospital_contract_provision (
    hospital_id text NOT NULL REFERENCES {schema}.hospital_registry(hospital_id),
    provision_ordinal integer NOT NULL,
    payer_name text,
    plan_name text,
    provisions text NOT NULL,
    source_sha256 char(64) NOT NULL,
    PRIMARY KEY (hospital_id, provision_ordinal)
);
CREATE TABLE {schema}.hospital_npi (
    hospital_id text NOT NULL REFERENCES {schema}.hospital_registry(hospital_id),
    npi char(10) NOT NULL,
    source_sha256 char(64) NOT NULL,
    source_ordinal integer NOT NULL,
    PRIMARY KEY (hospital_id, npi, source_sha256)
);
CREATE TABLE {schema}.hospital_tax_identity (
    hospital_id text NOT NULL REFERENCES {schema}.hospital_registry(hospital_id),
    ein char(9) NOT NULL CHECK (ein ~ '^[0-9]{{9}}$'),
    source_sha256 char(64) NOT NULL,
    source_filename text NOT NULL,
    PRIMARY KEY (hospital_id, ein, source_sha256)
);
CREATE UNLOGGED TABLE {schema}.fact_stage (
    hospital_id text NOT NULL, service_ordinal integer NOT NULL,
    description text NOT NULL,
    code_system text NOT NULL, code text NOT NULL, setting text NOT NULL,
    billing_class text NOT NULL,
    modifiers text, drug_unit numeric, drug_type text,
    gross_amount numeric, discounted_cash numeric,
    payer_name text NOT NULL, plan_name text NOT NULL,
    negotiated_dollar numeric, negotiated_percentage numeric,
    negotiated_algorithm text, methodology text NOT NULL,
    minimum_amount numeric, maximum_amount numeric, median_amount numeric,
    percentile_10 numeric, percentile_90 numeric, allowed_count text,
    additional_generic_notes text, additional_payer_notes text
);
"""
TYPED_DDL = """
CREATE TABLE {schema}.price_service (
    hospital_id text NOT NULL, service_ordinal integer NOT NULL,
    description text NOT NULL,
    code_system text NOT NULL, code text NOT NULL, setting text NOT NULL,
    billing_class text NOT NULL,
    modifiers text, drug_unit numeric, drug_type text,
    gross_amount numeric, discounted_cash numeric,
    minimum_amount numeric, maximum_amount numeric,
    additional_generic_notes text,
    PRIMARY KEY (hospital_id, service_ordinal)
);
CREATE TABLE {schema}.price_fact (
    hospital_id text NOT NULL, service_ordinal integer NOT NULL,
    payer_name text NOT NULL, plan_name text NOT NULL,
    negotiated_dollar numeric, negotiated_percentage numeric,
    negotiated_algorithm text, methodology text NOT NULL,
    median_amount numeric, percentile_10 numeric, percentile_90 numeric,
    allowed_count text, additional_payer_notes text,
    FOREIGN KEY (hospital_id, service_ordinal)
        REFERENCES {schema}.price_service (hospital_id, service_ordinal)
);
"""
DICTIONARY_DDL = """
CREATE TABLE {schema}.code_dictionary (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code_system text NOT NULL, code text NOT NULL, UNIQUE (code_system, code)
);
CREATE TABLE {schema}.service_dictionary (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    description text NOT NULL, setting text NOT NULL, billing_class text NOT NULL,
    UNIQUE (description, setting, billing_class)
);
CREATE TABLE {schema}.payer_plan_dictionary (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payer_name text NOT NULL, plan_name text NOT NULL, UNIQUE (payer_name, plan_name)
);
CREATE TABLE {schema}.methodology_dictionary (
    id smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    methodology text NOT NULL UNIQUE
);
CREATE TABLE {schema}.price_fact (
    hospital_id text NOT NULL, service_ordinal integer NOT NULL,
    code_id integer NOT NULL, service_id integer NOT NULL,
    payer_plan_id integer NOT NULL, methodology_id smallint NOT NULL,
    modifiers text, drug_unit numeric, drug_type text,
    gross_amount numeric, discounted_cash numeric,
    negotiated_dollar numeric, negotiated_percentage numeric,
    negotiated_algorithm text, minimum_amount numeric, maximum_amount numeric,
    median_amount numeric, percentile_10 numeric, percentile_90 numeric,
    allowed_count text, additional_generic_notes text,
    additional_payer_notes text
);
"""
BLOCK_DDL = """
CREATE TABLE {schema}.price_block (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hospital_id text NOT NULL, code_system text NOT NULL, code text NOT NULL,
    block_no integer NOT NULL, row_count integer NOT NULL, payload jsonb NOT NULL,
    UNIQUE (hospital_id, code_system, code, block_no)
);
ALTER TABLE {schema}.price_block ALTER COLUMN payload SET STORAGE EXTENDED;
ALTER TABLE {schema}.price_block ALTER COLUMN payload SET COMPRESSION pglz;
CREATE TABLE {schema}.price_lookup (
    hospital_id text NOT NULL, service_ordinal integer NOT NULL,
    code_system text NOT NULL, code text NOT NULL,
    payer_name text NOT NULL, plan_name text NOT NULL,
    negotiated_dollar numeric, comparison_amount numeric,
    block_id bigint NOT NULL REFERENCES {schema}.price_block(id),
    item_ordinal smallint NOT NULL
);
"""


def create_sql(candidate: str, schema: str) -> str:
    """Create common metadata and one candidate physical layout."""
    candidate_ddl = {
        "typed": TYPED_DDL,
        "dictionary": DICTIONARY_DDL,
        "blocks": BLOCK_DDL,
    }[candidate]
    return (
        f"CREATE SCHEMA {schema};\n"
        + COMMON_DDL.format(schema=schema)
        + candidate_ddl.format(schema=schema)
    )


def copy_sql(schema: str, paths: dict[str, Path]) -> str:
    """Build psql COPY commands for common bindings and canonical facts."""
    copies = []
    for table, columns in (
        ("facility_anchor", ("id", "name")),
        ("hospital_registry", ("hospital_id", "facility_anchor_id")),
        (
            "hospital_mrf_metadata",
            ("hospital_id", "financial_aid_policy", "source_sha256"),
        ),
        (
            "hospital_contract_provision",
            (
                "hospital_id", "provision_ordinal", "payer_name", "plan_name",
                "provisions", "source_sha256",
            ),
        ),
        ("hospital_npi", ("hospital_id", "npi", "source_sha256", "source_ordinal")),
        (
            "hospital_tax_identity",
            ("hospital_id", "ein", "source_sha256", "source_filename"),
        ),
        ("fact_stage", FACT_COLUMNS),
    ):
        source_path = paths["price_fact" if table == "fact_stage" else table]
        safe_path = str(source_path.resolve()).replace("'", "''")
        copies.append(
            f"\\copy {schema}.{table} ({', '.join(columns)}) FROM '{safe_path}' "
            "WITH (FORMAT csv, HEADER true, DELIMITER E'\\t')"
        )
    return "\n".join(copies)


def _typed_materialize_sql(schema: str) -> str:
    return f"""
INSERT INTO {schema}.price_service
SELECT hospital_id, service_ordinal, description, code_system, code, setting,
       billing_class,
       modifiers, drug_unit, drug_type, gross_amount, discounted_cash,
       minimum_amount, maximum_amount, additional_generic_notes
FROM {schema}.fact_stage
GROUP BY hospital_id, service_ordinal, description, code_system, code, setting,
         billing_class,
         modifiers, drug_unit, drug_type, gross_amount, discounted_cash,
         minimum_amount, maximum_amount, additional_generic_notes;
INSERT INTO {schema}.price_fact
SELECT hospital_id, service_ordinal, payer_name, plan_name,
       negotiated_dollar, negotiated_percentage, negotiated_algorithm,
       methodology, median_amount, percentile_10, percentile_90, allowed_count,
       additional_payer_notes
FROM {schema}.fact_stage;
DROP TABLE {schema}.fact_stage;
"""


def _dictionary_materialize_sql(schema: str) -> str:
    return f"""
INSERT INTO {schema}.code_dictionary (code_system, code) SELECT DISTINCT code_system, code FROM {schema}.fact_stage;
INSERT INTO {schema}.service_dictionary (description, setting, billing_class) SELECT DISTINCT description, setting, billing_class FROM {schema}.fact_stage;
INSERT INTO {schema}.payer_plan_dictionary (payer_name, plan_name) SELECT DISTINCT payer_name, plan_name FROM {schema}.fact_stage;
INSERT INTO {schema}.methodology_dictionary (methodology) SELECT DISTINCT methodology FROM {schema}.fact_stage;
INSERT INTO {schema}.price_fact
SELECT s.hospital_id, s.service_ordinal, c.id, d.id, p.id, m.id,
       s.modifiers, s.drug_unit,
       s.drug_type, s.gross_amount, s.discounted_cash, s.negotiated_dollar,
       s.negotiated_percentage, s.negotiated_algorithm, s.minimum_amount,
       s.maximum_amount, s.median_amount, s.percentile_10, s.percentile_90,
       s.allowed_count, s.additional_generic_notes, s.additional_payer_notes
FROM {schema}.fact_stage s
JOIN {schema}.code_dictionary c USING (code_system, code)
JOIN {schema}.service_dictionary d USING (description, setting, billing_class)
JOIN {schema}.payer_plan_dictionary p USING (payer_name, plan_name)
JOIN {schema}.methodology_dictionary m USING (methodology);
DROP TABLE {schema}.fact_stage;
"""


def _block_materialize_sql(schema: str) -> str:
    order = """payer_name, plan_name, negotiated_dollar, negotiated_percentage,
negotiated_algorithm, description, setting, billing_class, modifiers, drug_unit, drug_type,
gross_amount, discounted_cash, methodology, minimum_amount, maximum_amount,
median_amount, percentile_10, percentile_90, allowed_count,
additional_generic_notes, additional_payer_notes"""
    return f"""
BEGIN;
CREATE TEMP TABLE ranked ON COMMIT DROP AS
WITH ordered AS (
    SELECT s.*, row_number() OVER (
        PARTITION BY hospital_id, code_system, code ORDER BY {order}
    ) - 1 AS row_no
    FROM {schema}.fact_stage s
)
SELECT ordered.*, (row_no / 512)::integer AS block_no,
       (row_no % 512)::smallint AS item_ordinal
FROM ordered;
INSERT INTO {schema}.price_block (
    hospital_id, code_system, code, block_no, row_count, payload
)
SELECT hospital_id, code_system, code, block_no, count(*)::integer,
       jsonb_agg(jsonb_build_array(
    description, setting, billing_class, modifiers, drug_unit, drug_type, gross_amount,
    discounted_cash, negotiated_percentage, negotiated_algorithm, methodology,
    minimum_amount, maximum_amount, median_amount, percentile_10, percentile_90,
    allowed_count, additional_generic_notes, additional_payer_notes
) ORDER BY item_ordinal)
FROM ranked GROUP BY hospital_id, code_system, code, block_no;
INSERT INTO {schema}.price_lookup
SELECT r.hospital_id, r.service_ordinal, r.code_system, r.code,
       r.payer_name, r.plan_name,
       r.negotiated_dollar,
       COALESCE(r.negotiated_dollar, r.median_amount, r.gross_amount,
                r.discounted_cash),
       b.id, r.item_ordinal
FROM ranked r JOIN {schema}.price_block b
USING (hospital_id, code_system, code, block_no);
DROP TABLE {schema}.fact_stage;
COMMIT;
"""


def materialize_sql(candidate: str, schema: str) -> str:
    """Transform the unlogged canonical stage into one durable layout."""
    builder_by_candidate = {
        "typed": _typed_materialize_sql,
        "dictionary": _dictionary_materialize_sql,
        "blocks": _block_materialize_sql,
    }
    try:
        return builder_by_candidate[candidate](schema)
    except KeyError as exc:
        raise ValueError(f"unknown candidate: {candidate}") from exc


def index_sql(candidate: str, schema: str) -> str:
    """Build indexes required by the benchmark API query workload."""
    prefix = f"CREATE INDEX ON {schema}.hospital_tax_identity (ein, hospital_id);\n"
    if candidate == "typed":
        return prefix + f"""
CREATE INDEX ON {schema}.price_service (hospital_id, code_system, code, service_ordinal);
CREATE INDEX ON {schema}.price_service (code_system, code, hospital_id, service_ordinal);
CREATE INDEX ON {schema}.price_fact (hospital_id, service_ordinal, payer_name, plan_name);
CREATE INDEX ON {schema}.price_fact (payer_name, plan_name, hospital_id, service_ordinal);
CREATE INDEX ON {schema}.price_fact (
    (COALESCE(negotiated_dollar, median_amount)), hospital_id, service_ordinal
);
"""
    if candidate == "dictionary":
        return prefix + f"""
CREATE INDEX ON {schema}.price_fact (hospital_id, service_ordinal, code_id, payer_plan_id);
CREATE INDEX ON {schema}.price_fact (payer_plan_id, code_id, hospital_id, service_ordinal);
CREATE INDEX ON {schema}.price_fact (
    code_id,
    (COALESCE(negotiated_dollar, median_amount, gross_amount, discounted_cash)),
    hospital_id
);
"""
    if candidate == "blocks":
        return prefix + f"""
CREATE INDEX ON {schema}.price_lookup (hospital_id, code_system, code, payer_name, plan_name);
CREATE INDEX ON {schema}.price_lookup (payer_name, plan_name, code_system, code, hospital_id);
CREATE INDEX ON {schema}.price_lookup (code_system, code, comparison_amount, hospital_id);
"""
    raise ValueError(f"unknown candidate: {candidate}")


def analyze_sql(candidate: str, schema: str) -> str:
    """Analyze only the candidate relations used by API probes."""
    tables = {
        "typed": ("price_service", "price_fact"),
        "dictionary": (
            "price_fact",
            "code_dictionary",
            "service_dictionary",
            "payer_plan_dictionary",
            "methodology_dictionary",
        ),
        "blocks": ("price_lookup", "price_block"),
    }[candidate]
    return "\n".join(
        f"ANALYZE {schema}.{table};" for table in (*tables, "hospital_tax_identity")
    )


def publish_sql(candidate: str, schema: str) -> str:
    """Create one canonical read view as the atomic publication surrogate."""
    if candidate == "typed":
        projection = f"""SELECT f.hospital_id, f.service_ordinal,
s.description, s.code_system, s.code, s.setting, s.billing_class, s.modifiers, s.drug_unit,
s.drug_type, s.gross_amount, s.discounted_cash, f.payer_name, f.plan_name,
f.negotiated_dollar, f.negotiated_percentage, f.negotiated_algorithm,
f.methodology, s.minimum_amount, s.maximum_amount, f.median_amount,
f.percentile_10, f.percentile_90, f.allowed_count,
s.additional_generic_notes, f.additional_payer_notes
FROM {schema}.price_fact f JOIN {schema}.price_service s
USING (hospital_id, service_ordinal)"""
    elif candidate == "dictionary":
        projection = f"""SELECT f.hospital_id, f.service_ordinal,
s.description, c.code_system, c.code,
s.setting, s.billing_class, f.modifiers, f.drug_unit, f.drug_type, f.gross_amount,
f.discounted_cash, p.payer_name, p.plan_name, f.negotiated_dollar,
f.negotiated_percentage, f.negotiated_algorithm, m.methodology,
f.minimum_amount, f.maximum_amount, f.median_amount, f.percentile_10,
f.percentile_90, f.allowed_count, f.additional_generic_notes,
f.additional_payer_notes FROM {schema}.price_fact f
JOIN {schema}.code_dictionary c ON c.id=f.code_id
JOIN {schema}.service_dictionary s ON s.id=f.service_id
JOIN {schema}.payer_plan_dictionary p ON p.id=f.payer_plan_id
JOIN {schema}.methodology_dictionary m ON m.id=f.methodology_id"""
    else:
        projection = f"""SELECT l.hospital_id, l.service_ordinal,
b.payload -> l.item_ordinal ->> 0 AS description, l.code_system, l.code,
b.payload -> l.item_ordinal ->> 1 AS setting,
b.payload -> l.item_ordinal ->> 2 AS billing_class,
b.payload -> l.item_ordinal ->> 3 AS modifiers,
(b.payload -> l.item_ordinal ->> 4)::numeric AS drug_unit,
b.payload -> l.item_ordinal ->> 5 AS drug_type,
(b.payload -> l.item_ordinal ->> 6)::numeric AS gross_amount,
(b.payload -> l.item_ordinal ->> 7)::numeric AS discounted_cash,
l.payer_name, l.plan_name, l.negotiated_dollar,
(b.payload -> l.item_ordinal ->> 8)::numeric AS negotiated_percentage,
b.payload -> l.item_ordinal ->> 9 AS negotiated_algorithm,
b.payload -> l.item_ordinal ->> 10 AS methodology,
(b.payload -> l.item_ordinal ->> 11)::numeric AS minimum_amount,
(b.payload -> l.item_ordinal ->> 12)::numeric AS maximum_amount,
(b.payload -> l.item_ordinal ->> 13)::numeric AS median_amount,
(b.payload -> l.item_ordinal ->> 14)::numeric AS percentile_10,
(b.payload -> l.item_ordinal ->> 15)::numeric AS percentile_90,
b.payload -> l.item_ordinal ->> 16 AS allowed_count,
b.payload -> l.item_ordinal ->> 17 AS additional_generic_notes,
b.payload -> l.item_ordinal ->> 18 AS additional_payer_notes
FROM {schema}.price_lookup l JOIN {schema}.price_block b ON b.id=l.block_id"""
    return f"CREATE VIEW {schema}.published_price AS {projection};"
