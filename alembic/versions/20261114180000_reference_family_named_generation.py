# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Store ordinary reference serving locations by their declared names."""

import json
import os
import re

from sqlalchemy import text

from alembic import op
from db.plan_release_storage import storage_identifier

revision = "20261114180000_reference_family_named_generation"
down_revision = "20261114170000_source_profile_named_attachment"
branch_labels = None
depends_on = None

_NAMES = {
    "nucc": ("nucc_taxonomy",),
    "mrf": (
        "issuer",
        "plan",
        "plan_formulary",
        "plan_benefits_marketplace",
        "plan_transparency",
        "plan_drug_raw",
        "plan_drug_stats",
        "plan_drug_tier_stats",
        "plan_npi_raw",
        "plan_networktier",
        "mrf_address",
        "mrf_address_evidence",
    ),
    "mrf-address": ("mrf_address", "mrf_address_evidence"),
    "plan-attributes": ("plan_attributes", "plan_prices", "plan_rating_areas", "plan_benefits"),
    "places-zcta": ("pricing_places_zcta",),
    "geo": ("geo_zip_lookup",),
    "geo-census": ("geo_zip_census_profile",),
    "lodes": ("lodes_workplace_aggregate",),
    "cms-doctors": ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site"),
    "facility-anchors": ("facility_anchor", "facility_address_contribution"),
    "tiger": ("zip_state", "zcta5"),
    "medicare-enrollment": ("medicare_enrollment_county_stats", "medicare_enrollment_stats"),
    "pharmacy-economics": ("pharmacy_economics_summary",),
    "terminology-synonyms": ("terminology_synonym",),
    "provider-quality": (
        "pricing_qpp_provider",
        "pricing_svi_zcta",
        "pricing_provider_quality_measure",
        "pricing_provider_quality_domain",
        "pricing_provider_quality_score",
        "pricing_provider_quality_feature",
        "pricing_provider_quality_procedure_lsh",
        "pricing_provider_quality_peer_target",
    ),
}


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schema configuration differs")
    schema = runtime or legacy or "mrf"
    storage_identifier(schema)
    if schema == "tiger":
        raise RuntimeError("reference authority requires the declared application schema")
    return schema


def _replace_revision_body(schema):
    """Retain existing statement guards and counter assignments with a named predicate."""
    op.execute(f'''CREATE OR REPLACE FUNCTION "{schema}"."advance_reference_source_generation"() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE affected_importer text;
        BEGIN
            FOR affected_importer IN
                SELECT importer_id FROM "{schema}"."reference_family_result_generation"
                WHERE source_revision_tracked AND serving_schema=TG_TABLE_SCHEMA AND TG_TABLE_NAME=ANY(serving_relation_names)
                ORDER BY importer_id FOR UPDATE
            LOOP
                UPDATE "{schema}"."reference_family_result_generation"
                   SET local_generation=local_generation+1,
                       origin_lineage_id=local_lineage_id,
                       origin_generation=local_generation+1,
                       published_at=transaction_timestamp()
                 WHERE importer_id=affected_importer;
            END LOOP;
            RETURN NULL;
        END; $function$;''')


def _nucc_recorded_relation(generation_row, schema):
    """Use fixed heap declarations or the saved partition baseline, never old IDs."""
    if generation_row["origin_lineage_id"] is None:
        return None
    if generation_row["nucc_kind"] == "r":
        return schema, "nucc_taxonomy"
    action = "transition-legacy-nucc-generation: matching recorded child name is required"
    try:
        if generation_row["nucc_kind"] != "p":
            raise ValueError
        raw = generation_row["nucc_baseline"]
        if type(raw) is str:
            if len(raw.encode("utf-8")) > 131072:
                raise ValueError
            raw = json.loads(raw)
        if type(raw) is not dict or raw.get("contract") not in {
            "nucc-protected-baseline.v3",
            "nucc-protected-baseline.v4",
        }:
            raise ValueError
        child = tuple(raw["child"]) if "child" in raw else (raw["child_schema"], "nucc_taxonomy")
        if (
            len(child) != 2
            or child[1] != "nucc_taxonomy"
            or type(child[0]) is not str
            or re.fullmatch(r"reference_family_archive_[0-9a-f]{32}", child[0]) is None
            or "child_schema" in raw
            and raw["child_schema"] != child[0]
        ):
            raise ValueError
        if "parent" in raw and tuple(raw["parent"]) != (schema, "nucc_taxonomy"):
            raise ValueError
        from process.reference_family_result_generation import (
            _nucc_result_authority,
            validate_reference_family_result_generation_authority,
        )

        saved = _nucc_result_authority(raw["source_authority"], declared_schema=child[0], allow_untracked=True)
        current = validate_reference_family_result_generation_authority(
            {**generation_row, "serving_schema": child[0], "serving_relation_names": [child[1]]}
        )
        if saved != current:
            raise ValueError
        return child
    except (KeyError, TypeError, ValueError, RuntimeError) as error:
        raise RuntimeError(action) from error


def _map_nucc_generation(schema, table):
    connection = op.get_bind()
    row = (
        connection.execute(
            text(f"""SELECT g.*, c.relkind::text AS nucc_kind,
        obj_description(c.oid,'pg_class') AS nucc_baseline
        FROM {table} g LEFT JOIN pg_namespace n ON n.nspname=:schema
        LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname='nucc_taxonomy'
        WHERE g.importer_id='nucc' FOR UPDATE OF g"""),
            {"schema": schema},
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return
    relation = _nucc_recorded_relation(row, schema)
    if relation is not None:
        op.execute(
            text(
                f"UPDATE {table} SET serving_schema=:schema,serving_relation_names=:names WHERE importer_id=:importer"
            ).bindparams(schema=relation[0], names=[relation[1]], importer="nucc")
        )


def _flush_doctors_metadata_transition(schema):
    op.execute(f'''DO $metadata_transition$ BEGIN
        IF EXISTS(SELECT FROM pg_catalog.pg_constraint
            WHERE conrelid='"{schema}"."reference_family_result_generation"'::regclass
            AND conname='cms_serving_doctors_transition' AND contype='t' AND condeferrable) THEN
            SET CONSTRAINTS "{schema}".cms_serving_doctors_transition IMMEDIATE;
            SET CONSTRAINTS "{schema}".cms_serving_doctors_transition DEFERRED;
        END IF;
    END $metadata_transition$''')


def upgrade():
    """Map fixed configured declarations, preserving publication lineage and revision."""
    schema = _schema()
    table = storage_identifier(schema) + '."reference_family_result_generation"'
    op.execute(f"ALTER TABLE {table} ADD COLUMN serving_schema varchar(63), ADD COLUMN serving_relation_names text[]")
    _map_nucc_generation(schema, table)
    for importer, names in _NAMES.items():
        if importer == "nucc":
            continue
        serving_schema = "tiger" if importer == "tiger" else schema
        op.execute(
            text(
                f"UPDATE {table} SET serving_schema=:schema,serving_relation_names=:names WHERE importer_id=:importer AND origin_lineage_id IS NOT NULL"
            ).bindparams(schema=serving_schema, names=list(names), importer=importer)
        )
    _flush_doctors_metadata_transition(schema)
    _replace_revision_body(schema)
    op.execute(
        f"ALTER TABLE {table} DROP CONSTRAINT reference_family_result_generation_shape_check, DROP COLUMN relation_oids"
    )
    families = " OR ".join(
        "(importer_id='"
        + importer
        + "' AND serving_relation_names=ARRAY["
        + ",".join("'" + name + "'" for name in names)
        + "]::text[])"
        for importer, names in _NAMES.items()
    )
    op.execute(f"""ALTER TABLE {table} ADD CONSTRAINT reference_family_result_generation_shape_check CHECK (
        importer_id IN ({",".join("'" + importer + "'" for importer in _NAMES)})
        AND local_generation BETWEEN 0 AND 9223372036854775807 AND (
          (origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL
           AND serving_schema IS NULL AND serving_relation_names IS NULL) OR
          (origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL
           AND origin_generation BETWEEN 1 AND 9223372036854775807 AND published_at IS NOT NULL
           AND serving_schema IS NOT NULL AND serving_schema ~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$'
           AND serving_relation_names IS NOT NULL AND ({families}))))""")


def downgrade():
    """Require explicit retained-history retirement before removing named publication metadata."""
    raise RuntimeError("Named reference publication requires explicit retirement")
