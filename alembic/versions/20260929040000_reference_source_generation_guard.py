# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Track reference source revisions without trusting pre-guard history."""

import importlib.util
import os
import re
from pathlib import Path

import sqlalchemy as sa

from alembic import op

revision = "20260929040000_reference_source_generation_guard"
down_revision = "20260930140000_cms_capacity_preflight_receipt"
branch_labels = None
depends_on = None

_TABLE = "reference_family_result_generation"
_SHAPE = "reference_family_result_generation_shape_check"
_FUNCTION = "advance_reference_source_generation"
_TRIGGER = "reference_source_generation_revision_guard"
_TABLES = (
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
    "plan_attributes",
    "plan_prices",
    "plan_rating_areas",
    "plan_benefits",
    "pricing_places_zcta",
    "geo_zip_lookup",
    "geo_zip_census_profile",
    "lodes_workplace_aggregate",
    "doctor_clinician_address",
    "cms_doctor_education",
    "cms_doctor_group_site",
    "facility_anchor",
    "facility_address_contribution",
    "medicare_enrollment_county_stats",
    "medicare_enrollment_stats",
    "pharmacy_economics_summary",
    "terminology_synonym",
    "pricing_qpp_provider",
    "pricing_svi_zcta",
    "pricing_provider_quality_measure",
    "pricing_provider_quality_domain",
    "pricing_provider_quality_score",
    "pricing_provider_quality_feature",
    "pricing_provider_quality_procedure_lsh",
    "pricing_provider_quality_peer_target",
)


def _schema():
    runtime_schema, legacy_schema = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    schema = runtime_schema or legacy_schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or len(schema.encode()) > 63 or schema == "tiger":
        raise RuntimeError("reference source authority schema is invalid")
    return schema


def _revision_function(schema):
    op.execute(f'''
        CREATE FUNCTION "{schema}"."{_FUNCTION}"() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE affected_importer text;
        BEGIN
            FOR affected_importer IN
                SELECT importer_id FROM "{schema}"."{_TABLE}"
                WHERE source_revision_tracked AND TG_RELID::bigint = ANY(relation_oids)
                ORDER BY importer_id FOR UPDATE
            LOOP
                UPDATE "{schema}"."{_TABLE}"
                   SET local_generation=local_generation+1,
                       origin_lineage_id=local_lineage_id,
                       origin_generation=local_generation+1,
                       published_at=transaction_timestamp()
                 WHERE importer_id=affected_importer;
            END LOOP;
            RETURN NULL;
        END; $function$;
    ''')
    op.execute(f'REVOKE ALL ON FUNCTION "{schema}"."{_FUNCTION}"() FROM PUBLIC')


def _shape_check(mrf_cardinality):
    path = Path(__file__).with_name("20260929000000_cms_doctor_group_site.py")
    spec = importlib.util.spec_from_file_location("reference_source_shape_predecessor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    previous, counts = module._shape_support()
    return previous._shape({**counts, "cms-doctors": 3, "mrf": mrf_cardinality})


def _existing_relations(schema):
    # Protected static relations are guarded by their owner at explicit publication.
    # The application migrator must neither perform their DDL nor grant itself access.
    candidates = [(schema, name) for name in _TABLES]
    if op.get_context().as_sql:
        return candidates
    return [
        (namespace, name)
        for namespace, name in candidates
        if op.get_bind()
        .execute(
            sa.text("SELECT pg_catalog.to_regclass(:relation) IS NOT NULL"),
            {"relation": f'"{namespace}"."{name}"'},
        )
        .scalar_one()
    ]


def _protected_relation_owners():
    return (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT c.oid,r.rolname FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_roles r ON r.oid=c.relowner "
                "WHERE c.relnamespace=pg_catalog.to_regnamespace('tiger') "
                "AND c.relname IN ('zip_state','zcta5') AND c.relkind='r' AND c.relpersistence='p' "
                "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
                "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
                "ORDER BY c.relname"
            )
        )
        .all()
    )


def _grant_existing_protected_owners(schema):
    """Provision only the new function, never protected relation or schema rights.

    Offline upgrades and absent, unsupported, or future owners need explicit
    function privilege provisioning before their protected-owner publication.
    """
    if op.get_context().as_sql:
        return
    relations = _protected_relation_owners()
    if len(relations) != 2:
        return
    op.execute('LOCK TABLE ONLY "tiger"."zip_state", ONLY "tiger"."zcta5" IN ACCESS SHARE MODE NOWAIT')
    if _protected_relation_owners() != relations:
        raise RuntimeError("protected reference relation identity changed")
    preparer = op.get_bind().dialect.identifier_preparer
    for owner in sorted({row.rolname for row in relations}):
        op.execute(f'GRANT EXECUTE ON FUNCTION "{schema}"."{_FUNCTION}"() TO {preparer.quote(owner)}')


def _flush_cms_transition(schema):
    # Even an unrelated MRF update queues the deferred Doctors trigger on this ledger.
    # Validate that queue before further DDL, then retain the normal deferred boundary.
    op.execute(f'''DO $cms_transition$ BEGIN
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
            WHERE conrelid='"{schema}"."{_TABLE}"'::regclass
              AND conname='cms_serving_doctors_transition' AND contype='t' AND condeferrable) THEN
            SET CONSTRAINTS "{schema}".cms_serving_doctors_transition IMMEDIATE;
            SET CONSTRAINTS "{schema}".cms_serving_doctors_transition DEFERRED;
        END IF;
    END; $cms_transition$''')


def upgrade():
    """Preserve prior records; only explicit new boundaries become trusted."""
    schema = _schema()
    authority = f'"{schema}"."{_TABLE}"'
    op.execute(f"LOCK TABLE {authority} IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.drop_constraint(_SHAPE, _TABLE, schema=schema, type_="check")
    # The historical ninth OID is shared diagnostics, not an importer result.
    # Preserve provenance without treating the narrowed legacy identity as tracked.
    op.execute(
        f"UPDATE {authority} SET relation_oids=relation_oids[1:8] || relation_oids[10:13] "
        "WHERE importer_id='mrf' AND relation_oids IS NOT NULL"
    )
    _flush_cms_transition(schema)
    op.create_check_constraint(_SHAPE, _TABLE, _shape_check(12), schema=schema)
    op.add_column(
        _TABLE,
        sa.Column("source_revision_tracked", sa.Boolean(), nullable=False, server_default=sa.false()),
        schema=schema,
    )
    op.create_check_constraint(
        "reference_source_revision_tracked_check",
        _TABLE,
        "NOT source_revision_tracked OR origin_lineage_id IS NOT NULL",
        schema=schema,
    )
    _revision_function(schema)
    _grant_existing_protected_owners(schema)
    for namespace, name in _existing_relations(schema):
        relation = f'"{namespace}"."{name}"'
        create_guard = (
            f'CREATE TRIGGER "{_TRIGGER}" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE '
            f'ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION "{schema}"."{_FUNCTION}"()'
        )
        enable_guard = f'ALTER TABLE {relation} ENABLE ALWAYS TRIGGER "{_TRIGGER}"'
        if op.get_context().as_sql:
            op.execute(
                f"DO $guard$ BEGIN IF pg_catalog.to_regclass('{relation}') IS NOT NULL THEN "
                f"{create_guard}; {enable_guard}; END IF; END; $guard$"
            )
        else:
            op.execute(create_guard)
            op.execute(enable_guard)


def downgrade():
    """Never silently remove revision protection after a trusted boundary."""
    schema = _schema()
    op.execute(f'LOCK TABLE "{schema}"."{_TABLE}" IN ACCESS EXCLUSIVE MODE NOWAIT')
    if (
        op.get_bind()
        .execute(
            sa.text(
                f'SELECT EXISTS (SELECT 1 FROM "{schema}"."{_TABLE}" WHERE source_revision_tracked '
                "OR (importer_id='mrf' AND relation_oids IS NOT NULL))"
            )
        )
        .scalar_one()
    ):
        raise RuntimeError("reference source revision evidence prevents downgrade")
    guarded_relations = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT n.nspname,c.relname FROM pg_catalog.pg_trigger t "
                "JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                "WHERE t.tgname=:trigger AND t.tgfoid=pg_catalog.to_regprocedure(:function)"
            ),
            {"trigger": _TRIGGER, "function": f'"{schema}"."{_FUNCTION}"()'},
        )
        .all()
    )
    for namespace, name in guarded_relations:
        preparer = op.get_bind().dialect.identifier_preparer
        relation = f"{preparer.quote(namespace)}.{preparer.quote(name)}"
        op.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
        op.execute(f'DROP TRIGGER "{_TRIGGER}" ON {relation}')
    op.execute(f'DROP FUNCTION "{schema}"."{_FUNCTION}"()')
    op.drop_constraint("reference_source_revision_tracked_check", _TABLE, schema=schema, type_="check")
    op.drop_column(_TABLE, "source_revision_tracked", schema=schema)
    op.drop_constraint(_SHAPE, _TABLE, schema=schema, type_="check")
    op.create_check_constraint(_SHAPE, _TABLE, _shape_check(13), schema=schema)
