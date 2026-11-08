# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Store exact CMS serving coverage and freeze published proof inputs."""

import os

from alembic import op

revision = "20260930020000_cms_npd_serving_coverage"
down_revision = "20260930010000_provider_directory_resource_identity"
branch_labels = None
depends_on = None


def _schema():
    value = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    return '"' + value.replace('"', '""') + '"'


def _create_evidence_guards(schema):
    """Create the receipt and preserve exact source and network evidence."""
    op.execute(f"""CREATE TABLE {schema}.provider_directory_cms_serving_coverage (
        dataset_id varchar(96) NOT NULL,
        release_id varchar(256) NOT NULL,
        dataset_hash varchar(64) NOT NULL,
        published_at timestamp NOT NULL,
        created_at timestamptz NOT NULL,
        PRIMARY KEY (dataset_id, release_id),
        FOREIGN KEY (dataset_id) REFERENCES {schema}.provider_directory_endpoint_dataset(dataset_id)
            ON DELETE RESTRICT
    )""")
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_serving_evidence() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF OLD.source_id = 'cms-npd'
               OR (TG_OP = 'UPDATE' AND NEW.source_id = 'cms-npd') THEN
                RAISE EXCEPTION 'cms_npd_serving_evidence_immutable';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END; $$""")
    for table in (
        "provider_directory_entity_source_binding",
        "provider_directory_entity_release_evidence",
        "provider_directory_insurance_network_source_binding",
        "provider_directory_insurance_network_plan_evidence",
    ):
        op.execute(f"""CREATE TRIGGER cms_npd_serving_evidence_immutable
            BEFORE UPDATE OR DELETE ON {schema}.{table} FOR EACH ROW
            EXECUTE FUNCTION {schema}.guard_cms_npd_serving_evidence()""")
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_covered_network_witness() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        DECLARE witness_release text;
        BEGIN
            FOR witness_release IN
                SELECT DISTINCT release_id FROM inserted_witnesses
                WHERE source_id = 'cms-npd' ORDER BY release_id
            LOOP
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtext('cms-npd'), pg_catalog.hashtext(witness_release));
            END LOOP;
            PERFORM 1 FROM {schema}.provider_directory_cms_serving_coverage coverage
            JOIN (SELECT DISTINCT release_id FROM inserted_witnesses WHERE source_id = 'cms-npd') w
              ON coverage.release_id = w.release_id
            FOR SHARE OF coverage;
            IF FOUND THEN RAISE EXCEPTION 'cms_npd_covered_network_witness_immutable'; END IF;
            RETURN NULL;
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_covered_network_witness_immutable
        AFTER INSERT ON {schema}.provider_directory_insurance_network_plan_evidence
        REFERENCING NEW TABLE AS inserted_witnesses FOR EACH STATEMENT
        EXECUTE FUNCTION {schema}.guard_cms_npd_covered_network_witness()""")


def _create_resource_guards(schema):
    """Freeze published resource rows and committed coverage receipts."""

    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_published_resource() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                PERFORM 1 FROM {schema}.provider_directory_endpoint_dataset d
                JOIN (SELECT DISTINCT dataset_id FROM changed_new) changed USING (dataset_id)
                WHERE d.status IN ('published', 'superseded')
                  AND d.publication_metadata_summary_json->'source_ids' = '["cms-npd"]'::jsonb
                FOR SHARE OF d;
            ELSIF TG_OP = 'DELETE' THEN
                PERFORM 1 FROM {schema}.provider_directory_endpoint_dataset d
                JOIN (SELECT DISTINCT dataset_id FROM changed_old) changed USING (dataset_id)
                WHERE d.status IN ('published', 'superseded')
                  AND d.publication_metadata_summary_json->'source_ids' = '["cms-npd"]'::jsonb
                FOR SHARE OF d;
            ELSE
                PERFORM 1 FROM {schema}.provider_directory_endpoint_dataset d
                JOIN (SELECT dataset_id FROM changed_old UNION SELECT dataset_id FROM changed_new) changed
                  USING (dataset_id)
                WHERE d.status IN ('published', 'superseded')
                  AND d.publication_metadata_summary_json->'source_ids' = '["cms-npd"]'::jsonb
                FOR SHARE OF d;
            END IF;
            IF FOUND THEN RAISE EXCEPTION 'cms_npd_published_resource_immutable'; END IF;
            RETURN NULL;
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_published_resource_immutable
        AFTER INSERT ON {schema}.provider_directory_dataset_resource
        REFERENCING NEW TABLE AS changed_new FOR EACH STATEMENT
        EXECUTE FUNCTION {schema}.guard_cms_npd_published_resource()""")
    op.execute(f"""CREATE TRIGGER cms_npd_published_resource_update_immutable
        AFTER UPDATE ON {schema}.provider_directory_dataset_resource
        REFERENCING OLD TABLE AS changed_old NEW TABLE AS changed_new FOR EACH STATEMENT
        EXECUTE FUNCTION {schema}.guard_cms_npd_published_resource()""")
    op.execute(f"""CREATE TRIGGER cms_npd_published_resource_delete_immutable
        AFTER DELETE ON {schema}.provider_directory_dataset_resource
        REFERENCING OLD TABLE AS changed_old FOR EACH STATEMENT
        EXECUTE FUNCTION {schema}.guard_cms_npd_published_resource()""")
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_serving_coverage() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN RAISE EXCEPTION 'cms_npd_serving_coverage_immutable'; END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_serving_coverage_immutable
        BEFORE UPDATE OR DELETE ON {schema}.provider_directory_cms_serving_coverage
        FOR EACH ROW EXECUTE FUNCTION {schema}.guard_cms_npd_serving_coverage()""")


def _create_truncate_guards(schema):
    """Keep bulk deletion from bypassing row guards after coverage is sealed."""

    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_covered_truncate() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {schema}.provider_directory_cms_serving_coverage) THEN
                RAISE EXCEPTION 'cms_npd_covered_truncate_forbidden';
            END IF;
            RETURN NULL;
        END; $$""")
    for table in (
        "provider_directory_source",
        "provider_directory_api_endpoint",
        "provider_directory_endpoint_dataset",
        "provider_directory_dataset_resource",
        "provider_directory_entity_source_binding",
        "provider_directory_entity_release_evidence",
        "provider_directory_resource_identity",
        "provider_directory_insurance_network_source_binding",
        "provider_directory_insurance_network_plan_evidence",
        "provider_directory_cms_serving_coverage",
    ):
        op.execute(f"""CREATE TRIGGER cms_npd_covered_truncate_forbidden
            BEFORE TRUNCATE ON {schema}.{table} FOR EACH STATEMENT
            EXECUTE FUNCTION {schema}.guard_cms_npd_covered_truncate()""")


def upgrade():
    """Add CMS serving coverage after stable resource identities exist."""

    schema = _schema()
    _create_evidence_guards(schema)
    _create_resource_guards(schema)
    _create_truncate_guards(schema)


def downgrade():
    """Preserve published coverage until an explicit rollback plan exists."""

    raise RuntimeError("cms_npd_serving_coverage_downgrade_requires_explicit_plan")
