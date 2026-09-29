# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seal exact CMS candidate coverage before composite publication."""

import os

from alembic import op

revision = "20260930090000_cms_npd_candidate_coverage"
down_revision = "20260930080000_cms_npd_coverage_version"
branch_labels = None
depends_on = None


def _create_candidate_seal(schema):
    """Bind coverage to immutable admission and relationship receipts."""
    table = f"{schema}.provider_directory_cms_candidate_coverage"
    op.execute(f"""CREATE TABLE {table} (
        dataset_id varchar(96) NOT NULL REFERENCES {schema}.provider_directory_endpoint_dataset(dataset_id),
        endpoint_id varchar(96) NOT NULL,
        release_id varchar(64) NOT NULL CHECK (release_id ~ '^[0-9a-f]{{64}}$'),
        dataset_hash varchar(64) NOT NULL CHECK (dataset_hash ~ '^[0-9a-f]{{64}}$'),
        proof_version smallint NOT NULL CHECK (proof_version=2),
        admission_sha256 varchar(64) NOT NULL,
        metadata_sha256 varchar(64) NOT NULL,
        relationship_count bigint NOT NULL CHECK (relationship_count>=0),
        created_at timestamptz NOT NULL,
        PRIMARY KEY (dataset_id, release_id, proof_version)
    )""")
    op.execute(f"CREATE INDEX cms_npd_candidate_coverage_release_idx ON {table} (release_id)")
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_candidate_coverage_insert() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('cms-npd'),
                                                     pg_catalog.hashtext(NEW.release_id));
            PERFORM 1 FROM {schema}.provider_directory_endpoint_dataset d
            JOIN {schema}.provider_directory_cms_npd_relationship_receipt r USING (dataset_id)
            WHERE d.dataset_id=NEW.dataset_id AND d.endpoint_id=NEW.endpoint_id
              AND d.dataset_hash=NEW.dataset_hash
              AND ((d.status='validated' AND NOT d.is_current AND d.published_at IS NULL)
                OR (d.status='published' AND d.is_current AND d.published_at IS NOT NULL))
              AND d.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
              AND d.publication_metadata_json::jsonb->'source_release'->>'source_id'='cms-npd'
              AND d.publication_metadata_json::jsonb->'source_release'->>'vector_sha256'=NEW.release_id
              AND d.content_proof_admission_version=1
              AND d.content_proof_admission_kind='generic' AND d.content_proof_resource_types=ARRAY['Endpoint','HealthcareService','InsurancePlan','Location',
                  'Organization','OrganizationAffiliation','Practitioner','PractitionerRole']::varchar[]
              AND d.content_proof_admission_sha256=NEW.admission_sha256
              AND d.publication_metadata_sha256=NEW.metadata_sha256
              AND r.release_id=NEW.release_id AND r.projection_contract='cms-npd-reference-ledger-v1'
              AND r.relationship_count=NEW.relationship_count FOR SHARE OF d,r;
            IF NOT FOUND THEN RAISE EXCEPTION 'cms_npd_candidate_coverage_invalid'; END IF;
            RETURN NEW;
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_candidate_coverage_insert
        BEFORE INSERT ON {table} FOR EACH ROW
        EXECUTE FUNCTION {schema}.guard_cms_npd_candidate_coverage_insert()""")
    op.execute(f"""CREATE TRIGGER cms_npd_candidate_coverage_immutable
        BEFORE UPDATE OR DELETE OR TRUNCATE ON {table} FOR EACH STATEMENT
        EXECUTE FUNCTION {schema}.guard_cms_npd_serving_coverage()""")


def _freeze_candidate_inputs(schema):
    """Reuse the release lock so concurrent witness inserts cannot escape a candidate seal."""
    op.execute(f"""CREATE OR REPLACE FUNCTION {schema}.guard_cms_npd_covered_network_witness() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        DECLARE witness_release text;
        BEGIN
            FOR witness_release IN
                SELECT DISTINCT release_id FROM inserted_witnesses
                WHERE source_id='cms-npd' ORDER BY release_id
            LOOP
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtext('cms-npd'), pg_catalog.hashtext(witness_release));
            END LOOP;
            PERFORM 1 FROM {schema}.provider_directory_cms_serving_coverage c
            JOIN (SELECT DISTINCT release_id FROM inserted_witnesses WHERE source_id='cms-npd') w
              ON c.release_id=w.release_id FOR SHARE OF c;
            IF FOUND THEN RAISE EXCEPTION 'cms_npd_covered_network_witness_immutable'; END IF;
            PERFORM 1 FROM {schema}.provider_directory_cms_candidate_coverage c
            JOIN (SELECT DISTINCT release_id FROM inserted_witnesses WHERE source_id='cms-npd') w
              ON c.release_id=w.release_id FOR SHARE OF c;
            IF FOUND THEN RAISE EXCEPTION 'cms_npd_covered_network_witness_immutable'; END IF;
            RETURN NULL;
        END; $$""")
    op.execute(f"""CREATE OR REPLACE FUNCTION {schema}.guard_cms_npd_covered_truncate() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {schema}.provider_directory_cms_serving_coverage)
              OR EXISTS (SELECT 1 FROM {schema}.provider_directory_cms_candidate_coverage) THEN
                RAISE EXCEPTION 'cms_npd_covered_truncate_forbidden';
            END IF;
            RETURN NULL;
        END; $$""")


def upgrade():
    """Prepare coverage without moving any current pointer or changing serving receipts."""
    schema = '"' + (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").replace('"', '""') + '"'
    _create_candidate_seal(schema)
    _freeze_candidate_inputs(schema)


def downgrade():
    """Preserve sealed candidate evidence until an explicit retirement plan exists."""
    raise RuntimeError("cms_npd_candidate_coverage_downgrade_requires_explicit_plan")
