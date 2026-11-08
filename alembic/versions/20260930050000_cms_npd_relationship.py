# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep source-qualified CMS references through release replacement and replay."""

import os

from alembic import op

revision = "20260930050000_cms_npd_relationship"
down_revision = "20260930040000_cms_npd_resource_witness"
branch_labels = None
depends_on = None


def _schema():
    value = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    return '"' + value.replace('"', '""') + '"'


def upgrade():
    """Create the retained CMS reference ledger and published-row guards."""
    schema = _schema()
    op.execute(f"""CREATE TABLE {schema}.provider_directory_cms_npd_relationship (
        dataset_id varchar(96) NOT NULL,
        source_id varchar(64) NOT NULL DEFAULT 'cms-npd',
        release_id varchar(256) NOT NULL,
        resource_type varchar(64) NOT NULL,
        resource_id varchar(256) NOT NULL,
        source_payload_hash varchar(64) NOT NULL,
        raw_payload_sha256 varchar(64) NOT NULL,
        reference_field varchar(64) NOT NULL,
        parent_ordinal integer NOT NULL,
        reference_ordinal integer NOT NULL,
        target_type varchar(64) NOT NULL,
        target_reference text,
        target_resource_id varchar(256),
        resolution_status varchar(16) NOT NULL,
        period_start text,
        period_end text,
        PRIMARY KEY (dataset_id, resource_type, resource_id, reference_field, parent_ordinal, reference_ordinal),
        FOREIGN KEY (dataset_id, resource_type, resource_id)
          REFERENCES {schema}.provider_directory_dataset_resource(dataset_id, resource_type, resource_id)
          ON DELETE CASCADE,
        FOREIGN KEY (dataset_id, resource_type, resource_id)
          REFERENCES {schema}.provider_directory_cms_npd_resource_witness(dataset_id, resource_type, resource_id)
          ON DELETE CASCADE,
        CONSTRAINT cms_npd_relationship_source_check CHECK (source_id='cms-npd'),
        CONSTRAINT cms_npd_relationship_release_check CHECK (release_id ~ '^[0-9a-f]{{64}}$'),
        CONSTRAINT cms_npd_relationship_status_check
          CHECK (resolution_status IN ('resolved', 'unresolved', 'ambiguous')),
        CONSTRAINT cms_npd_relationship_resolved_target_check CHECK
          (resolution_status='unresolved' OR target_resource_id IS NOT NULL),
        CONSTRAINT cms_npd_relationship_ordinal_check CHECK
          (parent_ordinal >= 0 AND reference_ordinal > 0),
        CONSTRAINT cms_npd_relationship_hash_check CHECK
          (source_payload_hash ~ '^[0-9a-f]{{64}}$' AND raw_payload_sha256 ~ '^[0-9a-f]{{64}}$')
    )""")
    op.execute(
        f"CREATE INDEX cms_npd_relationship_target_idx ON "
        f"{schema}.provider_directory_cms_npd_relationship "
        "(dataset_id, target_type, target_resource_id)"
    )
    op.execute(f"""CREATE TABLE {schema}.provider_directory_cms_npd_relationship_receipt (
        dataset_id varchar(96) PRIMARY KEY REFERENCES {schema}.provider_directory_endpoint_dataset(dataset_id),
        release_id varchar(64) NOT NULL CHECK (release_id ~ '^[0-9a-f]{{64}}$'),
        projection_contract varchar(64) NOT NULL
          CHECK (projection_contract='cms-npd-reference-ledger-v1'),
        relationship_count bigint NOT NULL CHECK (relationship_count >= 0)
    )""")
    _create_guards(schema)
    _create_receipt_guards(schema)


def _create_guards(schema):
    """Freeze relationship rows after publication or receipt sealing."""
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_published_relationship_insert() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM inserted i
                       JOIN {schema}.provider_directory_endpoint_dataset d USING (dataset_id)
                       WHERE d.status IN ('published','superseded')
                          OR EXISTS (SELECT 1 FROM {schema}.provider_directory_cms_npd_relationship_receipt r
                                     WHERE r.dataset_id=i.dataset_id)) THEN
                RAISE EXCEPTION 'cms_npd_published_relationship_immutable';
            END IF;
            RETURN NULL;
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_published_relationship_insert_immutable
        AFTER INSERT ON {schema}.provider_directory_cms_npd_relationship
        REFERENCING NEW TABLE AS inserted
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.guard_cms_npd_published_relationship_insert()""")
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_published_relationship() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {schema}.provider_directory_endpoint_dataset d
                       WHERE d.dataset_id=OLD.dataset_id AND
                         (d.status IN ('published','superseded') OR EXISTS
                           (SELECT 1 FROM {schema}.provider_directory_cms_npd_relationship_receipt r
                            WHERE r.dataset_id=OLD.dataset_id))) THEN
                RAISE EXCEPTION 'cms_npd_published_relationship_immutable';
            END IF;
            IF TG_OP='DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_published_relationship_immutable
        BEFORE UPDATE OR DELETE ON {schema}.provider_directory_cms_npd_relationship
        FOR EACH ROW EXECUTE FUNCTION {schema}.guard_cms_npd_published_relationship()""")
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_relationship_truncate() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'cms_npd_relationship_truncate_forbidden';
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_relationship_truncate_forbidden
        BEFORE TRUNCATE ON {schema}.provider_directory_cms_npd_relationship
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.guard_cms_npd_relationship_truncate()""")


def _create_receipt_guards(schema):
    """Allow one acquiring receipt and cleanup only for failed candidates."""
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_relationship_receipt_insert() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM inserted i LEFT JOIN {schema}.provider_directory_endpoint_dataset d
                       USING (dataset_id)
                       WHERE d.dataset_id IS NULL OR d.status<>'acquiring' OR d.is_current
                          OR d.published_at IS NOT NULL
                          OR d.publication_metadata_json::jsonb -> 'source_release' ->> 'vector_sha256'
                             IS DISTINCT FROM i.release_id) THEN
                RAISE EXCEPTION 'cms_npd_relationship_receipt_invalid';
            END IF;
            RETURN NULL;
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_relationship_receipt_insert_guard
        AFTER INSERT ON {schema}.provider_directory_cms_npd_relationship_receipt
        REFERENCING NEW TABLE AS inserted
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.guard_cms_npd_relationship_receipt_insert()""")
    op.execute(f"""CREATE FUNCTION {schema}.guard_cms_npd_relationship_receipt_mutation() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
            IF TG_OP='DELETE' AND EXISTS (
                SELECT 1 FROM {schema}.provider_directory_endpoint_dataset d
                WHERE d.dataset_id=OLD.dataset_id AND d.status='failed'
                  AND d.is_current=false AND d.published_at IS NULL
            ) THEN RETURN OLD; END IF;
            RAISE EXCEPTION 'cms_npd_relationship_receipt_immutable';
        END; $$""")
    op.execute(f"""CREATE TRIGGER cms_npd_relationship_receipt_mutation_guard
        BEFORE UPDATE OR DELETE ON {schema}.provider_directory_cms_npd_relationship_receipt
        FOR EACH ROW EXECUTE FUNCTION {schema}.guard_cms_npd_relationship_receipt_mutation()""")
    op.execute(f"""CREATE TRIGGER cms_npd_relationship_receipt_no_truncate
        BEFORE TRUNCATE ON {schema}.provider_directory_cms_npd_relationship_receipt
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.guard_cms_npd_relationship_truncate()""")


def downgrade():
    """Require an explicit data-retention plan before removing this ledger."""
    raise RuntimeError("cms_npd_relationship_downgrade_requires_explicit_plan")
