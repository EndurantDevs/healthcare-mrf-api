# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain complete CMS bulk FHIR resource witnesses beside dataset projections."""

import os

from alembic import op

revision = "20260930040000_cms_npd_resource_witness"
down_revision = "20260930030000_cms_npd_stale_candidate"
branch_labels = None
depends_on = None


def _schema() -> str:
    return '"' + (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").replace('"', '""') + '"'


def _create_table(schema: str) -> None:
    """Store one raw witness per admitted dataset resource."""

    table = f"{schema}.provider_directory_cms_npd_resource_witness"
    op.execute(f"""
        CREATE TABLE {table} (
            dataset_id varchar(96) NOT NULL,
            source_id varchar(64) NOT NULL,
            release_id varchar(64) NOT NULL,
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            raw_payload_sha256 varchar(64) NOT NULL,
            normalized_payload_hash varchar(64) NOT NULL,
            raw_payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id),
            FOREIGN KEY (dataset_id, resource_type, resource_id)
                REFERENCES {schema}.provider_directory_dataset_resource
                    (dataset_id, resource_type, resource_id) ON DELETE CASCADE,
            CONSTRAINT cms_npd_witness_source_check CHECK (source_id = 'cms-npd'),
            CONSTRAINT cms_npd_witness_release_check CHECK (release_id ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT cms_npd_witness_raw_hash_check CHECK (raw_payload_sha256 ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT cms_npd_witness_normalized_hash_check CHECK (normalized_payload_hash ~ '^[0-9a-f]{{64}}$')
        )
    """)


def _create_insert_guard(schema: str) -> None:
    """Check each inserted batch against its exact acquiring parent."""

    table = f"{schema}.provider_directory_cms_npd_resource_witness"
    op.execute(f"""
        CREATE FUNCTION {schema}.guard_cms_npd_resource_witness_insert() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $body$
        DECLARE witness_group record;
        DECLARE parent_status text;
        BEGIN
            FOR witness_group IN
                SELECT DISTINCT dataset_id, release_id FROM inserted_witness_rows
            LOOP
                SELECT dataset.status INTO parent_status
                  FROM {schema}.provider_directory_endpoint_dataset AS dataset
                 WHERE dataset.dataset_id = witness_group.dataset_id
                   AND dataset.is_current = false
                   AND dataset.published_at IS NULL
                   AND dataset.publication_metadata_json::jsonb -> 'source_release' ->> 'source_id' = 'cms-npd'
                   AND dataset.publication_metadata_json::jsonb -> 'source_release' ->> 'vector_sha256'
                       = witness_group.release_id
                 FOR SHARE;
                IF parent_status IS DISTINCT FROM 'acquiring' THEN
                    RAISE EXCEPTION 'cms_npd_resource_witness_immutable';
                END IF;
            END LOOP;
            RETURN NULL;
        END; $body$
    """)
    op.execute(f"""
        CREATE TRIGGER cms_npd_resource_witness_insert_guard
        AFTER INSERT ON {table}
        REFERENCING NEW TABLE AS inserted_witness_rows
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.guard_cms_npd_resource_witness_insert()
    """)


def _create_mutation_guard(schema: str) -> None:
    """Reject changes except failed-candidate cleanup."""

    table = f"{schema}.provider_directory_cms_npd_resource_witness"
    op.execute(f"""
        CREATE FUNCTION {schema}.guard_cms_npd_resource_witness() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $body$
        DECLARE parent_status text;
        BEGIN
            IF TG_OP = 'TRUNCATE' OR TG_OP = 'UPDATE' THEN
                RAISE EXCEPTION 'cms_npd_resource_witness_immutable';
            END IF;
            SELECT dataset.status INTO parent_status
              FROM {schema}.provider_directory_endpoint_dataset AS dataset
             WHERE dataset.dataset_id = OLD.dataset_id
               AND dataset.is_current = false
               AND dataset.published_at IS NULL
               AND dataset.publication_metadata_json::jsonb -> 'source_release' ->> 'source_id' = 'cms-npd'
               AND dataset.publication_metadata_json::jsonb -> 'source_release' ->> 'vector_sha256'
                   = OLD.release_id
             FOR SHARE;
            IF TG_OP = 'DELETE' AND parent_status = 'failed' THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'cms_npd_resource_witness_immutable';
        END; $body$
    """)
    op.execute(f"""
        CREATE TRIGGER cms_npd_resource_witness_guard
        BEFORE UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION {schema}.guard_cms_npd_resource_witness()
    """)
    op.execute(f"""
        CREATE TRIGGER cms_npd_resource_witness_no_truncate
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {schema}.guard_cms_npd_resource_witness()
    """)


def upgrade() -> None:
    """Bind raw facts to one exact, mutable CMS candidate before publication."""

    schema = _schema()
    _create_table(schema)
    _create_insert_guard(schema)
    _create_mutation_guard(schema)


def downgrade() -> None:
    """Do not discard durable source witnesses through an implicit downgrade."""

    raise RuntimeError("cms_npd_resource_witness_downgrade_requires_explicit_plan")
