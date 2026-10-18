# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare NPPES binding structure without changing existing writer custody."""

import sqlalchemy as sa

from db.plan_release_storage import storage_relation


def upgrade_nppes_evidence_candidates(connection, schema):
    """Retain isolated-family bindings; protected custody is a separate cut."""
    relation = storage_relation(schema, "public_evidence_nppes_registry_storage")
    connection.execute(
        sa.text(
            f"CREATE TABLE IF NOT EXISTS {relation} ("
            "admission_ref varchar(50) PRIMARY KEY,"
            "source_release_ref varchar(50) NOT NULL UNIQUE,"
            "candidate_storage_json jsonb NOT NULL,"
            "created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),"
            "updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),"
            "CONSTRAINT public_evidence_nppes_registry_storage_object_check "
            "CHECK (jsonb_typeof(candidate_storage_json)='object'))"
        )
    )
    valid = connection.scalar(
        sa.text(
            "SELECT relation.relkind='r' AND relation.relpersistence='p' "
            "AND NOT relation.relispartition AND NOT relation.relrowsecurity "
            "AND NOT relation.relforcerowsecurity "
            "AND (SELECT array_agg(attribute.attname::text ORDER BY attribute.attnum) "
            "FROM pg_attribute AS attribute WHERE attribute.attrelid=relation.oid "
            "AND attribute.attnum>0 AND NOT attribute.attisdropped)="
            "ARRAY['admission_ref','source_release_ref','candidate_storage_json','created_at','updated_at'] "
            "AND (SELECT bool_and(attribute.attnotnull AND attribute.attgenerated::text='' "
            "AND attribute.attidentity::text='' AND CASE attribute.attname "
            "WHEN 'admission_ref' THEN attribute.atttypid='varchar'::regtype AND attribute.atttypmod=54 "
            "WHEN 'source_release_ref' THEN attribute.atttypid='varchar'::regtype AND attribute.atttypmod=54 "
            "WHEN 'candidate_storage_json' THEN attribute.atttypid='jsonb'::regtype "
            "ELSE attribute.atttypid='timestamptz'::regtype END) "
            "FROM pg_attribute AS attribute WHERE attribute.attrelid=relation.oid "
            "AND attribute.attnum>0 AND NOT attribute.attisdropped) "
            "AND NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid=relation.oid AND NOT tgisinternal) "
            "AND NOT EXISTS (SELECT FROM pg_constraint WHERE conrelid=relation.oid AND contype='f') "
            "AND (SELECT count(*) FROM pg_constraint WHERE conrelid=relation.oid "
            "AND contype='p' AND conkey=ARRAY[1]::smallint[] AND convalidated AND NOT condeferrable)=1 "
            "AND (SELECT count(*) FROM pg_constraint WHERE conrelid=relation.oid "
            "AND contype='u' AND conkey=ARRAY[2]::smallint[] AND convalidated AND NOT condeferrable)=1 "
            "FROM pg_class AS relation WHERE relation.oid=to_regclass(:relation)"
        ),
        {"relation": relation},
    )
    if valid is not True:
        raise RuntimeError("nppes_evidence_candidate_header_changed")


def downgrade_nppes_evidence_candidates(connection, schema):
    """Keep immutable publication and resume bindings on retained source data."""
    raise RuntimeError("nppes_evidence_candidate_storage_forward_only")
