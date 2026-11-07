# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain exact source-copy provenance and active pins without foreign keys."""

import os

from sqlalchemy import text

from alembic import op

revision = "20261107010000_custom_import_retained_copy"
down_revision = "20261104020000_custom_import_family_blocks"
branch_labels = None
depends_on = None

_DDL = """
CREATE TABLE __SCHEMA__.custom_import_retained_capture_copy (
 copy_id varchar(64) PRIMARY KEY, canonical_authorization text NOT NULL,
 dataset_id bigint NOT NULL, execution_id bigint NOT NULL, fence bigint NOT NULL,
 source_execution_id bigint NOT NULL, source_fence bigint NOT NULL, source_token_sha256 bytea NOT NULL,
 source_capture_bundle_id bigint NOT NULL, source_manifest_sha256 bytea NOT NULL,
 capture_bundle_id bigint NOT NULL, phase varchar(16) NOT NULL, source_pin_active boolean NOT NULL,
 root_device bigint, root_inode bigint, cleanup_started_at timestamptz,
 launch_sent_at timestamptz, first_job_uid varchar(128), terminal_job_state varchar(16),
 pods_stopped_at timestamptz, cleaned_at timestamptz,
 CONSTRAINT custom_import_retained_copy_execution_key UNIQUE (execution_id),
 CONSTRAINT custom_import_retained_copy_identity_check CHECK (
  copy_id ~ '^[0-9a-f]{64}$' AND dataset_id>0 AND execution_id>0 AND source_execution_id>0
  AND source_capture_bundle_id>0 AND capture_bundle_id>0 AND source_fence>0 AND fence=1
  AND source_capture_bundle_id<>capture_bundle_id AND source_execution_id<>execution_id
  AND octet_length(source_token_sha256)=32 AND octet_length(source_manifest_sha256)=32),
 CONSTRAINT custom_import_retained_copy_authorization_check CHECK (
  octet_length(canonical_authorization) BETWEEN 2 AND 16384
  AND copy_id=encode(sha256(convert_to(canonical_authorization,'UTF8')),'hex')
  AND jsonb_typeof(canonical_authorization::jsonb)='object'),
 CONSTRAINT custom_import_retained_copy_phase_check CHECK (
  phase IN ('active','sealed','closed') AND source_pin_active=(phase='active')
  AND ((root_device IS NULL)=(root_inode IS NULL)) AND (root_device IS NULL OR (root_device>0 AND root_inode>0))
  AND (cleanup_started_at IS NULL OR (root_device IS NOT NULL AND phase<>'active'))
  AND (first_job_uid IS NULL OR first_job_uid ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$')
  AND (terminal_job_state IS NULL OR (first_job_uid IS NOT NULL AND terminal_job_state IN ('succeeded','failed')))
  AND (pods_stopped_at IS NULL OR first_job_uid IS NOT NULL)
  AND (cleaned_at IS NULL OR (phase<>'active' AND (pods_stopped_at IS NOT NULL OR launch_sent_at IS NULL))))
)
"""


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def upgrade():
    """Use the established capture owner and refuse open default write grants."""
    schema = _schema()
    quoted = '"' + schema.replace('"', '""') + '"'
    owner = (
        op.get_bind()
        .execute(
            text("SELECT relowner=to_regrole(current_user) FROM pg_class WHERE oid=to_regclass(:name)"),
            {"name": quoted + ".custom_import_capture_bundle"},
        )
        .scalar_one_or_none()
    )
    if owner is not True:
        raise RuntimeError("retained copy migration requires the existing capture owner")
    op.execute(_DDL.replace("__SCHEMA__", quoted))
    closed = (
        op.get_bind()
        .execute(
            text("""SELECT NOT EXISTS (
          SELECT 1 FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
          WHERE c.oid=to_regclass(:name) AND acl.grantee<>c.relowner
          AND acl.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN'))"""),
            {"name": quoted + ".custom_import_retained_capture_copy"},
        )
        .scalar_one()
    )
    if closed is not True:
        raise RuntimeError("retained copy defaults permit untrusted writes")


def downgrade():
    """Keep immutable copy audit history until an explicit forward retirement."""
    raise RuntimeError("retained copy audit requires forward migration")
