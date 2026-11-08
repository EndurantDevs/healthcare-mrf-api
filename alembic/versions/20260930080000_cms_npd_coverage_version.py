# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Distinguish current CMS network-role coverage from earlier receipts."""

import os

from alembic import op

revision = "20260930080000_cms_npd_coverage_version"
down_revision = "20260930070000_provider_directory_entity_redirect"
branch_labels = None
depends_on = None


def _schema():
    return '"' + (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").replace('"', '""') + '"'


def _table():
    return f"{_schema()}.provider_directory_cms_serving_coverage"


def upgrade():
    """Require a staged upgrade if an earlier CMS generation is still served."""
    table = _table()
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"LOCK TABLE {_schema()}.provider_directory_endpoint_dataset, {table} IN SHARE MODE")
    op.execute(
        f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM {table} c "
        f"JOIN {_schema()}.provider_directory_endpoint_dataset d USING (dataset_id) "
        "WHERE d.status <> 'superseded') THEN "
        "RAISE EXCEPTION 'cms_npd_coverage_v1_current_requires_staged_upgrade'; "
        "END IF; END $$"
    )
    op.execute(f"ALTER TABLE {table} ADD COLUMN proof_version smallint NOT NULL DEFAULT 1")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT cms_npd_coverage_proof_version_check CHECK (proof_version IN (1, 2))"
    )
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT provider_directory_cms_serving_coverage_pkey")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT provider_directory_cms_serving_coverage_pkey "
        "PRIMARY KEY (dataset_id, release_id, proof_version)"
    )
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT cms_npd_coverage_new_receipt_v2_check CHECK (proof_version=2) NOT VALID"
    )


def downgrade():
    """Retain any current-version receipt until an explicit withdrawal plan exists."""
    table = _table()
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    op.execute(
        f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM {table} WHERE proof_version=2) THEN "
        "RAISE EXCEPTION 'cms_npd_coverage_v2_downgrade_requires_explicit_plan'; "
        "END IF; END $$"
    )
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT provider_directory_cms_serving_coverage_pkey")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT cms_npd_coverage_new_receipt_v2_check")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT cms_npd_coverage_proof_version_check")
    op.execute(f"ALTER TABLE {table} DROP COLUMN proof_version")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT provider_directory_cms_serving_coverage_pkey "
        "PRIMARY KEY (dataset_id, release_id)"
    )
