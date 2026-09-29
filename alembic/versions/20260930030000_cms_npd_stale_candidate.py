# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record immutable dispositions for stale CMS bulk candidates."""

from __future__ import annotations

import os

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260930030000_cms_npd_stale_candidate"
down_revision = "20260930030000_custom_import_registration_authority"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _create_disposition_table(schema: str) -> None:
    """Store one immutable disposition for each stale candidate."""

    op.create_table(
        "provider_directory_cms_npd_stale_candidate",
        sa.Column("dataset_id", sa.String(96), nullable=False),
        sa.Column("endpoint_id", sa.String(64), nullable=False),
        sa.Column("acquisition_root_run_id", sa.String(64), nullable=False),
        sa.Column("vector_sha256", sa.String(64), nullable=False),
        sa.Column("prior_status", sa.String(32), nullable=False),
        sa.Column("dataset_hash", sa.String(64)),
        sa.Column("observed_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("dataset_id"),
        sa.ForeignKeyConstraint(
            ("dataset_id",),
            (f"{schema}.provider_directory_endpoint_dataset.dataset_id",),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("vector_sha256 ~ '^[0-9a-f]{64}$'", name="cms_npd_stale_vector_check"),
        sa.CheckConstraint("prior_status IN ('acquiring', 'validated')", name="cms_npd_stale_status_check"),
        schema=schema,
    )


def _create_disposition_guard(schema: str) -> None:
    """Reject stale markers without an exact unpublished CMS parent."""

    table = f"{_q(schema)}.{_q('provider_directory_cms_npd_stale_candidate')}"
    parent = f"{_q(schema)}.{_q('provider_directory_endpoint_dataset')}"
    guard = f"{_q(schema)}.{_q('guard_cms_npd_stale_candidate')}"
    op.execute(f"""
        CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path = pg_catalog AS $body$
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'cms_npd_stale_disposition_immutable' USING ERRCODE = '55000';
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM {parent} AS dataset
                 WHERE dataset.dataset_id = NEW.dataset_id
                   AND dataset.endpoint_id = NEW.endpoint_id
                   AND dataset.acquisition_root_run_id = NEW.acquisition_root_run_id
                   AND dataset.status = NEW.prior_status
                   AND dataset.is_current = false
                   AND dataset.published_at IS NULL
                   AND dataset.dataset_hash IS NOT DISTINCT FROM NEW.dataset_hash
                   AND dataset.publication_metadata_json::jsonb
                       -> 'source_release' ->> 'source_id' = 'cms-npd'
                   AND dataset.publication_metadata_json::jsonb
                       -> 'source_release' ->> 'vector_sha256' = NEW.vector_sha256
            ) THEN
                RAISE EXCEPTION 'cms_npd_stale_disposition_parent_changed' USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END; $body$;
    """)
    op.execute(f"""
        CREATE TRIGGER {_q("cms_npd_stale_candidate_guard")}
        BEFORE INSERT OR UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION {guard}();
    """)
    op.execute(f"""
        CREATE TRIGGER {_q("cms_npd_stale_candidate_truncate_guard")}
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {guard}();
    """)


def upgrade() -> None:
    """Add exact candidate disposition without changing sealed dataset rows."""

    schema = _schema()
    _create_disposition_table(schema)
    _create_disposition_guard(schema)


def downgrade() -> None:
    """Drop the guard only before any disposition has been recorded."""

    schema = _schema()
    table = f"{_q(schema)}.{_q('provider_directory_cms_npd_stale_candidate')}"
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().exec_driver_sql(f"SELECT EXISTS (SELECT 1 FROM {table})").scalar():
        raise RuntimeError("cms_npd_stale_disposition_present")
    op.drop_table("provider_directory_cms_npd_stale_candidate", schema=schema)
    op.execute(f"DROP FUNCTION {_q(schema)}.{_q('guard_cms_npd_stale_candidate')}()")
