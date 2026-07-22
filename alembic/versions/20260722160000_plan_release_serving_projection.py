"""Add the canonical plan-release serving projection and snapshot pins.

Revision ID: 20260722160000_plan_release_serving_projection
Revises: 20260722130000_provider_directory_projection_child_read_lease
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260722160000_plan_release_serving_projection"
down_revision = "20260722130000_provider_directory_projection_child_read_lease"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    schema = _schema()
    revision_table = _qt(schema, "plan_release_serving_revision")
    binding_table = _qt(schema, "plan_release_snapshot_binding")
    pin_table = _qt(schema, "ptg2_snapshot_pin")
    snapshot_table = _qt(schema, "ptg2_snapshot")

    upgrade_sql = f"""
        CREATE TABLE {revision_table} (
            serving_revision_id varchar(64) NOT NULL,
            plan_release_id varchar(64) NOT NULL,
            healthporta_plan_id varchar(64) NOT NULL,
            plan_version_id varchar(64),
            release_month varchar(7) NOT NULL,
            release_status varchar(16) NOT NULL,
            serving_status varchar(16) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            expected_binding_count integer NOT NULL,
            binding_set_digest varchar(64) NOT NULL,
            source_manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            published_at timestamptz,
            retired_at timestamptz,
            CONSTRAINT {_q("plan_release_serving_revision_pkey")}
                PRIMARY KEY (serving_revision_id),
            CONSTRAINT {_q("plan_release_serving_release_id_check")}
                CHECK (plan_release_id ~ '^hprelease_[0-9A-HJKMNP-TV-Z]{{26}}$'),
            CONSTRAINT {_q("plan_release_serving_plan_id_check")}
                CHECK (healthporta_plan_id ~ '^hpplan_[0-9A-HJKMNP-TV-Z]{{26}}$'),
            CONSTRAINT {_q("plan_release_serving_revision_id_check")}
                CHECK (serving_revision_id ~ '^hpserve_[0-9A-HJKMNP-TV-Z]{{26}}$'),
            CONSTRAINT {_q("plan_release_serving_version_id_check")}
                CHECK (
                    plan_version_id IS NULL
                    OR plan_version_id ~ '^hpversion_[0-9A-HJKMNP-TV-Z]{{26}}$'
                ),
            CONSTRAINT {_q("plan_release_serving_month_check")}
                CHECK (
                    release_month ~ '^[0-9]{{4}}-(0[1-9]|1[0-2])$'
                ),
            CONSTRAINT {_q("plan_release_serving_release_status_check")}
                CHECK (
                    release_status IN (
                        'candidate', 'published', 'superseded', 'withdrawn'
                    )
                ),
            CONSTRAINT {_q("plan_release_serving_status_check")}
                CHECK (serving_status IN ('building', 'published', 'retired')),
            CONSTRAINT {_q("plan_release_serving_binding_count_check")}
                CHECK (expected_binding_count > 0),
            CONSTRAINT {_q("plan_release_serving_digest_check")}
                CHECK (binding_set_digest ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT {_q("plan_release_serving_current_state_check")}
                CHECK (
                    NOT is_current
                    OR (
                        serving_status = 'published'
                        AND release_status = 'published'
                    )
                )
        );

        CREATE UNIQUE INDEX {_q("plan_release_serving_current_release_uidx")}
            ON {revision_table} (plan_release_id)
            WHERE is_current;

        CREATE UNIQUE INDEX {_q("plan_release_serving_current_plan_uidx")}
            ON {revision_table} (healthporta_plan_id)
            WHERE is_current;

        CREATE INDEX {_q("plan_release_serving_plan_idx")}
            ON {revision_table} (healthporta_plan_id, release_month);

        CREATE TABLE {binding_table} (
            serving_revision_id varchar(64) NOT NULL,
            binding_ordinal integer NOT NULL,
            snapshot_id varchar(128) NOT NULL,
            source_key varchar(128) NOT NULL,
            plan_id varchar(128) NOT NULL,
            plan_market_type varchar(64),
            role varchar(32) NOT NULL,
            required boolean NOT NULL DEFAULT true,
            metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q("plan_release_snapshot_binding_pkey")}
                PRIMARY KEY (serving_revision_id, role, binding_ordinal),
            CONSTRAINT {_q("plan_release_snapshot_binding_revision_fkey")}
                FOREIGN KEY (serving_revision_id)
                REFERENCES {revision_table} (serving_revision_id)
                ON DELETE CASCADE,
            CONSTRAINT {_q("plan_release_snapshot_binding_ordinal_check")}
                CHECK (binding_ordinal >= 0),
            CONSTRAINT {_q("plan_release_snapshot_binding_role_check")}
                CHECK (role IN ('in_network', 'allowed_amounts')),
            CONSTRAINT {_q("plan_release_snapshot_binding_source_check")}
                CHECK (btrim(source_key) <> ''),
            CONSTRAINT {_q("plan_release_snapshot_binding_plan_check")}
                CHECK (btrim(plan_id) <> '')
        );

        CREATE INDEX {_q("plan_release_snapshot_binding_snapshot_idx")}
            ON {binding_table} (snapshot_id);

        CREATE TABLE {pin_table} (
            owner_type varchar(48) NOT NULL,
            owner_id varchar(96) NOT NULL,
            snapshot_id varchar(128) NOT NULL,
            reason varchar(256),
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q("ptg2_snapshot_pin_pkey")}
                PRIMARY KEY (owner_type, owner_id, snapshot_id),
            CONSTRAINT {_q("ptg2_snapshot_pin_snapshot_fkey")}
                FOREIGN KEY (snapshot_id)
                REFERENCES {snapshot_table} (snapshot_id)
                ON DELETE RESTRICT,
            CONSTRAINT {_q("ptg2_snapshot_pin_owner_check")}
                CHECK (btrim(owner_type) <> '' AND btrim(owner_id) <> '')
        );

        CREATE INDEX {_q("ptg2_snapshot_pin_snapshot_idx")}
            ON {pin_table} (snapshot_id);
        """
    # asyncpg rejects multiple commands in one prepared statement. Alembic's
    # async migration runner ultimately prepares each op.execute payload, so
    # execute every top-level DDL command independently.
    for statement in upgrade_sql.split(";"):
        statement = statement.strip()
        if statement:
            op.execute(statement)


def downgrade() -> None:
    schema = _schema()
    op.execute(
        f"DROP TABLE IF EXISTS {_qt(schema, 'ptg2_snapshot_pin')};"
    )
    op.execute(
        f"DROP TABLE IF EXISTS "
        f"{_qt(schema, 'plan_release_snapshot_binding')};"
    )
    op.execute(
        f"DROP TABLE IF EXISTS "
        f"{_qt(schema, 'plan_release_serving_revision')};"
    )
