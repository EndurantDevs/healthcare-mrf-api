"""Add facility anchor parent fields and NPI overrides.

Revision ID: 20260616110000
Revises: 20260615190000_facility_anchor_source_npi
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260616110000_facility_anchor_parent_npi_overrides"
down_revision = "20260615190000_facility_anchor_source_npi"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            text("SELECT to_regclass(:name)"),
            {"name": f"{schema}.{table}"},
        ).scalar()
    )


def _column_exists(bind, schema: str, table: str, column: str) -> bool:
    return bool(
        bind.execute(
            text(
                """
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = :schema
                   AND table_name = :table
                   AND column_name = :column
                """
            ),
            {"schema": schema, "table": table, "column": column},
        ).scalar()
    )


def _add_column_if_missing(bind, schema: str, table: str, column: str, definition: str) -> None:
    if not _column_exists(bind, schema, table, column):
        op.execute(f"ALTER TABLE {_qt(schema, table)} ADD COLUMN {column} {definition};")


def upgrade():
    bind = op.get_bind()
    schema = _schema()

    if _table_exists(bind, schema, "facility_anchor"):
        _add_column_if_missing(bind, schema, "facility_anchor", "health_center_number", "varchar(64)")
        _add_column_if_missing(
            bind,
            schema,
            "facility_anchor",
            "health_center_organization_id",
            "varchar(64)",
        )
        _add_column_if_missing(bind, schema, "facility_anchor", "bphc_assigned_number", "varchar(64)")
        _add_column_if_missing(bind, schema, "facility_anchor", "health_center_name", "varchar(256)")
        _add_column_if_missing(
            bind,
            schema,
            "facility_anchor",
            "health_center_organization_address_line1",
            "varchar(256)",
        )
        _add_column_if_missing(
            bind,
            schema,
            "facility_anchor",
            "health_center_organization_city",
            "varchar(128)",
        )
        _add_column_if_missing(
            bind,
            schema,
            "facility_anchor",
            "health_center_organization_state",
            "varchar(2)",
        )
        _add_column_if_missing(
            bind,
            schema,
            "facility_anchor",
            "health_center_organization_zip_code",
            "varchar(16)",
        )
        op.execute(
            f"CREATE INDEX IF NOT EXISTS facility_anchor_health_center_number_idx "
            f"ON {_qt(schema, 'facility_anchor')} (health_center_number) "
            "WHERE health_center_number IS NOT NULL;"
        )
        op.execute(
            f"CREATE INDEX IF NOT EXISTS facility_anchor_health_center_org_idx "
            f"ON {_qt(schema, 'facility_anchor')} (health_center_organization_id) "
            "WHERE health_center_organization_id IS NOT NULL;"
        )

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, 'facility_anchor_npi_override')} (
            facility_anchor_id varchar(128) NOT NULL,
            npi bigint NOT NULL,
            status varchar(32) NOT NULL DEFAULT 'approved',
            confidence double precision,
            method varchar(64),
            source varchar(128),
            evidence json,
            reviewed_by varchar(128),
            reviewed_at timestamp without time zone,
            updated_at timestamp without time zone,
            PRIMARY KEY (facility_anchor_id, npi)
        );
        """
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_override_status_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_override')} (status);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_override_npi_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_override')} (npi);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_override_updated_at_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_override')} (updated_at);"
    )
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, 'facility_anchor_npi_candidate')} (
            candidate_id varchar(64) NOT NULL,
            location_key varchar(64) NOT NULL,
            address_key uuid,
            facility_anchor_id varchar(128) NOT NULL,
            facility_type varchar(64),
            entity_name varchar(256),
            first_line varchar(256),
            city_name varchar(128),
            state_name varchar(2),
            postal_code varchar(16),
            telephone_number varchar(32),
            candidate_npi bigint,
            candidate_method varchar(64),
            candidate_priority integer,
            candidate_rank integer NOT NULL DEFAULT 1,
            candidate_count integer NOT NULL DEFAULT 0,
            candidate_status varchar(32) NOT NULL DEFAULT 'needs_review',
            review_status varchar(32) NOT NULL DEFAULT 'needs_review',
            match_confidence double precision,
            evidence json,
            reviewed_by varchar(128),
            reviewed_at timestamp without time zone,
            source_run_id varchar(32) NOT NULL,
            updated_at timestamp without time zone,
            PRIMARY KEY (candidate_id)
        );
        """
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_candidate_facility_anchor_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_candidate')} (facility_anchor_id);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_candidate_location_key_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_candidate')} (location_key);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_candidate_address_key_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_candidate')} (address_key) "
        "WHERE address_key IS NOT NULL;"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_candidate_candidate_npi_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_candidate')} (candidate_npi) "
        "WHERE candidate_npi IS NOT NULL;"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_candidate_candidate_status_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_candidate')} (candidate_status);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_candidate_review_status_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_candidate')} (review_status);"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS facility_anchor_npi_candidate_source_run_id_idx "
        f"ON {_qt(schema, 'facility_anchor_npi_candidate')} (source_run_id);"
    )


def downgrade():
    schema = _schema()
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'facility_anchor_npi_candidate')};")
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, 'facility_anchor_npi_override')};")
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.facility_anchor_health_center_org_idx;")
    op.execute(f"DROP INDEX IF EXISTS {_q(schema)}.facility_anchor_health_center_number_idx;")
    for column in (
        "health_center_organization_zip_code",
        "health_center_organization_state",
        "health_center_organization_city",
        "health_center_organization_address_line1",
        "health_center_name",
        "bphc_assigned_number",
        "health_center_organization_id",
        "health_center_number",
    ):
        op.execute(f"ALTER TABLE {_qt(schema, 'facility_anchor')} DROP COLUMN IF EXISTS {column};")
