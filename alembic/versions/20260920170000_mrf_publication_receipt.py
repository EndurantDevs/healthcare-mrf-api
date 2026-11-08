# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Track completion separately from the MRF table-family rotation."""

import os
import re

from sqlalchemy import text

from alembic import op

revision = "20260920170000_mrf_publication_receipt"
down_revision = "20260920160000_geo_census_result_generation"
branch_labels = None
depends_on = None


def _table():
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"
    if (
        os.getenv("HLTHPRT_DB_SCHEMA")
        and os.getenv("DB_SCHEMA")
        and os.environ["HLTHPRT_DB_SCHEMA"] != os.environ["DB_SCHEMA"]
    ):
        raise RuntimeError("database schema settings differ")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema):
        raise RuntimeError("invalid MRF receipt schema")
    return f'"{schema}"."mrf_publication_receipt"'


def upgrade():
    """Create the durable singleton publication receipt without backfilling it."""

    # Existing tables receive no fabricated completion authority.
    op.execute(f"""
        CREATE TABLE {_table()} (
            singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            contract_version integer NOT NULL DEFAULT 1 CHECK (contract_version=1),
            attempt_id uuid NOT NULL,
            import_id text NOT NULL,
            state text NOT NULL CHECK (state IN ('pending','complete')),
            started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            completed_at timestamptz,
            generation jsonb,
            summary_oid oid,
            summary_inputs jsonb,
            address_resolution_performed boolean,
            address_content jsonb,
            CHECK ((state='pending' AND completed_at IS NULL AND generation IS NULL
                    AND summary_oid IS NULL AND summary_inputs IS NULL
                    AND address_resolution_performed IS NULL AND address_content IS NULL)
                OR (state='complete' AND completed_at IS NOT NULL AND generation IS NOT NULL
                    AND summary_oid IS NOT NULL AND summary_inputs IS NOT NULL
                    AND address_resolution_performed IS NOT NULL AND address_content IS NOT NULL))
        )
    """)


def downgrade():
    """Remove the receipt only when no publication evidence would be lost."""

    op.execute(f"LOCK TABLE {_table()} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().execute(text(f"SELECT EXISTS(SELECT 1 FROM {_table()})")).scalar():
        raise RuntimeError("MRF publication evidence must be reconciled before downgrade")
    op.execute(f"DROP TABLE {_table()}")
