# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provide a write seal for newly finalized Doctors families."""

import os
import re

from alembic import op

revision = "20260930130000_cms_doctors_prepared_seal"
down_revision = "20260930120000_cms_native_input_revision"
branch_labels = None
depends_on = None


def _schema():
    """Use only the configured, validated database namespace."""
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("cms_doctors_prepared_schema_invalid")
    return f'"{schema}"'


def upgrade():
    """Leave historical tables untouched; new preparation installs the exact trigger."""
    schema = _schema()
    op.execute(f"""CREATE FUNCTION {schema}.cms_doctors_prepared_immutable() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog
        AS $$BEGIN RAISE EXCEPTION 'cms_doctors_prepared_read_only'; END;$$""")
    op.execute(f"REVOKE ALL ON FUNCTION {schema}.cms_doctors_prepared_immutable() FROM PUBLIC")


def downgrade():
    """Refuse to remove a function protecting any prepared or retained generation."""
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(f"DROP FUNCTION {_schema()}.cms_doctors_prepared_immutable() RESTRICT")
