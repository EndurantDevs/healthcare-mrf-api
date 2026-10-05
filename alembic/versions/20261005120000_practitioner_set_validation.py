# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit bounded Practitioner sets into isolated acquisition partitions."""

import os
import re
from pathlib import Path

from alembic import op
from db.migration_practitioner_candidates import migrate_practitioner_candidates

revision = "20261005120000_practitioner_set_validation"
down_revision = "20261005110000_ptg_set_validation"
branch_labels = None
depends_on = None


def upgrade():
    """Keep historical storage and replace per-resource guards with set admission."""
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    schema = runtime or legacy or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise RuntimeError("practitioner_schema_invalid")
    sql_path = Path(__file__).resolve().parents[2] / "db/sql/practitioner_set_validation.sql"
    sql = sql_path.read_text().replace("__SCHEMA__", schema)
    migrate_practitioner_candidates(op, schema, sql)


def downgrade():
    """Avoid rewriting immutable acquisition storage during rollback."""
    raise RuntimeError("practitioner_set_validation_downgrade_requires_explicit_data_migration")
