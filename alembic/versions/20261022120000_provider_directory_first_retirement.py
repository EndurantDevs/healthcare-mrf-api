# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Permit evidence-bound retirement of a terminal first acquisition."""

import os

from alembic import op
from db import migration_provider_directory_first_acquisition_retirement as first
from db.migration_provider_directory_terminal_root_retirement_evidence import _qf

revision = "20261022120000_provider_directory_first_retirement"
down_revision = "20261022110000_ptg_checker_execution"
branch_labels = None
depends_on = None


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def upgrade():
    """Extend only the recognized existing retirement guard bodies."""
    for old, new in first.replacements(_schema()):
        for statement in first.guarded_replacement(old, new):
            op.execute(statement)


def downgrade():
    """Restore prior guards only when first-acquisition retirement is unused."""
    dataset = _qf(_schema(), "provider_directory_endpoint_dataset")
    op.execute(f"""DO $migration$ BEGIN
        IF EXISTS(SELECT FROM {dataset} WHERE status='acquisition_retired' AND previous_dataset_id IS NULL) THEN
            RAISE EXCEPTION 'provider_directory_first_retirement_downgrade_blocked' USING ERRCODE='55000';
        END IF;
    END $migration$;""")
    for old, new in reversed(first.replacements(_schema())):
        for statement in first.guarded_replacement(new, old):
            op.execute(statement)
