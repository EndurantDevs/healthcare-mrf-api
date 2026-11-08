# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare provider publication resources, provenance and indexes before cutover."""

import os
import re

from alembic import op
from db.migration_provider_dataset_online import migrate_dataset_candidates

revision = "20261005130000_provider_dataset_candidates"
down_revision = "20261005120000_practitioner_set_validation"
branch_labels = None
depends_on = None


def upgrade():
    """Preserve snapshot storage and install isolated atomic publication."""
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    schema = runtime or legacy or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise RuntimeError("provider_dataset_schema_invalid")
    migrate_dataset_candidates(op, schema)


def downgrade():
    """Retain immutable snapshots until an explicit forward data migration."""
    raise RuntimeError("provider_dataset_candidates_requires_forward_migration")
