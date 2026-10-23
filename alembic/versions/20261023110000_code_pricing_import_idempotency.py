# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain the all-status owner of a complete code-pricing preparation request."""

import os

from sqlalchemy import text

from alembic import op
from db.migration_index_adoption import has_matching_index

revision = "20261023110000_code_pricing_import_idempotency"
down_revision = "20261022120000_provider_directory_first_retirement"
branch_labels = None
depends_on = None


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def upgrade():
    name = "import_run_code_pricing_idempotency_idx"
    options = {
        "unique": True,
        "postgresql_where": text("importer = 'plan-pricing-code-projection' AND idempotency_key IS NOT NULL"),
    }
    if not has_matching_index(op, name, "import_run", ["importer", "idempotency_key"], schema=_schema(), **options):
        op.create_index(name, "import_run", ["importer", "idempotency_key"], schema=_schema(), **options)


def downgrade():
    op.drop_index("import_run_code_pricing_idempotency_idx", table_name="import_run", schema=_schema())
