# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Install trigger-only address alias generation guards before explicit provisioning."""

import os

from alembic import op
from process.entity_address_alias_guard import generation_guard_statements

revision = "20261003000000_address_alias_generation_guard"
down_revision = "20261002040000_custom_import_child_memberships"
branch_labels = None
depends_on = None


def upgrade():
    """Preserve ordinary writes while installing reviewed code; do not transfer authority."""
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    for statement in generation_guard_statements(runtime or legacy or "mrf"):
        op.execute(statement)


def downgrade():
    """Never silently remove a guard that may now own protected counter authority."""
    raise RuntimeError("address alias guard downgrade requires explicit authority review")
