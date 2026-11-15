# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve the existing retained attachment guard by saved relation names."""

import os

from alembic import op
from db.plan_release_storage import storage_identifier
from process.source_profile_result_pins import _attachment_pin_guard_statements

revision = "20261114170000_source_profile_named_attachment"
down_revision = "20261114160000_npi_named_generation"
branch_labels = None
depends_on = None


def upgrade():
    """Replace only an installed guard, keeping its owner, ACL and statement triggers."""
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schema configuration differs")
    schema = runtime or legacy or "mrf"
    quoted_schema = storage_identifier(schema)
    signature = quoted_schema + '."provider_profile_attached_pin_guard"()'
    pin = quoted_schema + '."provider_profile_source_pin"'
    definition = next(
        statement
        for statement in _attachment_pin_guard_statements(schema)
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    )
    op.execute(
        "DO $migration$ BEGIN "
        + "IF to_regclass('"
        + pin
        + "') IS NULL THEN RETURN; END IF; "
        + "IF to_regprocedure('"
        + signature
        + "') IS NULL THEN "
        + "RAISE EXCEPTION 'source profile attachment guard is missing'; END IF; "
        + "EXECUTE $definition$"
        + definition
        + "$definition$; END $migration$;"
    )


def downgrade():
    """Keep the compatible named guard for both saved and current receipt editions."""
    return None
