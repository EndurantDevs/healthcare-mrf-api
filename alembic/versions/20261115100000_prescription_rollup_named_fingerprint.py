# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Re-bind verified prescription autocomplete rollups to their provider relation name.

Rollups written before named publication store a numeric relation fingerprint. Each one keeps
serving only when its complete aggregate still equals the current provider table.
"""

import os

from sqlalchemy import text

from alembic import op
from db.prescription_autocomplete_rollup_sql import (
    prescription_autocomplete_rollup_insert_sql,
    prescription_autocomplete_source_fingerprint_sql,
)

revision = "20261115100000_prescription_rollup_named_fingerprint"
down_revision = "20261114190000_cms_native_input_named_revision"
branch_labels = None
depends_on = None

ROLLUP_TABLE = "pricing_provider_rx_rollup"
PROVIDER_TABLE = "pricing_provider_prescription"
_COLUMNS = (
    "year,rx_code_system,rx_code,variant_id,rx_name,generic_name,brand_name,total_claims,total_drug_cost,total_benes"
)


def _schema():
    return os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


def _qualified(schema, name):
    return '"' + schema.replace('"', '""') + '"."' + name.replace('"', '""') + '"'


def rebind_sql(schema):
    """Update numeric fingerprints only when the rollup equals the provider aggregate exactly."""
    expected = prescription_autocomplete_rollup_insert_sql(
        schema=schema, rollup_table=ROLLUP_TABLE, provider_table=PROVIDER_TABLE, select_only=True
    )
    rollup = _qualified(schema, ROLLUP_TABLE)
    actual = f"SELECT {_COLUMNS} FROM {rollup}"
    fingerprint = prescription_autocomplete_source_fingerprint_sql(schema=schema, provider_table=PROVIDER_TABLE)
    return f"""UPDATE {rollup} SET source_relation_fingerprint={fingerprint}
        WHERE source_relation_fingerprint ~ '^[0-9]+$' AND NOT EXISTS (
          WITH expected AS ({expected})
          SELECT 1 FROM (({actual} EXCEPT ALL SELECT {_COLUMNS} FROM expected)
            UNION ALL (SELECT {_COLUMNS} FROM expected EXCEPT ALL {actual})) differences)"""


def upgrade():
    """A rollup that differs keeps its old fingerprint, so readers still aggregate provider rows."""
    schema = _schema()
    if op.get_context().as_sql:
        return
    present = op.get_bind().execute(
        text("SELECT to_regclass(:rollup) IS NOT NULL AND to_regclass(:provider) IS NOT NULL"),
        {"rollup": _qualified(schema, ROLLUP_TABLE), "provider": _qualified(schema, PROVIDER_TABLE)},
    )
    if present.scalar():
        op.execute(rebind_sql(schema))


def downgrade():
    """Leave rows as they are; an unmatched fingerprint only routes readers to provider rows."""
