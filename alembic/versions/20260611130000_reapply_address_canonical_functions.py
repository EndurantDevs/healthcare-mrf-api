"""Reapply canonical address v1 SQL functions.

The foundation revision was hardened during review after some dev databases had
already applied it. Alembic will not re-run an edited revision, so this migration
replays the CREATE OR REPLACE FUNCTION block without touching tables or data.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op


revision = "20260611130000_reapply_address_canonical_functions"
down_revision = "20260611100000_address_canonical_foundation"
branch_labels = None
depends_on = None


def _foundation_module():
    path = Path(__file__).with_name("20260611100000_address_canonical_foundation.py")
    spec = importlib.util.spec_from_file_location("_address_canonical_foundation", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load address canonical foundation migration from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def upgrade():
    foundation = _foundation_module()
    schema = foundation._schema()
    foundation._exec_sql_batch(op.get_bind(), foundation._create_functions_sql(schema))


def downgrade():
    # No downgrade: this migration only refreshes deterministic v1 function
    # definitions. The prior table/data state remains valid.
    return None
