"""Reapply canonical functions for an incomplete line-two unit marker."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op


revision = "20260713193000_address_canonical_incomplete_line2_unit"
down_revision = "20260713154500_address_canonical_repeated_bare_unit_value"
branch_labels = None
depends_on = None


def _migration_module(filename: str, module_name: str):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load address canonical migration from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _incomplete_line2_unit_is_current(bind, foundation, schema: str) -> bool:
    qschema = foundation._quote_ident(schema)
    return bool(
        bind.exec_driver_sql(
            f"""
            SELECT
                {qschema}.addr_unit_norm_v1(
                    '209 S HOUSTON AVE DEPT',
                    'DEPARTMENT'
                ) = ''
                AND {qschema}.addr_street_norm_v1(
                    '209 S HOUSTON AVE DEPT',
                    'DEPARTMENT'
                ) = '209shoustonavedeptdepartment'
                AND {qschema}.addr_identity_key_v1(
                    '209 S HOUSTON AVE DEPT',
                    'DEPARTMENT',
                    'CAMERON',
                    'TX',
                    '76520',
                    ''
                ) = 'v2|209shoustonavedeptdepartment|||TX|76520|US|street';
            """
        ).scalar()
    )


def upgrade():
    foundation = _migration_module(
        "20260611100000_address_canonical_foundation.py",
        "_address_canonical_foundation_incomplete_line2_unit",
    )
    prior_replay = _migration_module(
        "20260625123000_reapply_address_canonical_unit_parser.py",
        "_address_canonical_unit_replay",
    )
    schema = foundation._schema()
    bind = op.get_bind()
    if not prior_replay._functions_owned_by_current_user(bind, schema):
        if _incomplete_line2_unit_is_current(bind, foundation, schema):
            return
        raise RuntimeError(
            "Canonical address SQL functions need the incomplete line-two unit replay, "
            f"but the current database user does not own functions in schema {schema!r}."
        )
    foundation._exec_sql_batch(bind, foundation._create_functions_sql(schema))
    if not _incomplete_line2_unit_is_current(bind, foundation, schema):
        raise RuntimeError("Canonical incomplete line-two unit replay did not take effect")


def downgrade():
    return None
