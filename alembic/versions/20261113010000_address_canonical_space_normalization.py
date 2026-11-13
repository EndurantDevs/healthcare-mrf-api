"""Reapply canonical address functions with collapsed, trimmed spacing."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261113010000_address_canonical_space_normalization"
down_revision = "20261111010000_code_sets_publication_idempotency"
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


def _is_space_normalization_current(bind, foundation, schema: str) -> bool:
    qschema = foundation._quote_ident(schema)
    return bool(
        bind.exec_driver_sql(
            f"""
            SELECT
                {qschema}.addr_space_norm_v1(
                    '  100' || chr(12288) || chr(9) || chr(8200) || ' Main ' || chr(8239)
                ) = '100 Main'
                AND {qschema}.addr_identity_key_v1(
                    '100 Main Street Suite 200  A ',
                    '200 A',
                    'Austin',
                    'TX',
                    '78701',
                    'US'
                ) = 'v2|100mainst|ste200a||TX|78701|US|street';
            """
        ).scalar()
    )


def upgrade():
    """Replay the canonical address functions and confirm the spacing rule is live."""
    foundation = _migration_module(
        "20260611100000_address_canonical_foundation.py",
        "_address_canonical_foundation_space_normalization",
    )
    prior_replay = _migration_module(
        "20260625123000_reapply_address_canonical_unit_parser.py",
        "_address_canonical_unit_replay_space_normalization",
    )
    schema = foundation._schema()
    bind = op.get_bind()
    if not prior_replay._functions_owned_by_current_user(bind, schema):
        if _is_space_normalization_current(bind, foundation, schema):
            return
        raise RuntimeError(
            "Canonical address SQL functions need the space normalization replay, "
            f"but the current database user does not own functions in schema {schema!r}."
        )
    foundation._exec_sql_batch(bind, foundation._create_functions_sql(schema))
    if not _is_space_normalization_current(bind, foundation, schema):
        raise RuntimeError("Canonical space normalization replay did not take effect")


def downgrade():
    """Keep the replayed functions; the prior definitions keyed spacing variants apart."""
    return None
