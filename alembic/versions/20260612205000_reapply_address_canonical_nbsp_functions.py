"""Reapply canonical address functions after whitespace hardening.

Some databases had already applied the earlier function replay before the
non-breaking-space normalization fix landed in the foundation revision. Alembic
does not re-run edited revisions, so replay the deterministic canonical
functions at a new head without touching tables or data.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op


revision = "20260612205000_reapply_address_canonical_nbsp_functions"
down_revision = "20260612193000_address_key_from_identity_function"
branch_labels = None
depends_on = None

FUNCTION_NAMES = (
    "addr_clean_alnum_v1",
    "addr_space_norm_v1",
    "addr_zip5_norm_v1",
    "addr_country_code_v1",
    "addr_state_code_v1",
    "addr_unit_prefix_v1",
    "addr_unit_range_required_v1",
    "addr_unit_value_valid_v1",
    "addr_unit_norm_v1",
    "addr_street_token_norm_v1",
    "addr_street_token_is_suffix_v1",
    "addr_street_token_is_directional_v1",
    "addr_street_token_norm_context_v1",
    "addr_street_text_v1",
    "addr_street_norm_v1",
    "addr_street_suffix_token_v1",
    "addr_street_suffixless_norm_v1",
    "addr_street_direction_index_v1",
    "addr_street_direction_token_v1",
    "addr_street_directionless_norm_v1",
    "addr_street_completion_norm_v1",
    "addr_city_norm_v1",
    "addr_identity_key_v1",
    "addr_key_from_identity_v1",
    "addr_key_v1",
    "addr_premise_identity_key_v1",
    "addr_premise_key_v1",
)


def _foundation_module():
    path = Path(__file__).with_name("20260611100000_address_canonical_foundation.py")
    spec = importlib.util.spec_from_file_location("_address_canonical_foundation", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load address canonical foundation migration from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _functions_owned_by_current_user(bind, schema: str) -> bool:
    names = ", ".join(_sql_literal(name) for name in FUNCTION_NAMES)
    schema_lit = _sql_literal(schema)
    return bool(
        bind.exec_driver_sql(
            f"""
            SELECT COALESCE(bool_and(pg_get_userbyid(p.proowner) = current_user), true)
              FROM pg_proc p
              JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = {schema_lit}
               AND p.proname IN ({names});
            """
        ).scalar()
    )


def _nbsp_functions_current(bind, foundation, schema: str) -> bool:
    qschema = foundation._quote_ident(schema)
    return bool(
        bind.exec_driver_sql(
            f"""
            SELECT
                {qschema}.addr_unit_norm_v1(
                    '27 Dr Mellichamp Dr' || chr(160) || 'Ste 100',
                    ''
                ) = 'ste100'
                AND {qschema}.addr_street_norm_v1(
                    '27 Dr Mellichamp Dr' || chr(160) || 'Ste 100',
                    ''
                ) = '27drmellichampdr';
            """
        ).scalar()
    )


def upgrade():
    foundation = _foundation_module()
    schema = foundation._schema()
    bind = op.get_bind()
    if not _functions_owned_by_current_user(bind, schema):
        if _nbsp_functions_current(bind, foundation, schema):
            return
        raise RuntimeError(
            "Canonical address SQL functions need the NBSP replay, but the "
            f"current database user does not own functions in schema {schema!r}. "
            "Run this migration as the function owner/superuser, or replay "
            "20260611100000_address_canonical_foundation._create_functions_sql "
            "as the owner before stamping this revision."
        )
    foundation._exec_sql_batch(bind, foundation._create_functions_sql(schema))


def downgrade():
    return None
