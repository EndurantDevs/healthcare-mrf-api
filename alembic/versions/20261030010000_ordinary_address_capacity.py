# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit ordinary Address receipts and consumption without changing ledger custody."""

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261030010000_ordinary_address_capacity"
down_revision = "20261025020000_company_affiliation_join"
branch_labels = None
depends_on = None
_TABLE = "provider_directory_profile_capacity_lease_consumption"
_CHECK = "pd_profile_capacity_consumption_purpose_check"
_PREFLIGHT_TABLE = "provider_directory_profile_capacity_preflight_receipt"
_PREFLIGHT_CHECK = "pd_profile_capacity_preflight_values_check"
_RECEIPT = "healthporta.provider-directory-profile-capacity-preflight.v6"
_REQUEST = "healthporta.provider-directory-profile-capacity-preflight-request.v6"


def _previous_migration():
    """Preserve the installed predecessor predicate rather than restating its history."""
    path = Path(__file__).with_name("20261001110000_profile_initial_publication.py")
    spec = importlib.util.spec_from_file_location("_ordinary_capacity_predecessor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("ordinary_address_capacity_predecessor_unavailable")
    previous = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(previous)
    return previous


def _locked_tables(previous):
    schema = previous._ORIGINAL._schema()
    tables = tuple(previous._qt(schema, name) for name in (_TABLE, _PREFLIGHT_TABLE))
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("LOCK TABLE " + ", ".join(tables) + " IN ACCESS EXCLUSIVE MODE")
    return tables


def _purpose_check(*, ordinary):
    values = "'profile','cms_nonprofile'" + (",'serving_nonprofile'" if ordinary else "")
    return f"admission_purpose IN ({values})"


def _ordinary_values_check(previous):
    """Add one closed version with the paired Profile mode and unchanged common checks."""
    common = "limits_contract_id = " + previous._ORIGINAL._values_check().split("AND limits_contract_id = ", 1)[1]
    old_mode = "AND materialization_mode = 'source_delta' "
    if common.count(old_mode) != 1:
        raise RuntimeError("ordinary_address_capacity_predecessor_changed")
    common = common.replace(old_mode, "")
    geometry = "receipt_json::jsonb->'capacity_geometry'"
    scope = geometry + "->'ordinary_scope'"
    mode = f"""((materialization_mode='source_delta' AND {scope}->>'retention_kind'='delta_preimage')
        OR (materialization_mode='full_swap' AND {scope}->>'retention_kind'='snapshot'
        AND receipt_json::jsonb->'serving_generation_preflight'->>'contract_id'={previous._literal(previous._TARGET)}))"""
    added = f"""contract_id={previous._literal(_RECEIPT)} AND request_contract_id={previous._literal(_REQUEST)}
        AND receipt_json::jsonb->>'contract_id'=contract_id
        AND receipt_json::jsonb->>'request_contract_id'=request_contract_id
        AND receipt_json::jsonb->'profile_execution_identity'->>'materialization_mode'=materialization_mode
        AND jsonb_typeof({geometry})='object' AND jsonb_typeof({scope})='object'
        AND {geometry}->>'contract_id'='provider-directory-serving-nonprofile-capacity.v1'
        AND {geometry}->>'admission_purpose'='serving_nonprofile'
        AND {scope}->>'contract_id'='provider-directory-ordinary-serving-scope.v1'
        AND {scope}->>'input_sha256' ~ '^[0-9a-f]{{64}}$'
        AND {mode} AND {common}"""
    return f"({previous._initial_preflight_check()}) OR COALESCE(({added}),false)"


def _assert_check(table, name, expected):
    """Compare parsed native predicates under the table lock using application SQL."""
    probe = name[:48] + "_ordinary_probe"
    op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {probe} CHECK ({expected}) NOT VALID")
    valid = (
        op.get_bind()
        .execute(
            text("""
        SELECT EXISTS (SELECT 1 FROM pg_constraint live JOIN pg_constraint probe
          ON probe.conrelid=live.conrelid AND probe.conname=:probe
        JOIN pg_class relation ON relation.oid=live.conrelid
        WHERE live.conrelid=to_regclass(:table_name) AND live.conname=:name
          AND relation.relkind='r' AND relation.relpersistence='p'
          AND live.contype='c' AND live.convalidated AND NOT live.condeferrable
          AND NOT live.condeferred AND NOT live.connoinherit
          AND pg_get_expr(live.conbin,live.conrelid)=pg_get_expr(probe.conbin,probe.conrelid))
        """),
            {"probe": probe, "table_name": table, "name": name},
        )
        .scalar()
    )
    if valid is not True:
        raise RuntimeError("ordinary_address_capacity_constraint_drift")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {probe}")


def _replace_check(table, name, expected, replacement):
    _assert_check(table, name, expected)
    successor = name[:48] + "_ordinary_next"
    op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {successor} CHECK ({replacement}) NOT VALID")
    op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {successor}")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {name}")
    op.execute(f"ALTER TABLE {table} RENAME CONSTRAINT {successor} TO {name}")


def upgrade():
    """Keep unique identities and immutable history while adding the exact new pair."""
    previous = _previous_migration()
    consumption, preflight = _locked_tables(previous)
    _replace_check(consumption, _CHECK, _purpose_check(ordinary=False), _purpose_check(ordinary=True))
    _replace_check(preflight, _PREFLIGHT_CHECK, previous._initial_preflight_check(), _ordinary_values_check(previous))


def downgrade():
    """Refuse to hide issued or consumed ordinary authority or change historical bytes."""
    previous = _previous_migration()
    consumption, preflight = _locked_tables(previous)
    history = (
        op.get_bind()
        .execute(
            text(
                f"SELECT EXISTS (SELECT 1 FROM {consumption} WHERE admission_purpose='serving_nonprofile') "
                f"OR EXISTS (SELECT 1 FROM {preflight} WHERE contract_id=:receipt OR request_contract_id=:request)"
            ),
            {"receipt": _RECEIPT, "request": _REQUEST},
        )
        .scalar()
    )
    if history is not False:
        raise RuntimeError("ordinary_address_capacity_history_requires_retention")
    _replace_check(preflight, _PREFLIGHT_CHECK, _ordinary_values_check(previous), previous._initial_preflight_check())
    _replace_check(consumption, _CHECK, _purpose_check(ordinary=True), _purpose_check(ordinary=False))
