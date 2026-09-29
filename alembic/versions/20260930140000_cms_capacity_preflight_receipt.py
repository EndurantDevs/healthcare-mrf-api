# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit matched CMS preflight contracts without rewriting receipt history."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20260930140000_cms_capacity_preflight_receipt"
down_revision = "20260930130000_cms_doctors_prepared_seal"
branch_labels = None
depends_on = None

_PROFILE_RECEIPT = "healthporta.provider-directory-profile-capacity-preflight.v3"
_PROFILE_REQUEST = "healthporta.provider-directory-profile-capacity-preflight-request.v3"
_CMS_RECEIPT = "healthporta.provider-directory-profile-capacity-preflight.v4"
_CMS_REQUEST = "healthporta.provider-directory-profile-capacity-preflight-request.v4"
_PROBE = "pd_profile_capacity_preflight_values_probe"
_NEXT = "pd_profile_capacity_preflight_values_next"


def _original_migration():
    """Reuse the unchanged historical receipt predicate and namespace rules."""
    path = Path(__file__).with_name("20260811010000_provider_directory_profile_capacity_preflight_receipt.py")
    spec = importlib.util.spec_from_file_location("_cms_preflight_predecessor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cms_capacity_preflight_predecessor_unavailable")
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


_ORIGINAL = _original_migration()


def _cms_values_check() -> str:
    """Change only the matched receipt/request alternatives in the old check."""
    profile_pair = (
        f"contract_id = {_ORIGINAL._literal(_PROFILE_RECEIPT)} "
        f"AND request_contract_id = {_ORIGINAL._literal(_PROFILE_REQUEST)} "
    )
    original = _ORIGINAL._values_check()
    if not original.startswith(profile_pair + "AND limits_contract_id = "):
        raise RuntimeError("cms_capacity_preflight_predecessor_predicate_changed")
    cms_pair = (
        f"contract_id = {_ORIGINAL._literal(_CMS_RECEIPT)} AND request_contract_id = {_ORIGINAL._literal(_CMS_REQUEST)}"
    )
    return f"(({profile_pair.strip()}) OR ({cms_pair})) " + original[len(profile_pair) :]


def _locked_schema() -> str:
    """Bound the lock before any catalog check or immutable-history inspection."""
    schema = _ORIGINAL._schema()
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(f"LOCK TABLE {_ORIGINAL._qt(schema, _ORIGINAL._TABLE)} IN ACCESS EXCLUSIVE MODE")
    return schema


def _add_check(schema: str, name: str, condition: str) -> None:
    """Parse an exact predicate on the existing relation before validation."""
    op.execute(
        f"ALTER TABLE {_ORIGINAL._qt(schema, _ORIGINAL._TABLE)} "
        f"ADD CONSTRAINT {_ORIGINAL._q(name)} CHECK ({condition}) NOT VALID"
    )


def _assert_exact_check(schema: str, condition: str) -> None:
    """Reject changed check semantics or unvalidated/inherited ledger constraints."""
    _add_check(schema, _PROBE, condition)
    op.execute(f"""DO $$
        DECLARE live_row pg_constraint%ROWTYPE; probe_row pg_constraint%ROWTYPE;
        BEGIN
            SELECT constraint_row.* INTO STRICT live_row
              FROM pg_constraint AS constraint_row
              JOIN pg_class AS relation ON relation.oid=constraint_row.conrelid
              JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace
             WHERE namespace.nspname={_ORIGINAL._literal(schema)}
               AND relation.relname={_ORIGINAL._literal(_ORIGINAL._TABLE)}
               AND relation.relkind='r' AND relation.relpersistence='p'
               AND constraint_row.conname={_ORIGINAL._literal(_ORIGINAL._VALUES_CONSTRAINT)};
            SELECT constraint_row.* INTO STRICT probe_row FROM pg_constraint AS constraint_row
             WHERE constraint_row.conrelid=live_row.conrelid
               AND constraint_row.conname={_ORIGINAL._literal(_PROBE)};
            IF live_row.contype<>'c' OR NOT live_row.convalidated
               OR live_row.condeferrable OR live_row.condeferred OR live_row.connoinherit
               OR live_row.conbin IS DISTINCT FROM probe_row.conbin THEN
                RAISE EXCEPTION 'cms_capacity_preflight_constraint_drift';
            END IF;
        END;
    $$""")
    op.execute(f"ALTER TABLE {_ORIGINAL._qt(schema, _ORIGINAL._TABLE)} DROP CONSTRAINT {_ORIGINAL._q(_PROBE)}")


def _replace_check(schema: str, replacement: str) -> None:
    """Validate every retained row before atomically replacing only this check."""
    table = _ORIGINAL._qt(schema, _ORIGINAL._TABLE)
    _add_check(schema, _NEXT, replacement)
    op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {_ORIGINAL._q(_NEXT)}")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {_ORIGINAL._q(_ORIGINAL._VALUES_CONSTRAINT)}")
    op.execute(
        f"ALTER TABLE {table} RENAME CONSTRAINT {_ORIGINAL._q(_NEXT)} TO {_ORIGINAL._q(_ORIGINAL._VALUES_CONSTRAINT)}"
    )
    _assert_exact_check(schema, replacement)


def upgrade() -> None:
    """Keep Profile history and allow only the additional matched CMS pair."""
    schema = _locked_schema()
    _assert_exact_check(schema, _ORIGINAL._values_check())
    _replace_check(schema, _cms_values_check())


def downgrade() -> None:
    """Retain all history and refuse narrowing while any CMS version remains."""
    schema = _locked_schema()
    _assert_exact_check(schema, _cms_values_check())
    op.execute(f"""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM {_ORIGINAL._qt(schema, _ORIGINAL._TABLE)}
            WHERE contract_id={_ORIGINAL._literal(_CMS_RECEIPT)}
               OR request_contract_id={_ORIGINAL._literal(_CMS_REQUEST)}) THEN
            RAISE EXCEPTION 'cms_capacity_preflight_v4_history_requires_retention';
        END IF;
    END $$""")
    _replace_check(schema, _ORIGINAL._values_check())
