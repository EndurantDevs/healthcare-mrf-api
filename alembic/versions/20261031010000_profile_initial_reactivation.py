# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain immutable initial history while admitting signed recovered publications."""

import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261031010000_profile_initial_reactivation"
down_revision = "20261030010000_ordinary_address_capacity"
branch_labels = None
depends_on = None
_RECEIPT = "provider_directory_profile_initial_receipt"
_KEY_CHECK = "provider_directory_profile_initial_receipt_singleton_key_check"
_TARGET = "healthporta.provider-directory-profile-initial-target-state.v2"
_OLD_KEY = "singleton_key='global'"
_NEW_KEY = """singleton_key='global' OR COALESCE(
    singleton_key=to_hex((payload->'serving'->>'control_generation')::bigint)
    AND (payload->'serving'->>'control_generation')::bigint>0,false)"""
_OLD_DEFAULT = "'global'::character varying"
_NEW_DEFAULT = (
    "(COALESCE(NULLIF(current_setting('provider_directory.initial_receipt_key'::text, true), "
    "''::text), 'global'::text))::character varying"
)


def _previous():
    path = Path(__file__).with_name("20261030010000_ordinary_address_capacity.py")
    spec = importlib.util.spec_from_file_location("_profile_reactivation_previous", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("profile_reactivation_predecessor_unavailable")
    previous = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(previous)
    return previous, previous._previous_migration()


def _matches_sql(previous, schema):
    """Keep the frozen original predicate and authenticate the exact new key purpose."""
    target = "preflight.receipt_json::jsonb->'serving_generation_preflight'"
    signed = "consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_receipt'"
    predicate = f"""
          AND {signed}->'serving_generation_preflight'={target}
          AND ((receipt.singleton_key='global'
            AND {target}->>'contract_id'='healthporta.provider-directory-profile-initial-target-state.v1')
          OR (receipt.singleton_key=to_hex(serving.control_generation) AND serving.control_generation>0
            AND preflight.control_generation=serving.control_generation
            AND {target}->>'contract_id'='healthporta.provider-directory-profile-initial-target-state.v2'
            AND {target}->'initial_commit_receipt_absent'='false'::jsonb
            AND {target}->'serving_singleton_absent'='true'::jsonb
            AND jsonb_typeof({target}->'recovered_initial_publication')='object'))"""
    suffix = "\n    ),false)"
    original = previous._receipt_matches_sql(schema)
    if original.count(suffix) != 1:
        raise RuntimeError("profile_reactivation_predecessor_changed")
    return original.replace(suffix, predicate + suffix)


def _recovered_preflight_check(ordinary, initial):
    """Extend only recovered Profile and ordinary Address; old CMS remains unchanged."""
    original = ordinary._ordinary_values_check(initial)
    target = "receipt_json::jsonb->'serving_generation_preflight'"
    recovered = f"""{target}->>'contract_id'='{_TARGET}'
        AND {target}->'initial_commit_receipt_absent'='false'::jsonb
        AND {target}->'serving_singleton_absent'='true'::jsonb
        AND jsonb_typeof({target}->'recovered_initial_publication')='object'
        AND materialization_mode='full_swap'"""
    amended = original.replace(initial._TARGET, _TARGET)
    added = f"""contract_id IN ('healthporta.provider-directory-profile-capacity-preflight.v5',
        'healthporta.provider-directory-profile-capacity-preflight.v6') AND ({recovered}) AND ({amended})"""
    return f"({original}) OR COALESCE(({added}),false)"


def _locked(ordinary, initial):
    schema = initial._ORIGINAL._schema()
    names = (_RECEIPT, initial._SERVING, ordinary._PREFLIGHT_TABLE, ordinary._TABLE)
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SELECT pg_advisory_xact_lock(hashtextextended('provider-directory-profile-capacity-preflight:v3',0))")
    op.execute("LOCK TABLE " + ",".join(initial._qt(schema, name) for name in names) + " IN ACCESS EXCLUSIVE MODE")
    return schema


def _function_identity(schema, expected):
    row = (
        op.get_bind()
        .execute(
            text("""SELECT p.oid::bigint AS oid,p.proowner::bigint AS owner,p.proacl::text AS acl
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_class c ON c.relnamespace=n.oid
        JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname=:schema
        AND p.proname='pd_profile_initial_receipt_matches' AND c.relname=:receipt
        AND p.pronargs=1 AND p.proargtypes[0]=c.reltype AND p.proargnames=ARRAY['receipt']::text[]
        AND p.prorettype='bool'::regtype AND p.prokind='f' AND l.lanname='sql'
        AND p.provolatile='v' AND NOT p.prosecdef AND NOT p.proretset AND NOT p.proisstrict
        AND NOT p.proleakproof AND p.proparallel='u' AND p.procost=100 AND p.prorows=0
        AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND p.prosrc=:body"""),
            {"schema": schema, "receipt": _RECEIPT, "body": " " + expected + " "},
        )
        .mappings()
        .all()
    )
    if len(row) != 1:
        raise RuntimeError("profile_reactivation_guard_changed")
    return dict(row[0])


def _key_default(schema):
    return (
        op.get_bind()
        .execute(
            text("""SELECT pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a
        JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=:schema AND c.relname=:receipt AND a.attname='singleton_key' AND NOT a.attisdropped"""),
            {"schema": schema, "receipt": _RECEIPT},
        )
        .scalar_one()
    )


def _physical_identity(schema):
    return (
        op.get_bind()
        .execute(
            text("""SELECT c.oid::bigint,c.relfilenode::bigint,c.relowner::bigint,
        c.reltoastrelid::bigint FROM pg_class c WHERE c.oid IN (
          SELECT root.oid FROM pg_class root JOIN pg_namespace n ON n.oid=root.relnamespace
          WHERE n.nspname=:schema AND root.relname=:receipt
          UNION SELECT i.indexrelid FROM pg_index i JOIN pg_class root ON root.oid=i.indrelid
          JOIN pg_namespace n ON n.oid=root.relnamespace WHERE n.nspname=:schema AND root.relname=:receipt)
        ORDER BY c.oid"""),
            {"schema": schema, "receipt": _RECEIPT},
        )
        .all()
    )


def _replace(ordinary, initial, schema, *, recovered):
    table = initial._qt(schema, _RECEIPT)
    preflight = initial._qt(schema, ordinary._PREFLIGHT_TABLE)
    old_body, new_body = initial._receipt_matches_sql(schema), _matches_sql(initial, schema)
    old_preflight, new_preflight = (
        ordinary._ordinary_values_check(initial),
        _recovered_preflight_check(ordinary, initial),
    )
    before_body, after_body = (old_body, new_body) if recovered else (new_body, old_body)
    before_check, after_check = (_OLD_KEY, _NEW_KEY) if recovered else (_NEW_KEY, _OLD_KEY)
    before_default, after_default = (_OLD_DEFAULT, _NEW_DEFAULT) if recovered else (_NEW_DEFAULT, _OLD_DEFAULT)
    before_preflight, after_preflight = (old_preflight, new_preflight) if recovered else (new_preflight, old_preflight)
    identity, physical = _function_identity(schema, before_body), _physical_identity(schema)
    if _key_default(schema) != before_default:
        raise RuntimeError("profile_reactivation_default_changed")
    ordinary._replace_check(table, _KEY_CHECK, before_check, after_check)
    ordinary._replace_check(preflight, ordinary._PREFLIGHT_CHECK, before_preflight, after_preflight)
    op.execute(f"ALTER TABLE {table} ALTER COLUMN singleton_key SET DEFAULT {after_default}")
    function = initial._qt(schema, "pd_profile_initial_receipt_matches")
    op.execute(
        f"CREATE OR REPLACE FUNCTION {function}(receipt {table}) RETURNS boolean LANGUAGE sql "
        f"VOLATILE SET search_path=pg_catalog AS $$ {after_body} $$"
    )
    if (
        _function_identity(schema, after_body) != identity
        or _physical_identity(schema) != physical
        or _key_default(schema) != after_default
    ):
        raise RuntimeError("profile_reactivation_native_identity_changed")


def upgrade():
    """Extend only the locked existing predicates, preserving all native identities."""
    ordinary, initial = _previous()
    _replace(ordinary, initial, _locked(ordinary, initial), recovered=True)


def downgrade():
    """Refuse to erase any issued recovered authority or immutable recovered receipt."""
    ordinary, initial = _previous()
    schema = _locked(ordinary, initial)
    receipt = initial._qt(schema, _RECEIPT)
    preflight = initial._qt(schema, ordinary._PREFLIGHT_TABLE)
    used = (
        op.get_bind()
        .execute(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {receipt} WHERE singleton_key<>'global') OR "
                f"EXISTS(SELECT 1 FROM {preflight} WHERE receipt_json::jsonb->'serving_generation_preflight'->>'contract_id'=:target)"
            ),
            {"target": _TARGET},
        )
        .scalar()
    )
    if used is not False:
        raise RuntimeError("profile_reactivation_history_requires_retention")
    _replace(ordinary, initial, schema, recovered=False)
