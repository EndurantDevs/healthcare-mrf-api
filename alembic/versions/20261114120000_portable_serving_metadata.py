# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allow portable serving origins and logical recovery documents."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import sqlalchemy as sa

from alembic import op
from db.plan_release_storage import plan_release_schema, storage_identifier, storage_relation

revision = "20261114120000_portable_serving_metadata"
down_revision = "20261114020000_custom_import_snapshot_metadata"
branch_labels = None
depends_on = None
_GENERATION = "entity_address_result_generation"
_JOURNAL = "provider_directory_serving_recovery"
_CMS_RECEIPT = "provider_directory_cms_serving_receipt"


def _schema() -> str:
    schema = plan_release_schema()
    storage_identifier(schema)
    return schema


def _generation_check() -> str:
    return (
        "singleton IS TRUE AND local_generation >= 0 AND ("
        "(origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL) OR ("
        "origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL "
        "AND origin_generation > 0 AND published_at IS NOT NULL))"
    )


def _journal_check(*, portable: bool) -> str:
    versions = "IN ('1','2')" if portable else "='1'"
    return (
        f"jsonb_typeof(document)='object' AND document->>'version' {versions} AND octet_length(document::text)<=2097152"
    )


def _has_relation(bind, schema: str, table: str) -> bool:
    return bool(
        bind.execute(
            sa.text("SELECT to_regclass(:name) IS NOT NULL"), {"name": storage_relation(schema, table)}
        ).scalar()
    )


def _replace_check(table: str, name: str, expression: str) -> None:
    op.execute(
        f"ALTER TABLE {table} DROP CONSTRAINT {storage_identifier(name)}, "
        f"ADD CONSTRAINT {storage_identifier(name)} CHECK ({expression})"
    )


def _portable_native(value: str) -> str:
    for family, field in (
        ("address", "relation_oids"),
        ("doctors", "relation_oids"),
        ("profile", "profile_target_oid"),
        ("profile", "evidence_target_oid"),
    ):
        value = f"({value} #- ARRAY['{family}','{field}'])"
    return f"({value} - 'overlay_oid')"


def _portable_family(value: str, family: str) -> str:
    fields = "ARRAY['profile_target_oid','evidence_target_oid']" if family == "profile" else "'relation_oids'"
    return f"(({value}) - {fields})"


def _replace_once(definition: str, previous: str, current: str) -> str:
    if definition.count(previous) != 1:
        raise RuntimeError("portable_serving_metadata_routine_expression_changed")
    return definition.replace(previous, current, 1)


def _named_family(schema: str, names: str) -> str:
    literal = "'" + schema.replace("'", "''") + "'"
    return (
        f"NOT EXISTS (SELECT 1 FROM unnest({names}) relation(name) "
        f"WHERE to_regclass(format('%I.%I',{literal},name)) IS NULL)"
    )


def _portable_snapshot_definition(definition: str) -> str:
    """Project the existing snapshot without retired local identities."""
    for alias in ("a", "d"):
        definition = _replace_once(definition, f"'relation_oids',{alias}.relation_oids", "'relation_oids',NULL")
    for field in ("profile_target_oid", "evidence_target_oid"):
        definition = _replace_once(definition, f"'{field}',p.{field}", f"'{field}',NULL")
    definition, count = re.subn(r"('overlay_oid',)to_regclass\('[^']+'\)::oid::bigint", r"\1NULL", definition)
    if count != 1:
        raise RuntimeError("portable_serving_metadata_routine_expression_changed")
    return definition


def _portable_transition_definition(name: str, definition: str) -> str:
    """Keep transition comparisons independent of legacy identity fields."""
    fields = (
        ("profile_target_oid", "evidence_target_oid")
        if name == "cms_serving_profile_transition"
        else ("relation_oids",)
    )
    for alias in ("OLD", "NEW"):
        for field in fields:
            previous = f"'{field}',{alias}.{field}"
            if definition.count(previous) != (2 if alias == "OLD" else 1):
                raise RuntimeError("portable_serving_metadata_routine_expression_changed")
            definition = definition.replace(previous, f"'{field}',NULL")
    return definition


def _portable_native_match_definition(definition: str, schema: str) -> str:
    """Preserve data predicates while matching the logical serving family."""
    namespace = storage_identifier(schema)
    definition = _replace_once(
        definition,
        f"p={namespace}.cms_serving_native_snapshot()",
        f"{_portable_native('p')}={_portable_native(f'{namespace}.cms_serving_native_snapshot()')}",
    )
    for family in ("doctors", "address"):
        pattern = (
            rf"p->'{family}'->'relation_oids'=" + re.escape(namespace) + r"\.cms_serving_oids\((ARRAY\[[^\]]+\])\)"
        )
        definition, count = re.subn(pattern, lambda match: _named_family(schema, match[1]), definition)
        if count != 1:
            raise RuntimeError("portable_serving_metadata_routine_expression_changed")
    for field, relation in (
        ("profile_target_oid", "provider_directory_profile"),
        ("evidence_target_oid", "provider_directory_profile_evidence"),
    ):
        relation = f"{namespace}.{relation}"
        definition = _replace_once(
            definition,
            f"(p->'profile'->>'{field}')::bigint=to_regclass('{relation}')::oid::bigint",
            f"to_regclass('{relation}') IS NOT NULL",
        )
    return definition


def _portable_receipt_match_definition(name: str, definition: str, schema: str) -> str:
    """Match accepted predecessor metadata by portable origin and names."""
    namespace = storage_identifier(schema)
    for family in ("profile", "address", "doctors"):
        current = _portable_family(f"current_native->'{family}'", family)
        expected = _portable_family(f"p->'{family}'", family)
        definition = _replace_once(definition, f"current_native->'{family}'=p->'{family}'", f"{current}={expected}")
    if name == "cms_serving_snapshot_receipt_matches":
        definition = _replace_once(
            definition,
            "current_native->'overlay_oid'=p->'overlay_oid'",
            f"to_regclass('{namespace}.provider_directory_address_overlay') IS NOT NULL",
        )
    return definition


def _portable_previous(value):
    """Ignore retired local IDs when comparing predecessor metadata."""
    return (
        f"(CASE WHEN kind='profile' THEN ({value}) - ARRAY['profile_target_oid','evidence_target_oid'] "
        f"WHEN kind IN ('address','doctors') THEN ({value}) - 'relation_oids' ELSE ({value}) END)"
    )


def _portable_routine_definition(name: str, definition: str, schema: str) -> str:
    """Change existing identity expressions while preserving every business predicate."""
    namespace = storage_identifier(schema)
    if name == "cms_serving_native_snapshot":
        return _portable_snapshot_definition(definition)
    if name in {"cms_serving_address_transition", "cms_serving_doctors_transition", "cms_serving_profile_transition"}:
        return _portable_transition_definition(name, definition)
    if name == "cms_serving_native_matches":
        return _portable_native_match_definition(definition, schema)
    if name in {"cms_serving_current_receipt_matches", "cms_serving_snapshot_receipt_matches"}:
        return _portable_receipt_match_definition(name, definition, schema)
    if name == "cms_serving_receipt_matches":
        return _replace_once(
            definition,
            "(p->>'overlay_oid')::bigint IS NULL",
            f"to_regclass('{namespace}.provider_directory_address_overlay') IS NULL",
        )
    if name == "cms_serving_require_fresh":
        return _replace_once(
            definition,
            "previous IS DISTINCT FROM prior->kind",
            f"{_portable_previous('previous')} IS DISTINCT FROM {_portable_previous('prior->kind')}",
        )
    raise RuntimeError("portable_serving_metadata_routine_unknown")


def _portable_cms_routines(bind, schema: str) -> None:
    if not _has_relation(bind, schema, _CMS_RECEIPT):
        return
    for name, arguments in (
        ("cms_serving_native_snapshot", ""),
        ("cms_serving_native_matches", "jsonb"),
        ("cms_serving_current_receipt_matches", "jsonb"),
        ("cms_serving_snapshot_receipt_matches", "jsonb"),
        ("cms_serving_receipt_matches", "jsonb"),
        ("cms_serving_require_fresh", "text,jsonb"),
        ("cms_serving_address_transition", ""),
        ("cms_serving_doctors_transition", ""),
        ("cms_serving_profile_transition", ""),
    ):
        signature = f"{storage_relation(schema, name)}({arguments})"
        definition = bind.execute(
            sa.text("SELECT pg_get_functiondef(CAST(:signature AS regprocedure))"), {"signature": signature}
        ).scalar_one()
        # Replacing an existing definition preserves its signature, invoker/definer settings and grants.
        op.execute(_portable_routine_definition(name, definition, schema))


def upgrade() -> None:
    """Validate the journal first and hold the serving counter lock only at commit."""
    schema = _schema()
    bind = op.get_bind()
    op.execute("SET LOCAL lock_timeout='50ms'")
    if _has_relation(bind, schema, _JOURNAL):
        _replace_check(
            storage_relation(schema, _JOURNAL), "pd_serving_recovery_document", _journal_check(portable=True)
        )
    _portable_cms_routines(bind, schema)
    if _has_relation(bind, schema, _GENERATION):
        _replace_check(
            storage_relation(schema, _GENERATION), "entity_address_result_generation_shape_check", _generation_check()
        )


def _legacy_generation_check() -> str:
    path = Path(__file__).with_name("20260914100000_entity_address_result_generation.py")
    spec = importlib.util.spec_from_file_location("_legacy_address_generation_shape", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("portable_serving_metadata_legacy_migration_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._shape_check()


def downgrade() -> None:
    """Refuse installed portable routines and incompatible data before changing DDL."""
    schema = _schema()
    bind = op.get_bind()
    if _has_relation(bind, schema, _CMS_RECEIPT):
        raise RuntimeError("portable_serving_metadata_cms_routines_prevent_downgrade")
    has_journal = _has_relation(bind, schema, _JOURNAL)
    has_generation = _has_relation(bind, schema, _GENERATION)
    if (
        has_journal
        and bind.execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {storage_relation(schema, _JOURNAL)} WHERE document->>'version'='2')"
            )
        ).scalar()
    ):
        raise RuntimeError("portable_serving_metadata_v2_journal_prevents_downgrade")
    legacy_shape = _legacy_generation_check()
    if (
        has_generation
        and bind.execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {storage_relation(schema, _GENERATION)} "
                f"WHERE ({legacy_shape}) IS NOT TRUE)"
            )
        ).scalar()
    ):
        raise RuntimeError("portable_serving_metadata_origin_prevents_downgrade")
    op.execute("SET LOCAL lock_timeout='50ms'")
    if has_journal:
        _replace_check(
            storage_relation(schema, _JOURNAL), "pd_serving_recovery_document", _journal_check(portable=False)
        )
    if has_generation:
        _replace_check(
            storage_relation(schema, _GENERATION), "entity_address_result_generation_shape_check", legacy_shape
        )
