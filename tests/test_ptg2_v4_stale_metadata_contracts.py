# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shape and migration-order contracts for stale V4 metadata."""

from __future__ import annotations

import pytest

from db.migration_ptg2_v4_import_job import (
    _has_import_job_table_under_lock,
)
from process.ptg_parts.ptg2_v4_attempt_registry import (
    ATTEMPT_ATTACHMENTS,
    ATTEMPT_STATE_TABLES,
)
from process.ptg_parts.ptg2_v4_stale_metadata_json import (
    database_json_digest,
)
from tests.ptg2_v4_stale_metadata_postgres_support import (
    _attempt_migration_sql,
    _load_attempt_migration,
    _migration_sql,
    load_attempt_fence_migration,
)
from tests.test_ptg2_v4_stale_metadata_reconcile import (
    _plan,
    _ready_context,
)


def _attachment_signatures(attachments) -> tuple:
    return tuple(
        (
            attachment.name,
            attachment.table_name,
            attachment.snapshot_columns,
            attachment.run_columns,
            attachment.statement_trigger,
        )
        for attachment in attachments
    )


def _top_level_sql_statement_count(sql: str) -> int:
    """Count statements while treating quoted function bodies as opaque."""

    statements = 0
    has_content = False
    index = 0
    quote: str | None = None
    while index < len(sql):
        if quote is not None:
            if (
                quote in {"'", '"'}
                and sql.startswith(quote * 2, index)
            ):
                index += 2
            elif sql.startswith(quote, index):
                index += len(quote)
                quote = None
            else:
                index += 1
            continue
        character = sql[index]
        if character in {"'", '"'}:
            quote = character
            has_content = True
            index += 1
            continue
        if character == "$":
            tag_end = sql.find("$", index + 1)
            if tag_end >= 0:
                tag = sql[index:tag_end + 1]
                if tag == "$$" or tag[1:-1].replace("_", "a").isalnum():
                    quote = tag
                    has_content = True
                    index = tag_end + 1
                    continue
        if character == ";":
            if has_content:
                statements += 1
                has_content = False
        elif not character.isspace():
            has_content = True
        index += 1
    return statements + int(has_content)


class _ScalarResult:
    def scalar_one(self):
        return 1


class _LockOrderConnection:
    def __init__(self) -> None:
        self.events: list[str] = []

    def execute(self, statement, _parameters):
        self.events.append(str(statement))
        return _ScalarResult()

    def exec_driver_sql(self, statement):
        self.events.append(str(statement))
        return _ScalarResult()


def test_import_job_existence_is_decided_after_parent_locks():
    connection = _LockOrderConnection()

    assert _has_import_job_table_under_lock(
        connection,
        "synthetic_ptg",
    )

    assert "pg_advisory_xact_lock" in connection.events[0]
    assert "LOCK TABLE" in connection.events[1]
    assert '"ptg2_snapshot"' in connection.events[1]
    assert '"ptg2_import_run"' in connection.events[1]
    assert "to_regclass" in connection.events[2]
    assert '"ptg2_import_job"' in connection.events[3]


def test_runtime_registry_matches_the_frozen_migration_catalog():
    migration = load_attempt_fence_migration()

    assert _attachment_signatures(ATTEMPT_ATTACHMENTS) == (
        _attachment_signatures(migration.ATTEMPT_ATTACHMENTS)
    )
    assert _attachment_signatures(ATTEMPT_STATE_TABLES) == (
        _attachment_signatures(migration.ATTEMPT_STATE_TABLES)
    )


def test_lock_order_migration_precedes_online_index_adoption():
    fence_migration = load_attempt_fence_migration()
    lock_migration = _load_attempt_migration(
        "20260724104500_ptg2_v4_attempt_lock_order.py"
    )
    index_migration = _load_attempt_migration(
        "20260724103000_ptg2_v4_attempt_indexes.py"
    )
    hardening_migration = _load_attempt_migration(
        "20260724110000_ptg2_v4_attempt_fence_hardening.py"
    )
    guarded_tables = tuple(
        attachment.table_name
        for attachment in (*ATTEMPT_STATE_TABLES, *ATTEMPT_ATTACHMENTS)
        if attachment.statement_trigger
    )

    assert lock_migration.down_revision == fence_migration.revision
    assert index_migration.down_revision == lock_migration.revision
    assert hardening_migration.down_revision == index_migration.revision
    assert len(lock_migration._GUARDED_TABLES) == len(guarded_tables)
    assert set(lock_migration._GUARDED_TABLES) == set(guarded_tables)


def test_fence_migration_installs_lifecycle_locks_before_row_guards():
    statements = _migration_sql(
        load_attempt_fence_migration(),
        "synthetic_ptg",
    )
    first_before_trigger = next(
        index
        for index, statement in enumerate(statements)
        if "BEFORE INSERT OR UPDATE OR DELETE" in statement
    )
    first_after_trigger = next(
        index
        for index, statement in enumerate(statements)
        if "AFTER INSERT" in statement
    )

    assert first_before_trigger < first_after_trigger


def test_hardening_revision_refuses_unverifiable_offline_adoption():
    hardening_migration = _load_attempt_migration(
        "20260724110000_ptg2_v4_attempt_fence_hardening.py"
    )

    statements = _migration_sql(hardening_migration, "synthetic_ptg")

    assert len(statements) == 1
    assert "hardening_requires_online_catalog" in statements[0]
    assert "ERRCODE = '55000'" in statements[0]


def test_attempt_migrations_emit_one_statement_per_execute():
    hardening_migration = _load_attempt_migration(
        "20260724110000_ptg2_v4_attempt_fence_hardening.py"
    )
    statements = (
        *_attempt_migration_sql("synthetic_ptg"),
        *_migration_sql(hardening_migration, "synthetic_ptg"),
    )

    assert statements
    assert all(
        _top_level_sql_statement_count(statement) == 1
        for statement in statements
    )


@pytest.mark.parametrize(
    ("snapshot_overrides", "internal_run_overrides", "reason_code"),
    (
        ({"manifest": []}, {}, "snapshot_manifest_not_object"),
        (
            {"manifest": None, "manifest_is_sql_null": False},
            {},
            "snapshot_manifest_not_object",
        ),
        ({}, {"report": 7}, "internal_run_report_not_object"),
        ({}, {"report": "{}"}, "internal_run_report_not_object"),
        (
            {},
            {"report": None, "report_is_sql_null": False},
            "internal_run_report_not_object",
        ),
    ),
)
def test_plan_rejects_non_object_json_envelopes(
    snapshot_overrides,
    internal_run_overrides,
    reason_code,
):
    plan_by_field = _plan(
        _ready_context(
            snapshot_overrides=snapshot_overrides,
            internal_run_overrides=internal_run_overrides,
        )
    )

    assert plan_by_field["status"] == "ineligible"
    assert reason_code in plan_by_field["reason_codes"]


def test_plan_digest_preserves_json_value_types_and_sql_null_state():
    report_shapes = (
        ({}, False, "ready"),
        ("{}", False, "ineligible"),
        ([], False, "ineligible"),
        (7, False, "ineligible"),
        (None, False, "ineligible"),
        (None, True, "ready"),
    )
    shaped_plans = [
        _plan(
            _ready_context(
                internal_run_overrides={
                    "report": report_value,
                    "report_is_sql_null": is_sql_null,
                }
            )
        )
        for report_value, is_sql_null, _status in report_shapes
    ]
    assert [plan["status"] for plan in shaped_plans] == [
        status for _value, _is_sql_null, status in report_shapes
    ]
    assert len({plan["plan_digest"] for plan in shaped_plans}) == len(
        report_shapes
    )
    assert database_json_digest({}) != database_json_digest("{}")
