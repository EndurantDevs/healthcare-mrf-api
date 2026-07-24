"""Transactional contracts for the canonical address-archive cutover."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


address_canon = importlib.import_module("process.ext.address_canon")


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _RecordingSession:
    def __init__(self, scalar_values):
        self.scalar_values = list(scalar_values)
        self.statements = []

    async def execute(self, statement, _params=None):
        sql = str(statement)
        self.statements.append(sql)
        if sql.lstrip().lower().startswith("select count"):
            return _ScalarResult(self.scalar_values.pop(0))
        return _ScalarResult(None)


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_exc):
        return False


def _configure_swap(monkeypatch, *, scalar_values=(12, 12, 0, 0, 12)):
    session = _RecordingSession(scalar_values)
    monkeypatch.setattr(address_canon.db, "transaction", lambda: _Transaction(session))
    monkeypatch.setattr(address_canon, "_has_session_table", AsyncMock(return_value=True))

    async def has_column(_session, _schema, table_name, column_name):
        if table_name == "address_archive" and column_name == "address_key":
            return False
        if table_name == "address_archive" and column_name == "checksum":
            return True
        return table_name == "address_archive_v2" and column_name == "address_key"

    monkeypatch.setattr(address_canon, "_has_session_table_column", has_column)
    fingerprint_by_metric = {
        "rows": 12,
        "checksum_min": 10,
        "checksum_max": 90,
        "checksum_sum": 500,
    }
    monkeypatch.setattr(
        address_canon,
        "_legacy_archive_fingerprint",
        AsyncMock(side_effect=[fingerprint_by_metric, dict(fingerprint_by_metric)]),
    )
    return session


@pytest.mark.asyncio
async def test_archive_swap_renames_only_after_all_bridge_and_fingerprint_checks(monkeypatch):
    session = _configure_swap(monkeypatch)

    async def is_table_present(_session, _schema, table_name):
        return table_name in {
            "address_archive",
            "address_archive_v2",
            "address_checksum_map",
            "address_checksum_collision",
            "address_archive_legacy",
        }

    monkeypatch.setattr(address_canon, "_has_session_table", is_table_present)

    swap_stats = await address_canon.swap_archive_v2_to_current(
        schema="mrf",
        allow_replace_backup=True,
        timeout="90s",
    )

    assert swap_stats.swapped is True
    assert swap_stats.legacy_rows_before == swap_stats.legacy_rows_after == 12
    assert swap_stats.archive_rows_before == swap_stats.current_rows_after == 12
    assert swap_stats.checksum_map_rows == 12
    assert swap_stats.checksum_collision_rows == 0
    ddl_statements = [statement for statement in session.statements if statement.startswith(("DROP", "ALTER"))]
    assert ddl_statements == [
        'DROP TABLE "mrf"."address_archive_legacy";',
        'ALTER TABLE "mrf"."address_archive" RENAME TO "address_archive_legacy";',
        'ALTER TABLE "mrf"."address_archive_v2" RENAME TO "address_archive";',
    ]
    assert any("statement_timeout = '90s'" in statement for statement in session.statements)


@pytest.mark.asyncio
async def test_archive_swap_dry_run_verifies_the_same_invariants_without_ddl(monkeypatch):
    session = _configure_swap(monkeypatch)

    async def is_table_present(_session, _schema, table_name):
        return table_name != "address_archive_legacy"

    monkeypatch.setattr(address_canon, "_has_session_table", is_table_present)

    swap_stats = await address_canon.swap_archive_v2_to_current(schema="mrf", dry_run=True)

    assert swap_stats.swapped is False
    assert swap_stats.dry_run is True
    assert swap_stats.current_rows_after == 12
    assert not any(statement.startswith(("DROP", "ALTER")) for statement in session.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("missing_table", "required table is missing"),
        ("current_canonical", "already appears to be canonical"),
        ("current_not_legacy", "is not the legacy checksum archive"),
        ("archive_not_canonical", "is not the canonical archive"),
        ("backup_exists", "backup table already exists"),
        ("legacy_empty", "address_archive is empty"),
        ("archive_empty", "address_archive_v2 is empty"),
        ("map_empty", "address_checksum_map is empty"),
        ("missing_targets", "checksum bridge has 2 missing canonical target"),
        ("fingerprint_changed", "legacy archive fingerprint changed"),
        ("row_count_changed", "canonical archive row count changed"),
    ],
)
async def test_archive_swap_aborts_before_commit_when_any_cutover_invariant_fails(
    monkeypatch,
    failure,
    message,
):
    scalar_values = {
        "archive_empty": (0, 12, 0, 0),
        "map_empty": (12, 0, 0, 0),
        "missing_targets": (12, 12, 0, 2),
        "row_count_changed": (12, 12, 0, 0, 11),
    }.get(failure, (12, 12, 0, 0, 12))
    session = _configure_swap(monkeypatch, scalar_values=scalar_values)

    async def is_table_present(_session, _schema, table_name):
        if failure == "missing_table" and table_name == "address_checksum_map":
            return False
        if table_name == "address_archive_legacy":
            return failure == "backup_exists"
        return True

    async def has_column(_session, _schema, table_name, column_name):
        if table_name == "address_archive" and column_name == "address_key":
            return failure == "current_canonical"
        if table_name == "address_archive" and column_name == "checksum":
            return failure != "current_not_legacy"
        if table_name == "address_archive_v2" and column_name == "address_key":
            return failure != "archive_not_canonical"
        return False

    fingerprint_by_metric = {
        "rows": 0 if failure == "legacy_empty" else 12,
        "checksum_min": 10,
        "checksum_max": 90,
        "checksum_sum": 500,
    }
    after_fingerprint_by_metric = dict(fingerprint_by_metric)
    if failure == "fingerprint_changed":
        after_fingerprint_by_metric["checksum_sum"] = 501

    monkeypatch.setattr(address_canon, "_has_session_table", is_table_present)
    monkeypatch.setattr(address_canon, "_has_session_table_column", has_column)
    monkeypatch.setattr(
        address_canon,
        "_legacy_archive_fingerprint",
        AsyncMock(side_effect=[fingerprint_by_metric, after_fingerprint_by_metric]),
    )

    with pytest.raises(RuntimeError, match=message):
        await address_canon.swap_archive_v2_to_current(schema="mrf", dry_run=True)

    assert not any(statement.startswith(("DROP", "ALTER")) for statement in session.statements)


@pytest.mark.asyncio
async def test_legacy_fingerprint_has_zero_defaults_for_an_empty_result(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(first=lambda: None)))

    swap_stats = await address_canon._legacy_archive_fingerprint(session, '"mrf"."address_archive"')

    assert swap_stats == {
        "rows": 0,
        "checksum_min": None,
        "checksum_max": None,
        "checksum_sum": None,
    }
