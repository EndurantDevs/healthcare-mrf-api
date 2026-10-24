# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable aggregate quota checks for original, Parquet and prepared captures."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import BigInteger, Column, MetaData, String, Table, create_engine
from sqlalchemy.dialects import postgresql

from db.models.custom_import import CustomImportCaptureBundle
from process.custom_import.capture_retention import (
    require_capture_retention_read_access,
    validate_dataset_retained_bytes,
)
from process.custom_import.capture_store import (
    CaptureBundleConflict,
    CaptureStoreError,
    CaptureStoreTransactionRequired,
)


class _Session:
    new = dirty = deleted = ()
    active = True
    isolation = "read committed"

    def __init__(self, connection):
        self.connection = connection
        self.statements = []

    def in_transaction(self):
        return self.active

    async def execute(self, statement):
        self.statements.append(statement)
        return self.connection.execute(statement)


@pytest.fixture
def retained_session():
    engine = create_engine("sqlite://")
    schema = CustomImportCaptureBundle.__table__.schema
    metadata = MetaData(schema=schema)
    tables_by_kind = {
        "usage": Table(
            "custom_import_capture_usage",
            metadata,
            Column("dataset_id", BigInteger),
            Column("retained_bytes", BigInteger),
        ),
        "bundle": Table(
            "custom_import_capture_bundle",
            metadata,
            Column("capture_bundle_id", BigInteger),
            Column("dataset_id", BigInteger),
            Column("capture_state", String),
        ),
        "part": Table(
            "custom_import_capture_parquet_part",
            metadata,
            Column("capture_bundle_id", BigInteger),
            Column("byte_count", BigInteger),
            Column("canonical_capture_manifest", String),
        ),
        "raw": Table(
            "custom_import_capture_raw_object",
            metadata,
            Column("dataset_id", BigInteger),
            Column("byte_count", BigInteger),
        ),
        "prepared": Table(
            "custom_import_prepared_source",
            metadata,
            Column("dataset_id", BigInteger),
            Column("retained_byte_count", BigInteger),
        ),
    }
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql(
                f"ATTACH DATABASE ':memory:' AS {connection.dialect.identifier_preparer.quote(schema)}"
            )
            metadata.create_all(connection)
            session = _Session(connection)
            driver = connection.connection.driver_connection
            driver.create_function("current_setting", 1, lambda _name: session.isolation)
            driver.create_function(
                "convert_to", 2, lambda text, encoding: None if text is None else text.encode(encoding)
            )
            driver.create_function("octet_length", 1, lambda value: None if value is None else len(value))
            yield session, tables_by_kind
    finally:
        engine.dispose()


def _seed_mixed_history(session, tables_by_kind):
    session.connection.execute(
        tables_by_kind["bundle"].insert(),
        [
            {"capture_bundle_id": 1, "dataset_id": 3, "capture_state": "sealed"},
            {"capture_bundle_id": 2, "dataset_id": 3, "capture_state": "pending"},
            {"capture_bundle_id": 3, "dataset_id": 3, "capture_state": "abandoned"},
            {"capture_bundle_id": 4, "dataset_id": 4, "capture_state": "pending"},
        ],
    )
    session.connection.execute(
        tables_by_kind["part"].insert(),
        [
            {"capture_bundle_id": 1, "byte_count": 10, "canonical_capture_manifest": "é"},
            {"capture_bundle_id": 2, "byte_count": 20, "canonical_capture_manifest": None},
            {"capture_bundle_id": 3, "byte_count": 30, "canonical_capture_manifest": "abc"},
            {"capture_bundle_id": 4, "byte_count": 1000, "canonical_capture_manifest": None},
        ],
    )
    session.connection.execute(
        tables_by_kind["raw"].insert(),
        [
            {"dataset_id": 3, "byte_count": 7},
            {"dataset_id": 3, "byte_count": 11},
            {"dataset_id": 4, "byte_count": 1000},
        ],
    )
    session.connection.execute(
        tables_by_kind["prepared"].insert(),
        [
            {"dataset_id": 3, "retained_byte_count": 13},
            {"dataset_id": 3, "retained_byte_count": 17},
            {"dataset_id": 4, "retained_byte_count": 1000},
        ],
    )


async def test_read_access_checks_only_accounting_columns_without_rows(retained_session):
    session, tables_by_kind = retained_session
    _seed_mixed_history(session, tables_by_kind)
    await require_capture_retention_read_access(session)
    assert len(session.statements) == 3
    for statement, columns in zip(
        session.statements,
        (("dataset_id", "retained_bytes"), ("dataset_id", "byte_count"), ("dataset_id", "retained_byte_count")),
        strict=True,
    ):
        assert tuple(statement.selected_columns.keys()) == columns
        sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        assert "LIMIT 0" in sql
        assert session.connection.execute(statement).all() == []
        assert all(excluded not in sql for excluded in ("payload", "sum(", "UPDATE", "FOR UPDATE"))


@pytest.mark.parametrize("counter,expected", [(None, 118), (50, 103), (0, 53)])
async def test_combined_total_uses_counter_or_complete_fallback(retained_session, counter, expected):
    session, tables_by_kind = retained_session
    _seed_mixed_history(session, tables_by_kind)
    if counter is not None:
        session.connection.execute(tables_by_kind["usage"].insert(), {"dataset_id": 3, "retained_bytes": counter})
    await validate_dataset_retained_bytes(session, dataset_id=3, new_byte_count=5, maximum_retained_bytes=expected)
    with pytest.raises(CaptureBundleConflict, match="quota"):
        await validate_dataset_retained_bytes(
            session, dataset_id=3, new_byte_count=5, maximum_retained_bytes=expected - 1
        )
    assert len(session.statements) == 2


async def test_total_is_fresh_and_empty_missing_counter_is_not_an_error(retained_session):
    session, tables_by_kind = retained_session
    await validate_dataset_retained_bytes(session, dataset_id=3, new_byte_count=0, maximum_retained_bytes=0)
    session.connection.execute(tables_by_kind["raw"].insert(), {"dataset_id": 3, "byte_count": 1})
    with pytest.raises(CaptureBundleConflict, match="quota"):
        await validate_dataset_retained_bytes(session, dataset_id=3, new_byte_count=0, maximum_retained_bytes=0)
    assert len(session.statements) == 2


async def test_one_postgresql_statement_scopes_all_four_totals_to_dataset(retained_session):
    session, _ = retained_session
    await validate_dataset_retained_bytes(session, dataset_id=3, new_byte_count=5, maximum_retained_bytes=5)
    assert len(session.statements) == 1
    sql = str(session.statements[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert sql.count(".dataset_id = 3") == 4
    assert "coalesce((SELECT" in sql
    assert "custom_import_capture_usage.retained_bytes" in sql
    assert "custom_import_prepared_source.retained_byte_count" in sql
    assert "JOIN" in sql and "capture_bundle_id =" in sql
    assert "octet_length(convert_to(" in sql and "'UTF8'" in sql
    assert "sum(CAST(" in sql and " AS NUMERIC)" in sql
    for excluded in (
        "capture_state",
        "definition_revision_id",
        "schema_revision_id",
        "custom_import_prepared_source_part",
        "custom_import_execution",
        ".payload",
        "UPDATE",
        "FOR UPDATE",
    ):
        assert excluded not in sql


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable", "read uncommitted"])
async def test_retention_rejects_non_read_committed_transactions(retained_session, isolation):
    session, _ = retained_session
    session.isolation = isolation
    with pytest.raises(CaptureStoreError, match="READ COMMITTED"):
        await validate_dataset_retained_bytes(session, dataset_id=3, new_byte_count=0, maximum_retained_bytes=10)
    assert len(session.statements) == 1


@pytest.mark.parametrize("field", ["dataset_id", "new_byte_count", "maximum_retained_bytes"])
@pytest.mark.parametrize("value", [True, False, -1, 2**63, 1.0, "1", None])
async def test_invalid_bounds_fail_before_database_work(retained_session, field, value):
    session, _ = retained_session
    arguments_by_field = dict(dataset_id=3, new_byte_count=0, maximum_retained_bytes=10)
    arguments_by_field[field] = value
    with pytest.raises(CaptureStoreError):
        await validate_dataset_retained_bytes(session, **arguments_by_field)
    assert session.statements == []


async def test_bigint_limit_is_valid_but_combined_overflow_is_rejected(retained_session):
    session, _ = retained_session
    maximum = 2**63 - 1
    session.execute = AsyncMock(return_value=SimpleNamespace(one=lambda: ("read committed", Decimal(maximum))))
    await validate_dataset_retained_bytes(
        session, dataset_id=maximum, new_byte_count=maximum, maximum_retained_bytes=maximum
    )
    session.execute.return_value = SimpleNamespace(one=lambda: ("read committed", Decimal(maximum) + 1))
    with pytest.raises(CaptureBundleConflict, match="quota"):
        await validate_dataset_retained_bytes(session, dataset_id=3, new_byte_count=1, maximum_retained_bytes=maximum)


@pytest.mark.parametrize("state", ["inactive", "new", "dirty", "deleted"])
@pytest.mark.parametrize("check", [require_capture_retention_read_access, validate_dataset_retained_bytes])
async def test_retention_requires_an_active_clean_transaction(retained_session, state, check):
    session, _ = retained_session
    if state == "inactive":
        session.active = False
        expected = CaptureStoreTransactionRequired
    else:
        setattr(session, state, (object(),))
        expected = CaptureStoreError
    arguments = (
        dict(dataset_id=3, new_byte_count=0, maximum_retained_bytes=10)
        if check is validate_dataset_retained_bytes
        else {}
    )
    with pytest.raises(expected):
        await check(session, **arguments)
    assert session.statements == []
