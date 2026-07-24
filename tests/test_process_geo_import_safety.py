import csv
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import geo_import


CSV_FIELDNAMES = (
    "Zip Code",
    "Official USPS city name",
    "Official USPS State Code",
    "Official State Name",
    "Population",
    "Primary Official County Code",
    "Primary Official County Name",
    "Timezone",
    "Geo Point",
)


def _csv_fields_by_name(**overrides: str) -> dict[str, str]:
    fields_by_name = {
        "Zip Code": "60654",
        "Official USPS city name": "Chicago",
        "Official USPS State Code": "IL",
        "Official State Name": "Illinois",
        "Population": "23890.0",
        "Primary Official County Code": "17031",
        "Primary Official County Name": "Cook",
        "Timezone": "America/Chicago",
        "Geo Point": "41.8925, -87.6341",
    }
    fields_by_name.update(overrides)
    return fields_by_name


def _write_geo_csv(
    csv_path: Path,
    source_rows: list[dict[str, str]],
) -> None:
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=CSV_FIELDNAMES,
            delimiter=";",
        )
        writer.writeheader()
        writer.writerows(source_rows)


class _TransactionSpy:
    def __init__(self):
        self.active = False
        self.enter_count = 0
        self.exit_count = 0
        self.exit_exception_type = None

    async def __aenter__(self):
        assert not self.active
        self.active = True
        self.enter_count += 1
        return self

    async def __aexit__(self, exc_type, _exc, _traceback):
        assert self.active
        self.active = False
        self.exit_count += 1
        self.exit_exception_type = exc_type
        return False


@pytest.fixture
def database_boundary(monkeypatch):
    transaction_spy = _TransactionSpy()
    database_mocks = SimpleNamespace(
        ensure_database=AsyncMock(),
        create_table=AsyncMock(),
        status=AsyncMock(),
        transaction=transaction_spy,
    )
    monkeypatch.setattr(
        geo_import,
        "ensure_database",
        database_mocks.ensure_database,
    )
    monkeypatch.setattr(
        geo_import.db,
        "create_table",
        database_mocks.create_table,
    )
    monkeypatch.setattr(geo_import.db, "status", database_mocks.status)
    monkeypatch.setattr(
        geo_import.db,
        "transaction",
        lambda: transaction_spy,
    )
    return database_mocks


def _assert_database_not_reached(database_boundary) -> None:
    database_boundary.ensure_database.assert_not_awaited()
    database_boundary.create_table.assert_not_awaited()
    database_boundary.status.assert_not_awaited()
    assert database_boundary.transaction.enter_count == 0


class _InsertStatementSpy:
    def __init__(self, table):
        self.table = table
        self.excluded = SimpleNamespace(
            **{
                column.name: f"excluded_{column.name}"
                for column in table.c
            }
        )
        self.submitted_geo_rows = None

    def values(self, pending_geo_rows):
        self.submitted_geo_rows = list(pending_geo_rows)
        return self

    def on_conflict_do_update(self, **_kwargs):
        return self

    async def status(self):
        return None


def test_qualified_geo_table_uses_and_quotes_model_identifiers():
    target_table = SimpleNamespace(
        schema="geo_test",
        name="geo_zip_lookup",
    )

    assert (
        geo_import._qualified_geo_zip_table(target_table)
        == '"geo_test"."geo_zip_lookup"'
    )


@pytest.mark.parametrize(
    ("schema_name", "table_name"),
    [
        (None, "geo_zip_lookup"),
        ("geo-test", "geo_zip_lookup"),
        ("mrf", "geo_zip_lookup; DROP TABLE plans"),
    ],
)
def test_qualified_geo_table_rejects_missing_or_unsafe_identifiers(
    schema_name,
    table_name,
):
    target_table = SimpleNamespace(
        schema=schema_name,
        name=table_name,
    )

    with pytest.raises(ValueError, match="Invalid GeoZipLookup"):
        geo_import._qualified_geo_zip_table(target_table)


@pytest.mark.asyncio
async def test_load_uses_one_model_table_for_quoted_truncate_and_test_upsert(
    tmp_path,
    monkeypatch,
    database_boundary,
):
    csv_path = tmp_path / "geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    target_table = geo_import.GeoZipLookup.__table__
    insert_spy = _InsertStatementSpy(target_table)
    monkeypatch.setattr(
        geo_import.db,
        "insert",
        lambda submitted_table: (
            insert_spy
            if submitted_table is target_table
            else pytest.fail("upsert must use the validated model table")
        ),
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "divergent_runtime_schema")

    await geo_import.load_geo_lookup(csv_path, test_mode=True)

    database_boundary.ensure_database.assert_awaited_once_with(True)
    database_boundary.create_table.assert_awaited_once_with(
        target_table,
        checkfirst=True,
    )
    database_boundary.status.assert_awaited_once_with(
        f"TRUNCATE TABLE {geo_import._qualified_geo_zip_table(target_table)};"
    )
    assert insert_spy.table is target_table
    assert insert_spy.submitted_geo_rows == [
        geo_import._geo_zip_values_from_csv_fields(_csv_fields_by_name())
    ]
    assert database_boundary.transaction.enter_count == 1
    assert database_boundary.transaction.exit_exception_type is None


@pytest.mark.asyncio
async def test_load_exposes_write_failure_to_one_atomic_transaction(
    tmp_path,
    monkeypatch,
    database_boundary,
):
    csv_path = tmp_path / "large-geo.csv"
    _write_geo_csv(
        csv_path,
        [
            _csv_fields_by_name(**{"Zip Code": str(zip_number)})
            for zip_number in range(1, 2002)
        ],
    )
    transaction_events = []
    flush_sizes = []

    async def capture_truncate(statement):
        assert database_boundary.transaction.active
        transaction_events.append(("truncate", statement))

    async def fail_second_flush(pending_geo_rows, *, target_table):
        assert database_boundary.transaction.active
        assert target_table is geo_import.GeoZipLookup.__table__
        flush_sizes.append(len(pending_geo_rows))
        if len(flush_sizes) == 2:
            raise RuntimeError("second batch failed")
        pending_geo_rows.clear()

    database_boundary.status.side_effect = capture_truncate
    monkeypatch.setattr(geo_import, "_flush_rows", fail_second_flush)

    with pytest.raises(RuntimeError, match="second batch failed"):
        await geo_import.load_geo_lookup(csv_path)

    transaction_events.extend(("flush", size) for size in flush_sizes)
    assert transaction_events == [
        (
            "truncate",
            f"TRUNCATE TABLE "
            f"{geo_import._qualified_geo_zip_table(geo_import.GeoZipLookup.__table__)};",
        ),
        ("flush", geo_import.IMPORT_BATCH_SIZE),
        ("flush", 1),
    ]
    assert database_boundary.transaction.enter_count == 1
    assert database_boundary.transaction.exit_count == 1
    assert database_boundary.transaction.exit_exception_type is RuntimeError
    assert not database_boundary.transaction.active


@pytest.mark.asyncio
async def test_load_rolls_back_when_source_row_count_changes_after_preflight(
    tmp_path,
    monkeypatch,
    database_boundary,
):
    csv_path = tmp_path / "geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    monkeypatch.setattr(
        geo_import,
        "_preflight_geo_source",
        lambda _csv_path: 2,
    )

    async def capture_flush(pending_geo_rows, *, target_table):
        assert database_boundary.transaction.active
        assert target_table is geo_import.GeoZipLookup.__table__
        pending_geo_rows.clear()

    monkeypatch.setattr(geo_import, "_flush_rows", capture_flush)

    with pytest.raises(
        geo_import.GeoSourceValidationError,
        match="changed after preflight",
    ):
        await geo_import.load_geo_lookup(csv_path)

    assert database_boundary.transaction.enter_count == 1
    assert database_boundary.transaction.exit_count == 1
    assert (
        database_boundary.transaction.exit_exception_type
        is geo_import.GeoSourceValidationError
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_text", "error_pattern"),
    [
        ("", "required headers"),
        (
            ",".join(CSV_FIELDNAMES)
            + "\n60654,Chicago,IL,Illinois,23890,17031,Cook,"
            "America/Chicago,\"41.89, -87.63\"\n",
            "semicolon-delimited",
        ),
        (
            "Zip Code;Official USPS city name\n60654;Chicago\n",
            "missing: Official USPS State Code",
        ),
        (
            ";".join(CSV_FIELDNAMES) + "\n",
            "contains no valid ZIP and city rows",
        ),
    ],
    ids=[
        "empty",
        "wrong-delimiter",
        "missing-header",
        "header-only",
    ],
)
async def test_invalid_source_fails_before_database_mutation(
    source_text,
    error_pattern,
    tmp_path,
    database_boundary,
):
    csv_path = tmp_path / "invalid-geo.csv"
    csv_path.write_text(source_text, encoding="utf-8-sig")

    with pytest.raises(
        geo_import.GeoSourceValidationError,
        match=error_pattern,
    ):
        await geo_import.load_geo_lookup(csv_path)

    _assert_database_not_reached(database_boundary)


@pytest.mark.asyncio
async def test_preflight_reads_past_first_valid_row_before_database_mutation(
    tmp_path,
    database_boundary,
):
    csv_path = tmp_path / "late-malformed-geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    with csv_path.open("a", encoding="utf-8", newline="") as handle:
        handle.write('"unterminated')

    with pytest.raises(
        geo_import.GeoSourceValidationError,
        match="not a readable UTF-8 semicolon CSV",
    ):
        await geo_import.load_geo_lookup(csv_path)

    _assert_database_not_reached(database_boundary)


@pytest.mark.asyncio
async def test_unreadable_source_fails_before_database_mutation(
    tmp_path,
    monkeypatch,
    database_boundary,
):
    csv_path = tmp_path / "unreadable-geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    original_path_open = Path.open

    def deny_source_open(path, *args, **kwargs):
        if path == csv_path:
            raise PermissionError("permission denied")
        return original_path_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", deny_source_open)

    with pytest.raises(
        geo_import.GeoSourceValidationError,
        match="not a readable UTF-8 semicolon CSV",
    ):
        await geo_import.load_geo_lookup(csv_path)

    _assert_database_not_reached(database_boundary)


@pytest.mark.asyncio
async def test_non_file_source_fails_before_database_mutation(
    tmp_path,
    database_boundary,
):
    with pytest.raises(
        geo_import.GeoSourceValidationError,
        match="not a regular file",
    ):
        await geo_import.load_geo_lookup(tmp_path)

    _assert_database_not_reached(database_boundary)
