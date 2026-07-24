import asyncio
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
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, delimiter=";")
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
def database_mocks(monkeypatch):
    ensure_database_mock = AsyncMock()
    create_table_mock = AsyncMock()
    status_mock = AsyncMock()
    transaction_spy = _TransactionSpy()
    monkeypatch.setattr(geo_import, "ensure_database", ensure_database_mock)
    monkeypatch.setattr(geo_import.db, "create_table", create_table_mock)
    monkeypatch.setattr(geo_import.db, "status", status_mock)
    monkeypatch.setattr(
        geo_import.db,
        "transaction",
        lambda: transaction_spy,
    )
    return SimpleNamespace(
        ensure_database=ensure_database_mock,
        create_table=create_table_mock,
        status=status_mock,
        transaction=transaction_spy,
    )


def _assert_database_not_reached(database_mocks) -> None:
    database_mocks.ensure_database.assert_not_awaited()
    database_mocks.create_table.assert_not_awaited()
    database_mocks.status.assert_not_awaited()
    assert database_mocks.transaction.enter_count == 0


class _InsertStatementSpy:
    def __init__(self, table, *, should_fail: bool = False):
        self.table = table
        self.should_fail = should_fail
        self.excluded = SimpleNamespace(
            **{
                column.name: f"excluded_{column.name}"
                for column in table.c
            }
        )
        self.submitted_geo_rows = None
        self.index_elements = None
        self.updates_by_column = None

    def values(self, pending_geo_rows):
        self.submitted_geo_rows = list(pending_geo_rows)
        return self

    def on_conflict_do_update(self, *, index_elements, set_):
        self.index_elements = index_elements
        self.updates_by_column = set_
        return self

    async def status(self):
        if self.should_fail:
            raise RuntimeError("database write failed")


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("12.5", 12.5),
        (12, 12.0),
        (None, None),
        ("", None),
        ("not-a-number", None),
    ],
)
def test_parse_float_rejects_invalid_numeric_text(raw_value, expected):
    assert geo_import._parse_float(raw_value) == expected


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("23890.0", 23890),
        ("12.9", 12),
        (None, None),
        ("", None),
        ("not-a-number", None),
    ],
)
def test_parse_population_preserves_integer_coercion(raw_value, expected):
    assert geo_import._parse_population(raw_value) == expected


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (None, (None, None)),
        ("", (None, None)),
        ("41.8", (None, None)),
        ("41.8, -87.6, extra", (None, None)),
        ("invalid, -87.6", (None, -87.6)),
        ("41.8, -87.6", (41.8, -87.6)),
    ],
)
def test_parse_geo_point_rejects_invalid_coordinate_shapes(raw_value, expected):
    assert geo_import._parse_geo_point(raw_value) == expected


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (None, None),
        ("", None),
        ("county", None),
        ("31", "00031"),
        ("US-17031", "17031"),
        ("prefix-061037", "61037"),
    ],
)
def test_normalize_county_code_keeps_last_five_digits(raw_value, expected):
    assert geo_import._normalize_county_code(raw_value) == expected


@pytest.mark.asyncio
async def test_flush_skips_database_for_empty_buffer(monkeypatch):
    monkeypatch.setattr(
        geo_import.db,
        "insert",
        lambda _table: pytest.fail("empty buffers must not reach the database"),
    )

    await geo_import._flush_rows([])


@pytest.mark.asyncio
async def test_flush_upserts_every_mutable_column_and_clears_buffer(monkeypatch):
    table = geo_import.GeoZipLookup.__table__
    insert_spy = _InsertStatementSpy(table)
    pending_geo_rows = [{"zip_code": "60654", "city": "Chicago"}]
    monkeypatch.setattr(geo_import.db, "insert", lambda _table: insert_spy)

    await geo_import._flush_rows(pending_geo_rows)

    expected_updates_by_column = {
        column.name: f"excluded_{column.name}"
        for column in table.c
        if not column.primary_key
    }
    assert insert_spy.submitted_geo_rows == [
        {"zip_code": "60654", "city": "Chicago"}
    ]
    assert insert_spy.index_elements == geo_import.GeoZipLookup.__my_index_elements__
    assert insert_spy.updates_by_column == expected_updates_by_column
    assert "zip_code" not in insert_spy.updates_by_column
    assert pending_geo_rows == []


@pytest.mark.asyncio
async def test_flush_preserves_buffer_when_database_write_fails(monkeypatch):
    table = geo_import.GeoZipLookup.__table__
    insert_spy = _InsertStatementSpy(table, should_fail=True)
    pending_geo_rows = [{"zip_code": "60654", "city": "Chicago"}]
    expected_pending_geo_rows = list(pending_geo_rows)
    monkeypatch.setattr(geo_import.db, "insert", lambda _table: insert_spy)

    with pytest.raises(RuntimeError, match="database write failed"):
        await geo_import._flush_rows(pending_geo_rows)

    assert pending_geo_rows == expected_pending_geo_rows


@pytest.mark.asyncio
async def test_load_parses_bom_semicolon_and_skips_missing_keys(
    tmp_path,
    monkeypatch,
    database_mocks,
):
    csv_path = tmp_path / "geo.csv"
    _write_geo_csv(
        csv_path,
        [
            _csv_fields_by_name(
                **{
                    "Zip Code": "42",
                    "Official USPS city name": " Mixed Case ",
                    "Official USPS State Code": " ny ",
                    "Population": "unknown",
                    "Primary Official County Code": "county-31",
                    "Geo Point": "invalid, -73.9",
                }
            ),
            _csv_fields_by_name(**{"Zip Code": ""}),
            _csv_fields_by_name(**{"Official USPS city name": ""}),
        ],
    )
    captured_geo_rows = []

    async def capture_flush(pending_geo_rows, *, target_table):
        captured_geo_rows.extend(pending_geo_rows)
        pending_geo_rows.clear()

    monkeypatch.setattr(geo_import, "_flush_rows", capture_flush)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "geo_test")

    await geo_import.load_geo_lookup(csv_path)

    database_mocks.ensure_database.assert_awaited_once_with(False)
    database_mocks.create_table.assert_awaited_once_with(
        geo_import.GeoZipLookup.__table__,
        checkfirst=True,
    )
    database_mocks.status.assert_awaited_once_with(
        f"TRUNCATE TABLE "
        f"{geo_import._qualified_geo_zip_table(geo_import.GeoZipLookup.__table__)};"
    )
    assert captured_geo_rows == [
        {
            "zip_code": "00042",
            "city": "Mixed Case",
            "city_lower": "mixed case",
            "state": "NY",
            "state_name": "Illinois",
            "county_name": "Cook",
            "county_code": "00031",
            "latitude": None,
            "longitude": -73.9,
            "timezone": "America/Chicago",
            "population": None,
        }
    ]


@pytest.mark.asyncio
async def test_load_flushes_two_thousand_rows_then_remainder(
    tmp_path,
    monkeypatch,
    database_mocks,
):
    csv_path = tmp_path / "large-geo.csv"
    source_rows = [
        _csv_fields_by_name(**{"Zip Code": str(zip_number)})
        for zip_number in range(1, 2002)
    ]
    _write_geo_csv(csv_path, source_rows)
    batch_sizes = []

    async def capture_batch_size(pending_geo_rows, *, target_table):
        assert target_table is geo_import.GeoZipLookup.__table__
        assert database_mocks.transaction.active
        batch_sizes.append(len(pending_geo_rows))
        pending_geo_rows.clear()

    monkeypatch.setattr(geo_import, "_flush_rows", capture_batch_size)

    await geo_import.load_geo_lookup(csv_path)

    assert batch_sizes == [geo_import.IMPORT_BATCH_SIZE, 1]
    database_mocks.status.assert_awaited_once_with(
        f"TRUNCATE TABLE "
        f"{geo_import._qualified_geo_zip_table(geo_import.GeoZipLookup.__table__)};"
    )


@pytest.mark.asyncio
async def test_load_uses_default_source_and_model_schema(
    tmp_path,
    monkeypatch,
    database_mocks,
):
    csv_path = tmp_path / "default-geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    flushed_geo_rows = []

    async def capture_flush(pending_geo_rows, *, target_table):
        assert target_table is geo_import.GeoZipLookup.__table__
        flushed_geo_rows.extend(pending_geo_rows)
        pending_geo_rows.clear()

    monkeypatch.setattr(geo_import, "DEFAULT_SOURCE", csv_path)
    monkeypatch.setattr(geo_import, "_flush_rows", capture_flush)

    await geo_import.load_geo_lookup()

    assert [geo_values["zip_code"] for geo_values in flushed_geo_rows] == ["60654"]
    database_mocks.status.assert_awaited_once_with(
        f"TRUNCATE TABLE "
        f"{geo_import._qualified_geo_zip_table(geo_import.GeoZipLookup.__table__)};"
    )


@pytest.mark.asyncio
async def test_missing_source_fails_before_database_mutation(
    tmp_path,
    database_mocks,
):
    missing_path = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError, match=str(missing_path)):
        await geo_import.load_geo_lookup(missing_path)

    _assert_database_not_reached(database_mocks)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("file_path", "test_mode"),
    [(None, False), ("custom.csv", 1)],
)
async def test_main_reports_the_delegated_source(
    file_path,
    test_mode,
    monkeypatch,
):
    load_mock = AsyncMock()
    monkeypatch.setattr(geo_import, "load_geo_lookup", load_mock)

    response = await geo_import.main(test_mode=test_mode, file_path=file_path)

    expected_source = Path(file_path) if file_path else None
    load_mock.assert_awaited_once_with(
        source_file=expected_source,
        test_mode=bool(test_mode),
    )
    assert response == {
        "test_mode": bool(test_mode),
        "source_file": str(expected_source or geo_import.DEFAULT_SOURCE),
    }


def test_geo_lookup_forwards_path_to_synchronous_runner(tmp_path, monkeypatch):
    csv_path = tmp_path / "geo.csv"
    awaitable = object()
    runner_calls = []
    monkeypatch.setattr(geo_import, "load_geo_lookup", lambda _path: awaitable)
    monkeypatch.setattr(asyncio, "run", lambda candidate: runner_calls.append(candidate))

    geo_import.geo_lookup.callback(csv_path)

    assert runner_calls == [awaitable]
