import asyncio
import csv
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

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


@pytest.fixture
def database_mocks(monkeypatch):
    captured_rows = []

    async def publish(database, rows, expected_count, *, deadline, test_mode):
        assert database is geo_import.db and deadline > 0
        captured_rows.extend(rows)
        geo_import._validate_stable_source_row_count(len(captured_rows), expected_count)

    boundary = SimpleNamespace(authority=Mock(), producer=AsyncMock(side_effect=publish), rows=captured_rows)
    monkeypatch.setattr(geo_import, "source_transaction_available", boundary.authority)
    monkeypatch.setattr(geo_import, "publish_geo_rows", boundary.producer)
    return boundary


def _assert_database_not_reached(database_mocks):
    database_mocks.authority.assert_not_called()
    database_mocks.producer.assert_not_awaited()


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
async def test_empty_stream_does_not_copy(monkeypatch):
    from process import geo_source_snapshot as snapshot

    copy = AsyncMock()
    monkeypatch.setattr(snapshot, "_copy", copy)
    candidate = SimpleNamespace(deadline=float("inf"), raw="private")
    assert await snapshot._load(object(), candidate, []) == 0
    copy.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_copy_preserves_every_model_column(monkeypatch):
    from process import geo_source_snapshot as snapshot

    copy = AsyncMock()
    monkeypatch.setattr(snapshot, "_copy", copy)
    row = geo_import._geo_zip_values_from_csv_fields(_csv_fields_by_name())
    candidate = SimpleNamespace(deadline=float("inf"), raw="private")
    assert await snapshot._load(object(), candidate, [row]) == 1
    assert copy.await_args.args[3] == [tuple(row[name] for name in snapshot.COLUMNS) + (0, 0)]
    assert tuple(row) == snapshot.COLUMNS


@pytest.mark.asyncio
async def test_native_copy_failure_preserves_source_values(monkeypatch):
    from process import geo_source_snapshot as snapshot

    row = geo_import._geo_zip_values_from_csv_fields(_csv_fields_by_name())
    saved_by_column = dict(row)
    monkeypatch.setattr(snapshot, "_copy", AsyncMock(side_effect=RuntimeError("database write failed")))
    candidate = SimpleNamespace(deadline=float("inf"), raw="private")
    with pytest.raises(RuntimeError, match="database write failed"):
        await snapshot._load(object(), candidate, [row])
    assert row == saved_by_column


@pytest.mark.asyncio
async def test_load_parses_bom_semicolon_and_skips_missing_keys(
    tmp_path,
    monkeypatch,
    database_mocks,
):
    """Normalize one valid row while skipping records without required keys."""

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
    captured_geo_rows = database_mocks.rows
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "geo_test")

    await geo_import.load_geo_lookup(csv_path)

    _assert_load_database_calls(database_mocks)
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


def _assert_load_database_calls(database_mocks):
    database_mocks.authority.assert_called_once_with()
    database_mocks.producer.assert_awaited_once()
    assert database_mocks.producer.await_args.args[0] is geo_import.db


@pytest.mark.asyncio
async def test_load_preserves_complete_multi_batch_input(tmp_path, database_mocks):
    csv_path = tmp_path / "large-geo.csv"
    rows = [_csv_fields_by_name(**{"Zip Code": str(number)}) for number in range(1, 2002)]
    _write_geo_csv(csv_path, rows)
    await geo_import.load_geo_lookup(csv_path)
    assert len(database_mocks.rows) == 2001
    assert database_mocks.producer.await_args.args[2] == 2001
    _assert_load_database_calls(database_mocks)


@pytest.mark.asyncio
async def test_load_uses_default_source_and_model_schema(tmp_path, monkeypatch, database_mocks):
    csv_path = tmp_path / "default-geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    monkeypatch.setattr(geo_import, "DEFAULT_SOURCE", csv_path)
    await geo_import.load_geo_lookup()
    assert database_mocks.rows == [geo_import._geo_zip_values_from_csv_fields(_csv_fields_by_name())]
    _assert_load_database_calls(database_mocks)


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
