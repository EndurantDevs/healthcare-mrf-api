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
        writer = csv.DictWriter(
            handle,
            fieldnames=CSV_FIELDNAMES,
            delimiter=";",
        )
        writer.writeheader()
        writer.writerows(source_rows)


@pytest.fixture
def database_boundary(monkeypatch):
    captured_rows = []

    async def publish(database, rows, expected_count, *, deadline, test_mode):
        assert database is geo_import.db and deadline > 0
        captured_rows.extend(rows)
        geo_import._validate_stable_source_row_count(len(captured_rows), expected_count)

    boundary = SimpleNamespace(authority=Mock(), producer=AsyncMock(side_effect=publish), rows=captured_rows)
    monkeypatch.setattr(geo_import, "source_transaction_available", boundary.authority)
    monkeypatch.setattr(geo_import, "publish_geo_rows", boundary.producer)
    return boundary


def _assert_database_not_reached(database_boundary):
    database_boundary.authority.assert_not_called()
    database_boundary.producer.assert_not_awaited()


def test_qualified_geo_table_uses_and_quotes_model_identifiers():
    target_table = SimpleNamespace(
        schema="geo_test",
        name="geo_zip_lookup",
    )

    assert geo_import._qualified_geo_zip_table(target_table) == '"geo_test"."geo_zip_lookup"'


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
async def test_load_test_mode_still_uses_authenticated_snapshot(tmp_path, database_boundary):
    csv_path = tmp_path / "geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    await geo_import.load_geo_lookup(csv_path, test_mode=True)
    database_boundary.authority.assert_called_once_with()
    database_boundary.producer.assert_awaited_once()
    assert database_boundary.rows == [geo_import._geo_zip_values_from_csv_fields(_csv_fields_by_name())]


@pytest.mark.asyncio
async def test_load_exposes_snapshot_failure_without_fallback(tmp_path, database_boundary):
    csv_path = tmp_path / "large-geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name(**{"Zip Code": str(number)}) for number in range(1, 2002)])
    database_boundary.producer.side_effect = RuntimeError("second batch failed")
    with pytest.raises(RuntimeError, match="second batch failed"):
        await geo_import.load_geo_lookup(csv_path)
    database_boundary.producer.assert_awaited_once()
    assert database_boundary.rows == []


@pytest.mark.asyncio
async def test_load_preserves_changed_source_refusal(tmp_path, monkeypatch, database_boundary):
    csv_path = tmp_path / "geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    monkeypatch.setattr(geo_import, "_preflight_geo_source", lambda _path: 2)
    with pytest.raises(geo_import.GeoSourceValidationError, match="changed after preflight"):
        await geo_import.load_geo_lookup(csv_path)
    database_boundary.producer.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_exposes_generation_refusal_without_retry(tmp_path, database_boundary):
    csv_path = tmp_path / "geo.csv"
    _write_geo_csv(csv_path, [_csv_fields_by_name()])
    database_boundary.producer.side_effect = RuntimeError("generation unavailable")
    with pytest.raises(RuntimeError, match="generation unavailable"):
        await geo_import.load_geo_lookup(csv_path)
    database_boundary.producer.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_text", "error_pattern"),
    [
        ("", "required headers"),
        (
            ",".join(CSV_FIELDNAMES) + '\n60654,Chicago,IL,Illinois,23890,17031,Cook,America/Chicago,"41.89, -87.63"\n',
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
