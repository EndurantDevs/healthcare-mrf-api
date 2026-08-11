# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path
from unittest.mock import Mock

from sqlalchemy import SmallInteger, String, Text

from db.models import AddressArchiveV2, EntityAddressUnified
from process.ext.address_format import (
    ADDRESS_FORMAT_MAX_LENGTH,
    ADDRESS_FORMAT_SOURCE,
    ADDRESS_FORMAT_VERSION,
    render_formatted_address_v1,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic/versions/20260811110000_address_formatted_display.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "address_formatted_display_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _normalized_sql(value: object) -> str:
    return " ".join(str(value).split())


def test_renderer_has_a_stable_versioned_contract() -> None:
    assert ADDRESS_FORMAT_VERSION == 1
    assert ADDRESS_FORMAT_SOURCE == "canonical_v1"
    assert ADDRESS_FORMAT_MAX_LENGTH == 1024
    assert tuple(inspect.signature(render_formatted_address_v1).parameters) == (
        "first_line",
        "second_line",
        "city_name",
        "state_name",
        "postal_code",
        "country_code",
    )


def test_renderer_formats_us_unit_and_zip4_without_country() -> None:
    rendered = render_formatted_address_v1(
        " 100\u00a0Main\tStreet ",
        " Suite   200 ",
        " Springfield ",
        " IL ",
        "627041234",
        " us ",
    )

    assert rendered == (
        "100 Main Street, Suite 200, Springfield, IL 62704-1234"
    )


def test_renderer_includes_non_us_country_and_preserves_unit() -> None:
    suite_two = render_formatted_address_v1(
        "100 King St W",
        "Floor 2",
        "Toronto",
        "ON",
        "M5V 3A8",
        "ca",
    )
    suite_three = render_formatted_address_v1(
        "100 King St W",
        "Floor 3",
        "Toronto",
        "ON",
        "M5V 3A8",
        "ca",
    )

    assert suite_two == "100 King St W, Floor 2, Toronto, ON M5V 3A8, CA"
    assert suite_three == "100 King St W, Floor 3, Toronto, ON M5V 3A8, CA"
    assert suite_two != suite_three


def test_renderer_suppresses_only_an_exact_repeated_second_line() -> None:
    repeated_line = render_formatted_address_v1(
        "Suite 200",
        "Suite 200",
        None,
        None,
        None,
        None,
    )
    repeated_unit = render_formatted_address_v1(
        "100 Main St, Suite 200",
        " Suite   200 ",
        "Springfield",
        "IL",
        "62704",
        "US",
    )
    differently_cased_unit = render_formatted_address_v1(
        "100 Main St, Suite 200",
        "suite 200",
        "Springfield",
        "IL",
        "62704",
        "US",
    )
    different_unit = render_formatted_address_v1(
        "100 Main St, Suite 200",
        "Suite 201",
        "Springfield",
        "IL",
        "62704",
        "US",
    )

    assert repeated_line == "Suite 200"
    assert repeated_unit == "100 Main St, Suite 200, Springfield, IL 62704"
    assert differently_cased_unit == (
        "100 Main St, Suite 200, suite 200, Springfield, IL 62704"
    )
    assert different_unit == (
        "100 Main St, Suite 200, Suite 201, Springfield, IL 62704"
    )


def test_renderer_omits_empty_components_without_inventing_values() -> None:
    assert render_formatted_address_v1(
        None,
        None,
        "  Example   City ",
        None,
        " 12345-6789 ",
        None,
    ) == "Example City, 12345-6789"
    assert render_formatted_address_v1(
        None,
        None,
        None,
        None,
        None,
        "US",
    ) is None
    assert render_formatted_address_v1(
        None,
        None,
        None,
        None,
        None,
        "GB",
    ) == "GB"


def test_renderer_is_unicode_normalized_and_byte_stable() -> None:
    decomposed = render_formatted_address_v1(
        "Cafe\u0301 Road",
        None,
        "Montre\u0301al",
        "QC",
        "H2Y 1C6",
        "CA",
    )
    composed = render_formatted_address_v1(
        "Caf\u00e9 Road",
        None,
        "Montr\u00e9al",
        "QC",
        "H2Y 1C6",
        "CA",
    )

    assert decomposed == composed
    assert decomposed is not None
    assert decomposed.encode("utf-8") == composed.encode("utf-8")


def test_renderer_enforces_unicode_safe_max_length() -> None:
    exact = "\u00e9" * ADDRESS_FORMAT_MAX_LENGTH
    oversized = exact + "\u00e9"
    punctuation_boundary = (
        "x" * (ADDRESS_FORMAT_MAX_LENGTH - 1) + ",,,"
    )

    assert render_formatted_address_v1(
        exact, None, None, None, None, None
    ) == exact
    truncated = render_formatted_address_v1(
        oversized, None, None, None, None, None
    )
    assert truncated == exact
    assert truncated is not None
    assert truncated.encode("utf-8").decode("utf-8") == exact
    assert render_formatted_address_v1(
        punctuation_boundary,
        None,
        None,
        None,
        None,
        None,
    ) == "x" * (ADDRESS_FORMAT_MAX_LENGTH - 1)


def test_migration_is_schema_only_and_installs_renderer_contract(
    monkeypatch,
) -> None:
    migration = _load_migration()
    operation = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "display_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    sql = " ".join(
        _normalized_sql(call.args[0])
        for call in operation.execute.call_args_list
    )
    assert migration.revision == "20260811110000_address_formatted_display"
    assert migration.down_revision == "20260811100000_address_numeric_grid_alias"
    assert (
        'FUNCTION "display_contract"."addr_formatted_address_v1"'
        in sql
    )
    for table_name in (
        "address_archive_v2",
        "entity_address_unified",
        "provider_directory_address_overlay",
    ):
        assert f'ALTER TABLE IF EXISTS "display_contract"."{table_name}"' in sql
    assert "formatted_address_version smallint" in sql
    assert "formatted_address_source varchar(32)" in sql
    assert "formatted_address text" in sql
    assert "UPDATE " not in sql.upper()
    assert "INSERT " not in sql.upper()
    assert "GEOCODE" not in sql.upper()
    assert "LEVENSHTEIN" not in sql.upper()
    assert "SIMILARITY(" not in sql.upper()


def test_migration_and_models_expose_nullable_format_metadata() -> None:
    migration = _load_migration()
    function_sql = _normalized_sql(
        migration._formatted_address_function_sql("display_contract")
    )

    assert "IMMUTABLE" in function_sql
    assert "PARALLEL SAFE" in function_sql
    assert "normalize(COALESCE(first_line, ''), NFC)" in function_sql
    assert "postal_value ~ '^[0-9]{9}$'" in function_sql
    assert "postal_value ~ '^[0-9]{5}[- ][0-9]{4}$'" in function_sql
    assert "country_value IS NULL OR country_value = 'US'" in function_sql
    assert "ELSE country_value" in function_sql
    assert "right(line_one, char_length(line_two)) = line_two" in function_sql
    assert "FROM deduplicated" in function_sql
    assert "char_length(rendered) > 1024" in function_sql
    assert "substring(rendered FROM 1 FOR 1024)" in function_sql
    for model in (AddressArchiveV2, EntityAddressUnified):
        version_column = model.__table__.c.formatted_address_version
        source_column = model.__table__.c.formatted_address_source
        assert isinstance(version_column.type, SmallInteger)
        assert isinstance(source_column.type, String)
        assert source_column.type.length == 32
        assert version_column.nullable is True
        assert source_column.nullable is True
        assert isinstance(model.__table__.c.formatted_address.type, (String, Text))


def test_migration_downgrade_removes_only_display_contract(monkeypatch) -> None:
    migration = _load_migration()
    operation = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "display_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", operation)

    migration.downgrade()

    sql = " ".join(
        _normalized_sql(call.args[0])
        for call in operation.execute.call_args_list
    )
    assert 'DROP FUNCTION IF EXISTS "display_contract"."addr_formatted_address_v1"' in sql
    assert "DROP COLUMN IF EXISTS formatted_address_version" in sql
    assert "DROP COLUMN IF EXISTS formatted_address_source" in sql
    assert "DROP COLUMN IF EXISTS formatted_address" in sql
