# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native preparation matches database expressions, including locale fallbacks."""

import importlib
import io
import os

import asyncpg
import pytest

from process import entity_address_rows_native as rows


def _records():
    values = [
        {},
        {
            "entity_name": " Éxample ",
            "type": " ",
            "first_line": " 12 Main St. ",
            "city_name": " New York ",
            "state_name": " ny ",
            "postal_code": "10001-1234",
            "telephone_number": "3125550102 x007",
        },
        {
            "first_line": " I Avenue ",
            "city_name": "I",
            "state_name": "i",
            "country_code": "unitedstatesofamerica",
            "telephone_number": "3125550102 EXTENSION 005",
        },
        {"first_line": " K Street ", "city_name": "İstanbul", "telephone_number": "3125550102\u2003x7"},
        {"telephone_number": "undefİned", "fax_number": " None ", "country_code": " ", "place_id": "\t"},
        {"telephone_number": "3125550102\vx\v007\v", "fax_number": "x42", "country_code": "U.S.A."},
        {"telephone_number": "3125550102 ext 0-07", "fax_number": "3125550102 ext 12345678901234567"},
        {"telephone_number": " +44 20 7946 0958 ", "fax_number": "\t+44 20 7946 0958", "country_code": "GB"},
        {"entity_name": "\t\\N\n", "first_line": " ", "second_line": "\t", "postal_code": "１２３４５-10001"},
    ]
    return [(ordinal, *(record.get(field) for field in rows.TEXT_COLUMNS)) for ordinal, record in enumerate(values, 1)]


def _reference_expressions(sql):
    sanitized_by_field = {field: f"NULLIF(TRIM(b.{field}), '')" for field in rows.TEXT_COLUMNS}
    sanitized_by_field["type"] = "COALESCE(NULLIF(TRIM(b.type), ''), 'primary')"
    sanitized_by_field["country_code"] = "COALESCE(NULLIF(TRIM(b.country_code), ''), 'US')"
    for field in ("telephone_number", "fax_number"):
        sanitized_by_field[field] = sql._nullish_text_expr(f"b.{field}")
    for raw, number, extension in (
        ("telephone_number", "phone_number", "phone_extension"),
        ("fax_number", "fax_number_digits", "fax_extension"),
    ):
        sanitized_by_field[number] = sql._canonical_contact_number_expr(f"b.{raw}", "b.country_code")
        sanitized_by_field[extension] = sql._contact_extension_expr(f"b.{raw}")
    normalized_by_field = {
        "first_line_norm": sql._alnum_norm_expr("s.first_line"),
        "second_line_norm": sql._alnum_norm_expr("s.second_line"),
        "city_norm": sql._alnum_norm_expr("s.city_name"),
        "state_code": sql._state_norm_expr("s.state_name"),
        "zip5": sql._zip5_norm_expr("s.postal_code"),
        "country_norm": sql._state_norm_expr("s.country_code"),
        "telephone_digits": sql._phone_norm_expr("s.telephone_number"),
    }
    return sanitized_by_field, normalized_by_field


def _checksum_sql(sql):
    return sql._address_checksum_expr(
        {
            "entity_type": "'npi'",
            "entity_id": "row_id::text",
            "first_line": "first_line_norm",
            "second_line": "second_line_norm",
            "city_name": "city_norm",
            "state_name": "state_code",
            "postal_code": "zip5",
            "country_code": "country_norm",
            "telephone_number": "telephone_digits",
        }
    )


async def _collation_sql(connection, locale):
    if locale == "C":
        return 'pg_catalog."C"'
    collation = await connection.fetchval(
        "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.collname) FROM pg_collation c "
        "JOIN pg_namespace n ON n.oid=c.collnamespace WHERE c.collname LIKE 'tr-%' "
        "AND c.collprovider='i' ORDER BY c.collname LIMIT 1"
    )
    if collation is None:
        pytest.skip("Turkish ICU collation is unavailable")
    return collation


@pytest.mark.asyncio
@pytest.mark.parametrize("locale", ["C", "turkish"])
async def test_native_rows_match_postgres_text_contacts_and_actual_collation(locale):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select the test database")
    sql = importlib.import_module("process.entity_address_unified")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        collation = await _collation_sql(connection, locale)
        async with connection.transaction():
            text_columns = ", ".join(f"{field} text COLLATE {collation}" for field in rows.TEXT_COLUMNS)
            await connection.execute(
                f"CREATE TEMP TABLE input_rows(row_id bigint PRIMARY KEY, {text_columns}) ON COMMIT DROP"
            )
            placeholders = ", ".join(f"${index}" for index in range(1, len(rows.INPUT_COLUMNS) + 1))
            await connection.executemany(f"INSERT INTO input_rows VALUES ({placeholders})", _records())
            input_bytes = io.BytesIO()
            await connection.copy_from_query(
                f"SELECT {rows.input_projection_sql()} FROM input_rows b ORDER BY b.row_id",
                output=input_bytes,
                format="binary",
            )
            prepared, count = rows.prepare_entity_address_rows(input_bytes.getvalue())
            columns = ", ".join(
                f"{name} {kind}" for name, kind in zip(rows.OUTPUT_COLUMNS, rows.OUTPUT_TYPES, strict=True)
            )
            await connection.execute(f"CREATE TEMP TABLE prepared_rows({columns}) ON COMMIT DROP")
            assert (
                await connection.copy_to_table("prepared_rows", source=io.BytesIO(prepared), format="binary")
                == f"COPY {count}"
            )
            expressions = rows.prepared_value_expressions(
                sql, case_qualified=locale == "C", contact_qualified=locale == "C"
            )
            actual = await connection.fetch(
                "WITH projected AS (SELECT b.row_id, "
                + ", ".join(f"{expression} AS {field}" for field, expression in expressions.items())
                + " FROM input_rows b JOIN prepared_rows p USING(row_id)) SELECT projected.*, "
                + _checksum_sql(sql)
                + " AS checksum FROM projected ORDER BY row_id"
            )
            sanitized, normalized = _reference_expressions(sql)
            reference = await connection.fetch(
                "WITH s AS (SELECT b.row_id, "
                + ", ".join(f"{expression} AS {field}" for field, expression in sanitized.items())
                + " FROM input_rows b), projected AS (SELECT s.*, "
                + ", ".join(f"{expression} AS {field}" for field, expression in normalized.items())
                + " FROM s) SELECT projected.*, "
                + _checksum_sql(sql)
                + " AS checksum FROM projected ORDER BY row_id"
            )
            assert count == len(reference) == len(actual)
            assert [dict(result_row) for result_row in actual] == [dict(result_row) for result_row in reference]
            assert any(result_row["checksum"] < 0 for result_row in reference)
    finally:
        await connection.close()
