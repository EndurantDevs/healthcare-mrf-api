# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import inspect
import re
import struct
import sys
from types import SimpleNamespace

import pytest

from process import entity_address_rows_native as rows

HEADER = b"PGCOPY\n\xff\r\n\0" + bytes(8)


def _copy_input(records):
    output = bytearray(HEADER)
    for ordinal, fields in records:
        output.extend(struct.pack(">h", len(rows.INPUT_COLUMNS)))
        for value in (struct.pack(">q", ordinal), *(None if value is None else value.encode() for value in fields)):
            output.extend(struct.pack(">i", -1 if value is None else len(value)))
            if value is not None:
                output.extend(value)
    return bytes(output) + struct.pack(">h", -1)


def _decode_copy_value(field, wire):
    if field == "row_id":
        return struct.unpack(">q", wire)[0]
    if field.startswith("native_"):
        assert wire in (b"\0", b"\1")
        return wire == b"\1"
    return None if wire is None else wire.decode()


def _copy_output(payload):
    assert payload[:19] == HEADER
    offset, records = 19, []
    while (width := struct.unpack_from(">h", payload, offset)[0]) != -1:
        assert width == len(rows.OUTPUT_COLUMNS)
        offset += 2
        values = []
        for field in rows.OUTPUT_COLUMNS:
            length = struct.unpack_from(">i", payload, offset)[0]
            offset += 4
            value = None if length == -1 else payload[offset : offset + length]
            offset += max(0, length)
            values.append(_decode_copy_value(field, value))
        records.append(dict(zip(rows.OUTPUT_COLUMNS, values, strict=True)))
    assert offset + 2 == len(payload)
    return records


def _ascii_contact(raw, country):
    extension = re.search(r"(extension|ext\.?|;ext=|#|x)[\t\n\r\f\v ]*([0-9]{1,16})[\t\n\r\f\v ]*$", raw or "", re.I)
    main = (raw or "")[: extension.start()].rstrip("\t\n\r\f\v ") if extension else raw or ""
    digits = re.sub("[^0-9]", "", main)
    country = re.sub("[^A-Z]", "", (country or "").upper())
    is_default_us = country in ("", "US", "USA", "UNITEDSTATES", "UNITEDSTATESOFAMERICA")
    if is_default_us and len(digits) == 10:
        number = digits
    elif is_default_us and len(digits) == 11 and digits.startswith("1"):
        number = digits[1:]
    elif main.strip(" ").startswith("+") and 8 <= len(digits) <= 15:
        number = digits
    else:
        number = None
    return number, extension[2] if extension else None


def test_fixed_copy_schema_and_direct_projection():
    assert len(rows.INPUT_COLUMNS) == 14
    assert len(rows.OUTPUT_COLUMNS) == len(rows.OUTPUT_TYPES) == 27
    assert rows.OUTPUT_TYPES[0] == "bigint"
    assert rows.OUTPUT_TYPES[-2:] == ("boolean", "boolean")
    projection = rows.input_projection_sql("candidate", include_base_address_version=False)
    assert projection.startswith("candidate.row_id, candidate.entity_name::text")
    assert "NULL::text" in projection
    assert "candidate.base_address_version" not in projection
    assert rows.input_projection_sql().count("b.base_address_version::text") == 1
    with pytest.raises(ValueError):
        rows.input_projection_sql("b; SELECT")


def test_native_identity_uses_compiled_export_artifact_and_does_not_cache(monkeypatch):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    first = rows.native_preparation_identity()
    assert first["implementation"] == "native" and first["contract"] == rows.CONTRACT
    assert re.fullmatch("[0-9a-f]{64}", first["artifact_sha256"])
    assert set(first) == {"implementation", "contract", "artifact_sha256"}
    monkeypatch.setattr(rows.hashlib, "file_digest", lambda *_args: SimpleNamespace(hexdigest=lambda: "f" * 64))
    assert rows.native_preparation_identity()["artifact_sha256"] == "f" * 64
    monkeypatch.setattr(rows, "native_preparation_module", lambda: None)
    assert rows.native_preparation_identity() == {"implementation": "sql", "contract": rows.CONTRACT}


def test_prepared_builders_consume_shared_components_without_contact_reprocessing():
    sql = importlib.import_module("process.entity_address_unified")
    for builder in (sql._insert_raw_sanitized_cte_sql, sql._direct_materialize_sanitized_cte_sql):
        sanitized = builder(native_prepared=True)
        assert "telephone_number::varchar AS telephone_number" in sanitized
        assert "phone_number::varchar AS phone_number" in sanitized
        assert "regexp_replace" not in sanitized
        assert all(f"prepared_{field}" in sanitized for field in rows.NORMALIZED_COLUMNS)
    checksum = sql._direct_materialize_checksum_sql(native_prepared=True)
    assert "md5(lower(concat_ws" in checksum and "::bit(32)::int" in checksum
    assert "regexp_replace" not in checksum
    location = sql._direct_materialize_location_key_sql(native_prepared=True)
    assert all(field in location for field in ("prepared_zip5", "prepared_state_code", "prepared_city_norm"))


def test_adapter_passes_only_bytes_and_rejects_stale_or_invalid_results(monkeypatch):
    payload = HEADER + b"\xff\xff"
    module = SimpleNamespace(entity_address_rows_version=lambda: rows.CONTRACT)
    submitted_inputs = []
    module.prepare_entity_address_rows = lambda input_bytes: submitted_inputs.append(input_bytes) or (payload, 0)
    monkeypatch.setitem(sys.modules, "ptg2_address_canon", module)
    assert rows.prepare_entity_address_rows(payload) == (payload, 0)
    assert submitted_inputs == [payload]
    with pytest.raises(ValueError):
        rows.prepare_entity_address_rows(bytearray(payload))
    with pytest.raises(ValueError):
        rows.prepare_entity_address_rows(bytes(rows.MAX_INPUT_BYTES + 1))
    for result in [(payload, True), (payload, rows.MAX_ROWS + 1), (b"bad", 0), [payload, 0]]:
        module.prepare_entity_address_rows = lambda _input, result=result: result
        with pytest.raises(RuntimeError):
            rows.prepare_entity_address_rows(payload)
    module.entity_address_rows_version = lambda: "older"
    with pytest.raises(RuntimeError):
        rows.prepare_entity_address_rows(payload)


def test_sql_fallbacks_use_original_contacts_and_prepared_address_fields():
    sql = importlib.import_module("process.entity_address_unified")
    expressions = rows.prepared_value_expressions(sql, case_qualified=True, contact_qualified=True)
    assert tuple(expressions) == rows.OUTPUT_COLUMNS[1:-2]
    assert set(expressions) == set((*rows.TEXT_COLUMNS, *rows.CONTACT_COLUMNS, *rows.NORMALIZED_COLUMNS))
    assert expressions["first_line"] == "p.first_line"
    assert expressions["telephone_number"] == (
        "CASE WHEN p.native_contact_safe THEN p.telephone_number "
        f"ELSE {sql._nullish_text_expr('b.telephone_number')} END"
    )
    assert expressions["phone_number"] == (
        "CASE WHEN p.native_contact_safe THEN p.phone_number "
        f"ELSE {sql._canonical_contact_number_expr('b.telephone_number', 'b.country_code')} END"
    )
    city_fallback = sql._alnum_norm_expr("NULLIF(TRIM(b.city_name), '')")
    assert expressions["city_norm"] == (f"CASE WHEN p.native_case_safe THEN p.city_norm ELSE {city_fallback} END")
    assert expressions["telephone_digits"] == "p.telephone_digits"
    assert "location_key" not in expressions and "checksum" not in expressions
    unqualified = rows.prepared_value_expressions(sql)
    assert unqualified["city_norm"].startswith("CASE WHEN FALSE")
    assert unqualified["phone_number"].startswith("CASE WHEN FALSE")
    assert unqualified["zip5"] == sql._zip5_norm_expr("b.postal_code")
    with pytest.raises(ValueError):
        rows.prepared_value_expressions(sql, case_qualified="yes")


def test_compiled_batch_matches_independent_ascii_reference_and_unicode_fallbacks():
    native = pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    assert inspect.isbuiltin(native.prepare_entity_address_rows)
    contacts = [
        (None, None),
        ("x42", "US"),
        ("3125550102ext.007", "U.S.A."),
        ("3125550102 ext 0-07", "US"),
        ("3125550102 ;EXT=05\t", "US"),
        ("3125550102\vx\v007\v", "US"),
        ("3125550102", "UNITED STATES OF AMERICA"),
        ("+44 20 7946 0958", "GB"),
        ("\t+44 20 7946 0958", "GB"),
        ("3125550102 x12345678901234567", "US"),
    ]
    source_rows = []
    for ordinal, (phone, country) in enumerate(contacts, 1):
        source_row = dict.fromkeys(rows.TEXT_COLUMNS)
        source_row.update(
            entity_name=" Éxample ",
            first_line=" 12 Main St. ",
            city_name=" New York ",
            state_name=" ny ",
            postal_code="10001-1234",
            telephone_number=phone,
            country_code=country,
        )
        source_rows.append((ordinal, tuple(source_row.values())))
    prepared, count = rows.prepare_entity_address_rows(_copy_input(source_rows))
    output = _copy_output(prepared)
    assert count == len(output) == len(contacts)
    for prepared_row, (phone, country) in zip(output, contacts, strict=True):
        assert (prepared_row["phone_number"], prepared_row["phone_extension"]) == _ascii_contact(phone, country)
        assert prepared_row["entity_name"] == "Éxample"
        assert prepared_row["first_line_norm"] == "12mainst"
        assert prepared_row["city_norm"] == "newyork"
        assert prepared_row["state_code"] == "NY"
        assert prepared_row["zip5"] == "10001"
        assert prepared_row["native_case_safe"] and prepared_row["native_contact_safe"]
    unicode_fields = dict.fromkeys(rows.TEXT_COLUMNS)
    unicode_fields.update(first_line=" K Street ", telephone_number="3125550102\u2003x7")
    prepared, count = rows.prepare_entity_address_rows(_copy_input([(1, tuple(unicode_fields.values()))]))
    [prepared_row] = _copy_output(prepared)
    assert count == 1 and prepared_row["first_line"] == "K Street"
    assert prepared_row["first_line_norm"] is None and prepared_row["phone_number"] is None
    assert not prepared_row["native_case_safe"] and not prepared_row["native_contact_safe"]


def test_compiled_rejects_a_late_bad_row_without_returning_partial_copy():
    native = pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    assert inspect.isbuiltin(native.prepare_entity_address_rows)
    payload = _copy_input([(1, (None,) * 13), (2, (None,) * 13)])
    for corrupt in (payload[:-1], payload + b"extra", _copy_input([(1, (None,) * 13), (1, (None,) * 13)])):
        with pytest.raises(ValueError):
            native.prepare_entity_address_rows(corrupt)
