# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete-context keyset pages and bounded native cell insert statements."""

from dataclasses import replace

import pytest
from sqlalchemy import BigInteger, Column, LargeBinary, MetaData, Numeric, String, Table, Text, column, values
from sqlalchemy.dialects import postgresql

from process.custom_import.query_projection_pages import cell_insert_statement, context_key_statement
from tests.test_custom_import_query_projection_values import _fields


def _source(*, child=False):
    columns = [
        column("entity_binding_id", BigInteger),
        column("selected_value", BigInteger),
        column("group_value", String),
        column("metric", Numeric),
        column("weight", BigInteger),
    ]
    if child:
        columns += [column("child_key_sha256", LargeBinary), column("canonical_child_key", String)]
    return values(*columns, name="finalized_sources")


def _table(fields, slots, *, child=False):
    columns = [Column("entity_binding_id", BigInteger), Column("selected_value", BigInteger)]
    if child:
        columns += [Column("child_key_sha256", LargeBinary), Column("canonical_child_key", Text(collation="C"))]
    seen_slots = set()
    for field in fields:
        slot = slots.get(field.field_id)
        if type(slot) is int and 1 <= slot <= 32 and slot not in seen_slots:
            columns.append(Column(f"field_{slot}", BigInteger if field.value_type == "integer" else Numeric(31, 12)))
            seen_slots.add(slot)
    return Table("derived_contexts", MetaData(), *columns)


def _insert(*, child=False, keys=None, fields=None, slots=None, mapping=None):
    source = _source(child=child)
    fields = _fields(child=child) if fields is None else fields
    slots = {field.field_id: index for index, field in enumerate(fields, 1)} if slots is None else slots
    mapping = {"metric": source.c.metric, "weight": source.c.weight} if mapping is None else mapping
    if keys is None:
        keys = [(7, -2024, b"a" * 32, '["chosen"]')] if child else [(7, -2024)]
    table = _table(fields, slots, child=child)
    return cell_insert_statement(table, source, values_by_field=mapping, fields=fields, field_slots=slots, keys=keys)


@pytest.mark.parametrize("child", (False, True))
def test_context_pages_order_complete_distinct_keys_with_signed_selection(child):
    source = _source(child=child)
    cursor = (7, -(2**63), b"a" * 32) if child else (7, -(2**63))
    statement = context_key_statement(source, child=child, after=cursor, limit=256)
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert tuple(statement.selected_columns.keys()) == (
        "entity_binding_id",
        "selected_value",
        *(("child_key_sha256", "canonical_child_key") if child else ()),
    )
    assert sql.startswith("SELECT DISTINCT")
    assert 'COLLATE "C"' in sql if child else "COLLATE" not in sql
    assert "finalized_sources.entity_binding_id, finalized_sources.selected_value" in sql
    assert " > (" in sql and "OFFSET" not in sql
    assert -(2**63) in compiled.params.values() and 256 in compiled.params.values()
    predicates = sql.split("WHERE", 1)[1]
    assert "group_value" not in predicates and "metric" not in predicates and "weight" not in predicates


def test_initial_context_page_has_no_cursor_predicate_and_accepts_maximum_selection():
    source = _source()
    first = context_key_statement(source, child=False, after=None, limit=1)
    assert "WHERE" not in str(first.compile(dialect=postgresql.dialect()))
    last = context_key_statement(source, child=False, after=(2**63 - 1, 2**63 - 1), limit=1)
    assert list(last.compile(dialect=postgresql.dialect()).params.values()).count(2**63 - 1) == 2


@pytest.mark.parametrize("limit", (0, -1, 257, True, "1", None))
def test_context_page_limits_are_bounded_positive_integers(limit):
    with pytest.raises(ValueError, match="limit"):
        context_key_statement(_source(), child=False, after=None, limit=limit)


@pytest.mark.parametrize(
    "cursor",
    (
        (0, 2024),
        (-1, 2024),
        (True, 2024),
        (2**63, 2024),
        (7, -(2**63) - 1),
        (7, 2**63),
        (7, True),
        (7,),
        (7, 2024, b"a" * 32),
        "invalid",
    ),
)
def test_root_cursor_rejects_wrong_shape_types_and_integer_overflow(cursor):
    with pytest.raises(ValueError):
        context_key_statement(_source(), child=False, after=cursor, limit=1)


@pytest.mark.parametrize("tail", (None, b"a" * 31, b"a" * 33, "a" * 32, bytearray(b"a" * 32)))
def test_child_cursor_requires_an_exact_hash(tail):
    with pytest.raises(ValueError, match="hash"):
        context_key_statement(_source(child=True), child=True, after=(7, 2024, tail), limit=1)


@pytest.mark.parametrize("child", (False, True))
def test_context_page_scope_must_match_its_source(child):
    with pytest.raises(ValueError, match="scope"):
        context_key_statement(_source(child=child), child=not child, after=None, limit=1)


@pytest.mark.parametrize("child", (False, True))
def test_insert_restricts_whole_contexts_and_records_nullable_declaration_columns(child):
    statement = _insert(child=child, slots={"combined_metric": 9, "preferred_weight": 4})
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "INSERT INTO" in sql and "SELECT" in sql and "COPY" not in sql
    assert tuple(statement.select.selected_columns.keys()) == (
        "entity_binding_id",
        "selected_value",
        *(("canonical_child_key", "child_key_sha256") if child else ()),
        "field_9",
        "field_4",
    )
    assert "UNION" not in sql
    assert "field_slot" not in sql and "field_type" not in sql and "value_state" not in sql
    assert "WHERE (finalized_sources.entity_binding_id, finalized_sources.selected_value" in sql
    assert " IN (__[POSTCOMPILE_param_" in sql
    assert "FILTER (WHERE finalized_sources.group_value" not in sql
    assert "NUMERIC(31, 12)" in sql and "AS BIGINT" in sql
    if child:
        assert 'finalized_sources.canonical_child_key COLLATE "C"' in sql
        assert "canonical_child_key, child_key_sha256, field_9, field_4" in sql


def test_cell_insert_mapping_can_use_field_ids_equal_to_envelope_names():
    source = _source()
    fields = (replace(_fields()[0], source_field_id="selected_value", weight_field_id="group_value"),)
    statement = cell_insert_statement(
        _table(fields, {"combined_metric": 1}),
        source,
        values_by_field={"selected_value": source.c.metric, "group_value": source.c.weight},
        fields=fields,
        field_slots={"combined_metric": 1},
        keys=[(7, 2024)],
    )
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "max(anon_" in sql
    assert ".metric) FILTER" in sql and ".weight) FILTER" in sql
    assert ".selected_value) FILTER" not in sql and ".group_value) FILTER" not in sql


@pytest.mark.parametrize(
    "keys", ([], [(7, 2024)] * 257, [(7, 2024), (7, 2024)], [(7, None)], [(0, 2024)], [(7, 2**63)])
)
def test_cell_insert_rejects_unbounded_repeated_or_invalid_keys(keys):
    with pytest.raises(ValueError):
        _insert(keys=keys)


@pytest.mark.parametrize("canonical", ("", "\x00", "\ud800", "a" * 8193, "ě" * 4097, None))
def test_child_keys_require_bounded_valid_canonical_text(canonical):
    with pytest.raises(ValueError, match="canonical"):
        _insert(child=True, keys=[(7, 2024, b"a" * 32, canonical)])


def test_child_hash_cannot_repeat_with_another_canonical_key():
    with pytest.raises(ValueError, match="repeats"):
        _insert(child=True, keys=[(7, 2024, b"a" * 32, '["first"]'), (7, 2024, b"a" * 32, '["second"]')])


def test_child_keys_preserve_multibyte_text_at_the_storage_byte_limit():
    statement = _insert(child=True, keys=[(7, -(2**63), b"a" * 32, "ě" * 4096)])
    assert any(
        isinstance(value, list) and value[0][-1] == "ě" * 4096
        for value in statement.compile(dialect=postgresql.dialect()).params.values()
    )


@pytest.mark.parametrize("slots", ({}, {"combined_metric": 0}, {"combined_metric": 33}, {"combined_metric": True}))
def test_numeric_fields_require_valid_declaration_slots(slots):
    with pytest.raises(ValueError, match="slot"):
        _insert(fields=_fields()[:1], slots=slots)


def test_duplicate_fields_and_duplicate_slots_are_rejected():
    with pytest.raises(ValueError, match="unique"):
        _insert(fields=(_fields()[0], _fields()[0]))
    with pytest.raises(ValueError, match="unique"):
        _insert(slots={"combined_metric": 1, "preferred_weight": 1})


@pytest.mark.parametrize("count", (0, 33))
def test_field_count_stays_within_the_configured_numeric_limit(count):
    fields = tuple(replace(_fields()[0], field_id=f"metric_{index}") for index in range(count))
    with pytest.raises(ValueError, match="numeric fields"):
        _insert(fields=fields)


def test_maximum_key_page_and_declaration_count_are_accepted():
    fields = tuple(replace(_fields()[0], field_id=f"metric_{index}") for index in range(32))
    statement = _insert(fields=fields, keys=[(index + 1, -(2**63)) for index in range(256)])
    parameters = statement.compile(dialect=postgresql.dialect()).params
    assert any(isinstance(value, list) and len(value) == 256 for value in parameters.values())
    assert "field_32" in statement.select.selected_columns


def test_cell_insert_rejects_non_numeric_fields_and_external_value_columns():
    with pytest.raises(ValueError, match="numeric"):
        _insert(fields=(replace(_fields()[0], value_type="string"),))
    with pytest.raises(ValueError, match="source output"):
        _insert(mapping={"metric": column("external", Numeric)})


def test_cell_insert_rejects_source_destination_scope_mismatch():
    source = _source(child=True)
    with pytest.raises(ValueError, match="scopes"):
        cell_insert_statement(
            _table(_fields(), {"combined_metric": 1, "preferred_weight": 2}),
            source,
            values_by_field={},
            fields=_fields(),
            field_slots={},
            keys=[(7, 2024)],
        )
