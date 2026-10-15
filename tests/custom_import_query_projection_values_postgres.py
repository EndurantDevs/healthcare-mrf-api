# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only native checks of derived contexts and ambiguity evidence."""

from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process.custom_import.query_projection_values import ambiguity_statement, reduction_statement
from tests.custom_import_postgres_support import _database_url
from tests.test_custom_import_query_projection_values import _fields, _source


@pytest.mark.asyncio
async def test_native_contexts_keep_nulls_half_even_and_bigint_boundaries():
    maximum, minimum = 2**63 - 1, -(2**63)
    source_rows = [
        (1, 2024, "segment_a", Decimal("0"), 1),
        (1, 2024, "segment_b", Decimal("0.000000000001"), 1),
        (1, 2025, "segment_a", maximum, maximum),
        (1, 2025, "segment_b", maximum, maximum),
        (2, 2025, "segment_a", 9, 0),
        (2, 2025, "segment_b", 10, -1),
        (3, 2025, "segment_a", None, 999),
        (3, 2025, "segment_b", 5, 2),
        (4, 2025, "segment_a", Decimal("-0.000000000001"), 1),
        (4, 2025, "segment_b", Decimal("-0.000000000002"), 1),
        (5, 2025, "segment_a", minimum, 1),
        (5, 2025, "segment_b", minimum, 1),
        (6, 2025, "segment_b", 3, 2),
    ]
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            assert (await connection.execute(ambiguity_statement(_source(source_rows)))).all() == []
            source_relation = _source(source_rows)
            statement = reduction_statement(
                source_relation,
                _fields(),
                values_by_field={"metric": source_relation.c.metric, "weight": source_relation.c.weight},
                field_slots={"combined_metric": 1, "preferred_weight": 2},
            )
            contexts = (await connection.execute(statement)).all()
        actual_by_context = {
            (context.entity_binding_id, context.selected_value): (context.field_1, context.field_2)
            for context in contexts
        }
        expected_by_context = {
            (1, 2024): (Decimal("0.000000000000"), 1),
            (1, 2025): (Decimal("9223372036854775807.000000000000"), maximum),
            (2, 2025): (None, 0),
            (3, 2025): (Decimal("5.000000000000"), 999),
            (4, 2025): (Decimal("-0.000000000002"), 1),
            (5, 2025): (Decimal("-9223372036854775808.000000000000"), 1),
            (6, 2025): (Decimal("3.000000000000"), 2),
        }
        assert len(contexts) == len(expected_by_context)
        assert actual_by_context == expected_by_context
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_child_contexts_keep_distinct_complete_keys_and_null_values():
    rows = [
        (1, 2025, "segment_a", None, 2, '["first"]', b"a" * 32),
        (1, 2025, "segment_b", 3, 0, '["first"]', b"a" * 32),
        (1, 2025, "segment_b", 4, 2, '["second"]', b"b" * 32),
    ]
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            assert (await connection.execute(ambiguity_statement(_source(rows, child=True)))).all() == []
            source = _source(rows, child=True)
            statement = reduction_statement(
                source,
                _fields(child=True)[:1],
                values_by_field={"metric": source.c.metric, "weight": source.c.weight},
                field_slots={"combined_metric": 1},
            )
            cells = (await connection.execute(statement)).all()
        assert {(cell.canonical_child_key, cell.child_key_sha256, cell.field_1) for cell in cells} == {
            ('["first"]', b"a" * 32, None),
            ('["second"]', b"b" * 32, Decimal("4.000000000000")),
        }
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("child", (False, True))
async def test_native_ambiguity_cannot_hide_behind_nulls_weights_or_key_hashes(child):
    rows = [(1, 2025, "segment_a", None, 0), (1, 2025, "segment_a", 9, -1)]
    if child:
        rows = [(*row, '["same"]', bytes([index]) * 32) for index, row in enumerate(rows)]
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            ambiguous = (await connection.execute(ambiguity_statement(_source(rows, child=child)))).all()
        assert len(ambiguous) == 1 and ambiguous[0].matching_count == 2
    finally:
        await engine.dispose()
