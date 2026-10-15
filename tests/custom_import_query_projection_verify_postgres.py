# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native nullable multiset comparisons for persisted projection contexts."""

from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from process.custom_import import query_projection_verify as verify
from tests.custom_import_postgres_support import _database_url
from tests.test_custom_import_query_projection_verify import _context_relation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expected_rows", "stored_rows", "differs"),
    (
        ([(1, 2024, None)], [(1, 2024, None)], False),
        ([(1, 2024, None)], [(1, 2024, Decimal("1"))], True),
        ([(1, 2024, None)], [(1, 2024, None), (1, 2024, None)], True),
        ([(1, 2024, None), (1, 2024, None)], [(1, 2024, None)], True),
        ([(1, 2024, None), (1, 2025, Decimal("2"))], [(1, 2024, None)], True),
    ),
)
async def test_native_comparison_preserves_nulls_values_and_multiplicity(expected_rows, stored_rows, differs):
    expected = _context_relation(expected_rows, "expected_contexts")
    stored = _context_relation(stored_rows, "stored_contexts")
    statement = verify._comparison_statement(select(expected), stored, [1])
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            comparison = (await connection.execute(statement)).one()
        assert (comparison.expected_count, comparison.actual_count, comparison.differs) == (
            len(expected_rows),
            len(stored_rows),
            differs,
        )
    finally:
        await engine.dispose()
