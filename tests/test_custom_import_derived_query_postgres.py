# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native arithmetic parity with configured response-template reducers."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import Numeric, String, column, literal, select, values
from sqlalchemy.ext.asyncio import create_async_engine

from process.custom_import.derived_query import reduce_expression
from tests.custom_import_postgres_support import _database_url
from tests.test_custom_import_derived_query import definition


def scalar(value):
    return literal(None if value is None else Decimal(str(value)), type_=Numeric())


@pytest.mark.parametrize(
    ("first", "second", "first_weight", "second_weight", "expected"),
    [
        (4, 3, 7, 31, "3.184210526316"),
        (4, 5, 7, 31, "4.815789473684"),
        (None, 5, 99, 9, "5.000000000000"),
        (0, None, 1, 2, "0.000000000000"),
        (4, 3, None, 9, "3.000000000000"),
        (4, 3, 0, -1, None),
        (None, None, 1, 1, None),
        (1, 2, "0.1", "0.2", "1.666666666667"),
        (0, "0.000000000001", 1, 1, "0.000000000000"),
        ("0.000000000001", "0.000000000002", 1, 1, "0.000000000002"),
        ("-0.000000000001", "-0.000000000002", 1, 1, "-0.000000000002"),
        ("-0.000000000001", 0, 1, 1, "0.000000000000"),
        ("999999999999999999.999999999999", "999999999999999999.999999999999", 1, 1, "999999999999999999.999999999999"),
        (9223372036854775807, 9223372036854775807, 1, 1, "9223372036854775807.000000000000"),
    ],
)
@pytest.mark.asyncio
async def test_native_weighted_mean_matches_twelve_place_half_even(
    first, second, first_weight, second_weight, expected
):
    engine = create_async_engine(_database_url())
    field = definition().query.derived_by_id["combined_score"]
    expression = reduce_expression(
        field,
        {"segment_a": scalar(first), "segment_b": scalar(second)},
        {"segment_a": scalar(first_weight), "segment_b": scalar(second_weight)},
    )
    try:
        async with engine.connect() as connection:
            actual = await connection.scalar(select(expression))
        assert actual == (None if expected is None else Decimal(expected))
    finally:
        await engine.dispose()


@pytest.mark.parametrize(("first", "second", "expected"), [(0, 9, 0), (None, 9, 9), (None, None, None)])
@pytest.mark.asyncio
async def test_native_preferred_value_keeps_zero_and_uses_configured_group_order(first, second, expected):
    engine = create_async_engine(_database_url())
    field = definition().query.derived_by_id["merged_cost"]
    try:
        async with engine.connect() as connection:
            actual = await connection.scalar(
                select(
                    reduce_expression(
                        field,
                        {"segment_a": scalar(first), "segment_b": scalar(second)},
                    )
                )
            )
            reverse = await connection.scalar(
                select(
                    reduce_expression(
                        replace(field, group_values=("segment_b", "segment_a")),
                        {"segment_a": scalar(first), "segment_b": scalar(second)},
                    )
                )
            )
        assert actual == (None if expected is None else Decimal(expected))
        assert reverse == (Decimal(second) if second is not None else None)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_reducers_keep_absent_groups_absent_and_honor_group_subsets():
    fields = definition().query.derived_by_id
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            one_family = await connection.scalar(
                select(
                    reduce_expression(
                        fields["combined_score"],
                        {"segment_b": scalar(3)},
                        {"segment_b": scalar(2)},
                    )
                )
            )
            subset = await connection.scalar(
                select(
                    reduce_expression(
                        replace(fields["combined_score"], group_values=("segment_b",)),
                        {"segment_a": scalar(99), "segment_b": scalar(3)},
                        {"segment_a": scalar(99), "segment_b": scalar(2)},
                    )
                )
            )
            missing = await connection.scalar(select(reduce_expression(fields["merged_cost"], {})))
        assert one_family == subset == Decimal("3.000000000000")
        assert missing is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_native_score_filters_and_sorting_use_the_same_rounded_metric():
    field = definition().query.derived_by_id["combined_score"]
    family_values = values(
        column("identity", String),
        column("first", Numeric),
        column("second", Numeric),
        name="selected_family_values",
    ).data(
        [
            ("a", Decimal("0"), Decimal("0.000000000001")),
            ("b", Decimal("0.000000000001"), Decimal("0.000000000002")),
            ("c", None, None),
        ]
    )
    reduced = select(
        family_values.c.identity,
        reduce_expression(
            field,
            {"segment_a": family_values.c.first, "segment_b": family_values.c.second},
            {"segment_a": scalar(1), "segment_b": scalar(1)},
        ).label("score"),
    ).cte("scored_entities")
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            equal = (await connection.execute(select(reduced.c.identity).where(reduced.c.score == Decimal("0")))).all()
            ordered = (
                await connection.execute(
                    select(reduced.c.identity).order_by(
                        reduced.c.score.desc().nulls_last(),
                        reduced.c.identity,
                    )
                )
            ).all()
        assert equal == [("a",)]
        assert ordered == [("b",), ("a",), ("c",)]
    finally:
        await engine.dispose()
