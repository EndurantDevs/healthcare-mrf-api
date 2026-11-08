# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execute helper correlation and cardinality over synthetic eligible rows."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import BigInteger, String, column, false, select, values
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process.custom_import import grouped_read, read_core
from tests import test_custom_import_grouped_child_read_postgres as child_fixture
from tests.custom_import_postgres_support import _database_url


@asynccontextmanager
async def _connection():
    """Release the connection and engine without creating database objects."""

    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            yield connection
    finally:
        await engine.dispose()


def _helper_statement(monkeypatch, helper_rows):
    """Keep real family selection over the existing synthetic owner selectables."""

    context, selected_roots = child_fixture._candidate_context(child_fixture.fixture.context(), False)
    plan = grouped_read.normalize_plan(
        context, child_fixture.fixture.query(), read_core.ExtensionReadScope("synthetic:helper")
    )
    helper = (
        values(column("entity_binding_id", BigInteger), column("selected_value", BigInteger))
        .data(helper_rows or [(0, 0)])
        .cte("synthetic_helper_values")
    )
    helper_query = select(helper) if helper_rows else select(helper).where(false())

    def root_value(statement, _context, field, **_kwargs):
        name = "selected_value" if field.field_id == "period" else "group_value"
        return statement, selected_roots.selected_columns[name]

    # The fixture supplies already eligible roots and guarded helper output;
    # the production scalar correlation and missing/duplicate behavior stay real.
    monkeypatch.setattr(read_core, "_filtered_npi_winner_statement", lambda *_args: selected_roots)
    monkeypatch.setattr(grouped_read, "_join_root_scalar", root_value)
    monkeypatch.setattr(grouped_read, "_selected_value_relation", lambda *_args: helper_query.subquery())
    native_entities = values(column("npi", BigInteger)).data([(200,)]).cte("synthetic_native_entities")
    statement = grouped_read.selected_family_statement(
        context,
        plan,
        materialize_default=True,
        entity_values=select(native_entities.c.npi.cast(String)),
    )
    winner = context.model(read_core.CustomImportWinner)
    return statement.with_only_columns(winner.family_revision_id, maintain_column_froms=True).order_by(
        winner.family_revision_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("helper_rows", "expected_families"),
    [
        pytest.param([], [], id="missing-helper"),
        pytest.param([(300, 2024)], [], id="wrong-entity"),
        pytest.param([(200, 2024)], [10, 11], id="selected-sibling-families"),
        pytest.param([(200, 2024), (300, 2025)], [10, 11], id="unrelated-newer-helper"),
    ],
)
async def test_correlated_helper_requires_the_selected_entity(monkeypatch, helper_rows, expected_families):
    """Missing or foreign helpers cannot select a family or change its year."""

    statement = _helper_statement(monkeypatch, helper_rows)
    async with _connection() as connection:
        assert (await connection.execute(statement)).scalars().all() == expected_families


@pytest.mark.asyncio
@pytest.mark.parametrize("second_value", [2024, 2025], ids=["same-value", "different-value"])
async def test_duplicate_correlated_helper_retains_scalar_cardinality_error(monkeypatch, second_value):
    """Duplicate eligible helpers fail instead of being deduplicated or limited."""

    statement = _helper_statement(monkeypatch, [(200, 2024), (200, second_value)])
    async with _connection() as connection:
        with pytest.raises(DBAPIError, match="more than one row returned by a subquery used as an expression"):
            await connection.execute(statement)
