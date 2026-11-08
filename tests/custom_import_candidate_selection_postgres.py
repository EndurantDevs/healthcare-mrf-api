# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep scalar failure semantics while sharing bounded default selection."""

from dataclasses import replace

import pytest
from sqlalchemy import BigInteger, case, column, func, literal, select, text, values
from sqlalchemy.exc import DBAPIError

from process.custom_import import grouped_query, grouped_read, read_core
from tests import custom_import_grouped_child_support as fixture
from tests import test_custom_import_grouped_child_read_postgres as native
from tests.test_custom_import_grouped_child_read_postgres import values_connection as values_connection


@pytest.mark.asyncio
@pytest.mark.parametrize("helper", ["present", "missing", "duplicate", "explicit"])
async def test_candidate_default_is_shared_without_losing_scalar_guards(helper, values_connection, monkeypatch):
    context, families = native._candidate_context(fixture.context(), False)
    plan = grouped_read.normalize_plan(context, fixture.query(), read_core.ExtensionReadScope("synthetic"))
    if helper == "explicit":
        plan = replace(plan, selected_value=2024)
    candidates = values(column("entity_binding_id", BigInteger)).data([(200,)]).cte("bounded_candidates")
    await values_connection.execute(text("CREATE TEMP SEQUENCE candidate_selection_calls"))

    def selected_value_relation(*_args):
        rows = [(201,)] if helper == "missing" else [(200,), (200,)] if helper == "duplicate" else [(200,)]
        owners = values(column("entity_binding_id", BigInteger)).data(rows).alias("helper_owners")
        value = case((func.nextval("pg_temp.candidate_selection_calls") > 0, 2024), else_=None)
        return select(owners.c.entity_binding_id, value.label("selected_value")).subquery("selected_entity_value")

    def selected_families(_context, _plan, **options):
        value = options["default_selected_value"]
        if value is None and _plan.selected_value is None:
            latest = selected_value_relation()
            winner = context.model(read_core.CustomImportWinner)
            value = (
                select(latest.c.selected_value)
                .where(latest.c.entity_binding_id == winner.entity_binding_id)
                .correlate(winner)
                .scalar_subquery()
            )
        return families.where(literal(2024) == value) if value is not None else families

    monkeypatch.setattr(grouped_read, "_selected_value_relation", selected_value_relation)
    monkeypatch.setattr(grouped_read, "selected_family_statement", selected_families)
    selected = grouped_query.selected_identities(context, plan, entity_binding_ids=candidates)
    statement = select(selected.c.family_revision_id)
    if helper == "duplicate":
        with pytest.raises(DBAPIError, match="more than one row"):
            await values_connection.execute(statement)
        return
    assert set((await values_connection.execute(statement)).scalars()) == (set() if helper == "missing" else {10, 11})
    last_value, called = (
        await values_connection.execute(text("SELECT last_value, is_called FROM pg_temp.candidate_selection_calls"))
    ).one()
    assert last_value == 1
    assert called == (helper == "present")
