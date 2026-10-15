# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve independent default selection before expanding bounded families."""

import pytest
from sqlalchemy import BigInteger, column, values
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import visitors
from sqlalchemy.sql.selectable import CTE, ScalarSelect

from process.custom_import import grouped_query, grouped_read, read_core
from tests import custom_import_grouped_child_support as fixture


@pytest.mark.parametrize("entity_values", [None, ("1234567893",)])
def test_bounded_candidates_share_guarded_default_selection(entity_values):
    context = fixture.context()
    plan = grouped_read.normalize_plan(context, fixture.query(), read_core.ExtensionReadScope("synthetic"))
    candidates = values(column("entity_binding_id", BigInteger)).data([(11,), (12,)]).cte("bounded_candidates")
    statement = grouped_query.selected_identities(context, plan, entity_values, entity_binding_ids=candidates)
    relations_by_id = {
        id(node): node
        for node in visitors.iterate(statement)
        if isinstance(node, CTE) and node.name == "candidate_selected_values"
    }
    assert len(relations_by_id) == 1
    selected = next(iter(relations_by_id.values()))
    assert selected.element.get_final_froms() == [candidates]
    lookups_by_id = {
        id(node): node
        for node in visitors.iterate(selected.element)
        if isinstance(node, ScalarSelect) and node._correlate
    }
    assert len(lookups_by_id) == 1
    lookup = next(iter(lookups_by_id.values()))
    assert lookup._correlate == (candidates,)
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "candidate_selected_values AS MATERIALIZED" in sql
    assert "selected_entity_value.entity_binding_id = bounded_candidates.entity_binding_id" in sql
    assert "root_slot_6.integer_value = candidate_selected_values.selected_value" in sql
    assert "selected_entity_value AS MATERIALIZED" not in sql
    assert "LIMIT" not in str(lookup)
