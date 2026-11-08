# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded parity windows and nullable multiset comparison statements."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import BigInteger, LargeBinary, Numeric, String, cast, column, select, values
from sqlalchemy.dialects import postgresql

from process.custom_import import query_projection_verify as verify
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_query_projection_storage import _definition
from tests.test_custom_import_query_projection_values import _source


@pytest.mark.asyncio
@pytest.mark.parametrize("child", (False, True))
@pytest.mark.parametrize(("expected_count", "actual_count", "differs"), ((2, 2, False), (2, 1, False), (2, 2, True)))
async def test_window_requires_exact_values_and_counts(monkeypatch, child, expected_count, actual_count, differs):
    definition = _definition()
    source = _source(child=child)
    bindings = (
        {"quality": source.c.metric, "quantity": source.c.weight}
        if child
        else {"score": source.c.metric, "weight": source.c.weight}
    )
    validate = AsyncMock(return_value=(source, bindings))
    result = SimpleNamespace(expected_count=expected_count, actual_count=actual_count, differs=differs)
    execute = AsyncMock(return_value=SimpleNamespace(one=lambda: result))
    monkeypatch.setattr(verify, "_validated_source", validate)
    monkeypatch.setattr(verify, "_execute", execute)
    request, context, session = SimpleNamespace(definition=definition), object(), object()
    fields = tuple(field for field in definition.query.derived_fields if (field.collection is not None) == child)
    operation = verify._verify_window(
        session, request, context, SimpleNamespace(family_id=17), [1, 2], fields, child=child
    )
    if differs or expected_count != actual_count:
        with pytest.raises(CandidateRunnerError, match="persisted values differ"):
            await operation
    else:
        assert await operation == (2, 2)
    validate.assert_awaited_once_with(session, context, [1, 2], child=child)
    assert execute.await_count == 1


def _context_relation(context_rows, name, *, child=False):
    columns = [
        column("entity_binding_id", BigInteger),
        column("selected_value", BigInteger),
        column("field_1", Numeric),
    ]
    if child:
        columns.extend((column("canonical_child_key", String), column("child_key_sha256", LargeBinary)))
    native_values = values(*columns).data(context_rows).cte(name + "_values")
    return select(
        *(
            cast(field, Numeric(31, 12)).label(field.name) if field.name == "field_1" else field
            for field in native_values.c
        )
    ).cte(name)


def test_comparison_uses_both_native_multiset_directions_and_exact_child_text():
    expected = _context_relation([(1, 2024, None, '["a"]', b"a" * 32)], "expected_contexts", child=True)
    stored = _context_relation([(1, 2024, None, '["a"]', b"a" * 32)], "stored_contexts", child=True)
    statement = verify._comparison_statement(select(expected), stored, [1])
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert sql.count("EXCEPT ALL") == 2 and sql.count("AS MATERIALIZED") == 2
    assert sql.count('COLLATE "C"') >= 3
    assert "stored_contexts.entity_binding_id IN" in sql
    assert tuple(statement.selected_columns.keys()) == ("expected_count", "actual_count", "differs")
