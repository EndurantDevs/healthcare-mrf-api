# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep typed reads on independently verified heaps, excluding inherited rows."""

from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.selectable import Alias, FromGrouping, Join, Select, TableClause, TableSample


def _physical_froms(from_clause, names):
    if isinstance(from_clause, Join):
        yield from _physical_froms(from_clause.left, names)
        yield from _physical_froms(from_clause.right, names)
        return
    if isinstance(from_clause, FromGrouping):
        yield from _physical_froms(from_clause.element, names)
        return
    physical = from_clause
    while isinstance(physical, (Alias, TableSample)):
        physical = physical.element
    if isinstance(physical, TableClause) and (physical.schema, physical.name) in names:
        yield from_clause


def direct_heap_read_statement(statement, relations):
    """Add native ONLY hints without widening columns or replacing ORM entities.

    Callers authenticate exact pins and the original transaction independently.
    Fixed text fragments need explicit ONLY in their owning SQL builders.
    """
    if not relations or isinstance(statement, (str, TextClause)):
        return statement
    if not getattr(statement, "is_select", False):
        raise ValueError("direct_heap_scope_read_only")
    names = {(relation.schema_name, relation.table_name) for relation in relations}

    def replace_select(element):
        """Hint each actual physical FROM inside its own SELECT, including CTEs."""
        if not isinstance(element, Select):
            return None
        adapted = visitors.replacement_traverse(
            element, {}, lambda child: None if child is element else replace_select(child)
        )
        for from_clause in adapted.get_final_froms():
            for physical in _physical_froms(from_clause, names):
                adapted = adapted.with_hint(physical, "ONLY", "postgresql")
        return adapted

    return visitors.replacement_traverse(statement, {}, replace_select)
