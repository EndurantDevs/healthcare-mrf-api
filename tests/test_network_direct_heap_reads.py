# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ONLY hints preserve typed query shape, parameters and exact FROM scope."""

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, bindparam, exists, func, select, text, union_all
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from api.network_direct_heap_reads import direct_heap_read_statement
from process.network_compact_layout import CompactAddressRelation

PIN = CompactAddressRelation("held", "provider", 23)
TABLE = Table("provider", MetaData(), Column("id", Integer), Column("private", Integer), schema="held")


def _sql(statement):
    return str(statement.compile(dialect=dialect()))


@pytest.mark.parametrize(
    "kind", ["table", "alias", "chained-alias", "sample", "self-join", "exists", "union", "cte", "lateral"]
)
def test_every_physical_from_is_hinted_in_its_own_select(kind):
    alias = TABLE.alias("other")
    inner = select(TABLE.c.id).where(TABLE.c.id == bindparam("selected_id", 7))
    statement_by_kind = {
        "table": inner,
        "alias": select(alias.c.id),
        "chained-alias": select(alias.alias("outer_alias").c.id),
        "sample": select(TABLE.tablesample(func.system(100), name="sampled").c.id),
        "self-join": select(TABLE.c.id, alias.c.id).select_from(TABLE.join(alias, TABLE.c.id == alias.c.id)),
        "exists": inner.where(exists(select(alias.c.id).where(alias.c.id == TABLE.c.id).correlate(TABLE))),
        "union": union_all(inner, select(alias.c.id)),
        "cte": select(inner.cte("selected")),
        "lateral": select(inner.lateral("selected")),
    }
    original = statement_by_kind[kind]
    adapted = direct_heap_read_statement(original, (PIN,))
    sql = _sql(adapted)
    expected = 2 if kind in {"self-join", "exists", "union"} else 1
    assert sql.count("ONLY held.provider") == expected
    assert sql == _sql(direct_heap_read_statement(adapted, (PIN,)))
    assert adapted.compile().params == original.compile().params
    assert list(adapted.selected_columns.keys()) == list(original.selected_columns.keys())
    assert "private" not in sql
    assert "ONLY" not in _sql(original)


def test_schema_and_table_both_match_without_search_path_inference():
    other = Table("provider", MetaData(), Column("id", Integer), schema="other")
    unqualified = Table("provider", MetaData(), Column("id", Integer))
    statement = union_all(select(TABLE.c.id), select(other.c.id), select(unqualified.c.id))
    sql = _sql(direct_heap_read_statement(statement, (PIN,)))
    assert sql.count("ONLY") == 1
    assert "FROM other.provider" in sql and "FROM provider" in sql


def test_empty_scope_and_fixed_text_remain_with_their_owning_builders():
    statement = select(TABLE.c.id)
    assert direct_heap_read_statement(statement, ()) is statement
    raw = text("SELECT id FROM ONLY held.provider")
    assert direct_heap_read_statement(raw, (PIN,)) is raw


def test_typed_mutation_is_refused_in_a_direct_heap_read_scope():
    with pytest.raises(ValueError, match="read_only"):
        direct_heap_read_statement(TABLE.delete(), (PIN,))


def test_orm_entities_are_retained_without_replacing_their_columns():
    class ReadModelBase(DeclarativeBase):
        """Keep mapped identity in this test's isolated registry."""

    class Provider(ReadModelBase):
        __tablename__ = "provider"
        __table_args__ = {"schema": "held"}
        id: Mapped[int] = mapped_column(primary_key=True)

    statement = select(Provider).where(Provider.id == bindparam("selected_id", 7))
    adapted = direct_heap_read_statement(statement, (PIN,))
    assert adapted.column_descriptions == statement.column_descriptions
    assert adapted.compile().params == statement.compile().params
    assert "FROM ONLY held.provider" in _sql(adapted)


def test_address_scope_markers_survive_composition_with_direct_heap_hints():
    from api.network_address_scope import network_address_read_scope, scoped_address_statement
    from api.network_read_execution import network_read_relations
    from db.models import EntityAddressUnified
    from tests.test_network_read_execution import _manifest

    addresses = EntityAddressUnified.__table__
    statement = select(TABLE.c.id).where(exists(select(addresses.c.location_key)))
    with network_address_read_scope(_manifest(), (42,)):
        scoped = scoped_address_statement(statement)
        pins = network_read_relations(scoped)
        adapted = direct_heap_read_statement(scoped, (PIN,))
        assert network_read_relations(adapted) == pins
        assert "FROM ONLY held.provider" in _sql(adapted)


@pytest.mark.parametrize("refuse", [False, True])
async def test_buffered_execution_uses_only_hints_before_existing_identity_guard(refuse):
    from sanic.exceptions import ServiceUnavailable

    from api import network_read_execution as execution
    from tests.test_network_bound_names import _PIN, _installable_session

    session, driver, buffered = _installable_session()
    await execution.install_network_read_inventory(session, (_PIN,), only_relations=(_PIN,))
    physical = Table(_PIN.table_name, MetaData(), Column("id", Integer), schema=_PIN.schema_name)

    async def execute(statement, _parameters):
        assert f"FROM ONLY {_PIN.schema_name}.{_PIN.table_name}" in _sql(statement)
        driver.fetchrow.return_value["valid"] = not refuse
        return buffered

    session.execute.side_effect = execute
    operation = execution.execute_network_read(session, select(physical.c.id))
    if refuse:
        with pytest.raises(ServiceUnavailable):
            await operation
        buffered.close.assert_called_once()
    else:
        assert await operation is buffered
