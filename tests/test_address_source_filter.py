# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import Integer, bindparam, column, select, table, text
from sqlalchemy.dialects import postgresql

from api.address_source_filter import address_source_filter, address_source_filter_sql, parse_min_address_sources
from api.custom_import_provider_sql import compile_npi_entity_relation


@pytest.mark.parametrize("value,expected", [("0", 0), ("3", 3), ("03", 3), (3, 3), ("2147483647", 2147483647)])
def test_threshold_normalizes_bounded_integers(value, expected):
    assert parse_min_address_sources({"min_address_sources": value}) == expected
    assert parse_min_address_sources({}) == 0


@pytest.mark.parametrize("value", [None, "", "-1", "+3", " 3", "3.0", "٣", True, 3.0, 2147483648, "1" * 5000])
def test_threshold_rejects_invalid_values(value):
    with pytest.raises(InvalidUsage):
        parse_min_address_sources({"min_address_sources": value})


def test_threshold_rejects_duplicates():
    with pytest.raises(InvalidUsage):
        parse_min_address_sources(RequestParameters({"min_address_sources": ["2", "3"]}))


def test_predicates_use_stored_address_count_and_exclude_unknown_evidence():
    address = table("address", column("source_count"))
    legacy = table("legacy", column("npi"))
    assert str(address_source_filter(address, 3)) == "address.source_count >= 3"
    assert address_source_filter(address, 3).compile().params == {}
    assert str(address_source_filter(legacy, 3)) == "false"
    assert str(address_source_filter(legacy, 0)) == "true"
    assert address_source_filter_sql("a", 3) == "a.source_count >= 3"
    assert address_source_filter_sql("a", 3, has_source_count=False) == "FALSE"
    assert address_source_filter_sql("a", 0, has_source_count=False) == "TRUE"


@pytest.mark.parametrize("minimum,normalized", [(0, 0), ("00", 0), ("03", 3), (3, 3), (2147483647, 2147483647)])
def test_sql_composition_normalizes_integer_constants(minimum, normalized):
    address = table("address", column("npi", Integer), column("source_count", Integer))
    predicate = address_source_filter(address, minimum)
    expected = f"address.source_count >= {normalized}" if normalized else "true"
    assert str(predicate) == expected and predicate.compile().params == {}
    assert address_source_filter_sql("address", minimum) == (expected if normalized else "TRUE")
    if normalized:
        assert isinstance(predicate.right.type, Integer)


@pytest.mark.parametrize("minimum", [False, None, "", -1, 2147483648, "1" * 11, "3 OR TRUE", "3; SELECT 1", "٣", 3.0])
@pytest.mark.parametrize("known_column", [False, True])
def test_sql_composition_rejects_invalid_thresholds_before_rendering(minimum, known_column):
    address = table("address", column("source_count") if known_column else column("npi"))
    with pytest.raises(InvalidUsage):
        address_source_filter_sql("address", minimum, has_source_count=known_column)
    with pytest.raises(InvalidUsage):
        address_source_filter(address, minimum)


def test_imported_relation_compiler_keeps_threshold_constant_and_other_values_bound():
    address = table("address", column("npi", Integer), column("source_count", Integer))
    statement = select(address.c.npi).where(
        address_source_filter(address, "03"), address.c.npi.in_(bindparam("npis", [101, 202], expanding=True))
    )
    relation = compile_npi_entity_relation(statement)
    assert "address.source_count >= 3" in relation.sql
    assert list(relation.values.values()) == [101, 202]
    assert all(
        isinstance(parameter.type, Integer) and not parameter.literal_execute for parameter in relation.typed_binds
    )
    compiled = text(relation.sql).bindparams(*relation.typed_binds).compile(dialect=postgresql.asyncpg.dialect())
    assert list(compiled.params.values()) == [101, 202]
    assert "address.source_count >= 3" in str(compiled)
