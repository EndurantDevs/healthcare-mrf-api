# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Regression checks for bounded custom-import SQL composition."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    bindparam,
    select,
    text,
    tuple_,
)
from sqlalchemy.dialects import postgresql

from api.custom_import_provider_sql import compile_npi_entity_relation, merge_native_params

_SAMPLE = Table(
    "sample",
    MetaData(),
    Column("id", Integer),
    Column("code", String),
    Column("flag", Boolean),
    Column("amount", Numeric(8, 2)),
    Column("day", Date),
    Column("payload", JSON),
    Column("token", postgresql.UUID(as_uuid=True)),
)


def test_compiler_reuses_repeated_bind_identity_without_mutating_source():
    shared = bindparam("shared", "synthetic-value")
    statement = select(_SAMPLE.c.id).where(_SAMPLE.c.code == shared, _SAMPLE.c.code != shared)
    source_parameters_by_name = dict(statement.compile(dialect=postgresql.dialect(paramstyle="named")).params)

    relation = compile_npi_entity_relation(statement)

    assert shared.key == "shared"
    assert shared.value == "synthetic-value"
    assert statement.compile(dialect=postgresql.dialect(paramstyle="named")).params == source_parameters_by_name
    assert relation.sql.count(":__custom_import_0") == 2
    assert dict(relation.values) == {"__custom_import_0": "synthetic-value"}
    assert [parameter.key for parameter in relation.typed_binds] == ["__custom_import_0"]


def test_compiler_keeps_hostile_parameter_names_and_values_out_of_sql():
    hostile_name = "input; select"
    hostile_value = "' OR '1' = '1"
    statement = select(_SAMPLE.c.id).where(_SAMPLE.c.code == bindparam(hostile_name, hostile_value))

    relation = compile_npi_entity_relation(statement)

    assert hostile_name not in relation.sql
    assert hostile_value not in relation.sql
    assert dict(relation.values) == {"__custom_import_0": hostile_value}
    assert relation.typed_binds[0].type.python_type is str


def test_compiler_associates_expanded_names_with_types_including_empty_in():
    statement = select(_SAMPLE.c.id).where(_SAMPLE.c.id.in_(bindparam("ids", [3, 7], expanding=True)))

    relation = compile_npi_entity_relation(statement)

    assert ":__custom_import_0_1, :__custom_import_0_2" in relation.sql
    assert dict(relation.values) == {"__custom_import_0_1": 3, "__custom_import_0_2": 7}
    assert [parameter.key for parameter in relation.typed_binds] == [
        "__custom_import_0_1",
        "__custom_import_0_2",
    ]
    assert all(isinstance(parameter.type, Integer) for parameter in relation.typed_binds)

    empty_relation = compile_npi_entity_relation(
        select(_SAMPLE.c.id).where(_SAMPLE.c.id.in_(bindparam("ids", [], expanding=True)))
    )

    assert "WHERE 1!=1" in empty_relation.sql
    assert "POSTCOMPILE" not in empty_relation.sql
    assert dict(empty_relation.values) == {}
    assert empty_relation.typed_binds == ()


def test_compiler_preserves_bool_numeric_uuid_date_and_json_types():
    token = UUID("12345678-1234-5678-1234-567812345678")
    payload = {"kind": "synthetic"}
    statement = select(_SAMPLE.c.id).where(
        _SAMPLE.c.flag == bindparam("flag", True, type_=Boolean()),
        _SAMPLE.c.amount == bindparam("amount", Decimal("12.34"), type_=Numeric(8, 2)),
        _SAMPLE.c.token == bindparam("token", token, type_=postgresql.UUID(as_uuid=True)),
        _SAMPLE.c.day == bindparam("day", date(2026, 1, 2), type_=Date()),
        _SAMPLE.c.payload == bindparam("payload", payload, type_=JSON()),
    )

    relation = compile_npi_entity_relation(statement)

    assert list(relation.values.values()) == [True, Decimal("12.34"), token, date(2026, 1, 2), payload]
    assert isinstance(relation.typed_binds[0].type, Boolean)
    assert isinstance(relation.typed_binds[1].type, Numeric)
    assert isinstance(relation.typed_binds[2].type, postgresql.UUID)
    assert isinstance(relation.typed_binds[3].type, Date)
    assert isinstance(relation.typed_binds[4].type, JSON)


def test_merge_native_params_rejects_collisions_without_mutating_inputs():
    relation = compile_npi_entity_relation(select(_SAMPLE.c.id).where(_SAMPLE.c.id == bindparam("id", 3)))
    native_parameters_by_name = {"page_size": 2}

    merged = merge_native_params(native_parameters_by_name, relation)
    native_statement = text(f"SELECT relation.id FROM ({relation.sql}) AS relation WHERE :page_size > 0").bindparams(
        *relation.typed_binds
    )
    native_compiled = native_statement.compile(dialect=postgresql.dialect(paramstyle="named"))

    assert native_parameters_by_name == {"page_size": 2}
    assert merged == {"page_size": 2, "__custom_import_0": 3}
    assert native_compiled.params["__custom_import_0"] == 3
    assert isinstance(native_compiled.binds["__custom_import_0"].type, Integer)
    with pytest.raises(ValueError, match="parameters collide"):
        merge_native_params({"__custom_import_0": 4}, relation)


def test_compiler_rejects_tuple_expansion_and_literal_execution():
    tuple_statement = select(_SAMPLE.c.id).where(
        tuple_(_SAMPLE.c.id, _SAMPLE.c.code).in_(bindparam("pairs", [(3, "x")], expanding=True))
    )
    literal_statement = select(_SAMPLE.c.id).where(_SAMPLE.c.id == bindparam("id", 3, literal_execute=True))

    with pytest.raises(ValueError, match="tuple-expanding"):
        compile_npi_entity_relation(tuple_statement)
    with pytest.raises(ValueError, match="literal-execute"):
        compile_npi_entity_relation(literal_statement)
