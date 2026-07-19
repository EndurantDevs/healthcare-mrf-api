# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from db.maintenance import (
    _build_index_sql,
    _compile_column_definition,
    _iter_index_specs,
    _managed_schemas,
    _normalize_index_columns,
    _should_manage_table_schema,
)
from sqlalchemy import Boolean, Column, Computed, MetaData, String, Table
from sqlalchemy.dialects import postgresql


class _DummyModel:
    __tablename__ = "plan_networktier"
    __my_additional_indexes__ = [
        {"index_elements": ("checksum_network"), "using": "gin"},
    ]


class _PrimaryKeyModel:
    __tablename__ = "openaddresses_geocode"
    __table__ = Table(
        __tablename__,
        MetaData(),
        Column("row_hash", String, primary_key=True),
    )
    __my_index_elements__ = ["row_hash"]


class _CoveringIndexModel:
    __tablename__ = "immutable_plan"
    __my_additional_indexes__ = [
        {
            "index_elements": ("dataset_id", "resource_id"),
            "include": ("plan_identifier",),
            "name": "immutable_plan_active_idx",
            "where": "plan_active",
        }
    ]


def test_normalize_index_columns_handles_single_string() -> None:
    assert _normalize_index_columns("checksum_network") == ("checksum_network",)


def test_iter_index_specs_normalizes_single_string_tuple_bug() -> None:
    specs = list(_iter_index_specs(_DummyModel))
    assert len(specs) == 1
    assert specs[0]["name"] == "plan_networktier_checksum_network_idx"
    assert specs[0]["columns"] == ("checksum_network",)


def test_iter_index_specs_skips_duplicate_primary_key_index() -> None:
    assert list(_iter_index_specs(_PrimaryKeyModel)) == []


def test_build_index_sql_does_not_split_column_name_into_characters() -> None:
    sql = _build_index_sql(
        "plan_networktier_checksum_network_idx",
        '"mrf".',
        "plan_networktier",
        {"columns": "checksum_network", "using": "gin"},
    )
    assert "(checksum_network)" in sql
    assert "c, h, e, c, k, s, u, m" not in sql


def test_build_index_sql_preserves_covering_columns() -> None:
    index_spec = list(_iter_index_specs(_CoveringIndexModel))[0]

    index_sql = _build_index_sql(
        index_spec["name"],
        '"mrf".',
        _CoveringIndexModel.__tablename__,
        index_spec,
    )

    assert "(dataset_id, resource_id)" in index_sql
    assert "INCLUDE (plan_identifier)" in index_sql
    assert index_sql.endswith(" WHERE plan_active;")


def test_compile_column_definition_supports_stored_generated_columns() -> None:
    generated_column = Column(
        "plan_active",
        Boolean(),
        Computed("payload_json ->> 'status' = 'active'", persisted=True),
    )

    column_sql = _compile_column_definition(
        generated_column,
        postgresql.dialect(),
    )

    assert column_sql == (
        '"plan_active" BOOLEAN GENERATED ALWAYS AS '
        "(payload_json ->> 'status' = 'active') STORED"
    )


def test_managed_schemas_defaults_to_db_schema(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("HLTHPRT_MANAGED_SCHEMAS", raising=False)
    assert _managed_schemas() == ("mrf",)


def test_should_manage_table_schema_skips_tiger_when_only_mrf_managed() -> None:
    managed_schemas = {"mrf"}
    assert _should_manage_table_schema("mrf", managed_schemas) is True
    assert _should_manage_table_schema("tiger", managed_schemas) is False
