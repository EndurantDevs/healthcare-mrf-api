# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from db.maintenance import (
    _build_index_sql,
    _iter_index_specs,
    _managed_schemas,
    _normalize_index_columns,
    _should_manage_table_schema,
)


class _DummyModel:
    __tablename__ = "plan_networktier"
    __my_additional_indexes__ = [
        {"index_elements": ("checksum_network"), "using": "gin"},
    ]


def test_normalize_index_columns_handles_single_string() -> None:
    assert _normalize_index_columns("checksum_network") == ("checksum_network",)


def test_iter_index_specs_normalizes_single_string_tuple_bug() -> None:
    specs = list(_iter_index_specs(_DummyModel))
    assert len(specs) == 1
    assert specs[0]["name"] == "plan_networktier_checksum_network_idx"
    assert specs[0]["columns"] == ("checksum_network",)


def test_build_index_sql_does_not_split_column_name_into_characters() -> None:
    sql = _build_index_sql(
        "plan_networktier_checksum_network_idx",
        '"mrf".',
        "plan_networktier",
        {"columns": "checksum_network", "using": "gin"},
    )
    assert "(checksum_network)" in sql
    assert "c, h, e, c, k, s, u, m" not in sql


def test_managed_schemas_defaults_to_db_schema(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("HLTHPRT_MANAGED_SCHEMAS", raising=False)
    assert _managed_schemas() == ("mrf",)


def test_should_manage_table_schema_skips_tiger_when_only_mrf_managed() -> None:
    managed = {"mrf"}
    assert _should_manage_table_schema("mrf", managed) is True
    assert _should_manage_table_schema("tiger", managed) is False
