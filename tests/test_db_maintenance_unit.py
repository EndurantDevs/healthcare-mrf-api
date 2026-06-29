# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from db.maintenance import (
    _build_index_sql,
    _iter_index_specs,
    _managed_schemas,
    _normalize_index_columns,
    _retire_obsolete_objects,
    _should_manage_table_schema,
)
from sqlalchemy import Column, MetaData, String, Table


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


def test_managed_schemas_defaults_to_db_schema(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("HLTHPRT_MANAGED_SCHEMAS", raising=False)
    assert _managed_schemas() == ("mrf",)


def test_should_manage_table_schema_skips_tiger_when_only_mrf_managed() -> None:
    managed = {"mrf"}
    assert _should_manage_table_schema("mrf", managed) is True
    assert _should_manage_table_schema("tiger", managed) is False


def test_retire_obsolete_objects_drops_only_explicit_ptg_cache() -> None:
    class Conn:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement) -> None:
            self.statements.append(str(statement))

    class Inspector:
        def has_table(self, table_name: str, *, schema: str | None = None) -> bool:
            assert schema == "mrf"
            return table_name in {"ptg_address", "entity_address_unified"}

        def get_columns(self, table_name: str, *, schema: str | None = None) -> list[dict[str, str]]:
            assert table_name == "entity_address_unified"
            assert schema == "mrf"
            return [{"name": "location_key"}, {"name": "ptg_address_version"}]

    conn = Conn()
    results = {"retired": []}

    _retire_obsolete_objects(conn, Inspector(), "mrf", results)

    assert conn.statements == [
        'DROP TABLE IF EXISTS "mrf"."ptg_address" CASCADE',
        'ALTER TABLE "mrf"."entity_address_unified" DROP COLUMN IF EXISTS "ptg_address_version"',
    ]
    assert results["retired"] == [
        "mrf.ptg_address",
        "mrf.entity_address_unified.ptg_address_version",
    ]
