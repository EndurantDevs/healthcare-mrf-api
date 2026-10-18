# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the forward migration through the original typed adoption helper."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlalchemy as sa

from db import migration_adoption

PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20261019030000_provider_directory_role_schema.py"


def _migration():
    specification = importlib.util.spec_from_file_location("role_npi_migration", PATH)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "existing", [None, sa.BigInteger(), sa.Integer(), sa.Text(), "required", "default", "computed"]
)
def test_role_npi_requires_typed_adoption(monkeypatch, existing):
    migration = _migration()
    operation = SimpleNamespace(add_column=Mock(), drop_column=Mock())
    column_by_field = {"name": "npi", "type": sa.BigInteger(), "nullable": True, "default": None}
    if isinstance(existing, sa.types.TypeEngine):
        column_by_field["type"] = existing
    elif existing == "required":
        column_by_field["nullable"] = False
    elif existing == "default":
        column_by_field["default"] = "1"
    elif existing == "computed":
        column_by_field["computed"] = {"persisted": True, "sqltext": "1"}
    inspector = SimpleNamespace(get_columns=lambda table, schema: [] if existing is None else [column_by_field])
    monkeypatch.setattr(
        migration_adoption, "_schema_inspector", lambda actual: inspector if actual is operation else None
    )
    monkeypatch.setattr(migration, "op", operation)
    indexes = Mock()
    monkeypatch.setattr(migration, "_ensure_index", indexes)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    if existing is not None and not isinstance(existing, sa.BigInteger):
        with pytest.raises(RuntimeError, match="existing_schema_"):
            migration.upgrade()
        operation.add_column.assert_not_called()
    else:
        migration.upgrade()
        if existing is None:
            table, added = operation.add_column.call_args.args
            assert table == "provider_directory_practitioner_role" and added.name == "npi"
            assert type(added.type) is sa.BigInteger and added.nullable and added.server_default is None
            assert operation.add_column.call_args.kwargs == {"schema": "fixture"}
        else:
            operation.add_column.assert_not_called()
    if existing is None or isinstance(existing, sa.BigInteger):
        assert [call.args for call in indexes.call_args_list] == [("fixture", name) for name in migration._COLUMNS]
    else:
        indexes.assert_not_called()
    migration.downgrade()
    operation.drop_column.assert_not_called()


def test_role_npi_extends_current_join():
    migration = _migration()
    assert migration.down_revision == "20261019020000_registry_continuation_join"
    assert migration.revision == "20261019030000_provider_directory_role_schema"


@pytest.mark.parametrize("native_type", ["json", "jsonb"])
@pytest.mark.parametrize("state", ["missing", "existing", "shape", "invalid"])
def test_role_indexes_require_exact_native_shape(monkeypatch, native_type, state):
    from contextlib import nullcontext

    from sqlalchemy.dialects.postgresql import dialect

    migration = _migration()
    bind = SimpleNamespace(
        dialect=dialect(),
        execute=Mock(return_value=SimpleNamespace(scalar_one=lambda: native_type)),
        exec_driver_sql=Mock(),
    )
    operation = SimpleNamespace(
        get_bind=lambda: bind, get_context=lambda: SimpleNamespace(autocommit_block=nullcontext)
    )
    temporary = SimpleNamespace(
        quoted_index='"temp_index"', quoted_table='"temp_table"', index_name="temp_index", table_name="temp_table"
    )
    shape = object()
    expected_by_field = {"shape": shape}
    current_by_field = {
        "shape": object() if state == "shape" else shape,
        "indisvalid": state != "invalid",
        "indisready": True,
        "indislive": True,
    }
    catalog = Mock(
        side_effect=[expected_by_field, None, current_by_field]
        if state == "missing"
        else [expected_by_field, current_by_field]
    )
    monkeypatch.setattr(migration, "op", operation)
    monkeypatch.setattr(migration, "_create_temporary_index_table", lambda *args: temporary)
    monkeypatch.setattr(migration, "_temporary_table_schema", lambda *args: "pg_temp")
    monkeypatch.setattr(migration, "_index_catalog_record", catalog)
    monkeypatch.setattr(migration, "_shape_from_catalog", lambda entry: entry["shape"])
    if state in {"shape", "invalid"}:
        with pytest.raises(RuntimeError, match="existing_schema_role_index_"):
            migration._ensure_index("fixture", "network_refs")
    else:
        migration._ensure_index("fixture", "network_refs")
    statements = [call.args[0] for call in bind.exec_driver_sql.call_args_list]
    created_statements = [statement for statement in statements if "CREATE INDEX CONCURRENTLY" in statement]
    assert len(created_statements) == int(state == "missing")
    expression = "network_refs" if native_type == "jsonb" else "(network_refs::jsonb)"
    assert statements[0].endswith("USING gin (" + expression + ")")
    assert 'DROP TABLE "temp_table"' in statements
