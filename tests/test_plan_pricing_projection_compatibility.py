# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused fallback and interrupted-migration projection contracts."""

from __future__ import annotations

import importlib.util
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from api import plan_pricing_projection as projection
from .test_plan_pricing_projection import _selection, _Session


def _projection_migration():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260825150000_plan_pricing_card_projection.py"
    )
    module_spec = importlib.util.spec_from_file_location(
        "plan_pricing_card_projection_compatibility_migration",
        migration_path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@pytest.mark.asyncio
async def test_omitted_false_without_ready_projection_falls_back_to_legacy():
    session = _Session([])
    response = await projection.search_plan_pricing_projection(
        session,
        _selection(projection_id=None),
        {
            "include_providers": "false",
            "code_system": "CPT",
            "code": "27447",
            "zip5": "62401",
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response is None
    assert session.statements == []


def test_projection_migration_rebuilds_interrupted_invalid_zip_index(
    monkeypatch,
):
    migration = _projection_migration()
    operations = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(migration.op, "execute", lambda _statement: None)
    monkeypatch.setattr(
        migration.op,
        "get_context",
        lambda: SimpleNamespace(as_sql=False, autocommit_block=nullcontext),
    )
    monkeypatch.setattr(
        migration,
        "_zip_table_has_index_columns",
        lambda _schema: True,
    )
    monkeypatch.setattr(
        migration,
        "_zip_index_record",
        lambda _schema: {
            "table_schema": "mrf",
            "table_name": "geo_zip_lookup",
            "is_valid": False,
        },
    )
    monkeypatch.setattr(
        migration.op,
        "drop_index",
        lambda *args, **kwargs: operations.append(("drop", args, kwargs)),
    )
    monkeypatch.setattr(
        migration.op,
        "create_index",
        lambda *args, **kwargs: operations.append(("create", args, kwargs)),
    )

    migration.upgrade()

    assert [operation[0] for operation in operations] == ["drop", "create"]
    assert operations[0][1] == (migration.ZIP_INDEX_NAME,)
    assert operations[0][2] == {
        "table_name": migration.ZIP_TABLE_NAME,
        "schema": "mrf",
        "if_exists": True,
        "postgresql_concurrently": True,
    }
    assert operations[1][1] == (
        migration.ZIP_INDEX_NAME,
        migration.ZIP_TABLE_NAME,
        ["latitude", "longitude", "zip_code"],
    )
