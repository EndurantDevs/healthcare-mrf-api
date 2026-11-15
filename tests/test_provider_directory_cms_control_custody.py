# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed CMS model layouts remain compatible with native DML."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_control_custody as custody


def _columns(model):
    return [
        dict(name=name, type=kind, required=required, identity="", generated="")
        for name, (kind, required) in custody._model_columns(model).items()
    ]


def _indexes(model):
    from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

    index_rows = []
    for number, constraint in enumerate(model.__table__.constraints):
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
            index_rows.append(
                dict(
                    name=str(number),
                    method="btree",
                    unique=True,
                    primary=isinstance(constraint, PrimaryKeyConstraint),
                    ready=True,
                    plain=True,
                    columns=[column.name for column in constraint.columns],
                )
            )
    for specification in getattr(model, "__my_additional_indexes__", ()):
        index_rows.append(
            dict(
                name=specification["name"],
                method="btree",
                unique=False,
                primary=False,
                ready=True,
                plain=True,
                columns=list(specification["index_elements"]),
            )
        )
    return index_rows


@pytest.mark.parametrize("model", list(custody._models().values()), ids=lambda model: model.__name__)
@pytest.mark.asyncio
async def test_installed_managed_mutable_models_require_no_schema_ddl(model):
    assert model.__runtime_schema_sync__ is False
    assert not model.__table__.info.get("managed_immutable_storage")
    database = SimpleNamespace(all=AsyncMock(side_effect=[_columns(model), _indexes(model)]), status=AsyncMock())
    await custody.require_cms_control_model_layout(database, model, "snapshot_schema")
    assert database.all.await_count == 2
    database.status.assert_not_awaited()


@pytest.mark.parametrize(
    "drift", ["missing", "extra", "type", "nullability", "identity", "generated", "index", "wrong_index"]
)
@pytest.mark.asyncio
async def test_runtime_drift_refuses_instead_of_altering_tables_or_indexes(drift):
    model = custody._models()[custody.CONTROL_ROOTS[0]]
    columns, indexes = _columns(model), _indexes(model)
    if drift == "missing":
        columns.pop()
    elif drift == "extra":
        columns.append({**columns[-1], "name": "unrelated"})
    elif drift in {"type", "nullability", "identity", "generated"}:
        columns[0][{"nullability": "required"}.get(drift, drift)] = False if drift == "nullability" else "changed"
    elif drift == "index":
        indexes.clear()
    else:
        indexes[-1]["columns"] = ["source_id"]
    database = SimpleNamespace(all=AsyncMock(side_effect=[columns, indexes]), status=AsyncMock())
    with pytest.raises(ValueError, match="administrator_(shape|index)_required"):
        await custody.require_cms_control_model_layout(database, model, "snapshot_schema")
    database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_fhir_runtime_guard_is_read_only_and_existing_upsert_remains_usable(monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")

    model = custody._models()[custody.CONTROL_ROOTS[0]]
    check = AsyncMock()
    monkeypatch.setattr(custody, "require_cms_control_model_layout", check)
    monkeypatch.setattr(directory.db, "status", AsyncMock(side_effect=AssertionError("runtime DDL")))
    await directory._ensure_provider_directory_model_columns(model, "snapshot_schema")
    check.assert_awaited_once_with(directory.db, model, "snapshot_schema")
    session = SimpleNamespace(execute=AsyncMock())
    assert (
        await directory._upsert_rows_values(
            model,
            [{"source_id": "source", "org_name": "Example"}],
            ["source_id", "org_name"],
            ["source_id"],
            skip_unchanged=True,
            transaction_session=session,
        )
        == 1
    )
    assert "ON CONFLICT" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_fhir_missing_managed_source_refuses_before_create_schema(monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")
    from process import provider_directory_generic_source_adapters as adapters

    monkeypatch.setattr(adapters, "require_generic_source_layout", AsyncMock(return_value=None))
    monkeypatch.setattr(directory, "_is_table_present", AsyncMock(return_value=False))
    monkeypatch.setattr(directory.db, "status", AsyncMock())
    with pytest.raises(RuntimeError, match="administrator_cut_required"):
        await directory._ensure_provider_directory_tables()
    directory.db.status.assert_not_awaited()
