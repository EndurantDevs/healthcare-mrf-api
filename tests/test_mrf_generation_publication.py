# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from db.models import ImportLog
from process import initial
from process.reference_family_archive import reference_family_spec


@dataclass
class _StageModel:
    __main_table__: str
    __tablename__: str


class _Transaction:
    def __init__(self, database):
        self.database = database

    async def __aenter__(self):
        assert not self.database.in_transaction
        self.database.in_transaction = True
        return self.database

    async def __aexit__(self, exception_type, _exception, _traceback):
        self.database.in_transaction = False
        self.database.exit_exception = exception_type


class _Database:
    def __init__(self):
        self.in_transaction = False
        self.exit_exception = None
        self.statements = []

    def transaction(self):
        return _Transaction(self)

    async def status(self, statement):
        assert self.in_transaction
        self.statements.append(statement)


def _stage_model(model_type, import_date, *, schema_override):
    assert import_date == "synthetic-generation"
    assert schema_override == "mrf_test"
    return _StageModel(model_type.__main_table__, f"{model_type.__main_table__}_stage")


@pytest.mark.asyncio
async def test_mrf_finalizer_records_exact_family_generation_in_rotation_transaction(monkeypatch):
    database = _Database()
    observed_scopes = []

    async def prepare(session, **scope):
        assert session is database and database.in_transaction and not database.statements
        assert scope == dict(
            ledger_schema="mrf_test",
            relations=tuple(
                (model.__main_table__, model.__main_table__ + "_stage") for model in initial._MRF_PUBLICATION_MODELS
            ),
        )

    prepared = AsyncMock(side_effect=prepare)
    monkeypatch.setattr(initial, "prepare_local_native_input_publication", prepared)

    async def generation_writer(session, *, importer_id, schema_name):
        assert session is database
        assert database.in_transaction
        observed_scopes.append((importer_id, schema_name))

    monkeypatch.setattr(initial, "db", database)
    monkeypatch.setattr(initial, "make_class", _stage_model)
    monkeypatch.setattr(initial, "publish_local_reference_family_generation", generation_writer)

    spec = reference_family_spec("mrf")
    assert ImportLog in initial._MRF_PUBLICATION_MODELS
    assert ImportLog not in spec.model_types
    assert tuple(model for model in initial._MRF_PUBLICATION_MODELS if model is not ImportLog) == spec.model_types[:-1]
    assert spec.model_types[-1].__tablename__ == "plan_search_summary"
    await initial._publish_mrf_table_generation("synthetic-generation", "mrf_test")

    prepared.assert_awaited_once()
    assert observed_scopes == [("mrf", "mrf_test"), ("mrf-address", "mrf_test")]
    assert database.exit_exception is None
    for table_name in (model.__tablename__ for model in initial._MRF_PUBLICATION_MODELS):
        assert f"ALTER TABLE IF EXISTS mrf_test.{table_name}_stage RENAME TO {table_name};" in database.statements


@pytest.mark.asyncio
async def test_mrf_finalizer_generation_failure_rolls_back_rotation(monkeypatch):
    database = _Database()
    observed_importers = []
    monkeypatch.setattr(initial, "prepare_local_native_input_publication", AsyncMock())

    async def generation_writer(_session, *, importer_id, **_scope):
        assert database.in_transaction
        observed_importers.append(importer_id)
        if importer_id == "mrf-address":
            raise RuntimeError("generation write failed")

    monkeypatch.setattr(initial, "db", database)
    monkeypatch.setattr(initial, "make_class", _stage_model)
    monkeypatch.setattr(initial, "publish_local_reference_family_generation", generation_writer)

    with pytest.raises(RuntimeError, match="generation write failed"):
        await initial._publish_mrf_table_generation("synthetic-generation", "mrf_test")

    assert observed_importers == ["mrf", "mrf-address"]
    assert database.exit_exception is RuntimeError


@pytest.mark.asyncio
async def test_mrf_enrollment_failure_prevents_any_rotation(monkeypatch):
    database = _Database()
    monkeypatch.setattr(initial, "db", database)
    monkeypatch.setattr(initial, "make_class", _stage_model)
    monkeypatch.setattr(
        initial,
        "prepare_local_native_input_publication",
        AsyncMock(side_effect=PermissionError("native INSERT denied")),
    )
    with pytest.raises(PermissionError, match="native INSERT denied"):
        await initial._publish_mrf_table_generation("synthetic-generation", "mrf_test")
    assert database.statements == [] and database.exit_exception is PermissionError
