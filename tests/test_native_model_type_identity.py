# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Relocating an isolated model must preserve its declared shared native type."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy import Column, Enum, MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import AddressArchiveV2
from process import entity_address_native_publication as publication
from process import npi_result_archive as npi
from process import reference_family_archive as archive


@pytest.mark.parametrize("schema", [None, "synthetic_stage"])
@pytest.mark.parametrize("model", [AddressArchiveV2, archive.canonical_contribution_model("npi")])
def test_model_clone_preserves_shared_enum_and_original_metadata(schema, model):
    source = model.__table__
    original = str(CreateTable(source).compile(dialect=postgresql.dialect()))
    declared = source.c.geo_source.type
    cloned = archive._clone_model_table(source, MetaData(), schema=schema)
    assert cloned.schema == schema
    assert cloned.c.geo_source.type is not declared
    assert cloned.c.geo_source.type.schema == declared.schema
    assert cloned.c.geo_source.type.name == declared.name
    assert cloned.c.geo_source.type.enums == declared.enums
    absent_policy = object()
    assert getattr(cloned.c.geo_source.type, "create_type", absent_policy) is getattr(
        declared, "create_type", absent_policy
    )
    assert f"geo_source {declared.schema}.{declared.name}" in str(
        CreateTable(cloned).compile(dialect=postgresql.dialect())
    )
    assert str(CreateTable(source).compile(dialect=postgresql.dialect())) == original


def test_unqualified_model_enum_keeps_normal_target_schema_copy_semantics():
    source = Table("synthetic", MetaData(), Column("choice", Enum("one", "two", name="local_choice")))
    ordinary = source.to_metadata(MetaData(), schema="synthetic_stage")
    cloned = archive._clone_model_table(source, MetaData(), schema="synthetic_stage")
    assert source.c.choice.type.schema is None
    assert cloned.c.choice.type.schema == ordinary.c.choice.type.schema


@pytest.mark.asyncio
@pytest.mark.parametrize("producer", ["reference", "npi"])
async def test_real_heap_compilers_keep_declared_shared_type(producer, monkeypatch):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    if producer == "reference":
        await archive._create_model_heaps(
            session,
            archive.ReferenceFamilySpec("synthetic", (AddressArchiveV2,)),
            "synthetic_stage",
            create_indexes=False,
        )
    else:
        monkeypatch.setattr(npi, "capture_npi_stage_ownership", AsyncMock())
        await npi.precreate_npi_restore(session, dataset_id=UUID(int=1), canonical=True)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    declared = AddressArchiveV2.__table__.c.geo_source.type
    assert any(f"geo_source {declared.schema}.{declared.name}" in statement for statement in statements)
    assert not any("CREATE TYPE" in statement for statement in statements)


def test_real_temp_model_witness_keeps_the_same_native_type():
    table = publication._model_catalog_table(archive.canonical_contribution_model("npi"))
    declared = AddressArchiveV2.__table__.c.geo_source.type
    statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "CREATE TEMPORARY TABLE" in statement and "ON COMMIT DROP" in statement
    assert f"geo_source {declared.schema}.{declared.name}" in statement
