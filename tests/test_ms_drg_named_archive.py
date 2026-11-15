# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable MS-DRG stage and stored-edition compatibility boundaries."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import ms_drg_result_archive as archive
from process import ms_drg_result_generation as generation
from process import scoped_catalog_binding as binding


def receipt():
    return {
        "contract": generation.CONTRACT,
        "content_sha256": "a" * 64,
        "tables": [
            {
                "table": name,
                "sources": list(sources),
                "relation_oid": 40 + index,
                "row_count": 2,
                "row_sha256": "b" * 64,
                "schema_sha256": "c" * 64,
            }
            for index, (name, sources) in enumerate(generation._SCOPES)
        ],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_serving_receipt_checks_complete_content_without_local_identifiers(monkeypatch, changed):
    saved = receipt()
    before = deepcopy(saved)
    current_by_field = {
        **saved,
        "tables": [{key: value for key, value in table.items() if key != "relation_oid"} for table in saved["tables"]],
    }
    if changed:
        current_by_field["tables"][1]["row_count"] += 1
    generation_by_field = {"origin_generation": 3, "receipt": saved}
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: generation_by_field))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    monkeypatch.setattr(binding, "pin_catalog_source", AsyncMock())
    monkeypatch.setattr(generation, "capture_result", AsyncMock(return_value=current_by_field))
    if changed:
        with pytest.raises(RuntimeError, match="serving result changed"):
            await generation.read_current_generation(session, "serving")
    else:
        assert await generation.read_current_generation(session, "serving") == generation_by_field
    assert saved == before


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_stage_catalog_work_resolves_names_without_trusting_saved_ids(monkeypatch, legacy):
    dataset_id = uuid4()
    schema = archive.stage_schema(dataset_id)
    names = archive.TABLES + archive.PREDECESSORS
    saved_indexes = tuple(range(910, 910 + len(names))) if legacy else tuple(range(40, 40 + len(names)))
    stage_by_field = {
        "dataset_id": str(dataset_id),
        "schema_name": schema,
        "schema_oid": 900 if legacy else 20,
        "relation_oids": saved_indexes,
    }
    before = deepcopy(stage_by_field)
    actual_indexes = tuple(range(40, 40 + len(names)))
    resolve = AsyncMock(return_value=actual_indexes)
    monkeypatch.setattr(archive, "_oids", resolve)
    session = SimpleNamespace(scalar=AsyncMock(return_value=20))
    live = await archive._named_stage(session, stage_by_field, receiving=True)
    resolve.assert_awaited_once_with(session, schema, names)
    assert live["schema_oid"] == 20 and live["relation_oids"] == actual_indexes
    assert stage_by_field == before


@pytest.mark.asyncio
async def test_stage_refuses_a_different_declared_schema_before_catalog_access(monkeypatch):
    dataset_id = uuid4()
    stage_by_field = {
        "dataset_id": str(dataset_id),
        "schema_name": "other",
        "schema_oid": 20,
        "relation_oids": tuple(range(40, 40 + len(archive.TABLES))),
    }
    resolve = AsyncMock()
    monkeypatch.setattr(archive, "_oids", resolve)
    session = SimpleNamespace(scalar=AsyncMock())
    with pytest.raises(archive.MsDrgArchiveError, match="logical name differs"):
        await archive._named_stage(session, stage_by_field, receiving=False)
    resolve.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("receiving", [False, True])
async def test_export_and_restore_stages_round_trip_through_the_saved_stage_reader(monkeypatch, receiving):
    """Both returned stage paths keep the archive adapter's fields, and the reader resolves them by name."""
    monkeypatch.setattr(archive.native, "_create_model_heaps", AsyncMock())
    monkeypatch.setattr(archive, "match_catalog_text_columns", AsyncMock())
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=20))
    stage = await archive._create_stage(session, "source", uuid4(), predecessors=receiving)
    saved = json.loads(json.dumps(stage))
    assert set(saved) == {"dataset_id", "schema_name", "schema_oid", "relation_oids"}
    names = archive.TABLES + archive.PREDECESSORS if receiving else archive.TABLES
    monkeypatch.setattr(archive, "_oids", AsyncMock(return_value=tuple(range(40, 40 + len(names)))))
    live = await archive._named_stage(session, saved, receiving=receiving)
    assert live["schema_name"] == stage["schema_name"] == archive.stage_schema(archive.UUID(saved["dataset_id"]))
    archive._oids.assert_awaited_once_with(session, stage["schema_name"], names)
