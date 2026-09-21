# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

generation = importlib.import_module("process.entity_address_result_generation")
restore = importlib.import_module("process.entity_address_snapshot_restore")


def _serving(lineage: str, value: int):
    return {
        "origin_lineage_id": lineage,
        "origin_generation": value,
        "published_at": "2026-09-14T08:30:00Z",
    }


def test_canonical_relation_order_is_model_owned_and_complete():
    assert generation.RELATION_NAMES == tuple(model.__tablename__ for model in generation.ENTITY_ADDRESS_RESULT_MODELS)
    assert len(generation.RELATION_NAMES) == len(set(generation.RELATION_NAMES)) == 7


def test_generation_authority_accepts_complete_state():
    authority_by_field = {
        "singleton": True,
        "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        "local_generation": 14,
        "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        "origin_generation": 8,
        "published_at": datetime.datetime(2026, 9, 14, 8, 30, tzinfo=datetime.UTC),
        "relation_oids": list(range(10, 17)),
    }

    authority = generation.validate_entity_address_result_generation_authority(authority_by_field)

    assert authority.local_generation == 14
    assert authority.serving_generation.origin_generation == 8
    assert authority.relation_oids == tuple(range(10, 17))
    assert authority.as_dict()["serving_generation"]["origin_generation"] == 8
    assert authority.as_dict()["relation_oids"] == list(range(10, 17))


@pytest.mark.parametrize(
    "relation_oids",
    [
        [10, 10, 12, 13, 14, 15, 16],
        [10, 11, 12, 13, 14, 15, 0],
        [10, 11, 12, 13, 14, 15],
    ],
)
def test_generation_authority_rejects_non_distinct_non_positive_or_incomplete_oids(
    relation_oids,
):
    authority_by_field = {
        "singleton": True,
        "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        "local_generation": 14,
        "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        "origin_generation": 8,
        "published_at": "2026-09-14T08:30:00Z",
        "relation_oids": relation_oids,
    }

    with pytest.raises(RuntimeError, match="serving generation is invalid"):
        generation.validate_entity_address_result_generation_authority(authority_by_field)


def test_automatic_order_requires_strictly_newer_same_lineage():
    lineage = "c8f27af1-56ba-4cda-82d8-0fc67650918f"

    generation.require_entity_address_automatic_generation_order(
        _serving(lineage, 8),
        _serving(lineage, 7),
    )


@pytest.mark.parametrize(
    ("candidate", "incumbent"),
    [
        (None, None),
        (
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 8),
            _serving("edfc559e-0067-43ed-b7fa-983fdb7077fe", 7),
        ),
        (
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 7),
            _serving("c8f27af1-56ba-4cda-82d8-0fc67650918f", 7),
        ),
    ],
)
def test_automatic_order_fails_closed_without_same_lineage_monotonic_evidence(
    candidate,
    incumbent,
):
    with pytest.raises(ValueError, match="unavailable|unsupported"):
        generation.require_entity_address_automatic_generation_order(
            candidate,
            incumbent,
        )


def test_legacy_prepared_adoption_context_is_explicitly_generationless():
    context = restore._rehydrated_context({"address_alias_generation": 7, "stage_persistence": "p"})

    assert context["result_generation_mode"] == "adoption"
    assert context["source_serving_generation"] is None


def test_prepared_adoption_context_requires_both_generation_fields():
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="context is invalid"):
        restore._rehydrated_context(
            {
                "address_alias_generation": 7,
                "stage_persistence": "p",
                "result_generation_mode": "adoption",
            }
        )


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"origin_lineage_id": "bad", "origin_generation": 1, "published_at": "2026-09-14T08:30:00Z"},
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": True,
            "published_at": "2026-09-14T08:30:00Z",
        },
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 1,
            "published_at": "bad",
        },
        {
            "origin_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "origin_generation": 1,
            "published_at": datetime.datetime(2026, 9, 14, 8, 30),
        },
    ],
)
def test_serving_generation_rejects_malformed_identity(value):
    with pytest.raises(ValueError, match="invalid"):
        generation.validate_entity_address_serving_generation(value)


@pytest.mark.parametrize(
    "value",
    [
        object(),
        {"singleton": False},
        {
            "singleton": True,
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": True,
        },
        {
            "singleton": True,
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": 0,
            "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        },
    ],
)
def test_generation_authority_rejects_malformed_state(value):
    with pytest.raises(RuntimeError, match="unavailable|invalid|incomplete"):
        generation.validate_entity_address_result_generation_authority(value)


def test_generation_authority_serializes_generationless_state():
    authority = generation.validate_entity_address_result_generation_authority(
        {
            "singleton": True,
            "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
            "local_generation": 0,
            "origin_lineage_id": None,
            "origin_generation": None,
            "published_at": None,
            "relation_oids": None,
        }
    )
    assert authority.as_dict()["serving_generation"] is None
    assert authority.as_dict()["relation_oids"] is None


@pytest.mark.asyncio
async def test_generation_reads_require_authority_rows():
    database = SimpleNamespace(first=AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="singleton is unavailable"):
        await generation._locked_authority(database, "mrf")

    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    with pytest.raises(RuntimeError, match="singleton is unavailable"):
        await generation.read_entity_address_result_generation_authority(session, schema_name="mrf")

    authority_by_field = {
        "singleton": True,
        "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        "local_generation": 0,
        "origin_lineage_id": None,
        "origin_generation": None,
        "published_at": None,
        "relation_oids": None,
    }
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: authority_by_field))
    session.execute = AsyncMock(return_value=result)
    assert (
        await generation.read_entity_address_result_generation_authority(session, schema_name="mrf")
    ).local_generation == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [("wrong", value) for value in range(10, 17)],
        [(name, None) for name in generation.RELATION_NAMES],
    ],
)
async def test_relation_oid_read_rejects_malformed_rows(rows):
    database = SimpleNamespace(all=AsyncMock(return_value=rows))
    with pytest.raises(RuntimeError, match="relations are unavailable"):
        await generation._current_relation_oids(database, "mrf")


@pytest.mark.asyncio
async def test_generation_publication_rejects_exhausted_missing_and_changed_authority(monkeypatch):
    lineage = "c8f27af1-56ba-4cda-82d8-0fc67650918f"
    exhausted = generation.EntityAddressResultGenerationAuthority(
        lineage,
        generation._MAX_GENERATION,
        None,
        None,
    )
    monkeypatch.setattr(generation, "_locked_authority", AsyncMock(return_value=exhausted))
    with pytest.raises(RuntimeError, match="exhausted"):
        await generation.publish_local_entity_address_generation(object(), schema_name="mrf")

    current = generation.EntityAddressResultGenerationAuthority(lineage, 1, None, None)
    monkeypatch.setattr(generation, "_locked_authority", AsyncMock(return_value=current))
    monkeypatch.setattr(generation, "_current_relation_oids", AsyncMock(return_value=tuple(range(10, 17))))
    database = SimpleNamespace(first=AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="singleton is unavailable"):
        await generation.publish_local_entity_address_generation(database, schema_name="mrf")
    with pytest.raises(RuntimeError, match="singleton is unavailable"):
        await generation.publish_adopted_entity_address_generation(database, schema_name="mrf", source_generation=None)

    changed = generation.EntityAddressResultGenerationAuthority(
        "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        1,
        None,
        None,
    )
    database.first = AsyncMock(return_value={"updated": True})
    monkeypatch.setattr(generation, "validate_entity_address_result_generation_authority", lambda _row: changed)
    with pytest.raises(RuntimeError, match="changed during adoption"):
        await generation.publish_adopted_entity_address_generation(database, schema_name="mrf", source_generation=None)


@pytest.mark.asyncio
async def test_cutover_modes_dispatch_and_reject_unknown_mode(monkeypatch):
    local = AsyncMock(return_value="local")
    adopted = AsyncMock(return_value="adopted")
    monkeypatch.setattr(generation, "publish_local_entity_address_generation", local)
    monkeypatch.setattr(generation, "publish_adopted_entity_address_generation", adopted)

    assert (
        await generation.publish_cutover_entity_address_generation(
            object(), schema_name="mrf", context={"result_generation_mode": "ordinary"}
        )
        == "local"
    )
    assert (
        await generation.publish_cutover_entity_address_generation(
            object(),
            schema_name="mrf",
            context={"result_generation_mode": "adoption", "source_serving_generation": None},
        )
        == "adopted"
    )
    assert await generation.publish_cutover_entity_address_generation(object(), schema_name="mrf", context={}) is None
    with pytest.raises(RuntimeError, match="mode is invalid"):
        await generation.publish_cutover_entity_address_generation(
            object(), schema_name="mrf", context={"result_generation_mode": "unknown"}
        )


def test_schema_name_rejects_invalid_identifier():
    with pytest.raises(ValueError, match="schema is invalid"):
        generation._schema_name("not-a-schema")
