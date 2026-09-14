# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib

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
    assert generation.RELATION_NAMES == tuple(
        model.__tablename__ for model in generation.ENTITY_ADDRESS_RESULT_MODELS
    )
    assert len(generation.RELATION_NAMES) == len(set(generation.RELATION_NAMES)) == 7


def test_generation_authority_accepts_complete_state():
    row = {
        "singleton": True,
        "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        "local_generation": 14,
        "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        "origin_generation": 8,
        "published_at": datetime.datetime(2026, 9, 14, 8, 30, tzinfo=datetime.UTC),
        "relation_oids": list(range(10, 17)),
    }

    authority = generation.validate_entity_address_result_generation_authority(row)

    assert authority.local_generation == 14
    assert authority.serving_generation.origin_generation == 8
    assert authority.relation_oids == tuple(range(10, 17))


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
    row = {
        "singleton": True,
        "local_lineage_id": "c8f27af1-56ba-4cda-82d8-0fc67650918f",
        "local_generation": 14,
        "origin_lineage_id": "edfc559e-0067-43ed-b7fa-983fdb7077fe",
        "origin_generation": 8,
        "published_at": "2026-09-14T08:30:00Z",
        "relation_oids": relation_oids,
    }

    with pytest.raises(RuntimeError, match="serving generation is invalid"):
        generation.validate_entity_address_result_generation_authority(row)


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
    context = restore._rehydrated_context(
        {"address_alias_generation": 7, "stage_persistence": "p"}
    )

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
