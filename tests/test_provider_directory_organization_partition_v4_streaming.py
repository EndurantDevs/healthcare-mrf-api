# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compatibility-stream fences for semantic-v4 Organization partitions."""

from __future__ import annotations

import copy
import datetime
import importlib
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_organization_partition_v4 import (
    _context,
    _organization,
    _source,
    _stage,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _plan() -> importer.PartitionPlan:
    """Return one complete bounded partition plan."""

    return importer.PartitionPlan.create(
        datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        ceiling=10,
        minimum_width=datetime.timedelta(seconds=1),
    )


async def _one_row_stage() -> importer.LastUpdatedPartitionStage:
    """Stage one valid Organization candidate row."""

    return await _stage(
        (
            _organization(
                "Community Health Center",
                last_updated="2024-01-01T01:00:00Z",
            ),
        ),
        "window-a",
    )


def test_v4_occurrence_identity_keeps_semantic_fields() -> None:
    """Ignore only observation time even when planner paths are broader."""

    plan = importer.PartitionPlan.create(
        datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        ceiling=10,
        minimum_width=datetime.timedelta(seconds=1),
        volatile_metadata_paths=("/name", "/meta/lastUpdated"),
    )
    first = _organization(
        "Community Health Center",
        last_updated="2024-01-01T01:00:00Z",
    )
    second = _organization(
        "COMMUNITY HEALTH SERVICES",
        last_updated="2024-01-01T02:00:00Z",
    )
    occurrence_ids = [
        importer._partition_resource_bindings(
            _source(),
            "Organization",
            plan,
            plan.windows["root"],
            (observation,),
        )[2]
        for observation in (first, second)
    ]
    assert occurrence_ids[0] != occurrence_ids[1]


@pytest.mark.asyncio
async def test_v4_partition_stream_rehashes_rows(monkeypatch) -> None:
    """Reject payload drift between snapshot proof and compatibility output."""

    stage = await _one_row_stage()
    candidate_row = copy.deepcopy(stage.rows[0])
    candidate_row["payload_json"]["active"] = False
    proof_counts = importer.LastUpdatedPartitionProofCounts(
        leaf_count_sum=1,
        pass1_unique=1,
        pass2_unique=1,
        staged_candidate_count=1,
        invalid_candidate_count=0,
        orphan_proof_count=0,
        candidate_hashes_by_id={
            stage.rows[0]["resource_id"]: stage.rows[0]["payload_hash"]
        },
    )
    monkeypatch.setattr(
        importer,
        "_assert_last_updated_partition_candidate_proof",
        AsyncMock(return_value=proof_counts),
    )
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(side_effect=[[candidate_row], []]),
    )

    with pytest.raises(RuntimeError, match="stream_payload_mismatch"):
        await importer._stream_last_updated_partition_staged_rows(
            _context(),
            _source(),
            "Organization",
            importer.ProviderDirectoryOrganization,
            _plan(),
            run_id="run-organization-partition",
            row_batch_handler=AsyncMock(return_value=1),
            row_batch_size=10,
        )


@pytest.mark.asyncio
async def test_v4_empty_partition_rejects_late_insert(monkeypatch) -> None:
    """Do not release a row inserted after an exact empty snapshot proof."""

    stage = await _one_row_stage()
    proof_counts = importer.LastUpdatedPartitionProofCounts(
        leaf_count_sum=0,
        pass1_unique=0,
        pass2_unique=0,
        staged_candidate_count=0,
        invalid_candidate_count=0,
        orphan_proof_count=0,
        candidate_hashes_by_id={},
    )
    monkeypatch.setattr(
        importer,
        "_assert_last_updated_partition_candidate_proof",
        AsyncMock(return_value=proof_counts),
    )
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(side_effect=[[stage.rows[0]], []]),
    )
    row_handler = AsyncMock(return_value=1)

    with pytest.raises(RuntimeError, match="stream_payload_mismatch"):
        await importer._stream_last_updated_partition_staged_rows(
            _context(),
            _source(),
            "Organization",
            importer.ProviderDirectoryOrganization,
            _plan(),
            run_id="run-organization-partition",
            row_batch_handler=row_handler,
            row_batch_size=10,
        )
    row_handler.assert_not_awaited()
