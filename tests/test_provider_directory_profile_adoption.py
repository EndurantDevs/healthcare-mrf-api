# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded legacy serving-generation adoption contracts."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution


importer = importlib.import_module("process.provider_directory_fhir")


def test_serving_adoption_uses_terminal_counts_without_target_scan(
    monkeypatch,
):
    """Adopt exact terminal counts without scanning the large targets."""
    scalar = AsyncMock()
    monkeypatch.setattr(importer.db, "scalar", scalar)
    selection_proof_by_field = {
        "row_counts": {
            "profile_rows": 3,
            "profile_source_evidence_rows": 5,
        },
        "generation": 6,
        "proof_id": "b" * 64,
        "authority_revision": 6,
        "profile_schema_version": 1,
        "profile_strategy_version": "legacy-profile-v3",
    }

    assert importer._profile_adoption_attested_row_counts(
        selection_proof_by_field
    ) == (3, 5)
    scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_serving_adoption_binds_locked_targets_without_heap_scan(
    monkeypatch,
):
    """Use relation identity and an indexed generation witness only."""
    relation_oid = AsyncMock(side_effect=[101, 102, 101, 102])
    status = AsyncMock()
    first = AsyncMock(
        return_value={
            "has_profile_rows": True,
            "has_profile_generation": True,
            "has_evidence_rows": True,
        }
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_oid",
        relation_oid,
    )
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "first", first)

    adoption_targets = await importer._profile_adoption_targets(
        "mrf",
        generation_id="pdprofile_" + "a" * 32,
        profile_rows=3,
        evidence_rows=5,
    )

    observed_oids = (
        adoption_targets.profile_target_oid,
        adoption_targets.evidence_target_oid,
    )
    assert observed_oids == (101, 102)
    status.assert_awaited_once()
    query = first.await_args.args[0]
    assert "generation_id = :generation_id" in query
    assert "count(" not in query.lower()
    assert "min(" not in query.lower()
    assert "max(" not in query.lower()


def test_serving_adoption_accepts_real_selection_result_contract():
    execution = _execution()
    generation_id = "pdprofile_" + "a" * 32
    producer_result = importer.profile_selection_result(
        execution,
        profile_generation_id=generation_id,
        profile_as_of="2026-07-30",
        profile_rows=3,
        profile_source_evidence_rows=5,
    )

    assert importer._provider_directory_profile_adoption_result(
        producer_result,
        generation_id=generation_id,
    ) == producer_result
