# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""End-to-end PostgreSQL publication proofs for profile deltas."""

from __future__ import annotations

import datetime
import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_control_capacity import (
    _control_wal_plan_input,
)


importer = importlib.import_module("process.provider_directory_fhir")
from tests.provider_directory_profile_delta_publication import (
    _assert_delta_post_commit_scenario,
    _assert_delta_publication,
    _delta_capacity_admission,
    _prepared_scenario_delta,
    _publish_prepared_delta,
)
from tests.provider_directory_profile_delta_scenario import (
    _delta_capacity_context,
    _delta_lineage,
    _delta_relation_oid_by_name,
    _delta_relation_scenario,
    _insert_delta_checkpoint,
    _insert_delta_serving_generation,
)
from tests.provider_directory_profile_delta_test_support import (
    _delta_database,
    _prepared_delta,
)


def test_profile_delta_receipt_matching_accepts_zero_counts() -> None:
    """A valid empty delta receipt must not be mistaken for missing counts."""

    prepared_delta = _prepared_delta()
    receipt_by_field = {
        **importer._provider_directory_profile_delta_receipt_static_values(
            prepared_delta
        ),
        "evidence_rows": 0,
        "profile_rows": 0,
        "evidence_inserted": 0,
        "evidence_deleted": 0,
        "profile_inserted": 0,
        "profile_deleted": 0,
        "from_capacity_geometry_json": (
            prepared_delta.from_capacity_geometry_json
        ),
        "capacity_geometry_json": (
            prepared_delta.capacity_geometry_json
        ),
    }

    assert importer._is_provider_directory_profile_delta_receipt_matching(
        receipt_by_field,
        prepared_delta,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "post_commit_scenario",
    ("replay_without_scratch", "conflicting_receipt", "conflicting_geometry"),
)
async def test_profile_delta_preserves_retained_facts_and_replays(
    monkeypatch,
    post_commit_scenario: str,
):
    """Apply changed-source rows, retain global facts, and replay safely."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        scenario = await _delta_relation_scenario(database, schema)
        lineage = _delta_lineage()
        oid_by_name = await _delta_relation_oid_by_name(database, scenario)
        seed_geometry = capacity.validated_capacity_geometry(
            _geometry_payload(
                evidence_target_oid=oid_by_name["evidence_target"],
                profile_target_oid=oid_by_name["profile_target"],
            )
        )
        await _insert_delta_checkpoint(
            database, scenario, lineage, oid_by_name, seed_geometry
        )
        await _insert_delta_serving_generation(
            database, scenario, lineage, oid_by_name
        )
        capacity_context = await _delta_capacity_context(
            database, scenario, lineage, oid_by_name
        )
        geometry = capacity_context.geometry
        await database.status(
            f"UPDATE {scenario.checkpoint_ref} "
            "SET capacity_geometry_hash = :geometry_hash, "
            "capacity_geometry_json = CAST(:geometry_json AS jsonb) "
            "WHERE build_id = :build_id;",
            geometry_hash=capacity.capacity_geometry_hash(geometry),
            geometry_json=capacity.canonical_capacity_geometry_json(geometry),
            build_id=lineage.build_id,
        )
        profile_delta = _prepared_scenario_delta(
            scenario, lineage, oid_by_name, geometry
        )
        admission = await _delta_capacity_admission(
            database, lineage, capacity_context
        )
        await _publish_prepared_delta(profile_delta, admission)
        await _assert_delta_publication(
            database, scenario, lineage, profile_delta
        )
        await _assert_delta_post_commit_scenario(
            database,
            scenario,
            profile_delta,
            geometry,
            post_commit_scenario,
        )
