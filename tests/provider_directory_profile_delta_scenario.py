# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Profile-delta relation and capacity scenario fixtures."""

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
from tests.provider_directory_profile_delta_schema_fixtures import (
    _create_delta_contract_tables,
)
from tests.provider_directory_profile_delta_test_support import (
    _bound_control_geometry,
    _insert_evidence,
)


async def _seed_delta_rows(database: Database, scenario) -> None:
    """Seed changed evidence, retained evidence, affected NPI, and old profile."""
    evidence_fixtures = (
        (
            scenario.evidence_target_ref,
            "a" * 32,
            "name",
            "source-a",
            "dataset-a-old",
            {"text": "Old"},
        ),
        (
            scenario.evidence_target_ref,
            "b" * 32,
            "specialty",
            "source-b",
            "dataset-b",
            {"code": "207Q00000X"},
        ),
        (
            scenario.evidence_stage_ref,
            "c" * 32,
            "contact",
            "source-a",
            "dataset-a-new",
            {"system": "phone", "value": "555-0100"},
        ),
    )
    for table_ref, evidence_key, fact_type, source_id, dataset_id, value_json in evidence_fixtures:
        await _insert_evidence(
            database,
            table_ref,
            evidence_key=evidence_key,
            fact_type=fact_type,
            source_id=source_id,
            dataset_id=dataset_id,
            value_json=value_json,
        )
    await database.status(
        f"INSERT INTO {scenario.affected_ref} (npi) VALUES (1000000004);"
    )
    await database.status(
        f"""
        INSERT INTO {scenario.profile_target_ref} (
            npi, profile_json, evidence_json, source_ids,
            endpoint_ids, dataset_ids, source_count,
            independent_source_count, fact_count,
            generation_id, published_at
        ) VALUES (
            1000000004, '{{}}', '{{}}',
            ARRAY['source-a', 'source-b'],
            ARRAY['endpoint-source-a', 'endpoint-source-b'],
            ARRAY['dataset-a-old', 'dataset-b'],
            2, 2, 2, :generation_id, now()
        );
        """,
        generation_id=scenario.old_generation,
    )


async def _delta_relation_scenario(
    database: Database,
    schema: str,
):
    """Create, seed, and identify every target and scratch relation."""
    evidence_stage = "provider_directory_profile_evidence_stage_delta"
    profile_stage = "provider_directory_profile_stage_delta"
    affected_stage = "provider_directory_profile_affected_delta"
    await _create_delta_contract_tables(
        database,
        schema,
        evidence_stage=evidence_stage,
        profile_stage=profile_stage,
        affected_stage=affected_stage,
    )
    scenario = SimpleNamespace(
        schema=schema,
        evidence_stage=evidence_stage,
        profile_stage=profile_stage,
        affected_stage=affected_stage,
        evidence_target_ref=profile.qualified_table(
            schema, profile.PROFILE_EVIDENCE_TABLE
        ),
        profile_target_ref=profile.qualified_table(
            schema, profile.PROFILE_TABLE
        ),
        evidence_stage_ref=profile.qualified_table(schema, evidence_stage),
        profile_stage_ref=profile.qualified_table(schema, profile_stage),
        affected_ref=profile.qualified_table(schema, affected_stage),
        checkpoint_ref=profile.qualified_table(
            schema, "provider_directory_profile_build_checkpoint"
        ),
        serving_ref=profile.qualified_table(
            schema, "provider_directory_profile_serving_generation"
        ),
        old_generation="pdprofile_" + "1" * 32,
        new_generation="pdprofile_" + "2" * 32,
    )
    await _seed_delta_rows(database, scenario)
    await database.status(
        profile.profile_delta_insert_sql(
            current_evidence_ref=scenario.evidence_target_ref,
            delta_evidence_ref=scenario.evidence_stage_ref,
            affected_npi_ref=scenario.affected_ref,
            target_ref=scenario.profile_stage_ref,
        ),
        refresh_and_removed_source_ids=["source-a"],
        retained_source_ids=["source-a", "source-b"],
        profile_as_of="2026-07-30",
        generation_id=scenario.new_generation,
    )
    return scenario


async def _relation_oid(database: Database, relation_ref: str) -> int:
    """Return one exact relation OID."""
    return int(
        await database.scalar(
            "SELECT to_regclass(:relation_ref)::oid::bigint;",
            relation_ref=relation_ref,
        )
    )


async def _delta_relation_oid_by_name(
    database: Database,
    scenario,
) -> dict[str, int]:
    """Return target and scratch relation OIDs keyed by semantic role."""
    relation_ref_by_name = {
        "evidence_target": scenario.evidence_target_ref,
        "profile_target": scenario.profile_target_ref,
        "evidence_stage": scenario.evidence_stage_ref,
        "profile_stage": scenario.profile_stage_ref,
        "affected_stage": scenario.affected_ref,
    }
    return {
        relation_name: await _relation_oid(database, relation_ref)
        for relation_name, relation_ref in relation_ref_by_name.items()
    }


def _delta_lineage():
    """Return source/context vectors and their exact hashes."""
    from_vector = (("source-a", "dataset-a-old"), ("source-b", "dataset-b"))
    to_vector = (("source-a", "dataset-a-new"), ("source-b", "dataset-b"))
    from_context_vector = (
        ("source-a", "a" * 64),
        ("source-b", "b" * 64),
    )
    to_context_vector = (
        ("source-a", "c" * 64),
        ("source-b", "b" * 64),
    )
    return SimpleNamespace(
        from_vector=from_vector,
        to_vector=to_vector,
        from_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(from_vector)
        ),
        to_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(to_vector)
        ),
        from_context_vector=from_context_vector,
        to_context_vector=to_context_vector,
        from_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                from_context_vector
            )
        ),
        to_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                to_context_vector
            )
        ),
        build_id="pdpb_" + "5" * 32,
        resume_hash="5" * 64,
        plan_hash="4" * 64,
        proof_id="3" * 64,
    )


async def _insert_delta_checkpoint(
    database: Database,
    scenario,
    lineage,
    oid_by_name: dict[str, int],
    geometry,
) -> None:
    """Insert the exact ready checkpoint used by the delta cutover."""
    await database.status(
        f"""
        INSERT INTO {scenario.checkpoint_ref} (
            build_id, resume_lineage_hash, executable_plan_hash,
            owner_run_id, state, materialization_mode,
            refresh_source_ids, removed_source_ids,
            evidence_stage, profile_stage, affected_npi_stage,
            evidence_stage_oid, profile_stage_oid,
            affected_npi_stage_oid, evidence_target_oid,
            profile_target_oid, current_source_vector_hash,
            desired_source_vector_hash,
            current_source_context_vector_hash,
            desired_source_context_vector_hash,
            capacity_geometry_status, capacity_geometry_hash,
            capacity_geometry_json, profile_as_of,
            evidence_next_batch, evidence_total_batches,
            profile_next_batch, profile_total_batches
        ) VALUES (
            :build_id, :resume_hash, :plan_hash, 'run-delta',
            'ready', 'source_delta', '["source-a"]', '[]',
            :evidence_stage, :profile_stage, :affected_stage,
            :evidence_stage_oid, :profile_stage_oid,
            :affected_stage_oid, :evidence_target_oid,
            :profile_target_oid, :from_vector_hash,
            :to_vector_hash, :from_context_vector_hash,
            :to_context_vector_hash, 'verified', :geometry_hash,
            CAST(:geometry_json AS jsonb), '2026-07-30',
            1, 1, 1, 1
        );
        """,
        build_id=lineage.build_id,
        resume_hash=lineage.resume_hash,
        plan_hash=lineage.plan_hash,
        evidence_stage=scenario.evidence_stage,
        profile_stage=scenario.profile_stage,
        affected_stage=scenario.affected_stage,
        evidence_stage_oid=oid_by_name["evidence_stage"],
        profile_stage_oid=oid_by_name["profile_stage"],
        affected_stage_oid=oid_by_name["affected_stage"],
        evidence_target_oid=oid_by_name["evidence_target"],
        profile_target_oid=oid_by_name["profile_target"],
        from_vector_hash=lineage.from_vector_hash,
        to_vector_hash=lineage.to_vector_hash,
        from_context_vector_hash=lineage.from_context_vector_hash,
        to_context_vector_hash=lineage.to_context_vector_hash,
        geometry_hash=capacity.capacity_geometry_hash(geometry),
        geometry_json=capacity.canonical_capacity_geometry_json(geometry),
    )


async def _insert_delta_serving_generation(
    database: Database,
    scenario,
    lineage,
    oid_by_name: dict[str, int],
) -> None:
    """Insert the legacy serving generation from which the delta advances."""
    await database.status(
        f"""
        INSERT INTO {scenario.serving_ref} (
            singleton_key, status, operation, control_generation,
            generation_id, selection_proof_id, authority_revision,
            profile_schema_version, profile_strategy_version,
            source_vector_hash, source_vector_json,
            source_context_vector_hash, source_context_vector_json,
            executable_plan_hash, capacity_geometry_status,
            capacity_geometry_hash, capacity_geometry_json,
            evidence_target_oid, profile_target_oid,
            evidence_rows, profile_rows, profile_as_of,
            published_at, created_at, updated_at
        ) VALUES (
            'global', 'published', 'publish', 6,
            :generation_id, :proof_id, 6, 1,
            'source-fact-role32-npi5m-v1',
            :source_vector_hash, CAST(:source_vector_json AS jsonb),
            :source_context_vector_hash,
            CAST(:source_context_vector_json AS jsonb),
            :plan_hash, 'legacy_unavailable', NULL, NULL,
            :evidence_target_oid, :profile_target_oid,
            2, 1, '2026-07-30', now(), now(), now()
        );
        """,
        generation_id=scenario.old_generation,
        proof_id="6" * 64,
        source_vector_hash=lineage.from_vector_hash,
        source_vector_json=json.dumps(
            importer._provider_directory_profile_source_vector_json(
                lineage.from_vector
            )
        ),
        source_context_vector_hash=lineage.from_context_vector_hash,
        source_context_vector_json=json.dumps(
            importer._provider_directory_profile_source_context_vector_json(
                lineage.from_context_vector
            )
        ),
        plan_hash="7" * 64,
        evidence_target_oid=oid_by_name["evidence_target"],
        profile_target_oid=oid_by_name["profile_target"],
    )


def _relax_delta_relation_caps(geometry_by_field: dict[str, object]) -> None:
    """Set generous exact caps for the small PostgreSQL truth fixture."""
    for relation_cap in geometry_by_field["relation_byte_caps"]:
        relation_cap["max_temp_bytes"] = 1_024
        relation_cap["max_wal_bytes"] = 100 * 1024 * 1024
        if relation_cap["relation_name"].endswith("_target"):
            relation_cap["max_target_growth_bytes"] = 100 * 1024 * 1024
            relation_cap["max_deleted_logical_bytes"] = 100 * 1024 * 1024
        else:
            relation_cap["max_scratch_bytes"] = 100 * 1024 * 1024


async def _delta_capacity_context(
    database: Database,
    scenario,
    lineage,
    oid_by_name: dict[str, int],
):
    """Bind live database identity into the exact admitted delta geometry."""
    serving_state = await importer._provider_directory_profile_serving_state(
        scenario.schema
    )
    assert serving_state is not None
    database_identity = (
        await importer._provider_directory_profile_capacity_database_identity(
            scenario.schema,
            serving_state,
        )
    )
    geometry_by_field = _geometry_payload(
        selection_proof_id=lineage.proof_id,
        profile_schema_version=1,
        profile_strategy_version=profile.PROFILE_BUILD_STRATEGY_VERSION,
        executable_plan_hash=lineage.plan_hash,
        current_source_vector_hash=lineage.from_vector_hash,
        desired_source_vector_hash=lineage.to_vector_hash,
        current_context_vector_hash=lineage.from_context_vector_hash,
        desired_context_vector_hash=lineage.to_context_vector_hash,
        evidence_target_oid=oid_by_name["evidence_target"],
        profile_target_oid=oid_by_name["profile_target"],
    )
    geometry_by_field.update(
        {
            field_name: getattr(database_identity, field_name)
            for field_name in geometry_by_field
            if hasattr(database_identity, field_name)
        }
    )
    _relax_delta_relation_caps(geometry_by_field)
    geometry, control_projection = _bound_control_geometry(geometry_by_field)
    return SimpleNamespace(
        geometry=geometry,
        control_projection=control_projection,
        database_identity=database_identity,
    )
