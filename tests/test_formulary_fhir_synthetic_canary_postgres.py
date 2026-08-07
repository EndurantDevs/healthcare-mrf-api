# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for the synthetic seed candidate canary."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import db
import process.formulary_fhir.synthetic_canary as canary_module
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.synthetic_canary import SyntheticCanaryError
from process.formulary_fhir.synthetic_canary import (
    verify_synthetic_seed_candidate,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_ENABLED_ENV
from process.formulary_fhir.synthetic_canary_contract import CANARY_RUN_ID
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_BASE
from process.formulary_fhir.synthetic_canary_contract import (
    CANARY_SOURCE_DISPLAY_NAME,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import canary_metadata
from process.formulary_fhir.synthetic_canary_contract import canary_runtime_config
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_canary_transport import SyntheticCanaryClient
from tests.test_formulary_fhir_repository_postgres import _configure_database
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _load_migration
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action


TABLES = (
    "fhir_formulary_source",
    "fhir_formulary_dataset",
    "fhir_formulary_current",
    "fhir_formulary_coverage_plan",
    "fhir_formulary_coverage_plan_version",
    "fhir_formulary_dataset_coverage_plan",
    "fhir_formulary_drug_plan_alias",
    "fhir_formulary_drug_plan_alias_version",
    "fhir_formulary_dataset_alias",
    "fhir_formulary_medication",
    "fhir_formulary_alias_membership",
    "fhir_formulary_alternative",
    "fhir_formulary_checkpoint",
)


async def _prepare_empty_schema(monkeypatch, database_url, schema_name, engine):
    _configure_database(monkeypatch, database_url, schema_name)
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(
            f"CREATE SCHEMA {_quoted(schema_name)}"
        )
    await _run_migration_action(engine, _load_migration(), "upgrade")
    await db.disconnect()


async def _table_counts() -> dict[str, int]:
    return {
        table: int(
            await db.scalar(f"SELECT count(*) FROM {table_name(table)};") or 0
        )
        for table in TABLES
    }


async def _assert_candidate_state(expected_by_field: dict[str, object]) -> None:
    source_by_field = row_mapping(
        await db.first(
            f"SELECT source_id, canonical_base, display_name, enabled, "
            f"runtime_config_json, metadata_json FROM "
            f"{table_name('fhir_formulary_source')};"
        )
    )
    dataset_by_field = row_mapping(
        await db.first(
            f"SELECT dataset_id, run_id, previous_dataset_id, status, "
            "publish_requested, seed_eligible, list_count, alias_count, "
            f"medication_count, coverage_hash, membership_hash FROM "
            f"{table_name('fhir_formulary_dataset')};"
        )
    )
    assert source_by_field == {
        "source_id": CANARY_SOURCE_ID,
        "canonical_base": CANARY_SOURCE_BASE,
        "display_name": CANARY_SOURCE_DISPLAY_NAME,
        "enabled": False,
        "runtime_config_json": canary_runtime_config(),
        "metadata_json": canary_metadata(),
    }
    assert dataset_by_field == {
        "dataset_id": expected_by_field["dataset_id"],
        "run_id": CANARY_RUN_ID,
        "previous_dataset_id": None,
        "status": "verified",
        "publish_requested": False,
        "seed_eligible": True,
        "list_count": 1,
        "alias_count": 2,
        "medication_count": 2,
        "coverage_hash": expected_by_field["coverage_hash"],
        "membership_hash": expected_by_field["membership_hash"],
    }
    assert await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_current')};"
    ) == 0
    snapshot = await FHIRFormularyRepository(
        source_id=CANARY_SOURCE_ID
    ).current_snapshot()
    assert snapshot.dataset is None and snapshot.aliases == {}


async def _assert_partial_restart_state() -> None:
    source_enabled = await db.scalar(
        f"SELECT enabled FROM {table_name('fhir_formulary_source')};"
    )
    dataset_status = await db.scalar(
        f"SELECT status FROM {table_name('fhir_formulary_dataset')};"
    )
    checkpoint_rows = [
        row_mapping(checkpoint_row)
        for checkpoint_row in await db.all(
            f"SELECT source_plan_identifier, fence_token, "
            f"acquisition_mode, completed FROM "
            f"{table_name('fhir_formulary_checkpoint')} "
            "ORDER BY source_plan_identifier;"
        )
    ]
    assert source_enabled is False
    assert dataset_status == "building"
    assert checkpoint_rows == [
        {
            "source_plan_identifier": "SYNTH-A",
            "fence_token": 1,
            "acquisition_mode": "full",
            "completed": True,
        }
    ]


async def _insert_foreign_source() -> None:
    inserted_count = await db.status(
        f"INSERT INTO {table_name('fhir_formulary_source')} "
        f"(source_id, canonical_base, display_name, enabled, "
        f"runtime_config_json, metadata_json) VALUES "
        f"(:source_id, :canonical_base, :display_name, false, "
        f"CAST(:runtime_config_json AS jsonb), CAST(:metadata_json AS jsonb));",
        source_id="source-beta",
        canonical_base="https://source-beta.example.invalid/fhir",
        display_name="Synthetic Source Beta",
        runtime_config_json=json_text({}),
        metadata_json=json_text({"synthetic": True}),
    )
    assert inserted_count == 1


async def _insert_reserved_id_collision() -> None:
    inserted_count = await db.status(
        f"INSERT INTO {table_name('fhir_formulary_source')} "
        f"(source_id, canonical_base, display_name, enabled, "
        f"runtime_config_json, metadata_json) VALUES "
        f"(:source_id, :canonical_base, :display_name, true, "
        f"CAST(:runtime_config_json AS jsonb), CAST(:metadata_json AS jsonb));",
        source_id=CANARY_SOURCE_ID,
        canonical_base="https://collision.example.invalid/fhir",
        display_name="Synthetic Collision",
        runtime_config_json=json_text({}),
        metadata_json=json_text({"synthetic": False}),
    )
    assert inserted_count == 1


async def _source_rows() -> list[dict[str, object]]:
    return [
        row_mapping(source_row)
        for source_row in await db.all(
            f"SELECT source_id, canonical_base, display_name, enabled, "
            f"runtime_config_json, metadata_json FROM "
            f"{table_name('fhir_formulary_source')} ORDER BY source_id;"
        )
    ]


@pytest.mark.asyncio
async def test_synthetic_candidate_exact_graph_replay_and_no_publication(monkeypatch):
    """Prove fixed seed verification, source disablement, and exact replay."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    try:
        await _prepare_empty_schema(monkeypatch, database_url, schema_name, engine)
        first_result = await verify_synthetic_seed_candidate()
        expected_by_field = expected_evidence()
        assert first_result.dataset_id == expected_by_field["dataset_id"]
        assert first_result.source_configuration_hash == (
            expected_by_field["source_configuration_hash"]
        )
        assert first_result.acquisition_contract_hash == (
            expected_by_field["acquisition_contract_hash"]
        )
        assert first_result.request_count == 9
        assert first_result.full_aliases == 2
        assert first_result.resumed_aliases == 0
        await _assert_candidate_state(expected_by_field)
        expected_count_by_table = {
            "fhir_formulary_source": 1,
            "fhir_formulary_dataset": 1,
            "fhir_formulary_current": 0,
            "fhir_formulary_coverage_plan": 1,
            "fhir_formulary_coverage_plan_version": 1,
            "fhir_formulary_dataset_coverage_plan": 1,
            "fhir_formulary_drug_plan_alias": 2,
            "fhir_formulary_drug_plan_alias_version": 2,
            "fhir_formulary_dataset_alias": 2,
            "fhir_formulary_medication": 2,
            "fhir_formulary_alias_membership": 2,
            "fhir_formulary_alternative": 1,
            "fhir_formulary_checkpoint": 2,
        }
        assert await _table_counts() == expected_count_by_table

        replay_result = await verify_synthetic_seed_candidate()
        assert replay_result.dataset_id == first_result.dataset_id
        assert replay_result.request_count == 3
        assert replay_result.full_aliases == 0
        assert replay_result.resumed_aliases == 2
        assert await _table_counts() == expected_count_by_table
        await _assert_candidate_state(expected_by_field)
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_synthetic_candidate_partial_restart_skips_completed_alias(
    monkeypatch,
):
    """Prove cancellation after alias A resumes with exact alias B requests."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    second_alias_started = asyncio.Event()
    should_block_second_alias = True
    original_census = SyntheticCanaryClient.medication_current_census

    async def controlled_census(self, alias: str, *, cutoff):
        if should_block_second_alias and alias == "SYNTH-B":
            second_alias_started.set()
            await asyncio.Event().wait()
        return await original_census(self, alias, cutoff=cutoff)

    monkeypatch.setattr(
        SyntheticCanaryClient,
        "medication_current_census",
        controlled_census,
    )
    try:
        await _prepare_empty_schema(monkeypatch, database_url, schema_name, engine)
        canary_task = asyncio.create_task(verify_synthetic_seed_candidate())
        await second_alias_started.wait()
        canary_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await canary_task

        await _assert_partial_restart_state()

        should_block_second_alias = False
        resumed_result = await verify_synthetic_seed_candidate()
        assert resumed_result.request_count == 6
        assert resumed_result.full_aliases == 2
        assert resumed_result.resumed_aliases == 1
        replay_result = await verify_synthetic_seed_candidate()
        assert replay_result.request_count == 3
        assert replay_result.full_aliases == 0
        assert replay_result.resumed_aliases == 2
        await _assert_candidate_state(expected_evidence())
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_synthetic_candidate_rejects_foreign_catalog_without_mutation(
    monkeypatch,
):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    try:
        await _prepare_empty_schema(monkeypatch, database_url, schema_name, engine)
        await _insert_foreign_source()
        baseline_rows = await _source_rows()
        baseline_counts = await _table_counts()

        with pytest.raises(SyntheticCanaryError) as caught:
            await verify_synthetic_seed_candidate()

        assert caught.value.code == "catalog"
        assert await _source_rows() == baseline_rows
        assert await _table_counts() == baseline_counts
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_synthetic_candidate_does_not_mutate_reserved_id_collision(
    monkeypatch,
):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    try:
        await _prepare_empty_schema(monkeypatch, database_url, schema_name, engine)
        await _insert_reserved_id_collision()
        baseline_rows = await _source_rows()
        baseline_counts = await _table_counts()

        with pytest.raises(SyntheticCanaryError) as caught:
            await verify_synthetic_seed_candidate()

        assert caught.value.code == "catalog"
        assert await _source_rows() == baseline_rows
        assert await _table_counts() == baseline_counts
        assert baseline_rows[0]["enabled"] is True
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_synthetic_candidate_disables_source_when_catalog_drifts(
    monkeypatch,
):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    original_verify = canary_module._verify_enabled_candidate

    async def verify_then_drift(database):
        candidate_result = await original_verify(database)
        await _insert_foreign_source()
        return candidate_result

    monkeypatch.setattr(
        canary_module,
        "_verify_enabled_candidate",
        verify_then_drift,
    )
    try:
        await _prepare_empty_schema(monkeypatch, database_url, schema_name, engine)
        with pytest.raises(SyntheticCanaryError) as caught:
            await verify_synthetic_seed_candidate()

        assert caught.value.code == "catalog"
        source_rows = await _source_rows()
        assert [source_row["source_id"] for source_row in source_rows] == [
            CANARY_SOURCE_ID,
            "source-beta",
        ]
        assert all(source_row["enabled"] is False for source_row in source_rows)
        assert await db.scalar(
            f"SELECT status FROM {table_name('fhir_formulary_dataset')};"
        ) == "verified"
        assert await db.scalar(
            f"SELECT count(*) FROM {table_name('fhir_formulary_current')};"
        ) == 0
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
