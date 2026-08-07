# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for verify-only formulary synchronization."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from copy import deepcopy
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import db
from process.formulary_fhir.continuation import FHIRTransportError
from process.formulary_fhir.continuation import coverage_plan_search_contract
from process.formulary_fhir.continuation import medication_search_contract
from process.formulary_fhir.parser import parse_coverage_plan
from process.formulary_fhir.parser import parse_medication_knowledge
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import CheckpointWrite
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.planner import plan_coverage_census
from process.formulary_fhir.source import load_enabled_source
from process.formulary_fhir.synchronizer import synchronize_verified_dataset
from process.formulary_fhir.types import CurrentVersionCensus
from tests.test_formulary_fhir_repository_postgres import SOURCE_A
from tests.test_formulary_fhir_repository_postgres import SOURCE_B
from tests.test_formulary_fhir_repository_postgres import _prepare_repository_schema
from tests.test_formulary_fhir_storage_postgres import _connect
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _quoted


FIXTURES = Path(__file__).parent / "fixtures" / "formulary_fhir"
SOURCE_BASE = "https://a.example.invalid/fhir"
SEED_CUTOFF = dt.datetime(2026, 8, 6, 12, tzinfo=dt.UTC)
RUN_CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)
RUNTIME_CONFIG = {
    "timeout_seconds": 30,
    "max_attempts": 2,
    "page_size": 50,
    "max_pages": 100,
    "max_total_resources": 5_000,
    "max_response_bytes": 1_048_576,
}


def _fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _resources() -> tuple[dict[str, object], dict[str, object]]:
    coverage_resource = _fixture("coverage_plan.json")
    medication_resource = _fixture("medication_a.json")
    plan_extension = next(
        extension
        for extension in medication_resource["extension"]
        if "PlanID-extension" in str(extension.get("url"))
    )
    second_extension = deepcopy(plan_extension)
    second_extension["valueString"] = "SYNTH-B"
    medication_resource["extension"].append(second_extension)
    return coverage_resource, medication_resource


class _CensusClient:
    def __init__(self, config, *, fail_alias: str | None = None) -> None:
        self.config = config
        self.fail_alias = fail_alias
        self.failed_once = False
        self.medication_aliases: list[str] = []
        self.request_count = 0
        self.transient_retry_count = 0
        self.throttle_count = 0
        self.coverage_resource, self.medication_resource = _resources()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error) -> None:
        return None

    async def coverage_plan_current_census(self, *, cutoff):
        self.request_count += 1
        contract_hash = coverage_plan_search_contract(
            self.config,
            cutoff,
        ).contract_hash
        return CurrentVersionCensus(
            "List",
            cutoff,
            1,
            (self.coverage_resource,),
            contract_hash,
        )
    async def medication_current_census(self, alias, *, cutoff):
        self.request_count += 1
        self.medication_aliases.append(alias)
        if alias == self.fail_alias and not self.failed_once:
            self.failed_once = True
            raise FHIRTransportError("synthetic transient", is_transient=True)
        contract_hash = medication_search_contract(
            self.config,
            alias,
            cutoff,
        ).contract_hash
        return CurrentVersionCensus(
            "MedicationKnowledge",
            cutoff,
            1,
            (self.medication_resource,),
            contract_hash,
        )


class _ClientFactory:
    def __init__(self, *, fail_alias: str | None = None) -> None:
        self.fail_alias = fail_alias
        self.client: _CensusClient | None = None

    def __call__(self, config) -> _CensusClient:
        self.client = _CensusClient(config, fail_alias=self.fail_alias)
        return self.client


async def _enable_source(database_url, schema_name: str) -> None:
    connection = await _connect(database_url)
    try:
        await connection.execute(
            f"UPDATE {_quoted(schema_name)}.fhir_formulary_source "
            "SET enabled = true, runtime_config_json = $1::jsonb, "
            "metadata_json = $2::jsonb WHERE source_id = $3",
            json.dumps(RUNTIME_CONFIG),
            json.dumps({"mode": "manual"}),
            SOURCE_A,
        )
    finally:
        await connection.close()


async def _publish_predecessor() -> str:
    coverage_resource, medication_resource = _resources()
    plan = parse_coverage_plan(coverage_resource, canonical_base=SOURCE_BASE)
    medication = parse_medication_knowledge(medication_resource)
    repository = FHIRFormularyRepository(source_id=SOURCE_A)
    dataset = await repository.begin_dataset(
        run_id="synthetic-seed-run",
        cutoff_at=SEED_CUTOFF,
        acquisition_contract_hash="a" * 64,
        intent="seed",
    )
    coverage_result = await repository.put_coverage_plan(
        dataset=dataset,
        plan=plan,
    )
    for alias in coverage_result.aliases:
        await repository.put_alias_version(
            AliasVersionWrite(dataset, alias, 1, (medication,), 1)
        )
    await repository.verify_dataset(dataset=dataset)
    publication = await repository.publish_verified_seed(dataset=dataset)
    assert publication.generation == 1
    return dataset.dataset_id


async def _write_incomplete_second_alias() -> None:
    binding = await load_enabled_source(SOURCE_A)
    client = _CensusClient(binding.config)
    coverage_census = await client.coverage_plan_current_census(cutoff=RUN_CUTOFF)
    coverage_plan = plan_coverage_census(binding, coverage_census, RUN_CUTOFF)
    repository = FHIRFormularyRepository(source_id=SOURCE_A)
    dataset = await repository.begin_dataset(
        run_id="synthetic-restart-run",
        cutoff_at=RUN_CUTOFF,
        acquisition_contract_hash=coverage_plan.acquisition_contract_hash,
        intent="none",
    )
    coverage_result = await repository.put_coverage_plan(
        dataset=dataset,
        plan=coverage_plan.plans[0],
    )
    second_alias = next(
        alias
        for alias in coverage_result.aliases
        if alias.source_plan_identifier == "SYNTH-B"
    )
    await repository.save_checkpoint(
        CheckpointWrite(
            dataset=dataset,
            alias=second_alias,
            fence_token=1,
            acquisition_mode="full",
            expected_count=1,
            processed_count=0,
            membership_hash=None,
            completed=False,
        )
    )


async def _assert_interrupted_state() -> None:
    candidate_by_field = row_mapping(
        await db.first(
            f"SELECT status, error_json FROM {table_name('fhir_formulary_dataset')} "
            "WHERE source_id = :source_id AND run_id = :run_id;",
            source_id=SOURCE_A,
            run_id="synthetic-restart-run",
        )
    )
    completed_count = await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_checkpoint')} "
        "WHERE source_id = :source_id AND run_id = :run_id "
        "AND completed = true;",
        source_id=SOURCE_A,
        run_id="synthetic-restart-run",
    )
    assert candidate_by_field == {
        "status": "building",
        "error_json": {"resumable": True, "type": "FHIRTransportError"},
    }
    assert completed_count == 1


async def _assert_restart_state(
    seed_dataset_id: str,
    first_client: _CensusClient,
    restart_client: _CensusClient,
) -> None:
    assert first_client.medication_aliases == ["SYNTH-A", "SYNTH-B"]
    assert restart_client.medication_aliases == ["SYNTH-B"]
    current_dataset_id = await db.scalar(
        f"SELECT dataset_id FROM {table_name('fhir_formulary_current')} "
        "WHERE source_id = :source_id;",
        source_id=SOURCE_A,
    )
    candidate_by_field = await db.first(
        f"SELECT status, error_json FROM {table_name('fhir_formulary_dataset')} "
        "WHERE source_id = :source_id AND run_id = :run_id;",
        source_id=SOURCE_A,
        run_id="synthetic-restart-run",
    )
    checkpoint_count = await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_checkpoint')} "
        "WHERE source_id = :source_id AND run_id = :run_id "
        "AND completed = true;",
        source_id=SOURCE_A,
        run_id="synthetic-restart-run",
    )
    source_b_dataset_count = await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_dataset')} "
        "WHERE source_id = :source_id;",
        source_id=SOURCE_B,
    )
    checkpoint_rows = await db.all(
        f"SELECT source_plan_identifier, fence_token, acquisition_mode, "
        f"completed FROM {table_name('fhir_formulary_checkpoint')} "
        "WHERE source_id = :source_id AND run_id = :run_id "
        "ORDER BY source_plan_identifier;",
        source_id=SOURCE_A,
        run_id="synthetic-restart-run",
    )
    assert current_dataset_id == seed_dataset_id
    assert row_mapping(candidate_by_field) == {
        "status": "verified",
        "error_json": None,
    }
    assert checkpoint_count == 2
    assert source_b_dataset_count == 0
    assert [row_mapping(checkpoint_row) for checkpoint_row in checkpoint_rows] == [
        {
            "source_plan_identifier": "SYNTH-A",
            "fence_token": 1,
            "acquisition_mode": "reuse",
            "completed": True,
        },
        {
            "source_plan_identifier": "SYNTH-B",
            "fence_token": 2,
            "acquisition_mode": "full",
            "completed": True,
        },
    ]


@pytest.mark.asyncio
async def test_synchronizer_postgres_restart_reuse_and_no_publication(monkeypatch):
    """Prove atomic alias restart and unchanged publication across sources."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    try:
        await _prepare_repository_schema(
            monkeypatch,
            database_url,
            schema_name,
            engine,
        )
        await _enable_source(database_url, schema_name)
        seed_dataset_id = await _publish_predecessor()
        first_factory = _ClientFactory(fail_alias="SYNTH-B")
        with pytest.raises(FHIRTransportError, match="synthetic transient"):
            await synchronize_verified_dataset(
                source_id=SOURCE_A,
                run_id="synthetic-restart-run",
                cutoff=RUN_CUTOFF,
                client_factory=first_factory,
            )
        await _assert_interrupted_state()
        await _write_incomplete_second_alias()
        restart_factory = _ClientFactory()
        synchronization_result = await synchronize_verified_dataset(
            source_id=SOURCE_A,
            run_id="synthetic-restart-run",
            cutoff=RUN_CUTOFF,
            client_factory=restart_factory,
        )
        assert synchronization_result.reused_aliases == 1
        assert synchronization_result.full_aliases == 1
        assert synchronization_result.resumed_aliases == 1
        assert synchronization_result.alias_count == 2
        first_client = first_factory.client
        restart_client = restart_factory.client
        assert first_client is not None and restart_client is not None
        await _assert_restart_state(
            seed_dataset_id,
            first_client,
            restart_client,
        )
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
