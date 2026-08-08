# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for the reviewed verification candidate."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import db
from process.formulary_fhir.continuation import coverage_plan_search_contract
from process.formulary_fhir.continuation import medication_search_contract
import process.formulary_fhir.reviewed_source as reviewed_module
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.reviewed_source import register_reviewed_source
from process.formulary_fhir.reviewed_source import reviewed_source_manifest
from process.formulary_fhir.reviewed_source import (
    verify_reviewed_source_candidate,
)
from process.formulary_fhir.types import CurrentVersionCensus
from tests.test_formulary_fhir_repository_postgres import _configure_database
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _load_migration
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action


FIXTURES = Path(__file__).parent / "fixtures" / "formulary_fhir"
CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)
ALIASES = ("SYNTH-A", "SYNTH-B")


def _fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _set_alias(resource_by_field: dict[str, object], alias: str) -> None:
    for extension_by_field in resource_by_field["extension"]:
        if "PlanID-extension" in str(extension_by_field.get("url")):
            extension_by_field["valueString"] = alias


def _alias_medications(
    alias: str,
    correction_prefix: str,
) -> tuple[dict[str, object], ...]:
    alias_token = alias.lower()
    raw_target_id = f"target-{alias_token}"
    corrected_target_id = f"{correction_prefix}{raw_target_id}"
    referencing_medication = _fixture("medication_a.json")
    referencing_medication["id"] = f"source-{alias_token}"
    _set_alias(referencing_medication, alias)
    for extension_by_field in referencing_medication["extension"]:
        if "DrugAlternatives-extension" in str(extension_by_field.get("url")):
            extension_by_field["valueReference"]["reference"] = (
                f"MedicationKnowledge/{raw_target_id}"
            )
    corrected_medication = _fixture("medication_b.json")
    corrected_medication["id"] = corrected_target_id
    _set_alias(corrected_medication, alias)
    return referencing_medication, corrected_medication


class _ReviewedCensusClient:
    def __init__(self, config) -> None:
        self.config = config
        self.medication_aliases: list[str] = []
        self.request_count = 0
        self.transient_retry_count = 0
        self.throttle_count = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error) -> None:
        return None

    async def coverage_plan_current_census(self, *, cutoff):
        self.request_count += 1
        return CurrentVersionCensus(
            "List",
            cutoff,
            1,
            (_fixture("coverage_plan.json"),),
            coverage_plan_search_contract(self.config, cutoff).contract_hash,
        )

    async def medication_current_census(self, alias, *, cutoff):
        self.request_count += 1
        self.medication_aliases.append(alias)
        correction = reviewed_source_manifest().alternative_correction
        medications = _alias_medications(alias, correction.prefix)
        return CurrentVersionCensus(
            "MedicationKnowledge",
            cutoff,
            len(medications),
            medications,
            medication_search_contract(
                self.config,
                alias,
                cutoff,
            ).contract_hash,
        )


class _ClientFactory:
    def __init__(self) -> None:
        self.clients: list[_ReviewedCensusClient] = []

    def __call__(self, config) -> _ReviewedCensusClient:
        census_client = _ReviewedCensusClient(config)
        self.clients.append(census_client)
        return census_client


async def _prepare_empty_schema(
    monkeypatch,
    database_url,
    schema_name: str,
    engine,
) -> None:
    _configure_database(monkeypatch, database_url, schema_name)
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(
            f"CREATE SCHEMA {_quoted(schema_name)}"
        )
    await _run_migration_action(engine, _load_migration(), "upgrade")
    await db.disconnect()


async def _assert_registered_source() -> None:
    manifest = reviewed_source_manifest()
    source_by_field = row_mapping(
        await db.first(
            f"SELECT source_id, canonical_base, display_name, enabled, "
            f"runtime_config_json, metadata_json FROM "
            f"{table_name('fhir_formulary_source')};"
        )
    )
    assert source_by_field == reviewed_module._source_values(manifest)
    assert await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_source')};"
    ) == 1


async def _assert_nonpublishing_dataset(dataset_id: str) -> None:
    dataset_by_field = row_mapping(
        await db.first(
            f"SELECT status, publish_requested, seed_eligible, list_count, "
            f"alias_count, medication_count FROM "
            f"{table_name('fhir_formulary_dataset')} WHERE "
            "dataset_id = :dataset_id;",
            dataset_id=dataset_id,
        )
    )
    assert dataset_by_field == {
        "status": "verified",
        "publish_requested": False,
        "seed_eligible": False,
        "list_count": 1,
        "alias_count": 2,
        "medication_count": 4,
    }
    assert await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_current')};"
    ) == 0


async def _assert_correction_evidence() -> None:
    correction = reviewed_source_manifest().alternative_correction
    evidence_rows = [
        row_mapping(evidence_row)
        for evidence_row in await db.all(
            f"SELECT raw_reference, corrected_reference, "
            f"resolved_medication_id, resolved, rule_version FROM "
            f"{table_name('fhir_formulary_alternative')} "
            "ORDER BY raw_reference;"
        )
    ]
    assert len(evidence_rows) == len(ALIASES)
    for alias, evidence_by_field in zip(ALIASES, evidence_rows, strict=True):
        raw_target_id = f"target-{alias.lower()}"
        corrected_target_id = f"{correction.prefix}{raw_target_id}"
        assert evidence_by_field == {
            "raw_reference": f"MedicationKnowledge/{raw_target_id}",
            "corrected_reference": (
                f"MedicationKnowledge/{corrected_target_id}"
            ),
            "resolved_medication_id": corrected_target_id,
            "resolved": True,
            "rule_version": correction.rule_version,
        }


@pytest.mark.asyncio
async def test_reviewed_candidate_exact_replay_storage_and_no_publication(
    monkeypatch,
):
    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    client_factory = _ClientFactory()
    try:
        await _prepare_empty_schema(monkeypatch, database_url, schema_name, engine)
        first_result = await verify_reviewed_source_candidate(
            run_id="reviewed-candidate-run",
            cutoff=CUTOFF,
            client_factory=client_factory,
        )
        assert first_result.list_count == 1
        assert first_result.alias_count == 2
        assert first_result.medication_membership_count == 4
        assert first_result.full_aliases == 2
        assert client_factory.clients[0].medication_aliases == list(ALIASES)
        await _assert_registered_source()
        await _assert_nonpublishing_dataset(first_result.dataset_id)
        await _assert_correction_evidence()

        replay_result = await verify_reviewed_source_candidate(
            run_id="reviewed-candidate-run",
            cutoff=CUTOFF,
            client_factory=client_factory,
        )
        replay_binding = await register_reviewed_source()
        assert replay_result.dataset_id == first_result.dataset_id
        assert replay_result.resumed_aliases == 2
        assert client_factory.clients[1].medication_aliases == []
        assert replay_binding.alternative_correction is not None
        await _assert_registered_source()
        await _assert_nonpublishing_dataset(first_result.dataset_id)
        await _assert_correction_evidence()
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
