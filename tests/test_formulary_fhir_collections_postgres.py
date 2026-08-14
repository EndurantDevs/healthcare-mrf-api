# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for current formulary collection serving."""

from __future__ import annotations

import base64
from copy import deepcopy
import datetime as dt
from pathlib import Path
import uuid

import orjson
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.formulary_fhir_catalog import public_fhir_formulary_alias_page_payload
from api.formulary_fhir_catalog import public_fhir_formulary_page_payload
from api.formulary_fhir_catalog import read_current_fhir_formularies
from api.formulary_fhir_catalog import read_current_fhir_formulary_aliases
from api.formulary_fhir_drugs import read_current_fhir_formulary_drug
from api.formulary_fhir_drugs import read_current_fhir_formulary_drug_page
from api.formulary_fhir_drug_values import FHIRFormularyDrugFilters
from api.formulary_fhir_drug_values import public_fhir_formulary_drug_page_payload
from api.formulary_fhir_drug_values import public_fhir_formulary_drug_payload
from api.formulary_fhir_serving import FHIRFormularyNotFoundError
from db.models import db, FHIRFormularyDataset
from process.formulary_fhir.continuation import coverage_plan_search_contract
from process.formulary_fhir.reviewed_acquisition import acquire_reviewed_twins
from process.formulary_fhir.reviewed_operation import ACQUISITION_ENABLED_ENV
from process.formulary_fhir.reviewed_operation import PUBLICATION_ENABLED_ENV
from process.formulary_fhir.reviewed_publication import publish_reviewed_candidate
from process.formulary_fhir.types import CurrentVersionCensus
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.test_formulary_fhir_reviewed_source_postgres import (
    _ReviewedCensusClient,
)
from tests.test_formulary_fhir_storage_postgres import _connect
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action
from tests.test_formulary_fhir_twin_repository_postgres import _prepare_schema


ROOT = Path(__file__).resolve().parents[1]
SERVING_INDEX_PATH = ROOT / "alembic" / "versions" / (
    "20260808160000_fhir_formulary_serving_index.py"
)
COVERAGE_MIGRATION_PATHS = tuple(
    ROOT / "alembic" / "versions" / migration_name
    for migration_name in (
        "20260810030000_fhir_formulary_source_artifact.py",
        "20260810040000_fhir_formulary_uhc_admission_receipt.py",
        "20260814010000_fhir_formulary_uhc_selected_receipt.py",
    )
)
FIRST_CUTOFF = dt.datetime(2026, 8, 7, 6, tzinfo=dt.UTC)
PENDING_CUTOFF = dt.datetime(2026, 8, 7, 7, tzinfo=dt.UTC)
PLAN_ALIASES = (
    ("SYNTH-A", "SYNTH-B"),
    ("SYNTH-C", "SYNTH-D"),
    ("SYNTH-E", "SYNTH-F"),
)
SERVING_ENV = {
    "HLTHPRT_FHIR_FORMULARY_SERVING_ENABLED": "true",
    "HLTHPRT_FHIR_FORMULARY_CURSOR_KEY": base64.urlsafe_b64encode(
        b"synthetic-formulary-cursor-key-1"
    )
    .decode("ascii")
    .rstrip("="),
}


class _CatalogCensusClient(_ReviewedCensusClient):
    """Return a small exact multi-plan graph with private alternatives."""

    def __init__(self, config, plan_count: int) -> None:
        super().__init__(config)
        self.plan_count = plan_count

    async def coverage_plan_current_census(self, *, cutoff):
        base_census = await super().coverage_plan_current_census(cutoff=cutoff)
        base_plan = base_census.resources[0]
        plans = []
        for plan_index, aliases in enumerate(PLAN_ALIASES[: self.plan_count]):
            plan = deepcopy(base_plan)
            plan["id"] = f"synthetic-catalog-{plan_index + 1}"
            plan["title"] = f"Synthetic catalog {plan_index + 1}"
            plan["name"] = f"Synthetic formulary {plan_index + 1}"
            plan["identifier"][0]["value"] = f"CATALOG-{plan_index + 1}"
            retained_extensions = [
                deepcopy(extension)
                for extension in plan["extension"]
                if "PlanID-extension" not in str(extension.get("url"))
            ]
            plan_id_url = next(
                extension["url"]
                for extension in base_plan["extension"]
                if "PlanID-extension" in str(extension.get("url"))
            )
            plan["extension"] = retained_extensions + [
                {"url": plan_id_url, "valueString": alias}
                for alias in aliases
            ]
            plans.append(plan)
        return CurrentVersionCensus(
            "List",
            cutoff,
            len(plans),
            tuple(plans),
            coverage_plan_search_contract(self.config, cutoff).contract_hash,
        )

    async def medication_current_census(self, alias, *, cutoff):
        base_census = await super().medication_current_census(
            alias,
            cutoff=cutoff,
        )
        medications = [deepcopy(resource) for resource in base_census.resources]
        alternative = next(
            extension
            for extension in medications[0]["extension"]
            if "DrugAlternatives-extension" in str(extension.get("url"))
        )
        unresolved = deepcopy(alternative)
        unresolved["valueReference"]["reference"] = (
            f"MedicationKnowledge/missing-{alias.lower()}"
        )
        medications[0]["extension"].append(unresolved)
        return CurrentVersionCensus(
            "MedicationKnowledge",
            cutoff,
            len(medications),
            tuple(medications),
            base_census.search_contract_hash,
        )


class _CatalogClientFactory:
    def __init__(self, plan_count: int) -> None:
        self.plan_count = plan_count

    def __call__(self, config) -> _CatalogCensusClient:
        return _CatalogCensusClient(config, self.plan_count)


async def _acquire(monkeypatch, cutoff: dt.datetime, plan_count: int):
    monkeypatch.setenv(ACQUISITION_ENABLED_ENV, "true")
    monkeypatch.delenv(PUBLICATION_ENABLED_ENV, raising=False)
    return await acquire_reviewed_twins(
        cutoff=cutoff,
        client_factory=_CatalogClientFactory(plan_count),
    )


async def _publish(monkeypatch, cutoff: dt.datetime):
    monkeypatch.delenv(ACQUISITION_ENABLED_ENV, raising=False)
    monkeypatch.setenv(PUBLICATION_ENABLED_ENV, "true")
    return await publish_reviewed_candidate(cutoff=cutoff)


async def _fingerprints(connection, schema_name: str):
    table_rows = await connection.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = $1 "
        "AND tablename LIKE 'fhir_formulary_%' ORDER BY tablename",
        schema_name,
    )
    fingerprint_by_table = {}
    for table_row in table_rows:
        table_name = str(table_row["tablename"])
        fingerprint = await connection.fetchrow(
            f"SELECT count(*) AS row_count, md5(COALESCE(string_agg("
            f"to_jsonb(stored)::text, E'\\n' ORDER BY "
            f"to_jsonb(stored)::text), '')) AS content_hash FROM "
            f"{_quoted(schema_name)}.{_quoted(table_name)} AS stored"
        )
        fingerprint_by_table[table_name] = (
            int(fingerprint["row_count"]),
            str(fingerprint["content_hash"]),
        )
    return fingerprint_by_table


async def _dataset_plan_ids(connection, schema_name: str, dataset_id: str):
    rows = await connection.fetch(
        f"SELECT public_id FROM {_quoted(schema_name)}."
        "fhir_formulary_dataset_coverage_plan WHERE dataset_id = $1 "
        "ORDER BY public_id",
        dataset_id,
    )
    return tuple(str(row["public_id"]) for row in rows)


async def _private_values(connection, schema_name: str) -> set[str]:
    schema = _quoted(schema_name)
    rows = await connection.fetch(
        f"SELECT source_id::text AS value FROM {schema}.fhir_formulary_source "
        f"UNION ALL SELECT upstream_list_id FROM {schema}.fhir_formulary_coverage_plan "
        f"UNION ALL SELECT canonical_identity FROM {schema}.fhir_formulary_coverage_plan "
        f"UNION ALL SELECT source_plan_identifier FROM "
        f"{schema}.fhir_formulary_drug_plan_alias "
        f"UNION ALL SELECT upstream_medication_id FROM "
        f"{schema}.fhir_formulary_medication "
        f"UNION ALL SELECT raw_reference FROM {schema}.fhir_formulary_alternative "
        f"UNION ALL SELECT corrected_reference FROM "
        f"{schema}.fhir_formulary_alternative WHERE corrected_reference IS NOT NULL "
        f"UNION ALL SELECT resolved_medication_id FROM "
        f"{schema}.fhir_formulary_alternative WHERE resolved_medication_id IS NOT NULL"
    )
    return {str(row["value"]) for row in rows if row["value"]}


async def _read_formulary_pages(session, current_ids: tuple[str, ...]):
    first_forms = await read_current_fhir_formularies(
        session,
        limit=1,
        environment=SERVING_ENV,
    )
    assert first_forms.next_cursor is not None
    second_forms = await read_current_fhir_formularies(
        session,
        limit=1,
        cursor=first_forms.next_cursor,
        environment=SERVING_ENV,
    )
    assert second_forms.next_cursor is None
    assert tuple(
        formulary_detail.formulary_id
        for formulary_page in (first_forms, second_forms)
        for formulary_detail in formulary_page.items
    ) == current_ids
    return first_forms, second_forms


async def _read_alias_pages(session, formulary_id: str):
    first_aliases = await read_current_fhir_formulary_aliases(
        session,
        formulary_id,
        limit=1,
        environment=SERVING_ENV,
    )
    assert first_aliases.next_cursor is not None
    second_aliases = await read_current_fhir_formulary_aliases(
        session,
        formulary_id,
        limit=1,
        cursor=first_aliases.next_cursor,
        environment=SERVING_ENV,
    )
    assert second_aliases.next_cursor is None
    return first_aliases, second_aliases, first_aliases.items[0].alias_id


async def _read_drug_pages(session, formulary_id: str, alias_id: str):
    filters = FHIRFormularyDrugFilters()
    first_drugs = await read_current_fhir_formulary_drug_page(
        session,
        formulary_id,
        alias_id,
        filters=filters,
        limit=1,
        environment=SERVING_ENV,
    )
    assert first_drugs.next_cursor is not None
    second_drugs = await read_current_fhir_formulary_drug_page(
        session,
        formulary_id,
        alias_id,
        filters=filters,
        limit=1,
        cursor=first_drugs.next_cursor,
        environment=SERVING_ENV,
    )
    assert second_drugs.next_cursor is None
    drugs = first_drugs.items + second_drugs.items
    drug_ids = {drug.drug_id for drug in drugs}
    owner = next(drug for drug in drugs if drug.alternatives.resolved_drug_ids)
    assert set(owner.alternatives.resolved_drug_ids) < drug_ids
    assert owner.alternatives.unresolved_count == 1
    assert await read_current_fhir_formulary_drug(
        session,
        formulary_id,
        alias_id,
        owner.drug_id,
        environment=SERVING_ENV,
    ) == owner
    return first_drugs, second_drugs, owner


async def _read_public_graph(session, current_ids: tuple[str, ...]):
    """Page through the complete public graph and re-read one exact drug."""

    first_forms, second_forms = await _read_formulary_pages(session, current_ids)
    first_aliases, second_aliases, alias_id = await _read_alias_pages(
        session,
        current_ids[0],
    )
    first_drugs, second_drugs, owner = await _read_drug_pages(
        session,
        current_ids[0],
        alias_id,
    )
    return (
        first_forms,
        second_forms,
        first_aliases,
        second_aliases,
        first_drugs,
        second_drugs,
        owner,
    )


def _public_document(pages) -> dict[str, object]:
    return {
        "formularies": [
            public_fhir_formulary_page_payload(page) for page in pages[:2]
        ],
        "aliases": [
            public_fhir_formulary_alias_page_payload(page) for page in pages[2:4]
        ],
        "drugs": [
            public_fhir_formulary_drug_page_payload(page) for page in pages[4:6]
        ],
        "drug": public_fhir_formulary_drug_payload(pages[6]),
    }


async def _prepare_published_graph(
    monkeypatch,
    database_url,
    schema_name: str,
    migration_engine,
):
    await _prepare_schema(
        monkeypatch,
        database_url,
        schema_name,
        migration_engine,
    )
    for index, migration_path in enumerate(COVERAGE_MIGRATION_PATHS):
        coverage_migration = load_migration(
            migration_path,
            f"formulary_collections_coverage_{index}",
        )
        await _run_migration_action(
            migration_engine,
            coverage_migration,
            "upgrade",
        )
    serving_migration = load_migration(
        SERVING_INDEX_PATH,
        "formulary_collections_serving_index",
    )
    await _run_migration_action(migration_engine, serving_migration, "upgrade")
    first_admission = await _acquire(monkeypatch, FIRST_CUTOFF, 2)
    publication = await _publish(monkeypatch, FIRST_CUTOFF)
    assert publication.candidate_dataset_id == first_admission.candidate_dataset_id
    pending_admission = await _acquire(monkeypatch, PENDING_CUTOFF, 3)
    return publication, pending_admission


async def _assert_served_graph(
    connection,
    session_factory,
    schema_name: str,
    publication,
    pending_admission,
) -> None:
    current_ids = await _dataset_plan_ids(
        connection,
        schema_name,
        publication.candidate_dataset_id,
    )
    pending_ids = await _dataset_plan_ids(
        connection,
        schema_name,
        pending_admission.candidate_dataset_id,
    )
    assert len(current_ids) == 2
    assert len(pending_ids) == 3
    hidden_ids = set(pending_ids) - set(current_ids)
    assert len(hidden_ids) == 1
    baseline = await _fingerprints(connection, schema_name)
    async with session_factory() as session:
        pages = await _read_public_graph(session, current_ids)
        with pytest.raises(FHIRFormularyNotFoundError):
            await read_current_fhir_formulary_aliases(
                session,
                hidden_ids.pop(),
                limit=1,
                environment=SERVING_ENV,
            )
    public_text = orjson.dumps(_public_document(pages)).decode("utf-8")
    for private_value in await _private_values(connection, schema_name):
        assert private_value not in public_text
    assert await _fingerprints(connection, schema_name) == baseline


@pytest.mark.asyncio
async def test_full_chain_current_collections_are_paginated_private_and_read_only(
    monkeypatch,
):
    """Serve only current public IDs from a fully guarded published graph."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    migration_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    model_schema = FHIRFormularyDataset.__table__.schema
    serving_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"),
        execution_options={"schema_translate_map": {model_schema: schema_name}},
    )
    session_factory = async_sessionmaker(serving_engine, expire_on_commit=False)
    connection = None
    try:
        publication, pending_admission = await _prepare_published_graph(
            monkeypatch,
            database_url,
            schema_name,
            migration_engine,
        )
        connection = await _connect(database_url)
        await _assert_served_graph(
            connection,
            session_factory,
            schema_name,
            publication,
            pending_admission,
        )
    finally:
        await db.disconnect()
        if connection is not None:
            await connection.close()
        await serving_engine.dispose()
        await _drop_schema(migration_engine, schema_name)
        await migration_engine.dispose()
