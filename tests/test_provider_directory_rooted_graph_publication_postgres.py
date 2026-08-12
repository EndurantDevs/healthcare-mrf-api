# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable-PostgreSQL rooted graph twin and publication lifecycle proof."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.connection import Database
from process.provider_directory_dataset_scoped_publication import (
    exact_uhc_dataset_pair,
)
from process.provider_directory_rooted_graph_publication import (
    publish_provider_directory_rooted_graph_dataset,
)
from process.provider_directory_rooted_graph_registration import (
    register_provider_directory_rooted_graph_source,
)
from process.provider_directory_rooted_graph_store import (
    claim_provider_directory_rooted_graph_census,
    claim_provider_directory_rooted_graph_work,
    complete_provider_directory_rooted_graph_error,
    complete_provider_directory_rooted_graph_result,
    initialize_provider_directory_rooted_graph_acquisition,
    seal_provider_directory_rooted_graph_acquisition,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
)
from process.provider_directory_rooted_graph_twin_store import (
    admit_provider_directory_rooted_graph_twins,
    ProviderDirectoryRootedGraphTwinError,
)
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.test_provider_directory_rooted_graph_acquisition_postgres import (
    _complete_success,
    _configure_database,
    _extend_publication_foundation,
    _identity,
    _load_legacy_migrations,
    _query_result_for_work,
    _work_rows,
    LEGACY_ACQUISITION_PATH,
    LEGACY_COHORT_PATH,
    LEGACY_PUBLICATION_PATH,
    LEGACY_TWIN_PATH,
    MIGRATION_PATH,
    SINGLE_ROOT_MIGRATION_PATH,
)
from tests.provider_directory_rooted_graph_pg_assertions import (
    assert_missing_terminal_witnesses,
    assert_witness_immutability,
)
from tests.provider_directory_rooted_graph_rotation_pg_support import (
    locked_exact_current as _locked_exact_current,
    prove_recursive_rooted_rotation,
    prove_stale_legacy_replacement,
    publish_legacy_root as _publish_legacy_root,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    _prepare_publication_schema,
)


@asynccontextmanager
async def _lifecycle_scope(monkeypatch):
    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    migration = load_migration(MIGRATION_PATH, "rooted_graph_lifecycle_migration")
    single_root_migration = load_migration(
        SINGLE_ROOT_MIGRATION_PATH,
        "rooted_graph_lifecycle_single_root",
    )
    legacy_migrations = _load_legacy_migrations("rooted_graph_lifecycle_legacy")
    database = _configure_database(monkeypatch, url)
    connection = None
    try:
        await _prepare_publication_schema(
            engine, url, schema_name, schema, legacy_migrations
        )
        connection = await connect(url)
        await _extend_publication_foundation(connection, schema_name)
        await database.connect()
        registration = await register_provider_directory_rooted_graph_source(
            database=database
        )
        assert registration.endpoint_created and registration.source_created
        await run_migration(engine, migration, "upgrade")
        await run_migration(engine, single_root_migration, "upgrade")
        replay = await register_provider_directory_rooted_graph_source(
            database=database
        )
        assert not replay.endpoint_created and not replay.source_created
        yield SimpleNamespace(
            database=database,
            connection=connection,
            engine=engine,
            migration=migration,
            schema=schema,
            schema_name=schema_name,
        )
    finally:
        await database.disconnect()
        if connection is not None:
            await connection.close()
        await drop_schema(engine, schema_name)
        await engine.dispose()


async def _complete_error_acquisition(database: Database, current) -> None:
    error_identity = _identity(current, "baseline", "6", "7")
    await initialize_provider_directory_rooted_graph_acquisition(
        error_identity, database=database
    )
    work_records = await _work_rows(database, error_identity.acquisition_id)
    for work_index, work_record in enumerate(work_records):
        active_claim = await claim_provider_directory_rooted_graph_work(
            error_identity.acquisition_id,
            query_id=work_record.query_id,
            database=database,
        )
        assert active_claim is not None
        if work_index == 0:
            await complete_provider_directory_rooted_graph_error(
                active_claim, error_code="response_invalid", database=database
            )
        else:
            await complete_provider_directory_rooted_graph_result(
                active_claim,
                _query_result_for_work(active_claim, work_record.kind),
                database=database,
            )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await claim_provider_directory_rooted_graph_census(
            error_identity, database=database
        )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await seal_provider_directory_rooted_graph_acquisition(
            error_identity, database=database
        )


def _assert_matching_roots(baseline_summary, candidate_summary) -> None:
    assert candidate_summary.rooted_graph_sha256 == baseline_summary.rooted_graph_sha256
    assert candidate_summary.terminal_set_sha256 == baseline_summary.terminal_set_sha256
    assert baseline_summary.rooted_graph_complete is True
    assert baseline_summary.endpoint_collection_complete is False
    assert baseline_summary.endpoint_complete is False


async def _prove_mismatch(context, current) -> None:
    mismatch_baseline = _identity(current, "baseline", "4", "6")
    mismatch_candidate = _identity(current, "candidate", "5", "6")
    await _complete_success(context.database, mismatch_baseline)
    await _complete_success(
        context.database, mismatch_candidate, missing_http_status=410
    )
    with pytest.raises(ProviderDirectoryRootedGraphTwinError) as mismatch:
        await admit_provider_directory_rooted_graph_twins(
            mismatch_baseline.acquisition_id,
            mismatch_candidate.acquisition_id,
            database=context.database,
        )
    assert mismatch.value.code == "mismatch"
    assert (
        await context.database.scalar(
            f"SELECT count(*) FROM {context.schema}."
            "provider_directory_rooted_graph_twin_attempt WHERE matched IS FALSE"
        )
        == 1
    )
    assert (
        await context.database.scalar(
            f"SELECT count(*) FROM {context.schema}."
            "provider_directory_rooted_graph_twin_admission WHERE "
            "publication_acquisition_id = :acquisition_id",
            acquisition_id=mismatch_candidate.acquisition_id,
        )
        == 0
    )


async def _publish_generation(context, current, run_digits, intent_digit):
    baseline = _identity(current, "baseline", run_digits[0], intent_digit)
    candidate = _identity(current, "candidate", run_digits[1], intent_digit)
    baseline_summary = await _complete_success(context.database, baseline)
    candidate_summary = await _complete_success(context.database, candidate)
    _assert_matching_roots(baseline_summary, candidate_summary)
    admission = await admit_provider_directory_rooted_graph_twins(
        baseline.acquisition_id,
        candidate.acquisition_id,
        database=context.database,
    )
    assert admission.publication_acquisition_id == candidate.acquisition_id
    published = await publish_provider_directory_rooted_graph_dataset(
        candidate.acquisition_id, database=context.database, batch_size=4
    )
    assert published.replayed is False
    assert published.readiness.previous_dataset_id == current.dataset_id
    return baseline, baseline_summary, candidate, published


async def _prove_first_generation(context, current):
    baseline = _identity(current, "baseline", "1", "3")
    candidate = _identity(current, "candidate", "2", "3")
    baseline_summary = await _complete_success(
        context.database,
        baseline,
        reclaim_connection=context.connection,
        schema_name=context.schema_name,
    )
    candidate_summary = await _complete_success(context.database, candidate)
    _assert_matching_roots(baseline_summary, candidate_summary)
    await assert_missing_terminal_witnesses(context.connection, context.schema_name)
    await assert_witness_immutability(context.connection, context.schema_name)
    await _complete_error_acquisition(context.database, current)
    await _prove_mismatch(context, current)
    admission = await admit_provider_directory_rooted_graph_twins(
        baseline.acquisition_id,
        candidate.acquisition_id,
        database=context.database,
    )
    published = await publish_provider_directory_rooted_graph_dataset(
        admission.publication_acquisition_id,
        database=context.database,
        batch_size=4,
    )
    assert published.readiness.resource_counts == {
        "Practitioner": 1,
        "PractitionerRole": 1,
        "OrganizationAffiliation": 0,
        "Organization": 1,
        "Location": 0,
        "HealthcareService": 0,
        "InsurancePlan": 1,
        "Endpoint": 0,
    }
    replay = await publish_provider_directory_rooted_graph_dataset(
        candidate.acquisition_id, database=context.database
    )
    assert replay.replayed and replay.readiness == published.readiness
    return baseline, baseline_summary, published


async def _prove_stale_and_legacy(context, rooted_current, previous_ids) -> None:
    stale_baseline = _identity(rooted_current, "baseline", "a", "5")
    stale_candidate = _identity(rooted_current, "candidate", "b", "5")
    await _complete_success(context.database, stale_baseline)
    await _complete_success(context.database, stale_candidate)
    legacy_current = await _publish_legacy_root(context.database, "2" * 64)
    assert legacy_current.variant == "uhc_flex_practitioner"
    assert legacy_current.dataset_id not in previous_ids
    with pytest.raises(ProviderDirectoryRootedGraphTwinError) as stale:
        await admit_provider_directory_rooted_graph_twins(
            stale_baseline.acquisition_id,
            stale_candidate.acquisition_id,
            database=context.database,
        )
    assert stale.value.code == "stale"
    assert (
        await context.database.scalar(
            f"SELECT count(*) FROM {context.schema}."
            "provider_directory_rooted_graph_twin_admission WHERE "
            "publication_acquisition_id = :acquisition_id",
            acquisition_id=stale_candidate.acquisition_id,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_rooted_graph_twin_publication_generations_and_fences(
    monkeypatch,
) -> None:
    async with _lifecycle_scope(monkeypatch) as context:
        legacy_current = await _publish_legacy_root(context.database)
        baseline, baseline_summary, first = await _prove_first_generation(
            context, legacy_current
        )
        rooted_current = await _locked_exact_current(context.database)
        assert rooted_current is not None
        _, _, _, second = await _publish_generation(context, rooted_current, "89", "4")
        second_current = await _locked_exact_current(context.database)
        assert second_current is not None
        await _prove_stale_and_legacy(
            context,
            second_current,
            {
                legacy_current.dataset_id,
                first.readiness.dataset_id,
                second.readiness.dataset_id,
            },
        )
        assert (
            await context.database.scalar(
                f"SELECT count(*) FROM {context.schema}."
                "provider_directory_endpoint_dataset WHERE is_current IS TRUE "
                "AND endpoint_id IN (:legacy_endpoint_id, :rooted_endpoint_id)",
                legacy_endpoint_id=exact_uhc_dataset_pair().legacy_endpoint_id,
                rooted_endpoint_id=exact_uhc_dataset_pair().rooted_endpoint_id,
            )
            == 1
        )
        assert (
            await initialize_provider_directory_rooted_graph_acquisition(
                baseline, database=context.database
            )
            == 0
        )
        replayed_summary = await seal_provider_directory_rooted_graph_acquisition(
            baseline, database=context.database
        )
        assert (
            replayed_summary.rooted_graph_sha256 == baseline_summary.rooted_graph_sha256
        )
        with pytest.raises(DBAPIError, match="downgrade_blocked"):
            await run_migration(context.engine, context.migration, "downgrade")


@pytest.mark.asyncio
async def test_official_rotation_allows_stale_legacy_replacement(
    monkeypatch,
) -> None:
    async with _lifecycle_scope(monkeypatch) as context:
        await prove_stale_legacy_replacement(context)


@pytest.mark.asyncio
async def test_recursive_rooted_lineage_revokes_and_recovers_after_rotation(
    monkeypatch,
) -> None:
    async with _lifecycle_scope(monkeypatch) as context:
        await prove_recursive_rooted_rotation(context, _publish_generation)
