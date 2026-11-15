# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable-PostgreSQL proof for partial Flex-to-rooted lineage."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from sqlalchemy.exc import DBAPIError

import process.uhc_flex_practitioner_publication as flex_publication
from db.migration_rooted_publication_freeze import native_receipt_sql
from process.provider_directory_dataset_scoped_publication import (
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
)
from process.provider_directory_rooted_graph_publication_store import read_prepared_rooted_publication
from tests.formulary_fhir_twin_admission_pg_support import connect, database_url


async def _publish_and_verify_partial_rooted_graph(context, monkeypatch) -> None:
    """Publish one partial rooted graph and verify its stored readiness."""

    url, schema, database = context.url, context.schema, context.database
    from native_tests.fhir_source_rooted_worker_support import _SyntheticFHIR, rooted_dispatch_task
    from native_tests.test_fhir_source_worker_rooted_v2_postgres import (
        _configure_rooted_control,
        _invoke_rooted_control,
    )

    from tests.provider_directory_rooted_graph_acquisition_test_support import FakeResponse, bundle

    http = _SyntheticFHIR(FakeResponse, bundle)
    _configure_rooted_control(context, http, monkeypatch)
    task = await rooted_dispatch_task(context.current, monkeypatch, operation_key="d" * 64)
    completed = await _invoke_rooted_control(context, task)
    assert completed["status"] == "succeeded" and http.requests and http.opened == http.closed
    preparation_by_field = await context.connection.fetchrow(
        "SELECT state,handoff FROM hp_snapshot_retention.preparation WHERE handoff->'identity'->>'dataset_id'=$1",
        completed["result"]["rooted_graph"]["dataset_id"],
    )
    assert preparation_by_field is not None and preparation_by_field["state"] == "published"
    rooted = await read_prepared_rooted_publication(preparation_by_field, database=database)
    assert rooted.readiness.cohort_complete is False
    assert rooted.readiness.retry_exhausted_count == 1
    assert rooted.readiness.rooted_graph_complete is True
    assert rooted.readiness.endpoint_collection_complete is False
    assert rooted.readiness.endpoint_complete is False
    assert rooted.readiness.request_failure_coverage["failed_requests"] == 1
    assert rooted.readiness.request_failure_coverage["resource_coverage"] == "unknown"
    assert (
        await context.connection.fetchval(
            f"SELECT ({native_receipt_sql(context.schema_name, '$1')}) AND EXISTS("
            f"SELECT FROM {schema}.provider_directory_rooted_graph_dataset "
            "WHERE dataset_id=$1 AND status='published' AND is_current IS TRUE)",
            rooted.readiness.dataset_id,
        )
        is True
    )

    connection = await connect(url)
    try:
        metadata_json = await connection.fetchval(
            f"SELECT publication_metadata_json FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id = $1",
            rooted.readiness.dataset_id,
        )
    finally:
        await connection.close()
    metadata_by_field = json.loads(metadata_json)
    assert metadata_by_field["cohort_complete"] is False
    assert metadata_by_field["retry_exhausted_count"] == 1


async def _historical_partial_admission(
    candidate_acquisition_id, *, semantic_projection_as_of=None, operation_key=None, database
):
    """Use the native pre-conversion admission guard before a Source runtime can exist."""
    from process import uhc_flex_practitioner_twin_store as admissions
    from process.uhc_flex_practitioner_single_root_contract import build_single_root_admission

    async with database.transaction():
        stored = (
            await admissions._read_admission(database, candidate_acquisition_id)
            if semantic_projection_as_of is None
            else None
        )
        candidate_row = await database.first(
            f"SELECT {', '.join(admissions._ROOT_COLUMNS)} FROM {admissions.table_ref(admissions.ACQUISITION_TABLE)} "
            "WHERE acquisition_id=:candidate FOR SHARE",
            candidate=candidate_acquisition_id,
        )
        candidate = admissions._sealed_root(admissions.row_fields(candidate_row))
        expected = build_single_root_admission(
            candidate,
            semantic_projection_as_of=stored.semantic_projection_as_of if stored else semantic_projection_as_of,
            operation_key=stored.operation_key if stored else operation_key,
            admitted_at=stored.admitted_at if stored else await database.scalar("SELECT transaction_timestamp()"),
        )
        if stored is None:
            await admissions._insert_admission(database, expected)
        stored = await admissions._read_admission(database, candidate_acquisition_id)
        admissions._require_exact_admission(stored, expected)
    return stored


async def _assert_historical_lineage_guard(database, candidate_acquisition_id):
    """A different operation still fails the actual historical native admission guard."""
    from process import uhc_flex_practitioner_twin_store as admissions

    columns = admissions._ADMISSION_COLUMNS
    projection_expressions = [
        "repeat('e',64)" if name == "operation_key" else "transaction_timestamp()" if name == "admitted_at" else name
        for name in columns
    ]
    table = admissions.table_ref(admissions.ADMISSION_TABLE)
    with pytest.raises(DBAPIError, match="provider_directory_flex_single_root_admission_invalid"):
        async with database.transaction():
            await database.status(
                f"INSERT INTO {table} ({', '.join(columns)}) SELECT {', '.join(projection_expressions)} FROM {table} "
                "WHERE candidate_acquisition_id=:candidate",
                candidate=candidate_acquisition_id,
            )


async def _partial_predecessor(database, *, cohort):
    """Publish the genuine retry-exhausted cohort before the Source generation is issued."""
    from tests.test_provider_directory_uhc_flex_partial_publication_postgres import _partial_single_root

    flex_admission = await _partial_single_root(database, cohort=cohort)
    await _assert_historical_lineage_guard(database, flex_admission.candidate_acquisition_id)
    flex_result = await flex_publication.publish_uhc_flex_practitioner_dataset(
        flex_admission.candidate_acquisition_id, database=database, batch_size=1
    )
    async with database.transaction():
        current = await lock_exact_current_dataset(database, pair=exact_uhc_dataset_pair())
    assert current is not None
    assert current.dataset_id == flex_result.readiness.dataset_id
    assert current.cohort_complete is False
    assert current.retry_exhausted_count == 1
    return current


@pytest.mark.asyncio
async def test_partial_flex_root_admits_and_publishes_exact_rooted_readiness(monkeypatch) -> None:
    """Carry one exhausted Flex member through actual Source prepared publication."""
    database_url()
    core = Path(flex_publication.__file__).resolve().parents[1]
    with monkeypatch.context() as settings:
        settings.syspath_prepend(os.environ["SOURCE_WITNESS_PEER_ROOT"])
        settings.setenv("HEALTHCARE_MRF_API_SOURCE_ROOT", str(core))
        from directory_membership_test_support import _source_test_helpers

        with _source_test_helpers(settings):
            from native_tests import fhir_source_rooted_v2_support as fixture

            original_seed = fixture._seed_rooted_predecessor

            async def seed_partial(engine, environment):
                from process import uhc_flex_practitioner_publication_store as practitioner
                from tests import provider_directory_rooted_graph_rotation_pg_support as rotation
                from tests import test_provider_directory_uhc_flex_partial_publication_postgres as partial

                partial._configure_partial_cohort(environment)
                with environment.context() as precursor:
                    precursor.setattr(rotation, "publish_legacy_root", _partial_predecessor)
                    precursor.setattr(partial, "admit_uhc_flex_practitioner_single_root", _historical_partial_admission)
                    precursor.setattr(
                        practitioner, "require_uhc_flex_practitioner_admission", _historical_partial_admission
                    )
                    return await original_seed(engine, precursor)

            settings.setattr(fixture, "_seed_rooted_predecessor", seed_partial)
            async with fixture.v2_rooted_scope(settings) as context:
                await _publish_and_verify_partial_rooted_graph(context, settings)
