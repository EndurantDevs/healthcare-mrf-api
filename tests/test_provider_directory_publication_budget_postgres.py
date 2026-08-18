# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Production-sized PostgreSQL proof for provider publication budgets."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import time

import pytest

from process import provider_directory_proof_store as proof_store
from process.provider_directory_admission_seal import (
    admission_seal_from_validated_metadata,
)
from tests.provider_directory_effective_endpoint_pg_cases import (
    _load_effective_endpoint_migration,
    _split_source_endpoint_identity,
)
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)
from tests.provider_directory_reviewed_root_policy_pg import (
    _activate_policy_source,
    _insert_policy_source,
    _terminalize_candidate,
)
from tests.provider_directory_reviewed_subset_activation_pg_concurrency import (
    _close_scenario,
    _runtime_database,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    flush_deferred_fixture_events,
    load_activation_migration,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
    run_subset_migration,
)
from tests.provider_directory_subset_completion_pg_source_concurrency import (
    install_current_scoped_publication_surface,
)
from tests.provider_directory_subset_completion_pg_support import RESOURCE_TYPES
from tests.test_provider_directory_subset_completion_migration import (
    _load_publication_guard_migration,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _production_sized_single_root_inputs():
    """Return one valid generic proof with incompressible live-sized JSON."""

    source_record, dataset_rows, evidence = single_root_activation_inputs()
    dataset_row = dataset_rows[0]
    content_proof = dataset_row["publication_metadata_json"][
        importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    ]
    identity = hashlib.shake_256(
        b"provider-directory-publication-timeout-regression"
    ).hexdigest(2_750_000)
    descriptor = content_proof["shards"][0]
    descriptor["first_identity"][1] = identity
    descriptor["last_identity"][1] = identity
    content_proof["shard_set_sha256"] = proof_store._line_hash(
        proof_store._stable_json(shard).encode()
        for shard in content_proof["shards"]
    )
    content_proof.pop("proof_sha256")
    content_proof["proof_sha256"] = proof_store._json_hash(content_proof)
    return source_record, dataset_row, evidence


async def _seal_production_sized_candidate(scenario, dataset_row):
    """Persist the admission seal used by generic publication."""

    seal = admission_seal_from_validated_metadata(
        dataset_row["publication_metadata_json"]
    )
    assert seal is not None
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET publication_metadata_summary_json = $1::jsonb,
               publication_metadata_sha256 = $2,
               content_proof_admission_version = $3,
               content_proof_admission_kind = $4,
               content_proof_admission_sha256 = $5,
               content_proof_resource_types = $6::varchar[]
         WHERE dataset_id = 'dataset-candidate'
        """,
        json.dumps(seal.metadata_summary),
        seal.metadata_sha256,
        seal.admission_version,
        seal.admission_kind,
        seal.proof_sha256,
        list(seal.resource_types),
    )
    return seal


async def _prepare_publication_scenario(monkeypatch):
    """Build one committed generic candidate through current migrations."""

    publication_migration = _load_publication_guard_migration()
    scenario = await create_committed_subset_schema(monkeypatch)
    try:
        await install_current_scoped_publication_surface(
            scenario,
            publication_migration,
        )
        source_record, dataset_row, evidence = (
            _production_sized_single_root_inputs()
        )
        await _insert_policy_source(scenario, source_record)
        await insert_subset_candidate(
            scenario,
            dataset_id="dataset-candidate",
            root_run_id="root-candidate",
        )
        await insert_valid_subset_resources(scenario, "dataset-candidate")
        await _terminalize_candidate(scenario, dataset_row)
        await flush_deferred_fixture_events(scenario)
        await _activate_policy_source(
            scenario,
            load_activation_migration(),
            source_record,
            dataset_row,
            evidence,
        )
        await _split_source_endpoint_identity(
            scenario,
            _load_effective_endpoint_migration(),
        )
        seal = await _seal_production_sized_candidate(scenario, dataset_row)
        async with scenario.connection.transaction():
            await run_subset_migration(
                publication_migration,
                "upgrade",
                scenario.connection,
            )
        return scenario, seal
    except BaseException:
        await _close_scenario(scenario)
        raise


async def _load_candidate_state(scenario):
    """Load and validate the production-sized immutable candidate state."""

    state = await scenario.connection.fetchrow(
        f"""
        SELECT dataset_hash, resource_count, completion_proof_sha256,
               pg_catalog.pg_column_size(publication_metadata_json)
                   AS stored_bytes,
               pg_catalog.octet_length(publication_metadata_json::text)
                   AS text_bytes,
               pg_catalog.md5(publication_metadata_json::text) AS metadata_md5
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-candidate'
        """
    )
    assert 10_500_000 < state["text_bytes"] < 11_500_000
    assert state["stored_bytes"] > 6_000_000
    return dict(state)


def _publication_fence(state, seal):
    """Bind the candidate to the production artifact-promotion contract."""

    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="synthetic-source",
        endpoint_id="endpoint-a",
        serving_endpoint_id="endpoint-serving",
        dataset_id="dataset-candidate",
        evidence_run_id="root-candidate",
        selected_resources=tuple(RESOURCE_TYPES),
        recorded_expected_resources=tuple(RESOURCE_TYPES),
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        promote_on_cutover=True,
        dataset_hash=state["dataset_hash"],
        resource_count=state["resource_count"],
        content_proof_admission_sha256=seal.proof_sha256,
        generic_admission_sealed=True,
        completion_proof_required_version=3,
        completion_proof_sha256=state["completion_proof_sha256"],
    )
    return importer.ProviderDirectoryArtifactDatasetFence(
        (dataset,),
        should_select_validated_candidates=True,
    )


async def _production_sized_publication_scenario(monkeypatch):
    """Return the live-sized candidate, fence, and original state."""

    scenario, seal = await _prepare_publication_scenario(monkeypatch)
    try:
        state = await _load_candidate_state(scenario)
        return scenario, _publication_fence(state, seal), state
    except BaseException:
        await _close_scenario(scenario)
        raise


async def _promote_production_sized_candidate(database, fence):
    """Run the shared generic artifact-bundle promotion transaction."""

    async with asyncio.timeout(
        importer._provider_directory_artifact_transaction_timeout_seconds(
            fence
        )
    ) as cutover_timeout:
        async with database.transaction():
            await importer._configure_provider_directory_artifact_promotion(
                importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT,
                importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_STATEMENT_TIMEOUT,
            )
            await importer._apply_locked_provider_directory_artifact_bundle(
                (),
                importer._schema(),
                (),
                None,
                fence,
                cutover_timeout,
            )


@pytest.mark.asyncio
async def test_generic_publication_keeps_candidate_budget_through_promotion(
    monkeypatch,
):
    """Publish the production path without truncating its validated budget."""

    scenario, fence, original_state = (
        await _production_sized_publication_scenario(monkeypatch)
    )
    publication_database = _runtime_database()
    try:
        with monkeypatch.context() as publication_patch:
            publication_patch.setattr(importer, "db", publication_database)
            started_at = time.monotonic()
            await _promote_production_sized_candidate(
                publication_database,
                fence,
            )
            elapsed_seconds = time.monotonic() - started_at
        published_state = await scenario.connection.fetchrow(
            f"""
            SELECT status, is_current,
                   pg_catalog.md5(publication_metadata_json::text)
                       AS metadata_md5,
                   completion_proof_sha256
              FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'dataset-candidate'
            """
        )
        assert published_state["status"] == "published"
        assert published_state["is_current"] is True
        assert published_state["metadata_md5"] == original_state["metadata_md5"]
        assert published_state["completion_proof_sha256"] == (
            original_state["completion_proof_sha256"]
        )
        assert elapsed_seconds < 8
    finally:
        await _close_scenario(scenario, publication_database)
