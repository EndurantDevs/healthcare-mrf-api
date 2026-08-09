# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Artifact-publication races for reviewed subset activation."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
import importlib

from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_store as store
from tests.provider_directory_reviewed_subset_activation_pg_concurrency import (
    _close_scenario,
    _create_activation_scenario,
    _runtime_database,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    activation_evidence,
    is_activation_valid,
    publish_candidate_sql,
)
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    create_abandonment_relations,
)
from tests.provider_directory_effective_endpoint_pg_cases import (
    _load_effective_endpoint_migration,
)
from tests.provider_directory_reviewed_root_policy_pg import (
    _load_policy_migration,
    _run_upgrade_with_context,
)
from tests.provider_directory_subset_completion_pg_setup import (
    load_abandonment_migration,
    load_payload_guard_repair_migration,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _artifact_publication_fence(
    evidence,
    *,
    require_candidate_eligibility: bool,
):
    candidate = importer.ProviderDirectoryArtifactDataset(
        source_id="synthetic-source",
        endpoint_id="endpoint-a",
        serving_endpoint_id="endpoint-a",
        dataset_id="dataset-matched",
        evidence_run_id="root-matched",
        recorded_expected_resources=(),
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        promote_on_cutover=True,
        completion_proof_required_version=3,
        completion_proof_sha256=evidence.completion_proof_sha256,
    )
    return importer.ProviderDirectoryArtifactDatasetFence(
        (candidate,),
        should_select_validated_candidates=require_candidate_eligibility,
    )


@asynccontextmanager
async def _publication_scenario(monkeypatch, *, require_eligibility: bool):
    """Create and clean one two-connection activation scenario."""

    with monkeypatch.context() as scoped_patch:
        scenario, migration, evidence_pairs = await _create_activation_scenario(
            scoped_patch
        )
        await create_abandonment_relations(scenario)
        async with scenario.connection.transaction():
            for successor in (
                load_payload_guard_repair_migration(),
                load_abandonment_migration(),
                _load_effective_endpoint_migration(),
                _load_policy_migration(),
            ):
                await _run_upgrade_with_context(scenario, successor)
        activation_database = _runtime_database()
        publication_database = _runtime_database()
        fence = _artifact_publication_fence(
            activation_evidence(evidence_pairs),
            require_candidate_eligibility=require_eligibility,
        )
        try:
            yield (
                scoped_patch,
                scenario,
                migration,
                activation_database,
                publication_database,
                fence,
            )
        finally:
            await _close_scenario(
                scenario,
                activation_database,
                publication_database,
            )


async def _artifact_fence_transaction(
    database,
    scenario,
    fence,
    *,
    locked: asyncio.Event | None = None,
    release: asyncio.Event | None = None,
    publish: bool = False,
) -> None:
    """Hold the exact production fence and optionally publish its candidate."""

    async with database.transaction():
        await importer._lock_and_verify_artifact_dataset_fence(fence, database)
        if locked is not None:
            locked.set()
        if release is not None:
            await release.wait()
        if publish:
            assert await database.status(publish_candidate_sql(scenario)) == 1


async def _has_waiting_advisory(connection, backend_pid, task) -> bool:
    """Return whether the exact competing backend waits on an advisory lock."""

    for _attempt in range(100):
        if task.done():
            return False
        is_waiting = await connection.fetchval(
            """
            SELECT COALESCE(pg_catalog.bool_or(NOT lock_row.granted), false)
              FROM pg_catalog.pg_locks AS lock_row
             WHERE lock_row.pid = $1
               AND lock_row.locktype = 'advisory'
            """,
            backend_pid,
        )
        if is_waiting:
            return True
        await asyncio.sleep(0.01)
    return False


async def _cancel_tasks(*tasks) -> None:
    """Cancel unfinished race tasks without masking the primary assertion."""

    for task in tasks:
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


async def _activation_first_serializes_publication(monkeypatch) -> None:
    """Require publication to wait on an uncommitted activation advisory."""

    async with _publication_scenario(
        monkeypatch,
        require_eligibility=True,
    ) as race_inputs:
        patch, scenario, migration, activation_db, publication_db, fence = (
            race_inputs
        )
        activated = asyncio.Event()
        release = asyncio.Event()
        original_activate = store._activate_source

        async def paused_activate(*args, **kwargs):
            activation_result = await original_activate(*args, **kwargs)
            activated.set()
            await release.wait()
            return activation_result

        patch.setattr(store, "_activate_source", paused_activate)
        activation_task = asyncio.create_task(
            activation.sync_reviewed_subset_verified_state(
                database=activation_db
            )
        )
        publication_task = None
        try:
            await asyncio.wait_for(activated.wait(), timeout=5)
            publication_pid = await publication_db.scalar(
                "SELECT pg_catalog.pg_backend_pid();"
            )
            publication_task = asyncio.create_task(
                _artifact_fence_transaction(
                    publication_db, scenario, fence, publish=True
                )
            )
            assert await _has_waiting_advisory(
                scenario.connection, publication_pid, publication_task
            )
            release.set()
            activation_result = await asyncio.wait_for(
                activation_task,
                timeout=5,
            )
            assert activation_result.activated is True
            await asyncio.wait_for(publication_task, timeout=5)
            assert await is_activation_valid(scenario, migration) is True
        finally:
            release.set()
            await _cancel_tasks(activation_task, publication_task)


async def _assert_pending_source(scenario) -> None:
    """Require the publication-first race to leave activation untouched."""

    is_pending = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::jsonb
                   ->> 'provider_directory_candidate_status' =
                   '{activation.PENDING_STATUS}'
               AND NOT (
                   metadata_json::jsonb ?
                   'provider_directory_reviewed_subset_activation_v1'
               )
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
    )
    assert is_pending is True


async def _publication_first_returns_activation_busy(monkeypatch) -> None:
    """Require activation to fail fast while publication owns the advisory."""

    async with _publication_scenario(
        monkeypatch,
        require_eligibility=False,
    ) as race_inputs:
        _, scenario, migration, activation_db, publication_db, fence = (
            race_inputs
        )
        locked = asyncio.Event()
        release = asyncio.Event()
        publication_task = asyncio.create_task(
            _artifact_fence_transaction(
                publication_db,
                scenario,
                fence,
                locked=locked,
                release=release,
            )
        )
        try:
            await asyncio.wait_for(locked.wait(), timeout=5)
            try:
                await activation.sync_reviewed_subset_verified_state(
                    database=activation_db
                )
            except activation.ReviewedSubsetActivationError as error:
                assert error.code == "busy"
            else:
                raise AssertionError("activation ignored the publication lock")
            await _assert_pending_source(scenario)
            release.set()
            await asyncio.wait_for(publication_task, timeout=5)
            retry = await activation.sync_reviewed_subset_verified_state(
                database=activation_db
            )
            assert retry.activated is True
            assert await is_activation_valid(scenario, migration) is True
        finally:
            release.set()
            await _cancel_tasks(publication_task)


async def prove_activation_artifact_publication_is_serialized(
    monkeypatch,
) -> None:
    """Prove both advisory-lock orderings avoid source/dataset deadlock."""

    await _activation_first_serializes_publication(monkeypatch)
    await _publication_first_returns_activation_busy(monkeypatch)
