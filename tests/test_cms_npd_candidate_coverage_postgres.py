# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate coverage freezes exact inputs before any serving pointer moves."""

import asyncio
from contextvars import Context
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_serving_coverage as coverage
from process.provider_directory_entity_identity import bind_entity_batch
from process.provider_directory_insurance_network_identity import record_insurance_network_plan
from process.provider_directory_source_local_publication import publish_validated_source_local_dataset
from tests import cms_npd_admission_postgres_support as support
from tests.cms_npd_admission_postgres_support import cms_artifact_root, fhir


class _Staged(Exception):
    """Stop a synthetic import at its publication boundary."""


async def _stage(monkeypatch, directory):
    """Run real eight-file admission and stop before the first candidate coverage seal."""
    directory, receipt = support.retained_release(directory)
    candidates = []

    async def stop_before_publication(_fhir, candidate, *_args):
        candidates.append(candidate)
        raise _Staged

    with monkeypatch.context() as staging:
        staging.setattr(cms, "_prepare_serving_candidate", stop_before_publication)
        with support.release_probe_client(directory) as client, pytest.raises(_Staged):
            await cms._run_acquired({"context": {}}, {}, "candidate-coverage", directory, receipt, client)
    candidate = candidates[0]
    state = await fhir._endpoint_dataset_state(candidate.dataset_id)
    return candidate, cms.release_identity(receipt)["vector_sha256"], state["dataset_hash"]


def _include_candidate_migration(monkeypatch):
    prefix = "20260930090000"
    if prefix not in support.MIGRATION_PREFIXES:
        monkeypatch.setattr(support, "MIGRATION_PREFIXES", (*support.MIGRATION_PREFIXES, prefix))


async def _insert_extra_witness(connection):
    """Insert a different plan witness for the same exact release and network."""
    await connection.execute(
        text("""INSERT INTO mrf.provider_directory_insurance_network_plan_evidence
        SELECT source_id,release_id,network_resource_type,network_resource_id,'extra-plan',network_refs,
               owned_by_ref,administered_by_ref,plan_payload_sha256,plan_payload_json,observed_at
        FROM mrf.provider_directory_insurance_network_plan_evidence LIMIT 1""")
    )


async def _assert_seal_guards(database, proof):
    """Reject witness growth and bulk deletion after preparation but before publication."""
    async with database.engine.begin() as connection:
        for sql in (
            "TRUNCATE mrf.provider_directory_insurance_network_plan_evidence",
            "TRUNCATE mrf.provider_directory_cms_candidate_coverage",
            "UPDATE mrf.provider_directory_cms_candidate_coverage SET relationship_count=0",
            "DELETE FROM mrf.provider_directory_cms_candidate_coverage",
        ):
            with pytest.raises(DBAPIError):
                async with connection.begin_nested():
                    await connection.execute(text(sql))
        with pytest.raises(DBAPIError, match="covered_network_witness_immutable"):
            async with connection.begin_nested():
                await _insert_extra_witness(connection)
    async with database.session() as session:
        await session.begin()
        await coverage.assert_sealed_cms_candidate_coverage(session, "mrf", proof)
        for field in ("dataset_id", "endpoint_id", "release_id", "dataset_hash", "proof_version"):
            with pytest.raises(RuntimeError, match="candidate_coverage_changed"):
                await coverage.assert_sealed_cms_candidate_coverage(session, "mrf", {**proof, field: "wrong"})


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_candidate_coverage_is_unpublished_immutable_and_replayable(monkeypatch, cms_artifact_root, published):
    """A full fixture seals once, preserves the current pointer, and rejects changed identities."""
    _include_candidate_migration(monkeypatch)
    migration_prefixes = support.LEGACY_MIGRATION_PREFIXES if published else support.MIGRATION_PREFIXES
    async with support.admission_database(monkeypatch, migration_prefixes=migration_prefixes) as database:
        candidate, release_id, dataset_hash = await _stage(monkeypatch, cms_artifact_root)
        if published:
            await publish_validated_source_local_dataset(
                fhir,
                candidate,
                "cms-npd",
                before_cutover=lambda session: coverage.validate_cms_candidate_coverage(
                    session, candidate.dataset_id, release_id
                ),
                before_cutover_timeout_seconds=60,
                after_promotion=lambda: coverage.seal_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash),
            )
        proof = await coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash)
        assert await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE is_current"
        ) == int(published)
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_serving_coverage") == int(
            published
        )
        monkeypatch.setattr(coverage, "require_cms_bindings", AsyncMock(side_effect=AssertionError("unexpected_scan")))
        assert await coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash) == proof
        await _assert_seal_guards(database, proof)
        await _assert_other_source_writes(database, release_id)


@pytest.mark.asyncio
async def test_candidate_seal_serializes_first_network_witness_race(monkeypatch, cms_artifact_root):
    """An insert waiting on first seal must recheck the committed receipt and roll back."""
    _include_candidate_migration(monkeypatch)
    async with support.admission_database(monkeypatch) as database:
        candidate, release_id, dataset_hash = await _stage(monkeypatch, cms_artifact_root)
        locked, release = asyncio.Event(), asyncio.Event()
        original = coverage.require_cms_bindings

        async def pause_validation(*args):
            await original(*args)
            locked.set()
            await release.wait()

        monkeypatch.setattr(coverage, "require_cms_bindings", pause_validation)
        preparation = asyncio.create_task(
            coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash), context=Context()
        )
        try:
            await asyncio.wait_for(locked.wait(), 10)
            async with database.engine.connect() as connection:
                await connection.begin()
                await connection.execute(text("SET LOCAL statement_timeout='5s'"))
                insertion = asyncio.create_task(_insert_extra_witness(connection), context=Context())
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(asyncio.shield(insertion), 0.1)
                release.set()
                await preparation
                with pytest.raises(DBAPIError, match="covered_network_witness_immutable"):
                    await insertion
                await connection.rollback()
        finally:
            release.set()
            await preparation
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_candidate_coverage") == 1


@pytest.mark.asyncio
async def test_candidate_coverage_assertion_requires_transaction():
    """A bounded proof check cannot accidentally release its locks before cutover."""
    with pytest.raises(ValueError, match="requires_transaction"):
        await coverage.assert_sealed_cms_candidate_coverage(SimpleNamespace(in_transaction=lambda: False), "mrf", {})


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_boundary", ["require_cms_bindings", "assert_sealed_cms_candidate_coverage"])
async def test_failed_candidate_coverage_validation_leaves_no_seal(monkeypatch, cms_artifact_root, failure_boundary):
    """Failure before or after receipt insertion rolls back without freezing further candidate work."""
    _include_candidate_migration(monkeypatch)
    async with support.admission_database(monkeypatch) as database:
        candidate, release_id, dataset_hash = await _stage(monkeypatch, cms_artifact_root)
        monkeypatch.setattr(coverage, failure_boundary, AsyncMock(side_effect=RuntimeError("synthetic_failure")))
        with pytest.raises(RuntimeError, match="synthetic_failure"):
            await coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash)
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_candidate_coverage") == 0
        async with database.engine.begin() as connection:
            await _insert_extra_witness(connection)


async def _assert_other_source_writes(database, release_id):
    """A sealed CMS release does not prevent another source from recording network evidence."""
    async with database.session() as session:
        await bind_entity_batch(
            session,
            source_id="example-directory",
            release_id=release_id,
            resources=[{"resourceType": "Organization", "id": "network-1"}],
        )
        await record_insurance_network_plan(
            session,
            source_id="example-directory",
            release_id=release_id,
            network_resource_id="network-1",
            plan={
                "resourceType": "InsurancePlan",
                "id": "plan-1",
                "network": [{"reference": "Organization/network-1"}],
            },
        )
