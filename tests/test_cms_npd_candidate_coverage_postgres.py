# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate coverage and relationship witnesses retain exact source scope."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from contextvars import Context
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_npd_relationship as relationships
from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_generic_source_storage as source_storage
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_source_local_publication import publish_validated_source_local_dataset
from tests import cms_npd_admission_postgres_support as support
from tests.cms_npd_admission_postgres_support import cms_artifact_root, fhir
from tests.test_cms_npd_admission_postgres import _write_location_batch


async def test_native_isolated_source_custody_and_frozen_proof(monkeypatch):
    """Exercise migrated storage and proof guards in the native candidate cohort."""
    from tests.test_provider_directory_isolated_source_provision import (
        _verify_native_catalog_custody,
    )

    await _verify_native_catalog_custody(monkeypatch)


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


@pytest.mark.asyncio
async def test_prepared_coverage_body_keeps_original_native_transaction_and_atomic_rollback(
    monkeypatch, cms_artifact_root
):
    """Exercise existing native coverage rules without a SOURCE binding or pointer write."""
    async with support.intake_admission_database(monkeypatch) as database:
        candidate, release_id, dataset_hash = await _stage(monkeypatch, cms_artifact_root)
        ambient = (source_policy._FACTORY.get(), source_policy._PUBLISHER.get(), source_policy._OWNER.get())

        class Rollback(Exception):
            pass

        with pytest.raises(Rollback):
            async with database.session() as session, session.begin():
                before = await session.scalar(text("SELECT pg_current_xact_id()::text"))
                await coverage.prepare_cms_candidate_coverage_in_session(session, candidate, release_id, dataset_hash)
                assert await session.scalar(text("SELECT pg_current_xact_id()::text")) == before
                assert (
                    source_policy._FACTORY.get(),
                    source_policy._PUBLISHER.get(),
                    source_policy._OWNER.get(),
                ) == ambient
                raise Rollback
        async with database.session() as session, session.begin():
            assert await session.scalar(text("SELECT count(*) FROM mrf.provider_directory_cms_candidate_coverage")) == 0
            proof = await coverage.prepare_cms_candidate_coverage_in_session(
                session, candidate, release_id, dataset_hash
            )
            created = await session.scalar(text("SELECT created_at FROM mrf.provider_directory_cms_candidate_coverage"))
        async with database.session() as session, session.begin():
            assert (
                await coverage.prepare_cms_candidate_coverage_in_session(session, candidate, release_id, dataset_hash)
                == proof
            )
            assert (
                await session.scalar(text("SELECT created_at FROM mrf.provider_directory_cms_candidate_coverage"))
                == created
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE is_current")
                )
                == 0
            )
            assert (
                await session.scalar(
                    text("SELECT status FROM mrf.provider_directory_endpoint_dataset WHERE dataset_id=:dataset"),
                    {"dataset": candidate.dataset_id},
                )
                == "validated"
            )
        assert (source_policy._FACTORY.get(), source_policy._PUBLISHER.get(), source_policy._OWNER.get()) == ambient


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


@asynccontextmanager
async def _ordinary_runtime_connection(database):
    """Use the genuine Reader login without the API engine's READ ONLY option."""
    component = database._source_component
    engine = create_async_engine(component.database.engine.url.set(username=component.reader))
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT current_user=session_user")) is True
            assert await connection.scalar(text("SELECT current_user")) == component.reader
            assert await connection.scalar(text("SHOW transaction_read_only")) == "off"
            yield connection
    finally:
        await engine.dispose()


async def _assert_seal_guards(database, proof):
    """Reject witness growth and bulk deletion after preparation but before publication."""
    async with _ordinary_runtime_connection(database) as connection:
        for sql in (
            "TRUNCATE mrf.provider_directory_insurance_network_plan_evidence",
            "TRUNCATE mrf.provider_directory_cms_candidate_coverage",
            "UPDATE mrf.provider_directory_cms_candidate_coverage SET relationship_count=0",
            "DELETE FROM mrf.provider_directory_cms_candidate_coverage",
        ):
            with pytest.raises(DBAPIError) as denied:
                async with connection.begin_nested():
                    await connection.execute(text(sql))
            assert denied.value.orig.sqlstate == "42501"
        with pytest.raises(DBAPIError) as denied:
            async with connection.begin_nested():
                await _insert_extra_witness(connection)
        assert denied.value.orig.sqlstate == "42501"
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
    async with support.intake_admission_database(monkeypatch, migration_prefixes=migration_prefixes) as database:
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
        with monkeypatch.context() as replay:
            replay.setattr(coverage, "require_cms_bindings", AsyncMock(side_effect=AssertionError("unexpected_scan")))
            assert await coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash) == proof
        await _assert_seal_guards(database, proof)
        await _assert_other_candidate_writes(monkeypatch, cms_artifact_root, database, candidate, proof)


@pytest.mark.asyncio
async def test_candidate_seal_refuses_network_witness_append_while_coverage_held(monkeypatch, cms_artifact_root):
    """A sealed Source header rejects witness growth while coverage validation remains held."""
    _include_candidate_migration(monkeypatch)
    async with support.intake_admission_database(monkeypatch) as database:
        candidate, release_id, dataset_hash = await _stage(monkeypatch, cms_artifact_root)
        witnesses_before = await database.scalar(
            "SELECT jsonb_agg(to_jsonb(w) ORDER BY resource_type,resource_id) "
            "FROM mrf.provider_directory_cms_npd_resource_witness w WHERE dataset_id=:dataset",
            dataset=candidate.dataset_id,
        )
        witness = witnesses_before[0]
        locked, release = asyncio.Event(), asyncio.Event()
        original = coverage.require_cms_bindings

        async def pause_validation(*args):
            await original(*args)
            locked.set()
            await release.wait()

        monkeypatch.setattr(coverage, "require_cms_bindings", pause_validation)

        async def prepare():
            async with source_policy.bind_generic_source_transaction_factory(database._source_component.factory):
                return await coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash)

        async def append():
            async with source_policy.bind_generic_source_transaction_factory(database._source_component.factory):
                async with source_policy.source_transaction(fhir.db):
                    await source_storage.append_generic_source_batch(fhir.db, fhir, candidate, (), (witness,))

        preparation = asyncio.create_task(prepare(), context=Context())
        insertion = None
        try:
            await asyncio.wait_for(locked.wait(), 10)
            insertion = asyncio.create_task(append(), context=Context())
            with pytest.raises(GenericCandidateError, match="^provider_directory_generic_source_header_sealed$"):
                await asyncio.wait_for(asyncio.shield(insertion), 10)
            assert not release.is_set() and not preparation.done()
            assert await _candidate_seal_snapshot(database, candidate) == (None, witnesses_before)
            release.set()
            await preparation
        finally:
            release.set()
            if insertion is not None and not insertion.done():
                insertion.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await insertion
            await preparation
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_candidate_coverage") == 1
        assert (
            await database.scalar(
                "SELECT jsonb_agg(to_jsonb(w) ORDER BY resource_type,resource_id) "
                "FROM mrf.provider_directory_cms_npd_resource_witness w WHERE dataset_id=:dataset",
                dataset=candidate.dataset_id,
            )
            == witnesses_before
        )


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
    async with support.intake_admission_database(monkeypatch) as database:
        candidate, release_id, dataset_hash = await _stage(monkeypatch, cms_artifact_root)
        monkeypatch.setattr(coverage, failure_boundary, AsyncMock(side_effect=RuntimeError("synthetic_failure")))
        with pytest.raises(RuntimeError, match="synthetic_failure"):
            await coverage.prepare_cms_candidate_coverage(fhir, candidate, release_id, dataset_hash)
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_candidate_coverage") == 0
        async with database.engine.begin() as connection:
            await _insert_extra_witness(connection)


async def _candidate_seal_snapshot(database, candidate):
    return (
        await database.scalar(
            "SELECT to_jsonb(c) FROM mrf.provider_directory_cms_candidate_coverage c WHERE dataset_id=:dataset",
            dataset=candidate.dataset_id,
        ),
        await database.scalar(
            "SELECT jsonb_agg(to_jsonb(w) ORDER BY resource_type,resource_id) "
            "FROM mrf.provider_directory_cms_npd_resource_witness w WHERE dataset_id=:dataset",
            dataset=candidate.dataset_id,
        ),
    )


async def _assert_other_candidate_writes(monkeypatch, directory, database, candidate, proof):
    """A sealed candidate leaves another endpoint's original Source writer usable."""
    before = await _candidate_seal_snapshot(database, candidate)
    with monkeypatch.context() as independent:
        independent.setattr(cms.source, "DOWNLOADS_URL", "https://example.test/cms-independent/downloads")
        other, release_id, dataset_hash = await _stage(independent, directory)
        assert other.dataset_id != candidate.dataset_id and other.endpoint_id != candidate.endpoint_id
        await coverage.prepare_cms_candidate_coverage(fhir, other, release_id, dataset_hash)
    assert await _candidate_seal_snapshot(database, candidate) == before
    async with database.transaction() as session:
        await coverage.assert_sealed_cms_candidate_coverage(session, "mrf", proof)


def _relationship_page_params(candidate, identity):
    return {"dataset_id": candidate.dataset_id, "release_id": identity["vector_sha256"], "resource_type": "Location"}


def _page_resources():
    return [
        {
            "resourceType": "Location",
            "id": resource_id,
            "name": "Example",
            "endpoint": [{"reference": "Endpoint/example"}, {"display": "No reference"}],
            "unmapped": {"nullable": None, "is_active": False},
        }
        for resource_id in ("page-a", "page-b", "page-c")
    ]


def _bind_location_rows(monkeypatch, locations):
    original = support._resource_rows

    def resources(revision):
        resources_by_type = original(revision)
        resources_by_type["Location"].extend(locations)
        return resources_by_type

    monkeypatch.setattr(support, "_resource_rows", resources)
    return resources


async def _refuse_corrupt_witness(monkeypatch, directory, database):
    original = cms._insert_verified_witnesses

    async def corrupt(session, witness_by_id):
        changed_by_id = {key: dict(witness) for key, witness in witness_by_id.items()}
        if "page-a" in changed_by_id:
            witness = changed_by_id["page-a"]
            witness["normalized_payload_hash"] = (
                "0" * 64 if witness["normalized_payload_hash"] != "0" * 64 else "1" * 64
            )
        await original(session, changed_by_id)

    with monkeypatch.context() as corrupted:
        corrupted.setattr(cms, "_insert_verified_witnesses", corrupt)
        with pytest.raises(RuntimeError, match="^cms_npd_witness_counts_incomplete$"):
            await _stage(corrupted, directory)
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship_receipt") == 0
    assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE is_current") == 0


async def _relationship_page_rows(session, params_by_field):
    page = relationships._family_page_sql(
        fhir._qt(fhir._schema(), relationships._RESOURCE),
        fhir._qt(fhir._schema(), relationships._WITNESS),
    )
    return (
        await session.execute(
            text(f"SELECT * FROM ({page}) retained_page ORDER BY resource_type, resource_id"), params_by_field
        )
    ).all()


@pytest.mark.asyncio
async def test_relationship_key_join_preserves_scope_hash_and_raw_rows(monkeypatch, cms_artifact_root):
    """Completed private snapshots retain exact scope, bounds and raw JSON."""
    resources = _page_resources()
    resource_factory = _bind_location_rows(monkeypatch, resources)
    async with support.intake_admission_database(monkeypatch) as database:
        await _refuse_corrupt_witness(monkeypatch, cms_artifact_root, database)
        monkeypatch.setattr(cms.source, "DOWNLOADS_URL", "https://example.test/cms-key-join-clean")
        candidate, release_id, _ = await _stage(monkeypatch, cms_artifact_root)
        with monkeypatch.context() as independent:
            independent.setattr(cms.source, "DOWNLOADS_URL", "https://example.test/cms-key-join-other")

            def other_rows(revision):
                rows_by_type = resource_factory(revision)
                rows_by_type["Location"] = [
                    {**resource, "name": "Other dataset"} if resource["id"] == "page-b" else resource
                    for resource in rows_by_type["Location"]
                ]
                return rows_by_type

            independent.setattr(support, "_resource_rows", other_rows)
            other, other_release, _ = await _stage(independent, cms_artifact_root)
        assert other.dataset_id != candidate.dataset_id and other.endpoint_id != candidate.endpoint_id
        params_by_field = _relationship_page_params(candidate, {"vector_sha256": release_id})
        async with database.session() as session:
            witness_rows = await _relationship_page_rows(session, params_by_field)
            assert [witness_row[1] for witness_row in witness_rows] == ["page-a", "page-b", "page-c", "site-1"]
            assert [witness_row[4] for witness_row in witness_rows[:3]] == resources
            assert all(
                witness_row[3]
                == hashlib.sha256(
                    json.dumps(witness_row[4], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
                ).hexdigest()
                for witness_row in witness_rows
            )
            for fields, expected_ids in (
                ({"release_id": "0" * 64}, []),
                ({"resource_type": "Organization"}, ["insurer-1", "network-1"]),
                (
                    {"dataset_id": other.dataset_id, "release_id": other_release},
                    ["page-a", "page-b", "page-c", "site-1"],
                ),
            ):
                selected = await _relationship_page_rows(session, {**params_by_field, **fields})
                assert [witness_row[1] for witness_row in selected] == expected_ids
                if fields.get("dataset_id") == other.dataset_id:
                    assert selected[1][4]["name"] == "Other dataset"


@pytest.mark.asyncio
async def test_relationship_key_join_replay_completeness_and_rollback(monkeypatch, cms_artifact_root):
    """Real native COPY rolls back, replays once, and checks every NULL reference."""
    _bind_location_rows(
        monkeypatch,
        [
            {
                "id": "page-site",
                "name": "Example",
                "endpoint": [{"reference": "Endpoint/example"}, {"display": "No reference"}],
            }
        ],
    )
    async with support.intake_admission_database(monkeypatch) as database:
        original = relationships._insert_family
        copied_datasets = []

        async def fail_after_copy(directory, session, dataset_id, release_id, resource_type):
            assert source_policy.source_copy_session() is session
            inserted = await original(directory, session, dataset_id, release_id, resource_type)
            if resource_type != "Location":
                return inserted
            copied_datasets.append(dataset_id)
            # page-site carries two endpoint references, the fixture's site-1 three more.
            assert await relationships._family_complete(fhir, session, dataset_id, release_id, "Location") == 5
            raise RuntimeError("synthetic_relationship_page_failure")

        with monkeypatch.context() as interrupted:
            interrupted.setattr(relationships, "_insert_family", fail_after_copy)
            with pytest.raises(RuntimeError, match="^synthetic_relationship_page_failure$"):
                await _stage(interrupted, cms_artifact_root)
        assert len(copied_datasets) == 1
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship") == 0
        assert await database.scalar("SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship_receipt") == 0
        candidate, release_id, _ = await _stage(monkeypatch, cms_artifact_root)
        assert candidate.dataset_id == copied_datasets[0]
        with monkeypatch.context() as replay:
            replay.setattr(relationships, "_insert_family", AsyncMock(side_effect=AssertionError("unexpected_recopy")))
            repeated, repeated_release, _ = await _stage(replay, cms_artifact_root)
            assert repeated.dataset_id == candidate.dataset_id and repeated_release == release_id
        links = await database.all(
            "SELECT target_reference,resolution_status FROM mrf.provider_directory_cms_npd_relationship "
            "WHERE dataset_id=:dataset_id AND resource_id='page-site' ORDER BY reference_ordinal",
            dataset_id=candidate.dataset_id,
        )
        assert [tuple(link) for link in links] == [("Endpoint/example", "unresolved"), (None, "unresolved")]
        await _assert_missing_relationship(database, candidate, release_id)


async def _assert_missing_relationship(database, candidate, release_id):
    """Detect an exact independent-administrator fault in the completed projection."""
    await database.status(
        "DELETE FROM mrf.provider_directory_cms_npd_relationship "
        "WHERE dataset_id=:dataset_id AND resource_id='page-site' AND reference_ordinal=2",
        dataset_id=candidate.dataset_id,
    )
    async with database.session() as session:
        with pytest.raises(RuntimeError, match="^cms_npd_relationship_projection_incomplete$"):
            await relationships._family_complete(fhir, session, candidate.dataset_id, release_id, "Location")
