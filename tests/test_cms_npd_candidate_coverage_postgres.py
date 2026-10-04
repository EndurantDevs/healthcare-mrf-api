# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate coverage and relationship witnesses retain exact source scope."""

import asyncio
import hashlib
import json
from contextvars import Context
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_npd_relationship as relationships
from process import provider_directory_cms_serving_coverage as coverage
from process.provider_directory_entity_identity import bind_entity_batch
from process.provider_directory_insurance_network_identity import record_insurance_network_plan
from process.provider_directory_source_local_publication import publish_validated_source_local_dataset
from tests import cms_npd_admission_postgres_support as support
from tests.cms_npd_admission_postgres_support import cms_artifact_root, fhir
from tests.test_cms_npd_admission_postgres import _write_location_batch


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


def _relationship_page_params(candidate, identity):
    return {
        "dataset_id": candidate.dataset_id,
        "release_id": identity["vector_sha256"],
        "after_type": "",
        "after_id": "",
        "last_type": "zzzz",
        "last_id": "zzzz",
    }


async def _stage_scoped_witnesses(monkeypatch, candidate, identity, endpoint_id):
    resources = [
        {
            "resourceType": "Location",
            "id": resource_id,
            "name": "Example",
            "endpoint": [{"reference": "Endpoint/example"}, {"display": "No reference"}],
            "unmapped": {"nullable": None, "is_active": False},
        }
        for resource_id in ("page-a", "page-b", "page-c")
    ]
    original_insert = cms._insert_verified_witnesses

    async def insert_hash_mismatch(session, witness_by_id):
        changed_by_id = {key: dict(value) for key, value in witness_by_id.items()}
        bad_witness = changed_by_id["page-a"]
        bad_witness["normalized_payload_hash"] = (
            "0" * 64 if bad_witness["normalized_payload_hash"] != "0" * 64 else "1" * 64
        )
        await original_insert(session, changed_by_id)

    with monkeypatch.context() as changed:
        changed.setattr(cms, "_insert_verified_witnesses", insert_hash_mismatch)
        await _write_location_batch(candidate, resources)
    with monkeypatch.context() as other_source:
        other_source.setattr(cms.source, "DOWNLOADS_URL", "https://example.test/cms-key-join-other")
        other_endpoint_id = await cms._register_source(fhir)
    assert await cms._register_source(fhir) == endpoint_id
    assert endpoint_id != other_endpoint_id
    other = await cms._candidate(
        fhir, other_endpoint_id, "cms-key-join-other", identity, candidate_key="cms-key-join-other"
    )
    assert other.dataset_id != candidate.dataset_id
    await _write_location_batch(other, [{**resources[1], "name": "Other dataset"}])
    return resources, other


async def _relationship_page_rows(session, params_by_field):
    page = relationships._page_sql(
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
    """Filtering after the key join retains exact scope, bounds and raw JSON."""
    _, receipt = support.retained_release(cms_artifact_root)
    identity = cms.release_identity(receipt)
    async with support.admission_database(monkeypatch) as database:
        endpoint_id = await cms._register_source(fhir)
        candidate = await cms._admission_candidate(fhir, endpoint_id, "cms-key-join", identity, {})
        resources, other = await _stage_scoped_witnesses(monkeypatch, candidate, identity, endpoint_id)
        params_by_field = _relationship_page_params(candidate, identity)
        async with database.session() as session:
            witness_rows = await _relationship_page_rows(session, params_by_field)
            assert [witness_row[1] for witness_row in witness_rows] == ["page-b", "page-c"]
            assert [witness_row[4] for witness_row in witness_rows] == resources[1:]
            assert all(
                witness_row[3]
                == hashlib.sha256(
                    json.dumps(witness_row[4], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
                ).hexdigest()
                for witness_row in witness_rows
            )
            for fields, expected_ids in (
                (
                    {"after_type": "Location", "after_id": "page-a", "last_type": "Location", "last_id": "page-b"},
                    ["page-b"],
                ),
                (
                    {"after_type": "Location", "after_id": "page-b", "last_type": "Location", "last_id": "page-c"},
                    ["page-c"],
                ),
                ({"release_id": "0" * 64}, []),
                ({"dataset_id": other.dataset_id}, ["page-b"]),
            ):
                selected = await _relationship_page_rows(session, {**params_by_field, **fields})
                assert [witness_row[1] for witness_row in selected] == expected_ids
                if fields.get("dataset_id") == other.dataset_id:
                    assert selected[0][4]["name"] == "Other dataset"
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship WHERE dataset_id=:dataset_id",
                dataset_id=candidate.dataset_id,
            )
            == 0
        )


@pytest.mark.asyncio
async def test_relationship_key_join_replay_completeness_and_rollback(monkeypatch, cms_artifact_root):
    """The shared page preserves atomic inserts, NULL references and missing-link rejection."""
    _, receipt = support.retained_release(cms_artifact_root)
    identity = cms.release_identity(receipt)
    async with support.admission_database(monkeypatch) as database:
        endpoint_id = await cms._register_source(fhir)
        candidate = await cms._admission_candidate(fhir, endpoint_id, "cms-key-join-rollback", identity, {})
        await _write_location_batch(
            candidate,
            [
                {
                    "resourceType": "Location",
                    "id": "page-site",
                    "name": "Example",
                    "endpoint": [{"reference": "Endpoint/example"}, {"display": "No reference"}],
                }
            ],
        )
        params_by_field = _relationship_page_params(candidate, identity)
        with pytest.raises(RuntimeError, match="synthetic_relationship_page_failure"):
            async with database.transaction() as session:
                await session.execute(text(relationships._insert_sql(fhir)), params_by_field)
                assert (
                    await relationships._page_complete(
                        fhir, session, candidate.dataset_id, identity["vector_sha256"], "", "", "zzzz", "zzzz"
                    )
                    == 2
                )
                raise RuntimeError("synthetic_relationship_page_failure")
        ledger_count = await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship WHERE dataset_id=:dataset_id",
            dataset_id=candidate.dataset_id,
        )
        assert ledger_count == 0
        for _ in range(2):
            async with database.session() as session:
                await session.execute(text(relationships._insert_sql(fhir)), params_by_field)
                assert (
                    await relationships._page_complete(
                        fhir, session, candidate.dataset_id, identity["vector_sha256"], "", "", "zzzz", "zzzz"
                    )
                    == 2
                )
        links = await database.all(
            "SELECT target_reference,resolution_status FROM mrf.provider_directory_cms_npd_relationship "
            "WHERE dataset_id=:dataset_id ORDER BY reference_ordinal",
            dataset_id=candidate.dataset_id,
        )
        assert [tuple(link) for link in links] == [("Endpoint/example", "unresolved"), (None, "unresolved")]
        await database.status(
            "DELETE FROM mrf.provider_directory_cms_npd_relationship "
            "WHERE dataset_id=:dataset_id AND reference_ordinal=2",
            dataset_id=candidate.dataset_id,
        )
        async with database.session() as session:
            with pytest.raises(RuntimeError, match="cms_npd_relationship_projection_incomplete"):
                await relationships._page_complete(
                    fhir, session, candidate.dataset_id, identity["vector_sha256"], "", "", "zzzz", "zzzz"
                )
