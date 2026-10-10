# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy source publication/read/recovery against the admission dependency slice.

Current intake deliberately stops at a candidate; its separate native suite
proves that boundary. These fixtures retain lower-layer legacy publication
coverage and do not represent the prepared composite serving path.
"""

import asyncio
import hashlib
import importlib.util
import json
from contextvars import Context
from functools import partial
from itertools import count
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from api.provider_directory_cms_entities import read_cms_entities
from api.provider_directory_entities_contract import CURSOR_KEY_ENV, DirectoryRead, DirectoryReadError
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_npd_recovery as recovery
from process import provider_directory_cms_npd_relationship as relationships
from process import provider_directory_cms_serving_coverage as coverage
from process.provider_directory_source_local_publication import publish_validated_source_local_dataset
from tests import cms_npd_admission_postgres_support as support
from tests.cms_npd_admission_postgres_support import (
    cms_admission_template,
    cms_artifact_root,
    fhir,
    release_probe_client,
    retained_release,
)

pytestmark = pytest.mark.parametrize(
    "cms_admission_template", [support.LEGACY_MIGRATION_PREFIXES], indirect=True, ids=["legacy"]
)
admission_database = partial(support.admission_database, migration_prefixes=support.LEGACY_MIGRATION_PREFIXES)


async def _publish_legacy_source(fhir, candidate, identity, directory, receipt, client, ctx, task):
    """Publish the dependency-slice fixture through real source and coverage guards."""
    state = await fhir._endpoint_dataset_state(candidate.dataset_id)
    cms._assert_candidate_release(state, identity)
    await cms._verify_or_dispose(fhir, candidate, identity, directory, receipt, client, task)
    if not candidate.already_published:

        async def before_cutover(session):
            await coverage.validate_cms_candidate_coverage(session, candidate.dataset_id, identity["vector_sha256"])
            cms._assert_candidate_release(await fhir._endpoint_dataset_state(candidate.dataset_id), identity)
            await fhir._raise_if_resource_import_cancelled(ctx, task)

        await publish_validated_source_local_dataset(
            fhir,
            candidate,
            "cms-npd",
            before_cutover=before_cutover,
            before_cutover_timeout_seconds=16 * 60,
            after_promotion=lambda: coverage.seal_cms_candidate_coverage(
                fhir, candidate, identity["vector_sha256"], state["dataset_hash"]
            ),
        )
    else:
        async with fhir.db.session() as session:
            assert await coverage.build_cms_coverage(session) == candidate.dataset_id
    state = await fhir._endpoint_dataset_state(candidate.dataset_id)
    cms._assert_candidate_release(state, identity)
    assert state["status"] == "published" and state["is_current"] is True
    return state, None


async def _admit_legacy_source(directory, receipt, run_id, task=None):
    """Stage real retained rows, substituting only the explicit legacy publication boundary."""
    with patch.object(cms, "_prepare_serving_candidate", _publish_legacy_source):
        if task and task.get("cms_npd_rollback_vector_sha256"):
            return await cms._run_acquired({"context": {}}, task, run_id, directory, receipt, None)
        with release_probe_client(directory) as client:
            return await cms._run_acquired({"context": {}}, task or {}, run_id, directory, receipt, client)


async def _write_location_batch(candidate, resources):
    """Use the production atomic normalized-row and raw-witness boundary."""
    parsed_rows = [cms._parse_batch_row(fhir, resource, candidate)[1] for resource in resources]
    await cms._persist_source_batch(fhir, fhir.ProviderDirectoryLocation, parsed_rows, resources, candidate, "Location")


async def _raw_batch_snapshot(database, dataset_id):
    """Include unmatched normalized rows so a partial rollback cannot hide."""
    return await database.all(
        "SELECT r.resource_id,r.payload_hash,r.payload_json::text,w.raw_payload_sha256,"
        "w.normalized_payload_hash,w.raw_payload_json::text FROM mrf.provider_directory_dataset_resource r "
        "LEFT JOIN mrf.provider_directory_cms_npd_resource_witness w "
        "USING(dataset_id,resource_type,resource_id) WHERE r.dataset_id=:dataset_id ORDER BY r.resource_id",
        dataset_id=dataset_id,
    )


async def _assert_witness_copy_failure(monkeypatch, database, candidate, resource, baseline):
    """Fail after COPY has inserted a witness and require rollback of both tables."""
    original_copy = fhir._copy_upsert_rows

    async def fail_after_copy(model, *args, **kwargs):
        result = await original_copy(model, *args, **kwargs)
        if model is cms.ProviderDirectoryCMSNPDResourceWitness:
            raise RuntimeError("synthetic_after_witness_copy")
        return result

    with monkeypatch.context() as failed:
        failed.setattr(fhir, "_copy_upsert_rows", fail_after_copy)
        with pytest.raises(RuntimeError, match="synthetic_after_witness_copy"):
            await _write_location_batch(candidate, [resource])
    assert await _raw_batch_snapshot(database, candidate.dataset_id) == baseline


@pytest.mark.asyncio
async def test_witness_copy_replay_and_failures_are_atomic(monkeypatch, cms_artifact_root):
    """COPY preserves exact raw JSON and rolls back replay conflicts and transport failures."""
    _, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        endpoint_id = await cms._register_source(fhir)
        candidate = await cms._admission_candidate(
            fhir, endpoint_id, "cms-copy-regression", cms.release_identity(receipt), {}
        )
        resource_by_field = {
            "resourceType": "Location",
            "id": "copy-site",
            "name": "Example",
            "unmapped": {"label": "Café\\u0000", "values": [None, False, 2, 1.5]},
        }
        await _write_location_batch(candidate, [resource_by_field])
        baseline = await _raw_batch_snapshot(database, candidate.dataset_id)
        assert len(baseline) == 1 and json.loads(baseline[0][-1]) == resource_by_field
        await _write_location_batch(candidate, [resource_by_field, resource_by_field])
        assert await _raw_batch_snapshot(database, candidate.dataset_id) == baseline
        new_resource_by_field = {**resource_by_field, "id": "new-site"}
        conflicting_by_field = {**resource_by_field, "unmapped": {"changed": True}}
        with pytest.raises(RuntimeError, match="cms_npd_witness_payload_conflict"):
            await _write_location_batch(candidate, [new_resource_by_field, conflicting_by_field])
        assert await _raw_batch_snapshot(database, candidate.dataset_id) == baseline
        await _assert_witness_copy_failure(monkeypatch, database, candidate, new_resource_by_field, baseline)
        with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_resource_invalid"):
            await _write_location_batch(candidate, [{**new_resource_by_field, "unmapped": {"bad\x00key": "value"}}])
        assert await _raw_batch_snapshot(database, candidate.dataset_id) == baseline


async def _admit_without_witnesses(monkeypatch, directory, receipt, run_id, *, before_publish=None):
    """Model a release sealed before raw witness storage was installed."""

    async def persist_legacy_rows(fhir_module, model, parsed_rows, _raw_resources, candidate, _resource_type):
        await fhir_module._persist_endpoint_dataset_rows(
            model,
            parsed_rows,
            candidate.dataset_id,
            resource_hash_contract=candidate.resource_hash_contract,
            semantic_projection_as_of=candidate.semantic_projection_as_of,
        )

    with monkeypatch.context() as legacy:
        legacy.setattr(cms, "_persist_source_batch", persist_legacy_rows)
        legacy.setattr(cms, "_assert_witness_counts", AsyncMock())
        if before_publish is not None:
            legacy.setattr(f"{__name__}.publish_validated_source_local_dataset", before_publish)
        return await _admit_legacy_source(directory, receipt, run_id)


async def _assert_current(database, dataset_id):
    current = await database.all("SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset WHERE is_current")
    assert [row[0] for row in current] == [dataset_id]
    fence = await fhir._resolve_provider_directory_artifact_datasets(
        ["cms-npd"],
        should_select_validated_candidates=False,
    )
    assert len(fence.datasets) == 1 and fence.datasets[0].dataset_id == dataset_id


async def _bindings(database):
    return await database.all(
        "SELECT resource_type, resource_id, organization_id, site_id "
        "FROM mrf.provider_directory_entity_source_binding ORDER BY resource_type, resource_id"
    )


async def _resource_identities(database):
    return await database.all(
        "SELECT resource_type, resource_id, entity_id "
        "FROM mrf.provider_directory_resource_identity WHERE source_id='cms-npd' "
        "ORDER BY resource_type, resource_id"
    )


async def _assert_raw_witnesses(database, admission_result, receipt):
    """Compare complete raw release facts with their normalized row bindings."""

    witnesses = await database.all(
        "SELECT witness.resource_type, witness.resource_id, witness.source_id, witness.release_id, "
        "witness.raw_payload_json, witness.normalized_payload_hash, resource.payload_hash, "
        "witness.raw_payload_sha256 "
        "FROM mrf.provider_directory_cms_npd_resource_witness AS witness "
        "JOIN mrf.provider_directory_dataset_resource AS resource "
        "ON resource.dataset_id=witness.dataset_id AND resource.resource_type=witness.resource_type "
        "AND resource.resource_id=witness.resource_id "
        "WHERE witness.dataset_id=:dataset_id",
        dataset_id=admission_result["dataset_id"],
    )
    assert len(witnesses) == admission_result["resource_count"]
    assert {witness_row[0] for witness_row in witnesses} == cms.RESOURCE_SET
    assert all(
        witness_row[2] == "cms-npd" and witness_row[3] == receipt["vector_sha256"] and witness_row[5] == witness_row[6]
        for witness_row in witnesses
    )
    assert all(
        witness_row[7]
        == hashlib.sha256(
            json.dumps(witness_row[4], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        for witness_row in witnesses
    )
    raw_by_type = {witness_row[0]: witness_row[4] for witness_row in witnesses}
    location = raw_by_type["Location"]
    assert location["identifier"][0]["value"] == "site-source-1"
    assert location["address"]["id"] == "address-source-1"
    assert location["partOf"]["display"] == "Parent Site"
    assert location["endpoint"][0]["identifier"]["value"] == "site-endpoint"
    plan = raw_by_type["InsurancePlan"]
    assert plan["endpoint"][0]["display"] == "Plan Endpoint"
    assert plan["ownedBy"]["identifier"]["value"] == "insurer-source-1"
    assert plan["ownedBy"]["type"] == "Organization" and plan["ownedBy"]["display"] == "Example Insurer"
    assert plan["extension"][0]["valueString"] == "Synthetic value"


async def _assert_source_facts(database, admission_result, receipt):
    """Verify the sealed release, projected rows, raw witnesses, and network facts."""

    state = await fhir._endpoint_dataset_state(admission_result["dataset_id"])
    assert state["publication_metadata_json"]["source_release"] == cms.release_identity(receipt)
    seal = await database.first(
        "SELECT content_proof_admission_kind, content_proof_resource_types "
        "FROM mrf.provider_directory_endpoint_dataset WHERE dataset_id=:dataset_id",
        dataset_id=admission_result["dataset_id"],
    )
    assert seal[0] == "generic"
    assert set(seal[1]) == cms.RESOURCE_SET
    payloads = await database.all(
        "SELECT resource_type, payload_json FROM mrf.provider_directory_dataset_resource WHERE dataset_id=:dataset_id",
        dataset_id=admission_result["dataset_id"],
    )
    assert {resource_row[0] for resource_row in payloads} == cms.RESOURCE_SET
    assert next(resource_row[1] for resource_row in payloads if resource_row[0] == "Practitioner")["npi"] is None
    assert all(resource_row[1]["tax_id"] is None for resource_row in payloads if resource_row[0] == "Organization")
    normalized_location = next(resource_row[1] for resource_row in payloads if resource_row[0] == "Location")
    assert "identifier" not in normalized_location and "part_of_ref" not in normalized_location
    await _assert_raw_witnesses(database, admission_result, receipt)
    network_rows = await database.all(
        "SELECT network_resource_id, network_refs, owned_by_ref "
        "FROM mrf.provider_directory_insurance_network_plan_evidence WHERE release_id=:release_id",
        release_id=receipt["vector_sha256"],
    )
    assert len(network_rows) == 1
    assert network_rows[0][0] == "network-1"
    assert network_rows[0][1] == ["Organization/network-1", "Organization/unresolved"]
    assert network_rows[0][2] == "Organization/insurer-1"
    coverage = await database.first(
        "SELECT release_id FROM mrf.provider_directory_cms_serving_coverage WHERE dataset_id=:dataset_id",
        dataset_id=admission_result["dataset_id"],
    )
    assert coverage[0] == receipt["vector_sha256"]

    await _assert_relationship_facts(database, admission_result["dataset_id"])


async def _assert_relationship_facts(database, dataset_id):
    """Check all eight source types and exact resolution outcomes."""
    edges = await database.all(
        "SELECT resource_type, reference_field, target_reference, target_resource_id, resolution_status "
        "FROM mrf.provider_directory_cms_npd_relationship WHERE dataset_id=:dataset_id "
        "ORDER BY resource_type, resource_id, reference_field, reference_ordinal",
        dataset_id=dataset_id,
    )
    assert len(edges) == 19
    assert {relationship[0] for relationship in edges} == {
        "Organization",
        "Location",
        "Endpoint",
        "HealthcareService",
        "InsurancePlan",
        "Practitioner",
        "PractitionerRole",
        "OrganizationAffiliation",
    }
    assert {relationship[1] for relationship in edges} >= {
        "plan.network",
        "plan.coverageArea",
        "coverage.network",
        "qualification.issuer",
    }
    issuer_period = await database.scalar(
        "SELECT period_start FROM mrf.provider_directory_cms_npd_relationship "
        "WHERE dataset_id=:dataset_id AND reference_field='qualification.issuer'",
        dataset_id=dataset_id,
    )
    assert issuer_period == "2025-01-01"
    assert {
        (relationship[2], relationship[3], relationship[4])
        for relationship in edges
        if relationship[2] == "Organization/unresolved"
    } == {("Organization/unresolved", "unresolved", "unresolved")}
    assert {
        (relationship[0], relationship[1], relationship[2], relationship[4])
        for relationship in edges
        if relationship[4] == "unresolved"
    } == {
        ("InsurancePlan", "network", "Organization/unresolved", "unresolved"),
        ("Location", "partOf", "Location/parent-site", "unresolved"),
        ("Location", "endpoint", None, "unresolved"),
    }
    assert all(
        relationship[4] == "resolved"
        for relationship in edges
        if relationship[2] not in {"Organization/unresolved", "Location/parent-site", None}
    )


async def _assert_published_payload_guard(database, dataset_id):
    with pytest.raises(DBAPIError, match="cms_npd_published_resource_immutable"):
        await database.status(
            "UPDATE mrf.provider_directory_dataset_resource SET payload_json='{}'::jsonb WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    with pytest.raises(DBAPIError, match="cms_npd_resource_witness_immutable"):
        await database.status(
            "UPDATE mrf.provider_directory_cms_npd_resource_witness "
            "SET raw_payload_json='{}'::jsonb WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    with pytest.raises(DBAPIError, match="cms_npd_published_relationship_immutable"):
        await database.status(
            "UPDATE mrf.provider_directory_cms_npd_relationship "
            "SET resolution_status='unresolved' WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    with pytest.raises(DBAPIError, match="cms_npd_relationship_receipt_immutable"):
        await database.status(
            "UPDATE mrf.provider_directory_cms_npd_relationship_receipt "
            "SET relationship_count=0 WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    with pytest.raises(DBAPIError, match="cms_npd_published_relationship_immutable"):
        await database.status(
            "INSERT INTO mrf.provider_directory_cms_npd_relationship "
            "(dataset_id, source_id, release_id, resource_type, resource_id, "
            "source_payload_hash, raw_payload_sha256, reference_field, "
            "parent_ordinal, reference_ordinal, target_type, target_reference, "
            "target_resource_id, resolution_status, period_start, period_end) "
            "SELECT dataset_id, source_id, release_id, resource_type, resource_id, "
            "source_payload_hash, raw_payload_sha256, reference_field, "
            "parent_ordinal, reference_ordinal + 100, target_type, target_reference, "
            "target_resource_id, resolution_status, period_start, period_end "
            "FROM mrf.provider_directory_cms_npd_relationship "
            "WHERE dataset_id=:dataset_id LIMIT 1",
            dataset_id=dataset_id,
        )
    with pytest.raises(DBAPIError, match="cms_npd_relationship_truncate_forbidden"):
        await database.status("TRUNCATE mrf.provider_directory_cms_npd_relationship")


async def _assert_generic_publication_blocked():
    with pytest.raises(RuntimeError, match="cms_npd_verified_publication_required"):
        await fhir._prepare_artifact_publication_fence(
            None,
            run_id="cms-test-bypass",
            metrics={},
            publish_artifacts_targets=None,
            publish_corroboration=False,
            should_select_validated_candidates=True,
        )


async def _assert_validated_disposition(database, dataset_id):
    """Keep the sealed proof and reject mutation of its terminal marker."""

    assert (await fhir._endpoint_dataset_state(dataset_id))["status"] == "validated"
    assert await recovery.is_disposed(fhir, dataset_id)
    assert (
        await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_dataset_resource WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
        == 9
    )
    with pytest.raises(DBAPIError, match="cms_npd_stale_disposition_immutable"):
        await database.status(
            "DELETE FROM mrf.provider_directory_cms_npd_stale_candidate WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    with pytest.raises(DBAPIError, match="cms_npd_stale_disposition_immutable"):
        await database.status("TRUNCATE mrf.provider_directory_cms_npd_stale_candidate")


async def _assert_failed_disposition(database, dataset_id, receipt):
    """An acquiring candidate retains its marker but releases mutable payload."""

    assert (await fhir._endpoint_dataset_state(dataset_id))["status"] == "failed"
    for table in ("provider_directory_dataset_resource", "provider_directory_dataset_proof_shard"):
        assert (
            await database.scalar(
                f"SELECT count(*) FROM mrf.{table} WHERE dataset_id=:dataset_id",
                dataset_id=dataset_id,
            )
            == 0
        )
    assert (
        await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_cms_npd_resource_witness WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
        == 0
    )
    disposition = await database.first(
        "SELECT prior_status, vector_sha256 FROM mrf.provider_directory_cms_npd_stale_candidate "
        "WHERE dataset_id=:dataset_id",
        dataset_id=dataset_id,
    )
    assert tuple(disposition) == ("acquiring", receipt["vector_sha256"])


@pytest.mark.asyncio
async def test_complete_release_publishes_with_real_database_guards(monkeypatch, cms_artifact_root):
    """Publish, replay, replace and explicitly restore an eight-file accepted release."""
    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        admission_result = await _admit_legacy_source(directory, receipt, "cms-test-first")
        assert admission_result["status"] == "published"
        assert admission_result["resource_count"] == 9
        await _assert_current(database, admission_result["dataset_id"])
        await _assert_source_facts(database, admission_result, receipt)
        bindings = await _bindings(database)
        assert len(bindings) == 3
        resource_identities = await _resource_identities(database)
        assert {(identity_row[0], identity_row[1]) for identity_row in resource_identities} == {
            ("InsurancePlan", "plan-1"),
            ("PractitionerRole", "role-1"),
        }
        monkeypatch.setenv(CURSOR_KEY_ENV, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        async with database.session() as session:
            for kind, expected_count in (
                ("organizations", 2),
                ("sites", 1),
                ("networks", 1),
                ("plans", 1),
                ("practitioner-roles", 1),
            ):
                page = await read_cms_entities(session, DirectoryRead(kind, "cms-npd", "entities", None, None))
                assert len(page["items"]) == expected_count
            plan_id = str(
                next(identity_row[2] for identity_row in resource_identities if identity_row[0] == "InsurancePlan")
            )
            links = await read_cms_entities(
                session, DirectoryRead("plans", "cms-npd", "relationships", plan_id, None, 100)
            )
            plan_networks = [link for link in links["items"] if link["relationship_type"] == "plan-network"]
            assert len(plan_networks) == 4
            assert sum(link["status"] == "unresolved" for link in plan_networks) == 1
            assert all(link["source_id"] == "cms-npd" for link in links["items"])
            assert "network-1" not in json.dumps(links)
        await _assert_published_payload_guard(database, admission_result["dataset_id"])
        replay = await _admit_legacy_source(directory, receipt, "cms-test-replay")
        assert replay["dataset_id"] == admission_result["dataset_id"] and replay["replayed"] is True
        await _assert_source_facts(database, replay, receipt)
        await _assert_release_replacement(
            database, cms_artifact_root, directory, receipt, admission_result, bindings, resource_identities
        )


@pytest.mark.asyncio
async def test_coverage_only_plan_network_is_bound_and_served(monkeypatch, cms_artifact_root):
    original_rows = support._resource_rows

    def coverage_only_rows(revision):
        rows = original_rows(revision)
        rows["Organization"].append({"id": "coverage-network", "name": "Coverage Network"})
        rows["InsurancePlan"][0]["coverage"] = [{"network": [{"reference": "Organization/coverage-network"}]}]
        return rows

    monkeypatch.setattr(support, "_resource_rows", coverage_only_rows)
    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        await _admit_legacy_source(directory, receipt, "cms-coverage-network")
        network_id = await database.scalar(
            "SELECT network_id FROM mrf.provider_directory_insurance_network_source_binding "
            "WHERE source_id='cms-npd' AND resource_id='coverage-network'"
        )
        assert network_id is not None
        plan_id = await database.scalar(
            "SELECT entity_id FROM mrf.provider_directory_resource_identity "
            "WHERE source_id='cms-npd' AND resource_type='InsurancePlan' AND resource_id='plan-1'"
        )
        monkeypatch.setenv(CURSOR_KEY_ENV, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        async with database.session() as session:
            networks = await read_cms_entities(session, DirectoryRead("networks", "cms-npd", "entities", None, None))
            links = await read_cms_entities(
                session, DirectoryRead("plans", "cms-npd", "relationships", str(plan_id), None, 100)
            )
        assert len(networks["items"]) == 2
        assert any(
            link["relationship_type"] == "plan-network"
            and link["target_id"] == str(network_id)
            and link["status"] == "resolved"
            for link in links["items"]
        )


@pytest.mark.asyncio
async def test_published_plan_only_release_replay_backfills_network_role(monkeypatch, cms_artifact_root):
    """A v1 receipt cannot serve until exact new network roles are bound and rechecked."""
    directory, receipt = retained_release(cms_artifact_root, include_unplanned_network_role=True)
    async with admission_database(monkeypatch) as database:
        from process import provider_directory_insurance_network_batch as network_writer

        current_require = coverage.require_cms_bindings

        async def plan_only_require(session, schema, generation, kind):
            if kind != "networks":
                await current_require(session, schema, generation, kind)

        with monkeypatch.context() as legacy:
            legacy.setattr(
                network_writer,
                "_TARGETS_SQL",
                network_writer._TARGETS_SQL.replace("AND (observation_json->>'network_role')::boolean", "AND FALSE"),
            )
            legacy.setattr(cms, "_NETWORK_ROLE_SQL", "FALSE")
            legacy.setattr(coverage, "require_cms_bindings", plan_only_require)
            published = await _admit_legacy_source(directory, receipt, "cms-network-role-first")
        assert published["status"] == "published"
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_insurance_network_source_binding "
                "WHERE source_id='cms-npd' AND resource_id='unplanned-network'"
            )
            == 0
        )
        await _mark_legacy_coverage_receipt(database, published["dataset_id"])
        monkeypatch.setenv(CURSOR_KEY_ENV, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        async with database.session() as session:
            with pytest.raises(DirectoryReadError) as caught:
                await read_cms_entities(session, DirectoryRead("networks", "cms-npd", "entities", None, None))
        assert caught.value.status == 503

        replay = await _admit_legacy_source(directory, receipt, "cms-network-role-replay")
        assert replay["dataset_id"] == published["dataset_id"] and replay["replayed"] is True
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_insurance_network_source_binding "
                "WHERE source_id='cms-npd' AND resource_id='unplanned-network'"
            )
            == 1
        )
        assert [
            proof_version_row[0]
            for proof_version_row in await database.all(
                "SELECT proof_version FROM mrf.provider_directory_cms_serving_coverage "
                "WHERE dataset_id=:dataset_id ORDER BY proof_version",
                dataset_id=published["dataset_id"],
            )
        ] == [1, 2]
        async with database.session() as session:
            page = await read_cms_entities(session, DirectoryRead("networks", "cms-npd", "entities", None, None))
        assert len(page["items"]) == 2


async def _create_legacy_coverage_schema(connection):
    for statement in (
        "CREATE SCHEMA mrf",
        "CREATE TABLE mrf.provider_directory_endpoint_dataset "
        "(dataset_id varchar(96) PRIMARY KEY, status text NOT NULL, is_current boolean NOT NULL)",
        "CREATE TABLE mrf.provider_directory_cms_serving_coverage "
        "(dataset_id varchar(96) NOT NULL REFERENCES mrf.provider_directory_endpoint_dataset, "
        "release_id varchar(256) NOT NULL, dataset_hash varchar(64) NOT NULL, "
        "published_at timestamp NOT NULL, created_at timestamptz NOT NULL, "
        "PRIMARY KEY (dataset_id, release_id))",
        "INSERT INTO mrf.provider_directory_endpoint_dataset VALUES ('candidate', 'validated', false)",
        "INSERT INTO mrf.provider_directory_cms_serving_coverage VALUES ('candidate', 'release', 'hash', now(), now())",
    ):
        await connection.exec_driver_sql(statement)


@pytest.mark.asyncio
async def test_coverage_version_migration_blocks_publishable_v1_receipt():
    """An unserved v1 receipt may still become current during a rolling upgrade."""
    engine = create_async_engine(support._database_url())
    path = support._MIGRATIONS / "20260930080000_cms_npd_coverage_version.py"
    spec = importlib.util.spec_from_file_location("cms_npd_coverage_version_test", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def apply_migration(connection, action="upgrade"):
        with Operations.context(MigrationContext.configure(connection)):
            getattr(migration, action)()

    try:
        async with engine.begin() as connection:
            await _create_legacy_coverage_schema(connection)
            with pytest.raises(DBAPIError, match="cms_npd_coverage_v1_current_requires_staged_upgrade"):
                async with connection.begin_nested():
                    await connection.run_sync(apply_migration)
            await connection.exec_driver_sql(
                "UPDATE mrf.provider_directory_endpoint_dataset SET status='superseded' WHERE dataset_id='candidate'"
            )
            await connection.run_sync(apply_migration)
            proof_version_row = (
                await connection.exec_driver_sql(
                    "SELECT proof_version FROM mrf.provider_directory_cms_serving_coverage"
                )
            ).one()
            assert proof_version_row.proof_version == 1
            constraint = (
                await connection.exec_driver_sql(
                    "SELECT convalidated FROM pg_constraint WHERE conname='cms_npd_coverage_new_receipt_v2_check'"
                )
            ).one()
            assert constraint.convalidated is False
            with pytest.raises(DBAPIError):
                async with connection.begin_nested():
                    await connection.exec_driver_sql(
                        "INSERT INTO mrf.provider_directory_cms_serving_coverage "
                        "(dataset_id, release_id, dataset_hash, published_at, created_at) "
                        "VALUES ('candidate', 'later', 'hash', now(), now())"
                    )
            await connection.exec_driver_sql(
                "INSERT INTO mrf.provider_directory_cms_serving_coverage "
                "VALUES ('candidate', 'v2-release', 'hash', now(), now(), 2)"
            )
            with pytest.raises(DBAPIError, match="cms_npd_coverage_v2_downgrade_requires_explicit_plan"):
                async with connection.begin_nested():
                    await connection.run_sync(apply_migration, "downgrade")
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql("DROP SCHEMA IF EXISTS mrf CASCADE")
        await engine.dispose()


async def _mark_legacy_coverage_receipt(database, dataset_id):
    """Model a pre-version receipt in the disposable admission database."""
    async with database.session() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE mrf.provider_directory_cms_serving_coverage "
                "DISABLE TRIGGER cms_npd_serving_coverage_immutable"
            )
        )
        await session.execute(
            text(
                "ALTER TABLE mrf.provider_directory_cms_serving_coverage "
                "DROP CONSTRAINT cms_npd_coverage_new_receipt_v2_check"
            )
        )
        changed = await session.execute(
            text(
                "UPDATE mrf.provider_directory_cms_serving_coverage SET proof_version=1 "
                "WHERE dataset_id=:dataset_id AND proof_version=2"
            ),
            {"dataset_id": dataset_id},
        )
        assert changed.rowcount == 1
        await session.execute(
            text(
                "ALTER TABLE mrf.provider_directory_cms_serving_coverage "
                "ENABLE TRIGGER cms_npd_serving_coverage_immutable"
            )
        )
        await session.execute(
            text(
                "ALTER TABLE mrf.provider_directory_cms_serving_coverage "
                "ADD CONSTRAINT cms_npd_coverage_new_receipt_v2_check CHECK (proof_version=2) NOT VALID"
            )
        )


async def _assert_release_replacement(
    database, artifact_root, directory, receipt, first_admission, bindings, identities
):
    """A replacement withdraws an edge; replay of retained bytes restores it."""
    next_directory, next_receipt = retained_release(artifact_root, revision="second", include_missing_network=False)
    replacement = await _admit_legacy_source(next_directory, next_receipt, "cms-test-second")
    assert replacement["dataset_id"] != first_admission["dataset_id"]
    assert (await fhir._endpoint_dataset_state(first_admission["dataset_id"]))["status"] == "superseded"
    await _assert_current(database, replacement["dataset_id"])
    assert (
        await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship "
            "WHERE dataset_id=:dataset_id AND target_reference='Organization/unresolved'",
            dataset_id=replacement["dataset_id"],
        )
        == 0
    )
    assert (
        await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship "
            "WHERE dataset_id=:dataset_id AND target_reference='Organization/unresolved'",
            dataset_id=first_admission["dataset_id"],
        )
        == 1
    )
    restored = await _admit_legacy_source(
        directory,
        receipt,
        "cms-test-restore",
        {"cms_npd_rollback_vector_sha256": receipt["vector_sha256"]},
    )
    assert restored["dataset_id"] not in {first_admission["dataset_id"], replacement["dataset_id"]}
    await _assert_current(database, restored["dataset_id"])
    await _assert_source_facts(database, restored, receipt)
    assert await _bindings(database) == bindings
    assert await _resource_identities(database) == identities


@pytest.mark.asyncio
async def test_relationship_alias_is_ambiguous_without_merging_entities(monkeypatch, cms_artifact_root):
    """Two exact CMS resource IDs sharing one stable ID cannot silently resolve a link."""

    directory, receipt = retained_release(cms_artifact_root)
    identity = cms.release_identity(receipt)
    async with admission_database(monkeypatch) as database:
        endpoint_id = await cms._register_source(fhir)
        candidate = await cms._admission_candidate(fhir, endpoint_id, "cms-test-ambiguous", identity, {})
        for name, resource_type in cms.source.RESOURCE_FILES:
            await cms._stream_file(fhir, directory / f"{name}.zst", candidate, resource_type, {}, {})
        await cms._materialize_identity_evidence(fhir, directory, candidate, identity, {}, {})
        alias_payload_map = {"resourceType": "Organization", "id": "network-alias", "name": "Alias"}
        model, normalized = cms._parse_batch_row(fhir, alias_payload_map, candidate)
        await cms._persist_source_batch(fhir, model, [normalized], [alias_payload_map], candidate, "Organization")
        await database.status(
            "INSERT INTO mrf.provider_directory_entity_source_binding "
            "(source_id,resource_type,resource_id,organization_id,site_id,created_at) "
            "SELECT source_id,resource_type,'network-alias',organization_id,NULL,now() "
            "FROM mrf.provider_directory_entity_source_binding "
            "WHERE source_id='cms-npd' AND resource_type='Organization' AND resource_id='network-1'"
        )
        await relationships.materialize(fhir, candidate, identity["vector_sha256"])
        statuses = await database.all(
            "SELECT target_reference,resolution_status FROM mrf.provider_directory_cms_npd_relationship "
            "WHERE dataset_id=:dataset_id AND resource_type='InsurancePlan' ORDER BY reference_field,reference_ordinal",
            dataset_id=candidate.dataset_id,
        )
        assert ("Organization/network-1", "ambiguous") in [tuple(status) for status in statuses]
        assert ("Organization/unresolved", "unresolved") in [tuple(status) for status in statuses]
        assert ("Organization/insurer-1", "resolved") in [tuple(status) for status in statuses]


@pytest.mark.asyncio
async def test_relationship_pages_are_byte_bounded_and_receipted(monkeypatch, cms_artifact_root):
    directory, receipt = retained_release(cms_artifact_root)
    monkeypatch.setattr(relationships, "_BATCH_SIZE", 3)
    monkeypatch.setattr(relationships, "_BATCH_BYTES", 1)
    async with admission_database(monkeypatch) as database:
        admitted = await _admit_legacy_source(directory, receipt, "cms-small-pages")
        assert await relationships.completed_receipt_count(
            fhir, admitted["dataset_id"], receipt["vector_sha256"]
        ) == await database.scalar(
            "SELECT count(*) FROM mrf.provider_directory_cms_npd_relationship WHERE dataset_id=:dataset_id",
            dataset_id=admitted["dataset_id"],
        )
        await _assert_source_facts(database, admitted, receipt)


@pytest.mark.asyncio
async def test_coverage_failure_keeps_prior_generation_readable(monkeypatch, cms_artifact_root):
    """Neither preflight nor receipt failure may expose an uncovered replacement."""

    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "2")
    first_directory, first_receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(first_directory, first_receipt, "cms-test-covered-incumbent")
        monkeypatch.setenv(CURSOR_KEY_ENV, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

        async def readable_generation():
            async with database.session() as session:
                page = await read_cms_entities(
                    session, DirectoryRead("organizations", "cms-npd", "entities", None, None)
                )
                assert len(page["items"]) == 2
                return page["generation_id"]

        prior_generation = await readable_generation()
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="coverage-failure")
        original_validate = coverage.validate_cms_candidate_coverage

        async def fail_after_validation(*args):
            await original_validate(*args)
            raise RuntimeError("synthetic_coverage_preflight_failure")

        monkeypatch.setattr(coverage, "validate_cms_candidate_coverage", fail_after_validation)
        with pytest.raises(RuntimeError, match="synthetic_coverage_preflight_failure"):
            await _admit_legacy_source(next_directory, next_receipt, "cms-test-preflight-failed")
        monkeypatch.setattr(coverage, "validate_cms_candidate_coverage", original_validate)
        await _assert_current(database, incumbent["dataset_id"])
        assert await readable_generation() == prior_generation

        original_seal = coverage.seal_cms_candidate_coverage

        async def fail_after_seal(*args):
            await original_seal(*args)
            raise RuntimeError("synthetic_coverage_seal_failure")

        monkeypatch.setattr(coverage, "seal_cms_candidate_coverage", fail_after_seal)
        with pytest.raises(RuntimeError, match="synthetic_coverage_seal_failure"):
            await _admit_legacy_source(next_directory, next_receipt, "cms-test-seal-failed")
        monkeypatch.setattr(coverage, "seal_cms_candidate_coverage", original_seal)
        await _assert_current(database, incumbent["dataset_id"])
        assert await readable_generation() == prior_generation
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_cms_serving_coverage WHERE release_id=:release_id",
                release_id=next_receipt["vector_sha256"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_first_coverage_blocks_evidence_truncate_until_sealed(monkeypatch, cms_artifact_root):
    """A truncate racing first publication sees the new receipt and fails."""

    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        original_validate = coverage.validate_cms_candidate_coverage
        truncate_tasks = []

        async def validate_with_truncate(*args):
            await original_validate(*args)
            truncate_tasks.append(
                asyncio.create_task(
                    database.status("TRUNCATE mrf.provider_directory_insurance_network_plan_evidence"),
                    context=Context(),
                )
            )
            await asyncio.sleep(0.05)
            assert not truncate_tasks[0].done()

        monkeypatch.setattr(coverage, "validate_cms_candidate_coverage", validate_with_truncate)
        try:
            admitted = await _admit_legacy_source(directory, receipt, "cms-test-first-coverage-truncate")
            await _assert_current(database, admitted["dataset_id"])
            assert truncate_tasks
            with pytest.raises(DBAPIError, match="cms_npd_covered_truncate_forbidden"):
                await asyncio.wait_for(truncate_tasks[0], 3)
        finally:
            if truncate_tasks and not truncate_tasks[0].done():
                truncate_tasks[0].cancel()
                await asyncio.gather(truncate_tasks[0], return_exceptions=True)


@pytest.mark.asyncio
async def test_online_recurrence_republishes_a_superseded_vector(monkeypatch, cms_artifact_root):
    """A newly verified A after A-to-B cutover gets a fresh covered candidate."""
    first_directory, first_receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        first = await _admit_legacy_source(first_directory, first_receipt, "cms-test-first")
        second_directory, second_receipt = retained_release(cms_artifact_root, revision="second")
        second = await _admit_legacy_source(second_directory, second_receipt, "cms-test-second")
        recurred = await _admit_legacy_source(first_directory, first_receipt, "cms-test-recurred")
        assert recurred["dataset_id"] not in {first["dataset_id"], second["dataset_id"]}
        await _assert_current(database, recurred["dataset_id"])
        await _assert_source_facts(database, recurred, first_receipt)
        replay = await _admit_legacy_source(first_directory, first_receipt, "cms-test-recurred-replay")
        assert replay["dataset_id"] == recurred["dataset_id"] and replay["replayed"] is True


@pytest.mark.asyncio
async def test_pre_witness_published_release_is_reacquired_without_mutating_current(monkeypatch, cms_artifact_root):
    """An exact old release stays current until its witnessed successor cuts over."""

    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        legacy = await _admit_without_witnesses(monkeypatch, directory, receipt, "cms-legacy-published")
        await _assert_current(database, legacy["dataset_id"])
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_cms_npd_resource_witness WHERE dataset_id=:dataset_id",
                dataset_id=legacy["dataset_id"],
            )
            == 0
        )

        with monkeypatch.context() as interrupted:

            async def pause_before_first_witness(*_args):
                await _assert_current(database, legacy["dataset_id"])
                raise RuntimeError("synthetic_witness_upgrade_pause")

            interrupted.setattr(cms, "_persist_source_batch", pause_before_first_witness)
            with pytest.raises(RuntimeError, match="synthetic_witness_upgrade_pause"):
                await _admit_legacy_source(directory, receipt, "cms-upgrade-interrupted")
        interrupted_candidate = await database.first(
            "SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset "
            "WHERE status='acquiring' AND previous_dataset_id=:legacy_id",
            legacy_id=legacy["dataset_id"],
        )
        assert interrupted_candidate is not None
        await _assert_current(database, legacy["dataset_id"])
        upgraded = await _admit_legacy_source(directory, receipt, "cms-upgrade-resumed")
        assert upgraded["dataset_id"] == interrupted_candidate[0]
        await _assert_current(database, upgraded["dataset_id"])
        await _assert_source_facts(database, upgraded, receipt)
        replay = await _admit_legacy_source(directory, receipt, "cms-upgrade-replay")
        assert replay["dataset_id"] == upgraded["dataset_id"] and replay["replayed"] is True


@pytest.mark.asyncio
async def test_daily_check_upgrades_current_release_without_witnesses(monkeypatch, cms_artifact_root):
    directory, receipt = retained_release(cms_artifact_root)
    with release_probe_client(directory) as client:
        observed = cms.source.observe_release(client=client)
    async with admission_database(monkeypatch) as database:
        legacy = await _admit_without_witnesses(monkeypatch, directory, receipt, "cms-daily-legacy")
        await _assert_current(database, legacy["dataset_id"])
        acquired = Mock(return_value=(directory, receipt))
        verify_release = cms._verify_release

        async def verify_retained(path, release_receipt, _client, task=None):
            await verify_release(path, release_receipt, None, task)

        with monkeypatch.context() as daily:
            daily.setattr(cms, "durable_artifact_root", lambda: cms_artifact_root)
            daily.setattr(cms.source, "observe_release", lambda **_kwargs: observed)
            daily.setattr(cms.source, "acquire_release", acquired)
            daily.setattr(cms, "_verify_release", verify_retained)
            daily.setattr(cms, "_prepare_serving_candidate", _publish_legacy_source)
            upgraded = await cms.run({"context": {}}, {"import_resources": True, "full_refresh": True}, "cms-daily")
        acquired.assert_called_once()
        assert upgraded["dataset_id"] != legacy["dataset_id"]
        assert upgraded["cms_npd_check"]["outcome"] == "acquisition_required"
        await _assert_current(database, upgraded["dataset_id"])
        await _assert_source_facts(database, upgraded, receipt)


@pytest.mark.asyncio
async def test_daily_check_upgrades_witnessed_release_without_relationship_receipt(monkeypatch, cms_artifact_root):
    directory, receipt = retained_release(cms_artifact_root)
    with release_probe_client(directory) as client:
        observed = cms.source.observe_release(client=client)
    async with admission_database(monkeypatch) as database:
        with monkeypatch.context() as witness_era:
            witness_era.setattr(cms.relationships, "materialize", AsyncMock())
            legacy = await _admit_legacy_source(directory, receipt, "cms-witness-era")
        assert (
            await cms.relationships.completed_receipt_count(fhir, legacy["dataset_id"], receipt["vector_sha256"])
            is None
        )
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_cms_npd_resource_witness WHERE dataset_id=:dataset_id",
                dataset_id=legacy["dataset_id"],
            )
            == legacy["resource_count"]
        )
        acquired = Mock(return_value=(directory, receipt))
        verify_release = cms._verify_release

        async def verify_retained(path, release_receipt, _client, task=None):
            await verify_release(path, release_receipt, None, task)

        with monkeypatch.context() as daily:
            daily.setattr(cms, "durable_artifact_root", lambda: cms_artifact_root)
            daily.setattr(cms.source, "observe_release", lambda **_kwargs: observed)
            daily.setattr(cms.source, "acquire_release", acquired)
            daily.setattr(cms, "_verify_release", verify_retained)
            daily.setattr(cms, "_prepare_serving_candidate", _publish_legacy_source)
            upgraded = await cms.run({"context": {}}, {"import_resources": True, "full_refresh": True}, "cms-rel")
        acquired.assert_called_once()
        assert upgraded["dataset_id"] != legacy["dataset_id"]
        assert upgraded["cms_npd_check"]["outcome"] == "acquisition_required"
        assert (
            await cms.relationships.completed_receipt_count(fhir, upgraded["dataset_id"], receipt["vector_sha256"])
            is not None
        )
        await _assert_current(database, upgraded["dataset_id"])
        await _assert_source_facts(database, upgraded, receipt)


@pytest.mark.asyncio
async def test_disposed_witness_upgrade_retries_without_replacing_legacy(monkeypatch, cms_artifact_root):
    directory, receipt = retained_release(cms_artifact_root)
    alternate_directory, _ = retained_release(cms_artifact_root, revision="alternate-upgrade")
    filename = cms.source.RESOURCE_FILES[0][0] + ".zst"
    retained_file = directory / filename
    retained_bytes = retained_file.read_bytes()
    alternate_bytes = (alternate_directory / filename).read_bytes()
    async with admission_database(monkeypatch) as database:
        legacy = await _admit_without_witnesses(monkeypatch, directory, receipt, "cms-retry-legacy")
        original_verify = cms.source.verify_release
        verification_calls = count(1)

        def swap_after_intake(*args, **kwargs):
            verified = original_verify(*args, **kwargs)
            if next(verification_calls) == 1:
                retained_file.write_bytes(alternate_bytes)
            return verified

        with monkeypatch.context() as changed:
            changed.setattr(cms.source, "verify_release", swap_after_intake)
            with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_retained_file_missing"):
                await _admit_legacy_source(directory, receipt, "cms-upgrade-disposed")
        await _assert_current(database, legacy["dataset_id"])
        legacy_state = await fhir._endpoint_dataset_state(legacy["dataset_id"])
        failed_id = fhir._endpoint_dataset_candidate_id(
            legacy_state["endpoint_id"],
            tuple(sorted(cms.RESOURCE_SET)),
            f"cms-npd-witness-upgrade:{legacy['dataset_id']}:cms-upgrade-disposed",
        )
        await _assert_failed_disposition(database, failed_id, receipt)
        retained_file.write_bytes(retained_bytes)
        retried = await _admit_legacy_source(directory, receipt, "cms-upgrade-restored")
        assert retried["dataset_id"] not in {legacy["dataset_id"], failed_id}
        await _assert_current(database, retried["dataset_id"])
        await _assert_source_facts(database, retried, receipt)


@pytest.mark.asyncio
async def test_pre_witness_validated_candidate_gets_fresh_exact_release(monkeypatch, cms_artifact_root):
    """A sealed old candidate is left intact while a new one gains witnesses."""

    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(directory, receipt, "cms-incumbent")
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="validated-legacy")
        with pytest.raises(RuntimeError, match="synthetic_legacy_publish_hold"):
            await _admit_without_witnesses(
                monkeypatch,
                next_directory,
                next_receipt,
                "cms-legacy-validated",
                before_publish=AsyncMock(side_effect=RuntimeError("synthetic_legacy_publish_hold")),
            )
        legacy_candidate = await database.first(
            "SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset "
            "WHERE status='validated' AND publication_metadata_json::jsonb "
            "-> 'source_release' ->> 'vector_sha256'=:release_id",
            release_id=next_receipt["vector_sha256"],
        )
        assert legacy_candidate is not None
        await _assert_current(database, incumbent["dataset_id"])
        with pytest.raises(DBAPIError, match="cms_npd_resource_witness_immutable"):
            await database.status(
                "INSERT INTO mrf.provider_directory_cms_npd_resource_witness "
                "(dataset_id, source_id, release_id, resource_type, resource_id, raw_payload_sha256, "
                "normalized_payload_hash, raw_payload_json) "
                "SELECT dataset_id, 'cms-npd', :release_id, resource_type, resource_id, :raw_hash, "
                "payload_hash, jsonb_build_object('resourceType', resource_type, 'id', resource_id) "
                "FROM mrf.provider_directory_dataset_resource "
                "WHERE dataset_id=:dataset_id AND resource_type='Location'",
                dataset_id=legacy_candidate[0],
                release_id=next_receipt["vector_sha256"],
                raw_hash="a" * 64,
            )
        upgraded = await _admit_legacy_source(next_directory, next_receipt, "cms-upgrade-validated")
        assert upgraded["dataset_id"] != legacy_candidate[0]
        assert (await fhir._endpoint_dataset_state(legacy_candidate[0]))["status"] == "validated"
        await _assert_current(database, upgraded["dataset_id"])
        await _assert_source_facts(database, upgraded, next_receipt)


@pytest.mark.asyncio
async def test_complete_release_accepts_an_empty_member(monkeypatch, cms_artifact_root):
    """Zero source rows produce zero witnesses without losing the eight-file seal."""

    directory, receipt = retained_release(cms_artifact_root, empty_resource_type="Endpoint")
    assert receipt["files"]["03-Endpoint.ndjson"]["row_count"] == 0
    async with admission_database(monkeypatch) as database:
        admitted = await _admit_legacy_source(directory, receipt, "cms-empty-endpoint")
        assert admitted["resource_count"] == 8
        await _assert_current(database, admitted["dataset_id"])
        witnessed_types = await database.all(
            "SELECT resource_type, count(*) FROM mrf.provider_directory_cms_npd_resource_witness "
            "WHERE dataset_id=:dataset_id GROUP BY resource_type",
            dataset_id=admitted["dataset_id"],
        )
        assert {resource_type for resource_type, _count in witnessed_types} == cms.RESOURCE_SET - {"Endpoint"}
        assert sum(count for _resource_type, count in witnessed_types) == 8


@pytest.mark.asyncio
async def test_failed_cutover_rolls_back_incumbent_and_blocks_generic_promotion(monkeypatch, cms_artifact_root):
    """Fail after supersession inside the real transaction, then resume the sealed candidate."""
    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(directory, receipt, "cms-test-incumbent")
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="cutover-failed")
        original_publish = fhir._publish_validated_artifact_dataset

        async def fail_after_supersession(_dataset):
            assert (
                await database.scalar("SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE is_current")
                == 0
            )
            raise RuntimeError("synthetic_cutover_interruption")

        monkeypatch.setattr(fhir, "_publish_validated_artifact_dataset", fail_after_supersession)
        with pytest.raises(RuntimeError, match="synthetic_cutover_interruption"):
            await _admit_legacy_source(next_directory, next_receipt, "cms-test-cutover-failed")
        await _assert_current(database, incumbent["dataset_id"])
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE status='validated' AND NOT is_current"
            )
            == 1
        )
        await _assert_generic_publication_blocked()
        monkeypatch.setattr(fhir, "_publish_validated_artifact_dataset", original_publish)
        resumed = await _admit_legacy_source(next_directory, next_receipt, "cms-test-cutover-resumed")
        await _assert_current(database, resumed["dataset_id"])
        assert resumed["resource_count"] == 9


@pytest.mark.asyncio
async def test_pre_cutover_deadline_rolls_back_and_releases_proof_locks(monkeypatch, cms_artifact_root):
    """A stalled CMS proof cannot hold shared tables or replace the incumbent."""
    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(directory, receipt, "cms-test-incumbent")
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="deadline")
        original_validate = coverage.validate_cms_candidate_coverage
        original_publish = publish_validated_source_local_dataset
        proof_state = SimpleNamespace(is_locked=False)

        async def stall_after_locks(session, dataset_id, release_id):
            await original_validate(session, dataset_id, release_id)
            proof_state.is_locked = True
            await asyncio.Event().wait()

        async def short_deadline(*args, **kwargs):
            kwargs["before_cutover_timeout_seconds"] = 5.0
            return await original_publish(*args, **kwargs)

        monkeypatch.setattr(coverage, "validate_cms_candidate_coverage", stall_after_locks)
        monkeypatch.setattr(f"{__name__}.publish_validated_source_local_dataset", short_deadline)
        with pytest.raises(TimeoutError):
            await _admit_legacy_source(next_directory, next_receipt, "cms-test-deadline")
        assert proof_state.is_locked
        await _assert_current(database, incumbent["dataset_id"])
        async with database.transaction():
            await database.status("LOCK TABLE mrf.provider_directory_dataset_resource IN ACCESS EXCLUSIVE MODE NOWAIT")


@pytest.mark.asyncio
async def test_committed_batch_cancellation_keeps_incumbent_and_replays_safely(monkeypatch, cms_artifact_root):
    """Cancel after real bounded commits and recover the same unpublished candidate."""
    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(directory, receipt, "cms-test-incumbent")
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="cancelled")
        original_persist = cms._persist_source_batch
        batch_gate = asyncio.Lock()
        committed_batch_numbers = []
        cancellation = asyncio.CancelledError()

        async def cancel_after_three_commits(*args, **kwargs):
            async with batch_gate:
                if len(committed_batch_numbers) == 3:
                    raise cancellation
                await original_persist(*args, **kwargs)
                committed_batch_numbers.append(len(committed_batch_numbers) + 1)
                if len(committed_batch_numbers) == 3:
                    raise cancellation

        monkeypatch.setattr(cms, "BATCH_SIZE", 1)
        with monkeypatch.context() as cancelled:
            cancelled.setattr(cms, "_persist_source_batch", cancel_after_three_commits)
            with pytest.raises(asyncio.CancelledError) as caught:
                await _admit_legacy_source(next_directory, next_receipt, "cms-test-cancelled")
        assert caught.value is cancellation
        assert committed_batch_numbers == [1, 2, 3]
        await _assert_current(database, incumbent["dataset_id"])
        candidate = await database.first(
            "SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset WHERE status='acquiring'"
        )
        assert candidate is not None
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_dataset_resource WHERE dataset_id=:dataset_id",
                dataset_id=candidate[0],
            )
            == 3
        )
        resumed = await _admit_legacy_source(next_directory, next_receipt, "cms-test-resumed")
        assert resumed["dataset_id"] == candidate[0]
        await _assert_current(database, resumed["dataset_id"])
        assert resumed["resource_count"] == 9


@pytest.mark.asyncio
async def test_replaced_vector_retires_acquiring_candidate_and_admits_next(monkeypatch, cms_artifact_root):
    """Dispose a stale candidate and resume an interrupted recurrence."""

    first_directory, first_receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(first_directory, first_receipt, "cms-test-incumbent")
        stale_directory, stale_receipt = retained_release(cms_artifact_root, revision="stale-acquiring")
        original_verify = cms.source.verify_retained_release
        checks = count(1)

        def changed_vector(*_args, **_kwargs):
            if next(checks) == 2:
                raise cms.source.CmsNpdSourceError("cms_npd_source_vector_changed")
            return original_verify(*_args, **_kwargs)

        monkeypatch.setattr(cms.source, "verify_retained_release", changed_vector)
        with pytest.raises(cms.source.CmsNpdSourceError, match="source_vector_changed"):
            await _admit_legacy_source(stale_directory, stale_receipt, "cms-test-stale-acquiring")
        monkeypatch.setattr(cms.source, "verify_retained_release", original_verify)
        incumbent_state = await fhir._endpoint_dataset_state(incumbent["dataset_id"])
        stale_id = fhir._endpoint_dataset_candidate_id(
            incumbent_state["endpoint_id"],
            tuple(sorted(cms.RESOURCE_SET)),
            stale_receipt["vector_sha256"],
        )
        await _assert_failed_disposition(database, stale_id, stale_receipt)
        await _assert_current(database, incumbent["dataset_id"])
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="after-stale-acquiring")
        replacement = await _admit_legacy_source(next_directory, next_receipt, "cms-test-after-stale-acquiring")
        await _assert_current(database, replacement["dataset_id"])
        original_check = cms._verify_or_dispose

        async def interrupt_recurrence(*_args):
            raise RuntimeError("synthetic_recurrence_interruption")

        monkeypatch.setattr(cms, "_verify_or_dispose", interrupt_recurrence)
        with pytest.raises(RuntimeError, match="synthetic_recurrence_interruption"):
            await _admit_legacy_source(stale_directory, stale_receipt, "cms-test-recurred-interrupted")
        monkeypatch.setattr(cms, "_verify_or_dispose", original_check)
        interrupted = await recovery.reusable_vector_candidate(
            fhir, incumbent_state["endpoint_id"], cms.release_identity(stale_receipt)
        )
        assert interrupted is not None and interrupted != stale_id
        recurred = await _admit_legacy_source(stale_directory, stale_receipt, "cms-test-recurred-acquiring")
        assert recurred["dataset_id"] == interrupted
        assert recurred["dataset_id"] != stale_id
        await _assert_current(database, recurred["dataset_id"])
        later_directory, later_receipt = retained_release(cms_artifact_root, revision="after-recurrence")
        later = await _admit_legacy_source(later_directory, later_receipt, "cms-test-after-recurrence")
        restored = await _admit_legacy_source(
            stale_directory,
            stale_receipt,
            "cms-test-restore-recurrence",
            {"cms_npd_rollback_vector_sha256": stale_receipt["vector_sha256"]},
        )
        assert restored["dataset_id"] not in {stale_id, recurred["dataset_id"], later["dataset_id"]}
        await _assert_current(database, restored["dataset_id"])


@pytest.mark.asyncio
async def test_interrupted_stale_cleanup_resumes_before_next_vector(monkeypatch, cms_artifact_root):
    first_directory, first_receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(first_directory, first_receipt, "cms-test-incumbent")
        stale_directory, stale_receipt = retained_release(cms_artifact_root, revision="stale-interrupted")
        original_verify = cms._verify_release
        original_cleanup = recovery._clear_failed_rows

        async def changed_vector(directory, receipt, client, task=None):
            if client is None:
                raise cms.source.CmsNpdSourceError("cms_npd_source_vector_changed")
            return await original_verify(directory, receipt, client, task)

        monkeypatch.setattr(cms, "_verify_release", changed_vector)
        monkeypatch.setattr(
            recovery, "_clear_failed_rows", AsyncMock(side_effect=RuntimeError("synthetic_cleanup_interruption"))
        )
        with pytest.raises(RuntimeError, match="synthetic_cleanup_interruption"):
            await _admit_legacy_source(stale_directory, stale_receipt, "cms-test-stale-interrupted")
        monkeypatch.setattr(cms, "_verify_release", original_verify)
        monkeypatch.setattr(recovery, "_clear_failed_rows", original_cleanup)
        incumbent_state = await fhir._endpoint_dataset_state(incumbent["dataset_id"])
        stale_id = fhir._endpoint_dataset_candidate_id(
            incumbent_state["endpoint_id"], tuple(sorted(cms.RESOURCE_SET)), stale_receipt["vector_sha256"]
        )
        assert (await fhir._endpoint_dataset_state(stale_id))["status"] == "failed"
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_dataset_resource WHERE dataset_id=:dataset_id",
                dataset_id=stale_id,
            )
            == 9
        )
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="after-interruption")
        replacement = await _admit_legacy_source(next_directory, next_receipt, "cms-test-after-interruption")
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_dataset_resource WHERE dataset_id=:dataset_id",
                dataset_id=stale_id,
            )
            == 0
        )
        await _assert_current(database, replacement["dataset_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("acquiring", "validated"))
async def test_new_sealed_vector_disposes_interrupted_prior_candidate(monkeypatch, cms_artifact_root, phase):
    """Recover when the old run never reached its source-vector recheck."""

    first_directory, first_receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(first_directory, first_receipt, "cms-test-incumbent")
        stale_directory, stale_receipt = retained_release(cms_artifact_root, revision="interrupted-" + phase)
        if phase == "acquiring":
            original = cms._verify_or_dispose

            async def interrupt(*_args):
                raise RuntimeError("synthetic_prevalidation_interruption")

            monkeypatch.setattr(cms, "_verify_or_dispose", interrupt)
        else:
            original = publish_validated_source_local_dataset

            async def interrupt(*_args, **_kwargs):
                raise RuntimeError("synthetic_prepublication_interruption")

            monkeypatch.setattr(f"{__name__}.publish_validated_source_local_dataset", interrupt)
        with pytest.raises(RuntimeError, match="synthetic_.*_interruption"):
            await _admit_legacy_source(stale_directory, stale_receipt, "cms-test-interrupted-" + phase)
        if phase == "acquiring":
            monkeypatch.setattr(cms, "_verify_or_dispose", original)
        else:
            monkeypatch.setattr(f"{__name__}.publish_validated_source_local_dataset", original)
        incumbent_state = await fhir._endpoint_dataset_state(incumbent["dataset_id"])
        stale_id = fhir._endpoint_dataset_candidate_id(
            incumbent_state["endpoint_id"], tuple(sorted(cms.RESOURCE_SET)), stale_receipt["vector_sha256"]
        )
        assert (await fhir._endpoint_dataset_state(stale_id))["status"] == phase
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="after-" + phase)
        monkeypatch.setattr(recovery, "RECOVERY_PAGE_SIZE", 1)
        replacement = await _admit_legacy_source(next_directory, next_receipt, "cms-test-after-" + phase)
        await _assert_current(database, replacement["dataset_id"])
        disposition = await database.first(
            "SELECT prior_status FROM mrf.provider_directory_cms_npd_stale_candidate WHERE dataset_id=:dataset_id",
            dataset_id=stale_id,
        )
        assert disposition[0] == phase
        assert (await fhir._endpoint_dataset_state(stale_id))["status"] == (
            "failed" if phase == "acquiring" else "validated"
        )


@pytest.mark.asyncio
async def test_replaced_vector_disposes_validated_candidate_and_fences_cutover(monkeypatch, cms_artifact_root):
    """A stale validated proof cannot be promoted and cannot block a new vector."""

    first_directory, first_receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(first_directory, first_receipt, "cms-test-incumbent")
        stale_directory, stale_receipt = retained_release(cms_artifact_root, revision="stale-validated")
        original_verify = cms.source.verify_release
        original_verify_or_dispose = cms._verify_or_dispose
        original_publish = publish_validated_source_local_dataset
        captured_fences = []
        verification_calls = count(1)

        def changed_after_validation(*args, **kwargs):
            if captured_fences:
                raise cms.source.CmsNpdSourceError("cms_npd_source_vector_changed")
            return original_verify(*args, **kwargs)

        async def capture_fence_before_final_recheck(*args, **kwargs):
            if next(verification_calls) == 3:
                captured_fences.append(
                    await fhir._resolve_provider_directory_artifact_datasets(
                        ["cms-npd"], should_select_validated_candidates=True
                    )
                )
            return await original_verify_or_dispose(*args, **kwargs)

        monkeypatch.setattr(cms.source, "verify_release", changed_after_validation)
        monkeypatch.setattr(cms, "_verify_or_dispose", capture_fence_before_final_recheck)
        publisher = AsyncMock(side_effect=AssertionError("stale release reached cutover"))
        monkeypatch.setattr(f"{__name__}.publish_validated_source_local_dataset", publisher)
        with pytest.raises(cms.source.CmsNpdSourceError, match="source_vector_changed"):
            await _admit_legacy_source(stale_directory, stale_receipt, "cms-test-stale-validated")
        monkeypatch.setattr(cms.source, "verify_release", original_verify)
        monkeypatch.setattr(cms, "_verify_or_dispose", original_verify_or_dispose)
        monkeypatch.setattr(f"{__name__}.publish_validated_source_local_dataset", original_publish)
        publisher.assert_not_awaited()
        assert len(captured_fences) == 1
        stale_fence = captured_fences[0]
        stale_id = stale_fence.promotion_datasets[0].dataset_id
        await _assert_validated_disposition(database, stale_id)
        selected = await fhir._resolve_provider_directory_artifact_datasets(
            ["cms-npd"], should_select_validated_candidates=True
        )
        assert selected.datasets[0].dataset_id == incumbent["dataset_id"]
        unscoped = await fhir._resolve_provider_directory_artifact_datasets(
            None, should_select_validated_candidates=True
        )
        assert all(dataset.dataset_id != stale_id for dataset in unscoped.datasets)
        with pytest.raises(RuntimeError, match="candidate_changed"):
            async with database.transaction():
                await fhir._lock_artifact_cutover_fence(stale_fence)
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="after-stale-validated")
        replacement = await _admit_legacy_source(next_directory, next_receipt, "cms-test-after-stale-validated")
        await _assert_current(database, replacement["dataset_id"])
        recurred = await _admit_legacy_source(stale_directory, stale_receipt, "cms-test-recurred-validated")
        assert recurred["dataset_id"] != stale_id
        await _assert_current(database, recurred["dataset_id"])


@pytest.mark.asyncio
async def test_missing_eighth_file_never_publishes_seven_file_candidate(monkeypatch, cms_artifact_root):
    """Retain an incumbent while the last file disappears after acquisition."""
    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(directory, receipt, "cms-test-incumbent")
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="interrupted")
        last_file = next_directory / (cms.source.RESOURCE_FILES[-1][0] + ".zst")
        retained_bytes = last_file.read_bytes()
        last_file.unlink()
        with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_retained_file_missing"):
            await _admit_legacy_source(next_directory, next_receipt, "cms-test-interrupted")
        await _assert_current(database, incumbent["dataset_id"])
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_endpoint_dataset WHERE status='acquiring'"
            )
            == 0
        )
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_entity_release_evidence WHERE release_id=:release_id",
                release_id=next_receipt["vector_sha256"],
            )
            == 0
        )
        last_file.write_bytes(retained_bytes)
        resumed = await _admit_legacy_source(next_directory, next_receipt, "cms-test-repaired")
        await _assert_current(database, resumed["dataset_id"])
        assert resumed["resource_count"] == 9


@pytest.mark.asyncio
async def test_changed_retained_bytes_dispose_candidate_before_identity_and_retry(monkeypatch, cms_artifact_root):
    """A same-count file swap cannot leave reusable rows or release evidence."""
    directory, receipt = retained_release(cms_artifact_root)
    async with admission_database(monkeypatch) as database:
        incumbent = await _admit_legacy_source(directory, receipt, "cms-test-incumbent")
        next_directory, next_receipt = retained_release(cms_artifact_root, revision="next")
        alternate_directory, _ = retained_release(cms_artifact_root, revision="alternate")
        filename = cms.source.RESOURCE_FILES[0][0] + ".zst"
        retained_file = next_directory / filename
        retained_bytes = retained_file.read_bytes()
        alternate_bytes = (alternate_directory / filename).read_bytes()
        original_verify = cms.source.verify_release
        verification_calls = count(1)

        def swap_after_intake(*args, **kwargs):
            verified = original_verify(*args, **kwargs)
            if next(verification_calls) == 1:
                retained_file.write_bytes(alternate_bytes)
            return verified

        monkeypatch.setattr(cms.source, "verify_release", swap_after_intake)
        with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_retained_file_missing"):
            await _admit_legacy_source(next_directory, next_receipt, "cms-test-changed-retained")
        await _assert_current(database, incumbent["dataset_id"])
        incumbent_state = await fhir._endpoint_dataset_state(incumbent["dataset_id"])
        stale_id = fhir._endpoint_dataset_candidate_id(
            incumbent_state["endpoint_id"], tuple(sorted(cms.RESOURCE_SET)), next_receipt["vector_sha256"]
        )
        await _assert_failed_disposition(database, stale_id, next_receipt)
        assert (
            await database.scalar(
                "SELECT count(*) FROM mrf.provider_directory_entity_release_evidence WHERE release_id=:release_id",
                release_id=next_receipt["vector_sha256"],
            )
            == 0
        )

        retained_file.write_bytes(retained_bytes)
        monkeypatch.setattr(cms.source, "verify_release", original_verify)
        retried = await _admit_legacy_source(next_directory, next_receipt, "cms-test-retained-retry")
        assert retried["dataset_id"] != stale_id
        await _assert_current(database, retried["dataset_id"])


@pytest.mark.parametrize("query", ("database=unowned_example", "host=example.invalid", "port=5433"))
def test_admission_database_url_rejects_query_overrides(monkeypatch, query):
    monkeypatch.setenv(
        support._DSN_ENV,
        "postgresql+asyncpg://test_role@localhost:5432/hc_cms_admission_test_" + "1" * 32 + "?" + query,
    )
    with pytest.raises(pytest.fail.Exception, match="UUID-owned local PostgreSQL test database"):
        support._database_url()


@pytest.mark.asyncio
async def test_migrated_clone_setup_failure_removes_owned_database(monkeypatch):
    """A connection setup failure must leave neither a cloned database nor a backend."""
    attempted_database_names = []
    original_connect = support.Database.connect

    async def fail_after_connect(database):
        await original_connect(database)
        attempted_database_names.append(await database.scalar("SELECT current_database()"))
        raise RuntimeError("synthetic_connection_setup_failed")

    monkeypatch.setattr(support.Database, "connect", fail_after_connect)
    with pytest.raises(RuntimeError, match="synthetic_connection_setup_failed"):
        async with admission_database(monkeypatch):
            pytest.fail("failed setup yielded a database")
    assert len(attempted_database_names) == 1
    engine = create_async_engine(support._database_url())
    try:
        async with engine.connect() as connection:
            for catalog in ("pg_database", "pg_stat_activity"):
                assert not await connection.scalar(
                    text(f"SELECT EXISTS (SELECT 1 FROM {catalog} WHERE datname=:database_name)"),
                    {"database_name": attempted_database_names[0]},
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_historical_template_refuses_a_different_requested_profile(monkeypatch):
    """The real historical schema cannot silently satisfy a request for current receipt guards."""
    async with admission_database(monkeypatch) as database:
        assert await database.scalar("SELECT to_regclass('mrf.provider_directory_cms_serving_receipt') IS NULL")
        assert await database.scalar("SELECT to_regclass('mrf.provider_directory_cms_candidate_coverage') IS NOT NULL")
    with pytest.raises(AssertionError, match="migration profile mismatch"):
        async with support.admission_database(monkeypatch):
            pytest.fail("current migration profile used a historical template")
