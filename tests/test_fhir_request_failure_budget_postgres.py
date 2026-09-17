# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable native/CI PostgreSQL budget guards; no upstream requests."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import uuid

import asyncpg
import pytest

from tests.formulary_fhir_twin_admission_pg_support import connect, database_url, run_migration
from tests.test_fhir_request_failure_budget_migration import migration


def proof(total=51, failed=1, rooted_total=50, rooted_failed=1):
    return {"policy_id": "healthporta.fhir.request-failure-budget.v1",
            "total_requests": total, "failed_requests": failed,
            "rooted_total_requests": rooted_total, "rooted_failed_requests": rooted_failed,
            "resource_coverage": "unknown"}


@asynccontextmanager
async def coverage_scope():
    connection = await connect(database_url())
    schema = "fhir_twin_test_" + uuid.uuid4().hex
    budget_migration = migration()
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(budget_migration._coverage_valid_sql(schema))
        yield connection, schema, budget_migration
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_strict_budget_and_closed_shape():
    async with coverage_scope() as (connection, schema, _):
        for value, expected in (
            (proof(), True), (proof(total=50), False), (proof(total=100, failed=2), False),
            (proof(total=101, failed=2), True), (proof(failed=0), False),
            (proof(rooted_failed=2), False), (proof(rooted_total=51), False),
            (proof(rooted_total=0, rooted_failed=0), False),
            (proof(total=1000, failed=2, rooted_total=999, rooted_failed=0), False),
            (proof(total=9007199254740991), True), (proof(total=9007199254740992), False),
            ({**proof(), "extra": 1}, False), ({**proof(), "resource_coverage": "complete"}, False),
            ({**proof(), "failed_requests": "1"}, False), ({**proof(), "failed_requests": True}, False),
            ({**proof(), "failed_requests": 1.5}, False), (None, False), ([], False),
        ):
            assert await connection.fetchval(
                f'SELECT "{schema}".provider_directory_fhir_request_failure_coverage_valid($1::jsonb)',
                json.dumps(value),
            ) is expected


async def seed_census_tables(connection, schema, budget_migration):
    rooted = budget_migration._partial()._rooted()
    await connection.execute(f"""
        CREATE TABLE "{schema}".{rooted._ACQUISITION} (
            acquisition_id text, root_dataset_id text, root_endpoint_id text, root_dataset_hash text,
            root_source_id text, root_publication_contract_id text, root_cohort_id text,
            root_resource_count bigint, root_content_proof_sha256 text, source_authority_id text);
        CREATE TABLE "{schema}".{rooted._DATASET} (
            dataset_id text, endpoint_id text, dataset_hash text, status text, publication_metadata_json jsonb);
        CREATE TABLE "{schema}".{rooted._LEGACY_DATASET} (
            dataset_id text, source_id text, endpoint_id text, publication_contract_id text, dataset_hash text,
            cohort_id text, resource_count bigint, terminal_set_sha256 text, cohort_complete boolean);
        CREATE TABLE "{schema}".{rooted._ROOTED_DATASET} (
            dataset_id text, source_id text, endpoint_id text, publication_contract_id text, dataset_hash text,
            root_cohort_id text, practitioner_resource_count bigint, root_content_proof_sha256 text,
            cohort_complete boolean);
        CREATE TABLE "{schema}".{rooted._LEGACY_COHORT} (
            cohort_id text, contract_id text, authority_id text, official_source_id text, resource_type text,
            cohort_complete boolean, endpoint_collection_complete boolean, endpoint_complete boolean, npi_count bigint);
        CREATE TABLE "{schema}".{rooted._WORK} (
            acquisition_id text, query_id text PRIMARY KEY, status text, error_code text, attempt_count integer);
        INSERT INTO "{schema}".{rooted._ACQUISITION}
            VALUES ('candidate', 'root', 'endpoint', 'hash', 'source', 'publication', 'cohort', 1, 'proof', 'authority');
        INSERT INTO "{schema}".{rooted._DATASET}
            VALUES ('root', 'endpoint', 'hash', 'published', '{{"cohort_complete":true}}');
        INSERT INTO "{schema}".{rooted._LEGACY_DATASET}
            VALUES ('root', 'source', 'endpoint', 'publication', 'hash', 'cohort', 1, 'proof', true);
        INSERT INTO "{schema}".{rooted._LEGACY_COHORT}
            VALUES ('cohort', '{rooted._OFFICIAL_COHORT_CONTRACT}', 'authority',
                    '{rooted._OFFICIAL_SOURCE_ID}', 'Practitioner', true, false, false, 50);
        INSERT INTO "{schema}".{rooted._WORK}
            VALUES ('candidate', 'logical-request', 'error', 'transport_timeout', 1000);
    """)
    await connection.execute(budget_migration._coverage_sql(schema))


@pytest.mark.asyncio
async def test_postgres_census_counts_logical_keys_not_attempts_and_fails_closed():
    async with coverage_scope() as (connection, schema, budget_migration):
        await seed_census_tables(connection, schema, budget_migration)
        rooted = budget_migration._partial()._rooted()
        query = f'SELECT "{schema}".provider_directory_rooted_graph_request_failure_coverage($1)'
        actual = json.loads(await connection.fetchval(query, "candidate"))
        assert actual == proof(rooted_total=1)
        await connection.execute(f'UPDATE "{schema}".{rooted._LEGACY_COHORT} SET npi_count = 49')
        with pytest.raises(asyncpg.CheckViolationError, match="budget_exceeded"):
            await connection.fetchval(query, "candidate")
        await connection.execute(f'UPDATE "{schema}".{rooted._LEGACY_COHORT} SET npi_count = 50')
        for status, code in (("pending", None), ("leased", None), ("error", "response_invalid"),
                             ("error", "http_status_404"), ("error", None)):
            await connection.execute(f'UPDATE "{schema}".{rooted._WORK} SET status=$1,error_code=$2', status, code)
            with pytest.raises(asyncpg.CheckViolationError, match="terminal_invalid"):
                await connection.fetchval(query, "candidate")
        await connection.execute(f'UPDATE "{schema}".{rooted._WORK} SET status=\'completed\',error_code=NULL')
        assert await connection.fetchval(query, "candidate") is None
        await connection.execute(f'UPDATE "{schema}".{rooted._LEGACY_COHORT} SET cohort_id=\'different\'')
        with pytest.raises(asyncpg.CheckViolationError, match="lineage_invalid"):
            await connection.fetchval(query, "candidate")


async def install_budget(engine, schema_name):
    from tests.provider_directory_fhir_failure_pg_support import install_request_failure_budget

    return await install_request_failure_budget(engine, schema_name)


@pytest.mark.asyncio
async def test_postgres_full_upgrade_legacy_publication_and_empty_proof_downgrade(monkeypatch):
    from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope, _publish_generation
    from tests.provider_directory_rooted_graph_rotation_pg_support import publish_legacy_root

    async with _lifecycle_scope(monkeypatch, request_failure_budget=False) as context:
        budget_migration = await install_budget(context.engine, context.schema_name)
        current = await publish_legacy_root(context.database)
        _, _, _, published = await _publish_generation(context, current, ("1", "2"), "3")
        assert published.readiness.rooted_graph_complete is True
        assert await context.connection.fetchval(
            f'SELECT request_failure_coverage FROM {context.schema}.provider_directory_rooted_graph_dataset'
        ) is None
        await run_migration(context.engine, budget_migration, "downgrade")


@pytest.mark.asyncio
async def test_postgres_fresh_admission_recomputes_policy_for_a_proofless_seal(monkeypatch):
    from sqlalchemy.exc import IntegrityError
    from process.provider_directory_rooted_graph_single_root_contract import derive_single_root_identity
    from process.provider_directory_rooted_graph_twin_contract import build_rooted_graph_single_root_admission
    from process.provider_directory_rooted_graph_twin_store import _insert_authority, _lock_single_root
    from tests.test_provider_directory_rooted_graph_acquisition_postgres import _complete_success
    from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope
    from tests.provider_directory_rooted_graph_rotation_pg_support import publish_legacy_root

    async with _lifecycle_scope(monkeypatch) as context:
        current = await publish_legacy_root(context.database)
        identity = derive_single_root_identity(current, operation_key="6" * 64)
        sealed = await _complete_success(context.database, identity.candidate)
        assert sealed.request_failure_coverage is None
        async with context.database.transaction():
            sealed_root = await _lock_single_root(context.database, identity.candidate.acquisition_id)
        admission = build_rooted_graph_single_root_admission(
            sealed_root, acquisition_operation_key="6" * 64, admitted_at=datetime.now(timezone.utc)
        )
        function = f'{context.schema}.provider_directory_rooted_graph_request_failure_coverage'
        # Emulate a historical null-proof seal whose newly required policy
        # census produces a proof or fails. The INSERT must consult that census.
        for body in (
            "RETURN '" + json.dumps(proof()) + "'::jsonb;",
            "RAISE EXCEPTION 'provider_directory_fhir_request_failure_budget_exceeded' USING ERRCODE='23514';",
        ):
            await context.connection.execute(
                f"CREATE OR REPLACE FUNCTION {function}(target_acquisition_id text) RETURNS jsonb "
                f"LANGUAGE plpgsql STABLE AS $test$ BEGIN {body} END; $test$"
            )
            with pytest.raises(IntegrityError, match="single_root_admission_invalid|budget_exceeded"):
                async with context.database.transaction():
                    await _insert_authority(context.database, admission)
            assert await context.connection.fetchval(
                f"SELECT count(*) FROM {context.schema}.provider_directory_rooted_graph_twin_admission"
            ) == 0


def many_practitioner_result(npi, matched):
    from process.uhc_flex_practitioner_query import validate_uhc_flex_practitioner_search_bundle

    entries = [{"resource": {"resourceType": "Practitioner", "id": f"synthetic-{npi}-{index}",
                             "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": str(npi)}]}}
               for index in range(15)] if matched else []
    return validate_uhc_flex_practitioner_search_bundle(
        npi, {"resourceType": "Bundle", "type": "searchset", "total": len(entries), "entry": entries}
    )


async def complete_partial_root(database, identity, timeout_kind):
    from process.provider_directory_rooted_graph_store import (
        claim_provider_directory_rooted_graph_census,
        claim_provider_directory_rooted_graph_work,
        complete_provider_directory_rooted_graph_error,
        complete_provider_directory_rooted_graph_result,
        initialize_provider_directory_rooted_graph_acquisition,
        seal_provider_directory_rooted_graph_acquisition,
    )
    from process.provider_directory_rooted_graph_result_contract import build_provider_directory_rooted_graph_query_result

    await initialize_provider_directory_rooted_graph_acquisition(identity, database=database)
    request_count = 0
    has_completed_role = False
    while claim := await claim_provider_directory_rooted_graph_work(identity.acquisition_id, database=database):
        if request_count == 0 and timeout_kind == "root":
            await complete_provider_directory_rooted_graph_error(claim, error_code="transport_timeout", database=database)
        else:
            resources = []
            if claim.resource_type == "PractitionerRole" and not has_completed_role:
                resources = [{"resourceType": "PractitionerRole", "id": "synthetic-role",
                              "practitioner": {"reference": "Practitioner/" + claim.reference_id},
                              "location": [{"reference": f"Location/synthetic-location-{index}"} for index in range(60)]}]
                has_completed_role = True
            elif claim.kind == "direct_read":
                resources = [{"resourceType": claim.resource_type, "id": claim.reference_id}]
            await complete_provider_directory_rooted_graph_result(
                claim, build_provider_directory_rooted_graph_query_result(
                    claim, resources, advertised_total=len(resources) if claim.kind != "direct_read" else None
                ), database=database
            )
        request_count += 1
    census = await claim_provider_directory_rooted_graph_census(identity, database=database)
    assert census is not None
    if timeout_kind == "census":
        await complete_provider_directory_rooted_graph_error(census.work_claim, error_code="transport_timeout", database=database)
    else:
        await complete_provider_directory_rooted_graph_result(
            census.work_claim, build_provider_directory_rooted_graph_query_result(census.work_claim, [], advertised_total=0), database=database
        )
    assert request_count == 75
    return await seal_provider_directory_rooted_graph_acquisition(identity, database=database)


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_kind", ["root", "census"])
async def test_postgres_partial_single_root_publication_retains_unknown_coverage(monkeypatch, timeout_kind):
    from sqlalchemy.exc import DBAPIError
    from process.provider_directory_rooted_graph_single_root_contract import derive_single_root_identity
    from process.provider_directory_rooted_graph_twin_store import admit_rooted_graph_single_root
    from process.provider_directory_rooted_graph_publication import publish_provider_directory_rooted_graph_dataset
    from process.provider_directory_rooted_graph_store import initialize_provider_directory_rooted_graph_acquisition
    from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope
    from tests.provider_directory_rooted_graph_rotation_pg_support import publish_legacy_root, locked_exact_current
    import tests.test_provider_directory_uhc_flex_practitioner_publication_postgres as flex_fixture

    monkeypatch.setattr(flex_fixture, "_query_result", many_practitioner_result)
    async with _lifecycle_scope(monkeypatch, request_failure_budget=False) as context:
        budget_migration = await install_budget(context.engine, context.schema_name)
        current = await publish_legacy_root(context.database)
        identity = derive_single_root_identity(current, operation_key="8" * 64)
        sealed = await complete_partial_root(context.database, identity.candidate, timeout_kind)
        expected = proof(total=78, rooted_total=76)
        assert sealed.request_failure_coverage == expected
        assert sealed.rooted_graph_complete is False
        assert await context.connection.fetchval(
            f"SELECT insurance_plan_count FROM {context.schema}.provider_directory_rooted_graph_acquisition WHERE acquisition_id=$1",
            identity.candidate.acquisition_id,
        ) == (None if timeout_kind == "census" else 0)
        admission = await admit_rooted_graph_single_root(
            identity.candidate.acquisition_id, acquisition_operation_key="8" * 64, database=context.database
        )
        assert admission.request_failure_coverage == expected
        published = await publish_provider_directory_rooted_graph_dataset(
            admission.publication_acquisition_id, database=context.database, batch_size=10
        )
        assert published.readiness.request_failure_coverage == expected
        assert published.readiness.rooted_graph_complete is False
        assert published.readiness.cohort_complete is True
        metadata = json.loads(await context.connection.fetchval(
            f"SELECT publication_metadata_json::text FROM {context.schema}.provider_directory_endpoint_dataset WHERE dataset_id=$1",
            published.readiness.dataset_id,
        ))
        assert metadata["request_failure_coverage"] == expected
        assert metadata["retry_exhausted_count"] == 0
        assert metadata["rooted_graph_complete"] is False
        assert metadata["publication_contract_id"] == budget_migration._PUBLICATION
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await context.connection.execute(
                f"UPDATE {context.schema}.provider_directory_rooted_graph_dataset SET request_failure_coverage=NULL WHERE dataset_id=$1",
                published.readiness.dataset_id,
            )
        with pytest.raises(DBAPIError, match="downgrade_blocked"):
            await run_migration(context.engine, budget_migration, "downgrade")
        next_current = await locked_exact_current(context.database)
        next_identity = derive_single_root_identity(next_current, operation_key="9" * 64)
        assert await initialize_provider_directory_rooted_graph_acquisition(next_identity.candidate, database=context.database) == 1
