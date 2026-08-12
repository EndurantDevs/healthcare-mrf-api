# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.connection import Database
from process.provider_directory_rooted_graph_query import (
    build_provider_directory_organization_affiliation_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_store import (
    claim_provider_directory_rooted_graph_census,
    claim_provider_directory_rooted_graph_work,
    complete_provider_directory_rooted_graph_missing,
    complete_provider_directory_rooted_graph_result,
    initialize_provider_directory_rooted_graph_acquisition,
    release_provider_directory_rooted_graph_work,
    seal_provider_directory_rooted_graph_acquisition,
)
from process.provider_directory_rooted_graph_result_contract import (
    build_provider_directory_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
    build_provider_directory_rooted_graph_work_spec,
)
from tests.provider_directory_rooted_graph_pg_assertions import (
    assert_separate_registration_is_rejected as _assert_separate_registration_is_rejected,
)
from tests.provider_directory_rooted_graph_pg_assertions import (
    assert_setwise_root_query_identity as _assert_setwise_root_query_identity,
)
from tests.provider_directory_rooted_graph_pg_support import ACQUISITION_TABLE
from tests.provider_directory_rooted_graph_pg_support import API_BASE
from tests.provider_directory_rooted_graph_pg_support import (
    acquisition_identity as _identity,
)
from tests.provider_directory_rooted_graph_pg_support import (
    configure_database as _configure_database,
)
from tests.provider_directory_rooted_graph_pg_support import (
    create_foundation as _create_foundation,
)
from tests.provider_directory_rooted_graph_pg_support import (
    expire_claim as _expire_claim,
)
from tests.provider_directory_rooted_graph_pg_support import (
    extend_publication_foundation as _extend_publication_foundation,
)
from tests.provider_directory_rooted_graph_pg_support import (
    resources_for_kind as _resources_for_kind,
)
from tests.provider_directory_rooted_graph_pg_support import work_rows as _work_rows
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    _prepare_publication_schema,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    ACQUISITION_PATH as LEGACY_ACQUISITION_PATH,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    COHORT_PATH as LEGACY_COHORT_PATH,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    PUBLICATION_PATH as LEGACY_PUBLICATION_PATH,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    TWIN_PATH as LEGACY_TWIN_PATH,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / ("20260811020000_provider_directory_rooted_graph_acquisition.py")
)
SINGLE_ROOT_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / ("20260812030000_provider_directory_specialized_single_root_admission.py")
)


def _load_legacy_migrations(label_prefix: str):
    paths = (
        LEGACY_COHORT_PATH,
        LEGACY_ACQUISITION_PATH,
        LEGACY_TWIN_PATH,
        LEGACY_PUBLICATION_PATH,
    )
    return tuple(
        load_migration(path, f"{label_prefix}_{index}")
        for index, path in enumerate(paths)
    )


def _query_result_for_work(claim, work_kind: str, root_network_references=()):
    resource_inputs = _resources_for_kind(work_kind)
    advertised_total = (
        len(resource_inputs)
        if work_kind
        in {
            "exact_reference_search",
            "full_insurance_plan_census",
        }
        else None
    )
    return build_provider_directory_rooted_graph_query_result(
        claim,
        resource_inputs,
        advertised_total=advertised_total,
        reachable_network_references=root_network_references,
    )


async def _reclaim_work_claim(
    database: Database,
    identity,
    work_record,
    initial_claim,
    reclaim_connection,
    schema_name: str,
):
    await release_provider_directory_rooted_graph_work(
        initial_claim,
        database=database,
    )
    resumed_claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        query_id=work_record.query_id,
        database=database,
    )
    assert resumed_claim is not None
    assert resumed_claim.attempt == initial_claim.attempt + 1
    await _expire_claim(reclaim_connection, schema_name, identity.acquisition_id)
    reclaimed_claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        query_id=work_record.query_id,
        database=database,
    )
    assert reclaimed_claim is not None
    assert reclaimed_claim.attempt == resumed_claim.attempt + 1
    assert reclaimed_claim.lease_token != resumed_claim.lease_token
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await complete_provider_directory_rooted_graph_result(
            resumed_claim,
            build_provider_directory_rooted_graph_query_result(resumed_claim, []),
            database=database,
        )
    return reclaimed_claim


async def _complete_initial_work(
    database: Database,
    identity,
    reclaim_connection,
    schema_name: str,
):
    role_query_result = None
    work_records = await _work_rows(database, identity.acquisition_id)
    for work_record in work_records:
        active_claim = await claim_provider_directory_rooted_graph_work(
            identity.acquisition_id,
            query_id=work_record.query_id,
            database=database,
        )
        assert active_claim is not None
        if (
            reclaim_connection is not None
            and work_record.kind == "exact_reference_search"
        ):
            active_claim = await _reclaim_work_claim(
                database,
                identity,
                work_record,
                active_claim,
                reclaim_connection,
                schema_name,
            )
        query_result = _query_result_for_work(active_claim, work_record.kind)
        await complete_provider_directory_rooted_graph_result(
            active_claim,
            query_result,
            database=database,
        )
        if work_record.kind == "exact_reference_search":
            role_query_result = query_result
    assert role_query_result is not None
    return role_query_result


async def _complete_organization_read(database, identity, role_query_result):
    organization_edge = next(
        edge
        for edge in role_query_result.edges
        if edge.target_resource_type == "Organization"
    )
    direct_spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_rooted_graph_direct_read(
            api_base=API_BASE,
            resource_type="Organization",
            resource_id="org.synthetic-1",
        ),
        closure_scope="root",
        discovered_by_query_id=role_query_result.query_id,
        discovered_source_type="PractitionerRole",
        discovered_source_id="role.synthetic-1",
        discovered_edge_sha256=organization_edge.edge_sha256,
    )
    direct_claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        query_id=direct_spec.query_id,
        database=database,
    )
    assert direct_claim is not None
    direct_result = build_provider_directory_rooted_graph_query_result(
        direct_claim,
        [{"resourceType": "Organization", "id": "org.synthetic-1"}],
    )
    await complete_provider_directory_rooted_graph_result(
        direct_claim,
        direct_result,
        database=database,
    )
    return direct_spec


async def _complete_missing_endpoint(
    database,
    identity,
    role_query_result,
    *,
    missing_http_status: int = 404,
) -> None:
    endpoint_claim = await _claim_missing_endpoint(
        database,
        identity,
        role_query_result,
    )
    missing_response_json_text = json.dumps(
        {
            "issue": [
                {
                    "code": ("not-found" if missing_http_status == 404 else "deleted"),
                    "severity": "error",
                }
            ],
            "resourceType": "OperationOutcome",
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    missing_response_bytes = len(missing_response_json_text.encode("utf-8"))
    await complete_provider_directory_rooted_graph_missing(
        endpoint_claim,
        missing_http_status=missing_http_status,
        missing_response_sha256=hashlib.sha256(
            missing_response_json_text.encode("utf-8")
        ).hexdigest(),
        missing_response_bytes=missing_response_bytes,
        missing_response_json_text=missing_response_json_text,
        database=database,
    )


async def _claim_missing_endpoint(database, identity, role_query_result):
    endpoint_edge = next(
        edge
        for edge in role_query_result.edges
        if edge.target_resource_type == "Endpoint"
    )
    endpoint_spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_rooted_graph_direct_read(
            api_base=API_BASE,
            resource_type="Endpoint",
            resource_id="endpoint.synthetic-missing",
        ),
        closure_scope="root",
        discovered_by_query_id=role_query_result.query_id,
        discovered_source_type="PractitionerRole",
        discovered_source_id="role.synthetic-1",
        discovered_edge_sha256=endpoint_edge.edge_sha256,
    )
    endpoint_claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        query_id=endpoint_spec.query_id,
        database=database,
    )
    assert endpoint_claim is not None
    return endpoint_claim


async def _complete_affiliation(database, identity, direct_spec) -> None:
    affiliation_spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_organization_affiliation_query(
            API_BASE,
            "org.synthetic-1",
        ),
        closure_scope="root",
        discovered_by_query_id=direct_spec.query_id,
        discovered_source_type="Organization",
        discovered_source_id="org.synthetic-1",
    )
    affiliation_claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        query_id=affiliation_spec.query_id,
        database=database,
    )
    assert affiliation_claim is not None
    await complete_provider_directory_rooted_graph_result(
        affiliation_claim,
        build_provider_directory_rooted_graph_query_result(affiliation_claim, []),
        database=database,
    )


async def _complete_success(
    database: Database,
    identity,
    *,
    reclaim_connection=None,
    schema_name: str = "",
    missing_http_status: int = 404,
):
    """Complete root fixed point, dedicated census, and every derived query."""

    assert (
        await initialize_provider_directory_rooted_graph_acquisition(
            identity,
            database=database,
        )
        == 1
    )
    await _assert_setwise_root_query_identity(database, identity)
    role_query_result = await _complete_initial_work(
        database,
        identity,
        reclaim_connection,
        schema_name,
    )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await claim_provider_directory_rooted_graph_census(
            identity,
            database=database,
        )
    direct_spec = await _complete_organization_read(
        database,
        identity,
        role_query_result,
    )
    await _assert_separate_registration_is_rejected(
        database,
        identity,
        direct_spec,
    )
    await _complete_missing_endpoint(
        database,
        identity,
        role_query_result,
        missing_http_status=missing_http_status,
    )
    await _complete_affiliation(database, identity, direct_spec)
    return await _complete_census_and_seal(database, identity)


async def _complete_census_and_seal(database: Database, identity):
    """Admit the DB-derived census claim, terminalize it, and seal."""

    assert (
        await claim_provider_directory_rooted_graph_work(
            identity.acquisition_id,
            database=database,
        )
        is None
    )
    census_claim = await claim_provider_directory_rooted_graph_census(
        identity,
        database=database,
    )
    assert census_claim is not None
    assert census_claim.root_network_references == ("Organization/org.synthetic-1",)
    census_result = _query_result_for_work(
        census_claim.work_claim,
        "full_insurance_plan_census",
        census_claim.root_network_references,
    )
    await complete_provider_directory_rooted_graph_result(
        census_claim.work_claim,
        census_result,
        database=database,
    )
    return await seal_provider_directory_rooted_graph_acquisition(
        identity,
        database=database,
    )


@pytest.mark.asyncio
async def test_rooted_graph_migration_upgrades_and_empty_downgrades(
    monkeypatch,
) -> None:
    """Prove the empty schema can upgrade and downgrade without residue."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    migration = load_migration(MIGRATION_PATH, "rooted_graph_empty_migration")
    legacy_migrations = _load_legacy_migrations("rooted_graph_empty_legacy")
    try:
        await _prepare_publication_schema(
            engine,
            url,
            schema_name,
            quoted(schema_name),
            legacy_migrations,
        )
        connection = await connect(url)
        try:
            await _extend_publication_foundation(connection, schema_name)
        finally:
            await connection.close()
        await run_migration(engine, migration, "upgrade")
        connection = await connect(url)
        try:
            assert (
                await connection.fetchval(
                    "SELECT to_regclass($1)",
                    f"{schema_name}.{ACQUISITION_TABLE}",
                )
                == f"{schema_name}.{ACQUISITION_TABLE}"
            )
        finally:
            await connection.close()
        await run_migration(engine, migration, "downgrade")
        connection = await connect(url)
        try:
            assert (
                await connection.fetchval(
                    "SELECT to_regclass($1)",
                    f"{schema_name}.{ACQUISITION_TABLE}",
                )
                is None
            )
        finally:
            await connection.close()
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()
