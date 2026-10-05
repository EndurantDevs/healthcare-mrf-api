# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from process.provider_directory_rooted_graph_bulk import _WORK_COLUMNS
from process.provider_directory_rooted_graph_identity import (
    build_provider_directory_rooted_graph_scope,
)
from process.provider_directory_rooted_graph_query import (
    build_insurance_plan_census_query,
    build_provider_directory_practitioner_role_query,
)
from process.provider_directory_rooted_graph_result_store import (
    complete_provider_directory_rooted_graph_result,
)
from process.provider_directory_rooted_graph_result_contract import (
    build_provider_directory_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_store import (
    claim_provider_directory_rooted_graph_census,
    claim_provider_directory_rooted_graph_work,
    initialize_provider_directory_rooted_graph_acquisition,
    release_provider_directory_rooted_graph_work,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
    build_provider_directory_rooted_graph_acquisition_identity,
    build_provider_directory_rooted_graph_work_spec,
)
from process.provider_directory_rooted_graph_store_support import identity_fields
from tests.test_provider_directory_rooted_graph_store_contract import (
    API_BASE,
    DATASET_HASH,
    ENDPOINT_ID,
    ENDPOINT_SIGNATURE,
    ROOT_ID,
    ROOT_PROOF,
    _identity,
    _scope,
)


class _Database:
    def __init__(self, *, first_rows=(), all_rows=(), status_counts=(), admit_counts=None) -> None:
        self.first_rows = iter(first_rows)
        self.all_rows = list(all_rows)
        self.status_counts = iter(status_counts)
        self.statements: list[tuple[str, dict[str, object]]] = []
        self.transaction_count = 0
        self.admit_counts = admit_counts
        self.copied_rows = {}

    @asynccontextmanager
    async def transaction(self):
        self.transaction_count += 1
        yield self

    async def scalar(self, statement, **parameters):
        self.statements.append((statement, parameters))
        if "initialize_provider_directory_rooted_graph_work" in statement:
            return 1
        if "admit_provider_directory_rooted_graph_witnesses" in statement:
            return self.admit_counts or [len(self.copied_rows.get(name, ())) for name in (
                "pdrg_resource_stage", "pdrg_edge_stage",
            )]
        if "admit_provider_directory_rooted_graph_work" in statement:
            return len(self.copied_rows.get("pdrg_work_stage", ()))
        return ""

    async def copy_records_to_table(self, name, *, schema_name, columns, records):
        assert schema_name == "pg_temp"
        self.copied_rows.setdefault(name, []).extend(records)
        return f"COPY {len(records)}"

    async def status(self, statement, **parameters):
        self.statements.append((statement, parameters))
        return next(self.status_counts, 1)

    async def first(self, statement, **parameters):
        self.statements.append((statement, parameters))
        return next(self.first_rows, None)

    async def all(self, statement, **parameters):
        self.statements.append((statement, parameters))
        return self.all_rows


def _header(identity, status="building"):
    return {**identity_fields(identity), "status": status}


def _role_spec(identity):
    return build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
        closure_scope="root",
    )


def _claim_row(identity, *, attempt=1, lease_token="5" * 64):
    spec = _role_spec(identity)
    return {
        "acquisition_id": identity.acquisition_id,
        "scope_id": identity.scope_id,
        "query_id": spec.query_id,
        "query_identity_sha256": spec.query_identity_sha256,
        "kind": spec.kind,
        "resource_type": spec.resource_type,
        "reference_type": spec.reference_type,
        "reference_id": spec.reference_id,
        "closure_scope": spec.closure_scope,
        "attempt_count": attempt,
        "lease_token": lease_token,
    }


def _census_claim_row(identity, *, attempt=1, lease_token="6" * 64):
    spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_insurance_plan_census_query(API_BASE),
        closure_scope="census",
    )
    return {
        "acquisition_id": identity.acquisition_id,
        "scope_id": identity.scope_id,
        "query_id": spec.query_id,
        "query_identity_sha256": spec.query_identity_sha256,
        "kind": spec.kind,
        "resource_type": spec.resource_type,
        "reference_type": spec.reference_type,
        "reference_id": spec.reference_id,
        "closure_scope": spec.closure_scope,
        "attempt_count": attempt,
        "lease_token": lease_token,
    }


@pytest.mark.asyncio
async def test_initialize_builds_root_queries_set_wise_and_delays_plan_census() -> None:
    identity = _identity()
    database = _Database(
        first_rows=(
            _header(identity),
            {"work_count": 1, "role_count": 1, "plan_count": 0},
        ),
    )

    assert (
        await initialize_provider_directory_rooted_graph_acquisition(
            identity,
            database=database,
        )
        == 1
    )
    sql = "\n".join(statement for statement, _parameters in database.statements)
    assert "WHERE NOT EXISTS" in sql
    assert "INSERT INTO" in sql and "SELECT" in sql
    assert "initialize_provider_directory_rooted_graph_work" in sql
    assert "database.all" not in sql
    assert "full_insurance_plan_census" in sql


@pytest.mark.asyncio
async def test_large_root_initialization_requests_bounded_pages() -> None:
    """Large root counts retain bounded page requests instead of one Python workset."""
    root_count = 1_014_311
    root = _scope()
    scope = build_provider_directory_rooted_graph_scope(
        root_dataset_variant=root.root_dataset_variant,
        root_publication_contract_id=root.root_publication_contract_id,
        root_source_id=root.root_source_id,
        root_endpoint_id=root.root_endpoint_id,
        acquisition_source_id=root.acquisition_source_id,
        acquisition_endpoint_id=ENDPOINT_ID,
        source_authority_id=root.source_authority_id,
        root_dataset_id="synthetic-million-practitioner-dataset",
        root_dataset_hash=DATASET_HASH,
        root_content_proof_sha256=ROOT_PROOF,
        root_resource_count=root_count,
        max_work_items=root.max_work_items,
        max_resource_rows=root.max_resource_rows,
        max_edge_rows=root.max_edge_rows,
        max_payload_bytes=root.max_payload_bytes,
    )
    identity = build_provider_directory_rooted_graph_acquisition_identity(
        scope,
        root_cohort_id="synthetic-million-cohort",
        endpoint_signature_sha256=ENDPOINT_SIGNATURE,
        acquisition_role="baseline",
        run_id="pdrgr_" + "7" * 48,
        dataset_intent_id="pdrgi_" + "8" * 48,
    )

    class BoundedPageDatabase(_Database):
        async def all(self, statement, **parameters):
            assert "read_provider_directory_rooted_graph_initial_work" in statement
            assert 1 <= parameters["limit"] <= 1024
            if parameters["after_id"]:
                return []
            return [{**dict.fromkeys(_WORK_COLUMNS), "reference_id": "synthetic"}]

    database = BoundedPageDatabase(
        first_rows=(
            _header(identity),
            {
                "work_count": root_count,
                "role_count": root_count,
                "plan_count": 0,
            },
        )
    )
    await initialize_provider_directory_rooted_graph_acquisition(
        identity,
        database=database,
    )

    assert sum("initialize_provider_directory_rooted_graph_work" in statement
               for statement, _parameters in database.statements) == 1
    assert len(database.copied_rows["pdrg_work_stage"]) == 1


@pytest.mark.asyncio
async def test_claim_sql_reclaims_expired_generation_and_resume_increments_attempt() -> (
    None
):
    identity = _identity()
    database = _Database(first_rows=(_claim_row(identity, attempt=3),))

    claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        query_id=_role_spec(identity).query_id,
        database=database,
    )

    assert claim is not None and claim.attempt == 3
    update_sql, parameters = database.statements[-1]
    assert "work.lease_expires_at <= clock_timestamp()" in update_sql
    assert "work.kind <> 'full_insurance_plan_census'" in update_sql
    assert "attempt_count = work.attempt_count + 1" in update_sql
    assert parameters["lease_token"] != claim.lease_token
    assert len(parameters["lease_token"]) == 64


@pytest.mark.asyncio
async def test_dedicated_census_claim_returns_db_sorted_root_network_anchors() -> None:
    identity = _identity()
    closure_by_field = {
        "canonical_api_base": API_BASE,
        "root_network_references": (
            "Organization/network.synthetic-a",
            "Organization/network.synthetic-b",
        ),
        "root_frontier_drained": True,
        "census_count": 0,
    }
    database = _Database(
        first_rows=(
            _header(identity),
            closure_by_field,
            _census_claim_row(identity),
        )
    )

    census_claim = await claim_provider_directory_rooted_graph_census(
        identity,
        database=database,
    )

    assert census_claim is not None
    assert (
        census_claim.root_network_references
        == closure_by_field["root_network_references"]
    )
    sql = "\n".join(statement for statement, _ in database.statements)
    assert "FOR SHARE" in sql
    assert "root_frontier_drained" in sql
    assert "full_insurance_plan_census" in sql
    assert "claim_census" in str(database.statements)


@pytest.mark.asyncio
async def test_release_and_terminalization_are_exact_token_fenced() -> None:
    identity = _identity()
    claim_database = _Database(first_rows=(_claim_row(identity),))
    claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        database=claim_database,
    )
    assert claim is not None
    release_database = _Database(status_counts=(1,))
    await release_provider_directory_rooted_graph_work(
        claim,
        database=release_database,
    )
    release_sql, release_parameters = release_database.statements[-1]
    assert "attempt_count = :attempt" in release_sql
    assert "lease_token = :lease_token" in release_sql
    assert release_parameters["lease_token"] == claim.lease_token

    query_result = build_provider_directory_rooted_graph_query_result(
        claim,
        [
            {
                "resourceType": "PractitionerRole",
                "id": "role.synthetic-1",
                "practitioner": {"reference": f"Practitioner/{ROOT_ID}"},
            }
        ],
    )
    stale_database = _Database(status_counts=(0,))
    with pytest.raises(ProviderDirectoryRootedGraphStoreError) as error:
        await complete_provider_directory_rooted_graph_result(
            claim,
            query_result,
            database=stale_database,
        )
    assert error.value.code == "lease_lost"
    terminal_sql, terminal_parameters = stale_database.statements[-1]
    assert "lease_expires_at > clock_timestamp()" in terminal_sql
    assert terminal_parameters["attempt"] == claim.attempt
    assert terminal_parameters["lease_token"] == claim.lease_token


@pytest.mark.asyncio
async def test_release_rejects_a_lost_lease() -> None:
    identity = _identity()
    claim_database = _Database(first_rows=(_claim_row(identity),))
    claim = await claim_provider_directory_rooted_graph_work(
        identity.acquisition_id,
        database=claim_database,
    )
    assert claim is not None
    with pytest.raises(ProviderDirectoryRootedGraphStoreError) as error:
        await release_provider_directory_rooted_graph_work(
            claim,
            database=_Database(status_counts=(0,)),
        )
    assert error.value.code == "lease_lost"
