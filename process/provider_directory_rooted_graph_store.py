# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant, fenced persistence for source-neutral rooted graphs."""

from __future__ import annotations

import secrets
from typing import Any

from db.connection import db
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE,
)
from process.provider_directory_rooted_graph_persistence_sql import (
    initial_root_work_sql,
    root_closure_sql,
)
from process.provider_directory_rooted_graph_query import (
    build_insurance_plan_census_query,
)
from process.provider_directory_rooted_graph_result_store import (
    complete_provider_directory_rooted_graph_error,
    complete_provider_directory_rooted_graph_missing,
    complete_provider_directory_rooted_graph_result,
    seal_provider_directory_rooted_graph_acquisition,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphAcquisitionSummary,
    ProviderDirectoryRootedGraphQueryResult,
)
from process.provider_directory_rooted_graph_store_contract import (
    ACQUISITION_PATTERN,
    ROOTED_GRAPH_QUERY_PATTERN,
    ProviderDirectoryRootedGraphAcquisitionIdentity,
    ProviderDirectoryRootedGraphCensusClaim,
    ProviderDirectoryRootedGraphStoreError,
    ProviderDirectoryRootedGraphWorkClaim,
    ProviderDirectoryRootedGraphWorkSpec,
    build_provider_directory_rooted_graph_acquisition_identity,
    build_provider_directory_rooted_graph_work_spec,
)
from process.provider_directory_rooted_graph_store_support import (
    ACQUISITION_TABLE,
    EDGE_TABLE,
    RESOURCE_TABLE,
    WORK_TABLE,
    assert_identity_row,
    identity_fields,
    insert_work_spec,
    row_fields,
    set_store_action,
    table_ref,
)


async def _insert_header(
    database: Any,
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
) -> int:
    return await database.status(
        f"""
        INSERT INTO {table_ref(ACQUISITION_TABLE)} (
            acquisition_id, storage_contract_id, scope_id,
            root_publication_contract_id, root_source_id, root_endpoint_id,
            acquisition_source_id,
            acquisition_endpoint_id, source_authority_id,
            root_dataset_variant, endpoint_signature_sha256,
            root_dataset_id, root_dataset_hash, root_content_proof_sha256,
            root_cohort_id, root_resource_type, root_resource_count,
            connector_id, graph_contract_sha256, query_contract_sha256,
            acquisition_role, run_id, dataset_intent_id, status,
            max_work_items, max_resource_rows, max_edge_rows,
            max_payload_bytes, used_work_items, used_resource_rows,
            used_edge_rows, used_payload_bytes,
            rooted_graph_complete, endpoint_collection_complete, endpoint_complete
        ) SELECT
            CAST(:acquisition_id AS varchar(54)), :storage_contract_id, :scope_id,
            :root_publication_contract_id, :root_source_id, :root_endpoint_id,
            :acquisition_source_id,
            :acquisition_endpoint_id, :source_authority_id,
            :root_dataset_variant, :endpoint_signature_sha256,
            :root_dataset_id, :root_dataset_hash, :root_content_proof_sha256,
            :root_cohort_id, 'Practitioner', :root_resource_count,
            :connector_id, :graph_contract_sha256, :query_contract_sha256,
            :acquisition_role, :run_id, :dataset_intent_id, 'building',
            :max_work_items, :max_resource_rows, :max_edge_rows,
            :max_payload_bytes, 0, 0, 0, 0,
            :rooted_graph_complete, :endpoint_collection_complete, :endpoint_complete
         WHERE NOT EXISTS (
            SELECT 1
              FROM {table_ref(ACQUISITION_TABLE)}
             WHERE acquisition_id = CAST(:acquisition_id AS varchar(54))
        ) ON CONFLICT (acquisition_id) DO NOTHING;
        """,
        **identity_fields(identity),
    )


async def _insert_initial_root_work(
    database: Any,
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
) -> int:
    return await database.status(
        initial_root_work_sql(),
        acquisition_id=identity.acquisition_id,
        scope_id=identity.scope_id,
        root_dataset_id=identity.root_dataset_id,
        identity_contract=PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID,
        page_size=str(PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE),
    )


async def _initial_work_census(
    database: Any,
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
) -> dict[str, Any]:
    return row_fields(
        await database.first(
            f"""
            SELECT count(*)::bigint AS work_count,
                   count(*) FILTER (
                       WHERE kind = 'exact_reference_search'
                         AND resource_type = 'PractitionerRole'
                   )::bigint AS role_count,
                   count(*) FILTER (
                       WHERE kind = 'full_insurance_plan_census'
                   )::bigint AS plan_count
              FROM {table_ref(WORK_TABLE)}
             WHERE acquisition_id = :acquisition_id;
            """,
            acquisition_id=identity.acquisition_id,
        )
    )


async def initialize_provider_directory_rooted_graph_acquisition(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    database: Any = db,
) -> int:
    """Create one exact header and set-wise root-role workset."""

    if type(identity) is not ProviderDirectoryRootedGraphAcquisitionIdentity:
        raise ValueError("provider_directory_rooted_graph_identity_invalid")
    async with database.transaction():
        await database.scalar(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended(:identity, 0));",
            identity=identity.acquisition_id,
        )
        created_count = await _insert_header(database, identity)
        header = assert_identity_row(
            identity,
            await database.first(
                f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} "
                "WHERE acquisition_id = :acquisition_id FOR SHARE;",
                acquisition_id=identity.acquisition_id,
            ),
        )
        if header["status"] == "sealed":
            return created_count
        await set_store_action(database, "initialize", identity.acquisition_id)
        await _insert_initial_root_work(database, identity)
        census = await _initial_work_census(database, identity)
        if census != {
            "work_count": identity.root_resource_count,
            "role_count": identity.root_resource_count,
            "plan_count": 0,
        }:
            raise ProviderDirectoryRootedGraphStoreError("state")
        initialized_count = created_count
    return initialized_count


def _claim_from_row(database_row: Any) -> ProviderDirectoryRootedGraphWorkClaim | None:
    fields = row_fields(database_row)
    if not fields:
        return None
    try:
        return ProviderDirectoryRootedGraphWorkClaim(
            acquisition_id=fields.get("acquisition_id"),
            scope_id=fields.get("scope_id"),
            query_id=fields.get("query_id"),
            query_identity_sha256=fields.get("query_identity_sha256"),
            kind=fields.get("kind"),
            resource_type=fields.get("resource_type"),
            reference_type=fields.get("reference_type"),
            reference_id=fields.get("reference_id"),
            closure_scope=fields.get("closure_scope"),
            attempt=fields.get("attempt_count"),
            lease_token=fields.get("lease_token"),
        )
    except (TypeError, ValueError) as error:
        raise ProviderDirectoryRootedGraphStoreError("state") from error


def _claim_sql(*, census_only: bool) -> str:
    kind_predicate = (
        "work.kind = 'full_insurance_plan_census'"
        if census_only
        else "work.kind <> 'full_insurance_plan_census'"
    )
    return f"""
        WITH candidate AS (
            SELECT work.acquisition_id, work.query_id
              FROM {table_ref(WORK_TABLE)} AS work
              JOIN {table_ref(ACQUISITION_TABLE)} AS acquisition
                ON acquisition.acquisition_id = work.acquisition_id
             WHERE work.acquisition_id = :acquisition_id
               AND acquisition.status = 'building'
               AND {kind_predicate}
               AND (CAST(:query_id AS text) IS NULL OR work.query_id = :query_id)
               AND (work.status = 'pending' OR (
                   work.status = 'leased'
                   AND work.lease_expires_at <= clock_timestamp()
               ))
             ORDER BY work.query_id FOR UPDATE OF work SKIP LOCKED LIMIT 1
        )
        UPDATE {table_ref(WORK_TABLE)} AS work
           SET status = 'leased', attempt_count = work.attempt_count + 1,
               lease_token = :lease_token,
               lease_expires_at = transaction_timestamp()
                   + make_interval(secs => :lease_seconds),
               lease_heartbeat_at = transaction_timestamp(),
               updated_at = transaction_timestamp()
          FROM candidate
         WHERE work.acquisition_id = candidate.acquisition_id
           AND work.query_id = candidate.query_id
        RETURNING work.*;
    """


async def _claim_work_row(
    database: Any,
    acquisition_id: str,
    *,
    query_id: str | None,
    lease_token: str,
    lease_seconds: int,
    census_only: bool,
) -> Any:
    return await database.first(
        _claim_sql(census_only=census_only),
        acquisition_id=acquisition_id,
        query_id=query_id,
        lease_token=lease_token,
        lease_seconds=lease_seconds,
    )


async def _root_closure_fields(
    database: Any,
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
) -> dict[str, Any]:
    assert_identity_row(
        identity,
        await database.first(
            f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} "
            "WHERE acquisition_id = :acquisition_id FOR SHARE;",
            acquisition_id=identity.acquisition_id,
        ),
    )
    await database.status(
        f"LOCK TABLE {table_ref(WORK_TABLE)}, {table_ref(RESOURCE_TABLE)}, "
        f"{table_ref(EDGE_TABLE)} IN SHARE MODE;"
    )
    closure_fields = row_fields(
        await database.first(
            root_closure_sql(),
            acquisition_id=identity.acquisition_id,
        )
    )
    if (
        type(closure_fields.get("canonical_api_base")) is not str
        or closure_fields.get("root_closure_complete") is not True
        or closure_fields.get("census_count") not in {0, 1}
    ):
        raise ProviderDirectoryRootedGraphStoreError("state")
    return closure_fields


async def _admit_and_claim_census(
    database: Any,
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    lease_token: str,
    lease_seconds: int,
) -> tuple[Any, tuple[str, ...]]:
    closure_fields = await _root_closure_fields(database, identity)
    root_network_references = tuple(closure_fields.get("root_network_references") or ())
    census_spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_insurance_plan_census_query(closure_fields["canonical_api_base"]),
        closure_scope="census",
    )
    if closure_fields["census_count"] == 0:
        await set_store_action(database, "census", identity.acquisition_id)
        await insert_work_spec(database, identity.acquisition_id, census_spec)
    await set_store_action(
        database,
        "claim_census",
        identity.acquisition_id,
        lease_token,
    )
    claimed_row = await _claim_work_row(
        database,
        identity.acquisition_id,
        query_id=census_spec.query_id,
        lease_token=lease_token,
        lease_seconds=lease_seconds,
        census_only=True,
    )
    return claimed_row, root_network_references


async def claim_provider_directory_rooted_graph_census(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    lease_seconds: int = 300,
    database: Any = db,
) -> ProviderDirectoryRootedGraphCensusClaim | None:
    """Admit and claim the plan census only after locked root fixed-point proof."""

    if type(identity) is not ProviderDirectoryRootedGraphAcquisitionIdentity:
        raise ValueError("provider_directory_rooted_graph_identity_invalid")
    if type(lease_seconds) is not int or not 30 <= lease_seconds <= 3600:
        raise ValueError("provider_directory_rooted_graph_lease_invalid")
    lease_token = secrets.token_hex(32)
    async with database.transaction():
        await database.scalar(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended(:identity, 0));",
            identity=identity.acquisition_id,
        )
        claimed_row, root_network_references = await _admit_and_claim_census(
            database,
            identity,
            lease_token,
            lease_seconds,
        )
    work_claim = _claim_from_row(claimed_row)
    if work_claim is None:
        return None
    try:
        return ProviderDirectoryRootedGraphCensusClaim(
            work_claim=work_claim,
            root_network_references=root_network_references,
        )
    except ValueError as error:
        raise ProviderDirectoryRootedGraphStoreError("state") from error


async def claim_provider_directory_rooted_graph_work(
    acquisition_id: str,
    *,
    query_id: str | None = None,
    lease_seconds: int = 300,
    database: Any = db,
) -> ProviderDirectoryRootedGraphWorkClaim | None:
    """Claim non-census work with attempt and opaque-token fencing."""

    if (
        type(acquisition_id) is not str
        or ACQUISITION_PATTERN.fullmatch(acquisition_id) is None
    ):
        raise ValueError("provider_directory_rooted_graph_acquisition_id_invalid")
    if query_id is not None and (
        type(query_id) is not str
        or ROOTED_GRAPH_QUERY_PATTERN.fullmatch(query_id) is None
    ):
        raise ValueError("provider_directory_rooted_graph_query_id_invalid")
    if type(lease_seconds) is not int or not 30 <= lease_seconds <= 3600:
        raise ValueError("provider_directory_rooted_graph_lease_invalid")
    lease_token = secrets.token_hex(32)
    async with database.transaction():
        await set_store_action(database, "claim", acquisition_id, lease_token)
        claimed_row = await _claim_work_row(
            database,
            acquisition_id,
            query_id=query_id,
            lease_token=lease_token,
            lease_seconds=lease_seconds,
            census_only=False,
        )
    return _claim_from_row(claimed_row)


async def heartbeat_provider_directory_rooted_graph_work(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    *,
    lease_seconds: int = 300,
    database: Any = db,
) -> None:
    """Extend only the exact active query lease generation."""

    if type(claim) is not ProviderDirectoryRootedGraphWorkClaim:
        raise ValueError("provider_directory_rooted_graph_claim_invalid")
    if type(lease_seconds) is not int or not 30 <= lease_seconds <= 3600:
        raise ValueError("provider_directory_rooted_graph_lease_invalid")
    async with database.transaction():
        await set_store_action(
            database, "heartbeat", claim.acquisition_id, claim.lease_token
        )
        count = await database.status(
            f"""
            UPDATE {table_ref(WORK_TABLE)}
               SET lease_expires_at = transaction_timestamp()
                       + make_interval(secs => :lease_seconds),
                   lease_heartbeat_at = transaction_timestamp(),
                   updated_at = transaction_timestamp()
             WHERE acquisition_id = :acquisition_id AND query_id = :query_id
               AND status = 'leased' AND attempt_count = :attempt
               AND lease_token = :lease_token
               AND lease_expires_at > clock_timestamp();
            """,
            acquisition_id=claim.acquisition_id,
            query_id=claim.query_id,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
            lease_seconds=lease_seconds,
        )
    if count != 1:
        raise ProviderDirectoryRootedGraphStoreError("lease_lost")


async def release_provider_directory_rooted_graph_work(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    *,
    database: Any = db,
) -> None:
    """Release only an unmaterialized live lease for immediate resumption."""

    if type(claim) is not ProviderDirectoryRootedGraphWorkClaim:
        raise ValueError("provider_directory_rooted_graph_claim_invalid")
    async with database.transaction():
        await set_store_action(
            database, "release", claim.acquisition_id, claim.lease_token
        )
        count = await database.status(
            f"""
            UPDATE {table_ref(WORK_TABLE)}
               SET status = 'pending', lease_token = NULL,
                   lease_expires_at = NULL, lease_heartbeat_at = NULL,
                   updated_at = transaction_timestamp()
             WHERE acquisition_id = :acquisition_id AND query_id = :query_id
               AND status = 'leased' AND attempt_count = :attempt
               AND lease_token = :lease_token
               AND lease_expires_at > clock_timestamp();
            """,
            acquisition_id=claim.acquisition_id,
            query_id=claim.query_id,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
        )
    if count != 1:
        raise ProviderDirectoryRootedGraphStoreError("lease_lost")


__all__ = (
    "build_provider_directory_rooted_graph_acquisition_identity",
    "build_provider_directory_rooted_graph_work_spec",
    "claim_provider_directory_rooted_graph_census",
    "claim_provider_directory_rooted_graph_work",
    "complete_provider_directory_rooted_graph_error",
    "complete_provider_directory_rooted_graph_missing",
    "complete_provider_directory_rooted_graph_result",
    "heartbeat_provider_directory_rooted_graph_work",
    "initialize_provider_directory_rooted_graph_acquisition",
    "release_provider_directory_rooted_graph_work",
    "seal_provider_directory_rooted_graph_acquisition",
    "ProviderDirectoryRootedGraphAcquisitionIdentity",
    "ProviderDirectoryRootedGraphAcquisitionSummary",
    "ProviderDirectoryRootedGraphCensusClaim",
    "ProviderDirectoryRootedGraphQueryResult",
    "ProviderDirectoryRootedGraphStoreError",
    "ProviderDirectoryRootedGraphWorkClaim",
    "ProviderDirectoryRootedGraphWorkSpec",
)
