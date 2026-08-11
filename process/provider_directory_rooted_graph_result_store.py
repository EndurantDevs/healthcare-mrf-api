# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable result witnesses and fixed-point sealing for rooted graphs."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.provider_directory_rooted_graph_frontier import (
    register_rooted_graph_frontier,
)
from process.provider_directory_rooted_graph_result_contract import (
    ERROR_PATTERN,
    ProviderDirectoryRootedGraphAcquisitionSummary,
    ProviderDirectoryRootedGraphQueryResult,
    build_provider_directory_rooted_graph_missing_witness,
    provider_directory_rooted_graph_error_terminal_sha256,
    validate_provider_directory_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphAcquisitionIdentity,
    ProviderDirectoryRootedGraphStoreError,
    ProviderDirectoryRootedGraphWorkClaim,
)
from process.provider_directory_rooted_graph_store_support import (
    ACQUISITION_TABLE,
    EDGE_SET_FUNCTION,
    EDGE_TABLE,
    RESOURCE_SET_FUNCTION,
    RESOURCE_TABLE,
    ROOT_FUNCTION,
    TERMINAL_SET_FUNCTION,
    WORK_TABLE,
    assert_identity_row,
    function_ref,
    row_fields,
    set_store_action,
    table_ref,
)


def _complete_missing_sql() -> str:
    """Resolve the runtime schema when the missing witness is persisted."""

    return f"""
        UPDATE {table_ref(WORK_TABLE)}
           SET status = 'completed', lease_token = NULL,
               lease_expires_at = NULL, lease_heartbeat_at = NULL,
               result_sha256 = :result_sha256,
               resource_count = 0, edge_count = 0,
               resource_set_sha256 = :resource_set_sha256,
               edge_set_sha256 = :edge_set_sha256,
               advertised_total = NULL, terminal_page_count = 1,
               pagination_terminal = true, error_code = NULL,
               missing_http_status = :missing_http_status,
               missing_response_sha256 = :missing_response_sha256,
               missing_response_bytes = :missing_response_bytes,
               missing_response_json_text = :missing_response_json_text,
               terminal_record_sha256 = :terminal_record_sha256,
               terminal_at = transaction_timestamp(),
               updated_at = transaction_timestamp()
         WHERE acquisition_id = :acquisition_id AND query_id = :query_id
           AND status = 'leased' AND attempt_count = :attempt
           AND lease_token = :lease_token
           AND lease_expires_at > clock_timestamp();
    """


async def _insert_resources(
    database: Any,
    claim: ProviderDirectoryRootedGraphWorkClaim,
    query_result: ProviderDirectoryRootedGraphQueryResult,
) -> None:
    for resource in query_result.resources:
        count = await database.status(
            f"""
            INSERT INTO {table_ref(RESOURCE_TABLE)} (
                acquisition_id, scope_id, query_id, attempt,
                resource_type, resource_id, payload_sha256,
                payload_json_text, closure_scope
            ) VALUES (
                :acquisition_id, :scope_id, :query_id, :attempt,
                :resource_type, :resource_id, :payload_sha256,
                :payload_json_text, :closure_scope
            );
            """,
            acquisition_id=claim.acquisition_id,
            scope_id=claim.scope_id,
            query_id=claim.query_id,
            attempt=claim.attempt,
            resource_type=resource.resource_type,
            resource_id=resource.resource_id,
            payload_sha256=resource.payload_sha256,
            payload_json_text=resource.payload_json_text,
            closure_scope=resource.closure_scope,
        )
        if count != 1:
            raise ProviderDirectoryRootedGraphStoreError("state")


async def _insert_edges(
    database: Any,
    claim: ProviderDirectoryRootedGraphWorkClaim,
    query_result: ProviderDirectoryRootedGraphQueryResult,
) -> None:
    for edge in query_result.edges:
        count = await database.status(
            f"""
            INSERT INTO {table_ref(EDGE_TABLE)} (
                acquisition_id, scope_id, query_id, attempt,
                source_resource_type, source_resource_id, field_path,
                target_resource_type, target_resource_id, edge_sha256,
                closure_scope
            ) VALUES (
                :acquisition_id, :scope_id, :query_id, :attempt,
                :source_resource_type, :source_resource_id, :field_path,
                :target_resource_type, :target_resource_id, :edge_sha256,
                :closure_scope
            );
            """,
            acquisition_id=claim.acquisition_id,
            scope_id=claim.scope_id,
            query_id=claim.query_id,
            attempt=claim.attempt,
            source_resource_type=edge.source_resource_type,
            source_resource_id=edge.source_resource_id,
            field_path=edge.field_path,
            target_resource_type=edge.target_resource_type,
            target_resource_id=edge.target_resource_id,
            edge_sha256=edge.edge_sha256,
            closure_scope=edge.closure_scope,
        )
        if count != 1:
            raise ProviderDirectoryRootedGraphStoreError("state")


async def complete_provider_directory_rooted_graph_result(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    query_result: ProviderDirectoryRootedGraphQueryResult,
    *,
    database: Any = db,
) -> None:
    """Atomically retain witnesses and terminalize one live query lease."""

    validate_provider_directory_rooted_graph_query_result(claim, query_result)
    async with database.transaction():
        await set_store_action(
            database, "witness", claim.acquisition_id, claim.lease_token
        )
        await _insert_resources(database, claim, query_result)
        await _insert_edges(database, claim, query_result)
        await set_store_action(
            database, "terminal", claim.acquisition_id, claim.lease_token
        )
        count = await database.status(
            f"""
            UPDATE {table_ref(WORK_TABLE)}
               SET status = 'completed', lease_token = NULL,
                   lease_expires_at = NULL, lease_heartbeat_at = NULL,
                   result_sha256 = :result_sha256,
                   resource_count = :resource_count, edge_count = :edge_count,
                   resource_set_sha256 = :resource_set_sha256,
                   edge_set_sha256 = :edge_set_sha256,
                   advertised_total = :advertised_total,
                   terminal_page_count = :terminal_page_count,
                   pagination_terminal = true, error_code = NULL,
                   terminal_record_sha256 = :terminal_record_sha256,
                   terminal_at = transaction_timestamp(),
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
            result_sha256=query_result.result_sha256,
            resource_count=len(query_result.resources),
            edge_count=len(query_result.edges),
            resource_set_sha256=query_result.resource_set_sha256,
            edge_set_sha256=query_result.edge_set_sha256,
            advertised_total=query_result.advertised_total,
            terminal_page_count=query_result.terminal_page_count,
            terminal_record_sha256=query_result.terminal_record_sha256,
        )
        if count != 1:
            raise ProviderDirectoryRootedGraphStoreError("lease_lost")
        await register_rooted_graph_frontier(
            database,
            claim,
            query_result,
        )
        return None


async def complete_provider_directory_rooted_graph_missing(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    *,
    missing_http_status: int,
    missing_response_sha256: str,
    missing_response_bytes: int,
    missing_response_json_text: str,
    database: Any = db,
) -> None:
    """Terminalize an exact direct-read 404/410 as successful graph closure."""

    missing = build_provider_directory_rooted_graph_missing_witness(
        claim,
        missing_http_status,
        missing_response_sha256,
        missing_response_bytes,
        missing_response_json_text,
    )
    async with database.transaction():
        await set_store_action(
            database,
            "terminal",
            claim.acquisition_id,
            claim.lease_token,
        )
        count = await database.status(
            _complete_missing_sql(),
            acquisition_id=claim.acquisition_id,
            query_id=claim.query_id,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
            result_sha256=missing.result_sha256,
            resource_set_sha256=missing.resource_set_sha256,
            edge_set_sha256=missing.edge_set_sha256,
            missing_http_status=missing.missing_http_status,
            missing_response_sha256=missing.missing_response_sha256,
            missing_response_bytes=missing.missing_response_bytes,
            missing_response_json_text=missing.missing_response_json_text,
            terminal_record_sha256=missing.terminal_record_sha256,
        )
        if count != 1:
            raise ProviderDirectoryRootedGraphStoreError("lease_lost")


async def complete_provider_directory_rooted_graph_error(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    *,
    error_code: str,
    database: Any = db,
) -> None:
    """Record a stable terminal error; any such row prevents sealing."""

    if type(claim) is not ProviderDirectoryRootedGraphWorkClaim:
        raise ValueError("provider_directory_rooted_graph_claim_invalid")
    if type(error_code) is not str or ERROR_PATTERN.fullmatch(error_code) is None:
        raise ValueError("provider_directory_rooted_graph_error_invalid")
    terminal_hash = provider_directory_rooted_graph_error_terminal_sha256(
        claim,
        error_code,
    )
    async with database.transaction():
        await set_store_action(
            database, "terminal", claim.acquisition_id, claim.lease_token
        )
        count = await database.status(
            f"""
            UPDATE {table_ref(WORK_TABLE)}
               SET status = 'error', lease_token = NULL,
                   lease_expires_at = NULL, lease_heartbeat_at = NULL,
                   result_sha256 = NULL, resource_count = 0, edge_count = 0,
                   resource_set_sha256 = NULL, edge_set_sha256 = NULL,
                   advertised_total = NULL, terminal_page_count = 0,
                   pagination_terminal = false, error_code = :error_code,
                   terminal_record_sha256 = :terminal_record_sha256,
                   terminal_at = transaction_timestamp(),
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
            error_code=error_code,
            terminal_record_sha256=terminal_hash,
        )
        if count != 1:
            raise ProviderDirectoryRootedGraphStoreError("lease_lost")
        return None


def _summary_from_row(
    database_row: Any,
) -> ProviderDirectoryRootedGraphAcquisitionSummary:
    fields = row_fields(database_row)
    try:
        return ProviderDirectoryRootedGraphAcquisitionSummary(
            acquisition_id=fields.get("acquisition_id"),
            scope_id=fields.get("scope_id"),
            completed_count=fields.get("completed_count"),
            error_count=fields.get("error_count"),
            resource_count=fields.get("resource_count"),
            edge_count=fields.get("edge_count"),
            terminal_set_sha256=fields.get("terminal_set_sha256"),
            resource_set_sha256=fields.get("resource_set_sha256"),
            edge_set_sha256=fields.get("edge_set_sha256"),
            rooted_graph_sha256=fields.get("rooted_graph_sha256"),
            rooted_graph_complete=fields.get("rooted_graph_complete"),
            endpoint_collection_complete=fields.get("endpoint_collection_complete"),
            endpoint_complete=fields.get("endpoint_complete"),
        )
    except (TypeError, ValueError) as error:
        raise ProviderDirectoryRootedGraphStoreError("state") from error


def _seal_census_sql() -> str:
    return f"""
        WITH work_census AS (
            SELECT count(*) FILTER (WHERE status = 'pending')::bigint AS pending_count,
                   count(*) FILTER (WHERE status = 'leased')::bigint AS leased_count,
                   count(*) FILTER (WHERE status = 'completed')::bigint AS completed_count,
                   count(*) FILTER (WHERE status = 'error')::bigint AS error_count
              FROM {table_ref(WORK_TABLE)} WHERE acquisition_id = :acquisition_id
        ), witness_census AS (
            SELECT count(*)::bigint AS resource_count
              FROM {table_ref(RESOURCE_TABLE)} AS resource
              JOIN {table_ref(WORK_TABLE)} AS work
                ON work.acquisition_id = resource.acquisition_id
               AND work.query_id = resource.query_id
               AND work.attempt_count = resource.attempt
             WHERE resource.acquisition_id = :acquisition_id
               AND work.status = 'completed'
        ), edge_census AS (
            SELECT count(*)::bigint AS edge_count
              FROM {table_ref(EDGE_TABLE)} AS edge
              JOIN {table_ref(WORK_TABLE)} AS work
                ON work.acquisition_id = edge.acquisition_id
               AND work.query_id = edge.query_id
               AND work.attempt_count = edge.attempt
             WHERE edge.acquisition_id = :acquisition_id
               AND work.status = 'completed'
        ), plan_census AS (
            SELECT advertised_total::bigint AS insurance_plan_count,
                   terminal_page_count AS insurance_plan_page_count
              FROM {table_ref(WORK_TABLE)}
             WHERE acquisition_id = :acquisition_id
               AND kind = 'full_insurance_plan_census'
               AND status = 'completed'
        )
    """


def _seal_update_sql() -> str:
    return f"""
        UPDATE {table_ref(ACQUISITION_TABLE)} AS acquisition
           SET status = 'sealed', rooted_graph_complete = true,
               pending_count = work_census.pending_count,
               leased_count = work_census.leased_count,
               completed_count = work_census.completed_count,
               error_count = work_census.error_count,
               resource_count = witness_census.resource_count,
               edge_count = edge_census.edge_count,
               insurance_plan_count = plan_census.insurance_plan_count,
               insurance_plan_page_count = plan_census.insurance_plan_page_count,
               terminal_set_sha256 = {function_ref(TERMINAL_SET_FUNCTION)}(
                   acquisition.acquisition_id
               ),
               resource_set_sha256 = {function_ref(RESOURCE_SET_FUNCTION)}(
                   acquisition.acquisition_id
               ),
               edge_set_sha256 = {function_ref(EDGE_SET_FUNCTION)}(
                   acquisition.acquisition_id
               ),
               rooted_graph_sha256 = {function_ref(ROOT_FUNCTION)}(
                   acquisition.acquisition_id
               ),
               sealed_at = transaction_timestamp(),
               updated_at = transaction_timestamp()
          FROM work_census, witness_census, edge_census, plan_census
         WHERE acquisition.acquisition_id = :acquisition_id
           AND acquisition.status = 'building'
        RETURNING acquisition.*;
    """


async def _seal_header(database: Any, acquisition_id: str) -> Any:
    """Apply counts and comparison roots under the caller's table locks."""

    return await database.first(
        _seal_census_sql() + _seal_update_sql(),
        acquisition_id=acquisition_id,
    )


async def seal_provider_directory_rooted_graph_acquisition(
    identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    *,
    database: Any = db,
) -> ProviderDirectoryRootedGraphAcquisitionSummary:
    """Seal only a terminal error-free fixed point with a finite plan census."""

    if type(identity) is not ProviderDirectoryRootedGraphAcquisitionIdentity:
        raise ValueError("provider_directory_rooted_graph_identity_invalid")
    async with database.transaction():
        await database.scalar(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended(:identity, 0));",
            identity=identity.acquisition_id,
        )
        header = assert_identity_row(
            identity,
            await database.first(
                f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} "
                "WHERE acquisition_id = :acquisition_id;",
                acquisition_id=identity.acquisition_id,
            ),
        )
        if header["status"] == "sealed":
            return _summary_from_row(header)
        await database.status(
            f"LOCK TABLE {table_ref(WORK_TABLE)}, {table_ref(RESOURCE_TABLE)}, "
            f"{table_ref(EDGE_TABLE)} IN SHARE MODE;"
        )
        sealed = await _seal_header(database, identity.acquisition_id)
        if sealed is None:
            raise ProviderDirectoryRootedGraphStoreError("state")
        return _summary_from_row(sealed)


__all__ = (
    "complete_provider_directory_rooted_graph_error",
    "complete_provider_directory_rooted_graph_missing",
    "complete_provider_directory_rooted_graph_result",
    "seal_provider_directory_rooted_graph_acquisition",
)
