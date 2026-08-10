# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Terminal result, seal, and bounded-manifest operations for Flex."""

from __future__ import annotations

import json
from typing import Any

from db.connection import db
from process.uhc_flex_practitioner_query import UHCFlexPractitionerQueryResult
from process.uhc_flex_practitioner_store_contract import (
    ACQUISITION_PATTERN,
    ERROR_PATTERN,
    canonical_resource_fields_list,
    strict_identifier,
    terminal_record_sha256,
    UHCFlexPractitionerAcquisitionIdentity,
    UHCFlexPractitionerAcquisitionSummary,
    UHCFlexPractitionerResourceRow,
    UHCFlexPractitionerStoreError,
    UHCFlexPractitionerWorkClaim,
)
from process.uhc_flex_practitioner_store_support import (
    ACQUISITION_TABLE,
    assert_identity_row,
    function_ref,
    RESOURCE_TABLE,
    row_fields,
    set_store_action,
    table_ref,
    TERMINAL_SET_FUNCTION,
    WORK_TABLE,
)


async def _insert_resource_manifest(
    database: Any,
    claim: UHCFlexPractitionerWorkClaim,
    resource_fields_list: list[dict[str, object]],
) -> None:
    if not resource_fields_list:
        return
    await set_store_action(
        database,
        "resource",
        claim.acquisition_id,
        claim.lease_token,
    )
    inserted_count = await database.status(
        f"""
        INSERT INTO {table_ref(RESOURCE_TABLE)} (
            acquisition_id, cohort_id, npi, attempt, resource_id,
            payload_sha256, payload_json_text
        )
        SELECT :acquisition_id, :cohort_id, :npi, :attempt,
               resource.resource_id, resource.payload_sha256,
               resource.payload_json_text
          FROM pg_catalog.jsonb_to_recordset(CAST(:resources_json AS jsonb))
               AS resource(resource_id varchar(64),
                           payload_sha256 varchar(64), payload_json_text text);
        """,
        acquisition_id=claim.acquisition_id,
        cohort_id=claim.cohort_id,
        npi=claim.requested_npi,
        attempt=claim.attempt,
        resources_json=json.dumps(
            resource_fields_list,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    )
    if inserted_count != len(resource_fields_list):
        raise UHCFlexPractitionerStoreError("state")


async def _terminalize_query_result(
    database: Any,
    claim: UHCFlexPractitionerWorkClaim,
    query_result: UHCFlexPractitionerQueryResult,
    terminal_hash: str,
) -> None:
    await set_store_action(
        database,
        "terminal",
        claim.acquisition_id,
        claim.lease_token,
    )
    updated_count = await database.status(
        f"""
        UPDATE {table_ref(WORK_TABLE)}
           SET status = :status, lease_token = NULL,
               lease_expires_at = NULL, lease_heartbeat_at = NULL,
               result_sha256 = :result_sha256,
               resource_count = :resource_count, error_code = NULL,
               terminal_record_sha256 = :terminal_record_sha256,
               terminal_at = transaction_timestamp(),
               updated_at = transaction_timestamp()
         WHERE acquisition_id = :acquisition_id AND npi = :npi
           AND status = 'leased' AND attempt_count = :attempt
           AND lease_token = :lease_token
           AND lease_expires_at > clock_timestamp();
        """,
        status=query_result.outcome,
        result_sha256=query_result.result_sha256,
        resource_count=query_result.resource_count,
        terminal_record_sha256=terminal_hash,
        acquisition_id=claim.acquisition_id,
        npi=claim.requested_npi,
        attempt=claim.attempt,
        lease_token=claim.lease_token,
    )
    if updated_count != 1:
        raise UHCFlexPractitionerStoreError("lease_lost")


async def complete_uhc_flex_practitioner_result(
    claim: UHCFlexPractitionerWorkClaim,
    query_result: UHCFlexPractitionerQueryResult,
    *,
    database: Any = db,
) -> None:
    """Atomically retain a bounded exact response and terminalize its lease."""

    if type(claim) is not UHCFlexPractitionerWorkClaim:
        raise ValueError("Flex Practitioner work claim is invalid")
    if (
        type(query_result) is not UHCFlexPractitionerQueryResult
        or query_result.requested_npi != claim.requested_npi
    ):
        raise ValueError("Flex Practitioner query result does not match claim")
    resource_fields_list = canonical_resource_fields_list(query_result)
    terminal_hash = terminal_record_sha256(
        claim,
        status=query_result.outcome,
        result_sha256=query_result.result_sha256,
        resource_count=query_result.resource_count,
        error_code=None,
    )
    async with database.transaction():
        await _insert_resource_manifest(database, claim, resource_fields_list)
        await _terminalize_query_result(database, claim, query_result, terminal_hash)


async def complete_uhc_flex_practitioner_error(
    claim: UHCFlexPractitionerWorkClaim,
    *,
    error_code: str,
    database: Any = db,
) -> None:
    """Record one stable terminal error without retaining response content."""

    if type(claim) is not UHCFlexPractitionerWorkClaim:
        raise ValueError("Flex Practitioner work claim is invalid")
    if type(error_code) is not str or ERROR_PATTERN.fullmatch(error_code) is None:
        raise ValueError("Flex Practitioner terminal error code is invalid")
    terminal_hash = terminal_record_sha256(
        claim,
        status="error",
        result_sha256=None,
        resource_count=0,
        error_code=error_code,
    )
    async with database.transaction():
        await set_store_action(
            database,
            "terminal",
            claim.acquisition_id,
            claim.lease_token,
        )
        updated_count = await database.status(
            f"""
            UPDATE {table_ref(WORK_TABLE)}
               SET status = 'error', lease_token = NULL,
                   lease_expires_at = NULL, lease_heartbeat_at = NULL,
                   result_sha256 = NULL, resource_count = 0,
                   error_code = :error_code,
                   terminal_record_sha256 = :terminal_record_sha256,
                   terminal_at = transaction_timestamp(),
                   updated_at = transaction_timestamp()
             WHERE acquisition_id = :acquisition_id AND npi = :npi
               AND status = 'leased' AND attempt_count = :attempt
               AND lease_token = :lease_token
               AND lease_expires_at > clock_timestamp();
            """,
            error_code=error_code,
            terminal_record_sha256=terminal_hash,
            acquisition_id=claim.acquisition_id,
            npi=claim.requested_npi,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
        )
        if updated_count != 1:
            raise UHCFlexPractitionerStoreError("lease_lost")


def _summary_from_row(database_row: Any) -> UHCFlexPractitionerAcquisitionSummary:
    fields = row_fields(database_row)
    try:
        return UHCFlexPractitionerAcquisitionSummary(
            acquisition_id=fields.get("acquisition_id"),
            expected_npi_count=fields.get("expected_npi_count"),
            matched_count=fields.get("matched_count"),
            unmatched_count=fields.get("unmatched_count"),
            error_count=fields.get("error_count"),
            resource_count=fields.get("resource_count"),
            terminal_set_sha256=fields.get("terminal_set_sha256"),
            cohort_complete=fields.get("cohort_complete"),
            endpoint_collection_complete=fields.get("endpoint_collection_complete"),
            endpoint_complete=fields.get("endpoint_complete"),
        )
    except (TypeError, ValueError) as error:
        raise UHCFlexPractitionerStoreError("state") from error


async def _locked_building_header(
    database: Any,
    identity: UHCFlexPractitionerAcquisitionIdentity,
) -> dict[str, Any]:
    await database.status(
        f"LOCK TABLE {table_ref(WORK_TABLE)}, {table_ref(RESOURCE_TABLE)} "
        "IN SHARE MODE;"
    )
    header = await database.first(
        f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} "
        "WHERE acquisition_id = :acquisition_id FOR UPDATE;",
        acquisition_id=identity.acquisition_id,
    )
    return assert_identity_row(identity, header)


async def _seal_building_header(
    database: Any,
    acquisition_id: str,
) -> Any:
    return await database.first(
        f"""
        WITH census AS (
            SELECT count(*) FILTER (WHERE status = 'pending')::bigint
                       AS pending_count,
                   count(*) FILTER (WHERE status = 'leased')::bigint
                       AS leased_count,
                   count(*) FILTER (WHERE status = 'matched')::bigint
                       AS matched_count,
                   count(*) FILTER (WHERE status = 'unmatched')::bigint
                       AS unmatched_count,
                   count(*) FILTER (WHERE status = 'error')::bigint AS error_count
              FROM {table_ref(WORK_TABLE)} WHERE acquisition_id = :acquisition_id
        ), resource_census AS (
            SELECT count(*)::bigint AS resource_count
              FROM {table_ref(RESOURCE_TABLE)} AS resource
              JOIN {table_ref(WORK_TABLE)} AS work
                ON work.acquisition_id = resource.acquisition_id
               AND work.cohort_id = resource.cohort_id
               AND work.npi = resource.npi AND work.attempt_count = resource.attempt
             WHERE resource.acquisition_id = :acquisition_id
               AND work.status = 'matched'
        )
        UPDATE {table_ref(ACQUISITION_TABLE)} AS acquisition
           SET status = 'sealed', cohort_complete = true,
               pending_count = census.pending_count,
               leased_count = census.leased_count,
               matched_count = census.matched_count,
               unmatched_count = census.unmatched_count,
               error_count = census.error_count,
               resource_count = resource_census.resource_count,
               terminal_set_sha256 = {function_ref(TERMINAL_SET_FUNCTION)}(
                   acquisition.acquisition_id
               ), sealed_at = transaction_timestamp(),
               updated_at = transaction_timestamp()
          FROM census, resource_census
         WHERE acquisition.acquisition_id = :acquisition_id
           AND acquisition.status = 'building'
        RETURNING acquisition.*;
        """,
        acquisition_id=acquisition_id,
    )


async def seal_uhc_flex_practitioner_acquisition(
    identity: UHCFlexPractitionerAcquisitionIdentity,
    *,
    database: Any = db,
) -> UHCFlexPractitionerAcquisitionSummary:
    """Seal only matched/unmatched exact coverage; reject every error."""

    if type(identity) is not UHCFlexPractitionerAcquisitionIdentity:
        raise ValueError("Flex Practitioner acquisition identity is invalid")
    async with database.transaction():
        await database.scalar(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended(:identity, 0));",
            identity=identity.acquisition_id,
        )
        header = await database.first(
            f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} "
            "WHERE acquisition_id = :acquisition_id;",
            acquisition_id=identity.acquisition_id,
        )
        header_fields = assert_identity_row(identity, header)
        if header_fields.get("status") == "sealed":
            return _summary_from_row(header_fields)
        header_fields = await _locked_building_header(database, identity)
        if header_fields.get("status") == "sealed":
            return _summary_from_row(header_fields)
        sealed_header = await _seal_building_header(database, identity.acquisition_id)
        if sealed_header is None:
            raise UHCFlexPractitionerStoreError("state")
        return _summary_from_row(sealed_header)


async def _resource_page_records(
    database: Any,
    acquisition_id: str,
    after_npi: int,
    after_resource_id: str,
    limit: int,
) -> list[Any]:
    return await database.all(
        f"""
        SELECT resource.npi, resource.resource_id, resource.payload_sha256,
               resource.payload_json_text
          FROM {table_ref(RESOURCE_TABLE)} AS resource
          JOIN {table_ref(WORK_TABLE)} AS work
            ON work.acquisition_id = resource.acquisition_id
           AND work.cohort_id = resource.cohort_id AND work.npi = resource.npi
           AND work.attempt_count = resource.attempt
          JOIN {table_ref(ACQUISITION_TABLE)} AS acquisition
            ON acquisition.acquisition_id = resource.acquisition_id
         WHERE resource.acquisition_id = :acquisition_id
           AND acquisition.status = 'sealed' AND work.status = 'matched'
           AND (resource.npi, resource.resource_id)
               > (:after_npi, :after_resource_id)
         ORDER BY resource.npi, resource.resource_id LIMIT :limit;
        """,
        acquisition_id=acquisition_id,
        after_npi=after_npi,
        after_resource_id=after_resource_id,
        limit=limit,
    )


async def read_uhc_flex_practitioner_resource_page(
    acquisition_id: str,
    *,
    after_npi: int = 0,
    after_resource_id: str = "",
    limit: int = 500,
    database: Any = db,
) -> tuple[UHCFlexPractitionerResourceRow, ...]:
    """Read a bounded keyset page from the sealed terminal-attempt manifest."""

    strict_identifier(acquisition_id, ACQUISITION_PATTERN, "acquisition ID")
    if type(after_npi) is not int or not 0 <= after_npi <= 2999999999:
        raise ValueError("Flex Practitioner manifest cursor is invalid")
    if type(after_resource_id) is not str or len(after_resource_id) > 64:
        raise ValueError("Flex Practitioner manifest cursor is invalid")
    if after_npi == 0 and after_resource_id:
        raise ValueError("Flex Practitioner manifest cursor is invalid")
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Flex Practitioner manifest page limit is invalid")
    database_rows = await _resource_page_records(
        database,
        acquisition_id,
        after_npi,
        after_resource_id,
        limit,
    )
    return tuple(
        UHCFlexPractitionerResourceRow(
            requested_npi=row_fields(database_row).get("npi"),
            resource_id=row_fields(database_row).get("resource_id"),
            payload_sha256=row_fields(database_row).get("payload_sha256"),
            payload_json_text=row_fields(database_row).get("payload_json_text"),
        )
        for database_row in database_rows
    )


__all__ = (
    "complete_uhc_flex_practitioner_error",
    "complete_uhc_flex_practitioner_result",
    "read_uhc_flex_practitioner_resource_page",
    "seal_uhc_flex_practitioner_acquisition",
)
