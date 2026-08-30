# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant, fenced persistence for exact-cohort Flex Practitioner reads."""

from __future__ import annotations

import secrets
from typing import Any

from db.connection import db
from process.uhc_flex_practitioner_acquisition_contract import (
    UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY,
)
from process.uhc_flex_official_cohort_contract import canonical_uhc_flex_npi
from process.uhc_flex_practitioner_result_store import (
    complete_uhc_flex_practitioner_error,
    complete_uhc_flex_practitioner_result,
    read_uhc_flex_practitioner_resource_page,
    seal_uhc_flex_practitioner_acquisition,
)
from process.uhc_flex_practitioner_store_contract import (
    ACQUISITION_PATTERN,
    build_uhc_flex_practitioner_acquisition_identity,
    canonical_resource_fields_list,
    strict_identifier,
    terminal_record_sha256,
    UHCFlexPractitionerAcquisitionIdentity,
    UHCFlexPractitionerAcquisitionSummary,
    UHCFlexPractitionerResourceRow,
    UHCFlexPractitionerStoreError,
    UHCFlexPractitionerWorkClaim,
    UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_ACQUISITION_ROLES,
    UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID,
)
from process.uhc_flex_practitioner_store_support import (
    ACQUISITION_TABLE,
    assert_identity_row,
    identity_fields,
    MEMBER_TABLE,
    row_fields,
    set_store_action,
    table_ref,
    WORK_TABLE,
)


_terminal_record_sha256 = terminal_record_sha256
_canonical_resource_rows = canonical_resource_fields_list


async def _insert_acquisition_header(
    database: Any,
    identity: UHCFlexPractitionerAcquisitionIdentity,
) -> int:
    return await database.status(
        f"""
        INSERT INTO {table_ref(ACQUISITION_TABLE)} (
            acquisition_id, storage_contract_id, cohort_id,
            acquisition_role, source_id, connector_id, query_contract_id,
            run_id, dataset_intent_id, expected_npi_count, status,
            cohort_complete, endpoint_collection_complete, endpoint_complete
        ) VALUES (
            :acquisition_id, :storage_contract_id, :cohort_id,
            :acquisition_role, :source_id, :connector_id,
            :query_contract_id, :run_id, :dataset_intent_id,
            :expected_npi_count, 'building', false,
            :endpoint_collection_complete, :endpoint_complete
        ) ON CONFLICT (acquisition_id) DO NOTHING;
        """,
        **identity_fields(identity),
    )


async def _insert_pending_workset(
    database: Any,
    identity: UHCFlexPractitionerAcquisitionIdentity,
) -> None:
    await set_store_action(database, "initialize", identity.acquisition_id)
    await database.status(
        f"""
        INSERT INTO {table_ref(WORK_TABLE)} (
            acquisition_id, cohort_id, npi, status, attempt_count
        )
        SELECT :acquisition_id, member.cohort_id, member.npi, 'pending', 0
          FROM {table_ref(MEMBER_TABLE)} AS member
         WHERE member.cohort_id = :cohort_id
         ORDER BY member.npi
        ON CONFLICT (acquisition_id, npi) DO NOTHING;
        """,
        acquisition_id=identity.acquisition_id,
        cohort_id=identity.cohort_id,
    )


async def _exact_workset_census(
    database: Any,
    identity: UHCFlexPractitionerAcquisitionIdentity,
) -> dict[str, Any]:
    database_row = await database.first(
        f"""
        SELECT count(*)::bigint AS work_count,
               NOT EXISTS (
                   SELECT member.npi FROM {table_ref(MEMBER_TABLE)} AS member
                    WHERE member.cohort_id = :cohort_id
                   EXCEPT
                   SELECT work.npi FROM {table_ref(WORK_TABLE)} AS work
                    WHERE work.acquisition_id = :acquisition_id
               ) AND NOT EXISTS (
                   SELECT work.npi FROM {table_ref(WORK_TABLE)} AS work
                    WHERE work.acquisition_id = :acquisition_id
                   EXCEPT
                   SELECT member.npi FROM {table_ref(MEMBER_TABLE)} AS member
                    WHERE member.cohort_id = :cohort_id
               ) AS exact_members
          FROM {table_ref(WORK_TABLE)} AS work
         WHERE work.acquisition_id = :acquisition_id;
        """,
        acquisition_id=identity.acquisition_id,
        cohort_id=identity.cohort_id,
    )
    return row_fields(database_row)


async def initialize_uhc_flex_practitioner_acquisition(
    identity: UHCFlexPractitionerAcquisitionIdentity,
    *,
    database: Any = db,
) -> int:
    """Insert one header and set-wise pending workset; return inserted headers."""

    if type(identity) is not UHCFlexPractitionerAcquisitionIdentity:
        raise ValueError("Flex Practitioner acquisition identity is invalid")
    async with database.transaction():
        await database.scalar(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended(:identity, 0));",
            identity=identity.acquisition_id,
        )
        created_count = await _insert_acquisition_header(database, identity)
        header = await database.first(
            f"SELECT * FROM {table_ref(ACQUISITION_TABLE)} "
            "WHERE acquisition_id = :acquisition_id FOR SHARE;",
            acquisition_id=identity.acquisition_id,
        )
        assert_identity_row(identity, header)
        await _insert_pending_workset(database, identity)
        census = await _exact_workset_census(database, identity)
        if (
            census.get("work_count") != identity.expected_npi_count
            or census.get("exact_members") is not True
        ):
            raise UHCFlexPractitionerStoreError("state")
    return created_count


def _claim_from_row(database_row: Any) -> UHCFlexPractitionerWorkClaim | None:
    fields = row_fields(database_row)
    if not fields:
        return None
    try:
        return UHCFlexPractitionerWorkClaim(
            acquisition_id=fields.get("acquisition_id"),
            cohort_id=fields.get("cohort_id"),
            requested_npi=fields.get("npi"),
            attempt=fields.get("attempt_count"),
            lease_token=fields.get("lease_token"),
        )
    except ValueError as error:
        raise UHCFlexPractitionerStoreError("state") from error


def _validate_claim_selection(
    requested_npi: int | None,
    excluded_npis: tuple[int, ...],
    fresh_only: bool | None,
) -> None:
    if requested_npi is not None:
        canonical_uhc_flex_npi(requested_npi)
    if (
        type(excluded_npis) is not tuple
        or len(excluded_npis) > UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY
        or len(set(excluded_npis)) != len(excluded_npis)
        or requested_npi is not None and excluded_npis
        or fresh_only is not None and type(fresh_only) is not bool
        or requested_npi is not None and fresh_only is not None
    ):
        raise ValueError("Flex Practitioner claim selection is invalid")
    for excluded_npi in excluded_npis:
        canonical_uhc_flex_npi(excluded_npi)


def _uhc_flex_practitioner_claim_sql(
    fresh_filter: str,
    excluded_filter: str,
) -> str:
    return f"""
        WITH candidate AS (
            SELECT work.acquisition_id, work.npi
              FROM {table_ref(WORK_TABLE)} AS work
              JOIN {table_ref(ACQUISITION_TABLE)} AS acquisition
                ON acquisition.acquisition_id = work.acquisition_id
             WHERE work.acquisition_id = :acquisition_id
               AND acquisition.status = 'building'
               AND (CAST(:requested_npi AS bigint) IS NULL
                    OR work.npi = CAST(:requested_npi AS bigint))
               {excluded_filter}
               AND (work.status = 'pending' OR (
                   work.status = 'leased'
                   AND work.lease_expires_at <= clock_timestamp()
               ) OR (
                   work.status = 'error'
                   AND work.error_code = 'content_type_invalid'
                   AND work.attempt_count = 1
               ))
               {fresh_filter}
             ORDER BY work.attempt_count, work.npi
             FOR UPDATE OF work SKIP LOCKED LIMIT 1
        )
        UPDATE {table_ref(WORK_TABLE)} AS work
           SET status = 'leased', attempt_count = work.attempt_count + 1,
               lease_token = :lease_token,
               lease_expires_at = transaction_timestamp()
                   + make_interval(secs => :lease_seconds),
               lease_heartbeat_at = transaction_timestamp(),
               result_sha256 = NULL, resource_count = NULL,
               error_code = NULL, terminal_record_sha256 = NULL,
               terminal_at = NULL, updated_at = transaction_timestamp()
          FROM candidate
         WHERE work.acquisition_id = candidate.acquisition_id
           AND work.npi = candidate.npi
        RETURNING work.*;
    """


async def claim_uhc_flex_practitioner_work(
    acquisition_id: str,
    *,
    requested_npi: int | None = None,
    excluded_npis: tuple[int, ...] = (),
    fresh_only: bool | None = None,
    lease_seconds: int = 300,
    database: Any = db,
) -> UHCFlexPractitionerWorkClaim | None:
    """Claim one pending or expired NPI generation with SKIP LOCKED."""

    strict_identifier(acquisition_id, ACQUISITION_PATTERN, "acquisition ID")
    _validate_claim_selection(requested_npi, excluded_npis, fresh_only)
    if type(lease_seconds) is not int or not 30 <= lease_seconds <= 3600:
        raise ValueError("Flex Practitioner lease seconds are invalid")
    lease_token = secrets.token_hex(32)
    if requested_npi is not None or fresh_only is False:
        fresh_filters = ("",)
    elif fresh_only is True:
        fresh_filters = ("AND work.attempt_count = 0",)
    else:
        fresh_filters = ("AND work.attempt_count = 0", "")
    excluded_filter = (
        "AND work.npi <> ALL(CAST(:excluded_npis AS bigint[]))"
        if excluded_npis
        else ""
    )
    claim_parameter_by_name: dict[str, Any] = {
        "acquisition_id": acquisition_id,
        "requested_npi": requested_npi,
        "lease_token": lease_token,
        "lease_seconds": lease_seconds,
    }
    if excluded_npis:
        claim_parameter_by_name["excluded_npis"] = list(excluded_npis)
    database_row = None
    async with database.transaction():
        await set_store_action(database, "claim", acquisition_id, lease_token)
        for fresh_filter in fresh_filters:
            database_row = await database.first(
                _uhc_flex_practitioner_claim_sql(
                    fresh_filter,
                    excluded_filter,
                ),
                **claim_parameter_by_name,
            )
            if database_row is not None:
                break
    return _claim_from_row(database_row)


async def heartbeat_uhc_flex_practitioner_work(
    claim: UHCFlexPractitionerWorkClaim,
    *,
    lease_seconds: int = 300,
    database: Any = db,
) -> None:
    """Extend only the exact active lease token and attempt."""

    if type(claim) is not UHCFlexPractitionerWorkClaim:
        raise ValueError("Flex Practitioner work claim is invalid")
    if type(lease_seconds) is not int or not 30 <= lease_seconds <= 3600:
        raise ValueError("Flex Practitioner lease seconds are invalid")
    async with database.transaction():
        await set_store_action(
            database,
            "heartbeat",
            claim.acquisition_id,
            claim.lease_token,
        )
        updated_count = await database.status(
            f"""
            UPDATE {table_ref(WORK_TABLE)}
               SET lease_expires_at = transaction_timestamp()
                       + make_interval(secs => :lease_seconds),
                   lease_heartbeat_at = transaction_timestamp(),
                   updated_at = transaction_timestamp()
             WHERE acquisition_id = :acquisition_id AND npi = :npi
               AND status = 'leased' AND attempt_count = :attempt
               AND lease_token = :lease_token
               AND lease_expires_at > clock_timestamp();
            """,
            acquisition_id=claim.acquisition_id,
            npi=claim.requested_npi,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
            lease_seconds=lease_seconds,
        )
    if updated_count != 1:
        raise UHCFlexPractitionerStoreError("lease_lost")


async def release_uhc_flex_practitioner_work(
    claim: UHCFlexPractitionerWorkClaim,
    *,
    database: Any = db,
) -> None:
    """Return an active empty lease to pending for an immediate retry."""

    if type(claim) is not UHCFlexPractitionerWorkClaim:
        raise ValueError("Flex Practitioner work claim is invalid")
    async with database.transaction():
        await set_store_action(
            database,
            "release",
            claim.acquisition_id,
            claim.lease_token,
        )
        updated_count = await database.status(
            f"""
            UPDATE {table_ref(WORK_TABLE)}
               SET status = 'pending', lease_token = NULL,
                   lease_expires_at = NULL, lease_heartbeat_at = NULL,
                   updated_at = transaction_timestamp()
             WHERE acquisition_id = :acquisition_id AND npi = :npi
               AND status = 'leased' AND attempt_count = :attempt
               AND lease_token = :lease_token
               AND lease_expires_at > clock_timestamp();
            """,
            acquisition_id=claim.acquisition_id,
            npi=claim.requested_npi,
            attempt=claim.attempt,
            lease_token=claim.lease_token,
        )
    if updated_count != 1:
        raise UHCFlexPractitionerStoreError("lease_lost")


__all__ = (
    "build_uhc_flex_practitioner_acquisition_identity",
    "claim_uhc_flex_practitioner_work",
    "complete_uhc_flex_practitioner_error",
    "complete_uhc_flex_practitioner_result",
    "heartbeat_uhc_flex_practitioner_work",
    "initialize_uhc_flex_practitioner_acquisition",
    "read_uhc_flex_practitioner_resource_page",
    "release_uhc_flex_practitioner_work",
    "seal_uhc_flex_practitioner_acquisition",
    "UHCFlexPractitionerAcquisitionIdentity",
    "UHCFlexPractitionerAcquisitionSummary",
    "UHCFlexPractitionerResourceRow",
    "UHCFlexPractitionerStoreError",
    "UHCFlexPractitionerWorkClaim",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_ROLES",
    "UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID",
)
