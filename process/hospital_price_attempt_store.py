# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Attempt admission, evidence, and lease updates for hospital-price imports."""

from __future__ import annotations

import uuid
from typing import Any, Sequence

from db.models import db
from process.hospital_price_acquisition import (
    REGISTRY_VERSION,
    Attempt,
    Candidate,
    schema_name,
)
from process.hospital_price_store_sql import (
    ADMIT_ATTEMPTS_SQL,
    RENEW_ATTEMPTS_SQL,
)
from process.ptg_parts.db_tables import _quote_ident


async def admit_attempts(
    candidates: Sequence[Candidate], *, lease_owner: str, lease_seconds: int
) -> Sequence[Any]:
    """Fence and create running attempts for eligible hospital candidates."""

    lease_owner = lease_owner.strip()
    if not lease_owner or len(lease_owner) > 128 or lease_seconds < 2:
        raise ValueError("hospital attempt lease is invalid")
    stage_name = f"hospital_attempt_candidates_{uuid.uuid4().hex[:12]}"
    stage = _quote_ident(stage_name)
    schema = _quote_ident(schema_name())
    async with db.acquire() as connection:
        await connection.status(
            f"CREATE TEMP TABLE {stage} (hospital_id varchar(64), "
            "attempt_id varchar(64), locator_id varchar(64), observation_id "
            "varchar(64), source_url text) ON COMMIT DROP"
        )
        driver = getattr(
            connection.raw_connection, "driver_connection", connection.raw_connection
        )
        await driver.copy_records_to_table(
            stage_name,
            columns=[
                "hospital_id", "attempt_id", "locator_id", "observation_id", "source_url"
            ],
            records=[
                (
                    candidate.hospital_id,
                    uuid.uuid4().hex,
                    candidate.locator_id,
                    candidate.observation_id,
                    candidate.source_url,
                )
                for candidate in candidates
            ],
        )
        return await connection.all(
            ADMIT_ATTEMPTS_SQL.format(schema=schema, stage=stage),
            registry_version=REGISTRY_VERSION,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
        )


async def rebind_attempt_sources(
    bindings: Sequence[tuple[Attempt, Candidate]],
) -> None:
    """Atomically bind running attempts to refreshed locator observations."""

    if not bindings:
        return
    expected_attempt_ids = {attempt.attempt_id for attempt, _candidate in bindings}
    if (
        len(expected_attempt_ids) != len(bindings)
        or any(
            attempt.hospital_id != candidate.hospital_id
            for attempt, candidate in bindings
        )
    ):
        raise ValueError("hospital refreshed attempt binding is invalid")
    schema = _quote_ident(schema_name())
    stage_name = f"hospital_refreshed_attempts_{uuid.uuid4().hex[:12]}"
    stage = _quote_ident(stage_name)
    async with db.acquire() as connection:
        await connection.status(
            f"CREATE TEMP TABLE {stage} (attempt_id varchar(64), "
            "hospital_id varchar(64), locator_id varchar(64), "
            "observation_id varchar(64), source_url text) ON COMMIT DROP"
        )
        driver = getattr(
            connection.raw_connection, "driver_connection", connection.raw_connection
        )
        await driver.copy_records_to_table(
            stage_name,
            columns=[
                "attempt_id", "hospital_id", "locator_id", "observation_id",
                "source_url",
            ],
            records=[
                (
                    attempt.attempt_id, attempt.hospital_id, candidate.locator_id,
                    candidate.observation_id, candidate.source_url,
                )
                for attempt, candidate in bindings
            ],
        )
        updated_attempts = await connection.all(
            f"UPDATE {schema}.hospital_price_import_attempt attempt SET "
            "locator_id=staged.locator_id, "
            "locator_observation_id=staged.observation_id, "
            "requested_source_url=staged.source_url "
            f"FROM {stage} staged WHERE attempt.attempt_id=staged.attempt_id "
            "AND attempt.hospital_id=staged.hospital_id AND attempt.status='running' "
            "RETURNING attempt.attempt_id"
        )
        if {
            str(updated_attempt[0]) for updated_attempt in updated_attempts
        } != expected_attempt_ids:
            raise RuntimeError("hospital refreshed attempt changed before source retry")


async def fail_attempts(
    attempts: Sequence[Attempt], error_code: str, error_detail: str | None
) -> int:
    """Fail running attempts with their final bounded source evidence."""

    if not attempts:
        return 0
    schema = _quote_ident(schema_name())
    stage_name = f"hospital_failed_attempts_{uuid.uuid4().hex[:12]}"
    stage = _quote_ident(stage_name)
    async with db.acquire() as connection:
        await connection.status(
            f"CREATE TEMP TABLE {stage} (attempt_id varchar(64), final_source_url text, "
            "source_http_status integer) ON COMMIT DROP"
        )
        driver = getattr(
            connection.raw_connection, "driver_connection", connection.raw_connection
        )
        await driver.copy_records_to_table(
            stage_name, columns=["attempt_id", "final_source_url", "source_http_status"],
            records=[
                (attempt.attempt_id, attempt.final_source_url, attempt.source_http_status)
                for attempt in attempts
            ],
        )
        return int(await connection.status(
            f"UPDATE {schema}.hospital_price_import_attempt attempt SET status='failed', "
            "finished_at=clock_timestamp(), final_source_url=staged.final_source_url, "
            "source_http_status=staged.source_http_status, error_code=:code, "
            f"error_detail=:detail FROM {stage} staged "
            "WHERE attempt.attempt_id=staged.attempt_id "
            "AND attempt.status IN ('running', 'verified')",
            code=error_code[:64], detail=(error_detail or error_code)[:2000],
        ) or 0)


async def renew_attempt_leases(
    attempts: Sequence[Attempt], *, lease_owner: str, lease_seconds: int
) -> int:
    """Renew this worker's unexpired active attempts without resurrection."""

    attempt_ids = tuple(dict.fromkeys(attempt.attempt_id for attempt in attempts))
    if not attempt_ids:
        return 0
    schema = _quote_ident(schema_name())
    renewed, expired, foreign = await db.first(
        RENEW_ATTEMPTS_SQL.format(schema=schema),
        attempt_ids=attempt_ids,
        lease_owner=lease_owner,
        lease_seconds=lease_seconds,
    )
    if int(expired) or int(foreign):
        raise RuntimeError("hospital price attempt lease was lost")
    return int(renewed)
