# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persisted lease incarnations, bounded renewal and cancellation."""

from process.registry_named_preparation_catalog import locked_claim, locked_preparation, preparation_record
from process.registry_named_preparation_records import PREPARATION, claim_values, local_uuid, require, sha256


def lease_duration(seconds):
    """Server time plus at most two minutes bounds every claim."""
    require(type(seconds) is int and 1 <= seconds <= 120, "claim_duration_invalid")
    return seconds


async def claim_preparation(store, preparation_id, admission_sha256, lease_token, seconds):
    """A replay cannot extend an expiry or reuse an expired token."""
    seconds, token = lease_duration(seconds), local_uuid(lease_token)
    preparation_row = await locked_preparation(store, preparation_id, sha256(admission_sha256))
    require(preparation_row["state"] not in {"abandoned", "published"}, "preparation_terminal")
    live = await store.connection.fetchval(
        "SELECT $1::timestamptz > pg_catalog.clock_timestamp()", preparation_row["claim_expires_at"]
    )
    if live:
        require(preparation_row["claim_token"] == token, "claim_busy")
        previous = await store.connection.fetchval(
            "SELECT lease_seconds FROM hp_snapshot_retention.preparation_claim WHERE lease_token=$1", token
        )
        require(previous == seconds, "claim_conflict")
        return preparation_row
    require(
        not await store.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM hp_snapshot_retention.preparation_claim WHERE lease_token=$1)", token
        ),
        "claim_token_reused",
    )
    attempt = preparation_row["claim_attempt"] + 1
    await store.connection.execute(
        "INSERT INTO hp_snapshot_retention.preparation_claim(lease_token,preparation_id,attempt,lease_seconds) "
        "VALUES($1,$2,$3,$4)",
        token,
        preparation_row["preparation_id"],
        attempt,
        seconds,
    )
    return preparation_record(
        await store.connection.fetchrow(
            f"UPDATE {PREPARATION} SET claim_token=$2,claim_attempt=$3,"
            "claim_expires_at=pg_catalog.clock_timestamp()+$4*interval '1 second',"
            "updated_at=pg_catalog.clock_timestamp() WHERE preparation_id=$1 RETURNING *",
            preparation_row["preparation_id"],
            token,
            attempt,
            seconds,
        )
    )


async def renew_preparation(store, claim, seconds):
    """Canceled and expired claims cannot keep ownership alive."""
    seconds = lease_duration(seconds)
    preparation_row = await locked_claim(store, claim)
    return preparation_record(
        await store.connection.fetchrow(
            f"UPDATE {PREPARATION} SET claim_expires_at=pg_catalog.clock_timestamp()+$2*interval '1 second',"
            "updated_at=pg_catalog.clock_timestamp() WHERE preparation_id=$1 AND claim_token=$3 AND claim_attempt=$4 "
            "AND NOT cancellation_requested AND claim_expires_at>pg_catalog.clock_timestamp() RETURNING *",
            preparation_row["preparation_id"],
            seconds,
            preparation_row["claim_token"],
            preparation_row["claim_attempt"],
        )
    )


async def request_cancel(store, preparation_id, admission_sha256):
    """Cancellation is durable admission-scoped intent, not an implicit cleanup."""
    preparation_row = await locked_preparation(store, preparation_id, sha256(admission_sha256))
    require(preparation_row["state"] != "published", "preparation_terminal")
    return preparation_record(
        await store.connection.fetchrow(
            f"UPDATE {PREPARATION} SET cancellation_requested=true,updated_at=pg_catalog.clock_timestamp() "
            "WHERE preparation_id=$1 RETURNING *",
            preparation_row["preparation_id"],
        )
    )


async def abandon_preparation(store, claim, reason_sha256):
    """Retain all resources and evidence; only future cleanup may release anything."""
    reason = sha256(reason_sha256)
    preparation_id, token, attempt = claim_values(claim)
    preparation_row = await locked_preparation(store, preparation_id)
    if preparation_row["state"] == "abandoned":
        require(
            preparation_row["claim_token"] == token
            and preparation_row["claim_attempt"] == attempt
            and preparation_row["abandonment_sha256"] == reason,
            "abandonment_conflict",
        )
        return preparation_row
    preparation_row = await locked_claim(store, claim, allow_canceled=True)
    require(preparation_row["cancellation_requested"], "cancellation_required")
    return preparation_record(
        await store.connection.fetchrow(
            f"UPDATE {PREPARATION} SET state='abandoned',abandonment_sha256=$2,updated_at=pg_catalog.clock_timestamp() "
            "WHERE preparation_id=$1 AND claim_token=$3 AND claim_attempt=$4 AND cancellation_requested "
            "AND claim_expires_at>pg_catalog.clock_timestamp() RETURNING *",
            preparation_row["preparation_id"],
            reason,
            token,
            attempt,
        )
    )
