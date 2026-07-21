# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Completeness verification and immutable campaign sealing."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    ORDERED_STREAMS,
    TERMINAL_ZERO,
    LeaseIdentity,
    RetainedArtifactError,
    RetainedCampaignMismatch,
    item_census_digest,
    observation_set_digest,
    require_nonnegative_int,
    sealed_campaign_digest,
)
from process.provider_directory_retained_lease_store import _require_campaign_lease
from process.provider_directory_retained_seal_contract import (
    SealSnapshot,
    assert_item_layout_artifact_bindings,
    assert_layout_range_census,
    campaign_seal_counts,
    is_complete_campaign,
)
from process.provider_directory_retained_identity_contract import (
    assert_campaign_membership,
    assert_planned_item_commitments,
    assert_planned_item_identities,
)
from process.provider_directory_retained_status_store import _safe_campaign_summary
from process.provider_directory_retained_store_support import (
    MUTABLE_CAMPAIGN_STATES,
    SealTables,
    database_record_list,
    database_table,
    seal_tables,
)


async def set_campaign_disk_reservation(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    campaign_lease: LeaseIdentity,
    reserved_bytes: int,
    allow_increase: bool = False,
) -> None:
    """Persist the exact real allocation currently held by the worker."""

    require_nonnegative_int(reserved_bytes, "disk_reserved_bytes")
    async with connection.transaction():
        await _require_campaign_lease(connection, campaign_id, campaign_lease)
        await connection.execute(
            f"""UPDATE {database_table('provider_directory_retained_artifact_campaign')}
                   SET disk_reserved_bytes=CASE
                           WHEN $3 OR disk_reserved_bytes=0 THEN $2
                           ELSE LEAST(disk_reserved_bytes, $2)
                       END,
                       updated_at=now()
                 WHERE campaign_id=$1;""",
            campaign_id,
            reserved_bytes,
            allow_increase,
        )


async def _locked_campaign_census(
    connection: asyncpg.Connection,
    tables: SealTables,
    campaign_id: str,
    campaign_record_by_field: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    await connection.execute(
        f"""UPDATE {tables.campaign_item}
               SET status='unaccounted',
                   safe_failure_code='campaign_item_unaccounted',
                   updated_at=now()
             WHERE campaign_id=$1
               AND status NOT IN (
                   'admitted', 'terminal_zero', 'failed', 'unaccounted'
               );""",
        campaign_id,
    )
    campaign_item_states = database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {tables.campaign_item} WHERE campaign_id=$1
                  ORDER BY campaign_ordinal FOR UPDATE;""",
            campaign_id,
        )
    )
    if len(campaign_item_states) != int(
        campaign_record_by_field["expected_item_count"]
    ):
        raise RetainedCampaignMismatch("campaign_item_count_changed")
    assert_planned_item_identities(campaign_item_states)
    commitment_states = database_record_list(
        await connection.fetch(
            f"""SELECT campaign_id, source_item_id, campaign_ordinal,
                        planned_identity_sha256
                   FROM {tables.campaign_item_identity}
                  WHERE campaign_id=$1 ORDER BY campaign_ordinal FOR UPDATE;""",
            campaign_id,
        )
    )
    assert_planned_item_commitments(campaign_item_states, commitment_states)
    stream_states = database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {tables.stream} WHERE campaign_id=$1
                  ORDER BY stream_ordinal FOR UPDATE;""",
            campaign_id,
        )
    )
    assert_campaign_membership(
        campaign_record_by_field,
        campaign_item_states,
        stream_states,
    )
    return campaign_item_states, stream_states


def _stream_member_registry(
    stream_states: Sequence[Mapping[str, Any]],
    campaign_item_states: Sequence[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    stream_member_registry: dict[str, list[Mapping[str, Any]]] = {
        str(stream_state["stream_identity_sha256"]): []
        for stream_state in stream_states
    }
    for campaign_item_state in campaign_item_states:
        stream_members = stream_member_registry.get(
            str(campaign_item_state["stream_identity_sha256"])
        )
        if stream_members is None:
            raise RetainedCampaignMismatch("campaign_stream_identity_changed")
        stream_members.append(campaign_item_state)
    return stream_member_registry


def _stream_completion_proof(
    stream_members: Sequence[Mapping[str, Any]],
) -> tuple[bool, Mapping[str, Any] | None]:
    ordered_members = sorted(
        stream_members,
        key=lambda campaign_item_state: int(campaign_item_state["sequence_ordinal"]),
    )
    actual_ordinals = [
        int(campaign_item_state["sequence_ordinal"])
        for campaign_item_state in ordered_members
    ]
    is_sequence_contiguous = actual_ordinals == list(range(len(ordered_members)))
    terminal_candidates = [
        campaign_item_state
        for campaign_item_state in ordered_members
        if campaign_item_state["item_role"] == TERMINAL_ZERO
        and campaign_item_state["status"] == "terminal_zero"
    ]
    terminal_state = terminal_candidates[0] if len(terminal_candidates) == 1 else None
    is_terminal_last = bool(
        terminal_state
        and ordered_members
        and ordered_members[-1]["source_item_id"] == terminal_state["source_item_id"]
    )
    are_payloads_complete = bool(ordered_members) and all(
        campaign_item_state["status"] == "admitted"
        for campaign_item_state in ordered_members[:-1]
    )
    return (
        is_sequence_contiguous and is_terminal_last and are_payloads_complete,
        terminal_state,
    )


async def _persist_stream_completion(
    connection: asyncpg.Connection,
    stream_table: str,
    campaign_id: str,
    stream_identity_sha256: str,
    terminal_state: Mapping[str, Any] | None,
) -> None:
    if terminal_state is None:
        await connection.execute(
            f"""UPDATE {stream_table}
                   SET complete=FALSE, terminal_sequence_ordinal=NULL,
                       terminal_proof_sha256=NULL, completed_at=NULL
                 WHERE campaign_id=$1 AND stream_identity_sha256=$2;""",
            campaign_id,
            stream_identity_sha256,
        )
        return
    await connection.execute(
        f"""UPDATE {stream_table}
               SET complete=TRUE, terminal_sequence_ordinal=$3,
                   terminal_proof_sha256=$4,
                   completed_at=COALESCE(completed_at, now())
             WHERE campaign_id=$1 AND stream_identity_sha256=$2;""",
        campaign_id,
        stream_identity_sha256,
        terminal_state["sequence_ordinal"],
        terminal_state["terminal_proof_sha256"],
    )


async def _seal_ordered_streams(
    connection: asyncpg.Connection,
    tables: SealTables,
    campaign_id: str,
    expected_stream_count: int,
    campaign_item_states: Sequence[Mapping[str, Any]],
    stream_states: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    if len(stream_states) != expected_stream_count:
        raise RetainedCampaignMismatch("campaign_stream_count_changed")
    stream_member_registry = _stream_member_registry(
        stream_states,
        campaign_item_states,
    )
    terminal_stream_count = 0
    for stream_state in stream_states:
        stream_identity_sha256 = str(stream_state["stream_identity_sha256"])
        is_stream_complete, terminal_state = _stream_completion_proof(
            stream_member_registry[stream_identity_sha256]
        )
        await _persist_stream_completion(
            connection,
            tables.stream,
            campaign_id,
            stream_identity_sha256,
            terminal_state if is_stream_complete else None,
        )
        terminal_stream_count += int(is_stream_complete)
    refreshed_stream_states = database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {tables.stream} WHERE campaign_id=$1
                  ORDER BY stream_ordinal;""",
            campaign_id,
        )
    )
    return refreshed_stream_states, terminal_stream_count


async def _sealed_stream_states(
    connection: asyncpg.Connection,
    tables: SealTables,
    campaign_id: str,
    campaign_record_by_field: Mapping[str, Any],
    campaign_item_states: Sequence[Mapping[str, Any]],
    stream_states: Sequence[Mapping[str, Any]],
) -> tuple[Sequence[Mapping[str, Any]], int]:
    if campaign_record_by_field["census_mode"] == ORDERED_STREAMS:
        return await _seal_ordered_streams(
            connection,
            tables,
            campaign_id,
            int(campaign_record_by_field["expected_stream_count"]),
            campaign_item_states,
            stream_states,
        )
    if stream_states:
        raise RetainedCampaignMismatch("fixed_catalog_has_stream_rows")
    return stream_states, 0


async def _registry_states(
    connection: asyncpg.Connection,
    table_name: str,
    identity_column: str,
    identity_hashes: Sequence[str],
) -> list[dict[str, Any]]:
    if not identity_hashes:
        return []
    return database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {table_name}
                  WHERE {identity_column}=ANY($1::text[])
                  ORDER BY {identity_column};""",
            identity_hashes,
        )
    )


async def _verified_registry_snapshot(
    connection: asyncpg.Connection,
    tables: SealTables,
    admitted_item_states: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    artifact_hashes = sorted(
        {
            str(campaign_item_state["artifact_sha256"])
            for campaign_item_state in admitted_item_states
        }
    )
    layout_hashes = sorted(
        {
            str(campaign_item_state["layout_sha256"])
            for campaign_item_state in admitted_item_states
        }
    )
    artifact_states = await _registry_states(
        connection, tables.artifact, "artifact_sha256", artifact_hashes
    )
    layout_states = await _registry_states(
        connection, tables.layout, "layout_sha256", layout_hashes
    )
    is_registry_incomplete = (
        len(artifact_states) != len(artifact_hashes)
        or len(layout_states) != len(layout_hashes)
        or any(
            artifact_state["registry_status"] != "verified"
            for artifact_state in artifact_states
        )
        or any(
            layout_state["registry_status"] != "verified"
            for layout_state in layout_states
        )
    )
    if is_registry_incomplete:
        raise RetainedCampaignMismatch("campaign_artifact_registry_incomplete")
    assert_item_layout_artifact_bindings(
        admitted_item_states,
        layout_states,
    )
    layout_range_states = await _layout_range_states(
        connection,
        tables.layout_range,
        layout_hashes,
    )
    assert_layout_range_census(layout_states, layout_range_states)
    return artifact_states, layout_states, layout_range_states


async def _layout_range_states(
    connection: asyncpg.Connection,
    range_table: str,
    layout_hashes: Sequence[str],
) -> list[dict[str, Any]]:
    if not layout_hashes:
        return []
    return database_record_list(
        await connection.fetch(
            f"""SELECT * FROM {range_table}
                  WHERE layout_sha256=ANY($1::text[])
                  ORDER BY layout_sha256, range_ordinal;""",
            layout_hashes,
        )
    )


async def _persist_campaign_seal(
    connection: asyncpg.Connection,
    campaign_table: str,
    campaign_id: str,
    digest_record_by_field: Mapping[str, Any],
    campaign_sha256: str,
    is_complete: bool,
) -> dict[str, Any]:
    seal_state = "complete" if is_complete else "sealed_incomplete"
    await connection.execute(
        f"""UPDATE {campaign_table}
               SET state=$2, observed_item_count=$3, expected_byte_count=$4,
                   admitted_item_count=$5, admitted_byte_count=$6,
                   admitted_record_count=$7, terminal_zero_item_count=$8,
                   terminal_stream_count=$9, failed_item_count=$10,
                   unaccounted_item_count=$11, item_census_sha256=$12,
                   observation_set_sha256=$13, campaign_sha256=$14,
                   complete=$15, sealed_at=now(), updated_at=now()
             WHERE campaign_id=$1;""",
        campaign_id,
        seal_state,
        digest_record_by_field["observed_item_count"],
        digest_record_by_field["expected_byte_count"],
        digest_record_by_field["admitted_item_count"],
        digest_record_by_field["admitted_byte_count"],
        digest_record_by_field["admitted_record_count"],
        digest_record_by_field["terminal_zero_item_count"],
        digest_record_by_field["terminal_stream_count"],
        digest_record_by_field["failed_item_count"],
        digest_record_by_field["unaccounted_item_count"],
        digest_record_by_field["item_census_sha256"],
        digest_record_by_field["observation_set_sha256"],
        campaign_sha256,
        is_complete,
    )
    return {
        **digest_record_by_field,
        "state": seal_state,
        "campaign_sha256": campaign_sha256,
        "complete": is_complete,
    }


def _campaign_seal_identity(
    seal_snapshot: SealSnapshot,
) -> tuple[dict[str, Any], str, bool]:
    seal_counts = campaign_seal_counts(seal_snapshot)
    is_complete = is_complete_campaign(seal_snapshot, seal_counts)
    digest_record_by_field = {
        **seal_snapshot.campaign_record_by_field,
        **seal_counts,
        "item_census_sha256": item_census_digest(seal_snapshot.campaign_item_states),
        "observation_set_sha256": observation_set_digest(
            seal_snapshot.campaign_item_states
        ),
    }
    campaign_sha256 = sealed_campaign_digest(
        campaign_row=digest_record_by_field,
        item_rows=seal_snapshot.campaign_item_states,
        stream_rows=seal_snapshot.stream_states,
        artifact_rows=seal_snapshot.artifact_states,
        layout_rows=seal_snapshot.layout_states,
        range_rows=seal_snapshot.layout_range_states,
    )
    return digest_record_by_field, campaign_sha256, is_complete


async def seal_retained_artifact_campaign(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    campaign_lease: LeaseIdentity,
) -> dict[str, Any]:
    """Seal fixed catalogs or contiguous streams; reject incomplete claims."""

    tables = seal_tables()
    async with connection.transaction():
        campaign_record_by_field = await _require_campaign_lease(
            connection,
            campaign_id,
            campaign_lease,
        )
        if campaign_record_by_field["state"] not in MUTABLE_CAMPAIGN_STATES:
            raise RetainedArtifactError("retained_campaign_not_sealable")
        campaign_item_states, stream_states = await _locked_campaign_census(
            connection,
            tables,
            campaign_id,
            campaign_record_by_field,
        )
        stream_states, terminal_stream_count = await _sealed_stream_states(
            connection,
            tables,
            campaign_id,
            campaign_record_by_field,
            campaign_item_states,
            stream_states,
        )
        admitted_item_states = [
            campaign_item_state
            for campaign_item_state in campaign_item_states
            if campaign_item_state["status"] == "admitted"
        ]
        registry_snapshot = await _verified_registry_snapshot(
            connection,
            tables,
            admitted_item_states,
        )
        seal_snapshot = SealSnapshot(
            campaign_record_by_field,
            campaign_item_states,
            stream_states,
            *registry_snapshot,
            terminal_stream_count,
        )
        digest_record_by_field, campaign_sha256, is_complete = _campaign_seal_identity(
            seal_snapshot
        )
        summary_by_field = await _persist_campaign_seal(
            connection,
            tables.campaign,
            campaign_id,
            digest_record_by_field,
            campaign_sha256,
            is_complete,
        )
    return _safe_campaign_summary(summary_by_field)
