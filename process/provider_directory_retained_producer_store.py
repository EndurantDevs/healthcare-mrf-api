# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic registration for producer-verified retained artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    ARTIFACT_KINDS,
    PAYLOAD,
    PRODUCER_PROOF,
    PRODUCER_VERIFIED,
    LeaseIdentity,
    ProducedArtifact,
    RetainedArtifactError,
    RetainedCampaignMismatch,
    expected_range_set_digest,
    produced_layout_digest,
    require_digest,
    require_nonnegative_int,
    require_positive_int,
    require_safe_id,
)
from process.provider_directory_retained_blob_store import retained_artifact_blob_components
from process.provider_directory_retained_lease_store import (
    _assert_item_record_identity,
    _require_campaign_lease,
    _require_item_lease,
)
from process.provider_directory_retained_store_support import (
    MUTABLE_CAMPAIGN_STATES,
    database_record,
    database_record_list,
    database_table,
)


_ADMITTABLE_ITEM_STATUSES = frozenset({"expected", "failed", "unaccounted"})
_RANGE_FIELDS = (
    "range_ordinal",
    "raw_byte_start",
    "raw_byte_end",
    "raw_byte_count",
    "raw_sha256",
    "record_start",
    "record_end",
    "record_count",
    "canonical_sha256",
    "canonical_byte_count",
)


@dataclass(frozen=True)
class _PreparedArtifact:
    artifact_kind: str
    artifact: dict[str, Any]
    layout: dict[str, Any]
    ranges: tuple[dict[str, Any], ...]


def _snapshot_produced_artifact(produced: ProducedArtifact) -> ProducedArtifact:
    if type(produced) is not ProducedArtifact:
        raise RetainedArtifactError("produced_artifact_invalid")
    snapshot = ProducedArtifact(
        artifact_sha256=produced.artifact_sha256,
        artifact_kind=produced.artifact_kind,
        artifact_byte_count=produced.artifact_byte_count,
        artifact_record_count=produced.artifact_record_count,
        artifact_path=produced.artifact_path,
        layout_contract_id=produced.layout_contract_id,
        layout_contract_version=produced.layout_contract_version,
        range_set_sha256=produced.range_set_sha256,
        canonical_byte_count=produced.canonical_byte_count,
        manifest_sha256=produced.manifest_sha256,
        manifest_byte_count=produced.manifest_byte_count,
        manifest_path=produced.manifest_path,
        producer_build_id=produced.producer_build_id,
        ranges=produced.ranges,
    )
    require_digest(snapshot.artifact_sha256, "artifact_sha256")
    if type(snapshot.artifact_kind) is not str or snapshot.artifact_kind not in ARTIFACT_KINDS:
        raise RetainedArtifactError("artifact_kind_invalid")
    require_positive_int(snapshot.artifact_byte_count, "artifact_byte_count")
    require_nonnegative_int(snapshot.artifact_record_count, "artifact_record_count")
    require_safe_id(snapshot.layout_contract_id, "layout_contract_id", maximum=128)
    require_positive_int(snapshot.layout_contract_version, "layout_contract_version")
    if snapshot.layout_contract_version > 2**31 - 1:
        raise RetainedArtifactError("layout_contract_version_invalid")
    require_digest(snapshot.range_set_sha256, "range_set_sha256")
    require_positive_int(snapshot.canonical_byte_count, "canonical_byte_count")
    require_digest(snapshot.manifest_sha256, "manifest_sha256")
    require_positive_int(snapshot.manifest_byte_count, "manifest_byte_count")
    require_safe_id(snapshot.producer_build_id, "producer_build_id", maximum=256)
    return snapshot


def _validated_range_states(produced: ProducedArtifact) -> tuple[dict[str, Any], ...]:
    previous_raw_end = int(produced._range_payloads[0][1])
    previous_record_end = 0
    canonical_total = 0
    range_states: list[dict[str, Any]] = []
    for range_payload in produced._range_payloads:
        range_state_by_field = dict(zip(_RANGE_FIELDS, range_payload))
        if (
            range_state_by_field["raw_byte_start"] != previous_raw_end
            or range_state_by_field["record_start"] != previous_record_end
        ):
            raise RetainedArtifactError("artifact_layout_range_sequence_invalid")
        previous_raw_end = int(range_state_by_field["raw_byte_end"])
        previous_record_end = int(range_state_by_field["record_end"])
        canonical_total += int(range_state_by_field["canonical_byte_count"])
        range_states.append(range_state_by_field)
    if (
        previous_raw_end != produced.artifact_byte_count
        or previous_record_end != produced.artifact_record_count
        or canonical_total != produced.canonical_byte_count
    ):
        raise RetainedArtifactError("artifact_layout_summary_mismatch")
    if expected_range_set_digest(produced) != produced.range_set_sha256:
        raise RetainedArtifactError("artifact_range_set_mismatch")
    return tuple(range_states)


def _prepare_artifact(produced: ProducedArtifact) -> _PreparedArtifact:
    produced = _snapshot_produced_artifact(produced)
    range_states = _validated_range_states(produced)
    layout_sha256 = produced_layout_digest(produced)
    range_count = len(range_states)
    return _PreparedArtifact(
        artifact_kind=produced.artifact_kind,
        artifact={
            "artifact_sha256": produced.artifact_sha256,
            "artifact_byte_count": produced.artifact_byte_count,
            "artifact_locator": "/".join(retained_artifact_blob_components(produced.artifact_sha256)),
            "registry_status": "verified",
            "released_at": None,
        },
        layout={
            "layout_sha256": layout_sha256,
            "artifact_sha256": produced.artifact_sha256,
            "artifact_record_count": produced.artifact_record_count,
            "layout_contract_id": produced.layout_contract_id,
            "layout_contract_version": produced.layout_contract_version,
            "layout_range_count": range_count,
            "range_set_sha256": produced.range_set_sha256,
            "canonical_byte_count": produced.canonical_byte_count,
            "manifest_sha256": produced.manifest_sha256,
            "manifest_byte_count": produced.manifest_byte_count,
            "manifest_locator": "/".join(retained_artifact_blob_components(produced.manifest_sha256)),
            "producer_build_id": produced.producer_build_id,
            "registry_status": "verified",
            "released_at": None,
        },
        ranges=tuple(
            {
                "layout_sha256": layout_sha256,
                "artifact_sha256": produced.artifact_sha256,
                "layout_contract_version": produced.layout_contract_version,
                "layout_range_count": range_count,
                **range_state_by_field,
            }
            for range_state_by_field in range_states
        ),
    )


def _assert_planned_item(
    item_by_field: dict[str, Any],
    campaign: dict[str, Any],
    prepared: _PreparedArtifact,
) -> None:
    if item_by_field.get("item_role") != PAYLOAD:
        raise RetainedCampaignMismatch("retained_producer_item_mismatch")
    if item_by_field.get("status") not in _ADMITTABLE_ITEM_STATUSES:
        raise RetainedCampaignMismatch("retained_producer_item_state_mismatch")
    if item_by_field.get("artifact_kind") != prepared.artifact_kind:
        raise RetainedCampaignMismatch("retained_producer_item_mismatch")
    declared_byte_count = item_by_field.get("declared_byte_count")
    artifact_byte_count = prepared.artifact["artifact_byte_count"]
    if declared_byte_count is not None and declared_byte_count != artifact_byte_count:
        raise RetainedCampaignMismatch("retained_producer_declared_size_mismatch")
    if artifact_byte_count > int(campaign["per_item_byte_budget"]):
        raise RetainedArtifactError("per_item_byte_budget_exceeded")
    clean_fields = (
        "observed_byte_count",
        "acquisition_mode",
        "validator_kind",
        "validator_ciphertext",
        "validator_key_id",
        "validator_sha256",
        "immutable_identity_sha256",
        "retry_not_before",
        "downloaded_artifact_sha256",
        "artifact_sha256",
        "layout_sha256",
        "admitted_at",
    )
    if (
        any(item_by_field.get(field) is not None for field in clean_fields)
        or item_by_field.get("request_interval_ms") != 0
        or item_by_field.get("committed_byte_count") != 0
    ):
        raise RetainedCampaignMismatch("retained_producer_item_state_mismatch")


def _assert_admitted_item(
    item_by_field: dict[str, Any],
    prepared: _PreparedArtifact,
) -> None:
    expected_by_field = {
        "status": "admitted",
        "observed_byte_count": prepared.artifact["artifact_byte_count"],
        "acquisition_mode": PRODUCER_VERIFIED,
        "validator_kind": PRODUCER_PROOF,
        "validator_ciphertext": None,
        "validator_key_id": None,
        "validator_sha256": None,
        "immutable_identity_sha256": prepared.artifact["artifact_sha256"],
        "request_interval_ms": 0,
        "retry_not_before": None,
        "committed_byte_count": prepared.artifact["artifact_byte_count"],
        "safe_failure_code": None,
        "downloaded_artifact_sha256": prepared.artifact["artifact_sha256"],
        "artifact_sha256": prepared.artifact["artifact_sha256"],
        "layout_sha256": prepared.layout["layout_sha256"],
        "lease_owner": None,
        "lease_expires_at": None,
    }
    if (
        item_by_field.get("item_role") != PAYLOAD
        or item_by_field.get("artifact_kind") != prepared.artifact_kind
        or item_by_field.get("admitted_at") is None
        or any(
            item_by_field.get(field) != expected_value
            for field, expected_value in expected_by_field.items()
        )
    ):
        raise RetainedCampaignMismatch("retained_producer_admission_mismatch")


async def _insert_artifact(
    connection: asyncpg.Connection,
    prepared: _PreparedArtifact,
) -> None:
    await connection.execute(
        f"""INSERT INTO {database_table('provider_directory_retained_artifact')} (
                artifact_sha256, artifact_byte_count, artifact_locator,
                registry_status, verified_at, created_at
            ) VALUES ($1, $2, $3, 'verified', now(), now())
            ON CONFLICT DO NOTHING;""",
        prepared.artifact["artifact_sha256"],
        prepared.artifact["artifact_byte_count"],
        prepared.artifact["artifact_locator"],
    )


async def _insert_layout_and_ranges(
    connection: asyncpg.Connection,
    prepared: _PreparedArtifact,
) -> None:
    layout = prepared.layout
    await connection.execute(
        f"""INSERT INTO {database_table('provider_directory_retained_artifact_layout')} (
                layout_sha256, artifact_sha256, artifact_record_count,
                layout_contract_id, layout_contract_version, layout_range_count,
                range_set_sha256, canonical_byte_count, manifest_sha256,
                manifest_byte_count, manifest_locator, producer_build_id,
                registry_status, verified_at, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                      'verified', now(), now())
            ON CONFLICT DO NOTHING;""",
        layout["layout_sha256"],
        layout["artifact_sha256"],
        layout["artifact_record_count"],
        layout["layout_contract_id"],
        layout["layout_contract_version"],
        layout["layout_range_count"],
        layout["range_set_sha256"],
        layout["canonical_byte_count"],
        layout["manifest_sha256"],
        layout["manifest_byte_count"],
        layout["manifest_locator"],
        layout["producer_build_id"],
    )
    await _assert_registered_layout_identity(connection, prepared)
    for range_state in prepared.ranges:
        await connection.execute(
            f"""INSERT INTO {database_table('provider_directory_retained_artifact_range')} (
                    layout_sha256, artifact_sha256, layout_contract_version,
                    layout_range_count, range_ordinal, raw_byte_start, raw_byte_end,
                    raw_byte_count, raw_sha256, record_start, record_end, record_count,
                    canonical_sha256, canonical_byte_count, verified_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                          $13, $14, now())
                ON CONFLICT DO NOTHING;""",
            *(range_state[field] for field in (
                "layout_sha256",
                "artifact_sha256",
                "layout_contract_version",
                "layout_range_count",
                *_RANGE_FIELDS,
            )),
        )


async def _assert_registered_artifact(
    connection: asyncpg.Connection,
    prepared: _PreparedArtifact,
) -> None:
    await _assert_registered_artifact_identity(connection, prepared)
    await _assert_registered_layout_identity(connection, prepared)
    ranges = database_record_list(
        await connection.fetch(
            f"""SELECT layout_sha256, artifact_sha256, layout_contract_version,
                        layout_range_count, range_ordinal, raw_byte_start,
                        raw_byte_end, raw_byte_count, raw_sha256, record_start,
                        record_end, record_count, canonical_sha256,
                        canonical_byte_count
                   FROM {database_table('provider_directory_retained_artifact_range')}
                  WHERE layout_sha256=$1 ORDER BY range_ordinal FOR SHARE;""",
            prepared.layout["layout_sha256"],
        )
    )
    if ranges != list(prepared.ranges):
        raise RetainedCampaignMismatch("retained_layout_range_registry_mismatch")


async def _assert_registered_artifact_identity(
    connection: asyncpg.Connection,
    prepared: _PreparedArtifact,
) -> None:
    artifact = database_record(
        await connection.fetchrow(
            f"""SELECT artifact_sha256, artifact_byte_count, artifact_locator,
                        registry_status, released_at
                   FROM {database_table('provider_directory_retained_artifact')}
                  WHERE artifact_sha256=$1 FOR SHARE;""",
            prepared.artifact["artifact_sha256"],
        )
    )
    if artifact != prepared.artifact:
        raise RetainedCampaignMismatch("retained_artifact_registry_mismatch")


async def _assert_registered_layout_identity(
    connection: asyncpg.Connection,
    prepared: _PreparedArtifact,
) -> None:
    layout = database_record(
        await connection.fetchrow(
            f"""SELECT layout_sha256, artifact_sha256, artifact_record_count,
                        layout_contract_id, layout_contract_version,
                        layout_range_count, range_set_sha256, canonical_byte_count,
                        manifest_sha256, manifest_byte_count, manifest_locator,
                        producer_build_id, registry_status, released_at
                   FROM {database_table('provider_directory_retained_artifact_layout')}
                  WHERE layout_sha256=$1 FOR SHARE;""",
            prepared.layout["layout_sha256"],
        )
    )
    if layout != prepared.layout:
        raise RetainedCampaignMismatch("retained_layout_registry_mismatch")


async def _assert_campaign_budget(
    connection: asyncpg.Connection,
    campaign: dict[str, Any],
    campaign_id: str,
    artifact_byte_count: int,
) -> None:
    admitted_bytes = int(
        await connection.fetchval(
            f"""SELECT COALESCE(sum(observed_byte_count), 0)
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND status='admitted';""",
            campaign_id,
        )
    )
    if admitted_bytes + artifact_byte_count > int(campaign["aggregate_byte_budget"]):
        raise RetainedArtifactError("aggregate_byte_budget_exceeded")


async def _update_admitted_item(
    connection: asyncpg.Connection,
    campaign_id: str, source_item_id: str,
    campaign_lease: LeaseIdentity,
    item_lease: LeaseIdentity,
    prepared: _PreparedArtifact,
) -> None:
    """Atomically admit an item only while both exact leases remain live."""

    admitted_item_by_field = database_record(
        await connection.fetchrow(
            f"""WITH fence_time AS MATERIALIZED (SELECT clock_timestamp() AS observed_at
                   ), campaign_lease AS MATERIALIZED (
                       SELECT EXISTS (SELECT 1
                             FROM {database_table('provider_directory_retained_artifact_campaign')},
                                  fence_time
                            WHERE campaign_id=$1 AND lease_owner=$3 AND lease_epoch=$4
                              AND lease_expires_at > fence_time.observed_at
                       ) AS valid
                   ), admitted AS (
                    UPDATE {database_table('provider_directory_retained_artifact_campaign_item')} AS item
                   SET status='admitted', observed_byte_count=$7,
                       acquisition_mode='producer_verified',
                       validator_kind='producer_proof',
                       validator_ciphertext=NULL, validator_key_id=NULL,
                       validator_sha256=NULL, immutable_identity_sha256=$8,
                       request_interval_ms=0, retry_not_before=NULL,
                       committed_byte_count=$7, safe_failure_code=NULL,
                       downloaded_artifact_sha256=$8, artifact_sha256=$8,
                       layout_sha256=$9, admitted_at=fence_time.observed_at,
                       updated_at=fence_time.observed_at,
                       lease_owner=NULL, lease_expires_at=NULL
                  FROM fence_time, campaign_lease
                 WHERE campaign_lease.valid
                   AND item.campaign_id=$1 AND item.source_item_id=$2
                   AND item.lease_owner=$5 AND item.lease_epoch=$6
                   AND item.lease_expires_at > fence_time.observed_at
                   AND item.status IN ('expected', 'failed', 'unaccounted')
                 RETURNING item.*
                   )
                SELECT campaign_lease.valid AS campaign_lease_valid, admitted.*
                  FROM campaign_lease LEFT JOIN admitted ON TRUE;""",
            campaign_id,
            source_item_id,
            campaign_lease.owner,
            campaign_lease.epoch,
            item_lease.owner,
            item_lease.epoch,
            prepared.artifact["artifact_byte_count"],
            prepared.artifact["artifact_sha256"],
            prepared.layout["layout_sha256"],
        )
    )
    campaign_lease_valid = admitted_item_by_field.pop("campaign_lease_valid", False)
    if not campaign_lease_valid:
        raise RetainedArtifactError("campaign_lease_lost")
    if admitted_item_by_field.get("campaign_id") is None:
        raise RetainedArtifactError("item_lease_lost")
    await _assert_item_record_identity(connection, admitted_item_by_field)
    _assert_admitted_item(admitted_item_by_field, prepared)


async def admit_produced_artifact(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
    campaign_lease: LeaseIdentity,
    item_lease: LeaseIdentity,
    produced_artifact: ProducedArtifact,
) -> str:
    """Register one exact producer proof and admit its leased campaign item."""

    campaign_id = require_digest(campaign_id, "campaign_id")
    source_item_id = require_digest(source_item_id, "source_item_id")
    campaign_lease.validate()
    item_lease.validate()
    prepared = _prepare_artifact(produced_artifact)
    async with connection.transaction():
        campaign = await _require_campaign_lease(connection, campaign_id, campaign_lease)
        if campaign.get("state") not in MUTABLE_CAMPAIGN_STATES:
            raise RetainedCampaignMismatch("retained_producer_campaign_state_mismatch")
        item_by_field = database_record(
            await connection.fetchrow(
                f"""SELECT *
                       FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                      WHERE campaign_id=$1 AND source_item_id=$2 FOR UPDATE;""",
                campaign_id,
                source_item_id,
            )
        )
        if not item_by_field:
            raise RetainedArtifactError("retained_item_not_found")
        await _assert_item_record_identity(connection, item_by_field)
        if item_by_field.get("status") == "admitted":
            _assert_admitted_item(item_by_field, prepared)
            await _assert_registered_artifact(connection, prepared)
            return str(prepared.layout["layout_sha256"])
        _assert_planned_item(item_by_field, campaign, prepared)
        await _require_item_lease(connection, campaign_id, source_item_id, item_lease)
        await _assert_campaign_budget(
            connection, campaign, campaign_id, int(prepared.artifact["artifact_byte_count"])
        )
        await _insert_artifact(connection, prepared)
        await _assert_registered_artifact_identity(connection, prepared)
        await _insert_layout_and_ranges(connection, prepared)
        await _assert_registered_artifact(connection, prepared)
        await _update_admitted_item(
            connection, campaign_id, source_item_id, campaign_lease, item_lease, prepared
        )
    return str(prepared.layout["layout_sha256"])

__all__ = ("admit_produced_artifact",)
