# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import replace

import pytest

from process.provider_directory_retained_artifact_contract import (
    LeaseIdentity,
    RetainedArtifactError,
    RetainedCampaignMismatch,
    endpoint_request_fence_digest,
)
from process.provider_directory_retained_artifact_keys import (
    LOCATOR_PRIVATE_VALUE,
    VALIDATOR_PRIVATE_VALUE,
    _consume_private_value,
    private_item_binding,
    seal_private_value,
)
from process.provider_directory_retained_catalog_store import (
    initialize_retained_artifact_campaign,
)
from process.provider_directory_retained_lease_store import (
    acquire_campaign_lease,
    acquire_item_lease,
)
from process.provider_directory_retained_private_store import (
    _rewrap_validator,
    private_item_locator,
    rewrap_item_private_values,
)
from process.provider_directory_retained_request_fence_store import (
    defer_endpoint_request_fence,
    reserve_endpoint_request_slot,
)
from process.provider_directory_retained_status_store import (
    retained_artifact_campaign_status,
)
from process.provider_directory_retained_store_support import database_table
from tests.provider_directory_retained_core_postgres_support import (
    admit_campaign_item,
    campaign_item,
    digest,
    fixed_campaign_plan,
    registry_artifact,
    retained_database,
)


async def _leased_fixed_member(connection, label: str):
    retained_item = campaign_item(label)
    campaign_plan = fixed_campaign_plan(label, (retained_item,))
    campaign_id = await initialize_retained_artifact_campaign(
        connection, plan=campaign_plan
    )
    campaign_lease = await acquire_campaign_lease(
        connection, campaign_id=campaign_id, owner=f"campaign-{label}"
    )
    item_lease = await acquire_item_lease(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
        campaign_lease=campaign_lease,
        owner=f"item-{label}",
    )
    return retained_item, campaign_id, campaign_lease, item_lease


async def _seed_validator_ciphertext(connection, campaign_id: str, source_item_id: str):
    validator_text = '"key-rotation-etag"'
    endpoint_id = await connection.fetchval(
        f"SELECT endpoint_id FROM {database_table('provider_directory_retained_artifact_campaign')} WHERE campaign_id=$1",
        campaign_id,
    )
    binding_sha256 = private_item_binding(
        campaign_id=campaign_id,
        source_item_id=source_item_id,
        endpoint_id=str(endpoint_id),
    )
    await connection.execute(
        f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
               SET validator_ciphertext=$3, validator_key_id='core-test-v1',
                   validator_sha256=$4
             WHERE campaign_id=$1 AND source_item_id=$2""",
        campaign_id,
        source_item_id,
        seal_private_value(
            validator_text,
            purpose=VALIDATOR_PRIVATE_VALUE,
            binding_sha256=binding_sha256,
        ),
        hashlib.sha256(validator_text.encode("utf-8")).hexdigest(),
    )


def _configure_rotated_keyring(monkeypatch) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_PREVIOUS_KEYRING",
        json.dumps({"core-test-v1": "retained-core-postgres-test-key-material"}),
    )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY_ID", "core-test-v2"
    )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY",
        "retained-core-rotated-test-key-material",
    )


@pytest.mark.asyncio
async def test_private_values_rewrap_locator_and_validator_atomically(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, campaign_lease, item_lease = (
            await _leased_fixed_member(connection, "key-rotation")
        )
        await _seed_validator_ciphertext(
            connection, campaign_id, retained_item.source_item_id
        )
        assert not await rewrap_item_private_values(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            item_lease=item_lease,
        )
        _configure_rotated_keyring(monkeypatch)
        assert await rewrap_item_private_values(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            item_lease=item_lease,
        )
        key_state = await connection.fetchrow(
            f"""SELECT locator_key_id, validator_key_id
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            retained_item.source_item_id,
        )
        assert dict(key_state) == {
            "locator_key_id": "core-test-v2",
            "validator_key_id": "core-test-v2",
        }
        locator = await private_item_locator(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
        )
        assert (
            _consume_private_value(
                locator,
                purpose=LOCATOR_PRIVATE_VALUE,
            )
            == f"fixture://{retained_item.source_item_id}"
        )


async def _leased_matching_fence_member(connection):
    retained_item = campaign_item("fence-second")
    endpoint_id = digest("endpoint:fixed:fence-first")
    campaign_plan = replace(
        fixed_campaign_plan("fence-second", (retained_item,)),
        endpoint_id=endpoint_id,
        request_fence_id=endpoint_request_fence_digest(endpoint_id),
        credential_descriptor_sha256=digest("credential:fence-second"),
    ).validate()
    campaign_id = await initialize_retained_artifact_campaign(
        connection, plan=campaign_plan
    )
    campaign_lease = await acquire_campaign_lease(
        connection, campaign_id=campaign_id, owner="campaign-fence-second"
    )
    item_lease = await acquire_item_lease(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
        campaign_lease=campaign_lease,
        owner="item-fence-second",
    )
    return retained_item, campaign_id, campaign_lease, item_lease


async def _reserve_slot(
    connection,
    leased_state,
    *,
    request_interval_ms: int,
    retry_not_before: dt.datetime | None = None,
):
    return await reserve_endpoint_request_slot(
        connection,
        campaign_id=leased_state[1],
        source_item_id=leased_state[0].source_item_id,
        campaign_lease=leased_state[2],
        item_lease=leased_state[3],
        request_interval_ms=request_interval_ms,
        retry_not_before=retry_not_before,
    )


def test_validator_rewrap_rejects_key_and_digest_mismatch(monkeypatch) -> None:
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY_ID", "validator-test-v1"
    )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY",
        "validator-test-key-material-long-enough",
    )
    validator_text = '"validator-etag"'
    binding_sha256 = private_item_binding(
        campaign_id=digest("validator-campaign"),
        source_item_id=digest("validator-item"),
        endpoint_id=digest("validator-endpoint"),
    )
    validator_ciphertext = seal_private_value(
        validator_text,
        purpose=VALIDATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    )
    with pytest.raises(RetainedCampaignMismatch, match="validator_identity"):
        _rewrap_validator(
            {
                "validator_ciphertext": validator_ciphertext,
                "validator_key_id": "wrong-key",
                "validator_sha256": hashlib.sha256(
                    validator_text.encode("utf-8")
                ).hexdigest(),
            },
            binding_sha256=binding_sha256,
        )
    with pytest.raises(RetainedCampaignMismatch, match="validator_identity"):
        _rewrap_validator(
            {
                "validator_ciphertext": validator_ciphertext,
                "validator_key_id": "validator-test-v1",
                "validator_sha256": digest("wrong-validator"),
            },
            binding_sha256=binding_sha256,
        )
    assert _rewrap_validator(
        {"validator_ciphertext": None},
        binding_sha256=binding_sha256,
    ) == (None, False)


@pytest.mark.asyncio
async def test_private_locator_rejects_missing_and_tampered_identity(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item, campaign_id, campaign_lease, item_lease = (
            await _leased_fixed_member(connection, "locator-tamper")
        )
        await connection.execute(
            f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
                   SET locator_identity_hmac=$3
                 WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            retained_item.source_item_id,
            "pdhmac3:core-test-v1:" + "0" * 64,
        )
        with pytest.raises(RetainedCampaignMismatch, match="locator_identity"):
            await private_item_locator(
                connection,
                campaign_id=campaign_id,
                source_item_id=retained_item.source_item_id,
            )
        with pytest.raises(RetainedCampaignMismatch, match="locator_identity"):
            await rewrap_item_private_values(
                connection,
                campaign_id=campaign_id,
                source_item_id=retained_item.source_item_id,
                campaign_lease=campaign_lease,
                item_lease=item_lease,
            )
        with pytest.raises(RetainedArtifactError, match="locator_unavailable"):
            await private_item_locator(
                connection,
                campaign_id=digest("missing-campaign"),
                source_item_id=digest("missing-item"),
            )


async def _assert_locator_transplant_rejected(
    connection,
    *,
    plan_label,
    target_label,
    source_label=None,
    locator_row=None,
):
    target_item = campaign_item(target_label)
    if locator_row is None:
        source_item = campaign_item(source_label)
        planned_items = (source_item, target_item)
    else:
        source_item = None
        planned_items = (target_item,)
    campaign_id = await initialize_retained_artifact_campaign(
        connection,
        plan=fixed_campaign_plan(plan_label, planned_items),
    )
    if source_item is not None:
        locator_row = await connection.fetchrow(
            f"""SELECT locator_ciphertext, locator_identity_hmac, locator_key_id
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')}
                  WHERE campaign_id=$1 AND source_item_id=$2""",
            campaign_id,
            source_item.source_item_id,
        )
    await connection.execute(
        f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
               SET locator_ciphertext=$3, locator_identity_hmac=$4,
                   locator_key_id=$5
             WHERE campaign_id=$1 AND source_item_id=$2""",
        campaign_id,
        target_item.source_item_id,
        locator_row["locator_ciphertext"],
        locator_row["locator_identity_hmac"],
        locator_row["locator_key_id"],
    )
    with pytest.raises(
        RetainedArtifactError,
        match="retained_private_decryption_failed",
    ):
        await private_item_locator(
            connection,
            campaign_id=campaign_id,
            source_item_id=target_item.source_item_id,
        )
    return locator_row


@pytest.mark.asyncio
async def test_private_locator_rejects_cross_item_and_campaign_tuple_transplants(
    monkeypatch,
) -> None:
    """Private locator tuples cannot move across item or campaign bindings."""
    async with retained_database(monkeypatch) as (connection, _schema_name):
        locator_row = await _assert_locator_transplant_rejected(
            connection,
            plan_label="binding-shared-campaign",
            source_label="binding-first",
            target_label="binding-second",
        )
        await _assert_locator_transplant_rejected(
            connection,
            plan_label="binding-other-campaign",
            target_label="binding-third",
            locator_row=locator_row,
        )


@pytest.mark.asyncio
async def test_request_fence_serializes_campaigns_and_honors_deferral(
    monkeypatch,
) -> None:
    async with retained_database(monkeypatch) as (connection, _schema_name):
        first_state = await _leased_fixed_member(connection, "fence-first")
        second_state = await _leased_matching_fence_member(connection)
        first_slot = await _reserve_slot(
            connection, first_state, request_interval_ms=1000
        )
        second_slot = await _reserve_slot(
            connection, second_state, request_interval_ms=1000
        )
        assert second_slot >= first_slot + dt.timedelta(seconds=1)
        requested_slot = second_slot + dt.timedelta(seconds=30)
        explicit_slot = await _reserve_slot(
            connection,
            first_state,
            request_interval_ms=0,
            retry_not_before=requested_slot,
        )
        assert explicit_slot >= requested_slot
        deferred_until = explicit_slot + dt.timedelta(minutes=1)
        await defer_endpoint_request_fence(
            connection,
            campaign_id=second_state[1],
            source_item_id=second_state[0].source_item_id,
            campaign_lease=second_state[2],
            item_lease=second_state[3],
            retry_not_before=deferred_until,
        )
        deferred_slot = await _reserve_slot(
            connection, first_state, request_interval_ms=0
        )
        assert deferred_slot >= deferred_until


async def _assert_request_fence_input_validation():
    valid_lease = LeaseIdentity("fence-owner", 1)
    valid_digest = digest("fence-validation")
    with pytest.raises(RetainedArtifactError, match="request_interval_invalid"):
        await reserve_endpoint_request_slot(
            None,
            campaign_id=valid_digest,
            source_item_id=valid_digest,
            campaign_lease=valid_lease,
            item_lease=valid_lease,
            request_interval_ms=86400001,
        )
    with pytest.raises(RetainedArtifactError, match="retry_not_before_invalid"):
        await defer_endpoint_request_fence(
            None,
            campaign_id=valid_digest,
            source_item_id=valid_digest,
            campaign_lease=valid_lease,
            item_lease=valid_lease,
            retry_not_before=dt.datetime.now(),
        )


async def _assert_far_future_fence_rejected(connection, leased_state):
    retained_item, campaign_id, campaign_lease, item_lease = leased_state
    with pytest.raises(RetainedArtifactError, match="retry_not_before_too_far"):
        await defer_endpoint_request_fence(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            item_lease=item_lease,
            retry_not_before=dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=2),
        )
    assert (
        await connection.fetchval(
            f"SELECT count(*) FROM {database_table('provider_directory_retained_artifact_endpoint_fence')}"
        )
        == 0
    )


async def _assert_completed_item_fence_rejected(connection, leased_state):
    retained_item, campaign_id, campaign_lease, item_lease = leased_state
    artifact = registry_artifact("completed-fence", retained_item.artifact_kind)
    await admit_campaign_item(connection, campaign_id, retained_item, artifact)
    with pytest.raises(RetainedArtifactError, match="already_complete"):
        await reserve_endpoint_request_slot(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            item_lease=item_lease,
            request_interval_ms=0,
        )
    with pytest.raises(RetainedArtifactError, match="already_complete"):
        await defer_endpoint_request_fence(
            connection,
            campaign_id=campaign_id,
            source_item_id=retained_item.source_item_id,
            campaign_lease=campaign_lease,
            item_lease=item_lease,
            retry_not_before=dt.datetime.now(dt.timezone.utc),
        )


@pytest.mark.asyncio
async def test_request_fence_validates_timing_and_completed_items(monkeypatch) -> None:
    """Request fences reject invalid timing and any already-complete item."""
    await _assert_request_fence_input_validation()
    async with retained_database(monkeypatch) as (connection, _schema_name):
        leased_state = await _leased_fixed_member(connection, "completed-fence")
        await _assert_far_future_fence_rejected(connection, leased_state)
        await _assert_completed_item_fence_rejected(connection, leased_state)


@pytest.mark.asyncio
async def test_status_filters_validate_and_never_expose_capabilities(
    monkeypatch,
) -> None:
    with pytest.raises(RetainedArtifactError, match="status_limit_invalid"):
        await retained_artifact_campaign_status(None, limit=True)
    async with retained_database(monkeypatch) as (connection, _schema_name):
        retained_item = campaign_item("status-filter")
        campaign_plan = fixed_campaign_plan("status-filter", (retained_item,))
        campaign_id = await initialize_retained_artifact_campaign(
            connection, plan=campaign_plan
        )
        all_statuses = await retained_artifact_campaign_status(connection)
        adapter_statuses = await retained_artifact_campaign_status(
            connection, adapter_id=campaign_plan.adapter_id, limit=1
        )
        campaign_status = await retained_artifact_campaign_status(
            connection, campaign_id=campaign_id
        )
        assert len(all_statuses["items"]) == 1
        assert len(adapter_statuses["items"]) == 1
        assert campaign_status["items"][0]["census"][0]["item_count"] == 1
        serialized_status = repr(campaign_status)
        assert "locator" not in serialized_status
        assert "validator" not in serialized_status
