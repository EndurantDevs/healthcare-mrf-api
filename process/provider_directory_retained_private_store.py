# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Private locator and validator access with explicit key rewrap."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import asyncpg

from process.provider_directory_retained_artifact_contract import (
    LeaseIdentity,
    RetainedArtifactError,
    RetainedCampaignMismatch,
)
from process.provider_directory_retained_artifact_keys import (
    LOCATOR_PRIVATE_VALUE,
    VALIDATOR_PRIVATE_VALUE,
    _OpenedPrivateValue,
    _consume_private_value,
    _is_private_identity_valid,
    _open_private_value,
    active_private_key_id,
    private_item_binding,
    private_identity_hmac,
    seal_private_value,
)
from process.provider_directory_retained_lease_store import (
    _require_campaign_lease,
    _require_item_lease,
)
from process.provider_directory_retained_store_support import (
    database_record,
    database_table,
)


async def private_item_locator(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
) -> _OpenedPrivateValue:
    """Decrypt one worker-only locator and recheck its keyed identity."""

    locator_record_by_field = database_record(
        await connection.fetchrow(
            f"""SELECT item.locator_ciphertext, item.locator_identity_hmac,
                        item.locator_key_id, campaign.endpoint_id
                   FROM {database_table('provider_directory_retained_artifact_campaign_item')} AS item
                   JOIN {database_table('provider_directory_retained_artifact_campaign')} AS campaign
                     ON campaign.campaign_id=item.campaign_id
                  WHERE item.campaign_id=$1 AND item.source_item_id=$2;""",
            campaign_id,
            source_item_id,
        )
    )
    if (
        not locator_record_by_field
        or locator_record_by_field.get("locator_ciphertext") is None
    ):
        raise RetainedArtifactError("retained_item_locator_unavailable")
    binding_sha256 = private_item_binding(
        campaign_id=campaign_id,
        source_item_id=source_item_id,
        endpoint_id=str(locator_record_by_field["endpoint_id"]),
    )
    locator = _open_private_value(
        locator_record_by_field["locator_ciphertext"],
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    )
    if locator.key_id != locator_record_by_field.get(
        "locator_key_id"
    ) or not _is_private_identity_valid(
        locator,
        locator_record_by_field.get("locator_identity_hmac"),
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    ):
        raise RetainedCampaignMismatch("retained_locator_identity_mismatch")
    return locator


def _rewrap_validator(
    item_state_by_field: dict[str, object],
    *,
    binding_sha256: str,
) -> tuple[str | None, bool]:
    validator_ciphertext = item_state_by_field.get("validator_ciphertext")
    if not isinstance(validator_ciphertext, str):
        return None, False
    validator = _open_private_value(
        validator_ciphertext,
        purpose=VALIDATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    )
    if validator.key_id != item_state_by_field.get("validator_key_id"):
        raise RetainedCampaignMismatch("retained_validator_identity_mismatch")
    validator_value = _consume_private_value(
        validator,
        purpose=VALIDATOR_PRIVATE_VALUE,
    )
    expected_sha256 = hashlib.sha256(validator_value.encode("utf-8")).hexdigest()
    if expected_sha256 != item_state_by_field.get("validator_sha256"):
        raise RetainedCampaignMismatch("retained_validator_identity_mismatch")
    rewrapped_validator = seal_private_value(
        validator_value,
        purpose=VALIDATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    )
    is_key_changed = rewrapped_validator.split(":", 2)[1] != validator.key_id
    return rewrapped_validator, is_key_changed


def _rewrap_locator(
    item_state_by_field: dict[str, object],
    *,
    binding_sha256: str,
) -> tuple[str, str, bool]:
    locator = _open_private_value(
        item_state_by_field["locator_ciphertext"],
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    )
    if locator.key_id != item_state_by_field.get(
        "locator_key_id"
    ) or not _is_private_identity_valid(
        locator,
        item_state_by_field.get("locator_identity_hmac"),
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    ):
        raise RetainedCampaignMismatch("retained_locator_identity_mismatch")
    locator_value = _consume_private_value(
        locator,
        purpose=LOCATOR_PRIVATE_VALUE,
    )
    locator_ciphertext = seal_private_value(
        locator_value,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    )
    locator_identity = private_identity_hmac(
        locator_value,
        purpose=LOCATOR_PRIVATE_VALUE,
        binding_sha256=binding_sha256,
    )
    is_key_changed = locator_ciphertext.split(":", 2)[1] != locator.key_id
    return locator_ciphertext, locator_identity, is_key_changed


@dataclass(frozen=True)
class _PrivateItemRewrap:
    locator_ciphertext: str
    locator_identity: str
    validator_ciphertext: str | None
    active_key_id: str
    is_required: bool


def _rewrapped_private_item(
    item_state_by_field: dict[str, object],
    binding_sha256: str,
) -> _PrivateItemRewrap:
    locator_ciphertext, locator_identity, is_locator_key_changed = _rewrap_locator(
        item_state_by_field,
        binding_sha256=binding_sha256,
    )
    validator_ciphertext, is_validator_key_changed = _rewrap_validator(
        item_state_by_field,
        binding_sha256=binding_sha256,
    )
    return _PrivateItemRewrap(
        locator_ciphertext=locator_ciphertext,
        locator_identity=locator_identity,
        validator_ciphertext=validator_ciphertext,
        active_key_id=active_private_key_id(),
        is_required=is_locator_key_changed or is_validator_key_changed,
    )


async def _persist_private_item_rewrap(
    connection: asyncpg.Connection,
    item_table: str,
    campaign_id: str,
    source_item_id: str,
    rewrap: _PrivateItemRewrap,
) -> None:
    await connection.execute(
        f"""UPDATE {item_table}
               SET locator_ciphertext=$3, locator_identity_hmac=$4,
                   locator_key_id=$5::varchar(32), validator_ciphertext=$6,
                   validator_key_id=CASE
                       WHEN $6::text IS NULL THEN NULL
                       ELSE $5::varchar(32)
                   END,
                   updated_at=now()
             WHERE campaign_id=$1 AND source_item_id=$2;""",
        campaign_id,
        source_item_id,
        rewrap.locator_ciphertext,
        rewrap.locator_identity,
        rewrap.active_key_id,
        rewrap.validator_ciphertext,
    )


async def is_item_private_rewrap_applied(
    connection: asyncpg.Connection,
    *,
    campaign_id: str,
    source_item_id: str,
    campaign_lease: LeaseIdentity,
    item_lease: LeaseIdentity,
) -> bool:
    """Return whether one leased item was re-encrypted with the active key."""

    item_table = database_table("provider_directory_retained_artifact_campaign_item")
    async with connection.transaction():
        campaign_state_by_field = await _require_campaign_lease(
            connection,
            campaign_id,
            campaign_lease,
        )
        item_state_by_field = await _require_item_lease(
            connection,
            campaign_id,
            source_item_id,
            item_lease,
        )
        binding_sha256 = private_item_binding(
            campaign_id=campaign_id,
            source_item_id=source_item_id,
            endpoint_id=str(campaign_state_by_field["endpoint_id"]),
        )
        rewrap = _rewrapped_private_item(
            item_state_by_field,
            binding_sha256,
        )
        if rewrap.is_required:
            await _persist_private_item_rewrap(
                connection,
                item_table,
                campaign_id,
                source_item_id,
                rewrap,
            )
    return rewrap.is_required


rewrap_item_private_values = is_item_private_rewrap_applied
