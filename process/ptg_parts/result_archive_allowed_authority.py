# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operation-owned review retention for published allowed snapshots."""

from dataclasses import dataclass

from process.network_address_projection import _identifier
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_source_lifecycle_lock
from process.ptg_parts.result_archive_published_authority import (
    PtgPublishedResultSourceAuthority,
    _source_key_for_snapshot,
)
from process.ptg_parts.result_archive_source_authority import (
    PtgResultArchiveSourceAuthorityError,
    _acquire_operation_lock,
    _bounded_clone_lock_reads,
    _insert_or_verify_pin,
    _operation_id,
    _pin_rows,
    _require_transaction,
)
from process.registry_ptg_allowed_plan_contract import AUTHORITY_CONTRACT, validated_identity
from process.registry_ptg_allowed_plan_source import load_allowed_plan_inventory
from process.registry_ptg_producer_scope import _digest


@dataclass(frozen=True)
class AllowedSourceAuthority(PtgPublishedResultSourceAuthority):
    @property
    def pin_reason(self):
        """Bind this retention reason to the exact allowed role and source."""
        return "retain published allowed source " + _digest(
            {"contract": AUTHORITY_CONTRACT, "operation_id": self.operation_id, "identity": dict(self.identity)}
        )

    def as_dict(self):
        """Serialize the distinct closed allowed authority receipt."""
        return {**super().as_dict(), "contract": AUTHORITY_CONTRACT}


def validated_allowed_authority(receipt):
    """Verify role identity and operation-owned pin without database access."""
    if type(receipt) is not dict or set(receipt) != {
        "contract",
        "operation_id",
        "snapshot_id",
        "source_key",
        "identity",
        "pin",
    }:
        raise PtgResultArchiveSourceAuthorityError("allowed source authority is invalid")
    try:
        authority = AllowedSourceAuthority(
            _operation_id(receipt["operation_id"]), validated_identity(receipt["identity"])
        )
        if receipt != authority.as_dict():
            raise ValueError
    except (ValueError, KeyError, TypeError) as exc:
        raise PtgResultArchiveSourceAuthorityError("allowed source authority changed") from exc
    return authority


async def prepare_allowed_source_authority(session, schema_name, snapshot_id, operation_id):
    """Fence source lifecycle and capture actual complete allowed lineage."""
    _require_transaction(session)
    operation_id = _operation_id(operation_id)
    await _acquire_operation_lock(session, operation_id)
    source_key = await _source_key_for_snapshot(session, schema_name, snapshot_id)
    await acquire_ptg2_source_lifecycle_lock(session, source_key=source_key)
    inventory = await load_allowed_plan_inventory(session, schema_name, snapshot_id)
    if inventory["identity"]["source_key"] != source_key:
        raise PtgResultArchiveSourceAuthorityError("allowed source ownership changed")
    authority = AllowedSourceAuthority(operation_id, inventory["identity"])
    return {**inventory, "authority": authority.as_dict()}


async def commit_allowed_source_authority(session, *, schema_name, authority):
    """Recheck exact source evidence before inserting the owned retention pin."""
    expected = validated_allowed_authority(authority)
    inventory = await prepare_allowed_source_authority(
        session, schema_name, expected.snapshot_id, expected.operation_id
    )
    if inventory["authority"] != authority:
        raise PtgResultArchiveSourceAuthorityError("allowed source changed after preparation")
    await _insert_or_verify_pin(session, schema=_identifier(schema_name), authority=expected)
    return expected


async def lock_allowed_source_authority(session, *, schema_name, authority):
    """Require unchanged allowed lineage and the original retained pin."""
    _require_transaction(session)
    expected = validated_allowed_authority(authority)
    async with _bounded_clone_lock_reads(session):
        inventory = await load_allowed_plan_inventory(session, schema_name, expected.snapshot_id)
        pins = await _pin_rows(session, schema=_identifier(schema_name), authority=expected)
    if inventory["identity"] != expected.identity or pins != [
        {"snapshot_id": expected.snapshot_id, "reason": expected.pin_reason}
    ]:
        raise PtgResultArchiveSourceAuthorityError("allowed source pin changed")
    return expected
