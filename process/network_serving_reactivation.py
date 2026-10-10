# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reactivate one verified retained generation without changing custom approval."""

from dataclasses import dataclass, field

import asyncpg

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_membership_publication import _lock_publication_controls
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest


class NetworkServingReactivationError(ValueError):
    """A retained target or expected control differs; the serving head is unchanged."""


@dataclass(frozen=True)
class NetworkServingReactivationReceipt:
    previous_generation_id: int
    generation_id: int
    approved_custom_revision: int
    manifest_sha256: str
    changed: bool
    component: str = field(default="network_serving_reactivation", init=False)
    revision: int = field(default=1, init=False)


def _revision(value, *, positive):
    if type(value) is not int or not int(positive) <= value <= 9223372036854775807:
        raise NetworkServingReactivationError("network_reactivation_revision_invalid")


async def _reactivate(connection, namespace, generation_id, expected_head, expected_approved_revision):
    approved_revision, current_head = await _lock_publication_controls(connection, namespace)
    if current_head != expected_head:
        raise NetworkServingReactivationError("network_reactivation_head_conflict")
    if approved_revision != expected_approved_revision:
        raise NetworkServingReactivationError("network_reactivation_approved_conflict")
    # Only low-volume retained metadata is locked; active serving heaps stay readable.
    retained = await connection.fetchrow(
        f"SELECT manifest.generation_id FROM {namespace}.network_serving_manifest manifest "
        f"JOIN {namespace}.network_membership_candidate candidate USING(candidate_id) "
        "WHERE manifest.generation_id=$1 AND manifest.eligible AND candidate.state='published' "
        "FOR SHARE OF candidate",
        generation_id,
    )
    if retained is None:
        raise NetworkServingReactivationError("network_reactivation_target_unavailable")
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation_id, control_schema=namespace[1:-1]
    )
    if manifest.approved_custom_revision != approved_revision:
        raise NetworkServingReactivationError("network_reactivation_target_approval_conflict")
    has_changed = current_head != generation_id
    if has_changed:
        status = await connection.execute(
            f"UPDATE {namespace}.network_serving_control SET generation_id=$1 WHERE id=1 AND generation_id=$2",
            generation_id,
            expected_head,
        )
        if status != "UPDATE 1":
            raise NetworkServingReactivationError("network_reactivation_head_conflict")
    return NetworkServingReactivationReceipt(
        current_head, generation_id, approved_revision, manifest.manifest_sha256, has_changed
    )


async def reactivate_network_serving_generation(
    connection, *, generation_id, expected_head, expected_approved_revision, control_schema=None
):
    """Switch only the serving head inside the authorized caller's transaction.

    The protected caller must authenticate and authorize this distinct operation
    and persist its actor, reason, immutable command and durable retry receipt.
    This engine grants no authority and cannot identify an ambiguous committed
    retry. Older custom approvals require recomposition, not head reactivation.
    """
    _revision(generation_id, positive=True)
    _revision(expected_head, positive=True)
    _revision(expected_approved_revision, positive=False)
    if not connection.is_in_transaction():
        raise NetworkServingReactivationError("network_reactivation_requires_transaction")
    try:
        namespace = _identifier(control_schema if control_schema is not None else registry_schema())
        async with connection.transaction():
            return await _reactivate(connection, namespace, generation_id, expected_head, expected_approved_revision)
    except NetworkServingReadUnavailable:
        raise NetworkServingReactivationError("network_reactivation_target_unavailable") from None
    except asyncpg.PostgresError:
        raise NetworkServingReactivationError("network_reactivation_storage_unavailable") from None
    except (ValueError, TypeError) as error:
        if isinstance(error, NetworkServingReactivationError):
            raise
        raise NetworkServingReactivationError("network_reactivation_controls_unavailable") from None
