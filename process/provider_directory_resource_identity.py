# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind validated source resource IDs in bounded, caller-owned transactions."""

import json
import re
from uuid import UUID, uuid5

# A permanent public namespace, unrelated to operators, deployments or releases.
RESOURCE_ID_NAMESPACE = UUID("f923f6f9-55b3-5c1f-8308-ed036b2f223e")


def source_resource_uuid(source_id, resource_type, resource_id):
    """Name only exact source/type/resource identity; never classify or infer a join."""
    if (
        not isinstance(source_id, str)
        or re.fullmatch(r"[a-z0-9_-]{1,64}", source_id) is None
        or resource_type not in {"InsurancePlan", "PractitionerRole"}
        or not isinstance(resource_id, str)
        or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", resource_id) is None
    ):
        raise ValueError("provider_directory_resource_identity_invalid")
    return uuid5(RESOURCE_ID_NAMESPACE, json.dumps([source_id, resource_type, resource_id], separators=(",", ":")))


async def bind_resource_identity_batch(session, *, source_id, resource_type, resource_ids):
    """Stage at most 1,000 validated IDs for the authenticated source candidate.

    Intake never changes accepted identities. The complete candidate is reduced,
    indexed and checked before its atomic publication; UUID5 inputs stay exact.
    """
    from process.provider_directory_resource_identity_candidate import stage_resource_identities

    if not isinstance(resource_ids, list) or not resource_ids or len(resource_ids) > 1_000:
        raise ValueError("provider_directory_resource_identity_batch_invalid")
    identities_by_resource_id = {
        resource_id: source_resource_uuid(source_id, resource_type, resource_id) for resource_id in resource_ids
    }
    await stage_resource_identities(
        session,
        source_id=source_id,
        resource_type=resource_type,
        identities_by_resource_id=identities_by_resource_id,
    )
    return [identities_by_resource_id[resource_id] for resource_id in resource_ids]
