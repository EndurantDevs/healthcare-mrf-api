# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind validated source resource IDs in bounded, caller-owned transactions."""

import json
import re
from datetime import datetime, timezone
from uuid import UUID, uuid5

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from db.models.provider_directory_resource_identity import ProviderDirectoryResourceIdentity

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
    """Bind at most 100 already-validated IDs; caller commits each batch before cutover.

    This stores identity only. Admission must first reject conflicting duplicate
    resource payloads and must verify complete accepted-dataset binding coverage.
    It grants no publication authority, and replay cannot replace an existing ID.
    """
    if not isinstance(resource_ids, list) or not resource_ids or len(resource_ids) > 100:
        raise ValueError("provider_directory_resource_identity_batch_invalid")
    identities_by_resource_id = {
        resource_id: source_resource_uuid(source_id, resource_type, resource_id) for resource_id in resource_ids
    }
    table = ProviderDirectoryResourceIdentity.__table__
    await session.execute(
        insert(table)
        .values(
            [
                {
                    "source_id": source_id,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "entity_id": entity_id,
                    "created_at": datetime.now(timezone.utc),
                }
                for resource_id, entity_id in identities_by_resource_id.items()
            ]
        )
        .on_conflict_do_nothing(index_elements=["source_id", "resource_type", "resource_id"])
    )
    stored_by_resource_id = dict(
        (
            await session.execute(
                select(table.c.resource_id, table.c.entity_id).where(
                    table.c.source_id == source_id,
                    table.c.resource_type == resource_type,
                    table.c.resource_id.in_(identities_by_resource_id),
                )
            )
        ).all()
    )
    if stored_by_resource_id != identities_by_resource_id:
        raise ValueError("provider_directory_resource_identity_conflict")
    return [identities_by_resource_id[resource_id] for resource_id in resource_ids]
