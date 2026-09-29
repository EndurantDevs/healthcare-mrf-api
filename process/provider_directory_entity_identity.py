# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind exact FHIR resource identities to stable organization or site IDs."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    ProviderDirectoryCMSDoctorsGroupBinding,
    ProviderDirectoryEntityReleaseEvidence,
    ProviderDirectoryEntitySourceBinding,
    ProviderDirectoryOrganizationIdentity,
    ProviderDirectorySiteIdentity,
)

_IDENTITY_BY_RESOURCE_TYPE = {
    "Organization": (ProviderDirectoryOrganizationIdentity, "organization_id"),
    "Location": (ProviderDirectorySiteIdentity, "site_id"),
}


def _source_resource_identity(source_id: str, release_id: str, resource: Mapping[str, Any]) -> tuple[str, str]:
    if not isinstance(resource, Mapping):
        raise ValueError("provider_directory_entity_resource_invalid")
    resource_type = resource.get("resourceType")
    resource_id = resource.get("id")
    if not isinstance(resource_type, str) or resource_type not in _IDENTITY_BY_RESOURCE_TYPE:
        raise ValueError("provider_directory_entity_resource_type_invalid")
    for value, limit in ((source_id, 64), (release_id, 256), (resource_id, 256)):
        if not isinstance(value, str) or not value or value != value.strip() or len(value) > limit:
            raise ValueError("provider_directory_entity_source_identity_invalid")
    return resource_type, resource_id


def _validated_entity_batch(
    source_id: str,
    release_id: str,
    resources: list[Mapping[str, Any]],
) -> tuple[str, list[str], dict[str, tuple[str, dict[str, Any]]]]:
    if not isinstance(resources, list) or not resources or len(resources) > 100:
        raise ValueError("provider_directory_entity_batch_invalid")
    resource_type = _source_resource_identity(source_id, release_id, resources[0])[0]
    payloads_by_resource_id: dict[str, tuple[str, dict[str, Any]]] = {}
    ordered_resource_ids: list[str] = []
    for resource in resources:
        observed_type, resource_id = _source_resource_identity(source_id, release_id, resource)
        if observed_type != resource_type:
            raise ValueError("provider_directory_entity_batch_resource_type_mixed")
        payload_text = json.dumps(resource, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        payload_sha256 = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        prior_payload = payloads_by_resource_id.get(resource_id)
        if prior_payload is not None and prior_payload[0] != payload_sha256:
            raise ValueError("provider_directory_entity_release_payload_conflict")
        payloads_by_resource_id[resource_id] = (payload_sha256, json.loads(payload_text))
        ordered_resource_ids.append(resource_id)
    return resource_type, ordered_resource_ids, payloads_by_resource_id


async def _read_entity_batch(
    session: AsyncSession,
    source_id: str,
    release_id: str,
    resource_type: str,
    payloads_by_resource_id: dict[str, tuple[str, dict[str, Any]]],
) -> tuple[dict[str, UUID], set[str]]:
    resource_ids = list(payloads_by_resource_id)
    binding = ProviderDirectoryEntitySourceBinding.__table__
    evidence = ProviderDirectoryEntityReleaseEvidence.__table__
    id_column = _IDENTITY_BY_RESOURCE_TYPE[resource_type][1]
    # ponytail: this source/type lock serializes batches; shard only if measured throughput needs it.
    await session.execute(
        text("SELECT pg_catalog.pg_advisory_xact_lock(hashtextextended(:identity_key, 0))"),
        {"identity_key": f"provider-directory-entity-batch:{source_id}:{resource_type}"},
    )
    binding_rows = (
        await session.execute(
            select(binding.c.resource_id, binding.c[id_column]).where(
                binding.c.source_id == source_id,
                binding.c.resource_type == resource_type,
                binding.c.resource_id.in_(resource_ids),
            )
        )
    ).all()
    entity_ids_by_resource_id = {resource_id: entity_id for resource_id, entity_id in binding_rows}
    evidence_rows = (
        await session.execute(
            select(evidence.c.resource_id, evidence.c.payload_sha256).where(
                evidence.c.source_id == source_id,
                evidence.c.resource_type == resource_type,
                evidence.c.release_id == release_id,
                evidence.c.resource_id.in_(resource_ids),
            )
        )
    ).all()
    for resource_id, prior_hash in evidence_rows:
        if prior_hash != payloads_by_resource_id[resource_id][0]:
            raise ValueError("provider_directory_entity_release_payload_conflict")
    return entity_ids_by_resource_id, {resource_id for resource_id, _ in evidence_rows}


async def _insert_new_entity_bindings(
    session: AsyncSession,
    source_id: str,
    resource_type: str,
    payloads_by_resource_id: dict[str, tuple[str, dict[str, Any]]],
    existing_ids_by_resource_id: dict[str, UUID],
    observed_at: datetime,
) -> dict[str, UUID]:
    """Create only missing IDs and their exact source bindings."""
    identity_model, id_column = _IDENTITY_BY_RESOURCE_TYPE[resource_type]
    new_ids_by_resource_id = {
        resource_id: uuid4()
        for resource_id in payloads_by_resource_id
        if resource_id not in existing_ids_by_resource_id
    }
    if new_ids_by_resource_id:
        await session.execute(
            identity_model.__table__.insert().values(
                [{id_column: entity_id, "created_at": observed_at} for entity_id in new_ids_by_resource_id.values()]
            )
        )
        await session.execute(
            ProviderDirectoryEntitySourceBinding.__table__.insert().values(
                [
                    {
                        "source_id": source_id,
                        "resource_type": resource_type,
                        "resource_id": resource_id,
                        id_column: entity_id,
                        "created_at": observed_at,
                    }
                    for resource_id, entity_id in new_ids_by_resource_id.items()
                ]
            )
        )
    return new_ids_by_resource_id


async def bind_entity_batch(
    session: AsyncSession,
    *,
    source_id: str,
    release_id: str,
    resources: list[Mapping[str, Any]],
) -> list[UUID]:
    """Bind up to 100 same-type resources and exact release facts in one transaction."""
    resource_type, ordered_resource_ids, payloads_by_resource_id = _validated_entity_batch(
        source_id,
        release_id,
        resources,
    )
    entity_ids_by_resource_id, observed_resource_ids = await _read_entity_batch(
        session,
        source_id,
        release_id,
        resource_type,
        payloads_by_resource_id,
    )
    observed_at = datetime.now(timezone.utc)
    entity_ids_by_resource_id.update(
        await _insert_new_entity_bindings(
            session,
            source_id,
            resource_type,
            payloads_by_resource_id,
            entity_ids_by_resource_id,
            observed_at,
        )
    )
    new_evidence_rows = [
        {
            "source_id": source_id,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "release_id": release_id,
            "payload_sha256": payload_by_field[0],
            "payload_json": payload_by_field[1],
            "observed_at": observed_at,
        }
        for resource_id, payload_by_field in payloads_by_resource_id.items()
        if resource_id not in observed_resource_ids
    ]
    if new_evidence_rows:
        await session.execute(ProviderDirectoryEntityReleaseEvidence.__table__.insert().values(new_evidence_rows))
    return [entity_ids_by_resource_id[resource_id] for resource_id in ordered_resource_ids]


async def bind_entity_resource(
    session: AsyncSession,
    *,
    source_id: str,
    release_id: str,
    resource: Mapping[str, Any],
) -> UUID:
    """Observe one exact FHIR resource without merging by name, NPI or address."""
    return (
        await bind_entity_batch(
            session,
            source_id=source_id,
            release_id=release_id,
            resources=[resource],
        )
    )[0]


def _validated_org_pac_id(org_pac_id: str) -> str:
    if not isinstance(org_pac_id, str) or not org_pac_id or org_pac_id != org_pac_id.strip() or len(org_pac_id) > 64:
        raise ValueError("cms_doctors_group_org_pac_id_invalid")
    return org_pac_id


async def bind_cms_doctors_group_batch(session: AsyncSession, *, org_pac_ids: list[str]) -> list[UUID]:
    """Bind up to 100 exact CMS Doctors PAC IDs without FHIR cross-links."""
    if not isinstance(org_pac_ids, list) or not org_pac_ids or len(org_pac_ids) > 100:
        raise ValueError("cms_doctors_group_batch_invalid")
    ordered_org_pac_ids = [_validated_org_pac_id(org_pac_id) for org_pac_id in org_pac_ids]
    unique_org_pac_ids = list(dict.fromkeys(ordered_org_pac_ids))
    binding = ProviderDirectoryCMSDoctorsGroupBinding.__table__
    # ponytail: one CMS Doctors lock serializes batches; shard only if measured throughput needs it.
    await session.execute(
        text("SELECT pg_catalog.pg_advisory_xact_lock(hashtextextended(:identity_key, 0))"),
        {"identity_key": "provider-directory-cms-doctors-group-batch"},
    )
    binding_rows = (
        await session.execute(
            select(binding.c.org_pac_id, binding.c.organization_id).where(binding.c.org_pac_id.in_(unique_org_pac_ids))
        )
    ).all()
    organization_ids_by_pac_id = {pac_id: organization_id for pac_id, organization_id in binding_rows}
    new_organization_ids_by_pac_id = {
        pac_id: uuid4() for pac_id in unique_org_pac_ids if pac_id not in organization_ids_by_pac_id
    }
    created_at = datetime.now(timezone.utc)
    if new_organization_ids_by_pac_id:
        await session.execute(
            ProviderDirectoryOrganizationIdentity.__table__.insert().values(
                [
                    {"organization_id": organization_id, "created_at": created_at}
                    for organization_id in new_organization_ids_by_pac_id.values()
                ]
            )
        )
        await session.execute(
            binding.insert().values(
                [
                    {"org_pac_id": pac_id, "organization_id": organization_id, "created_at": created_at}
                    for pac_id, organization_id in new_organization_ids_by_pac_id.items()
                ]
            )
        )
    organization_ids_by_pac_id.update(new_organization_ids_by_pac_id)
    return [organization_ids_by_pac_id[pac_id] for pac_id in ordered_org_pac_ids]


async def bind_cms_doctors_group(session: AsyncSession, *, org_pac_id: str) -> UUID:
    """Bind one nonblank source PAC ID without inferring a FHIR match."""
    return (await bind_cms_doctors_group_batch(session, org_pac_ids=[org_pac_id]))[0]
