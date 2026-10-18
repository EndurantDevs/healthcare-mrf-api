# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind exact FHIR resource identities to stable organization or site IDs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
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
    if not isinstance(resources, list) or not resources or len(resources) > 1_000:
        raise ValueError("provider_directory_entity_batch_invalid")
    resource_type = _source_resource_identity(source_id, release_id, resources[0])[0]
    payloads_by_resource_id: dict[str, tuple[str, dict[str, Any]]] = {}
    ordered_resource_ids: list[str] = []
    input_bytes = 0
    for resource in resources:
        observed_type, resource_id = _source_resource_identity(source_id, release_id, resource)
        if observed_type != resource_type:
            raise ValueError("provider_directory_entity_batch_resource_type_mixed")
        payload_text = json.dumps(resource, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        input_bytes += len(payload_text.encode("utf-8"))
        if input_bytes > 32 * 1024 * 1024:
            raise ValueError("provider_directory_entity_batch_bytes_invalid")
        payload_sha256 = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        prior_payload = payloads_by_resource_id.get(resource_id)
        if prior_payload is not None and prior_payload[0] != payload_sha256:
            raise ValueError("provider_directory_entity_release_payload_conflict")
        payloads_by_resource_id[resource_id] = (payload_sha256, json.loads(payload_text))
        ordered_resource_ids.append(resource_id)
    return resource_type, ordered_resource_ids, payloads_by_resource_id


async def bind_entity_batch(
    session: AsyncSession,
    *,
    source_id: str,
    release_id: str,
    resources: list[Mapping[str, Any]],
) -> list[UUID]:
    """Observe one bounded batch in the protected unpublished source candidate."""
    from process.provider_directory_cms_identity_candidate import stage_entity_batch

    resource_type, ordered_resource_ids, payloads = _validated_entity_batch(source_id, release_id, resources)
    ids = await stage_entity_batch(session, source_id, release_id, resource_type, payloads)
    return [ids[resource_id] for resource_id in ordered_resource_ids]


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
    """Read a verified prepared map for low-volume compatibility; never mint IDs."""
    if not isinstance(org_pac_ids, list) or not org_pac_ids or len(org_pac_ids) > 100:
        raise ValueError("cms_doctors_group_batch_invalid")
    ordered_ids = [_validated_org_pac_id(org_pac_id) for org_pac_id in org_pac_ids]
    from process.cms_doctors_identity_candidate import prepared_doctors_ids

    return await prepared_doctors_ids(session, "group", ordered_ids)


async def bind_cms_doctors_group(session: AsyncSession, *, org_pac_id: str) -> UUID:
    """Bind one nonblank source PAC ID without inferring a FHIR match."""
    return (await bind_cms_doctors_group_batch(session, org_pac_ids=[org_pac_id]))[0]


def _validated_adrs_id(adrs_id: str) -> str:
    if not isinstance(adrs_id, str) or not adrs_id or adrs_id != adrs_id.strip() or len(adrs_id) > 256:
        raise ValueError("cms_doctors_site_adrs_id_invalid")
    return adrs_id


async def bind_cms_doctors_site_batch(session: AsyncSession, *, adrs_ids: list[str]) -> list[UUID]:
    """Read exact source site keys only from an already prepared private map."""
    if not isinstance(adrs_ids, list) or not adrs_ids or len(adrs_ids) > 100:
        raise ValueError("cms_doctors_site_batch_invalid")
    ordered_ids = [_validated_adrs_id(adrs_id) for adrs_id in adrs_ids]
    from process.cms_doctors_identity_candidate import prepared_doctors_ids

    return await prepared_doctors_ids(session, "site", ordered_ids)


async def bind_cms_doctors_site(session: AsyncSession, *, adrs_id: str) -> UUID:
    """Bind one complete, nonblank source address ID."""
    return (await bind_cms_doctors_site_batch(session, adrs_ids=[adrs_id]))[0]
