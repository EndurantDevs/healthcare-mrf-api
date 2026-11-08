# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record exact FHIR network roles without merging payer or TiC identities."""

from __future__ import annotations

import re
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

_FHIR_ID = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")


def _identity(value: str, limit: int) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > limit:
        raise ValueError("provider_directory_insurance_network_identity_invalid")
    return value


def _plan_network_refs(plan: Mapping[str, Any]) -> list[str]:
    """Keep only explicit FHIR network references."""
    items = [plan.get("network")]
    for field in ("plan", "coverage"):
        for entry in plan.get(field) or []:
            if isinstance(entry, Mapping):
                items.append(entry.get("network"))
    refs: list[str] = []
    for item in items:
        for reference in item if isinstance(item, list) else [item]:
            if isinstance(reference, Mapping) and isinstance(reference.get("reference"), str):
                refs.append(reference["reference"])
    return list(dict.fromkeys(refs))


def _payer_ref(plan: Mapping[str, Any], key: str) -> str | None:
    value = plan.get(key)
    return value.get("reference") if isinstance(value, Mapping) and isinstance(value.get("reference"), str) else None


def has_source_declared_network_role(organization: Mapping[str, Any]) -> bool:
    """Accept only an explicit FHIR Organization type, including CMS text-only types."""
    types = organization.get("type")
    return isinstance(types, list) and any(
        isinstance(entry, Mapping)
        and (
            entry.get("text") == "ntwk"
            or isinstance(entry.get("coding"), list)
            and any(isinstance(coding, Mapping) and coding.get("code") == "ntwk" for coding in entry["coding"])
        )
        for entry in types
    )


async def record_insurance_network_organization(
    session: AsyncSession, *, source_id: str, release_id: str, organization: Mapping[str, Any]
) -> UUID:
    """Bind a source-declared network Organization without requiring a plan."""
    source_id = _identity(source_id, 64)
    release_id = _identity(release_id, 256)
    if not isinstance(organization, Mapping) or organization.get("resourceType") != "Organization":
        raise ValueError("provider_directory_insurance_network_organization_invalid")
    network_resource_id = _identity(organization.get("id"), 256)
    if not _FHIR_ID.fullmatch(network_resource_id) or not has_source_declared_network_role(organization):
        raise ValueError("provider_directory_insurance_network_role_missing")
    from process.provider_directory_insurance_network_batch import record_insurance_network_batch

    await record_insurance_network_batch(
        session, source_id=source_id, release_id=release_id, resource_type="Organization", resources=[organization]
    )
    return await _staged_network_id(session, source_id, network_resource_id)


async def record_insurance_network_plan(
    session: AsyncSession,
    *,
    source_id: str,
    release_id: str,
    network_resource_id: str,
    plan: Mapping[str, Any],
) -> UUID:
    """Bind an explicit plan network without payer, name, or checksum inference.

    The Organization must already have an exact entity binding. The caller owns commit.
    """
    source_id = _identity(source_id, 64)
    release_id = _identity(release_id, 256)
    network_resource_id = _identity(network_resource_id, 256)
    if not isinstance(plan, Mapping) or plan.get("resourceType") != "InsurancePlan":
        raise ValueError("provider_directory_insurance_network_plan_invalid")
    _identity(plan.get("id"), 256)
    if not _FHIR_ID.fullmatch(network_resource_id):
        raise ValueError("provider_directory_insurance_network_resource_id_invalid")
    network_refs = _plan_network_refs(plan)
    # An absolute URL cannot establish source equivalence without an endpoint binding.
    if f"Organization/{network_resource_id}" not in network_refs:
        raise ValueError("provider_directory_insurance_network_ref_missing")
    from process.provider_directory_insurance_network_batch import _record_network_batch

    await _record_network_batch(
        session,
        source_id=source_id,
        release_id=release_id,
        resource_type="InsurancePlan",
        resources=[plan],
        network_resource_id=network_resource_id,
    )
    return await _staged_network_id(session, source_id, network_resource_id)


async def _staged_network_id(session, source_id, resource_id):
    from db.plan_release_storage import storage_relation
    from process.provider_directory_cms_identity_candidate import CMS_IDENTITY_TABLES, identity_candidate

    publisher, _, receipts = await identity_candidate(session)
    relation = storage_relation(publisher.policy.schema, receipts[CMS_IDENTITY_TABLES[2]]["name"])
    value = await publisher.connection.fetchval(
        f"SELECT network_id FROM {relation} WHERE source_id=$1 AND resource_id=$2", source_id, resource_id
    )
    if value is None:
        raise ValueError("provider_directory_insurance_network_release_evidence_missing")
    return value
