# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record exact FHIR network roles without merging payer or TiC identities."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    ProviderDirectoryEntityReleaseEvidence,
    ProviderDirectoryInsuranceNetworkIdentity,
    ProviderDirectoryInsuranceNetworkPlanEvidence,
    ProviderDirectoryInsuranceNetworkSourceBinding,
)

_FHIR_ID = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")


def _identity(value: str, limit: int) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > limit:
        raise ValueError("provider_directory_insurance_network_identity_invalid")
    return value


def _plan_network_refs(plan: Mapping[str, Any]) -> list[str]:
    """Keep only explicit FHIR references, including nested plan networks."""
    items = [plan.get("network")]
    for entry in plan.get("plan") or []:
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


async def _bind_network(session: AsyncSession, source_id: str, network_resource_id: str, observed_at: datetime) -> UUID:
    """Create one ID under the exact source/resource transaction lock."""
    binding = ProviderDirectoryInsuranceNetworkSourceBinding.__table__
    identity = ProviderDirectoryInsuranceNetworkIdentity.__table__
    await session.execute(
        text("SELECT pg_catalog.pg_advisory_xact_lock(hashtextextended(:identity_key, 0))"),
        {"identity_key": f"provider-directory-insurance-network:{source_id}:{network_resource_id}"},
    )
    network_id = (
        await session.execute(
            select(binding.c.network_id).where(
                binding.c.source_id == source_id,
                binding.c.resource_id == network_resource_id,
            )
        )
    ).scalar_one_or_none()
    if network_id is None:
        # The foreign key is the final guard; this check fails before making an ID.
        schema = binding.schema.replace('"', '""')
        organization_exists = (
            await session.execute(
                text(
                    f'SELECT 1 FROM "{schema}".provider_directory_entity_source_binding '
                    "WHERE source_id = :source_id AND resource_type = 'Organization' "
                    "AND resource_id = :resource_id FOR KEY SHARE"
                ),
                {"source_id": source_id, "resource_id": network_resource_id},
            )
        ).scalar_one_or_none()
        if organization_exists is None:
            raise ValueError("provider_directory_insurance_network_organization_binding_missing")
        network_id = uuid4()
        await session.execute(identity.insert().values(network_id=network_id, created_at=observed_at))
        await session.execute(
            binding.insert().values(
                source_id=source_id,
                resource_type="Organization",
                resource_id=network_resource_id,
                network_id=network_id,
                created_at=observed_at,
            )
        )
    return network_id


async def _existing_plan_networks(session: AsyncSession, evidence_values: dict[str, Any]) -> set[str]:
    """Reject conflicting plan facts before a network identity can be inserted."""
    evidence = ProviderDirectoryInsuranceNetworkPlanEvidence.__table__
    existing_network_hashes = (
        await session.execute(
            select(evidence.c.network_resource_id, evidence.c.plan_payload_sha256).where(
                evidence.c.source_id == evidence_values["source_id"],
                evidence.c.release_id == evidence_values["release_id"],
                evidence.c.insurance_plan_resource_id == evidence_values["insurance_plan_resource_id"],
            )
        )
    ).all()
    if any(plan_hash != evidence_values["plan_payload_sha256"] for _, plan_hash in existing_network_hashes):
        raise ValueError("provider_directory_insurance_network_release_plan_conflict")
    return {network_id for network_id, _ in existing_network_hashes}


async def _require_release_evidence(
    session: AsyncSession, source_id: str, network_resource_id: str, release_id: str
) -> None:
    evidence = ProviderDirectoryEntityReleaseEvidence.__table__
    resource_id = await session.scalar(
        select(evidence.c.resource_id)
        .where(
            evidence.c.source_id == source_id,
            evidence.c.resource_type == "Organization",
            evidence.c.resource_id == network_resource_id,
            evidence.c.release_id == release_id,
        )
        .with_for_update(read=True, key_share=True)
    )
    if resource_id is None:
        raise ValueError("provider_directory_insurance_network_release_evidence_missing")


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
    plan_id = _identity(plan.get("id"), 256)
    if not _FHIR_ID.fullmatch(network_resource_id):
        raise ValueError("provider_directory_insurance_network_resource_id_invalid")
    network_refs = _plan_network_refs(plan)
    # An absolute URL cannot establish source equivalence without an endpoint binding.
    if f"Organization/{network_resource_id}" not in network_refs:
        raise ValueError("provider_directory_insurance_network_ref_missing")
    plan_payload_text = json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    observed_at = datetime.now(timezone.utc)
    await session.execute(
        text("SELECT pg_catalog.pg_advisory_xact_lock(hashtextextended(:identity_key, 0))"),
        {"identity_key": f"provider-directory-insurance-plan:{source_id}:{release_id}:{plan_id}"},
    )
    evidence_dict = {
        "source_id": source_id,
        "release_id": release_id,
        "network_resource_type": "Organization",
        "network_resource_id": network_resource_id,
        "insurance_plan_resource_id": plan_id,
        "network_refs": network_refs,
        "owned_by_ref": _payer_ref(plan, "ownedBy"),
        "administered_by_ref": _payer_ref(plan, "administeredBy"),
        "plan_payload_sha256": hashlib.sha256(plan_payload_text.encode("utf-8")).hexdigest(),
        "plan_payload_json": json.loads(plan_payload_text),
        "observed_at": observed_at,
    }
    existing_networks = await _existing_plan_networks(session, evidence_dict)
    await _require_release_evidence(session, source_id, network_resource_id, release_id)
    network_id = await _bind_network(session, source_id, network_resource_id, observed_at)
    if network_resource_id not in existing_networks:
        await session.execute(ProviderDirectoryInsuranceNetworkPlanEvidence.__table__.insert().values(**evidence_dict))
    return network_id
