# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Apply explicit, reviewed CMS Organization links to existing payer IDs."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    MRFPayer,
    ProviderDirectoryEntityReleaseEvidence,
    ProviderDirectoryMRFPayerBinding,
    ProviderDirectoryMRFPayerReviewDecision,
)


@dataclass(frozen=True)
class ReviewedCMSPayerDecision:
    """One externally authorized insurer-company decision and review receipt."""

    decision_id: UUID
    action: str
    source_id: str
    resource_id: str
    release_id: str
    source_payload_sha256: str
    payer_id: str
    prior_decision_id: UUID | None
    review_receipt_id: str
    review_receipt_sha256: str
    review_actor: str
    reviewed_at: datetime


def _decision_fields(review_decision: ReviewedCMSPayerDecision) -> dict:
    if not isinstance(review_decision, ReviewedCMSPayerDecision):
        raise ValueError("provider_directory_mrf_payer_decision_invalid")
    if not isinstance(review_decision.decision_id, UUID) or (
        review_decision.prior_decision_id is not None and not isinstance(review_decision.prior_decision_id, UUID)
    ):
        raise ValueError("provider_directory_mrf_payer_decision_id_invalid")
    if review_decision.action not in {"bind", "close"} or (
        (review_decision.prior_decision_id is None) != (review_decision.action == "bind")
    ):
        raise ValueError("provider_directory_mrf_payer_action_invalid")
    if review_decision.source_id != "cms-npd":
        raise ValueError("provider_directory_mrf_payer_source_invalid")
    for field_content, limit in (
        (review_decision.resource_id, 256),
        (review_decision.release_id, 256),
        (review_decision.payer_id, 64),
        (review_decision.review_receipt_id, 160),
        (review_decision.review_actor, 160),
    ):
        if (
            not isinstance(field_content, str)
            or not field_content
            or (field_content != field_content.strip() or len(field_content) > limit)
        ):
            raise ValueError("provider_directory_mrf_payer_identity_invalid")
    if any(
        not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
        for digest in (review_decision.review_receipt_sha256, review_decision.source_payload_sha256)
    ):
        raise ValueError("provider_directory_mrf_payer_receipt_digest_invalid")
    reviewed_at = review_decision.reviewed_at
    if not isinstance(reviewed_at, datetime) or reviewed_at.tzinfo is None or reviewed_at.utcoffset() is None:
        raise ValueError("provider_directory_mrf_payer_reviewed_at_invalid")
    return asdict(review_decision) | {
        "resource_type": "Organization",
        "reviewed_at": reviewed_at.astimezone(timezone.utc),
    }


async def _require_exact_parents(session: AsyncSession, decision_fields: dict) -> None:
    evidence = ProviderDirectoryEntityReleaseEvidence.__table__
    source_payload_sha256 = await session.scalar(
        select(evidence.c.payload_sha256)
        .where(
            evidence.c.source_id == decision_fields["source_id"],
            evidence.c.resource_type == "Organization",
            evidence.c.resource_id == decision_fields["resource_id"],
            evidence.c.release_id == decision_fields["release_id"],
        )
        .with_for_update(read=True)
    )
    if source_payload_sha256 is None:
        raise ValueError("provider_directory_mrf_payer_release_evidence_missing")
    if source_payload_sha256 != decision_fields["source_payload_sha256"]:
        raise ValueError("provider_directory_mrf_payer_source_payload_conflict")
    payer_exists = await session.scalar(
        select(MRFPayer.payer_id).where(MRFPayer.payer_id == decision_fields["payer_id"])
    )
    if payer_exists is None:
        raise ValueError("provider_directory_mrf_payer_identity_missing")


async def _write_review_transition(
    session: AsyncSession,
    review_decision: ReviewedCMSPayerDecision,
    decision_fields: dict,
) -> None:
    decisions = ProviderDirectoryMRFPayerReviewDecision.__table__
    binding = ProviderDirectoryMRFPayerBinding.__table__
    await session.execute(decisions.insert().values(**decision_fields))
    if review_decision.action == "bind":
        await session.execute(
            binding.insert().values(
                source_id=review_decision.source_id,
                resource_type="Organization",
                resource_id=review_decision.resource_id,
                payer_id=review_decision.payer_id,
                binding_decision_id=review_decision.decision_id,
                created_at=decision_fields["reviewed_at"],
            )
        )
    else:
        await session.execute(
            binding.delete().where(
                binding.c.source_id == review_decision.source_id,
                binding.c.resource_id == review_decision.resource_id,
            )
        )


async def record_reviewed_cms_payer_decision(session: AsyncSession, review_decision: ReviewedCMSPayerDecision) -> None:
    """Record a supplied company review; caller must authorize its receipt.

    The operation runs inside the caller's transaction. Replaying a closed bind
    decision is a no-op, never a reactivation.
    """
    decision_fields = _decision_fields(review_decision)
    source_id = review_decision.source_id
    resource_id = review_decision.resource_id
    await session.execute(
        text("SELECT pg_catalog.pg_advisory_xact_lock(hashtextextended(:identity_key, 0))"),
        {"identity_key": f"provider-directory-mrf-payer:{source_id}:{resource_id}"},
    )
    decisions = ProviderDirectoryMRFPayerReviewDecision.__table__
    binding = ProviderDirectoryMRFPayerBinding.__table__
    existing = (
        (await session.execute(select(decisions).where(decisions.c.decision_id == review_decision.decision_id)))
        .mappings()
        .first()
    )
    if existing is not None:
        if any(existing[key] != field_content for key, field_content in decision_fields.items()):
            raise ValueError("provider_directory_mrf_payer_decision_replay_conflict")
        return
    await _require_exact_parents(session, decision_fields)
    active = (
        (
            await session.execute(
                select(binding).where(
                    binding.c.source_id == source_id,
                    binding.c.resource_id == resource_id,
                )
            )
        )
        .mappings()
        .first()
    )
    if review_decision.action == "bind":
        if active is not None:
            raise ValueError("provider_directory_mrf_payer_active_binding_conflict")
    elif (
        active is None
        or active["binding_decision_id"] != review_decision.prior_decision_id
        or active["payer_id"] != review_decision.payer_id
    ):
        raise ValueError("provider_directory_mrf_payer_close_target_invalid")
    await _write_review_transition(session, review_decision, decision_fields)
