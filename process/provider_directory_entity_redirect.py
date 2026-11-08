# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Apply explicitly authorized duplicate-identity reviews in the caller's transaction."""

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    ProviderDirectoryEntityRedirect,
    ProviderDirectoryEntityRedirectDecision,
    ProviderDirectoryEntityReleaseEvidence,
    ProviderDirectoryEntitySourceBinding,
)


@dataclass(frozen=True)
class ReviewedEntityRedirectDecision:
    """A supplied review receipt; its authorization remains the caller's responsibility."""

    decision_id: UUID
    action: str
    source_id: str
    resource_type: str
    old_entity_id: UUID
    canonical_entity_id: UUID
    old_resource_id: str
    canonical_resource_id: str
    old_release_id: str
    canonical_release_id: str
    old_payload_sha256: str
    canonical_payload_sha256: str
    prior_decision_id: UUID | None
    review_receipt_id: str
    review_receipt_sha256: str
    review_actor: str
    reviewed_at: datetime


def _decision_fields(decision: ReviewedEntityRedirectDecision) -> dict:
    if not isinstance(decision, ReviewedEntityRedirectDecision):
        raise ValueError("entity_redirect_decision_invalid")
    fields = asdict(decision)
    for name in ("decision_id", "old_entity_id", "canonical_entity_id"):
        if not isinstance(fields[name], UUID):
            raise ValueError("entity_redirect_identity_invalid")
    if decision.prior_decision_id is not None and not isinstance(decision.prior_decision_id, UUID):
        raise ValueError("entity_redirect_prior_invalid")
    if (
        decision.resource_type not in {"Organization", "Location"}
        or decision.old_entity_id == decision.canonical_entity_id
    ):
        raise ValueError("entity_redirect_kind_or_self_invalid")
    if decision.action not in {"redirect", "close"} or (
        (decision.prior_decision_id is None) != (decision.action == "redirect")
    ):
        raise ValueError("entity_redirect_action_invalid")
    for name, limit in (
        ("source_id", 64),
        ("old_resource_id", 256),
        ("canonical_resource_id", 256),
        ("old_release_id", 256),
        ("canonical_release_id", 256),
        ("review_receipt_id", 160),
        ("review_actor", 160),
    ):
        identity_text = fields[name]
        if (
            not isinstance(identity_text, str)
            or not identity_text
            or identity_text != identity_text.strip()
            or len(identity_text) > limit
        ):
            raise ValueError("entity_redirect_review_identity_invalid")
    for name in ("old_payload_sha256", "canonical_payload_sha256", "review_receipt_sha256"):
        if not isinstance(fields[name], str) or not re.fullmatch(r"[0-9a-f]{64}", fields[name]):
            raise ValueError("entity_redirect_hash_invalid")
    if not isinstance(decision.reviewed_at, datetime) or decision.reviewed_at.utcoffset() is None:
        raise ValueError("entity_redirect_reviewed_at_invalid")
    fields["reviewed_at"] = decision.reviewed_at.astimezone(timezone.utc)
    return fields


async def _require_evidence(session, fields):
    binding = ProviderDirectoryEntitySourceBinding.__table__
    evidence = ProviderDirectoryEntityReleaseEvidence.__table__
    id_column = "organization_id" if fields["resource_type"] == "Organization" else "site_id"
    for side in ("old", "canonical"):
        observed_hash = await session.scalar(
            select(evidence.c.payload_sha256)
            .join(
                binding,
                (binding.c.source_id == evidence.c.source_id)
                & (binding.c.resource_type == evidence.c.resource_type)
                & (binding.c.resource_id == evidence.c.resource_id),
            )
            .where(
                evidence.c.source_id == fields["source_id"],
                evidence.c.resource_type == fields["resource_type"],
                evidence.c.resource_id == fields[f"{side}_resource_id"],
                evidence.c.release_id == fields[f"{side}_release_id"],
                binding.c[id_column] == fields[f"{side}_entity_id"],
            )
            .with_for_update(read=True)
        )
        if observed_hash != fields[f"{side}_payload_sha256"]:
            raise ValueError("entity_redirect_exact_evidence_missing_or_changed")


async def _require_transition(session, fields):
    active = ProviderDirectoryEntityRedirect.__table__
    scope = (active.c.source_id == fields["source_id"]) & (active.c.resource_type == fields["resource_type"])
    current = (
        (await session.execute(select(active).where(scope, active.c.old_entity_id == fields["old_entity_id"])))
        .mappings()
        .first()
    )
    if fields["action"] == "close":
        if (
            current is None
            or current["decision_id"] != fields["prior_decision_id"]
            or (current["canonical_entity_id"] != fields["canonical_entity_id"])
        ):
            raise ValueError("entity_redirect_close_target_invalid")
    else:
        if current is not None:
            raise ValueError("entity_redirect_active_conflict")
        chained = await session.scalar(
            select(active.c.old_entity_id)
            .where(
                scope,
                or_(
                    active.c.old_entity_id == fields["canonical_entity_id"],
                    active.c.canonical_entity_id == fields["old_entity_id"],
                ),
            )
            .limit(1)
        )
        if chained is not None:
            raise ValueError("entity_redirect_one_hop_required")


async def record_reviewed_entity_redirect(session: AsyncSession, decision: ReviewedEntityRedirectDecision) -> None:
    """Append an authorized review and update its pointer atomically; never commit here.

    A closed redirect replay is a no-op. Retargeting requires closing the old
    decision and explicitly reviewing every replacement in the same transaction.
    """
    fields = _decision_fields(decision)
    if not session.in_transaction():
        raise ValueError("entity_redirect_requires_transaction")
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError("entity_redirect_requires_read_committed")
    # One source/kind lock and one-hop aliases; reviewed batches retarget canonical IDs.
    await session.execute(
        text("SELECT pg_catalog.pg_advisory_xact_lock(hashtextextended(:identity_key, 0))"),
        {"identity_key": f"entity-redirect:{decision.source_id}:{decision.resource_type}"},
    )
    decisions = ProviderDirectoryEntityRedirectDecision.__table__
    existing = (
        (await session.execute(select(decisions).where(decisions.c.decision_id == decision.decision_id)))
        .mappings()
        .first()
    )
    if existing is not None:
        if any(existing[name] != review_value for name, review_value in fields.items()):
            raise ValueError("entity_redirect_decision_replay_conflict")
        return
    await _require_evidence(session, fields)
    await _require_transition(session, fields)
    await session.execute(decisions.insert().values(**fields))
    active = ProviderDirectoryEntityRedirect.__table__
    if decision.action == "redirect":
        await session.execute(
            active.insert().values(
                source_id=decision.source_id,
                resource_type=decision.resource_type,
                old_entity_id=decision.old_entity_id,
                canonical_entity_id=decision.canonical_entity_id,
                decision_id=decision.decision_id,
                created_at=fields["reviewed_at"],
            )
        )
    else:
        await session.execute(active.delete().where(active.c.decision_id == decision.prior_decision_id))
