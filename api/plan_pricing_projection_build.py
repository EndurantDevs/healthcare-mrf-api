# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional builder for immutable plan-pricing projections."""

from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Mapping

from sqlalchemy import text

from api.plan_pricing_projection_contract import (
    HEX_DIGEST,
    PROJECTION_CONTRACT,
    canonical_json,
    lock_provider_generation,
    normalized_bindings,
    projection_id,
    provider_signature,
    table,
)
from api.plan_pricing_projection_materialize import project_code
from api.plan_pricing_projection_source import binding_projection
from db.connection import db


def receipt(candidate_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Return the stable public receipt for a ready candidate."""

    return {
        "contract": PROJECTION_CONTRACT,
        "projection_id": str(candidate_by_field["projection_id"]),
        "binding_manifest_digest": str(
            candidate_by_field["binding_manifest_digest"]
        ),
        "provider_signature": str(candidate_by_field["provider_signature"]),
        "content_digest": str(candidate_by_field["content_digest"]),
        "card_row_count": int(candidate_by_field["card_row_count"]),
        "aggregate_row_count": int(candidate_by_field["aggregate_row_count"]),
        "fragment_byte_count": int(candidate_by_field["fragment_byte_count"]),
        "build_seconds": float(candidate_by_field["build_seconds"]),
        "state": "ready",
    }


async def _existing_candidate_receipt(
    session: Any,
    candidate_id: str,
    binding_manifest: list[dict[str, Any]],
    binding_manifest_digest: str,
    provider_generation_signature: str,
) -> dict[str, Any] | None:
    existing_result = await session.execute(
        text(
            f"""
            SELECT *
              FROM {table('plan_pricing_projection_candidate')}
             WHERE projection_id = :projection_id
            """
        ),
        {"projection_id": candidate_id},
    )
    existing_candidate = existing_result.mappings().one_or_none()
    if existing_candidate is None:
        return None
    if existing_candidate.get("state") == "ready":
        has_matching_identity = (
            existing_candidate.get("binding_manifest") == binding_manifest
            and existing_candidate.get("binding_manifest_digest")
            == binding_manifest_digest
            and existing_candidate.get("provider_signature")
            == provider_generation_signature
        )
        if not has_matching_identity:
            raise ValueError("pricing projection identity collision")
        existing_receipt = receipt(existing_candidate)
        return existing_receipt
    await session.execute(
        text(
            f"""
            DELETE FROM {table('plan_pricing_projection_candidate')}
             WHERE projection_id = :projection_id
            """
        ),
        {"projection_id": candidate_id},
    )
    return None


async def _insert_candidate(
    session: Any,
    candidate_id: str,
    binding_manifest: list[dict[str, Any]],
    binding_manifest_digest: str,
    provider_generation_signature: str,
) -> None:
    await session.execute(
        text(
            f"""
            INSERT INTO {table('plan_pricing_projection_candidate')} (
                projection_id, contract_version, binding_manifest_digest,
                binding_manifest, provider_signature, state
            ) VALUES (
                :projection_id, :contract_version, :binding_manifest_digest,
                CAST(:binding_manifest AS jsonb), :provider_signature, 'building'
            )
            """
        ),
        {
            "projection_id": candidate_id,
            "contract_version": PROJECTION_CONTRACT,
            "binding_manifest_digest": binding_manifest_digest,
            "binding_manifest": canonical_json(binding_manifest),
            "provider_signature": provider_generation_signature,
        },
    )


async def _materialize_all_codes(
    session: Any,
    candidate_id: str,
    binding_manifest: list[dict[str, Any]],
) -> tuple[Any, int, int, int]:
    in_network_bindings = [
        binding_by_field
        for binding_by_field in binding_manifest
        if str(binding_by_field.get("role")) == "in_network"
    ]
    if not in_network_bindings:
        raise ValueError("pricing projection requires an in-network binding")
    binding_projections = [
        await binding_projection(session, binding_by_field)
        for binding_by_field in in_network_bindings
    ]
    code_identities = sorted(
        {
            code_identity
            for projected_binding in binding_projections
            for code_identity in projected_binding.code_rows_by_identity
        }
    )
    content_digest = hashlib.sha256()
    card_row_count = aggregate_row_count = fragment_byte_count = 0
    for code_identity in code_identities:
        card_count, aggregate_count, fragment_bytes = await project_code(
            session,
            candidate_id,
            code_identity,
            binding_projections,
            content_digest,
        )
        card_row_count += card_count
        aggregate_row_count += aggregate_count
        fragment_byte_count += fragment_bytes
        await asyncio.sleep(0)
    return (
        content_digest,
        card_row_count,
        aggregate_row_count,
        fragment_byte_count,
    )


async def _seal_candidate(
    session: Any,
    candidate_id: str,
    content_digest: Any,
    row_counts: tuple[int, int, int],
    build_seconds: float,
) -> dict[str, Any]:
    card_row_count, aggregate_row_count, fragment_byte_count = row_counts
    ready_result = await session.execute(
        text(
            f"""
            UPDATE {table('plan_pricing_projection_candidate')}
               SET state = 'ready',
                   content_digest = :content_digest,
                   card_row_count = :card_row_count,
                   aggregate_row_count = :aggregate_row_count,
                   fragment_byte_count = :fragment_byte_count,
                   build_seconds = :build_seconds,
                   completed_at = transaction_timestamp()
             WHERE projection_id = :projection_id
         RETURNING *
            """
        ),
        {
            "projection_id": candidate_id,
            "content_digest": content_digest.hexdigest(),
            "card_row_count": card_row_count,
            "aggregate_row_count": aggregate_row_count,
            "fragment_byte_count": fragment_byte_count,
            "build_seconds": build_seconds,
        },
    )
    return receipt(ready_result.mappings().one())


async def build_in_session(
    session: Any,
    *,
    binding_manifest_digest: str,
    bindings: Any,
) -> dict[str, Any]:
    """Build or reuse one candidate inside the caller's transaction."""

    if not HEX_DIGEST.fullmatch(binding_manifest_digest):
        raise ValueError("pricing projection binding digest is invalid")
    binding_manifest = normalized_bindings(bindings)
    provider_generation_signature = await provider_signature(session)
    candidate_id = projection_id(
        binding_manifest_digest,
        provider_generation_signature,
    )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": candidate_id},
    )
    existing_receipt = await _existing_candidate_receipt(
        session,
        candidate_id,
        binding_manifest,
        binding_manifest_digest,
        provider_generation_signature,
    )
    if existing_receipt is not None:
        return existing_receipt
    await _insert_candidate(
        session,
        candidate_id,
        binding_manifest,
        binding_manifest_digest,
        provider_generation_signature,
    )
    started_at = time.perf_counter()
    content_digest, *row_counts = await _materialize_all_codes(
        session,
        candidate_id,
        binding_manifest,
    )
    return await _seal_candidate(
        session,
        candidate_id,
        content_digest,
        tuple(row_counts),
        time.perf_counter() - started_at,
    )


async def build_plan_pricing_projection(
    *,
    binding_manifest_digest: str,
    bindings: Any,
) -> dict[str, Any]:
    """Build or reuse one complete invisible candidate atomically."""

    async with db.transaction() as session:
        await lock_provider_generation(session)
        return await build_in_session(
            session,
            binding_manifest_digest=binding_manifest_digest,
            bindings=bindings,
        )
