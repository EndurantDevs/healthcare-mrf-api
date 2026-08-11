# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private PTG layout candidates and seal-time fingerprint publication."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import PTG2LifecycleLockDeferred


PTG2_LAYOUT_BUILD_CANDIDATE_TABLE = "ptg2_layout_build_candidate"


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    return dict(getattr(row, "_mapping", row))


def layout_fingerprint_lock_key(fingerprint: bytes) -> int:
    """Derive the signed advisory-lock key for one semantic fingerprint."""

    normalized = bytes(fingerprint)
    if len(normalized) != 32:
        raise ValueError("PTG layout fingerprint must contain 32 bytes")
    return int.from_bytes(normalized[:8], byteorder="big", signed=True)


async def acquire_layout_digest_lock(
    session: Any,
    *,
    digest: bytes,
    purpose: str,
) -> None:
    """Insert once and verify one private layout-build fingerprint."""

    """Try one short transaction lock without waiting behind another builder."""

    lock_result = await session.execute(
        db.text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
        {"lock_key": layout_fingerprint_lock_key(digest)},
    )
    if not bool(lock_result.scalar()):
        raise PTG2LifecycleLockDeferred(f"PTG {purpose} seal is busy; retry")


async def insert_layout_build_candidate(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    semantic_fingerprint: bytes,
) -> None:
    """Create or exactly adopt one private layout-build candidate."""
    schema = _quote_ident(schema_name)
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.{PTG2_LAYOUT_BUILD_CANDIDATE_TABLE}
                (snapshot_key, semantic_fingerprint, created_at)
            VALUES
                (:snapshot_key, :semantic_fingerprint, transaction_timestamp())
            ON CONFLICT (snapshot_key) DO NOTHING
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "semantic_fingerprint": bytes(semantic_fingerprint),
        },
    )
    candidate_result = await session.execute(
        db.text(
            f"""
            SELECT semantic_fingerprint
              FROM {schema}.{PTG2_LAYOUT_BUILD_CANDIDATE_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND cleanup_pending_at IS NULL
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    stored = candidate_result.scalar()
    if stored is None or bytes(stored) != bytes(semantic_fingerprint):
        raise RuntimeError("PTG layout candidate fingerprint changed")


async def mark_layout_build_candidate_cleanup_pending(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    canonical_snapshot_key: int,
) -> None:
    """Durably defer heavy loser deletion to the exclusively fenced GC path."""

    loser_key = int(snapshot_key)
    winner_key = int(canonical_snapshot_key)
    if loser_key == winner_key:
        raise ValueError("PTG cleanup candidate cannot be its own canonical layout")
    schema = _quote_ident(schema_name)
    marker_result = await session.execute(
        db.text(
            f"""
            UPDATE {schema}.{PTG2_LAYOUT_BUILD_CANDIDATE_TABLE} AS candidate
               SET cleanup_pending_at = COALESCE(
                       candidate.cleanup_pending_at,
                       transaction_timestamp()
                   ),
                   canonical_snapshot_key = :canonical_snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout AS loser,
                   {schema}.ptg2_v3_snapshot_layout AS winner,
                   {schema}.ptg2_v3_layout_fingerprint AS fingerprint
             WHERE candidate.snapshot_key = :snapshot_key
               AND (
                    candidate.canonical_snapshot_key IS NULL
                    OR candidate.canonical_snapshot_key = :canonical_snapshot_key
               )
               AND loser.snapshot_key = candidate.snapshot_key
               AND loser.state = 'building'
               AND winner.snapshot_key = :canonical_snapshot_key
               AND winner.state = 'sealed'
               AND fingerprint.semantic_fingerprint =
                   candidate.semantic_fingerprint
               AND fingerprint.snapshot_key = winner.snapshot_key
               AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_binding AS binding
                     WHERE binding.snapshot_key = loser.snapshot_key
               )
            RETURNING candidate.snapshot_key
            """
        ),
        {
            "snapshot_key": loser_key,
            "canonical_snapshot_key": winner_key,
        },
    )
    if marker_result.scalar() != loser_key:
        raise RuntimeError("PTG losing layout cleanup marker was not persisted")


async def load_layout_build_candidate(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    for_update: bool = False,
) -> bytes | None:
    """Load one private candidate fingerprint, optionally locking its row."""

    schema = _quote_ident(schema_name)
    result = await session.execute(
        db.text(
            f"""
            SELECT semantic_fingerprint
              FROM {schema}.{PTG2_LAYOUT_BUILD_CANDIDATE_TABLE}
             WHERE snapshot_key = :snapshot_key
             {"FOR UPDATE" if for_update else ""}
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    value = result.scalar()
    return bytes(value) if value is not None else None


async def _legacy_layout_fingerprint(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> bytes | None:
    result = await session.execute(
        db.text(
            f"""
            SELECT semantic_fingerprint
              FROM {schema}.ptg2_v3_layout_fingerprint
             WHERE snapshot_key = :snapshot_key
             LIMIT 2
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    values = [bytes(row[0]) for row in result.all()]
    if len(values) > 1:
        raise RuntimeError("PTG layout has multiple canonical fingerprints")
    return values[0] if values else None


async def publish_layout_fingerprint(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    canonical_snapshot_key: int,
    generation: str,
    mapping_digest: bytes,
    support_digest: bytes,
) -> int:
    """Publish one canonical fingerprint after validating the sealed winner."""

    schema = _quote_ident(schema_name)
    fingerprint = await load_layout_build_candidate(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        for_update=True,
    )
    if fingerprint is None:
        fingerprint = await _legacy_layout_fingerprint(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
        )
    if fingerprint is None:
        raise RuntimeError("PTG sealed layout has no candidate fingerprint")
    await acquire_layout_digest_lock(
        session,
        digest=fingerprint,
        purpose="semantic fingerprint",
    )
    winner_by_field = await _locked_layout_fingerprint_owner(
        session,
        schema=schema,
        fingerprint=fingerprint,
    )
    existing_winner_key = _matching_sealed_layout_key(
        winner_by_field,
        generation=generation,
        mapping_digest=mapping_digest,
        support_digest=support_digest,
    )
    if existing_winner_key is not None:
        return existing_winner_key
    return await _insert_canonical_layout_fingerprint(
        session,
        schema=schema,
        fingerprint=fingerprint,
        canonical_snapshot_key=canonical_snapshot_key,
        generation=generation,
        mapping_digest=mapping_digest,
        support_digest=support_digest,
    )


async def _locked_layout_fingerprint_owner(
    session: Any,
    *,
    schema: str,
    fingerprint: bytes,
) -> dict[str, Any]:
    """Lock the canonical owner and discard a migrated building marker."""

    winner_result = await session.execute(
        db.text(
            f"""
            SELECT fingerprint.snapshot_key, layout.state, layout.generation,
                   layout.mapping_digest, layout.support_digest,
                   EXISTS (
                       SELECT 1
                         FROM {schema}.ptg2_layout_build_candidate AS candidate
                        WHERE candidate.snapshot_key = fingerprint.snapshot_key
                          AND candidate.semantic_fingerprint =
                              fingerprint.semantic_fingerprint
                   ) AS is_build_candidate
              FROM {schema}.ptg2_v3_layout_fingerprint AS fingerprint
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = fingerprint.snapshot_key
             WHERE fingerprint.semantic_fingerprint = :semantic_fingerprint
             FOR UPDATE OF fingerprint, layout
            """
        ),
        {"semantic_fingerprint": fingerprint},
    )
    winner_by_field = _row_mapping(winner_result.one_or_none())
    if winner_by_field and winner_by_field.get("state") == "building":
        if not bool(winner_by_field.get("is_build_candidate")):
            raise RuntimeError(
                "PTG building canonical fingerprint has no migration candidate"
            )
        await session.execute(
            db.text(
                f"""
                DELETE FROM {schema}.ptg2_v3_layout_fingerprint
                 WHERE semantic_fingerprint = :semantic_fingerprint
                   AND snapshot_key = :snapshot_key
                """
            ),
            {
                "semantic_fingerprint": fingerprint,
                "snapshot_key": int(winner_by_field["snapshot_key"]),
            },
        )
        winner_by_field = {}
    return winner_by_field


def _matching_sealed_layout_key(
    winner_by_field: Mapping[str, Any],
    *,
    generation: str,
    mapping_digest: bytes,
    support_digest: bytes,
) -> int | None:
    """Return one compatible sealed winner or reject conflicting content."""

    if not winner_by_field:
        return None
    if (
        winner_by_field.get("state") != "sealed"
        or winner_by_field.get("generation") != generation
        or bytes(winner_by_field.get("mapping_digest") or b"")
        != bytes(mapping_digest)
        or bytes(winner_by_field.get("support_digest") or b"")
        != bytes(support_digest)
    ):
        raise RuntimeError(
            "PTG semantic fingerprint resolved to incompatible sealed layout"
        )
    return int(winner_by_field["snapshot_key"])


async def _insert_canonical_layout_fingerprint(
    session: Any,
    *,
    schema: str,
    fingerprint: bytes,
    canonical_snapshot_key: int,
    generation: str,
    mapping_digest: bytes,
    support_digest: bytes,
) -> int:
    """Win the first sealed semantic-fingerprint CAS."""

    insert_result = await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.ptg2_v3_layout_fingerprint
                (semantic_fingerprint, snapshot_key, created_at)
            SELECT :semantic_fingerprint, layout.snapshot_key,
                   transaction_timestamp()
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
             WHERE layout.snapshot_key = :canonical_snapshot_key
               AND layout.state = 'sealed'
               AND layout.generation = :generation
               AND layout.mapping_digest = :mapping_digest
               AND layout.support_digest = :support_digest
            ON CONFLICT (semantic_fingerprint) DO NOTHING
            RETURNING snapshot_key
            """
        ),
        {
            "semantic_fingerprint": fingerprint,
            "canonical_snapshot_key": int(canonical_snapshot_key),
            "generation": generation,
            "mapping_digest": bytes(mapping_digest),
            "support_digest": bytes(support_digest),
        },
    )
    inserted_key = insert_result.scalar()
    if inserted_key is None:
        raise RuntimeError("PTG canonical layout fingerprint CAS did not persist")
    return int(inserted_key)


async def delete_layout_build_candidate(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> None:
    """Delete one private build-candidate row after durable publication."""

    await session.execute(
        db.text(
            f"DELETE FROM {_quote_ident(schema_name)}."
            f"{PTG2_LAYOUT_BUILD_CANDIDATE_TABLE} "
            "WHERE snapshot_key = :snapshot_key"
        ),
        {"snapshot_key": int(snapshot_key)},
    )


__all__ = [
    "PTG2_LAYOUT_BUILD_CANDIDATE_TABLE",
    "acquire_layout_digest_lock",
    "delete_layout_build_candidate",
    "insert_layout_build_candidate",
    "layout_fingerprint_lock_key",
    "load_layout_build_candidate",
    "mark_layout_build_candidate_cleanup_pending",
    "publish_layout_fingerprint",
]
