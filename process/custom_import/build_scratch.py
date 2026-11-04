# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only scratch TRUNCATE after complete verification and generation sealing.

The caller owns a clean transaction and must roll it back on every refusal or
error. This operation keeps registered relations, serving content, sequences,
original proofs and seals. It grants no authority and never commits implicitly.
"""

import asyncio
import json
from dataclasses import dataclass

from sqlalchemy import func, select, text

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildVerification,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportLease,
)
from db.models.custom_import_build_scratch import CustomImportBuildScratchCleanup
from db.models.custom_import_storage import CustomImportSnapshotRelation
from process.custom_import import publication, retirement
from process.custom_import.build_output import _COUNT_NAMES, _proof_matches
from process.custom_import.storage_layout import (
    _PHASE_INDEX_NAMES,
    snapshot_schema,
    snapshot_tables,
)

MAX_CLEANUP_SECONDS = 120
CONTRACT = "custom-import/build-scratch-cleanup/v1"
SCRATCH_RELATIONS = (
    "custom_import_build_occurrence",
    "custom_import_build_family",
    "custom_import_build_candidate_context",
    "source_bulk_landing",
)
_IDENTITY = (
    "family_id",
    "generation_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "execution_id",
    "capture_bundle_id",
    "producing_fence",
    "producing_token_sha256",
)
_BUILD_COUNTERS = (
    "source_occurrence_count",
    "candidate_error_count",
    "selected_family_count",
    "completed_family_count",
    "candidate_context_count",
    "generation_family_count",
    "winner_count",
    "next_rejection_ordinal",
)


class BuildScratchCleanupRefused(ValueError):
    """The exact snapshot has not established safe scratch cleanup."""


@dataclass(frozen=True)
class BuildScratchCleanupReceipt:
    family_id: int
    retained_bytes: int
    remaining_bytes: int
    replayed: bool


async def _eligible(session, family, *, require_automated=True):
    if family.generation_id is None or family.frozen_at is None:
        raise BuildScratchCleanupRefused("build scratch cleanup requires a frozen sealed generation")
    execution = await session.get(CustomImportExecution, family.execution_id, with_for_update=True)
    lease = await session.get(CustomImportLease, family.execution_id, with_for_update=True)
    build = (
        await session.scalars(
            select(CustomImportBuildAttempt)
            .where(
                CustomImportBuildAttempt.execution_id == family.execution_id,
                CustomImportBuildAttempt.producing_fence == family.producing_fence,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    generation = await session.get(CustomImportGeneration, family.generation_id, with_for_update=True)
    if execution is None or generation is None or build is None or execution.state not in {"completed", "no_change"}:
        raise BuildScratchCleanupRefused("build scratch cleanup requires its completed producing build")
    now = await session.scalar(select(func.clock_timestamp()))
    if lease is not None and lease.expires_at is not None and lease.expires_at > now:
        raise BuildScratchCleanupRefused("build scratch cleanup refuses an unexpired producer lease")
    names = _IDENTITY[1:]
    if any(getattr(generation, name) != getattr(family, name) for name in names):
        raise BuildScratchCleanupRefused("build scratch cleanup generation identity differs")
    proof = await session.scalar(
        select(CustomImportBuildVerification)
        .where(CustomImportBuildVerification.build_id == build.build_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    # The existing native acceptance CHECK makes manual provenance incompatible with this contract.
    if proof is None or proof.verification_contract not in {
        "custom-import/build-structure/v1",
        "custom-import/manual-acceptance/v1",
    }:
        raise BuildScratchCleanupRefused("build scratch cleanup structural proof is unavailable")
    if require_automated and proof.verification_contract != "custom-import/build-structure/v1":
        raise BuildScratchCleanupRefused("build scratch cleanup requires full automated structural verification")
    _proof_matches(build, generation, proof)
    seal = await publication._validated_generation_seal(session, generation)
    if any(getattr(proof, name) != getattr(seal, name) for name in _COUNT_NAMES):
        raise BuildScratchCleanupRefused("build scratch cleanup proof and seal counts differ")
    if build.candidate_error_count != 0 or build.plan_stage != "complete" or build.plan_complete_at is None:
        raise BuildScratchCleanupRefused("build scratch cleanup admission is incomplete")
    return build, generation, proof, seal


async def _manifest(session, quoted, family):
    await session.execute(
        text(f"SELECT {quoted}.resolve_custom_import_snapshot_relations(:family)"), {"family": family.family_id}
    )
    rows = (
        await session.scalars(
            select(CustomImportSnapshotRelation)
            .where(CustomImportSnapshotRelation.family_id == family.family_id)
            .order_by(CustomImportSnapshotRelation.relation_slot)
            .execution_options(populate_existing=True)
        )
    ).all()
    if [row.relation_slot for row in rows] != list(range(1, 16)) or family.landing_table_oid is None:
        raise BuildScratchCleanupRefused("build scratch cleanup relation census is incomplete")
    relations_by_slot = {
        str(row.relation_slot): [row.table_oid, row.table_owner, bytes(row.columns_sha256).hex()] for row in rows
    }
    for prefix in ("landing", "origin"):
        oid = getattr(family, f"{prefix}_table_oid")
        relations_by_slot[prefix] = (
            None
            if oid is None
            else [
                oid,
                getattr(family, f"{prefix}_table_owner"),
                bytes(getattr(family, f"{prefix}_columns_sha256")).hex(),
            ]
        )
    return relations_by_slot


def _affinity(family, build, generation, proof, seal):
    return {
        **{name: getattr(family, name) for name in _IDENTITY},
        "build_id": build.build_id,
        **{
            name: getattr(build, name)
            for name in ("source_frozen_at", "graph_frozen_at", "output_frozen_at", "verified_at")
        },
        "sealed_at": seal.sealed_at,
        "materialization_sha256": bytes(seal.materialization_sha256),
        "effective_output_sha256": bytes(seal.effective_output_sha256),
        "source_bundle_sha256": bytes(generation.source_bundle_sha256),
        "candidate_sha256": bytes(generation.candidate_sha256),
    }


async def _sizes(session, qualified):
    sizes = (
        await session.scalars(
            text("""
        SELECT pg_catalog.pg_total_relation_size(CAST(relation AS regclass))
        FROM unnest(CAST(:relations AS text[])) WITH ORDINALITY AS leaves(relation,ordinal)
        ORDER BY ordinal
    """),
            {"relations": list(qualified)},
        )
    ).all()
    return dict(zip(SCRATCH_RELATIONS, sizes, strict=True))


async def _empty(session, qualified):
    branches = " UNION ALL ".join(f"SELECT 1 FROM {table}" for table in qualified)
    return await session.scalar(text(f"SELECT NOT EXISTS({branches})"))


async def _truncate(session, qualified):
    await session.execute(text("TRUNCATE TABLE " + ",".join(qualified) + " CONTINUE IDENTITY RESTRICT"))


async def _drop_finished_indexes(session, family_id):
    """Remove only model-defined scratch indexes under the already-held exclusive locks."""
    names = frozenset().union(*_PHASE_INDEX_NAMES.values())
    table_by_index_name = {
        index.name: index.table.name
        for table in snapshot_tables(family_id).values()
        for index in table.indexes
        if index.name in names and not index.unique and table.name in SCRATCH_RELATIONS
    }
    if table_by_index_name.keys() != names:
        raise BuildScratchCleanupRefused("build scratch phase index model differs")
    namespace = snapshot_schema(family_id)
    indexes = (
        (
            await session.execute(
                text("""
        SELECT c.relname::text name,heap.relname::text table_name,
               i.indisunique unique_index,c.relowner=heap.relowner owner_matches,
               i.indisvalid AND i.indisready AND i.indislive live
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=c.oid
        LEFT JOIN pg_catalog.pg_class heap ON heap.oid=i.indrelid
        WHERE n.nspname=:namespace AND c.relname::text=ANY(CAST(:names AS text[]))
        ORDER BY c.relname
    """),
                {"namespace": namespace, "names": sorted(names)},
            )
        )
        .mappings()
        .all()
    )
    if any(
        index["table_name"] != table_by_index_name[index["name"]]
        or index["unique_index"] is not False
        or index["owner_matches"] is not True
        or index["live"] is not True
        for index in indexes
    ):
        raise BuildScratchCleanupRefused("build scratch phase index custody differs")
    quote = (await session.connection()).dialect.identifier_preparer.quote_identifier
    for index in indexes:
        await session.execute(text(f"DROP INDEX {quote(namespace)}.{quote(index['name'])}"))
    return [index["name"] for index in indexes]


async def _post_audit(session, family, proof, affinity, engine_id, marker=None):
    if proof.verification_contract == "custom-import/build-structure/v1":
        return None
    from process.custom_import import post_seal_audit as audit

    try:
        if marker is None:
            context = await audit.audit_context_for_trim(session, family.family_id, engine_id=engine_id)
            return await audit.verified_audit_for_trim(session, context, engine_id=engine_id)
        recorded = marker.aggregate_evidence.get("post_seal_audit")
        if type(recorded) is not dict or type(recorded.get("audit_id")) is not str:
            raise audit.PostSealAuditRefused("post-seal cleanup audit receipt is missing")
        verified = await audit.completed_audit_receipt(
            session, recorded["audit_id"], engine_id=engine_id, family_id=family.family_id, expected_candidate=affinity
        )
        if verified != recorded:
            raise audit.PostSealAuditRefused("post-seal cleanup audit receipt differs")
        return verified
    except audit.PostSealAuditRefused as error:
        raise BuildScratchCleanupRefused(
            "build scratch cleanup requires full automated post-seal verification"
        ) from error


async def _trim(session, family_id, engine_id):
    """Close exact scratch storage only after authority, replay and reader checks."""
    snapshot_schema(family_id)
    retirement.lifecycle_lock_key(engine_id, 1)
    try:
        _schema, quoted = await retirement._owner_schema(session)
        family, tombstone = await retirement._locked_family(session, family_id, engine_id)
    except retirement.SnapshotRetirementRefused as exc:
        raise BuildScratchCleanupRefused(
            "build scratch cleanup requires protected owner authority and exact storage"
        ) from exc
    if family is None or tombstone is not None:
        raise BuildScratchCleanupRefused("build scratch cleanup snapshot is retired")
    build, generation, proof, seal = await _eligible(session, family, require_automated=False)
    manifest = await _manifest(session, quoted, family)
    namespace = snapshot_schema(family_id)
    tables = tuple(f'"{namespace}"."{name}"' for name in SCRATCH_RELATIONS)
    affinity = _affinity(family, build, generation, proof, seal)
    marker = await session.get(CustomImportBuildScratchCleanup, family_id, with_for_update=True)
    if marker is not None:
        if marker.engine_id != engine_id or any(
            getattr(marker, name) != expected_attribute for name, expected_attribute in affinity.items()
        ):
            raise BuildScratchCleanupRefused("build scratch cleanup replay identity differs")
        if marker.aggregate_evidence.get("relations") != manifest or not await _empty(session, tables):
            raise BuildScratchCleanupRefused("build scratch cleanup replay storage differs")
        await _post_audit(session, family, proof, affinity, engine_id, marker)
        return BuildScratchCleanupReceipt(family_id, marker.retained_bytes, marker.remaining_bytes, True)
    audit_receipt = await _post_audit(session, family, proof, affinity, engine_id)
    # A frozen candidate was required to drain landing before verification. Never hide unfinished input.
    if await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {tables[-1]})")):
        raise BuildScratchCleanupRefused("build scratch cleanup landing is not drained")
    before = await _sizes(session, tables)
    # Existing API readers pin all registered leaves. Fail immediately if another reader still holds a pin.
    await session.execute(text("LOCK TABLE " + ",".join(tables) + " IN ACCESS EXCLUSIVE MODE NOWAIT"))
    removed_indexes = await _drop_finished_indexes(session, family_id)
    await _truncate(session, tables)
    if not await _empty(session, tables) or await _manifest(session, quoted, family) != manifest:
        raise BuildScratchCleanupRefused("build scratch cleanup changed registered storage identity")
    after = await _sizes(session, tables)
    evidence_by_name = {
        "contract": CONTRACT,
        "relations": manifest,
        "build_counters": {name: getattr(build, name) for name in _BUILD_COUNTERS},
        "structural_counts": {name: getattr(proof, name) for name in _COUNT_NAMES},
        "verification_contract": proof.verification_contract,
        "materialization_contract": seal.materialization_contract,
        "verification_evidence": seal.verification_evidence,
        "post_seal_audit": audit_receipt,
        "bytes_before": before,
        "bytes_after": after,
        "phase_indexes_dropped": removed_indexes,
    }
    return await _record_cleanup(session, affinity, engine_id, evidence_by_name)


async def _record_cleanup(session, affinity, engine_id, evidence_by_name):
    """Persist the same bounded cleanup evidence within the caller transaction."""
    if len(json.dumps(evidence_by_name, separators=(",", ":")).encode()) > 32768:
        raise BuildScratchCleanupRefused("build scratch cleanup aggregate evidence exceeds its bound")
    retained, remaining = sum(evidence_by_name["bytes_before"].values()), sum(evidence_by_name["bytes_after"].values())
    session.add(
        CustomImportBuildScratchCleanup(
            **affinity,
            engine_id=engine_id,
            retained_bytes=retained,
            remaining_bytes=remaining,
            aggregate_evidence=evidence_by_name,
        )
    )
    await session.flush()
    return BuildScratchCleanupReceipt(affinity["family_id"], retained, remaining, False)


async def trim_build_scratch(session, family_id, *, engine_id):
    """Keep exact relation identities; caller commits or rolls back the complete operation."""
    async with asyncio.timeout(MAX_CLEANUP_SECONDS):
        return await _trim(session, family_id, engine_id)
