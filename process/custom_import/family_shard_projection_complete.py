# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persist projection completion under fresh authority and unchanged closure.

This low-volume manifest records the projection comparison only. Full original
input validation, automated structural acceptance and publication still follow.
"""

from sqlalchemy import func, select

from db.models.custom_import import CustomImportGeneration
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.build_source import _flush_page, _page_session, _prepare_statement
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import check_projection_manifest, validate_family_block_indexes
from process.custom_import.family_block_store import _candidate, _execute
from process.custom_import.family_block_validate import CandidateScope, check_closed_identity
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_graph_control import _source_digest
from process.custom_import.family_shard_projection_verify import ClosedProjectionEvidence
from process.custom_import.family_shard_projection_verify import _scope as _proof_scope
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.query_projection_evidence import (
    ProjectionDigest,
    ProjectionDigestState,
    QueryProjectionEvidence,
    completed_projection_evidence,
)
from process.custom_import.query_projection_storage import QUERY_PROJECTION_CONTRACT, query_projection_index_count
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.runner_types import CandidateRunnerError


def _evidence(common, proof):
    if type(common) is not SourceCommonContext or type(proof) is not ClosedProjectionEvidence:
        raise ValueError("projection completion requires the original owner and closed comparison")
    if not configured_fields(common.request.definition):
        if proof.evidence is not None or proof.checkpoint is not None:
            raise CandidateRunnerError("unconfigured projection advertises completion evidence")
        return None
    if type(proof.evidence) is not QueryProjectionEvidence or type(proof.checkpoint) is not ProjectionDigestState:
        raise CandidateRunnerError("configured projection completion evidence is missing")
    if ProjectionDigest(common.request.definition, state=proof.checkpoint).finish() != proof.evidence:
        raise CandidateRunnerError("projection completion digest or definition differs")
    return proof.evidence


def _native_values(model):
    return {column.name: getattr(model, column.name) for column in model.__table__.c}


def _scope(common, build, generation):
    request = common.request
    if (
        build.phase not in {"output", "verifying"}
        or build.source_frozen_at is None
        or build.graph_frozen_at is None
        or build.verified_at is not None
        or build.candidate_error_count != 0
        or build.source_occurrence_count != common.source.source.counts[0]
        or (build.phase == "output") != (build.output_frozen_at is None)
        or generation is None
        or generation.base_generation_id != request.expected_base_generation_id
    ):
        raise CandidateRunnerError("projection completion requires the original unfinished output")
    scope = CandidateScope(
        common.source.snapshot_family_id,
        build.generation_id,
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        dict(common.source.source.scope[0])["capture_bundle_id"],
        request.fence,
        producer_token_sha256(request),
        request.expected_base_generation_id,
    )
    expected_by_name = scope.parameters()
    for producer in (build, generation):
        if any(
            getattr(producer, name) != expected_value
            for name, expected_value in expected_by_name.items()
            if name not in {"family_id", "base_generation_id"}
        ):
            raise CandidateRunnerError("projection completion native producer differs")
    return scope


async def _current(session, common, build, proof):
    descriptor, binding = await _candidate(
        session, common.request, common.source.snapshot_family_id, common.layout, {"closed"}
    )
    await _source_digest(session, common)
    generation = (
        await _execute(
            session,
            select(CustomImportGeneration)
            .where(CustomImportGeneration.generation_id == build.generation_id)
            .execution_options(populate_existing=True),
        )
    ).scalar_one_or_none()
    scope = _scope(common, build, generation)
    _proof_scope(
        binding,
        GenerationIdentity(
            generation.generation_id,
            generation.dataset_id,
            generation.definition_revision_id,
            generation.schema_revision_id,
        ),
        proof.identity,
    )
    snapshot = (
        await _execute(
            session,
            select(CustomImportSnapshotFamily).where(CustomImportSnapshotFamily.family_id == binding.family_id),
        )
    ).scalar_one()
    if check_closed_identity(binding, scope, _native_values(snapshot), _native_values(descriptor)) != proof.identity:
        raise CandidateRunnerError("projection completion closed identity changed")
    await validate_family_block_indexes(session, binding, prepare=_prepare_statement)
    return binding, generation


def _manifest_values(common, binding, generation, proof):
    values_by_name = {
        name: getattr(generation, name)
        for name in (
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "producing_fence",
            "producing_token_sha256",
        )
    }
    evidence, checkpoint = proof.evidence, proof.checkpoint
    values_by_name.update(
        family_id=binding.family_id,
        projection_contract=QUERY_PROJECTION_CONTRACT,
        definition_sha256=bytes.fromhex(common.request.definition.digest),
        capability_sha256=evidence.capability_sha256,
        phase="complete",
        index_count=query_projection_index_count(common.request.definition),
        after_entity_binding_id=0,
        root_count=evidence.root_count,
        child_count=evidence.child_count,
        verified_root_count=evidence.root_count,
        verified_child_count=evidence.child_count,
        digested_root_count=checkpoint.root_count,
        digested_child_count=checkpoint.child_count,
        digest_chain_sha256=checkpoint.chain_sha256,
        digest_after_entity=checkpoint.canonical_entity,
        digest_after_selected_value=checkpoint.selected_value,
        digest_after_child_key=checkpoint.canonical_child_key,
        projection_sha256=evidence.projection_sha256,
    )
    for scope in ("root", "child"):
        role = "query_" + scope
        values_by_name.update(
            {
                scope + "_table_oid": binding.oids_by_role[role],
                scope + "_table_owner": binding.owner_oid,
                scope + "_columns_sha256": binding.columns_sha256_by_role[role],
            }
        )
    return values_by_name


async def complete_shard_query_projection(factory, common, build_id, proof):
    """Write one exact completed manifest, or replay it without replacing history.

    The caller must retain the original assemblies through full candidate proof.
    This operation neither changes build phase nor accepts structural validation.
    """
    evidence = _evidence(common, proof)
    async with _page_session(factory, common.request, build_id) as (session, build):
        binding, generation = await _current(session, common, build, proof)
        manifest = (
            await _execute(
                session,
                select(CustomImportQueryProjection)
                .where(CustomImportQueryProjection.family_id == binding.family_id)
                .execution_options(populate_existing=True),
            )
        ).scalar_one_or_none()
        if evidence is None:
            if manifest is not None:
                raise CandidateRunnerError("unconfigured projection has a native manifest")
            return None
        expected_by_name = _manifest_values(common, binding, generation, proof)
        if manifest is None:
            manifest = CustomImportQueryProjection(**expected_by_name)
            manifest.complete_at = (await _execute(session, select(func.clock_timestamp()))).scalar_one()
            check_projection_manifest(binding, manifest)
            session.add(manifest)
            await _flush_page(session)
        elif any(getattr(manifest, name) != expected_value for name, expected_value in expected_by_name.items()):
            raise CandidateRunnerError("projection completion retry differs from retained evidence")
        check_projection_manifest(binding, manifest)
        if (
            completed_projection_evidence(manifest, generation=generation, definition=common.request.definition)
            != evidence
        ):
            raise CandidateRunnerError("projection native completion evidence differs")
        return evidence
