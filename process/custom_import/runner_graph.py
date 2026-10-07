# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced immutable family graph construction for candidate orchestration."""

from __future__ import annotations

import hmac
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Iterator

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportChildCollection,
    CustomImportChildRevision,
    CustomImportEntityBinding,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportPack,
    CustomImportRejection,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from process.custom_import.definition import canonical_json
from process.custom_import.execution import LeaseGrant, lease_token_sha256
from process.custom_import.family import FamilyBuildResult, FamilyRejection, RootFamily, has_child_membership
from process.custom_import.materialization import (
    ChildScalarTarget,
    GenerationIdentity,
    RootScalarTarget,
    ValidatedWinnerCandidateStream,
    WinnerCandidate,
    WinnerMaterialization,
    _profile_scopes,
    materialize_winners,
    persist_scalar_projections,
    persist_winner_materialization,
    project_child_scalars,
    project_root_scalars,
)
from process.custom_import.publication import _capture_source_bundle_digest
from process.custom_import.runner_codec import (
    candidate_hash,
    child_key_document,
    digest_text,
    family_child_payload_hashes,
    fields_by_collection,
    pack_hash,
    payload_values,
    root_key_contract_hash,
    root_key_document,
    root_key_evidence_from_tuple,
    root_key_hash,
    root_payload_hash,
)
from process.custom_import.runner_registry import (
    clear_materialization_authority,
    ensure_selection_profiles,
    locked_candidate_context,
    prepare_materialization_statement,
)
from process.custom_import.runner_types import (
    CandidateRegistry,
    CandidateRunnerError,
    CandidateRunRequest,
    CurrentGenerationPointer,
    MaterializedCandidate,
    PublishedCandidateChild,
    PublishedCandidateFamily,
    SessionFactory,
    StoredCandidateChild,
    StoredCandidateFamily,
)

_MATERIALIZATION_BATCH_SIZE = 256


async def materialize_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
) -> MaterializedCandidate:
    """Commit the immutable graph, then prepare its isolated serving indexes."""

    async with session_factory() as session, session.begin():
        materialized = await build_candidate_graph(session, request, grant, admitted)
    await prepare_legacy_snapshot_indexes(session_factory, request, grant, materialized.generation_id)
    return materialized


async def prepare_legacy_snapshot_indexes(session_factory, request, grant, generation_id):
    """Prepare one frozen-candidate index per freshly fenced transaction."""

    is_complete = False
    while not is_complete:
        async with session_factory() as session, session.begin():
            is_complete = await prepare_legacy_serving_step(session, request, grant, generation_id)


async def prepare_legacy_serving_step(session, request, grant, generation_id):
    """Resolve the full producer again; never index canonical or BASE storage."""

    from process.custom_import.materialization_store import _call

    try:
        _, execution, _ = await locked_candidate_context(session, request, grant)
        family_id = await _call(
            session,
            "resolve_custom_import_generation_finality_snapshot",
            (
                ("bigint", generation_id),
                ("bigint", request.dataset_id),
                ("bigint", request.definition_revision_id),
                ("bigint", request.schema_revision_id),
                ("bigint", request.execution_id),
                ("bigint", execution.capture_bundle_id),
                ("bigint", grant.fence),
                ("bytea", lease_token_sha256(request.lease_token)),
            ),
        )
        if type(family_id) is not int or not 0 < family_id < 2**63:
            raise CandidateRunnerError("legacy serving snapshot binding is malformed")
        complete = await _call(
            session,
            "prepare_custom_import_snapshot_indexes",
            (("bigint", family_id), ("text", "serving")),
        )
        if type(complete) is not bool:
            raise CandidateRunnerError("legacy snapshot index preparation result is malformed")
        return complete
    finally:
        clear_materialization_authority(session)


async def build_candidate_graph(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
) -> MaterializedCandidate:
    """Append packs, families, typed rows, winners, and rejections atomically."""

    try:
        registry, execution, pointer = await locked_candidate_context(session, request, grant)
        await prepare_materialization_statement(session)
        await ensure_selection_profiles(session, request, registry)
        selected_families = await select_candidate_families(session, request, admitted, pointer)
        source_bundle_sha256 = await source_bundle_digest(session, request, execution.capture_bundle_id)
        generation = await create_generation(
            session,
            request,
            grant,
            pointer,
            source_bundle_sha256,
            selected_families,
            execution.capture_bundle_id,
        )
        packs_by_collection = await create_packs(
            session, request, grant, registry, selected_families, execution.capture_bundle_id
        )
        published_families = await publish_families(
            session,
            request,
            grant,
            registry,
            packs_by_collection,
            selected_families,
        )
        await attach_generation_families(session, request, generation, published_families)
        await persist_projections_and_winners(session, request, registry, generation, published_families)
        await persist_rejections(session, request, grant, admitted)
        from process.custom_import.materialization_store import verify_materialization_authority

        await verify_materialization_authority(session)
        await freeze_legacy_snapshot(session, request, grant)
        return MaterializedCandidate(
            generation_id=generation.generation_id,
            pointer=pointer,
            accepted_family_count=len(admitted.families),
            rejection_count=len(admitted.rejections),
        )
    finally:
        clear_materialization_authority(session)


async def flush_materialization(session: AsyncSession) -> None:
    """Flush graph rows only while the transaction-local lease window is live."""

    await prepare_materialization_statement(session)
    await session.flush()


def batches(rows: Sequence[object], *, size: int = _MATERIALIZATION_BATCH_SIZE) -> Iterator[Sequence[object]]:
    """Yield bounded slices without making a second graph-sized collection."""

    if size <= 0:
        raise ValueError("materialization batch size must be positive")
    for offset in range(0, len(rows), size):
        yield rows[offset : offset + size]


async def source_bundle_digest(
    session: AsyncSession,
    request: CandidateRunRequest,
    capture_bundle_id: int,
) -> bytes:
    """Return the database-authoritative source digest for this execution."""

    await prepare_materialization_statement(session)
    return await _capture_source_bundle_digest(
        session,
        capture_bundle_id=capture_bundle_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )


async def select_candidate_families(
    session: AsyncSession,
    request: CandidateRunRequest,
    admitted: FamilyBuildResult,
    pointer: CurrentGenerationPointer | None,
) -> tuple[RootFamily | StoredCandidateFamily, ...]:
    """Apply upsert or scoped snapshot retention to the admitted root families."""

    await prepare_materialization_statement(session)
    accepted_by_root_hash = {root_key_hash(request.definition, family.root): family for family in admitted.families}
    if pointer is None:
        return tuple(family for _, family in sorted(accepted_by_root_hash.items(), key=lambda pair: pair[0]))
    rejected_root_hashes = {
        evidence[1]
        for rejection in admitted.rejections
        if (evidence := rejection_root_key_evidence(request, rejection.root_key)) is not None
    }
    retain_previous = request.definition.refresh_mode == "upsert" or bool(rejected_root_hashes)
    if retain_previous and pointer.schema_revision_id != request.schema_revision_id:
        raise CandidateRunnerError("candidate cannot retain a prior family across a schema revision")
    previous_families = await load_previous_families(session, request, pointer, retain_previous)
    previous_by_root_hash = {bytes(family.root_record.logical_key_sha256): family for family in previous_families}
    if len(previous_by_root_hash) != len(previous_families):
        raise CandidateRunnerError("current generation has duplicate root identities")
    selected_by_root_hash: dict[bytes, RootFamily | StoredCandidateFamily] = dict(accepted_by_root_hash)
    if request.definition.refresh_mode == "upsert":
        selected_by_root_hash = {**previous_by_root_hash, **accepted_by_root_hash}
    else:
        selected_by_root_hash.update(
            {
                root_hash: family
                for root_hash, family in previous_by_root_hash.items()
                if root_hash in rejected_root_hashes
            }
        )
    selected_families = tuple(family for _, family in sorted(selected_by_root_hash.items(), key=lambda pair: pair[0]))
    if request.definition.child_memberships:
        for family in selected_families:
            if isinstance(family, StoredCandidateFamily):
                children_by_collection = defaultdict(list)
                for child in family.children:
                    children_by_collection[child.collection].append(child.values_by_field)
                if not has_child_membership(request.definition, children_by_collection):
                    raise CandidateRunnerError("retained family violates child membership")
    return selected_families


async def load_previous_families(
    session: AsyncSession,
    request: CandidateRunRequest,
    pointer: CurrentGenerationPointer,
    required: bool,
) -> tuple[StoredCandidateFamily, ...]:
    """Load selected prior families only when refresh semantics retain them."""

    if not required:
        return ()
    family_rows = await load_selected_family_rows(session, request, pointer)
    children_by_family = await load_previous_children(session, request, pointer)
    return tuple(stored_family_from_models(request, family_row, children_by_family) for family_row in family_rows)


async def load_selected_family_rows(
    session: AsyncSession,
    request: CandidateRunRequest,
    pointer: CurrentGenerationPointer,
) -> list[
    tuple[
        object, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportRootRecord, CustomImportEntityBinding
    ]
]:
    """Load current generation memberships with their root and entity records."""

    models = await previous_snapshot_models(session, request, pointer)
    await prepare_materialization_statement(session)
    return list((await session.execute(selected_family_statement(request, pointer, models=models))).all())


async def previous_snapshot_models(session, request, pointer):
    """Pin the exact sealed base; only verified legacy storage returns no map."""

    from process.custom_import.read_contracts import PinnedReadTarget
    from process.custom_import.read_identity import resolve_generation_snapshot
    from process.custom_import.storage_layout import snapshot_models

    await prepare_materialization_statement(session)
    family_id = await resolve_generation_snapshot(
        session,
        PinnedReadTarget(
            request.dataset_id,
            pointer.generation_id,
            pointer.definition_revision_id,
            pointer.schema_revision_id,
            "default",
        ),
    )
    return None if family_id is None else snapshot_models(family_id)


def selected_family_statement(request, pointer, *, models=None):
    """Compile every retained hot relation against one resolved storage family."""

    membership = CustomImportGenerationFamily if models is None else models[CustomImportGenerationFamily]
    family = CustomImportFamilyRevision if models is None else models[CustomImportFamilyRevision]
    root_revision = CustomImportRootRevision if models is None else models[CustomImportRootRevision]
    root_record = CustomImportRootRecord if models is None else models[CustomImportRootRecord]
    binding = CustomImportEntityBinding if models is None else models[CustomImportEntityBinding]
    return (
        select(membership, family, root_revision, root_record, binding)
        .join(
            family,
            (family.family_revision_id == membership.family_revision_id) & (family.dataset_id == membership.dataset_id),
        )
        .join(
            root_revision,
            (root_revision.root_revision_id == family.root_revision_id)
            & (root_revision.dataset_id == family.dataset_id),
        )
        .join(
            root_record,
            (root_record.root_record_id == membership.root_record_id)
            & (root_record.dataset_id == membership.dataset_id),
        )
        .join(
            binding,
            (binding.entity_binding_id == family.entity_binding_id) & (binding.dataset_id == family.dataset_id),
        )
        .where(membership.generation_id == pointer.generation_id, membership.dataset_id == request.dataset_id)
    )


async def load_previous_children(
    session: AsyncSession,
    request: CandidateRunRequest,
    pointer: CurrentGenerationPointer,
) -> Mapping[int, tuple[tuple[str, CustomImportChildRevision], ...]]:
    """Load retained child rows through the selected generation membership.

    A generation-scoped join keeps the SQL parameter count fixed even when an
    upsert retains more families than a PostgreSQL driver can bind in one
    expanding ``IN`` predicate.
    """

    models = await previous_snapshot_models(session, request, pointer)
    await prepare_materialization_statement(session)
    child_rows = list((await session.execute(previous_children_statement(request, pointer, models=models))).all())
    children_by_family: dict[int, list[tuple[str, CustomImportChildRevision]]] = defaultdict(list)
    for family_child, child_model, collection_name in child_rows:
        children_by_family[family_child.family_revision_id].append((collection_name, child_model))
    return {
        family_id: tuple(sorted(children, key=lambda item: (item[0], bytes(item[1].child_key_sha256))))
        for family_id, children in children_by_family.items()
    }


def previous_children_statement(request: CandidateRunRequest, pointer: CurrentGenerationPointer, *, models=None):
    """Build the fixed-parameter retained-child query for one generation."""

    family_child = CustomImportFamilyChild if models is None else models[CustomImportFamilyChild]
    child = CustomImportChildRevision if models is None else models[CustomImportChildRevision]
    membership = CustomImportGenerationFamily if models is None else models[CustomImportGenerationFamily]
    return (
        select(family_child, child, CustomImportChildCollection.collection_name)
        .join(
            membership,
            (membership.family_revision_id == family_child.family_revision_id)
            & (membership.dataset_id == family_child.dataset_id),
        )
        .join(
            child,
            (child.child_revision_id == family_child.child_revision_id) & (child.dataset_id == family_child.dataset_id),
        )
        .join(
            CustomImportChildCollection,
            (CustomImportChildCollection.schema_revision_id == family_child.schema_revision_id)
            & (CustomImportChildCollection.dataset_id == family_child.dataset_id)
            & (CustomImportChildCollection.collection_slot == family_child.collection_slot),
        )
        .where(
            family_child.dataset_id == request.dataset_id,
            membership.generation_id == pointer.generation_id,
            membership.dataset_id == request.dataset_id,
        )
    )


def stored_family_from_models(
    request: CandidateRunRequest,
    family_row: tuple[
        object, CustomImportFamilyRevision, CustomImportRootRevision, CustomImportRootRecord, CustomImportEntityBinding
    ],
    children_by_family: Mapping[int, tuple[tuple[str, CustomImportChildRevision], ...]],
) -> StoredCandidateFamily:
    """Validate and decode one selected prior family for a fresh fence."""

    _membership, family_model, root_revision, root_record, entity_binding = family_row
    root_values_by_field = stored_root_values(request, root_record, root_revision, entity_binding)
    stored_child_rows = stored_family_children(
        request,
        root_record,
        family_model,
        children_by_family.get(family_model.family_revision_id, ()),
    )
    return StoredCandidateFamily(
        root_record=root_record,
        root_revision=root_revision,
        family=family_model,
        entity_binding=entity_binding,
        root_values_by_field=root_values_by_field,
        children=stored_child_rows,
    )


def stored_root_values(
    request: CandidateRunRequest,
    root_record: CustomImportRootRecord,
    root_revision: CustomImportRootRevision,
    entity_binding: CustomImportEntityBinding,
) -> Mapping[str, object]:
    """Decode and verify a retained root payload, key, and entity binding."""

    if not hmac.compare_digest(bytes(root_record.key_contract_sha256), root_key_contract_hash(request.definition)):
        raise CandidateRunnerError("current generation root-key contract does not match the definition")
    root_values_by_field = payload_values(
        request.definition.root_fields,
        root_revision.canonical_payload,
        label="current generation root payload",
    )
    canonical_root_key = root_key_document(request.definition, root_values_by_field)
    if root_record.canonical_logical_key != canonical_root_key or not hmac.compare_digest(
        bytes(root_record.logical_key_sha256), digest_text("root-key", canonical_root_key)
    ):
        raise CandidateRunnerError("current generation root identity does not match its payload")
    verify_entity_binding(request, entity_binding, root_values_by_field)
    return root_values_by_field


def verify_entity_binding(
    request: CandidateRunRequest,
    entity_binding: CustomImportEntityBinding,
    root_values_by_field: Mapping[str, object],
) -> None:
    """Require the retained NPI binding to match the root payload exactly."""

    entity_value = root_values_by_field.get(request.definition.entity_field)
    if (
        not isinstance(entity_value, str)
        or entity_binding.adapter_id != "npi"
        or entity_binding.canonical_value != entity_value
        or not hmac.compare_digest(bytes(entity_binding.value_sha256), entity_value_digest(entity_value))
    ):
        raise CandidateRunnerError("current generation entity binding does not match its root payload")


def entity_value_digest(entity_value: str) -> bytes:
    """Return the stable generic NPI binding digest."""

    return digest_text("entity:npi", entity_value)


def stored_family_children(
    request: CandidateRunRequest,
    root_record: CustomImportRootRecord,
    family_model: CustomImportFamilyRevision,
    child_models: Sequence[tuple[str, CustomImportChildRevision]],
) -> tuple[StoredCandidateChild, ...]:
    """Decode each retained child and validate its parent and logical key."""

    fields_by_name = fields_by_collection(request.definition)
    stored_child_rows = tuple(
        StoredCandidateChild(
            collection=collection_name,
            child=child_model,
            values_by_field=payload_values(
                fields_by_name[collection_name],
                child_model.canonical_payload,
                label="current generation child payload",
            ),
        )
        for collection_name, child_model in child_models
    )
    for stored_child in stored_child_rows:
        verify_stored_child(request, root_record, stored_child)
    if family_model.child_count != len(stored_child_rows):
        raise CandidateRunnerError("current generation child count does not match family membership")
    return stored_child_rows


def verify_stored_child(
    request: CandidateRunRequest,
    root_record: CustomImportRootRecord,
    stored_child: StoredCandidateChild,
) -> None:
    """Require retained child payload identity to remain bound to its root."""

    canonical_child_key = child_key_document(
        request.definition,
        stored_child.collection,
        stored_child.values_by_field,
    )
    child_model = stored_child.child
    if (
        child_model.canonical_parent_key != root_record.canonical_logical_key
        or not hmac.compare_digest(bytes(child_model.parent_key_sha256), bytes(root_record.logical_key_sha256))
        or child_model.canonical_child_key != canonical_child_key
        or not hmac.compare_digest(
            bytes(child_model.child_key_sha256),
            digest_text("child-key", canonical_child_key),
        )
    ):
        raise CandidateRunnerError("current generation child identity does not match its payload")


async def create_packs(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    registry: CandidateRegistry,
    selected_families: Sequence[RootFamily | StoredCandidateFamily],
    capture_bundle_id: int,
) -> Mapping[str | None, CustomImportPack]:
    """Create one current-fence pack for each declared source stream."""

    root_hashes = [root_payload_hash(request.definition, family) for family in selected_families]
    child_hashes_by_collection: dict[str, list[bytes]] = defaultdict(list)
    for family in selected_families:
        for collection_name, payload_hash in family_child_payload_hashes(request.definition, family):
            child_hashes_by_collection[collection_name].append(payload_hash)
    packs_by_collection = pack_models(
        request,
        grant,
        registry,
        capture_bundle_id,
        root_hashes,
        child_hashes_by_collection,
    )
    from process.custom_import.legacy_graph_store import persist_pack_models

    await persist_pack_models(session, packs_by_collection)
    return packs_by_collection


def pack_models(
    request: CandidateRunRequest,
    grant: LeaseGrant,
    registry: CandidateRegistry,
    capture_bundle_id: int,
    root_hashes: Sequence[bytes],
    child_hashes_by_collection: Mapping[str, Sequence[bytes]],
) -> Mapping[str | None, CustomImportPack]:
    """Build current-attempt pack models from deterministic payload hashes."""

    token_sha256 = lease_token_sha256(request.lease_token)
    packs_by_collection: dict[str | None, CustomImportPack] = {
        None: CustomImportPack(
            execution_id=request.execution_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            stream_slot=registry.root_stream_slot,
            pack_ordinal=0,
            capture_bundle_id=capture_bundle_id,
            record_count=len(root_hashes),
            pack_sha256=pack_hash("root", root_hashes),
            producing_fence=grant.fence,
            producing_token_sha256=token_sha256,
        )
    }
    child_stream_by_collection = {
        stream.child_collection: stream.stream_id
        for stream in request.definition.source_streams
        if stream.record_kind == "child"
    }
    for collection in request.definition.child_collections:
        stream_id = child_stream_by_collection[collection.name]
        collection_hashes = child_hashes_by_collection.get(collection.name, ())
        packs_by_collection[collection.name] = CustomImportPack(
            execution_id=request.execution_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            stream_slot=registry.stream_slots[stream_id],
            pack_ordinal=0,
            capture_bundle_id=capture_bundle_id,
            record_count=len(collection_hashes),
            pack_sha256=pack_hash(collection.name, collection_hashes),
            producing_fence=grant.fence,
            producing_token_sha256=token_sha256,
        )
    return packs_by_collection


async def create_generation(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    pointer: CurrentGenerationPointer | None,
    source_bundle_sha256: bytes,
    selected_families: Sequence[RootFamily | StoredCandidateFamily],
    capture_bundle_id: int,
) -> CustomImportGeneration:
    """Create the one current-fence generation row before attaching families."""

    root_key_hashes = [
        root_key_hash(request.definition, family.root)
        if isinstance(family, RootFamily)
        else bytes(family.root_record.logical_key_sha256)
        for family in selected_families
    ]
    generation = CustomImportGeneration(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=capture_bundle_id,
        base_generation_id=None if pointer is None else pointer.generation_id,
        base_dataset_id=None if pointer is None else request.dataset_id,
        source_bundle_sha256=source_bundle_sha256,
        candidate_sha256=candidate_hash(
            execution_id=request.execution_id,
            fence=grant.fence,
            base_generation_id=None if pointer is None else pointer.generation_id,
            root_key_hashes=root_key_hashes,
        ),
        root_count=len(selected_families),
        family_count=len(selected_families),
        producing_fence=grant.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )
    session.add(generation)
    await flush_materialization(session)
    from process.custom_import.materialization_store import _call

    family_id = await _call(
        session,
        "resolve_custom_import_legacy_generation_snapshot",
        (("bigint", generation.generation_id),),
    )
    if type(family_id) is not int or not 0 < family_id < 2**63:
        raise CandidateRunnerError("legacy generation snapshot binding is malformed")
    return generation


async def freeze_legacy_snapshot(session, request, grant):
    """Close candidate writes before publication scans or a caller can commit."""

    from process.custom_import.materialization_store import _call

    family_id = await _call(
        session,
        "freeze_custom_import_snapshot_family",
        (
            ("bigint", request.execution_id),
            ("bigint", grant.fence),
            ("bytea", lease_token_sha256(request.lease_token)),
        ),
    )
    if type(family_id) is not int or not 0 < family_id < 2**63:
        raise CandidateRunnerError("legacy snapshot freeze binding is malformed")


async def publish_families(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    registry: CandidateRegistry,
    packs_by_collection: Mapping[str | None, CustomImportPack],
    selected_families: Sequence[RootFamily | StoredCandidateFamily],
) -> tuple[PublishedCandidateFamily, ...]:
    """Append fresh and retained families through bounded protected set pages."""

    from process.custom_import.legacy_family_store import persist_families

    return await persist_families(session, request, grant, registry, packs_by_collection, selected_families)


async def attach_generation_families(
    session: AsyncSession,
    request: CandidateRunRequest,
    generation: CustomImportGeneration,
    published_families: Sequence[PublishedCandidateFamily],
) -> None:
    """Attach current-fence families through bounded protected set writes."""

    from process.custom_import.legacy_generation_store import persist_generation_families

    await persist_generation_families(session, request, generation, published_families)


async def persist_projections_and_winners(
    session: AsyncSession,
    request: CandidateRunRequest,
    registry: CandidateRegistry,
    generation: CustomImportGeneration,
    published_families: Sequence[PublishedCandidateFamily],
) -> None:
    """Persist typed scalar projections and deterministic winner rows together."""

    await prepare_materialization_statement(session)
    root_projections = build_root_projections(request, published_families)
    child_projections = build_child_projections(request, registry, published_families)
    await persist_scalar_projection_batches(
        session,
        request,
        registry,
        root_projections,
        child_projections,
    )
    await persist_generation_winners(session, request, registry, generation, published_families)


async def persist_scalar_projection_batches(
    session: AsyncSession,
    request: CandidateRunRequest,
    registry: CandidateRegistry,
    root_projections: Sequence[object],
    child_projections: Sequence[object],
) -> None:
    """Persist scalar rows in bounded flushes under the graph lease window."""

    for root_projection_batch in batches(root_projections):
        await prepare_materialization_statement(session)
        await persist_scalar_projections(
            session,
            request.definition,
            root_scalars=root_projection_batch,
            child_collection_slots=registry.child_collection_slots,
        )
    for child_projection_batch in batches(child_projections):
        await prepare_materialization_statement(session)
        await persist_scalar_projections(
            session,
            request.definition,
            child_scalars=child_projection_batch,
            child_collection_slots=registry.child_collection_slots,
        )


def build_root_projections(
    request: CandidateRunRequest,
    published_families: Sequence[PublishedCandidateFamily],
) -> list[object]:
    """Build every root scalar projection from durable insert identities."""

    root_projections: list[object] = []
    for family in published_families:
        root_projections.extend(
            project_root_scalars(
                request.definition,
                root_target=RootScalarTarget(
                    dataset_id=request.dataset_id,
                    schema_revision_id=request.schema_revision_id,
                    root_record_id=family.root_record_id,
                    root_revision_id=family.root_revision_id,
                ),
                root_values=family.root_values_by_field,
            )
        )
    return root_projections


def build_child_projections(
    request: CandidateRunRequest,
    registry: CandidateRegistry,
    published_families: Sequence[PublishedCandidateFamily],
) -> list[object]:
    """Build every child scalar projection from current child revision identities."""

    child_projections: list[object] = []
    for family in published_families:
        for child in family.children:
            child_projections.extend(
                project_child_scalars(
                    request.definition,
                    collection=child.collection,
                    child_target=ChildScalarTarget(
                        dataset_id=request.dataset_id,
                        schema_revision_id=request.schema_revision_id,
                        root_record_id=family.root_record_id,
                        collection_slot=registry.child_collection_slots[child.collection],
                        child_revision_id=child.child_revision_id,
                    ),
                    child_values=child.values_by_field,
                    child_collection_slots=registry.child_collection_slots,
                )
            )
    return child_projections


async def persist_generation_winners(
    session: AsyncSession,
    request: CandidateRunRequest,
    registry: CandidateRegistry,
    generation: CustomImportGeneration,
    published_families: Sequence[PublishedCandidateFamily],
) -> None:
    """Materialize and flush the bounded winner rows for this generation."""

    await prepare_materialization_statement(session)
    identity = GenerationIdentity(
        generation_id=generation.generation_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )
    winners = materialize_winners(
        request.definition,
        generation=identity,
        candidates=ValidatedWinnerCandidateStream(
            identity,
            winner_candidates(request, registry, published_families),
        ),
        child_collection_slots=registry.child_collection_slots,
    )
    for winner_batch in batches(winners.winners):
        await prepare_materialization_statement(session)
        await persist_winner_materialization(
            session,
            WinnerMaterialization(
                generation=winners.generation,
                profile_count=winners.profile_count,
                profile_context_slots=winners.profile_context_slots,
                winners=tuple(winner_batch),
            ),
        )


def winner_candidates(
    request: CandidateRunRequest,
    registry: CandidateRegistry,
    published_families: Sequence[PublishedCandidateFamily],
) -> tuple[WinnerCandidate, ...]:
    """Build bounded root and child selection contexts from immutable members."""

    candidates: list[WinnerCandidate] = []
    root_query_fields = request.definition.query.root_fields
    child_collection = request.definition.query.child_collection
    child_query_fields = request.definition.query.child_fields
    uses_child_context = any(
        scope.collection_slot != 0 for scope in _profile_scopes(request.definition, registry.child_collection_slots)
    )
    for family in published_families:
        candidates.append(root_winner_candidate(family, root_query_fields))
        if child_collection is not None and uses_child_context:
            candidates.extend(
                child_winner_candidates(
                    family,
                    child_collection,
                    registry.child_collection_slots[child_collection],
                    root_query_fields,
                    child_query_fields,
                )
            )
    return tuple(candidates)


def root_winner_candidate(
    family: PublishedCandidateFamily,
    root_query_fields: Sequence[str],
) -> WinnerCandidate:
    """Build one root-context winner candidate from a published family."""

    return WinnerCandidate(
        entity_binding_id=family.entity_binding_id,
        family_revision_id=family.family_revision_id,
        family_sha256=family.family_sha256,
        context_collection_slot=0,
        context_child_revision_id=None,
        context_child_key_sha256=None,
        values_by_field={
            field_id: family.root_values_by_field[field_id]
            for field_id in root_query_fields
            if field_id in family.root_values_by_field
        },
    )


def child_winner_candidates(
    family: PublishedCandidateFamily,
    collection: str,
    collection_slot: int,
    root_query_fields: Sequence[str],
    child_query_fields: Sequence[str],
) -> tuple[WinnerCandidate, ...]:
    """Build every child-context winner candidate in the queried collection."""

    candidates: list[WinnerCandidate] = []
    root_query_values_by_field = {
        field_id: family.root_values_by_field[field_id]
        for field_id in root_query_fields
        if field_id in family.root_values_by_field
    }
    for child in family.children:
        if child.collection != collection:
            continue
        values_by_field = dict(root_query_values_by_field)
        values_by_field.update(
            {
                field_id: child.values_by_field[field_id]
                for field_id in child_query_fields
                if field_id in child.values_by_field
            }
        )
        candidates.append(
            WinnerCandidate(
                entity_binding_id=family.entity_binding_id,
                family_revision_id=family.family_revision_id,
                family_sha256=family.family_sha256,
                context_collection_slot=collection_slot,
                context_child_revision_id=child.child_revision_id,
                context_child_key_sha256=child.child_key_sha256,
                values_by_field=values_by_field,
            )
        )
    return tuple(candidates)


async def persist_rejections(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
) -> None:
    """Append value-free family rejection evidence under the current fence."""

    rejection_models = [
        rejection_model(request, grant, ordinal, rejection)
        for ordinal, rejection in enumerate(
            sorted(admitted.rejections, key=lambda item: (repr(item.root_key), item.code))
        )
    ]
    from process.custom_import.legacy_graph_store import persist_rejection_models

    await persist_rejection_models(session, rejection_models)


def rejection_model(
    request: CandidateRunRequest,
    grant: LeaseGrant,
    ordinal: int,
    rejection: FamilyRejection,
) -> CustomImportRejection:
    """Build one retained rejection without embedding source values."""

    root_key_evidence = rejection_root_key_evidence(request, rejection.root_key)
    canonical_root_key = None if root_key_evidence is None else root_key_evidence[0]
    root_key_sha256 = None if root_key_evidence is None else root_key_evidence[1]
    code = rejection.code
    evidence = canonical_json(
        {
            "code": code,
            "contract": "custom-import-rejection/v1",
            "root_key_sha256": None if root_key_sha256 is None else root_key_sha256.hex(),
        }
    )
    return CustomImportRejection(
        execution_id=request.execution_id,
        rejection_ordinal=ordinal,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        pack_id=None,
        root_key_sha256=root_key_sha256,
        canonical_root_key=canonical_root_key,
        collection_slot=None,
        source_ordinal=None,
        code=code,
        field_slot=None,
        canonical_evidence=evidence,
        producing_fence=grant.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )


def rejection_root_key_evidence(
    request: CandidateRunRequest,
    root_key: object,
) -> tuple[str, bytes] | None:
    """Return safe rejection identity evidence, omitting malformed raw keys."""

    return root_key_evidence_from_tuple(request.definition, root_key)
