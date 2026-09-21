# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced immutable family graph construction for candidate orchestration."""

from __future__ import annotations

import hmac
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Iterator

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
from process.custom_import.definition import Field, canonical_json
from process.custom_import.execution import LeaseGrant, lease_token_sha256
from process.custom_import.family import FamilyBuildResult, FamilyRejection, RootFamily
from process.custom_import.materialization import (
    ChildScalarTarget,
    GenerationIdentity,
    RootScalarTarget,
    ValidatedWinnerCandidateStream,
    WinnerCandidate,
    WinnerMaterialization,
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
    child_key_hash,
    digest_text,
    family_child_payload_hashes,
    fields_by_collection,
    new_family_hash,
    pack_hash,
    payload_values,
    record_payload,
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


@dataclass(frozen=True)
class ChildRevisionContext:
    """Current family and stream identities needed to append one child row."""

    request: CandidateRunRequest
    registry: CandidateRegistry
    child_pack: CustomImportPack
    root_record: CustomImportRootRecord
    family_revision: CustomImportFamilyRevision
    collection: str


async def materialize_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
) -> MaterializedCandidate:
    """Commit one complete immutable candidate graph in its own transaction."""

    async with session_factory() as session, session.begin():
        return await build_candidate_graph(session, request, grant, admitted)


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
        packs_by_collection = await create_packs(
            session, request, grant, registry, selected_families, execution.capture_bundle_id
        )
        generation = await create_generation(
            session,
            request,
            grant,
            pointer,
            source_bundle_sha256,
            selected_families,
            execution.capture_bundle_id,
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
    return tuple(family for _, family in sorted(selected_by_root_hash.items(), key=lambda pair: pair[0]))


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

    await prepare_materialization_statement(session)
    return list(
        (
            await session.execute(
                select(
                    CustomImportGenerationFamily,
                    CustomImportFamilyRevision,
                    CustomImportRootRevision,
                    CustomImportRootRecord,
                    CustomImportEntityBinding,
                )
                .join(
                    CustomImportFamilyRevision,
                    (CustomImportFamilyRevision.family_revision_id == CustomImportGenerationFamily.family_revision_id)
                    & (CustomImportFamilyRevision.dataset_id == CustomImportGenerationFamily.dataset_id),
                )
                .join(
                    CustomImportRootRevision,
                    (CustomImportRootRevision.root_revision_id == CustomImportFamilyRevision.root_revision_id)
                    & (CustomImportRootRevision.dataset_id == CustomImportFamilyRevision.dataset_id),
                )
                .join(
                    CustomImportRootRecord,
                    (CustomImportRootRecord.root_record_id == CustomImportGenerationFamily.root_record_id)
                    & (CustomImportRootRecord.dataset_id == CustomImportGenerationFamily.dataset_id),
                )
                .join(
                    CustomImportEntityBinding,
                    (CustomImportEntityBinding.entity_binding_id == CustomImportFamilyRevision.entity_binding_id)
                    & (CustomImportEntityBinding.dataset_id == CustomImportFamilyRevision.dataset_id),
                )
                .where(
                    CustomImportGenerationFamily.generation_id == pointer.generation_id,
                    CustomImportGenerationFamily.dataset_id == request.dataset_id,
                )
            )
        ).all()
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

    await prepare_materialization_statement(session)
    child_rows = list((await session.execute(previous_children_statement(request, pointer))).all())
    children_by_family: dict[int, list[tuple[str, CustomImportChildRevision]]] = defaultdict(list)
    for family_child, child_model, collection_name in child_rows:
        children_by_family[family_child.family_revision_id].append((collection_name, child_model))
    return {
        family_id: tuple(sorted(children, key=lambda item: (item[0], bytes(item[1].child_key_sha256))))
        for family_id, children in children_by_family.items()
    }


def previous_children_statement(request: CandidateRunRequest, pointer: CurrentGenerationPointer):
    """Build the fixed-parameter retained-child query for one generation."""

    return (
        select(CustomImportFamilyChild, CustomImportChildRevision, CustomImportChildCollection.collection_name)
        .join(
            CustomImportGenerationFamily,
            (CustomImportGenerationFamily.family_revision_id == CustomImportFamilyChild.family_revision_id)
            & (CustomImportGenerationFamily.dataset_id == CustomImportFamilyChild.dataset_id),
        )
        .join(
            CustomImportChildRevision,
            (CustomImportChildRevision.child_revision_id == CustomImportFamilyChild.child_revision_id)
            & (CustomImportChildRevision.dataset_id == CustomImportFamilyChild.dataset_id),
        )
        .join(
            CustomImportChildCollection,
            (CustomImportChildCollection.schema_revision_id == CustomImportFamilyChild.schema_revision_id)
            & (CustomImportChildCollection.dataset_id == CustomImportFamilyChild.dataset_id)
            & (CustomImportChildCollection.collection_slot == CustomImportFamilyChild.collection_slot),
        )
        .where(
            CustomImportFamilyChild.dataset_id == request.dataset_id,
            CustomImportGenerationFamily.generation_id == pointer.generation_id,
            CustomImportGenerationFamily.dataset_id == request.dataset_id,
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
        bytes(root_record.logical_key_sha256), root_key_hash(request.definition, root_values_by_field)
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
            child_key_hash(request.definition, stored_child.collection, stored_child.values_by_field),
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
    session.add_all(tuple(packs_by_collection.values()))
    await flush_materialization(session)
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
    return generation


async def publish_families(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    registry: CandidateRegistry,
    packs_by_collection: Mapping[str | None, CustomImportPack],
    selected_families: Sequence[RootFamily | StoredCandidateFamily],
) -> tuple[PublishedCandidateFamily, ...]:
    """Append each selected family under the current execution fence."""

    published_families: list[PublishedCandidateFamily] = []
    child_ordinals_by_collection: dict[str, int] = defaultdict(int)
    for root_ordinal, family in enumerate(selected_families):
        if isinstance(family, RootFamily):
            published_family = await publish_new_family(
                session,
                request,
                grant,
                registry,
                packs_by_collection,
                family,
                root_ordinal,
                child_ordinals_by_collection,
            )
        else:
            published_family = await copy_stored_family(
                session,
                request,
                grant,
                registry,
                packs_by_collection,
                family,
                root_ordinal,
                child_ordinals_by_collection,
            )
        published_families.append(published_family)
    return tuple(published_families)


async def publish_new_family(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    registry: CandidateRegistry,
    packs_by_collection: Mapping[str | None, CustomImportPack],
    family: RootFamily,
    root_ordinal: int,
    child_ordinals_by_collection: dict[str, int],
) -> PublishedCandidateFamily:
    """Append one freshly accepted root family and all of its children."""

    root_record = await root_record_for_values(session, request, family.root)
    entity_binding = await entity_binding_for_values(session, request, family.root)
    root_revision = await create_root_revision(
        session,
        request,
        root_record,
        packs_by_collection[None],
        family.root,
        root_ordinal,
    )
    family_sha256 = new_family_hash(request.definition, family)
    family_revision = await create_family_revision(
        session,
        request,
        grant,
        root_record.root_record_id,
        root_revision.root_revision_id,
        entity_binding.entity_binding_id,
        family_sha256,
        sum(len(children) for children in family.children.values()),
    )
    published_child_rows = await publish_new_children(
        session,
        request,
        registry,
        packs_by_collection,
        family,
        root_record,
        family_revision,
        child_ordinals_by_collection,
    )
    return PublishedCandidateFamily(
        root_record_id=root_record.root_record_id,
        root_revision_id=root_revision.root_revision_id,
        family_revision_id=family_revision.family_revision_id,
        entity_binding_id=entity_binding.entity_binding_id,
        family_sha256=family_sha256,
        root_values_by_field=dict(family.root),
        children=published_child_rows,
    )


async def create_root_revision(
    session: AsyncSession,
    request: CandidateRunRequest,
    root_record: CustomImportRootRecord,
    root_pack: CustomImportPack,
    root_values_by_field: Mapping[str, Any],
    source_ordinal: int,
) -> CustomImportRootRevision:
    """Append one typed root payload under its current pack provenance."""

    canonical_payload = record_payload(request.definition.root_fields, root_values_by_field)
    root_revision = CustomImportRootRevision(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=root_record.root_record_id,
        pack_id=root_pack.pack_id,
        source_ordinal=source_ordinal,
        canonical_payload=canonical_payload,
        payload_sha256=digest_text("root-payload", canonical_payload),
    )
    session.add(root_revision)
    await flush_materialization(session)
    return root_revision


async def create_family_revision(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    root_record_id: int,
    root_revision_id: int,
    entity_binding_id: int,
    family_sha256: bytes,
    child_count: int,
) -> CustomImportFamilyRevision:
    """Append one fenced family header after its root revision exists."""

    family_revision = CustomImportFamilyRevision(
        dataset_id=request.dataset_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=root_record_id,
        root_revision_id=root_revision_id,
        entity_binding_id=entity_binding_id,
        family_sha256=family_sha256,
        child_count=child_count,
        producing_execution_id=request.execution_id,
        producing_fence=grant.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )
    session.add(family_revision)
    await flush_materialization(session)
    return family_revision


async def publish_new_children(
    session: AsyncSession,
    request: CandidateRunRequest,
    registry: CandidateRegistry,
    packs_by_collection: Mapping[str | None, CustomImportPack],
    family: RootFamily,
    root_record: CustomImportRootRecord,
    family_revision: CustomImportFamilyRevision,
    child_ordinals_by_collection: dict[str, int],
) -> tuple[PublishedCandidateChild, ...]:
    """Append each fresh child in deterministic collection and logical-key order."""

    published_child_rows: list[PublishedCandidateChild] = []
    fields_by_name = fields_by_collection(request.definition)
    for collection in request.definition.child_collections:
        ordered_children = sorted(
            family.children[collection.name],
            key=lambda child_values: child_key_hash(request.definition, collection.name, child_values),
        )
        for child_values in ordered_children:
            source_ordinal = child_ordinals_by_collection[collection.name]
            child_context = ChildRevisionContext(
                request=request,
                registry=registry,
                child_pack=packs_by_collection[collection.name],
                root_record=root_record,
                family_revision=family_revision,
                collection=collection.name,
            )
            published_child = await create_child_revision(
                session,
                child_context,
                fields_by_name[collection.name],
                child_values,
                source_ordinal,
            )
            child_ordinals_by_collection[collection.name] += 1
            published_child_rows.append(published_child)
    return tuple(published_child_rows)


async def create_child_revision(
    session: AsyncSession,
    child_context: ChildRevisionContext,
    collection_fields: Sequence[Field],
    child_values_by_field: Mapping[str, Any],
    source_ordinal: int,
) -> PublishedCandidateChild:
    """Append one child revision and its immutable family membership edge."""

    request = child_context.request
    collection = child_context.collection
    canonical_child_key = child_key_document(request.definition, collection, child_values_by_field)
    canonical_payload = record_payload(collection_fields, child_values_by_field)
    child_revision = CustomImportChildRevision(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=child_context.root_record.root_record_id,
        collection_slot=child_context.registry.child_collection_slots[collection],
        pack_id=child_context.child_pack.pack_id,
        source_ordinal=source_ordinal,
        canonical_parent_key=child_context.root_record.canonical_logical_key,
        parent_key_sha256=child_context.root_record.logical_key_sha256,
        canonical_child_key=canonical_child_key,
        child_key_sha256=child_key_hash(request.definition, collection, child_values_by_field),
        canonical_payload=canonical_payload,
        payload_sha256=digest_text("child-payload", canonical_payload),
    )
    session.add(child_revision)
    await flush_materialization(session)
    await add_family_child_membership(session, child_context, child_revision.child_revision_id)
    return PublishedCandidateChild(
        collection=collection,
        child_revision_id=child_revision.child_revision_id,
        child_key_sha256=bytes(child_revision.child_key_sha256),
        values_by_field=dict(child_values_by_field),
    )


async def add_family_child_membership(
    session: AsyncSession,
    child_context: ChildRevisionContext,
    child_revision_id: int,
) -> None:
    """Attach a child revision to the current family under its collection slot."""

    request = child_context.request
    session.add(
        CustomImportFamilyChild(
            family_revision_id=child_context.family_revision.family_revision_id,
            dataset_id=request.dataset_id,
            schema_revision_id=request.schema_revision_id,
            root_record_id=child_context.root_record.root_record_id,
            collection_slot=child_context.registry.child_collection_slots[child_context.collection],
            child_revision_id=child_revision_id,
        )
    )
    await flush_materialization(session)


async def copy_stored_family(
    session: AsyncSession,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    registry: CandidateRegistry,
    packs_by_collection: Mapping[str | None, CustomImportPack],
    stored_family: StoredCandidateFamily,
    root_ordinal: int,
    child_ordinals_by_collection: dict[str, int],
) -> PublishedCandidateFamily:
    """Clone one retained family into fresh current-fence revisions and edges."""

    root_revision = await copy_root_revision(
        session,
        request,
        stored_family,
        packs_by_collection[None],
        root_ordinal,
    )
    family_revision = await create_family_revision(
        session,
        request,
        grant,
        stored_family.root_record.root_record_id,
        root_revision.root_revision_id,
        stored_family.entity_binding.entity_binding_id,
        bytes(stored_family.family.family_sha256),
        len(stored_family.children),
    )
    copied_child_rows = await copy_stored_children(
        session,
        request,
        registry,
        packs_by_collection,
        stored_family,
        family_revision,
        child_ordinals_by_collection,
    )
    return PublishedCandidateFamily(
        root_record_id=stored_family.root_record.root_record_id,
        root_revision_id=root_revision.root_revision_id,
        family_revision_id=family_revision.family_revision_id,
        entity_binding_id=stored_family.entity_binding.entity_binding_id,
        family_sha256=bytes(family_revision.family_sha256),
        root_values_by_field=stored_family.root_values_by_field,
        children=copied_child_rows,
    )


async def copy_root_revision(
    session: AsyncSession,
    request: CandidateRunRequest,
    stored_family: StoredCandidateFamily,
    root_pack: CustomImportPack,
    source_ordinal: int,
) -> CustomImportRootRevision:
    """Copy a retained root payload under the fresh root pack provenance."""

    root_revision = CustomImportRootRevision(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=stored_family.root_record.root_record_id,
        pack_id=root_pack.pack_id,
        source_ordinal=source_ordinal,
        canonical_payload=stored_family.root_revision.canonical_payload,
        payload_sha256=stored_family.root_revision.payload_sha256,
    )
    session.add(root_revision)
    await flush_materialization(session)
    return root_revision


async def copy_stored_children(
    session: AsyncSession,
    request: CandidateRunRequest,
    registry: CandidateRegistry,
    packs_by_collection: Mapping[str | None, CustomImportPack],
    stored_family: StoredCandidateFamily,
    family_revision: CustomImportFamilyRevision,
    child_ordinals_by_collection: dict[str, int],
) -> tuple[PublishedCandidateChild, ...]:
    """Copy each retained child payload and attach it to the cloned family."""

    copied_child_rows: list[PublishedCandidateChild] = []
    for stored_child in stored_family.children:
        collection = stored_child.collection
        child_context = ChildRevisionContext(
            request=request,
            registry=registry,
            child_pack=packs_by_collection[collection],
            root_record=stored_family.root_record,
            family_revision=family_revision,
            collection=collection,
        )
        child_revision = await copy_child_revision(
            session,
            child_context,
            stored_child,
            child_ordinals_by_collection[collection],
        )
        child_ordinals_by_collection[collection] += 1
        copied_child_rows.append(child_revision)
    return tuple(copied_child_rows)


async def copy_child_revision(
    session: AsyncSession,
    child_context: ChildRevisionContext,
    stored_child: StoredCandidateChild,
    source_ordinal: int,
) -> PublishedCandidateChild:
    """Copy one retained child payload and attach its fresh membership edge."""

    request = child_context.request
    source_child = stored_child.child
    child_revision = CustomImportChildRevision(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=child_context.root_record.root_record_id,
        collection_slot=child_context.registry.child_collection_slots[stored_child.collection],
        pack_id=child_context.child_pack.pack_id,
        source_ordinal=source_ordinal,
        canonical_parent_key=child_context.root_record.canonical_logical_key,
        parent_key_sha256=child_context.root_record.logical_key_sha256,
        canonical_child_key=source_child.canonical_child_key,
        child_key_sha256=source_child.child_key_sha256,
        canonical_payload=source_child.canonical_payload,
        payload_sha256=source_child.payload_sha256,
    )
    session.add(child_revision)
    await flush_materialization(session)
    await add_family_child_membership(session, child_context, child_revision.child_revision_id)
    return PublishedCandidateChild(
        collection=stored_child.collection,
        child_revision_id=child_revision.child_revision_id,
        child_key_sha256=bytes(child_revision.child_key_sha256),
        values_by_field=stored_child.values_by_field,
    )


async def root_record_for_values(
    session: AsyncSession,
    request: CandidateRunRequest,
    root_values_by_field: Mapping[str, Any],
) -> CustomImportRootRecord:
    """Load or append the stable dataset-local root logical identity."""

    canonical_root_key = root_key_document(request.definition, root_values_by_field)
    logical_key_sha256 = root_key_hash(request.definition, root_values_by_field)
    key_contract_sha256 = root_key_contract_hash(request.definition)
    root_record = (
        await session.execute(
            select(CustomImportRootRecord)
            .where(
                CustomImportRootRecord.dataset_id == request.dataset_id,
                CustomImportRootRecord.key_contract_sha256 == key_contract_sha256,
                CustomImportRootRecord.logical_key_sha256 == logical_key_sha256,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if root_record is not None:
        if root_record.canonical_logical_key != canonical_root_key:
            raise CandidateRunnerError("root-key digest collision")
        return root_record
    root_record = CustomImportRootRecord(
        dataset_id=request.dataset_id,
        key_contract_sha256=key_contract_sha256,
        canonical_logical_key=canonical_root_key,
        logical_key_sha256=logical_key_sha256,
    )
    session.add(root_record)
    await flush_materialization(session)
    return root_record


async def entity_binding_for_values(
    session: AsyncSession,
    request: CandidateRunRequest,
    root_values_by_field: Mapping[str, Any],
) -> CustomImportEntityBinding:
    """Load or append the stable generic NPI binding for an accepted family."""

    entity_value = root_values_by_field.get(request.definition.entity_field)
    if not isinstance(entity_value, str):
        raise CandidateRunnerError("accepted family has no string entity value")
    value_sha256 = entity_value_digest(entity_value)
    entity_binding = (
        await session.execute(
            select(CustomImportEntityBinding)
            .where(
                CustomImportEntityBinding.dataset_id == request.dataset_id,
                CustomImportEntityBinding.adapter_id == "npi",
                CustomImportEntityBinding.canonical_value == entity_value,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if entity_binding is not None:
        if not hmac.compare_digest(bytes(entity_binding.value_sha256), value_sha256):
            raise CandidateRunnerError("entity binding digest does not match its value")
        return entity_binding
    entity_binding = CustomImportEntityBinding(
        dataset_id=request.dataset_id,
        adapter_id="npi",
        canonical_value=entity_value,
        value_sha256=value_sha256,
    )
    session.add(entity_binding)
    await flush_materialization(session)
    return entity_binding


async def attach_generation_families(
    session: AsyncSession,
    request: CandidateRunRequest,
    generation: CustomImportGeneration,
    published_families: Sequence[PublishedCandidateFamily],
) -> None:
    """Attach every current-fence family to the current candidate generation."""

    session.add_all(
        tuple(
            CustomImportGenerationFamily(
                generation_id=generation.generation_id,
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
                root_record_id=family.root_record_id,
                family_revision_id=family.family_revision_id,
            )
            for family in published_families
        )
    )
    await flush_materialization(session)


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
    for family in published_families:
        candidates.append(root_winner_candidate(family, root_query_fields))
        if child_collection is not None:
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
    if rejection_models:
        session.add_all(tuple(rejection_models))
        await flush_materialization(session)


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
