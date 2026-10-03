# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen bounded winner verification and ordinary generation finality."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from itertools import count, zip_longest

from sqlalchemy import BigInteger, cast, exists, func, select, tuple_

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildCandidateContext,
    CustomImportBuildFamily,
    CustomImportBuildVerification,
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportChildCollection,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportDefinitionRevision,
    CustomImportEntityBinding,
    CustomImportExecution,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportField,
    CustomImportFieldAlias,
    CustomImportFieldSlot,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPack,
    CustomImportRejection,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportRootScalar,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
    CustomImportWinner,
)
from process.custom_import import materialization as material
from process.custom_import import publication
from process.custom_import.build_graph import (
    _call,
    _candidate,
    _candidate_digest,
    _child_statement,
    _heartbeat,
    _model_bytes,
    _one_row,
    _page_cost,
    _read_rows,
    _read_transaction,
    _ReadPage,
    _renew_while_reading,
    _require_budget,
    _session,
    _snapshot,
    _source_digest,
    _verify_request,
)
from process.custom_import.build_source import (
    SourceBuildRequest,
    _flush_page,
    _lock_page,
    _page_session,
    _prepare_statement,
)
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_codec import (
    digest_text,
    fields_by_collection,
    new_family_hash_ordered,
    payload_values,
    record_payload,
)
from process.custom_import.runner_graph import stored_root_values, verify_stored_child
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError, StoredCandidateChild

_COUNT_NAMES = (
    "root_count",
    "family_count",
    "generation_family_count",
    "family_child_count",
    "winner_count",
    "profile_count",
    "root_scalar_count",
    "child_scalar_count",
)


@dataclass(frozen=True)
class BuildOutputResult:
    generation_id: int
    seal: publication.GenerationSealReceipt
    no_change: publication.PublicationReceipt | None = None


def _generation_identity(generation):
    return material.GenerationIdentity(
        generation.generation_id,
        generation.dataset_id,
        generation.definition_revision_id,
        generation.schema_revision_id,
    )


def _group_key(context):
    return context.profile_slot, context.entity_binding_id, bytes(context.context_key_sha256)


def _context_candidates(session, request, registry, build_id, group):
    context = CustomImportBuildCandidateContext
    family = CustomImportFamilyRevision
    root = CustomImportRootRevision
    child = CustomImportChildRevision
    statement = (
        select(context, family, root, child)
        .join(family, family.family_revision_id == context.family_revision_id)
        .join(root, root.root_revision_id == family.root_revision_id)
        .outerjoin(child, child.child_revision_id == context.context_child_revision_id)
        .where(
            context.build_id == build_id,
            tuple_(context.profile_slot, context.entity_binding_id, context.context_key_sha256) == group,
        )
    )
    contract = material._winner_candidate_contract(
        request.definition, material._profile_scopes(request.definition, registry.child_collection_slots)
    )
    profile = request.definition.selection_profiles[group[0] - 1]
    for context_row, family_row, root_row, child_row in _read_rows(
        session, request, build_id, statement, (context.candidate_context_id,), (context, family, root, child)
    ):
        root_values = payload_values(
            request.definition.root_fields, root_row.canonical_payload, label="winner root payload"
        )
        child_values = None
        if child_row is not None:
            collection = next(
                name for name, slot in registry.child_collection_slots.items() if slot == child_row.collection_slot
            )
            child_values = payload_values(
                fields_by_collection(request.definition)[collection],
                child_row.canonical_payload,
                label="winner child payload",
            )
        candidate = _candidate(request.definition, family_row, root_values, child_row, child_values)
        normalized = material._normalize_winner_candidate(candidate, contract)
        material._validate_selection_values(profile, normalized, contract.fields_by_id)
        canonical, digest = material._context_key(profile, normalized, contract.fields_by_id)
        if (
            context_row.canonical_context_key != canonical
            or bytes(context_row.context_key_sha256) != digest
            or context_row.entity_binding_id != candidate.entity_binding_id
            or context_row.context_collection_slot != candidate.context_collection_slot
        ):
            raise CandidateRunnerError("stored candidate context differs from its typed payload")
        yield candidate


def _reduce_group(session, request, registry, build_id, generation, group):
    identity = _generation_identity(generation)
    winners = material.iter_ordered_profile_winners(
        request.definition,
        generation=identity,
        profile_slot=group[0],
        candidates=material.ValidatedWinnerCandidateStream(
            identity, _context_candidates(session, request, registry, build_id, group)
        ),
        child_collection_slots=registry.child_collection_slots,
    )
    winner = next(winners, None)
    if winner is None or next(winners, None) is not None:
        raise CandidateRunnerError("one complete candidate group must produce one winner")
    context = CustomImportBuildCandidateContext
    statement = select(context).where(
        context.build_id == build_id,
        context.profile_slot == winner.profile_slot,
        context.entity_binding_id == winner.entity_binding_id,
        context.context_key_sha256 == winner.context_key_sha256,
        context.family_revision_id == winner.family_revision_id,
        context.context_child_revision_id.is_(None)
        if winner.context_child_revision_id is None
        else context.context_child_revision_id == winner.context_child_revision_id,
    )
    selected = _one_row(session, request, build_id, statement, (context.candidate_context_id,), (context,))
    if selected is None:
        raise CandidateRunnerError("selected winner has no retained candidate context")
    return winner, selected[0].candidate_context_id


async def _attach_families(session_factory, request, build_id):
    after_root_id = 0
    while True:
        async with _page_session(session_factory, request, build_id) as (session, build):
            plan, family, membership_model = (
                CustomImportBuildFamily,
                CustomImportFamilyRevision,
                CustomImportGenerationFamily,
            )
            await _prepare_statement(session)
            memberships = (
                await session.execute(
                    select(
                        plan.root_record_id,
                        family.family_revision_id,
                        exists(
                            select(1).where(
                                membership_model.generation_id == build.generation_id,
                                membership_model.root_record_id == plan.root_record_id,
                            )
                        ).label("attached"),
                    )
                    .join(family, family.family_revision_id == plan.family_revision_id)
                    .where(
                        plan.build_id == build_id,
                        plan.root_record_id > after_root_id,
                    )
                    .order_by(plan.root_record_id)
                    .limit(request.page_row_limit)
                )
            ).all()
            if not memberships:
                return
            models = [
                membership_model(
                    generation_id=build.generation_id,
                    dataset_id=request.dataset_id,
                    definition_revision_id=request.definition_revision_id,
                    schema_revision_id=request.schema_revision_id,
                    root_record_id=membership.root_record_id,
                    family_revision_id=membership.family_revision_id,
                )
                for membership in memberships
                if not membership.attached
            ]
            _page_cost(request, models)
            session.add_all(models)
            await _flush_page(session)
        after_root_id = memberships[-1].root_record_id
        await _heartbeat(session_factory, request)


def _next_group(session, request, build_id, after):
    context = CustomImportBuildCandidateContext
    statement = select(context).where(context.build_id == build_id)
    if after is not None:
        statement = statement.where(
            tuple_(context.profile_slot, context.entity_binding_id, context.context_key_sha256) > after
        )
    first = _one_row(
        session,
        request,
        build_id,
        statement,
        (context.profile_slot, context.entity_binding_id, context.context_key_sha256, context.candidate_context_id),
        (context,),
    )
    return None if first is None else _group_key(first[0])


async def _store_winner(session, request, build_id, generation, winner, context_id):
    await _prepare_statement(session)
    existing = await session.get(
        CustomImportWinner,
        (generation.generation_id, winner.profile_slot, winner.entity_binding_id, winner.context_key_sha256),
    )
    expected = CustomImportWinner(
        generation_id=generation.generation_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        profile_slot=winner.profile_slot,
        entity_binding_id=winner.entity_binding_id,
        family_revision_id=winner.family_revision_id,
        context_collection_slot=winner.context_collection_slot,
        context_key_sha256=winner.context_key_sha256,
        context_child_revision_id=winner.context_child_revision_id,
    )
    if existing is None:
        _page_cost(request, [expected])
        session.add(expected)
        await _flush_page(session)
    elif publication._model_document(existing) != publication._model_document(expected):
        raise CandidateRunnerError("winner retry differs from its complete candidate group")
    await _call(
        session,
        "commit_custom_import_build_winner_group",
        (build_id, context_id),
        ("profile_slot", "entity_binding_id", "context_key_sha256", "winner_count"),
    )


async def _write_winners(session_factory, request, registry, build_id, generation):
    while True:
        build = await _snapshot(session_factory, request, build_id)
        after = (
            None
            if build.output_after_profile_slot is None
            else (
                build.output_after_profile_slot,
                build.output_after_entity_binding_id,
                bytes(build.output_after_context_key_sha256),
            )
        )
        async with _session(session_factory) as session:

            def _reduce_next(sync_session):
                group = _next_group(sync_session, request, build_id, after)
                return (
                    None
                    if group is None
                    else _reduce_group(sync_session, request, registry, build_id, generation, group)
                )

            selected = await session.run_sync(_reduce_next)
        if selected is None:
            return
        winner, context_id = selected
        async with _page_session(session_factory, request, build_id) as (session, _build):
            await _store_winner(session, request, build_id, generation, winner, context_id)


def _verify_page(session, request, build_id):
    with _read_transaction(session, request, build_id) as (_build, deadline):
        declared_schema = CustomImportBuildAttempt.__table__.schema
        schema_map = session.connection().get_execution_options().get("schema_translate_map") or {}
        schema = schema_map.get(declared_schema, declared_schema)
        if not schema:
            raise CandidateRunnerError("build verification requires an explicit model schema")
        call = getattr(getattr(func, schema), "verify_custom_import_build_structure")(cast(build_id, BigInteger))
        result = session.execute(
            select(call.table_valued("verification_state", "scan_stage", "page_sequence", "rows_processed"))
        ).one()
        _require_budget(deadline)
        return result.verification_state


def _proof_matches(build, generation, proof):
    if (
        proof is None
        or proof.build_id != build.build_id
        or proof.generation_id != generation.generation_id
        or build.generation_id != generation.generation_id
        or build.phase != "verified"
        or proof.verification_state != "complete"
        or proof.scan_stage != "complete"
        or proof.verified_at is None
        or build.verified_at != proof.verified_at
        or any(
            getattr(build, name) != getattr(proof, name)
            for name in ("source_frozen_at", "graph_frozen_at", "output_frozen_at")
        )
        or any(
            getattr(build, name) != getattr(generation, name)
            for name in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "capture_bundle_id",
                "producing_fence",
                "producing_token_sha256",
            )
        )
    ):
        raise CandidateRunnerError("generation requires its exact complete frozen structural proof")


def _hash_models(session, request, build_id, digests, section, statement, keys, model):
    count = 0
    for (row,) in _read_rows(session, request, build_id, statement, keys, (model,)):
        for digest in digests:
            publication._add_digest_record(digest, section, publication._materialization_document(row))
        count += 1
    return count


def _schema_sections(request):
    slots, field, collection = CustomImportFieldSlot, CustomImportField, CustomImportChildCollection
    return (
        (
            "field_slot",
            select(slots).where(
                slots.dataset_id == request.dataset_id,
                exists(
                    select(1).where(
                        field.dataset_id == slots.dataset_id,
                        field.field_slot == slots.field_slot,
                        field.schema_revision_id == request.schema_revision_id,
                    )
                ),
            ),
            (slots.field_slot,),
            slots,
        ),
        (
            "field",
            select(field).where(
                field.dataset_id == request.dataset_id, field.schema_revision_id == request.schema_revision_id
            ),
            (field.collection_slot, field.field_slot),
            field,
        ),
        (
            "child_collection",
            select(collection).where(
                collection.dataset_id == request.dataset_id, collection.schema_revision_id == request.schema_revision_id
            ),
            (collection.collection_slot,),
            collection,
        ),
    )


def _shape_material(session, request, build_id, digests):
    stream, alias, profile = CustomImportSourceStream, CustomImportFieldAlias, CustomImportSelectionProfile
    sections = (
        *_schema_sections(request),
        (
            "source_stream",
            select(stream).where(stream.definition_revision_id == request.definition_revision_id),
            (stream.stream_slot,),
            stream,
        ),
        (
            "field_alias",
            select(alias).where(alias.definition_revision_id == request.definition_revision_id),
            (alias.stream_slot, alias.alias_name),
            alias,
        ),
        (
            "selection_profile",
            select(profile).where(profile.definition_revision_id == request.definition_revision_id),
            (profile.profile_slot,),
            profile,
        ),
    )
    profile_count = 0
    for section, statement, keys, model in sections:
        profile_count = _hash_models(session, request, build_id, digests, section, statement, keys, model)
    return profile_count


def _verify_projections(session, request, build_id, expected, statement, model, keys, *, reserve_bytes=0):
    actual_scalars = (
        row
        for (row,) in _read_rows(
            session, request, build_id, statement, keys, (model,), bounds=_ReadPage(reserve_bytes=reserve_bytes)
        )
    )
    for left, right in zip_longest(actual_scalars, expected):
        if (
            left is None
            or right is None
            or any(getattr(left, column.name) != getattr(right, column.name) for column in model.__table__.columns)
        ):
            raise CandidateRunnerError("typed scalar projection differs from the frozen payload")


def _verified_root(session, request, build_id, root, root_record, binding, retained_bytes):
    root_values = stored_root_values(request, root_record, root, binding)
    if (
        digest_text("root-payload", root.canonical_payload) != bytes(root.payload_sha256)
        or record_payload(request.definition.root_fields, root_values) != root.canonical_payload
    ):
        raise CandidateRunnerError("frozen root payload digest differs")
    scalars = material.project_root_scalars(
        request.definition,
        root_target=material.RootScalarTarget(
            request.dataset_id, request.schema_revision_id, root_record.root_record_id, root.root_revision_id
        ),
        root_values=root_values,
    )
    expected = material.scalar_projection_models(request.definition, root_scalars=scalars)
    _verify_projections(
        session,
        request,
        build_id,
        sorted(expected, key=lambda scalar: scalar.field_slot),
        select(CustomImportRootScalar).where(CustomImportRootScalar.root_revision_id == root.root_revision_id),
        CustomImportRootScalar,
        (CustomImportRootScalar.field_slot,),
        reserve_bytes=retained_bytes,
    )
    return root_values


def _verified_child_documents(session, request, registry, plan, root_record, retained_bytes, collection, child_counter):
    statement, keys = _child_statement(
        plan, request.definition, canonical=True, collection_slot=registry.child_collection_slots[collection]
    )
    for (child,) in _read_rows(
        session,
        request,
        plan.build_id,
        statement,
        keys,
        (CustomImportChildRevision,),
        bounds=_ReadPage(reserve_bytes=retained_bytes, row_limit=1),
    ):
        child_values = payload_values(
            fields_by_collection(request.definition)[collection], child.canonical_payload, label="frozen child payload"
        )
        verify_stored_child(request, root_record, StoredCandidateChild(collection, child, child_values))
        if digest_text("child-payload", child.canonical_payload) != bytes(child.payload_sha256):
            raise CandidateRunnerError("frozen child payload digest differs")
        scalars = material.project_child_scalars(
            request.definition,
            collection=collection,
            child_target=material.ChildScalarTarget(
                request.dataset_id,
                request.schema_revision_id,
                root_record.root_record_id,
                child.collection_slot,
                child.child_revision_id,
            ),
            child_values=child_values,
            child_collection_slots=registry.child_collection_slots,
        )
        expected = material.scalar_projection_models(
            request.definition, child_scalars=scalars, child_collection_slots=registry.child_collection_slots
        )
        _verify_projections(
            session,
            request,
            plan.build_id,
            sorted(expected, key=lambda scalar: scalar.field_slot),
            select(CustomImportChildScalar).where(CustomImportChildScalar.child_revision_id == child.child_revision_id),
            CustomImportChildScalar,
            (CustomImportChildScalar.field_slot,),
            reserve_bytes=retained_bytes + _model_bytes((child,)),
        )
        next(child_counter)
        yield bytes(child.child_key_sha256), child.canonical_child_key, child.canonical_payload


def _verify_family(session, request, registry, build_id, family, root, root_record, binding):
    retained_bytes = _model_bytes((family, root, root_record, binding))
    root_values = _verified_root(session, request, build_id, root, root_record, binding, retained_bytes)
    plan = _one_row(
        session,
        request,
        build_id,
        select(CustomImportBuildFamily).where(
            CustomImportBuildFamily.build_id == build_id,
            CustomImportBuildFamily.root_record_id == root_record.root_record_id,
        ),
        (CustomImportBuildFamily.root_record_id,),
        (CustomImportBuildFamily,),
    )[0]
    child_counter = count()
    documents_by_collection = {
        collection.name: _verified_child_documents(
            session, request, registry, plan, root_record, retained_bytes, collection.name, child_counter
        )
        for collection in request.definition.child_collections
    }
    digest = new_family_hash_ordered(request.definition, root_values, documents_by_collection)
    if next(child_counter) != family.child_count or digest != bytes(family.family_sha256):
        raise CandidateRunnerError("frozen family digest or child count differs")


def _family_material(session, request, registry, build_id, generation, digests):
    family_count = 0
    family_models = (
        CustomImportGenerationFamily,
        CustomImportFamilyRevision,
        CustomImportRootRevision,
        CustomImportRootRecord,
        CustomImportEntityBinding,
    )
    for family_record in _read_rows(
        session,
        request,
        build_id,
        publication._family_material_statement(generation),
        (CustomImportRootRecord.logical_key_sha256,),
        family_models,
        bounds=_ReadPage(row_limit=1),
    ):
        _membership, family, root, root_record, binding = family_record
        _verify_family(session, request, registry, build_id, family, root, root_record, binding)
        for index, digest in enumerate(digests):
            publication._add_family_revision_material(digest, family_record, {}, effective_output=bool(index))
        family_count += 1
    return family_count


def _child_material(session, request, build_id, generation, digests):
    child_count = 0
    child_models = (
        CustomImportGenerationFamily,
        CustomImportFamilyChild,
        CustomImportChildRevision,
        CustomImportRootRecord,
    )
    for _member, edge, child, root_record in _read_rows(
        session,
        request,
        build_id,
        publication._family_child_material_statement(generation),
        (
            CustomImportRootRecord.logical_key_sha256,
            CustomImportFamilyChild.collection_slot,
            CustomImportChildRevision.child_key_sha256,
        ),
        child_models,
    ):
        if child.canonical_parent_key != root_record.canonical_logical_key or bytes(child.parent_key_sha256) != bytes(
            root_record.logical_key_sha256
        ):
            raise CandidateRunnerError("frozen child parent differs from selected root")
        for index, digest in enumerate(digests):
            publication._add_digest_record(
                digest,
                "family_child",
                {
                    "child_key_sha256": bytes(child.child_key_sha256).hex(),
                    "collection_slot": edge.collection_slot,
                    "root_key_sha256": bytes(root_record.logical_key_sha256).hex(),
                },
            )
            document = (
                publication._effective_output_revision_document(child)
                if index
                else publication._materialization_document(child)
            )
            publication._add_digest_record(digest, "child_revision", document)
        child_count += 1
    return child_count


def _root_scalar_material(session, request, build_id, generation, digests):
    root_scalars = 0
    for _member, _family, scalar, root_record in _read_rows(
        session,
        request,
        build_id,
        publication._root_scalar_material_statement(generation),
        (CustomImportRootRecord.logical_key_sha256, CustomImportRootScalar.field_slot),
        (CustomImportGenerationFamily, CustomImportFamilyRevision, CustomImportRootScalar, CustomImportRootRecord),
    ):
        for digest in digests:
            publication._add_digest_record(
                digest,
                "root_scalar",
                {
                    "root_key_sha256": bytes(root_record.logical_key_sha256).hex(),
                    "scalar": publication._materialization_document(scalar),
                },
            )
        root_scalars += 1
    return root_scalars


def _child_scalar_material(session, request, build_id, generation, digests):
    child_scalars = 0
    for _member, _edge, scalar, child, root_record in _read_rows(
        session,
        request,
        build_id,
        publication._child_scalar_material_statement(generation),
        (
            CustomImportRootRecord.logical_key_sha256,
            CustomImportFamilyChild.collection_slot,
            CustomImportChildRevision.child_key_sha256,
            CustomImportChildScalar.field_slot,
        ),
        (
            CustomImportGenerationFamily,
            CustomImportFamilyChild,
            CustomImportChildScalar,
            CustomImportChildRevision,
            CustomImportRootRecord,
        ),
    ):
        for digest in digests:
            publication._add_digest_record(
                digest,
                "child_scalar",
                {
                    "child_key_sha256": bytes(child.child_key_sha256).hex(),
                    "root_key_sha256": bytes(root_record.logical_key_sha256).hex(),
                    "scalar": publication._materialization_document(scalar),
                },
            )
        child_scalars += 1
    return child_scalars


def _graph_material(session, request, registry, build_id, generation, material_digest, effective_digest):
    digests = (material_digest, effective_digest)
    return (
        _family_material(session, request, registry, build_id, generation, digests),
        _child_material(session, request, build_id, generation, digests),
        _root_scalar_material(session, request, build_id, generation, digests),
        _child_scalar_material(session, request, build_id, generation, digests),
    )


def _verify_winner_groups(session, request, registry, build_id, generation):
    context = CustomImportBuildCandidateContext
    previous = None
    count = 0
    while True:
        statement = select(context).where(context.build_id == build_id)
        if previous is not None:
            statement = statement.where(
                tuple_(context.profile_slot, context.entity_binding_id, context.context_key_sha256) > previous
            )
        group_context = _one_row(
            session,
            request,
            build_id,
            statement,
            (context.profile_slot, context.entity_binding_id, context.context_key_sha256, context.candidate_context_id),
            (context,),
        )
        if group_context is None:
            return count
        group = _group_key(group_context[0])
        winner, _context_id = _reduce_group(session, request, registry, build_id, generation, group)
        stored = _one_row(
            session,
            request,
            build_id,
            select(CustomImportWinner).where(
                CustomImportWinner.generation_id == generation.generation_id,
                CustomImportWinner.profile_slot == winner.profile_slot,
                CustomImportWinner.entity_binding_id == winner.entity_binding_id,
                CustomImportWinner.context_key_sha256 == winner.context_key_sha256,
            ),
            (
                CustomImportWinner.profile_slot,
                CustomImportWinner.entity_binding_id,
                CustomImportWinner.context_key_sha256,
            ),
            (CustomImportWinner,),
        )
        if stored is None or any(
            getattr(stored[0], name) != getattr(winner, name)
            for name in (
                "family_revision_id",
                "context_collection_slot",
                "context_child_revision_id",
                "entity_binding_id",
                "profile_slot",
                "context_key_sha256",
            )
        ):
            raise CandidateRunnerError("frozen winner differs from complete surviving-tie reduction")
        previous = group
        count += 1


def _winner_material(session, request, build_id, generation, digests):
    count = 0
    keys = (
        CustomImportWinner.profile_slot,
        CustomImportEntityBinding.adapter_id,
        CustomImportEntityBinding.value_sha256,
        CustomImportEntityBinding.canonical_value,
        CustomImportRootRecord.key_contract_sha256,
        CustomImportRootRecord.logical_key_sha256,
        func.coalesce(CustomImportChildRevision.child_key_sha256, b""),
        CustomImportFamilyRevision.family_sha256,
        CustomImportWinner.context_collection_slot,
        CustomImportWinner.context_key_sha256,
    )
    models = (
        CustomImportWinner,
        CustomImportEntityBinding,
        CustomImportFamilyRevision,
        CustomImportRootRecord,
        CustomImportChildRevision,
        CustomImportSelectionProfile,
    )
    for winner, binding, family, root, child, profile in _read_rows(
        session, request, build_id, publication._winner_material_statement(generation), keys, models
    ):
        if winner.context_collection_slot != (profile.context_collection_slot or 0):
            raise CandidateRunnerError("winner scope differs from immutable profile")
        document = publication._materialization_document(winner)
        document.update(
            context_child_key_sha256=None if child is None else bytes(child.child_key_sha256).hex(),
            family_sha256=bytes(family.family_sha256).hex(),
            root_key_sha256=bytes(root.logical_key_sha256).hex(),
        )
        for digest in digests:
            publication._add_digest_record(digest, "winner", document)
            publication._add_digest_record(digest, "winner_binding", publication._materialization_document(binding))
        count += 1
    return count


def _identity_material(session, request, build_id, generation, digest, effective):
    publication._add_digest_record(digest, "generation", publication._materialization_document(generation))
    for section, model, column, identity_id in (
        (
            "definition",
            CustomImportDefinitionRevision,
            CustomImportDefinitionRevision.definition_revision_id,
            request.definition_revision_id,
        ),
        (
            "schema",
            CustomImportSchemaRevision,
            CustomImportSchemaRevision.schema_revision_id,
            request.schema_revision_id,
        ),
        (
            "capture_bundle",
            CustomImportCaptureBundle,
            CustomImportCaptureBundle.capture_bundle_id,
            generation.capture_bundle_id,
        ),
    ):
        identity_record = _one_row(
            session, request, build_id, select(model).where(column == identity_id), (column,), (model,)
        )[0]
        for identity_digest in (digest,) if section == "capture_bundle" else (digest, effective):
            publication._add_digest_record(
                identity_digest, section, publication._materialization_document(identity_record)
            )
    _hash_models(
        session,
        request,
        build_id,
        (digest,),
        "capture",
        select(CustomImportCapture).where(CustomImportCapture.capture_bundle_id == generation.capture_bundle_id),
        (CustomImportCapture.stream_slot,),
        CustomImportCapture,
    )


def _attempt_material(session, request, build_id, digest):
    _hash_models(
        session,
        request,
        build_id,
        (digest,),
        "pack",
        select(CustomImportPack).where(
            CustomImportPack.execution_id == request.execution_id,
            CustomImportPack.producing_fence == request.fence,
            CustomImportPack.producing_token_sha256 == lease_token_sha256(request.lease_token),
        ),
        (CustomImportPack.stream_slot, CustomImportPack.pack_ordinal),
        CustomImportPack,
    )
    _hash_models(
        session,
        request,
        build_id,
        (digest,),
        "rejection",
        select(CustomImportRejection).where(
            CustomImportRejection.execution_id == request.execution_id,
            CustomImportRejection.producing_fence == request.fence,
            CustomImportRejection.producing_token_sha256 == lease_token_sha256(request.lease_token),
        ),
        (CustomImportRejection.rejection_ordinal,),
        CustomImportRejection,
    )


def _frozen_materialization(session, request, registry, build_id, generation, proof):
    with _read_transaction(session, request, build_id) as (build, _deadline):
        _proof_matches(build, generation, proof)
    source_digest = _source_digest(session, request, build_id, generation.capture_bundle_id)
    candidate_digest, candidate_count = _candidate_digest(session, request, build_id)
    if (
        source_digest != bytes(generation.source_bundle_sha256)
        or candidate_digest != bytes(generation.candidate_sha256)
        or candidate_count != generation.root_count
        or candidate_count != generation.family_count
    ):
        raise CandidateRunnerError("frozen generation header differs from its real source and selected roots")
    digest = publication._new_digest(publication._MATERIALIZATION_DOMAIN)
    effective = publication._new_digest(publication._EFFECTIVE_OUTPUT_DOMAIN)
    _identity_material(session, request, build_id, generation, digest, effective)
    profile_count = _shape_material(session, request, build_id, (digest, effective))
    _attempt_material(session, request, build_id, digest)
    family_count, child_count, root_scalars, child_scalars = _graph_material(
        session, request, registry, build_id, generation, digest, effective
    )
    expected_winners = _verify_winner_groups(session, request, registry, build_id, generation)
    winner_count = _winner_material(session, request, build_id, generation, (digest, effective))
    if winner_count != expected_winners:
        raise CandidateRunnerError("frozen winner count differs from complete candidate groups")
    verified_materialization = publication._Materialization(
        source_digest,
        digest.digest(),
        effective.digest(),
        family_count,
        family_count,
        family_count,
        child_count,
        winner_count,
        profile_count,
        root_scalars,
        child_scalars,
    )
    if any(getattr(verified_materialization, name) != getattr(proof, name) for name in _COUNT_NAMES):
        raise CandidateRunnerError("trusted materialization counts differ from protected structural proof")
    return verified_materialization


async def _unchanged_base(session, request, build, materialization):
    if build.base_generation_id is None:
        return None
    await _prepare_statement(session)
    base = await publication._locked_generation(
        session, dataset_id=request.dataset_id, generation_id=build.base_generation_id
    )
    await _prepare_statement(session)
    base_seal = await publication._validated_generation_seal(session, base)
    if not hmac.compare_digest(bytes(base_seal.effective_output_sha256), materialization.effective_output_sha256):
        return None
    await _prepare_statement(session)
    pointer = await publication._locked_pointer(session, request.dataset_id)
    if (
        pointer is None
        or pointer.generation_id != build.base_generation_id
        or pointer.pointer_version != build.base_pointer_version
    ):
        return None
    publication._require_expected_pointer(
        pointer, expected_generation_id=build.base_generation_id, expected_pointer_version=build.base_pointer_version
    )
    return base, base_seal


async def _finalize_sealed_build(session, request, build, generation, candidate_seal, unchanged_base, materialization):
    await _prepare_statement(session)
    execution = await session.get(CustomImportExecution, request.execution_id)
    await _prepare_statement(session)
    lease = await session.get(CustomImportLease, request.execution_id)
    seal_request = publication._GenerationSealRequest(
        request.dataset_id, generation.generation_id, request.fence, lease_token_sha256(request.lease_token)
    )
    no_change_request = None
    if unchanged_base is not None:
        base, base_seal = unchanged_base
        no_change_request = publication._NoChangeRequest(
            request.dataset_id,
            request.execution_id,
            build.base_generation_id,
            build.base_pointer_version,
            generation.generation_id,
            request.fence,
            lease_token_sha256(request.lease_token),
        )
        session.add(
            publication._new_no_change_seal(no_change_request, execution, base, generation, base_seal, candidate_seal)
        )
        await _flush_page(session)
    await _prepare_statement(session)
    now = await publication._database_now(session)
    publication._validate_generation_sealing_authority(execution, lease, generation, materialization, seal_request, now)
    if no_change_request is None:
        await publication._complete_generation_sealing_execution(session, execution, seal_request, now)
        await _prepare_statement(session)
        return None
    await publication._finalize_no_change_execution(session, no_change_request, now)
    event = publication._no_change_publication_event(no_change_request, unchanged_base[0])
    session.add(event)
    await _flush_page(session)
    return publication._receipt(event, replayed=False)


async def _terminal_seal(session_factory, request, build_id, materialization):
    async with _session(session_factory, transaction=True) as session:
        await session.execute(select(func.set_config("statement_timeout", str(request.statement_timeout_ms), True)))
        await publication._begin_finality_operation(session)
        build = await _lock_page(session, request, build_id)
        await _prepare_statement(session)
        generation = await session.get(CustomImportGeneration, build.generation_id, with_for_update=True)
        await _prepare_statement(session)
        proof = await session.get(CustomImportBuildVerification, build_id)
        _proof_matches(build, generation, proof)
        if any(getattr(materialization, name) != getattr(proof, name) for name in _COUNT_NAMES):
            raise CandidateRunnerError("final structural proof counts differ")
        unchanged_base = await _unchanged_base(session, request, build, materialization)
        await _prepare_statement(session)
        execution = await session.get(CustomImportExecution, request.execution_id)
        await _prepare_statement(session)
        lease = await session.get(CustomImportLease, request.execution_id)
        seal_request = publication._GenerationSealRequest(
            request.dataset_id, generation.generation_id, request.fence, lease_token_sha256(request.lease_token)
        )
        await _prepare_statement(session)
        now = await publication._database_now(session)
        publication._validate_generation_sealing_authority(
            execution, lease, generation, materialization, seal_request, now
        )
        candidate_seal = publication._new_generation_seal(generation, materialization, seal_request)
        session.add(candidate_seal)
        await _flush_page(session)
        no_change = await _finalize_sealed_build(
            session, request, build, generation, candidate_seal, unchanged_base, materialization
        )
        return BuildOutputResult(
            generation.generation_id, publication._seal_receipt(candidate_seal, replayed=False), no_change
        )


async def _replay(session_factory, request, build_id):
    async with _session(session_factory, transaction=True) as session:
        await session.execute(select(func.set_config("statement_timeout", str(request.statement_timeout_ms), True)))
        await publication._begin_finality_operation(session)
        await publication._locked_dataset(session, request.dataset_id)
        execution = await publication._locked_execution(
            session, execution_id=request.execution_id, dataset_id=request.dataset_id
        )
        await publication._locked_lease(session, request.execution_id)
        build = await session.get(CustomImportBuildAttempt, build_id, with_for_update=True)
        _verify_request(build, request, execution)
        if build.generation_id is None:
            return None
        generation = await publication._locked_generation(
            session, dataset_id=request.dataset_id, generation_id=build.generation_id
        )
        seal = await publication._locked_generation_seal(session, build.generation_id)
        if seal is None:
            return None
        if execution.state == "no_change":
            no_change_request = publication._NoChangeRequest(
                request.dataset_id,
                request.execution_id,
                build.base_generation_id,
                build.base_pointer_version,
                build.generation_id,
                request.fence,
                lease_token_sha256(request.lease_token),
            )
            receipt = await publication._replayed_no_change_receipt(session, no_change_request, execution)
            if receipt is None:
                raise CandidateRunnerError("terminal no-change execution has no receipt")
            return BuildOutputResult(build.generation_id, publication._seal_receipt(seal, replayed=True), receipt)
        if execution.state != "completed":
            raise CandidateRunnerError("sealed build producer is not terminal")
        seal_request = publication._GenerationSealRequest(
            request.dataset_id, build.generation_id, request.fence, lease_token_sha256(request.lease_token)
        )
        return BuildOutputResult(
            build.generation_id, publication._generation_seal_replay(seal, generation, seal_request)
        )


async def build_output(session_factory, request: SourceBuildRequest, build_id: int) -> BuildOutputResult:
    """Select all complete groups, _verify frozen content, and seal without activation."""

    replay = await _replay(session_factory, request, build_id)
    if replay is not None:
        return replay
    async with _renew_while_reading(session_factory, request):
        build = await _snapshot(session_factory, request, build_id)
        if build.phase not in {"output", "verifying", "verified"} or build.generation_id is None:
            raise CandidateRunnerError("output requires an atomically opened generation")
        async with _page_session(session_factory, request, build_id) as (session, _build):
            registry = await load_registry(session, request)
            await _prepare_statement(session)
            generation = await session.get(CustomImportGeneration, build.generation_id)
            session.expunge(generation)
        if build.phase == "output":
            await _attach_families(session_factory, request, build_id)
            await _write_winners(session_factory, request, registry, build_id, generation)
            async with _page_session(session_factory, request, build_id) as (session, _build):
                await _call(session, "freeze_custom_import_build_output", (build_id,))
        while True:
            current = await _snapshot(session_factory, request, build_id)
            if current.phase == "verified":
                break
            async with _session(session_factory) as session:
                await session.run_sync(lambda sync: _verify_page(sync, request, build_id))
            await _heartbeat(session_factory, request)
        async with _session(session_factory) as session:

            def _verify(sync_session):
                proof = _one_row(
                    sync_session,
                    request,
                    build_id,
                    select(CustomImportBuildVerification).where(CustomImportBuildVerification.build_id == build_id),
                    (CustomImportBuildVerification.build_id,),
                    (CustomImportBuildVerification,),
                )[0]
                return _frozen_materialization(sync_session, request, registry, build_id, generation, proof)

            materialization = await session.run_sync(_verify)
    return await _terminal_seal(session_factory, request, build_id, materialization)
