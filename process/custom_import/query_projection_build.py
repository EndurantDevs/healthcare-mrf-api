# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fenced, bounded context writes after winners and before normal output freeze."""

from dataclasses import dataclass

from sqlalchemy import func, select, text, tuple_

from db.models.custom_import import CustomImportGeneration, CustomImportWinner
from process.custom_import.admission_sql import _verify_writer_permit
from process.custom_import.build_graph import _build_storage_models
from process.custom_import.build_source import _flush_page, _page_session, _prepare_statement
from process.custom_import.query_projection_authorization import (
    PROJECTION_PATH,
    PROJECTION_PERMIT_CONTRACT,
    ProjectionPermit,
)
from process.custom_import.query_projection_binding import validate_projection_indexes
from process.custom_import.query_projection_initialize import initialize_projection
from process.custom_import.query_projection_pages import cell_insert_statement, context_key_statement
from process.custom_import.query_projection_sources import (
    ProjectionSource,
    child_hash_collision_statement,
    child_source,
    field_slots,
    root_source,
)
from process.custom_import.query_projection_storage import (
    query_projection_index_statements,
    query_projection_tables,
)
from process.custom_import.query_projection_values import ambiguity_statement, configured_fields
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError

_MAX_INTEGER = 2**63 - 1


@dataclass(frozen=True)
class ProjectionProgress:
    """The complete committed position; every coordinate participates in retries."""

    phase: str
    after_entity_binding_id: int
    after_selected_value: int | None
    after_child_key_sha256: str | None
    index_count: int
    root_count: int
    child_count: int
    proof_count: int = 0

    @classmethod
    def from_manifest(cls, manifest):
        """Detach the retained cursor from its owning transaction."""

        progress_by_name = {name: getattr(manifest, name) for name in cls.__dataclass_fields__ if name != "proof_count"}
        progress_by_name["proof_count"] = sum(
            getattr(manifest, name)
            for name in ("verified_root_count", "verified_child_count", "digested_root_count", "digested_child_count")
        )
        if progress_by_name["after_child_key_sha256"] is not None:
            progress_by_name["after_child_key_sha256"] = bytes(progress_by_name["after_child_key_sha256"]).hex()
        return cls(**progress_by_name)


@dataclass(frozen=True)
class ProjectionBatchReceipt:
    """An exact before/after pair returned only after the transaction commits."""

    execution_id: int
    build_id: int
    fence: int
    generation_id: int
    capture_bundle_id: int
    before: ProjectionProgress
    after: ProjectionProgress


class ProjectionCursorConflict(CandidateRunnerError):
    """Reconcile retained progress before attempting any further operation."""


async def _execute(session, statement):
    await _prepare_statement(session)
    return await session.execute(statement)


def _advance_phase(manifest):
    manifest.phase = {
        "root": "child",
        "child": "indexes",
        "indexes": "verify_root",
        "verify_root": "verify_child",
        "verify_child": "digest_root",
        "digest_root": "digest_child",
    }[manifest.phase]
    manifest.after_entity_binding_id = 0
    manifest.after_selected_value = None
    manifest.after_child_key_sha256 = None


def _cursor(manifest):
    if manifest.after_entity_binding_id == 0:
        return None
    cursor = (manifest.after_entity_binding_id, manifest.after_selected_value)
    if manifest.phase == "child":
        cursor += (bytes(manifest.after_child_key_sha256),)
    return cursor


async def _source_context(session, request, build):
    await _prepare_statement(session)
    candidate_models, _base_models = await session.run_sync(lambda sync: _build_storage_models(sync, build.build_id))
    selection = request.definition.query.entity_selection
    profile_slot = next(
        slot
        for slot, profile in enumerate(request.definition.selection_profiles, 1)
        if profile.profile_id == selection.by_value_profile
    )
    return ProjectionSource(
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        build.generation_id,
        profile_slot,
        request.definition,
        candidate_models,
    )


async def _entity_window(session, context, manifest, limit):
    winner = context.model(CustomImportWinner)
    is_entity_exhausted = manifest.after_selected_value == _MAX_INTEGER and (
        manifest.phase in {"root", "verify_root", "verify_child"} or manifest.after_child_key_sha256 == b"\xff" * 32
    )
    boundary = (
        winner.entity_binding_id > manifest.after_entity_binding_id
        if is_entity_exhausted
        else winner.entity_binding_id >= max(1, manifest.after_entity_binding_id)
    )
    statement = (
        select(winner.entity_binding_id)
        .where(
            winner.generation_id == context.generation_id,
            winner.dataset_id == context.dataset_id,
            winner.definition_revision_id == context.definition_revision_id,
            winner.schema_revision_id == context.schema_revision_id,
            winner.profile_slot == context.profile_slot,
            winner.context_collection_slot == 0,
            winner.context_child_revision_id.is_(None),
            boundary,
        )
        .distinct()
        .order_by(winner.entity_binding_id)
        .limit(limit)
    )
    return (await _execute(session, statement)).scalars().all()


async def _child_collection_slot(session, context):
    await _prepare_statement(session)
    registry = await load_registry(session, context)
    return registry.child_collection_slots[context.definition.query.child_collection]


async def _validated_source(session, context, entity_ids, *, child, collection_slot=None):
    roots, root_values = root_source(context, entity_ids)
    if (await _execute(session, ambiguity_statement(roots).limit(1))).first() is not None:
        raise CandidateRunnerError("query projection root context is ambiguous")
    if not child:
        return roots, root_values
    if collection_slot is None:
        collection_slot = await _child_collection_slot(session, context)
    source, values_by_field = child_source(context, roots, collection_slot)
    # Validate every child of these entities, including keys outside the page.
    for statement in (ambiguity_statement(source), child_hash_collision_statement(source)):
        if (await _execute(session, statement.limit(1))).first() is not None:
            raise CandidateRunnerError("query projection complete child key is ambiguous")
    return source, values_by_field


async def _bounded_key_metadata(session, source, request, manifest):
    is_child = manifest.phase == "child"
    contexts = context_key_statement(source, child=is_child, after=_cursor(manifest), limit=request.page_row_limit).cte(
        "projection_page_keys"
    )
    identities = [contexts.c.entity_binding_id, contexts.c.selected_value]
    if is_child:
        identities.append(contexts.c.child_key_sha256)
    # Fetch sizes first; canonical text cannot bypass a caller's byte bound.
    byte_size = 64 + 40 * len(configured_fields(request.definition))
    size = byte_size + func.octet_length(contexts.c.canonical_child_key) if is_child else byte_size
    metadata = (await _execute(session, select(*identities, size).order_by(*identities))).all()
    accepted, used = [], 0
    for row in metadata:
        if not accepted and row[-1] > request.page_byte_limit:
            raise CandidateRunnerError("one query projection context exceeds the byte page")
        if used + row[-1] > request.page_byte_limit:
            break
        accepted.append(tuple(row[:-1]))
        used += row[-1]
    return accepted


async def _write_contexts(session, request, build, manifest):
    is_child = manifest.phase == "child"
    fields = tuple(
        field for field in configured_fields(request.definition) if (field.collection is not None) == is_child
    )
    if not fields:
        _advance_phase(manifest)
        return
    context = await _source_context(session, request, build)
    entity_ids = await _entity_window(session, context, manifest, request.page_row_limit)
    if not entity_ids:
        _advance_phase(manifest)
        return
    collection_slot = await _child_collection_slot(session, context) if is_child else None
    discovery, _ = root_source(context, entity_ids)
    if is_child:
        discovery, _ = child_source(context, discovery, collection_slot)
    keys = await _bounded_key_metadata(session, discovery, request, manifest)
    # Include skipped entities, but do not repeatedly validate the unvisited tail.
    validation_ids = [entity_id for entity_id in entity_ids if not keys or entity_id <= keys[-1][0]]
    source_relation, values_by_field = await _validated_source(
        session, context, validation_ids, child=is_child, collection_slot=collection_slot
    )
    if not keys:
        # Empty/unknown groups and exhausted children must not hide later entities.
        manifest.after_entity_binding_id = entity_ids[-1]
        manifest.after_selected_value = _MAX_INTEGER
        manifest.after_child_key_sha256 = b"\xff" * 32 if is_child else None
        return
    if is_child:
        identities = [source_relation.c[name] for name in ("entity_binding_id", "selected_value", "child_key_sha256")]
        statement = (
            select(*identities, source_relation.c.canonical_child_key)
            .where(tuple_(*identities).in_(keys))
            .distinct()
            .order_by(*identities)
        )
        keys = [tuple(context_row) for context_row in (await _execute(session, statement)).all()]
    table = query_projection_tables(manifest.family_id, request.definition)[
        "custom_import_derived_child" if is_child else "custom_import_derived_root"
    ]
    insert_result = await _execute(
        session,
        cell_insert_statement(
            table,
            source_relation,
            values_by_field=values_by_field,
            fields=fields,
            field_slots=field_slots(request.definition),
            keys=keys,
        ),
    )
    if insert_result.rowcount != len(keys):
        raise CandidateRunnerError("query projection context coverage differs")
    manifest.after_entity_binding_id, manifest.after_selected_value = keys[-1][:2]
    manifest.after_child_key_sha256 = keys[-1][2] if is_child else None
    count_name = "child_count" if is_child else "root_count"
    setattr(manifest, count_name, getattr(manifest, count_name) + len(keys))


def _match_progress(before, expected):
    if expected is None:
        return
    if isinstance(expected, ProjectionProgress):
        is_matching = before == expected
    else:
        is_matching = all(
            getattr(before, name) == getattr(expected, f"expected_{name}")
            for name in (
                "phase",
                "after_entity_binding_id",
                "after_selected_value",
                "after_child_key_sha256",
                "index_count",
                "proof_count",
            )
        )
    if not is_matching:
        raise ProjectionCursorConflict("query projection progress conflict")


async def projection_step(session_factory, request, build_id, *, expected=None, permit=None):
    """Commit one data or index page with the existing locks and fresh lease check."""

    async with _page_session(session_factory, request, build_id) as (session, build):
        if permit is not None:
            await _verify_writer_permit(
                session, request, permit, (ProjectionPermit, PROJECTION_PERMIT_CONTRACT, PROJECTION_PATH)
            )
        manifest = await initialize_projection(session, request, build)
        before = ProjectionProgress.from_manifest(manifest)
        _match_progress(before, expected)
        await _advance_projection(session, request, build, manifest)
        await _flush_page(session)
        receipt = ProjectionBatchReceipt(
            request.execution_id,
            build_id,
            request.fence,
            build.generation_id,
            build.capture_bundle_id,
            before,
            ProjectionProgress.from_manifest(manifest),
        )
    return receipt


async def _advance_projection(session, request, build, manifest):
    if manifest.phase in {"root", "child"}:
        await _write_contexts(session, request, build, manifest)
        return
    if manifest.phase == "indexes":
        statements = query_projection_index_statements(manifest.family_id, request.definition)
        if manifest.index_count < len(statements):
            await _execute(session, text(statements[manifest.index_count]))
            manifest.index_count += 1
        else:
            await validate_projection_indexes(session, manifest, request.definition)
            _advance_phase(manifest)
        return
    if manifest.phase in {"verify_root", "verify_child"}:
        await _verify_contexts(session, request, build, manifest)
        return
    if manifest.phase in {"digest_root", "digest_child"}:
        from process.custom_import.query_projection_digest import digest_projection_page

        if await digest_projection_page(session, request, build, manifest):
            await _finish_projection(session, request, build, manifest)


async def _verify_contexts(session, request, build, manifest):
    from process.custom_import.query_projection_verify import _verify_window

    is_child = manifest.phase == "verify_child"
    fields = tuple(
        field for field in configured_fields(request.definition) if (field.collection is not None) == is_child
    )
    if not fields:
        _advance_phase(manifest)
        return
    context = await _source_context(session, request, build)
    entity_ids = await _entity_window(session, context, manifest, request.page_row_limit)
    if not entity_ids:
        _advance_phase(manifest)
        return
    expected, actual = await _verify_window(session, request, context, manifest, entity_ids, fields, child=is_child)
    if expected != actual:
        raise CandidateRunnerError("query projection verified coverage differs")
    name = "verified_child_count" if is_child else "verified_root_count"
    setattr(manifest, name, getattr(manifest, name) + expected)
    manifest.after_entity_binding_id = entity_ids[-1]
    manifest.after_selected_value = _MAX_INTEGER


async def _finish_projection(session, request, build, manifest):
    from process.custom_import.query_projection_digest import retained_projection_digest
    from process.custom_import.query_projection_evidence import completed_projection_evidence

    evidence = retained_projection_digest(manifest, request.definition).finish()
    await validate_projection_indexes(session, manifest, request.definition)
    for scope, table in zip(
        ("root", "child"), query_projection_tables(manifest.family_id, request.definition).values(), strict=True
    ):
        count = (await _execute(session, select(func.count()).select_from(table))).scalar_one()
        if not all(
            count == getattr(manifest, name)
            for name in (f"{scope}_count", f"verified_{scope}_count", f"digested_{scope}_count")
        ):
            raise CandidateRunnerError("query projection final coverage differs")
    manifest.projection_sha256 = evidence.projection_sha256
    manifest.phase = "complete"
    manifest.complete_at = func.clock_timestamp()
    await _flush_page(session)
    await _prepare_statement(session)
    await session.refresh(manifest, attribute_names=["complete_at"])
    generation = (
        await _execute(
            session, select(CustomImportGeneration).where(CustomImportGeneration.generation_id == build.generation_id)
        )
    ).scalar_one()
    if completed_projection_evidence(manifest, generation=generation, definition=request.definition) != evidence:
        raise CandidateRunnerError("query projection completed evidence differs")


async def stage_query_projections(session_factory, request, build_id, *, transport=None):
    """Prepare only enabled capabilities, without changing publication or activation."""

    if not configured_fields(request.definition):
        return
    if transport is not None:
        request = transport.bind_request(request)
    while True:
        if transport is None:
            receipt = await projection_step(session_factory, request, build_id)
        else:
            from process.custom_import.query_projection_worker import projection_next_batch

            receipt = await projection_next_batch(session_factory, request, build_id, transport)
        if receipt.after.phase == "complete":
            return


async def serve_projection_batch(session_factory, verified):
    """Load retained policy, then recheck the original signed purpose under locks."""

    from process.custom_import.source_batch import _load_request, _read_session

    pins = verified.pins
    async with _read_session(session_factory) as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        request, _capture_policy, _bundle_id = await _load_request(
            session, pins.execution_id, pins.build_id, pins.fence, verified.token.value, verified.permit
        )
    return await projection_step(session_factory, request, pins.build_id, expected=pins, permit=verified.permit)
