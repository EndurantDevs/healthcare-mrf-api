# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen bounded winner verification and ordinary generation finality."""

from __future__ import annotations

import hmac
from collections import deque
from contextlib import closing
from dataclasses import dataclass, replace
from itertools import chain, groupby, islice

from sqlalchemy import (
    BigInteger,
    LargeBinary,
    SmallInteger,
    any_,
    bindparam,
    cast,
    exists,
    func,
    literal_column,
    select,
    tuple_,
)
from sqlalchemy import column as sql_column
from sqlalchemy.dialects.postgresql import ARRAY

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
    _build_storage_models,
    _call,
    _candidate,
    _candidate_digest,
    _FamilyInput,
    _heartbeat,
    _model_bytes,
    _one_row,
    _prepare_read,
    _read_query_pages,
    _read_rows,
    _read_transaction,
    _ReadPage,
    _renew_while_reading,
    _require_budget,
    _session,
    _snapshot,
    _source_digest,
    _verify_request,
    _variable_bytes,
)
from process.custom_import.build_graph_prepare_page import _source_input_with_digest
from process.custom_import.build_source import (
    SourceBuildRequest,
    _flush_page,
    _lock_page,
    _page_session,
    _prepare_snapshot_indexes,
    _prepare_statement,
)
from process.custom_import.build_source import _call as _typed_call
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_codec import (
    digest_text,
    fields_by_collection,
    payload_values,
    record_payload,
)
from process.custom_import.runner_graph import stored_root_values
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError

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
# Native winner fields, five lookup arrays, ordinal/ID result and ID checks.
_WINNER_BUFFER_BYTES = 208
_LOOKUP_ARRAY_HEADERS = 116  # Five array headers and one defensive overflow row.


class _WinnerBatchFull(Exception):
    """Stop before completing the current group, never manufacture its EOF."""


@dataclass
class _WinnerReadBudget:
    winners: list
    pending_bytes: int = 0


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


def _output_rows(session, request, build_id, query, *, bounds=_ReadPage()):
    """Resolve the protected candidate and build its query inside every page."""

    return _read_query_pages(
        session,
        request,
        build_id,
        lambda sync: query(_build_storage_models(sync, build_id)[0]),
        bounds=replace(bounds, physical=True),
    )


def _winner_buffer_bytes(canonical_key, profile_id):
    if type(canonical_key) is not str or type(profile_id) is not str:
        raise CandidateRunnerError("winner context has invalid native text")
    return _WINNER_BUFFER_BYTES + len(canonical_key.encode("utf-8")) + len(profile_id.encode("utf-8"))


def _context_key(profile_slot, binding_id, digest, context_id):
    if (
        any(type(identity) is not int or not 0 < identity < 2**63 for identity in (binding_id, context_id))
        or type(profile_slot) is not int
        or not 0 < profile_slot < 2**15
        or not isinstance(digest, (bytes, bytearray, memoryview))
        or len(digest) != 32
    ):
        raise CandidateRunnerError("candidate context has invalid native identity")
    return profile_slot, binding_id, bytes(digest), context_id


def _context_capacity(read_budget):
    """Reserve retained winners and the current group before another read."""
    reserved_bytes = (
        _LOOKUP_ARRAY_HEADERS
        + read_budget.pending_bytes
        + sum(_winner_buffer_bytes(winner.canonical_context_key, winner.profile_id) for winner in read_budget.winners)
    )
    # Each winner reserves its object, native-array input and ID result. A read
    # also holds metadata and payload; reserve a pending group across chunks.
    remaining_rows = MAX_BATCH_ROWS - 3 * len(read_budget.winners) - 1
    remaining_bytes = MAX_BATCH_BYTES - reserved_bytes
    return remaining_rows // 5, remaining_bytes


def _context_batch_full(read_budget):
    if read_budget.winners:
        raise _WinnerBatchFull
    raise CandidateRunnerError("one candidate context exceeds the admitted physical batch")


def _admitted_context_keys(metadata, request, read_budget, available_bytes, fixed_bytes):
    """Pack policy-sized logical pages without exceeding the shared envelope."""
    admitted, used_bytes, logical_rows, logical_bytes = [], 0, 0, 0
    for *identity, payload_bytes, key_bytes in metadata:
        key = _context_key(*identity)
        if (
            type(payload_bytes) is not int
            or payload_bytes < 0
            or type(key_bytes) is not int
            or not 1 <= key_bytes <= 8192
            or key[0] > len(request.definition.selection_profiles)
        ):
            raise CandidateRunnerError("candidate context has invalid native metadata")
        if payload_bytes > request.page_byte_limit:
            raise CandidateRunnerError("one build record exceeds the admitted byte page")
        if logical_rows == request.page_row_limit or logical_bytes + payload_bytes > request.page_byte_limit:
            logical_rows, logical_bytes = 0, 0
        profile = request.definition.selection_profiles[key[0] - 1]
        size = fixed_bytes + payload_bytes + _WINNER_BUFFER_BYTES + key_bytes + len(profile.profile_id.encode("utf-8"))
        if used_bytes + size > available_bytes:
            break
        admitted.append(key)
        used_bytes += size
        logical_rows += 1
        logical_bytes += payload_bytes
    if metadata and not admitted:
        _context_batch_full(read_budget)
    return admitted


def _read_context_batch(session, request, build_id, query, after, read_budget):
    """Resolve, size and validate one native-array read before leaving its TX."""
    limit, available_bytes = _context_capacity(read_budget)
    # Four ordered key fields and two sizes; digest bytes are additional.
    metadata_bytes = 6 * 16 + 32
    if limit <= 0 or available_bytes <= 0:
        _context_batch_full(read_budget)
    with _read_transaction(session, request, build_id) as (_build, deadline):
        models = _build_storage_models(session, build_id)[0]
        statement, keys, row_models = query(models)
        context = models[CustomImportBuildCandidateContext]
        # Fixed native fields, both ordered-key copies and the ID selection;
        # every selected variable-width value is charged separately below.
        fixed_bytes = 64 + 16 * (2 * len(keys) + 2 + len(statement.selected_columns))
        limit = min(limit, available_bytes // (metadata_bytes + fixed_bytes + _WINNER_BUFFER_BYTES + 1))
        if not limit:
            _context_batch_full(read_budget)
        if after is not None:
            statement = statement.where(tuple_(*keys) > after)
        _prepare_read(session, request, deadline)
        metadata = session.execute(
            statement.with_only_columns(
                *keys,
                _variable_bytes(*row_models),
                func.octet_length(context.canonical_context_key),
                maintain_column_froms=True,
            )
            .order_by(*keys)
            .limit(limit)
        ).all()
        if len(metadata) > limit:
            raise CandidateRunnerError("candidate context metadata exceeds its physical batch")
        admitted = _admitted_context_keys(
            metadata, request, read_budget, available_bytes - len(metadata) * metadata_bytes, fixed_bytes
        )
        if not admitted:
            return (), None
        identifiers = tuple(key[-1] for key in admitted)
        if len(set(identifiers)) != len(identifiers):
            raise CandidateRunnerError("candidate context identities are not unique")
        _prepare_read(session, request, deadline)
        context_records = session.execute(
            statement.where(
                context.candidate_context_id == any_(bindparam("context_ids", identifiers, type_=ARRAY(BigInteger)))
            )
            .order_by(*keys)
            .limit(literal_column(str(len(admitted) + 1)))
        ).all()
        actual_keys = [
            _context_key(
                context_record[0].profile_slot,
                context_record[0].entity_binding_id,
                context_record[0].context_key_sha256,
                context_record[0].candidate_context_id,
            )
            for context_record in context_records
        ]
        if actual_keys != admitted:
            raise CandidateRunnerError("frozen build page changed during its read")
    return context_records, admitted[-1]


def _context_rows(session, request, build_id, *, group=None, after=None, generation=None, read_budget=None):
    read_budget = _WinnerReadBudget([]) if read_budget is None else read_budget
    query = lambda models: _context_query(models, build_id, group=group, after=after, generation=generation)
    cursor = None
    while True:
        context_records, cursor = _read_context_batch(session, request, build_id, query, cursor, read_budget)
        if not context_records:
            return
        for context_record in context_records:
            _require_budget(session.info["custom_import_build_read_deadline"])
            yield context_record
            _require_budget(session.info["custom_import_build_read_deadline"])
        del context_records, context_record


def _context_query(models, build_id, *, group=None, after=None, generation=None):
    context = models[CustomImportBuildCandidateContext]
    family = models[CustomImportFamilyRevision]
    root = models[CustomImportRootRevision]
    child = models[CustomImportChildRevision]
    statement = (
        select(context, family, root, child)
        .join(family, family.family_revision_id == context.family_revision_id)
        .join(root, root.root_revision_id == family.root_revision_id)
        .outerjoin(child, child.child_revision_id == context.context_child_revision_id)
        .where(context.build_id == build_id)
    )
    group_columns = (context.profile_slot, context.entity_binding_id, context.context_key_sha256)
    if group is not None:
        statement = statement.where(tuple_(*group_columns) == group)
    if after is not None:
        statement = statement.where(tuple_(*group_columns) > after)
    if generation is not None:
        winner = models[CustomImportWinner]
        statement = statement.add_columns(
            winner.family_revision_id, winner.context_collection_slot, winner.context_child_revision_id
        ).outerjoin(
            winner,
            (winner.generation_id == generation.generation_id)
            & (winner.profile_slot == context.profile_slot)
            & (winner.entity_binding_id == context.entity_binding_id)
            & (winner.context_key_sha256 == context.context_key_sha256),
        )
    return statement, (*group_columns, context.candidate_context_id), (context, family, root, child)


def _context_candidates(session, request, registry, build_id, group, *, context_records=None, contract=None):
    contract = contract or material._winner_candidate_contract(
        request.definition, material._profile_scopes(request.definition, registry.child_collection_slots)
    )
    profile = request.definition.selection_profiles[group[0] - 1]
    if context_records is None:
        context_records = _context_rows(session, request, build_id, group=group)
    for context_record in context_records:
        context_row, family_row, root_row, child_row = context_record[:4]
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


def _complete_group_winner(request, registry, generation, group, candidates):
    identity = _generation_identity(generation)
    winners = material.iter_ordered_profile_winners(
        request.definition,
        generation=identity,
        profile_slot=group[0],
        candidates=material.ValidatedWinnerCandidateStream(identity, candidates),
        child_collection_slots=registry.child_collection_slots,
    )
    winner = next(winners, None)
    if winner is None or next(winners, None) is not None:
        raise CandidateRunnerError("one complete candidate group must produce one winner")
    return winner


def _ordered_winners(session, request, registry, build_id, generation, *, after=None, verify=False, read_budget=None):
    """Reduce complete groups in one bounded scan; restart after writes from the committed group cursor."""

    read_budget = _WinnerReadBudget([]) if read_budget is None else read_budget
    contract = material._winner_candidate_contract(
        request.definition, material._profile_scopes(request.definition, registry.child_collection_slots)
    )
    with closing(
        _context_rows(
            session, request, build_id, after=after, generation=generation if verify else None, read_budget=read_budget
        )
    ) as context_records:
        for group, group_rows in groupby(context_records, key=lambda context_record: _group_key(context_record[0])):
            first = next(group_rows)
            stored_identity = first[4:]
            read_budget.pending_bytes = _winner_buffer_bytes(
                first[0].canonical_context_key, request.definition.selection_profiles[group[0] - 1].profile_id
            )
            candidates = _context_candidates(
                session,
                request,
                registry,
                build_id,
                group,
                context_records=chain((first,), group_rows),
                contract=contract,
            )
            del first
            winner = _complete_group_winner(request, registry, generation, group, candidates)
            if verify and tuple(stored_identity) != (
                winner.family_revision_id,
                winner.context_collection_slot,
                winner.context_child_revision_id,
            ):
                raise CandidateRunnerError("frozen winner differs from complete surviving-tie reduction")
            yield winner
            read_budget.pending_bytes = 0


async def _attach_families(session_factory, request, build_id):
    after_root_id = 0
    while True:
        async with _page_session(session_factory, request, build_id) as (session, _build):
            page = (
                await _typed_call(
                    session,
                    "membership_batch_finalize",
                    (("bigint", build_id), ("bigint", after_root_id)),
                )
            ).one()
        if not page.rows_processed:
            return
        after_root_id = page.after_root_record_id
        await _heartbeat(session_factory, request)


def _winner_batch(session, request, registry, build_id, generation, after):
    """Keep complete-group pages inside the existing bounded batch envelope.

    Logical policy pages share bounded native reads and one ID lookup. Close
    reduction before resolving IDs, and all reads before the next write page.
    """

    page_limit = min(256, request.page_row_limit, max(1, request.page_byte_limit // 32))
    read_budget, retained_bytes = _WinnerReadBudget([]), _LOOKUP_ARRAY_HEADERS
    winners = read_budget.winners
    try:
        with closing(
            _ordered_winners(session, request, registry, build_id, generation, after=after, read_budget=read_budget)
        ) as stream:
            for winner in islice(stream, MAX_BATCH_ROWS // 3):
                if request.page_byte_limit < 32:
                    raise CandidateRunnerError("one winner exceeds the admitted byte page")
                size = _winner_buffer_bytes(winner.canonical_context_key, winner.profile_id)
                if size + _LOOKUP_ARRAY_HEADERS > MAX_BATCH_BYTES:
                    raise CandidateRunnerError("one winner exceeds the admitted batch bytes")
                if retained_bytes + size > MAX_BATCH_BYTES:
                    break
                winners.append(winner)
                retained_bytes += size
    except _WinnerBatchFull:
        if not winners:
            raise CandidateRunnerError("winner batch ended without a complete group") from None
    if not winners:
        return (), ()
    page_sizes = []
    for start in range(0, len(winners), page_limit):
        page_sizes.append(min(page_limit, len(winners) - start))
    return _selected_winner_context_ids(session, request, build_id, winners), tuple(page_sizes)


def _selected_winner_context_ids(session, request, build_id, winners):
    """Resolve one closed reduction batch with five native arrays, not VALUES."""

    # Resolve this bounded selection in one join, after complete reduction has
    # closed its read stream. NULL child IDs compare by physical identity too.
    if not winners:
        return ()
    if (
        3 * len(winners) > MAX_BATCH_ROWS
        or _LOOKUP_ARRAY_HEADERS
        + sum(_winner_buffer_bytes(winner.canonical_context_key, winner.profile_id) for winner in winners)
        > MAX_BATCH_BYTES
    ):
        raise CandidateRunnerError("winner lookup exceeds its physical batch")
    for winner in winners:
        _context_key(
            winner.profile_slot, winner.entity_binding_id, winner.context_key_sha256, winner.family_revision_id
        )
        if winner.context_child_revision_id is not None and (
            type(winner.context_child_revision_id) is not int or not 0 < winner.context_child_revision_id < 2**63
        ):
            raise CandidateRunnerError("selected winner has invalid native child identity")
    chosen = (
        func.unnest(
            bindparam("profile_slots", [winner.profile_slot for winner in winners], type_=ARRAY(SmallInteger)),
            bindparam("binding_ids", [winner.entity_binding_id for winner in winners], type_=ARRAY(BigInteger)),
            bindparam("context_hashes", [winner.context_key_sha256 for winner in winners], type_=ARRAY(LargeBinary)),
            bindparam("family_ids", [winner.family_revision_id for winner in winners], type_=ARRAY(BigInteger)),
            bindparam("child_ids", [winner.context_child_revision_id for winner in winners], type_=ARRAY(BigInteger)),
        )
        .table_valued(
            sql_column("profile_slot", SmallInteger),
            sql_column("entity_binding_id", BigInteger),
            sql_column("context_key_sha256", LargeBinary),
            sql_column("family_revision_id", BigInteger),
            sql_column("context_child_revision_id", BigInteger),
            with_ordinality="ordinal",
        )
        .render_derived(name="chosen")
    )
    with _read_transaction(session, request, build_id) as (_build, deadline):
        models = _build_storage_models(session, build_id)[0]
        statement, keys, _models = _selected_context_query(models, build_id, chosen)
        _prepare_read(session, request, deadline)
        context_rows = session.execute(statement.order_by(*keys).limit(literal_column(str(len(winners) + 1)))).all()
        _require_budget(deadline)
    if len(context_rows) != len(winners) or any(
        type(context_row[0]) is not int
        or context_row[0] != ordinal
        or type(context_row[1]) is not int
        or not 0 < context_row[1] < 2**63
        for ordinal, context_row in enumerate(context_rows, 1)
    ):
        raise CandidateRunnerError("selected winner has no unique retained candidate context")
    if len({context_row[1] for context_row in context_rows}) != len(context_rows):
        raise CandidateRunnerError("selected winner contexts are not unique")
    return tuple(context_row[1] for context_row in context_rows)


def _selected_context_query(models, build_id, chosen):
    context = models[CustomImportBuildCandidateContext]
    statement = (
        select(chosen.c.ordinal, context.candidate_context_id)
        .select_from(chosen)
        .join(
            context,
            (context.build_id == build_id)
            & (context.profile_slot == chosen.c.profile_slot)
            & (context.entity_binding_id == chosen.c.entity_binding_id)
            & (context.context_key_sha256 == chosen.c.context_key_sha256)
            & (context.family_revision_id == chosen.c.family_revision_id)
            & context.context_child_revision_id.is_not_distinct_from(chosen.c.context_child_revision_id),
        )
    )
    return statement, (chosen.c.ordinal,), ()


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
            selected, page_sizes = await session.run_sync(
                lambda sync: _winner_batch(sync, request, registry, build_id, generation, after)
            )
        if not selected:
            return
        # Both immutable read streams and their session are closed before this
        # clean write page. Uncertain commits restart from the durable snapshot.
        async with _page_session(session_factory, request, build_id) as (session, _build):
            await _typed_call(
                session,
                "winner_batch_finalize",
                (
                    ("bigint", build_id),
                    ("bigint[]", selected),
                    ("smallint", None if after is None else after[0]),
                    ("bigint", None if after is None else after[1]),
                    ("bytea", None if after is None else after[2]),
                    ("integer[]", page_sizes),
                ),
            )


def _verify_page(session, request, build_id):
    with _read_transaction(session, request, build_id) as (_build, deadline):
        _build_storage_models(session, build_id)
        _prepare_read(session, request, deadline)
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
    for (row,) in _read_rows(session, request, build_id, statement, keys, (model,), bounds=_ReadPage(physical=True)):
        for digest in digests:
            publication._add_digest_record(digest, section, publication._materialization_document(row))
        count += 1
        del row
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


def _verified_root(request, root, root_record, binding):
    root_values = stored_root_values(request, root_record, root, binding)
    if (
        digest_text("root-payload", root.canonical_payload) != bytes(root.payload_sha256)
        or record_payload(request.definition.root_fields, root_values) != root.canonical_payload
    ):
        raise CandidateRunnerError("frozen root payload digest differs")
    return root_values


def _family_query(models, build_id, generation, after):
    """Load one root page and its plans in the persisted materialization order."""

    plan, record = models[CustomImportBuildFamily], models[CustomImportRootRecord]
    statement = publication._family_material_statement(generation, models=models).add_columns(plan)
    statement = statement.outerjoin(plan, (plan.build_id == build_id) & (plan.root_record_id == record.root_record_id))
    if after is not None:
        statement = statement.where(record.logical_key_sha256 > after)
    row_models = tuple(
        models[model]
        for model in (
            CustomImportGenerationFamily,
            CustomImportFamilyRevision,
            CustomImportRootRevision,
            CustomImportRootRecord,
            CustomImportEntityBinding,
            CustomImportBuildFamily,
        )
    )
    return statement, (record.logical_key_sha256,), row_models


def _family_children_query(models, generation, root_ids):
    """Feed the shared reducer all selected children in canonical family order."""

    child, record = models[CustomImportChildRevision], models[CustomImportRootRecord]
    collection = CustomImportChildCollection
    statement = publication._family_child_material_statement(generation, models=models).outerjoin(
        collection,
        (collection.dataset_id == child.dataset_id)
        & (collection.schema_revision_id == child.schema_revision_id)
        & (collection.collection_slot == child.collection_slot),
    )
    statement = statement.with_only_columns(
        record.root_record_id,
        func.coalesce(collection.collection_name, ""),
        child,
        collection.collection_slot.is_not(None),
        maintain_column_froms=True,
    ).where(record.root_record_id == any_(bindparam("root_ids", root_ids, type_=ARRAY(BigInteger))))
    keys = (
        record.root_record_id,
        func.coalesce(collection.collection_name, "").collate("C"),
        child.child_key_sha256,
        child.child_revision_id,
    )
    return statement, keys, (child, collection)


def _family_child_rows(session, request, build_id, generation, family_records):
    """Reserve the root page and stream children, checking each logical root/child pair."""
    root_bytes_by_id = {
        family_record[3].root_record_id: _model_bytes(family_record) for family_record in family_records
    }
    reserved_bytes = sum(
        root_bytes_by_id[family_record[3].root_record_id]
        + 128
        + 16 * sum(len(model.__table__.columns) for model in family_record)
        for family_record in family_records
    ) + max(root_bytes_by_id.values())
    with closing(
        _output_rows(
            session,
            request,
            build_id,
            lambda models: _family_children_query(models, generation, tuple(root_bytes_by_id)),
            bounds=_ReadPage(
                reserve_bytes=reserved_bytes,
                row_limit=(MAX_BATCH_ROWS - len(family_records) - 1) // 2,
            ),
        )
    ) as children:
        for child_row in children:
            root_id, collection, child, _valid = child_row
            if root_id not in root_bytes_by_id or child is None:
                raise CandidateRunnerError("frozen child stream contains an unexpected family or collection")
            if (
                root_bytes_by_id[root_id] + _model_bytes((child,)) + len(collection.encode("utf-8"))
                > request.page_byte_limit
            ):
                raise CandidateRunnerError("one build record exceeds the admitted byte page")
            yield child_row
            del child_row, child


def _verify_family_page(session, request, registry, build_id, generation, family_records):
    """Reuse graph's reducer with one cross-root child stream, never a family child list."""
    if any(row[5] is None or row[5].family_revision_id != row[1].family_revision_id for row in family_records):
        raise CandidateRunnerError("frozen family differs from its selected plan")
    with closing(_family_child_rows(session, request, build_id, generation, family_records)) as children:
        groups, pending = groupby(children, key=lambda row: (row[0], row[1])), deque()
        for _member, family, root, root_record, binding, plan in sorted(
            family_records, key=lambda family_record: family_record[3].root_record_id
        ):
            _require_budget(session.info["custom_import_build_read_deadline"])
            family_input = _FamilyInput(
                plan,
                root,
                root_record,
                _verified_root(request, root, root_record, binding),
                bytes(family.family_sha256),
                family.child_count,
                None,
            )
            actual = _source_input_with_digest(request, registry, family_input, groups, pending)
            if actual.child_count != family_input.child_count or actual.family_sha256 != family_input.family_sha256:
                raise CandidateRunnerError("frozen family digest or child count differs")
            _require_budget(session.info["custom_import_build_read_deadline"])
            del actual, family_input
        if pending or next(groups, None) is not None:
            raise CandidateRunnerError("frozen child stream contains an unexpected family or collection")


def _family_material(session, request, registry, build_id, generation, digests):
    family_count, after = 0, None
    while True:
        family_records = list(
            _output_rows(
                session,
                request,
                build_id,
                lambda models: _family_query(models, build_id, generation, after),
                bounds=_ReadPage(
                    row_limit=max(1, MAX_BATCH_ROWS // 2), reserve_bytes=MAX_BATCH_BYTES // 2, single_page=True
                ),
            )
        )
        if not family_records:
            return family_count
        while True:
            try:
                _verify_family_page(session, request, registry, build_id, generation, family_records)
                break
            except CandidateRunnerError as exc:
                if str(exc) != "one build record exceeds the admitted byte page" or len(family_records) == 1:
                    raise
                del family_records[max(1, len(family_records) // 2) :]
        for family_record in family_records:
            _require_budget(session.info["custom_import_build_read_deadline"])
            for index, digest in enumerate(digests):
                publication._add_family_revision_material(digest, family_record[:5], {}, effective_output=bool(index))
        family_count += len(family_records)
        after = bytes(family_records[-1][3].logical_key_sha256)
        del family_records, family_record


def _child_material(session, request, build_id, generation, digests):
    child_count = 0

    def query(models):
        """Keep child material in the existing root, collection, and key order."""
        row_models = tuple(
            models[model]
            for model in (
                CustomImportGenerationFamily,
                CustomImportFamilyChild,
                CustomImportChildRevision,
                CustomImportRootRecord,
            )
        )
        keys = (
            models[CustomImportRootRecord].logical_key_sha256,
            models[CustomImportFamilyChild].collection_slot,
            models[CustomImportChildRevision].child_key_sha256,
        )
        return publication._family_child_material_statement(generation, models=models), keys, row_models

    for _member, edge, child, root_record in _output_rows(
        session,
        request,
        build_id,
        query,
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
        del _member, edge, child, root_record, document
    return child_count


def _projection_query(models, generation, *, child):
    """Keep every selected revision visible, even when all its scalars are missing."""

    record = models[CustomImportRootRecord]
    revision = models[CustomImportChildRevision if child else CustomImportRootRevision]
    scalar = models[CustomImportChildScalar if child else CustomImportRootScalar]
    revision_id = revision.child_revision_id if child else revision.root_revision_id
    scalar_id = scalar.child_revision_id if child else scalar.root_revision_id
    statement = (
        publication._family_child_material_statement(generation, models=models)
        if child
        else publication._family_material_statement(generation, models=models)
    )
    # Compare every scalar column below. Joining only on revision identity also
    # exposes rows with wrong dataset/schema ownership instead of filtering them.
    statement = statement.outerjoin(scalar, scalar_id == revision_id).with_only_columns(
        scalar, revision, record.logical_key_sha256, maintain_column_froms=True
    )
    keys = (record.logical_key_sha256,)
    if child:
        keys += (models[CustomImportFamilyChild].collection_slot, revision.child_key_sha256)
    return statement, (*keys, func.coalesce(scalar.field_slot, 0)), (scalar, revision, record)


def _expected_projections(request, registry, revision, *, child):
    """Validate and order every projection without constructing scalar models."""

    if child:
        collection = next(
            name for name, slot in registry.child_collection_slots.items() if slot == revision.collection_slot
        )
        values_by_field = payload_values(
            fields_by_collection(request.definition)[collection],
            revision.canonical_payload,
            label="frozen child payload",
        )
        projections = material.project_child_scalars(
            request.definition,
            collection=collection,
            child_target=material.ChildScalarTarget(
                request.dataset_id,
                request.schema_revision_id,
                revision.root_record_id,
                revision.collection_slot,
                revision.child_revision_id,
            ),
            child_values=values_by_field,
            child_collection_slots=registry.child_collection_slots,
        )
        material._validate_scalar_projection_rows(request.definition, (), projections, registry.child_collection_slots)
    else:
        values_by_field = payload_values(
            request.definition.root_fields, revision.canonical_payload, label="frozen root payload"
        )
        projections = material.project_root_scalars(
            request.definition,
            root_target=material.RootScalarTarget(
                request.dataset_id, request.schema_revision_id, revision.root_record_id, revision.root_revision_id
            ),
            root_values=values_by_field,
        )
        material._validate_scalar_projection_rows(request.definition, projections, (), {})
    return sorted(projections, key=lambda projection: projection.field_slot)


def _verified_projection_rows(session, request, registry, build_id, generation, *, child):
    """Compare the bounded scalar stream without retaining payloads across pages."""

    model = CustomImportChildScalar if child else CustomImportRootScalar
    to_model = material._child_scalar_model if child else material._root_scalar_model
    previous_id, seen, expected_count = None, 0, 0
    projections = []

    def query(models):
        """Drop prepared values before reading each physical metadata/payload page."""
        projections.clear()
        return _projection_query(models, generation, child=child)

    try:
        with closing(_output_rows(session, request, build_id, query)) as projection_records:
            for scalar, revision, root_key in projection_records:
                revision_id = revision.child_revision_id if child else revision.root_revision_id
                if revision_id != previous_id and seen != expected_count:
                    raise CandidateRunnerError("typed scalar projection differs from the frozen payload")
                if revision_id != previous_id:
                    previous_id, seen = revision_id, 0
                    projections.clear()
                if not projections:
                    projections.extend(_expected_projections(request, registry, revision, child=child))
                    expected_count = len(projections)
                expected = to_model(projections[seen]) if seen < expected_count else None
                if scalar is None and not expected_count:
                    del expected, scalar, revision, root_key
                    continue
                if (
                    scalar is None
                    or seen >= expected_count
                    or any(
                        getattr(scalar, column.name) != getattr(expected, column.name)
                        for column in model.__table__.columns
                    )
                ):
                    raise CandidateRunnerError("typed scalar projection differs from the frozen payload")
                seen += 1
                del expected
                yield scalar, revision, root_key
                del scalar, revision, root_key
    finally:
        projections.clear()
    if seen != expected_count:
        raise CandidateRunnerError("typed scalar projection differs from the frozen payload")


def _scalar_material(session, request, registry, build_id, generation, digests, *, child):
    scalar_count = 0
    for scalar, revision, root_key in _verified_projection_rows(
        session, request, registry, build_id, generation, child=child
    ):
        document_by_field = {
            "root_key_sha256": bytes(root_key).hex(),
            "scalar": publication._materialization_document(scalar),
        }
        if child:
            document_by_field["child_key_sha256"] = bytes(revision.child_key_sha256).hex()
        for digest in digests:
            publication._add_digest_record(digest, "child_scalar" if child else "root_scalar", document_by_field)
        scalar_count += 1
        del scalar, revision, root_key, document_by_field
    return scalar_count


def _graph_material(session, request, registry, build_id, generation, material_digest, effective_digest):
    digests = (material_digest, effective_digest)
    return (
        _family_material(session, request, registry, build_id, generation, digests),
        _child_material(session, request, build_id, generation, digests),
        _scalar_material(session, request, registry, build_id, generation, digests, child=False),
        _scalar_material(session, request, registry, build_id, generation, digests, child=True),
    )


def _verify_winner_groups(session, request, registry, build_id, generation):
    return sum(1 for _ in _ordered_winners(session, request, registry, build_id, generation, verify=True))


def _winner_query(models, generation):
    winner = models[CustomImportWinner]
    binding = models[CustomImportEntityBinding]
    root = models[CustomImportRootRecord]
    child = models[CustomImportChildRevision]
    family = models[CustomImportFamilyRevision]
    keys = (
        winner.profile_slot,
        binding.adapter_id,
        binding.value_sha256,
        binding.canonical_value,
        root.key_contract_sha256,
        root.logical_key_sha256,
        func.coalesce(child.child_key_sha256, b""),
        family.family_sha256,
        winner.context_collection_slot,
        winner.context_key_sha256,
    )
    row_models = (winner, binding, family, root, child, CustomImportSelectionProfile)
    return publication._winner_material_statement(generation, models=models), keys, row_models


def _winner_material(session, request, build_id, generation, digests):
    count = 0
    for winner, binding, family, root, child, profile in _output_rows(
        session, request, build_id, lambda models: _winner_query(models, generation)
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
        del winner, binding, family, root, child, profile, document
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
    for section, model in (("pack", CustomImportPack), ("rejection", CustomImportRejection)):
        rows = _output_rows(session, request, build_id, lambda models: _attempt_query(models, request, model))
        for (row,) in rows:
            publication._add_digest_record(digest, section, publication._materialization_document(row))
            del row


def _attempt_query(models, request, model):
    row = models[model]
    keys = (row.stream_slot, row.pack_ordinal) if model is CustomImportPack else (row.rejection_ordinal,)
    return (
        select(row).where(
            row.execution_id == request.execution_id,
            row.producing_fence == request.fence,
            row.producing_token_sha256 == lease_token_sha256(request.lease_token),
        ),
        keys,
        (row,),
    )


def _frozen_materialization(session, request, registry, build_id, generation, proof):
    with _read_transaction(session, request, build_id) as (build, _deadline):
        _proof_matches(build, generation, proof)
    source_digest = _source_digest(session, request, build_id, generation.capture_bundle_id)
    candidate_digest, candidate_count = _candidate_digest(session, request, build_id, bounds=_ReadPage(physical=True))
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


async def _freeze_output_snapshot(session_factory, request, build_id):
    """Close candidate writes on normal completion and resumed verification."""

    async with _page_session(session_factory, request, build_id) as (session, _build):
        family_id = (
            await _typed_call(
                session,
                "freeze_custom_import_snapshot_family",
                (
                    ("bigint", request.execution_id),
                    ("bigint", request.fence),
                    ("bytea", lease_token_sha256(request.lease_token)),
                ),
            )
        ).scalar_one()
        if type(family_id) is not int or not 0 < family_id < 2**63:
            raise CandidateRunnerError("output snapshot freeze returned an invalid family identity")


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
            await _prepare_snapshot_indexes(session_factory, request, build_id, "output")
            await _attach_families(session_factory, request, build_id)
            await _write_winners(session_factory, request, registry, build_id, generation)
            async with _page_session(session_factory, request, build_id) as (session, _build):
                await _call(session, "freeze_custom_import_build_output", (build_id,))
        await _freeze_output_snapshot(session_factory, request, build_id)
        await _prepare_snapshot_indexes(session_factory, request, build_id, "serving")
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
