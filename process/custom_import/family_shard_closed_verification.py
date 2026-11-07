# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose original owner evidence with one protected closed candidate read.

The caller retains all original artifact owners, its pinned base and heartbeat.
Only fresh authorized transitions may consume the returned observed commitment;
this read-only composition never records verification, durability or a seal.
"""

import hashlib
from contextlib import closing, contextmanager
from dataclasses import astuple, dataclass
from functools import partial
from types import SimpleNamespace

from sqlalchemy import select

from db.models import custom_import as models
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import family_block_materialization as reconstruction
from process.custom_import import family_shard_original_validation as source_proof
from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_storage_validation as retained_proof
from process.custom_import import family_shard_source_values as source_values
from process.custom_import.build_source import _drain_cleanup, _source_cleanup_failure
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import validate_family_block_indexes
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import (
    ShardOutputProof,
    _candidate,
    _checked_receipt,
    _control_schema,
    _size,
)
from process.custom_import.family_block_validate import (
    CandidateScope,
    ClosedCandidateIdentity,
    FamilyBlockValidationError,
    _add,
    check_closed_identity,
    check_source_coverage,
    require_readonly_snapshot,
    run_structural_checks,
    structural_checks,
)
from process.custom_import.family_shard_admission import read_admission_occurrences
from process.custom_import.family_shard_admission_control import _original_admission
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_control import _source_capture_counts
from process.custom_import.family_shard_events import AdmissionEventsDescriptor, admission_event_batches
from process.custom_import.family_shard_events import _scope as event_scope
from process.custom_import.family_shard_graph_control import _complete_plan, _generation_values, _source_digest
from process.custom_import.family_shard_home_validation import compare_candidate_homes
from process.custom_import.family_shard_homes import _initial_ids
from process.custom_import.family_shard_output import FamilyShardOutput, _input_iterator, _pack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_projection_complete import _evidence as projection_evidence
from process.custom_import.family_shard_projection_complete import _manifest_values as projection_manifest_values
from process.custom_import.family_shard_projection_stage import ProjectionStageOutput
from process.custom_import.family_shard_projection_stage import _proof_inputs as projection_inputs
from process.custom_import.family_shard_projection_verify import ClosedProjectionEvidence
from process.custom_import.family_shard_projection_verify import _scope as projection_scope
from process.custom_import.family_shard_receipt_validation import compare_candidate_receipts
from process.custom_import.family_shard_retained_output import _replay_families, read_retained_destination
from process.custom_import.family_shard_role_validation import _same_snapshot, compare_candidate_roles
from process.custom_import.family_shard_source_plan import _entries as source_entries
from process.custom_import.family_shard_verification import _control_counts
from process.custom_import.family_shard_winner_validation import RetainedWinnerInput, prove_original_winners
from process.custom_import.family_shard_winner_validation import _retained_scope as retained_winner_scope
from process.custom_import.family_shard_winners import WinnerOutputDescriptor, read_winner_rows
from process.custom_import.materialization import DefinitionIdentity, GenerationIdentity, selection_profile_models
from process.custom_import.query_projection_finality import load_generation_evidence
from process.custom_import.read_identity import _read_transaction_identity
from process.custom_import.runner_registry import has_profile_mismatch
from process.custom_import.storage_layout import snapshot_schema


@dataclass(frozen=True)
class ClosedShardInputs:
    """Live genuine producers; the coordinator constructs its own expected streams."""

    workers: tuple
    source_families: object
    query_keys: object
    events: AdmissionEventsDescriptor
    retained: RetainedWinnerInput
    retained_output: FamilyShardOutput
    retained_stored: retained_proof.RetainedStoredComparison
    members: FamilyShardOutput
    winners: WinnerOutputDescriptor
    projection_output: ProjectionStageOutput | None
    projection: ClosedProjectionEvidence
    common_receipt: object
    events_receipt: object
    retained_common_receipt: object
    winner_receipt: object


@dataclass(frozen=True)
class ClosedShardVerification:
    identity: ClosedCandidateIdentity
    counts: tuple
    validation_sha256: bytes
    completed_shards: int
    source_record_count: int
    role_counts: tuple


def _native_values(model):
    return {column.name: getattr(model, column.name) for column in model.__table__.c}


def _work(parent, name):
    return PlanWork(parent.root / name, parent.limits, lambda _path: None, parent.current)


def _inputs(context, inputs, work, limits):
    if type(context) is not allocation.RetainedAllocationContext or type(inputs) is not ClosedShardInputs:
        raise ValueError("closed verification requires real original context and producer owners")
    if type(work) is not PlanWork or type(limits) is not SourceAllocationLimits or not callable(inputs.query_keys):
        raise ValueError("closed verification requires accounted work, batches and original key factory")
    if type(inputs.workers) is not tuple or not 0 < len(inputs.workers) <= 4096:
        raise ValueError("closed verification requires the actual bounded SOURCE worker cohort")
    if (len(inputs.workers) + 8) * 4096 > work.limits.memory_bytes // 4:
        raise FamilyBlockResourceLimit("closed producer metadata exceeds the existing owner reservation")
    if type(inputs.source_families) is not source_values.PreparedSourceFamilies:
        raise ValueError("closed verification requires the typed prepared SOURCE replay")
    inputs.source_families.check_scope(context.common, context.admission, context.final_plan)
    if (
        type(inputs.retained) is not RetainedWinnerInput
        or type(inputs.retained_stored) is not retained_proof.RetainedStoredComparison
        or type(inputs.members) is not FamilyShardOutput
        or type(inputs.winners) is not WinnerOutputDescriptor
        or type(inputs.events) is not AdmissionEventsDescriptor
        or type(inputs.projection) is not ClosedProjectionEvidence
        or (inputs.projection_output is not None and type(inputs.projection_output) is not ProjectionStageOutput)
    ):
        raise ValueError("closed verification requires completed real producer and comparison types")
    expected = event_scope(context.common.request, context.common.layout, context.common.source, context.admission)
    if _pack_row(inputs.events.scope) != _pack_row(expected):
        raise FamilyBlockValidationError("closed admission event producer scope differs")


def _retained(context, inputs, work):
    destination, original = inputs.retained.destination, inputs.retained.comparison
    scope, output = retained_proof._scope(
        context, destination, original, inputs.retained_output, inputs.query_keys, work
    )
    stored = inputs.retained_stored
    if (
        type(stored.scope) is not tuple
        or _pack_row(stored.scope) != _pack_row(scope)
        or type(stored.destination_sha256) is not bytes
        or stored.destination_sha256 != destination.sha256
        or type(stored.original_input_sha256) is not bytes
        or stored.original_input_sha256 != context.retained_input.sha256
        or stored.output_inputs != output
        or stored.counts != original.counts
        or (stored.root_scalar_count, stored.child_scalar_count)
        != (original.root_scalar_count, original.child_scalar_count)
        or type(stored.sha256) is not bytes
        or len(stored.sha256) != 32
    ):
        raise FamilyBlockValidationError("closed retained stored proof provenance differs")
    for count in (
        stored.root_scalar_count,
        stored.child_scalar_count,
        stored.dictionary_key_count,
        stored.used_query_key_count,
    ):
        _integer(count, "closed retained comparison count", zero=True)
    if stored.used_query_key_count > stored.dictionary_key_count:
        raise FamilyBlockValidationError("closed retained dictionary coverage differs")


def _artifact_inputs(context, inputs):
    return (
        context.common.source.source.sha256,
        context.common.source.sha256,
        context.admission.sha256,
        context.final_plan.sha256,
        context.common_output.sha256,
        context.retained_input.sha256,
        inputs.events.sha256,
        inputs.retained.destination.sha256,
        inputs.retained_stored.sha256,
        source_proof._worker_inputs(context.common.layout, inputs.workers),
        inputs.retained_output.sha256,
        inputs.members.sha256,
        inputs.winners.sha256,
        None if inputs.projection_output is None else inputs.projection_output.sha256,
        astuple(inputs.projection),
        tuple(
            _pack_row(_checked_receipt(context.common.layout, receipt))
            for receipt in (
                inputs.common_receipt,
                inputs.events_receipt,
                inputs.retained_common_receipt,
                inputs.winner_receipt,
            )
        ),
    )


async def _native_state(session, context, inputs, ready):
    await ready()
    descriptor, binding = await _candidate(
        session,
        context.common.request,
        context.common.source.snapshot_family_id,
        context.common.layout,
        {"closed", "verified", "logged"},
        locked=False,
    )
    await ready()
    build = await session.get(models.CustomImportBuildAttempt, context.build_id, populate_existing=True)
    generation = (
        None
        if build is None
        else await session.get(models.CustomImportGeneration, build.generation_id, populate_existing=True)
    )
    snapshot = await session.get(CustomImportSnapshotFamily, binding.family_id, populate_existing=True)
    if build is None or generation is None or snapshot is None:
        raise FamilyBlockValidationError("closed owner build, generation or snapshot is missing")
    request = context.common.request
    scope = CandidateScope(
        binding.family_id,
        generation.generation_id,
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        dict(context.common.source.source.scope[0])["capture_bundle_id"],
        request.fence,
        producer_token_sha256(request),
        request.expected_base_generation_id,
    )
    for producer in (build, generation):
        if any(
            getattr(producer, name) != expected_value
            for name, expected_value in scope.parameters().items()
            if name not in {"family_id", "base_generation_id"}
        ):
            raise FamilyBlockValidationError("closed build or generation producer differs")
    if (
        build.build_id != context.build_id
        or build.phase not in {"verifying", "verified"}
        or build.base_generation_id != request.expected_base_generation_id
        or generation.base_generation_id != request.expected_base_generation_id
        or build.source_occurrence_count != context.common.source.source.counts[0]
        or build.candidate_error_count != 0
        or build.next_rejection_ordinal != sum(inputs.events.counts[:2])
    ):
        raise FamilyBlockValidationError("closed build scope, SOURCE or admission accounting differs")
    identity = check_closed_identity(
        binding, scope, _native_values(snapshot), _native_values(descriptor), phase=descriptor.phase
    )
    await validate_family_block_indexes(session, binding, prepare=lambda _session: ready())
    return SimpleNamespace(
        binding=binding, descriptor=descriptor, build=build, generation=generation, identity=identity
    )


def _source_census(context, work):
    source = context.common.source.source
    counts_by_stream = {slot: [0, 0] for slot, _count in source.source_counts}

    def parts():
        """Exhaust every original frame while retaining exact zero-row parts."""
        with closing(source_entries(source, check=work.current)) as entries:
            for entry in entries:
                if entry[0] == "part":
                    slot, ordinal, count = entry[1:]
                    counts_by_stream[slot][0] = ordinal
                    counts_by_stream[slot][1] += count
                    yield slot, ordinal, count

    def occurrences():
        """Require complete original admitted occurrence positions."""
        with closing(read_admission_occurrences(context.admission, check=work.current)) as rows:
            for occurrence in rows:
                record = occurrence.record
                yield record.stream_slot, record.part_ordinal, record.row_ordinal, record.source_ordinal

    with closing(parts()) as original_parts, closing(occurrences()) as originals:
        if check_source_coverage(original_parts, originals, dict(source.source_counts)) != source.counts[0]:
            raise FamilyBlockValidationError("closed original SOURCE coverage differs")
    return counts_by_stream


async def _control_state(session, context, inputs, native, work, ready):
    planned = await _cpu(work, _complete_plan, context.common, context.final_plan, context.admission, work.current)
    errors = await _cpu(work, _original_admission, context.admission, inputs.events, work.current)
    if errors:
        raise FamilyBlockValidationError("closed admission contains candidate errors")
    await ready()
    expected_by_name = _generation_values(
        context.common, native.build, planned, await _source_digest(session, context.common)
    )
    if any(getattr(native.generation, name) != expected_value for name, expected_value in expected_by_name.items()):
        raise FamilyBlockValidationError("closed generation header differs from original plan or capture")
    source_counts = await _cpu(work, _source_census, context, work)
    await ready()
    await _source_capture_counts(session, context.common, source_counts)
    await ready()
    streams = (
        (
            await session.execute(
                select(models.CustomImportBuildStream)
                .where(models.CustomImportBuildStream.build_id == context.build_id)
                .order_by(models.CustomImportBuildStream.stream_slot)
            )
        )
        .scalars()
        .all()
    )
    tails_by_stream = dict(inputs.retained.destination.destination_pack_tail)
    if {stream.stream_slot for stream in streams} != set(source_counts) or set(tails_by_stream) != set(source_counts):
        raise FamilyBlockValidationError("closed SOURCE stream census differs")
    for stream in streams:
        part_count, record_count = source_counts[stream.stream_slot]
        if (
            stream.next_part_ordinal,
            stream.next_part_row_ordinal,
            stream.next_source_ordinal,
            stream.next_pack_ordinal,
        ) != (
            part_count + 1,
            0,
            record_count,
            tails_by_stream[stream.stream_slot],
        ) or stream.replay_verified_at is None:
            raise FamilyBlockValidationError("closed SOURCE stream position or retained pack tail differs")
    return planned


async def _profiles(session, context, ready):
    request = context.common.request
    expected = selection_profile_models(
        request.definition,
        identity=DefinitionIdentity(request.dataset_id, request.definition_revision_id, request.schema_revision_id),
        child_collection_slots=context.common.layout.collection_slots,
    )
    await ready()
    actual = (
        (
            await session.execute(
                select(models.CustomImportSelectionProfile)
                .where(
                    models.CustomImportSelectionProfile.dataset_id == request.dataset_id,
                    models.CustomImportSelectionProfile.definition_revision_id == request.definition_revision_id,
                    models.CustomImportSelectionProfile.schema_revision_id == request.schema_revision_id,
                )
                .order_by(models.CustomImportSelectionProfile.profile_slot)
            )
        )
        .scalars()
        .all()
    )
    if len(actual) != len(expected) or any(
        has_profile_mismatch(actual_profile, expected_profile)
        for actual_profile, expected_profile in zip(actual, expected, strict=True)
    ):
        raise FamilyBlockValidationError("closed configured selection profiles differ")
    return len(expected)


def _selected_scope(context, workers, prepared, retained, work):
    if (
        type(context) is not allocation.RetainedAllocationContext
        or type(work) is not PlanWork
        or type(workers) is not tuple
        or not workers
        or type(prepared) is not source_values.PreparedSourceFamilies
        or type(retained) is not RetainedWinnerInput
    ):
        raise ValueError("selected family union requires real typed producer owners and work")
    prepared.check_scope(context.common, context.admission, context.final_plan)
    source_scope = source_proof._input_scope(context.common, context.admission, context.final_plan, workers, work)
    worker_inputs = source_proof._worker_inputs(context.common.layout, workers)
    if tuple((row[0], row[1], row[4]) for row in worker_inputs) != context.admission.worker_inputs:
        raise FamilyBlockValidationError("selected family union differs from the complete admitted SOURCE cohort")
    scope = allocation._scope(
        context,
        allocation.RetainedAllocationWork(
            work.root,
            SourceAllocationLimits(work.limits, 1, work.limits.max_record_bytes),
            work.register_cleanup,
            work.check,
        ),
    )
    retained_winner_scope(context, scope, retained)
    return source_scope, worker_inputs, astuple(retained.comparison)


def _selected_families(context, workers, prepared, retained, work):
    cohort, counts_by_kind = len(workers), dict(source=0, retained=0)
    with (
        closing(prepared.families(check=work.current)) as sources,
        closing(_replay_families(retained.destination, context.common.layout.definition, work)) as old_families,
    ):
        source, original = next(sources, None), next(old_families, None)
        while source is not None or original is not None:
            work.current()
            if original is None or source is not None and source[0].typed_key[::-1] < original[0].typed_key[::-1]:
                plan, family = source
                counts_by_kind["source"] += 1
                yield int.from_bytes(plan.typed_key[1][:8], "big") % cohort, plan, family
                source = next(sources, None)
            else:
                counts_by_kind["retained"] += 1
                yield cohort, original[0], original[2]
                original = next(old_families, None)
    if any(count != context.final_plan.counts_by_kind[kind] for kind, count in counts_by_kind.items()):
        raise FamilyBlockValidationError("selected family union complete original coverage differs")


@contextmanager
def selected_shard_families(context, workers, prepared, retained, *, work):
    """Borrow the exact full selected union; close invalidates current children.

    Run this synchronous owner off the event loop with existing stop-and-drain.
    Full EOF is mandatory. The caller retains the real pin and native authority;
    this bounded replay writes only its registered local scratch.
    """
    scope = _selected_scope(context, workers, prepared, retained, work)
    deadline = _read_deadline(context.common.request)

    def current():
        """Preserve caller cancellation and the original immutable request deadline."""
        if work.check is not None:
            work.check()
        _require_deadline(deadline)

    scratch = _Scratch(PlanWork(work.root, work.limits, work.register_cleanup, current))
    state, primary = SimpleNamespace(is_complete=False), None

    def entries():
        """Observe both original EOFs before marking a complete borrowed union."""
        yield from _selected_families(context, workers, prepared, retained, scratch)
        state.is_complete = True

    families = entries()
    try:
        yield families
        if not state.is_complete:
            raise FamilyBlockValidationError("selected family union requires complete original EOF")
        scratch.current()
        if _selected_scope(context, workers, prepared, retained, work) != scope:
            raise FamilyBlockValidationError("selected family union producer custody changed")
    except BaseException as error:
        primary = error
    for cleanup in (families.close, scratch.close):
        try:
            cleanup()
        except BaseException as error:
            primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary


def _winner_batches(descriptor, limits, work):
    rows, size = [], 0
    maximum_bytes = min(limits.maximum_batch_bytes, work.limits.memory_bytes // 2)
    with closing(read_winner_rows(descriptor, check=work.current)) as winners:
        for row in winners:
            charge = 2 + sum(_size(cell) for cell in row)
            if charge > maximum_bytes:
                raise FamilyBlockResourceLimit("winner COPY row exceeds the existing batch budget")
            if rows and (len(rows) == limits.maximum_batch_records or size + charge > maximum_bytes):
                yield {"winner": tuple(rows)}
                rows, size = [], 0
            rows.append(row)
            size += charge
        if rows:
            yield {"winner": tuple(rows)}


def _common_batches(context, inputs, options_by_name, work):
    missing = object()
    with (
        closing(_input_iterator(inputs.query_keys())) as keys,
        closing(context.common_output.batches(**options_by_name)) as batches,
    ):
        for batch in batches:
            for row in batch.get("key_dictionary", ()):
                work.current()
                expected = next(keys, missing)
                if (
                    expected is missing
                    or type(expected) is not tuple
                    or len(expected) != 4
                    or row != (expected[2], expected[3], expected[0], expected[1])
                ):
                    raise FamilyBlockValidationError("closed original complete dictionary differs from common rows")
            yield batch
        if next(keys, missing) is not missing:
            raise FamilyBlockValidationError("closed original complete dictionary has trailing keys")


def _producers(context, inputs, limits, work):
    options_by_name = dict(
        maximum_rows=limits.maximum_batch_records,
        maximum_bytes=min(limits.maximum_batch_bytes, work.limits.memory_bytes // 2),
    )
    for worker in inputs.workers:
        yield worker.receipt, worker.batches(**options_by_name), True
    yield inputs.common_receipt, _common_batches(context, inputs, options_by_name, work), False
    yield inputs.events_receipt, admission_event_batches(inputs.events, **options_by_name), False
    yield inputs.retained_common_receipt, inputs.retained.destination.batches(**options_by_name), False
    yield inputs.retained_output.receipt, inputs.retained_output.batches(**options_by_name), False
    yield inputs.members.receipt, inputs.members.batches(**options_by_name), False
    yield inputs.winner_receipt, _winner_batches(inputs.winners, limits, work), False
    if inputs.projection_output is not None:
        yield inputs.projection_output.receipt, inputs.projection_output.batches(**options_by_name), False


def _expected_batches(context, inputs, limits, work, receipts):
    partition = bytes.fromhex(context.common.source.source.partition_sha256)
    seen_ids = set()
    for receipt, batches, is_source in _producers(context, inputs, limits, work):
        original = _checked_receipt(context.common.layout, receipt)
        if (
            original["shard_id"] in seen_ids
            or original["partition_sha256"] != partition
            or (not is_source and original["source_record_count"] != 0)
        ):
            raise FamilyBlockValidationError("closed original COPY cohort or partition differs")
        seen_ids.add(original["shard_id"])
        proof = ShardOutputProof(
            context.common.layout, original["shard_id"], partition, original["source_record_count"]
        )
        with closing(batches):
            for batch in batches:
                work.current()
                for role, rows in batch.items():
                    proof.add(role, rows)
                yield batch
        if proof.receipt() != original:
            raise FamilyBlockValidationError("closed COPY commitment differs from actual producer bytes")
        receipts.append(original)


def _receipt_census(context, roles, receipts):
    counts_by_role = dict.fromkeys(context.common.layout.tables(context.common.source.snapshot_family_id), 0)
    source_records = 0
    for receipt in receipts:
        source_records += receipt["source_record_count"]
        for role, count in receipt["role_counts"].items():
            counts_by_role[role] += count
    counts_by_role["shard_receipt"] = len(receipts)
    if (
        source_records != context.common.source.source.counts[0]
        or {role: count for role, count in counts_by_role.items() if role != "shard_receipt"} != roles.counts_by_role
    ):
        raise FamilyBlockValidationError("closed receipt, role or original SOURCE census differs")
    return source_records, tuple(sorted(counts_by_role.items()))


def _home_ids(context, inputs, work):
    yield from _initial_ids(work, context.common, context.admission)
    kinds_by_name = {"root_revision": 1, "child_revision": 2}
    with closing(read_retained_destination(inputs.retained.destination, check=work.current)) as entries:
        for entry in entries:
            if entry[0] == "allocation" and entry[-1] is True and entry[3] in kinds_by_name:
                yield kinds_by_name[entry[3]], entry[4]


async def _projection(session, context, inputs, native, ready):
    generation = GenerationIdentity(
        native.generation.generation_id,
        native.generation.dataset_id,
        native.generation.definition_revision_id,
        native.generation.schema_revision_id,
    )
    projection_scope(native.binding, generation, inputs.projection.identity)
    if inputs.projection.identity != native.identity:
        raise FamilyBlockValidationError("closed completed projection belongs to another closure")
    expected = projection_evidence(context.common, inputs.projection)
    if (inputs.projection_output is None) != (expected is None):
        raise FamilyBlockValidationError("closed configured projection producer is missing or unexpected")
    if inputs.projection_output is not None and inputs.projection_output.proof_inputs != projection_inputs(
        context.common, context.build_id, native.binding, generation
    ):
        raise FamilyBlockValidationError("closed projection COPY producer scope differs")
    await ready()
    manifest = (
        await session.execute(
            select(CustomImportQueryProjection)
            .where(CustomImportQueryProjection.family_id == native.binding.family_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if expected is None:
        if manifest is not None:
            raise FamilyBlockValidationError("closed disabled projection advertises a manifest")
    elif manifest is None or any(
        getattr(manifest, name) != expected_value
        for name, expected_value in projection_manifest_values(
            context.common, native.binding, native.generation, inputs.projection
        ).items()
    ):
        raise FamilyBlockValidationError("closed projection checkpoint or exact manifest differs")
    await ready()
    actual = await load_generation_evidence(
        session, native.generation, context.common.request.definition, frozen=False, family_block_binding=native.binding
    )
    if actual != expected:
        raise FamilyBlockValidationError("closed completed projection manifest differs")


def _counts(context, inputs, source, winners, roles, profiles):
    fresh, old = source.counts_by_name, dict(inputs.retained_stored.counts)
    selected = fresh["family_count"] + old["family"]
    counts = (
        selected,
        selected,
        roles.counts_by_role["generation_family"],
        fresh["family_child_count"] + old["child"],
        winners.winner_count,
        profiles,
        fresh["root_scalar_count"] + inputs.retained_stored.root_scalar_count,
        fresh["child_scalar_count"] + inputs.retained_stored.child_scalar_count,
    )
    for count in counts:
        _integer(count, "closed original structural count", zero=True)
    if counts[2] != selected or selected != sum(
        context.final_plan.counts_by_kind[kind] for kind in ("source", "retained")
    ):
        raise FamilyBlockValidationError("closed selected families and original member coverage differ")
    return counts


async def _original_comparisons(context, inputs, native, work, source_record_claims):
    source_comparison = await _cpu(
        work,
        partial(
            source_proof.prove_original_outputs,
            context.common,
            context.admission,
            context.final_plan,
            inputs.workers,
            inputs.query_keys(),
            work=_work(work, "source-original"),
            prepared=source_record_claims,
        ),
    )
    generation = GenerationIdentity(
        native.generation.generation_id,
        native.generation.dataset_id,
        native.generation.definition_revision_id,
        native.generation.schema_revision_id,
    )
    winners = await _cpu(
        work,
        partial(
            prove_original_winners,
            context,
            source_comparison,
            generation,
            inputs.winners,
            _selected_families(context, inputs.workers, inputs.source_families, inputs.retained, work),
            len(inputs.workers) + 1,
            retained_originals=inputs.retained,
            work=_work(work, "winner-original"),
        ),
    )
    return source_comparison, winners


async def _compare(session, context, inputs, native, work, limits, ready, source_record_claims):
    """Run each independent original/native comparison once under this read."""
    evidence = SimpleNamespace()
    evidence.planned = await _control_state(session, context, inputs, native, work, ready)
    evidence.profiles = await _profiles(session, context, ready)
    await _projection(session, context, inputs, native, ready)
    evidence.source, evidence.winners = await _original_comparisons(context, inputs, native, work, source_record_claims)
    evidence.source_record_claims = source_record_claims
    evidence.receipts = []
    evidence.roles = await compare_candidate_roles(
        session,
        native.binding,
        _expected_batches(context, inputs, limits, work, evidence.receipts),
        work=_work(work, "roles"),
        prepare=lambda _session: ready(),
    )
    if (
        evidence.source.dictionary_key_count != inputs.retained_stored.dictionary_key_count
        or evidence.roles.counts_by_role["key_dictionary"] != evidence.source.dictionary_key_count
    ):
        raise FamilyBlockValidationError("closed complete original dictionary census differs")
    evidence.source_record_count, evidence.role_counts = _receipt_census(context, evidence.roles, evidence.receipts)
    evidence.receipt_proof = await compare_candidate_receipts(
        session, native.binding, evidence.receipts, work=_work(work, "receipts"), prepare=lambda _session: ready()
    )
    if evidence.receipt_proof.counts_by_role != {"shard_receipt": len(evidence.receipts)}:
        raise FamilyBlockValidationError("closed native receipt census differs")
    evidence.homes = await compare_candidate_homes(
        session,
        native.binding,
        _home_ids(context, inputs, work),
        work=_work(work, "homes"),
        prepare=lambda _session: ready(),
    )
    originals_by_role = dict(inputs.retained.comparison.counts)
    if (evidence.homes.root_count, evidence.homes.child_count) != (
        context.common.source.source.counts[2] + originals_by_role["root"],
        context.common.source.source.counts[3] + originals_by_role["child"],
    ):
        raise FamilyBlockValidationError("closed native homes differ from all original allocations")
    return await _finish(session, context, inputs, native, work, ready, evidence)


async def _finish(session, context, inputs, native, work, ready, evidence):
    """Require native predicates and real reconstruction before committing observations."""
    _quote, control_schema = await _control_schema(session)
    base_schema = (
        None
        if context.base is None
        else (control_schema if context.base.pin.family_id is None else snapshot_schema(context.base.pin.family_id))
    )
    completed = await run_structural_checks(
        session,
        native.binding,
        native.identity.scope,
        control_schema,
        base_schema=base_schema,
        prepare=lambda _session: ready(),
    )
    if completed != tuple(
        check.code for check in structural_checks(native.binding, control_schema, base_schema=base_schema)
    ):
        raise FamilyBlockValidationError("closed native predicate coverage differs")
    counts = _counts(context, inputs, evidence.source, evidence.winners, evidence.roles, evidence.profiles)
    _control_counts(native.build, native.generation, dict(zip(reconstruction._COUNTS, counts, strict=True)))
    matching = sum(stream[2] for _profile, streams, _count in inputs.winners.profiles for stream in streams)
    if native.build.candidate_context_count != matching or counts[0] != evidence.planned.selected_count:
        raise FamilyBlockValidationError("closed matching contexts or selected plan totals differ")
    await ready()
    if evidence.source.record_claims_sha256 != evidence.source_record_claims.sha256:
        raise FamilyBlockValidationError("closed SOURCE proof does not bind its complete record claims")
    with reconstruction.materialization_work(
        session, work.root / "reconstruction", work.limits, lambda _path: None, check=work.current
    ) as plan:
        async with reconstruction.reconstruct_sections(
            session, native.binding, native.generation, plan, source_record_claims=evidence.source_record_claims
        ) as sections:
            if sections.completed_counts != counts:
                raise FamilyBlockValidationError("closed reconstruction differs from all eight original counts")
            if sections.source_record_claims_sha256 != evidence.source.record_claims_sha256:
                raise FamilyBlockValidationError("closed native SOURCE record claims differ from original proof")
    evidence.completed, evidence.counts, evidence.matching = completed, counts, matching
    return _commitment(context, inputs, native, evidence)


def _commitment(context, inputs, native, evidence):
    digest = hashlib.sha256(b"custom-import/closed-shard-owner-verification/v1\0")
    _add(digest, "identity", astuple(native.identity))
    _add(
        digest,
        "build",
        (
            context.build_id,
            *(getattr(native.build, name) for name in ("source_frozen_at", "graph_frozen_at", "output_frozen_at")),
            evidence.planned.candidate_sha256,
        ),
    )
    _add(digest, "originals", (evidence.source.sha256, inputs.retained_stored.sha256, evidence.winners.sha256))
    _add(
        digest,
        "native",
        (
            evidence.roles.sha256,
            evidence.receipt_proof.sha256,
            evidence.homes.sha256,
            astuple(inputs.projection),
            evidence.completed,
        ),
    )
    _add(
        digest,
        "census",
        (
            evidence.counts,
            evidence.matching,
            evidence.source_record_count,
            evidence.role_counts,
            len(evidence.receipts),
        ),
    )
    return ClosedShardVerification(
        native.identity,
        evidence.counts,
        digest.digest(),
        len(evidence.receipts),
        evidence.source_record_count,
        evidence.role_counts,
    )


async def verify_closed_shard_build(session, context, inputs, *, work, limits, prepare):
    """Return complete observed owner evidence in the caller's single protected read."""
    _inputs(context, inputs, work, limits)
    if not callable(prepare) or not session.in_transaction() or session.new or session.dirty or session.deleted:
        raise ValueError("closed verification requires a clean live read and trusted SQL preparation")
    deadline = _read_deadline(context.common.request)

    def current():
        """Keep the original request deadline and parent stop check authoritative."""
        if work.check is not None:
            work.check()
        _require_deadline(deadline)

    scratch, primary, comparison = (
        _Scratch(PlanWork(work.root, work.limits, work.register_cleanup, current)),
        None,
        None,
    )
    source_record_claims = None
    try:
        transactions = await _read_transaction_identity(session)

        async def ready():
            """Keep every native leaf inside the exact caller transaction and budget."""
            scratch.current()
            await _same_snapshot(session, transactions)
            await prepare(session)
            await _same_snapshot(session, transactions)
            await require_readonly_snapshot(session)
            scratch.current()

        await ready()
        _retained(context, inputs, _work(scratch, "retained-scope"))
        artifacts = _artifact_inputs(context, inputs)
        await allocation._check_base(context)
        native = await _native_state(session, context, inputs, ready)
        source_record_claims = await _source_record_claims(context, inputs, scratch)
        comparison = await _compare(session, context, inputs, native, scratch, limits, ready, source_record_claims)
        await allocation._check_base(context)
        _inputs(context, inputs, work, limits)
        if (
            _artifact_inputs(context, inputs) != artifacts
            or (await _native_state(session, context, inputs, ready)).identity != native.identity
        ):
            raise FamilyBlockValidationError("closed original artifacts or candidate custody changed")
        await ready()
    except BaseException as error:
        primary = error
    if source_record_claims is not None:
        primary = await _drain_cleanup(_cpu(scratch, source_record_claims.close), primary)
    primary = await _drain_cleanup(_cpu(scratch, scratch.close), primary)
    if primary is not None:
        raise primary
    return comparison


async def _source_record_claims(context, inputs, work):
    return await _cpu(
        work,
        partial(
            source_proof.SourceRecordClaims,
            context.common,
            inputs.source_families,
            work=_work(work, "source-record-claims"),
        ),
    )
