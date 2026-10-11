# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native bounded graph/output parity, retained copies, and ordinary finality."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from dataclasses import replace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, text, tuple_, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildCandidateContext,
    CustomImportBuildFamily,
    CustomImportBuildOccurrence,
    CustomImportBuildVerification,
    CustomImportChildScalar,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPack,
    CustomImportRootScalar,
)
from process.custom_import import build_graph as graph
from process.custom_import import build_graph_prepare_page as graph_prepare
from process.custom_import import build_graph_sets as graph_sets
from process.custom_import import build_output as output
from process.custom_import import compact_materialization as compact
from process.custom_import import execution as lifecycle
from process.custom_import import publication, read_core, runner
from process.custom_import.build_source import SourceBuildRequest, stage_segmented_source
from process.custom_import.capture_pending import seal_pending_parquet_bundle
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.definition_store import register_definition
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from tests import test_custom_import_runner_postgres as legacy_runner
from tests.custom_import_postgres_support import install_materialization_contract_migration
from tests.test_custom_import_build_source_postgres import _candidate_models, _sealed_parts, _source_case
from tests.test_custom_import_capture_pending_postgres import _receipt, _retain, _start_attempt
from tests.test_custom_import_compact_contract import _migration as _contract_migration
from tests.test_custom_import_definition import _raw_definition
from tests.test_custom_import_read_core_postgres import _service
from tests.test_custom_import_snowflake_shared_capture import _shared_definition


def _definition():
    document = _raw_definition()
    for stream in document["streams"]:
        stream.update(format="parquet", compression="none")
    document["schema"]["root"]["logical_key"] = ["npi", "display_name"]
    child = document["schema"]["children"][0]
    child["parent_key"].append({"child": "rate_name", "root": "display_name"})
    child["fields"].append({"id": "rate_name", "slot": 6, "type": "string", "nullable": False})
    return CustomImportDefinition.from_mapping(document)


def _records(root_count=3, first_child_count=1, *, amount="10", last_empty=False):
    roots = [dict(npi="1234567893", display_name=f"Synthetic {index}") for index in range(root_count)]
    child_records = []
    for index, root in enumerate(roots):
        child_count = first_child_count if index == 0 else 1
        if last_empty and index == len(roots) - 1:
            child_count = 0
        child_records.extend(
            dict(
                rate_npi=root["npi"],
                rate_name=root["display_name"],
                service_code=f"S{child_index:02}",
                amount=str(int(amount) + index),
            )
            for child_index in range(child_count)
        )
    return {
        "providers": [roots],
        "rates": [child_records[offset : offset + 12] for offset in range(0, len(child_records), 12)] or [[]],
    }


async def _request_for(case, records_by_stream, *, seed=None, base=None, version=0, page_rows=8, definition=None):
    definition = definition or _definition()
    if seed is None:
        async with case.sessions() as session, session.begin():
            seed = await register_definition(session, "synthetic_bounded_output", definition)
    pending, registration = await _start_attempt(case, seed=seed)
    parts = _sealed_parts(pending, definition, records_by_stream)
    await _retain(case, pending, registration, parts)
    async with case.sessions() as session, session.begin():
        await seal_pending_parquet_bundle(
            session,
            request=pending,
            capture_bundle_id=registration.capture_bundle_id,
            receipts=tuple(_receipt(pending, name, captures) for name, captures in parts.items()),
        )
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    return SourceBuildRequest(
        dataset_id=pending.dataset_id,
        definition_revision_id=pending.definition_revision_id,
        schema_revision_id=pending.schema_revision_id,
        execution_id=pending.execution_id,
        lease_token=pending.token,
        fence=pending.fence,
        definition=definition,
        expected_base_generation_id=base,
        expected_pointer_version=version,
        complete_scope=definition.refresh_mode == "snapshot",
        page_row_limit=page_rows,
        page_byte_limit=65_536,
        statement_timeout_ms=2000,
        build_deadline_at=now + dt.timedelta(seconds=180),
        lease_seconds=120,
    )


async def _build(case, request):
    staged = await stage_segmented_source(case.sessions, request)
    assert staged.phase == "graph"
    generation_id = await graph.build_graph(case.sessions, request, staged.build_id)
    assert generation_id is not None
    return staged.build_id, generation_id


async def _complete(case, request):
    build_id, generation_id = await _build(case, request)
    sealed = await output.build_output(case.sessions, request, build_id)
    assert sealed.generation_id == generation_id
    return build_id, sealed


async def _activate(case, request, generation_id, *, base=None, version=0):
    async with case.sessions() as session, session.begin():
        return await publication.activate_generation(
            session,
            dataset_id=request.dataset_id,
            target_generation_id=generation_id,
            expected_generation_id=base,
            expected_pointer_version=version,
        )


async def _assert_sealed_materialization(case, request, sealed):
    async with case.sessions() as session, session.begin():
        generation = await session.get(CustomImportGeneration, sealed.generation_id)
        legacy = await publication._materialization(session, generation)
        persisted = await session.get(CustomImportGenerationSeal, sealed.generation_id)
        publication._validate_generation_seal_identity(persisted, generation)
        assert persisted.materialization_contract == "custom-import/materialization/v2"
        assert persisted.verification_evidence["contract"] == "custom-import/verification-sample/v1"
        assert persisted.materialization_sha256.hex() == sealed.seal.materialization_sha256
        assert persisted.effective_output_sha256.hex() == sealed.seal.effective_output_sha256
        assert legacy.materialization_sha256 != persisted.materialization_sha256
        assert legacy.effective_output_sha256 != persisted.effective_output_sha256
        assert all(getattr(legacy, name) == getattr(sealed.seal, name) for name in output._COUNT_NAMES)
        assert (await session.get(CustomImportExecution, request.execution_id)).state in {"completed", "no_change"}
        return legacy


async def test_bounded_output_seals_versioned_digest_and_replays():
    async with _source_case() as case:
        request = await _request_for(case, _records(10, 25, last_empty=True))
        build_id, sealed = await _complete(case, request)
        assert sealed.no_change is None
        assert sealed.seal.family_count == 10 and sealed.seal.family_child_count == 33
        assert sealed.seal.winner_count == 25
        await _assert_sealed_materialization(case, request, sealed)
        replay = await output.build_output(case.sessions, request, build_id)
        assert replay.seal.replayed
        async with case.sessions() as session:
            build = await session.get(CustomImportBuildAttempt, build_id)
            proof = await session.get(CustomImportBuildVerification, build_id)
            assert build.plan_page_sequence > 1 and proof.page_sequence == 1
            assert build.candidate_context_count == 33
            assert await session.get(CustomImportCurrentGeneration, request.dataset_id) is None


async def test_retained_copy_pages_record_no_change_with_exact_pointer():
    async with _source_case() as case:
        first_request = await _request_for(case, _records(2, 25, last_empty=True))
        _, first = await _complete(case, first_request)
        await _activate(case, first_request, first.generation_id)
        request = await _request_for(
            case, {"providers": [[]], "rates": [[]]}, seed=first_request, base=first.generation_id, version=1
        )
        build_id, sealed = await _complete(case, request)
        assert sealed.no_change is not None and sealed.no_change.event_kind == "no_change"
        await _assert_sealed_materialization(case, request, sealed)
        assert sealed.seal.effective_output_sha256 == first.seal.effective_output_sha256
        replay = await output.build_output(case.sessions, request, build_id)
        assert replay.no_change.replayed
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            occurrence = models[CustomImportBuildOccurrence]
            pack = models[CustomImportPack]
            pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
            assert (pointer.generation_id, pointer.pointer_version) == (first.generation_id, 1)
            copies = (await session.scalars(select(occurrence).where(occurrence.build_id == build_id))).all()
            assert len(copies) == 27 and all(copy.origin == "retained" for copy in copies)
            assert all(copy.source_ordinal is None for copy in copies)
            packs = (await session.scalars(select(pack).where(pack.execution_id == request.execution_id))).all()
            assert len(packs) == 27 and all(pack.record_count == 1 for pack in packs)


async def _retained_revisions(case, request, build_id):
    """Resolve expected retained revisions from exact frozen family ownership."""
    async with case.sessions() as session:
        models = await session.run_sync(_candidate_models, request)
        plan, family = models[CustomImportBuildFamily], models[CustomImportFamilyRevision]
        edge = models[CustomImportFamilyChild]
        selected = (
            select(family.root_revision_id)
            .select_from(plan)
            .join(
                family,
                (family.family_revision_id == plan.family_revision_id) & (family.root_record_id == plan.root_record_id),
            )
            .where(plan.build_id == build_id, plan.selection_kind == "retained")
        )
        roots = set(await session.scalars(selected))
        child_ids = set(
            await session.scalars(
                selected.join(edge, edge.family_revision_id == family.family_revision_id).with_only_columns(
                    edge.child_revision_id, maintain_column_froms=True
                )
            )
        )
        return {False: roots, True: child_ids}


async def test_final_presence_reads_only_retained_ids_in_a_mixed_origin_build(monkeypatch):
    original = compact._present_scalars_query
    audited_ids_by_kind = {False: [], True: []}

    def observed(models, generation, *, child, revision_ids):
        audited_ids_by_kind[child].extend(revision_ids)
        return original(models, generation, child=child, revision_ids=revision_ids)

    monkeypatch.setattr(compact, "_present_scalars_query", observed)
    async with _source_case() as case:
        initial = await _request_for(case, _records(2, 1), page_rows=16)
        _, first = await _complete(case, initial)
        assert not any(audited_ids_by_kind.values())
        await _activate(case, initial, first.generation_id)
        request = await _request_for(
            case, _records(1, 1, amount="11"), seed=initial, base=first.generation_id, version=1, page_rows=16
        )
        build_id, generation_id = await _build(case, request)
        expected_ids = await _retained_revisions(case, request, build_id)
        assert len(expected_ids[False]) == len(expected_ids[True]) == 1
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id and sealed.no_change is None
        assert {child: set(identifiers) for child, identifiers in audited_ids_by_kind.items()} == expected_ids
        assert all(len(identifiers) == len(expected_ids[child]) for child, identifiers in audited_ids_by_kind.items())
        assert sealed.seal.family_count == sealed.seal.family_child_count == 2
        await _assert_sealed_materialization(case, request, sealed)
        replayed = await output.build_output(case.sessions, request, build_id)
        assert replayed.seal.replayed
        assert replayed.seal.materialization_sha256 == sealed.seal.materialization_sha256
        assert all(len(identifiers) == len(expected_ids[child]) for child, identifiers in audited_ids_by_kind.items())


@pytest.mark.parametrize("collection_names", [("details", "other"), ("a2", "a_1")])
async def test_name_ordered_source_and_membership_ordered_copy_have_legacy_parity(collection_names):
    document = json.loads(_shared_definition(interleaved=True).canonical)
    document["refresh_mode"] = "upsert"
    collection_names_by_name = dict(zip(("details", "other"), collection_names, strict=True))
    for child in document["schema"]["children"]:
        child["name"] = collection_names_by_name[child["name"]]
    for stream in document["streams"]:
        if stream["kind"] == "child":
            stream["child"] = collection_names_by_name[stream["child"]]
    document["schema"]["children"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    records_by_stream = {
        "root_source": [[dict(npi="1003000126", score="1", enabled=True)]],
        "detail_source": [[dict(detail_npi="1003000126", detail_id=f"D{index:02}", amount="2") for index in range(13)]],
        "other_source": [
            [dict(other_npi="1003000126", other_id=f"O{index:02}", other_amount="3") for index in range(13)]
        ],
    }
    async with _source_case() as case:
        first_request = await _request_for(case, records_by_stream, definition=definition)
        _, first = await _complete(case, first_request)
        assert first.seal.family_child_count == 26
        await _assert_sealed_materialization(case, first_request, first)
        await _activate(case, first_request, first.generation_id)
        request = await _request_for(
            case,
            {name: [[]] for name in records_by_stream},
            seed=first_request,
            base=first.generation_id,
            version=1,
            definition=definition,
        )
        _, copied = await _complete(case, request)
        assert copied.no_change is not None
        assert copied.seal.effective_output_sha256 == first.seal.effective_output_sha256
        await _assert_sealed_materialization(case, request, copied)


async def test_pointer_drift_allows_seal_but_not_no_change_or_old_cas():
    async with _source_case() as case:
        first_request = await _request_for(case, _records(1))
        _, first = await _complete(case, first_request)
        await _activate(case, first_request, first.generation_id)
        other_request = await _request_for(
            case, _records(1, amount="20"), seed=first_request, base=first.generation_id, version=1
        )
        _, other = await _complete(case, other_request)
        request = await _request_for(
            case, {"providers": [[]], "rates": [[]]}, seed=first_request, base=first.generation_id, version=1
        )
        build_id, generation_id = await _build(case, request)
        await _activate(case, other_request, other.generation_id, base=first.generation_id, version=1)
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.no_change is None and sealed.seal.effective_output_sha256 == first.seal.effective_output_sha256
        with pytest.raises(publication.PublicationConflict, match="compare-and-swap"):
            await _activate(case, request, generation_id, base=first.generation_id, version=1)
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
            assert (pointer.generation_id, pointer.pointer_version) == (other.generation_id, 2)


async def test_python_verification_rejects_structurally_valid_wrong_family_hash(monkeypatch):
    original = graph_prepare.new_family_hash_ordered

    def _wrong_digest(*args, **kwargs):
        original(*args, **kwargs)
        return b"z" * 32

    async with _source_case() as case:
        request = await _request_for(case, _records(1))
        monkeypatch.setattr(graph_prepare, "new_family_hash_ordered", _wrong_digest)
        build_id, generation_id = await _build(case, request)
        monkeypatch.setattr(graph_prepare, "new_family_hash_ordered", original)
        with pytest.raises(CandidateRunnerError, match="family digest"):
            await output.build_output(case.sessions, request, build_id)
        async with case.sessions() as session:
            assert (await session.get(CustomImportBuildAttempt, build_id)).phase == "verified"
            assert await session.get(CustomImportGenerationSeal, generation_id) is None
            assert (await session.get(CustomImportExecution, request.execution_id)).state == "running"


async def test_python_verification_rechecks_losers_before_sealing(monkeypatch):
    original = output._winner_batch
    written_flags = []

    def _wrong_first_page(session, request, registry, build_id, generation, after):
        context_ids, page_sizes = original(session, request, registry, build_id, generation, after)
        if written_flags or not context_ids:
            return context_ids, page_sizes
        written_flags.append(True)
        with graph._read_transaction(session, request, build_id):
            context = _candidate_models(session, request)[CustomImportBuildCandidateContext]
        selected = graph._one_row(
            session,
            request,
            build_id,
            select(context).where(context.build_id == build_id, context.candidate_context_id == context_ids[0]),
            (context.candidate_context_id,),
            (context,),
        )[0]
        other = graph._one_row(
            session,
            request,
            build_id,
            select(context).where(
                context.build_id == build_id,
                context.candidate_context_id != selected.candidate_context_id,
                tuple_(context.profile_slot, context.entity_binding_id, context.context_key_sha256)
                == output._group_key(selected),
            ),
            (context.candidate_context_id,),
            (context,),
        )[0]
        return (other.candidate_context_id, *context_ids[1:]), page_sizes

    monkeypatch.setattr(output, "_winner_batch", _wrong_first_page)
    async with _source_case() as case:
        request = await _request_for(case, _records(2))
        build_id, generation_id = await _build(case, request)
        with pytest.raises(CandidateRunnerError, match="surviving-tie reduction"):
            await output.build_output(case.sessions, request, build_id)
        async with case.sessions() as session:
            assert await session.get(CustomImportGenerationSeal, generation_id) is None
            assert (await session.get(CustomImportExecution, request.execution_id)).state == "running"


async def test_projection_fanout_failure_rolls_back_family_page():
    async with _source_case() as case:
        request = await _request_for(case, _records(1), page_rows=2)
        staged = await stage_segmented_source(case.sessions, request)
        with pytest.raises(CandidateRunnerError, match="fanout"):
            await graph.build_graph(case.sessions, request, staged.build_id)
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            assert (
                await session.execute(select(func.count()).select_from(models[CustomImportFamilyRevision]))
            ).scalar_one() == 0
            assert (await session.get(CustomImportBuildAttempt, staged.build_id)).generation_id is None


@pytest.mark.parametrize("committed", [False, True])
async def test_child_batches_resume_from_the_committed_cursor(monkeypatch, committed):
    original_receipts = graph_sets._child_receipts
    original_heartbeat = graph._heartbeat
    failed_counts = []
    pending_counts = []

    def _observe_receipts(groups, receipts):
        updates = original_receipts(groups, receipts)
        if not pending_counts:
            pending_counts.append(sum(len(group.children) for group in groups))
            if not committed:
                failed_counts.extend(pending_counts)
                raise ConnectionError("synthetic uncommitted child page")
        return updates

    async def _fail_after_commit(*args):
        await original_heartbeat(*args)
        if pending_counts and not failed_counts:
            failed_counts.extend(pending_counts)
            raise ConnectionError("synthetic committed child page acknowledgement")

    async with _source_case() as case:
        request = await _request_for(case, _records(1, 13), page_rows=32)
        monkeypatch.setattr(graph_sets, "MAX_BATCH_ROWS", 2 * request.page_row_limit)
        staged = await stage_segmented_source(case.sessions, request)
        monkeypatch.setattr(graph_sets, "_child_receipts", _observe_receipts)
        if committed:
            monkeypatch.setattr(graph, "_heartbeat", _fail_after_commit)
        with pytest.raises(ConnectionError, match="synthetic"):
            await graph.build_graph(case.sessions, request, staged.build_id)
        assert 1 < failed_counts[0] < 13
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            plan = (await session.scalars(select(models[CustomImportBuildFamily]))).one()
            assert plan.attached_child_count == (failed_counts[0] if committed else 0)
            assert (
                await session.scalar(select(func.count()).select_from(models[CustomImportFamilyChild]))
                == plan.attached_child_count
            )
            assert plan.complete_at is None
        monkeypatch.setattr(graph_sets, "_child_receipts", original_receipts)
        monkeypatch.setattr(graph, "_heartbeat", original_heartbeat)
        await graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.family_child_count == 13
        await _assert_sealed_materialization(case, request, sealed)


async def test_child_batches_shrink_after_exact_sql_byte_rejection(monkeypatch):
    original = graph._source_call
    rejected_sizes = []

    async def _observe_rollback(session, name, arguments):
        if name != "append_custom_import_build_source_families_page":
            return await original(session, name, arguments)
        try:
            return await original(session, name, arguments)
        except DBAPIError as error:
            assert graph._is_child_page_bound_error(error)
            async with case.sessions() as reader:
                models = await reader.run_sync(_candidate_models, request)
                plan = (
                    await reader.scalars(
                        select(models[CustomImportBuildFamily]).where(
                            models[CustomImportBuildFamily].build_id == arguments[0][1],
                            models[CustomImportBuildFamily].root_record_id == arguments[4][1][0],
                        )
                    )
                ).one()
                assert plan.attached_child_count == arguments[6][1][0]
                assert (
                    await reader.scalar(select(func.count()).select_from(models[CustomImportFamilyChild]))
                    == plan.attached_child_count
                )
            rejected_sizes.append(len(arguments[11][1]))
            raise

    monkeypatch.setattr(graph, "_source_call", _observe_rollback)
    async with _source_case() as case:
        request = await _request_for(case, _records(1, 13), page_rows=256)
        request = replace(request, page_byte_limit=4096)
        _, sealed = await _complete(case, request)
        assert rejected_sizes and min(rejected_sizes) > 1
        assert sealed.seal.family_child_count == 13
        await _assert_sealed_materialization(case, request, sealed)


@pytest.mark.parametrize("canceled", [False, True])
async def test_child_batches_stop_at_the_next_authority_check(monkeypatch, canceled):
    original_receipts = graph_sets._child_receipts
    original_heartbeat = graph._heartbeat
    pending_counts = []
    appended_sizes = []

    def _observe_receipts(groups, receipts):
        updates = original_receipts(groups, receipts)
        pending_counts.append(sum(len(group.children) for group in groups))
        return updates

    async def _stop_after_page(session_factory, request):
        if pending_counts and not appended_sizes:
            appended_sizes.extend(pending_counts)
            async with session_factory() as session, session.begin():
                if canceled:
                    await lifecycle.request_cancellation(session, execution_id=request.execution_id)
                else:
                    await session.execute(
                        update(CustomImportLease)
                        .where(CustomImportLease.execution_id == request.execution_id)
                        .values(expires_at=func.clock_timestamp() - dt.timedelta(seconds=1))
                    )
        await original_heartbeat(session_factory, request)

    monkeypatch.setattr(graph_sets, "_child_receipts", _observe_receipts)
    monkeypatch.setattr(graph, "_heartbeat", _stop_after_page)
    async with _source_case() as case:
        request = await _request_for(case, _records(1, 13), page_rows=32)
        monkeypatch.setattr(graph_sets, "MAX_BATCH_ROWS", 2 * request.page_row_limit)
        staged = await stage_segmented_source(case.sessions, request)
        with pytest.raises(CancellationRequested if canceled else LeaseAuthorityLost):
            await graph.build_graph(case.sessions, request, staged.build_id)
        assert len(appended_sizes) == 1 and 1 < appended_sizes[0] < 13
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            plan = (await session.scalars(select(models[CustomImportBuildFamily]))).one()
            assert plan.attached_child_count == appended_sizes[0] and plan.complete_at is None
            assert (await session.get(CustomImportBuildAttempt, staged.build_id)).generation_id is None


@pytest.mark.parametrize("membership", [False, True])
async def test_child_batches_preserve_duplicate_and_membership_admission(monkeypatch, membership):
    from tests import test_custom_import_identical_children_postgres as admission

    original = graph_sets._child_receipts
    batch_sizes = []

    def _observe_page(groups, receipts):
        updates = original(groups, receipts)
        batch_sizes.append(sum(len(group.children) for group in groups))
        return updates

    monkeypatch.setattr(graph_sets, "_child_receipts", _observe_page)
    if membership:
        case_factory = admission._source_case
        definition = admission._membership_definition(reverse=True)
        records_by_stream = admission._membership_records(inner_count=13, missing=True)
    else:
        case_factory = admission._source_case
        definition = admission._configured_definition()
        records_by_stream = _records(1, 13)
        records_by_stream["rates"].append([child.copy() for page in records_by_stream["rates"] for child in page])
    async with case_factory() as case:
        request = await _request_for(case, records_by_stream, definition=definition, page_rows=256)
        _, sealed = await _complete(case, request)
        assert max(batch_sizes) > 1
        assert sealed.seal.family_count == 1
        assert sealed.seal.family_child_count == (14 if membership else 13)
        await _assert_sealed_materialization(case, request, sealed)


async def test_compact_finalizer_matches_exhaustive_on_the_same_frozen_snapshot(monkeypatch):
    original = output._frozen_materialization
    exhaustive_materializations = []

    def compare(*arguments):
        sampled = original(*arguments)
        exhaustive = output._exhaustive_materialization(*arguments)
        assert sampled.materialization_contract == "custom-import/materialization/v2"
        assert exhaustive.materialization_contract == "custom-import/materialization/v1"
        assert sampled.source_bundle_sha256 == exhaustive.source_bundle_sha256
        assert all(getattr(sampled, name) == getattr(exhaustive, name) for name in output._COUNT_NAMES)
        exhaustive_materializations.append(exhaustive)
        return sampled

    monkeypatch.setattr(output, "_frozen_materialization", compare)
    async with _source_case() as case:
        request = await _request_for(case, _records(4, 25, last_empty=True), page_rows=64)
        _, sealed = await _complete(case, request)
        readback = await _assert_sealed_materialization(case, request, sealed)
        assert len(exhaustive_materializations) == 1
        assert exhaustive_materializations[0].materialization_sha256 == readback.materialization_sha256
        assert exhaustive_materializations[0].effective_output_sha256 == readback.effective_output_sha256
        assert sealed.seal.family_count == 4 and sealed.seal.family_child_count == 27


async def test_compact_checkpoint_cancellation_resumes_the_same_build_and_sample(monkeypatch):
    original = compact._complete_coverage
    seeds = []
    task = None
    loop = asyncio.get_running_loop()

    def checkpoint(session, request, build_id, generation, proof, winner_populations):
        result = original(session, request, build_id, generation, proof, winner_populations)
        if not seeds:
            seeds.append(compact._seed(generation, proof).hex())
            loop.call_soon(task.cancel)
        return result

    async with _source_case() as case:
        request = await _request_for(case, _records(1, 36), page_rows=128)
        build_id, generation_id = await _build(case, request)
        monkeypatch.setattr(compact, "_complete_coverage", checkpoint)
        task = asyncio.create_task(output.build_output(case.sessions, request, build_id))
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(seeds) == 1
        async with case.sessions() as session:
            assert await session.get(CustomImportGenerationSeal, generation_id) is None
            assert (await session.get(CustomImportBuildAttempt, build_id)).phase == "verified"
            assert (await session.get(CustomImportExecution, request.execution_id)).state == "running"
        monkeypatch.setattr(compact, "_complete_coverage", original)
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id
        await _assert_sealed_materialization(case, request, sealed)
        async with case.sessions() as session:
            persisted = await session.get(CustomImportGenerationSeal, generation_id)
            assert persisted.verification_evidence["seed_sha256"] == seeds[0]
        replay = await output.build_output(case.sessions, request, build_id)
        assert replay.seal.replayed and replay.generation_id == generation_id


def _null_and_missing_records():
    document = json.loads(_definition().canonical)
    document["schema"]["root"]["fields"].append(
        {"id": "optional_note", "slot": 7, "type": "string", "nullable": True, "projection_slot": 5}
    )
    document["query"]["root_fields"].append("optional_note")
    roots = [
        {"npi": "1234567893", "display_name": "Null", "optional_note": None},
        {"npi": "1234567893", "display_name": "Missing"},
    ]
    child_records = [
        {"rate_npi": "1234567893", "rate_name": "Null", "service_code": "N", "amount": None},
        {"rate_npi": "1234567893", "rate_name": "Missing", "service_code": "M"},
    ]
    return CustomImportDefinition.from_mapping(document), roots, child_records


async def test_compact_fresh_null_and_retained_missing_survive_publication_and_reads():
    definition, roots, children = _null_and_missing_records()
    async with _source_case() as case:
        seed = await legacy_runner._seed_case(case, "compact_states", definition)
        execution_id, token = await legacy_runner._new_execution(case, seed, "compact_states")
        initial = await runner.run_candidate(
            case.sessions, legacy_runner._request(seed, execution_id, token, roots, children)
        )
        assert initial.status == "activated"
        async with case.sessions() as session, session.begin():
            previous = await publication._materialization(
                session, await session.get(CustomImportGeneration, initial.generation_id)
            )
        # Fixed-schema Parquet represents absent cells as null; retained canonical rows preserve missing.
        request = await _request_for(
            case,
            {"providers": [[roots[0]]], "rates": [[children[0]]]},
            seed=seed,
            base=initial.generation_id,
            version=1,
            definition=definition,
            page_rows=16,
        )
        _, sealed = await _complete(case, request)
        assert sealed.no_change is None
        current = await _assert_sealed_materialization(case, request, sealed)
        assert current.effective_output_sha256 == previous.effective_output_sha256
        assert sealed.seal.root_scalar_count == 5 and sealed.seal.child_scalar_count == 3
        async with case.sessions() as session:
            models = await legacy_runner._generation_models(session, sealed.generation_id)
            for scalar, slot in ((models[CustomImportRootScalar], 7), (models[CustomImportChildScalar], 5)):
                states = (await session.scalars(select(scalar.value_state).where(scalar.field_slot == slot))).all()
                assert states == ["null"]
        await _activate(case, request, sealed.generation_id, base=initial.generation_id, version=1)
        read_target = read_core.PinnedReadTarget(
            request.dataset_id,
            sealed.generation_id,
            request.definition_revision_id,
            request.schema_revision_id,
            "default",
        )
        async with case.sessions() as session:
            page = await _service().search(
                session,
                authorization=read_core.ExtensionReadAuthorization("synthetic-compact-states"),
                request=read_core.SearchRequest(target=read_target, page_size=10),
            )
        assert page.total == len(page.items) == 2
        for fields, name in (("context_fields", "amount"), ("root_fields", "optional_note")):
            states = {
                next(field.state for field in getattr(entry, fields) if field.field_id == name) for entry in page.items
            }
            assert states == {"null", "missing"}


def _downgrade_temporary_materialization_metadata(connection):
    migration = _contract_migration()
    migration._schema = lambda: "pg_temp"
    migration.op = Operations(MigrationContext.configure(connection))
    migration.downgrade()


async def _reject_incompatible_materialization_metadata(connection):
    for contract, evidence in (
        ("custom-import/materialization/v1", "{}"),
        ("custom-import/materialization/v1", "null"),
        ("custom-import/materialization/v2", None),
        ("custom-import/materialization/v2", "[]"),
        ("custom-import/materialization/v2", "null"),
        ("unknown", None),
    ):
        with pytest.raises(DBAPIError) as failed:
            async with connection.begin_nested():
                await connection.execute(
                    text(
                        "INSERT INTO pg_temp.custom_import_generation_seal "
                        "VALUES (2, :contract, CAST(:evidence AS jsonb))"
                    ),
                    {"contract": contract, "evidence": evidence},
                )
        assert failed.value.orig.sqlstate == "23514"


async def test_compact_metadata_upgrade_enforces_checks_and_refuses_lossy_downgrade():
    async with _source_case() as case, case.engine.connect() as connection:
        async with connection.begin():
            await connection.execute(
                text("CREATE TEMP TABLE custom_import_generation_seal (id integer PRIMARY KEY) ON COMMIT DROP")
            )
            await connection.execute(text("INSERT INTO pg_temp.custom_import_generation_seal VALUES (1)"))
            await connection.run_sync(install_materialization_contract_migration, "pg_temp")
            legacy_metadata = (
                await connection.execute(
                    text(
                        "SELECT materialization_contract, verification_evidence FROM pg_temp.custom_import_generation_seal"
                    )
                )
            ).one()
            assert tuple(legacy_metadata) == ("custom-import/materialization/v1", None)
            await _reject_incompatible_materialization_metadata(connection)
            await connection.execute(
                text(
                    "INSERT INTO pg_temp.custom_import_generation_seal VALUES (3, 'custom-import/materialization/v2', '{}'::jsonb)"
                )
            )
            with pytest.raises(RuntimeError, match="compact_materialization_downgrade_blocked"):
                async with connection.begin_nested():
                    await connection.run_sync(_downgrade_temporary_materialization_metadata)
            retained_contracts = (
                await connection.execute(
                    text("SELECT id, materialization_contract FROM pg_temp.custom_import_generation_seal ORDER BY id")
                )
            ).all()
            assert retained_contracts == [
                (1, "custom-import/materialization/v1"),
                (3, "custom-import/materialization/v2"),
            ]
        assert await connection.scalar(text("SELECT to_regclass('pg_temp.custom_import_generation_seal')")) is None
