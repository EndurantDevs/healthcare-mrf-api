# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native bounded graph/output parity, retained copies, and ordinary finality."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace

import pytest
from sqlalchemy import func, select, tuple_

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildCandidateContext,
    CustomImportBuildOccurrence,
    CustomImportBuildVerification,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportPack,
)
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import publication
from process.custom_import.build_source import SourceBuildRequest, stage_segmented_source
from process.custom_import.capture_pending import seal_pending_parquet_bundle
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.definition_store import register_definition
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source_postgres import _sealed_parts, _source_case
from tests.test_custom_import_capture_pending_postgres import _receipt, _retain, _start_attempt
from tests.test_custom_import_definition import _raw_definition
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


async def _assert_legacy_parity(case, request, sealed):
    async with case.sessions() as session, session.begin():
        generation = await session.get(CustomImportGeneration, sealed.generation_id)
        legacy = await publication._materialization(session, generation)
        assert legacy.materialization_sha256.hex() == sealed.seal.materialization_sha256
        assert legacy.effective_output_sha256.hex() == sealed.seal.effective_output_sha256
        assert all(getattr(legacy, name) == getattr(sealed.seal, name) for name in output._COUNT_NAMES)
        assert (await session.get(CustomImportExecution, request.execution_id)).state in {"completed", "no_change"}


async def test_bounded_output_matches_legacy_digest_and_replays():
    async with _source_case() as case:
        request = await _request_for(case, _records(10, 25, last_empty=True))
        build_id, sealed = await _complete(case, request)
        assert sealed.no_change is None
        assert sealed.seal.family_count == 10 and sealed.seal.family_child_count == 33
        assert sealed.seal.winner_count == 25
        await _assert_legacy_parity(case, request, sealed)
        replay = await output.build_output(case.sessions, request, build_id)
        assert replay.seal.replayed
        async with case.sessions() as session:
            build = await session.get(CustomImportBuildAttempt, build_id)
            proof = await session.get(CustomImportBuildVerification, build_id)
            assert build.plan_page_sequence > 1 and proof.page_sequence > 1
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
        await _assert_legacy_parity(case, request, sealed)
        assert sealed.seal.effective_output_sha256 == first.seal.effective_output_sha256
        replay = await output.build_output(case.sessions, request, build_id)
        assert replay.no_change.replayed
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
            assert (pointer.generation_id, pointer.pointer_version) == (first.generation_id, 1)
            copies = (
                await session.scalars(
                    select(CustomImportBuildOccurrence).where(CustomImportBuildOccurrence.build_id == build_id)
                )
            ).all()
            assert len(copies) == 27 and all(copy.origin == "retained" for copy in copies)
            assert all(copy.source_ordinal is None for copy in copies)
            packs = (
                await session.scalars(
                    select(CustomImportPack).where(CustomImportPack.execution_id == request.execution_id)
                )
            ).all()
            assert len(packs) == 27 and all(pack.record_count == 1 for pack in packs)


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
        await _assert_legacy_parity(case, first_request, first)
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
        await _assert_legacy_parity(case, request, copied)


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
    original = graph.new_family_hash_ordered

    def _wrong_digest(*args, **kwargs):
        original(*args, **kwargs)
        return b"z" * 32

    async with _source_case() as case:
        request = await _request_for(case, _records(1))
        monkeypatch.setattr(graph, "new_family_hash_ordered", _wrong_digest)
        build_id, generation_id = await _build(case, request)
        with pytest.raises(CandidateRunnerError, match="family digest"):
            await output.build_output(case.sessions, request, build_id)
        async with case.sessions() as session:
            assert (await session.get(CustomImportBuildAttempt, build_id)).phase == "verified"
            assert await session.get(CustomImportGenerationSeal, generation_id) is None
            assert (await session.get(CustomImportExecution, request.execution_id)).state == "running"


async def test_python_verification_rechecks_losers_before_sealing(monkeypatch):
    original = output._reduce_group
    written_flags = []

    def _wrong_first_group(session, request, registry, build_id, generation, group):
        winner, context_id = original(session, request, registry, build_id, generation, group)
        if written_flags:
            return winner, context_id
        written_flags.append(True)
        context = CustomImportBuildCandidateContext
        other = graph._one_row(
            session,
            request,
            build_id,
            select(context).where(
                context.build_id == build_id,
                context.candidate_context_id != context_id,
                tuple_(context.profile_slot, context.entity_binding_id, context.context_key_sha256) == group,
            ),
            (context.candidate_context_id,),
            (context,),
        )[0]
        return replace(
            winner,
            family_revision_id=other.family_revision_id,
            context_child_revision_id=other.context_child_revision_id,
        ), other.candidate_context_id

    monkeypatch.setattr(output, "_reduce_group", _wrong_first_group)
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
            assert (
                await session.execute(select(func.count()).select_from(CustomImportFamilyRevision))
            ).scalar_one() == 0
            assert (await session.get(CustomImportBuildAttempt, staged.build_id)).generation_id is None
