# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded membership cells are scoped data, never full family acceptance."""

from dataclasses import replace

import pytest

from process.custom_import import runner_codec
from process.custom_import.family_block_records import FamilyRecord, SparseRecord
from process.custom_import.family_shard_output import AllocatedFamily, generation_member_batches
from process.custom_import.family_shard_plan import read_family_plan
from process.custom_import.materialization import GenerationIdentity
from tests.test_custom_import_family_shard_graph_control import _plan_case


def _generation(native):
    request = native.context.request
    return GenerationIdentity(93, request.dataset_id, request.definition_revision_id, request.schema_revision_id)


def _family(native, plan):
    """Supply only member identifiers; these records do not assert a semantic proof."""
    if plan.selection_kind == "retained":
        text, digest = plan.typed_key
        payload = "opaque original root"
        record = FamilyRecord(
            501,
            701,
            0,
            None,
            sparse=SparseRecord(payload, text, runner_codec.digest_text("root-payload", payload), digest),
        )
        return AllocatedFamily(
            801,
            plan.root_record_id,
            plan.base_header.entity_binding_id,
            record,
            {},
            plan.base_header.family_sha256,
            True,
        )
    occurrence = plan.source_root
    source = occurrence.record
    definition = native.context.request.definition
    values = source.normalized_values(definition, definition.source_streams[source.stream_slot - 1])
    record = FamilyRecord(occurrence.revision_id, occurrence.pack_id, source.source_ordinal, values)
    return AllocatedFamily(
        *plan.source_metadata[:1], plan.root_record_id, plan.source_metadata[1], record, {}, b"h" * 32
    )


def _batches(native, pairs, *, plan=None, generation=None, check=None):
    return generation_member_batches(
        native.context,
        generation or _generation(native),
        plan or native.plan,
        pairs,
        maximum_rows=1,
        maximum_bytes=1024,
        check=check,
    )


@pytest.mark.parametrize("kind", ["source", "empty", "retained", "mixed", "absent"])
async def test_complete_original_selections_make_only_scoped_member_cells(tmp_path, monkeypatch, kind):
    async with _plan_case(tmp_path, monkeypatch, kind) as native:
        plans = tuple(plan for plan in read_family_plan(native.plan) if plan.selection_kind != "absent")
        pairs = [(plan, _family(native, plan)) for plan in plans]
        batches = tuple(_batches(native, iter(pairs)))
        assert all(set(batch) == {"generation_family"} and len(batch["generation_family"]) == 1 for batch in batches)
        request = native.context.request
        assert tuple(cells for batch in batches for cells in batch["generation_family"]) == tuple(
            (
                93,
                request.dataset_id,
                request.definition_revision_id,
                request.schema_revision_id,
                plan.root_record_id,
                family.family_revision_id,
            )
            for plan, family in pairs
        )


@pytest.mark.parametrize("damage", ["missing", "extra", "root", "revision", "selection", "scope", "late_sha"])
async def test_missing_or_changed_original_member_input_cannot_finish(tmp_path, monkeypatch, damage):
    async with _plan_case(tmp_path, monkeypatch) as native:
        (plan,) = read_family_plan(native.plan)
        family = _family(native, plan)
        pairs, final_plan, generation = [(plan, family)], native.plan, _generation(native)
        match damage:
            case "missing":
                pairs = []
            case "extra":
                pairs *= 2
            case "root":
                pairs = [(plan, replace(family, root_record_id=True))]
            case "revision":
                pairs = [(plan, replace(family, root=replace(family.root, revision_id=9999)))]
            case "selection":
                pairs = [(plan, replace(family, retained=True))]
            case "scope":
                generation = replace(generation, dataset_id=generation.dataset_id + 1)
            case "late_sha":
                final_plan = replace(final_plan, sha256=b"x" * 32)
        with pytest.raises(ValueError):
            tuple(_batches(native, iter(pairs), plan=final_plan, generation=generation))


async def test_cancelled_member_stream_closes_original_inputs(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch) as native:
        (plan,) = read_family_plan(native.plan)
        close_calls = []

        def families():
            try:
                yield plan, _family(native, plan)
            finally:
                close_calls.append(True)

        failure = InterruptedError("synthetic member output stopped")

        def check():
            raise failure

        iterator = families()
        next_pair = next(iterator)

        class OpenedInputs:
            def __init__(self):
                self.first = True

            def __iter__(self):
                return self

            def __next__(self):
                if self.first:
                    self.first = False
                    return next_pair
                return next(iterator)

            def close(self):
                iterator.close()

        with pytest.raises(InterruptedError) as caught:
            tuple(_batches(native, OpenedInputs(), check=check))
        assert caught.value is failure and close_calls == [True]
