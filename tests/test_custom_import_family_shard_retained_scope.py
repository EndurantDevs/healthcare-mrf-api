# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual empty retained descriptors; only native authority/SQL is faked."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_base_values as originals
from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_admission import read_admission_occurrences
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.runner_codec import digest_text
from tests import test_custom_import_family_shard_common as common_fixture
from tests import test_custom_import_family_shard_retained_homes as base_fixture
from tests.test_custom_import_family_shard_retained_allocate import _produce_destination
from tests.test_custom_import_family_shard_source_values import _case as _source_case

_FACTS = SourceFactLimits(64 * 1024**2, 1024**2, 1024**2)
_LIMITS = allocation.SourceAllocationLimits(_FACTS, 128, 1024**2)


def _work(root, registered):
    return allocation.RetainedAllocationWork(root, _LIMITS, registered.append)


@asynccontextmanager
async def _case(tmp_path, monkeypatch):
    registered_paths = []
    async with _source_case(tmp_path, monkeypatch) as original_source:
        descriptor = await originals.build_retained_input(
            None,
            original_source.plan,
            work=originals.RetainedWork(tmp_path / "empty-retained", _FACTS, registered_paths.append),
        )
        common = None
        try:
            selected_items = tuple(
                plan.source_metadata[1]
                for plan in allocation.read_family_plan(original_source.plan)
                if plan.selection_kind == "source"
            )
            keys = sorted(
                {
                    (digest_text("child-key", source_item.record.child_key), source_item.record.child_key)
                    for source_item in read_admission_occurrences(original_source.admission)
                    if source_item.record.child_key is not None
                }
            )
            common_fixture._lookup_fixture(monkeypatch, original_source.context)
            common = await common_fixture._build_common(
                tmp_path,
                original_source.context,
                selected=selected_items,
                keys=tuple((digest, canonical, rank, 1) for rank, (digest, canonical) in enumerate(keys, 1)),
            )
            context = allocation.RetainedAllocationContext(
                original_source.context,
                31,
                original_source.admission,
                original_source.plan,
                descriptor,
                None,
                common,
            )
            yield context, registered_paths
        finally:
            if common is not None:
                common.close()
            originals.cleanup_retained_input(descriptor)


async def test_actual_empty_no_base_descriptor_completes_with_all_current_guards(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as (context, registered):
        original = context.retained_input
        assert original.current_definition_sha256 == original.definition_canonical == ""
        assert original.base_identity is original.origin_scope is None
        assert original.collection_slots == () and not any(original.counts_by_role.values())
        assert context.common.source.source.counts[0] > 0
        prefix = allocation._source_prefix(context, SimpleNamespace(current=lambda: None))
        native = base_fixture._NativeBoundary(
            SimpleNamespace(common=context.common, destination=SimpleNamespace(source_pack_prefix=prefix))
        )
        native.install(monkeypatch)
        monkeypatch.setattr(allocation, "_page_session", native.page)

        async def forbidden_ids(*_arguments):
            raise AssertionError("empty retained output must not observe native destination IDs")

        monkeypatch.setattr(allocation, "_observe_ids", forbidden_ids)
        destination = await _produce_destination(None, context, work=_work(tmp_path / "destination", registered))
        try:
            assert list(allocation.read_retained_destination(destination)) == []
            assert not any(dict(destination.counts).values())
            assert destination.source_pack_prefix == destination.destination_pack_tail == prefix
            assert native.observations == ["begin", "candidate", "fresh_authority", "commit"]
        finally:
            allocation.cleanup_retained_destination(destination)


@pytest.mark.parametrize(
    "damage",
    (
        "root",
        "child",
        "scalar",
        "pack",
        "origin",
        "counts_type",
        "definition",
        "collections",
        "origin_scope",
        "digest",
        "current_digest",
        "plan_sha",
        "base_identity",
    ),
)
async def test_no_base_accepts_only_exact_empty_producer_shape(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as (context, registered):
        descriptor = context.retained_input
        match damage:
            case "pack" | "scalar" | "root" | "origin" | "child":
                descriptor = replace(
                    descriptor, counts=tuple((name, count + int(name == damage)) for name, count in descriptor.counts)
                )
            case "counts_type":
                descriptor = replace(
                    descriptor,
                    counts=tuple((name, False if name == "root" else count) for name, count in descriptor.counts),
                )
            case _:
                changes_by_key = {
                    "definition": {"definition_canonical": context.common.request.definition.canonical},
                    "collections": {"collection_slots": (("details", 7),)},
                    "origin_scope": {"origin_scope": (77, 31)},
                    "digest": {"current_definition_sha256": "x" * 64},
                    "current_digest": {"current_definition_sha256": context.common.request.definition.digest},
                    "plan_sha": {"plan_sha256": b"x" * 32},
                    "base_identity": {"base_identity": (77, 31)},
                }
                descriptor = replace(descriptor, **changes_by_key[damage])
        with pytest.raises(FamilyBlockUnavailable, match="original-input scope"):
            allocation._scope(replace(context, retained_input=descriptor), _work(tmp_path / "unused", registered))
        assert not (tmp_path / "unused").exists()


@pytest.mark.parametrize("damage", ("admission", "source", "request_scope", "base_scope", "common_provenance"))
async def test_empty_sentinel_preserves_current_source_admission_plan_guards(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as (context, registered):
        match damage:
            case "admission":
                context = replace(context, admission=replace(context.admission, sha256=b"x" * 32))
            case "source":
                context = replace(
                    context, common=replace(context.common, source=replace(context.common.source, sha256=b"x" * 32))
                )
            case "request_scope":
                context = replace(context, final_plan=replace(context.final_plan, request_scope=()))
            case "base_scope":
                context = replace(context, final_plan=replace(context.final_plan, base_scope=(77, 31)))
            case _:
                context.common_output._proof_inputs = tuple(
                    sorted((context.common_output.proof_inputs | {"execution_id": 999}).items())
                )
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            allocation._scope(context, _work(tmp_path / "unused", registered))


@pytest.mark.parametrize("empty", (False, True))
async def test_live_base_keeps_exact_current_digest_even_without_retained_selection(tmp_path, monkeypatch, empty):
    async with base_fixture._case(tmp_path, monkeypatch, empty=empty) as case:
        context = case.context
        work = _work(tmp_path / "unused", case.registered)
        assert context.retained_input.current_definition_sha256 == context.common.request.definition.digest
        assert allocation._scope(context, work)
        for digest in ("", "x" * 64):
            with pytest.raises(FamilyBlockUnavailable, match="original-input scope"):
                allocation._scope(
                    replace(context, retained_input=replace(context.retained_input, current_definition_sha256=digest)),
                    work,
                )
        with pytest.raises(FamilyBlockUnavailable, match="actual live base"):
            allocation._scope(replace(context, base=None), work)
        with pytest.raises(FamilyBlockUnavailable, match="original-input scope"):
            allocation._scope(
                replace(context, retained_input=replace(context.retained_input, base_identity=None)), work
            )
