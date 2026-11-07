# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real retained spools/codecs; native SQL/pins/sequence observations are mocked."""

import asyncio
import hashlib
import threading
from contextlib import asynccontextmanager, closing
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_retained_encoding as encoding
from process.custom_import import family_shard_retained_storage_validation as validation
from process.custom_import import family_shard_retained_validation as original_validation
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_keys import iter_key_dictionary
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded
from process.custom_import.family_shard_output import FamilyOutputWork, _pack_row, _unpack_row, build_family_output
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_codec import child_key_document, digest_text
from tests import test_custom_import_family_shard_base_values as base_fixture
from tests import test_custom_import_family_shard_retained_encoding as encoding_fixture
from tests import test_custom_import_family_shard_retained_homes as fixture


def _work(tmp_path, *, check=None, memory=None, spool=None):
    limits = fixture._FACTS
    if memory is not None:
        limits = replace(limits, memory_bytes=memory)
    if spool is not None:
        limits = replace(limits, max_spool_bytes=spool)
    return PlanWork(tmp_path / "stored-proof", limits, lambda _path: None, check)


def _output_work(tmp_path):
    return FamilyOutputWork(
        tmp_path / "retained-output", FamilyBlockLimits(32 * 1024**2, 1024**2, 64, 1024**2), lambda _path: None
    )


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, empty=False, opaque=False, block=False):
    definition = fixture._definition("upsert")
    _root, children = base_fixture._records()
    key_parts = (
        []
        if empty
        else [
            (digest_text("child-key", key), key)
            for child in children
            for key in (child_key_document(definition, "details", child.values_by_field),)
        ]
    )
    dictionary_rows = tuple(
        (entry.child_key_sha256, entry.canonical_child_key, entry.key_rank, entry.key_variant)
        for entry in iter_key_dictionary(sorted(key_parts))
    )
    original_common = fixture.common_fixture._build_common

    async def common_with_keys(tmp_path, context, selected=()):
        return await original_common(tmp_path, context, selected=selected, keys=dictionary_rows)

    monkeypatch.setattr(fixture.common_fixture, "_build_common", common_with_keys)
    if block:

        def block_rows(base, header, root, children):
            return base_fixture._block_rows(tmp_path, base, header, root, children)

        monkeypatch.setattr(base_fixture, "_legacy_rows", block_rows)
    elif opaque:
        original_rows = base_fixture._legacy_rows

        def opaque_rows(base, header, root, children):
            return original_rows(base, header, root, children, opaque=True)

        monkeypatch.setattr(base_fixture, "_legacy_rows", opaque_rows)
    async with fixture._case(tmp_path, monkeypatch, empty=empty) as case:
        comparison = await original_validation.verify_retained_destination(
            case.context,
            case.destination,
            work=fixture.allocation.RetainedAllocationWork(
                tmp_path / "original-proof", fixture._LIMITS, case.registered.append
            ),
        )
        keys = lambda: iter(dictionary_rows)
        output = encoding.build_retained_output(
            case.context, case.destination, comparison, keys, work=_output_work(tmp_path), shard_id=17
        )
        try:
            yield SimpleNamespace(
                context=case.context,
                destination=case.destination,
                comparison=comparison,
                output=output,
                query_keys=keys,
            )
        finally:
            output.close()


async def _prove(tmp_path, case, **options):
    return await validation.prove_retained_output(
        case.context, case.destination, case.comparison, case.output, case.query_keys, work=_work(tmp_path, **options)
    )


@pytest.mark.parametrize(
    "empty,opaque,block", [(False, False, False), (False, True, False), (True, False, False), (False, False, True)]
)
async def test_actual_stored_records_match_completed_retained_assignment(tmp_path, monkeypatch, empty, opaque, block):
    async with _case(tmp_path, monkeypatch, empty=empty, opaque=opaque, block=block) as case:
        result = await _prove(tmp_path, case)
        assert result.scope == case.comparison.scope
        assert result.destination_sha256 == case.destination.sha256
        assert result.original_input_sha256 == case.context.retained_input.sha256
        assert result.counts == case.comparison.counts
        assert (result.root_scalar_count, result.child_scalar_count) == ((0, 0) if empty else (1, 2))
        assert (result.dictionary_key_count, result.used_query_key_count) == ((0, 0) if empty else (2, 2))
        assert result.unproven_boundaries == ("native_output", "winner_contexts")
        assert empty or case.output.receipt["role_counts"]["fragment"] > 1
        assert await _prove(tmp_path, case) == result
        with pytest.raises(FrozenInstanceError):
            result.scope = ()
        assert not (tmp_path / "stored-proof").exists() and case.output.path.exists()
        assert Path(case.destination.root).exists() and Path(case.context.retained_input.root).exists()


@pytest.mark.parametrize(
    "damage",
    ["scope", "scope_list", "destination", "mutable_sha", "counts", "duplicate", "split", "boolean", "missing"],
)
async def test_original_proof_is_mandatory_and_exactly_scoped(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        changes_by_damage = {
            "scope": dict(scope=()),
            "scope_list": dict(scope=list(case.comparison.scope)),
            "destination": dict(destination_sha256=b"x" * 32),
            "mutable_sha": dict(destination_sha256=memoryview(case.destination.sha256)),
            "counts": dict(counts=()),
            "duplicate": dict(counts=(*case.comparison.counts, case.comparison.counts[-1])),
            "split": dict(root_scalar_count=0),
            "boolean": dict(root_scalar_count=True),
        }
        case.comparison = None if damage == "missing" else replace(case.comparison, **changes_by_damage[damage])
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _prove(tmp_path, case)
        assert not (tmp_path / "stored-proof").exists()


def _rewrite_output(case, mutation):
    output, layout = case.output, case.context.common.layout
    frames = list(output._work.rows(output.path))
    mutation(frames)
    receipt = output.receipt
    proof = ShardOutputProof(layout, receipt["shard_id"], receipt["partition_sha256"], receipt["source_record_count"])
    output.path.chmod(0o600)
    with output.path.open("wb") as target:
        for role, packed in frames:
            proof.add(role, (_unpack_row(packed),))
            output._work.frame(target, (role, packed))
    output.path.chmod(0o400)
    output._identity = output.path.stat()
    output.sha256 = hashlib.sha256(output.path.read_bytes()).digest()
    output._receipt = proof.receipt()


@pytest.mark.parametrize(
    "damage", ["family", "root", "entity", "retained", "fragment", "membership", "missing", "extra"]
)
async def test_resealed_output_cannot_substitute_for_independent_proof(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:

        def mutate(frames):
            if damage in {"missing", "extra"}:
                frames.pop() if damage == "missing" else frames.append(frames[-1])
                return
            role = "fragment" if damage == "fragment" else "revision_run" if damage == "membership" else "family"
            index = next(index for index, entry in enumerate(frames) if entry[0] == role)
            cells = list(_unpack_row(frames[index][1]))
            if damage == "fragment":
                cells[3] = bytes([cells[3][0] ^ 1]) + cells[3][1:]
            elif damage == "retained":
                columns = case.context.common.layout.tables(case.context.common.source.snapshot_family_id)[
                    "family"
                ].c.keys()
                cells[list(columns).index("retained")] = False
            else:
                cells[{"family": 0, "root": 1, "entity": 3, "membership": 0}[damage]] += 1
            frames[index] = role, _pack_row(tuple(cells))

        _rewrite_output(case, mutate)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _prove(tmp_path, case)
        assert not (tmp_path / "stored-proof").exists() and case.output.path.exists()


@pytest.mark.parametrize("damage", ["partition", "source_count"])
async def test_output_receipt_keeps_real_source_partition_and_zero_source_rows(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        case.output._receipt["partition_sha256" if damage == "partition" else "source_record_count"] = (
            b"x" * 32 if damage == "partition" else 1
        )
        with pytest.raises(FamilyBlockUnavailable, match="partition or source count"):
            await _prove(tmp_path, case)


@pytest.mark.parametrize("owner", ["output", "destination", "original"])
async def test_proof_bindings_cannot_contain_mutable_digest_views(tmp_path, monkeypatch, owner):
    async with _case(tmp_path, monkeypatch) as case:
        if owner == "output":
            case.output.sha256 = memoryview(case.output.sha256)
        elif owner == "destination":
            case.destination = replace(case.destination, sha256=memoryview(case.destination.sha256))
        else:
            retained_input = replace(case.context.retained_input, sha256=memoryview(case.context.retained_input.sha256))
            case.context = replace(case.context, retained_input=retained_input)
        with pytest.raises(FamilyBlockUnavailable):
            await _prove(tmp_path, case)
        assert not (tmp_path / "stored-proof").exists()


async def test_changed_live_base_after_decode_refuses_evidence(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        original, checks = validation.allocation._check_base, []

        async def check(context):
            await original(context)
            checks.append(True)
            if len(checks) == 2:
                raise FamilyBlockUnavailable("synthetic base pin changed")

        monkeypatch.setattr(validation.allocation, "_check_base", check)
        with pytest.raises(FamilyBlockUnavailable, match="base pin changed"):
            await _prove(tmp_path, case)
        assert checks == [True, True] and not (tmp_path / "stored-proof").exists()


async def test_cancelled_decoder_drains_before_originals_can_close(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        original, entered, release, finished = (
            validation._verify_family,
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )

        def blocked(*arguments):
            entered.set()
            try:
                assert release.wait(5)
                return original(*arguments)
            finally:
                finished.set()

        monkeypatch.setattr(validation, "_verify_family", blocked)
        pending = asyncio.create_task(_prove(tmp_path, case))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            pending.cancel("first retained storage cancellation")
            await asyncio.sleep(0)
            assert not pending.done() and (tmp_path / "stored-proof").exists()
            pending.cancel("second retained storage cancellation")
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError, match="first retained storage cancellation"):
            await pending
        assert finished.is_set() and not (tmp_path / "stored-proof").exists()
        assert case.output.path.exists() and Path(case.destination.root).exists()


async def test_owner_resource_failure_keeps_original_artifacts(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        with pytest.raises((FamilyBlockResourceLimit, SourceFactBudgetExceeded)):
            await _prove(tmp_path, case, memory=1024)
        assert not (tmp_path / "stored-proof").exists()
        assert case.output.path.exists() and Path(case.destination.root).exists()


async def test_complete_source_and_retained_dictionary_includes_unused_keys(tmp_path, monkeypatch):
    async with encoding_fixture._case(tmp_path, monkeypatch) as case:
        result = await _prove(tmp_path, case)
        assert (result.dictionary_key_count, result.used_query_key_count) == (3, 2)
        assert case.reads == [True, True]
        assert result.counts == case.comparison.counts


@pytest.mark.parametrize("damage", ["rank", "variant", "scalar"])
async def test_query_rows_bind_original_key_identity_and_scalar_state(tmp_path, monkeypatch, damage):
    async with encoding_fixture._case(tmp_path, monkeypatch) as case:

        def mutate(frames):
            index = next(index for index, entry in enumerate(frames) if entry[0] == "query_record")
            cells = list(_unpack_row(frames[index][1]))
            position = {"rank": 3, "variant": 4, "scalar": 5}[damage]
            cells[position] = (cells[position] + 100) if damage != "scalar" else b""
            frames[index] = "query_record", _pack_row(tuple(cells))

        _rewrite_output(case, mutate)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _prove(tmp_path, case)
        assert not (tmp_path / "stored-proof").exists()


@pytest.mark.parametrize("damage", ["unused", "late_eof", "not_factory"])
async def test_dictionary_requires_exact_unused_keys_and_full_borrowed_eof(tmp_path, monkeypatch, damage):
    async with encoding_fixture._case(tmp_path, monkeypatch) as case:
        keys = tuple(case.query_keys())
        used_ranks = {
            _unpack_row(packed)[3:5]
            for role, packed in case.output._work.rows(case.output.path)
            if role == "query_record"
        }
        close_events = []

        def changed():
            try:
                for entry in keys:
                    if damage != "unused" or entry[2:] in used_ranks:
                        yield entry
                if damage == "late_eof":
                    raise RuntimeError("synthetic global dictionary EOF loss")
            finally:
                close_events.append(True)

        case.query_keys = iter(keys) if damage == "not_factory" else changed
        with pytest.raises((ValueError, RuntimeError, FamilyBlockUnavailable)):
            await _prove(tmp_path, case)
        assert close_events == ([] if damage == "not_factory" else [True])
        assert not (tmp_path / "stored-proof").exists() and case.output.path.exists()


@pytest.mark.parametrize("damage", ["pack", "ordinal", "scalar"])
async def test_consistent_reencoding_still_must_equal_assigned_records(tmp_path, monkeypatch, damage):
    async with encoding_fixture._case(tmp_path, monkeypatch) as case:
        definition, layout = case.context.common.request.definition, case.context.common.layout

        def changed_families():
            with validation.retained.retained_destination_families(
                case.destination,
                definition,
                work=PlanWork(tmp_path / "changed-inputs", fixture._FACTS, lambda _path: None),
            ) as families:
                for _plan, _original, assigned in families:
                    changes_by_damage = {
                        "pack": dict(pack_id=assigned.root.pack_id + 100),
                        "ordinal": dict(source_ordinal=assigned.root.source_ordinal + 100),
                        "scalar": dict(scalars={}),
                    }
                    yield replace(assigned, root=replace(assigned.root, **changes_by_damage[damage]))

        key_ids_by_key = {(digest, text): (rank, variant) for digest, text, rank, variant in case.query_keys()}
        with build_family_output(
            layout,
            case.context.common.source.snapshot_family_id,
            changed_families(),
            key_ids_by_key,
            work=replace(_output_work(tmp_path), root=tmp_path / "changed-output"),
            shard_id=99,
            partition_sha256=bytes.fromhex(case.context.common.source.source.partition_sha256),
            source_record_count=0,
        ) as changed:
            original = case.output
            case.output = changed
            try:
                with pytest.raises(FamilyBlockUnavailable, match="original assembly"):
                    await _prove(tmp_path, case)
            finally:
                case.output = original


@pytest.mark.parametrize("side", ["assigned", "stored"])
async def test_both_complete_family_streams_are_required(tmp_path, monkeypatch, side):
    async with _case(tmp_path, monkeypatch) as case:
        owner = validation.retained if side == "assigned" else validation.outputs
        name = "_replay_families" if side == "assigned" else "_direct_groups"
        original = getattr(owner, name)

        def missing(*arguments):
            with closing(original(*arguments)) as families:
                assert next(families, None) is not None
            yield from ()

        monkeypatch.setattr(owner, name, missing)
        with pytest.raises(FamilyBlockUnavailable, match="coverage differs"):
            await _prove(tmp_path, case)
        assert not (tmp_path / "stored-proof").exists() and case.output.path.exists()


async def test_original_operation_deadline_is_not_extended_by_decoding(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        request = replace(case.context.common.request, operation_deadline=0.0)
        case.context = replace(case.context, common=replace(case.context.common, request=request))
        with pytest.raises(TimeoutError, match="existing operation deadline"):
            await _prove(tmp_path, case)
        assert not (tmp_path / "stored-proof").exists() and case.output.path.exists()
