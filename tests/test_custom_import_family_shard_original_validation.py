# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent original records versus actual worker files; native IDs are fake."""

import json
from contextlib import asynccontextmanager, closing
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_admission as admission_reader
from process.custom_import import family_shard_original_validation as validation
from process.custom_import import family_shard_plan as planner
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_validate import FamilyBlockValidationError
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.runner_codec import child_key_document, digest_text
from tests.test_custom_import_family_shard_admission import _build_audit, _inputs, _metadata
from tests.test_custom_import_family_shard_output_groups import _frames, _rewrite_output, _selected
from tests.test_custom_import_family_shard_plan import _assignments, _base, _Headers, _opaque_key, _roots
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_source_values import _definition

_LIMITS = SourceFactLimits(64 * 1024**2, 4 * 1024**2, 8 * 1024**2)


@asynccontextmanager
async def _case(tmp_path, monkeypatch, rows_by_stream=None, *, definition=None, base=False):
    async with _inputs(
        tmp_path, monkeypatch, rows_by_stream, definition=definition or _definition(), complete_scope=False
    ) as (case, allocated, workers):
        request, plan = case[1], None
        admission = _build_audit(tmp_path, case, allocated, workers, _LIMITS, complete_scope=False)
        try:
            headers, base_scope = None, None
            if base:
                root = _roots(admission)[0]
                headers = _Headers(
                    [
                        _base(root.record.typed_parent_key, root=root.root_record_id),
                        _base(_opaque_key("unread-base"), root=100003, index=2),
                    ]
                )
                request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
                base_scope = 77, 31
            plan = await planner.build_family_plan(
                request,
                admission,
                headers,
                _assignments(admission),
                work=planner.PlanWork(tmp_path / "plan", _LIMITS, lambda _path: None),
                base_scope=base_scope,
            )
            context = SourceCommonContext(
                request, FamilyBlockLayout(request.definition, case[2].child_collection_slots), allocated
            )
            _records, queries = _metadata(allocated)
            keys = [(digest, text, *identities) for (digest, text), identities in sorted(queries.items())]
            yield SimpleNamespace(context=context, admission=admission, plan=plan, workers=workers, keys=keys)
        finally:
            if plan is not None:
                planner.cleanup_family_plan(plan)
            admission_reader.cleanup_admission_output(admission)


def _prove(tmp_path, native, *, keys=None, check=None, limits=_LIMITS, workers=None):
    return validation.prove_original_outputs(
        native.context,
        native.admission,
        native.plan,
        native.workers if workers is None else workers,
        native.keys if keys is None else keys,
        work=planner.PlanWork(tmp_path / "proof", limits, lambda _path: None, check),
    )


async def test_complete_source_comparison_binds_exact_originals(monkeypatch, tmp_path):
    rows_by_stream = {
        "root_source": [[_root(), _root("1234567893")]],
        "detail_source": [[_child(key="a", amount="1.2300"), _child("1234567893", key="a", amount="2.50")]],
    }
    async with _case(tmp_path, monkeypatch, rows_by_stream) as native:
        result = _prove(tmp_path, native)
        assert result.counts_by_name == dict(
            family_count=2, family_child_count=2, root_scalar_count=6, child_scalar_count=6
        )
        assert result.retained_family_count_unproven == 0
        assert result.source_record_count == 4
        assert result.dictionary_key_count == result.used_query_key_count == 1
        assert len(result.sha256) == 32 and result.inputs[2:4] == (native.admission.sha256, native.plan.sha256)
        assert len(result.workers) == len(native.workers)
        assert not hasattr(result, "verified") and "winner_count" not in result.counts_by_name
        with pytest.raises(FrozenInstanceError):
            result.sha256 = b"x" * 32
        repeated = _prove(tmp_path, native, workers=tuple(reversed(native.workers)))
        assert repeated == result
        assert not (tmp_path / "proof").exists()


async def test_retained_selection_remains_explicitly_unproven(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch, base=True) as native:
        result = _prove(tmp_path, native)
        assert result.retained_family_count_unproven == 1
        assert result.counts_by_name["family_count"] == 1
        assert result.producer[-3:] == (77, 3, False)


async def test_prepared_originals_keep_exact_comparison_without_restaging(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch, base=True) as native:
        expected = _prove(tmp_path, native)
        with validation.originals.prepared_source_family_inputs(
            native.context,
            native.admission,
            native.plan,
            work=planner.PlanWork(tmp_path / "prepared", _LIMITS, lambda _path: None),
        ) as prepared:

            def restaged(*arguments):
                raise AssertionError("prepared original SOURCE must not be staged again")

            monkeypatch.setattr(validation, "_source_inputs", restaged)
            for _iteration in range(2):
                actual = validation.prove_original_outputs(
                    native.context,
                    native.admission,
                    native.plan,
                    native.workers,
                    iter(native.keys),
                    work=planner.PlanWork(tmp_path / "proof", _LIMITS, lambda _path: None),
                    prepared=prepared,
                )
                assert actual == expected
            assert not prepared.is_closed and not (tmp_path / "proof").exists()
        assert not (tmp_path / "prepared").exists()


async def test_prepared_originals_reject_loose_factory_and_changed_scope(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch) as native:
        work = planner.PlanWork(tmp_path / "proof", _LIMITS, lambda _path: None)
        with pytest.raises(ValueError, match="typed prepared SOURCE"):
            validation.prove_original_outputs(
                native.context,
                native.admission,
                native.plan,
                native.workers,
                iter(native.keys),
                work=work,
                prepared=lambda: iter(()),
            )
        with validation.originals.prepared_source_family_inputs(
            native.context,
            native.admission,
            native.plan,
            work=planner.PlanWork(tmp_path / "prepared", _LIMITS, lambda _path: None),
        ) as prepared:
            with pytest.raises(ValueError, match="scope|custody"):
                validation.prove_original_outputs(
                    native.context,
                    native.admission,
                    replace(native.plan, admission_sha256=b"x" * 32),
                    native.workers,
                    iter(native.keys),
                    work=work,
                    prepared=prepared,
                )
        assert not (tmp_path / "proof").exists()


async def test_dictionary_allows_unused_base_keys_but_requires_full_eof(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch) as native:
        extra = None
        for ordinal in range(1000):
            text = child_key_document(native.context.request.definition, "details", {"detail_id": f"unused-{ordinal}"})
            digest = digest_text("child-key", text)
            if not native.keys or digest > native.keys[-1][0]:
                extra = digest, text, len(native.keys) + 1, 1
                break
        assert extra is not None
        result = _prove(tmp_path, native, keys=[*native.keys, extra])
        assert result.dictionary_key_count == len(native.keys) + 1 and result.used_query_key_count == len(native.keys)
        assert not (tmp_path / "proof").exists()


async def test_unused_global_hash_variant_keeps_its_exact_rank(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch) as native:
        digest, text, rank, variant = native.keys[-1]
        result = _prove(tmp_path, native, keys=[*native.keys, (digest, text + " ", rank, variant + 1)])
        assert result.dictionary_key_count == len(native.keys) + 1
        assert result.used_query_key_count == len(native.keys)


@pytest.mark.parametrize("changed", ["receipt", "file_sha"])
async def test_worker_provenance_cannot_change_after_family_eof(monkeypatch, tmp_path, changed):
    async with _case(tmp_path, monkeypatch) as native:
        worker = _selected(native.workers)[0]

        def keys():
            yield from native.keys
            if changed == "receipt":
                worker._output._receipt["result_sha256"] = b"x" * 32
            else:
                worker._output.sha256 = b"x" * 32

        with pytest.raises(FamilyBlockValidationError, match="provenance changed"):
            _prove(tmp_path, native, keys=keys())
        assert not (tmp_path / "proof").exists()


@pytest.mark.parametrize("missing_side", ["original", "output"])
async def test_selected_source_coverage_requires_both_streams(monkeypatch, tmp_path, missing_side):
    target = validation if missing_side == "original" else validation.outputs
    name = "_source_inputs" if missing_side == "original" else "_groups"
    original = getattr(target, name)

    def missing(*arguments, **keywords):
        with closing(original(*arguments, **keywords)) as values:
            observed = sum(1 for _value in values)
            assert observed > 0
        yield from ()

    monkeypatch.setattr(target, name, missing)
    async with _case(tmp_path, monkeypatch) as native:
        with pytest.raises(FamilyBlockValidationError, match="coverage differ"):
            _prove(tmp_path, native)
        assert not (tmp_path / "proof").exists()


@pytest.mark.parametrize("damaged", [False, True])
async def test_no_query_dictionary_still_verifies_every_family(monkeypatch, tmp_path, damaged):
    document = json.loads(_definition().canonical)
    document["query"].pop("child")
    document["selection_profiles"] = [{"id": "default", "selection": [], "context_dimensions": []}]
    definition = CustomImportDefinition.from_mapping(document)
    async with _case(tmp_path, monkeypatch, definition=definition) as native:
        if damaged:
            worker = _selected(native.workers)[0]
            frames = _frames(worker)
            index = next(index for index, entry in enumerate(frames) if entry[0] == "fragment")
            cells = list(_unpack_row(frames[index][1]))
            cells[3] = cells[3][:-1] + bytes([cells[3][-1] ^ 1])
            frames[index] = "fragment", _pack_row(tuple(cells))
            _rewrite_output(worker, native.context.layout, frames)
            with pytest.raises(ValueError):
                _prove(tmp_path, native, keys=[])
        else:
            result = _prove(tmp_path, native, keys=[])
            assert result.counts_by_name["family_child_count"] == 1
            assert result.dictionary_key_count == result.used_query_key_count == 0


@pytest.mark.parametrize(
    "damage", ["missing", "wrong_rank", "wrong_variant", "wrong_hash", "wrong_text", "duplicate", "tail_failure"]
)
async def test_global_query_assignment_refusals(monkeypatch, tmp_path, damage):
    async with _case(tmp_path, monkeypatch) as native:
        query_keys = list(native.keys)
        digest, text, rank, variant = query_keys[0]
        match damage:
            case "missing":
                query_keys = []
            case "wrong_rank":
                query_keys[0] = digest, text, rank + 1, variant
            case "wrong_variant":
                query_keys[0] = digest, text, rank, variant + 1
            case "wrong_hash":
                query_keys[0] = b"x" * 32, text, rank, variant
            case "wrong_text":
                query_keys[0] = digest, text + " ", rank, variant
            case "duplicate":
                query_keys += query_keys
            case "tail_failure":
                original_keys = tuple(query_keys)

                def failed_tail():
                    yield from original_keys
                    raise RuntimeError("synthetic dictionary tail failure")

                query_keys = failed_tail()
        with pytest.raises((ValueError, RuntimeError)):
            _prove(tmp_path, native, keys=query_keys)
        assert not (tmp_path / "proof").exists()


@pytest.mark.parametrize(
    "damage", ["root_id", "root_revision", "family_sha", "fragment_body", "query_scalar", "query_rank", "boolean_rank"]
)
async def test_validly_hashed_output_must_match_independent_source(monkeypatch, tmp_path, damage):
    async with _case(tmp_path, monkeypatch) as native:
        worker = _selected(native.workers)[0]
        frames = _frames(worker)
        role = (
            "fragment"
            if damage == "fragment_body"
            else "query_record"
            if damage.startswith("query") or damage == "boolean_rank"
            else "family"
        )
        index = next(index for index, entry in enumerate(frames) if entry[0] == role)
        cells = list(_unpack_row(frames[index][1]))
        match damage:
            case "root_id":
                cells[1] += 1
            case "root_revision":
                cells[2] += 1
            case "family_sha":
                cells[4] = b"x" * 32
            case "fragment_body":
                cells[3] = cells[3][:-1] + bytes([cells[3][-1] ^ 1])
            case "query_scalar":
                cells[-1] = Decimal("999.000000000000")
            case "query_rank":
                cells[3] += 10
            case "boolean_rank":
                cells[3] = True
        frames[index] = role, _pack_row(tuple(cells))
        _rewrite_output(worker, native.context.layout, frames)
        with pytest.raises((ValueError, FamilyBlockValidationError)):
            _prove(tmp_path, native)
        assert not (tmp_path / "proof").exists()


async def test_empty_source_and_unused_dictionary_still_complete(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch, {"root_source": [[]], "detail_source": [[]]}) as native:
        result = _prove(tmp_path, native)
        assert all(count == 0 for _name, count in result.counts)
        assert result.dictionary_key_count == result.used_query_key_count == 0


async def test_existing_original_plan_eof_is_not_a_placeholder(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch) as native:
        native.plan = replace(
            native.plan,
            counts=tuple((name, value + 1 if name == "source" else value) for name, value in native.plan.counts),
        )
        with pytest.raises(ValueError):
            _prove(tmp_path, native)
        assert not (tmp_path / "proof").exists()


async def test_owner_cancel_and_resource_refusal_preserve_inputs(monkeypatch, tmp_path):
    state = SimpleNamespace(calls=0)

    def check():
        state.calls += 1
        if state.calls > 25:
            raise RuntimeError("synthetic original comparison cancellation")

    async with _case(tmp_path, monkeypatch) as native:
        with pytest.raises(RuntimeError, match="comparison cancellation"):
            _prove(tmp_path, native, check=check)
        assert not (tmp_path / "proof").exists()
        with pytest.raises((SourceFactBudgetExceeded, FamilyBlockResourceLimit)):
            _prove(tmp_path, native, limits=SourceFactLimits(4096, 2048, 2048))
        assert not (tmp_path / "proof").exists()
        assert native.admission.sha256 and native.plan.sha256
        assert all(worker.path.is_file() for worker in native.workers)


async def test_full_codec_receives_responsive_owner_callback(monkeypatch, tmp_path):
    state = SimpleNamespace(is_cancelled=False)
    original = validation.verify_stored_family

    def check():
        if state.is_cancelled:
            raise RuntimeError("synthetic full codec cancellation")

    def intercepted(*arguments, **keywords):
        assert callable(keywords.get("check"))
        state.is_cancelled = True
        return original(*arguments, **keywords)

    monkeypatch.setattr(validation, "verify_stored_family", intercepted)
    async with _case(tmp_path, monkeypatch) as native:
        with pytest.raises(RuntimeError, match="full codec cancellation"):
            _prove(tmp_path, native, check=check)
        assert not (tmp_path / "proof").exists()
