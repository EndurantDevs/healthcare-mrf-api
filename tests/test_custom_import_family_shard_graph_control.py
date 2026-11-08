# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real closed plans; explicitly fake only the native control transaction boundary."""

import asyncio
import hashlib
import threading
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_admission as audit
from process.custom_import import family_shard_graph_control as control
from process.custom_import import family_shard_plan as planner
from process.custom_import import publication
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.runner_codec import candidate_hash
from tests.test_custom_import_family_shard_admission import _build_audit, _inputs
from tests.test_custom_import_family_shard_plan import _assignments, _base, _definition, _Headers, _opaque_key
from tests.test_custom_import_family_shard_source_plan import _child, _root

_STAMP = datetime(2030, 1, 2, tzinfo=timezone.utc)
_LIMITS = SourceFactLimits(64 * 1024**2, 1024**2, 512)


@asynccontextmanager
async def _plan_case(tmp_path, monkeypatch, kind="source", *, source_binding=True):
    rows_by_stream = {"root_source": [[_root()]], "detail_source": [[_child()]]}
    if kind in {"empty", "retained"}:
        rows_by_stream = {"root_source": [[]], "detail_source": [[]]}
    mode = "snapshot" if kind == "absent" else "upsert"
    async with _inputs(
        tmp_path,
        monkeypatch,
        rows_by_stream,
        definition=_definition(mode),
        complete_scope=True,
        source_binding=source_binding,
    ) as (case, allocated, workers):
        admission = _build_audit(tmp_path, case, allocated, workers, _LIMITS)
        final_plan = None
        try:
            request = replace(case[1], complete_scope=True)
            headers, scope = None, None
            if kind in {"retained", "mixed", "absent"}:
                request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
                headers = _Headers([_base(_opaque_key('{ "old" : "key" } '), root=100003)])
                scope = (77, 31)
            context = SourceCommonContext(
                request, FamilyBlockLayout(request.definition, case[2].child_collection_slots), allocated
            )
            final_plan = await planner.build_family_plan(
                request,
                admission,
                headers,
                _assignments(admission),
                work=planner.PlanWork(tmp_path / "final-plan", _LIMITS, lambda _path: None),
                base_scope=scope,
            )
            yield SimpleNamespace(context=context, admission=admission, plan=final_plan, capture=case[4])
        finally:
            if final_plan is not None:
                planner.cleanup_family_plan(final_plan)
            audit.cleanup_admission_output(admission)


def _build_header(native):
    request = native.context.request
    return SimpleNamespace(
        build_id=31,
        producing_fence=request.fence,
        capture_bundle_id=native.capture.identity.capture_bundle_id,
        base_generation_id=request.expected_base_generation_id,
        source_occurrence_count=native.context.source.source.counts[0],
        candidate_error_count=0,
        phase="graph",
        source_frozen_at=_STAMP,
        graph_frozen_at=None,
        output_frozen_at=None,
        verified_at=None,
        generation_id=None,
        plan_stage="base",
        plan_complete_at=None,
        selected_family_count=0,
        completed_family_count=0,
    )


def _bundle(native):
    identity = native.capture.identity
    return SimpleNamespace(
        capture_bundle_id=identity.capture_bundle_id,
        dataset_id=identity.dataset_id,
        definition_revision_id=identity.definition_revision_id,
        schema_revision_id=identity.schema_revision_id,
        capture_state="sealed",
        manifest_sha256=bytes.fromhex(identity.manifest_sha256),
        source_binding_revision_id=identity.source_binding_revision_id,
        source_binding_sha256=None
        if identity.source_binding_sha256 is None
        else bytes.fromhex(identity.source_binding_sha256),
        stream_count=len(native.capture.streams),
    )


class _Cursor:
    def __init__(self, values, observations):
        self.values, self.observations = values, observations

    def __aiter__(self):
        return self._rows()

    async def _rows(self):
        for value in self.values:
            yield value

    async def close(self):
        self.observations.append("cursor_closed")


class _NativeBoundary:
    """Transaction/SQL boundary fake; assertions are not native guard acceptance."""

    def __init__(self, native):
        self.native, self.build = native, _build_header(native)
        self.bundle, self.generation, self.info = _bundle(native), None, {}
        self.observations, self.fail_commit, self.fail_output_commit = [], None, None
        self.bound_family = native.context.source.snapshot_family_id
        counts = native.plan.counts_by_kind
        self.counts = (counts["source"] + counts["retained"], counts["source"], counts["retained"])
        self.captures = [
            SimpleNamespace(
                stream_slot=slot,
                byte_count=receipt.byte_count,
                content_sha256=bytes.fromhex(receipt.content_sha256),
                manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
            )
            for slot, receipt in native.capture.streams
        ]

    @asynccontextmanager
    async def page(self, _factory, request, build_id):
        assert request is self.native.context.request and build_id == self.build.build_id
        original = deepcopy(vars(self.build)), deepcopy(self.generation)
        self.observations.append("begin")
        try:
            yield self, self.build
            self.observations.append("fresh_authority")
            if self.fail_commit is not None:
                raise self.fail_commit
            if self.build.phase == "output" and self.fail_output_commit is not None:
                raise self.fail_output_commit
        except BaseException:
            vars(self.build).clear()
            vars(self.build).update(original[0])
            self.generation = original[1]
            self.observations.append("rollback")
            raise
        self.observations.append("commit")

    async def candidate(self, session, request, family, layout, phases):
        assert session is self and request is self.native.context.request
        assert family == self.native.context.source.snapshot_family_id and phases == {"loading"}
        self.observations.append("candidate")
        return None, SimpleNamespace(tables_by_role=layout.tables(family))

    async def execute(self, statement, _parameters=None):
        sql = str(statement)
        if "custom_import_block_family" in sql:
            self.observations.append("native_census")
            value = self.counts
        elif "custom_import_capture_bundle" in sql:
            self.observations.append("capture_bundle")
            value = self.bundle
        elif "custom_import_generation" in sql:
            self.observations.append("generation_lookup")
            value = self.generation
        else:
            assert "clock_timestamp()" in sql
            value = _STAMP
        return SimpleNamespace(one=lambda: value, scalar_one=lambda: value, scalar_one_or_none=lambda: value)

    async def prepared_execute(self, session, statement, parameters=None):
        assert session is self
        return await self.execute(statement, parameters)

    async def stream_scalars(self, statement):
        sql = str(statement)
        if "custom_import_source_stream" in sql:
            values = [slot for slot, _receipt in self.native.capture.streams]
        else:
            assert "custom_import_capture" in sql
            values = self.captures
        return _Cursor(values, self.observations)

    async def prepare(self, session):
        assert session is self

    def add(self, generation):
        assert self.generation is None
        self.generation = generation
        self.observations.append("generation_insert")

    async def flush(self, session):
        assert session is self
        if self.generation is None:
            assert self.build.phase == "graph" and self.build.plan_stage == "complete"
            assert self.build.plan_complete_at is not None
            assert self.build.selected_family_count == self.build.completed_family_count == self.counts[0]
            self.observations.append("graph_plan_flush")
        elif self.generation.generation_id is None:
            assert self.build.phase == "graph"
            self.generation.generation_id = 21001
            self.observations.append("generation_flush")
        else:
            assert self.build.phase == "output" and self.build.generation_id == self.generation.generation_id
            self.observations.append("output_flush")

    async def bind(self, session, name, arguments):
        request = self.native.context.request
        assert session is self and name == "bind_custom_import_snapshot_generation"
        assert arguments == (
            ("bigint", request.execution_id),
            ("bigint", request.fence),
            ("bytea", control.producer_token_sha256(request)),
            ("bigint", self.generation.generation_id),
        )
        self.observations.append("bind")
        return SimpleNamespace(scalar_one=lambda: self.bound_family)

    def install(self, monkeypatch):
        for name, method in (
            ("_page_session", self.page),
            ("_candidate", self.candidate),
            ("_execute", self.prepared_execute),
            ("_prepare_statement", self.prepare),
            ("_flush_page", self.flush),
            ("_call", self.bind),
        ):
            monkeypatch.setattr(control, name, method)


async def _open(native):
    return await control.open_shard_output(None, native.context, 31, native.plan, native.admission)


@pytest.mark.parametrize("kind", ["source", "empty", "mixed", "retained", "absent"])
async def test_original_candidate_hash_excludes_absent_union_keys(tmp_path, monkeypatch, kind):
    async with _plan_case(tmp_path, monkeypatch, kind) as native:
        actual = control._complete_plan(native.context, native.plan, native.admission, None)
        selected_plans = [entry for entry in planner.read_family_plan(native.plan) if entry.selection_kind != "absent"]
        request = native.context.request
        assert actual.candidate_sha256 == candidate_hash(
            execution_id=request.execution_id,
            fence=request.fence,
            base_generation_id=request.expected_base_generation_id,
            root_key_hashes=[entry.typed_key[1] for entry in selected_plans],
        )
        assert actual.candidate_sha256 != native.plan.sha256
        assert actual.selected_count == len(selected_plans)
        assert (actual.source_count, actual.retained_count) == (
            native.plan.counts_by_kind["source"],
            native.plan.counts_by_kind["retained"],
        )


@pytest.mark.parametrize("kind", ["source", "empty", "mixed", "retained", "absent"])
async def test_open_flushes_graph_before_generation_and_binds_atomically(tmp_path, monkeypatch, kind):
    async with _plan_case(tmp_path, monkeypatch, kind) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        assert await _open(native) == 21001
        relevant_events = [
            event for event in boundary.observations if event.endswith("flush") or event == "generation_insert"
        ]
        assert relevant_events == ["graph_plan_flush", "generation_insert", "generation_flush", "output_flush"]
        assert boundary.observations.index("commit") < boundary.observations.index("generation_insert")
        assert boundary.observations[-3:] == ["bind", "fresh_authority", "commit"]
        assert boundary.observations.count("cursor_closed") == 2
        assert boundary.build.phase == "output" and boundary.build.output_frozen_at is None
        assert boundary.build.verified_at is None
        expected_source = publication._new_digest(publication._SOURCE_BUNDLE_DOMAIN)
        publication._add_digest_record(
            expected_source,
            "bundle",
            {"manifest_sha256": native.capture.identity.manifest_sha256, "stream_count": len(native.capture.streams)},
        )
        for slot, receipt in native.capture.streams:
            publication._add_digest_record(
                expected_source,
                "capture",
                {
                    "byte_count": receipt.byte_count,
                    "content_sha256": receipt.content_sha256,
                    "manifest_sha256": receipt.manifest_sha256,
                    "stream_slot": slot,
                },
            )
        assert boundary.generation.source_bundle_sha256 == expected_source.digest()


async def test_exact_retry_rechecks_plan_census_capture_and_header(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        identity = await _open(native)
        generation, frozen = boundary.generation, boundary.build.graph_frozen_at
        boundary.observations.clear()
        assert await _open(native) == identity
        assert boundary.generation is generation and boundary.build.graph_frozen_at == frozen
        assert boundary.observations.count("native_census") == 2
        assert boundary.observations.count("capture_bundle") == 2
        assert boundary.observations.count("generation_lookup") == 1
        assert "generation_insert" not in boundary.observations and "output_flush" not in boundary.observations


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("plan_stage", "base"),
        ("plan_complete_at", None),
        ("graph_frozen_at", None),
        ("selected_family_count", 2),
        ("completed_family_count", 2),
        ("generation_id", None),
    ],
)
async def test_retry_requires_completed_plan_metadata(tmp_path, monkeypatch, field, value):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        await _open(native)
        setattr(boundary.build, field, value)
        with pytest.raises(FamilyBlockUnavailable, match="retry plan metadata"):
            await _open(native)


async def test_existing_unbound_generation_refuses_new_insert(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        boundary.generation = SimpleNamespace(generation_id=999)
        with pytest.raises(FamilyBlockUnavailable, match="earlier unbound"):
            await _open(native)
        assert "generation_insert" not in boundary.observations


@pytest.mark.parametrize(
    "field",
    [
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "capture_bundle_id",
        "base_generation_id",
        "base_dataset_id",
        "source_bundle_sha256",
        "candidate_sha256",
        "root_count",
        "family_count",
        "producing_fence",
        "producing_token_sha256",
        "generation_id",
    ],
)
async def test_retry_rejects_any_generation_header_difference(tmp_path, monkeypatch, field):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        await _open(native)
        value = getattr(boundary.generation, field)
        setattr(boundary.generation, field, b"x" * 32 if isinstance(value, bytes) else 99999)
        with pytest.raises(FamilyBlockUnavailable, match="generation header"):
            await _open(native)
        assert boundary.observations[-1] == "rollback"


@pytest.mark.parametrize("counts", [(2, 2, 0), (1, 0, 1), (1, 0, 0)])
async def test_native_count_and_retained_flags_are_required(tmp_path, monkeypatch, counts):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        boundary.counts = counts
        with pytest.raises(FamilyBlockUnavailable, match="native source/retained"):
            await _open(native)
        assert boundary.generation is None and boundary.build.phase == "graph"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("phase", "admission"),
        ("candidate_error_count", 1),
        ("source_frozen_at", None),
        ("source_occurrence_count", 999),
        ("capture_bundle_id", 999),
        ("output_frozen_at", _STAMP),
        ("verified_at", _STAMP),
        ("plan_stage", "source"),
        ("plan_complete_at", _STAMP),
        ("selected_family_count", 1),
        ("completed_family_count", 1),
        ("generation_id", 999),
        ("graph_frozen_at", _STAMP),
    ],
)
async def test_graph_requires_original_complete_source_and_unused_plan(tmp_path, monkeypatch, field, value):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        setattr(boundary.build, field, value)
        with pytest.raises(FamilyBlockUnavailable):
            await _open(native)
        assert boundary.generation is None


@pytest.mark.parametrize("damage", ["manifest", "binding", "unsealed", "missing_stream"])
async def test_source_manifest_and_original_capture_digest_guards(tmp_path, monkeypatch, damage):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        if damage == "manifest":
            boundary.bundle.manifest_sha256 = b"x" * 32
        elif damage == "binding":
            boundary.bundle.source_binding_sha256 = b"x" * 32
        elif damage == "unsealed":
            boundary.bundle.capture_state = "pending"
        else:
            boundary.captures.pop()
        with pytest.raises((FamilyBlockUnavailable, publication.PublicationConflict)):
            await _open(native)
        assert boundary.generation is None and boundary.observations[-1] == "rollback"


async def test_original_absent_source_binding_stays_absent(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch, source_binding=False) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        assert boundary.bundle.source_binding_revision_id is boundary.bundle.source_binding_sha256 is None
        assert await _open(native) == 21001


@pytest.mark.parametrize(
    "damage", ["plan_sha", "request", "typed_request", "admission", "source", "typed_source", "base", "counts"]
)
async def test_scope_and_late_eof_failure_precede_native_work(tmp_path, monkeypatch, damage):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        match damage:
            case "plan_sha":
                native.plan = replace(native.plan, sha256=b"x" * 32)
            case "request" | "typed_request":
                changed = 999 if damage == "request" else 1
                native.plan = replace(native.plan, request_scope=(*native.plan.request_scope[:-1], changed))
            case "admission":
                native.plan = replace(native.plan, admission_sha256=b"x" * 32)
            case "source":
                native.admission = replace(native.admission, allocation_sha256=b"x" * 32)
            case "typed_source":
                scope = native.admission.source_scope
                native.admission = replace(native.admission, source_scope=(*scope[:2], True, *scope[3:]))
            case "base":
                native.plan = replace(native.plan, base_scope=(77, 31))
            case "counts":
                native.plan = replace(
                    native.plan, counts=tuple((name, count + 1) for name, count in native.plan.counts)
                )
        with pytest.raises((FamilyBlockUnavailable, ValueError)):
            await _open(native)
        assert boundary.observations == []


@contextmanager
def _missing_completion(descriptor):
    path = Path(descriptor.root) / "family-plan.mpk"
    original = path.read_bytes()
    entries = list(planner._entries(descriptor, None))[:-1]
    chunks = [_pack_row(entry) for entry in entries]
    changed = b"".join(len(chunk).to_bytes(8, "big") + chunk for chunk in chunks)
    path.chmod(0o600)
    path.write_bytes(changed)
    try:
        yield replace(descriptor, file_bytes=len(changed), sha256=hashlib.sha256(changed).digest())
    finally:
        path.write_bytes(original)
        path.chmod(0o400)


async def test_correct_byte_hash_without_complete_plan_is_refused(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        with _missing_completion(native.plan) as truncated:
            native.plan = truncated
            with pytest.raises(ValueError, match="incomplete EOF"):
                await _open(native)
        assert boundary.observations == []


@pytest.mark.parametrize("absent", [False, True])
async def test_root_hash_collision_keeps_original_union_guard(tmp_path, monkeypatch, absent):
    async with _plan_case(tmp_path, monkeypatch) as native:
        selected = next(planner.read_family_plan(native.plan))
        different = replace(
            selected,
            typed_key=("other exact text", selected.typed_key[1]),
            selection_kind="absent" if absent else "source",
        )

        def collision_rows(_descriptor, *, check):
            # Artificial hash-collision branch only; production codec is unchanged.
            yield selected
            yield different

        monkeypatch.setattr(control, "read_family_plan", collision_rows)
        with pytest.raises(FamilyBlockUnavailable, match="digest collision"):
            control._complete_plan(native.context, native.plan, native.admission, None)


@pytest.mark.parametrize("failure_kind", ["binding", "precommit", "output_precommit"])
async def test_control_failure_rolls_back_generation_and_phase(tmp_path, monkeypatch, failure_kind):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        if failure_kind == "binding":
            boundary.bound_family += 1
        elif failure_kind == "output_precommit":
            boundary.fail_output_commit = RuntimeError("changed owner authority before generation commit")
        else:
            boundary.fail_commit = RuntimeError("changed owner authority before commit")
        with pytest.raises((FamilyBlockUnavailable, RuntimeError)):
            await _open(native)
        assert boundary.build.phase == "graph"
        assert boundary.build.plan_complete_at == (None if failure_kind == "precommit" else _STAMP)
        assert boundary.generation is None and boundary.observations[-1] == "rollback"


@pytest.mark.parametrize("lost_phase", ["graph", "output"])
async def test_committed_checkpoint_and_generation_reply_loss_replay_exactly(tmp_path, monkeypatch, lost_phase):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        boundary.reply_loss_phase = lost_phase

        @asynccontextmanager
        async def interrupted(*args):
            async with boundary.page(*args) as opened:
                yield opened
            if boundary.reply_loss_phase == boundary.build.phase:
                boundary.reply_loss_phase = None
                raise ConnectionError("synthetic committed reply loss")

        monkeypatch.setattr(control, "_page_session", interrupted)
        with pytest.raises(ConnectionError, match="committed reply loss"):
            await _open(native)
        assert boundary.build.plan_complete_at == _STAMP
        assert boundary.build.phase == lost_phase
        assert await _open(native) == 21001
        assert boundary.observations.count("graph_plan_flush") == 1
        assert boundary.observations.count("generation_insert") == 1


async def test_interruption_after_graph_checkpoint_preserves_existing_continuation_eligibility(tmp_path, monkeypatch):
    from process.custom_import.build_continuation import _eligible

    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)

        def stop_before_open():
            if boundary.build.plan_stage == "complete":
                raise InterruptedError("synthetic interruption after GRAPH commit")

        with pytest.raises(InterruptedError, match="after GRAPH commit"):
            await control.open_shard_output(
                None, native.context, 31, native.plan, native.admission, check=stop_before_open
            )
        assert boundary.generation is None and boundary.build.phase == "graph"
        assert boundary.build.plan_complete_at == _STAMP
        assert boundary.build.graph_frozen_at is None
        assert boundary.observations.count("commit") == 1
        _eligible(
            SimpleNamespace(state="running"),
            SimpleNamespace(fence=native.context.request.fence),
            boundary.build,
            SimpleNamespace(generation_id=None, frozen_at=None),
            first=True,
        )


async def test_generation_open_refuses_a_reset_committed_plan_checkpoint(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)

        @asynccontextmanager
        async def reset_before_open(*args):
            if boundary.build.plan_stage == "complete":
                boundary.build.plan_stage, boundary.build.plan_complete_at = "base", None
                boundary.build.selected_family_count = boundary.build.completed_family_count = 0
            async with boundary.page(*args) as opened:
                yield opened

        monkeypatch.setattr(control, "_page_session", reset_before_open)
        with pytest.raises(FamilyBlockUnavailable, match="committed plan checkpoint"):
            await _open(native)
        assert boundary.generation is None and "generation_insert" not in boundary.observations


async def test_cancelled_plan_thread_drains_before_return(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        entered, released, finished = asyncio.Event(), threading.Event(), threading.Event()
        loop, original = asyncio.get_running_loop(), control._complete_plan

        def paused(*arguments):
            loop.call_soon_threadsafe(entered.set)
            try:
                assert released.wait(5)
                return original(*arguments)
            finally:
                finished.set()

        monkeypatch.setattr(control, "_complete_plan", paused)
        pending = asyncio.create_task(_open(native))
        await asyncio.wait_for(entered.wait(), 5)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done() and not finished.is_set()
        released.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pending, 5)
        assert finished.is_set() and boundary.observations == []


async def test_owner_check_failure_precedes_all_native_work(tmp_path, monkeypatch):
    async with _plan_case(tmp_path, monkeypatch) as native:
        boundary = _NativeBoundary(native)
        boundary.install(monkeypatch)
        failure = InterruptedError("synthetic owner stopped")

        def check():
            raise failure

        with pytest.raises(InterruptedError) as caught:
            await control.open_shard_output(None, native.context, 31, native.plan, native.admission, check=check)
        assert caught.value is failure and boundary.observations == []
