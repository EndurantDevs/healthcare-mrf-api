# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set caller checks. The DB double is not native SQL rejection evidence."""

from __future__ import annotations

import copy
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as sets
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.runner_codec import new_family_hash
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_prepare import child_row, root_row
from tests.test_custom_import_definition import _rate, _raw_definition


def family_input(request, root_id, *, retained=False, count=1, values=None):
    npi = f"{root_id:010d}"
    values = values or [_rate(npi, f"A{index:05d}") for index in range(count)]
    child_models = {collection.name: () for collection in request.definition.child_collections} | {
        "rates": tuple(values)
    }
    row = root_row(request, root_id, retained=retained)
    item = graph._FamilyInput(
        row[0],
        row[1],
        row[2],
        {"npi": npi, "display_name": "Synthetic Provider"},
        new_family_hash(
            request.definition, RootFamily((npi,), {"npi": npi, "display_name": "Synthetic Provider"}, child_models)
        ),
        len(values),
        6000 + root_id if retained else None,
    )
    child_models = [child_row(request, row, value, root_id * 1000 + index + 1)[2] for index, value in enumerate(values)]
    child_models.sort(
        key=(lambda child: child.child_revision_id)
        if retained
        else (lambda child: (child.child_key_sha256, child.child_revision_id))
    )
    return item, tuple(child_models)


class CallerDatabase:
    """Exercise the real metadata reader and codecs; synthesize native receipts."""

    def __init__(self, monkeypatch, request, families):
        self.request = request
        self.families = {
            family_input.plan.root_record_id: (family_input, children) for family_input, children in families
        }
        self.progress = {}
        self.seen = set()
        self.calls = []
        self.reads = []
        self.read_windows = 0
        self.read_open = False
        self.write_open = False
        self.fail_before_commit = None
        self.fail_after_commit = None
        self.fail_call = None
        self.root_cap, self.child_cap = 100_000, 100_000
        self.valid = True
        self.pending = []
        self.session = SimpleNamespace(info={}, execute=self.execute)
        self.async_session = SimpleNamespace(
            run_sync=self.run_sync,
            get=AsyncMock(side_effect=AssertionError("per-family get")),
            add=Mock(side_effect=AssertionError("ORM insert")),
            flush=AsyncMock(side_effect=AssertionError("ORM flush")),
        )
        self.heartbeat = AsyncMock()
        children_statement = sets._children_statement

        def _bound_children(*args):
            self.read_states = {state.current.root_record_id: state for state in args[2]}
            return children_statement(*args)

        monkeypatch.setattr(sets, "_children_statement", _bound_children)
        monkeypatch.setattr(graph, "_read_transaction", self.read_transaction)
        monkeypatch.setattr(
            graph, "_build_storage_models", Mock(return_value=(snapshot_models(17), snapshot_models(18)))
        )
        monkeypatch.setattr(graph, "_prepare_read", Mock())
        monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
        monkeypatch.setattr(graph, "_session", self.read_session)
        monkeypatch.setattr(graph, "_page_session", self.write_session)
        monkeypatch.setattr(graph, "_source_call", self.call)
        monkeypatch.setattr(graph, "_heartbeat", self.heartbeat)

    @contextmanager
    def read_transaction(self, session, _request, _build_id):
        assert not self.write_open
        self.read_windows += 1
        self.read_open = True
        session.info["custom_import_build_read_deadline"] = 20
        try:
            yield None, 20
        finally:
            self.read_open = False

    @asynccontextmanager
    async def read_session(self, *_):
        yield self.async_session

    @asynccontextmanager
    async def write_session(self, *_):
        assert not self.read_open
        self.write_open = True
        before = copy.deepcopy(self.progress), self.seen.copy()
        try:
            yield self.async_session, object()
            if self.fail_before_commit:
                raise self.fail_before_commit
        except BaseException:
            self.progress, self.seen = before
            raise
        finally:
            self.write_open = False
        if self.fail_after_commit:
            raise self.fail_after_commit

    async def run_sync(self, callback):
        return callback(self.session)

    def execute(self, statement):
        assert self.read_open
        self.reads.append(statement)
        params = statement.compile(dialect=postgresql.dialect()).params
        if statement._limit_clause is not None:
            kind = params["selection_kind_1"]
            rows = [
                (root_id, child, self.valid)
                for root_id, (item, children) in sorted(self.families.items())
                if item.plan.selection_kind == kind and root_id in self.read_states
                for child in children[self.read_states[root_id].current.attached_child_count :]
            ]
            self.pending = rows[: statement._limit_clause.value]
            result_rows = [self.metadata(kind, row) for row in self.pending]
        else:
            ids = set(params["child_ids"])
            result_rows = [row for row in self.pending if row[1].child_revision_id in ids]
        return SimpleNamespace(all=lambda: result_rows)

    @staticmethod
    def metadata(kind, row):
        root_id, child, _valid = row
        order = (
            (child.collection_slot, child.child_revision_id)
            if kind == "retained"
            else (
                "rates",
                child.child_key_sha256,
                child.child_revision_id,
            )
        )
        return root_id, *order, graph._model_bytes((child,))

    async def call(self, _session, name, arguments):
        assert self.write_open and not self.read_open
        self.calls.append((name, arguments))
        failure = self.fail_call(name, arguments) if callable(self.fail_call) else self.fail_call
        if failure:
            raise failure
        roots = arguments[4][1]
        if "roots_page" in name:
            if len(roots) > self.root_cap:
                raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")
            receipt_rows = []
            for root_id in roots:
                prepared_family, _children = self.families[root_id]
                self.progress.setdefault(
                    root_id,
                    SimpleNamespace(
                        root_record_id=root_id,
                        family_revision_id=20_000 + root_id,
                        root_revision_id=30_000 + root_id
                        if prepared_family.plan.selection_kind == "retained"
                        else prepared_family.root.root_revision_id,
                        entity_binding_id=6000 + root_id,
                        attached_child_count=0,
                        last_child_collection_slot=None,
                        last_child_key_sha256=None,
                        last_input_child_revision_id=None,
                        complete=prepared_family.child_count == 0,
                    ),
                )
                receipt_rows.append(self.progress[root_id])
            return SimpleNamespace(all=lambda: receipt_rows)
        is_retained = "retained" in name
        child_roots = arguments[9 if is_retained else 10][1]
        child_ids = arguments[10 if is_retained else 11][1]
        if len(child_ids) > self.child_cap:
            raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")
        root_by_child_id = dict(zip(child_ids, child_roots, strict=True))
        receipt_rows = []
        for root_id, expected in zip(roots, arguments[6][1], strict=True):
            prepared_family, all_children = self.families[root_id]
            child_models = [child for child in all_children if root_by_child_id.get(child.child_revision_id) == root_id]
            before = self.progress[root_id]
            assert expected == before.attached_child_count
            after = copy.copy(before)
            after.attached_child_count += len(child_models)
            after.complete = after.attached_child_count == prepared_family.child_count
            if child_models:
                after.last_child_collection_slot = child_models[-1].collection_slot
                after.last_child_key_sha256 = None if is_retained else child_models[-1].child_key_sha256
                after.last_input_child_revision_id = child_models[-1].child_revision_id
            self.progress[root_id] = after
            self.seen.update(child.child_revision_id for child in child_models)
            receipt_rows.append(after)
        return SimpleNamespace(all=lambda: receipt_rows)


def alter_receipts(monkeypatch, db, *, roots, transform):
    async def call(*args):
        result = await db.call(*args)
        if ("roots_page" in args[1]) == roots:
            rows = transform(copy.deepcopy(result.all()))
            return SimpleNamespace(all=lambda: rows)
        return result

    monkeypatch.setattr(graph, "_source_call", call)


@pytest.mark.parametrize("roots", [False, True])
@pytest.mark.parametrize("receipt_order", [(0,), (1, 0), (0, 0)])
async def test_native_receipts_require_the_complete_ordered_root_prefix(monkeypatch, roots, receipt_order):
    request = _request(page_row_limit=32)
    families = [family_input(request, root_id) for root_id in (1, 2)]
    db = CallerDatabase(monkeypatch, request, families)
    alter_receipts(monkeypatch, db, roots=roots, transform=lambda rows: [rows[index] for index in receipt_order])
    with pytest.raises(CandidateRunnerError, match="different family prefix"):
        await sets.consume_family_page(
            None, request, _registry(request.definition), tuple(item for item, _ in families)
        )
    assert len(db.calls) == (1 if roots else 2)
    assert not db.seen and not db.heartbeat.called and not db.write_open and not db.read_open
    assert not db.progress if roots else all(row.attached_child_count == 0 for row in db.progress.values())


@pytest.mark.parametrize(
    ("retained", "field", "value"),
    [
        (False, "family_revision_id", 0),
        (False, "root_revision_id", True),
        (False, "entity_binding_id", None),
        (False, "attached_child_count", True),
        (False, "attached_child_count", -1),
        (False, "attached_child_count", 2),
        (False, "complete", 0),
        (False, "complete", True),
        (False, "root_revision_id", 999),
        (False, "last_child_collection_slot", 1),
        (False, "last_child_key_sha256", b"x" * 32),
        (True, "root_revision_id", 1001),
        (True, "entity_binding_id", 999),
    ],
)
async def test_invalid_root_progress_rolls_back_before_child_reads(monkeypatch, retained, field, value):
    request = _request()
    family = family_input(request, 1, retained=retained)
    db = CallerDatabase(monkeypatch, request, [family])

    def corrupt(rows):
        setattr(rows[0], field, value)
        return rows

    alter_receipts(monkeypatch, db, roots=True, transform=corrupt)
    with pytest.raises(CandidateRunnerError, match="inconsistent durable progress"):
        await sets.consume_family_page(None, request, _registry(request.definition), (family[0],))
    assert len(db.calls) == 1 and not db.progress and not db.reads and not db.heartbeat.called
    assert not db.write_open


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attached_child_count", True),
        ("attached_child_count", 0),
        ("last_child_collection_slot", 99),
        ("last_child_key_sha256", b"x" * 32),
        ("last_input_child_revision_id", 999),
        ("complete", 1),
        ("complete", False),
    ],
)
async def test_invalid_child_progress_rolls_back_without_advancing_the_durable_tip(monkeypatch, field, value):
    request = _request()
    family = family_input(request, 1)
    db = CallerDatabase(monkeypatch, request, [family])

    def corrupt(rows):
        setattr(rows[0], field, value)
        return rows

    alter_receipts(monkeypatch, db, roots=False, transform=corrupt)
    with pytest.raises(CandidateRunnerError, match="inconsistent durable progress"):
        await sets.consume_family_page(None, request, _registry(request.definition), (family[0],))
    assert len(db.calls) == 2 and db.progress[1].attached_child_count == 0
    assert db.progress[1].last_input_child_revision_id is None and not db.seen and not db.heartbeat.called
    assert not db.write_open and not db.read_open


@pytest.mark.parametrize("corruption", ["reversed", "duplicate", "build", "selection", "row_budget"])
async def test_invalid_prepared_page_is_rejected_before_opening_a_transaction(monkeypatch, corruption):
    request = _request()
    families = [family_input(request, root_id) for root_id in (1, 2)]
    db = CallerDatabase(monkeypatch, request, families)
    inputs = tuple(item for item, _ in families)
    if corruption == "reversed":
        inputs = inputs[::-1]
    elif corruption == "duplicate":
        inputs = (inputs[0], inputs[0])
    elif corruption == "build":
        inputs[1].plan.build_id = 8
    elif corruption == "selection":
        inputs[1].plan.selection_kind = "unknown"
    else:
        monkeypatch.setattr(sets, "MAX_BATCH_ROWS", 1)
    with pytest.raises(CandidateRunnerError, match="family page identity or read budget differs"):
        await sets.consume_family_page(None, request, _registry(request.definition), inputs)
    assert not db.calls and not db.reads and not db.progress


async def test_empty_prepared_page_has_no_database_or_heartbeat_work(monkeypatch):
    request = _request()
    db = CallerDatabase(monkeypatch, request, [])
    await sets.consume_family_page(None, request, _registry(request.definition), ())
    assert not db.calls and not db.reads and not db.heartbeat.called


async def test_one_root_must_fit_the_physical_write_envelope(monkeypatch):
    request = _request()
    registry = _registry(request.definition)
    family = family_input(request, 1)
    db = CallerDatabase(monkeypatch, request, [family])
    monkeypatch.setattr(sets, "MAX_BATCH_ROWS", sets._root_fanout(request, registry) - 1)
    with pytest.raises(CandidateRunnerError, match="fanout exceeds the admitted byte page"):
        await sets.consume_family_page(None, request, registry, (family[0],))
    assert not db.calls and not db.reads and not db.progress


async def test_duplicate_child_ids_across_roots_are_rejected_before_payload(monkeypatch):
    request = _request(page_row_limit=32)
    families = [family_input(request, root_id) for root_id in (1, 2)]
    families[1][1][0].child_revision_id = families[0][1][0].child_revision_id
    db = CallerDatabase(monkeypatch, request, families)
    with pytest.raises(CandidateRunnerError, match="child page identity or provenance differs"):
        await sets.consume_family_page(
            None, request, _registry(request.definition), tuple(item for item, _ in families)
        )
    assert len(db.calls) == len(db.reads) == 1
    assert not db.read_open and not db.seen and not db.heartbeat.called


@pytest.mark.parametrize("payload_order", [(0,), (1, 0), (0, 0)])
async def test_child_payload_cannot_skip_reorder_or_duplicate_metadata_keys(monkeypatch, payload_order):
    request = _request(page_row_limit=32)
    families = [family_input(request, root_id) for root_id in (1, 2)]
    db = CallerDatabase(monkeypatch, request, families)

    def execute(statement):
        result = db.execute(statement)
        if statement._limit_clause is None:
            rows = result.all()
            return SimpleNamespace(all=lambda: [rows[index] for index in payload_order])
        return result

    db.session.execute = execute
    with pytest.raises(CandidateRunnerError, match="frozen build page changed during its read"):
        await sets.consume_family_page(
            None, request, _registry(request.definition), tuple(item for item, _ in families)
        )
    assert len(db.calls) == 1 and len(db.reads) == 2
    assert not db.read_open and not db.seen and not db.heartbeat.called


async def test_missing_children_cannot_complete_a_nonempty_family(monkeypatch):
    request = _request()
    family, _children = family_input(request, 1)
    db = CallerDatabase(monkeypatch, request, [(family, ())])
    with pytest.raises(CandidateRunnerError, match="fanout exceeds the admitted row page"):
        await sets.consume_family_page(None, request, _registry(request.definition), (family,))
    assert len(db.calls) == 1 and db.progress[1].attached_child_count == 0 and not db.progress[1].complete
    assert not db.read_open and not db.seen and not db.heartbeat.called


async def test_child_payload_digest_is_rechecked_before_the_write(monkeypatch):
    request = _request()
    family = family_input(request, 1)
    family[1][0].payload_sha256 = b"x" * 32
    db = CallerDatabase(monkeypatch, request, [family])
    with pytest.raises(CandidateRunnerError, match="build child payload digest differs"):
        await sets.consume_family_page(None, request, _registry(request.definition), (family[0],))
    assert len(db.calls) == 1 and db.progress[1].attached_child_count == 0
    assert not db.read_open and not db.seen and not db.heartbeat.called


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("count", [1, 8, 32])
async def test_dispatch_and_read_queries_scale_with_pages(monkeypatch, count, retained):
    request = _request(page_row_limit=128, page_byte_limit=1_048_576)
    registry = _registry(request.definition)
    families = [family_input(request, index + 1, retained=retained) for index in range(count)]
    db = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, registry, tuple(item for item, _ in families))
    assert len(db.calls) == 2
    root_args = db.calls[0][1]
    assert sum(root_args[-1][1]) == count and max(root_args[-1][1]) <= sets._root_limit(request, registry)
    assert len(db.reads) == 2 and db.read_windows == 1
    assert db.heartbeat.await_count == 1
    assert len(db.seen) == count and all(row.complete for row in db.progress.values())
    assert not db.async_session.get.called and not db.async_session.add.called and not db.async_session.flush.called


async def test_mixed_empty_and_nonempty_families_keep_real_ids(monkeypatch):
    request = _request(page_row_limit=128)
    families = [family_input(request, index + 1, retained=bool(index % 2), count=index // 2) for index in range(6)]
    db = CallerDatabase(monkeypatch, request, families)
    await graph._consume_family_page(None, request, _registry(request.definition), tuple(item for item, _ in families))
    assert {name for name, _ in db.calls} == {
        "start_custom_import_build_source_roots_page",
        "copy_custom_import_build_retained_roots_page",
        "append_custom_import_build_source_families_page",
        "copy_custom_import_build_retained_families_page",
    }
    assert len(db.calls) == 4 and len(db.reads) == 4
    for name, arguments in db.calls:
        if "families_page" in name:
            assert arguments[5][1] == tuple(20_000 + root for root in arguments[4][1])
            assert not set(arguments[4][1]) & {1, 2}


@pytest.mark.parametrize("retained", [False, True])
async def test_empty_families_need_only_root_set_calls(monkeypatch, retained):
    request = _request(page_row_limit=128)
    families = [family_input(request, index + 1, retained=retained, count=0) for index in range(8)]
    db = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, _registry(request.definition), tuple(item for item, _ in families))
    assert len(db.calls) == 1 and not db.reads and not db.heartbeat.called


@pytest.mark.parametrize("retained", [False, True])
async def test_large_family_keeps_bounded_reads_and_resumes_from_durable_receipts(monkeypatch, retained):
    request = _request(page_row_limit=32)
    families = [
        family_input(request, 1, retained=retained, count=43),
        family_input(request, 2, retained=retained, count=1),
    ]
    db = CallerDatabase(monkeypatch, request, families)
    inputs = tuple(item for item, _ in families)
    await sets.consume_family_page(None, request, _registry(request.definition), inputs)
    assert len(db.reads) == 2 and db.read_windows == 1
    assert max(
        statement._limit_clause.value for statement in db.reads if statement._limit_clause is not None
    ) <= sets.MAX_BATCH_ROWS // sets._child_fanout(request, _registry(request.definition))
    child_arguments = next(arguments for name, arguments in db.calls if "families_page" in name)
    assert len(child_arguments[-1][1]) > 1
    assert max(child_arguments[-1][1]) <= sets._child_limit(request, _registry(request.definition))
    first_writes, first_reads = len(db.calls), len(db.reads)
    await sets.consume_family_page(None, request, _registry(request.definition), inputs)
    assert len(db.calls) == first_writes + 1
    assert len(db.reads) == first_reads and len(db.seen) == 44


async def test_native_size_rejections_split_only_after_whole_page_rollback(monkeypatch):
    request = _request(page_row_limit=128)
    families = [family_input(request, index + 1) for index in range(8)]
    db = CallerDatabase(monkeypatch, request, families)
    db.root_cap = db.child_cap = 2
    await sets.consume_family_page(None, request, _registry(request.definition), tuple(item for item, _ in families))
    assert len(db.seen) == 8 and all(row.attached_child_count == 1 for row in db.progress.values())
    assert sum("roots_page" in name for name, _ in db.calls) == 7  # 8 -> 4 + 4 -> 2 + 2 + 2 + 2.
    assert any(len(args[11][1]) > 2 for name, args in db.calls if "families_page" in name)


@pytest.mark.parametrize(
    "error",
    [CancellationRequested("canceling"), LeaseAuthorityLost("expired"), CandidateRunnerError("bad canonical value")],
)
async def test_authority_and_corruption_failures_are_not_size_retries(monkeypatch, error):
    request = _request(page_row_limit=128)
    families = [family_input(request, 1), family_input(request, 2)]
    db = CallerDatabase(monkeypatch, request, families)
    db.fail_call = error
    with pytest.raises(type(error), match=str(error)):
        await sets.consume_family_page(
            None, request, _registry(request.definition), tuple(item for item, _ in families)
        )
    assert len(db.calls) == 1 and not db.progress and not db.heartbeat.called


async def test_precommit_cancellation_rolls_back_receipts_and_closes_context(monkeypatch):
    request = _request(page_row_limit=128)
    families = [family_input(request, 1)]
    db = CallerDatabase(monkeypatch, request, families)
    db.fail_before_commit = CancellationRequested("canceling")
    with pytest.raises(CancellationRequested):
        await sets.consume_family_page(
            None, request, _registry(request.definition), tuple(item for item, _ in families)
        )
    assert not db.progress and not db.read_open and not db.write_open and not db.heartbeat.called


def test_native_root_arrays_preserve_single_root_codecs_and_nulls():
    raw = _raw_definition()
    raw["schema"]["root"]["fields"][1]["nullable"] = True
    request = _request(definition=CustomImportDefinition.from_mapping(raw), page_row_limit=128)
    registry = _registry(request.definition)
    families = [family_input(request, 1)[0], family_input(request, 2)[0]]
    name, args = sets._root_arguments(request, registry, families)
    old_argument_rows = [graph._source_root_arguments(request, registry, item) for item in families]
    assert name == "start_custom_import_build_source_roots_page"
    for index in range(4, 12):
        assert args[index][1] == tuple(row[index][1] for row in old_argument_rows)
    for index in range(12, 21):
        assert args[index + 1] == (
            old_argument_rows[0][index][0],
            tuple(value for row in old_argument_rows for value in row[index][1]),
        )
    assert any(value is None for value in args[17][1])


def test_source_child_arrays_keep_decimal_precision_and_legacy_projection_bytes(monkeypatch):
    request = _request(page_row_limit=128)
    registry = _registry(request.definition)
    families = [
        family_input(request, index + 1, values=[_rate(f"{index + 1:010d}", amount=Decimal("1.000000000001"))])
        for index in range(2)
    ]
    db = CallerDatabase(monkeypatch, request, families)
    receipts = [
        SimpleNamespace(
            root_record_id=prepared_family.plan.root_record_id,
            family_revision_id=20_000 + prepared_family.plan.root_record_id,
            root_revision_id=prepared_family.root.root_revision_id,
            entity_binding_id=6000 + prepared_family.plan.root_record_id,
            attached_child_count=0,
            last_child_collection_slot=None,
            last_child_key_sha256=None,
            last_input_child_revision_id=None,
            complete=False,
        )
        for prepared_family, _ in families
    ]
    states = sets._root_receipts(request, registry, [prepared_family for prepared_family, _ in families], receipts)
    groups = tuple(sets._ChildGroup(state, children) for state, (_item, children) in zip(states, families, strict=True))
    name, args = sets._child_arguments(request, registry, groups)
    projections = [
        projection_model for group in groups for projection_model in sets._source_projections(request, registry, group)
    ]
    scalars = [projection_model for projection_model in projections if isinstance(projection_model, sets.Scalar)]
    for index, (kind, field) in enumerate(sets._SCALAR_COLUMNS, 12):
        assert args[index] == (kind, tuple(getattr(projection_model, field) for projection_model in scalars))
    assert [array_value for array_value in args[18][1] if array_value is not None] == [Decimal("1.000000000001")] * 2
    assert name == "append_custom_import_build_source_families_page" and not db.calls


def test_child_queries_bind_full_identity_and_keep_duplicate_policy(monkeypatch):
    raw = _raw_definition()
    raw["streams"][1]["duplicate_policy"] = "collapse_identical"
    request = _request(definition=CustomImportDefinition.from_mapping(raw), page_row_limit=128)
    item, _children = family_input(request, 1)
    state = sets._Started(
        item,
        graph.CustomImportBuildFamily(build_id=7, root_record_id=1, selection_kind="source", family_revision_id=9),
        graph.CustomImportFamilyRevision(family_revision_id=9),
        False,
    )
    statement, keys, _ = sets._children_statement(
        request, _registry(request.definition), (state,), snapshot_models(17), snapshot_models(18)
    )
    sql = str(statement.order_by(*keys).compile(dialect=postgresql.dialect()))
    assert len(keys) == 4 and 'COLLATE "C"' in sql and "NOT (EXISTS" in sql
    for text in (
        "producing_token_sha256",
        "capture_bundle_id",
        "schema_revision_id",
        "pack_ordinal <",
        "source_ordinal >",
    ):
        assert text in sql
    later = sql[sql.index("NOT (EXISTS") :]
    assert "resolved_rejection_id" not in later


def test_child_budget_reserves_whole_prepared_page_not_only_active_family(monkeypatch):
    request = _request(page_row_limit=128)
    families = [family_input(request, 1), family_input(request, 2, retained=True)]
    inputs = tuple(item for item, _ in families)
    states = tuple(
        sets._Started(
            item,
            graph.CustomImportBuildFamily(
                root_record_id=item.plan.root_record_id, selection_kind=item.plan.selection_kind
            ),
            graph.CustomImportFamilyRevision(family_sha256=item.family_sha256),
            False,
        )
        for item in inputs
    )
    expected = sum(graph._model_bytes((item.plan, item.root, item.record)) for item in inputs)
    expected += sum(graph._model_bytes((state.current, state.family)) for state in states)
    _tips, reserved_bytes_by_root_id, _spans = sets._child_reservations(request, inputs, states)
    assert sum(reserved_bytes_by_root_id.values()) == expected


def test_retained_collection_transition_preserves_logical_pack_boundaries():
    request = _request(page_row_limit=128)
    item, children = family_input(request, 1, retained=True, count=2)
    children[1].collection_slot = 2
    current = graph.CustomImportBuildFamily(
        build_id=7, root_record_id=1, selection_kind="retained", family_revision_id=8
    )
    state = sets._Started(item, current, graph.CustomImportFamilyRevision(family_revision_id=8), False)
    rows = [(1, child, True) for child in children]
    groups = sets._logical_child_groups(rows, (state,))
    assert groups[0].children == (children[0],)
    next_state = sets._child_state(state, 1, (1, None, children[0].child_revision_id), False)
    next_groups = sets._logical_child_groups(rows[1:], (next_state,))
    assert next_groups[0].children == (children[1],)


async def test_broken_payload_join_closes_reader_before_any_write(monkeypatch):
    request = _request(page_row_limit=128)
    families = [family_input(request, 1)]
    db = CallerDatabase(monkeypatch, request, families)
    db.valid = False
    with pytest.raises(CandidateRunnerError, match="provenance"):
        await sets.consume_family_page(None, request, _registry(request.definition), (families[0][0],))
    assert not db.read_open and not db.write_open and not db.seen and len(db.calls) == 1


@pytest.mark.parametrize("retained", [False, True])
async def test_restart_after_committed_child_page_uses_the_durable_tip(monkeypatch, retained):
    request = _request(page_row_limit=32)
    monkeypatch.setattr(sets, "MAX_BATCH_ROWS", 2 * request.page_row_limit)
    registry = _registry(request.definition)
    families = [family_input(request, 1, retained=retained, count=13)]
    db = CallerDatabase(monkeypatch, request, families)
    db.heartbeat.side_effect = LeaseAuthorityLost("stopped after commit")
    inputs = (families[0][0],)
    with pytest.raises(LeaseAuthorityLost, match="after commit"):
        await sets.consume_family_page(None, request, registry, inputs)
    committed = len(db.seen)
    assert 0 < committed < 13
    assert db.progress[1].attached_child_count == committed
    db.heartbeat.side_effect = None
    prior_calls = len(db.calls)
    await sets.consume_family_page(None, request, registry, inputs)
    resumed_child = next(args for name, args in db.calls[prior_calls:] if "families_page" in name)
    assert resumed_child[6][1] == (committed,)
    assert resumed_child[7][1] == (1,)
    assert db.progress[1].attached_child_count == 13 and len(db.seen) == 13


async def test_stale_native_cursor_is_not_a_size_retry_and_can_resume(monkeypatch):
    request = _request(page_row_limit=128)
    families = [family_input(request, 1)]
    db = CallerDatabase(monkeypatch, request, families)
    original = RuntimeError("stale")
    original.sqlstate = "40001"
    original.diag = SimpleNamespace(message_primary="graph_children_progress_conflict")
    error = DBAPIError("SELECT", {}, original, False)
    db.fail_call = lambda name, _args: error if "families_page" in name else None
    with pytest.raises(DBAPIError):
        await sets.consume_family_page(None, request, _registry(request.definition), (families[0][0],))
    assert len(db.calls) == 2 and not db.seen and not db.heartbeat.called
    db.fail_call = None
    await sets.consume_family_page(None, request, _registry(request.definition), (families[0][0],))
    assert db.progress[1].complete and len(db.seen) == 1


@pytest.mark.parametrize("retained", [False, True])
async def test_logical_child_pages_share_one_native_call_and_initial_tip(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [
        family_input(request, 1, retained=retained, count=43),
        family_input(request, 2, retained=retained, count=2),
    ]
    db = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, registry, tuple(item for item, _ in families))
    calls = [(name, arguments) for name, arguments in db.calls if "families_page" in name]
    assert len(calls) == db.heartbeat.await_count == 1
    assert len(db.reads) == 2 and db.read_windows == 1
    payload_query = db.reads[1].compile(dialect=postgresql.dialect())
    assert "ANY (%(child_ids)s::BIGINT[])" in str(payload_query)
    assert len(payload_query.params["child_ids"]) == 45
    assert len(payload_query.params) < 50
    name, arguments = calls[0]
    assert arguments[4][1] == (1, 2) and arguments[6][1] == (0, 0)
    assert arguments[7][1] == (None, None)
    assert sum(arguments[-1][1]) == 45 and len(arguments[-1][1]) > 1
    assert max(arguments[-1][1]) <= sets._child_limit(request, registry)
    assert len(arguments[10 if retained else 11][1]) == 45
    assert len(db.seen) == 45 and all(row.complete for row in db.progress.values())


@pytest.mark.parametrize("retained", [False, True])
async def test_physical_work_bound_keeps_logical_policy_and_one_heartbeat_per_batch(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    monkeypatch.setattr(sets, "MAX_BATCH_ROWS", 64)
    families = [family_input(request, 1, retained=retained, count=43)]
    db = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, registry, (families[0][0],))
    calls = [arguments for name, arguments in db.calls if "families_page" in name]
    assert len(calls) > 1 and db.heartbeat.await_count == len(calls)
    assert db.read_windows == len(calls) and len(db.reads) == 2 * len(calls)
    for arguments in calls:
        count = sum(arguments[-1][1])
        assert count * sets._child_fanout(request, registry) + 1 <= 64
        assert max(arguments[-1][1]) <= sets._child_limit(request, registry)
    assert len(db.seen) == 43 and db.progress[1].complete


async def test_physical_child_failure_rolls_back_all_logical_page_receipts(monkeypatch):
    request = _request(page_row_limit=32)
    families = [family_input(request, 1, count=43)]
    db = CallerDatabase(monkeypatch, request, families)
    receipts = sets._child_receipts

    def _cancel_before_commit(groups, rows):
        receipts(groups, rows)
        raise CancellationRequested("canceling coalesced child batch")

    monkeypatch.setattr(sets, "_child_receipts", _cancel_before_commit)
    with pytest.raises(CancellationRequested, match="coalesced"):
        await sets.consume_family_page(None, request, _registry(request.definition), (families[0][0],))
    assert db.progress[1].attached_child_count == 0 and not db.seen and not db.heartbeat.called
    assert not db.read_open and not db.write_open
    arguments = db.calls[-1][1]
    assert sum(arguments[-1][1]) == 43 and len(arguments[-1][1]) > 1
    monkeypatch.setattr(sets, "_child_receipts", receipts)
    await sets.consume_family_page(None, request, _registry(request.definition), (families[0][0],))
    assert db.progress[1].complete and len(db.seen) == 43


@pytest.mark.parametrize("retained", [False, True])
async def test_physical_child_byte_boundary_splits_without_widening_logical_pages(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, 1, retained=retained, count=43)]
    db = CallerDatabase(monkeypatch, request, families)
    monkeypatch.setattr(sets, "MAX_BATCH_BYTES", 8192)
    await sets.consume_family_page(None, request, registry, (families[0][0],))
    calls = [arguments for name, arguments in db.calls if "families_page" in name]
    assert len(calls) > 1 and db.heartbeat.await_count == len(calls)
    assert all(sets._child_argument_bytes(arguments) < 8192 for arguments in calls)
    assert all(max(arguments[-1][1]) <= sets._child_limit(request, registry) for arguments in calls)
    assert len(db.seen) == 43 and db.progress[1].complete


@pytest.mark.parametrize("shortfall", [0, 1])
async def test_metadata_byte_boundary_includes_every_held_root_model(monkeypatch, shortfall):
    request = _request(page_row_limit=128)
    registry = _registry(request.definition)
    item, children = family_input(request, 1)
    receipt = SimpleNamespace(
        root_record_id=1,
        family_revision_id=20_001,
        root_revision_id=item.root.root_revision_id,
        entity_binding_id=6001,
        attached_child_count=0,
        last_child_collection_slot=None,
        last_child_key_sha256=None,
        last_input_child_revision_id=None,
        complete=False,
    )
    states = sets._root_receipts(request, registry, (item,), (receipt,))
    _tips, reserved_bytes_by_root_id, _spans = sets._child_reservations(request, (item,), states)
    budget = sum(reserved_bytes_by_root_id.values()) + graph._model_bytes(children) - shortfall
    request = replace(request, page_byte_limit=budget)
    db = CallerDatabase(monkeypatch, request, ((item, children),))
    if shortfall:
        with pytest.raises(CandidateRunnerError, match="one build record exceeds"):
            await sets.consume_family_page(None, request, registry, (item,))
        assert len(db.reads) == 2 and len(db.calls) == 1 and not db.seen and not db.read_open
    else:
        await sets.consume_family_page(None, request, registry, (item,))
        assert len(db.reads) == 2 and len(db.calls) == 2 and db.progress[1].complete


async def test_read_deadline_expires_before_projection_or_write(monkeypatch):
    request = _request(page_row_limit=128)
    families = [family_input(request, 1)]
    db = CallerDatabase(monkeypatch, request, families)
    execute = db.execute

    def expire_after_payload(statement):
        result = execute(statement)
        if statement._limit_clause is None:
            monkeypatch.setattr(graph.time, "monotonic", lambda: 20)
        return result

    db.session.execute = expire_after_payload
    with pytest.raises(LeaseAuthorityLost, match="deadline"):
        await sets.consume_family_page(None, request, _registry(request.definition), (families[0][0],))
    assert len(db.calls) == 1 and not db.seen and not db.read_open and not db.write_open


@pytest.mark.parametrize("retained", [False, True])
async def test_many_logical_root_pages_share_one_protected_native_call(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, retained=retained, count=0) for root_id in range(1, 401)]
    db = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    assert len(db.calls) == 1 and not db.reads and not db.heartbeat.called
    arguments = db.calls[0][1]
    assert len(arguments[4][1]) == sum(arguments[-1][1]) == 400
    assert len(arguments[-1][1]) == 100 and max(arguments[-1][1]) == sets._root_limit(request, registry)
    assert all(row.complete for row in db.progress.values())


@pytest.mark.parametrize("retained", [False, True])
async def test_root_physical_work_split_keeps_each_logical_policy(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, retained=retained, count=0) for root_id in range(1, 44)]
    db = CallerDatabase(monkeypatch, request, families)
    monkeypatch.setattr(sets, "MAX_BATCH_ROWS", 64)
    await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    assert len(db.calls) == 6 and not db.reads and not db.heartbeat.called
    for _name, arguments in db.calls:
        assert len(arguments[4][1]) * sets._root_fanout(request, registry) <= 64
        assert sum(arguments[-1][1]) == len(arguments[4][1])
        assert max(arguments[-1][1]) <= sets._root_limit(request, registry)
    assert len(db.progress) == 43 and all(row.complete for row in db.progress.values())


async def test_failed_root_physical_transaction_rolls_back_every_logical_span(monkeypatch):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, count=0) for root_id in range(1, 44)]
    db = CallerDatabase(monkeypatch, request, families)
    db.fail_before_commit = CancellationRequested("canceling physical roots")
    with pytest.raises(CancellationRequested, match="physical roots"):
        await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    assert len(db.calls) == 1 and sum(db.calls[0][1][-1][1]) == 43
    assert not db.progress and not db.reads and not db.write_open
    db.fail_before_commit = None
    await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    assert len(db.progress) == 43 and all(row.complete for row in db.progress.values())


async def test_lost_root_acknowledgement_resumes_from_committed_native_receipts(monkeypatch):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, count=0) for root_id in range(1, 44)]
    db = CallerDatabase(monkeypatch, request, families)
    db.fail_after_commit = ConnectionError("lost physical root acknowledgement")
    with pytest.raises(ConnectionError, match="root acknowledgement"):
        await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    durable_ids_by_root = {
        root_id: (row.family_revision_id, row.root_revision_id) for root_id, row in db.progress.items()
    }
    assert len(durable_ids_by_root) == 43 and all(row.complete for row in db.progress.values())
    db.fail_after_commit = None
    await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    assert {
        root_id: (row.family_revision_id, row.root_revision_id) for root_id, row in db.progress.items()
    } == durable_ids_by_root
    assert len(db.calls) == 2 and not db.reads and not db.heartbeat.called


async def test_root_physical_byte_envelope_splits_with_logical_spans_intact(monkeypatch):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, count=0) for root_id in range(1, 13)]
    db = CallerDatabase(monkeypatch, request, families)
    monkeypatch.setattr(sets, "MAX_BATCH_BYTES", 8192)
    await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    held_bytes = sum(graph._model_bytes((item.plan, item.root, item.record)) for item, _children in families)
    assert len(db.calls) > 1
    assert all(held_bytes + sets._child_argument_bytes(arguments) <= 8192 for _name, arguments in db.calls)
    assert all(max(arguments[-1][1]) <= sets._root_limit(request, registry) for _name, arguments in db.calls)
    assert len(db.progress) == 12 and all(row.complete for row in db.progress.values())


@pytest.mark.parametrize("retained", [False, True])
async def test_cross_root_children_share_one_physical_read_and_write(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, retained=retained) for root_id in range(1, 401)]
    db = CallerDatabase(monkeypatch, request, families)
    reservations = Mock(wraps=sets._child_reservations)
    monkeypatch.setattr(sets, "_child_reservations", reservations)
    await sets.consume_family_page(None, request, registry, tuple(item for item, _children in families))
    assert len(db.calls) == 2 and db.read_windows == db.heartbeat.await_count == 1
    assert len(db.reads) == 2 and reservations.call_count == 1
    arguments = db.calls[1][1]
    assert len(arguments[4][1]) == sum(arguments[-1][1]) == 400
    assert max(arguments[-1][1]) <= sets._child_limit(request, registry)
    assert set(arguments[4][1]) == set(arguments[9 if retained else 10][1])
    for statement in db.reads:
        compiled = statement.compile(dialect=postgresql.dialect())
        assert "unnest(" in str(compiled) and "AS tips(root_id, family_id, base_family_id," in str(compiled)
        assert len(compiled.params) < 50
        assert len(compiled.params["tip_root_record_id"]) == 400
        assert compiled.params["tip_last_child_collection_slot"] == (None,) * 400
    assert len(db.seen) == 400 and all(row.complete for row in db.progress.values())


@pytest.mark.parametrize("retained", [False, True])
def test_cross_root_tip_arrays_keep_nullable_native_cursor_types(retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, retained=retained) for root_id in (1, 2)]
    states = tuple(
        sets._Started(
            prepared_family,
            graph.CustomImportBuildFamily(
                build_id=7,
                root_record_id=prepared_family.plan.root_record_id,
                selection_kind=prepared_family.plan.selection_kind,
                family_revision_id=20_000 + prepared_family.plan.root_record_id,
                base_family_revision_id=prepared_family.plan.base_family_revision_id,
                last_child_collection_slot=child.collection_slot if prepared_family.plan.root_record_id == 2 else None,
                last_child_key_sha256=child.child_key_sha256
                if prepared_family.plan.root_record_id == 2 and not retained
                else None,
                last_input_child_revision_id=child.child_revision_id
                if prepared_family.plan.root_record_id == 2
                else None,
            ),
            graph.CustomImportFamilyRevision(family_revision_id=20_000 + prepared_family.plan.root_record_id),
            False,
        )
        for prepared_family, (child,) in families
    )
    statement, _keys, _models = sets._children_statement(
        request, registry, states, snapshot_models(17), snapshot_models(18)
    )
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "::SMALLINT[]" in sql and "::BYTEA[]" in sql and "::BIGINT[]" in sql
    assert "tips.after_slot IS NULL" in sql and "IS NOT DISTINCT FROM tips.base_family_id" in sql
    assert compiled.params["tip_root_record_id"] == (1, 2)
    assert compiled.params["tip_last_child_collection_slot"] == (None, 1)
    assert compiled.params["tip_last_input_child_revision_id"] == (None, families[1][1][0].child_revision_id)
    assert compiled.params["tip_last_child_key_sha256"] == (
        None,
        None if retained else families[1][1][0].child_key_sha256,
    )


@pytest.mark.parametrize("retained", [False, True])
async def test_byte_limited_root_spans_do_not_reopen_physical_child_windows(monkeypatch, retained):
    request = _request(page_row_limit=32)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, retained=retained) for root_id in range(1, 41)]
    db = CallerDatabase(monkeypatch, request, families)
    root_codec = sets._root_arguments
    spans = []

    def observe_span(*args):
        groups = logical_page(*args)
        spans.append(tuple(group.state.current.root_record_id for group in groups))
        return groups

    # Root preparation is independently native-bounded; exercise only child read reservations here.
    async def start_roots(*args):
        inputs = args[3]
        for prepared_family in inputs:
            name, arguments = root_codec(request, registry, (prepared_family,))
            async with db.write_session() as (session, _build):
                await db.call(session, name, arguments)
        return sets._root_receipts(
            request, registry, inputs, [db.progress[prepared_family.plan.root_record_id] for prepared_family in inputs]
        )

    monkeypatch.setattr(sets, "_start_roots", start_roots)
    logical_page = sets._logical_child_page
    monkeypatch.setattr(sets, "_logical_child_page", observe_span)
    prepared_family, children = families[0]
    request = replace(
        request,
        page_byte_limit=graph._model_bytes(
            (prepared_family.plan, prepared_family.root, prepared_family.record, children[0])
        )
        + 512,
    )
    db.request = request
    await sets.consume_family_page(
        None, request, registry, tuple(prepared_family for prepared_family, _children in families)
    )
    calls = [arguments for name, arguments in db.calls if "families_page" in name]
    assert len(calls) == db.read_windows == db.heartbeat.await_count == 1
    assert spans and all(len(span) == 1 for span in spans)
    assert len(db.seen) == 40 and all(receipt.complete for receipt in db.progress.values())


async def test_retained_cross_root_logical_policy_charges_each_touched_tip(monkeypatch):
    request = _request(page_row_limit=256)
    registry = _registry(request.definition)
    families = [family_input(request, root_id, retained=True) for root_id in range(1, 401)]
    db = CallerDatabase(monkeypatch, request, families)
    logical_sizes = []
    logical_page = sets._logical_child_page

    def record_logical_cost(*args):
        groups = logical_page(*args)
        count = sum(len(group.children) for group in groups)
        assert count * (sets._child_fanout(request, registry) - 1) + 2 * len(groups) <= request.page_row_limit
        logical_sizes.append(count)
        return groups

    monkeypatch.setattr(sets, "_logical_child_page", record_logical_cost)
    await sets.consume_family_page(None, request, registry, tuple(prepared for prepared, _children in families))
    assert len(db.calls) == 2 and db.read_windows == db.heartbeat.await_count == 1
    assert sum(logical_sizes) == 400 and max(logical_sizes) < sets._child_limit(request, registry)
    assert all(receipt.complete for receipt in db.progress.values())


async def test_retained_single_family_keeps_logical_pack_size_when_native_cost_fits(monkeypatch):
    request = _request(page_row_limit=252)
    registry = _registry(request.definition)
    limit = sets._child_limit(request, registry)
    families = [family_input(request, 1, retained=True, count=limit)]
    db = CallerDatabase(monkeypatch, request, families)
    await sets.consume_family_page(None, request, registry, (families[0][0],))
    assert len(db.calls) == 2 and db.read_windows == db.heartbeat.await_count == 1
    arguments = db.calls[-1][1]
    assert arguments[-1][1] == (limit,) and len(arguments[4][1]) == 1
    assert db.progress[1].complete and len(db.seen) == limit
