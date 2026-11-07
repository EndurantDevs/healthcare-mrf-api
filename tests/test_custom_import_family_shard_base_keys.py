# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real key spools and full codecs; native SQL, pins and cursor I/O are fakes."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql.util import find_tables

from process.custom_import import family_shard_base_keys as keys
from process.custom_import import runner_codec as codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_binding import FamilyBlockBinding, FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockError, FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_keys import iter_key_dictionary
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import FamilyRecord, record_parts
from process.custom_import.family_shard_base import PinnedBase
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_output import AllocatedFamily, FamilyOutputWork, build_family_output
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_retained import restore_legacy_record
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_family_block_codec import _case
from tests.test_custom_import_family_shard_retained import _legacy

_LIMITS = SourceFactLimits(32 * 1024**2, 4 * 1024**2, 4 * 1024**2)


class _Cursor:
    def __init__(self, rows):
        self.rows, self.closed, self.pause, self.close_pause = iter(rows), False, None, None
        self.close_error = None
        self.on_eof = None

    def mappings(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.pause is not None:
            await self.pause.wait()
        try:
            return next(self.rows)
        except StopIteration:
            if self.on_eof is not None:
                self.on_eof()
            raise StopAsyncIteration from None

    async def close(self):
        if self.close_pause is not None:
            await self.close_pause.wait()
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class _Session:
    def __init__(self, rows):
        self.rows, self.executed, self.streamed, self.cursors = rows, [], [], []
        self.transaction, self.nested = SimpleNamespace(is_active=True), None
        self.link = SimpleNamespace(
            transaction=SimpleNamespace(is_active=True),
            nested=None,
            get_transaction=lambda: self.link.transaction,
            get_nested_transaction=lambda: self.link.nested,
        )
        self.census, self.invalid, self.isolation, self.hook = {}, False, ("repeatable read", "on"), None
        self.pause, self.close_pause, self.close_error = None, None, None
        self.on_eof = None

    def get_transaction(self):
        return self.transaction

    def get_nested_transaction(self):
        return self.nested

    async def connection(self):
        return self.link

    def role(self, statement):
        names = {table.name for table in find_tables(statement)}
        for role in ("fragment", "revision_run", "family"):
            if "custom_import_block_" + role in names:
                return role
        return "direct"

    async def execute(self, statement):
        self.executed.append(statement)
        if "transaction_isolation" in str(statement):
            return SimpleNamespace(one=lambda: self.isolation)
        if self.hook is not None:
            self.hook(statement)
        if "row_count" in statement.selected_columns.keys():
            role = self.role(statement)
            rows = self.rows.get(role, [])
            census = self.census.get(role, SimpleNamespace(row_count=len(rows), row_bytes=4096 if rows else 0))
            return SimpleNamespace(one=lambda: census)
        return SimpleNamespace(first=lambda: (1,) if self.invalid else None)

    async def stream(self, statement):
        self.streamed.append(statement)
        cursor = _Cursor(self.rows.get(self.role(statement), []))
        cursor.pause, cursor.close_pause, cursor.close_error = self.pause, self.close_pause, self.close_error
        cursor.on_eof = self.on_eof
        self.cursors.append(cursor)
        return cursor


def _context(monkeypatch, rows=(), *, block=False, canonical=False, definition=None, current=None):
    definition = definition or _case(0)[0]
    current = current or definition
    session = _Session({"direct": list(rows)} if not isinstance(rows, dict) else rows)
    request = _request(definition=current, expected_base_generation_id=9, expected_pointer_version=1)
    generation = SimpleNamespace(generation_id=9, dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    slots_by_name = {name: index + 7 for index, name in enumerate(definition.collections_by_name)}
    layout = FamilyBlockLayout(definition, slots_by_name)
    binding = FamilyBlockBinding(
        17, layout, layout.tables(17), {role: 1000 + i for i, role in enumerate(layout.tables(17))}, 71
    )
    base = PinnedBase(
        session,
        generation,
        SimpleNamespace(family_count=len(session.rows.get("family", []))),
        definition,
        SimpleNamespace(child_collection_slots=slots_by_name),
        SimpleNamespace(family_id=None if canonical else 17, legacy_snapshot=not block and not canonical),
        binding if block else None,
        request,
        (session.transaction, None, session.link.transaction, None),
        None,
        4 * 1024**2,
    )
    monkeypatch.setattr(keys, "resolve_pinned_family_block_binding", AsyncMock(return_value=binding))
    return base, session


def _key_rows(values, *, block=False):
    ordered = sorted(set(values), key=lambda row: (row[0], row[1].encode("utf-8")))
    if block:
        return [row._asdict() for row in iter_key_dictionary(ordered)]
    return [dict(child_key_sha256=digest, canonical_child_key=text) for digest, text in ordered]


def _work(tmp_path, *, limits=_LIMITS, check=None):
    return PlanWork(tmp_path / "keys", limits, lambda path: None, check)


@pytest.mark.parametrize("layout", ["canonical", "legacy", "block"])
async def test_zero_keys_require_real_native_eof(monkeypatch, tmp_path, layout):
    base, session = _context(monkeypatch, [], block=layout == "block", canonical=layout == "canonical")
    async with keys.pinned_base_query_keys(base, work=_work(tmp_path)) as replay:
        assert list(replay()) == []
        assert len(session.cursors) == 1 and session.cursors[0].closed
        assert session.streamed[0].get_execution_options()["yield_per"] == 1
        assert not base.complete


async def test_empty_cursor_rechecks_pin_after_eof(monkeypatch, tmp_path):
    base, session = _context(monkeypatch)
    session.on_eof = lambda: setattr(session.link, "transaction", SimpleNamespace(is_active=True))
    with pytest.raises(FamilyBlockUnavailable, match="no longer live"):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("empty cursor cannot hide pin loss")
    assert session.cursors[0].closed and not (tmp_path / "keys").exists()


@pytest.mark.parametrize("layout", ["canonical", "legacy", "block"])
async def test_complete_keys_preserve_exact_bytes_and_global_variants(monkeypatch, tmp_path, layout):
    original_keys = [(b"a" * 32, " λ"), (b"a" * 32, "λ "), (b"b" * 32, "e\u0301"), (b"c" * 32, "é")]
    base, session = _context(
        monkeypatch,
        _key_rows(original_keys, block=layout == "block"),
        block=layout == "block",
        canonical=layout == "canonical",
    )
    async with keys.pinned_base_query_keys(base, work=_work(tmp_path)) as replay:
        assert session.cursors and all(cursor.closed for cursor in session.cursors)
        assert list(replay()) == original_keys == list(replay())
        assert not base.complete and base._headers is None
        assert replay.path.is_file() and not replay.path.stat().st_mode & 0o222
        assert not hasattr(replay, "verified")
    assert not (tmp_path / "keys").exists()
    with pytest.raises(FamilyBlockUnavailable, match="outlived"):
        replay()


@pytest.mark.parametrize("absent", [True, False])
async def test_empty_base_and_no_query_collection_check_live_pin(monkeypatch, tmp_path, absent):
    definition = _case(0)[0]
    document = json.loads(definition.canonical)
    document["query"].pop("child")
    document["query"]["order"] = []
    document["selection_profiles"] = [{"id": "default", "selection": [], "context_dimensions": []}]
    current = CustomImportDefinition.from_mapping(document)
    base, session = _context(monkeypatch, current=current)
    async with keys.pinned_base_query_keys(None if absent else base, work=_work(tmp_path)) as replay:
        assert list(replay()) == []
        assert session.streamed == []
    assert not (tmp_path / "keys").exists()


@pytest.mark.parametrize("layout", ["canonical", "legacy"])
def test_legacy_sql_uses_only_exact_native_scoped_joins(monkeypatch, layout):
    base, _session = _context(monkeypatch, canonical=layout == "canonical")
    guard, statement = keys._legacy_statements(base, "rates")
    compiled = str(guard.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))
    assert "generation_id = 9" in compiled and "collection_slot = 7" in compiled
    for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "root_record_id"):
        assert name + " IS DISTINCT FROM" in compiled or name + " IS DISTINCT FROM" in compiled.replace("base_key_", "")
    schemas_by_table = {table.name: table.schema for table in find_tables(statement)}
    expected_schema = None if layout == "canonical" else "ci_snapshot_17"
    assert set(schemas_by_table) >= {
        "custom_import_generation_family",
        "custom_import_family_revision",
        "custom_import_family_child",
        "custom_import_child_revision",
    }
    if expected_schema is not None:
        assert set(schemas_by_table.values()) == {expected_schema}
    assert 'COLLATE "C"' in str(statement.compile(dialect=dialect()))
    assert "canonical_payload" not in compiled and "json" not in compiled.lower()


@pytest.mark.parametrize("damage", ["scope", "isolation", "guard", "width", "count", "zero_width", "empty_width"])
async def test_native_refusals_clean_exact_work(monkeypatch, tmp_path, damage):
    base, session = _context(monkeypatch, _key_rows([(b"a" * 32, "key")]))
    match damage:
        case "scope":
            base.request = replace(base.request, dataset_id=99)
        case "isolation":
            session.isolation = "read committed", "on"
        case "guard":
            session.invalid = True
        case "width":
            session.census["direct"] = SimpleNamespace(row_count=1, row_bytes=_LIMITS.memory_bytes)
        case "count":
            session.census["direct"] = SimpleNamespace(row_count=2, row_bytes=4096)
        case "zero_width":
            session.census["direct"] = SimpleNamespace(row_count=1, row_bytes=0)
        case "empty_width":
            session.census["direct"] = SimpleNamespace(row_count=0, row_bytes=4096)
    with pytest.raises((FamilyBlockUnavailable, FamilyBlockResourceLimit)):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("a prefix or invalid native input cannot yield replay")
    assert all(cursor.closed for cursor in session.cursors)
    assert not (tmp_path / "keys").exists()


@pytest.mark.parametrize("damage", ["order", "duplicate", "rank", "variant", "hash", "text"])
async def test_block_key_domain_and_rank_guards(monkeypatch, tmp_path, damage):
    rows = _key_rows([(b"a" * 32, "key"), (b"b" * 32, "other")], block=True)
    match damage:
        case "order":
            rows.reverse()
        case "duplicate":
            rows.append(rows[-1])
        case "rank":
            rows[1]["key_rank"] = 999
        case "variant":
            rows[1]["key_variant"] = True
        case "hash":
            rows[1]["child_key_sha256"] = b"short"
        case "text":
            rows[1]["canonical_child_key"] = "bad\x00text"
    base, session = _context(monkeypatch, rows, block=True)
    with pytest.raises((ValueError, FamilyBlockUnavailable)):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("invalid dictionary cannot yield replay")
    assert session.cursors[0].closed and not (tmp_path / "keys").exists()


@pytest.mark.parametrize("boundary", ["census", "replay", "exit"])
@pytest.mark.parametrize("replacement", ["session_root", "session_nested", "connection_root", "connection_nested"])
async def test_transaction_replacement_never_reuses_keys(monkeypatch, tmp_path, boundary, replacement):
    base, session = _context(monkeypatch, _key_rows([(b"a" * 32, "key")]))

    def replace_transaction(_statement=None):
        target = session if replacement.startswith("session") else session.link
        setattr(target, "nested" if replacement.endswith("nested") else "transaction", SimpleNamespace(is_active=True))

    if boundary == "census":
        session.hook = replace_transaction
    with pytest.raises(FamilyBlockUnavailable):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)) as replay:
            replace_transaction()
            if boundary == "replay":
                list(replay())
    assert all(cursor.closed for cursor in session.cursors)
    assert not (tmp_path / "keys").exists()


async def test_replay_partial_reader_closed_and_file_tamper_refused(monkeypatch, tmp_path):
    base, _session = _context(monkeypatch, _key_rows([(b"a" * 32, "key")]))
    async with keys.pinned_base_query_keys(base, work=_work(tmp_path)) as replay:
        reader = replay()
        assert next(reader) == (b"a" * 32, "key")
        assert replay.readers
        replay.path.chmod(0o600)
        with replay.path.open("r+b") as changed:
            changed.seek(-1, 2)
            changed.write(b"X")
        with pytest.raises(FamilyBlockUnavailable, match="bytes changed"):
            list(replay())
    assert reader.gi_frame is None and not replay.readers
    assert not (tmp_path / "keys").exists()


async def test_native_cancel_drains_cursor_close_before_removing_work(monkeypatch, tmp_path):
    base, session = _context(monkeypatch, _key_rows([(b"a" * 32, "key")]))
    session.pause, session.close_pause = asyncio.Event(), asyncio.Event()

    async def run():
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("paused cursor cannot yield replay")

    task = asyncio.create_task(run())
    while not session.cursors:
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and (tmp_path / "keys").exists()
    session.close_pause.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.cursors[0].closed and not (tmp_path / "keys").exists()


async def test_existing_work_is_preserved_before_native_reads(monkeypatch, tmp_path):
    base, session = _context(monkeypatch)
    (tmp_path / "keys").mkdir()
    sentinel = tmp_path / "keys" / "sentinel"
    sentinel.write_text("preserve")
    with pytest.raises(FileExistsError):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("existing scratch belongs to its original owner")
    assert sentinel.read_text() == "preserve" and not session.executed


async def test_block_binding_must_match_the_actual_pin(monkeypatch, tmp_path):
    base, session = _context(monkeypatch, block=True)
    monkeypatch.setattr(
        keys, "resolve_pinned_family_block_binding", AsyncMock(return_value=replace(base.binding, owner_oid=99))
    )
    with pytest.raises(FamilyBlockUnavailable, match="binding changed"):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("a changed binding cannot select its dictionary")
    assert session.streamed == [] and not (tmp_path / "keys").exists()


async def test_native_close_failure_and_spool_exhaustion_clean_work(monkeypatch, tmp_path):
    base, session = _context(monkeypatch, _key_rows([(b"a" * 32, "key")]))
    session.close_error = OSError("synthetic close refusal")
    with pytest.raises(OSError, match="close refusal"):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("failed EOF cleanup cannot yield keys")
    assert session.cursors[0].closed and not (tmp_path / "keys").exists()
    session.close_error = None
    session.rows["direct"] = _key_rows([(i.to_bytes(32), "key" * 10) for i in range(1, 40)])
    session.census["direct"] = SimpleNamespace(row_count=39, row_bytes=1)
    with pytest.raises(SourceFactBudgetExceeded):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path, limits=SourceFactLimits(1024, 512, 512))):
            pytest.fail("the operational spool budget is finite")
    assert session.cursors[-1].closed and not (tmp_path / "keys").exists()


def _changed_definitions():
    original = _case(0)[0]
    document = json.loads(original.canonical)
    child = json.loads(json.dumps(document["schema"]["children"][0]))
    child["name"], child["child_key"] = "other", ["other_code"]
    for field in child["fields"]:
        field["id"], field["slot"] = "other_" + field["id"], field["slot"] + 100
        if "projection_slot" in field:
            field["projection_slot"] += 4
    child["parent_key"] = [{"child": "other_rate_npi", "root": "npi"}]
    child["child_key"] = ["other_service_code"]
    document["schema"]["children"].append(child)
    document["streams"].append(
        {
            "id": "other_source",
            "kind": "child",
            "child": "other",
            "format": "ndjson",
            "compression": "none",
            "snapshot_token": "snapshot_id",
        }
    )
    before = CustomImportDefinition.from_mapping(document)
    document["revision"]["definition"] += 1
    document["query"]["child"] = {"collection": "other", "fields": ["other_service_code", "other_amount"]}
    document["query"]["order"] = []
    for profile in document["selection_profiles"]:
        for term in profile["selection"]:
            term["field"] = "other_" + term["field"]
        profile["context_dimensions"] = ["other_" + name for name in profile["context_dimensions"]]
    return before, CustomImportDefinition.from_mapping(document)


def _other_records(definition, count):
    _unused, root, _children = _case(count)
    other_records = tuple(
        FamilyRecord(
            800 + i * 13,
            71,
            i,
            {"other_rate_npi": "1234567893", "other_service_code": f"sparse-{i}", "other_amount": None},
        )
        for i in range(count)
    )
    # The retained sparse lane preserves a byte-distinct key in the new query collection.
    revision, key = _legacy(
        definition,
        other_records[0],
        "other",
        key_text=' {"other_service_code":"sparse-0"} ',
        parent=codec.root_key_document(definition, root.values_by_field),
    )
    other_records = (restore_legacy_record(definition, "other", revision, key=key, scalars={}), *other_records[1:])
    return other_records


def _block_rows(tmp_path, definition, *, count=3):
    _unused, root, children = _case(count)
    other_records = _other_records(definition, count)
    # Hash the original fragments, including the preserved sparse key spelling.
    digest = codec._family_hash_from_documents(
        definition,
        root.values_by_field,
        {
            name: sorted(
                record_parts(definition, name, original_record, retained=True)[1]
                for original_record in collection_records
            )
            for name, collection_records in {"rates": children, "other": other_records}.items()
        },
    )
    layout = FamilyBlockLayout(definition, {"rates": 7, "other": 8})
    rows_by_role = {role: [] for role in ("family", "fragment", "revision_run")}
    limits = FamilyBlockLimits(16 * 1024**2, 1024**2, 256, 8192)
    allocated = AllocatedFamily(
        44, 88, 99, root, {"rates": lambda: iter(children), "other": lambda: iter(other_records)}, digest, True
    )
    ids_by_key = {
        (digest, text): (rank, variant)
        for rank, variant, digest, text in iter_key_dictionary(
            sorted(
                (
                    codec.child_key_hash(definition, "rates", child_record.values_by_field),
                    codec.child_key_document(definition, "rates", child_record.values_by_field),
                )
                for child_record in children
            )
        )
    }
    with build_family_output(
        layout,
        17,
        [allocated],
        ids_by_key,
        work=FamilyOutputWork(tmp_path / "output", limits, lambda _path: None),
        shard_id=0,
        partition_sha256=b"p" * 32,
        source_record_count=count,
    ) as output:
        for batch in output.batches(maximum_rows=2, maximum_bytes=1024**2):
            for role, cells in batch.items():
                if role in rows_by_role:
                    rows_by_role[role].extend(
                        dict(zip(layout.tables(17)[role].c.keys(), copy_row, strict=True)) for copy_row in cells
                    )
    return rows_by_role, other_records


async def test_changed_query_collection_uses_full_original_codec(monkeypatch, tmp_path):
    original, current = _changed_definitions()
    assert (
        original.schema_digest == current.schema_digest
        and original.query.child_collection != current.query.child_collection
    )
    rows, other = _block_rows(tmp_path, original)
    base, session = _context(monkeypatch, rows, block=True, definition=original, current=current)
    expected = sorted(
        {
            (record.sparse.key_sha256, record.sparse.key_text)
            if record.sparse
            else (
                codec.child_key_hash(original, "other", record.values_by_field),
                codec.child_key_document(original, "other", record.values_by_field),
            )
            for record in other
        }
    )
    async with keys.pinned_base_query_keys(base, work=_work(tmp_path)) as replay:
        assert list(replay()) == expected
        assert len(session.streamed) == 3 and all(cursor.closed for cursor in session.cursors)
        assert not base.complete and base._headers is None
    assert not (tmp_path / "keys").exists()


async def test_changed_collection_empty_base_exhausts_all_role_cursors(monkeypatch, tmp_path):
    original, current = _changed_definitions()
    base, session = _context(monkeypatch, {}, block=True, definition=original, current=current)
    async with keys.pinned_base_query_keys(base, work=_work(tmp_path)) as replay:
        assert list(replay()) == []
        assert len(session.cursors) == 3 and all(cursor.closed for cursor in session.cursors)
        assert not base.complete


async def test_borrowed_replay_checks_owner_cancellation(monkeypatch, tmp_path):
    base, _session = _context(monkeypatch, _key_rows([(b"a" * 32, "key")]))
    state = SimpleNamespace(is_cancelled=False)

    def check():
        if state.is_cancelled:
            raise RuntimeError("synthetic owner cancellation")

    with pytest.raises(RuntimeError, match="owner cancellation"):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path, check=check)) as replay:
            state.is_cancelled = True
            list(replay())
    assert not (tmp_path / "keys").exists()


@pytest.mark.parametrize(
    "damage",
    [
        "fragment",
        "membership",
        "missing_fragment",
        "orphan_fragment",
        "trailing_run",
        "family_order",
        "family_count",
        "native_eof",
        "scope",
    ],
)
async def test_changed_collection_refuses_incomplete_original_blocks(monkeypatch, tmp_path, damage):
    original, current = _changed_definitions()
    rows_by_role, _other = _block_rows(tmp_path, original)
    base, session = _context(monkeypatch, rows_by_role, block=True, definition=original, current=current)
    match damage:
        case "fragment":
            changed = rows_by_role["fragment"][0]
            changed["encoded_fragment"] = changed["encoded_fragment"][:-1] + bytes(
                [changed["encoded_fragment"][-1] ^ 1]
            )
        case "membership":
            rows_by_role["revision_run"][0]["first_revision_id"] += 1
        case "missing_fragment":
            rows_by_role["fragment"].pop()
        case "orphan_fragment":
            rows_by_role["fragment"][0]["family_revision_id"] = 1
        case "trailing_run":
            rows_by_role["revision_run"].append(rows_by_role["revision_run"][-1] | {"family_revision_id": 999})
        case "family_order":
            rows_by_role["family"].append(dict(rows_by_role["family"][0]))
            base.seal.family_count += 1
        case "family_count":
            base.seal.family_count += 1
        case "native_eof":
            session.census["fragment"] = SimpleNamespace(row_count=len(rows_by_role["fragment"]) + 1, row_bytes=4096)
        case "scope":
            session.invalid = True
    with pytest.raises((FamilyBlockError, FamilyBlockUnavailable, ValueError)):
        async with keys.pinned_base_query_keys(base, work=_work(tmp_path)):
            pytest.fail("incomplete block evidence cannot yield keys")
    assert all(cursor.closed for cursor in session.cursors)
    assert not (tmp_path / "keys").exists()
