# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lazy paired audit/output groups; original semantics remain caller-owned.

Only one family is spooled at a time. Complete cohort EOF is required even for
empty output; a borrowed group never grants acceptance or source authority.
"""

import heapq
import os
import stat
from contextlib import ExitStack, closing, contextmanager
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace

from process.custom_import.build_source import _close_iterator, _source_cleanup_failure
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_directory import RevisionMembership
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import ShardOutputProof, _checked_receipt, _size
from process.custom_import.family_shard_admission import AdmissionWork, _digest_file, _scope, _worker_rows
from process.custom_import.family_shard_facts import _Replay
from process.custom_import.family_shard_output import FamilyShardOutput, _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _key_order, _Scratch

_ROLES = ("fragment", "revision_run", "query_record")
_MISSING = object()
_HEAD_FIXED_BYTES = 32768


def _open_output(output, buffer_bytes):
    if type(output) is not FamilyShardOutput or output._closed:
        raise ValueError("family grouping requires the live original output")
    root, observed = output._work.root.lstat(), output.path.lstat()
    if not stat.S_ISDIR(root.st_mode) or (root.st_dev, root.st_ino) != (
        output._work.identity.st_dev,
        output._work.identity.st_ino,
    ):
        raise ValueError("family grouping output directory custody differs")
    source = os.fdopen(os.open(output.path, os.O_RDONLY | os.O_NOFOLLOW), "rb", buffering=buffer_bytes)
    try:
        opened = os.fstat(source.fileno())
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o400
            or (opened.st_dev, opened.st_ino, opened.st_size)
            != (output._identity.st_dev, output._identity.st_ino, output._identity.st_size)
            or (observed.st_dev, observed.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise ValueError("family grouping output file custody differs")
        return source
    except BaseException:
        source.close()
        raise


class _FamilyOutputRows:
    def __init__(self, output, layout, columns_by_role, work, buffer_bytes):
        if type(output) is not FamilyShardOutput or output._closed:
            raise ValueError("family grouping requires the live original output")
        self.layout, self.work = layout, work
        self.output, self.receipt = output, _checked_receipt(layout, output.receipt)
        self.columns_by_role = columns_by_role
        self.sha256 = self.output.sha256
        self.proof = ShardOutputProof(
            layout, self.receipt["shard_id"], self.receipt["partition_sha256"], self.receipt["source_record_count"]
        )
        self.source = _open_output(self.output, buffer_bytes)

    def take(self, role, family_id=None):
        """Read one existing frame without retaining a lookahead row per shard."""
        self.work.current()
        with closing(self.work.read(self.source)) as entries:
            entry = next(entries, _MISSING)
        if entry is _MISSING or type(entry) is not tuple or len(entry) != 2 or entry[0] != role:
            raise ValueError("family grouping output role or EOF differs")
        cells = _unpack_row(entry[1])
        if type(cells) is not tuple or len(cells) != len(self.columns_by_role[role]):
            raise ValueError("family grouping native COPY columns differ")
        if 1024 + sum(_size(value) for value in cells) > self.work.limits.memory_bytes:
            raise FamilyBlockResourceLimit("family grouping native row exceeds its owner reservation")
        if family_id is not None and cells[1 if role == "query_record" else 0] != family_id:
            raise ValueError("family grouping output belongs to another family")
        self.proof.add(role, (cells,))
        return cells

    def finish(self):
        """Bind every physical row and true file EOF to the original receipt."""
        with closing(self.work.read(self.source)) as entries:
            if next(entries, _MISSING) is not _MISSING:
                raise ValueError("family grouping has trailing output rows")
        self.source.seek(0)
        if _digest_file(self.source, self.work.limits.memory_bytes, self.work.current) != self.sha256:
            raise ValueError("family grouping original output bytes changed")
        if (
            self.proof.receipt() != self.receipt
            or self.output.receipt != self.receipt
            or self.output.sha256 != self.sha256
        ):
            raise ValueError("family grouping complete output receipt differs")

    def close(self):
        """Close only this borrowed file; the worker retains cleanup ownership."""
        self.source.close()


class _OutputRows(_FamilyOutputRows):
    def __init__(self, worker, layout, columns_by_role, work, buffer_bytes):
        self.worker = worker
        super().__init__(worker._output, layout, columns_by_role, work, buffer_bytes)

    def finish(self):
        """Retain the original SOURCE wrapper's independent receipt comparison."""
        super().finish()
        if self.worker.receipt != self.receipt:
            raise ValueError("family grouping complete output receipt differs")


class BorrowedSourceFamily:
    """One exact provisional family group, borrowed until the next advance."""

    def __init__(self, work, header, family_row):
        self.is_semantically_selected = True
        self.work, self.header = work, MappingProxyType(dict(header))
        self.family_row = MappingProxyType(family_row)
        self.replays_by_role, self.readers, self.is_active = {}, set(), True
        try:
            for role in _ROLES:
                self.replays_by_role[role] = _Replay(work)
        except BaseException:
            self.close()
            raise

    def rows(self, role):
        """Replay exact native COPY cells without decoding this family's values."""
        if role not in self.replays_by_role or not self.is_active:
            raise ValueError("family grouping replay outlived its current family")

        def cells():
            """Stop borrowed readers before the current family's scratch closes."""
            try:
                for packed in self.replays_by_role[role].rows():
                    self.work.current()
                    if not self.is_active:
                        raise ValueError("family grouping replay outlived its current family")
                    yield _unpack_row(packed)
            finally:
                self.readers.discard(reader)

        reader = cells()
        self.readers.add(reader)
        return reader

    def fragments(self):
        """Expose original native directory cells and compressed frame bytes."""
        with closing(self.rows("fragment")) as rows:
            for row in rows:
                yield (row[1], row[2], *row[4:]), row[3]

    def memberships(self):
        """Expose explicit actual-ID runs; the full verifier checks their content."""
        with closing(self.rows("revision_run")) as rows:
            for row in rows:
                yield RevisionMembership(*row[1:])

    def query_rows(self):
        """Expose native rank/variant query cells for the owner's dictionary join."""
        return self.rows("query_record")

    def close(self):
        """Close this current family only, preserving every original worker file."""
        self.is_active = False
        primary = None
        for reader in tuple(self.readers):
            primary = _close_iterator(reader, primary)
        for replay in self.replays_by_role.values():
            try:
                replay.close()
            except BaseException as error:
                primary = _source_cleanup_failure(primary, error)
        if primary is not None:
            raise primary


class UnauditedSourceFamily(BorrowedSourceFamily):
    """A consumed native envelope; unselected body readers are deliberately unavailable."""

    def __init__(self, work, header, family_row):
        self.work, self.header = work, MappingProxyType(dict(header))
        self.family_row = MappingProxyType(family_row)
        self.replays_by_role, self.readers, self.is_active = {}, set(), True
        self.is_semantically_selected = False


def _native_family(rows):
    cells = rows.take("family")
    family_by_name = dict(zip(rows.columns_by_role["family"], cells, strict=True))
    for name in (
        "family_revision_id",
        "entity_binding_id",
        "root_record_id",
        "root_revision_id",
        "fragment_count",
        "raw_bytes",
        "stored_bytes",
        "membership_count",
    ):
        _integer(family_by_name[name], "family grouping " + name)
    _integer(family_by_name["child_count"], "family grouping children", zero=True)
    if family_by_name["membership_count"] > family_by_name["child_count"] + 1:
        raise ValueError("family grouping declared membership count differs")
    if type(family_by_name["retained"]) is not bool:
        raise ValueError("family grouping retained mode differs")
    return family_by_name


def _family_header(rows, header):
    family_by_name = _native_family(rows)
    if (family_by_name["family_revision_id"], family_by_name["entity_binding_id"]) != header[
        "family_ids"
    ] or family_by_name["retained"] is not False:
        raise ValueError("family grouping header and original output identity differ")
    return family_by_name


def _fill_group(layout, output_rows, group, audit=None):
    family = group.family_row
    identity = family["family_revision_id"]
    raw_bytes = stored_bytes = 0
    for ordinal in range(family["fragment_count"]):
        cells = output_rows.take("fragment", identity)
        if cells[1] != ordinal:
            raise ValueError("family grouping fragment order differs")
        raw_bytes += cells[2]
        stored_bytes += 8 + len(cells[3])
        if audit is not None:
            audit.observe("fragment", cells, family)
        if group.is_semantically_selected:
            group.replays_by_role["fragment"].append(_pack_row(cells))
    if (raw_bytes, stored_bytes) != (family["raw_bytes"], family["stored_bytes"]):
        raise ValueError("family grouping fragment byte census differs")
    count, query_count, root_count, previous = 0, 0, 0, None
    query_slot = layout.collection_slots.get(layout.definition.query.child_collection)
    for _entry in range(family["membership_count"]):
        cells = output_rows.take("revision_run", identity)
        member = RevisionMembership(*cells[1:])
        order = member.collection_slot, member.first_revision_id
        if member.collection_slot not in (0, *layout.collection_slots.values()) or (
            previous is not None and order <= previous
        ):
            raise ValueError("family grouping membership collection or order differs")
        count += member.record_count
        query_count += member.record_count if member.collection_slot == query_slot else 0
        root_count += member.record_count if member.collection_slot == 0 else 0
        previous = order
        if audit is not None:
            audit.observe("revision_run", cells, family)
        if group.is_semantically_selected:
            group.replays_by_role["revision_run"].append(_pack_row(cells))
    if count != family["child_count"] + 1 or root_count != 1:
        raise ValueError("family grouping membership extent differs")
    for _ordinal in range(query_count):
        cells = output_rows.take("query_record", identity)
        if cells[2] != query_slot:
            raise ValueError("family grouping query collection differs")
        if audit is not None:
            audit.observe("query_record", cells, family)
        if group.is_semantically_selected:
            group.replays_by_role["query_record"].append(_pack_row(cells))
    if audit is not None:
        audit.end()


def _next_header(entries, shard, count):
    for entry in entries:
        if entry[0] != "family":
            continue
        header = entry[2]
        key = header["typed_key"]
        if key is None:
            order, routed = (1, b"", b""), 0
        else:
            digest, text = _key_order(key)
            order, routed = (0, digest, text.encode("utf-8")), int.from_bytes(digest[:8], "big") % count
        if routed != shard or header["selection"] == "error":
            raise ValueError("family grouping original shard routing or status differs")
        return order, header
    return None


def _heads(work, allocated_source, layout, workers, complete_scope, stack):
    heap, seen, size, record_count = [], set(), 0, 0
    for worker in workers:
        entries = stack.enter_context(
            closing(_worker_rows(work, allocated_source, layout.definition, worker, len(workers), complete_scope))
        )
        first = next(entries, None)
        if first is None or first[0] != "worker" or first[1] in seen or worker.status != "source_eof":
            raise ValueError("family grouping worker cohort repeats or is refused")
        shard = first[1]
        seen.add(shard)
        record_count += first[2]["shard"]["records"]
        head = _next_header(entries, shard, len(workers))
        if head is not None:
            charge = _HEAD_FIXED_BYTES + 8 * len(_pack_row(head[1]))
            size += charge
            if size > work.limits.memory_bytes * 8:
                raise FamilyBlockResourceLimit("family grouping header cohort exceeds the owner reservation")
            heapq.heappush(heap, (head[0], shard, head[1], entries, worker, charge))
    if seen != set(range(len(workers))) or record_count != allocated_source.source.counts[0]:
        raise ValueError("family grouping complete original source cohort differs")
    if size > work.limits.memory_bytes * 8:
        raise FamilyBlockResourceLimit("family grouping header cohort exceeds the owner reservation")
    return heap, size


def _output_cursors(allocated_source, layout, workers, work, stack):
    columns_by_role = {
        role: tuple(table.c.keys()) for role, table in layout.tables(allocated_source.snapshot_family_id).items()
    }
    buffer_bytes = min(65536, work.limits.memory_bytes // max(1, len(workers)))
    rows_by_shard = {}
    for worker in workers:
        output_rows = _OutputRows(worker, layout, columns_by_role, work, buffer_bytes if buffer_bytes != 1 else 0)
        stack.callback(output_rows.close)
        rows_by_shard[output_rows.receipt["shard_id"]] = output_rows
    return rows_by_shard


def _groups(allocated_source, layout, workers, work, complete_scope, state, audit=None):
    if audit is not None:
        from process.custom_import.family_shard_semantic_audit import SourceOutputAudit

        if type(audit) is not SourceOutputAudit:
            raise ValueError("family grouping requires its typed original semantic audit")
    with ExitStack() as stack:
        heap, head_bytes = _heads(work, allocated_source, layout, workers, complete_scope, stack)
        rows_by_shard = _output_cursors(allocated_source, layout, workers, work, stack)
        previous = None
        while heap:
            work.current()
            order, shard, header, entries, worker, charge = heapq.heappop(heap)
            head_bytes -= charge
            if previous is not None and order <= previous:
                raise ValueError("family grouping global typed keys repeat or are unordered")
            previous = order
            if header["selection"] == "source":
                family_row = _family_header(rows_by_shard[shard], header)
                keep = audit is None or audit.begin(header, family_row)
                group_type = BorrowedSourceFamily if keep else UnauditedSourceFamily
                group = group_type(work, header, family_row)
                try:
                    _fill_group(layout, rows_by_shard[shard], group, audit)
                    yield group
                finally:
                    group.close()
            head = _next_header(entries, shard, len(workers))
            if head is not None:
                charge = _HEAD_FIXED_BYTES + 8 * len(_pack_row(head[1]))
                head_bytes += charge
                if head_bytes > work.limits.memory_bytes * 8:
                    raise FamilyBlockResourceLimit("family grouping header cohort exceeds the owner reservation")
                heapq.heappush(heap, (head[0], shard, head[1], entries, worker, charge))
        for output_rows in rows_by_shard.values():
            output_rows.finish()
        state.is_complete = True


def _direct_groups(output, layout, snapshot_family_id, work, state):
    """Read physical family groups without inventing an audit or original header."""
    columns_by_role = {role: tuple(table.c.keys()) for role, table in layout.tables(snapshot_family_id).items()}
    buffer_bytes = min(65536, work.limits.memory_bytes)
    rows = _FamilyOutputRows(output, layout, columns_by_role, work, buffer_bytes if buffer_bytes != 1 else 0)
    try:
        for _ordinal in range(rows.receipt["role_counts"]["family"]):
            work.current()
            group = BorrowedSourceFamily(work, {}, _native_family(rows))
            try:
                _fill_group(layout, rows, group)
                yield group
            finally:
                group.close()
        rows.finish()
        state.is_complete = True
    finally:
        rows.close()


@contextmanager
def family_output_groups(output, layout, snapshot_family_id, *, work):
    """Borrow exact physical groups; full EOF grants no original-input authority."""
    if type(work) is not PlanWork or type(layout) is not FamilyBlockLayout:
        raise ValueError("family grouping requires exact layout and accounted PlanWork")
    _integer(snapshot_family_id, "family grouping snapshot")
    bounded = replace(
        work,
        limits=replace(
            work.limits,
            memory_bytes=max(1, work.limits.memory_bytes // 16),
            max_record_bytes=min(work.limits.max_record_bytes, max(1, work.limits.memory_bytes // 16)),
        ),
    )
    scratch, state, primary = _Scratch(bounded), SimpleNamespace(is_complete=False), None
    groups = _direct_groups(output, layout, snapshot_family_id, scratch, state)
    try:
        yield groups
        if not state.is_complete:
            raise ValueError("family grouping requires complete physical output EOF")
    except BaseException as error:
        primary = error
    primary = _close_iterator(groups, primary)
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary


@contextmanager
def source_family_output_groups(allocated_source, layout, workers, *, work, complete_scope):
    """Borrow one ordered family at a time, requiring full cohort EOF on success.

    Run CPU/file work off the coordinator event loop. Keep all original worker,
    SOURCE and allocation owners alive; this adapter does not verify semantics.
    """
    if type(work) is not PlanWork or type(layout) is not FamilyBlockLayout:
        raise ValueError("family grouping requires exact layout and accounted PlanWork")
    _scope(
        allocated_source,
        layout.definition,
        workers,
        AdmissionWork(work.root, work.limits, work.register_cleanup, work.check),
        complete_scope,
    )
    if dict(allocated_source.source.collection_slots) != dict(layout.collection_slots):
        raise ValueError("family grouping native collection scope differs")
    bounded = replace(
        work,
        limits=replace(
            work.limits,
            memory_bytes=max(1, work.limits.memory_bytes // 16),
            max_record_bytes=min(work.limits.max_record_bytes, max(1, work.limits.memory_bytes // 16)),
        ),
    )
    scratch, state, primary = _Scratch(bounded), SimpleNamespace(is_complete=False), None
    groups = _groups(allocated_source, layout, workers, scratch, complete_scope, state)
    try:
        yield groups
        if not state.is_complete:
            raise ValueError("family grouping requires complete audit and output EOF")
    except BaseException as error:
        primary = error
    primary = _close_iterator(groups, primary)
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
