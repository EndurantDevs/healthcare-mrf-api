# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded external facts for owner-side SOURCE/admission; no database authority."""

import heapq
import itertools
import json
import os
import shutil
import stat
import struct
import tempfile
from compression import zstd
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import msgpack

from process.custom_import import family_shard_build as admission
from process.custom_import import runner_codec as codec
from process.custom_import.family_shard_input_records import decode_record, encode_record

_LENGTH = struct.Struct(">Q")
_COMPRESSED_SORT = "sort-run-zstd/v1"


class SourceFactBudgetExceeded(OSError):
    """Operational work budget exhausted; no family or input is rejected."""


@dataclass(frozen=True)
class SourceFactLimits:
    max_spool_bytes: int
    max_record_bytes: int
    memory_bytes: int = 1024 * 1024

    def __post_init__(self):
        if any(type(value) is not int or value <= 0 for value in asdict(self).values()):
            raise ValueError("source fact budgets differ")
        if max(self.max_record_bytes, self.memory_bytes) > self.max_spool_bytes:
            raise ValueError("source fact budgets exceed the spool budget")


def _pack(value):
    return msgpack.packb(value, use_bin_type=True)


def _unpack(value):
    return msgpack.unpackb(value, raw=False, use_list=False, strict_map_key=True)


class _Work:
    def __init__(self, root, limits, register_cleanup, *, check=None):
        if check is not None and not callable(check):
            raise ValueError("source fact cancellation check differs")
        self._io_check = check
        self.root, self.limits = Path(root).absolute(), limits
        if self.root.exists() or self.root.is_symlink():
            raise FileExistsError("source fact work directory already exists")
        parent = os.open(self.root.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            filesystem = os.fstatvfs(parent)
        finally:
            os.close(parent)
        self._entry_charge = 2 * max(512, filesystem.f_frsize or filesystem.f_bsize)
        self._entry_bytes = 0
        self.paths = {}
        self.next_file = self.used = self.peak_bytes = 0
        self._reserve_entry()
        register_cleanup(self.root)
        self.root.mkdir(mode=0o700)
        self.identity = self.root.stat()

    @property
    def remaining_bytes(self):
        """Remaining delegated space includes live and provisional entry reservations."""
        return self.limits.max_spool_bytes - self.used - self._entry_bytes

    def _reserve_entry(self):
        if self._io_check is not None:
            self._io_check()
        if self._entry_charge > self.remaining_bytes:
            raise SourceFactBudgetExceeded("source fact resource budget is exhausted before entry creation")
        self._entry_bytes += self._entry_charge
        self.peak_bytes = max(self.peak_bytes, self.used + self._entry_bytes)

    def _release_entry(self, raw_bytes=0):
        if self._entry_bytes < self._entry_charge or not 0 <= raw_bytes <= self.used:
            raise ValueError("source fact entry accounting differs")
        self._entry_bytes -= self._entry_charge
        self.used -= raw_bytes

    def path(self):
        """Create a task-owned scratch path inside this bounded work root."""
        self._reserve_entry()
        name = self.root / f"run-{self.next_file:09}.mpk"
        self.next_file += 1
        self.paths[name] = 0
        return name

    def charge(self, count):
        """Charge actual spool bytes before the existing disk budget is exceeded."""
        if count > self.remaining_bytes:
            raise SourceFactBudgetExceeded("source fact spool budget is exhausted")
        self.used += count
        self.peak_bytes = max(self.peak_bytes, self.used + self._entry_bytes)

    def frame(self, target, value, path=None):
        """Encode one raw frame after enforcing the singleton memory bound."""
        if self._io_check is not None:
            self._io_check()
        payload = _pack(value)
        if len(payload) > self.limits.max_record_bytes:
            raise SourceFactBudgetExceeded("source fact singleton exceeds the resource budget")
        size = len(payload) + _LENGTH.size
        self.charge(size)
        target.write(_LENGTH.pack(len(payload)))
        target.write(payload)
        if path is not None:
            self.paths[path] += size
        return size

    def read(self, source):
        """Read exact legacy frames while enforcing raw length and EOF bounds."""
        while prefix := source.read(_LENGTH.size):
            if self._io_check is not None:
                self._io_check()
            if len(prefix) != _LENGTH.size:
                raise ValueError("source fact spool prefix is truncated")
            size = _LENGTH.unpack(prefix)[0]
            if not 0 < size <= self.limits.max_record_bytes:
                raise SourceFactBudgetExceeded("source fact spool frame exceeds its resource budget")
            payload = source.read(size)
            if len(payload) != size:
                raise ValueError("source fact spool record is truncated")
            yield _unpack(payload)

    def write(self, values):
        """Write frames with cancellation and charge-before-write accounting."""
        path = self.path()
        values = iter(values)
        try:
            with path.open("xb") as target:
                for value in values:
                    self.frame(target, value, path)
        finally:
            close = getattr(values, "close", None)
            if close is not None:
                close()
        return path

    def rows(self, path):
        """Decode framed rows from an existing accounted spool."""
        with path.open("rb") as source:
            yield from self.read(source)

    def remove(self, path):
        """Remove one owned spool and release its exact recorded charge."""
        size = self.paths[path]
        path.unlink()
        self._confirm_removed(path)
        self.used -= size
        self.paths.pop(path)
        self._release_entry()

    def close(self):
        """Remove every owned work path and release its parent budget charge."""
        observed = self.root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (
            self.identity.st_dev,
            self.identity.st_ino,
        ):
            raise ValueError("source fact work custody differs")
        shutil.rmtree(self.root)
        self._confirm_removed(self.root)
        self._entry_bytes = 0

    def _confirm_removed(self, path):
        _require_removed(path)


def _require_removed(path):
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise ValueError("source fact removal is unconfirmed; its entry reservation remains")


class _Replay:
    """One group can spill; no list of all its children is retained."""

    def __init__(self, work):
        self.work, self.size = work, 0
        self.work._reserve_entry()
        self.file = tempfile.SpooledTemporaryFile(max_size=work.limits.memory_bytes, mode="w+b", dir=work.root)

    def append(self, value):
        """Append one replay record through the bounded work writer."""
        self.size += self.work.frame(self.file, value)

    def rows(self):
        """Reopen replay rows using the original bounded frame reader."""
        self.file.seek(0)
        yield from self.work.read(self.file)

    def close(self):
        """Remove this replay scratch and release its exact accounted bytes."""
        if not self.file.closed:
            self.file.close()
            if not self.file.closed:
                raise ValueError("source fact replay close is unconfirmed; its reservation remains")
            self.work._release_entry(self.size)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


def _sort_entry(work, value):
    check = work._io_check or getattr(work, "current", None)
    if check is not None:
        check()
    raw = _pack(value)
    if len(raw) > work.limits.max_record_bytes:
        raise SourceFactBudgetExceeded("source fact singleton exceeds the resource budget")
    compressed = zstd.compress(
        raw,
        options={zstd.CompressionParameter.compression_level: 3, zstd.CompressionParameter.checksum_flag: True},
    )
    stored = (_COMPRESSED_SORT, len(raw), compressed)
    if check is not None:
        check()
    return stored if len(_pack(stored)) < len(raw) else value


def _write_sort_run(work, values):
    values = iter(values)
    try:
        return work.write(_sort_entry(work, value) for value in values)
    finally:
        close = getattr(values, "close", None)
        if close is not None:
            close()


def _decoded_sort_entry(work, entry):
    if type(entry) is tuple and len(entry) == 2:
        return entry
    if (
        type(entry) is not tuple
        or len(entry) != 3
        or entry[0] != _COMPRESSED_SORT
        or type(entry[1]) is not int
        or not 0 < entry[1] <= work.limits.max_record_bytes
        or type(entry[2]) is not bytes
    ):
        raise ValueError("source fact compressed sort bounds differ")
    decoded = _unpack(_decode_sort_frame(work, entry[1], entry[2]))
    if type(decoded) is not tuple or len(decoded) != 2:
        raise ValueError("source fact compressed sort record differs")
    check = work._io_check or getattr(work, "current", None)
    if check is not None:
        check()
    return decoded


def _decode_sort_frame(work, raw_size, frame):
    """Decode one sort frame with the original window, checksum and EOF checks."""
    check = work._io_check or getattr(work, "current", None)
    if check is not None:
        check()
    window_log = min(31, max(10, (raw_size - 1).bit_length()))
    decoder = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: window_log})
    try:
        try:
            raw = decoder.decompress(frame, max_length=raw_size + 1)
        except zstd.ZstdError as error:
            raise ValueError("source fact compressed sort is malformed") from error
        if len(raw) != raw_size or not decoder.eof or decoder.unused_data:
            raise ValueError("source fact compressed sort is incomplete or oversized")
        return raw
    finally:
        del decoder


def _sort_rows(work, path):
    rows = work.rows(path)
    try:
        for entry in rows:
            yield _decoded_sort_entry(work, entry)
    finally:
        rows.close()


def _merged(work, paths):
    readers = [_sort_rows(work, path) for path in paths]
    try:
        yield from heapq.merge(*readers, key=lambda row: row[0])
    finally:
        for reader in readers:
            reader.close()


def _route_run(work, paths, route_key, native):
    if native is None:
        return _write_sort_run(work, _merged(work, paths))
    from process.custom_import.family_shard_route_merge import write_merged

    return write_merged(work, paths, route_key, native[1])


def _coalesce_sort_runs(work, paths, route_key=None, native=None):
    while len(paths) > 8:
        merged_paths = []
        for offset in range(0, len(paths), 8):
            group = paths[offset : offset + 8]
            merged_paths.append(_route_run(work, group, route_key, native))
            for path in group:
                work.remove(path)
        paths = merged_paths
    return paths


def _sort_backend(route_key):
    if route_key is None:
        return None
    if route_key not in {"raw", "family"}:
        raise ValueError("route sort key differs")
    from process.custom_import.family_shard_route_merge import backend

    return backend()


def _sort_batch(work, batch, route_key, native):
    if native is not None:
        from process.custom_import.family_shard_route_merge import write_initial

        return write_initial(work, batch, route_key, native[0])
    batch.sort(key=lambda row: row[0])
    return _write_sort_run(work, ((order, _unpack(payload)) for order, payload in batch))


def _sorted(work, entries, key, *, route_key=None):
    """Fixed eight-way merge; only a resource-bounded run sorts in memory."""
    levels, paths, batch = [], [], []
    size = 0
    entries = iter(entries)
    native = _sort_backend(route_key)

    def add_run(path, level=0):
        """Merge full eight-way levels while retaining stable order and byte charges."""
        while True:
            if level == len(levels):
                levels.append([])
            levels[level].append(path)
            if len(levels[level]) < 8:
                return
            paths = levels[level]
            path = _route_run(work, paths, route_key, native)
            for source in paths:
                work.remove(source)
            levels[level] = []
            level += 1

    def flush():
        """Sort the bounded batch and persist it as one accounted intermediate run."""
        path = _sort_batch(work, batch, route_key, native)
        batch.clear()
        add_run(path)

    try:
        for source_value in entries:
            order = key(source_value)
            packed = _pack((order, source_value))
            charge = 2 * len(packed) + 1024
            if batch and size + charge > work.limits.memory_bytes:
                flush()
                batch, size = [], 0
            batch.append((order, packed if native is not None else _pack(source_value)))
            size += charge
        if batch:
            flush()
        batch.clear()
        paths = [path for level in levels for path in level]
        paths = _coalesce_sort_runs(work, paths, route_key, native)
        merged_paths = _merged(work, paths)
        try:
            for _order, source_value in merged_paths:
                yield source_value
        finally:
            merged_paths.close()
    finally:
        close = getattr(entries, "close", None)
        if close is not None:
            close()
        for path in itertools.chain(paths, itertools.chain.from_iterable(levels)):
            if path in work.paths and path.exists():
                work.remove(path)


def _coordinate(record):
    return admission.occurrence_order(record)


def _raw_order(encoded):
    record = decode_record(encoded)
    if record.raw_parent_key is None:
        return 0, b"", _coordinate(record), 0
    return 1, record.raw_parent_key[1], (0 if record.collection_slot == 0 else 1, 0), _coordinate(record)


def _raw_group(encoded):
    record = decode_record(encoded)
    return (None, _coordinate(record)) if record.raw_parent_key is None else (record.raw_parent_key[1],)


def _encoded_source_records(records, expected_records):
    """Encode one complete source stream and require its original EOF census."""
    count = 0
    for record in records:
        count += 1
        yield encode_record(record)
    if count != expected_records:
        raise ValueError("complete source fact occurrence count differs")


def _replay_raw_group(group, source_values):
    roots = 0
    first_root = None
    first_by_kind, typed_by_raw = {}, {}
    for source_value in source_values:
        source_record = decode_record(source_value)
        kind = int(source_record.collection_slot != 0)
        raw = None if source_record.raw_parent_key is None else source_record.raw_parent_key[0]
        if raw is not None:
            if kind in first_by_kind and first_by_kind[kind] != raw:
                raise ValueError("custom_import_build_structure_mismatch: raw key digest collision")
            first_by_kind.setdefault(kind, raw)
            if source_record.typed_parent_key is not None:
                if raw in typed_by_raw and typed_by_raw[raw] != source_record.typed_parent_key:
                    raise ValueError("raw source equality has conflicting typed identities")
                typed_by_raw[raw] = source_record.typed_parent_key
        if kind == 0 and raw is not None:
            roots += 1
            if first_root is None or _coordinate(source_record) < first_root:
                first_root = _coordinate(source_record)
        group.append(source_value)
    return roots, first_root, first_by_kind, typed_by_raw


def _resolved_raw_group(group, roots, first_root, first_by_kind, typed_by_raw):
    for source_value in group.rows():
        source_record = decode_record(source_value)
        raw = None if source_record.raw_parent_key is None else source_record.raw_parent_key[0]
        typed = source_record.typed_parent_key or typed_by_raw.get(raw)
        is_parent_raw_different = first_root is not None and raw != first_by_kind[0]
        yield typed, source_value, roots, first_root, is_parent_raw_different


def _routed_records(work, records, expected_records, *, route_merge=False):
    entries = _encoded_source_records(records, expected_records)
    ordered = _sorted(work, entries, _raw_order, route_key="raw") if route_merge else _sorted(work, entries, _raw_order)
    try:
        for _key, values in itertools.groupby(ordered, _raw_group):
            with _Replay(work) as group:
                roots, first_root, first_by_kind, typed_by_raw = _replay_raw_group(group, values)
                yield from _resolved_raw_group(group, roots, first_root, first_by_kind, typed_by_raw)
    finally:
        ordered.close()


def _family_order(row):
    typed, encoded, *_facts = row
    record = decode_record(encoded)
    return (1, b"", "") + (_coordinate(record),) if typed is None else (0, typed[1], typed[0]) + (_coordinate(record),)


def _family_key(row):
    return row[0]


def _base_facts(row, typed_roots, *, child=None, membership=None):
    _typed, _encoded, raw_count, parent, differs = row
    count, last, payload_differs, collision = (0, None, None, False) if child is None else child
    return admission.SourceAdmissionFacts(
        raw_count,
        typed_roots,
        parent,
        parent_raw_differs=differs,
        child_count=count,
        collapse_identical=False,
        last_child=last,
        payload_differs_from_last=payload_differs,
        child_key_collision=collision,
        membership_problem=membership,
    )


def _valid_children(group):
    for row in group.rows():
        record = decode_record(row[1])
        if record.collection_slot and record.rejection_code is None:
            yield row


def _child_order(row):
    record = decode_record(row[1])
    return (
        record.collection_slot,
        codec.digest_text("child-key", record.child_key),
        record.raw_parent_key[1],
        record.source_ordinal,
    )


def _child_key(row):
    record = decode_record(row[1])
    return record.collection_slot, codec.digest_text("child-key", record.child_key)


def _raw_child_deltas(work, raw_children, first, definition, fields, collection_by_slot):
    first_row = next(raw_children)
    second_row = next(raw_children, None)
    if second_row is None:
        # A singleton cannot disagree with another payload.
        source_record = decode_record(first_row[1])
        coordinate = _coordinate(source_record)
        yield coordinate, (1, coordinate, False, source_record.child_key != first[1])
        return
    with _Replay(work) as duplicate:
        count = 0
        last = None
        for source_fact in itertools.chain((first_row, second_row), raw_children):
            count += 1
            source_record = decode_record(source_fact[1])
            child_payload = codec.record_payload(
                fields[collection_by_slot[source_record.collection_slot]],
                source_record.normalized_values(definition, definition.source_streams[source_record.stream_slot - 1]),
            )
            last = _coordinate(source_record), child_payload
            duplicate.append((source_fact, child_payload))
        for source_fact, child_payload in duplicate.rows():
            source_record = decode_record(source_fact[1])
            yield (
                _coordinate(source_record),
                (count, last[0], child_payload != last[1], source_record.child_key != first[1]),
            )


def _child_deltas(work, group, definition):
    fields = codec.fields_by_collection(definition)
    collection_by_slot = {index: collection.name for index, collection in enumerate(definition.child_collections, 1)}
    ordered = _sorted(work, _valid_children(group), _child_order)
    try:
        for _key, children in itertools.groupby(ordered, _child_key):
            with _Replay(work) as same_key:
                first = None
                for source_fact in children:
                    source_record = decode_record(source_fact[1])
                    candidate = _coordinate(source_record), source_record.child_key
                    first = candidate if first is None or candidate[0] < first[0] else first
                    same_key.append(source_fact)
                for _raw, raw_children in itertools.groupby(
                    same_key.rows(), lambda source_fact: decode_record(source_fact[1]).raw_parent_key[1]
                ):
                    yield from _raw_child_deltas(work, raw_children, first, definition, fields, collection_by_slot)
    finally:
        ordered.close()


def _initial_facts(work, group, definition, typed_roots):
    deltas = _sorted(work, _child_deltas(work, group, definition), lambda row: row[0])
    delta = next(deltas, None)
    try:
        for row in group.rows():
            record = decode_record(row[1])
            order = _coordinate(record)
            if delta is not None and delta[0] < order:
                raise ValueError("source child facts have no occurrence")
            child = None
            if delta is not None and delta[0] == order:
                child = delta[1]
                delta = next(deltas, None)
            facts = _base_facts(row, typed_roots, child=child)
            facts = replace(
                facts,
                collapse_identical=definition.source_streams[record.stream_slot - 1].duplicate_policy
                == "collapse_identical",
            )
            yield row, asdict(facts)
        if delta is not None:
            raise ValueError("source child facts have an unmatched suffix")
    finally:
        deltas.close()


def _membership_candidates(initial, definition):
    for source_row, fields in initial.rows():
        source_record = decode_record(source_row[1])
        facts = admission.SourceAdmissionFacts(**fields)
        if (
            not source_record.collection_slot
            or source_record.child_key is None
            or admission.primary_code(source_record, facts) is not None
        ):
            continue
        name = definition.child_collections[source_record.collection_slot - 1].name
        cells_by_field = {cell["field"]: cell["value"] for cell in json.loads(source_record.child_key)["fields"]}
        for ordinal, relation in enumerate(definition.child_memberships):
            if relation.inner_collection != name:
                continue
            yield source_record, ordinal, relation, cells_by_field


def _membership_request_rows(candidates, definition, slot_by_collection):
    for source_record, ordinal, relation, cells_by_field in candidates:
        outer = definition.collections_by_name[relation.outer_collection]
        try:
            values_by_field = {}
            for outer_name, inner_name in relation.key_mapping:
                cell = cells_by_field[inner_name]
                if cell["state"] != "value" or cell.get("type") not in ("string", "integer"):
                    raise ValueError("membership key state differs")
                values_by_field[outer_name] = cell["value"]
            expected = codec.key_document(outer.child_key, definition.fields_by_id, values_by_field)
        except KeyError, TypeError, ValueError:
            yield (
                None,
                _coordinate(source_record),
                ordinal,
                "custom_import_build_structure_mismatch: membership key differs",
            )
            continue
        yield (
            (slot_by_collection[relation.outer_collection], codec.digest_text("child-key", expected)),
            _coordinate(source_record),
            ordinal,
            expected,
        )


def _member_requests(initial, definition):
    slot_by_collection = {collection.name: index for index, collection in enumerate(definition.child_collections, 1)}
    candidates = _membership_candidates(initial, definition)
    try:
        yield from _membership_request_rows(candidates, definition, slot_by_collection)
    finally:
        candidates.close()


def _available_keys(group):
    for row in _valid_children(group):
        record = decode_record(row[1])
        yield _child_key(row), _coordinate(record), record.child_key


def _membership_problems(work, group, initial, definition):
    if not definition.child_memberships:
        return
    available = _sorted(work, _available_keys(group), lambda row: (row[0], row[1]))
    requests = _sorted(
        work,
        _member_requests(initial, definition),
        lambda row: (int(row[0] is not None), row[0] or (0, b""), row[1], row[2]),
    )
    peer = next(available, None)
    last_key = None
    first_peer = None
    try:
        for key, occurrence, ordinal, expected in requests:
            if key is None:
                yield occurrence, ordinal, expected
                continue
            while peer is not None and peer[0] < key:
                peer = next(available, None)
            if key != last_key:
                first_peer = None if peer is None or peer[0] != key else peer[2]
                last_key = key
            if first_peer is None:
                yield occurrence, ordinal, "child_membership_missing"
            elif first_peer != expected:
                yield occurrence, ordinal, "custom_import_build_structure_mismatch: membership key digest collision"
    finally:
        available.close()
        requests.close()


def _complete_facts(work, group, initial, definition):
    problems = _sorted(work, _membership_problems(work, group, initial, definition), lambda row: (row[0], row[1]))
    first_problems = (next(values) for _key, values in itertools.groupby(problems, lambda row: row[0]))
    problem = next(first_problems, None)
    try:
        for row, fields in initial.rows():
            coordinate = _coordinate(decode_record(row[1]))
            if problem is not None and problem[0] < coordinate:
                raise ValueError("source membership fact has no occurrence")
            if problem is not None and problem[0] == coordinate:
                fields = dict(fields) | {"membership_problem": problem[2]}
                problem = next(first_problems, None)
            yield row, fields
        if problem is not None:
            raise ValueError("source membership facts have an unmatched suffix")
    finally:
        first_problems.close()
        problems.close()


def _child_targets(facts):
    for row, fields in facts.rows():
        record = decode_record(row[1])
        if record.collection_slot:
            decision = admission.decide_occurrence(record, admission.SourceAdmissionFacts(**fields))
            for target in decision.targets:
                if target.occurrence != _coordinate(record):
                    yield target.occurrence, target.event.occurrence, target.event.origin, target.event.code


def _decisions(work, facts):
    targets = _sorted(work, _child_targets(facts), lambda row: (row[0], row[1]))
    first_targets = (next(values) for _key, values in itertools.groupby(targets, lambda row: row[0]))
    target = next(first_targets, None)
    try:
        for row, fields in facts.rows():
            record = decode_record(row[1])
            coordinate = _coordinate(record)
            while target is not None and target[0] < coordinate:
                target = next(first_targets, None)
            earlier = None
            if record.collection_slot == 0 and target is not None and target[0] == coordinate:
                earlier = admission.RejectionTarget(
                    target[0], admission.RejectionEvent(target[2], target[1], target[3])
                )
            decision = admission.decide_occurrence(
                record, admission.SourceAdmissionFacts(**fields), earlier_child_target=earlier
            )
            yield row, fields, asdict(decision)
    finally:
        first_targets.close()
        targets.close()


def _decision_from_document(document):
    event = None if document["event"] is None else admission.RejectionEvent(**document["event"])
    targets = tuple(
        admission.RejectionTarget(item["occurrence"], admission.RejectionEvent(**item["event"]))
        for item in document["targets"]
    )
    return admission.AdmissionDecision(
        document["occurrence"], document["code"], document["error_code"], event, targets, document["fatal_problem"]
    )


class PreparedSourceFamily:
    """Provisional complete facts for ONE typed key; consume while its context lives.

    Root selection and all-source event/ID allocation belong to the owner. No
    caller may publish before every family and the complete source reach EOF.
    """

    def __init__(self, work, definition, typed_key, group):
        self.work, self.definition, self.typed_key = work, definition, typed_key
        self.group = group
        self.completed = None
        self.source_root = self.source_facts = self.root_resolution = None
        self.candidate_errors = 0
        self.fatal_problem = None
        self.fatal_order = None
        try:
            self._prepare()
        except BaseException:
            self.close()
            raise

    def _prepare(self):
        typed_roots = 0
        for source_row in self.group.rows():
            source_record = decode_record(source_row[1])
            if source_record.collection_slot == 0 and source_record.typed_parent_key is not None:
                typed_roots += 1
                if self.source_root is None or _coordinate(source_record) < _coordinate(self.source_root):
                    self.source_root = source_record
        with _Replay(self.work) as initial, _Replay(self.work) as facts:
            for source_row in _initial_facts(self.work, self.group, self.definition, typed_roots):
                initial.append(source_row)
            for source_row in _complete_facts(self.work, self.group, initial, self.definition):
                facts.append(source_row)
            self.completed = _Replay(self.work)
            try:
                for source_row, fields, decision in _decisions(self.work, facts):
                    self.completed.append((source_row, fields, decision))
                    self.candidate_errors += int(decision["error_code"] in admission._CANDIDATE_ERRORS)
                    if self.fatal_problem is None and decision["fatal_problem"] is not None:
                        self.fatal_problem = decision["fatal_problem"]
                        self.fatal_order = decision["occurrence"]
                    if self.source_root is not None and _coordinate(decode_record(source_row[1])) == _coordinate(
                        self.source_root
                    ):
                        self.source_facts = admission.SourceAdmissionFacts(**fields)
            except BaseException:
                self.completed.close()
                raise
        if self.source_root is not None:
            root = _coordinate(self.source_root)
            for _record, _facts, decision in self.rows():
                for source_target in decision.targets:
                    if source_target.occurrence == root and (
                        self.root_resolution is None
                        or source_target.event.occurrence < self.root_resolution.event.occurrence
                    ):
                        self.root_resolution = source_target

    def rows(self):
        """Decode completed facts and decisions without changing occurrence order."""
        for row, fields, decision in self.completed.rows():
            yield decode_record(row[1]), admission.SourceAdmissionFacts(**fields), _decision_from_document(decision)

    def source_events(self):
        """Yield original source rejection events from completed family facts."""
        for record, _facts, _decision in self.rows():
            event = admission.initial_rejection(record)
            if event is not None:
                yield event

    def admission_events(self):
        """Yield admission-origin events from the completed immutable family facts."""
        for _record, _facts, decision in self.rows():
            if decision.event is not None and decision.event.origin == "admission":
                yield decision.event

    def selection(self, *, has_base, complete_scope):
        """Resolve this completed family only after all rejection guards succeed."""
        if self.fatal_problem is not None:
            raise ValueError(self.fatal_problem)
        if self.candidate_errors:
            raise admission.CandidateRejected("source candidate has occurrence errors")
        return admission.family_selection(
            self.source_root,
            self.root_resolution,
            source_facts=self.source_facts,
            has_base=has_base,
            refresh_mode=self.definition.refresh_mode,
            complete_scope=complete_scope,
        )

    def children(self, collection):
        """Supply fresh accepted children in the unchanged codec document order."""
        if collection not in self.definition.collections_by_name:
            raise ValueError("source family collection differs")
        if self.source_root is None or self.root_resolution is not None:
            return
        if self.candidate_errors or self.fatal_problem is not None:
            raise ValueError("source family facts are rejected")
        slot = next(
            index
            for index, child_collection in enumerate(self.definition.child_collections, 1)
            if child_collection.name == collection
        )
        fields = codec.fields_by_collection(self.definition)[collection]

        def selected():
            """Yield accepted child bytes in the existing deterministic codec order."""
            for record, facts, decision in self.rows():
                if record.collection_slot != slot or decision.code is not None:
                    continue
                if facts.collapse_identical and facts.last_child != _coordinate(record):
                    continue
                payload = codec.record_payload(
                    fields,
                    record.normalized_values(self.definition, self.definition.source_streams[record.stream_slot - 1]),
                )
                yield (
                    (codec.digest_text("child-key", record.child_key), record.child_key, payload),
                    encode_record(record),
                )

        ordered = _sorted(self.work, selected(), lambda row: row[0])
        try:
            for _key, encoded in ordered:
                yield decode_record(encoded)
        finally:
            ordered.close()

    def close(self):
        """Release completed fact scratch while preserving the owning work budget."""
        if self.completed is not None:
            self.completed.close()


def _prepared_families(work, definition, ordered):
    previous = None
    try:
        for typed, rows in itertools.groupby(ordered, _family_key):
            if typed is not None:
                if previous is not None and previous[1] == typed[1] and previous[0] != typed[0]:
                    raise ValueError("source typed root key digest collision")
                previous = typed
            with _Replay(work) as group:
                for row in rows:
                    group.append(row)
                family = PreparedSourceFamily(work, definition, typed, group)
                try:
                    yield family
                finally:
                    family.close()
    finally:
        ordered.close()


@contextmanager
def prepare_source_families(records, definition, *, root, register_cleanup, limits, expected_records):
    """Reconcile complete raw evidence before grouping typed families.

    Results remain provisional until complete EOF and all owner checks. The
    sharded owner shares this same family reducer after its global routing pass.
    """
    if type(limits) is not SourceFactLimits or type(expected_records) is not int or expected_records < 0:
        raise ValueError("source fact input bounds differ")
    work = _Work(root, limits, register_cleanup)
    ordered = _sorted(work, _routed_records(work, records, expected_records), _family_order)
    iterator = _prepared_families(work, definition, ordered)
    try:
        yield iterator
    finally:
        try:
            iterator.close()
        finally:
            work.close()
