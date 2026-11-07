# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""External metadata joins and bounded asynchronous source-worker dispatch.

Completed artifacts belong to the owner until closed validation and terminal
cleanup. Neither a task spool nor a returned worker result authorizes sealing.
SOURCE allocations and routed inputs remain independently owned and unchanged.
"""

import asyncio
import hashlib
import itertools
import multiprocessing
import os
import stat
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager, closing
from dataclasses import InitVar, asdict, astuple, dataclass
from pathlib import Path
from types import SimpleNamespace

from process.custom_import.build_source import _drain_cleanup, _source_cleanup_failure
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import ShardOutputProof, _checked_receipt
from process.custom_import.family_shard_allocate import AllocatedSourcePlan, read_source_allocations
from process.custom_import.family_shard_common import _offload
from process.custom_import.family_shard_facts import SourceFactLimits, _sorted, _Work
from process.custom_import.family_shard_output import FamilyShardOutput, _input_iterator, _pack_row, _unpack_row
from process.custom_import.family_shard_owner_lock import OwnerJobLock, adopt_worker_job_lock
from process.custom_import.family_shard_route import RoutedShard
from process.custom_import.family_shard_source_plan import _plan_digest
from process.custom_import.family_shard_worker import (
    SourceShardOutput,
    SourceShardPlan,
    SourceWorkerLimits,
    _key_evidence,
    _layout,
    run_source_shard,
)

_JOINED = "joined.mpk"
_JOINED_CONTRACT = "custom-import/joined-source-attempt/v1"
_WORKER_CUSTODY = SimpleNamespace(owner_lock=None)


def _initialize_owner_job_lock(handle):
    """Retain the inherited lock until this worker process exits, including idle time."""
    if handle is not None:
        _WORKER_CUSTODY.owner_lock = adopt_worker_job_lock(handle)


@dataclass(frozen=True)
class SourceTaskScope:
    """Original owner-request scope; capture EOF never implies complete scope."""

    definition: CustomImportDefinition
    complete_scope: bool
    check: InitVar[object] = None

    def __post_init__(self, check):
        if type(self.definition) is not CustomImportDefinition or type(self.complete_scope) is not bool:
            raise ValueError("source task definition and exact complete-scope flag are required")
        if check is not None and not callable(check):
            raise ValueError("source task cancellation check differs")
        object.__setattr__(self, "_check", check)


@dataclass(frozen=True)
class SourceTaskFile:
    shard: RoutedShard
    name: str
    device: int
    inode: int
    byte_count: int
    sha256: bytes


def _scope(shards, source, definition):
    if type(shards) is not tuple or not 0 < len(shards) <= 4096 or type(source) is not AllocatedSourcePlan:
        raise ValueError("source task inputs require complete bounded descriptors")
    if type(definition) is not CustomImportDefinition:
        raise ValueError("source task definition differs")
    identity_by_name = dict(source.source.scope[0])
    if (identity_by_name["definition_sha256"], identity_by_name["schema_sha256"]) != (
        definition.digest,
        definition.schema_digest,
    ):
        raise ValueError("source task allocation definition differs")
    for ordinal, shard in enumerate(shards):
        if type(shard) is not RoutedShard or (
            shard.shard_id,
            shard.shard_count,
            shard.partition_sha256,
            shard.definition_sha256,
        ) != (ordinal, len(shards), source.source.partition_sha256, definition.digest):
            raise ValueError("source task routed cohort differs")
    if sum(shard.records for shard in shards) != source.source.counts[0]:
        raise ValueError("source task routed SOURCE census differs")
    return identity_by_name


def _pack_entries(pack, pack_id, roots, revisions, query_slot, shard_count, counts_by_kind):
    for index, source_record in enumerate(pack.records):
        counts_by_kind["records"] += 1
        key = source_record.typed_parent_key
        if key is None:
            if revisions[index] is not None:
                raise ValueError("source task revision has no typed parent")
            continue
        shard_id = int.from_bytes(key[1][:8], "big") % shard_count
        if source_record.collection_slot == 0:
            yield "request", 1, (key[1], key[0]), shard_id
        if revisions[index] is None:
            continue
        counts_by_kind["revisions"] += 1
        yield (
            "record",
            shard_id,
            (source_record.stream_slot, source_record.source_ordinal),
            (revisions[index], pack_id, roots[index]),
        )
        if source_record.collection_slot == query_slot:
            digest = pack.landing.records[index].child_key_sha256
            yield "request", 2, (bytes(digest), source_record.child_key), shard_id


def _source_entries(source, definition, shard_count, counts_by_kind, check):
    query_slot = next(
        (
            index
            for index, child in enumerate(definition.child_collections, 1)
            if child.name == definition.query.child_collection
        ),
        None,
    )
    with closing(read_source_allocations(source, check=check)) as packs:
        for pack, pack_id, roots, revisions, _rejections in packs:
            counts_by_kind["packs"] += 1
            yield from _pack_entries(pack, pack_id, roots, revisions, query_slot, shard_count, counts_by_kind)
    if (counts_by_kind["records"], counts_by_kind["packs"], counts_by_kind["revisions"]) != (
        source.source.counts[0],
        source.source.counts[1],
        sum(source.source.counts[2:4]),
    ):
        raise ValueError("source task allocation EOF census differs")


def _dictionary_entries(family_ids, query_keys, maximum_bytes):
    with closing(_input_iterator(family_ids)) as families:
        for key, assignment in families:
            _key_evidence(key, "root-key", maximum_bytes)
            if type(assignment) is not tuple or len(assignment) != 2:
                raise ValueError("source task family identity differs")
            for identifier in assignment:
                _integer(identifier, "task family identity")
            yield 1, (key[1], key[0]), 0, assignment
    with closing(_input_iterator(query_keys)) as queries:
        for digest, text, rank, variant in queries:
            _key_evidence((text, digest), "child-key", maximum_bytes)
            _integer(rank, "task global key rank")
            _integer(variant, "task global key variant")
            yield 2, (digest, text), 0, (rank, variant)


def _join_entries(work, path, family_ids, query_keys):
    yield from _dictionary_entries(family_ids, query_keys, work.limits.max_record_bytes)
    with closing(work.rows(path)) as entries:
        for tag, *cells in entries:
            if tag == "request":
                kind, key, shard = cells
                yield kind, key, 1, shard


def _resolved(entries, counts_by_kind):
    previous, assignment, last_shard = None, None, None
    with closing(entries):
        for kind, key, entry_kind, cell in entries:
            current = kind, key
            if current != previous:
                previous, assignment, last_shard = current, None, None
            if entry_kind == 0:
                if assignment is not None and assignment != cell:
                    raise ValueError("source task global dictionary assignment conflicts")
                assignment = cell
                continue
            if cell == last_shard:
                continue
            last_shard = cell
            if assignment is None:
                counts_by_kind["missing_family" if kind == 1 else "missing_query"] += 1
                continue
            yield cell, kind, (key[1], key[0]) if kind == 1 else key, assignment


def _final_entries(work, path, family_ids, query_keys, counts_by_kind):
    with closing(work.rows(path)) as entries:
        for tag, *cells in entries:
            if tag == "record":
                yield cells[0], 0, cells[1], cells[2]
    ordered = _sorted(work, _join_entries(work, path, family_ids, query_keys), lambda row: row)
    yield from _resolved(ordered, counts_by_kind)


def _unique_cells(rows):
    previous = None
    for _shard, kind, key, assignment in rows:
        if (kind, key) == previous:
            raise ValueError("source task metadata repeats an identity")
        previous = kind, key
        yield kind, key, assignment


def _task_files(work, shards, entries):
    files = []
    with closing(entries):
        groups = iter(itertools.groupby(entries, lambda row: row[0]))
        pending = next(groups, None)
        for shard in shards:
            rows = pending[1] if pending is not None and pending[0] == shard.shard_id else ()
            path = work.write(_unique_cells(rows))
            path.chmod(0o400)
            observed = path.stat()
            with path.open("rb") as source:
                os.fsync(source.fileno())
                digest = _plan_digest(source, work._io_check)
            files.append(SourceTaskFile(shard, path.name, observed.st_dev, observed.st_ino, observed.st_size, digest))
            if pending is not None and pending[0] == shard.shard_id:
                pending = next(groups, None)
        if pending is not None:
            raise ValueError("source task metadata contains a foreign shard")
    return tuple(files)


class PreparedSourceTasks:
    """Closed metadata files; loading a task enforces its own memory budgets."""

    def __init__(self, work, files, source, scope, counts_by_kind):
        self._work, self.files, self.source = work, files, source
        self.definition_canonical = scope.definition.canonical
        self.complete_scope = scope.complete_scope
        self.collection_slots = source.source.collection_slots
        self._counts = dict(counts_by_kind)
        self._closed = False

    @property
    def counts(self):
        """Return provisional metadata-join counters, including unmatched requests."""
        return dict(self._counts)

    def load_plan(self, shard_id, limits):
        """Load one bounded map cohort without retaining other shard maps."""
        if self._closed or type(shard_id) is not int or not 0 <= shard_id < len(self.files):
            raise ValueError("source task descriptor is unavailable")
        if type(limits) is not SourceWorkerLimits:
            raise ValueError("source task worker resource limits differ")
        rows_by_kind = ({}, {}, {})
        size = len(self.definition_canonical.encode("utf-8")) + len(_pack_row(dict(self.collection_slots)))
        count = 0
        with closing(self._rows(self.files[shard_id])) as rows:
            for kind, key, assignment in rows:
                count += 1
                size += len(_pack_row((("record", "family", "query")[kind], key, assignment))) + 8
                if count > limits.maximum_metadata_rows or size > limits.maximum_metadata_bytes:
                    raise FamilyBlockResourceLimit("source task metadata exceeds worker resources")
                rows_by_kind[kind][key] = assignment
        if size > limits.maximum_metadata_bytes:
            raise FamilyBlockResourceLimit("source task metadata exceeds worker resources")
        identity_by_name = dict(self.source.source.scope[0])
        return SourceShardPlan(
            self.files[shard_id].shard,
            self.definition_canonical,
            dict(self.collection_slots),
            identity_by_name["dataset_id"],
            identity_by_name["schema_revision_id"],
            self.source.snapshot_family_id,
            *rows_by_kind,
            self.complete_scope,
        )

    def _rows(self, descriptor):
        root = self._work.root.lstat()
        if not stat.S_ISDIR(root.st_mode) or (root.st_dev, root.st_ino) != (
            self._work.identity.st_dev,
            self._work.identity.st_ino,
        ):
            raise ValueError("source task metadata root custody differs")
        with os.fdopen(os.open(self._work.root / descriptor.name, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
            observed = os.fstat(source.fileno())
            if not stat.S_ISREG(observed.st_mode) or (
                observed.st_dev,
                observed.st_ino,
                observed.st_size,
                stat.S_IMODE(observed.st_mode),
            ) != (descriptor.device, descriptor.inode, descriptor.byte_count, 0o400):
                raise ValueError("source task metadata file custody differs")
            if _plan_digest(source, self._work._io_check) != descriptor.sha256:
                raise ValueError("source task metadata bytes differ")
            source.seek(0)
            yield from self._work.read(source)
            source.seek(0)
            if _plan_digest(source, self._work._io_check) != descriptor.sha256:
                raise ValueError("source task metadata changed during loading")

    def close(self):
        """Remove only the metadata root after its owner finishes using it."""
        if not self._closed:
            self._work.close()
            self._closed = True


def prepare_source_tasks(shards, allocated_source, scope, family_ids, query_keys, *, root, register_cleanup, limits):
    """Externally join observed allocations and owner-assigned global dictionaries.

    Call from a coordinator thread with a stop/deadline check in the parent-only
    scope, and drain that thread before cleanup when cancelling its caller.
    Routed files and original allocations must outlive all worker consumers.
    Missing mappings stay provisional; the worker refuses any missing mapping
    actually needed by an accepted family.
    """
    if type(scope) is not SourceTaskScope:
        raise ValueError("source task preparation requires the original owner request scope")
    definition = scope.definition
    _scope(shards, allocated_source, definition)
    if type(limits) is not SourceFactLimits:
        raise ValueError("source task preparation budgets differ")
    work = _Work(root, limits, register_cleanup, check=scope._check)
    counts_by_kind = dict(records=0, packs=0, revisions=0, missing_family=0, missing_query=0)
    try:
        initial = work.write(_source_entries(allocated_source, definition, len(shards), counts_by_kind, scope._check))
        final = _sorted(
            work, _final_entries(work, initial, family_ids, query_keys, counts_by_kind), lambda row: row[:3]
        )
        files = _task_files(work, shards, final)
        work.remove(initial)
        directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return PreparedSourceTasks(work, files, allocated_source, scope, counts_by_kind)
    except BaseException as error:
        try:
            work.close()
        except BaseException as cleanup:
            raise _source_cleanup_failure(error, cleanup) from cleanup
        raise


def _join_pool(executor, processes, terminate):
    if terminate:
        executor.terminate_workers()
    executor.shutdown(wait=True, cancel_futures=True)
    for process in processes:
        process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join()


@dataclass(frozen=True)
class JoinedSourceAttempt:
    """Closed evidence of one joined attempt; old artifact ownership stays local."""

    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    scope_sha256: bytes
    max_frame_bytes: int


@dataclass(frozen=True)
class SourceOutputCustody:
    """One closed output protected by the owner journal, without invented join evidence."""

    root: str
    directory: tuple
    scope_sha256: bytes
    artifact: tuple

    def __post_init__(self):
        if type(self.root) is not str or not Path(self.root).is_absolute() or "\0" in self.root:
            raise ValueError("protected SOURCE output root differs")
        if type(self.scope_sha256) is not bytes or len(self.scope_sha256) != 32:
            raise ValueError("protected SOURCE output scope differs")
        _output_custody_cells(self.directory, self.artifact)


def _output_custody_cells(directory, artifact):
    if type(artifact) is not tuple or len(artifact) != 7:
        raise ValueError("protected SOURCE output descriptor differs")
    _integer(artifact[0], "protected SOURCE shard", zero=True)
    for identity in (directory, artifact[1], artifact[3]):
        if type(identity) is not tuple or len(identity) != 2:
            raise ValueError("protected SOURCE directory identity differs")
        for value in identity:
            _integer(value, "protected SOURCE directory")
    for identity in (artifact[2], artifact[4]):
        if type(identity) is not tuple or len(identity) != 5:
            raise ValueError("protected SOURCE file identity differs")
        name = identity[0]
        if type(name) is not str or name in ("", ".", "..") or Path(name).name != name:
            raise ValueError("protected SOURCE filename differs")
        for value in identity[1:4]:
            _integer(value, "protected SOURCE file", zero=True)
        if type(identity[4]) is not bytes or len(identity[4]) != 32:
            raise ValueError("protected SOURCE file digest differs")
    names = ("records", "families", "source", "absent", "error", "candidate_errors", "fatal_families")
    if type(artifact[5]) is not tuple or tuple(name for name, _count in artifact[5]) != names:
        raise ValueError("protected SOURCE counters differ")
    for _name, count in artifact[5]:
        _integer(count, "protected SOURCE count", zero=True)
    if type(artifact[6]) is not bytes:
        raise ValueError("protected SOURCE receipt differs")


def source_output_custody(prepared, output, root, limits):
    """Validate and sync the genuine worker's closed files before native checkpointing."""
    root = Path(root).absolute()
    artifact = _closed_artifact(prepared, output, root, limits)
    for path in (output.path, output._output.path):
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    for path in (output._output._work.root, output._work.root, root, root.parent):
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return SourceOutputCustody(str(root), _directory_identity(root), _prepared_sha256(prepared), artifact)


def _prepared_sha256(prepared):
    digest = hashlib.sha256(b"custom-import/source-task-resume-scope/v1\x00")
    source = prepared.source
    digest.update(
        _pack_row(
            (
                source.source.sha256,
                source.sha256,
                source.source.scope,
                source.snapshot_family_id,
                prepared.definition_canonical,
                prepared.complete_scope,
                prepared.collection_slots,
            )
        )
    )
    for descriptor in prepared.files:
        digest.update(_pack_row(astuple(descriptor)))
    return digest.digest()


def _file_identity(path, digest):
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode) or stat.S_IMODE(observed.st_mode) != 0o400:
        raise ValueError("completed source artifact file custody differs")
    return path.name, observed.st_dev, observed.st_ino, observed.st_size, digest


def _directory_identity(path):
    observed = path.lstat()
    if not stat.S_ISDIR(observed.st_mode):
        raise ValueError("completed source artifact directory differs")
    return observed.st_dev, observed.st_ino


class _BorrowedWork:
    def __init__(self, root, identity, limits):
        self.root, self.limits, self._io_check = root, limits, None
        if _directory_identity(root) != identity:
            raise ValueError("borrowed source artifact directory custody differs")
        self.identity = root.lstat()

    def read(self, source):
        """Use the existing frame limits without granting physical cleanup ownership."""
        yield from _Work.read(self, source)

    def close(self):
        """Borrowers can close their view without deleting the original owner's files."""


def _open_file(root, identity):
    name, device, inode, size, digest = identity
    if Path(name).name != name or _file_identity(root / name, digest) != identity:
        raise ValueError("borrowed source artifact file custody differs")
    return root / name


def _reopen_output(root, artifact, limits):
    shard, audit_directory, audit_file, output_directory, output_file, counts, receipt = artifact
    audit_root = root / f"shard-{shard:04d}"
    audit_work = _BorrowedWork(audit_root, audit_directory, limits.audit)
    record_budget = min(
        limits.output.max_spool_bytes, 4 * limits.output.max_record_bytes + limits.output.fragment_bytes + 1024
    )
    output_limits = SourceFactLimits(
        limits.output.max_spool_bytes, record_budget, max(1, limits.output.memory_bytes // 8)
    )
    output_work = _BorrowedWork(audit_root / "output", output_directory, output_limits)
    family = FamilyShardOutput(output_work, _open_file(output_work.root, output_file), _unpack_row(receipt))
    output = SourceShardOutput(audit_work, _open_file(audit_root, audit_file), family, dict(counts))
    if output.sha256 != audit_file[4] or family.sha256 != output_file[4]:
        raise ValueError("borrowed source artifact bytes differ")
    for replay, expected in ((output, audit_file), (family, output_file)):
        opened = replay._identity
        if (opened.st_dev, opened.st_ino, opened.st_size) != expected[1:4]:
            raise ValueError("borrowed source artifact opened custody differs")
    _artifact_census(audit_root, audit_file[0], output_file[0])
    return output


def _artifact_census(root, audit_name, output_name):
    if {path.name for path in root.iterdir()} != {audit_name, "output"} or {
        path.name for path in (root / "output").iterdir()
    } != {output_name}:
        raise ValueError("completed source artifact file census differs")


def _validate_closed_output(prepared, output, shard, limits):
    from process.custom_import.family_shard_admission import _worker_rows

    plan = prepared.load_plan(shard, limits)
    layout, metadata_bytes, metadata_sha256 = _layout(plan, limits)
    with closing(
        _worker_rows(
            SimpleNamespace(current=lambda: None),
            prepared.source,
            layout.definition,
            output,
            len(prepared.files),
            prepared.complete_scope,
        )
    ) as audit_entries:
        first = next(audit_entries, None)
        if (
            first is None
            or first[0] != "worker"
            or first[2]["shard"] != asdict(plan.shard)
            or (first[2]["metadata_bytes"], first[2]["metadata_sha256"]) != (metadata_bytes, metadata_sha256)
        ):
            raise ValueError("completed source native metadata commitment differs")
        deque(audit_entries, maxlen=0)
    if output.status not in {"source_eof", "refused"}:
        raise ValueError("source output has no complete typed worker EOF")
    receipt = _checked_receipt(layout, output.receipt)
    if receipt["shard_id"] != shard:
        raise ValueError("completed source output shard differs")
    proof = ShardOutputProof(layout, shard, receipt["partition_sha256"], plan.shard.records)
    # Verify closed bytes even for a refused audit; SourceShardOutput.batches
    # still bars those bytes from COPY.
    with closing(output._output.batches(maximum_rows=4096, maximum_bytes=limits.output.max_spool_bytes)) as batches:
        for batch in batches:
            for role, cells in batch.items():
                proof.add(role, cells)
    if proof.receipt() != receipt:
        raise ValueError("completed source output receipt differs")


def _validate_output(prepared, output, shard, limits):
    if output.status != "source_eof":
        raise ValueError("refused source output cannot be reused")
    _validate_closed_output(prepared, output, shard, limits)


def _closed_artifact(prepared, output, root, limits):
    shard = output.receipt["shard_id"]
    if type(output) is not SourceShardOutput or output._work.root != root / f"shard-{shard:04d}":
        raise ValueError("completed source output belongs to another attempt")
    if output._output._work.root != output._work.root / "output":
        raise ValueError("completed source family output belongs to another attempt")
    _artifact_census(output._work.root, output.path.name, output._output.path.name)
    _validate_closed_output(prepared, output, shard, limits)
    return (
        shard,
        _directory_identity(output._work.root),
        _file_identity(output.path, output.sha256),
        _directory_identity(output._output._work.root),
        _file_identity(output._output.path, output._output.sha256),
        tuple(output.counts.items()),
        _pack_row(output.receipt),
    )


def _completed_artifact(prepared, output, root, limits):
    if output.status != "source_eof":
        raise ValueError("refused source output cannot be reused")
    return _closed_artifact(prepared, output, root, limits)


def _write_checkpoint(run):
    processes = tuple((process.pid, process.exitcode) for process in run._processes.values())
    if run._running or any(process.is_alive() or process.exitcode is None for process in run._processes.values()):
        raise ValueError("source workers must be joined before checkpoint")
    if any(output.status != "source_eof" for output in run._owned_completed.values()):
        raise ValueError("refused source output cannot be reused")
    scope = _prepared_sha256(run.prepared)
    path = run._work.path()
    with path.open("xb") as checkpoint_output:
        run._work.frame(checkpoint_output, ("header", _JOINED_CONTRACT, scope), path)
        for shard in sorted(run._owned_completed):
            artifact = _completed_artifact(run.prepared, run._owned_completed[shard], run._work.root, run.limits)
            run._work.frame(checkpoint_output, ("shard", artifact), path)
        run._work.frame(checkpoint_output, ("joined", processes, len(run._owned_completed)), path)
        checkpoint_output.flush()
        os.fsync(checkpoint_output.fileno())
    ready = run._work.root / _JOINED
    if ready.exists() or ready.is_symlink():
        raise FileExistsError("source attempt already has a joined checkpoint")
    os.link(path, ready, follow_symlinks=False)
    path.unlink()
    run._work.paths[ready] = run._work.paths.pop(path)
    ready.chmod(0o400)
    with ready.open("rb") as checkpoint_source:
        digest = hashlib.file_digest(checkpoint_source, "sha256").digest()
        os.fsync(checkpoint_source.fileno())
    directory = os.open(run._work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    observed = ready.stat()
    return JoinedSourceAttempt(
        str(run._work.root),
        run._work.identity.st_dev,
        run._work.identity.st_ino,
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        digest,
        scope,
        run._work.limits.max_record_bytes,
    )


def _checkpoint_artifacts(checkpoint, scope, limits):
    if type(checkpoint) is not JoinedSourceAttempt or checkpoint.scope_sha256 != scope:
        raise ValueError("joined source attempt scope differs")
    root = Path(checkpoint.root)
    if _directory_identity(root) != (checkpoint.device, checkpoint.inode):
        raise ValueError("joined source attempt directory custody differs")
    identity = (_JOINED, checkpoint.file_device, checkpoint.file_inode, checkpoint.file_bytes, checkpoint.sha256)
    path = _open_file(root, identity)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as checkpoint_source:
        opened = os.fstat(checkpoint_source.fileno())
        if (opened.st_dev, opened.st_ino, opened.st_size) != identity[1:4]:
            raise ValueError("joined source attempt open custody differs")
        if hashlib.file_digest(checkpoint_source, "sha256").digest() != checkpoint.sha256:
            raise ValueError("joined source attempt bytes differ")
        checkpoint_source.seek(0)
        reader = _BorrowedWork(root, (checkpoint.device, checkpoint.inode), limits)
        entries = reader.read(checkpoint_source)
        if next(entries, None) != ("header", _JOINED_CONTRACT, scope):
            raise ValueError("joined source attempt header differs")
        count, previous, is_joined = 0, -1, False
        for entry in entries:
            if is_joined:
                raise ValueError("joined source attempt has trailing records")
            if entry[0] == "joined":
                if (
                    entry[2] != count
                    or (count and not entry[1])
                    or len(entry[1]) > 8
                    or any(type(pid) is not int or pid <= 0 or type(exitcode) is not int for pid, exitcode in entry[1])
                ):
                    raise ValueError("joined source attempt process evidence differs")
                is_joined = True
                continue
            if entry[0] != "shard" or entry[1][0] <= previous:
                raise ValueError("joined source attempt shard order differs")
            previous, count = entry[1][0], count + 1
            yield root, entry[1]
        if not is_joined:
            raise ValueError("joined source attempt is incomplete")
        checkpoint_source.seek(0)
        if hashlib.file_digest(checkpoint_source, "sha256").digest() != checkpoint.sha256:
            raise ValueError("joined source attempt changed during replay")


def _resume_outputs(prepared, completed, root, limits, owner_lock=None):
    if type(completed) is not tuple or len(completed) > len(prepared.files):
        raise ValueError("completed source attempts require closed descriptors")
    scope, outputs_by_shard, attempt_roots = _prepared_sha256(prepared), {}, set()
    for checkpoint in completed:
        if type(checkpoint) is SourceOutputCustody:
            if type(owner_lock) is not OwnerJobLock or checkpoint.scope_sha256 != scope:
                raise ValueError("protected SOURCE output requires its current owner lock and tasks")
            owner_lock.check()
            previous, current = Path(checkpoint.root), Path(root)
            shard = checkpoint.artifact[0]
            if (
                previous.parent != current.parent
                or not previous.is_relative_to(owner_lock.root)
                or previous == current
                or _directory_identity(previous) != checkpoint.directory
                or not 0 <= shard < len(prepared.files)
                or shard in outputs_by_shard
            ):
                raise ValueError("protected SOURCE output attempt or shard differs")
            output = _reopen_output(previous, checkpoint.artifact, limits)
            _validate_closed_output(prepared, output, shard, limits)
            outputs_by_shard[shard] = output
            continue
        if type(checkpoint) is not JoinedSourceAttempt:
            raise ValueError("completed source attempts require joined descriptors")
        previous = Path(checkpoint.root).resolve()
        current = Path(root).resolve()
        if previous in attempt_roots or previous.is_relative_to(current) or current.is_relative_to(previous):
            raise ValueError("source resume attempt roots must be disjoint")
        attempt_roots.add(previous)
        with closing(_checkpoint_artifacts(checkpoint, scope, prepared._work.limits)) as artifacts:
            for attempt_root, artifact in artifacts:
                shard = artifact[0]
                if type(shard) is not int or not 0 <= shard < len(prepared.files) or shard in outputs_by_shard:
                    raise ValueError("completed source attempt shard repeats or differs")
                output = _reopen_output(attempt_root, artifact, limits)
                _validate_output(prepared, output, shard, limits)
                outputs_by_shard[shard] = output
    return outputs_by_shard


def reopen_source_outputs(prepared, completed, root, limits, owner_lock):
    """Borrow an already complete protected cohort without launching or joining workers."""
    if type(completed) is not tuple or any(type(output) is not SourceOutputCustody for output in completed):
        raise ValueError("frozen SOURCE reopening requires protected closed outputs")
    outputs = _resume_outputs(prepared, completed, root, limits, owner_lock)
    if set(outputs) != set(range(len(prepared.files))):
        raise ValueError("frozen SOURCE reopening requires every original worker output")
    return tuple(outputs[shard] for shard in sorted(outputs))


class SourceTaskRun:
    """At most eight spawned tasks; returned evidence remains caller-owned."""

    def __init__(self, prepared, work, register_cleanup, limits, maximum_workers, borrowed, owner_lock):
        self.prepared, self._work, self._register = prepared, work, register_cleanup
        self.limits, self.maximum_workers = limits, maximum_workers
        self._executor = ProcessPoolExecutor(
            max_workers=maximum_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_initialize_owner_job_lock,
            initargs=(None if owner_lock is None else owner_lock.worker_handle(),),
        )
        self._pending, self._completed, self._processes = {}, dict(borrowed), {}
        self._owned_completed = {}
        self._borrowed = tuple(borrowed)
        self._missing = tuple(shard for shard in range(len(prepared.files)) if shard not in borrowed)
        self._next = 0
        self._loading = None
        self._loading_shard = None
        self._running = True
        self._cleaned = False
        self._checkpoint = None
        self.failures_by_shard = {}
        self.peak_inflight = 0
        self.all_returned = False
        self._iterator = self._outputs()

    @property
    def completed(self):
        """Retain every completed descriptor, including refused and unconsumed results."""
        return tuple(self._completed[key] for key in sorted(self._completed))

    async def _submit(self):
        shard_id = self._missing[self._next]
        self._loading_shard = shard_id
        self._loading = asyncio.create_task(asyncio.to_thread(self.prepared.load_plan, shard_id, self.limits))
        plan = await asyncio.shield(self._loading)
        self._loading = None
        self._loading_shard = None
        root = self._work.root / f"shard-{shard_id:04d}"
        if root.exists() or root.is_symlink():
            raise FileExistsError("source task worker root already exists")
        self._register(root)
        native = self._executor.submit(run_source_shard, plan, str(root), self.limits)
        self._pending[asyncio.wrap_future(native)] = (native, shard_id)
        self.peak_inflight = max(self.peak_inflight, len(self._pending))
        # Capture only handles created by this exact executor; join before cleanup.
        self._processes.update({process.pid: process for process in self._executor._processes.values()})
        self._next += 1

    async def _outputs(self):
        for _index in range(min(self.maximum_workers, len(self._missing))):
            await self._submit()
        for shard in sorted(self._borrowed):
            yield self._completed[shard]
        while self._pending:
            done, _pending = await asyncio.wait(self._pending, return_when=asyncio.FIRST_COMPLETED)
            completed, failure = [], None
            for future in done:
                _native, shard_id = self._pending.pop(future)
                try:
                    output = await future
                except BaseException as error:
                    self.failures_by_shard[shard_id] = type(error).__name__
                    failure = failure or error
                else:
                    self._completed[shard_id] = output
                    self._owned_completed[shard_id] = output
                    completed.append(output)
            if failure is not None:
                raise failure
            for output in completed:
                if self._next < len(self._missing):
                    await self._submit()
                yield output
        self.all_returned = len(self._completed) == len(self.prepared.files)

    def __aiter__(self):
        return self._iterator

    async def stop(self, *, terminate):
        """Join every exact owned process before releasing any task resources."""
        if self._running:
            failure = await _drain_cleanup(self._stop_owned(terminate), None)
            if failure is not None:
                raise failure

    async def _stop_owned(self, terminate):
        if self._running:
            if self._loading is not None:
                try:
                    await self._loading
                except BaseException as error:
                    self.failures_by_shard[self._loading_shard] = type(error).__name__
                self._loading = None
                self._loading_shard = None
            self._processes.update({process.pid: process for process in (self._executor._processes or {}).values()})
            await _offload(_join_pool, self._executor, tuple(self._processes.values()), terminate)
            self._running = False
            for future, (native, shard_id) in self._pending.items():
                try:
                    observed = asyncio.wrap_future(native) if future.cancelled() else future
                    output = await observed
                    self._completed[shard_id] = self._owned_completed[shard_id] = output
                except BaseException as error:
                    self.failures_by_shard[shard_id] = type(error).__name__
            self._pending.clear()

    async def checkpoint(self):
        """Close primitive joined evidence for ONLY this attempt's owned outputs."""
        if self._running or self._cleaned:
            raise ValueError("source workers must be joined and retained before checkpoint")
        if self._checkpoint is None:
            self._checkpoint = await _offload(_write_checkpoint, self)
        return self._checkpoint

    async def cleanup(self):
        """Explicit terminal cleanup; preserve evidence until the owner calls this."""
        if self._running:
            raise ValueError("source task workers must be joined before cleanup")
        if not self._cleaned:
            await _offload(self._remove_owned_outputs)

    def _remove_owned_outputs(self):
        for output in self._owned_completed.values():
            output.close()
        self._work.close()
        self._cleaned = True


@asynccontextmanager
async def run_source_tasks(
    prepared,
    *,
    root,
    register_cleanup,
    worker_limits,
    maximum_workers=8,
    preserve_completed=True,
    completed=(),
    owner_lock=None,
):
    """Keep coordinator heartbeat live and retain completed artifacts after exit.

    On early exit or failure, terminate and join exact owned workers. Resume
    mode preserves the registered job root, including uncertain interrupted
    artifacts; terminal cleanup is explicit. No source closure is inferred.
    """
    if type(prepared) is not PreparedSourceTasks or type(worker_limits) is not SourceWorkerLimits:
        raise ValueError("source task run requires prepared metadata and worker budgets")
    if type(maximum_workers) is not int or not 1 <= maximum_workers <= 8 or type(preserve_completed) is not bool:
        raise ValueError("source task concurrency or preservation mode differs")
    if owner_lock is not None:
        if type(owner_lock) is not OwnerJobLock:
            raise ValueError("source task owner lock differs")
        owner_lock.check()
        if not Path(root).absolute().is_relative_to(owner_lock.root):
            raise ValueError("source task root differs from its owner lock")
    work = _Work(root, prepared._work.limits, register_cleanup)
    try:
        borrowed = await _offload(_resume_outputs, prepared, completed, work.root, worker_limits, owner_lock)
        run = SourceTaskRun(prepared, work, register_cleanup, worker_limits, maximum_workers, borrowed, owner_lock)
    except BaseException:
        work.close()
        raise
    primary = None
    try:
        yield run
    except BaseException as failure:
        primary = failure
        raise
    finally:
        failure = await _drain_cleanup(run._iterator.aclose(), primary)
        failure = await _drain_cleanup(run.stop(terminate=not run.all_returned), failure)
        if not preserve_completed:
            failure = await _drain_cleanup(run.cleanup(), failure)
        if failure is not None and failure is not primary:
            raise failure
