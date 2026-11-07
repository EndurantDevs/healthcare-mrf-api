# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original SOURCE common rows and exact native dictionary COPY evidence.

Only SOURCE initial rejections are emitted here. ADMISSION extra events still
need their complete ordered audit: SQL uses evidence '{}', numbers them after
ALL SOURCE initial rejections, and stores NULLIF(collection_slot,0). Retained
origins, membership, winners and owner verification remain separate boundaries.
"""

import asyncio
import hashlib
import os
import re
from contextlib import closing
from dataclasses import astuple, dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType

import msgpack
from sqlalchemy import select

from db.models.custom_import import CustomImportExecution
from process.custom_import.build_source import _drain_cleanup, _page_session
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer, _sha256
from process.custom_import.family_block_store import _candidate, _control_schema, _execute
from process.custom_import.family_block_validate import check_key_dictionary
from process.custom_import.family_shard_allocate import (
    SourceAllocationLimits,
    _checked_source,
    _checked_source_provenance,
    read_source_allocations,
)
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_output import FamilyShardOutput, _input_iterator, _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.runner_codec import root_key_contract_hash
from process.custom_import.runner_graph import entity_value_digest

_ROLES = ("root_record", "entity_binding", "pack", "rejection", "key_dictionary")
_CUSTODY = "custom-import/source-common-custody/v1"


@dataclass(frozen=True)
class SourceCommonContext:
    request: object
    layout: object
    source: object

    def __post_init__(self):
        _checked_source_provenance(self.request, self.source.source)
        if self.request.definition.digest != self.layout.definition.digest:
            raise FamilyBlockUnavailable("common COPY definition differs")
        tables = self.layout.tables(self.source.snapshot_family_id)
        object.__setattr__(
            self, "columns_by_role", MappingProxyType({role: tuple(tables[role].c.keys()) for role in _ROLES})
        )


def _cells(context, role, values_by_name):
    columns = context.columns_by_role[role]
    if set(columns) != set(values_by_name):
        raise FamilyBlockUnavailable("common COPY physical columns differ")
    return tuple(values_by_name[name] for name in columns)


def _producer(context):
    request = context.request
    return dict(
        execution_id=request.execution_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
    )


def _root_evidence(roots_by_id, root_id, key):
    evidence = root_id, *key
    if root_id in roots_by_id and roots_by_id[root_id] != evidence:
        raise FamilyBlockUnavailable("common root identity has inconsistent exact keys")
    roots_by_id[root_id] = evidence


def _source_rows(context, stamp, counts_by_kind, check=None):
    producer = _producer(context)
    capture = dict(context.source.source.scope[0])["capture_bundle_id"]
    with closing(read_source_allocations(context.source, check=check)) as packs:
        for pack, pack_id, root_ids, revision_ids, rejection_ids in packs:
            counts_by_kind["packs"] += 1
            rejection_ordinals = pack.initial_rejection_ordinals
            pack_by_name = dict(
                producer,
                pack_id=pack_id,
                stream_slot=pack.stream_slot,
                pack_ordinal=pack.pack_ordinal,
                capture_bundle_id=capture,
                record_count=sum(revision_id is not None for revision_id in revision_ids),
                pack_sha256=pack.pack_sha256,
                created_at=stamp,
            )
            yield "pack", _pack_row(_cells(context, "pack", pack_by_name))
            roots_by_id = {}
            for index, (source_record, landing) in enumerate(zip(pack.records, pack.landing.records, strict=True)):
                counts_by_kind["records"] += 1
                if source_record.typed_parent_key is not None:
                    _root_evidence(roots_by_id, root_ids[index], source_record.typed_parent_key)
                if rejection_ids[index] is not None:
                    rejection_by_name = dict(
                        producer,
                        rejection_id=rejection_ids[index],
                        rejection_ordinal=rejection_ordinals[index],
                        pack_id=pack_id,
                        root_key_sha256=landing.rejection_root_key_sha256,
                        canonical_root_key=landing.rejection_root_key_canonical,
                        collection_slot=pack.collection_slot or None,
                        source_ordinal=landing.source_ordinal,
                        code=landing.rejection_code,
                        field_slot=None,
                        canonical_evidence=landing.canonical_evidence,
                        created_at=stamp,
                    )
                    yield "rejection", _pack_row(_cells(context, "rejection", rejection_by_name))
                    counts_by_kind["rejections"] += 1
            yield from (("root_request", evidence) for evidence in roots_by_id.values())
    if (counts_by_kind["records"], counts_by_kind["packs"], counts_by_kind["rejections"]) != (
        context.source.source.counts[0],
        context.source.source.counts[1],
        context.source.source.counts[4],
    ):
        raise FamilyBlockUnavailable("common SOURCE EOF census differs")


def _requests(work, path):
    with closing(work.rows(path)) as rows:
        for role, payload in rows:
            if role == "root_request":
                yield payload


def _unique_roots(work, path):
    previous = None
    with closing(_sorted(work, _requests(work, path), lambda row: row)) as rows:
        for row in rows:
            if previous is not None and row[0] == previous[0]:
                if row != previous:
                    raise FamilyBlockUnavailable("common root dictionary identity conflicts")
                continue
            previous = row
            yield row


def _unique_entities(work, values):
    with closing(_sorted(work, _input_iterator(values), lambda identity: identity)) as rows:
        previous = None
        for identity in rows:
            _integer(identity, "selected entity identity")
            if identity != previous:
                yield identity
            previous = identity


def _query_rows(values):
    with closing(_input_iterator(values)) as rows:
        for digest, canonical, rank, variant in rows:
            yield rank, variant, digest, canonical


def _batches(work, path, limits, *, root):
    batch, size = [], 0
    with closing(work.rows(path)) as rows:
        for row in rows:
            charge = len(_pack_row(row)) + (128 if root else 96)
            if charge > limits.maximum_batch_bytes:
                raise FamilyBlockResourceLimit("common metadata singleton exceeds the lookup budget")
            if batch and (len(batch) == limits.maximum_batch_records or size + charge > limits.maximum_batch_bytes):
                yield tuple(batch)
                batch, size = [], 0
            batch.append(row)
            size += charge
        if batch:
            yield tuple(batch)


async def _lookup(session_factory, context, role=None, requests=()):
    request, allocated_source = context.request, context.source
    capture = _checked_source(request, allocated_source.source)
    async with _page_session(session_factory, request, None) as (session, _):
        await _candidate(session, request, allocated_source.snapshot_family_id, context.layout, {"loading"})
        actual = (
            await _execute(
                session,
                select(CustomImportExecution.capture_bundle_id, CustomImportExecution.source_binding_revision_id).where(
                    CustomImportExecution.execution_id == request.execution_id
                ),
            )
        ).one()
        if tuple(actual) != (capture["capture_bundle_id"], capture["source_binding_revision_id"]):
            raise FamilyBlockUnavailable("common metadata capture binding differs")
        if role is None:
            stamp = (await _execute(session, "SELECT pg_catalog.transaction_timestamp()", {})).scalar_one()
            if not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp.utcoffset() is None:
                raise FamilyBlockUnavailable("common metadata owner timestamp differs")
            return stamp
        quote, schema = await _control_schema(session)
        table = f"{quote.quote_schema(schema)}.{quote.quote('custom_import_' + role)}"
        parameter_by_name = {"dataset": request.dataset_id}
        if role == "root_record":
            parameter_by_name.update(
                ids=[native_row[0] for native_row in requests],
                keys=[native_row[1] for native_row in requests],
                hashes=[native_row[2] for native_row in requests],
                contract=root_key_contract_hash(request.definition),
            )
            statement = (
                "WITH requested AS (SELECT * FROM unnest(CAST(:ids AS bigint[]),CAST(:keys AS text[]),"
                "CAST(:hashes AS bytea[])) WITH ORDINALITY AS i(id,key,hash,ordinality)) "
                f"SELECT g.* FROM requested i JOIN {table} g ON g.root_record_id=i.id AND g.dataset_id=:dataset "
                "AND g.key_contract_sha256=:contract AND g.logical_key_sha256=i.hash "
                "AND convert_to(g.canonical_logical_key,'UTF8')=convert_to(i.key,'UTF8') ORDER BY i.ordinality"
            )
        elif role == "entity_binding":
            parameter_by_name["ids"] = list(requests)
            statement = (
                "WITH requested AS (SELECT * FROM unnest(CAST(:ids AS bigint[])) WITH ORDINALITY AS i(id,ordinality)) "
                f"SELECT g.* FROM requested i JOIN {table} g ON g.entity_binding_id=i.id "
                "AND g.dataset_id=:dataset AND g.adapter_id='npi' ORDER BY i.ordinality"
            )
        else:
            raise ValueError("common dictionary role differs")
        native_rows = (await _execute(session, statement, parameter_by_name)).mappings().all()
        if len(native_rows) != len(requests):
            raise FamilyBlockUnavailable("common dictionary scope or exact identity differs")
        for expected, native_row in zip(requests, native_rows, strict=True):
            identifier = expected[0] if role == "root_record" else expected
            if native_row["root_record_id" if role == "root_record" else "entity_binding_id"] != identifier:
                raise FamilyBlockUnavailable("common dictionary result order differs")
            if role == "entity_binding" and bytes(native_row["value_sha256"]) != entity_value_digest(
                native_row["canonical_value"]
            ):
                raise FamilyBlockUnavailable("common entity dictionary digest differs")
        return tuple(_cells(context, role, native_row) for native_row in native_rows)


class SourceCommonOutput:
    """Closed replay, native row census and primitive proof inputs; no receipt mint."""

    def __init__(self, replay, proof_inputs):
        self._replay, self._proof_inputs = replay, tuple(sorted(proof_inputs.items()))

    @property
    def proof_inputs(self):
        """Return independent scope/EOF evidence, including the pending admission boundary."""
        return dict(self._proof_inputs)

    @property
    def role_counts(self):
        """Return physical common-role counts without claiming complete candidate coverage."""
        return self._replay.receipt["role_counts"]

    @property
    def sha256(self):
        """Return the completed spool's exact byte digest."""
        return self._replay.sha256

    def batches(self, *, maximum_rows, maximum_bytes):
        """Replay the existing bounded COPY spool with its full custody/byte checks."""
        yield from self._replay.batches(maximum_rows=maximum_rows, maximum_bytes=maximum_bytes)

    def close(self):
        """Remove only this exact owned spool after owner validation or terminal failure."""
        self._replay.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


@dataclass(frozen=True)
class SourceCommonCustody:
    """Original closed common spool, including its observed transaction timestamp."""

    root: str
    directory: tuple
    parent_directory: tuple
    file_stamp: tuple
    sha256: bytes
    proof_inputs: bytes
    receipt: bytes
    shard_id: int

    def __post_init__(self):
        if type(self.root) is not str or not Path(self.root).is_absolute() or "\0" in self.root:
            raise ValueError("SOURCE common custody root differs")
        for stamp, length in ((self.directory, 4), (self.parent_directory, 4), (self.file_stamp, 7)):
            if type(stamp) is not tuple or len(stamp) != length:
                raise ValueError("SOURCE common custody stamp differs")
            for cell in stamp:
                _integer(cell, "SOURCE common custody", zero=True)
        _sha256(self.sha256)
        _integer(self.shard_id, "SOURCE common shard", zero=True)
        if any(type(encoded) is not bytes or not encoded for encoded in (self.proof_inputs, self.receipt)):
            raise ValueError("SOURCE common custody evidence differs")


def source_common_scope(custody, source_root, shard_count):
    """Limit adopted common files to this SOURCE tree and the original common shard."""
    if type(custody) is not SourceCommonCustody or custody.shard_id != shard_count:
        raise ValueError("SOURCE common custody shard differs")
    parts = Path(custody.root).relative_to(source_root).parts
    if parts != ("common",) and not (
        len(parts) == 2 and re.fullmatch(r"resume-[0-9a-f]{32}", parts[0]) and parts[1] == "common"
    ):
        raise ValueError("SOURCE common custody is outside its original work root")


def source_common_custody(context, admission, output, receipt, limits, check):
    """Verify and sync the actual closed producer before its first COPY transaction."""
    from process.custom_import.family_shard_control import _common_receipt, _completion_scope
    from process.custom_import.family_shard_source_journal import _directory, _stamp, _sync

    if type(output) is not SourceCommonOutput:
        raise ValueError("SOURCE common custody requires its original producer")
    _completion_scope(context, admission, output)
    expected = _common_receipt(context, output, receipt, limits, check)
    root, path = output._replay._work.root, output._replay.path
    if path != root / "output.mpk" or {entry.name for entry in root.iterdir()} != {"output.mpk"}:
        raise ValueError("SOURCE common closed file census differs")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        stamp = _stamp(os.fstat(descriptor))
        if stamp != _stamp(path.lstat()):
            raise ValueError("SOURCE common opened file custody differs")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _sync(root)
    _sync(root.parent)
    _sync(root.parent.parent)
    return SourceCommonCustody(
        str(root),
        _directory(root),
        _directory(root.parent),
        stamp,
        output.sha256,
        _pack_row(output.proof_inputs),
        _pack_row(expected),
        expected["shard_id"],
    )


def source_common_record(ready_sha256, custody):
    """Bind existing common spool bytes to the protected SOURCE-ready owner record."""
    from process.custom_import.family_shard_checkpoint import OwnerCheckpointRecord

    _sha256(ready_sha256)
    if type(custody) is not SourceCommonCustody:
        raise ValueError("SOURCE common checkpoint custody differs")
    payload = _pack_row((_CUSTODY, ready_sha256, astuple(custody)))
    return OwnerCheckpointRecord(
        "artifact",
        custody.shard_id,
        hashlib.sha256(payload).digest(),
        (("source_common", custody.sha256), ("source_ready", ready_sha256)),
        payload,
    )


def source_common_payload(checkpoint_record, cells):
    """Decode only this exact bounded control envelope; files still need full replay."""
    if type(cells) is not tuple or len(cells) != 3 or cells[0] != _CUSTODY:
        raise ValueError("SOURCE common checkpoint contract differs")
    _sha256(cells[1])
    if type(cells[2]) is not tuple or len(cells[2]) != 8:
        raise ValueError("SOURCE common checkpoint descriptor differs")
    custody = SourceCommonCustody(*cells[2])
    if checkpoint_record != source_common_record(cells[1], custody):
        raise ValueError("SOURCE common checkpoint commitment differs")
    return cells[1], custody


def _common_control(encoded):
    decoded = msgpack.unpackb(
        encoded,
        raw=False,
        use_list=False,
        strict_map_key=True,
        max_array_len=128,
        max_map_len=64,
        max_str_len=4096,
        max_bin_len=64,
    )
    if type(decoded) is not dict:
        raise ValueError("SOURCE common protected evidence shape differs")
    return decoded


def reopen_source_common(context, admission, custody, limits, check):
    """Borrow the original common bytes and timestamp; never call the native clock."""
    from process.custom_import.family_shard_control import _common_receipt, _completion_scope
    from process.custom_import.family_shard_source_journal import _directory, _stamp
    from process.custom_import.family_shard_tasks import _BorrowedWork

    root, path = Path(custody.root), Path(custody.root) / "output.mpk"
    if (
        _directory(root) != custody.directory
        or _directory(root.parent) != custody.parent_directory
        or _stamp(path.lstat()) != custody.file_stamp
        or custody.file_stamp[4] > limits.spool.max_spool_bytes
        or {entry.name for entry in root.iterdir()} != {"output.mpk"}
    ):
        raise ValueError("SOURCE common retained file custody differs")
    work = _BorrowedWork(root, custody.directory[:2], limits.spool)
    work._io_check = check
    receipt = _common_control(custody.receipt)
    replay = FamilyShardOutput(work, path, receipt)
    if replay.sha256 != custody.sha256 or receipt.get("shard_id") != custody.shard_id:
        raise ValueError("SOURCE common retained bytes or receipt differ")
    proof_by_name = _common_control(custody.proof_inputs)
    original_stamp = proof_by_name.get("owner_created_at")
    if type(original_stamp) is not str:
        raise ValueError("SOURCE common original timestamp differs")
    stamp = datetime.fromisoformat(original_stamp)
    if stamp.tzinfo is None or stamp.utcoffset() is None or stamp.isoformat() != original_stamp:
        raise ValueError("SOURCE common original timestamp differs")
    output = SourceCommonOutput(replay, proof_by_name)
    _completion_scope(context, admission, output)
    _common_receipt(context, output, receipt, limits, check)
    if (
        _directory(root) != custody.directory
        or _directory(root.parent) != custody.parent_directory
        or _stamp(path.lstat()) != custody.file_stamp
    ):
        raise ValueError("SOURCE common file custody changed during reopening")
    return output


def _emit(work, target, path, role, row, counts_by_role):
    work.frame(target, (role, _pack_row(row)), path)
    counts_by_role[role] += 1


def _copy_staged(work, stage, target, path, counts_by_role):
    with closing(work.rows(stage)) as rows:
        for role, payload in rows:
            if role != "root_request":
                _emit(work, target, path, role, _unpack_row(payload), counts_by_role)


def _append_queries(work, queries, target, path, counts_by_role):
    with closing(work.rows(queries)) as validation_rows:
        check_key_dictionary(validation_rows)
    with closing(work.rows(queries)) as rows:
        for row in rows:
            _emit(work, target, path, "key_dictionary", row, counts_by_role)


def _finish(work, path):
    ready = work.root / "output.mpk"
    path.rename(ready)
    work.paths[ready] = work.paths.pop(path)
    ready.chmod(0o400)
    with ready.open("rb") as source:
        os.fsync(source.fileno())
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return ready


async def _offload(operation, *arguments):
    pending = asyncio.create_task(asyncio.to_thread(operation, *arguments))
    try:
        return await asyncio.shield(pending)
    except asyncio.CancelledError as failure:

        async def _join():
            await pending

        raise await _drain_cleanup(_join(), failure)


async def _write_dictionaries(session_factory, context, work, target, path, sources, counts_by_role, limits):
    for role, source_path in zip(("root_record", "entity_binding"), sources, strict=True):
        with closing(_batches(work, source_path, limits, root=role == "root_record")) as batches:
            for batch in batches:
                for row in await _lookup(session_factory, context, role, batch):
                    _emit(work, target, path, role, row, counts_by_role)


async def build_source_common(
    session_factory, context, selected_entity_ids, query_keys, *, root, register_cleanup, limits
):
    """Use existing frozen SOURCE artifacts; no new codec, native IDs or transitions.

    CPU file work runs off the coordinator loop. Every native lookup uses fresh
    current authority while producer fields remain the original source fields.
    Empty SOURCE still checks current authority. Retry reuses this exact closed
    spool, including its genuinely observed owner timestamp.
    """
    if type(context) is not SourceCommonContext or type(limits) is not SourceAllocationLimits:
        raise ValueError("common COPY requires explicit source context and resource budgets")
    from process.custom_import.family_shard_base import _read_deadline, _require_deadline

    deadline = _read_deadline(context.request)
    stamp = await _lookup(session_factory, context)
    work = _Scratch(PlanWork(root, limits.spool, register_cleanup, lambda: _require_deadline(deadline)))
    counts_by_kind = dict(records=0, packs=0, rejections=0)
    counts_by_role = dict.fromkeys(_ROLES, 0)
    try:
        stage = await _cpu(work, work.write, _source_rows(context, stamp, counts_by_kind, work.current))
        roots = await _cpu(work, work.write, _unique_roots(work, stage))
        entities = await _cpu(work, work.write, _unique_entities(work, selected_entity_ids))
        queries = await _cpu(work, work.write, _query_rows(query_keys))
        path = work.path()
        with path.open("xb") as copy_output:
            await _write_dictionaries(
                session_factory, context, work, copy_output, path, (roots, entities), counts_by_role, limits
            )
            await _cpu(work, _copy_staged, work, stage, copy_output, path, counts_by_role)
            await _cpu(work, _append_queries, work, queries, copy_output, path, counts_by_role)
            copy_output.flush()
            os.fsync(copy_output.fileno())
        await _lookup(session_factory, context)
        for temporary in (stage, roots, entities, queries):
            work.remove(temporary)
        ready = _finish(work, path)
        capture = _checked_source(context.request, context.source.source)
        proof_by_name = dict(
            capture,
            family_id=context.source.snapshot_family_id,
            source_plan_sha256=context.source.source.sha256.hex(),
            allocation_sha256=context.source.sha256.hex(),
            owner_created_at=stamp.isoformat(),
            source_records=counts_by_kind["records"],
            source_initial_rejections=counts_by_kind["rejections"],
            next_admission_rejection_ordinal=counts_by_kind["rejections"],
            admission_events_pending=True,
            execution_id=context.request.execution_id,
            producing_fence=context.request.fence,
            producing_token_sha256=producer_token_sha256(context.request).hex(),
            complete_scope=context.request.complete_scope,
        )
        replay = await _cpu(work, FamilyShardOutput, work, ready, {"role_counts": counts_by_role})
        return SourceCommonOutput(replay, proof_by_name)
    except BaseException:
        work.close()
        raise
