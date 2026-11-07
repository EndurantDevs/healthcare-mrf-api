# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete owned family COPY spools; workers receive no database authority."""

import hashlib
import os
import stat
import struct
from collections.abc import Callable, Mapping
from contextlib import ExitStack, closing
from dataclasses import dataclass, replace
from pathlib import Path

import msgpack

from process.custom_import.family_block_codec import (
    FamilyBlockLimits,
    FamilyBlockResourceLimit,
    encode_family,
    encode_retained_family,
    verify_family,
)
from process.custom_import.family_block_directory import RevisionMembership, iter_revision_memberships
from process.custom_import.family_block_records import FamilyRecord, _integer, _sha256, record_from_wire, record_parts
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields
from process.custom_import.family_block_storage_codec import (
    iter_fragment_directory,
    stored_manifest,
    verify_stored_family,
)
from process.custom_import.family_block_store import ShardOutputProof, _size
from process.custom_import.family_shard_facts import SourceFactLimits, _sorted, _Work
from process.custom_import.family_shard_input_records import _decode_scalar, _encode_scalar

_FRAME = struct.Struct("!QQ")


@dataclass(frozen=True)
class AllocatedFamily:
    """Each child value is a fresh iterator factory, borrowed until this family ends.

    Retained mode requires the caller's completed base/pin/lineage proof. It is
    never selected automatically when a fresh record or hash check fails.
    """

    family_revision_id: int
    root_record_id: int
    entity_binding_id: int
    root: FamilyRecord
    children_by_collection: Mapping
    expected_family_sha256: bytes
    retained: bool = False


@dataclass(frozen=True)
class FamilyOutputWork:
    """Caller-owned output location, resource bounds and cleanup registration."""

    root: Path
    limits: FamilyBlockLimits
    register_cleanup: Callable[[Path], None]
    check: Callable[[], None] | None = None


def _pack_row(row):
    return msgpack.packb(row, use_bin_type=True, default=_encode_scalar)


def _unpack_row(payload):
    return msgpack.unpackb(payload, raw=False, use_list=False, strict_map_key=True, ext_hook=_decode_scalar)


def _input_iterator(values):
    iterator = iter(values)
    try:
        yield from iterator
    finally:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()


def _generation_scope(context, generation, final_plan):
    from process.custom_import.continuation_runtime import producer_token_sha256
    from process.custom_import.family_shard_allocate import _checked_source_provenance as _checked_source
    from process.custom_import.family_shard_common import SourceCommonContext
    from process.custom_import.family_shard_plan import FinalPlanDescriptor
    from process.custom_import.materialization import GenerationIdentity

    if type(context) is not SourceCommonContext:
        raise ValueError("generation membership owner scope differs")
    _checked_source(context.request, context.source.source)
    request = context.request
    expected = (
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        request.fence,
        producer_token_sha256(request),
        request.expected_base_generation_id,
        request.expected_pointer_version,
        request.definition.refresh_mode,
        request.complete_scope,
    )
    if (
        type(generation) is not GenerationIdentity
        or type(final_plan) is not FinalPlanDescriptor
        or (generation.dataset_id, generation.definition_revision_id, generation.schema_revision_id) != expected[:3]
        or _pack_row(final_plan.request_scope) != _pack_row(expected)
    ):
        raise ValueError("generation membership original scope differs")


def _selected_plans(final_plan, check):
    from process.custom_import.family_shard_plan import read_family_plan

    with closing(read_family_plan(final_plan, check=check)) as plans:
        for plan in plans:
            if plan.selection_kind != "absent":
                yield plan


def _member_cells(layout, generation, planned, supplied):
    if type(supplied) is not tuple or len(supplied) != 2 or supplied[0] != planned:
        raise ValueError("generation membership complete selected family differs")
    family = supplied[1]
    if type(family) is not AllocatedFamily or family.root_record_id != planned.root_record_id:
        raise ValueError("generation membership original root differs")
    for identifier in (family.family_revision_id, family.root_record_id, family.entity_binding_id):
        _integer(identifier, "generation member")
    if type(family.retained) is not bool or family.retained != (planned.selection_kind == "retained"):
        raise ValueError("generation membership selection differs")
    if family.retained:
        if (family.entity_binding_id, family.expected_family_sha256) != (
            planned.base_header.entity_binding_id,
            planned.base_header.family_sha256,
        ):
            raise ValueError("generation membership retained identity differs")
    elif (family.family_revision_id, family.entity_binding_id, family.root.revision_id, family.root.pack_id) != (
        *planned.source_metadata,
        planned.source_root.revision_id,
        planned.source_root.pack_id,
    ):
        raise ValueError("generation membership SOURCE allocation differs")
    _wire, parts = record_parts(layout.definition, None, family.root, retained=family.retained)
    from process.custom_import.runner_codec import digest_text

    if (parts[1], digest_text("root-key", parts[1])) != planned.typed_key:
        raise ValueError("generation membership original root key differs")
    return (
        generation.generation_id,
        generation.dataset_id,
        generation.definition_revision_id,
        generation.schema_revision_id,
        family.root_record_id,
        family.family_revision_id,
    )


def generation_member_batches(context, generation, final_plan, families, *, maximum_rows, maximum_bytes, check=None):
    """COPY cells from all original selected inputs; prefixes never prove coverage.

    This grants no membership or family proof. The owner compares the committed
    role and complete originals again after writes close.
    """
    from itertools import zip_longest

    _generation_scope(context, generation, final_plan)
    if any(type(bound) is not int or bound <= 0 for bound in (maximum_rows, maximum_bytes)):
        raise ValueError("generation member COPY budgets differ")
    batch, size = [], 0
    with ExitStack() as stack:
        plans = stack.enter_context(closing(_selected_plans(final_plan, check)))
        inputs = iter(families)
        close = getattr(inputs, "close", None)
        if close is not None:
            stack.callback(close)
        for planned, supplied in zip_longest(plans, inputs):
            if check is not None:
                check()
            if planned is None or supplied is None:
                raise ValueError("generation membership complete coverage differs")
            cells = _member_cells(context.layout, generation, planned, supplied)
            charge = len(_pack_row(cells)) + 128
            if charge > maximum_bytes:
                raise FamilyBlockResourceLimit("generation member exceeds COPY byte budget")
            if batch and (len(batch) == maximum_rows or size + charge > maximum_bytes):
                yield {"generation_family": tuple(batch)}
                batch, size = [], 0
            batch.append(cells)
            size += charge
        if batch:
            yield {"generation_family": tuple(batch)}


def _input_wire(definition, name, source, retained):
    with closing(_input_iterator(source())) as records:
        for record in records:
            wire, order = record_parts(definition, name, record, retained=retained)
            yield order, msgpack.packb(wire, use_bin_type=True)


def _records(work, path, definition, name, retained):
    with closing(work.rows(path)) as payloads:
        for payload in payloads:
            yield record_from_wire(
                definition, name, msgpack.unpackb(payload, raw=False, strict_map_key=True), retained=retained
            )[0]


def _sorted_children(work, layout, family):
    paths, counts = {}, {}
    for name in sorted(layout.definition.collections_by_name):
        counts[name] = 0

        def ordered(name=name):
            """Count complete sorted children while staging their original frames."""
            values = _input_wire(layout.definition, name, family.children_by_collection[name], family.retained)
            with closing(_sorted(work, values, key=lambda row: row[0])) as rows:
                for _order, payload in rows:
                    counts[name] += 1
                    yield payload

        paths[name] = work.write(ordered())
    return paths, counts


def _membership_rows(work, layout, family, verified, counts, query_path, key_ids):
    chunk = max(1, min(4096, work.limits.memory_bytes // 128))
    yield from (
        member.cells() for member in iter_revision_memberships(0, [(family.root.revision_id, 0)], chunk_records=chunk)
    )
    offsets, ordinal = {}, 1
    for name in sorted(counts):
        offsets[name], ordinal = ordinal, ordinal + counts[name]
    with query_path.open("xb") as query_output:
        for name in sorted(counts, key=layout.collection_slots.__getitem__):
            slot = layout.collection_slots[name]

            def identities(name=name, slot=slot):
                """Stage query cells and preserve every verified child ID/ordinal."""
                with closing(verified.records(name)) as records:
                    for ordinal, record in enumerate(records, offsets[name]):
                        if name == layout.definition.query.child_collection:
                            _wire, parts = record_parts(layout.definition, name, record, retained=family.retained)
                            try:
                                rank, variant = key_ids[(parts[0], parts[1])]
                            except KeyError as error:
                                raise ValueError(
                                    "completed query child key lacks its owner dictionary identity"
                                ) from error
                            _integer(rank, "query key rank")
                            _integer(variant, "query key variant")
                            row = (
                                record.revision_id,
                                family.family_revision_id,
                                slot,
                                rank,
                                variant,
                                *pack_query_scalars(query_scalar_fields(layout.definition, name), record.scalars),
                            )
                            work.frame(query_output, _pack_row(row), query_path)
                        yield record.revision_id, ordinal

            with closing(_sorted(work, identities(), key=lambda row: row[0])) as pairs:
                yield from (member.cells() for member in iter_revision_memberships(slot, pairs, chunk_records=chunk))


def _check_allocated_family(layout, family):
    """Require explicit mode, original identities and the complete collection set."""
    if (
        type(family) is not AllocatedFamily
        or type(family.retained) is not bool
        or type(family.root) is not FamilyRecord
    ):
        raise ValueError("family output requires explicit allocated input and mode")
    _sha256(family.expected_family_sha256)
    for identifier in (family.family_revision_id, family.root_record_id, family.entity_binding_id):
        _integer(identifier, "allocated identity")
    if not isinstance(family.children_by_collection, Mapping) or set(family.children_by_collection) != set(
        layout.definition.collections_by_name
    ):
        raise ValueError("allocated family collections differ")


def _verified_storage(work, layout, encoded, codec_limits, directory, memberships):
    """Prove the complete envelope and directory under a separately charged budget."""

    def members():
        """Replay all observed ID memberships for each independent proof pass."""
        return (RevisionMembership(*cells) for cells in work.rows(memberships))

    manifest = encoded.manifest
    storage = stored_manifest(manifest, work.rows(directory), members())
    proof_bytes = (
        manifest.stored_bytes
        + manifest.raw_bytes
        + (manifest.child_count + 1) * 17
        + work.paths[directory]
        + work.paths[memberships]
    )
    proof_bytes = max(
        proof_bytes, codec_limits.max_record_bytes, codec_limits.fragment_bytes, codec_limits.memory_bytes
    )
    work.charge(proof_bytes)
    try:
        proof_limits = replace(codec_limits, max_spool_bytes=proof_bytes)
        with verify_stored_family(
            layout.definition,
            layout.collection_slots,
            zip(work.rows(directory), encoded.fragments(), strict=True),
            members(),
            storage,
            limits=proof_limits,
            check=work._io_check,
        ):
            return storage
    finally:
        work.used -= proof_bytes


def _stored_family_rows(work, layout, family, encoded, storage, directory, memberships, query):
    """Emit the original role/cell order after every whole-family proof succeeds."""
    yield (
        "family",
        (
            family.family_revision_id,
            family.root_record_id,
            family.root.revision_id,
            family.entity_binding_id,
            storage.family_sha256,
            storage.storage_sha256,
            storage.retained,
            storage.child_count,
            storage.fragment_count,
            storage.raw_bytes,
            storage.stored_bytes,
            storage.membership_count,
            *pack_query_scalars(query_scalar_fields(layout.definition), family.root.scalars),
        ),
    )
    for metadata, frame in zip(work.rows(directory), encoded.fragments(), strict=True):
        yield "fragment", (family.family_revision_id, metadata[0], metadata[1], frame, *metadata[2:])
    for cells in work.rows(memberships):
        yield "revision_run", (family.family_revision_id, *cells)
    for packed_cells in work.rows(query):
        yield "query_record", _unpack_row(packed_cells)


def _completed_family_rows(work, layout, family, limits, key_ids):
    """Reserve, encode and fully verify one family before exposing any COPY cells."""
    _check_allocated_family(layout, family)
    paths, counts = _sorted_children(work, layout, family)
    capacity = work.remaining_bytes // 2
    memory = max(1, limits.memory_bytes // 8)
    if capacity < max(limits.max_record_bytes, limits.fragment_bytes, memory):
        raise FamilyBlockResourceLimit("family output codec budget is exhausted")
    codec_limits = replace(limits, max_spool_bytes=capacity, memory_bytes=memory, spool_directory=str(work.root))
    reserved = 2 * capacity
    work.charge(reserved)
    directory = memberships = query = None
    children_by_name = {}
    try:
        children_by_name = {
            name: _records(work, path, layout.definition, name, family.retained) for name, path in paths.items()
        }
        encode = encode_retained_family if family.retained else encode_family
        with encode(
            layout.definition, family.root, children_by_name, family.expected_family_sha256, limits=codec_limits
        ) as encoded:
            manifest = encoded.manifest
            needed = manifest.stored_bytes + manifest.raw_bytes
            work.used -= reserved - needed
            reserved = needed
            for path in paths.values():
                work.remove(path)
            paths.clear()
            with verify_family(
                layout.definition, encoded.fragments(), manifest, limits=codec_limits, check=work._io_check
            ) as verified:
                directory = work.write(
                    iter_fragment_directory(
                        verified,
                        layout.collection_slots,
                        (_FRAME.unpack(frame[:16])[1] for frame in encoded.fragments()),
                        check=work._io_check,
                    )
                )
                query = work.path()
                memberships = work.write(_membership_rows(work, layout, family, verified, counts, query, key_ids))
            work.used -= manifest.raw_bytes
            reserved -= manifest.raw_bytes

            storage = _verified_storage(work, layout, encoded, codec_limits, directory, memberships)
            yield from _stored_family_rows(work, layout, family, encoded, storage, directory, memberships, query)
    finally:
        for child_records in children_by_name.values():
            child_records.close()
        work.used -= reserved
        for path in (*paths.values(), directory, memberships, query):
            if path is not None and path in work.paths and path.exists():
                work.remove(path)


class FamilyShardOutput:
    """Completed partial-role spool; owner recomputes proof when adding other roles."""

    def __init__(self, work, path, receipt):
        self._work, self.path, self._receipt = work, path, receipt
        self._identity = path.stat()
        self._closed = False
        with path.open("rb") as source:
            self.sha256 = _output_digest(source, work)

    @property
    def receipt(self):
        """Return an independent receipt copy without sharing mutable role counts."""
        return {**self._receipt, "role_counts": dict(self._receipt["role_counts"])}

    def batches(self, *, maximum_rows, maximum_bytes):
        """Yield bounded precomputed native cells, preserving lossless scalar types."""
        if type(maximum_rows) is not int or type(maximum_bytes) is not int or min(maximum_rows, maximum_bytes) <= 0:
            raise ValueError("family output COPY budgets must be positive")
        if self._closed:
            raise ValueError("completed family output is closed")
        root, output_stat = self._work.root.lstat(), self.path.lstat()
        if (
            not stat.S_ISDIR(root.st_mode)
            or not stat.S_ISREG(output_stat.st_mode)
            or (root.st_dev, root.st_ino) != (self._work.identity.st_dev, self._work.identity.st_ino)
            or (output_stat.st_dev, output_stat.st_ino, output_stat.st_size)
            != (self._identity.st_dev, self._identity.st_ino, self._identity.st_size)
        ):
            raise ValueError("completed family output custody differs")
        role, batch_rows, size = None, [], 0
        with os.fdopen(os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as output_file:
            for current_role, packed_cells in self._work.read(output_file):
                cells = _unpack_row(packed_cells)
                row_size = 2 + sum(_size(cell) for cell in cells)
                if row_size > maximum_bytes:
                    raise FamilyBlockResourceLimit("family output singleton exceeds the COPY budget")
                if batch_rows and (
                    current_role != role
                    or len(batch_rows) == maximum_rows
                    or size + row_size > min(maximum_bytes, self._work.limits.memory_bytes)
                ):
                    yield {role: tuple(batch_rows)}
                    batch_rows, size = [], 0
                role = current_role
                batch_rows.append(cells)
                size += row_size
            output_file.seek(0)
            if _output_digest(output_file, self._work) != self.sha256:
                raise ValueError("completed family output bytes differ")
        if batch_rows:
            yield {role: tuple(batch_rows)}

    def close(self):
        """Remove this output's exact owned spool once, including failed consumers."""
        if not self._closed:
            self._work.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


def _output_cells(rows, width_by_role, proof):
    """Validate each COPY shape before adding it to the original framed receipt."""
    for role, cells in rows:
        if len(cells) != width_by_role[role]:
            raise ValueError("completed family COPY row shape differs")
        proof.add(role, (cells,))
        yield role, _pack_row(cells)


def _output_digest(source, work):
    digest = hashlib.sha256()
    while True:
        if work._io_check is not None:
            work._io_check()
        block = source.read(max(1, min(work.limits.memory_bytes, 1024**2)))
        if not block:
            return digest.digest()
        digest.update(block)


def build_family_output(
    layout,
    snapshot_family_id,
    families,
    query_key_ids,
    *,
    work,
    shard_id,
    partition_sha256,
    source_record_count,
):
    """Prove each family and atomically publish one bounded output file.

    The owner key lookup preserves existing hash/text pairs and (rank, variant).
    The receipt covers family, fragment, revision_run and query_record roles.
    """
    if (
        type(work) is not FamilyOutputWork
        or type(work.limits) is not FamilyBlockLimits
        or not isinstance(query_key_ids, Mapping)
    ):
        raise ValueError("family output requires resource bounds and an owner key lookup")
    limits = work.limits
    tables = layout.tables(snapshot_family_id)
    width_by_role = {role: len(table.c) for role, table in tables.items()}
    record_budget = min(limits.max_spool_bytes, 4 * limits.max_record_bytes + limits.fragment_bytes + 1024)
    proof = ShardOutputProof(layout, shard_id, partition_sha256, source_record_count)
    fact_limits = SourceFactLimits(limits.max_spool_bytes, record_budget, max(1, limits.memory_bytes // 8))
    work = _Work(work.root, fact_limits, work.register_cleanup, check=work.check)

    def output_rows():
        """Consume complete family inputs and close borrowed iterators on failure."""
        with closing(_input_iterator(families)) as inputs:
            for family in inputs:
                with closing(_completed_family_rows(work, layout, family, limits, query_key_ids)) as rows:
                    yield from _output_cells(rows, width_by_role, proof)

    try:
        # One encoded spool can coexist with all five storage-proof spools.
        for _ in range(6):
            work._reserve_entry()
        path = work.write(output_rows())
        ready = work.root / "output.mpk"
        os.chmod(path, 0o400)
        with path.open("rb") as output_file:
            os.fsync(output_file.fileno())
        os.link(path, ready, follow_symlinks=False)
        path.unlink()
        work.paths[ready] = work.paths.pop(path)
        directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return FamilyShardOutput(work, ready, proof.receipt())
    except BaseException:
        work.close()
        raise
