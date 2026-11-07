# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""CPU-only source families and complete provisional audit evidence.

Tasks contain closed-file custody and bounded primitive allocation maps. The
owner retains admission authority, pinned-base merging, winner selection and
publication. A source-only absent plan never means that a retained family may
be discarded. Candidate errors produce an explicit refused result after EOF.
"""

import hashlib
import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path

from process.custom_import import runner_codec as codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import CandidateRejected
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import FamilyRecord, _integer, _sha256
from process.custom_import.family_shard_facts import SourceFactLimits, _Work
from process.custom_import.family_shard_input_records import encode_record
from process.custom_import.family_shard_output import AllocatedFamily, FamilyOutputWork, _pack_row, build_family_output
from process.custom_import.family_shard_route import RoutedShard, prepare_routed_families
from process.custom_import.materialization import (
    ChildScalarTarget,
    RootScalarTarget,
    project_child_scalars,
    project_root_scalars,
)


@dataclass(frozen=True)
class SourceShardPlan:
    shard: RoutedShard
    definition_canonical: str
    collection_slots: dict
    dataset_id: int
    schema_revision_id: int
    snapshot_family_id: int
    record_ids: dict
    family_ids: dict
    query_key_ids: dict
    complete_scope: bool


@dataclass(frozen=True)
class SourceWorkerLimits:
    facts: SourceFactLimits
    output: FamilyBlockLimits
    audit: SourceFactLimits
    maximum_metadata_rows: int
    maximum_metadata_bytes: int

    def __post_init__(self):
        if type(self.facts) is not SourceFactLimits or type(self.audit) is not SourceFactLimits:
            raise ValueError("source worker fact or audit budgets differ")
        if type(self.output) is not FamilyBlockLimits:
            raise ValueError("source worker output budgets differ")
        for bound in (self.maximum_metadata_rows, self.maximum_metadata_bytes):
            if type(bound) is not int or bound <= 0:
                raise ValueError("source worker metadata budgets must be positive")


def _key_evidence(key, domain, maximum_bytes):
    if type(key) is not tuple or len(key) != 2 or type(key[0]) is not str:
        raise ValueError("source worker dictionary key differs")
    if len(key[0]) > maximum_bytes:
        raise FamilyBlockResourceLimit("source worker metadata byte budget exceeded")
    _sha256(key[1])
    if codec.digest_text(domain, key[0]) != key[1]:
        raise ValueError("source worker dictionary key digest differs")


def _metadata(plan, limits):
    mappings = (plan.record_ids, plan.family_ids, plan.query_key_ids)
    if any(type(mapping) is not dict for mapping in (*mappings, plan.collection_slots)):
        raise ValueError("source worker metadata must be plain bounded dictionaries")
    if sum(len(mapping) for mapping in mappings) > limits.maximum_metadata_rows:
        raise FamilyBlockResourceLimit("source worker metadata row budget exceeded")
    size = len(plan.definition_canonical.encode("utf-8")) + len(_pack_row(plan.collection_slots))
    digest = hashlib.sha256()
    for label, mapping in zip(("record", "family", "query"), mappings, strict=True):
        for key, assignment in mapping.items():
            if type(assignment) is not tuple or len(assignment) != (3 if label == "record" else 2):
                raise ValueError("source worker native allocation shape differs")
            for identifier in assignment:
                _integer(identifier, "worker allocation")
            if label == "record":
                if type(key) is not tuple or len(key) != 2:
                    raise ValueError("source worker occurrence coordinate differs")
                _integer(key[0], "worker stream")
                _integer(key[1], "worker source ordinal", zero=True)
            else:
                if type(key) is not tuple or len(key) != 2:
                    raise ValueError("source worker dictionary key differs")
                evidence = key if label == "family" else (key[1], key[0])
                _key_evidence(evidence, "root-key" if label == "family" else "child-key", limits.maximum_metadata_bytes)
            encoded_entry = _pack_row((label, key, assignment))
            size += len(encoded_entry) + 8
            if size > limits.maximum_metadata_bytes:
                raise FamilyBlockResourceLimit("source worker metadata byte budget exceeded")
            digest.update(encoded_entry)
    if size > limits.maximum_metadata_bytes:
        raise FamilyBlockResourceLimit("source worker metadata byte budget exceeded")
    return size, digest.digest()


def _layout(plan, limits):
    if type(plan) is not SourceShardPlan or type(limits) is not SourceWorkerLimits:
        raise ValueError("source worker requires a frozen plan and resource limits")
    if (
        type(plan.shard) is not RoutedShard
        or type(plan.complete_scope) is not bool
        or type(plan.definition_canonical) is not str
    ):
        raise ValueError("source worker input or scope differs")
    for identifier in (plan.dataset_id, plan.schema_revision_id, plan.snapshot_family_id):
        _integer(identifier, "worker scope")
    definition = CustomImportDefinition.from_json(plan.definition_canonical)
    if plan.shard.definition_sha256 != definition.digest:
        raise ValueError("source worker definition differs from routed input")
    metadata_bytes, metadata_sha256 = _metadata(plan, limits)
    return FamilyBlockLayout(definition, plan.collection_slots), metadata_bytes, metadata_sha256


def _coordinate(record):
    return record.stream_slot, record.source_ordinal


def _values(definition, record):
    return record.normalized_values(definition, definition.source_streams[record.stream_slot - 1])


def _record(plan, layout, source_record, root_id, collection=None):
    try:
        revision_id, pack_id, allocated_root = plan.record_ids[_coordinate(source_record)]
    except KeyError as error:
        raise ValueError("source worker occurrence lacks an observed native allocation") from error
    if allocated_root != root_id:
        raise ValueError("source worker root allocation differs across its family")
    source_values = _values(layout.definition, source_record)
    if collection is None:
        projections = project_root_scalars(
            layout.definition,
            root_target=RootScalarTarget(plan.dataset_id, plan.schema_revision_id, root_id, revision_id),
            root_values=source_values,
        )
    else:
        projections = project_child_scalars(
            layout.definition,
            collection=collection,
            child_target=ChildScalarTarget(
                plan.dataset_id, plan.schema_revision_id, root_id, plan.collection_slots[collection], revision_id
            ),
            child_values=source_values,
            child_collection_slots=plan.collection_slots,
        )
    return FamilyRecord(
        revision_id,
        pack_id,
        source_record.source_ordinal,
        source_values,
        {projection.field_id: projection.scalar for projection in projections},
    )


def _documents(definition, family, collection):
    fields = codec.fields_by_collection(definition)[collection]
    for record in family.children(collection):
        values = _values(definition, record)
        yield codec.digest_text("child-key", record.child_key), record.child_key, codec.record_payload(fields, values)


def _allocated(plan, layout, family):
    try:
        family_id, entity_id = plan.family_ids[family.typed_key]
        root_id = plan.record_ids[_coordinate(family.source_root)][2]
    except KeyError as error:
        raise ValueError("source worker family lacks observed native identities") from error
    root = _record(plan, layout, family.source_root, root_id)
    if codec.root_key_document(layout.definition, root.values_by_field) != family.typed_key[0]:
        raise ValueError("source worker typed root evidence differs")
    digest = codec.new_family_hash_ordered(
        layout.definition,
        root.values_by_field,
        {name: _documents(layout.definition, family, name) for name in layout.collection_slots},
    )

    def _children(name):
        for record in family.children(name):
            yield _record(plan, layout, record, root_id, name)

    return AllocatedFamily(
        family_id,
        root_id,
        entity_id,
        root,
        {name: lambda name=name: _children(name) for name in layout.collection_slots},
        digest,
    )


def _selection(family, complete_scope):
    if family.fatal_problem is not None:
        return "error", family.fatal_problem
    try:
        return family.selection(has_base=False, complete_scope=complete_scope), None
    except CandidateRejected as error:
        return "error", str(error)


def _audit_family(work, audit_output, path, plan, family, counts):
    selection, problem = _selection(family, plan.complete_scope)
    header_by_name = {
        "typed_key": family.typed_key,
        "selection": selection,
        "selection_problem": problem,
        "source_root": None if family.source_root is None else _coordinate(family.source_root),
        "source_facts": None if family.source_facts is None else asdict(family.source_facts),
        "root_resolution": None if family.root_resolution is None else asdict(family.root_resolution),
        "candidate_errors": family.candidate_errors,
        "fatal_problem": family.fatal_problem,
        "fatal_order": family.fatal_order,
        "family_ids": plan.family_ids.get(family.typed_key),
    }
    work.frame(audit_output, ("family", header_by_name), path)
    counts["families"] += 1
    counts[selection] += 1
    counts["candidate_errors"] += family.candidate_errors
    counts["fatal_families"] += int(family.fatal_problem is not None)
    for source_record, facts, decision in family.rows():
        work.frame(
            audit_output,
            (
                "occurrence",
                family.typed_key,
                encode_record(source_record),
                asdict(facts),
                asdict(decision),
                plan.record_ids.get(_coordinate(source_record)),
            ),
            path,
        )
        counts["records"] += 1
    return selection


class SourceShardOutput:
    """Complete source audit and provisional COPY cells; owner proof is separate."""

    def __init__(self, work, path, output, counts):
        self._work, self.path, self._output, self._counts = work, path, output, dict(counts)
        self._identity, self._closed = path.stat(), False
        with path.open("rb") as source:
            self.sha256 = hashlib.file_digest(source, "sha256").digest()
        self.status = "refused" if counts["error"] else "source_eof"

    @property
    def counts(self):
        """Return independent source audit counters, without publication authority."""
        return dict(self._counts)

    @property
    def receipt(self):
        """Return provisional family-role evidence; owner closure is separate."""
        return self._output.receipt

    def batches(self, *, maximum_rows, maximum_bytes):
        """Yield provisional COPY cells only after error-free routed source EOF."""
        if self.status != "source_eof":
            raise CandidateRejected("source worker audit contains candidate or structural errors")
        yield from self._output.batches(maximum_rows=maximum_rows, maximum_bytes=maximum_bytes)

    def audit_rows(self):
        """Read complete audit evidence with custody and same-file byte checks."""
        if self._closed:
            raise ValueError("source worker output is closed")
        observed, root = self.path.lstat(), self._work.root.lstat()
        if not stat.S_ISDIR(root.st_mode) or (root.st_dev, root.st_ino) != (
            self._work.identity.st_dev,
            self._work.identity.st_ino,
        ):
            raise ValueError("source worker audit directory custody differs")
        with os.fdopen(os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
            opened = os.fstat(source.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or (opened.st_dev, opened.st_ino, opened.st_size, stat.S_IMODE(opened.st_mode))
                != (self._identity.st_dev, self._identity.st_ino, self._identity.st_size, 0o400)
                or (observed.st_dev, observed.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise ValueError("source worker audit file custody differs")
            if hashlib.file_digest(source, "sha256").digest() != self.sha256:
                raise ValueError("source worker audit bytes differ")
            source.seek(0)
            yield from self._work.read(source)
            source.seek(0)
            if hashlib.file_digest(source, "sha256").digest() != self.sha256:
                raise ValueError("source worker audit changed while reading")

    def close(self):
        """Remove only this result's exact family-output and audit roots."""
        if not self._closed:
            try:
                self._output.close()
            finally:
                self._work.close()
                self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


def _source_output(work, audit_output, path, plan, layout, counts, limits):
    with prepare_routed_families(
        plan.shard,
        layout.definition,
        root=work.root / "facts",
        register_cleanup=lambda _path: None,
        limits=limits.facts,
    ) as families:

        def _allocated_families():
            for family in families:
                if _audit_family(work, audit_output, path, plan, family, counts) == "source":
                    yield _allocated(plan, layout, family)

        return build_family_output(
            layout,
            plan.snapshot_family_id,
            _allocated_families(),
            plan.query_key_ids,
            work=FamilyOutputWork(work.root / "output", limits.output, lambda _path: None),
            shard_id=plan.shard.shard_id,
            partition_sha256=bytes.fromhex(plan.shard.partition_sha256),
            source_record_count=plan.shard.records,
        )


def build_source_shard(plan, *, root, register_cleanup, limits):
    """Finish actual routed EOF, complete audit and family output before returning.

    Budgets bound metadata memory and fact, audit and output disk envelopes.
    Borrowed family factories are
    consumed synchronously before advancing the prepared-family iterator.
    """
    layout, metadata_bytes, metadata_sha256 = _layout(plan, limits)
    work = _Work(root, limits.audit, register_cleanup)
    path, output = work.path(), None
    counts_by_kind = dict(records=0, families=0, source=0, absent=0, error=0, candidate_errors=0, fatal_families=0)
    try:
        with path.open("xb") as audit_output:
            work.frame(
                audit_output,
                (
                    "worker",
                    {
                        "shard": asdict(plan.shard),
                        "dataset_id": plan.dataset_id,
                        "schema_revision_id": plan.schema_revision_id,
                        "snapshot_family_id": plan.snapshot_family_id,
                        "metadata_bytes": metadata_bytes,
                        "metadata_sha256": metadata_sha256,
                        "complete_scope": plan.complete_scope,
                    },
                ),
                path,
            )
            output = _source_output(work, audit_output, path, plan, layout, counts_by_kind, limits)
            if counts_by_kind["records"] != plan.shard.records:
                raise ValueError("source worker complete occurrence census differs")
            work.frame(audit_output, ("eof", counts_by_kind), path)
            audit_output.flush()
            os.fsync(audit_output.fileno())
        path.chmod(0o400)
        directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return SourceShardOutput(work, path, output, counts_by_kind)
    except BaseException:
        try:
            if output is not None:
                output.close()
        finally:
            work.close()
        raise


def run_source_shard(plan, root, limits):
    """Process entry point; parent registers this exact cleanup root before dispatch."""
    expected = Path(root).absolute()

    def _registered(path):
        if path != expected:
            raise ValueError("source worker cleanup root differs from its registered task")

    return build_source_shard(plan, root=expected, register_cleanup=_registered, limits=limits)
