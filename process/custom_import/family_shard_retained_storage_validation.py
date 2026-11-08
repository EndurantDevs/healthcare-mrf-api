# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose exact retained assignments with independently decoded stored output.

The completed original comparison binds pinned bytes, lineage and allocations.
This pass proves the assigned records and global query dictionary against the
actual output file; native rows, winners and final acceptance remain separate.
"""

import hashlib
from contextlib import closing
from dataclasses import dataclass, field, replace
from itertools import zip_longest
from types import SimpleNamespace

from process.custom_import import family_shard_output_groups as outputs
from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_output as retained
from process.custom_import.build_source import _drain_cleanup
from process.custom_import.family_block_codec import FamilyBlockLimits
from process.custom_import.family_block_records import _integer, record_parts
from process.custom_import.family_block_storage_codec import _fixed_storage_bytes, verify_stored_family
from process.custom_import.family_block_store import _checked_receipt
from process.custom_import.family_block_validate import (
    FamilyBlockValidationError,
    _add,
    check_family_records,
    check_key_dictionary,
)
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_facts import _Replay, _sorted
from process.custom_import.family_shard_original_validation import _dictionary_rows, _expected_records
from process.custom_import.family_shard_output import FamilyShardOutput, _pack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_retained_encoding import _stage_global_dictionary
from process.custom_import.family_shard_retained_homes import _destination_scope
from process.custom_import.family_shard_retained_validation import RetainedOriginalComparison
from process.custom_import.runner_codec import digest_text, root_key_contract_hash

_MISSING = object()


@dataclass(frozen=True)
class RetainedStoredComparison:
    """Complete assigned/stored equality; no native or winner acceptance."""

    scope: tuple
    destination_sha256: bytes
    original_input_sha256: bytes
    output_inputs: tuple
    counts: tuple
    root_scalar_count: int
    child_scalar_count: int
    dictionary_key_count: int
    used_query_key_count: int
    sha256: bytes
    unproven_boundaries: tuple = field(default=("native_output", "winner_contexts"), init=False)


def _output_inputs(context, output):
    if type(output) is not FamilyShardOutput or output._closed:
        raise ValueError("retained stored comparison requires a live real family output")
    if type(output.sha256) is not bytes or len(output.sha256) != 32:
        raise FamilyBlockValidationError("retained stored output digest is not immutable SHA-256")
    receipt = _checked_receipt(context.common.layout, output.receipt)
    if (
        receipt["partition_sha256"] != bytes.fromhex(context.common.source.source.partition_sha256)
        or receipt["source_record_count"] != 0
    ):
        raise FamilyBlockValidationError("retained output original partition or source count differs")
    return (
        output.sha256,
        receipt["shard_id"],
        receipt["partition_sha256"],
        receipt["result_sha256"],
        receipt["source_record_count"],
        tuple(sorted(receipt["role_counts"].items())),
    )


def _scope(context, destination, comparison, output, original_query_keys, work):
    if type(work) is not PlanWork or type(context) is not allocation.RetainedAllocationContext:
        raise ValueError("retained stored comparison requires real original owners and accounted work")
    if type(comparison) is not RetainedOriginalComparison or not callable(original_query_keys):
        raise ValueError("retained stored comparison requires completed original proof and a fresh key factory")
    scope_work = allocation.RetainedAllocationWork(
        work.root,
        SourceAllocationLimits(work.limits, 1, work.limits.max_record_bytes),
        work.register_cleanup,
        work.check,
    )
    scope = _destination_scope(context, destination, scope_work)
    if (
        type(comparison.scope) is not tuple
        or type(comparison.destination_sha256) is not bytes
        or type(destination.sha256) is not bytes
        or type(context.retained_input.sha256) is not bytes
        or _pack_row(comparison.scope) != _pack_row(scope)
        or comparison.destination_sha256 != destination.sha256
        or comparison.counts != context.retained_input.counts
    ):
        raise FamilyBlockValidationError("retained stored original comparison scope differs")
    for _role, count in comparison.counts:
        _integer(count, "retained stored original count", zero=True)
    for count in (comparison.root_scalar_count, comparison.child_scalar_count):
        _integer(count, "retained stored scalar count", zero=True)
    if comparison.root_scalar_count + comparison.child_scalar_count != dict(comparison.counts)["scalar"]:
        raise FamilyBlockValidationError("retained stored original scalar split differs")
    return scope, _output_inputs(context, output)


def _family_identity(plan, original, family, group):
    row, base = group.family_row, plan.base_header
    if (
        plan.selection_kind != "retained"
        or base is None
        or original.retained is not True
        or family.retained is not True
        or row["retained"] is not True
        or original.family_revision_id != base.family_revision_id
        or original.root_record_id != base.root_record_id
        or original.entity_binding_id != base.entity_binding_id
        or (row["family_revision_id"], row["root_record_id"], row["root_revision_id"], row["entity_binding_id"])
        != (family.family_revision_id, family.root_record_id, family.root.revision_id, family.entity_binding_id)
        or family.root_record_id != original.root_record_id
        or family.entity_binding_id != original.entity_binding_id
        or bytes(row["family_sha256"]) != family.expected_family_sha256
        or family.expected_family_sha256 != base.family_sha256
        or row["child_count"] != base.child_count
    ):
        raise FamilyBlockValidationError("retained stored family differs from its original assignment")


def _query_rows(layout, group, family, claims, work):
    collection = layout.definition.query.child_collection
    if collection is None:
        with closing(group.query_rows()) as cells:
            if next(cells, _MISSING) is not _MISSING:
                raise FamilyBlockValidationError("retained stored output has undeclared query rows")
        return
    with closing(group.query_rows()) as cells, closing(family.children_by_collection[collection]()) as records:
        for row, record in zip_longest(cells, records, fillvalue=_MISSING):
            work.current()
            if row is _MISSING or record is _MISSING:
                raise FamilyBlockValidationError("retained query rows differ from complete assigned children")
            _integer(row[3], "retained query rank")
            _integer(row[4], "retained query variant")
            _wire, (digest, text, _payload) = record_parts(layout.definition, collection, record, retained=True)
            claims.append((row[3], row[4], digest, text))
            yield row[1], row[0], row[2], digest, text, *row[5:]


def _verify_family(context, family_input, group, claims, work):
    plan, original, family = family_input
    _family_identity(plan, original, family, group)
    layout, family_row = context.common.layout, group.family_row
    manifest = layout.manifest(family_row)
    reservation = _fixed_storage_bytes(manifest) + sum(
        group.replays_by_role[role].size for role in ("fragment", "revision_run")
    )
    work.charge(reservation)
    limits = FamilyBlockLimits(
        reservation,
        min(work.limits.max_record_bytes, reservation),
        fragment_bytes=min(65536, reservation),
        memory_bytes=min(work.limits.memory_bytes, reservation),
        spool_directory=str(work.root),
    )
    try:
        with verify_stored_family(
            layout.definition,
            layout.collection_slots,
            group.fragments(),
            group.memberships(),
            manifest,
            limits=limits,
            check=work.current,
        ) as verified:
            work.used -= reservation - manifest.raw_bytes
            reservation = manifest.raw_bytes
            identity = None
            if family.root.sparse is None:
                entity = family.root.values_by_field.get(layout.definition.entity_field)
                identity = "npi", entity, digest_text("entity:npi", entity)
            with (
                closing(_expected_records(layout, family, work)) as expected,
                closing(_query_rows(layout, group, family, claims, work)) as query,
            ):
                return check_family_records(
                    layout,
                    family_row,
                    verified,
                    expected,
                    root_key_contract=root_key_contract_hash(layout.definition),
                    root_key=(plan.typed_key[1], plan.typed_key[0]),
                    entity=identity,
                    query_rows=query,
                )
    finally:
        work.used -= reservation


def _family_claims(context, destination, output, work, totals, digest):
    state, previous = SimpleNamespace(is_complete=False), None
    with (
        closing(retained._replay_families(destination, context.common.request.definition, work)) as families,
        closing(
            outputs._direct_groups(output, context.common.layout, context.common.source.snapshot_family_id, work, state)
        ) as groups,
    ):
        for family_input, group in zip_longest(families, groups, fillvalue=_MISSING):
            work.current()
            if family_input is _MISSING or group is _MISSING:
                raise FamilyBlockValidationError("retained assigned and stored family coverage differs")
            plan = family_input[0]
            order = plan.typed_key[1], plan.typed_key[0].encode("utf-8")
            if previous is not None and order <= previous:
                raise FamilyBlockValidationError("retained assigned family keys repeat or are unordered")
            previous = order
            with _Replay(work) as claims:
                children, root_scalars, child_scalars = _verify_family(context, family_input, group, claims, work)
                for name, count in zip(
                    ("family", "child", "root_scalar", "child_scalar"),
                    (1, children, root_scalars, child_scalars),
                    strict=True,
                ):
                    totals[name] += count
                _add(digest, "family", (plan.typed_key, dict(group.family_row)))
                yield from claims.rows()
        if not state.is_complete:
            raise FamilyBlockValidationError("retained stored comparison lacks complete output EOF")


def _dictionary(keys, claims, work, totals, digest):
    with closing(work.rows(keys)) as rows:
        ordered_keys = ((key_hash, text, rank, variant) for rank, variant, key_hash, text in rows)
        return check_key_dictionary(_dictionary_rows(ordered_keys, claims, work, digest, totals))


def _compare(context, destination, comparison, output, original_query_keys, work):
    digest = hashlib.sha256(b"custom-import/retained-stored-comparison/v1\0")
    inputs = _output_inputs(context, output)
    _add(digest, "inputs", (comparison.scope, destination.sha256, context.retained_input.sha256, inputs))
    keys, key_count = _stage_global_dictionary(context, original_query_keys, work)
    totals = dict.fromkeys(("family", "child", "root_scalar", "child_scalar", "used_query_keys"), 0)
    claims = _family_claims(context, destination, output, work, totals, digest)
    with closing(_sorted(work, claims, lambda row: row)) as ordered:
        if _dictionary(keys, ordered, work, totals, digest) != key_count:
            raise FamilyBlockValidationError("retained complete dictionary count changed")
    expected_by_role = dict(comparison.counts)
    if (
        (totals["family"], totals["child"], totals["root_scalar"], totals["child_scalar"])
        != (
            expected_by_role["family"],
            expected_by_role["child"],
            comparison.root_scalar_count,
            comparison.child_scalar_count,
        )
        or expected_by_role["root"] != totals["family"]
        or _output_inputs(context, output) != inputs
    ):
        raise FamilyBlockValidationError("retained stored counts or output provenance changed")
    _add(digest, "counts", (comparison.counts, tuple(sorted(totals.items())), key_count))
    return RetainedStoredComparison(
        comparison.scope,
        destination.sha256,
        context.retained_input.sha256,
        inputs,
        comparison.counts,
        comparison.root_scalar_count,
        comparison.child_scalar_count,
        key_count,
        totals["used_query_keys"],
        digest.digest(),
    )


async def prove_retained_output(context, destination, comparison, output, original_query_keys, *, work):
    """Require live originals and both EOFs before returning stored-only evidence."""
    scope, inputs = _scope(context, destination, comparison, output, original_query_keys, work)
    deadline = _read_deadline(context.common.request)

    def current():
        """Preserve parent cancellation and the original immutable deadline."""
        if work.check is not None:
            work.check()
        _require_deadline(deadline)

    bounded = replace(
        work, limits=replace(work.limits, memory_bytes=max(1, work.limits.memory_bytes // 16)), check=current
    )
    scratch, primary, comparison_receipt = _Scratch(bounded), None, None
    try:
        for _ in range(5):
            scratch._reserve_entry()
        await allocation._check_base(context)
        comparison_receipt = await _cpu(
            scratch, _compare, context, destination, comparison, output, original_query_keys, scratch
        )
        await allocation._check_base(context)
        if _scope(context, destination, comparison, output, original_query_keys, work) != (scope, inputs):
            raise FamilyBlockValidationError("retained stored comparison original scope changed")
        scratch.current()
    except BaseException as error:
        primary = error
    primary = await _drain_cleanup(_cpu(scratch, scratch.close), primary)
    if primary is not None:
        raise primary
    return comparison_receipt
