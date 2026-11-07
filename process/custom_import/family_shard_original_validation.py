# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare selected SOURCE output with independent original admitted records.

This is SOURCE-only semantic evidence. Retained bytes, all winner contexts,
native rows, current authority and final acceptance remain separate obligations.
"""

import hashlib
from contextlib import closing
from dataclasses import dataclass, replace
from itertools import zip_longest
from types import SimpleNamespace

from process.custom_import import family_shard_output_groups as outputs
from process.custom_import import family_shard_source_values as originals
from process.custom_import.build_source import _source_cleanup_failure
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_codec import FamilyBlockLimits
from process.custom_import.family_block_records import _integer, record_parts
from process.custom_import.family_block_storage_codec import _fixed_storage_bytes, verify_stored_family
from process.custom_import.family_block_store import _checked_receipt
from process.custom_import.family_block_validate import (
    FamilyBlockValidationError,
    _add,
    _equal_native,
    check_family_records,
    check_key_dictionary,
)
from process.custom_import.family_shard_facts import _Replay, _sorted
from process.custom_import.family_shard_output import _input_iterator
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from process.custom_import.family_shard_semantic_audit import (
    OwnerSemanticAudit,
    SourceOutputAudit,
    SourceRecordClaims,
    SourceSemanticSelection,
)
from process.custom_import.runner_codec import digest_text, root_key_contract_hash

_MISSING = object()
_COUNTS = ("family_count", "family_child_count", "root_scalar_count", "child_scalar_count")


@dataclass(frozen=True)
class SourceOriginalComparison:
    """Full observed SOURCE totals; optional audit explicitly qualifies semantics."""

    producer: tuple
    inputs: tuple
    workers: tuple
    counts: tuple
    source_record_count: int
    retained_family_count_unproven: int
    dictionary_key_count: int
    used_query_key_count: int
    sha256: bytes
    semantic_audit: OwnerSemanticAudit | None = None
    record_claims_sha256: bytes | None = None

    @property
    def counts_by_name(self):
        """Return only observed SOURCE totals; winners and retained data are absent."""
        return dict(self.counts)


def _producer(context):
    request = context.request
    capture_by_name = dict(context.source.source.scope[0])
    return (
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        request.fence,
        producer_token_sha256(request),
        capture_by_name["capture_bundle_id"],
        capture_by_name["source_binding_revision_id"],
        context.source.snapshot_family_id,
        request.expected_base_generation_id,
        request.expected_pointer_version,
        request.complete_scope,
    )


def _input_scope(context, admission, final_plan, workers, work):
    originals._checked_scope(context, admission, final_plan, work)
    outputs._scope(
        context.source,
        context.layout.definition,
        workers,
        outputs.AdmissionWork(work.root, work.limits, work.register_cleanup, work.check),
        context.request.complete_scope,
    )
    if dict(context.source.source.collection_slots) != dict(context.layout.collection_slots):
        raise FamilyBlockValidationError("original comparison native collection scope differs")
    return (
        context.source.source.sha256,
        context.source.sha256,
        admission.sha256,
        final_plan.sha256,
        context.layout.digest,
    )


def _worker_inputs(layout, workers):
    observed_workers = []
    for worker in workers:
        receipt = _checked_receipt(layout, worker.receipt)
        observed_workers.append(
            (
                receipt["shard_id"],
                worker.sha256,
                worker._output.sha256,
                receipt["partition_sha256"],
                receipt["result_sha256"],
                receipt["source_record_count"],
                tuple(sorted(receipt["role_counts"].items())),
            )
        )
    return tuple(sorted(observed_workers))


def _expected_records(layout, family, work, plan=None, record_claims=None):
    work.current()
    if record_claims is not None:
        record_claims.observe(plan, plan.source_root, None, family.root, 0)
    yield 0, family.root
    ordinal = 0
    for collection in sorted(layout.collection_slots):
        factory = family.children_by_collection[collection]
        with closing(_expected_pairs(factory, record_claims)) as records:
            for occurrence, record in records:
                work.current()
                ordinal += 1
                if record_claims is not None:
                    record_claims.observe(plan, occurrence, collection, record, ordinal)
                yield layout.collection_slots[collection], record


def _expected_pairs(factory, claims):
    if claims is None:
        with closing(factory()) as records:
            for record in records:
                yield None, record
    else:
        with closing(factory.records_with_occurrences()) as records:
            yield from records


def _query_rows(layout, group, family, claims, work):
    collection = layout.definition.query.child_collection
    if collection is None:
        with closing(group.query_rows()) as cells:
            if next(cells, _MISSING) is not _MISSING:
                raise FamilyBlockValidationError("original comparison has undeclared query rows")
        return
    with closing(group.query_rows()) as cells, closing(family.children_by_collection[collection]()) as records:
        for row, record in zip_longest(cells, records, fillvalue=_MISSING):
            work.current()
            if row is _MISSING or record is _MISSING:
                raise FamilyBlockValidationError("original query rows differ from complete child coverage")
            _integer(row[3], "original query rank")
            _integer(row[4], "original query variant")
            _wire, (digest, text, _payload) = record_parts(layout.definition, collection, record, retained=False)
            claims.append((row[3], row[4], digest, text))
            yield (row[1], row[0], row[2], digest, text, *row[5:])


def _family_identity(plan, family, group):
    row = group.family_row
    if (
        not _equal_native(dict(group.header), plan.source_header)
        or plan.typed_key != group.header["typed_key"]
        or (row["family_revision_id"], row["root_record_id"], row["root_revision_id"], row["entity_binding_id"])
        != (family.family_revision_id, family.root_record_id, family.root.revision_id, family.entity_binding_id)
        or row["retained"] is not False
        or family.retained is not False
        or bytes(row["family_sha256"]) != family.expected_family_sha256
    ):
        raise FamilyBlockValidationError("source family output differs from its independent selected plan")


def _verify_family(context, plan, family, group, claims, work, record_claims=None):
    _family_identity(plan, family, group)
    layout, family_by_name = context.layout, group.family_row
    manifest = layout.manifest(family_by_name)
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
            entity = family.root.values_by_field[layout.definition.entity_field]
            with (
                closing(_expected_records(layout, family, work, plan, record_claims)) as expected,
                closing(_query_rows(layout, group, family, claims, work)) as query,
            ):
                return check_family_records(
                    layout,
                    family_by_name,
                    verified,
                    expected,
                    root_key_contract=root_key_contract_hash(layout.definition),
                    root_key=(plan.typed_key[1], plan.typed_key[0]),
                    entity=("npi", entity, digest_text("entity:npi", entity)),
                    query_rows=query,
                )
    finally:
        work.used -= reservation


def _source_inputs(context, admission, final_plan, work):
    selected = work.write(originals._selected_plans(final_plan, work))
    with closing(_sorted(work, originals._all_occurrences(context, admission, work), lambda row: row[:2])) as rows:
        occurrences = work.write(rows)
    fields = originals.codec.fields_by_collection(context.request.definition)
    try:
        with closing(originals._families(context, selected, occurrences, work, fields)) as families:
            yield from families
    finally:
        work.remove(selected)
        work.remove(occurrences)


def _family_claims(context, admission, final_plan, workers, work, totals, digest, prepared=None):
    record_claims = prepared if type(prepared) is SourceRecordClaims else None
    if record_claims is not None:
        prepared = record_claims.prepared
    state = SimpleNamespace(is_complete=False)
    with (
        closing(
            _source_inputs(context, admission, final_plan, work)
            if prepared is None
            else prepared.families(check=work.current)
        ) as families,
        closing(
            outputs._groups(context.source, context.layout, workers, work, context.request.complete_scope, state)
        ) as groups,
    ):
        for original, group in zip_longest(families, groups, fillvalue=_MISSING):
            work.current()
            if original is _MISSING or group is _MISSING:
                raise FamilyBlockValidationError("source selected plan and original output coverage differ")
            plan, family = original
            with _Replay(work) as claims:
                child_count, root_scalars, child_scalars = _verify_family(
                    context, plan, family, group, claims, work, record_claims
                )
                for name, count in zip(_COUNTS, (1, child_count, root_scalars, child_scalars), strict=True):
                    totals[name] += count
                _add(digest, "family", (plan.typed_key, dict(group.family_row)))
                yield from claims.rows()
        if not state.is_complete or totals["family_count"] != final_plan.counts_by_kind["source"]:
            raise FamilyBlockValidationError("source comparison did not exhaust complete selected input/output")


def _dictionary_rows(keys, claims, work, digest, totals):
    claim = next(claims, None)
    for key_hash, key_text, rank, variant in keys:
        work.current()
        _integer(rank, "original dictionary rank")
        _integer(variant, "original dictionary variant")
        row = rank, variant, key_hash, key_text
        _add(digest, "dictionary", row)
        identity = rank, variant
        if claim is not None and claim[:2] < identity:
            raise FamilyBlockValidationError("original query rank has no owner dictionary entry")
        if claim is not None and claim[:2] == identity:
            if claim != row:
                raise FamilyBlockValidationError("original query rank resolves to different key bytes")
            totals["used_query_keys"] += 1
            previous = claim
            claim = next(claims, None)
            while claim is not None and claim[:2] == identity:
                if claim != previous:
                    raise FamilyBlockValidationError("original query rank has conflicting complete keys")
                work.current()
                claim = next(claims, None)
        yield row
    if claim is not None:
        raise FamilyBlockValidationError("original query rank lies outside the complete dictionary")


def _sampled_family_claims(context, workers, work, totals, digest, prepared, audit):
    state = SimpleNamespace(is_complete=False)
    with (
        closing(prepared.families(check=work.current, selection=audit.selection)) as families,
        closing(
            outputs._groups(context.source, context.layout, workers, work, context.request.complete_scope, state, audit)
        ) as groups,
    ):
        for group in groups:
            work.current()
            if group.is_semantically_selected:
                original = next(families, None)
                if original is None:
                    raise FamilyBlockValidationError("semantic audit selected original family is missing")
                plan, family = original
                with _Replay(work) as sampled_claims:
                    counts = _verify_family(context, plan, family, group, sampled_claims, work)
                audit.audited(counts)
            _add(digest, "family", (group.header["typed_key"], dict(group.family_row)))
        if next(families, None) is not None or not state.is_complete:
            raise FamilyBlockValidationError("semantic audit requires complete original/output EOF")
    totals.update(audit.selection.counts)
    yield from audit.claims()


def _comparison_inputs(context, admission, final_plan, workers, work, prepared, semantic_selection):
    if type(work) is not PlanWork:
        raise ValueError("original comparison requires explicit accounted PlanWork")
    record_claims = prepared if type(prepared) is SourceRecordClaims else None
    if record_claims is not None:
        if semantic_selection is not None:
            raise ValueError("sampled SOURCE record-claim integration remains disabled")
        record_claims.check_scope(context, admission, final_plan)
        prepared = record_claims.prepared
    if prepared is not None:
        if type(prepared) is not originals.PreparedSourceFamilies:
            raise ValueError("original comparison requires its typed prepared SOURCE replay")
        prepared.check_scope(context, admission, final_plan)
    if semantic_selection is not None:
        if type(semantic_selection) is not SourceSemanticSelection or prepared is None:
            raise ValueError("sampled original comparison requires its typed prepared semantic selection")
        semantic_selection.check_scope(context, admission, final_plan, prepared)
        semantic_selection._retain_outside(work.root)
        semantic_selection.check_outputs(workers)
    return (
        _input_scope(context, admission, final_plan, workers, work),
        _worker_inputs(context.layout, workers),
        prepared,
        record_claims,
    )


def prove_original_outputs(
    context,
    admission,
    final_plan,
    workers,
    original_query_keys,
    *,
    work,
    prepared=None,
    semantic_selection=None,
):
    """Compare SOURCE originals, qualifying only the explicitly optional sampled mode.

    Run this synchronous CPU/file pass off the coordinator loop. All original
    owners remain live; hidden identity joins and final acceptance stay separate.
    A SourceRecordClaims in prepared binds its complete receipt for native comparison.
    """
    inputs, worker_inputs, prepared, record_claims = _comparison_inputs(
        context, admission, final_plan, workers, work, prepared, semantic_selection
    )
    prepared = prepared if record_claims is None else record_claims
    bounded = replace(work, limits=replace(work.limits, memory_bytes=max(1, work.limits.memory_bytes // 16)))
    scratch, primary, comparison, audit = _Scratch(bounded), None, None, None
    try:
        for _ in range(5):
            scratch._reserve_entry()
        producer = _producer(context)
        digest = hashlib.sha256(b"custom-import/source-original-comparison/v1\0")
        _add(digest, "inputs", (producer, inputs, worker_inputs))
        totals = dict.fromkeys((*_COUNTS, "used_query_keys"), 0)
        if semantic_selection is None:
            claims = _family_claims(context, admission, final_plan, workers, scratch, totals, digest, prepared)
        else:
            audit = SourceOutputAudit(semantic_selection, scratch)
            claims = _sampled_family_claims(context, workers, scratch, totals, digest, prepared, audit)
        with (
            closing(_sorted(scratch, claims, lambda claim: claim)) as ordered,
            closing(_input_iterator(original_query_keys)) as keys,
        ):
            dictionary_count = check_key_dictionary(_dictionary_rows(keys, ordered, scratch, digest, totals))
        if _worker_inputs(context.layout, workers) != worker_inputs:
            raise FamilyBlockValidationError("source worker provenance changed during original comparison")
        if prepared is not None:
            prepared.check_scope(context, admission, final_plan)
        provenance = producer, inputs, worker_inputs, dictionary_count
        comparison = _comparison_result(context, final_plan, record_claims, audit, totals, digest, provenance)
    except BaseException as error:
        primary = error
    primary = _close_comparison(audit, scratch, primary)
    if primary is not None:
        raise primary
    return comparison


def _comparison_result(context, final_plan, record_claims, audit, totals, digest, provenance):
    """Bind complete expected identities into the existing SOURCE proof digest."""
    semantic_audit = None if audit is None else audit.receipt()
    if semantic_audit is not None:
        semantic_audit.check()
        _add(digest, "semantic_audit", semantic_audit.sha256)
    counts = tuple((name, totals[name]) for name in _COUNTS)
    record_claims_sha256 = None
    if record_claims is not None:
        record_claims.seal(dict(counts))
        record_claims.check_scope(context, record_claims.prepared.admission, final_plan)
        if dict(counts) != dict(record_claims.counts):
            raise FamilyBlockValidationError("SOURCE complete record declaration counts differ")
        record_claims_sha256 = record_claims.sha256
        _add(digest, "source_record_claims", record_claims_sha256)
    _add(digest, "counts", counts)
    _add(digest, "source_records", context.source.source.counts[0])
    producer, inputs, worker_inputs, dictionary_count = provenance
    return SourceOriginalComparison(
        producer,
        inputs,
        worker_inputs,
        counts,
        context.source.source.counts[0],
        final_plan.counts_by_kind["retained"],
        dictionary_count,
        totals["used_query_keys"],
        digest.digest(),
        semantic_audit,
        record_claims_sha256,
    )


def _close_comparison(audit, scratch, primary):
    if audit is not None:
        try:
            audit.close()
        except BaseException as error:
            primary = _source_cleanup_failure(primary, error)
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    return primary
