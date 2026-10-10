# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete frozen content commitments with bounded, reproducible value audits."""

from __future__ import annotations

import hashlib
import heapq
from collections import defaultdict, deque
from contextlib import closing

from sqlalchemy import (
    BigInteger,
    LargeBinary,
    SmallInteger,
    String,
    any_,
    bindparam,
    case,
    cast,
    func,
    inspect,
    literal,
    select,
    true,
    tuple_,
)
from sqlalchemy import column as sql_column
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from db.models.custom_import import (
    CustomImportBuildCandidateContext,
    CustomImportBuildFamily,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportEntityBinding,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportField,
    CustomImportGenerationFamily,
    CustomImportPack,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportRootScalar,
    CustomImportSelectionProfile,
    CustomImportWinner,
)
from process.custom_import import build_graph, publication
from process.custom_import.definition import MAX_SELECTION_PROFILES
from process.custom_import.runner_codec import digest_text, fields_by_collection, payload_values
from process.custom_import.runner_graph import verify_stored_child
from process.custom_import.runner_types import CandidateRunnerError, StoredCandidateChild

MATERIALIZATION_CONTRACT = "custom-import/materialization/v2"
SAMPLE_CONTRACT = "custom-import/verification-sample/v1"
ROOT_SAMPLE_LIMIT = 64
CHILD_SAMPLE_LIMIT = 32
WINNER_SAMPLE_LIMIT = 4
WINNER_CANDIDATES = 16
COMPLETE_FAMILY_CHILD_LIMIT = 128
COMPLETE_WINNER_GROUP_LIMIT = 256


def _output():
    """Load the existing bounded readers after their module has initialized."""
    from process.custom_import import build_output

    return build_output


def _seed(generation, proof):
    """Use protected database freeze identities so retries select the same rows."""
    digest = publication._new_digest(SAMPLE_CONTRACT)
    publication._add_digest_record(
        digest,
        "frozen_attempt",
        {
            "generation": generation.generation_id,
            "build": proof.build_id,
            **{
                name: publication._json_value(getattr(proof, name))
                for name in ("source_frozen_at", "graph_frozen_at", "output_frozen_at")
            },
        },
    )
    return digest.digest()


class _Sample:
    """Retain the smallest seeded key priorities without retaining the population."""

    def __init__(self, seed, limit):
        self.seed, self.limit, self.rows = seed, limit, []

    def add(self, key, value):
        """Keep a bounded deterministic sample using immutable semantic keys."""
        rank = int.from_bytes(hashlib.sha256(self.seed + key).digest(), "big")
        item = (-rank, key, value)
        if len(self.rows) < self.limit:
            heapq.heappush(self.rows, item)
        elif item > self.rows[0]:
            heapq.heapreplace(self.rows, item)

    def values(self):
        """Return the selected values in stable semantic-key order."""
        return [item[2] for item in sorted(self.rows, key=lambda item: item[1])]


def _aggregate(session, request, build_id, query):
    """Compute bounded aggregate results against the exact frozen relation binding."""
    output = _output()
    with output._read_transaction(session, request, build_id) as (_build, deadline):
        models = output._build_storage_models(session, build_id)[0]
        output._prepare_read(session, request, deadline)
        rows = session.execute(query(models)).all()
        output._require_budget(deadline)
        return rows


def _coverage_page(statement, keys, after):
    """Bound numeric native-key work before any grouping or winner lookup."""
    if after is not None:
        cursor_terms = tuple(bindparam(f"coverage_after_{index}", value) for index, value in enumerate(after))
        statement = statement.where(keys[0] >= cursor_terms[0], tuple_(*keys) > tuple_(*cursor_terms))
    page = (
        statement.order_by(*keys)
        .limit(_output().MAX_BATCH_ROWS)
        .cte("coverage_page")
        .prefix_with("MATERIALIZED", dialect="postgresql")
    )
    page_keys = tuple(page.c[key.key] for key in keys)
    tail = select(*page_keys).order_by(*(key.desc() for key in page_keys)).limit(1).cte("coverage_tail")
    return page, tail


def _coverage_aggregate_pages(session, request, build_id, query, *, key_count, count_index):
    """Return only grouped counters; require a progressing full cursor and explicit EOF."""
    after = None
    while True:
        rows = _aggregate(session, request, build_id, lambda models: query(models, after))
        if not rows:
            return
        cursor_values = tuple(rows[0][-key_count:])
        counts = [row[count_index] for row in rows]
        if (
            len(cursor_values) != key_count
            or any(type(value) is not int or value <= 0 for value in cursor_values)
            or (after is not None and cursor_values <= after)
            or any(tuple(row[-key_count:]) != cursor_values for row in rows)
            or any(type(count) is not int or count <= 0 for count in counts)
            or sum(counts) > _output().MAX_BATCH_ROWS
        ):
            raise CandidateRunnerError("frozen coverage aggregate page is incomplete")
        yield from rows
        after = cursor_values


def _population_query(models, build_id, generation, *, child, after):
    """Count proven selected identities without rereading SOURCE revisions or payloads."""
    # The matched frozen structural proof already closes every revision lookup,
    # owner and edge; these counters retain the exact sampling populations.
    member, edge = models[CustomImportGenerationFamily], models[CustomImportFamilyChild]
    plan = models[CustomImportBuildFamily]
    if child:
        keys = (edge.family_revision_id, edge.collection_slot, edge.child_revision_id)
        statement = (
            select(*keys, edge.root_record_id)
            .select_from(edge)
            .join(
                member,
                (member.family_revision_id == edge.family_revision_id)
                & (member.dataset_id == edge.dataset_id)
                & (member.schema_revision_id == edge.schema_revision_id)
                & (member.root_record_id == edge.root_record_id),
            )
        )
    else:
        keys = (member.root_record_id,)
        statement = select(member.family_revision_id, literal(0).label("collection_slot"), member.root_record_id)
    statement = statement.where(
        member.generation_id == generation.generation_id,
        member.dataset_id == generation.dataset_id,
        member.definition_revision_id == generation.definition_revision_id,
        member.schema_revision_id == generation.schema_revision_id,
    )
    page, tail = _coverage_page(statement, keys, after)
    groups = (page.c.collection_slot, plan.selection_kind)
    return (
        select(*groups, func.count(), func.min(page.c.root_record_id), *tail.c)
        .select_from(page)
        .outerjoin(
            plan,
            (plan.build_id == build_id)
            & (plan.root_record_id == page.c.root_record_id)
            & (plan.family_revision_id == page.c.family_revision_id),
        )
        .join(tail, true())
        .group_by(*groups, *tail.c)
    )


def _population_coverage(session, request, build_id, generation, proof, *, child):
    """Keep all-origin populations exact while retaining separate retained-reader totals."""
    populations, retained = {}, {}
    query = lambda models, after: _population_query(models, build_id, generation, child=child, after=after)
    for slot, origin, count, root, *_cursor in _coverage_aggregate_pages(
        session, request, build_id, query, key_count=3 if child else 1, count_index=2
    ):
        if (
            origin not in {"source", "retained"}
            or type(slot) is not int
            or not (1 <= slot <= 32767 if child else slot == 0)
            or type(root) is not int
            or root <= 0
        ):
            raise CandidateRunnerError("frozen revision selection plan is incomplete")
        target_counts = (populations, retained) if origin == "retained" else (populations,)
        for counts_by_slot in target_counts:
            previous, representative = counts_by_slot.get(slot, (0, root))
            counts_by_slot[slot] = previous + count, min(representative, root)
    expected = proof.family_child_count if child else proof.family_count
    if sum(count for count, _root in populations.values()) != expected:
        raise CandidateRunnerError("frozen revision coverage differs from exact structural counts")
    return populations, retained


def _root_coverage_query(models, build_id, generation):
    """Page retained family keys while exposing missing root lookups as null metadata."""
    member, family = models[CustomImportGenerationFamily], models[CustomImportFamilyRevision]
    revision = models[CustomImportRootRevision]
    plan = models[CustomImportBuildFamily]
    statement = (
        select(
            member.family_revision_id,
            literal(0),
            member.root_record_id,
            revision.root_revision_id,
            func.octet_length(revision.canonical_payload),
            case((plan.selection_kind == "source", False), (plan.selection_kind == "retained", True)),
        )
        .select_from(member)
        .outerjoin(
            plan,
            (plan.build_id == build_id)
            & (plan.root_record_id == member.root_record_id)
            & (plan.family_revision_id == member.family_revision_id),
        )
        .outerjoin(
            family,
            (family.family_revision_id == member.family_revision_id)
            & (family.dataset_id == member.dataset_id)
            & (family.schema_revision_id == member.schema_revision_id)
            & (family.root_record_id == member.root_record_id)
            & (family.producing_execution_id == generation.execution_id)
            & (family.producing_fence == generation.producing_fence)
            & (family.producing_token_sha256 == generation.producing_token_sha256),
        )
        .outerjoin(
            revision,
            (revision.root_revision_id == family.root_revision_id)
            & (revision.dataset_id == member.dataset_id)
            & (revision.definition_revision_id == member.definition_revision_id)
            & (revision.schema_revision_id == member.schema_revision_id)
            & (revision.root_record_id == member.root_record_id),
        )
        .where(
            member.generation_id == generation.generation_id,
            member.dataset_id == generation.dataset_id,
            member.definition_revision_id == generation.definition_revision_id,
            member.schema_revision_id == generation.schema_revision_id,
            plan.selection_kind == "retained",
        )
    )
    return statement, (member.family_revision_id,), ()


def _child_coverage_query(models, build_id, generation):
    """Page retained edge keys without hiding a missing child revision."""
    member, edge = models[CustomImportGenerationFamily], models[CustomImportFamilyChild]
    revision = models[CustomImportChildRevision]
    plan = models[CustomImportBuildFamily]
    statement = (
        select(
            edge.family_revision_id,
            edge.collection_slot,
            member.root_record_id,
            edge.child_revision_id,
            func.octet_length(revision.canonical_payload),
            case((plan.selection_kind == "source", False), (plan.selection_kind == "retained", True)),
        )
        .select_from(edge)
        .join(
            member,
            (member.family_revision_id == edge.family_revision_id)
            & (member.dataset_id == edge.dataset_id)
            & (member.schema_revision_id == edge.schema_revision_id)
            & (member.root_record_id == edge.root_record_id),
        )
        .outerjoin(
            plan,
            (plan.build_id == build_id)
            & (plan.root_record_id == member.root_record_id)
            & (plan.family_revision_id == member.family_revision_id),
        )
        .outerjoin(
            revision,
            (revision.child_revision_id == edge.child_revision_id)
            & (revision.dataset_id == edge.dataset_id)
            & (revision.definition_revision_id == member.definition_revision_id)
            & (revision.schema_revision_id == edge.schema_revision_id)
            & (revision.collection_slot == edge.collection_slot)
            & (revision.root_record_id == edge.root_record_id),
        )
        .where(
            member.generation_id == generation.generation_id,
            member.dataset_id == generation.dataset_id,
            member.definition_revision_id == generation.definition_revision_id,
            member.schema_revision_id == generation.schema_revision_id,
            plan.selection_kind == "retained",
        )
    )
    keys = (edge.family_revision_id, edge.collection_slot, edge.child_revision_id)
    return statement, keys, ()


def _context_coverage_query(models, build_id, generation, after):
    """Count complete numeric context pages and expose every missing winner group."""
    context, winner = models[CustomImportBuildCandidateContext], models[CustomImportWinner]
    statement = select(
        context.candidate_context_id, context.profile_slot, context.entity_binding_id, context.context_key_sha256
    ).where(context.build_id == build_id)
    page, tail = _coverage_page(statement, (context.candidate_context_id,), after)
    return (
        select(
            page.c.profile_slot,
            func.count(),
            func.count().filter(winner.profile_slot.is_(None)),
            tail.c.candidate_context_id,
        )
        .select_from(page)
        .outerjoin(
            winner,
            (winner.generation_id == generation.generation_id)
            & (winner.dataset_id == generation.dataset_id)
            & (winner.definition_revision_id == generation.definition_revision_id)
            & (winner.schema_revision_id == generation.schema_revision_id)
            & (winner.profile_slot == page.c.profile_slot)
            & (winner.entity_binding_id == page.c.entity_binding_id)
            & (winner.context_key_sha256 == page.c.context_key_sha256),
        )
        .join(tail, true())
        .group_by(page.c.profile_slot, tail.c.candidate_context_id)
    )


def _coverage_read_budget():
    """Reserve pending numeric identities separately from metadata-only transport."""
    output = _output()
    return output._ReadPage(reserve_bytes=128 * output.MAX_BATCH_ROWS, variable_columns=())


def _revision_batches(request, metadata, populations):
    """Account for retained revisions and admit their payload work within existing caps."""
    field_count = max(
        1,
        len(request.definition.root_fields),
        *(len(fields) for fields in fields_by_collection(request.definition).values()),
    )
    row_limit = max(1, _output().MAX_BATCH_ROWS // max(2, field_count))
    identifiers, payload_bytes = [], 0
    for _family, collection, root, revision, size, retained in metadata:
        if type(revision) is not int or revision <= 0 or type(size) is not int or size < 0:
            raise CandidateRunnerError("frozen revision coverage lookup is incomplete")
        if retained is not True:
            raise CandidateRunnerError("frozen revision selection plan is incomplete")
        if size > request.page_byte_limit:
            raise CandidateRunnerError("one build record exceeds the admitted byte page")
        count, representative = populations.get(collection, (0, root))
        populations[collection] = count + 1, min(representative, root)
        if identifiers and (len(identifiers) == row_limit or payload_bytes + size > request.page_byte_limit):
            yield tuple(identifiers)
            identifiers.clear()
            payload_bytes = 0
        identifiers.append(revision)
        payload_bytes += size
    if identifiers:
        yield tuple(identifiers)


def _presence_coverage(session, request, build_id, generation, proof, *, child):
    """Keep all selected accounting exact and close retained scalar presence per batch."""
    # Generation/root uniqueness and revision ownership make complete native-key
    # pages disjoint revision sets. Preserve multiplicity; do not deduplicate IDs.
    output = _output()
    query = _child_coverage_query if child else _root_coverage_query
    populations, expected_retained = _population_coverage(session, request, build_id, generation, proof, child=child)
    if not expected_retained:
        return [(slot, count, root) for slot, (count, root) in sorted(populations.items())]
    retained_by_slot = {}
    with closing(
        output._output_rows(
            session,
            request,
            build_id,
            lambda models: query(models, build_id, generation),
            bounds=_coverage_read_budget(),
        )
    ) as metadata:
        for identifiers in _revision_batches(request, metadata, retained_by_slot):
            expected, matched, actual = _aggregate(
                session,
                request,
                build_id,
                lambda models: _present_scalars_query(models, generation, child=child, revision_ids=identifiers),
            )[0]
            if expected != matched or expected != actual:
                raise CandidateRunnerError("frozen scalar coverage is incomplete")
    if retained_by_slot != expected_retained:
        raise CandidateRunnerError("frozen revision coverage differs from exact structural counts")
    return [(slot, count, root) for slot, (count, root) in sorted(populations.items())]


def _context_coverage(session, request, build_id, generation):
    """Merge bounded SQL group counts without transporting individual candidate rows."""
    counts_by_profile = defaultdict(int)
    query = lambda models, after: _context_coverage_query(models, build_id, generation, after)
    for profile, count, missing, _cursor in _coverage_aggregate_pages(
        session, request, build_id, query, key_count=1, count_index=1
    ):
        if type(profile) is not int or not 1 <= profile <= 32767 or type(missing) is not int or missing != 0:
            raise CandidateRunnerError("frozen winner group coverage is incomplete")
        counts_by_profile[profile] += count
    return counts_by_profile


def _admitted_revisions(revision_ids):
    """Preserve the exact metadata-admitted identity stream in each bounded SQL lookup."""
    return (
        func.unnest(bindparam("coverage_revision_ids", revision_ids, type_=ARRAY(BigInteger)))
        .table_valued(sql_column("revision_id", BigInteger))
        .render_derived(name="admitted_revisions")
    )


def _eligible_presence_payloads(models, generation, *, child, revision_ids):
    """Decode admitted PK lookups; the frozen metadata sweep proves their membership."""
    revision = models[CustomImportChildRevision if child else CustomImportRootRevision]
    field, pack = CustomImportField, models[CustomImportPack]
    revision_id = revision.child_revision_id if child else revision.root_revision_id
    collection_slot = revision.collection_slot if child else literal(0, SmallInteger)
    admitted = _admitted_revisions(revision_ids)
    statement = select(revision).select_from(admitted).join(revision, revision_id == admitted.c.revision_id)
    join_pack = publication._join_child_revision_pack if child else publication._join_root_revision_pack
    statement = publication._where_pack_attempt(join_pack(statement, pack, models=models), pack, generation)
    return (
        statement.where(
            revision.dataset_id == generation.dataset_id,
            revision.definition_revision_id == generation.definition_revision_id,
            revision.schema_revision_id == generation.schema_revision_id,
            select(field.field_slot)
            .where(
                field.dataset_id == revision.dataset_id,
                field.schema_revision_id == revision.schema_revision_id,
                field.collection_slot == collection_slot,
                field.projection_slot > 0,
            )
            .exists(),
        )
        .with_only_columns(
            revision_id.label("revision_id"),
            collection_slot.label("collection_slot"),
            revision.dataset_id,
            revision.schema_revision_id,
            revision.canonical_payload,
            maintain_column_froms=True,
        )
        .cte("eligible_payloads")
        .prefix_with("MATERIALIZED", dialect="postgresql")
    )


def _decoded_presence_fields(models, generation, *, child, revision_ids):
    """Decode only admitted eligible payloads once before their individual field joins."""
    eligible = _eligible_presence_payloads(models, generation, child=child, revision_ids=revision_ids)
    payload_json = cast(func.replace(eligible.c.canonical_payload, "\\u0000", "\\u0001"), JSONB)
    cells = (
        func.jsonb_array_elements(payload_json["fields"])
        .table_valued(sql_column("value", JSONB))
        .render_derived(name="cells")
    )
    return (
        select(
            eligible.c.revision_id,
            eligible.c.collection_slot,
            eligible.c.dataset_id,
            eligible.c.schema_revision_id,
            cells.c.value["field"].astext.label("field_name"),
            cells.c.value["value"]["state"].astext.label("value_state"),
        )
        .select_from(eligible)
        .join(cells, true())
        .cte("decoded_fields")
        .prefix_with("MATERIALIZED", dialect="postgresql")
    )


def _present_scalars_query(models, generation, *, child, revision_ids):
    """Match retained field/state coverage and count all stored cells, including extras."""
    scalar = models[CustomImportChildScalar if child else CustomImportRootScalar]
    scalar_id = scalar.child_revision_id if child else scalar.root_revision_id
    decoded = _decoded_presence_fields(models, generation, child=child, revision_ids=revision_ids)
    field = CustomImportField
    admitted = _admitted_revisions(revision_ids)
    actual = (
        select(func.count())
        .select_from(admitted)
        .join(scalar, scalar_id == admitted.c.revision_id)
        .correlate(None)
        .scalar_subquery()
    )
    return (
        select(func.count(), func.count(scalar.field_slot).filter(scalar.value_state == decoded.c.value_state), actual)
        .select_from(decoded)
        .join(
            field,
            (field.dataset_id == decoded.c.dataset_id)
            & (field.schema_revision_id == decoded.c.schema_revision_id)
            & (field.collection_slot == decoded.c.collection_slot)
            & (field.field_name == decoded.c.field_name)
            & (field.projection_slot > 0),
        )
        .outerjoin(scalar, (scalar_id == decoded.c.revision_id) & (scalar.field_slot == field.field_slot))
        .where(decoded.c.value_state.is_distinct_from("missing"))
    )


def _check_coverage(proof, children, profiles, contexts):
    """Subset checks plus native uniqueness and equal cardinalities prove completeness."""
    population_by_slot = {0: proof.family_count, **{slot: count for slot, count, _root in children}}
    if (
        proof.root_count != proof.family_count
        or proof.generation_family_count != proof.family_count
        or sum(count for slot, count in population_by_slot.items() if slot) != proof.family_child_count
    ):
        raise CandidateRunnerError("frozen family coverage is incomplete")
    counts_by_profile = {slot: (count, groups) for slot, count, groups in contexts}
    if set(counts_by_profile) - {slot for slot, _collection in profiles} or any(
        counts_by_profile.get(slot, (0, 0))[0] != population_by_slot.get(collection or 0, 0)
        for slot, collection in profiles
    ):
        raise CandidateRunnerError("frozen candidate context coverage is incomplete")
    if proof.winner_count != sum(groups for _count, groups in counts_by_profile.values()):
        raise CandidateRunnerError("frozen winner group coverage is incomplete")
    return {slot: counts_by_profile.get(slot, (0, 0))[1] for slot, _collection in profiles}


def _complete_coverage(session, request, build_id, generation, proof, winner_populations):
    """Keep missing scalars, contexts and winner groups outside the sampling policy."""
    # SOURCE page writers close exact field/state coverage; retained copies need
    # payload-derived closure here. Neither proof is replaced by sampled values.
    _presence_coverage(session, request, build_id, generation, proof, child=False)
    children = _presence_coverage(session, request, build_id, generation, proof, child=True)
    profile = CustomImportSelectionProfile
    profiles = _aggregate(
        session,
        request,
        build_id,
        lambda _models: (
            select(profile.profile_slot, profile.context_collection_slot)
            .where(
                profile.dataset_id == request.dataset_id,
                profile.definition_revision_id == request.definition_revision_id,
                profile.schema_revision_id == request.schema_revision_id,
            )
            .order_by(profile.profile_slot)
        ),
    )
    counts_by_profile = _context_coverage(session, request, build_id, generation)
    contexts = [
        (slot, counts_by_profile.get(slot, 0), winner_populations.get(slot, 0))
        for slot in sorted(counts_by_profile.keys() | winner_populations.keys())
    ]
    return children, _check_coverage(proof, children, profiles, contexts)


def _family_query(models, generation, build_id):
    """Read complete family commitments without fetching canonical payloads or scalars."""
    root, family = models[CustomImportRootRecord], models[CustomImportFamilyRevision]
    revision, binding = models[CustomImportRootRevision], models[CustomImportEntityBinding]
    plan = models[CustomImportBuildFamily]
    columns = (
        root.root_record_id,
        root.logical_key_sha256,
        root.key_contract_sha256,
        root.canonical_logical_key,
        family.family_sha256,
        family.child_count,
        revision.payload_sha256,
        revision.source_ordinal,
        binding.adapter_id,
        binding.value_sha256,
        binding.canonical_value,
        plan.selection_kind,
    )
    statement = (
        publication._family_material_statement(generation, models=models)
        .join(plan, (plan.build_id == build_id) & (plan.family_revision_id == family.family_revision_id))
        .order_by(None)
        .with_only_columns(*columns, maintain_column_froms=True)
    )
    return statement, (root.logical_key_sha256,), ()


def _family_document(row, *, effective):
    """Bind all logical family content through its canonical construction-time digest."""
    names = (
        "root_key_sha256",
        "key_contract_sha256",
        "canonical_logical_key",
        "family_sha256",
        "child_count",
        "root_payload_sha256",
        "source_ordinal",
        "adapter_id",
        "value_sha256",
        "canonical_value",
        "selection_kind",
    )
    return {
        name: publication._json_value(value)
        for name, value in zip(names, row[1:], strict=True)
        if not (effective and name in {"source_ordinal", "selection_kind"})
    }


def _winner_query(models, generation):
    """Read complete winner identities without fetching root or child payloads."""
    winner, binding = models[CustomImportWinner], models[CustomImportEntityBinding]
    family, root = models[CustomImportFamilyRevision], models[CustomImportRootRecord]
    child = models[CustomImportChildRevision]
    columns = (
        winner.profile_slot,
        winner.entity_binding_id,
        winner.context_key_sha256,
        winner.family_revision_id,
        winner.context_collection_slot,
        winner.context_child_revision_id,
        binding.adapter_id,
        binding.value_sha256,
        binding.canonical_value,
        root.key_contract_sha256,
        root.logical_key_sha256,
        func.coalesce(child.child_key_sha256, b""),
        family.family_sha256,
    )
    keys = (columns[0], *columns[6:], columns[4], columns[2])
    statement = (
        publication._winner_material_statement(generation, models=models)
        .order_by(None)
        .with_only_columns(*columns, maintain_column_froms=True)
    )
    return statement, keys, ()


def _winner_document(row):
    """Exclude allocation identities while preserving winner context and selected content."""
    names = (
        "profile_slot",
        "context_key_sha256",
        "collection_slot",
        "adapter_id",
        "binding_sha256",
        "binding_value",
        "key_contract_sha256",
        "root_key_sha256",
        "child_key_sha256",
        "family_sha256",
    )
    values = (row[0], row[2], row[4], *row[6:])
    return {name: publication._json_value(value) for name, value in zip(names, values, strict=True)}


def _compact_rows(session, request, build_id, query):
    """Charge only selected variable-width columns in the existing physical reader."""
    output = _output()
    with output._read_transaction(session, request, build_id):
        statement, _keys, _models = query(output._build_storage_models(session, build_id)[0])
        variable_columns = tuple(
            column for column in statement.selected_columns if isinstance(column.type, (LargeBinary, String))
        )
    return output._output_rows(
        session, request, build_id, query, bounds=output._ReadPage(variable_columns=variable_columns)
    )


def _winner_cursor_bounds(statement, request):
    """Reserve driver prefetch, row copies and retained samples before transfer."""
    columns = tuple(statement.selected_columns)
    row_bytes = build_graph._physical_key_bytes(columns)
    widths_by_index = {
        index: column.type.length * (4 if isinstance(column.type, String) else 1)
        for index, column in enumerate(columns)
        if isinstance(column.type, (String, LargeBinary))
    }
    # The asyncpg server cursor prefetches 50 rows, even for fetchmany(1).
    retained = 50 + MAX_SELECTION_PROFILES * WINNER_CANDIDATES + 2
    limit = min(
        request.page_row_limit,
        (build_graph.MAX_BATCH_ROWS - retained) // 2,
        (build_graph.MAX_BATCH_BYTES // row_bytes - retained) // 2,
    )
    if limit < 1:
        raise CandidateRunnerError("winner cursor has no remaining physical page budget")
    return limit, widths_by_index


def _validate_winner_row(winner_row, widths_by_index, previous_key):
    """Check transferred column widths and preserve strict canonical cursor order."""
    for index, width in widths_by_index.items():
        column_value = winner_row[index]
        size = len(column_value.encode("utf-8")) if isinstance(column_value, str) else len(column_value)
        if size > width:
            raise CandidateRunnerError("winner cursor record exceeds its bounded column width")
    winner_key = (winner_row[0], *winner_row[6:], winner_row[4], winner_row[2])
    if previous_key is not None and winner_key <= previous_key:
        raise CandidateRunnerError("frozen winner cursor is not in canonical order")
    return winner_key


def _winner_rows(session, request, build_id, generation):
    """Sort canonical winner identities once, checking live authority before every fetch."""
    with build_graph._read_transaction(session, request, build_id):
        models, _base = build_graph._build_storage_models(session, build_id)
        binding = inspect(models[CustomImportWinner]).selectable.fullname
        statement, keys, _models = _winner_query(models, generation)
        limit, widths_by_index = _winner_cursor_bounds(statement, request)
    if sum(widths_by_index.values()) > request.page_byte_limit:
        # Tiny policies still need metadata-first admission of each actual record.
        yield from _compact_rows(session, request, build_id, lambda current: _winner_query(current, generation))
        return
    previous_key = None
    with session.begin():
        _build, deadline = build_graph._read_authority(session, request, build_id)
        models, _base = build_graph._build_storage_models(session, build_id)
        if inspect(models[CustomImportWinner]).selectable.fullname != binding:
            raise CandidateRunnerError("frozen winner snapshot changed during its read")
        build_graph._prepare_read(session, request, deadline)
        with closing(
            session.execute(
                statement.order_by(*keys).execution_options(stream_results=True, yield_per=limit, max_row_buffer=limit)
            )
        ) as winner_cursor:
            while True:
                _build, deadline = build_graph._read_authority(session, request, build_id)
                models, _base = build_graph._build_storage_models(session, build_id)
                if inspect(models[CustomImportWinner]).selectable.fullname != binding:
                    raise CandidateRunnerError("frozen winner snapshot changed during its read")
                build_graph._prepare_read(session, request, deadline)
                winner_batch = winner_cursor.fetchmany(limit)
                build_graph._require_budget(deadline)
                if len(winner_batch) > limit:
                    raise CandidateRunnerError("winner cursor exceeds its physical page")
                if not winner_batch:
                    break
                for winner_row in winner_batch:
                    previous_key = _validate_winner_row(winner_row, widths_by_index, previous_key)
                    build_graph._require_budget(deadline)
                    yield winner_row
                    build_graph._require_budget(deadline)
                del winner_batch, winner_row
        build_graph._require_budget(deadline)
        session.expunge_all()


def _summaries(session, request, build_id, generation, proof, digests, seed):
    """Hash every family and winner, retaining only bounded audit candidates."""
    roots, winners, family_count, winner_count = _Sample(seed, ROOT_SAMPLE_LIMIT), {}, 0, 0
    root_by_origin, winner_populations = {}, defaultdict(int)
    for row in _compact_rows(session, request, build_id, lambda models: _family_query(models, generation, build_id)):
        for effective, digest in enumerate(digests):
            publication._add_digest_record(
                digest, "family_commitment", _family_document(row, effective=bool(effective))
            )
        roots.add(bytes(row[1]), row[0])
        root_by_origin.setdefault(row[-1], row[0])
        family_count += 1
    with closing(_winner_rows(session, request, build_id, generation)) as rows:
        for row in rows:
            publication._add_digest_record_to_all(digests, "winner_commitment", _winner_document(row))
            sample = winners.setdefault(row[0], _Sample(seed, WINNER_CANDIDATES))
            sample.add(bytes(row[2]) + bytes(row[7]) + bytes(row[10]), tuple(row))
            winner_count += 1
            winner_populations[row[0]] += 1
    if (family_count, winner_count) != (proof.family_count, proof.winner_count):
        raise CandidateRunnerError("compact materialization differs from exact structural counts")
    return sorted(set(roots.values()) | set(root_by_origin.values())), winners, winner_populations


def _child_scopes_query(models, generation, root_ids, collection_slots):
    """Seek both child-ID endpoints for bounded roots and nonempty collection slots."""
    edge, member = models[CustomImportFamilyChild], models[CustomImportGenerationFamily]
    collections = (
        func.unnest(bindparam("sample_collections", collection_slots, type_=ARRAY(SmallInteger)))
        .table_valued(sql_column("collection_slot", SmallInteger))
        .render_derived(name="collections")
    )
    children = select(edge.child_revision_id).where(
        edge.family_revision_id == member.family_revision_id,
        edge.collection_slot == collections.c.collection_slot,
    )
    first = children.order_by(edge.child_revision_id).limit(1).lateral("first_child")
    last = children.order_by(edge.child_revision_id.desc()).limit(1).lateral("last_child")
    return (
        select(
            member.root_record_id,
            member.family_revision_id,
            collections.c.collection_slot,
            first.c.child_revision_id,
            last.c.child_revision_id,
        )
        .select_from(member)
        .join(collections, true())
        .join(first, true())
        .join(last, true())
        .where(
            member.generation_id == generation.generation_id,
            member.root_record_id == any_(bindparam("sample_roots", root_ids, type_=ARRAY(BigInteger))),
        )
    )


def _child_probe_query(models, probes):
    """Seek one child per seeded key probe through the existing membership primary key."""
    edge = models[CustomImportFamilyChild]
    chosen = (
        func.unnest(
            bindparam("sample_families", [probe[1] for probe in probes], type_=ARRAY(BigInteger)),
            bindparam("sample_collections", [probe[2] for probe in probes], type_=ARRAY(SmallInteger)),
            bindparam("sample_pivots", [probe[3] for probe in probes], type_=ARRAY(BigInteger)),
        )
        .table_valued(
            sql_column("family_id", BigInteger),
            sql_column("collection_slot", SmallInteger),
            sql_column("pivot", BigInteger),
            with_ordinality="ordinal",
        )
        .render_derived(name="chosen")
    )
    candidate = (
        select(edge.child_revision_id)
        .where(
            edge.family_revision_id == chosen.c.family_id,
            edge.collection_slot == chosen.c.collection_slot,
            edge.child_revision_id >= chosen.c.pivot,
        )
        .order_by(edge.child_revision_id)
        .limit(1)
        .lateral("sampled")
    )
    return (
        select(chosen.c.ordinal, candidate.c.child_revision_id)
        .select_from(chosen)
        .join(candidate, true())
        .order_by(chosen.c.ordinal)
    )


def _child_probes(scopes, seed):
    """Create capped, deterministic key probes without assuming contiguous child IDs."""
    by_collection = defaultdict(list)
    for scope in sorted(scopes):
        by_collection[scope[2]].append(scope)
    probes = []
    for collection, rows in sorted(by_collection.items()):
        rows.sort(
            key=lambda scope: hashlib.sha256(seed + f"child-scope:{scope[0]}:{collection}".encode("ascii")).digest()
        )
        for ordinal in range(CHILD_SAMPLE_LIMIT):
            root, family, _slot, lower, upper = rows[ordinal % len(rows)]
            key = f"child:{root}:{collection}:{ordinal}".encode("ascii")
            pivot = lower + int.from_bytes(hashlib.sha256(seed + key).digest(), "big") % (upper - lower + 1)
            probes.append((root, family, collection, pivot))
    return probes


def _sample_children(session, request, build_id, generation, roots, collection_slots, seed):
    """Use bounded seeded key probes; the receipt does not claim uniform row sampling."""
    if not roots or not collection_slots:
        return {}
    scopes = _aggregate(
        session, request, build_id, lambda models: _child_scopes_query(models, generation, roots, collection_slots)
    )
    probes = _child_probes(scopes, seed)
    sampled = defaultdict(set)
    if probes:
        actual = _aggregate(session, request, build_id, lambda models: _child_probe_query(models, probes))
        if len(actual) != len(probes) or any(row[0] != index for index, row in enumerate(actual, 1)):
            raise CandidateRunnerError("frozen child sample coverage changed")
        for ordinal, child_id in actual:
            root, _family, collection, _pivot = probes[ordinal - 1]
            sampled[root].add((collection, child_id))
    return sampled


def _sample_family_query(models, build_id, generation, root_ids):
    """Fetch only selected root payloads through the existing canonical family join."""
    statement, keys, row_models = _output()._family_query(models, build_id, generation, None)
    return (
        statement.order_by(None).where(
            models[CustomImportRootRecord].root_record_id
            == any_(bindparam("sample_roots", root_ids, type_=ARRAY(BigInteger)))
        ),
        keys,
        row_models,
    )


def _sample_child_query(models, generation, child_ids):
    """Read chosen child payloads with their exact selected-family provenance."""
    child, record = models[CustomImportChildRevision], models[CustomImportRootRecord]
    statement = (
        publication._family_child_material_statement(generation, models=models)
        .order_by(None)
        .with_only_columns(child, record, maintain_column_froms=True)
        .where(child.child_revision_id == any_(bindparam("sample_children", child_ids, type_=ARRAY(BigInteger))))
    )
    return statement, (child.child_revision_id,), (child, record)


def _verify_children(session, request, registry, build_id, generation, children, bounds):
    """Bulk-verify chosen child payloads and cells against their exact sampled roots."""
    output = _output()
    fields, collections = (
        fields_by_collection(request.definition),
        {slot: name for name, slot in registry.child_collection_slots.items()},
    )
    expected_by_id = {
        child_id: (root_id, slot) for root_id, selected in children.items() for slot, child_id in selected
    }
    if len(expected_by_id) != sum(map(len, children.values())):
        raise CandidateRunnerError("frozen revision sample coverage changed")
    verified = defaultdict(set)
    if expected_by_id:
        query = lambda models: _sample_child_query(models, generation, tuple(sorted(expected_by_id)))
        with closing(output._output_rows(session, request, build_id, query, bounds=bounds)) as child_records:
            for child, root_record in child_records:
                if (
                    expected_by_id.get(child.child_revision_id) != (root_record.root_record_id, child.collection_slot)
                    or child.root_record_id != root_record.root_record_id
                ):
                    raise CandidateRunnerError("sampled child differs from its selected root")
                collection = collections[child.collection_slot]
                child_values = payload_values(
                    fields[collection], child.canonical_payload, label="sampled child payload"
                )
                verify_stored_child(request, root_record, StoredCandidateChild(collection, child, child_values))
                if digest_text("child-payload", child.canonical_payload) != bytes(child.payload_sha256):
                    raise CandidateRunnerError("sampled child payload digest differs")
                verified[child.collection_slot].add(child.child_revision_id)
                del child, root_record, child_values
        deque(
            output._verified_projection_rows(
                session,
                request,
                registry,
                build_id,
                generation,
                child=True,
                revision_ids=tuple(sorted(expected_by_id)),
                bounds=bounds,
            ),
            maxlen=0,
        )
    if set().union(*verified.values()) != set(expected_by_id):
        raise CandidateRunnerError("frozen revision sample coverage changed")
    return verified


def _sample_read_budget(roots, children):
    """Reserve conversion scratch and bounded sample identities before bulk reads."""
    output = _output()
    metadata_bytes = 128 * (len(roots) + sum(map(len, children.values())))
    return output._ReadPage(reserve_bytes=output.MAX_BATCH_BYTES // 2 + metadata_bytes)


def _verify_root_samples(session, request, build_id, generation, roots, bounds):
    """Verify sampled roots in one stream, retaining only bounded revision identities."""
    output = _output()
    verified_roots, revision_ids, family_by_origin = set(), [], {}
    with closing(
        output._output_rows(
            session,
            request,
            build_id,
            lambda models: _sample_family_query(models, build_id, generation, roots),
            bounds=bounds,
        )
    ) as family_records:
        for family_record in family_records:
            family, root, root_record, binding, plan = family_record[1:]
            if (
                plan is None
                or plan.family_revision_id != family.family_revision_id
                or plan.selection_kind not in {"source", "retained"}
            ):
                raise CandidateRunnerError("sampled family differs from selected plan")
            output._verified_root(request, root, root_record, binding)
            if family.child_count <= COMPLETE_FAMILY_CHILD_LIMIT:
                family_by_origin.setdefault(plan.selection_kind, root_record.root_record_id)
            verified_roots.add(root_record.root_record_id)
            revision_ids.append(root.root_revision_id)
            del family_record, family, root, root_record, binding, plan
    if verified_roots != set(roots) or len(revision_ids) != len(verified_roots):
        raise CandidateRunnerError("frozen revision sample coverage changed")
    return tuple(revision_ids), tuple(family_by_origin.values())


def _verify_family_samples(session, request, registry, build_id, generation, roots, bounds):
    """Rehash at most one complete bounded family per source or retained origin."""
    if not roots:
        return
    output = _output()
    with closing(
        output._output_rows(
            session,
            request,
            build_id,
            lambda models: _sample_family_query(models, build_id, generation, roots),
            bounds=output._ReadPage(reserve_bytes=bounds.reserve_bytes, row_limit=1),
        )
    ) as family_records:
        verified_roots = set()
        for family_record in family_records:
            output._verify_family_page(session, request, registry, build_id, generation, [family_record])
            verified_roots.add(family_record[3].root_record_id)
            del family_record
    if verified_roots != set(roots):
        raise CandidateRunnerError("frozen family sample coverage changed")


def _verify_revisions(session, request, registry, build_id, generation, roots, children):
    """Compare every selected cell in bulk; rehash at most two bounded families whole."""
    output = _output()
    if set(children) - set(roots):
        raise CandidateRunnerError("frozen revision sample coverage changed")
    bounds = _sample_read_budget(roots, children)
    revision_ids, complete_roots = _verify_root_samples(session, request, build_id, generation, roots, bounds)
    if revision_ids:
        deque(
            output._verified_projection_rows(
                session, request, registry, build_id, generation, child=False, revision_ids=revision_ids, bounds=bounds
            ),
            maxlen=0,
        )
    verified_children = _verify_children(session, request, registry, build_id, generation, children, bounds)
    _verify_family_samples(session, request, registry, build_id, generation, complete_roots, bounds)
    return verified_children


def _winner_group_counts_query(models, build_id, candidates):
    """Count at most one row beyond the complete-group audit cap per selected key."""
    context = models[CustomImportBuildCandidateContext]
    chosen = (
        func.unnest(
            bindparam("sample_profiles", [winner[0] for winner in candidates], type_=ARRAY(SmallInteger)),
            bindparam("sample_entities", [winner[1] for winner in candidates], type_=ARRAY(BigInteger)),
            bindparam("sample_contexts", [bytes(winner[2]) for winner in candidates], type_=ARRAY(LargeBinary)),
        )
        .table_valued(
            sql_column("profile_slot", SmallInteger),
            sql_column("entity_binding_id", BigInteger),
            sql_column("context_key_sha256", LargeBinary),
        )
        .render_derived(name="chosen")
    )
    bounded = (
        select(context.candidate_context_id)
        .where(
            context.build_id == build_id,
            context.profile_slot == chosen.c.profile_slot,
            context.entity_binding_id == chosen.c.entity_binding_id,
            context.context_key_sha256 == chosen.c.context_key_sha256,
        )
        .order_by(context.candidate_context_id)
        .limit(COMPLETE_WINNER_GROUP_LIMIT + 1)
        .lateral("bounded")
    )
    return (
        select(*chosen.c, func.count(bounded.c.candidate_context_id))
        .select_from(chosen)
        .join(bounded, true())
        .group_by(*chosen.c)
    )


def _verify_winners(session, request, registry, build_id, generation, samples):
    """Recompute bounded complete groups and explicitly omit oversized ranking audits."""
    output = _output()
    candidates = [row for sample in samples.values() for row in sample.values()]
    if not candidates:
        return {}
    counts = _aggregate(
        session, request, build_id, lambda models: _winner_group_counts_query(models, build_id, candidates)
    )
    size_by_group = {(slot, entity, bytes(key)): count for slot, entity, key, count in counts}
    selected = defaultdict(list)
    for row in candidates:
        group = (row[0], row[1], bytes(row[2]))
        if group not in size_by_group:
            raise CandidateRunnerError("sampled winner has no complete candidate group")
        if size_by_group[group] > COMPLETE_WINNER_GROUP_LIMIT or len(selected[row[0]]) == WINNER_SAMPLE_LIMIT:
            continue
        with closing(output._context_rows(session, request, build_id, group=group)) as records:
            group_candidates = output._context_candidates(
                session, request, registry, build_id, group, context_records=records
            )
            winner = output._complete_group_winner(request, registry, generation, group, group_candidates)
        if (winner.family_revision_id, winner.context_collection_slot, winner.context_child_revision_id) != tuple(
            row[3:6]
        ):
            raise CandidateRunnerError("sampled winner differs from complete surviving-tie reduction")
        selected[row[0]].append(group)
    return selected


def _evidence(seed, roots, children, winners, populations):
    """Report actual audited revision/group coverage without claiming an uncapped percentage."""
    selection = publication._new_digest(SAMPLE_CONTRACT)
    coverage_entries = []
    for (kind, slot), population in populations.items():
        sampled_keys = sorted(roots if kind == "root" else (children if kind == "child" else winners).get(slot, ()))
        publication._add_digest_record(
            selection,
            kind,
            {
                "slot": slot,
                "keys": [
                    [publication._json_value(component) for component in key] if isinstance(key, tuple) else key
                    for key in sampled_keys
                ],
            },
        )
        coverage_entries.append(
            {
                "kind": kind,
                "slot": slot,
                "population": population,
                "sampled": len(sampled_keys),
                "capped": len(sampled_keys) < population,
            }
        )
    return {
        "contract": SAMPLE_CONTRACT,
        "seed_sha256": seed.hex(),
        "selection_sha256": selection.hexdigest(),
        "coverage": coverage_entries,
    }


def _audit_materialization(session, request, registry, build_id, generation, proof, digests):
    """Bind exact coverage and reproducible bounded audits to the complete commitments."""
    seed = _seed(generation, proof)
    roots, winner_samples, winner_populations = _summaries(session, request, build_id, generation, proof, digests, seed)
    children, winner_populations = _complete_coverage(session, request, build_id, generation, proof, winner_populations)
    roots = tuple(sorted(set(roots) | {root for _slot, _count, root in children}))
    child_population_by_slot = {slot: count for slot, count, _root in children}
    collection_slots = tuple(sorted(slot for slot, count in child_population_by_slot.items() if count))
    selected_children = _sample_children(session, request, build_id, generation, roots, collection_slots, seed)
    verified_children = _verify_revisions(session, request, registry, build_id, generation, roots, selected_children)
    winners = _verify_winners(session, request, registry, build_id, generation, winner_samples)
    population_by_scope = {
        ("root", 0): proof.root_count,
        **{
            ("child", slot): child_population_by_slot.get(slot, 0)
            for slot in sorted(registry.child_collection_slots.values())
        },
        **{("winner", slot): population for slot, population in sorted(winner_populations.items())},
    }
    return _evidence(seed, roots, verified_children, winners, population_by_scope)


def frozen_materialization(session, request, registry, build_id, generation, proof):
    """Seal complete canonical commitments after exact coverage and bounded redundant audits."""
    output = _output()
    with output._read_transaction(session, request, build_id) as (build, _deadline):
        output._proof_matches(build, generation, proof)
    source_digest = output._source_digest(session, request, build_id, generation.capture_bundle_id)
    candidate, count = output._candidate_digest(session, request, build_id, bounds=output._ReadPage(physical=True))
    if (
        source_digest != bytes(generation.source_bundle_sha256)
        or candidate != bytes(generation.candidate_sha256)
        or count != proof.family_count
        or count != generation.root_count
        or count != generation.family_count
    ):
        raise CandidateRunnerError("frozen generation header differs from its real source and selected roots")
    digests = (
        publication._new_digest("generation-materialization/v2"),
        publication._new_digest("generation-effective-output/v2"),
    )
    output._identity_material(session, request, build_id, generation, *digests)
    if output._shape_material(session, request, build_id, digests) != proof.profile_count:
        raise CandidateRunnerError("frozen profile count differs from structural proof")
    output._attempt_material(session, request, build_id, digests[0])
    evidence = _audit_materialization(session, request, registry, build_id, generation, proof, digests)
    publication._add_digest_record(digests[0], "verification_evidence", evidence)
    return publication._Materialization(
        source_digest,
        *(digest.digest() for digest in digests),
        *(getattr(proof, name) for name in output._COUNT_NAMES),
        materialization_contract=MATERIALIZATION_CONTRACT,
        verification_evidence=evidence,
    )
