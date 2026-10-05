# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy outcomes from bounded native summaries of one admitted snapshot."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import and_, case, func, or_, select, true, tuple_, union_all
from sqlalchemy.orm import aliased

from db.models.custom_import import CustomImportBuildOccurrence, CustomImportChildCollection, CustomImportRejection
from process.custom_import.build_graph import (
    _build_storage_models,
    _prepare_read,
    _read_transaction,
    _renew_while_reading,
    _require_budget,
    _session,
    _snapshot,
)
from process.custom_import.build_source import SourceBuildRequest
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.runner_types import CandidateRunnerError

_FIELD_CODES = frozenset(
    {"required_field_missing", "required_field_null", "field_type_invalid", "field_storage_invalid"}
)
_UNKEYED_ROOT_CODES = frozenset({"root_not_object", "root_key_missing"})
_ROOT_CODES = _FIELD_CODES | _UNKEYED_ROOT_CODES | {"entity_binding_invalid"}
_CHILD_CANDIDATE_CODES = frozenset({"child_not_object", "orphan_child"})
_CHILD_RESOLVED_CODES = frozenset({"duplicate_child_key", "child_membership_missing"})
_CHILD_CODES = _FIELD_CODES | _CHILD_CANDIDATE_CODES | {"child_key_missing"} | _CHILD_RESOLVED_CODES
_ADMITTED_PHASES = frozenset({"graph", "rejected", "output", "verifying", "verified"})
_CODE_BITS = {
    code: 1 << index
    for index, code in enumerate(
        sorted((_ROOT_CODES | _CHILD_CODES | {"duplicate_root_key"}) - _UNKEYED_ROOT_CODES - _CHILD_CANDIDATE_CODES)
    )
}
# Fourteen fixed summary columns, using the frozen reader's per-column reserve.
_SUMMARY_BYTES = 14 * 64


@dataclass(frozen=True)
class SourceOutcomeCounts:
    """Source-family outcomes, excluding retained families and candidate errors."""

    accepted_family_count: int
    rejection_count: int


@dataclass(frozen=True)
class _CountScan:
    session: object
    request: SourceBuildRequest
    build_id: int


@dataclass
class _FamilySummary:
    raw_hash: bytes
    raw_key: str
    collision: bool
    root_count: int
    first_root_id: int | None
    root_code: str | None
    root_record_id: int | None
    root_revision_id: int | None
    typed_duplicate: bool
    child_codes: int

    @classmethod
    def from_partial(cls, partial):
        """Keep one boundary family's fixed fields, never its source records."""
        return cls(*(getattr(partial, name) for name in cls.__dataclass_fields__))

    def merge(self, partial):
        """Fold adjacent pieces while preserving the earliest root's evidence."""
        self.collision |= bool(partial.collision) or self.raw_key != partial.raw_key
        self.root_count = min(2, self.root_count + partial.root_count)
        self.child_codes |= partial.child_codes
        if partial.first_root_id is not None and (
            self.first_root_id is None or partial.first_root_id < self.first_root_id
        ):
            for name in ("first_root_id", "root_code", "root_record_id", "root_revision_id", "typed_duplicate"):
                setattr(self, name, getattr(partial, name))


@dataclass
class _OutcomeTotal:
    accepted: int = 0
    rejected: int = 0
    family: _FamilySummary | None = None

    def finish_family(self):
        """Count each raw family's distinct errors before typed collisions."""
        family = self.family
        if family is None or family.root_count == 0:
            self.family = None
            return
        if family.collision:
            raise CandidateRunnerError("source outcome raw-key digest collision")
        codes = family.child_codes | _CODE_BITS.get(family.root_code, 0)
        if family.root_count > 1:
            codes |= _CODE_BITS["duplicate_root_key"]
        if codes:
            self.rejected += codes.bit_count()
        elif family.root_record_id is None or family.root_revision_id is None:
            raise CandidateRunnerError("accepted source outcome lacks a typed root revision")
        elif family.typed_duplicate:
            self.rejected += 1
        else:
            self.accepted += 1
        self.family = None

    def add(self, partial):
        """Validate every source partial, including those without a root."""
        if partial.bad_collection:
            raise CandidateRunnerError("source outcome collection differs from the definition")
        if partial.bad_code:
            raise CandidateRunnerError("source outcome contains an unknown or unbacked rejection code")
        self.rejected += partial.malformed
        if partial.raw_hash is None:
            return
        if self.family is not None and self.family.raw_hash == partial.raw_hash:
            self.family.merge(partial)
        else:
            self.finish_family()
            self.family = _FamilySummary.from_partial(partial)

    def result(self):
        """Close the last family and return immutable source-only counts."""
        self.finish_family()
        return SourceOutcomeCounts(self.accepted, self.rejected)

    def reserved_bytes(self):
        """Reserve only the fixed summary and raw key carried across pages."""
        return 0 if self.family is None else _SUMMARY_BYTES + len(self.family.raw_key.encode("utf-8"))


def _source_scope(occurrence, build_id, maximum_id):
    return (
        occurrence.build_id == build_id,
        occurrence.origin == "source",
        occurrence.occurrence_id <= maximum_id,
    )


def _position(keys, cursor, *, after):
    if cursor is None:
        return true()
    columns = tuple_(*keys)
    values = tuple(cursor[-len(keys) :])
    return columns > values if after else columns <= values


def _indexed_source_rows(occurrence, build_id, maximum_id, after, unkeyed, through=None):
    """Bound keyed prefixes and the separate all-source occurrence-ID pass."""
    keys = (
        (occurrence.occurrence_id,)
        if unkeyed
        else (
            occurrence.raw_parent_key_sha256,
            occurrence.occurrence_id,
        )
    )
    limit = MAX_BATCH_ROWS // 2
    raw_bytes = func.coalesce(func.octet_length(occurrence.raw_parent_key_canonical), 0)
    if unkeyed:
        raw_bytes = case((occurrence.raw_parent_key_sha256.is_(None), raw_bytes), else_=0)
    prefix = (
        select(
            occurrence.raw_parent_key_sha256.label("raw_hash"), occurrence.occurrence_id, raw_bytes.label("raw_bytes")
        )
        .where(
            *_source_scope(occurrence, build_id, maximum_id),
            _position(keys, after, after=True),
            true() if through is None else _position(keys, through, after=False),
        )
        .order_by(*keys)
        .limit(limit)
    )
    if unkeyed:
        return prefix.subquery()
    arms = [
        select(prefix.where(occurrence.record_kind == kind, occurrence.raw_parent_key_sha256.is_not(None)).subquery())
        for kind in ("root", "child")
    ]
    return union_all(*arms).subquery()


def _metadata_query(build_id, models, maximum_id, after, unkeyed, available_bytes):
    """Merge two indexed prefixes; all metadata work stays within the row cap."""
    candidates = _indexed_source_rows(models[CustomImportBuildOccurrence], build_id, maximum_id, after, unkeyed)
    metadata_keys = (candidates.c.occurrence_id,) if unkeyed else (candidates.c.raw_hash, candidates.c.occurrence_id)
    metadata = (
        select(candidates)
        .order_by(*metadata_keys)
        .limit(MAX_BATCH_ROWS // 2)
        .cte("source_outcome_metadata")
        .prefix_with("MATERIALIZED")
    )
    order = (metadata.c.occurrence_id,) if unkeyed else (metadata.c.raw_hash, metadata.c.occurrence_id)
    sized = select(
        metadata,
        func.sum(metadata.c.raw_bytes + _SUMMARY_BYTES).over(order_by=order).label("page_bytes"),
        func.row_number().over(order_by=order).label("page_count"),
        func.count()
        .filter(metadata.c.raw_hash.is_(None) if unkeyed else true())
        .over(order_by=order)
        .label("evidence_count"),
    ).cte("sized_outcome_metadata")
    end_order = (sized.c.occurrence_id,) if unkeyed else (sized.c.raw_hash, sized.c.occurrence_id)
    cutoff = (
        select(sized)
        .where(sized.c.page_bytes <= available_bytes)
        .order_by(*(column.desc() for column in end_order))
        .limit(1)
        .subquery()
    )
    totals = (
        select(func.count().label("examined"), func.max(metadata.c.raw_bytes).label("largest_record"))
        .select_from(metadata)
        .subquery()
    )
    return select(totals, cutoff).select_from(totals.outerjoin(cutoff, true()))


def _source_evidence(build_id, models, maximum_id, after, through, unkeyed):
    """Fetch rejection evidence only inside the metadata-admitted index range."""
    occurrence = models[CustomImportBuildOccurrence]
    initial, resolved = aliased(models[CustomImportRejection]), aliased(models[CustomImportRejection])
    window = _indexed_source_rows(occurrence, build_id, maximum_id, after, unkeyed, through)
    identifiers = select(window.c.occurrence_id).cte("source_outcome_ids").prefix_with("MATERIALIZED")
    return (
        select(
            occurrence.occurrence_id,
            occurrence.record_kind,
            occurrence.collection_slot,
            occurrence.raw_parent_key_sha256.label("raw_hash"),
            occurrence.raw_parent_key_canonical.collate("C").label("raw_key"),
            occurrence.root_record_id,
            occurrence.root_revision_id,
            initial.code.label("initial_code"),
            resolved.code.label("resolved_code"),
            or_(
                and_(occurrence.rejection_id.is_not(None), initial.rejection_id.is_(None)),
                and_(occurrence.resolved_rejection_id.is_not(None), resolved.rejection_id.is_(None)),
            ).label("missing_rejection"),
        )
        .select_from(identifiers)
        .join(occurrence, occurrence.occurrence_id == identifiers.c.occurrence_id)
        .outerjoin(initial, initial.rejection_id == occurrence.rejection_id)
        .outerjoin(resolved, resolved.rejection_id == occurrence.resolved_rejection_id)
        .where(occurrence.raw_parent_key_sha256.is_(None) if unkeyed else true())
        .cte("source_outcome_evidence")
        .prefix_with("MATERIALIZED")
    )


def _invalid_code(source):
    root_error = and_(
        source.record_kind == "root",
        or_(
            source.initial_code.not_in(_ROOT_CODES),
            and_(source.raw_hash.is_not(None), source.initial_code.in_(_UNKEYED_ROOT_CODES)),
        ),
    )
    child_error = and_(
        source.record_kind == "child",
        or_(
            source.initial_code.not_in(_CHILD_CODES - _CHILD_RESOLVED_CODES),
            source.resolved_code.not_in(_CHILD_CODES),
            and_(
                source.initial_code.is_(None),
                source.resolved_code.not_in(_CHILD_RESOLVED_CODES | _CHILD_CANDIDATE_CODES),
            ),
        ),
    )
    return or_(source.missing_rejection, root_error, child_error)


def _family_partials(evidence, collection_slots):
    columns = evidence.c
    child_code = case(
        (
            or_(columns.initial_code.in_(_CHILD_CANDIDATE_CODES), columns.resolved_code.in_(_CHILD_CANDIDATE_CODES)),
            None,
        ),
        (columns.initial_code.is_not(None), columns.initial_code),
        else_=columns.resolved_code,
    )
    child_bits = case(
        (columns.record_kind == "child", case(_CODE_BITS, value=child_code, else_=0)),
        else_=0,
    )
    return (
        select(
            columns.raw_hash,
            func.min(columns.raw_key).label("raw_key"),
            (func.min(columns.raw_key) != func.max(columns.raw_key)).label("collision"),
            func.count().filter(columns.record_kind == "root").label("root_count"),
            func.min(columns.occurrence_id).filter(columns.record_kind == "root").label("first_root_id"),
            func.bit_or(child_bits).label("child_codes"),
            func.count()
            .filter(and_(columns.record_kind == "root", columns.initial_code.in_(_UNKEYED_ROOT_CODES)))
            .label("malformed"),
            (func.count().filter(_invalid_code(columns)) > 0).label("bad_code"),
            (
                func.count().filter(
                    and_(columns.record_kind == "child", columns.collection_slot.not_in(collection_slots))
                )
                > 0
            ).label("bad_collection"),
            func.count().label("source_count"),
        )
        .group_by(columns.raw_hash)
        .cte("source_family_partials")
    )


def _typed_duplicate(build_id, models, maximum_id, root, groups):
    """Probe global typed identity only when a raw-duplicate family cannot win."""
    other = aliased(models[CustomImportBuildOccurrence])
    others = select(1).where(
        *_source_scope(other, build_id, maximum_id),
        other.record_kind == "root",
        other.occurrence_id != root.c.occurrence_id,
    )
    same_raw = others.where(other.raw_parent_key_sha256 == root.c.raw_hash).exists()
    different_typed = others.where(
        other.raw_parent_key_sha256.is_not(None),
        other.root_record_id == root.c.root_record_id,
        other.raw_parent_key_canonical.collate("C") != root.c.raw_key,
    ).exists()
    eligible = and_(
        groups.c.root_count == 1,
        groups.c.child_codes == 0,
        root.c.initial_code.is_(None),
        root.c.root_record_id.is_not(None),
        root.c.root_revision_id.is_not(None),
    )
    return case((eligible, case((same_raw, False), else_=different_typed)), else_=False)


def _partial_query(build_id, models, maximum_id, after, through, unkeyed, collection_slots):
    evidence = _source_evidence(build_id, models, maximum_id, after, through, unkeyed)
    groups = _family_partials(evidence, collection_slots)
    root = evidence.alias("first_root")
    return (
        select(
            groups,
            root.c.initial_code.label("root_code"),
            root.c.root_record_id,
            root.c.root_revision_id,
            _typed_duplicate(build_id, models, maximum_id, root, groups).label("typed_duplicate"),
        )
        .outerjoin(root, root.c.occurrence_id == groups.c.first_root_id)
        .order_by(groups.c.raw_hash)
    )


def _count_scope(scan, models, deadline):
    collection = CustomImportChildCollection
    _prepare_read(scan.session, scan.request, deadline)
    collections = scan.session.execute(
        select(collection.collection_slot, collection.collection_name).where(
            collection.dataset_id == scan.request.dataset_id,
            collection.schema_revision_id == scan.request.schema_revision_id,
        )
    ).all()
    expected_names = {entry.name for entry in scan.request.definition.child_collections}
    if len(collections) != len(expected_names) or {entry.collection_name for entry in collections} != expected_names:
        raise CandidateRunnerError("source outcome collection differs from the definition")
    occurrence = models[CustomImportBuildOccurrence]
    _prepare_read(scan.session, scan.request, deadline)
    maximum_id = scan.session.execute(
        select(occurrence.occurrence_id)
        .where(occurrence.build_id == scan.build_id, occurrence.origin == "source")
        .order_by(occurrence.occurrence_id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return maximum_id, tuple(entry.collection_slot for entry in collections)


def _has_admitted_page(metadata, request):
    if metadata.examined == 0:
        return False
    if metadata.largest_record > request.page_byte_limit or metadata.occurrence_id is None:
        raise CandidateRunnerError("source outcome evidence exceeds the admitted byte page")
    return True


def _count_source(scan):
    total, scope = _OutcomeTotal(), None
    for unkeyed in (False, True):
        after = None
        while True:
            with _read_transaction(scan.session, scan.request, scan.build_id) as (build, deadline):
                _require_admitted(build)
                models, _base = _build_storage_models(scan.session, scan.build_id)
                if scope is None:
                    scope = _count_scope(scan, models, deadline)
                maximum_id, collection_slots = scope
                if maximum_id is None:
                    return total.result()
                _prepare_read(scan.session, scan.request, deadline)
                metadata = scan.session.execute(
                    _metadata_query(
                        scan.build_id, models, maximum_id, after, unkeyed, MAX_BATCH_BYTES - total.reserved_bytes()
                    )
                ).one()
                if not _has_admitted_page(metadata, scan.request):
                    break
                through = (metadata.raw_hash, metadata.occurrence_id)
                partials = []
                if metadata.evidence_count:
                    _prepare_read(scan.session, scan.request, deadline)
                    partials = scan.session.execute(
                        _partial_query(scan.build_id, models, maximum_id, after, through, unkeyed, collection_slots)
                    ).all()
                if sum(partial.source_count for partial in partials) != metadata.evidence_count:
                    raise CandidateRunnerError("source outcome page coverage differs")
            for partial in partials:
                total.add(partial)
                del partial
            del partials
            _require_budget(deadline)
            after = through
        total.finish_family()
    return total.result()


def _require_admitted(build):
    if build.phase not in _ADMITTED_PHASES or build.source_frozen_at is None:
        raise CandidateRunnerError("source outcome counts require completed source admission")


async def count_source_outcomes(session_factory, request: SourceBuildRequest, build_id: int) -> SourceOutcomeCounts:
    """Project legacy counts under the same live fence before execution finality."""
    async with _renew_while_reading(session_factory, request):
        _require_admitted(await _snapshot(session_factory, request, build_id))
        async with _session(session_factory) as session:
            counts = await session.run_sync(lambda sync: _count_source(_CountScan(sync, request, build_id)))
        _require_admitted(await _snapshot(session_factory, request, build_id))
        return counts
