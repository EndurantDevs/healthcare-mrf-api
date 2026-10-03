# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded legacy result counts from immutable source admission evidence."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import aliased

from db.models.custom_import import CustomImportBuildOccurrence, CustomImportChildCollection, CustomImportRejection
from process.custom_import.build_graph import (
    _read_rows,
    _read_transaction,
    _ReadPage,
    _renew_while_reading,
    _session,
    _snapshot,
)
from process.custom_import.build_source import SourceBuildRequest
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


def _rows(scan, statement, keys, variable_columns=(), *, bounds=_ReadPage()):
    return _read_rows(
        scan.session,
        scan.request,
        scan.build_id,
        statement,
        keys,
        (),
        bounds=_ReadPage(bounds.reserve_bytes, bounds.row_limit, variable_columns),
    )


def _source_statement(build_id):
    occurrence = CustomImportBuildOccurrence
    return select(occurrence).where(occurrence.build_id == build_id, occurrence.origin == "source")


def _require_code(code, allowed):
    if code is not None and code not in allowed:
        raise CandidateRunnerError("source outcome contains an unknown rejection code")
    return code


def _malformed_root_count(scan):
    occurrence, rejection = CustomImportBuildOccurrence, CustomImportRejection
    statement = _source_statement(scan.build_id).outerjoin(rejection, rejection.rejection_id == occurrence.rejection_id)
    statement = statement.with_only_columns(occurrence.occurrence_id, occurrence.record_kind, rejection.code)
    rejected = 0
    # Visit every source position so a child-only prefix cannot become an unbounded filtered scan.
    with closing(_rows(scan, statement, (occurrence.occurrence_id,), (occurrence.record_kind, rejection.code))) as rows:
        for _identifier, kind, code in rows:
            if kind == "root":
                _require_code(code, _ROOT_CODES)
                rejected += code in _UNKEYED_ROOT_CODES
    return rejected


def _collection_slots(scan):
    collection = CustomImportChildCollection
    statement = select(collection.collection_slot, collection.collection_name).where(
        collection.dataset_id == scan.request.dataset_id,
        collection.schema_revision_id == scan.request.schema_revision_id,
    )
    expected_names = {entry.name for entry in scan.request.definition.child_collections}
    slots_by_name = {}
    with closing(_rows(scan, statement, (collection.collection_slot,), (collection.collection_name,))) as rows:
        for slot, name in rows:
            if name not in expected_names or name in slots_by_name:
                raise CandidateRunnerError("source outcome collection differs from the definition")
            slots_by_name[name] = slot
    if set(slots_by_name) != expected_names:
        raise CandidateRunnerError("source outcome collection set is incomplete")
    return tuple(slots_by_name.values())


def _next_root(scan, after_sha):
    occurrence, rejection = CustomImportBuildOccurrence, CustomImportRejection
    statement = (
        _source_statement(scan.build_id)
        .outerjoin(rejection, rejection.rejection_id == occurrence.rejection_id)
        .with_only_columns(
            occurrence.occurrence_id,
            occurrence.raw_parent_key_sha256,
            occurrence.raw_parent_key_canonical,
            occurrence.root_record_id,
            occurrence.root_revision_id,
            rejection.code.label("initial_code"),
        )
        .where(occurrence.record_kind == "root", occurrence.raw_parent_key_sha256.is_not(None))
    )
    if after_sha is not None:
        statement = statement.where(occurrence.raw_parent_key_sha256 > after_sha)
    rows = _rows(
        scan,
        statement,
        (occurrence.raw_parent_key_sha256, occurrence.occurrence_id),
        (occurrence.raw_parent_key_sha256, occurrence.raw_parent_key_canonical, rejection.code),
        bounds=_ReadPage(row_limit=1),
    )
    try:
        return next(rows, None)
    finally:
        rows.close()


def _has_other_root(scan, root, *, typed):
    occurrence = CustomImportBuildOccurrence
    statement = _source_statement(scan.build_id).where(
        occurrence.record_kind == "root", occurrence.occurrence_id != root.occurrence_id
    )
    if typed:
        statement = statement.where(
            occurrence.root_record_id == root.root_record_id,
            occurrence.raw_parent_key_canonical != root.raw_parent_key_canonical,
        )
    else:
        statement = statement.where(
            occurrence.raw_parent_key_sha256 == root.raw_parent_key_sha256,
            occurrence.raw_parent_key_canonical == root.raw_parent_key_canonical,
        )
    with _read_transaction(scan.session, scan.request, scan.build_id):
        return scan.session.execute(select(statement.with_only_columns(1).exists())).scalar_one()


def _child_statement(scan, root, collection_slot, *, keyed):
    occurrence = CustomImportBuildOccurrence
    initial, resolved = aliased(CustomImportRejection), aliased(CustomImportRejection)
    statement = (
        _source_statement(scan.build_id)
        .outerjoin(initial, initial.rejection_id == occurrence.rejection_id)
        .outerjoin(resolved, resolved.rejection_id == occurrence.resolved_rejection_id)
        .with_only_columns(
            occurrence.occurrence_id,
            occurrence.raw_parent_key_canonical,
            occurrence.child_key_sha256,
            initial.code.label("initial_code"),
            resolved.code.label("resolved_code"),
        )
        .where(
            occurrence.record_kind == "child",
            occurrence.raw_parent_key_sha256 == root.raw_parent_key_sha256,
            occurrence.collection_slot == collection_slot,
            occurrence.child_key_sha256.is_not(None) if keyed else occurrence.child_key_sha256.is_(None),
        )
    )
    keys = (occurrence.child_key_sha256, occurrence.occurrence_id) if keyed else (occurrence.occurrence_id,)
    columns = (occurrence.raw_parent_key_canonical, occurrence.child_key_sha256, initial.code, resolved.code)
    return statement, keys, columns


def _child_code(child, root):
    if child.raw_parent_key_canonical != root.raw_parent_key_canonical:
        raise CandidateRunnerError("source outcome raw-key digest collision")
    initial = _require_code(child.initial_code, _CHILD_CODES - _CHILD_RESOLVED_CODES)
    resolved = _require_code(child.resolved_code, _CHILD_CODES)
    if resolved in _CHILD_CANDIDATE_CODES or initial in _CHILD_CANDIDATE_CODES:
        return None
    if initial is not None:
        return initial
    if resolved is not None and resolved not in _CHILD_RESOLVED_CODES:
        raise CandidateRunnerError("source outcome lacks its initial rejection evidence")
    return resolved


def _collect_child_errors(scan, root, slot, keyed, retained_bytes):
    statement, keys, columns = _child_statement(scan, root, slot, keyed=keyed)
    codes = set()
    with closing(_rows(scan, statement, keys, columns, bounds=_ReadPage(reserve_bytes=retained_bytes))) as rows:
        for child in rows:
            code = _child_code(child, root)
            if code is not None:
                codes.add(code)
    return codes


def _family_codes(scan, root, collection_slots):
    initial = _require_code(root.initial_code, _ROOT_CODES - _UNKEYED_ROOT_CODES)
    codes = set() if initial is None else {initial}
    if _has_other_root(scan, root, typed=False):
        codes.add("duplicate_root_key")
    retained_bytes = len(root.raw_parent_key_canonical.encode("utf-8")) + len(root.raw_parent_key_sha256)
    if initial is not None:
        retained_bytes += len(initial.encode("utf-8"))
    for slot in collection_slots:
        for keyed in (True, False):
            codes.update(_collect_child_errors(scan, root, slot, keyed, retained_bytes))
    if not codes:
        if root.root_record_id is None or root.root_revision_id is None:
            raise CandidateRunnerError("accepted source outcome lacks a typed root revision")
        if _has_other_root(scan, root, typed=True):
            codes.add("duplicate_root_key")
    return codes


def _count_source(scan):
    collection_slots = _collection_slots(scan)
    rejected = _malformed_root_count(scan)
    accepted, after_sha = 0, None
    while (root := _next_root(scan, after_sha)) is not None:
        codes = _family_codes(scan, root, collection_slots)
        accepted += not codes
        rejected += len(codes)
        after_sha = bytes(root.raw_parent_key_sha256)
    return SourceOutcomeCounts(accepted, rejected)


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
