# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded set persistence for legacy graph packs and rejection evidence."""

from __future__ import annotations

import re

from sqlalchemy import inspect

from db.models.custom_import import CustomImportPack, CustomImportRejection
from process.custom_import.definition import MAX_CHILD_COLLECTIONS
from process.custom_import.materialization_store import (
    _authority,
    _authority_arguments,
    _call,
    _flush_pending,
    verify_materialization_authority,
)
from process.custom_import.runner_types import CandidateRunnerError

_PAGE_ROWS = 256
# A multirow transport target, not a source-value admission limit. A larger
# admitted key uses the same protected function with a scalar TEXT payload.
_REJECTION_PAGE_BYTES = 8 * 1024 * 1024
_PAGE_OVERHEAD_BYTES = 1024
_REJECTION_ROW_OVERHEAD_BYTES = 128
_REJECTION_EVIDENCE_BYTES = 256


def _check_owner(row, window):
    dataset, definition, schema, execution, _capture, fence, token = window.authority
    if (
        row.dataset_id,
        row.definition_revision_id,
        row.schema_revision_id,
        row.execution_id,
        row.producing_fence,
        row.producing_token_sha256,
    ) != (dataset, definition, schema, execution, fence, token) or not inspect(row).transient:
        raise CandidateRunnerError("legacy graph row authority or pending state differs")


async def persist_pack_models(session, packs_by_collection):
    """Persist the declared stream packs and bind their returned native IDs."""

    window = await _authority(session)
    models = tuple(packs_by_collection.values())
    if not 1 <= len(models) <= MAX_CHILD_COLLECTIONS + 1:
        raise CandidateRunnerError("legacy pack stream count differs")
    for pack_model in models:
        if not isinstance(pack_model, CustomImportPack) or pack_model.capture_bundle_id != window.authority[4]:
            raise CandidateRunnerError("legacy pack capture identity differs")
        _check_owner(pack_model, window)
    if len({(pack_model.stream_slot, pack_model.pack_ordinal) for pack_model in models}) != len(models):
        raise CandidateRunnerError("legacy pack identities repeat")
    arguments = _authority_arguments(window) + (
        ("smallint[]", tuple(pack_model.stream_slot for pack_model in models)),
        ("integer[]", tuple(pack_model.pack_ordinal for pack_model in models)),
        ("bigint[]", tuple(pack_model.record_count for pack_model in models)),
        ("bytea[]", tuple(pack_model.pack_sha256 for pack_model in models)),
    )
    await _flush_pending(session)
    pack_ids = await _call(session, "persist_custom_import_legacy_pack_set", arguments)
    if (
        not isinstance(pack_ids, (list, tuple))
        or len(pack_ids) != len(models)
        or any(type(returned_id) is not int or not 0 < returned_id < 2**63 for returned_id in pack_ids)
        or len(set(pack_ids)) != len(pack_ids)
        or any(
            pack_model.pack_id is not None and pack_model.pack_id != returned_id
            for pack_model, returned_id in zip(models, pack_ids, strict=True)
        )
    ):
        raise CandidateRunnerError("legacy pack returned identity differs")
    for pack_model, pack_id in zip(models, pack_ids, strict=True):
        pack_model.pack_id = pack_id
    await verify_materialization_authority(session)


def _rejection_page_ranges(models, window):
    ranges = []
    start = 0
    page_bytes = _PAGE_OVERHEAD_BYTES
    for index, rejection_model in enumerate(models):
        if not isinstance(rejection_model, CustomImportRejection):
            raise CandidateRunnerError("legacy rejection row is malformed")
        _check_owner(rejection_model, window)
        if (
            any(
                getattr(rejection_model, name) is not None
                for name in ("pack_id", "collection_slot", "source_ordinal", "field_slot")
            )
            or type(rejection_model.rejection_ordinal) is not int
            or not 0 <= rejection_model.rejection_ordinal < 2**63
            or not isinstance(rejection_model.code, str)
            or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", rejection_model.code) is None
            or not isinstance(rejection_model.canonical_evidence, str)
            or (rejection_model.canonical_root_key is None) != (rejection_model.root_key_sha256 is None)
            or (
                rejection_model.canonical_root_key is not None
                and not isinstance(rejection_model.canonical_root_key, str)
            )
        ):
            raise CandidateRunnerError("legacy rejection identity or evidence differs")
        evidence_bytes = len(rejection_model.canonical_evidence.encode("utf-8"))
        if evidence_bytes > _REJECTION_EVIDENCE_BYTES:
            raise CandidateRunnerError("legacy rejection evidence exceeds its codec bound")
        row_bytes = _REJECTION_ROW_OVERHEAD_BYTES + len(rejection_model.code) + evidence_bytes
        if rejection_model.canonical_root_key is not None:
            row_bytes += len(rejection_model.canonical_root_key.encode("utf-8")) + 32
        if index > start and (index - start == _PAGE_ROWS or page_bytes + row_bytes > _REJECTION_PAGE_BYTES):
            ranges.append((start, index))
            start = index
            page_bytes = _PAGE_OVERHEAD_BYTES
        page_bytes += row_bytes
    if len({rejection_model.rejection_ordinal for rejection_model in models}) != len(models):
        raise CandidateRunnerError("legacy rejection ordinals repeat")
    if start < len(models):
        ranges.append((start, len(models)))
    return ranges


async def persist_rejection_models(session, models):
    """Append ordered evidence pages without owning or committing a transaction."""

    window = await _authority(session)
    ranges = _rejection_page_ranges(models, window)
    if models:
        await _flush_pending(session)
    for start, end in ranges:
        page = models[start:end]
        arguments = _authority_arguments(window) + (
            ("bigint[]", tuple(row.rejection_ordinal for row in page)),
            ("text[]", (None,) if len(page) == 1 else tuple(row.canonical_root_key for row in page)),
            ("bytea[]", tuple(row.root_key_sha256 for row in page)),
            ("text[]", tuple(row.code for row in page)),
            ("text[]", tuple(row.canonical_evidence for row in page)),
            # Avoid an array datum's extra header narrowing the native TEXT
            # domain for a single admitted key. This is payload, not authority.
            ("text", page[0].canonical_root_key if len(page) == 1 else None),
        )
        count = await _call(session, "persist_custom_import_legacy_rejection_set", arguments)
        if type(count) is not int or count != len(page):
            raise CandidateRunnerError("legacy rejection persisted count differs")
    await verify_materialization_authority(session)
