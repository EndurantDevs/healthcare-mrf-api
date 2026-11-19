# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare bulk FHIR NDJSON lines natively where the result is proven equal, else with the Python chain."""

from __future__ import annotations

import datetime
import importlib
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from process import provider_directory_bulk_grouping as grouping
from process import provider_directory_role_merge as role_merge
from process.provider_directory_bulk_prepare import PrepareContext, prepare_lines

NATIVE_TYPES = frozenset({"PractitionerRole"})
ROWS_CONTRACT_VERSION = "provider_directory_rows_v1"
BULK_FETCH_MODE = "bulk_ndjson"
ROW_LINES_EXPORT = "prepare_provider_directory_row_lines_v1"


@dataclass(frozen=True)
class PreparedDirectoryRows:
    """Rows ready for retention; `payload_texts` is the stored payload text when the native path made it."""

    rows: list[dict[str, Any]]
    rows_fetched: int
    error: str | None = None
    payload_texts: list[str] | None = None
    unsupported_reason: str | None = None


@dataclass(frozen=True)
class PreparedRowLines:
    """One group file line per prepared line, in input order, with merged roles on."""

    resource_ids: list[str]
    encoded_lines: list[bytes]
    rows_fetched: int
    error: str | None = None
    fallback_counts_by_reason: dict[str, int] = field(default_factory=dict)


def _native_export(name: str):
    """Return one compiled preparer, refusing a build whose contract differs from this module's."""

    try:
        native = importlib.import_module("ptg2_address_canon")
    except ModuleNotFoundError:
        return None
    export = getattr(native, name, None)
    if export is None:
        return None
    version = native.provider_directory_rows_version()
    if version != ROWS_CONTRACT_VERSION:
        raise RuntimeError(f"provider_directory_rows_native_version_mismatch:{version}")
    return export


def has_native_row_lines(resource_type: str) -> bool:
    """Whether per-line native preparation serves this type; a mismatched build raises."""

    return resource_type in NATIVE_TYPES and _native_export(ROW_LINES_EXPORT) is not None


def _validated_projection(context: Any) -> None:
    """Fail as prepare_lines does for a malformed projection date, before any native work."""

    if context.semantic_projection_as_of:
        datetime.date.fromisoformat(context.semantic_projection_as_of)


def _native_context_by_name(context: Any, merge_roles: bool, payload_text: bool) -> dict[str, Any]:
    return {
        "source_id": context.source_id,
        "dataset_id": context.dataset_id,
        "resource_hash_contract": context.resource_hash_contract,
        "merge_roles": merge_roles,
        "resource_url": context.log_identity,
        "self_url": None,
        "fetch_url": context.log_identity,
        "fetch_mode": BULK_FETCH_MODE,
        "payload_text": payload_text,
        "strict": context.strict,
    }


def _native_rows(prepare, lines: list[bytes], context: Any, payload_text: bool) -> PreparedDirectoryRows | str:
    """Return the native rows, or the reason the batch is left to Python."""

    outcome = prepare(lines, context.resource_type, _native_context_by_name(context, context.merge_roles, payload_text))
    if outcome[0] != "rows":
        return outcome[1]
    _, prepared, rows_fetched, payload_texts, merged_roles = outcome
    for prepared_row, components in zip(prepared, merged_roles, strict=True):
        if components is not None:
            # The same carrier fold_merged_roles returns, so downstream reuses the commitments.
            hashed_role = role_merge._HashedRole(prepared_row["payload_json"])
            hashed_role.components = (components[0], list(components[1]), components[2])
            prepared_row["payload_json"] = hashed_role
    return PreparedDirectoryRows(prepared, rows_fetched, payload_texts=payload_texts)


def prepare_directory_rows(lines: list[bytes], context: Any, *, payload_text: bool = False) -> PreparedDirectoryRows:
    """Prepare one batch of NDJSON lines exactly as `prepare_lines` does, natively when proven equal.

    `context` is a `PrepareContext` or any object with its attributes.
    """

    _validated_projection(context)
    prepare = _native_export("prepare_provider_directory_rows_v1") if context.resource_type in NATIVE_TYPES else None
    if prepare is not None:
        native_result = _native_rows(prepare, list(lines), context, payload_text)
        if isinstance(native_result, PreparedDirectoryRows):
            return native_result
    else:
        native_result = None
    python_result = prepare_lines(list(lines), context)
    return PreparedDirectoryRows(
        python_result.rows, python_result.rows_fetched, python_result.error, unsupported_reason=native_result
    )


def _native_row_entries(lines: list[bytes], context: Any) -> tuple[list[Any], str | None]:
    """Native per-line entries and the batch error after them; every line is a fallback without native."""

    prepare = _native_export(ROW_LINES_EXPORT) if context.resource_type in NATIVE_TYPES else None
    if prepare is None:
        return ["native_unavailable"] * len(lines), None
    outcome = prepare(lines, context.resource_type, _native_context_by_name(context, True, False))
    if outcome[0] != "lines":
        return [outcome[1]] * len(lines), None
    return outcome[1], outcome[2]


def prepare_directory_row_lines(lines: list[bytes], context: Any) -> PreparedRowLines:
    """Prepare each NDJSON line on its own into one group file line, merged roles on.

    No in-batch identity rule applies, with or without the compiled preparer: the reducer unions every
    sighting of a merged role.
    shortcut: one id repeated with equal content but another merged-role id (no Meta in one sighting, only
    volatile Meta in the other) stays two roles, as the stream leaves it across batches; prepare repeated
    ids together if a source ever sends such pairs. A line the
    native preparer leaves alone runs `prepare_lines` as a one-line batch and is encoded with the
    grouping encoder. A line for which today's handling returns a batch error stops the call with
    that error, as today.
    """

    lines = list(lines)
    merged_context = PrepareContext(**{**_context_fields(context), "merge_roles": True})
    _validated_projection(merged_context)
    entries, native_error = _native_row_entries(lines, merged_context)
    resource_ids: list[str] = []
    encoded_lines: list[bytes] = []
    fallback_counts_by_reason: Counter[str] = Counter()
    rows_fetched = 0
    for line, entry in zip(lines, entries):
        if entry is None:
            continue
        if isinstance(entry, tuple):
            resource_ids.append(entry[0])
            encoded_lines.append(entry[1])
            rows_fetched += 1
            continue
        fallback_counts_by_reason[entry] += 1
        single = prepare_lines([line], merged_context)
        if single.error is not None:
            return PreparedRowLines(
                resource_ids, encoded_lines, rows_fetched, single.error, dict(fallback_counts_by_reason)
            )
        rows_fetched += single.rows_fetched
        for prepared_row in single.rows:
            resource_ids.append(prepared_row["resource_id"])
            encoded_lines.append(grouping.encoded_row(prepared_row))
    return PreparedRowLines(resource_ids, encoded_lines, rows_fetched, native_error, dict(fallback_counts_by_reason))


def _context_fields(context: Any) -> dict[str, Any]:
    return {name: getattr(context, name) for name in PrepareContext.__dataclass_fields__}
