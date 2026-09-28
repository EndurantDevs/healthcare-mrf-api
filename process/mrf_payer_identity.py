# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist discovery payer identities without replacing independent source facts."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import or_, select, text, update

from db.models import MRFPayer, MRFSource, db
from process.mrf_source_discovery import (
    SourceCandidate,
    _candidate_target_payer_query,
    _candidate_to_rows,
    _canonical_or_none,
    _clean_text,
    _merge_payer_candidate_row,
    _utc_now,
)


def _candidate_source_locator(candidate: SourceCandidate) -> tuple[str, str | None, str | None]:
    return (
        candidate.provider,
        _canonical_or_none(candidate.index_url or candidate.human_url),
        _candidate_target_payer_query(candidate),
    )


def _stored_source_locator(row: dict[str, Any]) -> tuple[str, str | None, str | None]:
    metadata = row.get("metadata_json") or {}
    raw = metadata.get("raw") or {}
    query = _clean_text(metadata.get("target_payer_query") or raw.get("target_payer_query"))
    return (
        row.get("seed_provider"),
        row.get("canonical_url") or _canonical_or_none(row.get("index_url") or row.get("human_url")),
        query.lower() if query else None,
    )


def is_candidate_bound_to_source(candidate: SourceCandidate, row: dict[str, Any]) -> bool:
    """A shared URL or a shared name alone cannot bind two payer identities."""
    if _candidate_source_locator(candidate) != _stored_source_locator(row):
        return False
    known_names = {
        _clean_text(name).casefold() for name in (candidate.payer_name, *candidate.aliases) if _clean_text(name)
    }
    return _clean_text(row.get("display_name")).casefold() in known_names


def _curated_payer_row_key(candidate: SourceCandidate) -> tuple[str, ...] | None:
    """Two URLs listed in one reviewed row describe one payer candidate."""
    raw = candidate.raw_payload or {}
    if candidate.provider != "master-list" or not raw.get("raw_payer_name") or not raw.get("url_cell"):
        return None
    return (
        candidate.provider,
        candidate.source_url or "",
        str(raw.get("section") or ""),
        str(raw["raw_payer_name"]),
        str(raw["url_cell"]),
        str(raw.get("notes") or ""),
    )


def _merge_discovery_metadata(existing: Any, incoming: Any) -> dict[str, Any]:
    """Add observed values without replacing independent catalog or crawl facts."""
    merged_metadata_by_key = dict(existing or {})
    incoming_metadata_by_key = dict(incoming or {})
    for key, value in incoming_metadata_by_key.items():
        if key == "discovery_run_id":
            merged_metadata_by_key[key] = value
        elif key in {
            "aliases",
            "benefit_lines",
            "providers",
            "source_coverage",
            "vendor_names",
            "network_names",
            "plan_names",
            "supersedes_urls",
        }:
            merged_metadata_by_key[key] = sorted(
                set(merged_metadata_by_key.get(key) or []).union(value or []),
                key=str.casefold,
            )
        else:
            merged_metadata_by_key.setdefault(key, value)
    return merged_metadata_by_key


def _preserved_payer_updates(
    existing: dict[str, Any],
    candidate_row: dict[str, Any],
    *,
    is_confirmed_rename: bool,
) -> dict[str, Any]:
    aliases = sorted(
        set(existing.get("aliases") or [])
        | set(candidate_row.get("aliases") or [])
        | {existing["canonical_name"], candidate_row["canonical_name"]},
        key=str.casefold,
    )
    updates_by_field = {
        "aliases": aliases,
        "metadata_json": _merge_discovery_metadata(existing.get("metadata_json"), candidate_row.get("metadata_json")),
    }
    if is_confirmed_rename:
        updates_by_field["canonical_name"] = candidate_row["canonical_name"]
    return {key: value for key, value in updates_by_field.items() if existing.get(key) != value}


def _preserved_source_updates(
    existing: dict[str, Any],
    candidate_row: dict[str, Any],
    *,
    is_confirmed_rename: bool,
) -> dict[str, Any]:
    merged_metadata_by_key = _merge_discovery_metadata(
        existing.get("metadata_json"), candidate_row.get("metadata_json")
    )
    merged_metadata_by_key["source_tier"] = candidate_row["metadata_json"]["source_tier"]
    updates_by_field = {
        "metadata_json": merged_metadata_by_key,
    }
    if is_confirmed_rename:
        updates_by_field["display_name"] = candidate_row["display_name"]
    if not existing.get("payer_id"):
        updates_by_field["payer_id"] = candidate_row["payer_id"]
    return {key: value for key, value in updates_by_field.items() if existing.get(key) != value}


async def _match_existing_sources(session, candidates):
    """Resolve only source continuity supported by scoped URL and name evidence."""
    canonical_urls = sorted(
        {_candidate_source_locator(candidate)[1] for candidate in candidates if _candidate_source_locator(candidate)[1]}
    )
    raw_urls = sorted({candidate.index_url or candidate.human_url for candidate in candidates})
    source_table = MRFSource.__table__
    source_records = [
        dict(source_record)
        for source_record in (
            await session.execute(
                select(source_table)
                .where(
                    or_(
                        source_table.c.canonical_url.in_(canonical_urls),
                        source_table.c.index_url.in_(raw_urls),
                        source_table.c.human_url.in_(raw_urls),
                    )
                )
                .with_for_update()
            )
        ).mappings()
    ]
    source_records_by_locator = {}
    for source_record in source_records:
        source_records_by_locator.setdefault(_stored_source_locator(source_record), []).append(source_record)
    matched_sources = []
    payer_ids_by_curated_row = {}
    for candidate in candidates:
        matches = [
            source_record
            for source_record in source_records_by_locator.get(_candidate_source_locator(candidate), [])
            if is_candidate_bound_to_source(candidate, source_record)
        ]
        if len(matches) > 1:
            raise ValueError("mrf_discovery_source_identity_ambiguous")
        matched_source = matches[0] if matches else None
        matched_sources.append(matched_source)
        curated_row_key = _curated_payer_row_key(candidate)
        existing_payer_id = matched_source.get("payer_id") if matched_source else None
        if curated_row_key and existing_payer_id:
            previous_id = payer_ids_by_curated_row.setdefault(curated_row_key, existing_payer_id)
            if previous_id != existing_payer_id:
                raise ValueError("mrf_discovery_payer_identity_conflict")
    return matched_sources, payer_ids_by_curated_row


def _prepare_candidate_rows(candidates, matched_sources, payer_ids_by_curated_row, now, discovery_run_id):
    """Carry persisted IDs forward and group only URLs from one curated row."""
    payer_rows_by_id = {}
    source_rows_by_id = {}
    source_updates_by_id = {}
    payer_renames_by_id = {}
    for candidate, matched_source in zip(candidates, matched_sources):
        curated_row_key = _curated_payer_row_key(candidate)
        payer_id = (
            (matched_source.get("payer_id") if matched_source else None)
            or (payer_ids_by_curated_row.get(curated_row_key) if curated_row_key else None)
            or f"mrfpayer_{uuid.uuid4().hex}"
        )
        if curated_row_key:
            payer_ids_by_curated_row[curated_row_key] = payer_id
        source_id = matched_source["source_id"] if matched_source else f"mrfsource_{uuid.uuid4().hex}"
        payer_row, source_row = _candidate_to_rows(
            candidate,
            now,
            discovery_run_id=discovery_run_id,
            payer_id=payer_id,
            source_id=source_id,
        )
        existing_candidate = payer_rows_by_id.get(payer_id)
        if existing_candidate:
            _merge_payer_candidate_row(existing_candidate, payer_row, candidate)
        else:
            payer_rows_by_id[payer_id] = payer_row
        assert source_row is not None
        if matched_source:
            is_confirmed_rename = (
                _clean_text(matched_source.get("display_name")).casefold()
                != _clean_text(candidate.payer_name).casefold()
            )
            source_updates_by_id[source_id] = _preserved_source_updates(
                matched_source,
                source_row,
                is_confirmed_rename=is_confirmed_rename,
            )
            source_rows_by_id[source_id] = {**matched_source, **source_updates_by_id[source_id]}
            if is_confirmed_rename:
                previous_name = payer_renames_by_id.setdefault(payer_id, candidate.payer_name)
                if previous_name != candidate.payer_name:
                    raise ValueError("mrf_discovery_payer_rename_ambiguous")
        else:
            source_rows_by_id[source_id] = source_row
    return payer_rows_by_id, source_rows_by_id, source_updates_by_id, payer_renames_by_id


def _is_reviewed_payer_rename(existing_payer, renamed_name, candidates):
    if not renamed_name:
        return False
    old_name = _clean_text(existing_payer["canonical_name"]).casefold()
    return any(
        old_name in {_clean_text(alias).casefold() for alias in candidate.aliases}
        for candidate in candidates
        if _clean_text(candidate.payer_name) == renamed_name
    )


async def _save_payers(session, candidates, payer_rows_by_id, payer_renames_by_id, now):
    payer_table = MRFPayer.__table__
    existing_payers_by_id = {
        payer_record["payer_id"]: dict(payer_record)
        for payer_record in (
            await session.execute(
                select(payer_table).where(payer_table.c.payer_id.in_(payer_rows_by_id)).with_for_update()
            )
        ).mappings()
    }
    for payer_id, payer_row in payer_rows_by_id.items():
        existing_payer = existing_payers_by_id.get(payer_id)
        if existing_payer is None:
            await session.execute(payer_table.insert().values(**payer_row))
            continue
        is_confirmed_rename = _is_reviewed_payer_rename(
            existing_payer,
            payer_renames_by_id.get(payer_id),
            candidates,
        )
        updates_by_field = _preserved_payer_updates(
            existing_payer,
            payer_row,
            is_confirmed_rename=is_confirmed_rename,
        )
        if updates_by_field:
            updates_by_field["updated_at"] = now
            await session.execute(
                update(payer_table).where(payer_table.c.payer_id == payer_id).values(**updates_by_field)
            )
            payer_rows_by_id[payer_id] = {**existing_payer, **updates_by_field}
        else:
            payer_rows_by_id[payer_id] = existing_payer


async def _save_sources(session, source_rows_by_id, source_updates_by_id, now):
    source_table = MRFSource.__table__
    for source_id, source_row in source_rows_by_id.items():
        if source_id not in source_updates_by_id:
            await session.execute(source_table.insert().values(**source_row))
            continue
        updates_by_field = source_updates_by_id[source_id]
        if updates_by_field:
            updates_by_field["updated_at"] = now
            await session.execute(
                update(source_table).where(source_table.c.source_id == source_id).values(**updates_by_field)
            )
            source_rows_by_id[source_id] = {**source_row, **updates_by_field}


async def store_candidates(
    candidates: list[SourceCandidate],
    *,
    discovery_run_id: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Persist payer discovery with source-bound IDs and additive updates."""
    if any(not (candidate.index_url or candidate.human_url) for candidate in candidates):
        raise ValueError("mrf_discovery_source_url_required")
    if not candidates:
        return [], []
    now = _utc_now()
    async with db.session() as session:
        # ponytail: one catalog lock is sufficient at current discovery volume;
        # use source-scoped locks if concurrent discovery becomes necessary.
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_name))"),
            {"lock_name": "mrf_source_discovery_payer_identity"},
        )
        matched_sources, payer_ids_by_curated_row = await _match_existing_sources(session, candidates)
        (payer_rows_by_id, source_rows_by_id, source_updates_by_id, payer_renames_by_id) = _prepare_candidate_rows(
            candidates,
            matched_sources,
            payer_ids_by_curated_row,
            now,
            discovery_run_id,
        )
        await _save_payers(session, candidates, payer_rows_by_id, payer_renames_by_id, now)
        await _save_sources(session, source_rows_by_id, source_updates_by_id, now)
    return list(payer_rows_by_id.values()), list(source_rows_by_id.values())
