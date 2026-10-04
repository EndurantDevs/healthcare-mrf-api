# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional payer identity regression in a disposable PostgreSQL database."""

import asyncio
import datetime as dt
import os
import re
import uuid

import pytest
from sqlalchemy import select

from db.models import MRFPayer, MRFSource, db
from process import mrf_source_discovery as discovery


async def _seed_legacy_source(suffix, payer_ids, source_ids):
    old_candidate = discovery.SourceCandidate(
        payer_name=f"Example Old {suffix}",
        provider="master-list",
        index_url=f"https://example.test/{suffix}/mrf",
    )
    legacy_payer_id = f"mrfpayer_legacy_{suffix}"
    legacy_source_id = f"mrfsource_legacy_{suffix}"
    legacy_payer, legacy_source = discovery._candidate_to_rows(
        old_candidate,
        dt.datetime(2025, 1, 1),
        payer_id=legacy_payer_id,
        source_id=legacy_source_id,
    )
    assert legacy_source is not None
    legacy_payer.update(lifecycle="reviewed", eins=["12-3456789"])
    legacy_source.update(source_key=f"legacy-{suffix}", status="active", etag="keep-etag")
    legacy_source["metadata_json"]["catalog_paging_manifest"] = {"snapshot": "keep"}
    async with db.session() as session:
        await session.execute(MRFPayer.__table__.insert().values(**legacy_payer))
        await session.execute(MRFSource.__table__.insert().values(**legacy_source))
    payer_ids.add(legacy_payer_id)
    source_ids.add(legacy_source_id)
    return old_candidate, legacy_payer, legacy_source


async def _check_legacy_rename(old_candidate, legacy_payer, legacy_source):
    renamed_candidate = discovery.SourceCandidate(
        payer_name=old_candidate.payer_name.replace("Old", "New"),
        provider="master-list",
        index_url=old_candidate.index_url,
        aliases=(old_candidate.payer_name,),
    )
    for run_id in ("run_renamed", None):
        payer_rows, source_rows = await discovery._store_candidates([renamed_candidate], discovery_run_id=run_id)
        assert payer_rows[0]["payer_id"] == legacy_payer["payer_id"]
        assert source_rows[0]["source_id"] == legacy_source["source_id"]
    async with db.session() as session:
        payer_record_by_field = dict(
            (await session.execute(select(MRFPayer.__table__).where(MRFPayer.payer_id == legacy_payer["payer_id"])))
            .mappings()
            .one()
        )
        source_record_by_field = dict(
            (
                await session.execute(
                    select(MRFSource.__table__).where(MRFSource.source_id == legacy_source["source_id"])
                )
            )
            .mappings()
            .one()
        )
    assert payer_record_by_field["canonical_name"] == renamed_candidate.payer_name
    assert payer_record_by_field["lifecycle"] == "reviewed"
    assert payer_record_by_field["eins"] == ["12-3456789"]
    assert payer_record_by_field["created_at"] == legacy_payer["created_at"]
    assert source_record_by_field["source_key"] == legacy_source["source_key"]
    assert source_record_by_field["etag"] == "keep-etag"
    assert source_record_by_field["status"] == "active"
    assert source_record_by_field["metadata_json"]["catalog_paging_manifest"] == {"snapshot": "keep"}
    return renamed_candidate


async def _check_shared_url_and_query_scope(old_candidate, renamed_candidate, suffix, payer_ids, source_ids):
    unrelated_candidate = discovery.SourceCandidate(
        payer_name=f"Unrelated {suffix}",
        provider="master-list",
        index_url=old_candidate.index_url,
    )

    async def persist_unrelated():
        payer_rows, source_rows = await discovery._store_candidates([unrelated_candidate])
        return payer_rows[0]["payer_id"], source_rows[0]["source_id"]

    first_identity, second_identity = await asyncio.gather(persist_unrelated(), persist_unrelated())
    assert first_identity == second_identity
    assert first_identity != (f"mrfpayer_legacy_{suffix}", f"mrfsource_legacy_{suffix}")
    payer_ids.add(first_identity[0])
    source_ids.add(first_identity[1])

    scoped_candidate = discovery.SourceCandidate(
        payer_name=renamed_candidate.payer_name,
        provider="master-list",
        index_url=old_candidate.index_url,
        raw_payload={"target_payer_query": "Distinct Employer"},
    )
    scoped_payers, scoped_sources = await discovery._store_candidates([scoped_candidate])
    assert scoped_payers[0]["payer_id"] != f"mrfpayer_legacy_{suffix}"
    assert scoped_sources[0]["source_id"] != f"mrfsource_legacy_{suffix}"
    payer_ids.add(scoped_payers[0]["payer_id"])
    source_ids.add(scoped_sources[0]["source_id"])


async def _check_curated_multi_url_row(suffix, payer_ids, source_ids):
    curated_candidates = discovery.parse_master_list(
        "| Payer | Type | Public MRF TOC / landing URL | Notes |\n"
        "|---|---|---|---|\n"
        f"| Example Group {suffix} | regional | "
        f"https://example.test/{suffix}/one · https://example.test/{suffix}/two "
        "| public indexes |\n"
    )
    grouped_payers, grouped_sources = await discovery._store_candidates(curated_candidates)
    assert len(grouped_payers) == 1
    assert len({source_record["source_id"] for source_record in grouped_sources}) == 2
    assert {source_record["payer_id"] for source_record in grouped_sources} == {grouped_payers[0]["payer_id"]}
    payer_ids.add(grouped_payers[0]["payer_id"])
    source_ids.update(source_record["source_id"] for source_record in grouped_sources)


async def _seed_ambiguous_sources(candidate, suffix, payer_ids, source_ids):
    ambiguous_source_ids = set()
    async with db.session() as session:
        for index in range(2):
            payer_id = f"mrfpayer_ambiguous_{suffix}_{index}"
            source_id = f"mrfsource_ambiguous_{suffix}_{index}"
            payer_row, source_row = discovery._candidate_to_rows(
                candidate,
                dt.datetime(2025, 1, 1),
                payer_id=payer_id,
                source_id=source_id,
            )
            assert source_row is not None
            payer_row.update(lifecycle="reviewed", eins=["12-3456789"])
            source_row.update(status="active", etag=f"retained-{index}")
            source_row["metadata_json"]["catalog_paging_manifest"] = {"snapshot": f"keep-{index}"}
            await session.execute(MRFPayer.__table__.insert().values(**payer_row))
            await session.execute(MRFSource.__table__.insert().values(**source_row))
            payer_ids.add(payer_id)
            source_ids.add(source_id)
            ambiguous_source_ids.add(source_id)
    return ambiguous_source_ids


async def _identity_snapshot(selected_source_ids):
    async with db.session() as session:
        sources = [
            dict(row)
            for row in (
                await session.execute(
                    select(MRFSource.__table__)
                    .where(MRFSource.source_id.in_(selected_source_ids))
                    .order_by(MRFSource.source_id)
                )
            ).mappings()
        ]
        payers = [
            dict(row)
            for row in (
                await session.execute(
                    select(MRFPayer.__table__)
                    .where(MRFPayer.payer_id.in_(source["payer_id"] for source in sources))
                    .order_by(MRFPayer.payer_id)
                )
            ).mappings()
        ]
    return payers, sources


async def _check_ambiguous_curated_row_isolation(suffix, payer_ids, source_ids):
    ambiguous, sibling = discovery.parse_master_list(
        "| Payer | Type | Public MRF TOC / landing URL | Notes |\n"
        "|---|---|---|---|\n"
        f"| Example Ambiguous {suffix} | regional | "
        f"https://example.test/{suffix}/ambiguous · https://example.test/{suffix}/sibling "
        "| public indexes |\n"
    )
    safe = discovery.SourceCandidate(
        payer_name=f"Independent {suffix}",
        provider="master-list",
        index_url=f"https://example.test/{suffix}/independent",
    )
    ambiguous_ids = await _seed_ambiguous_sources(ambiguous, suffix, payer_ids, source_ids)
    before = await _identity_snapshot(ambiguous_ids)
    with pytest.raises(ValueError, match="^mrf_discovery_source_identity_ambiguous$"):
        await discovery._store_candidates([safe, sibling, ambiguous])
    async with db.session() as session:
        partial_sources = (
            (
                await session.execute(
                    select(MRFSource.__table__).where(MRFSource.index_url.in_([safe.index_url, sibling.index_url]))
                )
            )
            .mappings()
            .all()
        )
    assert partial_sources == []
    first_errors = []
    first_payers, first_sources = await discovery._store_candidates(
        [sibling, safe, ambiguous], discovery_run_id="run_isolated", identity_errors=first_errors
    )
    assert len(first_payers) == len(first_sources) == 1
    assert first_sources[0]["index_url"] == safe.index_url
    payer_ids.add(first_payers[0]["payer_id"])
    source_ids.add(first_sources[0]["source_id"])
    repeated_errors = []
    repeated_payers, repeated_sources = await discovery._store_candidates(
        [sibling, safe, ambiguous], identity_errors=repeated_errors
    )
    assert repeated_payers[0]["payer_id"] == first_payers[0]["payer_id"]
    assert repeated_sources[0]["source_id"] == first_sources[0]["source_id"]
    assert repeated_errors == first_errors
    assert len(first_errors) == 2
    assert {error["curated_row_pending"] for error in first_errors} == {False, True}
    assert await _identity_snapshot(ambiguous_ids) == before
    async with db.session() as session:
        sibling_rows = (
            (await session.execute(select(MRFSource.__table__).where(MRFSource.index_url == sibling.index_url)))
            .mappings()
            .all()
        )
    assert sibling_rows == []


@pytest.mark.asyncio
async def test_discovery_preserves_legacy_ids_and_serializes_new_identity():
    """A rerun or alias rename keeps IDs while scoped siblings remain distinct."""
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.fullmatch(r"ptg2_v3_lifecycle_test_[a-z0-9_]{8,}", database_name):
        pytest.skip("requires an explicitly selected disposable PostgreSQL database")
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema)
    await db.connect()
    await db.execute_ddl(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await db.create_table(MRFPayer.__table__, checkfirst=True)
    await db.create_table(MRFSource.__table__, checkfirst=True)
    payer_ids = set()
    source_ids = set()
    try:
        suffix = uuid.uuid4().hex
        old_candidate, legacy_payer, legacy_source = await _seed_legacy_source(suffix, payer_ids, source_ids)
        renamed_candidate = await _check_legacy_rename(old_candidate, legacy_payer, legacy_source)
        await _check_shared_url_and_query_scope(old_candidate, renamed_candidate, suffix, payer_ids, source_ids)
        await _check_curated_multi_url_row(suffix, payer_ids, source_ids)
        await _check_ambiguous_curated_row_isolation(suffix, payer_ids, source_ids)
    finally:
        async with db.session() as session:
            await session.execute(MRFSource.__table__.delete().where(MRFSource.source_id.in_(source_ids)))
            await session.execute(MRFPayer.__table__.delete().where(MRFPayer.payer_id.in_(payer_ids)))
        await db.disconnect()
