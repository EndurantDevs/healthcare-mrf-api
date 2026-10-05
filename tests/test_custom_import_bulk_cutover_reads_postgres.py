# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected bulk loading keeps immutable API pins through atomic cutover."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from db.models.custom_import import CustomImportCurrentGeneration
from process.custom_import import build_graph, build_output, build_source, publication, read_core
from process.custom_import.read_identity import resolve_generation_snapshot
from tests import test_custom_import_build_output_postgres as fixture
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_read_core_postgres import _service

pytestmark = pytest.mark.asyncio
_AUTHORIZATION = read_core.ExtensionReadAuthorization("synthetic-bulk-cutover")


def _target(request, generation_id):
    return read_core.PinnedReadTarget(
        dataset_id=request.dataset_id,
        generation_id=generation_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        profile_id="default",
    )


async def _page(case, target, amount, *, cursor=None):
    """Verify exact totals and selected child values against one admitted pin."""
    async with case.sessions() as session:
        page = await _service().search(
            session,
            authorization=_AUTHORIZATION,
            request=read_core.SearchRequest(target=target, page_size=1, cursor=cursor),
        )
    assert page.target == target and page.total == 3 and len(page.items) == 1
    values_by_field = {field.field_id: field.value for field in page.items[0].context_fields}
    assert values_by_field["amount"] == Decimal(amount)
    return page


async def _pair(case):
    first_request = await fixture._request_for(case, fixture._records(1, 3, amount="10"))
    _, first = await fixture._complete(case, first_request)
    await fixture._activate(case, first_request, first.generation_id)
    next_request = await fixture._request_for(
        case,
        fixture._records(1, 3, amount="20"),
        seed=first_request,
        base=first.generation_id,
        version=1,
    )
    return first_request, first, next_request


async def _pointer(case, target, version):
    async with case.sessions() as session:
        pointer = await session.get(CustomImportCurrentGeneration, target.dataset_id)
    assert (pointer.generation_id, pointer.pointer_version) == (target.generation_id, version)


async def test_incumbent_reads_survive_bulk_loading_cutover_and_exact_rollback(monkeypatch):
    """A committed candidate batch cannot replace or block the serving snapshot."""
    async with _source_case() as case:
        first_request, first, next_request = await _pair(case)
        old_target = _target(first_request, first.generation_id)
        old_page = await _page(case, old_target, "10")
        assert old_page.next_cursor is not None
        batch_committed, continue_loading = asyncio.Event(), asyncio.Event()
        original = build_source._store_pages

        async def pause_after_commit(*args, **kwargs):
            await original(*args, **kwargs)
            if not batch_committed.is_set():
                batch_committed.set()
                await asyncio.wait_for(continue_loading.wait(), timeout=10)

        monkeypatch.setattr(build_source, "_store_pages", pause_after_commit)
        loading = asyncio.create_task(build_source.stage_segmented_source(case.sessions, next_request))
        try:
            await asyncio.wait_for(batch_committed.wait(), timeout=10)
            await asyncio.wait_for(_page(case, old_target, "10"), timeout=5)
            await _pointer(case, old_target, 1)
        finally:
            continue_loading.set()
            staged = await asyncio.wait_for(loading, timeout=10)
            monkeypatch.setattr(build_source, "_store_pages", original)

        generation_id = await build_graph.build_graph(case.sessions, next_request, staged.build_id)
        sealed = await build_output.build_output(case.sessions, next_request, staged.build_id)
        assert sealed.generation_id == generation_id and sealed.no_change is None
        new_target = _target(next_request, generation_id)
        async with case.sessions() as session:
            old_family = await resolve_generation_snapshot(session, old_target)
            new_family = await resolve_generation_snapshot(session, new_target)
        assert old_family is not None and new_family is not None and old_family != new_family
        second_page = await _page(case, old_target, "10", cursor=old_page.next_cursor)
        assert second_page.items[0].winner != old_page.items[0].winner
        await fixture._activate(case, next_request, generation_id, base=first.generation_id, version=1)
        await _pointer(case, new_target, 2)
        await _page(case, new_target, "20")
        repeated_page = await _page(case, old_target, "10", cursor=old_page.next_cursor)
        assert repeated_page.items[0].winner == second_page.items[0].winner
        async with case.sessions() as session, session.begin():
            rolled_back = await publication.rollback_generation(
                session,
                dataset_id=next_request.dataset_id,
                target_generation_id=first.generation_id,
                expected_generation_id=generation_id,
                expected_pointer_version=2,
            )
        assert rolled_back.event_kind == "rolled_back"
        await _pointer(case, old_target, 3)
        await _page(case, old_target, "10")
        await _page(case, new_target, "20")


async def test_inflight_bulk_snapshot_read_keeps_its_pin_during_activation(monkeypatch):
    """A pointer change between count and hydration cannot mix snapshot values."""
    async with _source_case() as case:
        first_request, first, next_request = await _pair(case)
        _, second = await fixture._complete(case, next_request)
        old_target = _target(first_request, first.generation_id)
        count_complete, continue_read = asyncio.Event(), asyncio.Event()
        original = read_core._exact_count

        async def pause_after_count(session, statement):
            total = await original(session, statement)
            count_complete.set()
            await asyncio.wait_for(continue_read.wait(), timeout=10)
            return total

        monkeypatch.setattr(read_core, "_exact_count", pause_after_count)
        reading = asyncio.create_task(_page(case, old_target, "10"))
        try:
            await asyncio.wait_for(count_complete.wait(), timeout=10)
            await asyncio.wait_for(
                fixture._activate(case, next_request, second.generation_id, base=first.generation_id, version=1),
                timeout=5,
            )
        finally:
            continue_read.set()
            await asyncio.wait_for(reading, timeout=10)
            monkeypatch.setattr(read_core, "_exact_count", original)
        await _page(case, replace(old_target, generation_id=second.generation_id), "20")
