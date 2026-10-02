# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Group winners stay selected before filtering, counting, pagination and detail."""

from __future__ import annotations

from decimal import Decimal

import pytest

from process.custom_import.read_core import ExtensionReadAuthorization, PinnedReadTarget, ReadFilter, SearchRequest
from process.custom_import.runner import run_candidate
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_runner_postgres as runner_fixture
from tests.custom_import_postgres_support import isolated_publication_case


def _values(fields):
    return {field.field_id: field.value for field in fields}


async def _grouped_target(case):
    seed = await runner_fixture._seed_case(case, "grouped_read", read_fixture._ranked_family_definition())
    execution, token = await runner_fixture._new_execution(case, seed, "grouped_read")
    roots = [
        runner_fixture._root_with_rank(npi, name, rank)
        for npi in ("1234567893", "1234567802")
        for name, rank in (("Before", 1), ("Selected", 2))
    ]
    child_records = [
        runner_fixture._rate_with_rank(npi, code, Decimal(amount), rank)
        for npi in ("1234567893", "1234567802")
        for code, amount, rank in (("A", "10", 1), ("B", "20", 1), ("A", "4", 2), ("B", "8", 2))
    ]
    run_result = await run_candidate(
        case.sessions, runner_fixture._request(seed, execution, token, roots, child_records)
    )
    assert run_result.status == "activated" and run_result.accepted_family_count == 4
    assert run_result.rejection_count == 0
    return PinnedReadTarget(
        dataset_id=seed.dataset_id,
        generation_id=run_result.generation_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        profile_id="default",
    )


@pytest.mark.asyncio
async def test_grouped_reads_filter_selected_children_before_exact_count_and_pages():
    async with isolated_publication_case() as case:
        pinned_target = await _grouped_target(case)
        service = read_fixture._service()
        authorization = ExtensionReadAuthorization("synthetic-grouped-read")
        filters = (ReadFilter("service_code", "eq", "A"), ReadFilter("amount", "lt", "5"))
        async with case.sessions() as session:
            first = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(target=pinned_target, filters=filters, page_size=1),
            )
            assert first.total == 2 and len(first.items) == 1 and first.next_cursor
            second = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(target=pinned_target, filters=filters, page_size=1, cursor=first.next_cursor),
            )
            assert second.total == 2 and len(second.items) == 1 and second.next_cursor is None
            assert first.items[0].winner != second.items[0].winner
            assert {_values(page_item.root_fields)["npi"] for page_item in (*first.items, *second.items)} == {
                "1234567893",
                "1234567802",
            }
            for page_item in (*first.items, *second.items):
                assert _values(page_item.root_fields)["display_name"] == "Selected"
                assert _values(page_item.context_fields) == {"service_code": "A", "amount": Decimal("4")}
                detail = await service.root_detail(
                    session, authorization=authorization, target=pinned_target, winner=page_item.winner
                )
                assert len(detail.children) == 2
                assert {
                    _values(child.fields)["service_code"]: _values(child.fields)["amount"] for child in detail.children
                } == {"A": Decimal("4"), "B": Decimal("8")}


@pytest.mark.asyncio
async def test_grouped_reads_never_use_siblings_or_older_family_to_satisfy_metrics():
    async with isolated_publication_case() as case:
        pinned_target = await _grouped_target(case)
        service = read_fixture._service()
        authorization = ExtensionReadAuthorization("synthetic-grouped-boundary")
        async with case.sessions() as session:
            rejected = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(
                    target=pinned_target,
                    filters=(ReadFilter("service_code", "eq", "A"), ReadFilter("amount", "gt", "5")),
                    page_size=1,
                ),
            )
            assert (rejected.total, rejected.items, rejected.next_cursor) == (0, (), None)
            accepted = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(
                    target=pinned_target,
                    filters=(ReadFilter("service_code", "eq", "B"), ReadFilter("amount", "gt", "5")),
                ),
            )
            assert accepted.total == len(accepted.items) == 2
            assert {_values(page_item.context_fields)["amount"] for page_item in accepted.items} == {Decimal("8")}
            assert {_values(page_item.root_fields)["display_name"] for page_item in accepted.items} == {"Selected"}
