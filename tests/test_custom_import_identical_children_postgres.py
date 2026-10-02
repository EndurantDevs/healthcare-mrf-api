# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Repeated child content remains bound to its distinct logical family identity."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGenerationFamily,
    CustomImportRejection,
)
from process.custom_import.runner import run_candidate
from tests import test_custom_import_runner_postgres as runner_fixture
from tests.custom_import_postgres_support import isolated_publication_case


async def _generation_children(session, generation_id):
    return (
        await session.execute(
            select(CustomImportFamilyRevision, CustomImportChildRevision)
            .join(
                CustomImportGenerationFamily,
                CustomImportGenerationFamily.family_revision_id == CustomImportFamilyRevision.family_revision_id,
            )
            .join(
                CustomImportFamilyChild,
                CustomImportFamilyChild.family_revision_id == CustomImportFamilyRevision.family_revision_id,
            )
            .join(
                CustomImportChildRevision,
                CustomImportChildRevision.child_revision_id == CustomImportFamilyChild.child_revision_id,
            )
            .where(CustomImportGenerationFamily.generation_id == generation_id)
        )
    ).all()


def _assert_fresh_children_after_partial_update(previous_child_rows, current_child_rows):
    assert len(previous_child_rows) == len(current_child_rows) == 2
    previous_children_by_parent = {child.canonical_parent_key: child for _family, child in previous_child_rows}
    assert (
        sum(
            previous_children_by_parent[child.canonical_parent_key].canonical_payload == child.canonical_payload
            for _family, child in current_child_rows
        )
        == 1
    )
    for _family, child in current_child_rows:
        previous_child = previous_children_by_parent[child.canonical_parent_key]
        assert child.child_revision_id != previous_child.child_revision_id
        assert child.pack_id != previous_child.pack_id


@pytest.mark.asyncio
async def test_repeated_child_values_keep_distinct_keys_and_parent_membership():
    async with isolated_publication_case() as case:
        seed = await runner_fixture._seed_case(case, "repeated_children")
        execution, token = await runner_fixture._new_execution(case, seed, "repeated_children")
        roots = [runner_fixture._root(npi, "Synthetic") for npi in ("1234567893", "1234567802")]
        child_records = [runner_fixture._rate(root["npi"], code, Decimal("4")) for root in roots for code in ("A", "B")]
        first = await run_candidate(
            case.sessions, runner_fixture._request(seed, execution, token, roots, child_records)
        )
        assert first.status == "activated" and first.accepted_family_count == 2
        assert first.rejection_count == 0
        async with case.sessions() as session:
            family_child_rows = await _generation_children(session, first.generation_id)
            assert len(family_child_rows) == len({child.child_revision_id for _family, child in family_child_rows}) == 4
            assert len({family.family_revision_id for family, _child in family_child_rows}) == 2
            for family, child in family_child_rows:
                assert family.child_count == 2
                assert child.root_record_id == family.root_record_id
            assert len({bytes(child.child_key_sha256) for _family, child in family_child_rows}) == 2
            assert len({bytes(child.parent_key_sha256) for _family, child in family_child_rows}) == 2
        replay_execution, replay_token = await runner_fixture._new_execution(case, seed, "repeated_children_replay")
        replay = await run_candidate(
            case.sessions,
            runner_fixture._request(seed, replay_execution, replay_token, roots, list(reversed(child_records))),
        )
        assert replay.status == "no_change"
        async with case.sessions() as session:
            replay_rows = await _generation_children(session, replay.generation_id)
            assert {(family.root_record_id, bytes(child.child_key_sha256)) for family, child in replay_rows} == {
                (family.root_record_id, bytes(child.child_key_sha256)) for family, child in family_child_rows
            }
            assert len(replay_rows) == 4


@pytest.mark.asyncio
async def test_identical_duplicate_key_rejects_only_its_parent_and_retains_previous_family():
    async with isolated_publication_case() as case:
        seed = await runner_fixture._seed_case(case, "duplicate_content")
        roots = [runner_fixture._root(npi, "Before") for npi in ("1234567893", "1234567802")]
        child_records = [runner_fixture._rate(root["npi"], "A", Decimal("4")) for root in roots]
        first_execution, first_token = await runner_fixture._new_execution(case, seed, "duplicate_content_first")
        first = await run_candidate(
            case.sessions, runner_fixture._request(seed, first_execution, first_token, roots, child_records)
        )
        assert first.status == "activated"
        next_execution, next_token = await runner_fixture._new_execution(case, seed, "duplicate_content_next")
        changed_roots = [runner_fixture._root(root["npi"], "After") for root in roots]
        changed_child_records = [
            child_records[0],
            dict(child_records[0]),
            runner_fixture._rate(roots[1]["npi"], "A", Decimal("8")),
        ]
        changed = await run_candidate(
            case.sessions,
            runner_fixture._request(seed, next_execution, next_token, changed_roots, changed_child_records),
        )
        assert changed.status == "activated" and changed.accepted_family_count == 1
        assert changed.rejection_count == 1
        async with case.sessions() as session:
            rejections = (
                await session.scalars(
                    select(CustomImportRejection).where(CustomImportRejection.execution_id == next_execution)
                )
            ).all()
            assert [rejection.code for rejection in rejections] == ["duplicate_child_key"]
            before = await _generation_children(session, first.generation_id)
            after = await _generation_children(session, changed.generation_id)
        _assert_fresh_children_after_partial_update(before, after)
