# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Action-only transport preserves the unchanged native admission oracle."""

from __future__ import annotations

import datetime as dt
import json

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildOccurrence,
    CustomImportChildRevision,
    CustomImportLease,
    CustomImportRejection,
)
from process.custom_import import admission_sql as admission
from process.custom_import import build_source as staging
from process.custom_import import execution as lifecycle
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests.test_custom_import_build_output_postgres import _records, _request_for
from tests.test_custom_import_build_source_batch_postgres import _synthetic_npi
from tests.test_custom_import_build_source_postgres import _candidate_models, _child, _root, _source_case
from tests.test_custom_import_identical_children_postgres import _membership_definition, _membership_records
from tests.test_custom_import_snowflake_bundle import _definition


def _admission_records(scenario):
    if scenario.startswith("membership"):
        return _membership_definition(), _membership_records(missing=scenario == "membership_missing")
    roots = [_root(), _root(_synthetic_npi(1))]
    child_rows = [_child(), _child(_synthetic_npi(1), key="b")]
    if scenario == "initial":
        roots.append(_root(_synthetic_npi(2), score=None))
        child_rows.insert(0, _child(key="invalid", amount=None))
    elif scenario == "duplicate":
        roots.append(_root())
        child_rows.append(_child(_synthetic_npi(1), key="b"))
    elif scenario == "child_first":
        roots.append(_root())
        child_rows.insert(0, _child(key="invalid", amount=None))
    definition_map = json.loads(_definition().canonical)
    if scenario == "child_first":
        definition_map["streams"].reverse()
    return CustomImportDefinition.from_mapping(definition_map), {"root_source": [roots], "detail_source": [child_rows]}


async def _freeze_admission(case, request, monkeypatch):
    async def stop_before_admission(_sessions, _request, build_id, _direct):
        return build_id

    with monkeypatch.context() as patch:
        patch.setattr(staging, "_admit_local_pages", stop_before_admission)
        return await staging.stage_segmented_source(case.sessions, request)


async def _admission_state(session, request, build_id):
    """Normalize only new rejection IDs/timestamps, never missing resolutions."""
    models = await session.run_sync(_candidate_models, request)
    rejections = (
        await session.scalars(
            select(models[CustomImportRejection]).order_by(models[CustomImportRejection].rejection_ordinal)
        )
    ).all()
    rejection_by_id = {rejection.rejection_id: rejection for rejection in rejections}
    occurrences = (
        await session.scalars(
            select(models[CustomImportBuildOccurrence]).order_by(models[CustomImportBuildOccurrence].occurrence_id)
        )
    ).all()
    occurrence_fields = [
        column.name
        for column in models[CustomImportBuildOccurrence].__table__.columns
        if column.name != "resolved_rejection_id"
    ]
    rejection_fields = [
        column.name
        for column in models[CustomImportRejection].__table__.columns
        if column.name not in {"rejection_id", "created_at"}
    ]
    resolutions = [
        None
        if occurrence.resolved_rejection_id is None
        else (
            rejection_by_id[occurrence.resolved_rejection_id].rejection_ordinal,
            rejection_by_id[occurrence.resolved_rejection_id].code,
        )
        for occurrence in occurrences
    ]
    build = (
        await session.execute(
            select(
                CustomImportBuildAttempt.phase,
                CustomImportBuildAttempt.admission_after_occurrence_id,
                CustomImportBuildAttempt.candidate_error_count,
                CustomImportBuildAttempt.next_rejection_ordinal,
            ).where(CustomImportBuildAttempt.build_id == build_id)
        )
    ).one()
    return (
        tuple(build),
        tuple(
            (tuple(getattr(occurrence, field) for field in occurrence_fields), resolution)
            for occurrence, resolution in zip(occurrences, resolutions, strict=True)
        ),
        tuple(tuple(getattr(rejection, field) for field in rejection_fields) for rejection in rejections),
    )


async def _legacy_admission_oracle(case, request, build_id):
    async with staging._page_session(case.sessions, request, build_id) as (session, _build):
        savepoint = await session.begin_nested()
        try:
            result = (
                await staging._call(session, "admit_custom_import_build_page", (("bigint", build_id), ("bigint", 0)))
            ).one()
            return tuple(result), await _admission_state(session, request, build_id)
        finally:
            await savepoint.rollback()


@pytest.mark.parametrize(
    "scenario", ["accepted", "initial", "duplicate", "child_first", "membership_present", "membership_missing"]
)
async def test_action_only_decisions_match_unchanged_native_admission(monkeypatch, scenario):
    definition, stream_batches_by_name = _admission_records(scenario)
    async with _source_case() as case:
        request = await _request_for(case, stream_batches_by_name, definition=definition, page_rows=256)
        build_id = await _freeze_admission(case, request, monkeypatch)
        expected_result, expected_state = await _legacy_admission_oracle(case, request, build_id)
        payloads, execute = [], admission._execute

        async def inspect_actions(session, statement, parameters):
            if "decisions" in parameters and "inserted_n" not in parameters:
                payloads.append((parameters["rows_processed"], parameters["last_id"], tuple(parameters["decisions"])))
            return await execute(session, statement, parameters)

        monkeypatch.setattr(admission, "_execute", inspect_actions)
        admission_result = await admission.admit_source_batch(case.sessions, request, build_id, 0)
        assert tuple(admission_result) == expected_result
        async with case.sessions() as session:
            assert await _admission_state(session, request, build_id) == expected_state
        assert len(payloads) == 1
        rows_processed, last_id, actions = payloads[0]
        assert rows_processed == sum(len(part) for parts in stream_batches_by_name.values() for part in parts)
        assert last_id == expected_result[1] and len(actions) < rows_processed
        assert all(action["own_action"] is True for action in actions)
        assert [action["occurrence_id"] for action in actions] == sorted(action["occurrence_id"] for action in actions)
        if scenario in {"accepted", "membership_present"}:
            assert actions == ()
        if scenario == "initial":
            assert any(
                action["code"] == action["initial_code"] and action["rejection_id"] is not None for action in actions
            )
        if scenario == "child_first":
            assert not any(
                action["record_kind"] == "root" and action["occurrence_id"] == actions[0]["parent_id"]
                for action in actions
            )
        await staging._admit_local_pages(case.sessions, request, build_id, True)
        assert len(payloads) == 1


async def test_actionless_prefix_keeps_later_parent_rejection(monkeypatch):
    stream_batches_by_name = {
        "root_source": [[_root()]],
        "detail_source": [[_child(), _child(amount="3.0")]],
    }
    async with _source_case() as case:
        request = await _request_for(case, stream_batches_by_name, definition=_definition(), page_rows=1)
        build_id = await _freeze_admission(case, request, monkeypatch)
        async with staging._page_session(case.sessions, request, build_id) as (session, _build):
            savepoint = await session.begin_nested()
            try:
                expected_pages, expected_cursor = [], 0
                for _ in range(3):
                    oracle_page = (
                        await staging._call(
                            session,
                            "admit_custom_import_build_page",
                            (("bigint", build_id), ("bigint", expected_cursor)),
                        )
                    ).one()
                    expected_pages.append(tuple(oracle_page))
                    expected_cursor = oracle_page.after_occurrence_id
                expected_state = await _admission_state(session, request, build_id)
            finally:
                await savepoint.rollback()
        payloads, execute = [], admission._execute

        async def inspect_actions(session, statement, parameters):
            if "decisions" in parameters and "inserted_n" not in parameters:
                payloads.append(tuple(parameters["decisions"]))
            return await execute(session, statement, parameters)

        monkeypatch.setattr(admission, "_execute", inspect_actions)
        actual_pages, actual_cursor = [], 0
        for page_index in range(3):
            async with staging._page_session(case.sessions, request, build_id) as (session, build):
                ordinary_page = await admission._admit_locked(session, build, actual_cursor, physical_row_cap=1)
                actual_pages.append(tuple(ordinary_page))
                actual_cursor = ordinary_page.after_occurrence_id
            if page_index == 0:
                assert payloads == [()] and ordinary_page.phase == "admission" and ordinary_page.rows_processed == 1
                async with case.sessions() as session:
                    prefix_state = await _admission_state(session, request, build_id)
                assert prefix_state[0][:2] == ("admission", actual_cursor)
                assert prefix_state[1][0][1] is None and prefix_state[2] == ()
        assert actual_pages == expected_pages
        async with case.sessions() as session:
            final_state = await _admission_state(session, request, build_id)
        assert final_state == expected_state and final_state[0][0] == "graph"
        assert len(payloads) == 3 and len(payloads[1]) == len(payloads[2]) == 1
        assert all(
            action["own_action"]
            and action["code"] == "duplicate_child_key"
            and action["parent_id"] == actual_pages[0][1]
            for actions in payloads[1:]
            for action in actions
        )
        assert final_state[1][0][1] is not None and final_state[1][0][1] == final_state[1][1][1]
        assert final_state[1][0][1] != final_state[1][2][1]


async def _corrupt_admission_input(session, request, mutation):
    models = await session.run_sync(_candidate_models, request)
    occurrence = models[CustomImportBuildOccurrence]
    children = (
        await session.scalars(
            select(occurrence).where(occurrence.record_kind == "child").order_by(occurrence.occurrence_id)
        )
    ).all()
    target_child = children[1]
    if mutation == "invalid_pack":
        root = await session.scalar(
            select(occurrence).where(occurrence.record_kind == "root").order_by(occurrence.occurrence_id)
        )
        updates_by_column = {"pack_id": root.pack_id}
    elif mutation == "raw_collision":
        updates_by_column = {"raw_parent_key_canonical": target_child.raw_parent_key_canonical + " "}
    else:
        child = await session.scalar(
            select(models[CustomImportChildRevision]).where(
                models[CustomImportChildRevision].child_revision_id == target_child.child_revision_id
            )
        )
        await session.execute(
            update(models[CustomImportChildRevision])
            .where(models[CustomImportChildRevision].child_revision_id == child.child_revision_id)
            .values(canonical_child_key=child.canonical_child_key + " ")
        )
        return
    await session.execute(
        update(occurrence).where(occurrence.occurrence_id == target_child.occurrence_id).values(**updates_by_column)
    )


@pytest.mark.parametrize(
    "mutation,problem",
    [
        ("invalid_pack", "custom_import_build_identity_mismatch"),
        ("raw_collision", "raw key digest collision"),
        ("child_collision", "child key digest collision"),
    ],
)
async def test_action_filter_preserves_native_fatal_checks_and_rollback(monkeypatch, mutation, problem):
    stream_batches_by_name = {
        "root_source": [[_root()]],
        "detail_source": [[_child(), _child(key="a" if mutation == "child_collision" else "b")]],
    }
    async with _source_case() as case:
        request = await _request_for(case, stream_batches_by_name, definition=_definition(), page_rows=256)
        build_id = await _freeze_admission(case, request, monkeypatch)
        async with case.sessions() as session, session.begin():
            await _corrupt_admission_input(session, request, mutation)
            unchanged = await _admission_state(session, request, build_id)
        with pytest.raises(DBAPIError, match=problem):
            await _legacy_admission_oracle(case, request, build_id)
        with pytest.raises(admission.AdmissionError, match=problem):
            await admission.admit_source_batch(case.sessions, request, build_id, 0)
        async with case.sessions() as session:
            assert await _admission_state(session, request, build_id) == unchanged


@pytest.mark.parametrize("authority_loss", ["cancellation", "new_fence"])
async def test_action_filter_still_requires_native_live_admission_authority(monkeypatch, authority_loss):
    definition, records = _admission_records("accepted")
    async with _source_case() as case:
        request = await _request_for(case, records, definition=definition, page_rows=256)
        build_id = await _freeze_admission(case, request, monkeypatch)
        async with case.sessions() as session, session.begin():
            unchanged = await _admission_state(session, request, build_id)
            if authority_loss == "cancellation":
                await lifecycle.request_cancellation(session, execution_id=request.execution_id)
            else:
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == request.execution_id)
                    .values(expires_at=func.clock_timestamp() - dt.timedelta(seconds=1))
                )
                grant = await lifecycle.resume_execution(
                    session, execution_id=request.execution_id, token=b"synthetic-next-owner"
                )
                assert grant.fence == request.fence + 1
        with pytest.raises((CancellationRequested, LeaseAuthorityLost)):
            await admission.admit_source_batch(case.sessions, request, build_id, 0)
        async with case.sessions() as session:
            assert await _admission_state(session, request, build_id) == unchanged


async def test_apply_timeout_never_retries_after_empty_decision_payload(monkeypatch):
    async with _source_case() as case:
        request = await _request_for(case, _records(1), page_rows=256)
        build_id = await _freeze_admission(case, request, monkeypatch)
        original, pages = admission._statements, []

        async def timeout_apply(session, family_id):
            statements, relations = await original(session, family_id)
            pages.append(session)
            return (*statements[:2], text("SELECT pg_catalog.pg_sleep(3)"), statements[3]), relations

        monkeypatch.setattr(admission, "_statements", timeout_apply)
        with pytest.raises(DBAPIError) as caught:
            await admission.admit_source_batch(case.sessions, request, build_id, 0)
        assert not hasattr(caught.value, "_custom_import_admission_cursor") and len(pages) == 1
        async with case.sessions() as session:
            build = await session.get(CustomImportBuildAttempt, build_id)
            assert (
                build.phase == "admission" and build.admission_after_occurrence_id == build.candidate_error_count == 0
            )
