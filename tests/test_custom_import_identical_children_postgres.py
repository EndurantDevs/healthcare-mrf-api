# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native source-retention, admission, resume and publication parity for equal children."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from itertools import count
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, inspect, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildFamily,
    CustomImportBuildOccurrence,
    CustomImportCaptureBundle,
    CustomImportChildRevision,
    CustomImportCurrentGeneration,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportRejection,
)
from process.custom_import import build_graph, build_output, build_source
from process.custom_import import execution as lifecycle
from process.custom_import.admission_sql import AdmissionError
from process.custom_import.build_counts import count_source_outcomes
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner import run_candidate
from process.custom_import.runner_codec import new_family_hash
from process.custom_import.runner_types import CancellationRequested
from tests import test_custom_import_runner_postgres as runner_fixture
from tests.custom_import_postgres_support import _migration, isolated_publication_case
from tests.test_custom_import_build_output_postgres import (
    _activate,
    _assert_legacy_parity,
    _complete,
    _definition,
    _records,
    _request_for,
)
from tests.test_custom_import_build_source_postgres import _candidate_models, _source_case
from tests.test_custom_import_snowflake_shared_capture import _shared_definition

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20261002030000_custom_import_identical_children.py"


def _migrate(connection, schema_name, *, downgrade=False):
    migration = _migration(_MIGRATION, "custom_import_identical_children_test_migration")
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(connection))
    (migration.downgrade if downgrade else migration.upgrade)()


def _configured_definition(*, revision=1):
    document = json.loads(_definition().canonical)
    document["revision"]["definition"] = revision
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    return CustomImportDefinition.from_mapping(document)


def _duplicate_records(*, amount="10.000"):
    records = _records(2)
    records["rates"].append([records["rates"][0][0] | {"amount": amount}])
    return records


async def test_identical_children_resume_with_final_occurrence(monkeypatch):
    definition = _configured_definition()
    source_records = _duplicate_records()
    original = build_source._store_pages
    acknowledgements = count()

    async def lost_acknowledgement(*args, **kwargs):
        await original(*args, **kwargs)
        if next(acknowledgements) == 0:
            raise ConnectionError("synthetic lost source-page acknowledgement")

    async with _source_case() as case:
        request = await _request_for(case, source_records, definition=definition, page_rows=8)
        monkeypatch.setattr(build_source, "_store_pages", lost_acknowledgement)
        with pytest.raises(ConnectionError, match="acknowledgement"):
            await build_source.stage_segmented_source(case.sessions, request)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        assert staged.source_occurrence_count == 5
        assert await build_source.stage_segmented_source(case.sessions, request) == staged
        counts = await count_source_outcomes(case.sessions, request, staged.build_id)
        assert (counts.accepted_family_count, counts.rejection_count) == (2, 0)
        await build_graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await build_output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.family_child_count == 2 and sealed.seal.winner_count == 1
        await _assert_legacy_parity(case, request, sealed)
        replay = await build_output.build_output(case.sessions, request, staged.build_id)
        assert replay.seal.replayed
        eager = assemble_root_families(
            definition, source_records["providers"][0], {"rates": sum(source_records["rates"], [])}
        )
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            capture = (await session.scalars(select(CustomImportCaptureBundle))).one()
            assert capture.committed_record_count == 5 and capture.committed_part_count == 3
            occurrences = (await session.scalars(select(models[CustomImportBuildOccurrence]))).all()
            assert len(occurrences) == 5 and all(entry.resolved_rejection_id is None for entry in occurrences)
            children = (await session.scalars(select(models[CustomImportChildRevision]))).all()
            assert len(children) == 3
            selected_ids = set(await session.scalars(select(models[CustomImportFamilyChild].child_revision_id)))
            assert selected_ids == {child.child_revision_id for child in children if child.source_ordinal in {1, 2}}
            families = (await session.scalars(select(models[CustomImportFamilyRevision]))).all()
            assert sorted(bytes(family.family_sha256) for family in families) == sorted(
                new_family_hash(definition, family) for family in eager.families
            )


@pytest.mark.parametrize("enabled,amount,accepted", [(False, "10.000", 1), (True, "12", 1), (True, "bad", 1)])
async def test_database_default_conflict_and_invalid_rows_keep_family_rejection(enabled, amount, accepted):
    async with _source_case() as case:
        definition = _configured_definition() if enabled else _definition()
        request = await _request_for(case, _duplicate_records(amount=amount), definition=definition)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        counts = await count_source_outcomes(case.sessions, request, staged.build_id)
        assert (counts.accepted_family_count, counts.rejection_count) == (accepted, 1)
        await build_graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await build_output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.family_count == sealed.seal.family_child_count == accepted
        async with case.sessions() as session:
            build = await session.get(CustomImportBuildAttempt, staged.build_id)
            assert build.source_occurrence_count == 5


async def test_policy_revision_preserves_previous_family_when_payloads_conflict():
    async with _source_case() as case:
        initial = await _request_for(case, _records(2), definition=_definition())
        _, base = await _complete(case, initial)
        await _activate(case, initial, base.generation_id)
        source_records = _duplicate_records(amount="12")
        source_records["rates"][0][1]["amount"] = "21"
        request = await _request_for(
            case, source_records, definition=_configured_definition(revision=2), base=base.generation_id, version=1
        )
        assert request.schema_revision_id == initial.schema_revision_id
        build_id, completed = await _complete(case, request)
        assert completed.seal.family_count == completed.seal.family_child_count == 2
        await _assert_legacy_parity(case, request, completed)
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            plans = (
                await session.scalars(
                    select(models[CustomImportBuildFamily]).where(models[CustomImportBuildFamily].build_id == build_id)
                )
            ).all()
            assert sorted(plan.selection_kind for plan in plans) == ["retained", "source"]
            previous_models = await session.run_sync(_candidate_models, initial)
            occurrence_count = await session.scalar(
                select(func.count()).select_from(models[CustomImportBuildOccurrence])
            )
            previous_count = await session.scalar(
                select(func.count()).select_from(previous_models[CustomImportBuildOccurrence])
            )
            assert occurrence_count + previous_count == 11


async def test_final_lookup_uses_ordered_partial_index():
    source_records = _records(64)
    first_child = source_records["rates"][0][0]
    source_records["rates"] = [[first_child] * 64, sum(source_records["rates"], [])]
    async with _source_case() as case:
        request = await _request_for(case, source_records, definition=_configured_definition(), page_rows=256)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            occurrence_model = models[CustomImportBuildOccurrence]
            occurrence = await session.scalar(
                select(occurrence_model)
                .where(occurrence_model.child_revision_id.is_not(None))
                .order_by(occurrence_model.source_ordinal)
                .limit(1)
            )
            relation = session.get_bind().dialect.identifier_preparer.format_table(inspect(occurrence_model).selectable)
            await session.execute(text(f"ANALYZE {relation}"))
            plan = await session.scalar(
                text(f"""EXPLAIN (ANALYZE, FORMAT JSON)
                    SELECT child_revision_id FROM {relation}
                    WHERE build_id=:build AND origin='source' AND stream_slot=:stream AND root_record_id=:root
                      AND collection_slot=:collection AND raw_parent_key_sha256=:raw AND child_key_sha256=:child
                      AND child_revision_id IS NOT NULL ORDER BY source_ordinal DESC LIMIT 1"""),
                dict(
                    build=staged.build_id,
                    stream=occurrence.stream_slot,
                    root=occurrence.root_record_id,
                    collection=occurrence.collection_slot,
                    raw=occurrence.raw_parent_key_sha256,
                    child=occurrence.child_key_sha256,
                ),
            )
            lookup = plan[0]["Plan"]["Plans"][0]
            assert lookup["Index Name"] == "custom_import_build_final_child_idx"
            assert lookup["Actual Rows"] == 1 and lookup.get("Rows Removed by Filter", 0) == 0


@pytest.mark.parametrize("enabled", [False, True])
async def test_payload_comparison_respects_page_byte_bound(enabled):
    document = json.loads((_configured_definition() if enabled else _definition()).canonical)
    document["schema"]["children"][0]["fields"].append({"id": "note", "slot": 7, "type": "string", "nullable": True})
    definition = CustomImportDefinition.from_mapping(document)
    source_records = _duplicate_records()
    for part in source_records["rates"]:
        for child in part:
            child["note"] = "x" * 800
    async with _source_case() as case:
        request = await _request_for(case, source_records, definition=definition)
        prepared = build_source._prepare_row(request, definition.source_streams[1], source_records["rates"][0][0])
        request = replace(request, page_byte_limit=prepared.byte_count)
        if enabled:
            with pytest.raises(AdmissionError, match="custom_import_build_page_too_large"):
                await build_source.stage_segmented_source(case.sessions, request)
            async with case.sessions() as session:
                models = await session.run_sync(_candidate_models, request)
                assert await session.scalar(select(func.count()).select_from(models[CustomImportBuildOccurrence])) == 5
        else:
            assert (await build_source.stage_segmented_source(case.sessions, request)).phase == "graph"


async def test_forward_migration_restores_old_functions_only_without_retained_policy():
    async with isolated_publication_case(migration_through="20261002030000") as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(lambda conn: _migrate(conn, case.schema_name, downgrade=True))
            index = await connection.scalar(
                text("SELECT to_regclass(:name)"), {"name": f"{case.schema_name}.custom_import_build_final_child_idx"}
            )
            assert index is None
            await connection.run_sync(_migrate, case.schema_name)
        await _request_for(case, _records(2), definition=_configured_definition())
        with pytest.raises(DBAPIError, match="custom_import_identical_children_retention_required"):
            async with case.engine.begin() as connection:
                await connection.run_sync(lambda conn: _migrate(conn, case.schema_name, downgrade=True))


def _migrate_memberships(connection, schema_name, *, downgrade=False):
    path = _MIGRATION.with_name("20261002040000_custom_import_child_memberships.py")
    migration = _migration(path, "custom_import_child_memberships_test_migration")
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(connection))
    (migration.downgrade if downgrade else migration.upgrade)()


def _membership_definition(*, enabled=True, revision=1, refresh="snapshot", reverse=False):
    document = json.loads(_shared_definition(interleaved=True).canonical)
    document["revision"]["definition"] = revision
    document["refresh_mode"] = refresh
    root = document["schema"]["root"]
    root["logical_key"].append("panel")
    root["fields"].append({"id": "panel", "slot": 10, "type": "string", "nullable": False})
    for collection, slot in zip(document["schema"]["children"], (11, 14), strict=True):
        prefix = "detail" if collection["name"] == "details" else "other"
        collection["parent_key"].append({"child": f"{prefix}_panel", "root": "panel"})
        collection["child_key"].append(f"{prefix}_version")
        collection["fields"].extend(
            [
                {"id": f"{prefix}_panel", "slot": slot, "type": "string", "nullable": False},
                {"id": f"{prefix}_version", "slot": slot + 1, "type": "integer", "nullable": False},
            ]
        )
        if prefix == "other":
            collection["child_key"].append("measure_id")
            collection["fields"].append({"id": "measure_id", "slot": 16, "type": "integer", "nullable": False})
    if reverse:
        document["streams"][1:] = reversed(document["streams"][1:])
    if enabled:
        document["child_memberships"] = [
            {
                "outer_collection": "details",
                "inner_collection": "other",
                "key_mapping": [
                    {"outer_field": "detail_id", "inner_field": "other_id"},
                    {"outer_field": "detail_version", "inner_field": "other_version"},
                ],
            }
        ]
    return CustomImportDefinition.from_mapping(document)


def _membership_records(*, inner_count=1, missing=False, panel_suffix=""):
    key = 'Key "quoted" \\ café\n'
    return {
        "root_source": [
            [dict(npi="1003000126", panel=panel + panel_suffix, score="1", enabled=True) for panel in ("a", "b")]
        ],
        "detail_source": [
            [
                dict(
                    detail_npi="1003000126",
                    detail_panel=panel + panel_suffix,
                    detail_id=key,
                    detail_version=7,
                    amount="2",
                )
            ]
            for panel in (("b",) if missing else ("a", "b"))
        ],
        "other_source": [
            [
                dict(
                    other_npi="1003000126",
                    other_panel=panel + panel_suffix,
                    other_id=key,
                    other_version=7,
                    measure_id=index,
                    other_amount="3",
                )
                for index in range(inner_count)
            ]
            for panel in ("a", "b")
        ],
    }


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("missing", [False, True])
async def test_membership_admission_is_typed_root_scoped_and_independent_of_pack_order(reverse, missing):
    async with _source_case() as case:
        definition = _membership_definition(reverse=reverse)
        records_by_stream = _membership_records(missing=missing)
        request = await _request_for(case, records_by_stream, definition=definition)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        assert staged.phase == "graph" and staged.candidate_error_count == 0
        assert await build_source.stage_segmented_source(case.sessions, request) == staged
        counts = await count_source_outcomes(case.sessions, request, staged.build_id)
        assert (counts.accepted_family_count, counts.rejection_count) == ((1, 1) if missing else (2, 0))
        eager = assemble_root_families(
            definition,
            records_by_stream["root_source"][0],
            {
                "details": sum(records_by_stream["detail_source"], []),
                "other": sum(records_by_stream["other_source"], []),
            },
        )
        assert len(eager.families) == counts.accepted_family_count
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            occurrence_model = models[CustomImportBuildOccurrence]
            rejected = (await session.scalars(select(models[CustomImportRejection]))).all()
            assert [rejection.code for rejection in rejected] == (["child_membership_missing"] if missing else [])
            assert all(rejection.canonical_evidence == "{}" for rejection in rejected)
            resolved = (
                await session.scalars(
                    select(occurrence_model).where(occurrence_model.resolved_rejection_id.is_not(None))
                )
            ).all()
            assert sorted(occurrence.record_kind for occurrence in resolved) == (["child", "root"] if missing else [])
        await build_graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await build_output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.family_count == counts.accepted_family_count
        await _assert_legacy_parity(case, request, sealed)


@pytest.mark.parametrize("enabled", [False, True])
async def test_membership_absence_is_legacy_and_every_declared_mapping_is_required(enabled):
    document = json.loads(_membership_definition(enabled=enabled).canonical)
    if enabled:
        document["child_memberships"].append(
            {
                **document["child_memberships"][0],
                "key_mapping": [
                    {"outer_field": "detail_id", "inner_field": "other_id"},
                    {"outer_field": "detail_version", "inner_field": "measure_id"},
                ],
            }
        )
    async with _source_case() as case:
        request = await _request_for(
            case, _membership_records(), definition=CustomImportDefinition.from_mapping(document)
        )
        staged = await build_source.stage_segmented_source(case.sessions, request)
        counts = await count_source_outcomes(case.sessions, request, staged.build_id)
        assert (counts.accepted_family_count, counts.rejection_count) == ((0, 2) if enabled else (2, 0))
        assert staged.candidate_error_count == 0


@pytest.mark.parametrize("reverse", [False, True])
async def test_duplicate_inner_key_keeps_native_primary_rejection_before_membership(reverse):
    definition = _membership_definition(reverse=reverse)
    records_by_stream = _membership_records(missing=True)
    records_by_stream["other_source"].append([records_by_stream["other_source"][0][0] | {"other_amount": "4"}])
    eager = assemble_root_families(
        definition,
        records_by_stream["root_source"][0],
        {"details": sum(records_by_stream["detail_source"], []), "other": sum(records_by_stream["other_source"], [])},
    )
    assert {rejection.code for rejection in eager.rejections} == {"duplicate_child_key", "child_membership_missing"}
    async with _source_case() as case:
        request = await _request_for(case, records_by_stream, definition=definition)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        counts = await count_source_outcomes(case.sessions, request, staged.build_id)
        assert counts.accepted_family_count == len(eager.families) == 1
        assert counts.rejection_count == 1 and staged.candidate_error_count == 0
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            codes = (await session.scalars(select(models[CustomImportRejection].code))).all()
            assert codes == ["duplicate_child_key", "duplicate_child_key"]
        await build_graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await build_output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.family_count == 1
        await _assert_legacy_parity(case, request, sealed)


async def _plan_membership_page(case, request, build_id):
    async with build_graph._page_session(case.sessions, request, build_id) as (session, current):
        return await build_graph._call(
            session, "plan_custom_import_build_family_page", (build_id, current.plan_page_sequence), ("progress",)
        )


async def _membership_progress(case, build_id):
    async with case.sessions() as session:
        return (
            await session.execute(
                text(f'''SELECT plan_page_sequence,plan_complete_at,
            plan_membership_after_collection_slot,plan_membership_after_child_revision_id,selected_family_count
            FROM "{case.schema_name}".custom_import_build_attempt WHERE build_id=:build'''),
                {"build": build_id},
            )
        ).one()


@pytest.mark.parametrize("refresh", ["snapshot", "upsert"])
async def test_new_memberships_revalidate_and_resume_compatible_retained_families(refresh):
    async with _source_case() as case:
        initial = await _request_for(
            case, _membership_records(inner_count=13), definition=_membership_definition(enabled=False, refresh=refresh)
        )
        _, base = await _complete(case, initial)
        await _activate(case, initial, base.generation_id)
        records_by_stream = (
            _membership_records(missing=True)
            if refresh == "snapshot"
            else {name: [[]] for name in _membership_records()}
        )
        request = await _request_for(
            case,
            records_by_stream,
            definition=_membership_definition(revision=2, refresh=refresh),
            base=base.generation_id,
            version=1,
        )
        assert request.schema_revision_id == initial.schema_revision_id
        staged = await build_source.stage_segmented_source(case.sessions, request)
        first_page = await _plan_membership_page(case, request, staged.build_id)
        progress = await _membership_progress(case, staged.build_id)
        assert first_page.rows_processed == request.page_row_limit and not first_page.plan_complete
        assert progress.plan_membership_after_child_revision_id > 0 and progress.selected_family_count == 0
        with pytest.raises(DBAPIError, match="custom_import_build_progress_conflict"):
            async with build_graph._page_session(case.sessions, request, staged.build_id) as (session, _):
                await build_graph._call(
                    session, "plan_custom_import_build_family_page", (staged.build_id, 0), ("progress",)
                )
        assert await _membership_progress(case, staged.build_id) == progress
        await build_graph.build_graph(case.sessions, request, staged.build_id)
        sealed = await build_output.build_output(case.sessions, request, staged.build_id)
        assert sealed.seal.family_count == 2
        await _assert_legacy_parity(case, request, sealed)
        completed = await _membership_progress(case, staged.build_id)
        assert completed.plan_page_sequence > progress.plan_page_sequence + 1
        assert completed.plan_membership_after_child_revision_id == completed.plan_membership_after_collection_slot == 0


@pytest.mark.parametrize("refresh", ["snapshot", "upsert"])
async def test_incompatible_late_retained_child_blocks_candidate_before_any_copy_or_generation(refresh):
    async with _source_case() as case:
        records_by_stream = _membership_records(inner_count=13)
        records_by_stream["other_source"][0][12]["other_version"] = 8
        initial = await _request_for(
            case, records_by_stream, definition=_membership_definition(enabled=False, refresh=refresh)
        )
        _, base = await _complete(case, initial)
        await _activate(case, initial, base.generation_id)
        candidate_records_by_stream = (
            _membership_records(missing=True) if refresh == "snapshot" else {name: [[]] for name in records_by_stream}
        )
        request = await _request_for(
            case,
            candidate_records_by_stream,
            definition=_membership_definition(revision=2, refresh=refresh),
            base=base.generation_id,
            version=1,
        )
        staged = await build_source.stage_segmented_source(case.sessions, request)
        await _plan_membership_page(case, request, staged.build_id)
        before = await _membership_progress(case, staged.build_id)
        assert before.plan_membership_after_child_revision_id > 0
        with pytest.raises(DBAPIError, match="retained family violates child membership"):
            await build_graph.build_graph(case.sessions, request, staged.build_id)
        assert await _membership_progress(case, staged.build_id) == before
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            occurrence_model = models[CustomImportBuildOccurrence]
            assert await session.scalar(select(func.count()).select_from(CustomImportGeneration)) == 1
            pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
            assert (pointer.generation_id, pointer.pointer_version) == (base.generation_id, 1)
            copied = (
                select(func.count())
                .select_from(occurrence_model)
                .where(
                    occurrence_model.build_id == staged.build_id,
                    occurrence_model.origin == "retained",
                )
            )
            assert await session.scalar(copied) == 0


@pytest.mark.parametrize("failure", ["cancelled", "byte_bound"])
async def test_retained_membership_pages_remain_fenced_and_byte_bounded(failure):
    async with _source_case() as case:
        records_by_stream = _membership_records(inner_count=2)
        initial = await _request_for(
            case, records_by_stream, definition=_membership_definition(enabled=False, refresh="upsert")
        )
        _, base = await _complete(case, initial)
        await _activate(case, initial, base.generation_id)
        request = await _request_for(
            case,
            {name: [[]] for name in records_by_stream},
            definition=_membership_definition(revision=2, refresh="upsert"),
            base=base.generation_id,
            version=1,
            page_rows=1,
        )
        if failure == "byte_bound":
            request = replace(request, page_byte_limit=256)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        if failure == "cancelled":
            page = await _plan_membership_page(case, request, staged.build_id)
            assert page.rows_processed == 1 and not page.plan_complete
            async with case.sessions() as session, session.begin():
                await lifecycle.request_cancellation(session, execution_id=request.execution_id)
        progress = await _membership_progress(case, staged.build_id)
        expected = CancellationRequested if failure == "cancelled" else DBAPIError
        message = "candidate execution is canceling" if failure == "cancelled" else "custom_import_build_page_too_large"
        with pytest.raises(expected, match=message):
            await _plan_membership_page(case, request, staged.build_id)
        assert await _membership_progress(case, staged.build_id) == progress


@pytest.mark.parametrize("retain_prior", [False, True])
async def test_membership_schema_check_applies_only_to_actually_retained_families(retain_prior):
    async with _source_case() as case:
        initial = await _request_for(case, _membership_records(), definition=_membership_definition(enabled=False))
        _, base = await _complete(case, initial)
        await _activate(case, initial, base.generation_id)
        document = json.loads(_membership_definition(revision=2).canonical)
        document["revision"]["schema"] = 2
        document["schema"]["root"]["fields"].append({"id": "extra", "slot": 17, "type": "string", "nullable": True})
        records_by_stream = _membership_records(missing=retain_prior, panel_suffix="" if retain_prior else "_new")
        request = await _request_for(
            case,
            records_by_stream,
            definition=CustomImportDefinition.from_mapping(document),
            base=base.generation_id,
            version=1,
        )
        assert request.schema_revision_id != initial.schema_revision_id
        if retain_prior:
            staged = await build_source.stage_segmented_source(case.sessions, request)
            with pytest.raises(DBAPIError, match="retained schema differs"):
                await build_graph.build_graph(case.sessions, request, staged.build_id)
        else:
            _, sealed = await _complete(case, request)
            assert sealed.seal.family_count == 2
            await _assert_legacy_parity(case, request, sealed)
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
            assert (pointer.generation_id, pointer.pointer_version) == (base.generation_id, 1)


async def test_membership_migration_roundtrip_retains_checks_for_declared_policy():
    async with isolated_publication_case(migration_through="20261002040000") as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(lambda conn: _migrate_memberships(conn, case.schema_name, downgrade=True))
            await connection.run_sync(_migrate_memberships, case.schema_name)
            guard_is_definer = await connection.scalar(
                text("SELECT prosecdef FROM pg_proc WHERE oid=to_regprocedure(:name)"),
                {"name": f"{case.schema_name}.guard_custom_import_build_owned()"},
            )
            assert not guard_is_definer
        await _request_for(case, _membership_records(), definition=_membership_definition())
        with pytest.raises(DBAPIError, match="custom_import_child_memberships_retention_required"):
            async with case.engine.begin() as connection:
                await connection.run_sync(lambda conn: _migrate_memberships(conn, case.schema_name, downgrade=True))


async def _generation_children(session, generation_id, request):
    generation = await session.get(CustomImportGeneration, generation_id)
    assert generation.execution_id == request.execution_id
    producer = SimpleNamespace(**(vars(request) | {"fence": generation.producing_fence}))
    models = await session.run_sync(_candidate_models, producer)
    family = models[CustomImportFamilyRevision]
    await _assert_ordinary_serving_indexes(session, request, inspect(family).selectable.schema)
    child = models[CustomImportChildRevision]
    generation_family = models[CustomImportGenerationFamily]
    family_child = models[CustomImportFamilyChild]
    return (
        await session.execute(
            select(family, child)
            .join(
                generation_family,
                generation_family.family_revision_id == family.family_revision_id,
            )
            .join(
                family_child,
                family_child.family_revision_id == family.family_revision_id,
            )
            .join(
                child,
                child.child_revision_id == family_child.child_revision_id,
            )
            .where(generation_family.generation_id == generation_id)
        )
    ).all()


async def _assert_ordinary_serving_indexes(session, request, namespace):
    """Ordinary seal and no-change require the complete candidate serving set."""

    assert (
        await session.scalar(
            select(func.count(CustomImportBuildAttempt.build_id)).where(
                CustomImportBuildAttempt.execution_id == request.execution_id
            )
        )
        == 0
    )
    index_names = set(
        await session.scalars(
            text("SELECT indexname FROM pg_indexes WHERE schemaname=:namespace AND indexname LIKE '%scalar%_idx'"),
            {"namespace": namespace},
        )
    )
    assert index_names == {
        f"custom_import_{record_kind}_scalar_{value_kind}_idx"
        for record_kind in ("root", "child")
        for value_kind in ("text", "int", "number", "date", "time")
    }


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
        request = runner_fixture._request(seed, execution, token, roots, child_records)
        first = await run_candidate(case.sessions, request)
        assert first.status == "activated" and first.accepted_family_count == 2
        assert first.rejection_count == 0
        async with case.sessions() as session:
            family_child_rows = await _generation_children(session, first.generation_id, request)
            assert len(family_child_rows) == len({child.child_revision_id for _family, child in family_child_rows}) == 4
            assert len({family.family_revision_id for family, _child in family_child_rows}) == 2
            for family, child in family_child_rows:
                assert family.child_count == 2
                assert child.root_record_id == family.root_record_id
            assert len({bytes(child.child_key_sha256) for _family, child in family_child_rows}) == 2
            assert len({bytes(child.parent_key_sha256) for _family, child in family_child_rows}) == 2
        replay_execution, replay_token = await runner_fixture._new_execution(case, seed, "repeated_children_replay")
        replay_request = runner_fixture._request(
            seed, replay_execution, replay_token, roots, list(reversed(child_records))
        )
        replay = await run_candidate(case.sessions, replay_request)
        assert replay.status == "no_change"
        async with case.sessions() as session:
            replay_rows = await _generation_children(session, replay.generation_id, replay_request)
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
        initial = runner_fixture._request(seed, first_execution, first_token, roots, child_records)
        first = await run_candidate(case.sessions, initial)
        assert first.status == "activated"
        next_execution, next_token = await runner_fixture._new_execution(case, seed, "duplicate_content_next")
        changed_roots = [runner_fixture._root(root["npi"], "After") for root in roots]
        changed_child_records = [
            child_records[0],
            dict(child_records[0]),
            runner_fixture._rate(roots[1]["npi"], "A", Decimal("8")),
        ]
        request = runner_fixture._request(seed, next_execution, next_token, changed_roots, changed_child_records)
        changed = await run_candidate(case.sessions, request)
        assert changed.status == "activated" and changed.accepted_family_count == 1
        assert changed.rejection_count == 1
        async with case.sessions() as session:
            generation = await session.get(CustomImportGeneration, changed.generation_id)
            producer = SimpleNamespace(**(vars(request) | {"fence": generation.producing_fence}))
            models = await session.run_sync(_candidate_models, producer)
            rejections = (
                await session.scalars(
                    select(models[CustomImportRejection]).where(
                        models[CustomImportRejection].execution_id == next_execution
                    )
                )
            ).all()
            assert [rejection.code for rejection in rejections] == ["duplicate_child_key"]
            before = await _generation_children(session, first.generation_id, initial)
            after = await _generation_children(session, changed.generation_id, request)
        _assert_fresh_children_after_partial_update(before, after)
