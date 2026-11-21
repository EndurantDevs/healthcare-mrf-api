# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native projection lifecycle on finalized bounded-build winners."""

import asyncio
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, literal, select, text
from sqlalchemy.exc import IntegrityError

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportCurrentGeneration,
    CustomImportFamilyChild,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportRootScalar,
)
from db.models.custom_import_query_projection import CustomImportQueryProjection
from process.custom_import import build_output as output
from process.custom_import import publication, read_core
from process.custom_import import query_projection_build as projection
from process.custom_import.build_source import _page_session
from process.custom_import.definition import ROOTED_CHILD_TEXT_STORAGE_PROFILE, CustomImportDefinition
from process.custom_import.definition_store import register_definition
from process.custom_import.query_projection_build import (
    ProjectionCursorConflict,
    projection_step,
    stage_query_projections,
)
from process.custom_import.query_projection_default_roots import DEFAULT_ROOTS, default_root_table
from process.custom_import.query_projection_evidence import ProjectionDigest
from process.custom_import.query_projection_sources import capability_sha256
from process.custom_import.query_projection_storage import query_projection_tables
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.runner_codec import child_key_document
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.custom_import_grouped_child_support import child_descriptor
from tests.test_custom_import_build_output_postgres import _activate, _build, _request_for
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_derived_query import derived_document
from tests.test_custom_import_query_projection_sources import _context, _owner_rows
from tests.test_custom_import_read_core_postgres import _service


def _definition(*, default_roots=False, revision=1):
    document = derived_document()
    document["revision"]["definition"] = revision
    if default_roots:
        document["storage_profile"] = ROOTED_CHILD_TEXT_STORAGE_PROFILE
    for stream in document["streams"]:
        stream.update(format="parquet", compression="none")
    return CustomImportDefinition.from_mapping(document)


def _records():
    roots, children = [], []
    for period in (10, 20):
        for group in ("segment_a", "segment_b"):
            roots.append(
                dict(
                    npi="1234567893",
                    display_name="Synthetic",
                    period=period,
                    segment=group,
                    score="0" if group == "segment_a" else "0.000000000001",
                    weight=1,
                )
            )
            if period == 20 and group == "segment_b":
                continue
            for ordinal in range(260 if period == 10 and group == "segment_a" else 1):
                children.append(
                    dict(
                        rate_npi="1234567893",
                        rate_period=period,
                        rate_segment=group,
                        service_code=f"S{ordinal:03}",
                        amount="3",
                        quality="9" if period == 20 else None,
                        quantity=1 if period == 20 else (0 if group == "segment_a" else -1),
                    )
                )
    roots.append(
        dict(npi="1003000126", display_name="Synthetic null", period=10, segment="segment_a", score=None, weight=0)
    )
    return {"providers": [roots], "rates": [children[offset : offset + 100] for offset in range(0, len(children), 100)]}


async def _projection_request(case, rows_by_stream, *, default_roots):
    definition = _definition(default_roots=default_roots)
    async with case.sessions() as session, session.begin():
        seed = await register_definition(
            session, "synthetic_bounded_output", definition, storage_profile_authorized=default_roots
        )
    return await _request_for(case, rows_by_stream, seed=seed, page_rows=64, definition=definition)


async def _winning_build(case, *, rows_by_stream=None, default_roots=True):
    request = await _projection_request(
        case, _records() if rows_by_stream is None else rows_by_stream, default_roots=default_roots
    )
    request = replace(request, statement_timeout_ms=10_000)
    build_id, generation_id = await _build(case, request)
    async with _page_session(case.sessions, request, build_id) as (session, _build_row):
        registry = await load_registry(session, request)
        generation = await session.get(CustomImportGeneration, generation_id)
        session.expunge(generation)
    await output._attach_families(case.sessions, request, build_id)
    await output._write_winners(case.sessions, request, registry, build_id, generation)
    return request, build_id


def _expected_projection_digest(definition, *, default_roots=True):
    digest = ProjectionDigest(definition, default_roots=default_roots)
    digest.add_root("1003000126", 10, {"combined_score": None})
    for period in (10, 20):
        digest.add_root("1234567893", period, {"combined_score": Decimal("0")})
    for ordinal in range(260):
        key = child_key_document(definition, "rates", {"service_code": f"S{ordinal:03}"})
        digest.add_child("1234567893", 10, key, {"combined_quality": None, "merged_cost": Decimal("3")})
    key = child_key_document(definition, "rates", {"service_code": "S000"})
    digest.add_child("1234567893", 20, key, {"combined_quality": Decimal("9"), "merged_cost": Decimal("3")})
    return digest.finish()


async def _assert_physical_projection_reads(case, request, generation_id, *, default_roots):
    await _activate(case, request, generation_id)
    service = _service()
    pinned_target = read_core.PinnedReadTarget(
        request.dataset_id,
        generation_id,
        request.definition_revision_id,
        request.schema_revision_id,
        "families_by_period",
    )
    base_query = read_core.NpiEntityRelationQuery(
        grouped_entity_selection=request.definition.query.entity_selection.document(),
        grouped_child_query=child_descriptor(),
        context_filters=(read_core.ReadFilter("service_code", "eq", "S000"),),
        order_terms=(read_core.ReadOrderTerm("combined_quality", "asc", "last"),),
    )
    async with case.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        for period, group, expected in (
            (10, "segment_b", [None]),
            (20, "segment_a", [Decimal("9")]),
            (20, "segment_b", []),
        ):
            query = replace(
                base_query,
                context_filters=base_query.context_filters
                + (
                    read_core.ReadFilter("period", "eq", period),
                    read_core.ReadFilter("segment", "eq", group),
                ),
            )
            prepared = await service.prepare_npi_entity_relation(
                session,
                authorization=read_core.ExtensionReadAuthorization("synthetic-projection"),
                target=pinned_target,
                query=query,
            )
            statement_text = str(prepared.statement)
            assert "custom_import_derived_child" in statement_text and "div(" not in statement_text
            actual = (await session.execute(prepared.statement)).all()
            assert [context_row.sort_0 for context_row in actual] == expected
            assert await session.scalar(select(func.count()).select_from(prepared.statement.subquery())) == len(
                expected
            )
        await _assert_default_scope_reads(session, service, pinned_target, base_query, default_roots)


async def _assert_default_scope_reads(session, service, target, query, default_roots):
    authorization = read_core.ExtensionReadAuthorization("synthetic-projection")
    query = replace(query, require_match=True)
    for npi, expected in ((1234567893, [("1234567893", Decimal("9"))]), (1003000126, [])):
        scored_pages = []
        native = select(literal(npi, type_=CustomImportRootScalar.integer_value.type).label("npi"))
        for use_default_roots in (False, True):
            baseline = await service.prepare_npi_entity_relation(
                session, authorization=authorization, target=target, query=query
            )
            prepared = await service.scope_npi_entity_relation(
                session,
                authorization=authorization,
                pinned_target=target,
                prepared=baseline,
                query=query,
                native_entity_scope=native,
                use_default_roots=use_default_roots,
            )
            optimized = default_roots and use_default_roots
            statement_sql = str(prepared.statement)
            assert (DEFAULT_ROOTS in statement_sql) is optimized
            assert ("custom_import_generation_family" in statement_sql) is not optimized
            assert prepared.query_fingerprint == baseline.query_fingerprint
            assert prepared.authorization_scope_sha256 == baseline.authorization_scope_sha256
            scored_pages.append((await session.execute(prepared.statement)).all())
        assert scored_pages == [expected, expected]


async def _assert_companion_rows(session, manifest, definition, default_roots):
    table = default_root_table(manifest.family_id)
    assert (
        manifest.projection_sha256
        == _expected_projection_digest(definition, default_roots=default_roots).projection_sha256
    )
    if default_roots:
        actual = (
            await session.execute(select(table.c.entity_value, table.c.selected_value, table.c.group_value))
        ).all()
        assert sorted(actual) == [
            ("1003000126", 10, "segment_a"),
            ("1234567893", 20, "segment_a"),
            ("1234567893", 20, "segment_b"),
        ]
    else:
        assert await session.scalar(select(func.to_regclass(table.fullname))) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("physical_pages", "default_roots"), ((1, True), (32, True), (32, False)))
async def test_projection_resumes_context_pages_and_preserves_all_null_rows(monkeypatch, physical_pages, default_roots):
    monkeypatch.setattr(projection, "_MAX_LOGICAL_PAGES", physical_pages)
    monkeypatch.setattr(projection, "time", SimpleNamespace(monotonic=lambda: 0.0))
    async with _source_case() as case:
        request, build_id = await _winning_build(case, default_roots=default_roots)
        first = await projection_step(case.sessions, request, build_id)
        with pytest.raises(ProjectionCursorConflict):
            await projection_step(case.sessions, request, build_id, expected=first.before)
        for _ in range(100):
            page = await projection_step(case.sessions, request, build_id)
            if (
                "digest_child" in (page.before.phase, page.after.phase)
                and page.after.proof_count > page.before.proof_count
            ):
                break
        else:
            pytest.fail("projection did not reach a retained child digest page")
        with pytest.raises(ProjectionCursorConflict):
            await projection_step(
                case.sessions, request, build_id, expected=replace(page.after, proof_count=page.after.proof_count - 1)
            )
        await stage_query_projections(case.sessions, request, build_id)
        replay = await projection_step(case.sessions, request, build_id)
        assert replay.before == replay.after and replay.after.phase == "complete"
        async with case.sessions() as session, session.begin():
            manifest = (await session.scalars(select(CustomImportQueryProjection))).one()
            assert (manifest.root_count, manifest.child_count, manifest.index_count) == (3, 261, 4)
            await _assert_companion_rows(session, manifest, request.definition, default_roots)
            tables = query_projection_tables(manifest.family_id, request.definition)
            roots = (await session.execute(select(tables["custom_import_derived_root"]))).all()
            assert sorted(
                (context_row.selected_value, context_row.field_1)
                for context_row in roots
                if context_row.field_1 is not None
            ) == [
                (10, Decimal("0.000000000000")),
                (20, Decimal("0.000000000000")),
            ]
            children = (await session.execute(select(tables["custom_import_derived_child"]))).all()
            assert len(children) == 261 and all(context_row.field_3 == 3 for context_row in children)
            assert sum(context_row.field_2 is None for context_row in children) == 260
        sealed = await output.build_output(case.sessions, request, build_id)
        async with case.sessions() as session:
            seal = await session.get(CustomImportGenerationSeal, sealed.generation_id)
            assert seal.query_projection_sha256 == manifest.projection_sha256
            assert (seal.query_projection_root_count, seal.query_projection_child_count) == (3, 261)
        with pytest.raises(CandidateRunnerError):
            await projection_step(case.sessions, request, build_id)
        assert (await output.build_output(case.sessions, request, build_id)).seal.replayed
        await _assert_physical_projection_reads(case, request, sealed.generation_id, default_roots=default_roots)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("extra_binding", "missing_root"))
async def test_default_root_projection_rejects_incomplete_or_extra_coverage(damage):
    """Refuse changed candidate rows before a generation can be sealed or read."""

    async with _source_case() as case:
        request, build_id = await _winning_build(case)
        for _ in range(20):
            if (await projection_step(case.sessions, request, build_id)).after.phase == "verify_root":
                break
        else:
            pytest.fail("projection did not enter native verification")
        async with case.sessions() as session, session.begin():
            manifest = (await session.scalars(select(CustomImportQueryProjection))).one()
            assert manifest.capability_sha256 == capability_sha256(request.definition, default_roots=True)
            table = default_root_table(manifest.family_id)
            if damage == "extra_binding":
                await _inject_extra_binding(session, table)
            else:
                await session.execute(table.delete().where(table.c.entity_value == "1234567893"))
        with pytest.raises(CandidateRunnerError, match="default root projection verified coverage differs"):
            await stage_query_projections(case.sessions, request, build_id)
        async with case.sessions() as session:
            manifest = (await session.scalars(select(CustomImportQueryProjection))).one()
            assert manifest.phase == "verify_root" and manifest.complete_at is None
            assert await session.get(CustomImportGenerationSeal, manifest.generation_id) is None


async def _inject_extra_binding(session, table):
    unowned = table.insert().values(
        family_revision_id=999,
        root_record_id=999,
        root_revision_id=999,
        entity_value="1000000008",
        selected_value=20,
        group_value="segment_a",
    )
    with pytest.raises(IntegrityError) as impossible_binding:
        async with session.begin_nested():
            await session.execute(unowned.values(entity_binding_id=0))
    assert impossible_binding.value.orig.sqlstate == "23514"
    await session.execute(unowned.values(entity_binding_id=999))


@pytest.mark.asyncio
async def test_default_root_projection_resumes_between_periods_of_one_binding(monkeypatch):
    """Split a binding across root pages without duplicating or losing its companion rows."""

    monkeypatch.setattr(projection, "_MAX_LOGICAL_PAGES", 1)
    bounded_metadata = projection._bounded_key_metadata

    async def one_accepted_context(session, source, request, manifest):
        return (await bounded_metadata(session, source, request, manifest))[:1]

    monkeypatch.setattr(projection, "_bounded_key_metadata", one_accepted_context)
    async with _source_case() as case:
        rows_by_stream = _records()
        rows_by_stream["rates"] = [[rows_by_stream["rates"][0][0], rows_by_stream["rates"][-1][-1]]]
        request, build_id = await _winning_build(case, rows_by_stream=rows_by_stream)
        retained_pages = []
        for _ in range(10):
            page = await projection_step(case.sessions, request, build_id)
            if page.after.phase != "root":
                break
            retained_pages.append(page.after)
        else:
            pytest.fail("projection did not finish its root scope")
        assert any(
            left.after_entity_binding_id == right.after_entity_binding_id
            and left.after_selected_value == 10
            and right.after_selected_value == 20
            for left, right in zip(retained_pages, retained_pages[1:])
        )
        async with case.sessions() as session:
            manifest = (await session.scalars(select(CustomImportQueryProjection))).one()
            assert manifest.root_count == 3
            table = default_root_table(manifest.family_id)
            roots = (
                await session.execute(select(table.c.entity_value, table.c.selected_value, table.c.group_value))
            ).all()
        assert sorted(roots) == [
            ("1003000126", 10, "segment_a"),
            ("1234567893", 20, "segment_a"),
            ("1234567893", 20, "segment_b"),
        ]


async def _assert_activation_companion(case, request, generation_id, pointer_version, *, default_roots):
    from process.custom_import.query_projection_evidence import read_projection_tables
    from tests.test_custom_import_segmented_runner_postgres import _ROOTED_TEXT_INDEX

    async with case.sessions() as session, session.begin():
        pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
        assert (pointer.generation_id, pointer.pointer_version) == (generation_id, pointer_version)
        manifest = await session.scalar(
            select(CustomImportQueryProjection).where(CustomImportQueryProjection.generation_id == generation_id)
        )
        tables = await read_projection_tables(
            session, manifest, request.definition, snapshot_family_id=manifest.family_id
        )
        assert (DEFAULT_ROOTS in tables) is default_roots
        index_name = f"{default_root_table(manifest.family_id).schema}.{_ROOTED_TEXT_INDEX}"
        assert (await session.scalar(select(func.to_regclass(index_name))) is not None) is default_roots


async def _approved_successor(case, monkeypatch, definition):
    from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
    from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
    from tests.custom_import_query_projection_authority_postgres import _binding
    from tests.test_custom_import_segmented_capture_postgres import _POLICY
    from tests.test_custom_import_segmented_runner_postgres import _register_profile

    policy = ProcessingPolicy(
        SegmentedCapturePolicy.from_mapping(_POLICY), 17, BuildPolicy(64, 65_536, 10_000, 120, 300)
    )
    with monkeypatch.context() as approved:
        return await _register_profile(
            case, approved, "synthetic_bounded_output", definition, _binding(definition, policy)
        )


@pytest.mark.asyncio
async def test_default_root_projection_can_publish_same_data_then_roll_back(monkeypatch):
    """Publish the new capability through the normal pointer and retain the original edition."""

    from tests.custom_import_query_projection_authority_postgres import _source_definition
    from tests.test_custom_import_segmented_runner_postgres import _install_storage_profile_fixture

    rows_by_stream = _records()
    rows_by_stream["rates"] = [[rows_by_stream["rates"][0][0], rows_by_stream["rates"][-1][-1]]]
    async with _source_case() as case:
        await _install_storage_profile_fixture(case)
        baseline_request = await _request_for(
            case, rows_by_stream, definition=_source_definition(default_roots=False), page_rows=64
        )
        build_id, first_generation = await _build(case, baseline_request)
        await output.build_output(case.sessions, baseline_request, build_id)
        await _activate(case, baseline_request, first_generation)
        definition = _source_definition(default_roots=True, revision=2)
        activated_profile = await _approved_successor(case, monkeypatch, definition)
        next_request = await _request_for(
            case,
            rows_by_stream,
            seed=activated_profile,
            base=first_generation,
            version=1,
            definition=definition,
            page_rows=64,
        )
        build_id, second_generation = await _build(case, next_request)
        sealed = await output.build_output(case.sessions, next_request, build_id)
        assert sealed.generation_id == second_generation and sealed.no_change is None
        await _assert_activation_companion(case, baseline_request, first_generation, 1, default_roots=False)
        await _activate(case, next_request, second_generation, base=first_generation, version=1)
        await _assert_activation_companion(case, next_request, second_generation, 2, default_roots=True)
        async with case.sessions() as session, session.begin():
            rolled_back = await publication.rollback_generation(
                session,
                dataset_id=baseline_request.dataset_id,
                target_generation_id=first_generation,
                expected_generation_id=second_generation,
                expected_pointer_version=2,
            )
        assert rolled_back.event_kind == "rolled_back"
        await _assert_activation_companion(case, baseline_request, first_generation, 3, default_roots=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", (CandidateRunnerError, LeaseAuthorityLost, asyncio.CancelledError))
async def test_later_projection_chunk_rolls_back(monkeypatch, failure_type):
    monkeypatch.setattr(projection, "time", SimpleNamespace(monotonic=lambda: 0.0))
    async with _source_case() as case:
        request, build_id = await _winning_build(case)
        monkeypatch.setattr(projection, "_MAX_LOGICAL_PAGES", 32)
        before = (await projection_step(case.sessions, request, build_id)).after
        assert before.phase == "child" and before.child_count == 0
        original = projection._advance_projection
        written_counts = []

        async def fail_later(session, bound_request, build, manifest):
            await original(session, bound_request, build, manifest)
            written_counts.append(manifest.child_count)
            if len(written_counts) == 2:
                assert manifest.child_count == 2 * request.page_row_limit
                raise failure_type("synthetic later projection chunk failure")

        monkeypatch.setattr(projection, "_advance_projection", fail_later)
        with pytest.raises(failure_type):
            await projection_step(case.sessions, request, build_id, expected=before)
        assert written_counts == [request.page_row_limit, 2 * request.page_row_limit]
        async with case.sessions() as session:
            manifest = (await session.scalars(select(CustomImportQueryProjection))).one()
            assert projection.ProjectionProgress.from_manifest(manifest) == before
            table = query_projection_tables(manifest.family_id, request.definition)["custom_import_derived_child"]
            assert await session.scalar(select(func.count()).select_from(table)) == 0
        monkeypatch.setattr(projection, "_advance_projection", original)
        await stage_query_projections(case.sessions, request, build_id)
        async with case.sessions() as session:
            manifest = (await session.scalars(select(CustomImportQueryProjection))).one()
            assert (manifest.root_count, manifest.child_count) == (3, 261)
            assert manifest.projection_sha256 == _expected_projection_digest(request.definition).projection_sha256


class _WriterReadProbe:
    """Run native validation SQL while refusing premature payload reads or DML."""

    def __init__(self, connection):
        self.connection = connection
        self.payload_reads = 0
        self.write_attempts = 0
        self.ambiguity_reads = 0

    async def execute(self, statement):
        if statement.is_dml:
            self.write_attempts += 1
            raise AssertionError("projection DML preceded source validation")
        columns = set(statement.selected_columns.keys())
        if "canonical_child_key" in columns and "matching_count" not in columns:
            self.payload_reads += 1
            raise AssertionError("canonical payload preceded source validation")
        self.ambiguity_reads += "matching_count" in columns
        return await self.connection.execute(statement)


def _append_child(rows, definition, child_id, *, canonical_key, key_hash, quantity):
    """Clone a finalized owner with its own nullable metric and weight rows."""

    quantity_slot = definition.fields_by_id["quantity"].field_slot
    for model in (CustomImportFamilyChild, CustomImportChildRevision, CustomImportChildScalar):
        copies = [dict(row, child_revision_id=child_id) for row in rows[model] if row["child_revision_id"] == 400]
        for row in copies:
            if model is CustomImportChildRevision:
                row.update(canonical_child_key=canonical_key, child_key_sha256=key_hash)
            if model is CustomImportChildScalar and row["field_slot"] == quantity_slot:
                row["integer_value"] = quantity
        rows[model].extend(copies)


def _ambiguous_roots(rows, definition):
    """Give two distinct finalized roots the same entity, selection and group."""

    period_slot = definition.fields_by_id[definition.query.entity_selection.field_id].field_slot
    row = next(
        row
        for row in rows[CustomImportRootScalar]
        if row["root_revision_id"] == 302 and row["field_slot"] == period_slot
    )
    row["integer_value"] = 2024


def _append_entity(rows, entity_id):
    """Keep every synthetic owner relationship when adding a later entity."""

    identifiers = {"family_revision_id", "root_record_id", "root_revision_id", "child_revision_id"}
    for records in rows.values():
        copies = []
        for row in records:
            owner_by_column = {key: value + 1000 if key in identifiers else value for key, value in row.items()}
            if "entity_binding_id" in owner_by_column:
                owner_by_column["entity_binding_id"] = entity_id
            copies.append(owner_by_column)
        records.extend(copies)


async def _assert_writer_rejects(
    connection, monkeypatch, context, *, phase="child", exhausted=False, expected_scope="complete child key", **limits
):
    """Exercise the real page flow and assert failed integrity never advances it."""

    probe = _WriterReadProbe(connection)
    monkeypatch.setattr(projection, "_source_context", AsyncMock(return_value=context))
    monkeypatch.setattr(projection, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(
        projection, "load_registry", AsyncMock(return_value=SimpleNamespace(child_collection_slots={"rates": 1}))
    )
    request = SimpleNamespace(
        definition=context.definition, **({"page_row_limit": 1, "page_byte_limit": 65536} | limits)
    )
    manifest = SimpleNamespace(
        phase=phase,
        family_id=17,
        after_entity_binding_id=10 if exhausted else 0,
        after_selected_value=2025 if exhausted else None,
        after_child_key_sha256=b"\xff" * 32 if exhausted and phase == "child" else None,
        root_count=0,
        child_count=0,
        capability_sha256=capability_sha256(context.definition),
    )
    original = vars(manifest).copy()
    with pytest.raises(CandidateRunnerError, match=f"query projection {expected_scope} is ambiguous"):
        await projection._write_contexts(probe, request, SimpleNamespace(build_id=1), manifest)
    assert probe.ambiguity_reads > 0
    assert (probe.payload_reads, probe.write_attempts) == (0, 0)
    assert vars(manifest) == original


@pytest.mark.asyncio
async def test_writer_validates_off_page_null_nonpositive_children_before_payload(values_connection, monkeypatch):
    definition = _definition()
    rows = _owner_rows(definition)
    for child_id, quantity in ((403, 0), (404, -1)):
        _append_child(
            rows, definition, child_id, canonical_key='["off-page"]', key_hash=b"\xff" * 32, quantity=quantity
        )
    await _assert_writer_rejects(values_connection, monkeypatch, _context(definition, rows))


@pytest.mark.asyncio
async def test_writer_rejects_colliding_canonical_payloads_before_exceeding_byte_page(values_connection, monkeypatch):
    definition = _definition()
    rows = _owner_rows(definition)
    first, second = '["' + "a" * 1000 + '"]', '["' + "b" * 1000 + '"]'
    for child in rows[CustomImportChildRevision]:
        child["canonical_child_key"] = first
    _append_child(rows, definition, 403, canonical_key=second, key_hash=b"a" * 32, quantity=0)
    byte_limit = 64 + 40 * len(configured_fields(definition)) + len(first.encode())
    assert len(first.encode()) == len(second.encode()) and len((first + second).encode()) > byte_limit
    await _assert_writer_rejects(
        values_connection, monkeypatch, _context(definition, rows), page_row_limit=2, page_byte_limit=byte_limit
    )


@pytest.mark.asyncio
async def test_writer_rejects_one_canonical_child_key_with_different_hashes(values_connection, monkeypatch):
    definition = _definition()
    rows = _owner_rows(definition)
    rows[CustomImportChildRevision][1]["child_key_sha256"] = b"\xff" * 32
    await _assert_writer_rejects(values_connection, monkeypatch, _context(definition, rows))


@pytest.mark.asyncio
async def test_writer_validates_skipped_entity_roots_before_later_child_page(values_connection, monkeypatch):
    definition = _definition()
    rows = _owner_rows(definition)
    _append_entity(rows, 20)
    rows[CustomImportFamilyChild] = [row for row in rows[CustomImportFamilyChild] if row["child_revision_id"] >= 1400]
    _ambiguous_roots(rows, definition)
    await _assert_writer_rejects(
        values_connection, monkeypatch, _context(definition, rows), page_row_limit=2, expected_scope="root context"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("root", "child"))
async def test_writer_validates_exhausted_window_before_advancing_cursor(phase, values_connection, monkeypatch):
    definition = _definition()
    rows = _owner_rows(definition)
    _ambiguous_roots(rows, definition)
    await _assert_writer_rejects(
        values_connection,
        monkeypatch,
        _context(definition, rows),
        phase=phase,
        exhausted=True,
        expected_scope="root context",
    )
