# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native regressions requiring the all-writer migration in the standard fixture."""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportDataset,
    CustomImportExecution,
    CustomImportField,
    CustomImportFieldSlot,
    CustomImportLease,
    CustomImportRootScalar,
)
from process.custom_import import materialization_store as store
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import LeaseGrant
from process.custom_import.materialization import (
    GenerationIdentity,
    RootScalarTarget,
    ValidatedWinnerCandidateStream,
    materialize_winners,
    persist_scalar_projections,
    persist_winner_materialization,
    project_root_scalars,
)
from process.custom_import.publication import seal_generation
from process.custom_import.runner_registry import _MATERIALIZATION_WINDOW_KEY, establish_materialization_authority
from process.custom_import.runner_types import CandidateRunnerError
from tests.custom_import_postgres_support import _seed_publication_identity, isolated_publication_case
from tests.test_custom_import_materialization_postgres import (
    _add_definition_fields,
    _assert_index_safe_scalar_persistence,
    _definition,
    _definition_document,
    _live_generation,
    _p4a_family,
    _persist_definition_profiles,
    _persist_index_safe_projections,
    _persist_selected_winner,
    _root_projection_rows,
)


async def _header_count(session):
    connection = await session.connection()
    model_schema = CustomImportRootScalar.__table__.schema
    schema = connection.sync_connection.get_execution_options()["schema_translate_map"][model_schema]
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    installed = await session.scalar(
        text("SELECT to_regclass(:name)"), {"name": f"{quoted}.custom_import_materialization_page"}
    )
    assert installed is not None, "register the completed all-writer migration in the standard PostgreSQL fixture"
    return await session.scalar(text(f"SELECT count(*) FROM {quoted}.custom_import_materialization_page"))


async def _seed(session, suffix):
    seed = await _seed_publication_identity(session, suffix, include_selection_profile=False)
    await _add_definition_fields(session, seed)
    graph, attempt = await _live_generation(session, seed, suffix)
    await _persist_definition_profiles(session, _definition(), graph)
    family = await _p4a_family(session, graph, attempt, suffix)
    return seed, graph, attempt, family


@pytest.mark.asyncio
async def test_standalone_native_pages_can_seal_then_commit_without_runner_binding():
    suffix = uuid.uuid4().hex
    contract = _definition()
    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            assert await _header_count(session) == 0
            _, graph, attempt, family = await _seed(session, suffix)
            _, root_text, child_texts = await _persist_index_safe_projections(session, contract, graph, family, suffix)
            await _assert_index_safe_scalar_persistence(session, family, root_text, child_texts)
            await _persist_selected_winner(session, contract, graph, attempt, family, child_texts, suffix)
            assert await _header_count(session) > 0
            receipt = await seal_generation(
                session,
                dataset_id=graph.dataset_id,
                generation_id=attempt.generation_id,
                lease_fence=attempt.fence,
                lease_token=attempt.token,
            )
            assert (receipt.root_scalar_count, receipt.child_scalar_count, receipt.winner_count) == (1, 4, 1)
            assert _MATERIALIZATION_WINDOW_KEY not in session.info
        async with case.sessions() as session, session.begin():
            assert await _header_count(session) == 0
            assert await session.scalar(select(func.count()).select_from(CustomImportRootScalar)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ("READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"))
async def test_one_standalone_native_page_resolves_multiple_live_producers(isolation):
    suffix = uuid.uuid4().hex
    contract = _definition()
    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            seed, graph, first, family = await _seed(session, suffix)
            _, second = await _live_generation(session, seed, suffix + "second")
            other = await _p4a_family(session, graph, second, suffix + "second")
            assert first.execution_id != second.execution_id
            rows = (
                *_root_projection_rows(contract, graph, family, "first"),
                *_root_projection_rows(contract, graph, other, "second"),
            )
        async with case.sessions() as session, session.begin():
            await session.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
            await _header_count(session)
            assert await persist_scalar_projections(session, contract, root_scalars=rows) == 2
            assert await _header_count(session) == 1
            assert _MATERIALIZATION_WINDOW_KEY not in session.info
        async with case.sessions() as session, session.begin():
            assert await _header_count(session) == 0
            assert set((await session.scalars(select(CustomImportRootScalar.string_value))).all()) == {
                "first",
                "second",
            }


@pytest.mark.asyncio
async def test_later_native_page_failure_rolls_back_earlier_page_and_header(monkeypatch):
    suffix = uuid.uuid4().hex
    contract = _definition()
    monkeypatch.setattr(store, "_PAGE_ROWS", 1)
    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            await _header_count(session)
            _, graph, _, family = await _seed(session, suffix)
            row = _root_projection_rows(contract, graph, family, "first")[0]
        with pytest.raises(DBAPIError, match="scope_or_finality|authority_lost|materialization_deadline"):
            async with case.sessions() as session, session.begin():
                await persist_scalar_projections(
                    session,
                    contract,
                    root_scalars=(row, replace(row, target=replace(row.target, root_revision_id=2**63 - 1))),
                )
        async with case.sessions() as session, session.begin():
            assert await _header_count(session) == 0
            assert await session.scalar(select(func.count()).select_from(CustomImportRootScalar)) == 0


@pytest.mark.asyncio
async def test_deferred_completion_does_not_add_a_standalone_commit_before_expiry_rule():
    suffix = uuid.uuid4().hex
    contract = _definition()
    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            await _header_count(session)
            _, graph, attempt, family = await _seed(session, suffix)
            rows = _root_projection_rows(contract, graph, family, "first")
            assert await persist_scalar_projections(session, contract, root_scalars=rows) == 1
            # Advance only this synthetic lease to its expired state after the
            # protected write. The original deferred link guard did not renew
            # or recheck leases at commit; no wall-clock sleep is necessary.
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == attempt.execution_id)
                .values(expires_at=dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1))
            )
        async with case.sessions() as session, session.begin():
            assert await _header_count(session) == 0
            assert await session.scalar(select(func.count()).select_from(CustomImportRootScalar)) == 1


def _typed_projection_contract():
    """Declare the native boundary values and their immutable field slots."""
    document = _definition_document()
    kinds = ("integer", "decimal", "boolean", "date", "timestamp", "string")
    for slot, kind in enumerate(kinds, start=6):
        document["schema"]["root"]["fields"].append(
            {
                "id": "typed_" + kind,
                "slot": slot,
                "type": kind,
                "nullable": True,
                "projection_slot": slot,
            }
        )
    contract = CustomImportDefinition.from_mapping(document)
    typed_values_by_field = {
        "npi": "synthetic",
        "typed_integer": -(2**63),
        "typed_decimal": Decimal("123456789012345678.123456789012"),
        "typed_boolean": False,
        "typed_date": dt.date(2026, 1, 2),
        "typed_timestamp": dt.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=dt.UTC),
        "typed_string": None,
    }
    return contract, kinds, typed_values_by_field


async def _assert_native_projection_values(session, typed_values_by_field):
    stored_by_field = {
        scalar.field_slot: scalar for scalar in (await session.scalars(select(CustomImportRootScalar))).all()
    }
    assert stored_by_field[6].integer_value == typed_values_by_field["typed_integer"]
    assert stored_by_field[7].decimal_value == typed_values_by_field["typed_decimal"]
    assert stored_by_field[8].boolean_value is False
    assert stored_by_field[9].date_value == typed_values_by_field["typed_date"]
    assert stored_by_field[10].timestamp_value == typed_values_by_field["typed_timestamp"]
    assert stored_by_field[11].value_state == "null" and stored_by_field[11].string_value is None


@pytest.mark.asyncio
async def test_native_values_null_and_missing_match_public_projection_models():
    """Commit typed projections, preserving null and absent-field semantics."""
    suffix = uuid.uuid4().hex
    contract, kinds, typed_values_by_field = _typed_projection_contract()
    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            await _header_count(session)
            seed, graph, _, family = await _seed(session, suffix)
            session.add_all(
                CustomImportFieldSlot(dataset_id=graph.dataset_id, field_slot=slot, field_id="typed_" + kind)
                for slot, kind in enumerate(kinds, start=6)
            )
            await session.flush()
            session.add_all(
                CustomImportField(
                    dataset_id=graph.dataset_id,
                    schema_revision_id=graph.schema_revision_id,
                    field_slot=slot,
                    collection_slot=0,
                    field_name="typed_" + kind,
                    field_type=kind,
                    is_nullable=True,
                    projection_slot=slot,
                )
                for slot, kind in enumerate(kinds, start=6)
            )
            projections = project_root_scalars(
                contract,
                root_target=RootScalarTarget(
                    graph.dataset_id, graph.schema_revision_id, family.root_record_id, family.root_revision_id
                ),
                root_values=typed_values_by_field,
            )
            assert await persist_scalar_projections(session, contract, root_scalars=projections) == 7
            await _assert_native_projection_values(session, typed_values_by_field)
            _, second = await _live_generation(session, seed, suffix + "missing")
            other = await _p4a_family(session, graph, second, suffix + "missing")
            missing = project_root_scalars(
                contract,
                root_target=RootScalarTarget(
                    graph.dataset_id, graph.schema_revision_id, other.root_record_id, other.root_revision_id
                ),
                root_values={"npi": "missing"},
            )
            assert await persist_scalar_projections(session, contract, root_scalars=missing) == 1
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(CustomImportRootScalar)
                    .where(CustomImportRootScalar.root_revision_id == other.root_revision_id)
                )
                == 1
            )
        async with case.sessions() as session, session.begin():
            assert await _header_count(session) == 0


@pytest.mark.asyncio
async def test_empty_native_exports_flush_pending_work_without_authority_or_headers():
    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            assert await _header_count(session) == 0
            pending = CustomImportDataset(dataset_key="synthetic_empty_" + uuid.uuid4().hex[:16])
            session.add(pending)
            assert pending.dataset_id is None
            assert await persist_scalar_projections(session, _definition()) == 0
            assert pending.dataset_id is not None
            generation = GenerationIdentity(1, 1, 1, 1)
            empty = materialize_winners(
                _definition(),
                generation=generation,
                candidates=ValidatedWinnerCandidateStream(generation, ()),
                child_collection_slots={"rates": 1},
            )
            assert await persist_winner_materialization(session, empty) == 0
            assert await _header_count(session) == 0
            assert _MATERIALIZATION_WINDOW_KEY not in session.info


@pytest.mark.asyncio
async def test_real_native_runner_binding_cannot_be_reused_after_root_commit():
    suffix = uuid.uuid4().hex
    async with isolated_publication_case() as case, case.sessions() as session:
        async with session.begin():
            await _header_count(session)
            _, graph, attempt, family = await _seed(session, suffix)
            await session.scalar(
                select(CustomImportDataset).where(CustomImportDataset.dataset_id == graph.dataset_id).with_for_update()
            )
            execution = await session.scalar(
                select(CustomImportExecution)
                .where(CustomImportExecution.execution_id == attempt.execution_id)
                .with_for_update()
            )
            lease = await session.scalar(
                select(CustomImportLease)
                .where(CustomImportLease.execution_id == attempt.execution_id)
                .with_for_update()
            )
            now = await session.scalar(select(func.clock_timestamp()))
            request = SimpleNamespace(
                dataset_id=graph.dataset_id,
                definition_revision_id=graph.definition_revision_id,
                schema_revision_id=graph.schema_revision_id,
                lease_token=attempt.token,
            )
            await establish_materialization_authority(
                session,
                request,
                LeaseGrant(attempt.execution_id, attempt.fence, lease.expires_at, "running"),
                execution,
                lease,
                now,
            )
            assert (
                await persist_scalar_projections(
                    session, _definition(), root_scalars=_root_projection_rows(_definition(), graph, family, "first")
                )
                == 1
            )
        async with session.begin():
            with pytest.raises(CandidateRunnerError, match="not bound"):
                await persist_scalar_projections(session, _definition())
            assert await _header_count(session) == 0
