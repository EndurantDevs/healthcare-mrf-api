# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL proof for the bounded custom-import read core."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from db.models.custom_import import (
    CustomImportChildScalar,
    CustomImportCurrentGeneration,
    CustomImportDefinitionRevision,
    CustomImportEntityBinding,
    CustomImportField,
    CustomImportFieldSlot,
    CustomImportRootScalar,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportWinner,
)
from process.custom_import import read_core, read_identity
from process.custom_import.definition import (
    CustomImportDefinition,
    canonical_json,
    canonical_sha256,
)
from process.custom_import.publication import (
    activate_generation,
    record_no_change,
    rollback_generation,
    seal_generation,
)
from process.custom_import.read_core import (
    CustomImportReadCursorError,
    CustomImportReadRequestError,
    CustomImportReadService,
    CustomImportReadUnavailableError,
    EntityLocator,
    ExtensionReadAuthorization,
    ExtensionReadScope,
    PinnedReadTarget,
    ReadFilter,
    RootDetailRequest,
    SearchRequest,
)
from tests.custom_import_postgres_support import (
    FamilyMaterial,
    FamilyMaterialSpec,
    GenerationAttempt,
    PublicationGraph,
    _seed_identity_capture_bundle,
    _seed_identity_schema,
    attach_generation_family,
    digest,
    isolated_publication_case,
    seed_family_material,
    seed_publication_graph,
    seed_running_generation,
    transaction_session,
)


class _AllowSyntheticRead:
    """A synthetic host-side authorization decision for read-core tests."""

    def authorize(self, authorization, *, target):
        del authorization, target
        return ExtensionReadScope("synthetic:read")


class _ScopedSyntheticRead:
    """Issue one synthetic scope per accepted authorization for cursor tests."""

    def authorize(self, authorization, *, target):
        del target
        return ExtensionReadScope(f"synthetic:{authorization.credential}")


@dataclass(frozen=True)
class _ReadFixture:
    target: PinnedReadTarget
    graph: PublicationGraph
    current_attempt: GenerationAttempt
    selected_family: FamilyMaterial
    foreign_child_revision_id: int


@dataclass(frozen=True)
class _ReadIdentity:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    capture_bundle_id: int


class _RecordingCache:
    """Record whether a failed race attempts to cache an inconsistent page."""

    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.set_calls = 0

    async def get(self, key: str) -> object | None:
        return self.values.get(key)

    async def set(self, key: str, value: object, *, expires_at: int) -> None:
        del expires_at
        self.set_calls += 1
        self.values[key] = value


def _source_stream_documents() -> list[dict[str, object]]:
    return [
        {
            "id": "providers",
            "kind": "root",
            "format": "csv",
            "compression": "none",
            "snapshot_token": "snapshot_id",
        },
        {
            "id": "rates",
            "kind": "child",
            "child": "synthetic_children",
            "format": "csv",
            "compression": "none",
            "snapshot_token": "snapshot_id",
        },
    ]


def _root_schema_document() -> dict[str, object]:
    return {
        "logical_key": ["npi"],
        "entity": {"adapter": "npi", "field": "npi"},
        "fields": [
            {
                "id": "npi",
                "slot": 3,
                "type": "string",
                "nullable": False,
                "projection_slot": 3,
            }
        ],
    }


def _child_schema_document() -> dict[str, object]:
    return {
        "name": "synthetic_children",
        "parent_key": [{"child": "rate_npi", "root": "npi"}],
        "child_key": ["child_id"],
        "fields": [
            {
                "id": "synthetic_alpha",
                "slot": 1,
                "type": "string",
                "nullable": False,
                "projection_slot": 1,
            },
            {
                "id": "synthetic_beta",
                "slot": 2,
                "type": "string",
                "nullable": False,
                "projection_slot": 2,
            },
            {"id": "rate_npi", "slot": 4, "type": "string", "nullable": False},
            {"id": "child_id", "slot": 5, "type": "string", "nullable": False},
            {
                "id": "amount",
                "slot": 6,
                "type": "decimal",
                "nullable": True,
                "projection_slot": 4,
            },
        ],
    }


def _aliases_by_stream() -> dict[str, dict[str, str]]:
    return {
        "providers": {"Provider ID": "npi"},
        "rates": {
            "Provider ID": "rate_npi",
            "Child ID": "child_id",
            "Alpha": "synthetic_alpha",
            "Beta": "synthetic_beta",
            "Amount": "amount",
        },
    }


def _definition_document() -> dict[str, object]:
    """Return a single-child query contract matching the synthetic P1 rows."""

    return {
        "contract": "custom-import/v1",
        "revision": {"definition": 1, "schema": 1},
        "refresh_mode": "upsert",
        "streams": _source_stream_documents(),
        "schema": {"root": _root_schema_document(), "children": [_child_schema_document()]},
        "aliases": _aliases_by_stream(),
        "query": {
            "root_fields": ["npi"],
            "child": {
                "collection": "synthetic_children",
                "fields": ["synthetic_alpha", "synthetic_beta", "amount"],
            },
            "order": [{"field": "synthetic_beta", "direction": "asc", "nulls": "last"}],
        },
        "selection_profiles": [
            {
                "id": "synthetic_profile",
                "selection": [{"field": "synthetic_beta", "direction": "asc", "nulls": "last"}],
                "context_dimensions": ["synthetic_alpha"],
            }
        ],
    }


def _profile_document() -> dict[str, object]:
    return {
        "context_dimensions": ["synthetic_alpha"],
        "id": "synthetic_profile",
        "selection": [{"field": "synthetic_beta", "direction": "asc", "nulls": "last"}],
        "scope": {"kind": "child", "collection": "synthetic_children"},
    }


async def _add_declared_field_rows(session, dataset_id: int, schema_revision_id: int) -> None:
    """Add root, identity, and nullable metric fields absent from the shared fixture."""

    session.add_all(
        (
            CustomImportFieldSlot(dataset_id=dataset_id, field_slot=3, field_id="npi"),
            CustomImportFieldSlot(dataset_id=dataset_id, field_slot=4, field_id="rate_npi"),
            CustomImportFieldSlot(dataset_id=dataset_id, field_slot=5, field_id="child_id"),
            CustomImportFieldSlot(dataset_id=dataset_id, field_slot=6, field_id="amount"),
        )
    )
    await session.flush()
    session.add_all(
        (
            CustomImportField(
                schema_revision_id=schema_revision_id,
                dataset_id=dataset_id,
                field_slot=3,
                collection_slot=0,
                field_name="npi",
                field_type="string",
                is_nullable=False,
                projection_slot=3,
            ),
            CustomImportField(
                schema_revision_id=schema_revision_id,
                dataset_id=dataset_id,
                field_slot=4,
                collection_slot=1,
                field_name="rate_npi",
                field_type="string",
                is_nullable=False,
                projection_slot=0,
            ),
            CustomImportField(
                schema_revision_id=schema_revision_id,
                dataset_id=dataset_id,
                field_slot=5,
                collection_slot=1,
                field_name="child_id",
                field_type="string",
                is_nullable=False,
                projection_slot=0,
            ),
            CustomImportField(
                schema_revision_id=schema_revision_id,
                dataset_id=dataset_id,
                field_slot=6,
                collection_slot=1,
                field_name="amount",
                field_type="decimal",
                is_nullable=True,
                projection_slot=4,
            ),
        )
    )
    await session.flush()


async def _seed_read_identity(session, suffix: str) -> _ReadIdentity:
    """Insert a valid immutable definition before the synthetic publication graph."""

    definition = CustomImportDefinition.from_mapping(_definition_document())
    dataset, schema_revision = await _seed_identity_schema(
        session,
        suffix,
        suffix,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    await _add_declared_field_rows(session, dataset.dataset_id, schema_revision.schema_revision_id)
    definition_revision = CustomImportDefinitionRevision(
        dataset_id=dataset.dataset_id,
        schema_revision_id=schema_revision.schema_revision_id,
        revision_number=definition.definition_revision,
        contract_version="custom-import/v1",
        refresh_mode="upsert",
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    session.add(definition_revision)
    await session.flush()
    profile_document = _profile_document()
    session.add(
        CustomImportSelectionProfile(
            definition_revision_id=definition_revision.definition_revision_id,
            dataset_id=dataset.dataset_id,
            schema_revision_id=schema_revision.schema_revision_id,
            profile_slot=1,
            profile_id="synthetic_profile",
            context_collection_slot=1,
            canonical_profile=canonical_json(profile_document),
            profile_sha256=bytes.fromhex(canonical_sha256(profile_document, domain="profile")),
        )
    )
    await session.flush()
    capture_bundle = await _seed_identity_capture_bundle(
        session,
        dataset,
        schema_revision,
        definition_revision,
        suffix,
        suffix,
    )
    return _ReadIdentity(
        dataset_id=dataset.dataset_id,
        definition_revision_id=definition_revision.definition_revision_id,
        schema_revision_id=schema_revision.schema_revision_id,
        capture_bundle_id=capture_bundle.capture_bundle_id,
    )


def _publication_graph(seed: _ReadIdentity) -> PublicationGraph:
    return PublicationGraph(
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        capture_bundle_id=seed.capture_bundle_id,
        first_execution_id=0,
        first_generation_id=0,
        first_materialization_sha256=b"",
        second_execution_id=0,
        second_generation_id=0,
        no_change_execution_id=0,
        no_change_candidate_generation_id=0,
        no_change_token="",
        no_change_fence=0,
    )


async def _add_selected_scalars(session, graph: PublicationGraph, family: FamilyMaterial) -> None:
    """Persist one root value and an explicit-null metric on the selected child."""

    selected_child_revision_id = family.child_revision_ids[1]
    session.add_all(
        (
            CustomImportRootScalar(
                root_revision_id=family.root_revision_id,
                dataset_id=graph.dataset_id,
                schema_revision_id=graph.schema_revision_id,
                root_record_id=family.root_record_id,
                field_slot=3,
                field_collection_slot=0,
                projection_slot=3,
                field_type="string",
                value_state="value",
                string_value="synthetic-root",
            ),
            CustomImportChildScalar(
                child_revision_id=selected_child_revision_id,
                dataset_id=graph.dataset_id,
                schema_revision_id=graph.schema_revision_id,
                root_record_id=family.root_record_id,
                collection_slot=1,
                field_slot=6,
                field_collection_slot=1,
                projection_slot=4,
                field_type="decimal",
                value_state="null",
            ),
        )
    )
    await session.flush()


async def _add_selected_winners(
    session, graph: PublicationGraph, attempt: GenerationAttempt, family: FamilyMaterial
) -> None:
    """Create two deterministic winner rows for exact-count page coverage."""

    selected_child_revision_id = family.child_revision_ids[1]
    session.add_all(
        CustomImportWinner(
            generation_id=attempt.generation_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            profile_slot=1,
            entity_binding_id=family.entity_binding_id,
            family_revision_id=family.family_revision_id,
            context_collection_slot=1,
            context_key_sha256=digest(f"read-core-context:{ordinal}"),
            context_child_revision_id=selected_child_revision_id,
        )
        for ordinal in (1, 2)
    )
    await session.flush()


async def _seed_current_family(
    session, graph: PublicationGraph, suffix: str
) -> tuple[GenerationAttempt, FamilyMaterial]:
    """Seal and activate the sole family whose winner rows the read core may use."""

    attempt = await seed_running_generation(
        session,
        graph,
        suffix=f"read-core-current-{suffix}",
        base_generation_id=None,
        root_count=1,
        family_count=1,
    )
    selected_family = await seed_family_material(
        session,
        graph,
        attempt,
        FamilyMaterialSpec(
            suffix=f"read-core-family-{suffix}",
            child_keys=("high", "low"),
            child_payloads=("high", "low"),
        ),
    )
    await attach_generation_family(session, graph, attempt, selected_family)
    await _add_selected_scalars(session, graph, selected_family)
    await _add_selected_winners(session, graph, attempt, selected_family)
    await seal_generation(
        session,
        dataset_id=graph.dataset_id,
        generation_id=attempt.generation_id,
        lease_fence=attempt.fence,
        lease_token=attempt.token,
    )
    await activate_generation(
        session,
        dataset_id=graph.dataset_id,
        target_generation_id=attempt.generation_id,
        expected_generation_id=None,
        expected_pointer_version=0,
    )
    return attempt, selected_family


async def _seed_foreign_family(
    session,
    graph: PublicationGraph,
    current_attempt: GenerationAttempt,
    selected_family: FamilyMaterial,
    suffix: str,
) -> FamilyMaterial:
    """Persist an unselected sibling family sharing the root identity for detail proof."""

    foreign_attempt = await seed_running_generation(
        session,
        graph,
        suffix=f"read-core-foreign-{suffix}",
        base_generation_id=current_attempt.generation_id,
        root_count=1,
        family_count=1,
    )
    foreign_family = await seed_family_material(
        session,
        graph,
        foreign_attempt,
        FamilyMaterialSpec(
            suffix=f"read-core-foreign-family-{suffix}",
            child_keys=("foreign",),
            root_record_id=selected_family.root_record_id,
            entity_binding_id=selected_family.entity_binding_id,
        ),
    )
    return foreign_family


async def _seed_read_fixture(session) -> _ReadFixture:
    """Build a sealed/current P1 graph with selected, sibling, and foreign children."""

    suffix = uuid.uuid4().hex
    seed = await _seed_read_identity(session, suffix)
    graph = _publication_graph(seed)
    current_attempt, selected_family = await _seed_current_family(session, graph, suffix)
    foreign_family = await _seed_foreign_family(session, graph, current_attempt, selected_family, suffix)
    return _ReadFixture(
        target=PinnedReadTarget(
            dataset_id=graph.dataset_id,
            generation_id=current_attempt.generation_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            profile_id="synthetic_profile",
        ),
        graph=graph,
        current_attempt=current_attempt,
        selected_family=selected_family,
        foreign_child_revision_id=foreign_family.child_revision_ids[0],
    )


def _service(*, cache=None, statement_timeout_ms: int = 10_000, authorizer=None) -> CustomImportReadService:
    return CustomImportReadService(
        authorizer=_AllowSyntheticRead() if authorizer is None else authorizer,
        cache=cache,
        cursor_secret=b"r" * 32,
        now=lambda: 1_000,
        statement_timeout_ms=statement_timeout_ms,
    )


async def _statement_timeout_ms(session) -> int:
    timeout_ms = await session.scalar(
        text("SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'statement_timeout'")
    )
    assert isinstance(timeout_ms, int)
    return timeout_ms


async def _exact_counted_first_page(session, service, authorization, fixture: _ReadFixture):
    selected_request = SearchRequest(
        target=fixture.target,
        filters=(ReadFilter("synthetic_beta", "eq", "low-2"),),
        page_size=1,
    )
    first_page = await service.search(session, authorization=authorization, request=selected_request)
    assert first_page.total == 2
    assert len(first_page.items) == 1
    assert first_page.next_cursor is not None
    assert first_page.items[0].context_child_revision_id == fixture.selected_family.child_revision_ids[1]

    second_page = await service.search(
        session,
        authorization=authorization,
        request=SearchRequest(
            target=fixture.target,
            filters=selected_request.filters,
            page_size=1,
            cursor=first_page.next_cursor,
        ),
    )
    assert second_page.total == 2
    assert len(second_page.items) == 1
    assert second_page.next_cursor is None
    return first_page


async def _assert_selected_child_filtering(session, service, authorization, fixture: _ReadFixture) -> None:
    sibling_only = await service.search(
        session,
        authorization=authorization,
        request=SearchRequest(
            target=fixture.target,
            filters=(ReadFilter("synthetic_beta", "eq", "high-2"),),
        ),
    )
    cross_child_values = await service.search(
        session,
        authorization=authorization,
        request=SearchRequest(
            target=fixture.target,
            filters=(
                ReadFilter("synthetic_alpha", "eq", "high-1"),
                ReadFilter("synthetic_beta", "eq", "low-2"),
            ),
        ),
    )
    assert sibling_only.total == 0
    assert cross_child_values.total == 0


async def _assert_metric_states(session, service, authorization, fixture: _ReadFixture) -> None:
    explicit_null_metric = await service.search(
        session,
        authorization=authorization,
        request=SearchRequest(target=fixture.target, filters=(ReadFilter("amount", "is_null"),)),
    )
    missing_metric = await service.search(
        session,
        authorization=authorization,
        request=SearchRequest(target=fixture.target, filters=(ReadFilter("amount", "is_missing"),)),
    )
    assert explicit_null_metric.total == 2
    assert missing_metric.total == 0


async def _assert_exact_family_detail(session, service, authorization, fixture: _ReadFixture, first_page) -> None:
    detail = await service.root_detail(
        session,
        authorization=authorization,
        target=fixture.target,
        winner=first_page.items[0].winner,
    )
    assert {child.child_revision_id for child in detail.children} == set(fixture.selected_family.child_revision_ids)
    assert fixture.foreign_child_revision_id not in {child.child_revision_id for child in detail.children}
    amount_state_by_child_id = {
        child.child_revision_id: next(field.state for field in child.fields if field.field_id == "amount")
        for child in detail.children
    }
    assert amount_state_by_child_id[fixture.selected_family.child_revision_ids[0]] == "missing"
    assert amount_state_by_child_id[fixture.selected_family.child_revision_ids[1]] == "null"
    entity_binding = await session.get(CustomImportEntityBinding, fixture.selected_family.entity_binding_id)
    assert entity_binding is not None
    entity_detail = await service.root_detail_for_entity(
        session,
        authorization=authorization,
        request=RootDetailRequest(
            target=fixture.target,
            entity=EntityLocator(entity_binding.adapter_id, entity_binding.canonical_value),
            family_entitlement="full_family",
        ),
    )
    assert entity_detail.winner.family_revision_id == fixture.selected_family.family_revision_id
    assert {child.child_revision_id for child in entity_detail.children} == set(
        fixture.selected_family.child_revision_ids
    )


@pytest.mark.asyncio
async def test_read_core_uses_one_selected_child_and_exact_family_membership():
    """Search predicates never fall back, while detail returns every family child."""

    async with transaction_session() as session, session.begin():
        fixture = await _seed_read_fixture(session)
        service = _service()
        authorization = ExtensionReadAuthorization("synthetic-read-token")
        first_page = await _exact_counted_first_page(session, service, authorization, fixture)
        await _assert_selected_child_filtering(session, service, authorization, fixture)
        await _assert_metric_states(session, service, authorization, fixture)
        await _assert_exact_family_detail(session, service, authorization, fixture, first_page)


@pytest.mark.asyncio
async def test_read_core_restores_caller_statement_timeout_after_reads_and_request_failure():
    """The bounded read window never widens or leaks its transaction-local setting."""

    async with transaction_session() as session, session.begin():
        fixture = await _seed_read_fixture(session)
        await session.execute(
            text("SELECT set_config('statement_timeout', :timeout_text, true)"),
            {"timeout_text": "17000"},
        )
        service = _service(statement_timeout_ms=5_000)
        authorization = ExtensionReadAuthorization("synthetic-read-token")
        first_page = await service.search(
            session,
            authorization=authorization,
            request=SearchRequest(target=fixture.target, page_size=1),
        )
        assert await _statement_timeout_ms(session) == 17_000

        await service.root_detail(
            session,
            authorization=authorization,
            target=fixture.target,
            winner=first_page.items[0].winner,
        )
        assert await _statement_timeout_ms(session) == 17_000

        with pytest.raises(CustomImportReadRequestError, match="filter field"):
            await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(
                    target=fixture.target,
                    filters=(ReadFilter("undeclared_field", "eq", "synthetic"),),
                ),
            )
        assert await _statement_timeout_ms(session) == 17_000

        await session.execute(
            text("SELECT set_config('statement_timeout', :timeout_text, true)"),
            {"timeout_text": "700"},
        )
        async with read_core._bounded_read_window(session, timeout_ms=5_000):
            assert await _statement_timeout_ms(session) == 700
        assert await _statement_timeout_ms(session) == 700


@pytest.mark.asyncio
async def test_read_core_maps_database_timeout_and_never_caches_partial_page(monkeypatch):
    """A real PostgreSQL cancellation fails closed and leaves rollback to the caller."""

    async with isolated_publication_case() as case:
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_read_fixture(seed_session)
        original_exact_count = read_core._exact_count

        async def delayed_exact_count(delayed_session, statement):
            await delayed_session.execute(text("SELECT pg_sleep(1)"))
            return await original_exact_count(delayed_session, statement)

        monkeypatch.setattr(read_core, "_exact_count", delayed_exact_count)
        async with case.sessions() as session:
            cache = _RecordingCache()
            service = _service(cache=cache, statement_timeout_ms=20)
            with pytest.raises(CustomImportReadUnavailableError, match="^bounded read is unavailable$"):
                await service.search(
                    session,
                    authorization=ExtensionReadAuthorization("synthetic-read-token"),
                    request=SearchRequest(target=fixture.target),
                )

            assert cache.set_calls == 0
            assert cache.values == {}
            assert session.in_transaction()
            await session.rollback()
            assert await session.scalar(text("SELECT 1")) == 1


@pytest.mark.asyncio
async def test_read_core_applies_the_same_database_timeout_to_detail(monkeypatch):
    """Detail hydration cannot bypass the common bounded read window."""

    async with isolated_publication_case() as case:
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_read_fixture(seed_session)
        authorization = ExtensionReadAuthorization("synthetic-read-token")
        async with case.sessions() as session:
            first_page = await _service().search(
                session,
                authorization=authorization,
                request=SearchRequest(target=fixture.target, page_size=1),
            )
            await session.rollback()
            original_selected_winner_row = read_core._selected_winner_row

            async def delayed_selected_winner_row(delayed_session, context, winner):
                await delayed_session.execute(text("SELECT pg_sleep(1)"))
                return await original_selected_winner_row(delayed_session, context, winner)

            monkeypatch.setattr(read_core, "_selected_winner_row", delayed_selected_winner_row)
            cache = _RecordingCache()
            service = _service(cache=cache, statement_timeout_ms=20)
            with pytest.raises(CustomImportReadUnavailableError, match="^bounded read is unavailable$"):
                await service.root_detail(
                    session,
                    authorization=authorization,
                    target=fixture.target,
                    winner=first_page.items[0].winner,
                )

            assert cache.set_calls == 0
            assert cache.values == {}
            await session.rollback()
            assert await session.scalar(text("SELECT 1")) == 1


@pytest.mark.asyncio
async def test_read_core_enforces_one_cumulative_operation_deadline(monkeypatch):
    """Individually short steps cannot collectively exceed the read budget."""

    async with isolated_publication_case() as case:
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_read_fixture(seed_session)

        async def delayed_exact_count(delayed_session, statement):
            del delayed_session, statement
            await asyncio.sleep(0.26)
            return 2

        async def delayed_page_winner_rows(delayed_session, statement, context, plan, offset):
            del delayed_session, statement, context, plan, offset
            await asyncio.sleep(0.26)
            return ()

        monkeypatch.setattr(read_core, "_exact_count", delayed_exact_count)
        monkeypatch.setattr(read_core, "_page_winner_rows", delayed_page_winner_rows)
        async with case.sessions() as session:
            cache = _RecordingCache()
            service = _service(cache=cache, statement_timeout_ms=500)
            with pytest.raises(CustomImportReadUnavailableError, match="^bounded read is unavailable$"):
                await service.search(
                    session,
                    authorization=ExtensionReadAuthorization("synthetic-read-token"),
                    request=SearchRequest(target=fixture.target),
                )

            assert cache.set_calls == 0
            assert cache.values == {}
            assert await _statement_timeout_ms(session) == 0
            assert await session.scalar(text("SELECT 1")) == 1


@pytest.mark.asyncio
async def test_read_core_does_not_cache_when_timeout_restoration_fails(monkeypatch):
    """A restoration failure rejects an otherwise complete page before caching."""

    async with isolated_publication_case() as case:
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_read_fixture(seed_session)

        async def failed_restoration(session, previous_timeout_text, *, has_read_failed):
            del session, previous_timeout_text, has_read_failed
            raise RuntimeError("synthetic restoration failure")

        monkeypatch.setattr(read_core, "_restore_statement_timeout", failed_restoration)
        async with case.sessions() as session:
            cache = _RecordingCache()
            service = _service(cache=cache)
            with pytest.raises(RuntimeError, match="^synthetic restoration failure$"):
                await service.search(
                    session,
                    authorization=ExtensionReadAuthorization("synthetic-read-token"),
                    request=SearchRequest(target=fixture.target),
                )

            assert cache.set_calls == 0
            assert cache.values == {}
            await session.rollback()


@pytest.mark.asyncio
async def test_read_core_preserves_external_task_cancellation(monkeypatch):
    """Caller cancellation remains distinct from the service-owned deadline."""

    async with isolated_publication_case() as case:
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_read_fixture(seed_session)
        delayed_step_started = asyncio.Event()

        async def interrupted_exact_count(delayed_session, statement):
            del delayed_session, statement
            delayed_step_started.set()
            await asyncio.sleep(60)
            return 0

        monkeypatch.setattr(read_core, "_exact_count", interrupted_exact_count)
        async with case.sessions() as session:
            cache = _RecordingCache()
            read_task = asyncio.create_task(
                _service(cache=cache).search(
                    session,
                    authorization=ExtensionReadAuthorization("synthetic-read-token"),
                    request=SearchRequest(target=fixture.target),
                )
            )
            await asyncio.wait_for(delayed_step_started.wait(), timeout=5)
            read_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await read_task

            assert cache.set_calls == 0
            assert cache.values == {}
            assert await _statement_timeout_ms(session) == 0


@pytest.mark.parametrize(
    ("attribute", "invalid_value"),
    (
        ("canonical_schema", '{"unexpected":true}'),
        ("schema_sha256", b"x" * 32),
    ),
)
@pytest.mark.asyncio
async def test_read_core_rejects_schema_content_and_digest_mismatches(attribute, invalid_value):
    """A revision number alone cannot substitute for exact persisted schema identity."""

    async with transaction_session() as session, session.begin():
        fixture = await _seed_read_fixture(session)
        schema_revision = await session.get(CustomImportSchemaRevision, fixture.target.schema_revision_id)
        definition_revision = await session.get(
            CustomImportDefinitionRevision,
            fixture.target.definition_revision_id,
        )
        assert schema_revision is not None
        assert definition_revision is not None
        mismatched_schema = SimpleNamespace(
            revision_number=schema_revision.revision_number,
            canonical_schema=schema_revision.canonical_schema,
            schema_sha256=schema_revision.schema_sha256,
        )
        setattr(mismatched_schema, attribute, invalid_value)

        with pytest.raises(CustomImportReadUnavailableError, match="persisted definition identity"):
            read_identity.verified_definition(definition_revision, mismatched_schema)


async def _sealed_empty_successor(session, fixture: _ReadFixture) -> GenerationAttempt:
    successor = await seed_running_generation(
        session,
        fixture.graph,
        suffix=f"read-core-successor-{uuid.uuid4().hex}",
        base_generation_id=fixture.current_attempt.generation_id,
    )
    await seal_generation(
        session,
        dataset_id=fixture.graph.dataset_id,
        generation_id=successor.generation_id,
        lease_fence=successor.fence,
        lease_token=successor.token,
    )
    return successor


async def _current_target(session, fixture: _ReadFixture) -> PinnedReadTarget:
    pointer = await session.get(CustomImportCurrentGeneration, fixture.graph.dataset_id)
    assert pointer is not None
    return PinnedReadTarget(
        dataset_id=pointer.dataset_id,
        generation_id=pointer.generation_id,
        definition_revision_id=pointer.definition_revision_id,
        schema_revision_id=pointer.schema_revision_id,
        profile_id=fixture.target.profile_id,
    )


async def _assert_retained_cursor_behavior(
    read_session,
    service,
    authorization,
    fixture: _ReadFixture,
    successor: GenerationAttempt,
    first_request: SearchRequest,
    first_page,
) -> None:
    """Verify cache, cursor, and current-target behavior after supersession."""

    assert (
        await service.search(
            read_session,
            authorization=authorization,
            request=first_request,
        )
        is first_page
    )
    resumed_page = await service.search(
        read_session,
        authorization=authorization,
        request=SearchRequest(target=fixture.target, page_size=1, cursor=first_page.next_cursor),
    )
    assert resumed_page.total == 2
    assert len(resumed_page.items) == 1
    assert resumed_page.next_cursor is None

    current_target = await _current_target(read_session, fixture)
    assert current_target.generation_id == successor.generation_id
    current_page = await service.search(
        read_session,
        authorization=authorization,
        request=SearchRequest(target=current_target, page_size=1),
    )
    assert current_page.total == 0

    for request, cursor_authorization in (
        (
            SearchRequest(
                target=fixture.target,
                filters=(ReadFilter("npi", "eq", "synthetic-root"),),
                page_size=1,
                cursor=first_page.next_cursor,
            ),
            authorization,
        ),
        (
            SearchRequest(target=current_target, page_size=1, cursor=first_page.next_cursor),
            authorization,
        ),
        (
            SearchRequest(target=fixture.target, page_size=1, cursor=first_page.next_cursor),
            ExtensionReadAuthorization("synthetic-other-token"),
        ),
    ):
        with pytest.raises(CustomImportReadCursorError):
            await service.search(read_session, authorization=cursor_authorization, request=request)


@pytest.mark.asyncio
async def test_read_core_reads_retained_pinned_generation_after_publication():
    """A retained pin and its cursor survive a newer current publication."""

    async with isolated_publication_case() as case:
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_read_fixture(seed_session)
            successor = await _sealed_empty_successor(seed_session, fixture)

        cache = _RecordingCache()
        service = _service(cache=cache, authorizer=_ScopedSyntheticRead())
        authorization = ExtensionReadAuthorization("synthetic-read-token")
        first_request = SearchRequest(target=fixture.target, page_size=1)

        async with case.sessions() as read_session:
            first_page = await service.search(
                read_session,
                authorization=authorization,
                request=first_request,
            )
        assert first_page.total == 2
        assert first_page.next_cursor is not None
        assert cache.set_calls == 1

        async with case.sessions() as publish_session, publish_session.begin():
            await activate_generation(
                publish_session,
                dataset_id=fixture.graph.dataset_id,
                target_generation_id=successor.generation_id,
                expected_generation_id=fixture.current_attempt.generation_id,
                expected_pointer_version=1,
            )
            rollback = await rollback_generation(
                publish_session,
                dataset_id=fixture.graph.dataset_id,
                target_generation_id=fixture.current_attempt.generation_id,
                expected_generation_id=successor.generation_id,
                expected_pointer_version=2,
            )
            assert rollback.event_kind == "rolled_back"
            await activate_generation(
                publish_session,
                dataset_id=fixture.graph.dataset_id,
                target_generation_id=successor.generation_id,
                expected_generation_id=fixture.current_attempt.generation_id,
                expected_pointer_version=3,
            )

        async with case.sessions() as read_session:
            await _assert_retained_cursor_behavior(
                read_session,
                service,
                authorization,
                fixture,
                successor,
                first_request,
                first_page,
            )


@pytest.mark.asyncio
async def test_read_core_keeps_retained_pin_after_pointer_move(monkeypatch):
    """A pointer move cannot invalidate an already admitted retained target."""

    async with isolated_publication_case() as case:
        async with case.sessions() as seed_session, seed_session.begin():
            fixture = await _seed_read_fixture(seed_session)
            successor = await _sealed_empty_successor(seed_session, fixture)

        count_complete = asyncio.Event()
        continue_read = asyncio.Event()
        original_exact_count = read_core._exact_count

        async def delayed_exact_count(session, statement):
            total = await original_exact_count(session, statement)
            count_complete.set()
            await continue_read.wait()
            return total

        monkeypatch.setattr(read_core, "_exact_count", delayed_exact_count)
        async with case.sessions() as read_session:
            cache = _RecordingCache()
            read_task = asyncio.create_task(
                _service(cache=cache).search(
                    read_session,
                    authorization=ExtensionReadAuthorization("synthetic-read-token"),
                    request=SearchRequest(target=fixture.target),
                )
            )
            try:
                await asyncio.wait_for(count_complete.wait(), timeout=5)
                async with case.sessions() as publish_session, publish_session.begin():
                    await activate_generation(
                        publish_session,
                        dataset_id=fixture.graph.dataset_id,
                        target_generation_id=successor.generation_id,
                        expected_generation_id=fixture.current_attempt.generation_id,
                        expected_pointer_version=1,
                    )
            finally:
                continue_read.set()

            page = await asyncio.wait_for(read_task, timeout=5)

    assert page.total == 2
    assert len(page.items) == 2
    assert cache.set_calls == 1


@pytest.mark.asyncio
async def test_read_core_rejects_unsealed_or_mismatched_pinned_targets():
    """Only the exact sealed generation, dataset, schema, and profile may read."""

    async with transaction_session() as session, session.begin():
        fixture = await _seed_read_fixture(session)
        unsealed = await seed_running_generation(
            session,
            fixture.graph,
            suffix=f"read-core-unsealed-{uuid.uuid4().hex}",
            base_generation_id=fixture.current_attempt.generation_id,
        )
        sealed_never_published = await _sealed_empty_successor(session, fixture)
        service = _service()
        authorization = ExtensionReadAuthorization("synthetic-read-token")
        for target in (
            replace(fixture.target, generation_id=2**62),
            replace(fixture.target, generation_id=unsealed.generation_id),
            replace(fixture.target, generation_id=sealed_never_published.generation_id),
            replace(fixture.target, dataset_id=fixture.target.dataset_id + 1),
            replace(fixture.target, definition_revision_id=fixture.target.definition_revision_id + 1),
            replace(fixture.target, schema_revision_id=fixture.target.schema_revision_id + 1),
            replace(fixture.target, profile_id="unknown_profile"),
        ):
            with pytest.raises(CustomImportReadUnavailableError):
                await service.search(
                    session,
                    authorization=authorization,
                    request=SearchRequest(target=target),
                )


@pytest.mark.asyncio
async def test_read_identity_rejects_sealed_no_change_candidate_without_target_event():
    """A no-change receipt publishes its retained base, never its candidate."""

    async with transaction_session() as session, session.begin():
        graph = await seed_publication_graph(session)
        await activate_generation(
            session,
            dataset_id=graph.dataset_id,
            target_generation_id=graph.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
        receipt = await record_no_change(
            session,
            dataset_id=graph.dataset_id,
            execution_id=graph.no_change_execution_id,
            expected_generation_id=graph.first_generation_id,
            expected_pointer_version=1,
            candidate_generation_id=graph.no_change_candidate_generation_id,
            lease_fence=graph.no_change_fence,
            lease_token=graph.no_change_token,
        )
        assert receipt.to_generation_id == graph.first_generation_id
        pinned_target = PinnedReadTarget(
            dataset_id=graph.dataset_id,
            generation_id=graph.first_generation_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            profile_id="synthetic_profile",
        )

        await read_identity.verify_published_generation(session, pinned_target)
        with pytest.raises(CustomImportReadUnavailableError, match="pinned generation"):
            await read_identity.verify_published_generation(
                session,
                replace(pinned_target, generation_id=graph.no_change_candidate_generation_id),
            )
