# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL proof for the bounded custom-import read core."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from db.models.custom_import import (
    CustomImportChildScalar,
    CustomImportDefinitionRevision,
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
from process.custom_import.publication import activate_generation, seal_generation
from process.custom_import.read_core import (
    CustomImportReadService,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
    ExtensionReadScope,
    PinnedReadTarget,
    ReadFilter,
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
    seed_running_generation,
    transaction_session,
)


class _AllowSyntheticRead:
    """A synthetic host-side authorization decision for read-core tests."""

    def authorize(self, authorization, *, target):
        del authorization, target
        return ExtensionReadScope("synthetic:read")


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


def _service(*, cache=None) -> CustomImportReadService:
    return CustomImportReadService(
        authorizer=_AllowSyntheticRead(),
        cache=cache,
        cursor_secret=b"r" * 32,
        now=lambda: 1_000,
    )


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


@pytest.mark.asyncio
async def test_read_core_rejects_publication_between_count_and_page(monkeypatch):
    """A concurrent activation cannot produce or cache a torn exact-count page."""

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
        cache = _RecordingCache()
        service = _service(cache=cache)
        authorization = ExtensionReadAuthorization("synthetic-read-token")

        async with case.sessions() as read_session:
            read_task = asyncio.create_task(
                service.search(
                    read_session,
                    authorization=authorization,
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

            with pytest.raises(CustomImportReadUnavailableError, match="changed during"):
                await asyncio.wait_for(read_task, timeout=5)

        assert cache.set_calls == 0
        assert cache.values == {}
