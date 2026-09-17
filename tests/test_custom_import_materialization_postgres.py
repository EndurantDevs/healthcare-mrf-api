# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL proof for P4a rows sealed by the P3 finality path."""

from __future__ import annotations

import datetime as dt
import hashlib
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, IntegrityError

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportEntityBinding,
    CustomImportExecution,
    CustomImportField,
    CustomImportFieldSlot,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportRootScalar,
    CustomImportSelectionProfile,
    CustomImportWinner,
)
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.materialization import (
    ChildScalarTarget,
    DefinitionIdentity,
    GenerationIdentity,
    RootScalarTarget,
    ValidatedWinnerCandidateStream,
    WinnerCandidate,
    materialize_winners,
    persist_scalar_projections,
    persist_selection_profiles,
    persist_winner_materialization,
    project_child_scalars,
    project_root_scalars,
    scalar_projection_models,
)
from process.custom_import.publication import seal_generation
from tests.custom_import_postgres_support import (
    FamilyMaterial,
    FamilyMaterialSpec,
    GenerationAttempt,
    PublicationGraph,
    _seed_publication_identity,
    attach_generation_family,
    digest,
    lease_digest,
    seed_family_material,
    transaction_session,
)


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
            "child": "rates",
            "format": "csv",
            "compression": "none",
            "snapshot_token": "snapshot_id",
        },
    ]


def _schema_document() -> dict[str, object]:
    return {
        "root": {
            "logical_key": ["npi"],
            "entity": {"adapter": "npi", "field": "npi"},
            "fields": [{"id": "npi", "slot": 3, "type": "string", "nullable": False, "projection_slot": 3}],
        },
        "children": [
            {
                "name": "rates",
                "parent_key": [{"child": "rate_npi", "root": "npi"}],
                "child_key": ["child_id"],
                "fields": [
                    {"id": "synthetic_alpha", "slot": 1, "type": "string", "nullable": False, "projection_slot": 1},
                    {"id": "synthetic_beta", "slot": 2, "type": "string", "nullable": False, "projection_slot": 2},
                    {"id": "rate_npi", "slot": 4, "type": "string", "nullable": False},
                    {"id": "child_id", "slot": 5, "type": "string", "nullable": False},
                ],
            }
        ],
    }


def _definition_document() -> dict[str, object]:
    return {
        "contract": "custom-import/v1",
        "revision": {"definition": 1, "schema": 1},
        "refresh_mode": "upsert",
        "streams": _source_stream_documents(),
        "schema": _schema_document(),
        "aliases": {
            "providers": {"Provider ID": "npi"},
            "rates": {
                "Provider ID": "rate_npi",
                "Child ID": "child_id",
                "Alpha": "synthetic_alpha",
                "Beta": "synthetic_beta",
            },
        },
        "query": {
            "root_fields": ["npi"],
            "child": {"collection": "rates", "fields": ["synthetic_alpha", "synthetic_beta"]},
            "order": [{"field": "synthetic_beta", "direction": "asc", "nulls": "last"}],
        },
        "selection_profiles": [
            {
                "id": "default",
                "selection": [{"field": "synthetic_beta", "direction": "asc", "nulls": "last"}],
                "context_dimensions": ["synthetic_alpha"],
            }
        ],
    }


def _definition() -> CustomImportDefinition:
    """Return a small synthetic definition matching the persisted hot slots."""

    return CustomImportDefinition.from_mapping(_definition_document())


async def _add_definition_fields(session, publication_seed) -> None:
    """Add synthetic root/non-hot fields before any generation can seal."""

    session.add_all(
        (
            CustomImportFieldSlot(dataset_id=publication_seed.dataset.dataset_id, field_slot=3, field_id="npi"),
            CustomImportFieldSlot(dataset_id=publication_seed.dataset.dataset_id, field_slot=4, field_id="rate_npi"),
            CustomImportFieldSlot(dataset_id=publication_seed.dataset.dataset_id, field_slot=5, field_id="child_id"),
        )
    )
    await session.flush()
    session.add_all(
        (
            CustomImportField(
                schema_revision_id=publication_seed.schema_revision.schema_revision_id,
                dataset_id=publication_seed.dataset.dataset_id,
                field_slot=3,
                collection_slot=0,
                field_name="npi",
                field_type="string",
                is_nullable=False,
                projection_slot=3,
            ),
            CustomImportField(
                schema_revision_id=publication_seed.schema_revision.schema_revision_id,
                dataset_id=publication_seed.dataset.dataset_id,
                field_slot=4,
                collection_slot=1,
                field_name="rate_npi",
                field_type="string",
                is_nullable=False,
                projection_slot=0,
            ),
            CustomImportField(
                schema_revision_id=publication_seed.schema_revision.schema_revision_id,
                dataset_id=publication_seed.dataset.dataset_id,
                field_slot=5,
                collection_slot=1,
                field_name="child_id",
                field_type="string",
                is_nullable=False,
                projection_slot=0,
            ),
        )
    )
    await session.flush()


async def _new_live_execution(session, publication_seed, suffix: str) -> tuple[CustomImportExecution, str]:
    lease_token = f"synthetic-materialization-{suffix}"
    now = dt.datetime.now(dt.UTC)
    execution = CustomImportExecution(
        dataset_id=publication_seed.dataset.dataset_id,
        definition_revision_id=publication_seed.definition_revision.definition_revision_id,
        schema_revision_id=publication_seed.schema_revision.schema_revision_id,
        capture_bundle_id=publication_seed.capture_bundle.capture_bundle_id,
        idempotency_key=f"synthetic-materialization-{suffix}",
        mechanism="local",
        state="running",
    )
    session.add(execution)
    await session.flush()
    session.add(
        CustomImportLease(
            execution_id=execution.execution_id,
            fence=1,
            token_sha256=lease_digest(lease_token),
            heartbeat_at=now,
            expires_at=now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()
    return execution, lease_token


async def _new_candidate_generation(
    session,
    publication_seed,
    execution: CustomImportExecution,
    lease_token: str,
    suffix: str,
) -> CustomImportGeneration:
    generation = CustomImportGeneration(
        dataset_id=publication_seed.dataset.dataset_id,
        definition_revision_id=publication_seed.definition_revision.definition_revision_id,
        schema_revision_id=publication_seed.schema_revision.schema_revision_id,
        execution_id=execution.execution_id,
        capture_bundle_id=publication_seed.capture_bundle.capture_bundle_id,
        source_bundle_sha256=publication_seed.source_bundle_sha256,
        candidate_sha256=digest(f"candidate:{suffix}"),
        root_count=1,
        family_count=1,
        producing_fence=1,
        producing_token_sha256=lease_digest(lease_token),
    )
    session.add(generation)
    await session.flush()
    return generation


def _publication_graph(publication_seed) -> PublicationGraph:
    return PublicationGraph(
        dataset_id=publication_seed.dataset.dataset_id,
        definition_revision_id=publication_seed.definition_revision.definition_revision_id,
        schema_revision_id=publication_seed.schema_revision.schema_revision_id,
        capture_bundle_id=publication_seed.capture_bundle.capture_bundle_id,
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


async def _live_generation(session, publication_seed, suffix: str) -> tuple[PublicationGraph, GenerationAttempt]:
    """Create one fenced candidate over the exact synthetic capture bundle."""

    execution, lease_token = await _new_live_execution(session, publication_seed, suffix)
    generation = await _new_candidate_generation(session, publication_seed, execution, lease_token, suffix)
    return _publication_graph(publication_seed), GenerationAttempt(
        execution_id=execution.execution_id,
        generation_id=generation.generation_id,
        token=lease_token,
        fence=1,
    )


async def _persist_definition_profiles(session, definition: CustomImportDefinition, graph: PublicationGraph) -> None:
    profile_count = await persist_selection_profiles(
        session,
        definition,
        identity=DefinitionIdentity(
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
        ),
        child_collection_slots={"rates": 1},
    )
    assert profile_count == 1
    profile = await session.scalar(
        select(CustomImportSelectionProfile).where(
            CustomImportSelectionProfile.definition_revision_id == graph.definition_revision_id
        )
    )
    assert profile is not None
    assert (profile.profile_id, profile.context_collection_slot) == ("default", 1)


async def _p4a_family(session, graph: PublicationGraph, attempt: GenerationAttempt, suffix: str) -> FamilyMaterial:
    family = await seed_family_material(
        session,
        graph,
        attempt,
        FamilyMaterialSpec(suffix=suffix, child_keys=("first", "second"), include_child_scalars=False),
    )
    await attach_generation_family(session, graph, attempt, family)
    return family


def _index_safe_text(label: str) -> str:
    fragments = [hashlib.sha256(f"{label}:{index}".encode("utf-8")).hexdigest() for index in range(64)]
    return "".join(fragments)[:2_048]


def _ordered_index_safe_text(first_character: str, label: str) -> str:
    return first_character + _index_safe_text(label)[1:]


def _root_projection_rows(
    definition: CustomImportDefinition,
    graph: PublicationGraph,
    family: FamilyMaterial,
    root_index_text: str,
):
    return project_root_scalars(
        definition,
        root_target=RootScalarTarget(
            dataset_id=graph.dataset_id,
            schema_revision_id=graph.schema_revision_id,
            root_record_id=family.root_record_id,
            root_revision_id=family.root_revision_id,
        ),
        root_values={"npi": root_index_text},
    )


def _child_projection_rows(
    definition: CustomImportDefinition,
    graph: PublicationGraph,
    family: FamilyMaterial,
    child_beta_values: tuple[str, str],
):
    return tuple(
        scalar_projection
        for child_revision_id, beta_value in zip(family.child_revision_ids, child_beta_values, strict=True)
        for scalar_projection in project_child_scalars(
            definition,
            collection="rates",
            child_collection_slots={"rates": 1},
            child_target=ChildScalarTarget(
                dataset_id=graph.dataset_id,
                schema_revision_id=graph.schema_revision_id,
                root_record_id=family.root_record_id,
                collection_slot=1,
                child_revision_id=child_revision_id,
            ),
            child_values={"synthetic_alpha": "shared", "synthetic_beta": beta_value},
        )
    )


async def _persist_index_safe_projections(
    session,
    definition: CustomImportDefinition,
    graph: PublicationGraph,
    family: FamilyMaterial,
    suffix: str,
):
    root_index_text = _index_safe_text(f"root:{suffix}")
    child_beta_values = (
        _ordered_index_safe_text("a", f"child-a:{suffix}"),
        _ordered_index_safe_text("b", f"child-b:{suffix}"),
    )
    root_scalar_rows = _root_projection_rows(definition, graph, family, root_index_text)
    child_scalar_rows = _child_projection_rows(definition, graph, family, child_beta_values)
    persisted_count = await persist_scalar_projections(
        session,
        definition,
        root_scalars=root_scalar_rows,
        child_scalars=child_scalar_rows,
        child_collection_slots={"rates": 1},
    )
    assert persisted_count == 5
    return root_scalar_rows, root_index_text, child_beta_values


async def _assert_index_safe_scalar_persistence(
    session,
    family: FamilyMaterial,
    root_index_text: str,
    child_beta_values: tuple[str, str],
) -> None:
    stored_root_text = await session.scalar(
        select(CustomImportRootScalar.string_value).where(
            CustomImportRootScalar.root_revision_id == family.root_revision_id
        )
    )
    stored_child_texts = (
        (
            await session.execute(
                select(CustomImportChildScalar.string_value).where(
                    CustomImportChildScalar.child_revision_id.in_(family.child_revision_ids),
                    CustomImportChildScalar.field_slot == 2,
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(root_index_text.encode("utf-8")) == 2_048
    assert all(len(beta_value.encode("utf-8")) == 2_048 for beta_value in child_beta_values)
    assert stored_root_text == root_index_text
    assert set(stored_child_texts) == set(child_beta_values)


async def _assert_ownership_and_binding_guards(
    session,
    definition: CustomImportDefinition,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    family: FamilyMaterial,
    root_scalar_rows,
    suffix: str,
) -> None:
    wrong_root_scalar = scalar_projection_models(definition, root_scalars=root_scalar_rows)[0]
    wrong_root_scalar.root_record_id = family.root_record_id + 1_000_000
    with pytest.raises(DBAPIError, match="custom_import_output_missing_producing_authority"):
        async with session.begin_nested():
            session.add(wrong_root_scalar)
            await session.flush()

    foreign_binding = CustomImportEntityBinding(
        dataset_id=graph.dataset_id,
        adapter_id="synthetic",
        canonical_value=f"foreign-binding-{suffix}",
        value_sha256=digest(f"foreign-binding:{suffix}"),
    )
    session.add(foreign_binding)
    await session.flush()
    with pytest.raises(IntegrityError, match="custom_import_winner_family_entity_fkey"):
        async with session.begin_nested():
            session.add(
                CustomImportWinner(
                    generation_id=attempt.generation_id,
                    dataset_id=graph.dataset_id,
                    definition_revision_id=graph.definition_revision_id,
                    schema_revision_id=graph.schema_revision_id,
                    profile_slot=1,
                    entity_binding_id=foreign_binding.entity_binding_id,
                    family_revision_id=family.family_revision_id,
                    context_collection_slot=1,
                    context_key_sha256=digest(f"wrong-binding-context:{suffix}"),
                    context_child_revision_id=family.child_revision_ids[0],
                )
            )
            await session.flush()


async def _ordered_child_revisions(session, family: FamilyMaterial) -> tuple[CustomImportChildRevision, ...]:
    return tuple(
        (
            await session.execute(
                select(CustomImportChildRevision)
                .where(CustomImportChildRevision.child_revision_id.in_(family.child_revision_ids))
                .order_by(CustomImportChildRevision.source_ordinal)
            )
        )
        .scalars()
        .all()
    )


async def _persist_selected_winner(
    session,
    definition: CustomImportDefinition,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    family: FamilyMaterial,
    child_beta_values: tuple[str, str],
    suffix: str,
) -> int:
    child_revisions = await _ordered_child_revisions(session, family)
    generation = GenerationIdentity(
        generation_id=attempt.generation_id,
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
    )
    materialization = materialize_winners(
        definition,
        generation=generation,
        child_collection_slots={"rates": 1},
        candidates=ValidatedWinnerCandidateStream(
            generation=generation,
            candidate_iterable=(
                WinnerCandidate(
                    entity_binding_id=family.entity_binding_id,
                    family_revision_id=family.family_revision_id,
                    family_sha256=digest(f"synthetic-family:{suffix}"),
                    context_collection_slot=1,
                    context_child_revision_id=child_revision.child_revision_id,
                    context_child_key_sha256=bytes(child_revision.child_key_sha256),
                    values_by_field={"synthetic_alpha": "shared", "synthetic_beta": beta_value},
                )
                for child_revision, beta_value in zip(child_revisions, child_beta_values, strict=True)
            ),
        ),
    )
    assert materialization.winner_count == 1
    assert await persist_winner_materialization(session, materialization) == 1
    return child_revisions[0].child_revision_id


@pytest.mark.asyncio
async def test_p4a_scalar_winners_seal_with_index_safe_text():
    """Persist 2,048-byte root/child strings, profiles, winners, and P3 finality."""

    suffix = uuid.uuid4().hex
    definition = _definition()
    async with transaction_session() as session, session.begin():
        publication_seed = await _seed_publication_identity(session, suffix, include_selection_profile=False)
        await _add_definition_fields(session, publication_seed)
        graph, attempt = await _live_generation(session, publication_seed, suffix)
        await _persist_definition_profiles(session, definition, graph)
        family = await _p4a_family(session, graph, attempt, suffix)
        root_scalar_rows, root_index_text, child_beta_values = await _persist_index_safe_projections(
            session,
            definition,
            graph,
            family,
            suffix,
        )
        await _assert_index_safe_scalar_persistence(session, family, root_index_text, child_beta_values)
        await _assert_ownership_and_binding_guards(
            session,
            definition,
            graph,
            attempt,
            family,
            root_scalar_rows,
            suffix,
        )
        selected_child_revision_id = await _persist_selected_winner(
            session,
            definition,
            graph,
            attempt,
            family,
            child_beta_values,
            suffix,
        )
        receipt = await seal_generation(
            session,
            dataset_id=graph.dataset_id,
            generation_id=attempt.generation_id,
            lease_fence=attempt.fence,
            lease_token=attempt.token,
        )
        winner = await session.scalar(
            select(CustomImportWinner).where(CustomImportWinner.generation_id == attempt.generation_id)
        )
        assert winner is not None
        assert winner.context_child_revision_id == selected_child_revision_id
        assert (receipt.root_scalar_count, receipt.child_scalar_count, receipt.winner_count) == (1, 4, 1)
