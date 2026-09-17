# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic unit coverage for v1 projection and winner materialization."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

import process.custom_import.materialization as materialization_module
from db.models.custom_import import (
    CustomImportChildScalar,
    CustomImportRootScalar,
    CustomImportSelectionProfile,
    CustomImportWinner,
)
from process.custom_import.definition import (
    CustomImportDefinition,
    load_json_definition,
)
from process.custom_import.materialization import (
    ChildScalarTarget,
    DefinitionIdentity,
    GenerationIdentity,
    RootScalarTarget,
    ScalarProjectionError,
    TypedScalar,
    ValidatedWinnerCandidateStream,
    WinnerCandidate,
    WinnerMaterializationError,
    materialize_winners,
    persist_selection_profiles,
    project_child_scalars,
    project_root_scalars,
    scalar_projection_models,
    selection_profile_models,
    winner_materialization_models,
)

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"
_MISSING = object()


@pytest.fixture
def definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json((FIXTURES / "v1_valid.json").read_text())


def _raw_definition() -> dict[str, object]:
    return dict(load_json_definition((FIXTURES / "v1_valid.json").read_text()))


def _digest(label: str) -> bytes:
    return hashlib.sha256(label.encode("utf-8")).digest()


def _generation() -> GenerationIdentity:
    return GenerationIdentity(
        generation_id=101,
        dataset_id=11,
        definition_revision_id=21,
        schema_revision_id=31,
    )


def _validated_candidates(
    candidate_iterable: Iterable[WinnerCandidate],
    generation: GenerationIdentity | None = None,
) -> ValidatedWinnerCandidateStream:
    return ValidatedWinnerCandidateStream(
        generation=_generation() if generation is None else generation,
        candidate_iterable=candidate_iterable,
    )


def _child_candidate(
    *,
    family_revision_id: int,
    child_revision_id: int,
    semantic_suffix: str,
    service_code: str,
    amount: Decimal | None | object = Decimal("1.00"),
) -> WinnerCandidate:
    values_by_field: dict[str, object] = {"service_code": service_code}
    if amount is not _MISSING:
        values_by_field["amount"] = amount
    return WinnerCandidate(
        entity_binding_id=41,
        family_revision_id=family_revision_id,
        family_sha256=_digest(f"family:{semantic_suffix}"),
        context_collection_slot=7,
        context_child_revision_id=child_revision_id,
        context_child_key_sha256=_digest(f"child:{semantic_suffix}"),
        values_by_field=values_by_field,
    )


def _root_scalar_target() -> RootScalarTarget:
    return RootScalarTarget(dataset_id=11, schema_revision_id=31, root_record_id=51, root_revision_id=61)


def _child_scalar_target(child_revision_id: int) -> ChildScalarTarget:
    return ChildScalarTarget(
        dataset_id=11,
        schema_revision_id=31,
        root_record_id=51,
        collection_slot=7,
        child_revision_id=child_revision_id,
    )


def _root_projection(definition: CustomImportDefinition, display_name: str = "Synthetic Provider"):
    return project_root_scalars(
        definition,
        root_target=_root_scalar_target(),
        root_values={"npi": "1234567893", "display_name": display_name},
    )


def _child_projection(
    definition: CustomImportDefinition,
    child_revision_id: int,
    child_values: dict[str, object],
):
    return project_child_scalars(
        definition,
        collection="rates",
        child_collection_slots={"rates": 7},
        child_target=_child_scalar_target(child_revision_id),
        child_values=child_values,
    )


def test_projection_preserves_scalar_states_and_native_columns(definition):
    root_scalars = _root_projection(definition)
    child_scalars = _child_projection(definition, 71, {"service_code": "A100", "amount": None})
    absent_nullable = _child_projection(definition, 72, {"service_code": "A200"})

    assert [(projection.field_id, projection.scalar.value_state) for projection in root_scalars] == [
        ("npi", "value"),
        ("display_name", "value"),
    ]
    assert [(projection.field_id, projection.scalar.value_state) for projection in child_scalars] == [
        ("service_code", "value"),
        ("amount", "null"),
    ]
    assert [projection.field_id for projection in absent_nullable] == ["service_code"]

    models = scalar_projection_models(
        definition,
        root_scalars=root_scalars,
        child_scalars=child_scalars,
        child_collection_slots={"rates": 7},
    )
    assert isinstance(models[0], CustomImportRootScalar)
    assert models[0].string_value == "1234567893"
    assert isinstance(models[-1], CustomImportChildScalar)
    assert models[-1].value_state == "null"
    assert models[-1].decimal_value is None


def test_projection_rejects_required_values_and_wrong_child_scope(definition):
    with pytest.raises(ScalarProjectionError, match="npi is required but missing"):
        project_root_scalars(
            definition,
            root_target=_root_scalar_target(),
            root_values={"display_name": "Synthetic Provider"},
        )

    forged_required_null = replace(
        _root_projection(definition)[0], scalar=TypedScalar(field_type="string", value_state="null")
    )
    with pytest.raises(ScalarProjectionError, match="cannot store null for a required field"):
        scalar_projection_models(definition, root_scalars=(forged_required_null,))

    with pytest.raises(ScalarProjectionError, match="does not match the declared collection"):
        project_child_scalars(
            definition,
            collection="rates",
            child_collection_slots={"rates": 8},
            child_target=_child_scalar_target(72),
            child_values={"service_code": "A200"},
        )


def _typed_definition() -> CustomImportDefinition:
    definition_document = _raw_definition()
    root_fields = definition_document["schema"]["root"]["fields"]
    assert isinstance(root_fields, list)
    root_fields.extend(
        [
            {"id": "rank", "slot": 6, "type": "integer", "nullable": False, "projection_slot": 5},
            {"id": "active", "slot": 7, "type": "boolean", "nullable": False, "projection_slot": 6},
            {"id": "born_on", "slot": 8, "type": "date", "nullable": False, "projection_slot": 7},
            {"id": "seen_at", "slot": 9, "type": "timestamp", "nullable": False, "projection_slot": 8},
        ]
    )
    return CustomImportDefinition.from_mapping(definition_document)


def _typed_root_values_by_field() -> dict[str, object]:
    return {
        "npi": "1234567893",
        "display_name": "Synthetic Provider",
        "rank": 9_223_372_036_854_775_807,
        "active": True,
        "born_on": date(2026, 9, 17),
        "seen_at": datetime(2026, 9, 17, 2, tzinfo=UTC),
    }


def test_projection_normalizes_timestamp():
    typed_definition = _typed_definition()
    projected = project_root_scalars(
        typed_definition,
        root_target=_root_scalar_target(),
        root_values=_typed_root_values_by_field(),
    )
    assert projected[-1].scalar.timestamp_value == datetime(2026, 9, 17, 2, tzinfo=UTC)


def test_projection_rejects_lossy_values(definition):
    typed_definition = _typed_definition()
    for field_id, invalid_value in (
        ("rank", True),
        ("rank", 9_223_372_036_854_775_808),
        ("active", 1),
        ("born_on", datetime(2026, 9, 17, tzinfo=UTC)),
        ("seen_at", datetime(2026, 9, 17)),
    ):
        root_values_by_field = _typed_root_values_by_field()
        root_values_by_field[field_id] = invalid_value
        with pytest.raises(ScalarProjectionError):
            project_root_scalars(
                typed_definition,
                root_target=_root_scalar_target(),
                root_values=root_values_by_field,
            )

    for invalid_amount in (
        Decimal("1000000000000000000"),
        Decimal("1.1234567890123"),
        Decimal("NaN"),
        1.25,
    ):
        with pytest.raises(ScalarProjectionError, match="decimal field amount"):
            _child_projection(definition, 71, {"service_code": "A100", "amount": invalid_amount})


def test_hot_string_bound_is_utf8_byte_safe(definition):
    index_safe_text = "😺" * 512
    too_large_text = "😺" * 513
    assert len(index_safe_text.encode("utf-8")) == 2_048
    _root_projection(definition, index_safe_text)
    _child_projection(definition, 71, {"service_code": index_safe_text})

    with pytest.raises(ScalarProjectionError, match="exceeds its storage shape"):
        _root_projection(definition, too_large_text)
    with pytest.raises(ScalarProjectionError, match="exceeds its storage shape"):
        _child_projection(definition, 71, {"service_code": too_large_text})


def _child_profile_candidates() -> tuple[WinnerCandidate, WinnerCandidate, WinnerCandidate]:
    return (
        _child_candidate(
            family_revision_id=301,
            child_revision_id=401,
            semantic_suffix="nine",
            service_code="A",
            amount=Decimal("9"),
        ),
        _child_candidate(
            family_revision_id=302,
            child_revision_id=402,
            semantic_suffix="four",
            service_code="A",
            amount=Decimal("4"),
        ),
        _child_candidate(
            family_revision_id=303,
            child_revision_id=403,
            semantic_suffix="five",
            service_code="B",
            amount=Decimal("5"),
        ),
    )


def _child_profile_winners(definition: CustomImportDefinition):
    return materialize_winners(
        definition,
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=_validated_candidates(_child_profile_candidates()),
    )


def test_child_profiles_bind_before_later_filters(definition):
    profile_rows = selection_profile_models(
        definition,
        identity=DefinitionIdentity(dataset_id=11, definition_revision_id=21, schema_revision_id=31),
        child_collection_slots={"rates": 7},
    )
    assert len(profile_rows) == 1
    assert isinstance(profile_rows[0], CustomImportSelectionProfile)
    assert profile_rows[0].context_collection_slot == 7
    assert json.loads(profile_rows[0].canonical_profile)["scope"] == {
        "collection": "rates",
        "kind": "child",
    }

    materialization = _child_profile_winners(definition)

    assert {(winner.family_revision_id, winner.context_child_revision_id) for winner in materialization.winners} == {
        (302, 402),
        (303, 403),
    }
    assert materialization.winner_count == 2
    models = winner_materialization_models(materialization)
    assert all(isinstance(model, CustomImportWinner) for model in models)
    assert {(model.family_revision_id, model.context_child_revision_id) for model in models} == {
        (302, 402),
        (303, 403),
    }
    forged_scope = replace(
        materialization.winners[0],
        context_collection_slot=0,
        context_child_revision_id=None,
    )
    with pytest.raises(WinnerMaterializationError, match="context collection does not match"):
        winner_materialization_models(replace(materialization, winners=(forged_scope,)))


def test_root_only_profiles_need_no_child_scope_and_materialize_root_winners():
    raw = _raw_definition()
    profile = raw["selection_profiles"][0]
    assert isinstance(profile, dict)
    profile["selection"] = [{"field": "display_name", "direction": "asc", "nulls": "last"}]
    profile["context_dimensions"] = []
    root_definition = CustomImportDefinition.from_mapping(raw)

    profiles = selection_profile_models(
        root_definition,
        identity=DefinitionIdentity(dataset_id=11, definition_revision_id=21, schema_revision_id=31),
    )
    assert profiles[0].context_collection_slot is None
    winners = materialize_winners(
        root_definition,
        generation=_generation(),
        candidates=_validated_candidates(
            (
                WinnerCandidate(
                    entity_binding_id=41,
                    family_revision_id=301,
                    family_sha256=_digest("root-family:z"),
                    context_collection_slot=0,
                    context_child_revision_id=None,
                    context_child_key_sha256=None,
                    values_by_field={"display_name": "Zeta"},
                ),
                WinnerCandidate(
                    entity_binding_id=41,
                    family_revision_id=302,
                    family_sha256=_digest("root-family:a"),
                    context_collection_slot=0,
                    context_child_revision_id=None,
                    context_child_key_sha256=None,
                    values_by_field={"display_name": "Alpha"},
                ),
            )
        ),
    )
    assert [(winner.family_revision_id, winner.context_child_revision_id) for winner in winners.winners] == [
        (302, None)
    ]


@pytest.mark.asyncio
async def test_selection_profile_persistence_flushes_before_candidate_work(definition):
    class Recorder:
        def __init__(self) -> None:
            self.models: tuple[object, ...] = ()
            self.flushes = 0

        def in_transaction(self) -> bool:
            return True

        def add_all(self, models) -> None:
            self.models = tuple(models)

        async def flush(self) -> None:
            self.flushes += 1

    recorder = Recorder()
    assert (
        await persist_selection_profiles(
            recorder,
            definition,
            identity=DefinitionIdentity(dataset_id=11, definition_revision_id=21, schema_revision_id=31),
            child_collection_slots={"rates": 7},
        )
        == 1
    )
    assert isinstance(recorder.models[0], CustomImportSelectionProfile)
    assert recorder.flushes == 1


def _context_definition() -> CustomImportDefinition:
    definition_document = copy.deepcopy(_raw_definition())
    profile_documents = definition_document["selection_profiles"]
    assert isinstance(profile_documents, list)
    profile_document = profile_documents[0]
    assert isinstance(profile_document, dict)
    profile_document["selection"] = [{"field": "service_code", "direction": "asc", "nulls": "last"}]
    profile_document["context_dimensions"] = ["amount"]
    return CustomImportDefinition.from_mapping(definition_document)


def _missing_and_null_candidates() -> tuple[WinnerCandidate, WinnerCandidate]:
    return (
        _child_candidate(
            family_revision_id=501,
            child_revision_id=601,
            semantic_suffix="missing",
            service_code="A",
            amount=_MISSING,
        ),
        _child_candidate(
            family_revision_id=502,
            child_revision_id=602,
            semantic_suffix="null",
            service_code="A",
            amount=None,
        ),
    )


def test_winner_context_retains_missing_and_null():
    materialization = materialize_winners(
        _context_definition(),
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=_validated_candidates(_missing_and_null_candidates()),
    )
    assert len(materialization.winners) == 2
    assert {
        json.loads(winner.canonical_context_key)["dimensions"][0]["value_state"] for winner in materialization.winners
    } == {"missing", "null"}


def test_winner_context_rejects_digest_collision(monkeypatch):
    def colliding_context_key(*args, **kwargs):
        candidate = args[1]
        return f'{{"candidate":{candidate.candidate.family_revision_id}}}', b"x" * 32

    monkeypatch.setattr(materialization_module, "_context_key", colliding_context_key)
    with pytest.raises(WinnerMaterializationError, match="digest collision"):
        materialize_winners(
            _context_definition(),
            generation=_generation(),
            child_collection_slots={"rates": 7},
            candidates=_validated_candidates(_missing_and_null_candidates()),
        )


def _amount_selection_definition(nulls: str, *, amount_is_required: bool = False) -> CustomImportDefinition:
    definition_document = _raw_definition()
    child_fields = definition_document["schema"]["children"][0]["fields"]
    assert isinstance(child_fields, list)
    if amount_is_required:
        amount_field = next(field for field in child_fields if field["id"] == "amount")
        amount_field["nullable"] = False
    profile_document = definition_document["selection_profiles"][0]
    assert isinstance(profile_document, dict)
    profile_document["selection"] = [{"field": "amount", "direction": "asc", "nulls": nulls}]
    return CustomImportDefinition.from_mapping(definition_document)


def _mixed_amount_candidates() -> tuple[WinnerCandidate, WinnerCandidate, WinnerCandidate]:
    return (
        _child_candidate(
            family_revision_id=701,
            child_revision_id=801,
            semantic_suffix="present",
            service_code="A",
            amount=Decimal("4"),
        ),
        _child_candidate(
            family_revision_id=702,
            child_revision_id=802,
            semantic_suffix="null",
            service_code="A",
            amount=None,
        ),
        _child_candidate(
            family_revision_id=703,
            child_revision_id=803,
            semantic_suffix="missing",
            service_code="A",
            amount=_MISSING,
        ),
    )


def test_nullable_missing_selection_obeys_nulls_last():
    materialization = materialize_winners(
        _amount_selection_definition("last"),
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=_validated_candidates(_mixed_amount_candidates()),
    )
    assert materialization.winners[0].family_revision_id == 701


def test_nullable_missing_selection_obeys_nulls_first():
    candidates = _mixed_amount_candidates()
    materialization = materialize_winners(
        _amount_selection_definition("first"),
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=_validated_candidates(candidates),
    )
    expected_family = min(
        candidates[1:], key=lambda candidate: (candidate.family_sha256, candidate.context_child_key_sha256)
    )
    assert materialization.winners[0].family_revision_id == expected_family.family_revision_id


def test_required_selection_field_rejects_missing():
    with pytest.raises(WinnerMaterializationError, match="required selection field amount"):
        materialize_winners(
            _amount_selection_definition("last", amount_is_required=True),
            generation=_generation(),
            child_collection_slots={"rates": 7},
            candidates=_validated_candidates(
                (
                    _child_candidate(
                        family_revision_id=701,
                        child_revision_id=801,
                        semantic_suffix="selection-missing",
                        service_code="A",
                        amount=_MISSING,
                    ),
                )
            ),
        )


def test_winner_materialization_accepts_a_single_use_generator(definition):
    candidate_entries = (
        _child_candidate(
            family_revision_id=family_revision_id,
            child_revision_id=child_revision_id,
            semantic_suffix=suffix,
            service_code="A",
            amount=amount,
        )
        for family_revision_id, child_revision_id, suffix, amount in (
            (801, 901, "larger", Decimal("9")),
            (802, 902, "smaller", Decimal("4")),
        )
    )
    winners = materialize_winners(
        definition,
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=_validated_candidates(candidate_entries),
    )
    assert winners.winners[0].family_revision_id == 802


def test_winner_candidate_stream_rejects_generation_mismatch(definition):
    generation = _generation()
    stream = _validated_candidates((), replace(generation, generation_id=102))

    with pytest.raises(WinnerMaterializationError, match="generation does not match"):
        materialize_winners(
            definition,
            generation=generation,
            child_collection_slots={"rates": 7},
            candidates=stream,
        )


def test_winner_candidate_stream_can_only_be_consumed_once(definition):
    stream = _validated_candidates(())
    materialize_winners(
        definition,
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=stream,
    )

    with pytest.raises(WinnerMaterializationError, match="already consumed"):
        materialize_winners(
            definition,
            generation=_generation(),
            child_collection_slots={"rates": 7},
            candidates=stream,
        )


def test_equal_semantic_tie_rejects_conflicting_physical_identity(definition):
    first = _child_candidate(
        family_revision_id=801,
        child_revision_id=901,
        semantic_suffix="same",
        service_code="A",
        amount=Decimal("4"),
    )
    second = replace(first, family_revision_id=802, context_child_revision_id=902)

    with pytest.raises(WinnerMaterializationError, match="conflicting physical identity"):
        materialize_winners(
            definition,
            generation=_generation(),
            child_collection_slots={"rates": 7},
            candidates=_validated_candidates((first, second)),
        )


def test_equal_semantic_tie_rejects_conflicting_typed_values(definition):
    first = _child_candidate(
        family_revision_id=801,
        child_revision_id=901,
        semantic_suffix="same",
        service_code="A",
        amount=Decimal("4"),
    )
    first = replace(first, values_by_field={**first.values_by_field, "display_name": "Alpha"})
    second = replace(first, values_by_field={**first.values_by_field, "display_name": "Beta"})

    with pytest.raises(WinnerMaterializationError, match="conflicting typed values"):
        materialize_winners(
            definition,
            generation=_generation(),
            child_collection_slots={"rates": 7},
            candidates=_validated_candidates((first, second)),
        )


def test_complete_ties_use_semantic_hashes_not_input_or_allocation_order(definition):
    first = _child_candidate(
        family_revision_id=999,
        child_revision_id=999,
        semantic_suffix="zeta",
        service_code="A",
        amount=Decimal("4"),
    )
    second = _child_candidate(
        family_revision_id=1,
        child_revision_id=1,
        semantic_suffix="alpha",
        service_code="A",
        amount=Decimal("4"),
    )
    forwards = materialize_winners(
        definition,
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=_validated_candidates((first, second)),
    )
    backwards = materialize_winners(
        definition,
        generation=_generation(),
        child_collection_slots={"rates": 7},
        candidates=_validated_candidates((second, first)),
    )
    assert [(winner.family_revision_id, winner.context_child_revision_id) for winner in forwards.winners] == [
        (winner.family_revision_id, winner.context_child_revision_id) for winner in backwards.winners
    ]
    assert forwards.winners[0].family_revision_id == (
        first.family_revision_id
        if (first.family_sha256, first.context_child_key_sha256)
        < (second.family_sha256, second.context_child_key_sha256)
        else second.family_revision_id
    )
