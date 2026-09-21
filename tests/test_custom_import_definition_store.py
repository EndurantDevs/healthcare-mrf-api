# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from db.models.custom_import import (
    CustomImportChildCollection,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportField,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
)
from process.custom_import.definition import CustomImportDefinition, load_json_definition
import process.custom_import.definition_store as definition_store
from process.custom_import.definition_store import DefinitionRegistrationError, register_definition

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"
_IDENTITY_FIELDS = {
    "custom_import_schema_revision": "schema_revision_id",
    "custom_import_definition_revision": "definition_revision_id",
}


class _SyntheticResult:
    def __init__(self, rows=()):
        self._rows = tuple(rows)

    def scalar_one_or_none(self):
        if len(self._rows) > 1:
            raise AssertionError("synthetic result is not scalar")
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _SyntheticSession:
    def __init__(self):
        self._models_by_table: dict[str, list[object]] = {}
        self._pending_models: list[object] = []
        self._next_identity = 1
        self.new: set[object] = set()
        self.dirty: set[object] = set()
        self.deleted: set[object] = set()
        self.no_autoflush = nullcontext()

    def in_transaction(self):
        return True

    async def execute(self, statement):
        table = statement.table if getattr(statement, "is_insert", False) else statement.get_final_froms()[0]
        table_name = table.name
        if getattr(statement, "is_insert", False):
            dataset_models = self._models_by_table.setdefault(table_name, [])
            if not dataset_models:
                dataset_models.append(
                    CustomImportDataset(dataset_id=self._next_identity, dataset_key="synthetic_store")
                )
                self._next_identity += 1
            return _SyntheticResult()
        return _SyntheticResult(self._models_by_table.get(table_name, ()))

    async def get(self, model, identity):
        identity_field = _IDENTITY_FIELDS[model.__tablename__]
        return next(
            (
                row
                for row in self._models_by_table.get(model.__tablename__, ())
                if getattr(row, identity_field) == identity
            ),
            None,
        )

    async def scalars(self, statement):
        return (await self.execute(statement)).scalars()

    def add(self, model):
        self._pending_models.append(model)

    def add_all(self, models):
        self._pending_models.extend(models)

    async def flush(self):
        for model in self._pending_models:
            identity_field = _IDENTITY_FIELDS.get(model.__tablename__)
            if identity_field is not None:
                setattr(model, identity_field, self._next_identity)
                self._next_identity += 1
            self._models_by_table.setdefault(model.__tablename__, []).append(model)
        self._pending_models.clear()

    def table_count(self, table_name: str) -> int:
        return len(self._models_by_table.get(table_name, ()))


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json(_FIXTURE.read_text())


def _raw_definition() -> dict[str, object]:
    return copy.deepcopy(load_json_definition(_FIXTURE.read_text()))


def _revised_definition(previous: CustomImportDefinition) -> CustomImportDefinition:
    revised_document = _raw_definition()
    revised_document["revision"]["definition"] = 2
    revised_document["aliases"]["providers"]["Provider Identifier"] = "npi"
    return CustomImportDefinition.from_mapping(revised_document, previous=previous)


def _table_counts(session: _SyntheticSession) -> tuple[int, ...]:
    return tuple(
        session.table_count(table_name)
        for table_name in (
            "custom_import_dataset",
            "custom_import_schema_revision",
            "custom_import_field_slot",
            "custom_import_child_collection",
            "custom_import_field",
            "custom_import_definition_revision",
            "custom_import_source_stream",
            "custom_import_field_alias",
            "custom_import_selection_profile",
        )
    )


@pytest.mark.asyncio
async def test_registration_replays_exact_content_and_rejects_drift_before_persistence():
    session = _SyntheticSession()
    first_definition = _definition()

    first = await register_definition(session, "synthetic_store", first_definition)
    replay = await register_definition(session, "synthetic_store", first_definition)

    assert first.created is True
    assert replay == first.__class__(
        dataset_id=first.dataset_id,
        definition_revision_id=first.definition_revision_id,
        schema_revision_id=first.schema_revision_id,
        created=False,
    )
    assert _table_counts(session) == (1, 1, 5, 1, 5, 1, 2, 5, 1)

    expected_counts = _table_counts(session)
    content_drift = _raw_definition()
    content_drift["refresh_mode"] = "snapshot"
    with pytest.raises(DefinitionRegistrationError, match="different content"):
        await register_definition(session, "synthetic_store", CustomImportDefinition.from_mapping(content_drift))

    revision_drift = _raw_definition()
    revision_drift["revision"] = {"definition": 2, "schema": 2}
    with pytest.raises(DefinitionRegistrationError, match="content is already bound"):
        await register_definition(session, "synthetic_store", CustomImportDefinition.from_mapping(revision_drift))

    key_drift = _raw_definition()
    key_drift["revision"] = {"definition": 2, "schema": 2}
    key_drift["schema"]["root"]["fields"][1]["id"] = "provider_name"
    key_drift["aliases"]["providers"]["Provider Name"] = "provider_name"
    key_drift["query"]["root_fields"][1] = "provider_name"
    with pytest.raises(DefinitionRegistrationError, match="transition is invalid"):
        await register_definition(session, "synthetic_store", CustomImportDefinition.from_mapping(key_drift))

    assert _table_counts(session) == expected_counts


@pytest.mark.asyncio
async def test_registration_reuses_an_unchanged_schema_for_a_new_definition_revision():
    session = _SyntheticSession()
    first_definition = _definition()
    first = await register_definition(session, "synthetic_store", first_definition)

    second = await register_definition(session, "synthetic_store", _revised_definition(first_definition))

    assert second.created is True
    assert second.dataset_id == first.dataset_id
    assert second.schema_revision_id == first.schema_revision_id
    assert second.definition_revision_id != first.definition_revision_id
    assert _table_counts(session) == (1, 1, 5, 1, 5, 2, 4, 11, 2)


@pytest.mark.asyncio
async def test_registration_rejects_drift_in_a_reused_schema_graph():
    session = _SyntheticSession()
    first_definition = _definition()
    first = await register_definition(session, "synthetic_store", first_definition)
    original = session._models_by_table[CustomImportChildCollection.__tablename__][0]
    session._models_by_table[CustomImportChildCollection.__tablename__].append(
        CustomImportChildCollection(
            schema_revision_id=first.schema_revision_id,
            dataset_id=first.dataset_id,
            collection_slot=2,
            collection_name="unexpected_collection",
            canonical_key_shape=original.canonical_key_shape,
            key_shape_sha256=original.key_shape_sha256,
        )
    )

    with pytest.raises(DefinitionRegistrationError, match="persisted schema graph"):
        await register_definition(session, "synthetic_store", _revised_definition(first_definition))

    assert _table_counts(session) == (1, 1, 5, 2, 5, 1, 2, 5, 1)


@pytest.mark.asyncio
async def test_registration_reuses_the_persisted_collection_slot_mapping():
    session = _SyntheticSession()
    first_definition = _definition()
    first = await register_definition(session, "synthetic_store", first_definition)
    collection = session._models_by_table[CustomImportChildCollection.__tablename__][0]
    collection.collection_slot = 2
    for field in session._models_by_table[CustomImportField.__tablename__]:
        if field.collection_slot == 1:
            field.collection_slot = 2
    for stream in session._models_by_table[CustomImportSourceStream.__tablename__]:
        if stream.collection_slot == 1:
            stream.collection_slot = 2
    for profile in session._models_by_table[CustomImportSelectionProfile.__tablename__]:
        if profile.context_collection_slot == 1:
            profile.context_collection_slot = 2

    second = await register_definition(session, "synthetic_store", _revised_definition(first_definition))
    second_streams = [
        stream
        for stream in session._models_by_table[CustomImportSourceStream.__tablename__]
        if stream.definition_revision_id == second.definition_revision_id
    ]
    second_profiles = [
        profile
        for profile in session._models_by_table[CustomImportSelectionProfile.__tablename__]
        if profile.definition_revision_id == second.definition_revision_id
    ]

    assert {stream.collection_slot for stream in second_streams if stream.record_kind == "child"} == {2}
    assert {profile.context_collection_slot for profile in second_profiles} == {2}
    assert second.schema_revision_id == first.schema_revision_id


@pytest.mark.asyncio
async def test_registration_replay_rejects_descendant_drift():
    session = _SyntheticSession()
    definition = _definition()
    first = await register_definition(session, "synthetic_store", definition)
    session._models_by_table[CustomImportSourceStream.__tablename__].append(
        CustomImportSourceStream(
            definition_revision_id=first.definition_revision_id,
            dataset_id=first.dataset_id,
            schema_revision_id=first.schema_revision_id,
            stream_slot=3,
            stream_id="unexpected_stream",
            record_kind="root",
            collection_slot=None,
            decoder="csv",
            compression="none",
            snapshot_token_selector="snapshot_token",
            record_path=None,
        )
    )

    with pytest.raises(DefinitionRegistrationError, match="persisted definition graph"):
        await register_definition(session, "synthetic_store", definition)


def test_definition_store_rejects_untrusted_or_ambiguous_identity_boundaries():
    definition = _definition()

    with pytest.raises(DefinitionRegistrationError, match="dataset_key"):
        definition_store._normalized_dataset_key("Synthetic Store")
    with pytest.raises(TypeError, match="CustomImportDefinition"):
        definition_store._canonical_definition(object())
    with pytest.raises(DefinitionRegistrationError, match="canonical content is invalid"):
        definition_store._canonical_definition(replace(definition, canonical="not-json"))
    with pytest.raises(DefinitionRegistrationError, match="does not match its fields"):
        definition_store._canonical_definition(replace(definition, refresh_mode="snapshot"))
    with pytest.raises(DefinitionRegistrationError, match="active caller transaction"):
        definition_store._require_transaction(object())
    with pytest.raises(DefinitionRegistrationError, match="clean session"):
        definition_store._require_clean_session(SimpleNamespace(new=(object(),), dirty=(), deleted=()))

    duplicate_revision_rows = (SimpleNamespace(revision_number=1),) * 2
    with pytest.raises(DefinitionRegistrationError, match="revision identity is ambiguous"):
        definition_store._row_by_revision(duplicate_revision_rows, 1, "definition")
    assert not definition_store._is_matching_digest(object(), b"digest")
    duplicate_digest_rows = (
        SimpleNamespace(definition_sha256=b"digest"),
        SimpleNamespace(definition_sha256=b"digest"),
    )
    with pytest.raises(DefinitionRegistrationError, match="content identity is ambiguous"):
        definition_store._row_by_digest(duplicate_digest_rows, "definition_sha256", b"digest", "definition")

    invalid_row = SimpleNamespace(canonical_definition="not-json")
    with pytest.raises(DefinitionRegistrationError, match="canonical content is invalid"):
        definition_store._persisted_definition(invalid_row, SimpleNamespace())
    mismatched_row = SimpleNamespace(
        canonical_definition=definition.canonical,
        schema_revision_id=1,
        revision_number=definition.definition_revision,
        contract_version="wrong",
        refresh_mode=definition.refresh_mode,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    mismatched_schema = SimpleNamespace(
        schema_revision_id=1,
        revision_number=definition.schema_revision,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    with pytest.raises(DefinitionRegistrationError, match="identity is invalid"):
        definition_store._persisted_definition(mismatched_row, mismatched_schema)
    with pytest.raises(DefinitionRegistrationError, match="schema is unavailable"):
        definition_store._validate_transition(
            definition, (SimpleNamespace(revision_number=1, schema_revision_id=1),), ()
        )
    with pytest.raises(DefinitionRegistrationError, match="stable field slot"):
        definition_store._new_field_slots(
            (definition.fields[0],),
            (SimpleNamespace(field_slot=definition.fields[0].field_slot, field_id="other"),),
        )


class _AbsentDatasetSession:
    no_autoflush = nullcontext()

    async def execute(self, _statement):
        return _SyntheticResult()


@pytest.mark.asyncio
async def test_definition_store_rejects_an_unavailable_locked_dataset():
    with pytest.raises(DefinitionRegistrationError, match="registered dataset is unavailable"):
        await definition_store._locked_dataset(_AbsentDatasetSession(), "synthetic_store")
