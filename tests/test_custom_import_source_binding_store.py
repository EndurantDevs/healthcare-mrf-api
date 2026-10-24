# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact persisted object identities and unchanged legacy binding dispatch."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import snowflake_source_binding as legacy
from process.custom_import import source_binding_store as store
from process.custom_import.object_source_binding import ObjectSourceBinding, ObjectSourceBindingError
from tests.test_custom_import_object_source_binding import _document
from tests.test_custom_import_snowflake_source_binding import _binding, _definition, _loaded_rows, _v2_binding


def _object_binding(definition, connector="https"):
    document = _document(connector)
    locator_by_key = {key: value for key, value in document["streams"][0].items() if key != "stream_id"}
    document.update(
        definition_sha256=definition.digest,
        schema_sha256=definition.schema_digest,
        streams=[{"stream_id": stream.stream_id, **locator_by_key} for stream in definition.source_streams],
    )
    return ObjectSourceBinding.from_mapping(document)


def _object_rows(definition, binding):
    rows = _loaded_rows(definition, _binding(definition))
    rows[0].__dict__.update(
        binding_contract=binding.contract,
        connector_kind=binding.connector,
        canonical_binding=binding.canonical,
        binding_sha256=bytes.fromhex(binding.digest),
        definition_sha256=bytes.fromhex(binding.definition_sha256),
        schema_sha256=bytes.fromhex(binding.schema_sha256),
        source_object_fingerprint_sha256=bytes.fromhex(binding.source_object["fingerprint_sha256"]),
        source_object_version=binding.source_object["version"],
    )
    return rows


def _session_with(*rows):
    return SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)))


@pytest.mark.asyncio
@pytest.mark.parametrize("connector", ("https", "s3", "upload"))
async def test_load_object_variant_verifies_the_complete_owner_join(connector):
    definition = _definition()
    binding = _object_binding(definition, connector)
    session = _session_with(_object_rows(definition, binding))
    loaded = await store.load_source_binding(session, definition_revision_id=12, source_binding_revision_id=14)
    assert loaded == store.LoadedObjectSourceBinding(11, 12, 13, 14, bytes.fromhex(binding.digest), definition, binding)
    session.execute.assert_awaited_once()
    statement = str(session.execute.call_args.args[0])
    for relationship in (
        "custom_import_definition_revision.definition_revision_id = mrf.custom_import_source_binding_revision.definition_revision_id",
        "custom_import_definition_revision.dataset_id = mrf.custom_import_source_binding_revision.dataset_id",
        "custom_import_definition_revision.schema_revision_id = mrf.custom_import_source_binding_revision.schema_revision_id",
        "custom_import_schema_revision.schema_revision_id = mrf.custom_import_source_binding_revision.schema_revision_id",
        "custom_import_schema_revision.dataset_id = mrf.custom_import_source_binding_revision.dataset_id",
    ):
        assert relationship in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", (_binding, _v2_binding))
async def test_common_loader_keeps_legacy_validated_type_bytes_and_errors(factory):
    definition = _definition()
    binding = factory(definition)
    rows = _loaded_rows(definition, binding)
    rows[0].binding_contract = binding.contract
    loaded = await store.load_source_binding(
        _session_with(rows), definition_revision_id=12, source_binding_revision_id=14
    )
    assert loaded == legacy._loaded_snowflake_source_binding(*rows)
    assert type(loaded) is legacy.LoadedSnowflakeSourceBinding
    assert loaded.binding.canonical == binding.canonical
    rows[0].binding_sha256 = b"x" * 32
    with pytest.raises(legacy.SnowflakeSourceBindingUnavailableError, match="persisted identity"):
        await store.load_source_binding(_session_with(rows), definition_revision_id=12, source_binding_revision_id=14)


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", (_binding, _v2_binding))
async def test_common_registration_delegates_legacy_without_changing_arguments_or_errors(monkeypatch, factory):
    definition = _definition()
    binding = factory(definition)
    session = object()
    receipt = legacy.SnowflakeSourceBindingReceipt(11, 12, 13, 14, 1, bytes.fromhex(binding.digest), False)
    delegated = AsyncMock(return_value=receipt)
    monkeypatch.setattr(legacy, "register_snowflake_source_binding", delegated)
    arguments_by_key = dict(dataset_key="synthetic_binding", definition=definition, binding=binding)
    assert await store.register_source_binding(session, **arguments_by_key) is receipt
    delegated.assert_awaited_once_with(session, **arguments_by_key)
    failure = legacy.SnowflakeSourceBindingUnavailableError("synthetic failure")
    delegated.side_effect = failure
    with pytest.raises(legacy.SnowflakeSourceBindingUnavailableError) as caught:
        await store.register_source_binding(session, **arguments_by_key)
    assert caught.value is failure


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row_index,field,value",
    [
        (0, "dataset_id", 0),
        (0, "definition_revision_id", 99),
        (0, "schema_revision_id", 99),
        (0, "source_binding_revision_id", True),
        (0, "revision_number", 0),
        (0, "connector_kind", "upload"),
        (0, "canonical_binding", "{}"),
        (0, "binding_sha256", b"x" * 32),
        (0, "definition_sha256", b"x" * 32),
        (0, "schema_sha256", b"x" * 32),
        (0, "source_object_fingerprint_sha256", b"x" * 32),
        (0, "source_object_version", "changed"),
        (1, "dataset_id", 99),
        (1, "definition_revision_id", 99),
        (1, "schema_revision_id", 99),
        (1, "revision_number", 2),
        (1, "contract_version", "custom-import/v2"),
        (1, "canonical_definition", "{}"),
        (1, "definition_sha256", b"x" * 32),
        (2, "dataset_id", 99),
        (2, "schema_revision_id", 99),
        (2, "revision_number", 2),
        (2, "canonical_schema", "{}"),
        (2, "schema_sha256", b"x" * 32),
    ],
)
async def test_object_loader_rejects_corrupted_retained_evidence(row_index, field, value):
    definition = _definition()
    rows = _object_rows(definition, _object_binding(definition))
    setattr(rows[row_index], field, value)
    with pytest.raises(store.ObjectSourceBindingUnavailableError, match="persisted identity"):
        await store.load_source_binding(_session_with(rows), definition_revision_id=12, source_binding_revision_id=14)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("definition_digest", "schema_digest", "missing_stream", "canonical_whitespace"))
async def test_object_loader_requires_definition_coverage_and_exact_canonical_bytes(change):
    definition = _definition()
    binding = _object_binding(definition)
    if change == "definition_digest":
        binding = replace(binding, definition_sha256="4" * 64)
    elif change == "schema_digest":
        binding = replace(binding, schema_sha256="4" * 64)
    elif change == "missing_stream":
        binding = replace(binding, streams=binding.streams[:1])
    rows = _object_rows(definition, binding)
    if change == "canonical_whitespace":
        rows[0].canonical_binding += "\n"
    with pytest.raises(store.ObjectSourceBindingUnavailableError):
        await store.load_source_binding(_session_with(rows), definition_revision_id=12, source_binding_revision_id=14)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "contract,connector", (("unknown", "https"), ("custom-import/source-binding/v3", "snowflake_bundle"))
)
async def test_common_load_refuses_unknown_or_crossed_contracts(contract, connector):
    definition = _definition()
    rows = _object_rows(definition, _object_binding(definition))
    rows[0].binding_contract, rows[0].connector_kind = contract, connector
    with pytest.raises(store.ObjectSourceBindingUnavailableError):
        await store.load_source_binding(_session_with(rows), definition_revision_id=12, source_binding_revision_id=14)


@pytest.mark.asyncio
async def test_common_lookup_keeps_exact_selectors_and_one_row_requirement():
    for rows in ((), ((object(), object(), object()),) * 2):
        with pytest.raises(legacy.SnowflakeSourceBindingUnavailableError, match="unavailable"):
            await store.load_source_binding(
                _session_with(*rows), definition_revision_id=12, source_binding_revision_id=14
            )
    session = _session_with()
    for invalid in (True, 0, -1, 2**63, "12"):
        with pytest.raises(legacy.SnowflakeSourceBindingUnavailableError, match="identifiers"):
            await store.load_source_binding(session, definition_revision_id=invalid, source_binding_revision_id=14)
    session.execute.assert_not_awaited()
    for loader in (store.load_source_binding, legacy.load_snowflake_source_binding):
        with pytest.raises(ValueError):
            await loader(_session_with((object(), object())), definition_revision_id=12, source_binding_revision_id=14)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("canonical", "digest", "definition", "binding", "coverage"))
async def test_registration_rejects_invalid_or_forged_inputs_before_persistence(monkeypatch, change):
    definition = _definition()
    binding = _object_binding(definition)
    if change in {"canonical", "digest"}:
        object.__setattr__(binding, change, "invalid")
    elif change == "definition":
        definition = None
    elif change == "binding":
        binding = None
    else:
        binding = replace(binding, streams=binding.streams[:1])
    persist = AsyncMock()
    monkeypatch.setattr(store, "register_definition", persist)
    with pytest.raises(ObjectSourceBindingError, match="registration inputs"):
        await store.register_source_binding(
            object(), dataset_key="synthetic_binding", definition=definition, binding=binding
        )
    persist.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", (("dataset_id", 99), ("source_binding_sha256", b"x" * 32), ("binding", None), ("definition", None))
)
async def test_object_receipt_requires_exact_loader_readback(monkeypatch, field, value):
    definition = _definition()
    binding = _object_binding(definition)
    rows = _object_rows(definition, binding)
    loaded = store._loaded_object_source_binding(*rows)
    monkeypatch.setattr(store, "load_source_binding", AsyncMock(return_value=replace(loaded, **{field: value})))
    registration = SimpleNamespace(dataset_id=11, definition_revision_id=12, schema_revision_id=13)
    with pytest.raises(store.ObjectSourceBindingUnavailableError, match="readback"):
        await store._object_readback_receipt(
            object(),
            definition=definition,
            binding=binding,
            definition_registration=registration,
            binding_revision=rows[0],
            created=False,
        )
