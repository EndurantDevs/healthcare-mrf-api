# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic protocol checks; native source admission is qualified separately."""

import asyncio
import hashlib
import json
import sqlite3
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.exc import InvalidRequestError

from process import reference_family_archive as archive
from process import registry_aca_source_recipe as producer
from process.reference_family_result_generation import ReferenceFamilyServingGeneration

_ROLES = ("source_reader",)
_DATASET = UUID("11111111-1111-4111-8111-111111111111")


def _metadata():
    return {
        "network_membership": {
            "source_id": "source-example",
            "release_id": "release-one",
            "import_id": "import-one",
            "issuer_id": 7,
            "year": 2026,
            "source_url": "https://source.example.test/providers",
            "alias_scope": "medical",
            "reviewed_alias_sha256": "a" * 64,
            "runtime_roles": list(_ROLES),
        },
        "network_bindings": {
            "source_system": "aca",
            "source_id": "source-example",
            "dataset_schema": archive.reference_family_stage_schema(_DATASET),
            "dataset_id": str(_DATASET),
            "producer_id": "producer-example",
            "edition_id": "edition-one",
            "source_key_kind": "hios_plan_id",
        },
    }


def _completed():
    generation = ReferenceFamilyServingGeneration(str(_DATASET), 2, datetime(2026, 1, 1, tzinfo=timezone.utc))
    return {
        "attempt_id": _DATASET,
        "import_id": "import-one",
        "generation": {"serving_generation": generation.as_dict()},
        "summary_oid": 88,
        "summary_inputs": {"plan": 89},
        "native_address_coverage": {"contract": "native-proof"},
    }


async def _prepared(monkeypatch, *, metadata=None):
    monkeypatch.setattr(producer, "require_completed_publication", AsyncMock(return_value=_completed()))
    metadata = await producer.capture_registry_aca_source_metadata(
        object(), _metadata() if metadata is None else metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
    )
    spec = archive.reference_family_spec("mrf", canonical=True)
    tables = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 0) for model in spec.model_types
    )
    manifest = archive.ReferenceFamilyManifest(
        "mrf",
        tables,
        metadata,
        archive._source_metadata(metadata)[1],
        archive._schema_digest(tables),
        {"plan-attributes": "c" * 64},
        archive._canonical_model_receipt(),
        "tracked-generation",
        ReferenceFamilyServingGeneration(str(_DATASET), 2, datetime(2026, 1, 1, tzinfo=timezone.utc)),
    )
    relations = tuple((name, index + 100) for index, name in enumerate(sorted(spec.table_names)))
    ownership = archive.ReferenceFamilyStageOwnership(
        "mrf", _DATASET, archive.reference_family_stage_schema(_DATASET), 12, relations
    )
    prepared = archive.ReferenceFamilyPreparedSource(manifest, ownership)
    validation_fields_map = {
        "contract": archive.VALIDATION_CONTRACT,
        "importer_id": "mrf",
        "package_id": "b" * 64,
        "profile_contract": archive.TYPED_MRF_CONTRACT,
        "stage_schema": ownership.schema_name,
        "stage_schema_oid": 12,
        "relation_oids": [list(pair) for pair in relations],
        "sealed_owner_oid": 13,
        "manifest_sha256": hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest(),
        "tables": [table.as_dict() for table in tables],
    }
    validation = archive.validate_reference_family_validation_receipt(
        {**validation_fields_map, "validation_sha256": archive._validation_digest(validation_fields_map)}
    )
    return prepared, validation


@pytest.mark.asyncio
async def test_capture_requires_completed_summary_and_matching_import(monkeypatch):
    receipt = _completed()
    gate = AsyncMock(return_value=receipt)
    monkeypatch.setattr(producer, "require_completed_publication", gate)
    metadata = _metadata()
    result = await producer.capture_registry_aca_source_metadata(
        object(), metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
    )
    assert "aca_completed_publication" not in metadata
    assert result["aca_completed_publication"]["attempt_id"] == str(_DATASET)
    gate.assert_awaited_once_with(gate.await_args.args[0], "mrf", native_set=True)
    receipt["import_id"] = "unrelated-import"
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.capture_registry_aca_source_metadata(
            object(), metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
        )
    gate.side_effect = RuntimeError("publication pending")
    with pytest.raises(RuntimeError, match="publication pending"):
        await producer.capture_registry_aca_source_metadata(
            object(), metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "extra", "cross_source", "source_kind", "roles", "key_kind"])
async def test_untrusted_scope_never_reaches_completion_reader(monkeypatch, damage):
    metadata = _metadata()
    if damage == "missing":
        del metadata["network_membership"]
    elif damage == "extra":
        metadata["runtime_options"] = {}
    else:
        replacements_map = {
            "cross_source": ("network_bindings", "source_id", "different-source"),
            "source_kind": ("network_bindings", "source_system", "fhir"),
            "roles": ("network_membership", "runtime_roles", ["unconfigured_reader"]),
            "key_kind": ("network_bindings", "source_key_kind", "name"),
        }
        partition, key, value = replacements_map[damage]
        metadata[partition][key] = value
    gate = AsyncMock()
    monkeypatch.setattr(producer, "require_completed_publication", gate)
    with pytest.raises(ValueError):
        await producer.capture_registry_aca_source_metadata(
            object(), metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
        )
    gate.assert_not_awaited()


@pytest.mark.asyncio
async def test_admission_orders_native_validation_custody_and_durable_stamp(monkeypatch):
    prepared, validation = await _prepared(monkeypatch)
    call_order_list = []

    async def activation(*_args, **kwargs):
        assert kwargs["sealed_owner_oid"] == 13
        call_order_list.append("validation")
        return validation

    connection = SimpleNamespace(execute=AsyncMock())

    async def custody(driver, pin):
        assert driver is connection and pin.prepared == prepared
        call_order_list.append("custody")

    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=None), execute=AsyncMock())
    monkeypatch.setattr(producer, "protected_publisher_owner", AsyncMock(return_value=13))
    monkeypatch.setattr(producer, "prepare_reference_family_activation", activation)
    monkeypatch.setattr(producer, "native_driver", AsyncMock(return_value=connection))
    monkeypatch.setattr(producer, "_require_archive", custody)
    recipe = await producer.admit_registry_aca_source_recipe(session, prepared, runtime_roles=_ROLES)
    assert call_order_list == ["validation", "custody"]
    statement = connection.execute.await_args.args[0]
    assert statement.startswith('COMMENT ON SCHEMA "reference_family_archive_')
    assert producer.CONTRACT in statement and recipe.source_pin.validation == validation
    session.scalar.return_value = producer._PREFIX + producer.canonical_registry_source_recipes((recipe,))
    assert await producer.require_registry_aca_source_recipe(session, prepared, runtime_roles=_ROLES) == recipe


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["validation", "custody", "comment", "cancel", "stamp_io"])
async def test_admission_failure_never_records_receipt(monkeypatch, failure):
    prepared, validation = await _prepared(monkeypatch)
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(return_value=None if failure == "stamp_io" else "unrelated-comment"),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(producer, "protected_publisher_owner", AsyncMock(return_value=13))
    gate = AsyncMock(return_value=validation)
    custody = AsyncMock()
    if failure == "validation":
        gate.side_effect = RuntimeError("validation rejected")
    elif failure in {"custody", "cancel"}:
        custody.side_effect = asyncio.CancelledError() if failure == "cancel" else RuntimeError("custody rejected")
    monkeypatch.setattr(producer, "prepare_reference_family_activation", gate)
    connection = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("stamp rejected")))
    monkeypatch.setattr(producer, "native_driver", AsyncMock(return_value=connection))
    monkeypatch.setattr(producer, "_require_archive", custody)
    with pytest.raises((ValueError, RuntimeError, asyncio.CancelledError)):
        await producer.admit_registry_aca_source_recipe(session, prepared, runtime_roles=_ROLES)
    session.execute.assert_not_awaited()
    assert connection.execute.await_count == int(failure == "stamp_io")


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["generation", "dataset", "completion", "manual"])
async def test_recipe_refuses_different_generation_or_archive(monkeypatch, damage):
    prepared, validation = await _prepared(monkeypatch)
    metadata = deepcopy(dict(prepared.manifest.source_metadata))
    if damage == "generation":
        metadata["aca_completed_publication"]["generation"]["serving_generation"]["origin_generation"] += 1
    elif damage == "dataset":
        metadata["network_bindings"]["dataset_id"] = str(UUID(int=7))
    elif damage == "completion":
        del metadata["aca_completed_publication"]
    manifest = replace(prepared.manifest, source_metadata=metadata)
    if damage == "manual":
        manifest = replace(manifest, publication_authority="manual-only")
    with pytest.raises(ValueError):
        producer._recipe(replace(prepared, manifest=manifest), validation, _ROLES)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "roles", "native"])
async def test_retained_receipt_is_not_independent_admission(monkeypatch, damage):
    prepared, validation = await _prepared(monkeypatch)
    recipe = producer._recipe(prepared, validation, _ROLES)
    encoded = producer._PREFIX + producer.canonical_registry_source_recipes((recipe,))
    session = SimpleNamespace(
        in_transaction=lambda: True, scalar=AsyncMock(return_value=None if damage == "missing" else encoded)
    )
    gate = AsyncMock(side_effect=ValueError("native custody changed") if damage == "native" else None)
    monkeypatch.setattr(producer, "native_driver", AsyncMock())
    monkeypatch.setattr(producer, "_require_archive", gate)
    roles = ("different_reader",) if damage == "roles" else _ROLES
    with pytest.raises(ValueError):
        await producer.require_registry_aca_source_recipe(session, prepared, runtime_roles=roles)
    assert gate.await_count == (1 if damage == "native" else 0)


@pytest.mark.asyncio
async def test_server_dataset_binding_refuses_conflicting_claims(monkeypatch):
    gate = AsyncMock(return_value=_completed())
    monkeypatch.setattr(producer, "require_completed_publication", gate)
    metadata = _metadata()
    del metadata["network_bindings"]["dataset_schema"]
    del metadata["network_bindings"]["dataset_id"]
    captured = await producer.capture_registry_aca_source_metadata(
        object(), metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
    )
    assert captured["network_bindings"]["dataset_id"] == str(_DATASET)
    assert captured["network_bindings"]["dataset_schema"] == archive.reference_family_stage_schema(_DATASET)
    assert "dataset_id" not in metadata["network_bindings"]
    metadata["network_bindings"].update(dataset_id=str(UUID(int=7)), dataset_schema="unrelated")
    with pytest.raises(ValueError):
        await producer.capture_registry_aca_source_metadata(
            object(), metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
        )
    assert gate.await_count == 1


def test_literal_json_numeric_colons_reproduce_sqlalchemy_bind_failure():
    document = json.dumps({"issuer_id": 7, "label": "O'Reilly\\folder café"}, ensure_ascii=False, separators=(",", ":"))
    statement = text("COMMENT ON SCHEMA synthetic_archive IS '" + document.replace("'", "''") + "'")
    compiled = statement.compile(dialect=PGDialect_asyncpg())
    assert "7" in compiled.params
    with pytest.raises(InvalidRequestError, match="bind parameter '7'"):
        compiled.construct_params()


def _comment_session(kind):
    """Keep actual native_driver/facade routing; only database I/O is mocked."""
    from process.network_cms_registry_address_capture import NativeReceiptSession

    statement = SimpleNamespace(get_attributes=lambda: [], fetch=AsyncMock(return_value=[{"comment": None}]))
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        prepare=AsyncMock(return_value=statement),
        execute=AsyncMock(),
        transaction=Mock(side_effect=AssertionError("must retain original transaction")),
        close=AsyncMock(side_effect=AssertionError("must retain original connection")),
    )
    if kind == "native_facade":
        return NativeReceiptSession(connection), connection
    raw = SimpleNamespace(driver_connection=connection)
    sqlalchemy_connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw))
    session = SimpleNamespace(
        in_transaction=lambda: True,
        connection=AsyncMock(return_value=sqlalchemy_connection),
        scalar=AsyncMock(return_value=None),
        execute=AsyncMock(side_effect=AssertionError("literal JSON must bypass TextClause parsing")),
    )
    return session, connection


def _assert_comment_literal(statement, encoded, schema):
    """Use an independent standard SQL string parser to verify literal UTF-8 bytes."""
    assert type(statement) is str
    prefix = f'COMMENT ON SCHEMA "{schema}" IS '
    assert statement.startswith(prefix)
    with sqlite3.connect(":memory:") as database:
        decoded = database.execute("SELECT " + statement[len(prefix) :]).fetchone()[0]
    assert decoded.encode("utf-8") == encoded.encode("utf-8")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sqlalchemy_session", "native_facade"])
async def test_recipe_comment_preserves_json_and_original_physical_transaction(monkeypatch, kind):
    metadata = _metadata()
    label = "source:7 O'Reilly\\folder café"
    metadata["network_membership"]["source_id"] = metadata["network_bindings"]["source_id"] = label
    prepared, validation = await _prepared(monkeypatch, metadata=metadata)
    session, connection = _comment_session(kind)
    monkeypatch.setattr(producer, "protected_publisher_owner", AsyncMock(return_value=13))
    monkeypatch.setattr(producer, "prepare_reference_family_activation", AsyncMock(return_value=validation))
    custody = AsyncMock()
    monkeypatch.setattr(producer, "_require_archive", custody)
    recipe = await producer.admit_registry_aca_source_recipe(session, prepared, runtime_roles=_ROLES)
    custody.assert_awaited_once_with(connection, recipe.source_pin)
    encoded = producer._PREFIX + producer.canonical_registry_source_recipes((recipe,))
    (statement,) = connection.execute.await_args.args
    _assert_comment_literal(statement, encoded, prepared.ownership.schema_name)
    assert label == json.loads(encoded[len(producer._PREFIX) :])[0]["source_pin"]["source_id"]
    assert session.in_transaction()
    connection.transaction.assert_not_called()
    connection.close.assert_not_awaited()
