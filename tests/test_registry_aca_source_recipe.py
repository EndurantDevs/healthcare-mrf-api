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


async def _prepared(monkeypatch, *, metadata=None, accounted=False):
    completed = _completed()
    if accounted:
        from tests.test_mrf_address_accounting import _coverage

        completed["native_address_coverage"] = _coverage()
    monkeypatch.setattr(producer, "require_completed_publication", AsyncMock(return_value=completed))
    metadata = await producer.capture_registry_aca_source_metadata(
        object(), _metadata() if metadata is None else metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
    )
    spec = archive.reference_family_spec("mrf", canonical=True)
    tables = tuple(
        archive.ReferenceTableReceipt(
            model.__name__,
            model.__tablename__,
            "a" * 64,
            3 if accounted and model.__tablename__ in {"mrf_address", "mrf_address_evidence"} else 0,
        )
        for model in spec.model_types
    )
    manifest = archive.ReferenceFamilyManifest(
        "mrf",
        tables,
        metadata,
        archive._source_metadata(metadata)[1],
        archive._schema_digest(tables),
        {"plan-attributes": "c" * 64},
        archive._canonical_model_receipt(completed) if accounted else archive._canonical_model_receipt(),
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
@pytest.mark.parametrize("damage", [None, "coverage", "attempt", "generation", "digest"])
async def test_accounted_recipe_binds_the_complete_clone_proof_and_completion(monkeypatch, damage):
    prepared, validation = await _prepared(monkeypatch, accounted=True)
    if damage is None:
        assert producer._recipe(prepared, validation, _ROLES).source_pin.prepared == prepared
        return
    metadata = deepcopy(prepared.manifest.source_metadata)
    auxiliary = deepcopy(prepared.manifest.auxiliary)
    completion = metadata["aca_completed_publication"]
    if damage == "coverage":
        counts = auxiliary["address_coverage"]["tables"]["mrf_address"]
        counts["covered"] -= 1
        counts["unkeyable_null"] += 1
    elif damage == "attempt":
        completion["attempt_id"] = "20000000-0000-0000-0000-000000000001"
    elif damage == "generation":
        completion["generation"]["local_generation"] = 99
    else:
        auxiliary["publication_sha256"] = "f" * 64
    manifest = replace(prepared.manifest, source_metadata=metadata, auxiliary=auxiliary)
    pin = Mock(side_effect=AssertionError("mismatched proof reached native pin construction"))
    monkeypatch.setattr(producer, "PinnedACAMembershipSource", pin)
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        producer._recipe(replace(prepared, manifest=manifest), validation, _ROLES)
    pin.assert_not_called()


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


def _batch_scopes():
    first, second = _metadata(), _metadata()
    second["network_membership"].update(source_id="source-second", source_url="https://source.example.test/second")
    second["network_bindings"]["source_id"] = "source-second"
    return sorted([first, second], key=archive._canonical_json)


def _batch_coverage(scopes=None):
    scopes = _batch_scopes() if scopes is None else scopes
    return [
        {
            "issuer_id": 7,
            "year": 2026,
            "import_id": "import-one",
            "raw_rows": len(scopes),
            "covered_raw_rows": len(scopes),
            "urls": [
                {"source_url": url, "raw_rows": 1, "evidence_rows": 2, "unmatched_evidence_rows": 0}
                for url in sorted(scope["network_membership"]["source_url"] for scope in scopes)
            ],
        }
    ]


async def _batch_prepared(monkeypatch, scopes=None):
    prepared, validation = await _prepared(monkeypatch)
    scopes = _batch_scopes() if scopes is None else scopes
    monkeypatch.setattr(producer, "_capture_batch_coverage", AsyncMock(return_value=_batch_coverage(scopes)))
    metadata = await producer.capture_registry_aca_source_batch_metadata(
        object(), scopes, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
    )
    manifest = replace(
        prepared.manifest, source_metadata=metadata, source_metadata_sha256=archive._source_metadata(metadata)[1]
    )
    fields = validation.as_dict()
    fields.pop("validation_sha256")
    fields["manifest_sha256"] = hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest()
    validation = archive.validate_reference_family_validation_receipt(
        {**fields, "validation_sha256": archive._validation_digest(fields)}
    )
    return replace(prepared, manifest=manifest), validation


@pytest.mark.asyncio
async def test_batch_one_physical_manifest_keeps_distinct_exact_scopes(monkeypatch):
    from process.network_legacy_membership_source import _aca_binding_coordinates, _reviewed_descriptor
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    prepared, validation = await _batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, _ROLES)
    assert len(recipes) == 2 and len({recipe.source_pin.generation_id for recipe in recipes}) == 2
    assert {recipe.source_pin.prepared.ownership.dataset_id for recipe in recipes} == {_DATASET}
    assert all(recipe.source_pin.prepared == prepared for recipe in recipes)
    assert decode_registry_source_recipes(canonical_registry_source_recipes(recipes)) == recipes
    for recipe in recipes:
        assert _aca_binding_coordinates(recipe.source_pin) == recipe.binding_coordinates
        with pytest.raises(ValueError):
            replace(recipe, binding_coordinates=recipes[1 if recipe == recipes[0] else 0].binding_coordinates)
        assert _reviewed_descriptor(recipe.source_pin, None, None) is None


@pytest.mark.asyncio
async def test_batch_descriptor_cache_reuses_only_exact_metadata_and_returns_fresh_scopes(monkeypatch):
    from process import network_legacy_membership_source as legacy

    prepared, validation = await _batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, _ROLES)
    validate = Mock(wraps=producer._batch_descriptors)
    monkeypatch.setattr(producer, "_batch_descriptors", validate)
    legacy._cached_batch_descriptors.cache_clear()
    try:
        first = legacy.aca_source_descriptor(recipes[0].source_pin)
        expected = deepcopy(first)
        first["network_membership"]["source_url"] = "https://changed.example.test/providers"
        assert legacy.aca_source_descriptor(recipes[0].source_pin) == expected
        second = legacy.aca_source_descriptor(recipes[1].source_pin)
        assert second["network_membership"] == recipes[1].source_pin.coordinates
        assert validate.call_count == 1
        altered = deepcopy(prepared.manifest.source_metadata)
        altered[producer._BATCH_KEY]["coverage"][0]["unproved"] = True
        changed_source = SimpleNamespace(
            prepared=SimpleNamespace(manifest=SimpleNamespace(source_metadata=altered)),
            runtime_roles=_ROLES,
            coordinates=recipes[0].source_pin.coordinates,
        )
        with pytest.raises(ValueError):
            legacy.aca_source_descriptor(changed_source)
        changed_source.prepared = prepared
        changed_source.runtime_roles = ("foreign_reader",)
        with pytest.raises(ValueError):
            legacy.aca_source_descriptor(changed_source)
        assert validate.call_count == 3
        assert legacy._cached_batch_descriptors.cache_info().currsize == 1
    finally:
        legacy._cached_batch_descriptors.cache_clear()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "third_url",
        "uncovered",
        "foreign_import",
        "orphan",
        "boolean",
        "duplicate_group",
        "missing_group",
        "missing_url",
    ],
)
async def test_batch_coverage_requires_exact_union_and_separate_occurrences(monkeypatch, damage):
    scopes = _batch_scopes()
    coverage = _batch_coverage(scopes)
    match damage:
        case "third_url":
            coverage[0]["urls"].append({**coverage[0]["urls"][0], "source_url": "https://source.example.test/third"})
        case "uncovered":
            coverage[0]["covered_raw_rows"] = 1
        case "foreign_import":
            coverage[0]["import_id"] = "unrelated"
        case "orphan":
            coverage[0]["urls"][0]["unmatched_evidence_rows"] = 1
        case "boolean":
            coverage[0]["raw_rows"] = True
        case "duplicate_group":
            coverage *= 2
        case "missing_group":
            coverage.clear()
        case _:
            coverage[0]["urls"].pop()
    with pytest.raises(ValueError):
        producer._validate_batch_coverage(coverage, scopes)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["duplicate_scope", "same_coordinates", "mixed_import", "mixed_dataset", "extra", "oversize", "too_many"]
)
async def test_batch_descriptor_failures_cannot_admit_partial_scopes(monkeypatch, damage):
    scopes = _batch_scopes()
    match damage:
        case "duplicate_scope":
            scopes.append(deepcopy(scopes[0]))
        case "same_coordinates":
            scopes[1]["network_bindings"] = deepcopy(scopes[0]["network_bindings"])
            scopes[1]["network_membership"]["source_id"] = scopes[0]["network_membership"]["source_id"]
        case "mixed_import":
            scopes[1]["network_membership"]["import_id"] = "foreign"
        case "mixed_dataset":
            scopes[1]["network_bindings"]["dataset_id"] = str(UUID(int=9))
        case "extra":
            scopes[0]["bypass"] = True
        case "oversize":
            scopes = [deepcopy(scopes[0]) for _ in range(12)]
            for index, scope in enumerate(scopes):
                scope["network_membership"].update(
                    source_id=f"source-{index}", source_url=f"https://source.example.test/{index}/" + "a" * 1900
                )
                scope["network_bindings"]["source_id"] = f"source-{index}"
        case _:
            scopes *= 51
    monkeypatch.setattr(producer, "require_completed_publication", AsyncMock(return_value=_completed()))
    coverage = AsyncMock()
    monkeypatch.setattr(producer, "_capture_batch_coverage", coverage)
    with pytest.raises((ValueError, archive.ReferenceFamilyArchiveError)):
        await producer.capture_registry_aca_source_batch_metadata(
            object(), scopes, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
        )
    coverage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 2])
async def test_batch_stamp_is_whole_and_singleton_requires_explicit_selector(monkeypatch, count):
    prepared, validation = await _batch_prepared(monkeypatch, _batch_scopes()[:count])
    session, connection = _comment_session("native_facade")
    monkeypatch.setattr(producer, "protected_publisher_owner", AsyncMock(return_value=13))
    monkeypatch.setattr(producer, "prepare_reference_family_activation", AsyncMock(return_value=validation))
    native = AsyncMock()
    monkeypatch.setattr(producer, "_require_native_archive", native)
    recipes = await producer.admit_registry_aca_source_batch(session, prepared, runtime_roles=_ROLES)
    assert native.await_count == 1
    encoded = producer._BATCH_PREFIX + producer.canonical_registry_source_recipes(recipes)
    _assert_comment_literal(connection.execute.await_args.args[0], encoded, prepared.ownership.schema_name)
    session.scalar = AsyncMock(return_value=encoded)
    assert await producer.require_registry_aca_source_batch(session, prepared, runtime_roles=_ROLES) == recipes
    for recipe in recipes:
        assert (
            await producer.require_registry_aca_source_recipe(
                session, prepared, runtime_roles=_ROLES, binding_coordinates=recipe.binding_coordinates
            )
            == recipe
        )
    with pytest.raises(ValueError):
        await producer.require_registry_aca_source_recipe(session, prepared, runtime_roles=_ROLES)
    session.scalar.return_value = producer._BATCH_PREFIX + producer.canonical_registry_source_recipes(recipes[:1])
    if count > 1:
        with pytest.raises(ValueError):
            await producer.require_registry_aca_source_batch(session, prepared, runtime_roles=_ROLES)


@pytest.mark.asyncio
async def test_batch_cannot_enter_singleton_notification_path(monkeypatch):
    from process import registry_aca_source_notifications as notifications

    prepared, validation = await _batch_prepared(monkeypatch)
    recipe = producer._batch_recipes(prepared, validation, _ROLES)[0]
    with pytest.raises(ValueError):
        await notifications._require_subject(SimpleNamespace(in_transaction=lambda: True), recipe)


@pytest.mark.asyncio
async def test_batch_scope_paging_is_url_local_and_singleton_is_unchanged(monkeypatch):
    from process.network_legacy_membership_source import _raw_fact_selection_sql

    ordinary = _raw_fact_selection_sql()[0]
    batch = _raw_fact_selection_sql(True)[0]
    assert "selected_evidence" not in ordinary
    for clause in (
        "selected_evidence.import_id=$8",
        "selected_evidence.source_url=$11",
        "selected_evidence.network_tier=raw.network_tier",
        "selected_evidence.issuer_id=raw.issuer_id",
        "selected_evidence.year=raw.year",
    ):
        assert clause in batch
    assert "addresses" not in batch


@pytest.mark.asyncio
async def test_batch_anchors_keep_exact_scope_through_global_epoch_reuse(monkeypatch):
    from process.network_legacy_membership_source import with_aca_global_epoch
    from process.registry_source_recipe_store import (
        RegistrySourceMembershipRecipe,
        canonical_registry_source_recipes,
        decode_registry_source_recipes,
    )
    from tests.test_registry_aca_global_epoch import _global_prepared

    prepared, validation = await _batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, _ROLES)
    global_prepared, global_validation = await _global_prepared(monkeypatch)
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch

    epoch = PinnedACAGlobalEpoch(global_prepared, global_validation, _ROLES)
    combined_recipes = tuple(
        RegistrySourceMembershipRecipe(with_aca_global_epoch(recipe.source_pin, epoch), recipe.binding_coordinates)
        for recipe in recipes
    )
    assert decode_registry_source_recipes(canonical_registry_source_recipes(combined_recipes)) == combined_recipes
    assert len({recipe.source_pin.retained_epoch.generation_id for recipe in combined_recipes}) == 1
    assert [recipe.binding_coordinates for recipe in combined_recipes] == [
        recipe.binding_coordinates for recipe in recipes
    ]


@pytest.mark.asyncio
async def test_batch_successor_preserves_anchor_and_exact_physical_selector(monkeypatch):
    from process import network_legacy_membership_source as aca
    from process import registry_source_recipe_store as codec

    original, original_validation = await _batch_prepared(monkeypatch)
    anchors = producer._batch_recipes(original, original_validation, _ROLES)
    monkeypatch.setitem(globals(), "_DATASET", UUID(int=2))
    prepared, validation = await _batch_prepared(monkeypatch)
    successors = producer._batch_recipes(prepared, validation, _ROLES)
    combined_recipes = tuple(
        codec.RegistrySourceMembershipRecipe(
            aca.with_aca_successor(anchor.source_pin, successor.source_pin), anchor.binding_coordinates
        )
        for anchor, successor in zip(anchors, successors, strict=True)
    )
    assert (
        codec.decode_registry_source_recipes(codec.canonical_registry_source_recipes(combined_recipes))
        == combined_recipes
    )
    origin = AsyncMock()
    stamp = AsyncMock()
    monkeypatch.setattr(aca, "_require_flat_archive", origin)
    monkeypatch.setattr(producer, "require_registry_aca_source_recipe", stamp)
    for combined, anchor, successor in zip(combined_recipes, anchors, successors, strict=True):
        stamp.return_value = successor
        await aca._require_archive(object(), combined.source_pin)
        assert origin.await_args.args[1] == anchor.source_pin
        assert stamp.await_args.args[1] == successor.source_pin.prepared
        assert stamp.await_args.kwargs["binding_coordinates"] == successor.binding_coordinates
    stamp.return_value = successors[0]
    with pytest.raises(aca.LegacyMembershipSourceError):
        await aca._require_archive(object(), combined_recipes[1].source_pin)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_network_dsn", [False, True])
async def test_native_fixture_reference_fallback_keeps_original_cleanup(monkeypatch, has_network_dsn):
    import os
    from contextlib import asynccontextmanager

    from tests import test_mrf_publication_receipt_postgres as native

    previous = os.environ.get("NETWORK_REGISTRY_TEST_DSN")
    reference_dsn = "postgresql+asyncpg://postgres@localhost:5440/hc_reference_family_" + "1" * 32
    network_dsn = "postgresql+asyncpg://postgres@localhost:5440/explicit_network_fixture"
    events = []

    async def registry_fixture():
        events.append(os.environ["NETWORK_REGISTRY_TEST_DSN"])
        try:
            yield ("original fixture",)
        finally:
            events.append("closed")

    with monkeypatch.context() as scoped:
        scoped.setenv("HLTHPRT_REFERENCE_FAMILY_ARCHIVE_TEST_DSN", reference_dsn)
        if has_network_dsn:
            scoped.setenv("NETWORK_REGISTRY_TEST_DSN", network_dsn)
        else:
            scoped.delenv("NETWORK_REGISTRY_TEST_DSN", raising=False)
        scoped.setattr(native, "_registry_serving_schema", SimpleNamespace(__wrapped__=registry_fixture))
        async with asynccontextmanager(native.serving_schema.__wrapped__)() as fixture:
            assert fixture == ("original fixture",)
        assert events == [network_dsn if has_network_dsn else reference_dsn, "closed"]
        assert os.environ.get("NETWORK_REGISTRY_TEST_DSN") == (network_dsn if has_network_dsn else None)
    assert os.environ.get("NETWORK_REGISTRY_TEST_DSN") == previous


@pytest.mark.asyncio
async def test_native_fixture_never_replaces_explicit_invalid_network_dsn(monkeypatch):
    from contextlib import asynccontextmanager

    import asyncpg

    from tests import test_mrf_publication_receipt_postgres as native

    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", "invalid-explicit")
    reference_selector = Mock(side_effect=AssertionError("must not replace explicit selection"))
    monkeypatch.setattr(native, "_database_url", reference_selector)
    with pytest.raises(asyncpg.ClientConfigurationError):
        async with asynccontextmanager(native.serving_schema.__wrapped__)():
            pytest.fail("invalid explicit network selection was accepted")
    reference_selector.assert_not_called()


@pytest.mark.asyncio
async def test_native_fixture_refuses_unvalidated_reference_endpoint(monkeypatch):
    from contextlib import asynccontextmanager

    from tests import test_mrf_publication_receipt_postgres as native

    monkeypatch.delenv("NETWORK_REGISTRY_TEST_DSN", raising=False)
    monkeypatch.setenv(
        "HLTHPRT_REFERENCE_FAMILY_ARCHIVE_TEST_DSN",
        "postgresql+asyncpg://postgres@localhost:12345/hc_reference_family_" + "1" * 32,
    )
    registry_factory = Mock(side_effect=AssertionError("invalid reference must fail before registry setup"))
    monkeypatch.setattr(native, "_registry_serving_schema", SimpleNamespace(__wrapped__=registry_factory))
    with pytest.raises(pytest.fail.Exception, match="UUID-owned local PostgreSQL"):
        async with asynccontextmanager(native.serving_schema.__wrapped__)():
            pytest.fail("invalid reference selection was accepted")
    registry_factory.assert_not_called()


def _representative_batch_scopes():
    """Use synthetic 21-scope coordinates with explicit representative identity widths."""
    scopes = []
    for issuer, count in ((10007, 2), (10008, 15), (10009, 3), (10010, 1)):
        for index in range(count):
            scope = _metadata()
            scope["network_membership"].update(
                source_id=f"source-{issuer}-{index}".ljust(32, "x"),
                release_id="release-example".ljust(24, "x"),
                issuer_id=issuer,
                source_url=f"https://source.example.test/{issuer}/providers-{index}.json".ljust(85, "x"),
                alias_scope="medical".ljust(24, "x"),
            )
            scope["network_bindings"].update(
                source_id=scope["network_membership"]["source_id"],
                producer_id=f"producer-{issuer}-{index}".ljust(36, "x"),
                edition_id="edition-one".ljust(24, "x"),
            )
            scopes.append(scope)
    return sorted(scopes, key=archive._canonical_json)


def _representative_batch_coverage(scopes):
    """Preserve distinct union and occurrence accounting across four issuer scopes."""
    groups = []
    for issuer in sorted({scope["network_membership"]["issuer_id"] for scope in scopes}):
        urls = sorted(
            scope["network_membership"]["source_url"]
            for scope in scopes
            if scope["network_membership"]["issuer_id"] == issuer
        )
        groups.append(
            {
                "issuer_id": issuer,
                "year": 2026,
                "import_id": "import-one",
                "raw_rows": 10000000,
                "covered_raw_rows": 10000000,
                "urls": [
                    {"source_url": url, "raw_rows": 10000000, "evidence_rows": 20000000, "unmatched_evidence_rows": 0}
                    for url in urls
                ],
            }
        )
    return sorted(groups, key=archive._canonical_json)


def _with_representative_manifest(prepared, validation, metadata):
    """Exercise the real 14-table shape with eight-digit counts and native identities."""
    tables = tuple(replace(table, row_count=10000000) for table in prepared.manifest.tables)
    manifest = replace(
        prepared.manifest,
        tables=tables,
        source_metadata=metadata,
        source_metadata_sha256=archive._source_metadata(metadata)[1],
    )
    ownership = replace(
        prepared.ownership,
        schema_oid=10000000,
        relation_oids=tuple((name, oid + 10000000) for name, oid in prepared.ownership.relation_oids),
    )
    values = validation.as_dict()
    values.pop("validation_sha256")
    values.update(
        tables=[table.as_dict() for table in tables],
        stage_schema_oid=ownership.schema_oid,
        relation_oids=[list(pair) for pair in ownership.relation_oids],
        sealed_owner_oid=10000001,
        manifest_sha256=hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest(),
    )
    result = archive.validate_reference_family_validation_receipt(
        {**values, "validation_sha256": archive._validation_digest(values)}
    )
    return replace(prepared, manifest=manifest, ownership=ownership), result


async def _representative_batch(monkeypatch):
    from tests.test_registry_aca_global_epoch import _global_completion

    prepared, validation = await _prepared(monkeypatch)
    scopes = _representative_batch_scopes()
    monkeypatch.setattr(producer, "require_completed_publication", AsyncMock(return_value=_global_completion()))
    monkeypatch.setattr(
        producer, "_capture_batch_coverage", AsyncMock(return_value=_representative_batch_coverage(scopes))
    )
    metadata = await producer.capture_registry_aca_source_batch_metadata(
        object(), scopes, schema="mrf", runtime_roles=_ROLES, dataset_id=_DATASET
    )
    return _with_representative_manifest(prepared, validation, metadata)


@pytest.mark.asyncio
async def test_compact_twenty_one_scope_initial_and_global_refresh_fit_same_limits(monkeypatch):
    from process import registry_source_recipe_store as codec
    from process.network_legacy_membership_source import with_aca_global_epoch
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch
    from tests.test_registry_aca_global_epoch import _global_prepared

    roles = tuple(f"source_reader_{index}".ljust(32, "x") for index in range(3))
    monkeypatch.setitem(globals(), "_ROLES", roles)
    prepared, validation = await _representative_batch(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, roles)
    initial = codec.canonical_registry_source_recipes(recipes)
    assert len(recipes) == 21 and len(prepared.manifest.tables) == 14
    assert len(archive._canonical_json(prepared.manifest.source_metadata)) < 16384
    assert len(json.loads(initial)) == 1
    assert len(json.loads(initial)[0]["recipes"]) == 21
    assert codec.decode_registry_source_recipes(initial) == recipes
    epoch_prepared, epoch_validation = await _global_prepared(monkeypatch)
    epoch = PinnedACAGlobalEpoch(epoch_prepared, epoch_validation, ("source_reader",))
    refreshed_recipes = tuple(
        codec.RegistrySourceMembershipRecipe(
            with_aca_global_epoch(recipe.source_pin, epoch), recipe.binding_coordinates
        )
        for recipe in recipes
    )
    legacy_documents = [codec._recipe_document(recipe) for recipe in refreshed_recipes]
    assert len(json.dumps(legacy_documents, separators=(",", ":")).encode()) > codec.MAX_SOURCE_RECIPE_BYTES
    encoded = codec.canonical_registry_source_recipes(refreshed_recipes)
    assert len(encoded.encode()) < codec.MAX_SOURCE_RECIPE_BYTES
    assert codec.decode_registry_source_recipes(encoded) == refreshed_recipes
    assert codec.canonical_registry_source_recipes(tuple(reversed(refreshed_recipes))) == encoded
    assert codec.registry_source_recipes_sha256(encoded) == hashlib.sha256(encoded.encode()).hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "physical_extra",
        "physical_dataset",
        "roles",
        "source_id",
        "scope_extra",
        "import_override",
        "coverage_import",
        "coverage_extra",
        "completion_import",
    ],
)
async def test_compact_metadata_reconstructs_only_the_complete_original_contract(monkeypatch, damage):
    prepared, _validation = await _batch_prepared(monkeypatch)
    metadata = deepcopy(prepared.manifest.source_metadata)
    batch = metadata[producer._BATCH_KEY]
    match damage:
        case "physical_extra":
            batch["physical"]["producer_id"] = "foreign"
        case "physical_dataset":
            batch["physical"]["dataset_id"] = []
        case "roles":
            batch["physical"]["runtime_roles"] = ["foreign"]
        case "source_id":
            batch["scopes"][0].pop("source_id")
        case "scope_extra":
            batch["scopes"][0]["source_system"] = "fhir"
        case "import_override":
            batch["scopes"][0]["import_id"] = "foreign"
        case "coverage_import":
            batch["coverage"][0]["import_id"] = "foreign"
        case "coverage_extra":
            batch["coverage"][0]["unproved"] = True
        case _:
            metadata["aca_completed_publication"]["import_id"] = ""
    with pytest.raises(ValueError):
        producer._batch_descriptors(metadata, _ROLES)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "shared_extra",
        "child_override",
        "foreign_kind",
        "nested",
        "missing_scope",
        "foreign_coordinates",
        "foreign_owner",
        "foreign_validation",
        "empty",
        "too_many",
        "non_batch",
        "wire_bytes",
    ],
)
async def test_batch_wire_rejects_malformed_or_substituted_shared_capture(monkeypatch, damage):
    from process import registry_source_recipe_store as codec

    prepared, validation = await _batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, _ROLES)
    documents = json.loads(codec.canonical_registry_source_recipes(recipes))
    batch, child = documents[0], documents[0]["recipes"][0]
    match damage:
        case "shared_extra":
            batch["reference"] = "another-capture"
        case "child_override":
            child["source_pin"]["prepared"] = batch["prepared"]
        case "foreign_kind":
            child["source_kind"] = "fhir"
        case "nested":
            batch["recipes"][0] = deepcopy(batch)
        case "missing_scope":
            child["source_pin"].pop("source_url")
        case "foreign_coordinates":
            child["binding_coordinates"]["producer_id"] = "foreign"
        case "foreign_owner":
            batch["prepared"]["ownership"]["schema_oid"] += 1
        case "foreign_validation":
            batch["validation"]["manifest_sha256"] = "f" * 64
        case "empty":
            batch["recipes"] = []
        case "too_many":
            batch["recipes"] *= 51
            monkeypatch.setattr(codec, "_decode_recipe", Mock(side_effect=AssertionError("count before decoding")))
        case "non_batch":
            batch["prepared"]["manifest"]["source_metadata"].pop(producer._BATCH_KEY)
        case _:
            batch["unbounded"] = "x" * codec.MAX_SOURCE_RECIPE_BYTES
    with pytest.raises(codec.RegistrySourceRecipeError):
        codec.decode_registry_source_recipes(documents)


@pytest.mark.asyncio
async def test_singleton_wire_stays_exact_and_mixed_batches_remain_complete(monkeypatch):
    from process import registry_source_recipe_store as codec
    from tests.test_registry_source_recipe_store_postgres import _example

    prepared, validation = await _prepared(monkeypatch)
    singleton = producer._recipe(prepared, validation, _ROLES)
    ordinary = (_example(), singleton)
    legacy = codec._canonical_json(
        sorted([codec._recipe_document(recipe) for recipe in ordinary], key=codec._canonical_json)
    )
    assert codec.canonical_registry_source_recipes(ordinary) == legacy
    prepared, validation = await _batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, _ROLES)
    mixed = codec.canonical_registry_source_recipes((*recipes, *ordinary))
    decoded = codec.decode_registry_source_recipes(mixed)
    assert len(decoded) == 4
    assert {recipe.source_pin.generation_id for recipe in decoded} == {
        recipe.source_pin.generation_id for recipe in (*recipes, *ordinary)
    }
    assert codec.canonical_registry_source_recipes(decoded) == mixed


@pytest.mark.asyncio
async def test_batch_unicode_ids_require_the_canonical_admitted_stamp(monkeypatch):
    scopes = _batch_scopes()
    for scope, source_id in zip(scopes, ("source-é", "source-z"), strict=True):
        scope["network_membership"]["source_id"] = source_id
        scope["network_bindings"]["source_id"] = source_id
    prepared, validation = await _batch_prepared(monkeypatch, scopes)
    session, _connection = _comment_session("native_facade")
    monkeypatch.setattr(producer, "protected_publisher_owner", AsyncMock(return_value=13))
    monkeypatch.setattr(producer, "prepare_reference_family_activation", AsyncMock(return_value=validation))
    native = AsyncMock()
    monkeypatch.setattr(producer, "_require_native_archive", native)
    admitted = await producer.admit_registry_aca_source_batch(session, prepared, runtime_roles=_ROLES)
    encoded = producer._BATCH_PREFIX + producer.canonical_registry_source_recipes(admitted)
    session.scalar = AsyncMock(return_value=encoded)
    required = await producer.require_registry_aca_source_batch(session, prepared, runtime_roles=_ROLES)
    assert [recipe.source_pin.source_id for recipe in admitted] == ["source-é", "source-z"]
    assert [recipe.source_pin.source_id for recipe in required] == ["source-z", "source-é"]
    assert producer.canonical_registry_source_recipes(required) == producer.canonical_registry_source_recipes(admitted)
    assert native.await_count == 2
    for recipe in admitted:
        assert (
            await producer.require_registry_aca_source_recipe(
                session, prepared, runtime_roles=_ROLES, binding_coordinates=recipe.binding_coordinates
            )
            == recipe
        )
    session.scalar.return_value = producer._BATCH_PREFIX + producer.canonical_registry_source_recipes(admitted[:1])
    before = native.await_count
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.require_registry_aca_source_batch(session, prepared, runtime_roles=_ROLES)
    assert native.await_count == before


async def _verified_batch_fixture(monkeypatch):
    prepared, validation = await _batch_prepared(monkeypatch)
    recipes = producer._batch_recipes(prepared, validation, _ROLES)
    encoded = producer._BATCH_PREFIX + producer.canonical_registry_source_recipes(recipes)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=[("4/23", "reader", "reader", "repeatable read")]),
        fetchval=AsyncMock(return_value=encoded),
    )
    decode = AsyncMock(return_value=producer.decode_registry_source_recipes(encoded[len(producer._BATCH_PREFIX) :]))
    native = AsyncMock()
    monkeypatch.setattr(producer, "require_registry_aca_source_batch", decode)
    monkeypatch.setattr(producer, "_require_native_archive", native)
    return connection, recipes, decode, native


@pytest.mark.asyncio
async def test_verified_batch_shares_one_decode_and_keeps_every_page_native_check(monkeypatch):
    connection, recipes, decode, native = await _verified_batch_fixture(monkeypatch)
    verified = await producer.prepare_registry_aca_batch_read(connection, recipes[0].source_pin)
    for recipe in recipes * 2:
        assert (
            await producer.prepare_registry_aca_batch_read(connection, recipe.source_pin, verified=verified) is verified
        )
    assert decode.await_count == 1 and native.await_count == 4
    assert connection.fetchval.await_count == 4 and connection.fetch.await_count == 6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["transaction", "role", "stamp", "metadata", "roles", "connection", "inactive", "isolation", "ambiguous"]
)
async def test_verified_batch_refuses_changes_without_falling_back_to_decode(monkeypatch, damage):
    connection, recipes, decode, native = await _verified_batch_fixture(monkeypatch)
    source = recipes[0].source_pin
    verified = await producer.prepare_registry_aca_batch_read(connection, source)
    transactions_by_damage = {
        "transaction": [("4/24", "reader", "reader", "repeatable read")],
        "role": [("4/23", "other", "reader", "repeatable read")],
        "isolation": [("4/23", "reader", "reader", "read committed")],
        "ambiguous": connection.fetch.return_value * 2,
    }
    if damage in transactions_by_damage:
        connection.fetch.return_value = transactions_by_damage[damage]
    if damage == "stamp":
        connection.fetchval.return_value += " "
    if damage == "metadata":
        source.prepared.manifest.source_metadata[producer._BATCH_KEY]["coverage"] = []
    if damage == "roles":
        verified = replace(verified, runtime_roles=("another_reader",))
    if damage == "connection":
        connection = deepcopy(connection)
    if damage == "inactive":
        connection.is_in_transaction = lambda: False
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.require_registry_aca_batch_read(connection, source, verified)
    assert decode.await_count == 1
    native.assert_not_awaited()


@pytest.mark.asyncio
async def test_verified_batch_refuses_preflight_transaction_change(monkeypatch):
    connection, recipes, decode, native = await _verified_batch_fixture(monkeypatch)
    connection.fetch.side_effect = [
        [("4/23", "reader", "reader", "repeatable read")],
        [("4/24", "reader", "reader", "repeatable read")],
    ]
    with pytest.raises(ValueError, match="registry_aca_source_recipe_invalid"):
        await producer.prepare_registry_aca_batch_read(connection, recipes[0].source_pin)
    assert decode.await_count == 1
    native.assert_not_awaited()
