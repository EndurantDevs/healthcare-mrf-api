# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated preparation, relocation and alias-fence refusal contracts."""

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_snapshot_preparation as preparation
from tests.test_entity_address_preparation_catalog import _inventory
from tests.test_entity_address_snapshot_preparation import _bound, _session


@pytest.mark.parametrize("validated", [False, True])
async def test_authenticated_inventory_requires_actual_native_owner(monkeypatch, validated):
    owner, proof = _inventory()
    seal_by_field = {"catalog_sha256": "a" * 64} if validated else None
    if validated:
        proof.update(state="validated", validation={"evidence": seal_by_field})
    stored_by_field = {"restored": {"ownership": owner.as_dict()}}
    metadata = Mock()
    monkeypatch.setattr(preparation.destination, "_validated_destination_metadata", metadata)
    session = _session({"owner_oid": 31, "owner_safe": True, "caller_safe": True})
    authenticate = AsyncMock(return_value=proof)
    actual, actual_owner = await preparation._authenticated_preparation(
        session, stored_by_field, {}, seal_by_field, authenticate
    )
    assert actual == proof and actual_owner == owner
    authenticate.assert_awaited_once_with(session, {}, seal_by_field)
    metadata.assert_called_once_with(stored_by_field)


@pytest.mark.parametrize(
    "damage", ["state", "owner", "same_builder", "builder_type", "digest", "schema", "oid", "relations", "seal"]
)
async def test_authenticated_proof_drift_fails_before_relocation(monkeypatch, damage):
    owner, proof = _inventory()
    stored_by_field = {"restored": {"ownership": owner.as_dict()}}
    seal_by_field = None
    if damage in {"schema", "oid", "relations"}:
        field, changed_value = {
            "schema": ("schema_name", "substituted"),
            "oid": ("schema_oid", 42),
            "relations": ("relations", []),
        }[damage]
        proof["inventory"][field] = changed_value
        proof["inventory_sha256"] = preparation._digest(proof["inventory"])
    elif damage == "seal":
        seal_by_field = {"catalog_sha256": "a" * 64}
        proof.update(state="validated", validation={"evidence": {}})
    else:
        field, changed_value = {
            "state": ("state", "consumed"),
            "owner": ("frozen_owner_oid", 33),
            "same_builder": ("builder_oid", 31),
            "builder_type": ("builder_oid", True),
            "digest": ("inventory_sha256", "b" * 64),
        }[damage]
        proof[field] = changed_value
    monkeypatch.setattr(preparation.destination, "_validated_destination_metadata", Mock())
    session = _session({"owner_oid": 31, "owner_safe": True, "caller_safe": True})
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError):
        await preparation._authenticated_preparation(
            session, stored_by_field, {}, seal_by_field, AsyncMock(return_value=proof)
        )
    assert session.execute.await_count == 1


async def test_missing_authentication_is_not_ordinary_authority():
    session = _session({"owner_oid": 31, "owner_safe": True, "caller_safe": True})
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="authentication is unavailable"):
        await preparation._authenticated_preparation(session, {}, {}, None, None)


async def test_publisher_uses_authenticated_namespace_owner(monkeypatch):
    session = _session({"owner_oid": 31, "owner_safe": True, "caller_safe": True})
    guard = AsyncMock(return_value="a" * 64)
    monkeypatch.setattr(preparation.alias_guard, "require_entity_address_alias_guard", guard)
    assert await preparation.require_entity_address_archive_publisher(session, db_schema="example") == 31
    guard.assert_awaited_once_with(session, schema="example", owner_oid=31)


@pytest.mark.parametrize("value", [{"not_serializable": object()}, {"not_finite": float("nan")}])
def test_unserializable_seals_are_typed_refusals(value):
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="payload is invalid"):
        preparation._digest(value)


@pytest.mark.parametrize("bindings", [None, {"synthetic": [1, 2]}])
async def test_private_prepare_preserves_inventory_and_dependency_binding(monkeypatch, bindings):
    owner, _proof = _inventory()
    session = _session()
    prepared = object()
    callback = AsyncMock(return_value=prepared)
    normalize = Mock(return_value={"normalized": [1, 2]})
    monkeypatch.setattr(preparation.destination, "_prepare_bound_destination", callback)
    monkeypatch.setattr(preparation.destination.db, "bind_existing_session", _bound)
    monkeypatch.setattr(preparation.destination.geo_projection, "validate_projection_dependency_bindings", normalize)
    destination_by_field = {"db_schema": "example", "import_date": "20260913", "dependency_bindings": bindings}
    assert (
        await preparation.prepare_private_entity_address_archive_destination(
            session, owner=owner, semantic_receipt={}, source_alias_receipt={}, destination=destination_by_field
        )
        is prepared
    )
    assert callback.await_args.kwargs["preserve_private_stage"] is True
    payload = callback.await_args.args[1]
    assert payload["owner"] is owner
    assert payload["dependency_bindings"] == (bindings if bindings is None else normalize.return_value)
    assert normalize.call_count == (0 if bindings is None else 1)


@pytest.mark.parametrize("destination", [None, {}, {"db_schema": "example", "import_date": "20260913", "extra": True}])
async def test_private_prepare_rejects_unbounded_destination(destination):
    session = _session()
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="destination is invalid"):
        await preparation.prepare_private_entity_address_archive_destination(
            session, owner=None, semantic_receipt={}, source_alias_receipt={}, destination=destination
        )
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("changed", [False, True])
async def test_relocation_requires_exact_persisted_stage_oids(monkeypatch, changed):
    owner, _proof = _inventory()
    schema, _date, names = preparation.destination.restore._stage_plan(db_schema="example", import_date="20260913")
    pairs = tuple(sorted((names[name], oid) for name, oid in owner.relation_oids))
    stored_by_field = {
        "restored": {
            "db_schema": schema,
            "import_date": "20260913",
            "stage_relation_oids": [{"table_name": name, "oid": oid} for name, oid in pairs],
        }
    }
    move = AsyncMock(return_value=pairs[:-1] if changed else pairs)
    monkeypatch.setattr(preparation.destination.restore, "_move_owned_relations", move)
    if changed:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="relocation differs"):
            await preparation._move_to_destination(object(), stored_by_field, owner)
    else:
        assert await preparation._move_to_destination(object(), stored_by_field, owner) == (schema, names)


@pytest.mark.parametrize("changed", [None, "catalog", "generation"])
async def test_alias_fence_binds_catalog_and_local_generation(monkeypatch, changed):
    _owner, proof = _inventory()
    alias = preparation.alias.EntityAddressAliasSemanticReceipt(2, 1, 7, 0, "a" * 64)
    validation_by_field = {"alias": alias.as_dict(), "alias_catalog_sha256": "b" * 64}
    monkeypatch.setattr(preparation.alias, "_lock_alias_relations", AsyncMock())
    monkeypatch.setattr(
        preparation, "_require_alias_authority", AsyncMock(return_value="c" * 64 if changed == "catalog" else "b" * 64)
    )
    state = AsyncMock(return_value=(2, 1, 8 if changed == "generation" else 7))
    monkeypatch.setattr(preparation.alias, "_alias_state", state)
    if changed:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="alias .* changed"):
            await preparation._require_alias_fence(object(), "example", proof, validation_by_field)
        assert state.await_count == (0 if changed == "catalog" else 1)
    else:
        await preparation._require_alias_fence(object(), "example", proof, validation_by_field)


def test_adoption_rehydrates_sealed_context_without_rescanning(monkeypatch):
    context, validation = {"source": "synthetic"}, {"row_count": 4}
    monkeypatch.setattr(
        preparation.destination.restore,
        "_validated_rehydration_state",
        Mock(return_value=("example", "20260913", {}, (), object(), context, validation)),
    )
    result = preparation._prepared_adoption({"restored": {}})
    assert result.db_schema == "example"
    assert result.context is context and result.publish_validation is validation
    assert len(result.required_names) == 7
    assert len(result.relation_names) == 21


@asynccontextmanager
async def _transaction(session):
    yield session


async def test_disabled_numeric_alias_reads_generation_without_writer_lock(monkeypatch):
    numeric_alias = importlib.import_module("process.address_numeric_grid_alias")
    session = _session()
    session.execute.return_value.first.return_value = SimpleNamespace(
        schema_version=2, active_ruleset_version=1, generation=7
    )
    monkeypatch.setattr(numeric_alias.db, "transaction", lambda: _transaction(session))
    result = await numeric_alias._NumericGridAliasRunner(None)._off_result("example", "numeric_grid")
    assert result.run_id is None and result.mode == result.status == "off"
    assert result.generation == 7 and result.source_count == result.candidate_rows == 0
    assert session.execute.await_count == 1
    assert "FOR UPDATE" not in str(session.execute.await_args.args[0])


@pytest.mark.parametrize("provisional,changed", [(False, False), (True, False), (True, True)])
async def test_destination_capture_preserves_portable_semantics_not_local_counter(monkeypatch, provisional, changed):
    destination = preparation.destination
    source = preparation.alias.EntityAddressAliasSemanticReceipt(2, 1, 7, 0, "a" * 64)
    local = preparation.alias.EntityAddressAliasSemanticReceipt(2, 1, 11, 0, ("b" if changed else "a") * 64)
    session = _session()
    regular, provisional_capture = AsyncMock(return_value=local), AsyncMock(return_value=local)
    monkeypatch.setattr(destination, "preserve_transaction_sql_settings", lambda *_args: _bound(session))
    monkeypatch.setattr(destination, "capture_entity_address_alias_semantic_receipt", regular)
    monkeypatch.setattr(destination, "_capture_alias_semantic_receipt", provisional_capture)
    if changed:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError):
            await destination._destination_alias_binding(
                session, db_schema="example", source_alias_receipt=source, provisional=provisional
            )
    else:
        assert await destination._destination_alias_binding(
            session, db_schema="example", source_alias_receipt=source, provisional=provisional
        ) == (source, local)
    assert regular.await_count == int(not provisional)
    assert provisional_capture.await_count == int(provisional)


@pytest.mark.parametrize("late_failure", [False, True])
async def test_private_preparation_returns_exact_inventory_after_complete_evidence(monkeypatch, late_failure):
    destination = preparation.destination
    owner, _proof = _inventory()
    schema, import_date, names = destination.restore._stage_plan(db_schema="example", import_date="20260913")
    stage_oids = tuple(sorted((names[name], oid) for name, oid in owner.relation_oids))
    alias = preparation.alias.EntityAddressAliasSemanticReceipt(2, 1, 7, 0, "a" * 64)
    semantic_receipt, remap, integrity, geo = object(), object(), object(), object()
    prepared = SimpleNamespace(db_schema=schema, context={"source": "synthetic"}, publish_validation={"row_count": 4})
    monkeypatch.setattr(destination, "_destination_alias_binding", AsyncMock(return_value=(alias, alias)))
    monkeypatch.setattr(
        destination,
        "_validate_owned_source",
        AsyncMock(return_value=(owner, semantic_receipt, schema, import_date, names)),
    )
    monkeypatch.setattr(destination, "_remap_base_versions", AsyncMock(return_value=(remap, semantic_receipt)))
    monkeypatch.setattr(destination, "_move_remapped_destination", AsyncMock(return_value=stage_oids))
    monkeypatch.setattr(destination, "_prepare_moved_destination", AsyncMock(return_value=(prepared, geo, integrity)))
    handback = AsyncMock(side_effect=RuntimeError("handback refused") if late_failure else None)
    monkeypatch.setattr(destination.restore, "_return_owned_relations", handback)
    payload_by_field = {
        "owner": owner,
        "semantic_receipt": semantic_receipt,
        "source_alias_receipt": alias,
        "db_schema": schema,
        "import_date": import_date,
    }
    if late_failure:
        with pytest.raises(RuntimeError, match="handback refused"):
            await destination._prepare_bound_destination(object(), payload_by_field, preserve_private_stage=True)
    else:
        prepared_destination = await destination._prepare_bound_destination(
            object(), payload_by_field, preserve_private_stage=True
        )
        assert (
            prepared_destination.restored.ownership is owner
            and prepared_destination.restored.stage_relation_oids == stage_oids
        )
        assert (
            prepared_destination.restored.semantic_receipt is semantic_receipt
            and prepared_destination.restored.stage_integrity is integrity
        )
        assert prepared_destination.base_version_remap is remap and prepared_destination.geo_assurance is geo
    handback.assert_awaited_once()
    assert handback.await_args.kwargs == {"owner": owner, "db_schema": schema, "stage_names": names}


async def test_sequence_handback_drift_refuses_before_any_ddl(monkeypatch):
    restore = preparation.destination.restore
    owner, _proof = _inventory()
    names_by_table = {name: name + "_candidate" for name, _oid in owner.relation_oids}
    monkeypatch.setattr(restore, "_require_empty_owned_schema", AsyncMock())
    monkeypatch.setattr(restore, "_verify_moved_stage_oids", AsyncMock())
    monkeypatch.setattr(restore, "_owned_sequence_names", AsyncMock(return_value=("substituted_sequence",)))
    verified = AsyncMock()
    monkeypatch.setattr(restore, "verify_entity_address_archive_stage_ownership", verified)
    session = _session()
    with pytest.raises(restore.EntityAddressSnapshotRestoreError, match="sequence ownership changed"):
        await restore._return_owned_relations(session, owner=owner, db_schema="example", stage_names=names_by_table)
    session.execute.assert_not_awaited()
    verified.assert_not_awaited()
