# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_snapshot_preparation as preparation


def _session(row=None):
    result = Mock()
    result.mappings.return_value.one_or_none.return_value = row
    return SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(return_value=result), scalar=AsyncMock())


@asynccontextmanager
async def _bound(_session):
    yield


@pytest.mark.parametrize(
    "row",
    [
        None,
        {"owner_oid": 31, "owner_safe": False, "caller_safe": True},
        {"owner_oid": 31, "owner_safe": True, "caller_safe": False},
    ],
)
async def test_ordinary_or_assumed_caller_cannot_supply_its_own_authentication(row):
    authenticate = AsyncMock()
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="publisher is unavailable"):
        await preparation.validate_entity_address_archive_preparation(
            _session(row),
            stored={},
            preparation={},
            authenticate_preparation=authenticate,
        )
    authenticate.assert_not_awaited()


def _proof():
    return {"inventory_sha256": "a" * 64, "frozen_owner_oid": 31, "builder_oid": 32}


def _stored():
    return {"restored": {"db_schema": "example"}, "destination_alias_receipt": {"local_generation": 7}}


def _seal(stored, proof):
    return {
        "contract": preparation.CONTRACT,
        "stored_sha256": preparation._digest(stored),
        "inventory_sha256": proof["inventory_sha256"],
        "protected_owner_oid": proof["frozen_owner_oid"],
        "builder_oid": proof["builder_oid"],
        "catalog_sha256": "b" * 64,
        "alias": stored["destination_alias_receipt"],
        "alias_catalog_sha256": "c" * 64,
    }


def _install_native_boundaries(monkeypatch):
    monkeypatch.setattr(preparation.destination.db, "bind_existing_session", _bound)
    monkeypatch.setattr(preparation.alias, "_lock_alias_relations", AsyncMock())
    monkeypatch.setattr(preparation, "_require_alias_authority", AsyncMock(return_value="c" * 64))
    monkeypatch.setattr(preparation, "_protected_catalog", AsyncMock(return_value="b" * 64))
    monkeypatch.setattr(preparation, "_move_to_destination", AsyncMock(return_value=("example", {})))


async def test_deferred_validation_scans_then_returns_the_same_private_candidate(monkeypatch):
    stored, proof, owner = _stored(), _proof(), object()
    _install_native_boundaries(monkeypatch)
    authenticate = AsyncMock(return_value=(proof, owner))
    monkeypatch.setattr(preparation, "_authenticated_preparation", authenticate)
    scan = AsyncMock()
    return_private = AsyncMock()
    publish = AsyncMock(side_effect=AssertionError("deferred validation must not publish"))
    monkeypatch.setattr(preparation.destination, "_validated_bound_destination", scan)
    monkeypatch.setattr(preparation.destination.restore, "_return_owned_relations", return_private)
    monkeypatch.setattr(preparation.destination.adoption, "adopt_prepared_entity_address_snapshot", publish)
    session, callback = object(), object()

    seal = await preparation.validate_entity_address_archive_preparation(
        session,
        stored=stored,
        preparation=proof,
        authenticate_preparation=callback,
    )

    authenticate.assert_awaited_once_with(session, stored, proof, None, callback)
    scan.assert_awaited_once_with(session, stored=stored)
    return_private.assert_awaited_once_with(session, owner=owner, db_schema="example", stage_names={})
    assert seal == _seal(stored, proof)
    publish.assert_not_awaited()


def _install_activation(monkeypatch):
    _install_native_boundaries(monkeypatch)
    stored, proof, owner = _stored(), _proof(), object()
    monkeypatch.setattr(preparation, "_authenticated_preparation", AsyncMock(return_value=(proof, owner)))
    receive = AsyncMock()
    monkeypatch.setattr(preparation.serving, "require_entity_address_receive_incumbent", receive)
    monkeypatch.setattr(preparation, "_require_alias_fence", AsyncMock())
    monkeypatch.setattr(preparation, "_lock_publication_state", AsyncMock())
    prepared = object()
    monkeypatch.setattr(preparation, "_prepared_adoption", Mock(return_value=prepared))
    geo = SimpleNamespace(stage_table_oid=17, projected_rows=4)
    stored["geo_assurance"] = {}
    monkeypatch.setattr(preparation.destination, "_validated_geo_preparation", Mock(return_value=geo))
    monkeypatch.setattr(preparation.destination, "_capture_geo_preparation", AsyncMock(return_value=geo))
    handback = AsyncMock()
    monkeypatch.setattr(preparation, "_restore_builder_owner", handback)
    publish = AsyncMock(return_value={"rows": 4})
    monkeypatch.setattr(preparation.destination.adoption, "adopt_prepared_entity_address_snapshot", publish)
    for module, name in (
        (preparation.destination, "_validated_bound_destination"),
        (preparation.destination.restore, "rehydrate_entity_address_archive_restore"),
        (preparation.destination, "_require_prepared_base_versions"),
        (preparation.alias, "capture_entity_address_alias_semantic_receipt"),
    ):
        monkeypatch.setattr(module, name, AsyncMock(side_effect=AssertionError("cutover must not rescan")))
    return stored, proof, prepared, receive, handback, publish


async def test_sealed_cutover_uses_complete_receive_fence_and_never_rescans(monkeypatch):
    stored, proof, prepared, receive, handback, publish = _install_activation(monkeypatch)
    expected, session, callbacks = {"complete": "receive-token"}, object(), object()
    result = await preparation.activate_validated_entity_address_archive_destination(
        session,
        stored=stored,
        validation=_seal(stored, proof),
        preparation=proof,
        expected_incumbent=expected,
        callbacks=callbacks,
        authenticate_preparation=object(),
    )
    receive.assert_awaited_once_with(session, schema_name="example", expected=expected)
    handback.assert_awaited_once()
    publish.assert_awaited_once_with(prepared, callbacks=callbacks)
    assert result == {"rows": 4}


@pytest.mark.parametrize("changed", ["stored", "inventory", "owner", "catalog", "incumbent", "geo", "alias"])
async def test_changed_authority_cannot_reach_ownership_handback_or_publication(monkeypatch, changed):
    stored, proof, _prepared, receive, handback, publish = _install_activation(monkeypatch)
    seal = _seal(stored, proof)
    if changed == "stored":
        stored["substituted"] = True
    if changed == "inventory":
        proof["inventory_sha256"] = "d" * 64
    if changed == "owner":
        proof["builder_oid"] += 1
    if changed == "catalog":
        preparation._protected_catalog.return_value = "e" * 64
    if changed == "incumbent":
        receive.side_effect = preparation.EntityAddressSnapshotDestinationError("incumbent changed")
    if changed == "geo":
        preparation.destination._capture_geo_preparation.return_value = object()
    if changed == "alias":
        stored["destination_alias_receipt"] = {"local_generation": 8}
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError):
        await preparation.activate_validated_entity_address_archive_destination(
            object(),
            stored=stored,
            validation=seal,
            preparation=proof,
            expected_incumbent={},
            callbacks=object(),
            authenticate_preparation=object(),
        )
    handback.assert_not_awaited()
    publish.assert_not_awaited()


async def test_late_native_failure_propagates_to_the_callers_transaction(monkeypatch):
    stored, proof, _prepared, _receive, handback, publish = _install_activation(monkeypatch)
    publish.side_effect = RuntimeError("late publication failure")
    with pytest.raises(RuntimeError, match="late publication failure"):
        await preparation.activate_validated_entity_address_archive_destination(
            object(),
            stored=stored,
            validation=_seal(stored, proof),
            preparation=proof,
            expected_incumbent={},
            callbacks=object(),
            authenticate_preparation=object(),
        )
    handback.assert_awaited_once()


async def test_private_return_preserves_seven_oids_and_the_evidence_sequence(monkeypatch):
    restore = preparation.destination.restore
    names = sorted(model.__tablename__ for model in restore._models())
    owner = SimpleNamespace(
        schema_name="private_archive", relation_oids=tuple((name, i + 100) for i, name in enumerate(names))
    )
    stage_name_by_table = {name: name + "_candidate" for name in names}
    monkeypatch.setattr(restore, "_require_empty_owned_schema", AsyncMock())
    monkeypatch.setattr(restore, "_verify_moved_stage_oids", AsyncMock())
    monkeypatch.setattr(restore, "verify_entity_address_archive_stage_ownership", AsyncMock())
    evidence_oid = dict(owner.relation_oids)["entity_address_evidence"]

    async def sequences(_session, oid):
        return ("entity_address_evidence_candidate_evidence_id_seq",) if oid == evidence_oid else ()

    monkeypatch.setattr(restore, "_owned_sequence_names", sequences)
    session = _session()
    await restore._return_owned_relations(session, owner=owner, db_schema="example", stage_names=stage_name_by_table)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert sum(" SET SCHEMA " in statement for statement in statements) == 7
    assert sum("ALTER TABLE" in statement and " RENAME TO " in statement for statement in statements) == 7
    assert sum("ALTER SEQUENCE" in statement for statement in statements) == 1
    assert not any("DROP" in statement for statement in statements)
    restore.verify_entity_address_archive_stage_ownership.assert_awaited_once_with(session, owner=owner)


@pytest.mark.parametrize("provisional,mode", [(False, "SHARE"), (True, "ACCESS SHARE")])
async def test_alias_capture_keeps_default_writer_order_and_provisional_read_lock(provisional, mode):
    session = _session()
    await preparation.alias._lock_alias_relations(session, "example", provisional=provisional)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert "pg_advisory_xact_lock" in statements[0]
    assert (
        statements[1] == f'LOCK TABLE "example"."address_alias_state_v1", "example"."address_alias_v1" IN {mode} MODE'
    )


@pytest.mark.parametrize("preserve_private_stage", [False, True])
async def test_only_preserved_private_preparation_selects_provisional_capture(monkeypatch, preserve_private_stage):
    binding = AsyncMock(side_effect=RuntimeError("capture reached"))
    monkeypatch.setattr(preparation.destination, "_destination_alias_binding", binding)
    with pytest.raises(RuntimeError, match="capture reached"):
        await preparation.destination._prepare_bound_destination(
            object(),
            {"db_schema": "example", "source_alias_receipt": {}},
            preserve_private_stage=preserve_private_stage,
        )
    assert binding.await_args.kwargs["provisional"] is preserve_private_stage


@pytest.mark.parametrize("preserve_private_stage", [False, True])
async def test_invalid_source_generation_precedes_alias_reads_and_candidate_changes(
    monkeypatch, preserve_private_stage
):
    binding = AsyncMock()
    monkeypatch.setattr(preparation.destination, "_destination_alias_binding", binding)
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="source serving generation is invalid"):
        await preparation.destination._prepare_bound_destination(
            object(),
            {"source_serving_generation": {"invalid": True}},
            preserve_private_stage=preserve_private_stage,
        )
    binding.assert_not_awaited()


@pytest.mark.parametrize("changed", ["generation", "content"])
async def test_stale_or_altered_provisional_alias_receipt_cannot_be_sealed(monkeypatch, changed):
    _install_native_boundaries(monkeypatch)
    stored, proof, owner = _stored(), _proof(), object()
    monkeypatch.setattr(preparation, "_authenticated_preparation", AsyncMock(return_value=(proof, owner)))
    expected = preparation.alias.EntityAddressAliasSemanticReceipt(2, 1, 7, 2, "a" * 64)
    current = preparation.alias.EntityAddressAliasSemanticReceipt(
        2,
        1,
        8 if changed == "generation" else 7,
        2,
        "b" * 64 if changed == "content" else "a" * 64,
    )
    monkeypatch.setattr(
        preparation.destination, "_validated_destination_metadata", Mock(return_value=(expected, expected, {}))
    )
    binding = AsyncMock(return_value=(expected, current))
    monkeypatch.setattr(preparation.destination, "_destination_alias_binding", binding)
    scan = AsyncMock(side_effect=AssertionError("stale aliases must fail before candidate validation"))
    monkeypatch.setattr(preparation.destination.restore, "rehydrate_entity_address_archive_restore", scan)
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="destination alias generation changed"):
        await preparation.validate_entity_address_archive_preparation(
            object(),
            stored=stored,
            preparation=proof,
            authenticate_preparation=object(),
        )
    assert "provisional" not in binding.await_args.kwargs
    scan.assert_not_awaited()


async def test_ordinary_owned_alias_surface_is_not_a_publisher_prerequisite(monkeypatch):
    session = _session()
    session.execute.return_value.mappings.return_value.all.return_value = [
        {"relname": name, "relowner": 32} for name in ("address_alias_state_v1", "address_alias_v1")
    ]
    monkeypatch.setattr(preparation, "_publisher_authority", AsyncMock(return_value=31))
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="relation authority is unavailable"):
        await preparation.require_entity_address_archive_publisher(session, db_schema="example")


@pytest.mark.parametrize("sequence", [False, True])
async def test_effective_ordinary_mutation_is_not_accepted_for_frozen_objects(sequence):
    session = _session()
    session.scalar.return_value = True
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="ordinary mutation is available"):
        await preparation._require_no_untrusted_mutation(session, [17], 31, sequence=sequence)
    statement = str(session.scalar.await_args.args[0])
    assert "has_sequence_privilege" in statement if sequence else "has_any_column_privilege" in statement
