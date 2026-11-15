# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_dependency_bindings as dependencies
from process import entity_address_snapshot_preparation as preparation
from tests.test_geo_assurance_dependency_bindings import _example_bindings


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
    session = _session(row)
    session.in_transaction = lambda: False
    authenticate = AsyncMock()
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="caller transaction"):
        await preparation.validate_entity_address_archive_preparation(
            session, stored={}, preparation={}, authenticate_preparation=authenticate
        )
    authenticate.assert_not_awaited()


def _proof(owner=None):
    from tests.test_entity_address_preparation_catalog import _inventory

    proof_by_field = _inventory()[1]
    if owner is not None:
        inventory_by_field = {
            "schema_name": owner.schema_name,
            "relations": [{"table_name": name} for name in owner.relation_names],
            "sequences": [
                {
                    "sequence_name": preparation._EVIDENCE_SEQUENCE,
                    "owner_table": "entity_address_evidence",
                    "owner_column": "evidence_id",
                }
            ],
        }
        proof_by_field.update(inventory=inventory_by_field, inventory_sha256=preparation._digest(inventory_by_field))
    return proof_by_field


def _selected():
    return {"contract": dependencies.PUBLISHER_SELECTED_INPUTS_CONTRACT, "dependency_bindings": _example_bindings()}


@pytest.mark.parametrize(
    "change", ["map_oid", "map_filenode", "map_missing", "context_map", "digest", "geo_digest", "missing"]
)
def test_selected_input_seal_rejects_changed_map_or_missing_independent_evidence(change):
    selected = _selected()
    prepared_by_field = {
        "restored": {
            "db_schema": "mrf",
            "context": {
                "dependency_bindings": deepcopy(selected["dependency_bindings"]),
                "publisher_selected_inputs": selected,
            },
        },
        "geo_assurance": {"publisher_selected_inputs_sha256": preparation._digest(selected)},
    }
    seal_by_field = {"publisher_selected_inputs_sha256": preparation._digest(selected)}
    preparation._require_selected_inputs_seal(prepared_by_field, seal_by_field)
    match change:
        case "map_oid":
            selected["dependency_bindings"]["mrf.npi_address"]["table_name"] += "_changed"
        case "map_filenode":
            selected["dependency_bindings"]["mrf.npi_address"]["schema_name"] += "_changed"
        case "map_missing":
            selected["dependency_bindings"].pop("tiger.zcta5")
        case "context_map":
            prepared_by_field["restored"]["context"]["dependency_bindings"]["tiger.zcta5"]["table_name"] += "_changed"
        case "digest":
            seal_by_field["publisher_selected_inputs_sha256"] = "f" * 64
        case "geo_digest":
            prepared_by_field["geo_assurance"]["publisher_selected_inputs_sha256"] = "f" * 64
        case "missing":
            prepared_by_field["restored"]["context"].pop("publisher_selected_inputs")
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError):
        preparation._require_selected_inputs_seal(prepared_by_field, seal_by_field)


@pytest.mark.parametrize("matches", [True, False])
async def test_selected_heap_lock_checks_native_identities_before_projection(matches):
    session = _session()
    session.scalar.return_value = matches
    if matches:
        selected = await dependencies.lock_publisher_selected_inputs(session, "mrf", _selected())
        assert selected == dependencies.publisher_selected_input_names("mrf", _selected())
    else:
        with pytest.raises(RuntimeError, match="changed"):
            await dependencies.lock_publisher_selected_inputs(session, "mrf", _selected())
    sql = str(session.execute.await_args.args[0])
    assert sql.endswith(" NOWAIT") and sql.count("ONLY") == 6
    for entry in _example_bindings().values():
        relation = f'"{entry["schema_name"]}"."{entry["table_name"]}"'
        assert relation in str(session.scalar.await_args.args[0])


@pytest.mark.parametrize("changed", [False, True])
def test_selected_context_rehydration_is_closed_and_keeps_the_original_map(changed):
    context_by_field = {
        "address_alias_generation": 1,
        "stage_persistence": "p",
        "snapshot_contract": preparation.destination.restore.CONTRACT,
        "dependency_bindings": _example_bindings(),
        "publisher_selected_inputs": _selected(),
    }
    if changed:
        context_by_field["dependency_bindings"]["mrf.npi_address"]["table_name"] += "_changed"
        with pytest.raises(preparation.destination.restore.EntityAddressSnapshotRestoreError):
            preparation.destination.restore._rehydrated_context(context_by_field, db_schema="mrf")
    else:
        actual = preparation.destination.restore._rehydrated_context(context_by_field, db_schema="mrf")
        assert actual["publisher_selected_inputs"] == _selected()
        actual["publisher_selected_inputs"]["dependency_bindings"]["mrf.npi_address"]["relation_oid"] += 100
        assert context_by_field["publisher_selected_inputs"] == _selected()


@pytest.mark.parametrize("digest", [None, "a" * 64])
def test_geo_receipt_preserves_ordinary_shape_and_selected_digest(digest):
    native = preparation.destination
    signature_entries = tuple(
        (name, value["relation_oid"], value["relfilenode"]) for name, value in sorted(_example_bindings().items())
    )
    receipt = native.EntityAddressGeoAssurancePreparation(7, 10, signature_entries, digest)
    stored = receipt.as_dict()
    assert ("publisher_selected_inputs_sha256" in stored) is (digest is not None)
    assert native._validated_geo_preparation(stored, db_schema="mrf") == receipt


@pytest.mark.parametrize("mutation", ["builder_owner", "schema_oid", "missing_auxiliary", "inventory_digest"])
def test_v2_loaded_custody_requires_exact_original_eight_oid_inventory(mutation):
    from tests.test_entity_address_archive_ownership_guards import _set_owner

    owner = _set_owner()
    inventory_by_field = {
        "schema_name": owner.schema_name,
        "schema_oid": owner.schema_oid,
        "relations": [{"table_name": name} for name in owner.relation_names],
        "sequences": [
            {
                "sequence_name": preparation._EVIDENCE_SEQUENCE,
                "owner_table": "entity_address_evidence",
                "owner_column": "evidence_id",
            }
        ],
    }
    proof_by_field = {
        **_proof(),
        "state": "handed_off",
        "inventory": inventory_by_field,
        "inventory_sha256": preparation._digest(inventory_by_field),
    }
    preparation._require_loaded_inventory(proof_by_field, owner, 31)
    if mutation == "builder_owner":
        proof_by_field["state"] = "loading"
    elif mutation == "schema_oid":
        inventory_by_field["schema_name"] = "different"
    elif mutation == "missing_auxiliary":
        inventory_by_field["relations"].pop()
    else:
        proof_by_field["inventory_sha256"] = "b" * 64
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError):
        preparation._require_loaded_inventory(proof_by_field, owner, 31)


async def test_loaded_preparation_refuses_frozen_input_before_candidate_writes(monkeypatch):
    """A frozen candidate cannot reenter the mutable producer transform phase."""
    from tests.test_address_native_transition_refusals import _loaded_inputs

    owner, semantic, aliases, destination_options = _loaded_inputs()
    session = _session()
    authenticate = AsyncMock(return_value={**_proof(owner), "state": "frozen"})
    catalogue = AsyncMock()
    transforms = AsyncMock()
    monkeypatch.setattr(preparation, "_protected_catalog", catalogue)
    monkeypatch.setattr(preparation, "_prepare_loaded_sets", transforms)
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="handed-off operation differs"):
        await preparation.prepare_loaded_entity_address_archive_destination(
            session,
            owner=owner,
            semantic_receipt=semantic,
            source_alias_receipt=aliases,
            destination=destination_options,
            authenticate_preparation=authenticate,
        )
    authenticate.assert_awaited_once_with(session)
    catalogue.assert_not_awaited()
    transforms.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("selected", [None, _selected()])
async def test_v2_loaded_candidate_completes_all_indexes_before_any_set_validation(monkeypatch, selected):
    native, events = preparation.destination, []
    owner, semantic, source_alias, destination_alias, geo = (
        SimpleNamespace(schema_name="private"),
        object(),
        object(),
        object(),
        object(),
    )

    async def mark(label, *args, **kwargs):
        events.append(label)

    monkeypatch.setattr(native, "_remap_base_versions", AsyncMock(return_value=(object(), semantic)))
    monkeypatch.setattr(native.restore, "_reset_restored_evidence_sequence", AsyncMock())
    stage_names = native.restore._stage_plan(db_schema="example", import_date="20261005")[2]
    stage_relations = tuple(sorted(("example", name) for name in stage_names.values()))
    monkeypatch.setattr(native.restore, "_move_owned_relations", AsyncMock(return_value=stage_relations))
    monkeypatch.setattr(native.restore, "_return_owned_relations", AsyncMock())
    bindings = None if selected is None else selected["dependency_bindings"]
    monkeypatch.setattr(preparation, "_project_loaded_geo", AsyncMock(return_value=(geo, bindings)))
    monkeypatch.setattr(
        native.restore, "complete_entity_address_archive_restore", lambda *a, **kw: mark("indexes", *a, **kw)
    )
    monkeypatch.setattr(native.restore, "_actual_receipt", lambda *a, **kw: mark("accounting", *a, **kw))
    monkeypatch.setattr(
        preparation.alias,
        "require_matching_entity_address_alias_authority",
        lambda *a, **kw: mark("alias_set", *a, **kw),
    )

    async def validated(*args, **kwargs):
        events.append("native_set")
        return SimpleNamespace(context={}, stage_cls=SimpleNamespace(__tablename__="entity_address_unified_stage"))

    monkeypatch.setattr(native.adoption, "prepare_completed_entity_address_snapshot_adoption", validated)
    monkeypatch.setattr(native, "capture_entity_address_stage_integrity_receipt", AsyncMock())
    monkeypatch.setattr(native, "_prepared_restore_receipt", Mock(return_value=object()))
    destination_options_by_field = {"db_schema": "example", "import_date": "20261005"}
    original = deepcopy(destination_options_by_field)
    session = object()
    await preparation._prepare_loaded_sets(
        session,
        owner,
        semantic,
        source_alias,
        destination_alias,
        destination_options_by_field,
        selected,
    )
    assert events == ["indexes", "accounting", "alias_set", "native_set"]
    native.restore._reset_restored_evidence_sequence.assert_awaited_once_with(session, owner)
    assert destination_options_by_field == original
    projected_options = preparation._project_loaded_geo.await_args.args[2]
    assert projected_options.get("dependency_bindings") == bindings
    context = native._prepared_restore_receipt.call_args.kwargs["prepared"].context
    assert context.get("publisher_selected_inputs") == selected


@pytest.mark.parametrize("mutation", ["extra_field", "legacy_contract", "changed_destination"])
def test_v2_input_marker_is_closed_and_unchanged(mutation):
    from tests.test_entity_address_archive_ownership_guards import _set_owner
    from tests.test_entity_address_archive_receipt_guards import _set_receipt

    owner = _set_owner()
    proof = _proof(owner)
    semantic = preparation.destination.validate_entity_address_archive_receipt(_set_receipt())
    source_alias = preparation.alias.validate_entity_address_alias_semantic_receipt(
        {
            "contract": preparation.alias.SET_CONTRACT,
            "receipt_version": preparation.alias.SET_CONTRACT,
            "alias_schema_version": 2,
            "active_ruleset_version": 1,
            "local_generation": 3,
            "active_alias_count": 1,
        }
    )
    marker_by_field = preparation._loaded_input(
        owner, semantic, source_alias, {"db_schema": "example", "import_date": "20261005"}
    )
    stored_by_field = {"contract": semantic.contract, **marker_by_field}
    prepared_by_field = {
        "restored": {"db_schema": "example"},
        "source_semantic_receipt": semantic.as_dict(),
        "source_alias_receipt": source_alias.as_dict(),
        "destination_alias_receipt": source_alias.as_dict(),
    }
    seal_by_field = {
        **_seal(prepared_by_field, proof, owner),
        "contract": preparation.SET_CONTRACT,
        "input_sha256": preparation._digest(marker_by_field),
    }
    preparation._require_set_input(stored_by_field, seal_by_field, prepared_by_field, proof, owner)
    if mutation == "extra_field":
        stored_by_field["extra"] = True
    elif mutation == "legacy_contract":
        stored_by_field["contract"] = "entity_address_unified.postgres.v1"
    else:
        stored_by_field["destination"] = {"db_schema": "other", "import_date": "20261005"}
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError):
        preparation._require_set_input(stored_by_field, seal_by_field, prepared_by_field, proof, owner)


def _stored():
    from tests.test_entity_address_preparation_catalog import _inventory

    owner, _ = _inventory()
    return {
        "restored": {"db_schema": "example", "ownership": owner.as_dict()},
        "destination_alias_receipt": {"local_generation": 7},
    }


def _seal(stored, proof, owner=None):
    from tests.test_entity_address_preparation_catalog import _inventory

    if owner is None:
        owner, _ = _inventory()
    return {
        "contract": preparation.NAMED_CONTRACT,
        "stored_sha256": preparation._digest(stored),
        "inventory": preparation._logical_inventory(proof["inventory"], owner),
        "alias": stored["destination_alias_receipt"],
    }


def _install_native_boundaries(monkeypatch):
    monkeypatch.setattr(preparation.destination.db, "bind_existing_session", _bound)
    monkeypatch.setattr(preparation.alias, "_lock_alias_relations", AsyncMock())
    monkeypatch.setattr(preparation, "_require_alias_authority", AsyncMock(return_value="c" * 64))
    monkeypatch.setattr(preparation, "_protected_catalog", AsyncMock(return_value="b" * 64))
    monkeypatch.setattr(preparation, "_move_to_destination", AsyncMock(return_value=("example", {})))


async def test_deferred_validation_scans_then_returns_the_same_private_candidate(monkeypatch):
    from tests.test_entity_address_preparation_catalog import _inventory

    stored, proof = _stored(), _proof()
    owner, _ = _inventory()
    _install_native_boundaries(monkeypatch)
    authenticate = AsyncMock(return_value=(proof, owner))
    monkeypatch.setattr(preparation, "_authenticated_preparation", authenticate)
    scan = AsyncMock()
    return_private = AsyncMock()
    publish = AsyncMock(side_effect=AssertionError("deferred validation must not publish"))
    monkeypatch.setattr(preparation, "_validate_private_candidate_contents", scan)
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
    scan.assert_awaited_once_with(session, stored, owner)
    return_private.assert_not_awaited()
    preparation._protected_catalog.assert_awaited_once_with(session, proof, owner, read_only=True)
    assert seal == _seal(stored, proof)
    publish.assert_not_awaited()


def _install_activation(monkeypatch):
    _install_native_boundaries(monkeypatch)
    from tests.test_entity_address_preparation_catalog import _inventory

    stored, proof = _stored(), _proof()
    owner, _ = _inventory()
    monkeypatch.setattr(preparation, "_authenticated_preparation", AsyncMock(return_value=(proof, owner)))
    receive = AsyncMock()
    monkeypatch.setattr(preparation.serving, "require_entity_address_receive_incumbent", receive)
    monkeypatch.setattr(preparation, "_require_alias_fence", AsyncMock())
    monkeypatch.setattr(preparation, "_lock_publication_state", AsyncMock())
    prepared = SimpleNamespace(context={}, db_schema="example", stage_cls=SimpleNamespace(__tablename__="stage"))
    monkeypatch.setattr(preparation, "_prepared_adoption", Mock(return_value=prepared))
    signature_entries = tuple(
        (name.replace("mrf.", "example.", 1), entry["schema_name"], entry["table_name"])
        for name, entry in sorted(_example_bindings().items())
    )
    geo = preparation.destination.EntityAddressGeoAssurancePreparation(
        None, 4, signature_entries, stage_relation=("example", "stage")
    )
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
    assert result["rows"] == 4
    assert result["publication"]["contract"] == "entity-address-table-publication.v3"
    assert result["publication"]["alias_authority"] is None
    assert len(result["publication"]["relations"]) == 7


@pytest.mark.parametrize("changed", ["stored", "inventory", "owner", "catalog", "incumbent", "geo", "alias"])
async def test_changed_authority_cannot_reach_ownership_handback_or_publication(monkeypatch, changed):
    stored, proof, _prepared, receive, handback, publish = _install_activation(monkeypatch)
    seal = _seal(stored, proof)
    if changed == "stored":
        stored["substituted"] = True
    if changed == "inventory":
        proof["inventory"]["relations"].pop()
    if changed == "owner":
        proof["inventory"]["schema_name"] = "different"
    if changed == "catalog":
        preparation._protected_catalog.side_effect = preparation.EntityAddressSnapshotDestinationError(
            "candidate layout changed"
        )
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
    owner = SimpleNamespace(schema_name="private_archive", relation_names=tuple(names))
    stage_name_by_table = {name: name + "_candidate" for name in names}
    monkeypatch.setattr(restore, "_require_empty_owned_schema", AsyncMock())
    monkeypatch.setattr(restore, "_verify_moved_stage_relations", AsyncMock())
    monkeypatch.setattr(restore, "verify_entity_address_archive_stage_ownership", AsyncMock())
    handles_by_stage = {stage: index for index, stage in enumerate(stage_name_by_table.values(), 1)}
    monkeypatch.setattr(
        restore,
        "_local_relation_handle",
        AsyncMock(side_effect=lambda _session, _schema, table: handles_by_stage[table]),
    )
    evidence_handle = handles_by_stage["entity_address_evidence_candidate"]

    async def sequences(_session, handle):
        return ("entity_address_evidence_candidate_evidence_id_seq",) if handle == evidence_handle else ()

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
    from tests.test_entity_address_preparation_catalog import _inventory

    stored, proof = _stored(), _proof()
    owner, _ = _inventory()
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
    assert binding.await_args.kwargs["provisional"] is True
    scan.assert_not_awaited()


async def test_ordinary_owned_alias_surface_is_not_a_publisher_prerequisite(monkeypatch):
    session = _session()
    read = AsyncMock(return_value=(2, 1, 7))
    monkeypatch.setattr(preparation.alias, "_alias_state", read)
    await preparation.require_entity_address_archive_publisher(session, db_schema="example")
    read.assert_awaited_once_with(session, "example")
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("sequence", [False, True])
async def test_effective_ordinary_mutation_is_not_accepted_for_frozen_objects(monkeypatch, sequence):
    session = _session()
    failure = PermissionError("native access denied")
    monkeypatch.setattr(preparation.alias, "_alias_state", AsyncMock(side_effect=failure))
    with pytest.raises(PermissionError, match="native access denied"):
        await preparation.require_entity_address_archive_publisher(session, db_schema="example")
    session.execute.assert_not_awaited()


async def test_loaded_contents_use_complete_existing_codec_and_restore_settings(monkeypatch):
    """Read all eight candidate relations without leaking normalized session settings."""
    from process import entity_address_snapshot_receipt as receipts
    from tests.test_entity_address_archive_ownership_guards import _set_owner

    owner = _set_owner()
    rollback = AsyncMock()

    @asynccontextmanager
    async def settings():
        yield SimpleNamespace(rollback=rollback)

    session = SimpleNamespace(begin_nested=settings)
    normalize = AsyncMock()
    rows = AsyncMock(return_value=(1, "a" * 64))
    monkeypatch.setattr(receipts, "_normalize_receipt_session", normalize)
    monkeypatch.setattr(receipts, "_row_identity", rows)
    contents = await preparation._loaded_candidate_contents(session, owner)
    assert contents == [{"relation_name": name, "rows": 1, "sha256": "a" * 64} for name in owner.relation_names]
    assert [call.args for call in rows.await_args_list] == [
        (session, owner.schema_name, name) for name in owner.relation_names
    ]
    normalize.assert_awaited_once_with(session, owner.schema_name)
    rollback.assert_awaited_once_with()


@pytest.mark.parametrize("damage", ["missing", "extra", "reordered", "duplicate", "boolean_rows", "bad_digest"])
def test_loaded_contents_require_complete_typed_commitments(damage):
    """Shape or type drift cannot substitute a counts-only or incomplete commitment."""
    from tests.test_entity_address_archive_ownership_guards import _set_owner

    owner = _set_owner()
    contents = [{"relation_name": name, "rows": 1, "sha256": "a" * 64} for name in owner.relation_names]
    assert preparation._validated_loaded_contents(contents, owner) == contents
    match damage:
        case "missing":
            contents.pop()
        case "extra":
            contents[0]["other"] = "unrecognized"
        case "reordered":
            contents.reverse()
        case "duplicate":
            contents[-1]["relation_name"] = contents[0]["relation_name"]
        case "boolean_rows":
            contents[0]["rows"] = True
        case "bad_digest":
            contents[0]["sha256"] = "a" * 63
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="frozen candidate contents differ"):
        preparation._validated_loaded_contents(contents, owner)


def test_typed_named_validation_requires_contents_and_preserves_legacy_raw():
    """New typed validation cannot borrow a hashless historical envelope."""
    prepared_by_field = {"contract": preparation.destination.SET_CONTRACT}
    validation_by_field = {
        "contract": preparation.NAMED_CONTRACT,
        "prepared": prepared_by_field,
        "input_sha256": "a" * 64,
        "stored_sha256": "b" * 64,
        "inventory": {},
        "alias": {},
    }
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="validation differs"):
        preparation._validated_set_prepared(validation_by_field)
    legacy_by_field = {name: value for name, value in validation_by_field.items() if name != "inventory"}
    legacy_by_field.update(
        contract=preparation.SET_CONTRACT,
        inventory_sha256="c" * 64,
        protected_owner_oid="opaque former owner",
        builder_oid="opaque former builder",
        catalog_sha256="d" * 64,
        alias_catalog_sha256="e" * 64,
    )
    raw = deepcopy(legacy_by_field)
    assert preparation._validated_set_prepared(legacy_by_field) == prepared_by_field
    assert legacy_by_field == raw and "contents" not in legacy_by_field
