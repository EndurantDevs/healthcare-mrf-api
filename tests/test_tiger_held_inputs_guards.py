# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic catalog observations exercise immutable TIGER selection boundaries."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as archive
from process import tiger_held_inputs as held
from process import tiger_snapshot_inheritance as inherited
from tests.test_tiger_captured_epoch_guards import _graph_rows

_EPOCH_ID = "550e8400-e29b-41d4-a716-446655440000"
_OWNER = 90


def _result(*rows):
    return SimpleNamespace(
        mappings=lambda: SimpleNamespace(all=lambda: list(rows), one_or_none=lambda: rows[0] if rows else None)
    )


def _session(*rows, scalars=(True,)):
    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=_result(*rows)),
        scalar=AsyncMock(side_effect=scalars),
    )


def _inventory():
    return {
        "database_oid": 77,
        "relations": [
            {
                "relation_oid": oid,
                "owner_oid": _OWNER,
                "schema_oid": 200,
                "schema_owner_oid": _OWNER,
                "relfilenode": oid + 1000,
                "schema_name": "reference_family_archive_" + UUID(_EPOCH_ID).hex,
                "relation_name": name,
            }
            for oid, name in ((501, "zip_state"), (502, "zcta5"))
        ],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("missing", "shape", None))
async def test_named_catalog_requires_native_inert_shape_and_read_permission(change):
    row_by_field = {
        "schema_name": "hp_snapshot_retention",
        "relation_name": "tiger_captured_epoch",
        "valid": change != "shape",
    }
    session = _session(*(() if change == "missing" else (row_by_field,)), scalars=())
    if change is None:
        assert await held._protected_relation(session, held._CAPTURE, catalog=True) == {
            key: row_by_field[key] for key in ("schema_name", "relation_name")
        }
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._protected_relation(session, held._CAPTURE, catalog=True)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0] == f"LOCK TABLE ONLY {held._CAPTURE} IN ACCESS SHARE MODE NOWAIT"
    assert "pg_rewrite" in statements[1] and "pg_trigger" in statements[1]
    assert not any(word in statements[1] for word in ("owner", "aclexplode", "pg_roles", "filenode"))


@pytest.mark.asyncio
async def test_native_reader_permission_denial_propagates():
    session = _session()
    session.execute.side_effect = PermissionError("native SELECT denied")
    with pytest.raises(PermissionError, match="native SELECT"):
        await held._protected_relation(session, held._CAPTURE)


@pytest.mark.asyncio
@pytest.mark.parametrize("locked", (False, None))
async def test_selection_refuses_busy_or_unavailable_advisory_fence(locked):
    session = _session(scalars=(locked,))
    with pytest.raises(RuntimeError, match="protected TIGER input changed"):
        await held._scope(session, "tiger-captured-epoch:" + _EPOCH_ID)
    assert session.scalar.await_args.args[1] == {"scope": "snapshot-retention:tiger-captured-epoch:" + _EPOCH_ID}


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", ("absent", "canonical", "ambiguous", "installed"))
async def test_current_selection_never_falls_back_from_ambiguous_or_installed_custody(monkeypatch, selection):
    inventory = _inventory()
    if selection == "canonical":
        for relation in inventory["relations"]:
            relation["schema_name"] = "tiger"
    generation_by_field = {"inventory": inventory}
    rows = [] if selection == "absent" else [generation_by_field] * (2 if selection == "ambiguous" else 1)
    session = _session(*rows)
    monkeypatch.setattr(held, "_protected_relation", AsyncMock(return_value={"owner_oid": _OWNER}))
    source = AsyncMock()
    monkeypatch.setattr(held, "_require_captured_selection", source)
    monkeypatch.setattr(held, "_source_selection_relations", lambda: None)
    installed = AsyncMock()
    checked_inventory = AsyncMock(return_value=inventory)
    monkeypatch.setattr(held, "_require_installed_selection", installed)
    monkeypatch.setattr(held, "_require_inventory", checked_inventory)
    if selection == "ambiguous":
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held.selected_tiger_inventory(session)
    elif selection == "installed":
        assert await held.selected_tiger_inventory(session) == inventory
        installed.assert_awaited_once_with(session, generation_by_field)
        checked_inventory.assert_awaited_once_with(session, inventory)
    else:
        assert await held.selected_tiger_inventory(session) is None
    source.assert_not_awaited()
    if selection != "installed":
        installed.assert_not_awaited()
        checked_inventory.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", ("absent", "ambiguous", "legacy", "noncanonical-legacy", "captured"))
async def test_source_binding_requires_one_current_scope_and_captured_authority(monkeypatch, selection):
    inventory = _inventory()
    family_by_field = {"publication_authority": "captured-epoch" if selection == "captured" else "manual-only"}
    selection_by_field = {
        "node_id": "synthetic-node",
        "inventory": inventory,
        "manifest": {"adapter_metadata": {"family": family_by_field}},
    }
    if selection == "legacy":
        for relation in inventory["relations"]:
            relation["schema_name"] = "tiger"
    rows = [] if selection == "absent" else [selection_by_field] * (2 if selection == "ambiguous" else 1)
    session = _session(*rows, scalars=(None, 301, 302))
    monkeypatch.setattr(held, "_source_selection_relations", lambda: ('"example"."binding"', '"example"."package"'))
    capture = AsyncMock(return_value=inventory)
    monkeypatch.setattr(held, "_require_captured_selection", capture)
    if selection in {"ambiguous", "noncanonical-legacy"}:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held.selected_tiger_inventory(session)
    else:
        assert await held.selected_tiger_inventory(session) == (inventory if selection == "captured" else None)
    if selection == "captured":
        capture.assert_awaited_once_with(session, selection_by_field, family_by_field)
    else:
        capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_binding_missing_relation_cannot_supply_authority(monkeypatch):
    session = _session(scalars=(None, None))
    monkeypatch.setattr(held, "_source_selection_relations", lambda: ('"example"."binding"', '"example"."package"'))
    with pytest.raises(RuntimeError, match="protected TIGER input changed"):
        await held.selected_tiger_inventory(session)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection",
    (
        "installed",
        "both_nodes",
        "canonical",
        "foreign_installed",
        "foreign_only",
        "duplicate_installed",
        "duplicate_source",
    ),
)
async def test_tiger_selection_is_node_local(monkeypatch, selection):
    """Only the local installation preempts a local capture; foreign scopes stay invisible."""
    inventory = _inventory()
    generation_by_field = {"node_id": "source", "inventory": deepcopy(inventory)}
    current_rows = [generation_by_field]
    if selection in {"both_nodes", "foreign_installed", "foreign_only"}:
        current_rows.append({**generation_by_field, "node_id": "foreign"})
    if selection in {"foreign_installed", "foreign_only", "duplicate_source"}:
        current_rows.pop(0)
    if selection == "duplicate_installed":
        current_rows *= 2
    if selection == "canonical":
        for relation in generation_by_field["inventory"]["relations"]:
            relation["schema_name"] = "tiger"
    family_by_field = {"publication_authority": "captured-epoch"}
    capture_by_field = {
        "node_id": "source",
        "inventory": inventory,
        "manifest": {"adapter_metadata": {"family": family_by_field}},
    }
    source_rows = [capture_by_field, {**capture_by_field, "node_id": "foreign"}]
    if selection == "foreign_only":
        source_rows.pop(0)
    if selection == "duplicate_source":
        source_rows.append(capture_by_field)

    async def execute(statement, parameters):
        is_current = held._CURRENT in str(statement)
        alias = "c" if is_current else "b"
        assert f"(CAST(:node_id AS text) IS NULL OR {alias}.node_id=:node_id)" in str(statement)
        assert parameters == {"node_id": "source"}
        return _result(
            *(entry for entry in (current_rows if is_current else source_rows) if entry["node_id"] == "source")
        )

    session = _session(scalars=(301, 302, 303))
    session.execute = execute
    monkeypatch.setattr(held, "_source_selection_relations", lambda: ('"example"."binding"', '"example"."package"'))
    monkeypatch.setattr(held, "_protected_relation", AsyncMock(return_value={"owner_oid": _OWNER}))
    installed, captured = AsyncMock(), AsyncMock(return_value=inventory)
    monkeypatch.setattr(held, "_require_installed_selection", installed)
    monkeypatch.setattr(held, "_require_inventory", AsyncMock(return_value=inventory))
    monkeypatch.setattr(held, "_require_captured_selection", captured)
    if selection.startswith("duplicate"):
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held.selected_tiger_inventory(session, node_id="source")
    else:
        expected = None if selection in {"canonical", "foreign_only"} else inventory
        assert await held.selected_tiger_inventory(session, node_id="source") == expected
    assert installed.await_count == int(selection in {"installed", "both_nodes"})
    assert captured.await_count == int(selection == "foreign_installed")
    if captured.await_count:
        captured.assert_awaited_once_with(session, capture_by_field, family_by_field)


def _captured_selection():
    graph_by_field = {"contract": "tiger.closed-inheritance-graph.v1", "database_oid": 77, "relations": _graph_rows()}
    origin_by_field = {
        "contract": "tiger.captured-origin.v1",
        "epoch_id": _EPOCH_ID,
        "source_graph_sha256": held._digest(graph_by_field),
    }
    receipts = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 1)
        for model in archive.reference_family_spec("tiger").model_types
    )
    metadata, metadata_digest = archive._source_metadata(origin_by_field)
    family = archive.ReferenceFamilyManifest(
        "tiger",
        receipts,
        metadata,
        metadata_digest,
        archive._schema_digest(receipts),
        publication_authority="captured-epoch",
        source_capture_contract=archive.CAPTURED_TIGER_CONTRACT,
    ).as_dict()
    epoch_by_field = {
        "epoch_id": UUID(_EPOCH_ID),
        "node_id": "synthetic-node",
        "state": "retained",
        "origin_kind": "captured",
        "authority": {"manifest": family},
        "source_graph": graph_by_field,
        "inventory": _inventory(),
    }
    _register_epoch(epoch_by_field)
    return (
        epoch_by_field,
        {"node_id": epoch_by_field["node_id"], "inventory": deepcopy(epoch_by_field["inventory"])},
        family,
    )


def _register_epoch(epoch):
    epoch["registration_sha256"] = held._digest(
        {
            **{key: epoch[key] for key in ("node_id", "authority", "source_graph", "inventory")},
            "epoch_id": str(epoch["epoch_id"]),
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "missing", "retired", "origin", "digest", "graph", "manifest", "inventory"))
async def test_captured_selection_rechecks_registration_graph_and_bound_package(monkeypatch, change):
    epoch, selected, family = _captured_selection()
    if change in {"retired", "origin", "digest"}:
        key, value = {
            "retired": ("state", "retired"),
            "origin": ("origin_kind", "installed"),
            "digest": ("registration_sha256", "b" * 64),
        }[change]
        epoch[key] = value
    elif change == "graph":
        epoch["source_graph"]["database_oid"] += 1
        _register_epoch(epoch)
    elif change == "manifest":
        epoch["authority"] = {"manifest": {**family, "schema_sha256": "b" * 64}}
        _register_epoch(epoch)
    elif change == "inventory":
        selected["inventory"]["relations"][0]["schema_name"] = "reference_family_archive_" + "f" * 32
    session = _session(*(() if change == "missing" else (epoch,)))
    monkeypatch.setattr(held, "_protected_relation", AsyncMock(return_value={"owner_oid": _OWNER}))
    monkeypatch.setattr(held, "_captured_schema_identity", AsyncMock(return_value="a" * 64))
    inventory_check = AsyncMock(return_value=epoch["inventory"])
    monkeypatch.setattr(held, "_require_inventory", inventory_check)
    if change in {None, "digest", "graph"}:
        assert await held._require_captured_selection(session, selected, family) == epoch["inventory"]
        inventory_check.assert_awaited_once_with(session, epoch["inventory"])
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_captured_selection(session, selected, family)
        inventory_check.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "legacy", "missing", "namespace", "canonical", "different_name"))
async def test_inventory_requires_exact_two_named_leaves_and_ignores_old_physical_tokens(monkeypatch, change):
    inventory = _inventory()
    match change:
        case "missing":
            inventory["relations"].pop()
        case "namespace":
            inventory["relations"][0]["schema_name"] += "a"
        case "canonical":
            inventory["relations"] = [{**row, "schema_name": "tiger"} for row in inventory["relations"]]
        case "legacy":
            inventory["database_oid"] = 12345
            for row in inventory["relations"]:
                row.update(owner_oid=12345, relation_oid=0, relfilenode=0)
    expected = held._named_inventory(inventory)
    actual = deepcopy(expected["relations"])
    if change == "different_name":
        actual[0]["relation_name"] = "other"
    reader = AsyncMock(side_effect=actual)
    monkeypatch.setattr(held, "_protected_relation", reader)
    session = _session()
    if change in {None, "legacy"}:
        assert await held._require_inventory(session, inventory) == expected
        assert [call.kwargs for call in reader.await_args_list] == [{}, {}]
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_inventory(session, inventory)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "retired", "origin", "current", "missing", "package", "receipt", "children"))
async def test_installed_selection_rechecks_current_incarnation_and_inheritance_custody(monkeypatch, change):
    generation_by_field = {
        "state": "retained",
        "origin_kind": "installed",
        "node_id": "synthetic-node",
        "generation_id": _EPOCH_ID,
        "installation_id": "synthetic-installation",
        "package_id": "a" * 64,
        "inventory": _inventory(),
    }
    pairs = sorted(
        [relation["relation_name"], relation["relation_oid"]]
        for relation in generation_by_field["inventory"]["relations"]
    )
    receipt_by_field = {
        "contract": inherited.CONTRACT,
        "relation_oids": pairs,
        "parent_inventory": {"contract": inherited.CONTRACT, "parent_oids": [["zcta5", 201], ["zip_state", 202]]},
    }
    custody_by_field = {
        "installation_id": generation_by_field["installation_id"],
        "package_id": generation_by_field["package_id"],
        "receipt": receipt_by_field,
        "receipt_sha256": held._digest(receipt_by_field),
    }
    if change == "retired":
        generation_by_field["state"] = "retired"
    elif change == "origin":
        generation_by_field["origin_kind"] = "captured"
    elif change == "package":
        custody_by_field["package_id"] = "b" * 64
    elif change == "receipt":
        receipt_by_field["contract"] = "other"
    session = _session(
        *(() if change == "missing" else (custody_by_field,)),
        scalars=(True, "stale" if change == "current" else _EPOCH_ID),
    )
    monkeypatch.setattr(held, "_protected_relation", AsyncMock())
    parent_check = AsyncMock()
    monkeypatch.setattr(held, "_require_received_parents", parent_check)
    monkeypatch.setattr(
        inherited,
        "current_tiger_snapshot_children",
        AsyncMock(
            return_value=() if change == "children" else inherited.relation_names(generation_by_field["inventory"])
        ),
    )
    if change is None:
        await held._require_installed_selection(session, generation_by_field)
        parent_check.assert_awaited_once_with(session, receipt_by_field["parent_inventory"])
    else:
        with pytest.raises(RuntimeError, match="TIGER"):
            await held._require_installed_selection(session, generation_by_field)
        if change != "children":
            parent_check.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [None, PermissionError("native lock denied"), archive.ReferenceFamilyArchiveError("parent populated")]
)
async def test_received_parent_reader_preserves_native_permission_and_empty_parent_checks(monkeypatch, failure):
    parents_by_field = {
        "contract": inherited.CONTRACT,
        "parent_oids": [["zcta5", 201], ["zip_state", 202]],
        "catalog_sha256": "opaque",
    }
    check = AsyncMock(side_effect=failure)
    monkeypatch.setattr(inherited, "require_tiger_snapshot_parents", check)
    session = _session()
    if failure is None:
        await held._require_received_parents(session, parents_by_field)
    else:
        with pytest.raises(type(failure)) as raised:
            await held._require_received_parents(session, parents_by_field)
        assert raised.value is failure
    check.assert_awaited_once_with(session)
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["captured-source", "captured-epoch", "installed", "absent"])
async def test_tiger_source_observation_preserves_captured_priority_and_installed_fallback(monkeypatch, kind):
    generation_by_field = dict(
        node_id="synthetic-node",
        generation_id=UUID(_EPOCH_ID),
        selected_package_id="a" * 64,
        registration_sha256="b" * 64,
    )
    is_generic = kind == "captured-source"
    reader = AsyncMock(return_value=({"owner_oid": _OWNER}, [generation_by_field] if is_generic else []))
    monkeypatch.setattr(held, "_selected_captured_generations", reader)
    model = AsyncMock(return_value=_inventory())
    monkeypatch.setattr(held, "_require_model_capture", model)
    fallback = None if kind == "absent" else dict(selection_kind="installed", inventory=_inventory())
    if kind == "captured-epoch":
        fallback = held._captured_tiger_proof(kind, "synthetic-node", _EPOCH_ID, "a" * 64, "b" * 64, _inventory())
    observer = AsyncMock(return_value=fallback)
    monkeypatch.setattr(held, "_selected_tiger_observation", observer)
    native_owner = AsyncMock(return_value={"owner_oid": _OWNER})
    monkeypatch.setattr(held, "_protected_relation", native_owner)
    session = _session()
    observed = await held.selected_tiger_source_observation(session, node_id="synthetic-node", owner_oid=_OWNER)
    assert reader.await_args.kwargs == dict(node_id="synthetic-node", families=("tiger",))
    if kind in {"captured-source", "captured-epoch"}:
        assert observed["selection_kind"] == kind and observed["source_id"] == _EPOCH_ID
        assert observed["selection_sha256"] == held._digest(
            {key: field_value for key, field_value in observed.items() if key != "selection_sha256"}
        )
    else:
        assert observed is None
    if is_generic:
        model.assert_awaited_once_with(session, generation_by_field)
        observer.assert_not_awaited()
    else:
        model.assert_not_awaited()
        assert observer.await_args.kwargs == dict(node_id="synthetic-node", observe=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["node", "ambiguous"])
async def test_tiger_source_observation_refuses_scope_before_native_model_reads(monkeypatch, fault):
    generation_by_field = dict(node_id="other" if fault == "node" else "synthetic-node")
    monkeypatch.setattr(
        held,
        "_selected_captured_generations",
        AsyncMock(
            return_value=(
                {"owner_oid": _OWNER + int(fault == "owner")},
                [generation_by_field] * (2 if fault == "ambiguous" else 1),
            )
        ),
    )
    check = AsyncMock()
    monkeypatch.setattr(held, "_require_model_capture", check)
    with pytest.raises(RuntimeError, match="protected TIGER"):
        await held.selected_tiger_source_observation(_session(), node_id="synthetic-node", owner_oid=_OWNER)
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_epoch_observation_preserves_full_registration_and_package_guards(monkeypatch):
    epoch, selected, family = _captured_selection()
    selected.update(manifest={"adapter_metadata": {"family": family}}, selected_package_id="a" * 64)
    selected["selected_package_id"] = held._digest(selected["manifest"])
    session = _session(epoch, scalars=(True, True))
    monkeypatch.setattr(held, "_protected_relation", AsyncMock(return_value={"owner_oid": _OWNER}))
    monkeypatch.setattr(held, "_require_inventory", AsyncMock(return_value=epoch["inventory"]))
    monkeypatch.setattr(held, "_captured_schema_identity", AsyncMock(return_value="a" * 64))
    proof = await held._require_captured_selection(session, selected, family, observe=True)
    assert proof == held._captured_tiger_proof(
        "captured-epoch",
        selected["node_id"],
        _EPOCH_ID,
        selected["selected_package_id"],
        epoch["registration_sha256"],
        epoch["inventory"],
    )
    selected["selected_package_id"] = "f" * 64
    with pytest.raises(RuntimeError, match="protected TIGER"):
        await held._require_captured_selection(session, selected, family, observe=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_captured_schema_keeps_semantic_column_key_and_sequence_compatibility(monkeypatch, changed):
    epoch, _selected, family = _captured_selection()
    schema = AsyncMock(return_value=("b" if changed else "a") * 64)
    monkeypatch.setattr(held, "_captured_schema_identity", schema)
    inventory = held._named_inventory(epoch["inventory"])
    if changed:
        with pytest.raises(RuntimeError, match="protected TIGER"):
            await held._require_model_schema(_session(), "tiger", inventory, family)
    else:
        await held._require_model_schema(_session(), "tiger", inventory, family)
        assert schema.await_count == 2
    assert all(set(call.args[2]) == {"schema_name", "relation_name"} for call in schema.await_args_list)
