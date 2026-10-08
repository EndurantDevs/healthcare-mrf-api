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
@pytest.mark.parametrize("change", ("missing", "writable", "owner", "inheritance", None))
async def test_protected_catalog_requires_inert_owner_and_select_only_access(change):
    relation_by_field = {**_inventory()["relations"][0], "protected": change != "writable"}
    session = _session(*(() if change == "missing" else (relation_by_field,)), scalars=(change == "inheritance",))
    if change is None:
        assert await held._protected_relation(session, held._CAPTURE, expected_owner=_OWNER, catalog=True) == {
            key: value for key, value in relation_by_field.items() if key != "protected"
        }
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._protected_relation(
                session, held._CAPTURE, expected_owner=_OWNER + (change == "owner"), catalog=True
            )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0] == f"LOCK TABLE ONLY {held._CAPTURE} IN ACCESS SHARE MODE NOWAIT"
    assert "LATERAL aclexplode(col.attacl)" in statements[1]
    assert "privilege_type<>'SELECT'" in statements[1] and "AND NOT r.rolcanlogin" in statements[1]


@pytest.mark.asyncio
async def test_leaf_relation_does_not_apply_catalog_no_inheritance_rule():
    relation_by_field = {**_inventory()["relations"][0], "protected": True}
    session = _session(relation_by_field, scalars=())
    assert await held._protected_relation(session, '"held"."zip_state"') == _inventory()["relations"][0]
    session.scalar.assert_not_awaited()


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
        installed.assert_awaited_once_with(session, generation_by_field, _OWNER)
        checked_inventory.assert_awaited_once_with(session, inventory, _OWNER)
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
        selected["inventory"]["database_oid"] += 1
    session = _session(*(() if change == "missing" else (epoch,)))
    monkeypatch.setattr(held, "_protected_relation", AsyncMock(return_value={"owner_oid": _OWNER}))
    inventory_check = AsyncMock(return_value=epoch["inventory"])
    monkeypatch.setattr(held, "_require_inventory", inventory_check)
    if change is None:
        assert await held._require_captured_selection(session, selected, family) == epoch["inventory"]
        inventory_check.assert_awaited_once_with(session, epoch["inventory"], _OWNER)
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_captured_selection(session, selected, family)
        inventory_check.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "database", "missing", "namespace", "canonical", "physical"))
async def test_inventory_requires_exact_two_owned_physical_leaves(monkeypatch, change):
    inventory = _inventory()
    actual = deepcopy(inventory["relations"])
    match change:
        case "database":
            inventory["database_oid"] += 1
        case "missing":
            inventory["relations"].pop()
        case "namespace":
            inventory["relations"][0]["schema_name"] += "a"
        case "canonical":
            inventory["relations"] = [{**relation, "schema_name": "tiger"} for relation in inventory["relations"]]
        case "physical":
            actual[0]["relfilenode"] += 1
    reader = AsyncMock(side_effect=actual)
    monkeypatch.setattr(held, "_protected_relation", reader)
    session = _session(scalars=(77,))
    if change is None:
        assert await held._require_inventory(session, inventory, _OWNER) == inventory
        assert [call.kwargs for call in reader.await_args_list] == [{"expected_owner": _OWNER}] * 2
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_inventory(session, inventory, _OWNER)


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
    receipt_by_field = {"contract": inherited.CONTRACT, "relation_oids": pairs, "parent_inventory": {"synthetic": True}}
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
        custody_by_field["receipt_sha256"] = "b" * 64
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
        AsyncMock(return_value=() if change == "children" else tuple(map(tuple, pairs))),
    )
    if change is None:
        await held._require_installed_selection(session, generation_by_field, _OWNER)
        parent_check.assert_awaited_once_with(session, receipt_by_field["parent_inventory"], _OWNER)
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_installed_selection(session, generation_by_field, _OWNER)
        parent_check.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "oid", "populated", "objects", "extension", "catalog"))
async def test_received_parent_reader_rechecks_identity_without_sequence_state_grants(monkeypatch, change):
    objects, extension, catalog = [{"relation_oid": 201}], {"members": [201, 202]}, {"columns": [1, 2]}
    parent_inventory_by_field = {
        "parent_oids": [["zcta5", 201], ["zip_state", 202]],
        "parents": [],
        "objects": objects,
        "extension_sha256": held._digest(extension),
        "catalog_sha256": held._digest(catalog),
    }
    session = _session(
        scalars=(
            999 if change == "oid" else 201,
            change == "populated",
            202,
            False,
            {"changed": True} if change == "catalog" else catalog,
        )
    )
    monkeypatch.setattr(
        inherited, "_tiger_parent_object_inventory", AsyncMock(return_value=[] if change == "objects" else objects)
    )
    monkeypatch.setattr(
        inherited, "_tiger_extension_inventory", AsyncMock(return_value={} if change == "extension" else extension)
    )
    if change is None:
        await held._require_received_parents(session, parent_inventory_by_field, _OWNER)
    else:
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            await held._require_received_parents(session, parent_inventory_by_field, _OWNER)
    assert not any(
        "last_value" in str(call.args[0]) or "nextval" in str(call.args[0]) for call in session.scalar.await_args_list
    )
