# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Selected inventory rows prove raw registration only; native source replay remains mandatory."""

import hashlib
import json
import re
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_initial_cms_office_bindings as cms
from process import network_initial_source_office_bindings as offices
from process import registry_ptg_office_bindings as ptg
from process import registry_source_inventory_store as inventory_store
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_membership_source import FHIRMembershipBatch, PinnedFHIRMembershipSource
from process.network_fhir_retained_capture_contract import PRODUCER as RETAINED_FHIR_PRODUCER
from process.network_legacy_membership_source import ACAMembershipBatch
from process.network_membership_copy import MembershipCopyTarget
from process.registry_ptg_office_membership import PTGOfficeMembershipBatch
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import SourceInventoryReference
from process.registry_source_recipe_store import MAX_SOURCE_RECIPE_BYTES, canonical_registry_source_recipes
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_inventory_preparation import _aca_recipe, _derivation, _descriptor
from tests.test_registry_source_inventory_store_postgres import _capture_recipe
from tests.test_registry_source_recipe_store_postgres import _cms_epoch_recipe

_APPROVED = ApprovedMembershipSource(7, "b" * 64)
_ADDRESS = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "c" * 64)
_CONTROL = '"synthetic_control"'


@asynccontextmanager
async def _transaction():
    yield


def _batch(recipe):
    checksum = hashlib.sha256(b"[]").hexdigest()
    if recipe.binding_coordinates.source_system == "ptg":
        verified = SimpleNamespace(descriptor=SimpleNamespace(schema_name="synthetic_retained"))
        return PTGOfficeMembershipBatch(recipe.source_pin, _APPROVED, verified, 0, 0, 0, 0, b"[]", checksum, None, ())
    model = FHIRMembershipBatch if type(recipe.source_pin) is PinnedFHIRMembershipSource else ACAMembershipBatch
    return model(
        recipe.source_pin,
        0,
        0,
        0,
        None,
        b"[]",
        checksum,
        approved_source=_APPROVED,
        binding_coordinates=recipe.binding_coordinates,
        approved_only=True,
    )


def _scope(family):
    recipe = {"cms": _cms_epoch_recipe, "capture": _capture_recipe, "aca": _aca_recipe, "ptg": _ptg_recipe}[family]()
    if family == "capture":
        recipe = replace(
            recipe, binding_coordinates=replace(recipe.binding_coordinates, producer_id=RETAINED_FHIR_PRODUCER)
        )
    preparation = SourceInventoryPreparation((recipe,))
    descriptor = _descriptor(recipe) if type(recipe.source_pin) is PinnedFHIRMembershipSource else None
    if descriptor is not None:
        preparation.add_contribution(descriptor)
    preparation.add_derivation(_derivation(recipe, descriptor))
    reference, entries = preparation.finish()
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=index)) for index in range(1, 5)), "network_candidate_" + UUID(int=4).hex
    )
    candidate_by_field = {
        "state": "open",
        "approved_custom_revision": 7,
        "source_recipes_json": reference.as_dict(),
        "source_generations": {
            "unified_address": _ADDRESS.generation_id,
            "custom_membership": _APPROVED.generation_id,
            "registry_source_recipes": reference.root.recipes_sha256,
            "registry_source_inventory": reference.root.entries_sha256,
        },
    }
    record_by_field = dict(
        zip(inventory_store._COLUMNS, inventory_store.source_inventory_record(entries[0]), strict=True)
    )
    return SimpleNamespace(
        recipe=recipe,
        batch=_batch(recipe),
        copy_target=copy_target,
        candidate=candidate_by_field,
        native_record=record_by_field,
        root=reference.root,
    )


def _native(monkeypatch, scope, *, native_records=None):
    selected_records = [scope.native_record] if native_records is None else native_records

    async def lookup(_statement, *arguments):
        matches = [
            native_record for native_record in selected_records if native_record["recipe_sha256"] == arguments[0]
        ][:2]
        expected = (arguments[1], *arguments[2:10])
        fields = ("recipe_json", *inventory_store._SCOPE_FIELDS, "fhir_scope_key", "fhir_scope_value")
        return {
            "entry_count": len(matches),
            "ordinal": max((native_record["ordinal"] for native_record in matches), default=None),
            "recipe_bytes": max(
                (len(native_record["recipe_json"].encode()) for native_record in matches), default=None
            ),
            "is_exact": bool(matches)
            and all(
                tuple(native_record[field] for field in fields) == expected
                and native_record["office_derivation_json"] is not None
                and (native_record["contribution_json"] is not None) == (arguments[2] == "fhir")
                for native_record in matches
            ),
        }

    connection = SimpleNamespace(
        transaction=_transaction,
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(
            side_effect=lambda query, *_arguments: "repeatable read" if query.startswith("SHOW") else 42
        ),
        fetchrow=AsyncMock(side_effect=lookup),
    )
    monkeypatch.setattr(offices, "_locked_candidate", AsyncMock(return_value=scope.candidate))
    physical = AsyncMock(return_value='"synthetic_candidate".network_source_inventory')
    monkeypatch.setattr(inventory_store, "_physical_context", physical)
    return connection, physical


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["cms", "capture", "aca", "ptg"])
@pytest.mark.parametrize("encoded", [False, True])
async def test_registered_pin_uses_full_source(monkeypatch, family, encoded):
    scope = _scope(family)
    if encoded:
        for field in ("source_recipes_json", "source_generations"):
            scope.candidate[field] = json.dumps(scope.candidate[field])
    connection, physical = _native(monkeypatch, scope)
    await offices._candidate(connection, scope.copy_target, scope.batch, _ADDRESS, _CONTROL)
    physical.assert_awaited_once_with(connection, scope.copy_target, 42, "synthetic_control")
    statement, identity, payload, *fields = connection.fetchrow.await_args.args
    singleton = canonical_registry_source_recipes((scope.recipe,))
    assert identity == hashlib.sha256(singleton.encode()).hexdigest() and payload == singleton[1:-1]
    assert tuple(fields[:6]) == scope.recipe.binding_coordinates.sql_parameters
    assert "LIMIT 2" in statement and "IS NOT DISTINCT FROM" in statement and "CASE WHEN octet_length" in statement
    assert fields[-1] == MAX_SOURCE_RECIPE_BYTES - 2
    assert scope.batch.generation_id not in _document(scope.candidate["source_generations"]).values()


def _document(native_value):
    return json.loads(native_value) if type(native_value) is str else native_value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "duplicate",
        "payload",
        "coordinates",
        "ordinal",
        "office_pair",
        "contribution_pair",
        "source_pin",
        "binding_pin",
        "approved",
        "root",
        "incomplete",
        "oid",
        "isolation",
        "batch_type",
        "selection",
    ],
)
async def test_registered_pin_refuses_drift(monkeypatch, damage):
    scope = _scope("cms")
    native_records = [scope.native_record]
    match damage:
        case "missing":
            native_records.clear()
        case "duplicate":
            native_records.append(dict(scope.native_record))
        case "payload":
            scope.native_record["recipe_json"] += " "
        case "coordinates":
            scope.native_record["producer_id"] = "other"
        case "ordinal":
            scope.native_record["ordinal"] = 2
        case "office_pair":
            scope.native_record["office_derivation_json"] = None
        case "contribution_pair":
            scope.native_record["contribution_json"] = None
        case "source_pin":
            epoch = replace(scope.batch.source.retained_epoch, content_sha256="1" * 64)
            scope.batch = replace(scope.batch, source=replace(scope.batch.source, retained_epoch=epoch))
        case "binding_pin":
            scope.batch = replace(
                scope.batch, binding_coordinates=replace(scope.batch.binding_coordinates, producer_id="other")
            )
        case "approved":
            scope.batch = replace(scope.batch, approved_source=ApprovedMembershipSource(8, "d" * 64))
        case "root":
            scope.candidate["source_generations"]["registry_source_inventory"] = "0" * 64
        case "incomplete":
            scope.candidate["source_recipes_json"] = SourceInventoryReference(
                replace(scope.root, derivation_count=0)
            ).as_dict()
        case "batch_type":
            scope.batch = SimpleNamespace(**vars(scope.batch))
        case "selection":
            scope.batch = replace(scope.batch, approved_only=False)
    connection, physical = _native(monkeypatch, scope, native_records=native_records)
    if damage == "oid":
        physical.side_effect = ValueError("physical_identity_invalid")
    if damage == "isolation":
        connection.fetchval.side_effect = None
        connection.fetchval.return_value = "read committed"
    with pytest.raises(ValueError):
        await offices._candidate(connection, scope.copy_target, scope.batch, _ADDRESS, _CONTROL)
    if damage in {"approved", "root", "incomplete", "oid", "isolation", "batch_type", "selection"}:
        connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["cms", "aca", "ptg"])
@pytest.mark.parametrize("registered", [False, True])
async def test_legacy_generation_guard_stays_exact(monkeypatch, family, registered):
    scope = _scope(family)
    scope.candidate["source_recipes_json"] = []
    scope.candidate["source_generations"] = {
        "unified_address": _ADDRESS.generation_id,
        "custom_membership": _APPROVED.generation_id,
    }
    if registered:
        scope.candidate["source_generations"]["legacy_source"] = scope.batch.generation_id
    connection, physical = _native(monkeypatch, scope)
    if registered:
        await offices._candidate(connection, scope.copy_target, scope.batch, _ADDRESS, _CONTROL)
    else:
        with pytest.raises(offices.InitialACAOfficeBindingError, match="candidate_pin_differs"):
            await offices._candidate(connection, scope.copy_target, scope.batch, _ADDRESS, _CONTROL)
    physical.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


class _BooleanAll:
    def __init__(self):
        self.is_valid = True

    def step(self, flag):
        self.is_valid = self.is_valid and bool(flag)

    def finalize(self):
        return int(self.is_valid)


@pytest.mark.parametrize("damage", [None, "null_selector", "payload", "unpaired", "duplicate", "bound", "missing"])
def test_scalar_sql_checks_nullable_registration(damage):
    scope = _scope("ptg")
    record_by_field = dict(scope.native_record)
    if damage == "null_selector":
        record_by_field["fhir_scope_value"] = "other"
    if damage == "payload":
        record_by_field["recipe_json"] += " "
    if damage == "unpaired":
        record_by_field["office_derivation_json"] = None
    fields = inventory_store._COLUMNS
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        connection.create_function("octet_length", 1, lambda native_value: len(native_value.encode()))
        connection.create_function("convert_to", 2, lambda native_value, _encoding: native_value.encode())
        connection.create_aggregate("bool_and", 1, _BooleanAll)
        connection.execute(
            "CREATE TABLE inventory("
            + ",".join(name + (" integer" if name == "ordinal" else " text") for name in fields)
            + ")"
        )
        if damage != "missing":
            connection.executemany(
                "INSERT INTO inventory VALUES(" + ",".join("?" for _field in fields) + ")",
                [tuple(record_by_field[field] for field in fields)] * (2 if damage == "duplicate" else 1),
            )
        expected = scope.native_record
        arguments = (
            expected["recipe_sha256"],
            expected["recipe_json"],
            *(expected[field] for field in fields[5:]),
            1 if damage == "bound" else MAX_SOURCE_RECIPE_BYTES - 2,
        )
        query = re.sub(r"::(?:integer|text)", "", offices._REGISTERED_PIN_SQL.format(relation="inventory"))
        result_by_field = dict(
            connection.execute(
                query, {str(index): native_value for index, native_value in enumerate(arguments, 1)}
            ).fetchone()
        )
    assert set(result_by_field) == {"entry_count", "ordinal", "recipe_bytes", "is_exact"}
    assert result_by_field["entry_count"] == (0 if damage == "missing" else 2 if damage == "duplicate" else 1)
    assert result_by_field["is_exact"] == int(damage in {None, "duplicate"})


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["cms", "aca", "ptg"])
async def test_registration_never_skips_native_replay(monkeypatch, family):
    """Passing raw registration cannot authorize copying a changed or unadmitted source page."""
    scope = _scope(family)
    connection, _physical = _native(monkeypatch, scope)
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())
    module = {"cms": cms, "aca": offices, "ptg": ptg}[family]
    capture_name = {
        "cms": "_capture_offices",
        "aca": "capture_aca_office_address_source",
        "ptg": "capture_ptg_office_address_source",
    }[family]
    capture = AsyncMock(side_effect=ValueError("native_replay_changed"))
    monkeypatch.setattr(module, capture_name, capture)
    monkeypatch.setattr(module, "_candidate", offices._candidate)
    copy_name = "_copy_source_office_records" if family == "cms" else "_copy_bindings"
    copied = AsyncMock()
    monkeypatch.setattr(module, copy_name, copied)
    common = (
        _ADDRESS,
        (1, 2, 3, "c" * 64),
        scope.batch.source.generation_id,
        scope.batch.generation_id,
        scope.batch.input_sha256,
        "d" * 64,
        None,
        0,
    )
    pin_type = {
        "cms": cms.PinnedCMSOfficeAddressSource,
        "aca": offices.PinnedACAOfficeAddressSource,
        "ptg": ptg.PinnedPTGOfficeAddressSource,
    }[family]
    pin = pin_type(*((common[0], common[2], common[1], *common[3:]) if family == "aca" else common))
    copier = {
        "cms": cms.copy_initial_cms_office_bindings,
        "aca": offices.copy_initial_aca_office_bindings,
        "ptg": ptg.copy_ptg_office_bindings,
    }[family]
    with pytest.raises(ValueError, match="native_replay_changed"):
        await copier(
            connection,
            scope.batch,
            scope.copy_target,
            pin,
            runtime_roles=("reader",),
            control_schema="synthetic_control",
        )
    capture.assert_awaited_once()
    copied.assert_not_awaited()
