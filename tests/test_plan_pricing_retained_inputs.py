# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected retained-input orchestration with explicit native catalog doubles."""

from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.plan_release_storage import PIN_TABLE, plan_release_schema
from process import plan_pricing_retained_inputs as retained
from process.plan_pricing_admission import PricingAdmission
from process.plan_pricing_control import pricing_control_digest
from process.plan_release_preparation_pin_catalog import _definition
from process.plan_release_preparation_pin_contract import digest
from tests.test_plan_release_preparation_pin_contract import REVISION, heap, scope


def _input_inventory(*, incumbent):
    identity_by_field = {"revisions": [REVISION], "plans": ["hpplan_example"], "source_keys": ["synthetic_source"]}
    input_key = pricing_control_digest(identity_by_field)
    inputs_by_key = {
        input_key: {
            "identity": identity_by_field,
            "admission_inputs": True,
            "scope_digest": "a" * 64,
            "outcome_receipt_digest": "b" * 64,
        }
    }
    seal_by_field = {
        "scope_digest": "a" * 64,
        "outcome_receipt_digest": "b" * 64,
        "cohorts_digest": pricing_control_digest(inputs_by_key),
        "revision_count": 1,
    }
    contract_by_field = {
        "existing_revision": incumbent,
        "publications": [["e" * 64, REVISION]],
        "input_key": input_key,
        "scope_digest": "a" * 64,
        "outcome_receipt_digest": "b" * 64,
        "schema": plan_release_schema(),
        "roots": {PIN_TABLE: 21, "plan_release_snapshot_binding": 22},
    }
    return inputs_by_key, seal_by_field, contract_by_field


def _retained_heaps():
    catalog_by_name = {
        name: {"oid": oid, "columns": [], "constraints": [], "relkind": "r", "relpersistence": "p", "indexes": []}
        for name, oid in (("retained_binding", 31), ("retained_pin", 32))
    }
    heaps = [
        asdict(
            replace(
                heap(),
                schema="control_catalog",
                name=name,
                oid=catalog["oid"],
                definition_digest=digest(_definition(catalog)),
                indexes_digest=digest(catalog["indexes"]),
            )
        )
        for name, catalog in catalog_by_name.items()
    ]
    return catalog_by_name, heaps


def _retained_case(monkeypatch, *, incumbent=False):
    """Use canonical scope fixtures while keeping native qualification explicitly separate."""
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "pricing_storage")
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "control_catalog")
    inputs_by_key, seal, contract_by_field = _input_inventory(incumbent=incumbent)
    identity = replace(
        scope(),
        binding_inputs_digest=pricing_control_digest(seal),
        cohort_digest=pricing_control_digest(contract_by_field),
    )
    catalog_by_name, heaps = _retained_heaps()
    staged_by_field = {"contract": contract_by_field, "heaps": heaps}
    receipt_by_field = {
        "contract": "plan-release-incumbent-retention-v1" if incumbent else "plan-release-preparation-pins-v1",
        "scope": identity.payload(),
        "owner_oid": 12,
        **(
            {"heaps": deepcopy(heaps), "roots": dict(contract_by_field["roots"])}
            if incumbent
            else {
                "heap": deepcopy(heaps[1]),
                "root_oid": 21,
                "retained_revision_ids": [REVISION],
            }
        ),
    }
    inventory_by_field = {
        "binding_inputs": inputs_by_key,
        "binding_input_seal": seal,
        "projection_inputs": {"cohort": staged_by_field},
        "projection_incumbents" if incumbent else "projection_retention": {"cohort": receipt_by_field},
    }
    record_by_field = {
        "inventory": inventory_by_field,
        "preparation_id": identity.preparation_id,
        "scope_digest": identity.scope_digest,
        "receipt_digest": identity.outcome_receipt_digest,
        "publication_identity": "e" * 64,
        "healthporta_plan_id": "hpplan_example",
    }
    family_by_field = {
        "owner_oid": 12,
        "reader_oids": [13],
        "relations": [
            {"nspname": plan_release_schema(), "relname": name, "oid": oid}
            for name, oid in contract_by_field["roots"].items()
        ],
    }
    admission = PricingAdmission(record_by_field, {}, {}, None, family_by_field)
    return SimpleNamespace(
        admission=admission, staged=staged_by_field, receipt=receipt_by_field, catalogs=catalog_by_name
    )


def _native_heap_observations(monkeypatch, case):
    async def catalog(_connection, schema, name, **options):
        assert schema == "control_catalog" and options["owner_oid"] == 12 and options["reader_oids"] == [13]
        return case.catalogs[name]

    observe = AsyncMock(side_effect=catalog)
    monkeypatch.setattr(retained, "preparation_heap_catalog", observe)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"filenode": heap().filenode, "database_oid": heap().database_oid})
    )
    return connection, observe


@pytest.mark.asyncio
@pytest.mark.parametrize("incumbent", [False, True])
async def test_retained_cohort_authenticates_both_exact_native_images_without_writing(monkeypatch, incumbent):
    case = _retained_case(monkeypatch, incumbent=incumbent)
    connection, observe = _native_heap_observations(monkeypatch, case)
    staged, receipt, identity, revision, actual_incumbent = retained._retained_inventory(case.admission)
    assert staged is case.staged and receipt is case.receipt and revision == REVISION
    assert identity.preparation_id == case.admission.record_by_field["preparation_id"] and actual_incumbent is incumbent
    await retained.require_retained_pricing_inputs(connection, case.admission)
    assert [call.args[2] for call in observe.await_args_list] == ["retained_binding", "retained_pin"]
    assert [call.kwargs["pin"] for call in observe.await_args_list] == [False, True]
    assert [call.kwargs["parent_oid"] for call in observe.await_args_list] == [None, None if incumbent else 21]
    assert [call.args[1] for call in connection.fetchrow.await_args_list] == [31, 32]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["seal", "plan", "scope", "source_roots", "receipt_heap", "duplicate", "foreign_publication", "owner"]
)
async def test_stale_or_foreign_retention_refuses_before_native_heap_reads(monkeypatch, damage):
    case = _retained_case(monkeypatch)
    inventory = case.admission.record_by_field["inventory"]
    if damage == "seal":
        inventory["binding_input_seal"]["revision_count"] = 2
    if damage == "plan":
        case.admission.record_by_field["healthporta_plan_id"] = "hpplan_foreign"
    if damage == "scope":
        case.receipt["scope"]["preparation_id"] = "00000000-0000-0000-0000-000000000002"
    if damage == "source_roots":
        case.admission.family["relations"][0]["oid"] = 99
    if damage == "receipt_heap":
        case.receipt["heap"]["filenode"] = 99
    if damage == "duplicate":
        inventory["projection_inputs"]["other"] = deepcopy(case.staged)
    if damage == "foreign_publication":
        case.admission.record_by_field["publication_identity"] = "f" * 64
    if damage == "owner":
        case.receipt["owner_oid"] = 99
    connection, observe = _native_heap_observations(monkeypatch, case)
    with pytest.raises(ValueError):
        await retained.require_retained_pricing_inputs(connection, case.admission)
    observe.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["oid", "definition", "indexes", "filenode", "database"])
async def test_replaced_native_heap_never_authorizes_following_pin(monkeypatch, damage):
    case = _retained_case(monkeypatch)
    connection, observe = _native_heap_observations(monkeypatch, case)
    if damage == "oid":
        case.catalogs["retained_binding"]["oid"] = 99
    elif damage == "definition":
        case.catalogs["retained_binding"]["columns"] = [["changed"]]
    elif damage == "indexes":
        case.catalogs["retained_binding"]["indexes"] = [{"changed": True}]
    else:
        connection.fetchrow.return_value["filenode" if damage == "filenode" else "database_oid"] = 99
    with pytest.raises(ValueError, match="retained_heap"):
        await retained.require_retained_pricing_inputs(connection, case.admission)
    assert observe.await_count == 1 and observe.await_args.args[2] == "retained_binding"
