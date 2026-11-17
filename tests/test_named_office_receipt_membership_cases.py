# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named office receipts retain reviewed data and bound membership transfer."""

import copy
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_office_approval as approval
from process import registry_ptg_office_membership as offices
from process import registry_ptg_office_preparation as preparation
from process import registry_retained_site_adoption as sites
from process.network_compact_layout import CompactAddressRelation
from process.registry_ptg_office_retention import office_hold_document
from tests.test_named_retained_sites import manifest, selected, selection
from tests.test_registry_ptg_office_membership_recipe import _owner, _recipe
from tests.test_registry_ptg_office_preparation import _persisted_named_office
from tests.test_registry_ptg_office_retention import _verified


def test_compact_named_receipt_keeps_base_custom_rows_and_source_editions():
    receipt = replace(
        sites._receipt(manifest(), selected()),
        address_relation=CompactAddressRelation("copied_base", "address_rows"),
        compact_address_relations=(
            ("base", "copied_base", "address_rows"),
            ("custom", "copied_custom", "address_rows"),
        ),
    )
    document = receipt.as_dict()
    original = copy.deepcopy(document)
    projected = sites.retained_site_receipt_document(document)
    assert projected == original == receipt.as_dict()
    assert projected["records"] == selected_records()
    assert projected["source_generations"] == manifest().source_generations
    assert projected["manifest_sha256"] == manifest().manifest_sha256
    assert document == original


def selected_records():
    return json.loads(selected()["records_json"])


@pytest.mark.parametrize("change", ["type", "shape"])
def test_saved_receipt_requires_a_complete_document(change):
    document = sites._receipt(manifest(), selected()).as_dict()
    if change == "type":
        document = None
    else:
        document.pop("records")
    with pytest.raises(sites.RetainedSiteAdoptionError):
        sites.retained_site_receipt_document(document)


@pytest.mark.parametrize(
    "relations",
    [
        [["base", "copied_base"]],
        [["other", "copied_base", "address_rows"]],
        [["base", "copied_base", "address_rows"], ["base", "other_base", "address_rows"]],
        [["base", "invalid.name", "address_rows"]],
    ],
)
def test_compact_receipt_refuses_ambiguous_or_invalid_relation_names(relations):
    document = sites._receipt(manifest(), selected()).as_dict()
    document["compact_address_relations"] = relations
    with pytest.raises(ValueError):
        sites.retained_site_receipt_document(document)


@pytest.mark.parametrize(
    "address", [None, [], {"schema_name": "copied_base"}, {"schema_name": "a", "table_name": "b.c"}]
)
def test_named_receipt_retains_default_or_validates_complete_address_relation(address):
    document = sites._receipt(manifest(), selected()).as_dict()
    document["address_relation"] = address
    if address is None:
        assert sites.retained_site_receipt_document(document)["address_relation"] == {
            "schema_name": manifest().schema_name,
            "table_name": "entity_address_unified",
        }
    else:
        with pytest.raises(ValueError):
            sites.retained_site_receipt_document(document)


def test_legacy_compact_receipt_requires_a_base_before_data_reuse():
    document = sites._receipt(manifest(), selected()).as_dict()
    document.pop("address_relation")
    document.update(dict.fromkeys(("schema_oid", "address_table_oid", "binding_table_oid", "owner_role_oid"), 123))
    document["compact_address_relations"] = [["custom", "custom_example", "address_rows", 456]]
    with pytest.raises(sites.RetainedSiteAdoptionError):
        sites.retained_site_receipt_document(document)


@pytest.mark.parametrize(
    "roles",
    [
        {
            "namespace_owner_role": "office_owner",
            "publisher_role": "office_owner",
            "application_roles": ["office_owner"],
        },
        {"namespace_owner_role": "office_owner", "publisher_role": "publisher", "application_roles": ["reader"]},
        {
            "namespace_owner_role": "office_owner",
            "publisher_role": "publisher",
            "application_roles": ["reader", "publisher"],
        },
        {"namespace_owner_role": "office_owner", "publisher_role": "publisher", "application_roles": []},
    ],
)
def test_office_configuration_rejects_overlapping_or_incomplete_roles(roles):
    with pytest.raises(ValueError, match="profile_unavailable"):
        preparation.named_office_preparation_roles(roles)


def test_office_configuration_accepts_existing_sorted_application_roles():
    configured_roles_by_field = {
        "namespace_owner_role": "office_owner",
        "publisher_role": "publisher",
        "application_roles": ("publisher", "reader"),
    }
    assert preparation.named_office_preparation_roles(configured_roles_by_field) == {
        **configured_roles_by_field,
        "application_roles": ["publisher", "reader"],
    }


@pytest.mark.parametrize("change", ["candidate", "accounting_shape", "count", "retained_rows"])
def test_office_manifest_refuses_incomplete_or_mismatched_accounting(change):
    driver, request, _ = _persisted_named_office()
    document = json.loads(driver.descriptor.manifest_json)
    candidate_id = str(request.capture_id)
    if change == "candidate":
        candidate_id = "00000000-0000-0000-0000-000000000000"
    elif change == "accounting_shape":
        document["accounting"].pop("input_row_count")
    elif change == "count":
        document["accounting"]["input_row_count"] += 1
    else:
        document["accounting"]["retained_site_rows_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="manifest_changed"):
        preparation.office_manifest_data(document, driver.descriptor.manifest_sha256, candidate_id, "registry")


async def _retained_case(monkeypatch):
    prepared, witness = await _verified(monkeypatch)
    command = json.loads(prepared.descriptor.manifest_json)["command"]
    document = approval._document(
        {
            "actor": {"kind": "platform_admin", "user_id": str(prepared.request.capture_id), "client_id": "system"},
            "command": command,
        },
        prepared.descriptor,
        witness,
        {"operation_id": "registry_ptg_office_review_" + prepared.request.capture_id.hex},
    )
    approval_digest = offices._digest(document)
    hold = office_hold_document(prepared.descriptor, witness, approval_digest)
    pin = replace(
        _recipe().source_pin,
        capture_id=str(prepared.request.capture_id),
        client_id=command["client_id"],
        manifest_sha256=prepared.descriptor.manifest_sha256,
        approval_sha256=approval_digest,
        hold_sha256=offices._digest(hold),
    )
    retained_document_rows = [
        {
            "approval_sha256": pin.approval_sha256,
            "approval_json": document,
            "hold_sha256": pin.hold_sha256,
            "hold_json": hold,
        }
    ]
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: retained_document_rows))
        )
    )
    monkeypatch.setattr(
        offices, "_protected_store", AsyncMock(side_effect=lambda *args, table_name, **kwargs: table_name)
    )
    return session, pin, retained_document_rows, document, hold


@pytest.mark.asyncio
async def test_retained_membership_documents_match_reviewed_data(monkeypatch):
    session, pin, rows, document, hold = await _retained_case(monkeypatch)
    assert await offices._retained_documents(session, object(), pin) == (document, hold)
    assert (
        rows[0]["approval_json"]["command"]["canonical_input_sha256"] == document["command"]["canonical_input_sha256"]
    )
    assert session.execute.await_args.args[1] == {"capture": pin.capture_id}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "duplicate", "review", "hold", "capture", "client", "manifest"])
async def test_membership_document_drift_refuses_before_transfer(monkeypatch, change):
    session, pin, rows, document, hold = await _retained_case(monkeypatch)
    if change == "missing":
        rows.clear()
    elif change == "duplicate":
        rows.append(copy.deepcopy(rows[0]))
    elif change == "review":
        document["command"]["reason"] = "Different review"
    elif change == "hold":
        hold["manifest_sha256"] = "d" * 64
    else:
        field = {"capture": "capture_id", "client": "client_id", "manifest": "manifest_sha256"}[change]
        document[field] = "different"
        rows[0]["approval_sha256"] = offices._digest(document)
        pin = replace(pin, approval_sha256=rows[0]["approval_sha256"])
    with pytest.raises(ValueError, match="recipe_unavailable|recipe_changed"):
        await offices._retained_documents(session, object(), pin)


@pytest.mark.asyncio
async def test_membership_page_keeps_all_records_and_terminal_cursor(monkeypatch):
    driver, verified, approved = _owner()
    retained = sites._receipt(manifest(), selected())
    verified = replace(verified, request=SimpleNamespace(input_row_count=1), serving=manifest())
    office_row_by_field = {**selection(), "ordinal": 1, "evidence_id": "d" * 64}
    driver.fetchrow.return_value = {"rows": 1, "bytes": 1024}
    driver.fetch.return_value = [office_row_by_field]
    monkeypatch.setattr(offices, "pin_approved_membership_source", AsyncMock(return_value=approved))
    resolver = AsyncMock(return_value=retained)
    monkeypatch.setattr(sites, "resolve_retained_site_adoptions", resolver)
    batch = await offices.read_ptg_office_membership_batch(driver, verified, approved, None, 1, "control_fixture")
    assert batch.source_rows == batch.membership_rows == 1 and batch.next_ordinal == 1
    assert batch.omitted_rows == batch.unresolved_rows == 0 and batch.office_records == retained.records
    assert batch.input_sha256 == hashlib.sha256(batch.input_bytes).hexdigest()
    assert json.loads(batch.input_bytes) == [
        {
            "network_id": verified.pin.network_id,
            **{
                field: office_row_by_field[field]
                for field in ("provider_system", "provider_id", "location_id", "evidence_id")
            },
        }
    ]
    resolver.assert_awaited_once_with(
        driver, verified.serving, offices._canonical([selection()]), control_schema="control_fixture"
    )
    driver.fetchrow.return_value, driver.fetch.return_value = {"rows": 0, "bytes": 2}, []
    terminal = await offices.read_ptg_office_membership_batch(driver, verified, approved, 1, 1, "control_fixture")
    assert terminal.source_rows == 0 and terminal.next_ordinal is None and terminal.input_bytes == b"[]"
    assert terminal.office_records == () and resolver.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("after,limit", [(-1, 1), (True, 1), (None, 0), (None, True)])
async def test_invalid_membership_page_never_transfers_rows(monkeypatch, after, limit):
    driver, verified, approved = _owner()
    monkeypatch.setattr(offices, "pin_approved_membership_source", AsyncMock(return_value=approved))
    with pytest.raises(ValueError, match="recipe_page_changed"):
        await offices.read_ptg_office_membership_batch(driver, verified, approved, after, limit, "control_fixture")
    driver.fetchrow.assert_not_awaited()
    driver.fetch.assert_not_awaited()
