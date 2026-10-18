# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate immutable native retained inputs without changing any principal."""

from db.plan_release_storage import PIN_TABLE, plan_release_schema, storage_relation
from process.plan_pricing_admission import require
from process.plan_pricing_control import pricing_control_digest
from process.plan_pricing_control_policy import pricing_control_schema
from process.plan_release_preparation_pin_catalog import _definition, preparation_heap_catalog
from process.plan_release_preparation_pin_contract import (
    PreparationPinHeap,
    digest,
    preparation_pin_scope_from_payload,
)
from process.plan_release_storage_custody import _document


def _staged_retention(admission):
    """Select exactly one protected native input cohort for this publication."""
    record_by_field = admission.record_by_field
    inventory = _document(record_by_field["inventory"])
    inputs = inventory.get("projection_inputs", {})
    matches = [
        (key, staged)
        for key, staged in inputs.items()
        if any(member[0] == record_by_field["publication_identity"] for member in staged["contract"]["publications"])
    ]
    require(len(matches) == 1, "retained_inputs_missing")
    key, staged = matches[0]
    is_incumbent = staged["contract"].get("existing_revision") is True
    kind = "projection_incumbents" if is_incumbent else "projection_retention"
    receipt = inventory.get(kind, {}).get(key)
    require(type(receipt) is dict, "retention_missing")
    contract = "plan-release-incumbent-retention-v1" if is_incumbent else "plan-release-preparation-pins-v1"
    require(
        receipt.get("contract") == contract and receipt["owner_oid"] == admission.family["owner_oid"],
        "retention_changed",
    )
    return record_by_field, inventory, staged, receipt, is_incumbent


def _retained_inventory(admission):
    """Bind the exact preparation and physical image without reopening writes."""
    record_by_field, inventory, staged, receipt, is_incumbent = _staged_retention(admission)
    _require_binding_seal(record_by_field, inventory, staged)
    scope = preparation_pin_scope_from_payload({**receipt["scope"], "contract": "plan-release-preparation-pins-v1"})
    require(
        scope.preparation_id == record_by_field["preparation_id"]
        and scope.scope_digest == record_by_field["scope_digest"]
        and scope.outcome_receipt_digest == record_by_field["receipt_digest"]
        and scope.binding_inputs_digest == pricing_control_digest(inventory["binding_input_seal"])
        and scope.cohort_digest == pricing_control_digest(staged["contract"])
        and [list(member) for member in scope.publication_revisions] == staged["contract"]["publications"]
        and staged["contract"]["scope_digest"] == scope.scope_digest
        and staged["contract"]["outcome_receipt_digest"] == scope.outcome_receipt_digest
        and staged["contract"]["schema"] == plan_release_schema(),
        "retained_scope_changed",
    )
    root_oids_by_name = {
        native_relation["relname"]: native_relation["oid"]
        for native_relation in admission.family["relations"]
        if native_relation["nspname"] == plan_release_schema()
    }
    require(
        all(root_oids_by_name.get(name) == oid for name, oid in staged["contract"]["roots"].items()),
        "retained_roots_changed",
    )
    if is_incumbent:
        require(
            receipt["heaps"] == staged["heaps"] and receipt["roots"] == staged["contract"]["roots"],
            "retained_heaps_changed",
        )
    else:
        require(
            receipt["heap"] == staged["heaps"][1]
            and receipt["root_oid"] == root_oids_by_name[PIN_TABLE]
            and receipt["retained_revision_ids"] == list(scope.revision_ids),
            "retained_heaps_changed",
        )
    revision = next(
        revision
        for publication, revision in scope.publication_revisions
        if publication == record_by_field["publication_identity"]
    )
    return staged, receipt, scope, revision, is_incumbent


def _require_binding_seal(record_by_field, inventory, staged):
    """Reuse the committed aggregate semantic seal, never a caller assertion."""
    inputs_by_key = inventory["binding_inputs"]
    seal = inventory["binding_input_seal"]
    require(
        seal
        == {
            "scope_digest": record_by_field["scope_digest"],
            "outcome_receipt_digest": record_by_field["receipt_digest"],
            "cohorts_digest": pricing_control_digest(inputs_by_key),
            "revision_count": sum(len(entry["identity"]["revisions"]) for entry in inputs_by_key.values()),
        },
        "aggregate_binding_seal_changed",
    )
    input_key = staged["contract"]["input_key"]
    entry = inputs_by_key[input_key]
    identity_by_field = entry["identity"]
    require(
        input_key == pricing_control_digest(identity_by_field)
        and entry["admission_inputs"] is True
        and entry["scope_digest"] == record_by_field["scope_digest"]
        and entry["outcome_receipt_digest"] == record_by_field["receipt_digest"]
        and sorted(member[1] for member in staged["contract"]["publications"]) == identity_by_field["revisions"]
        and record_by_field["healthporta_plan_id"] in identity_by_field["plans"],
        "aggregate_binding_membership_changed",
    )


async def _closed_heap(connection, admission, descriptor, *, pin, parent_oid=None):
    """Closed owner/ACL plus physical identity authenticates the previously sealed bytes."""
    expected = PreparationPinHeap(**descriptor).validate()
    require(expected.schema == pricing_control_schema(), "retained_heap_schema_changed")
    relation = await preparation_heap_catalog(
        connection,
        expected.schema,
        expected.name,
        owner_oid=admission.family["owner_oid"],
        reader_oids=admission.family["reader_oids"],
        pin=pin,
        parent_oid=parent_oid,
    )
    require(
        relation["oid"] == expected.oid
        and digest(_definition(relation)) == expected.definition_digest
        and digest(_document(relation["indexes"])) == expected.indexes_digest,
        "retained_heap_changed",
    )
    identity = await connection.fetchrow(
        "SELECT pg_relation_filenode($1::oid)::bigint AS filenode,"
        "(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid",
        expected.oid,
    )
    require(
        identity["filenode"] == expected.filenode and identity["database_oid"] == expected.database_oid,
        "retained_heap_identity_changed",
    )
    return storage_relation(expected.schema, expected.name)


async def require_retained_pricing_inputs(connection, admission):
    """Trust the protected aggregate seal while rechecking native immutable custody."""
    staged, receipt, _scope, _revision, is_incumbent = _retained_inventory(admission)
    await _closed_heap(connection, admission, staged["heaps"][0], pin=False)
    parent_oid = None if is_incumbent else receipt["root_oid"]
    await _closed_heap(connection, admission, staged["heaps"][1], pin=True, parent_oid=parent_oid)
