# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resident rollback quota and newly allocated before-images have different disk costs."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_nonprofile_capacity as capacity
from process import provider_directory_ordinary_retention_capacity as retention
from process import provider_directory_serving_capacity_contract as contract
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation
from tests.test_provider_directory_ordinary_retention_capacity import _captured_owner, _measured_request


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _initial_measurements(monkeypatch):
    directory = SimpleNamespace(db=SimpleNamespace(_transaction_binding=lambda: object()))
    producer, _admission = _captured_owner(monkeypatch, directory, "ab" * 32, initial=True)
    scope = producer.captured_inputs.scope_payload
    scope.update(max_retention_data_bytes=1_500_000, resident_retention_data_bytes=1_400_000)
    producer.captured_inputs.predecessor = {
        "profile": {"profile_target_bytes": 700_000, "evidence_target_bytes": 700_000}
    }
    # Keep the original candidate reservation; add only 100k of new retention allocation.
    producer.plan = replace(
        producer.plan,
        ordinary_scope_json=contract.canonical(scope),
        reservation_bytes=(
            ("data", 1_148_576),
            ("temp", 1_048_576),
            ("wal", dict(producer.plan.reservation_bytes)["wal"]),
        ),
    )
    request = _measured_request(producer)
    entries = producer.captured_inputs.retention_plan["families"]
    relations, annotations = list(request.relations), list(request.ordinary_relations)
    for ordinal in (0, 1):
        entry = entries[ordinal]
        relations[ordinal] = OwnedRelation("fixture", entry["source_relation"], entry["source_oid"], 700_000, "p")
        annotations[ordinal] = replace(annotations[ordinal], relation=entry["source_relation"], oid=entry["source_oid"])
    return producer, replace(
        request, plan=producer.plan, relations=tuple(relations), ordinary_relations=tuple(annotations)
    )


async def test_resident_initial_targets_count_in_quota_without_consuming_new_disk_twice(monkeypatch):
    producer, request = _initial_measurements(monkeypatch)
    assert sum(relation.total_bytes for relation in request.relations) > dict(producer.plan.reservation_bytes)["data"]
    assert retention.resident_retained_bytes(producer, request) == 1_400_000
    assert len(retention.retained_relations(producer, request)) == 12
    producer._assert_wal_budget = AsyncMock()
    producer._checked_relation_policies = lambda checked: capacity._CapacityProducer._checked_relation_policies(
        producer, checked
    )
    producer._raw_relation_policies = lambda _request: {}
    producer._native_relation_policies = lambda _request: {}
    producer._native_catalog_observations = AsyncMock(return_value={})
    monkeypatch.setattr(
        retention,
        "capture_retained_layout",
        AsyncMock(
            side_effect=lambda _producer, relation, _annotation: SimpleNamespace(
                relation_oid=relation.oid, effective_tablespace_oids=(42,)
            )
        ),
    )
    await capacity._CapacityProducer._assert_physical(producer, request, {"data_tablespace_oid": 42})
    assert retention.capture_retained_layout.await_count == 12


@pytest.mark.parametrize("changed", ["bytes", "oid", "name", "quota"])
def test_residency_cannot_mask_changed_initial_data_or_an_owned_copy(monkeypatch, changed):
    producer, request = _initial_measurements(monkeypatch)
    relation = request.relations[0]
    if changed == "bytes":
        relation = replace(relation, total_bytes=700_001)
    if changed == "oid":
        relation = replace(relation, oid=999)
    if changed == "name":
        relation = replace(relation, relation="pd_profile_before_" + "f" * 20)
    if changed == "quota":
        relation = replace(relation, total_bytes=2_000_000)
    request = replace(request, relations=(relation, *request.relations[1:]))
    with pytest.raises(RuntimeError, match="retention_changed"):
        retention.resident_retained_bytes(producer, request)


def test_owned_copies_have_no_resident_discount(monkeypatch):
    directory = SimpleNamespace(db=object())
    producer, _admission = _captured_owner(monkeypatch, directory, "ab" * 32)
    request = _measured_request(producer)
    assert retention.resident_retained_bytes(producer, request) == 0
    scope = producer.captured_inputs.scope_payload
    scope["resident_retention_data_bytes"] = 1
    with pytest.raises(ValueError, match="resident_retention_kind"):
        contract.validate_scope(scope)


@pytest.mark.parametrize("resident", [0, 100, 200])
def test_only_new_retention_allocation_extends_reviewed_data_budget(monkeypatch, resident):
    from tests.provider_directory_serving_capacity_signing_test_support import serving_case

    case = serving_case(initial_mode=True)
    scope = case.plan.payload["ordinary_scope"]
    scope.update(max_retention_data_bytes=200, resident_retention_data_bytes=resident)
    old = case.plan.payload
    old.pop("ordinary_scope")
    old.update(contract_id=contract.BASE_GEOMETRY, admission_purpose="cms_nonprofile")
    extended = contract.extend_plan_values(old, scope, paired_profile_wal=100)
    assert dict(extended["reservation_bytes"])["data"] - dict(old["reservation_bytes"])["data"] == 200 - resident
    assert extended["ordinary_scope"]["max_retention_data_bytes"] == 200


def test_operator_cannot_budget_less_than_resident_retention():
    from tests.provider_directory_serving_capacity_request_test_support import ordinary_scope

    scope = ordinary_scope(initial_mode=True)
    scope["resident_retention_data_bytes"] = scope["max_retention_data_bytes"] + 1
    with pytest.raises(ValueError, match="resident_retention_budget"):
        contract.validate_scope(scope)
