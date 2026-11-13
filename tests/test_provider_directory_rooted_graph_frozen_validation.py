# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named frozen outputs retain full logical inputs, predicates and content validation."""

import asyncio
import hashlib
import importlib
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_rooted_graph_bulk as bulk
from process import provider_directory_rooted_graph_current_preflight as preflight
from process import provider_directory_rooted_graph_header_checks as header_checks
from process import provider_directory_rooted_graph_publication_candidate as candidate
from process import provider_directory_rooted_graph_source_runtime as source_runtime
from process.provider_directory_dataset_scoped_publication_contract import ROOTED_COMBINED_VARIANT
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_resource_hash import SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
    rooted_graph_query_filter_validation_sql,
)
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.provider_directory_rooted_graph_publication_test_support import (
    dataset_identity,
    exact_current,
    twin_admission,
)
from tests.test_provider_directory_rooted_graph_handoff import rooted_handoff as rooted_handoff

directory = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, PermissionError("denied"), asyncio.CancelledError()])
async def test_capture_resolves_exact_names_or_propagates_error(rooted_handoff, failure):
    connection = SimpleNamespace(execute=AsyncMock(side_effect=failure))
    relations_by_root = {root: table["name"] for root, table in rooted_handoff.document["tables"].items()}
    operation = candidate._capture_rooted_tables(connection, SimpleNamespace(schema="candidate"), relations_by_root)
    if failure is not None:
        with pytest.raises(type(failure)) as caught:
            await operation
        assert caught.value is failure
    else:
        assert await operation == {name: {"schema": "candidate", "name": name} for name in relations_by_root.values()}
        assert [call.args[0] for call in connection.execute.await_args_list] == [
            "SELECT 1 FROM " + storage_relation("candidate", name) + " LIMIT 0" for name in relations_by_root.values()
        ]


@pytest.fixture
def builder(rooted_handoff):
    scope = rooted_handoff
    scope.runtime.catalog_schema = "catalog"
    scope.runtime.candidate_policy = SimpleNamespace(schema="candidate")
    scope.runtime.run_id = "run_" + "e" * 32
    scope.runtime.operation = {"operation_key": "a" * 64}
    scope.identity, scope.admission = dataset_identity(), twin_admission()
    scope.relations = {root: table["name"] for root, table in scope.document["tables"].items()}
    scope.snapshot = {"acquisition": {"request_failure_coverage": None}}
    scope.lineage = {"cohort_id": "synthetic-cohort", "cohort": {"is_current": True}}
    scope.proof = SimpleNamespace(
        snapshot_json='{"header":"root"}', input_fence_json='{"roots":{}}', dataset_sha256="b" * 64, resource_count=1
    )
    scope.connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=AssertionError("unexpected query before recipe validation")),
        execute=AsyncMock(),
        fetchval=AsyncMock(
            side_effect=[
                {"closed": True, "valid": True, "rows": 0, "last_query_id": None},
                {"closed": True, "valid": True, "rows": 0, "last_query_id": None},
                json.dumps(scope.snapshot),
                "source-attempt-one",
                json.dumps(scope.lineage),
            ]
        ),
    )
    return scope


async def _seal(scope):
    return await candidate.seal_rooted_handoff(
        scope.runtime,
        SimpleNamespace(connection=scope.connection),
        scope.identity,
        scope.admission,
        scope.relations,
        {"Practitioner": 1},
        "a" * 64,
        scope.proof,
    )


@pytest.mark.asyncio
async def test_seal_persists_exact_builder_receipt_and_discoverable_pointer(builder):
    scope = builder
    handoff = await _seal(scope)
    document = handoff.document
    assert document["identity"]["dataset_id"] == scope.identity.dataset_id
    assert document["identity"]["metadata"] == {
        "source_ids": [scope.identity.source_id],
        "source_run_id": scope.runtime.run_id,
        "source_attempt_id": "source-attempt-one",
        "query_filter_recipe_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
    }
    assert document["predecessor_proof"] == {
        "snapshot": {"header": "root"},
        "input_fence": {"roots": {}},
        "dataset_hash": "b" * 64,
        "resource_count": 1,
    }
    assert document["snapshot"] == scope.snapshot and document["lineage"] == scope.lineage
    assert document["admission"]["previous_dataset_id"] == scope.identity.root_dataset_id
    assert document["tables"] == scope.document["tables"]
    update, pointer = scope.connection.execute.await_args_list[-2:]
    assert update.args[1] == pointer.args[1] == scope.identity.dataset_id
    assert json.loads(update.args[2]) == document
    assert update.args[0].startswith("UPDATE " + storage_relation("candidate", scope.relations[candidate.PARENT]))
    assert pointer.args[0].startswith('INSERT INTO "candidate"."provider_directory_endpoint_dataset" SELECT * FROM ')
    snapshot_call = next(
        call for call in scope.connection.fetchval.await_args_list if call.args[1:] == (scope.identity.dataset_id,)
    )
    assert snapshot_call.args[1] == scope.identity.dataset_id
    for root in (candidate.HEADER, candidate.PARENT):
        assert storage_relation("candidate", scope.relations[root]) in snapshot_call.args[0]
    assert storage_relation("catalog", candidate.HEADER) in snapshot_call.args[0]
    assert storage_relation("catalog", candidate.PARENT) in snapshot_call.args[0]


@pytest.mark.asyncio
async def test_seal_missing_original_source_attempt_never_writes_receipt(builder):
    builder.connection.fetchval.side_effect = [
        {"closed": True, "valid": True, "rows": 0, "last_query_id": None},
        {"closed": True, "valid": True, "rows": 0, "last_query_id": None},
        builder.snapshot,
        None,
        builder.lineage,
    ]
    with pytest.raises(GenericCandidateError, match="rooted_source_attempt_changed"):
        await _seal(builder)
    assert not any(
        call.args[0].startswith("UPDATE ") or call.args[0].startswith("INSERT ")
        for call in builder.connection.execute.await_args_list
    )


def _semantic_resources(document):
    resources = []
    for kind, identity in (("Organization", "organization-one"), ("Practitioner", "practitioner-one")):
        payload = {"resource_id": identity}
        resources.append(
            {
                "resource_type": kind,
                "resource_id": identity,
                "payload_json": payload,
                "acquired_resource_sha256": None,
                "payload_hash": directory.resource_payload_sha256_for_contract(
                    payload, SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT, resource_type=kind
                ),
            }
        )
    document["identity"]["semantic_projection_as_of"] = "2026-08-10"
    document["counts"] = {"Organization": 1, "Practitioner": 1}
    document["dataset_hash"] = hashlib.sha256(
        "\n".join(
            candidate.candidate_json([row[key] for key in ("resource_type", "resource_id", "payload_hash")])
            for row in resources
        ).encode()
    ).hexdigest()
    resources[0]["payload_json"] = json.dumps(resources[0]["payload_json"])
    return resources


def _frozen_connection(scope):
    async def fetchval(query, *arguments):
        if "WHERE dataset_id IS DISTINCT FROM $1" in query:
            scope.inventory_checks += 1
            return scope.invalid_inventory
        if query == scope.fence_query:
            assert arguments == (scope.proof.current.dataset_id,)
            return json.dumps(scope.observed_fence)
        if query == rooted_graph_query_filter_validation_sql("candidate"):
            assert arguments[0] in {
                scope.document["admission"][name]
                for name in ("publication_acquisition_id", "comparison_acquisition_id")
            }
            assert arguments[1:] == ("", 4096)
            return {"closed": True, "valid": scope.query_recipes_valid, "rows": 0, "last_query_id": None}
        assert query == candidate._frozen_validation_query(
            None,
            "catalog",
            scope.document,
            predecessor_resource=scope.predecessor_resource,
            comparison_inputs=scope.document["inputs"],
        )
        assert arguments == (scope.document["identity"]["dataset_id"], scope.proof.current.dataset_id)
        return scope.predicates_valid

    scope.cursor = SimpleNamespace(fetch=AsyncMock(side_effect=[scope.resources[:1], scope.resources[1:], []]))
    return SimpleNamespace(
        fetchval=AsyncMock(side_effect=fetchval),
        execute=AsyncMock(),
        cursor=AsyncMock(return_value=scope.cursor),
        is_in_transaction=lambda: scope.in_transaction,
    )


@pytest.fixture
def frozen(rooted_handoff, monkeypatch):
    scope = rooted_handoff
    scope.resources = _semantic_resources(scope.document)
    scope.input_fence = {
        "roots": {"provider_directory_dataset_resource": {"schema": "previous_payload", "name": "resources"}},
        "origin": True,
    }
    scope.observed_fence = deepcopy(scope.input_fence)
    scope.proof = SimpleNamespace(
        current=exact_current(),
        snapshot_json='{"root":"snapshot"}',
        input_fence_json=candidate.candidate_json(scope.input_fence),
        dataset_sha256="2" * 64,
        resource_count=1,
    )
    scope.document["predecessor_proof"] = {
        "snapshot": {"root": "snapshot"},
        "input_fence": scope.input_fence,
        "dataset_hash": "2" * 64,
        "resource_count": 1,
    }
    scope.document["snapshot"] = {"acquisition": {"request_failure_coverage": None}}
    scope.predecessor_resource = '"previous_payload"."resources"'
    scope.fence_query = root_input_fence_sql("catalog", "$1", variant=scope.proof.current.variant)
    scope.inventory_checks, scope.invalid_inventory = 0, False
    scope.predicates_valid, scope.in_transaction, scope.query_recipes_valid = True, True, True
    scope.connection = _frozen_connection(scope)
    scope.header = AsyncMock()
    monkeypatch.setattr(header_checks, "validate_rooted_header_check", scope.header)
    monkeypatch.setattr(preflight, "current_root_proof", lambda database: scope.proof)
    monkeypatch.setattr(
        candidate,
        "dataset_source_relations",
        AsyncMock(return_value={candidate.RESOURCE: {"schema": "previous_payload", "name": "resources"}}),
    )
    monkeypatch.setattr(source_runtime, "rooted_input_relations", AsyncMock(return_value=scope.document["inputs"]))
    scope.coverage = AsyncMock(return_value=None)
    monkeypatch.setattr(bulk, "named_request_coverage", scope.coverage)
    return scope


async def _validate(scope):
    handoff = FHIRSourceCandidateHandoff(candidate.candidate_json(scope.document))
    return await candidate.validate_frozen_rooted_handoff(
        scope.connection, handoff, scope.runtime.manifest, protected_owner_role="protected_owner"
    )


@pytest.mark.asyncio
async def test_frozen_validation_keeps_original_predicates_semantics_and_double_inventory(frozen):
    validation = await _validate(frozen)
    assert validation == {
        "dataset_hash": frozen.document["dataset_hash"],
        "resource_count": 2,
        "resource_counts": frozen.document["counts"],
        "predecessor_input_fence": frozen.input_fence,
    }
    assert frozen.inventory_checks == 14
    assert all(call.args == (4096,) for call in frozen.cursor.fetch.await_args_list)
    query, dataset_id = frozen.connection.cursor.await_args.args
    assert storage_relation("candidate", frozen.document["tables"][candidate.RESOURCE]["name"]) in query
    assert "ORDER BY resource_type,resource_id" in query and dataset_id == frozen.document["identity"]["dataset_id"]
    assert len(frozen.connection.execute.await_args_list) == 2
    assert all("IN ACCESS SHARE MODE NOWAIT" in call.args[0] for call in frozen.connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_frozen_rooted_successor_binds_and_rechecks_its_protected_predecessor(frozen, changed):
    frozen.proof.current = exact_current(variant=ROOTED_COMBINED_VARIANT)
    frozen.input_fence = {
        "dataset_id": frozen.proof.current.dataset_id,
        "variant": ROOTED_COMBINED_VARIANT,
        "header": {"dataset_hash": "a" * 64},
        "parent": {"dataset_hash": "b" * 64},
    }
    frozen.observed_fence = deepcopy(frozen.input_fence)
    frozen.proof.input_fence_json = candidate.candidate_json(frozen.input_fence)
    frozen.document["predecessor_proof"]["input_fence"] = frozen.input_fence
    frozen.fence_query = root_input_fence_sql("catalog", "$1", variant=ROOTED_COMBINED_VARIANT)
    if changed:
        frozen.observed_fence["header"]["dataset_hash"] = "c" * 64
        with pytest.raises(GenericCandidateError, match="rooted_input_changed"):
            await _validate(frozen)
        frozen.cursor.fetch.assert_not_awaited()
    else:
        validation = await _validate(frozen)
        assert validation["predecessor_input_fence"] == frozen.input_fence
        assert frozen.inventory_checks == 14


def _change_projection(scope):
    payload_by_field = scope.resources[1]["payload_json"]
    payload_by_field.update(age_years=30, age_as_of="2026-08-09")
    scope.resources[1]["payload_hash"] = directory.resource_payload_sha256_for_contract(
        payload_by_field, SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT, resource_type="Practitioner"
    )


def _damage_frozen_evidence(scope, damage):
    changes_by_damage = {
        "transaction": lambda: setattr(scope, "in_transaction", False),
        "identity": lambda: scope.document["identity"]["metadata"].update(source_run_id="other-run"),
        "predecessor": lambda: scope.document["predecessor_proof"].update(dataset_hash="3" * 64),
        "fence": lambda: scope.observed_fence.update(origin=False),
        "coverage": lambda: setattr(scope.coverage, "return_value", {"changed": True}),
        "predicates": lambda: setattr(scope, "predicates_valid", False),
        "payload": lambda: scope.resources[0].update(payload_hash="4" * 64),
        "subset": lambda: scope.resources[0].update(acquired_resource_sha256="5" * 64),
        "projection": lambda: _change_projection(scope),
        "digest": lambda: scope.document.update(dataset_hash="6" * 64),
        "count": lambda: scope.document["counts"].update(Practitioner=2),
    }
    changes_by_damage[damage]()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,code",
    [
        ("transaction", "transaction_required"),
        ("identity", "rooted_source_attempt_changed"),
        ("predecessor", "rooted_predecessor_changed"),
        ("fence", "rooted_input_changed"),
        ("coverage", "rooted_request_coverage_changed"),
        ("predicates", "rooted_candidate_invalid"),
        ("payload", "rooted_payload_hash_changed"),
        ("subset", "semantic_resource_subset_invalid"),
        ("projection", "semantic_projection_as_of_mismatch"),
        ("digest", "rooted_hash_changed"),
        ("count", "rooted_hash_changed"),
    ],
)
async def test_frozen_validation_refuses_consequential_drift_before_receipt(frozen, damage, code):
    _damage_frozen_evidence(frozen, damage)
    with pytest.raises((GenericCandidateError, RuntimeError, ValueError), match=code):
        await _validate(frozen)
    if damage in {"transaction", "identity"}:
        frozen.connection.execute.assert_not_awaited()
    if damage in {"transaction", "identity", "predecessor", "fence", "coverage", "predicates"}:
        frozen.connection.cursor.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "foreign-data", "missing-column"])
async def test_frozen_inventory_refuses_names_and_data(frozen, damage):
    if damage == "missing":
        frozen.document["tables"].pop(candidate.RESOURCE)
    elif damage == "foreign-data":
        frozen.invalid_inventory = True
    else:
        frozen.connection.fetchval.side_effect = asyncpg.UndefinedColumnError("missing")
    with pytest.raises((GenericCandidateError, asyncpg.UndefinedColumnError)):
        await _validate(frozen)
