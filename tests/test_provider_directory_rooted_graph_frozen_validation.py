# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen output invariants with scripted IO, without private Source authority."""

import hashlib
import importlib
import json
import re
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import migration_rooted_publication_freeze as freeze
from db.plan_release_storage import storage_relation
from process import provider_directory_rooted_graph_current_preflight as preflight
from process import provider_directory_rooted_graph_foreign_guards as foreign
from process import provider_directory_rooted_graph_header_checks as header_checks
from process import provider_directory_rooted_graph_publication_candidate as candidate
from process.plan_release_storage_catalog import CATALOG_SQL
from process.provider_directory_dataset_scoped_publication_contract import ROOTED_COMBINED_VARIANT
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_storage import _table_proof
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
from tests.test_provider_directory_generic_source_storage import _catalog_row
from tests.test_provider_directory_rooted_graph_handoff import rooted_handoff as rooted_handoff
from tests.test_provider_directory_rooted_graph_publication_freeze import SCHEMA, _current_bodies

directory = importlib.import_module("process.provider_directory_fhir")


def _catalog_inventory(document, owner):
    catalog_rows = []
    for table in document["tables"].values():
        catalog_row = _catalog_row() | {
            "oid": table["oid"],
            "relname": table["name"],
            "relowner": owner,
            "nspname": document["storage"]["storage_schema"],
        }
        table.update(layout_sha256=_table_proof(catalog_row, private=True)["layout_sha256"])
        table["relfilenode"] = table["oid"] + 1000
        catalog_rows.append(catalog_row)
    return catalog_rows


def _encoded_catalog(catalog_rows):
    encoded_rows = deepcopy(catalog_rows)
    for catalog_row in encoded_rows:
        for key in ("columns", "constraints", "indexes"):
            catalog_row[key] = json.dumps(catalog_row[key])
    return encoded_rows


def _capture_connection(document, catalog_rows, *, encoded=True):
    filenodes = [{"oid": table["oid"], "relfilenode": table["relfilenode"]} for table in document["tables"].values()]
    return SimpleNamespace(
        fetchval=AsyncMock(return_value=document["storage"]["owner_oid"]),
        fetch=AsyncMock(
            side_effect=[catalog_rows, filenodes, _encoded_catalog(catalog_rows) if encoded else catalog_rows]
        ),
        execute=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True])
async def test_capture_binds_seven_real_table_proofs_and_physical_generations(rooted_handoff, encoded):
    document = rooted_handoff.document
    catalog_rows = _catalog_inventory(document, document["storage"]["owner_oid"])
    connection = _capture_connection(document, catalog_rows, encoded=encoded)
    policy = SimpleNamespace(schema="candidate", owner_role="builder")
    relations_by_root = {root: table["name"] for root, table in document["tables"].items()}
    tables, filenodes, catalog = await candidate._capture_rooted_tables(connection, policy, relations_by_root)
    assert tables == {row["relname"]: _table_proof(row, private=True) for row in catalog_rows}
    assert filenodes == {table["oid"]: table["relfilenode"] for table in document["tables"].values()}
    assert catalog == catalog_rows
    first, physical, last = connection.fetch.await_args_list
    assert first.args == (
        CATALOG_SQL,
        [storage_relation("candidate", name) for name in relations_by_root.values()],
        4,
        [],
    )
    assert physical.args[1] == [table["oid"] for table in tables.values()]
    assert last.args == (CATALOG_SQL, [str(table["oid"]) for table in tables.values()], 4, [])
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_capture_rejects_open_native_custody_before_filenodes(rooted_handoff):
    catalog_rows = _catalog_inventory(rooted_handoff.document, 4)
    catalog_rows[0]["direct_acl_closed"] = False
    connection = _capture_connection(rooted_handoff.document, catalog_rows)
    relations_by_root = {root: table["name"] for root, table in rooted_handoff.document["tables"].items()}
    with pytest.raises(GenericCandidateError, match="storage_custody_changed"):
        await candidate._capture_rooted_tables(
            connection, SimpleNamespace(schema="candidate", owner_role="builder"), relations_by_root
        )
    assert connection.fetch.await_count == 1
    connection.execute.assert_not_awaited()


@pytest.fixture
def builder(rooted_handoff):
    scope = rooted_handoff
    scope.catalog_rows = _catalog_inventory(scope.document, 4)
    scope.connection = _capture_connection(scope.document, scope.catalog_rows)
    scope.runtime.catalog_schema = "catalog"
    scope.runtime.candidate_policy = SimpleNamespace(schema="candidate", owner_role="builder")
    scope.runtime.run_id = "run_" + "e" * 32
    scope.runtime.operation = {"operation_key": "a" * 64}
    scope.identity = dataset_identity()
    scope.admission = twin_admission()
    scope.relations = {root: table["name"] for root, table in scope.document["tables"].items()}
    scope.snapshot = {"foreign_guards": {"guard": "immutable"}}
    scope.lineage = {"cohort_id": "synthetic-cohort", "cohort": {"is_current": True}}
    scope.proof = SimpleNamespace(
        snapshot_json='{"header":"root"}', input_fence_json='{"roots":{}}', dataset_sha256="b" * 64, resource_count=1
    )
    scope.connection.fetchval.side_effect = [
        {"closed": True, "valid": True, "rows": 0, "last_query_id": None},
        {"closed": True, "valid": True, "rows": 0, "last_query_id": None},
        4,
        json.dumps(scope.snapshot),
        "source-attempt-one",
        json.dumps(scope.lineage),
    ]
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
    assert document["catalog"] == sorted(scope.catalog_rows, key=lambda row: row["oid"])
    assert document["tables"] == scope.document["tables"]
    update, pointer = scope.connection.execute.await_args_list
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
        4,
        builder.snapshot,
        None,
        builder.lineage,
    ]
    with pytest.raises(GenericCandidateError, match="rooted_source_attempt_changed"):
        await _seal(builder)
    builder.connection.execute.assert_not_awaited()


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
        if query == "SELECT oid::bigint FROM pg_roles WHERE rolname=$1":
            assert arguments == ("protected_owner",)
            return 40
        if query.startswith("SELECT relfilenode"):
            scope.generation_checks += 1
            generation = next(
                table["relfilenode"] for table in scope.document["tables"].values() if table["oid"] == arguments[0]
            )
            return generation + int(scope.generation_drift and scope.generation_checks > 7)
        if query == scope.fence_query:
            assert len(arguments) == max(map(int, re.findall(r"\$(\d+)", query)))
            assert arguments == (scope.proof.current.dataset_id, candidate.candidate_json(scope.input_fence["roots"]))
            return json.dumps(scope.observed_fence)
        if query.startswith("SELECT prosrc FROM pg_proc"):
            return scope.body
        if query == rooted_graph_query_filter_validation_sql("catalog"):
            assert arguments[0] in {
                scope.document["admission"][name]
                for name in ("publication_acquisition_id", "comparison_acquisition_id")
            }
            assert arguments[1:] == ("", 4096)
            return {"closed": True, "valid": scope.query_recipes_valid, "rows": 0, "last_query_id": None}
        assert query == candidate._frozen_validation_query(scope.body, "catalog", scope.document)
        assert arguments == (scope.document["identity"]["dataset_id"], scope.proof.current.dataset_id)
        return scope.predicates_valid

    scope.cursor = SimpleNamespace(fetch=AsyncMock(side_effect=[scope.resources[:1], scope.resources[1:], []]))
    return SimpleNamespace(
        fetchval=AsyncMock(side_effect=fetchval),
        fetch=AsyncMock(return_value=scope.catalog_rows),
        execute=AsyncMock(),
        cursor=AsyncMock(return_value=scope.cursor),
        is_in_transaction=lambda: scope.in_transaction,
    )


@pytest.fixture
def frozen(rooted_handoff, monkeypatch):
    scope = rooted_handoff
    scope.catalog_rows = _catalog_inventory(scope.document, 40)
    scope.resources = _semantic_resources(scope.document)
    scope.input_fence = {"roots": {"provider_directory_dataset_resource": [11]}, "origin": True}
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
    scope.document["snapshot"]["foreign_guards"] = {"guard": "immutable"}
    scope.body = _current_bodies()[freeze.ROOTED].replace(SCHEMA, "catalog")
    scope.fence_query = root_input_fence_sql("catalog", "$1", "$2::jsonb", variant=scope.proof.current.variant)
    scope.generation_checks, scope.generation_drift = 0, False
    scope.predicates_valid, scope.in_transaction = True, True
    scope.query_recipes_valid = True
    scope.connection = _frozen_connection(scope)
    scope.foreign = AsyncMock(return_value={"guard": "immutable"})
    scope.header = AsyncMock()
    monkeypatch.setattr(foreign, "capture_parent_foreign_guards", scope.foreign)
    monkeypatch.setattr(header_checks, "validate_rooted_header_check", scope.header)
    monkeypatch.setattr(preflight, "current_root_proof", lambda database: scope.proof)
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
        "handoff_sha256": FHIRSourceCandidateHandoff(candidate.candidate_json(frozen.document)).document_sha256,
        "dataset_hash": frozen.document["dataset_hash"],
        "resource_count": 2,
        "resource_counts": frozen.document["counts"],
        "predecessor_input_fence": frozen.input_fence,
    }
    assert frozen.generation_checks == 14 and frozen.connection.fetch.await_count == 2
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
        "roots": {},
        "catalog": {
            "control_epochs": {"header": "10", "parent": "11"},
            "published_proof": {"handoff_sha256": "a" * 64, "publication_sha256": "b" * 64},
        },
    }
    frozen.observed_fence = deepcopy(frozen.input_fence)
    frozen.proof.input_fence_json = candidate.candidate_json(frozen.input_fence)
    frozen.document["predecessor_proof"]["input_fence"] = frozen.input_fence
    frozen.fence_query = root_input_fence_sql("catalog", "$1", "$2::jsonb", variant=ROOTED_COMBINED_VARIANT)
    if changed:
        frozen.observed_fence["catalog"]["published_proof"]["handoff_sha256"] = "c" * 64
        with pytest.raises(GenericCandidateError, match="rooted_input_changed"):
            await _validate(frozen)
        frozen.cursor.fetch.assert_not_awaited()
    else:
        validation = await _validate(frozen)
        assert validation["predecessor_input_fence"] == frozen.input_fence
        assert frozen.generation_checks == 14


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
        "foreign": lambda: setattr(scope.foreign, "return_value", {"guard": "changed"}),
        "validator": lambda: setattr(scope, "body", "SELECT true;"),
        "predicates": lambda: setattr(scope, "predicates_valid", False),
        "payload": lambda: scope.resources[0].update(payload_hash="4" * 64),
        "subset": lambda: scope.resources[0].update(acquired_resource_sha256="5" * 64),
        "projection": lambda: _change_projection(scope),
        "digest": lambda: scope.document.update(dataset_hash="6" * 64),
        "count": lambda: scope.document["counts"].update(Practitioner=2),
        "generation_recheck": lambda: setattr(scope, "generation_drift", True),
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
        ("foreign", "rooted_foreign_guard_changed"),
        ("validator", "rooted_validator_changed"),
        ("predicates", "rooted_candidate_invalid"),
        ("payload", "rooted_payload_hash_changed"),
        ("subset", "semantic_resource_subset_invalid"),
        ("projection", "semantic_projection_as_of_mismatch"),
        ("digest", "rooted_hash_changed"),
        ("count", "rooted_hash_changed"),
        ("generation_recheck", "rooted_physical_generation_changed"),
    ],
)
async def test_frozen_validation_refuses_consequential_drift_before_receipt(frozen, damage, code):
    scope = frozen
    _damage_frozen_evidence(scope, damage)
    with pytest.raises((GenericCandidateError, RuntimeError, ValueError), match=code):
        await _validate(scope)
    if damage in {"transaction", "identity"}:
        scope.connection.execute.assert_not_awaited()
    if damage in {"transaction", "identity", "predecessor", "fence", "foreign", "validator", "predicates"}:
        scope.connection.cursor.assert_not_awaited()
    if damage == "generation_recheck":
        assert scope.generation_checks == 8
    else:
        assert scope.generation_checks <= 7


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,code",
    [
        ("missing", "rooted_inventory"),
        ("owner", "rooted_inventory"),
        ("layout", "rooted_inventory"),
        ("hooks", "storage_custody"),
        ("generation", "rooted_physical_generation"),
    ],
)
async def test_frozen_inventory_authenticates_layout_owner_and_generation(frozen, damage, code):
    scope = frozen
    if damage == "missing":
        scope.catalog_rows.pop()
    elif damage == "owner":
        scope.catalog_rows[0]["relowner"] = 41
    elif damage == "layout":
        scope.catalog_rows[0]["columns"].append(["extra", "text", False, None])
    elif damage == "hooks":
        scope.catalog_rows[0]["inert"] = False
    else:
        scope.document["tables"][candidate.RESOURCE]["relfilenode"] += 1
        scope.connection.fetchval = AsyncMock(side_effect=[40, 9999])
    with pytest.raises(GenericCandidateError, match=code):
        await candidate._require_frozen_inventory(scope.connection, scope.document, "protected_owner")
    assert scope.connection.execute.await_count == 1
