# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named publication preserves complete data validation and historical query contracts."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db import migration_rooted_publication_freeze as freeze
from process import provider_directory_rooted_graph_publication_candidate as candidate
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_rooted_graph_publication_candidate import ROOTS
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
)
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration
from tests.test_source_validator_argument_types_migration import _load

SCHEMA = "snapshot_schema"


def _current_bodies(names=freeze.ORIGINAL_DIGESTS):
    bodies_by_name = {}
    if freeze.WORK_GUARD in names:
        sql = _load("20260909120000_fhir_request_failure_budget.py")._work_guard_sql(SCHEMA)
        bodies_by_name[freeze.WORK_GUARD] = sql.split("$guard$", 2)[1]
    for (name, _kinds), attributes in _current_functions().items():
        if name not in names:
            continue
        body = attributes[-1]
        for caller, old, new, count in _migration()._calls(SCHEMA):
            if name == caller:
                assert body.count(old) == count
                body = body.replace(old, new)
        bodies_by_name[name] = body
    assert bodies_by_name.keys() == set(names)
    return bodies_by_name


@pytest.mark.parametrize("name", freeze.ORIGINAL_DIGESTS)
def test_historical_wrapper_retains_every_original_data_predicate(name):
    original = _current_bodies()[name]
    successor = freeze.wrapped_body(name, SCHEMA, original)
    if name == freeze.SINGLE_ROOT_GUARD:
        old, replacement = freeze._SINGLE_ROOT_SELECT
        assert successor.replace(replacement, old, 1) == original
    elif name == freeze.GUARD:
        assert successor.replace("\n" + freeze._guard_extension(SCHEMA), "", 1) == original
    elif name == freeze.WORK_GUARD:
        assert successor.replace(freeze._work_guard_extension(), "", 1) == original
    else:
        assert freeze.original_query(successor) == original


def test_single_root_successor_keeps_record_types():
    original = _current_bodies()[freeze.SINGLE_ROOT_GUARD]
    successor = freeze.wrapped_body(freeze.SINGLE_ROOT_GUARD, SCHEMA, original)
    old, new = freeze._SINGLE_ROOT_SELECT
    assert successor.replace(new, old, 1) == original
    assert successor.count("::text AS acquisition_source_id") == 2
    assert successor.count("::text AS acquisition_endpoint_id") == 2
    assert successor.count("INTO current_root") == 2
    assert "FOR SHARE OF header, parent, graph_endpoint" in successor
    assert "FOR SHARE OF header, parent;" in successor


def test_named_receipt_checks_complete_headers_counts_hashes_and_lineage():
    query = freeze.native_receipt_sql(SCHEMA)
    assert "count(*)=1 AND bool_and(" in query and "LIMIT 1" not in query
    for predicate in (
        "b->>'contract'",
        "b->>'phase'='ready_for_freeze'",
        "b->'identity'->>'dataset_id'=h.dataset_id",
        "h.resource_count=(SELECT sum(value::bigint) FROM jsonb_each_text(b->'counts'))",
        "h.resource_count=p.resource_count AND h.status=p.status AND h.is_current=p.is_current",
        "h.dataset_hash=b->>'dataset_hash' AND p.dataset_hash=h.dataset_hash",
        "b->'snapshot'->'header'=to_jsonb(h)",
        "b->'snapshot'->'parent'=(to_jsonb(p)",
    ):
        assert predicate in query
    assert freeze._lineage_metadata_sql(SCHEMA) in query
    assert "WHERE h.dataset_id=candidate_dataset_id" in query
    assert "coalesce((SELECT" in query and query.endswith(",false)")


@pytest.fixture
def named_handoff():
    """Supply current named tables and complete logical source identities."""
    binding = FHIRSourceStorageBinding(SCHEMA, "candidate")
    storage_by_field = asdict(binding)
    proof = SimpleNamespace(
        current=SimpleNamespace(dataset_id="previous", variant="rooted_combined"),
        snapshot_json='{"header":"previous"}',
        input_fence_json='{"root":"pinned"}',
        dataset_sha256="b" * 64,
        resource_count=1,
    )
    document_by_field = {
        "contract": freeze.CONTRACT,
        "phase": "ready_for_freeze",
        "operation": {},
        "preparation_id": "12345678-1234-5678-1234-567812345678",
        "storage": storage_by_field,
        "identity": {
            "dataset_id": "pdrgpd_" + "d" * 48,
            "source_id": "example-source",
            "metadata": {
                "source_ids": ["example-source"],
                "source_run_id": "run_" + "e" * 32,
                "source_attempt_id": "attempt-one",
                "query_filter_recipe_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
            },
        },
        "admission": {"publication_acquisition_id": "publication", "comparison_acquisition_id": None},
        "tables": {
            root: {"schema": "candidate", "name": "candidate_" + str(index)} for index, root in enumerate(ROOTS)
        },
        "inputs": {
            "provider_directory_rooted_graph_" + root: {
                "schema": "candidate",
                "name": "provider_directory_rooted_graph_" + root,
            }
            for root in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
        },
        "dataset_hash": "a" * 64,
        "counts": {"Practitioner": 1},
        "snapshot": {"acquisition": {"request_failure_coverage": {"completed": 1}}},
        "lineage": {},
        "predecessor_proof": {
            "snapshot": json.loads(proof.snapshot_json),
            "input_fence": json.loads(proof.input_fence_json),
            "dataset_hash": proof.dataset_sha256,
            "resource_count": proof.resource_count,
        },
    }
    return SimpleNamespace(document=document_by_field, binding=binding, proof=proof)


@pytest.mark.parametrize("change", [None, "missing", "extra", "schema", "duplicate", "input"])
def test_named_inventory_requires_seven_unique_scoped_outputs(named_handoff, change):
    document = named_handoff.document
    first, second = list(document["tables"].values())[:2]
    if change is None:
        assert candidate.validate_rooted_handoff_document(document, asdict(named_handoff.binding)) is document
        return
    mutations_by_name = {
        "missing": lambda: document["tables"].pop(candidate.HEADER),
        "extra": lambda: first.update(unexpected=True),
        "schema": lambda: first.update(schema="unrelated"),
        "duplicate": lambda: second.update(first),
        "input": lambda: next(iter(document["inputs"].values())).update(schema="unrelated"),
    }
    mutations_by_name[change]()
    with pytest.raises(GenericCandidateError, match="rooted_inventory_changed"):
        candidate.validate_rooted_handoff_document(document, asdict(named_handoff.binding))


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [False, True, None])
async def test_named_inventory_locks_every_table_and_rejects_other_datasets(named_handoff, invalid):
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=invalid))
    if invalid is False:
        await candidate._require_frozen_inventory(connection, named_handoff.document, "configured_owner")
        assert connection.fetchval.await_count == len(ROOTS)
    else:
        with pytest.raises(GenericCandidateError, match="rooted_inventory_changed"):
            await candidate._require_frozen_inventory(connection, named_handoff.document, "configured_owner")
        assert connection.fetchval.await_count == 1
    statement = connection.execute.call_args.args[0]
    assert "IN ACCESS SHARE MODE NOWAIT" in statement
    for table in named_handoff.document["tables"].values():
        assert freeze.storage_relation(table["schema"], table["name"]) in statement
    for call in connection.fetchval.call_args_list:
        assert "dataset_id IS DISTINCT FROM $1" in call.args[0]
        assert call.args[1] == named_handoff.document["identity"]["dataset_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False, None])
async def test_header_check_uses_original_data_expression_and_check_null_semantics(named_handoff, accepted):
    from process import provider_directory_rooted_graph_header_checks as checks

    connection = SimpleNamespace(fetchval=AsyncMock(return_value=accepted), execute=AsyncMock())
    if accepted is True:
        await checks.validate_rooted_header_check(connection, named_handoff.document)
    else:
        with pytest.raises(GenericCandidateError, match="rooted_header_check_invalid"):
            await checks.validate_rooted_header_check(connection, named_handoff.document)
    query, dataset_id = connection.fetchval.call_args.args
    assert checks._header_expression(SCHEMA) in query
    assert "count(*)=1 AND bool_and((" in query and ") IS NOT FALSE)" in query
    table = named_handoff.document["tables"][candidate.HEADER]
    assert freeze.storage_relation(table["schema"], table["name"]) in query
    assert dataset_id == named_handoff.document["identity"]["dataset_id"]
    connection.execute.assert_not_awaited()


def test_historical_candidates_keep_original_validation_dispatch():
    body = freeze.wrapped_body(freeze.ROOTED, SCHEMA, _current_bodies()[freeze.ROOTED])
    assert "candidate_storage_json->>'contract'='" + freeze.CONTRACT + "'" in body
    assert freeze.original_query(body) == _current_bodies()[freeze.ROOTED]


@pytest.mark.parametrize("active", [False, True])
def test_upgrade_requires_transaction_and_configured_roles_before_ddl(active):
    connection = Mock()
    connection.in_transaction.return_value = active
    code = "roles_required" if active else "transaction_required"
    with pytest.raises(RuntimeError, match=code):
        freeze.upgrade_rooted_publication_freeze(connection, SCHEMA)
    connection.execute.assert_not_called()
    connection.exec_driver_sql.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "parent,candidate,accepted",
    [
        ([{"signature": {"keys": "1 2"}, "valid": True}], [{"signature": '{"keys":"1 2"}', "valid": True}], True),
        ([{"signature": {"keys": "1 2"}, "valid": True}], [{"signature": {"keys": "1"}, "valid": True}], False),
        ([{"signature": {"keys": "1"}, "valid": False}], [{"signature": {"keys": "1"}, "valid": True}], False),
        ([], [], False),
        ([{"signature": {"keys": "1"}, "valid": True}] * 2, [{"signature": {"keys": "1"}, "valid": True}], False),
    ],
)
async def test_parent_indexes_require_complete_valid_prepared_signatures(parent, candidate, accepted):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process.provider_directory_generic_candidate import GenericCandidateError
    from process.provider_directory_rooted_graph_candidate_indexes import require_prepared_parent_indexes

    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[parent, candidate]), execute=AsyncMock())
    if accepted:
        await require_prepared_parent_indexes(connection, SCHEMA, "private_source", {"root": "candidate"})
    else:
        with pytest.raises(GenericCandidateError, match="rooted_parent_index"):
            await require_prepared_parent_indexes(connection, SCHEMA, "private_source", {"root": "candidate"})
    connection.execute.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_npi_index_is_prepared_without_building_unknown_definitions():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process import provider_directory_rooted_graph_candidate_indexes as indexes
    from process.provider_directory_generic_candidate import GenericCandidateError

    legacy_by_field = {"signature": {"keys": "0 1 2", "predicate": indexes._LEGACY_NPI_PREDICATE}, "valid": True}
    current_by_field = {"signature": {"keys": "0 1 2", "predicate": indexes._CURRENT_NPI_PREDICATE}, "valid": True}
    relations_by_root = {"provider_directory_dataset_resource": "candidate"}
    connection = SimpleNamespace(
        fetch=AsyncMock(
            side_effect=[[legacy_by_field], [current_by_field], [legacy_by_field], [current_by_field, legacy_by_field]]
        ),
        execute=AsyncMock(),
    )
    await indexes.prepare_rooted_parent_indexes(connection, SCHEMA, "private_source", relations_by_root)
    connection.execute.assert_awaited_once()
    assert "CREATE INDEX" in connection.execute.call_args.args[0]
    assert indexes._LEGACY_NPI_PREDICATE in connection.execute.call_args.args[0]
    changed_by_field = {"signature": {"keys": "0 1 2", "predicate": "false"}, "valid": True}
    connection.fetch = AsyncMock(side_effect=[[changed_by_field], [current_by_field]])
    connection.execute.reset_mock()
    with pytest.raises(GenericCandidateError, match="rooted_parent_index_unsupported"):
        await indexes.prepare_rooted_parent_indexes(connection, SCHEMA, "private_source", relations_by_root)
    connection.execute.assert_not_called()


@pytest.mark.asyncio
async def test_frozen_hash_preserves_projection_date_contract(monkeypatch):
    import hashlib
    import importlib
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from process import provider_directory_rooted_graph_publication_candidate as candidate

    directory = importlib.import_module("process.provider_directory_fhir")
    resource_by_field = {
        "resource_type": "Practitioner",
        "resource_id": "resource",
        "payload_hash": "a" * 64,
        "payload_json": '{"resource_id":"resource"}',
    }
    cursor = SimpleNamespace(fetch=AsyncMock(side_effect=[[resource_by_field], []]))
    connection = SimpleNamespace(cursor=AsyncMock(return_value=cursor))
    monkeypatch.setattr(directory, "resource_payload_sha256_for_contract", Mock(return_value="a" * 64))
    monkeypatch.setattr(directory, "_validated_semantic_dataset_payload", Mock())
    projection = Mock()
    monkeypatch.setattr(directory, "_assert_practitioner_semantic_projection_as_of", projection)
    document_by_field = {
        "storage": {"storage_schema": "private_source"},
        "identity": {"dataset_id": "candidate", "semantic_projection_as_of": "2026-01-01"},
        "tables": {candidate.RESOURCE: {"name": "candidate"}},
    }
    digest, count = await candidate._frozen_resource_hash(connection, document_by_field)
    assert (
        count == 1
        and digest
        == hashlib.sha256(candidate.candidate_json(["Practitioner", "resource", "a" * 64]).encode()).hexdigest()
    )
    assert projection.call_args.args[-1] == "2026-01-01"


def _validation_connection(named_handoff, monkeypatch):
    """Script current validation boundaries while preserving exact values and call arguments."""
    from process import provider_directory_rooted_graph_current_preflight as preflight
    from process import provider_directory_rooted_graph_header_checks as checks

    events = []
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(side_effect=lambda *args: events.append("fence") or named_handoff.proof.input_fence_json),
    )
    monkeypatch.setattr(preflight, "current_root_proof", lambda database: named_handoff.proof)
    boundaries_by_name = {
        "inventory": (candidate, "_require_frozen_inventory", None),
        "header": (checks, "validate_rooted_header_check", None),
        "recipes": (candidate, "validate_rooted_query_filter_recipes", None),
        "relations": (candidate, "_validate_frozen_rooted_relations", None),
        "hash": (candidate, "_frozen_resource_hash", ("a" * 64, 1)),
    }
    mocks_by_name = {}
    for name, (module, attribute, returned_value) in boundaries_by_name.items():

        async def invoke(*args, step=name, result=returned_value):
            events.append(step)
            return result

        mocks_by_name[name] = AsyncMock(side_effect=invoke)
        monkeypatch.setattr(module, attribute, mocks_by_name[name])
    predecessor_by_root = {candidate.RESOURCE: {"schema": "previous_snapshot", "name": "resources"}}
    mock = AsyncMock(return_value=predecessor_by_root)
    monkeypatch.setattr(candidate, "dataset_source_relations", mock)
    return SimpleNamespace(connection=connection, events=events, boundaries=mocks_by_name, predecessor=mock)


async def _validate_named(scope, named_handoff):
    return await candidate.validate_frozen_rooted_handoff(
        scope.connection,
        named_handoff,
        SimpleNamespace(storage_binding=named_handoff.binding),
        protected_owner_role="configured_owner",
    )


@pytest.mark.asyncio
async def test_full_validation_rechecks_inventory_and_pinned_content(named_handoff, monkeypatch):
    scope = _validation_connection(named_handoff, monkeypatch)
    result = await _validate_named(scope, named_handoff)
    assert scope.events == ["inventory", "header", "recipes", "fence", "relations", "hash", "inventory"]
    assert result == {
        "dataset_hash": "a" * 64,
        "resource_count": 1,
        "resource_counts": {"Practitioner": 1},
        "predecessor_input_fence": {"root": "pinned"},
    }
    scope.predecessor.assert_awaited_once_with(scope.connection, SCHEMA, "previous", (candidate.RESOURCE,))
    scope.boundaries["relations"].assert_awaited_once_with(
        scope.connection, named_handoff.document, named_handoff.proof, SCHEMA, '"previous_snapshot"."resources"'
    )
    scope.boundaries["recipes"].assert_awaited_once_with(
        scope.connection, "candidate", named_handoff.document["admission"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["transaction", "predecessor", "fence", "hash", "count"])
async def test_full_validation_refuses_changed_data_before_acceptance(named_handoff, monkeypatch, change):
    scope = _validation_connection(named_handoff, monkeypatch)
    if change == "transaction":
        scope.connection.is_in_transaction = lambda: False
        code = "transaction_required"
    elif change == "predecessor":
        named_handoff.document["predecessor_proof"]["dataset_hash"] = "c" * 64
        code = "rooted_predecessor_changed"
    elif change == "fence":
        scope.connection.fetchval = AsyncMock(return_value={"root": "changed"})
        code = "rooted_input_changed"
    else:
        scope.boundaries["hash"].side_effect = None
        scope.boundaries["hash"].return_value = ("c" * 64, 1) if change == "hash" else ("a" * 64, 2)
        code = "rooted_hash_changed"
    with pytest.raises(GenericCandidateError, match=code):
        await _validate_named(scope, named_handoff)
    assert scope.boundaries["inventory"].await_count == (0 if change == "transaction" else 1)
    if change in {"transaction", "predecessor", "fence"}:
        scope.boundaries["relations"].assert_not_awaited()
        scope.boundaries["hash"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("step", ["header", "recipes", "relations", "hash"])
async def test_full_validation_propagates_original_error_without_final_acceptance(named_handoff, monkeypatch, step):
    scope = _validation_connection(named_handoff, monkeypatch)
    error = RuntimeError("original validation error")
    scope.boundaries[step].side_effect = error
    with pytest.raises(RuntimeError) as caught:
        await _validate_named(scope, named_handoff)
    assert caught.value is error
    assert scope.boundaries["inventory"].await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [True, False, None])
async def test_frozen_relations_require_full_original_validator(named_handoff, monkeypatch, accepted):
    from process import provider_directory_rooted_graph_bulk as bulk

    coverage = named_handoff.document["snapshot"]["acquisition"]["request_failure_coverage"]
    coverage_read = AsyncMock(return_value=coverage)
    monkeypatch.setattr(bulk, "named_request_coverage", coverage_read)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=accepted))
    if accepted is True:
        await candidate._validate_frozen_rooted_relations(
            connection, named_handoff.document, named_handoff.proof, SCHEMA, '"previous_snapshot"."resources"'
        )
    else:
        with pytest.raises(GenericCandidateError, match="rooted_candidate_invalid"):
            await candidate._validate_frozen_rooted_relations(
                connection, named_handoff.document, named_handoff.proof, SCHEMA, '"previous_snapshot"."resources"'
            )
    coverage_read.assert_awaited_once()
    assert coverage_read.call_args.args[1] == "publication"
    query, selected, previous = connection.fetchval.call_args.args
    assert selected == named_handoff.document["identity"]["dataset_id"] and previous == "previous"
    for table in named_handoff.document["tables"].values():
        assert freeze.storage_relation(table["schema"], table["name"]) in query
    original = freeze.validation_body_sql(freeze.ROOTED, SCHEMA)
    for root, table in named_handoff.document["inputs"].items():
        if freeze.storage_relation(SCHEMA, root) in original:
            assert freeze.storage_relation(table["schema"], table["name"]) in query
    assert '"previous_snapshot"."resources" WHERE dataset_id=$2' in query


@pytest.mark.asyncio
async def test_request_coverage_change_refuses_before_content_query(named_handoff, monkeypatch):
    from process import provider_directory_rooted_graph_bulk as bulk

    monkeypatch.setattr(bulk, "named_request_coverage", AsyncMock(return_value={"completed": 2}))
    connection = SimpleNamespace(fetchval=AsyncMock())
    with pytest.raises(GenericCandidateError, match="rooted_request_coverage_changed"):
        await candidate._validate_frozen_rooted_relations(
            connection, named_handoff.document, named_handoff.proof, SCHEMA, '"previous_snapshot"."resources"'
        )
    connection.fetchval.assert_not_awaited()


def test_strongest_guard_predicate_preserves_null_and_failure_semantics():
    from itertools import product

    def sql_and(left, right):
        return False if left is False or right is False else (None if left is None or right is None else True)

    for status, intrinsic, lineage, headers_ready in product(
        (None, "building", "validated", "published", "superseded", "unknown"),
        (True, False, None),
        (True, False, None),
        (True, False, None),
    ):
        valid = sql_and(intrinsic, sql_and(intrinsic, lineage))
        is_ready = headers_ready is True and valid is True
        is_original_invalid = (
            intrinsic is not True
            or (status in {"validated", "published"} and valid is not True)
            or (status == "published" and is_ready is not True)
        )
        strongest = is_ready if status == "published" else valid if status == "validated" else intrinsic
        direct = sql_and(
            sql_and(intrinsic, lineage if status in {"validated", "published"} else True),
            headers_ready if status == "published" else True,
        )
        assert is_original_invalid == (strongest is not True) == (direct is not True)
    query = freeze._protected_guard_valid_sql(SCHEMA)
    assert "receipt(accepted) AS MATERIALIZED" in query
    assert "NEW.status IN ('validated','published')" in query and "NEW.status='published'" in query
    assert freeze._lineage_metadata_sql(SCHEMA, "NEW.dataset_id") in query
    assert freeze._published_headers_sql(SCHEMA, "NEW.dataset_id") in query


def test_historical_guard_preserves_delete_and_immutable_header_fields():
    original = _current_bodies()[freeze.GUARD]
    assert "ELSIF TG_OP = 'DELETE' THEN" in original
    assert "NEW.dataset_id" in original and "OLD.dataset_id" in original
    assert "NEW.created_at" in original and "OLD.created_at" in original


def test_direct_header_predicate_is_literal_ready_contract_without_valid():
    name = "provider_directory_rooted_graph_dataset_ready"
    body = _current_bodies({name})[name]
    valid = freeze.storage_relation(SCHEMA, "provider_directory_rooted_graph_dataset_valid")
    original = " ".join(body.split()).removesuffix(";")
    for argument in ("header.dataset_id::text", "header.dataset_id"):
        original = original.replace(" AND " + valid + "(" + argument + ")", "")
    direct = " ".join(("SELECT " + freeze._published_headers_sql(SCHEMA)).split())
    assert direct == original


def test_direct_lineage_retains_every_logical_data_comparison():
    lineage = freeze._lineage_receipt_sql(SCHEMA)
    direct = freeze._lineage_metadata_sql(SCHEMA)
    assert (
        lineage
        == "SELECT " + freeze.storage_relation(SCHEMA, freeze.ROOTED) + "(candidate_dataset_id)\n        AND " + direct
    )
    for comparison in (
        "to_jsonb(c)=proof.handoff->'lineage'->'cohort'",
        "official.dataset_id=c.official_dataset_id",
        "official.endpoint_id=c.official_endpoint_id",
        "official.acquisition_root_run_id=c.official_acquisition_root_run_id",
        "official.dataset_hash=c.official_dataset_hash",
        "official.status='published' AND official.is_current IS TRUE",
    ):
        assert comparison in direct
