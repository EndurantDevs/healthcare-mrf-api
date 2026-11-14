# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable named candidates retain data validation and permission-enforced freezing."""

import json
import re
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db import migration_rooted_publication_freeze as freeze
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_rooted_graph_current_preflight as preflight
from process import provider_directory_rooted_graph_publication_candidate as candidate
from process import provider_directory_source_storage_binding as binding
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from process.provider_directory_rooted_graph_header_checks import _header_expression
from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
)


@pytest.fixture
def named_handoff():
    return {
        "contract": freeze.CONTRACT,
        "operation": {"publication_acquisition_id": "pdrga_" + "a" * 48},
        "phase": "ready_for_freeze",
        "preparation_id": "11111111-1111-4111-8111-111111111111",
        "storage": {"catalog_schema": "catalog_schema", "storage_schema": "snapshot_a"},
        "identity": {
            "dataset_id": "pdrgpd_" + "b" * 48,
            "source_id": "directory_source",
            "metadata": {
                "source_ids": ["directory_source"],
                "source_run_id": "run_" + "c" * 32,
                "source_attempt_id": "attempt_one",
                "query_filter_recipe_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
            },
        },
        "admission": {},
        "tables": {
            root: {"schema": "snapshot_a", "name": "output_" + str(index)} for index, root in enumerate(candidate.ROOTS)
        },
        "inputs": {
            "provider_directory_rooted_graph_" + root: {
                "schema": "snapshot_a",
                "name": "provider_directory_rooted_graph_" + root,
            }
            for root in ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
        },
        "dataset_hash": "d" * 64,
        "counts": {"Practitioner": 1},
        "snapshot": {},
        "lineage": {},
        "predecessor_proof": {},
    }


def test_storage_binding_contains_only_portable_schema_names():
    configured = binding.FHIRSourceStorageBinding("catalog_schema", "snapshot_a")
    assert asdict(configured) == {"catalog_schema": "catalog_schema", "storage_schema": "snapshot_a"}
    assert binding.storage_document(SimpleNamespace(storage_binding=configured)) == asdict(configured)
    with pytest.raises(TypeError):
        binding.FHIRSourceStorageBinding("catalog_schema", "snapshot_a", database_oid=10)
    with pytest.raises(ValueError):
        binding.FHIRSourceStorageBinding("catalog_schema", "unsafe.schema")


def test_same_handoff_bytes_are_accepted_after_copy_to_another_database(named_handoff):
    encoded = candidate.candidate_json(named_handoff)
    for database_name in ("first_database", "copied_database"):
        issued = SimpleNamespace(
            database_name=database_name,
            storage_binding=binding.FHIRSourceStorageBinding("catalog_schema", "snapshot_a"),
        )
        copied = json.loads(encoded)
        assert candidate.validate_rooted_handoff_document(copied, asdict(issued.storage_binding)) == named_handoff
        assert candidate.candidate_json(copied) == encoded


@pytest.mark.parametrize("change", ["missing_root", "duplicate_name", "different_schema", "physical_field"])
def test_named_handoff_rejects_incomplete_or_redirected_output_groups(named_handoff, change):
    roots = list(candidate.ROOTS)
    if change == "missing_root":
        del named_handoff["tables"][roots[0]]
    elif change == "duplicate_name":
        named_handoff["tables"][roots[0]] = dict(named_handoff["tables"][roots[1]])
    elif change == "different_schema":
        named_handoff["tables"][roots[0]]["schema"] = "other_snapshot"
    else:
        named_handoff["tables"][roots[0]]["oid"] = 10
    with pytest.raises(Exception, match="rooted_inventory_changed"):
        candidate.validate_rooted_handoff_document(named_handoff, named_handoff["storage"])


@pytest.mark.asyncio
async def test_output_inventory_resolves_qualified_names_without_catalog_queries():
    connection = SimpleNamespace(execute=AsyncMock())
    observed = await storage._capture(connection, SimpleNamespace(schema="snapshot_a"), ["output_one", "output_two"])
    assert observed == {
        "output_one": {"schema": "snapshot_a", "name": "output_one"},
        "output_two": {"schema": "snapshot_a", "name": "output_two"},
    }
    assert [call.args[0] for call in connection.execute.await_args_list] == [
        'SELECT 1 FROM "snapshot_a"."output_one" LIMIT 0',
        'SELECT 1 FROM "snapshot_a"."output_two" LIMIT 0',
    ]


@pytest.mark.asyncio
async def test_frozen_inventory_keeps_dataset_isolation_validation(named_handoff):
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=False))
    await candidate._require_frozen_inventory(connection, named_handoff, "publication_owner")
    assert connection.fetchval.await_count == len(candidate.ROOTS)
    assert all(
        "dataset_id IS DISTINCT FROM $1" in call.args[0] and call.args[1] == named_handoff["identity"]["dataset_id"]
        for call in connection.fetchval.await_args_list
    )
    connection.fetchval.return_value = True
    with pytest.raises(Exception, match="rooted_inventory_changed"):
        await candidate._require_frozen_inventory(connection, named_handoff, "publication_owner")


@pytest.mark.parametrize("variant", ["uhc_flex_practitioner", "rooted_combined"])
def test_predecessor_fence_contains_logical_fields_and_one_parameter(variant):
    sql = root_input_fence_sql("catalog_schema", "$1", variant=variant)
    assert "WHERE h.dataset_id=$1" in sql
    assert "to_jsonb(h)" in sql and "to_jsonb(p)" in sql
    assert "candidate_storage_json" in sql and "superseded_at" in sql
    for forbidden in ("pg_class", "pg_proc", "oid", "xmin", "relfilenode", "hp_snapshot_retention", "$2"):
        assert forbidden not in sql


@pytest.mark.asyncio
async def test_exact_current_cas_rejects_a_different_logical_predecessor(monkeypatch):
    current = SimpleNamespace(dataset_id="dataset_one", variant="rooted_combined")
    proof = preflight.ValidatedCurrentRoot(current, "snapshot", "fence", "d" * 64, 1)
    monkeypatch.setattr(preflight, "_current_metadata", AsyncMock(return_value=current))
    monkeypatch.setattr(preflight, "_snapshot", AsyncMock(return_value="snapshot"))
    monkeypatch.setattr(preflight, "capture_root_input_fence", AsyncMock(return_value="fence"))
    assert await preflight.require_current_root_fence(object(), proof) == current
    preflight._current_metadata.return_value = SimpleNamespace(dataset_id="dataset_two")
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await preflight.require_current_root_fence(object(), proof)


def test_frozen_validation_renders_real_data_predicates_for_each_named_root(named_handoff):
    sql = candidate._frozen_validation_query(
        None, "catalog_schema", named_handoff, predecessor_resource='"root_snapshot"."resources"'
    )
    for table in named_handoff["tables"].values():
        assert '"snapshot_a"."' + table["name"] + '"' in sql
    assert "resource_count" in sql and "publication_acquisition_id" in sql
    assert "candidate_dataset_id" not in sql
    assert "pg_proc" not in sql and "relfilenode" not in sql
    policy_literals = [literal for literal in re.findall(r"'([^']*)'::jsonb", sql) if "policy_version" in literal]
    assert policy_literals and all(json.loads(literal)["required_root_count"] == 1 for literal in policy_literals)
    expression = _header_expression("catalog_schema")
    assert "request_failure_coverage" in expression and "publication_contract_id" in expression


@pytest.mark.asyncio
async def test_freeze_transfers_and_revokes_before_granting_read_access():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetch=AsyncMock(
            side_effect=[
                [{"relname": "output_one", "relkind": "r"}, {"relname": "input_seq", "relkind": "S"}],
                [
                    {"role": "candidate_writer", "column_name": None},
                    {"role": "candidate_writer", "column_name": "payload_json"},
                ],
                [{"role": "candidate_writer", "column_name": None}],
                [{"role": "candidate_writer"}, {"role": "PUBLIC"}],
            ]
        ),
    )
    await freeze_fhir_source_snapshot(
        connection, "snapshot_a", namespace_owner="publication_owner", reader_roles=("api_reader", "publisher")
    )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    owner = 'ALTER TABLE "snapshot_a"."output_one" OWNER TO "publication_owner"'
    revoke = 'REVOKE ALL PRIVILEGES ON TABLE "snapshot_a"."output_one" FROM "candidate_writer" CASCADE'
    columns = (
        'REVOKE ALL PRIVILEGES ("payload_json") ON TABLE "snapshot_a"."output_one" FROM "candidate_writer" CASCADE'
    )
    select = 'GRANT SELECT ON TABLE "snapshot_a"."output_one" TO "api_reader","publisher"'
    assert statements.index(owner) < statements.index(revoke) < statements.index(select)
    assert columns in statements
    assert 'ALTER SEQUENCE "snapshot_a"."input_seq" OWNER TO "publication_owner"' in statements
    assert 'REVOKE ALL ON SCHEMA "snapshot_a" FROM "candidate_writer" CASCADE' in statements
    assert statements[-1] == 'GRANT USAGE ON SCHEMA "snapshot_a" TO "api_reader","publisher"'
    assert not any("GRANT INSERT" in sql or "GRANT UPDATE" in sql or "GRANT CREATE" in sql for sql in statements)


@pytest.mark.asyncio
async def test_freeze_failure_propagates_before_read_admission():
    failure = RuntimeError("permission denied")
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(side_effect=[[{"relname": "output_one", "relkind": "r"}], []]),
        execute=AsyncMock(side_effect=failure),
    )
    with pytest.raises(RuntimeError, match="permission denied"):
        await freeze_fhir_source_snapshot(
            connection, "snapshot_a", namespace_owner="publication_owner", reader_roles=("api_reader",)
        )
    assert connection.execute.await_count == 1


def test_retained_physical_handoff_projects_names_and_ignores_physical_drift(named_handoff):
    original = json.loads(candidate.candidate_json(named_handoff))
    original["catalog"] = [{"oid": 17, "relowner": 23}]
    original["storage"].update(database_oid=1, owner_oid=2, enrollment_sha256="a" * 64)
    for table in original["tables"].values():
        table.pop("schema")
        table.update(oid=17, owner=23, relfilenode=99, layout_sha256="b" * 64)
    original["snapshot"].update(header_check={"oid": 10}, foreign_guards={"oid": 11})
    projected = binding.FHIRSourceCandidateHandoff(candidate.candidate_json(original)).document
    assert projected == named_handoff
    assert candidate.validate_rooted_handoff_document(projected, named_handoff["storage"]) == named_handoff
    original["storage"]["database_oid"] = 999
    original["catalog"][0]["oid"] = 12345
    assert binding.named_source_document(original) == named_handoff
    assert "catalog" in original and original["tables"][candidate.ROOTS[0]]["oid"] == 17


@pytest.mark.asyncio
async def test_retained_candidate_cas_compares_logical_data_and_uses_raw_row_version():
    raw_by_field = {
        "storage": {"catalog_schema": "catalog_schema", "storage_schema": "snapshot_a", "owner_oid": 50},
        "tables": {"resource_input": {"name": "input_one", "oid": 60}},
        "identity": {"dataset_id": "dataset_one"},
        "phase": "loading",
        "ordinal": 1,
    }
    previous = binding.named_source_document(raw_by_field)
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=json.dumps(raw_by_field)), execute=AsyncMock(return_value="UPDATE 1")
    )
    current_by_field = {**previous, "ordinal": 2}
    await storage._store(
        connection, SimpleNamespace(schema="snapshot_a"), previous["identity"], previous, current_by_field
    )
    arguments = connection.execute.await_args.args
    assert connection.fetchval.await_args.args[0].endswith("FOR NO KEY UPDATE")
    assert json.loads(arguments[2]) == current_by_field
    assert json.loads(arguments[3]) == raw_by_field
    raw_by_field["tables"]["resource_input"]["oid"] = 999
    connection.fetchval.return_value = json.dumps(raw_by_field)
    await storage._store(
        connection, SimpleNamespace(schema="snapshot_a"), previous["identity"], previous, current_by_field
    )
    raw_by_field["tables"]["resource_input"]["name"] = "different_input"
    connection.fetchval.return_value = json.dumps(raw_by_field)
    with pytest.raises(Exception, match="header_compare_failed"):
        await storage._store(
            connection, SimpleNamespace(schema="snapshot_a"), previous["identity"], previous, current_by_field
        )
    assert connection.execute.await_count == 2


@pytest.mark.asyncio
async def test_retained_rooted_handoff_rejects_a_different_logical_dataset(named_handoff):
    runtime = SimpleNamespace(
        candidate_schema="snapshot_a",
        operation=named_handoff["operation"],
        manifest=SimpleNamespace(storage_binding=binding.FHIRSourceStorageBinding("catalog_schema", "snapshot_a")),
    )
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=candidate.candidate_json(named_handoff)))
    with pytest.raises(Exception, match="rooted_identity_changed"):
        await candidate.retained_rooted_handoff(runtime, connection, "pdrgpd_" + "e" * 48)


@pytest.mark.asyncio
async def test_named_dataset_resolver_pins_recorded_schema(named_handoff):
    database = SimpleNamespace(
        first=AsyncMock(return_value={"candidate_storage_json": candidate.candidate_json(named_handoff)})
    )
    tables = await binding.dataset_source_relations(
        database, "catalog_schema", named_handoff["identity"]["dataset_id"], (storage.RESOURCE,)
    )
    assert tables == {storage.RESOURCE: named_handoff["tables"][storage.RESOURCE]}
    assert database.first.await_count == 1
    assert '"catalog_schema"."provider_directory_endpoint_dataset"' in database.first.await_args.args[0]
    assert database.first.await_args.kwargs == {"dataset_id": named_handoff["identity"]["dataset_id"]}
    del named_handoff["tables"][storage.RESOURCE]
    database.first.return_value = {"candidate_storage_json": candidate.candidate_json(named_handoff)}
    with pytest.raises(Exception, match="tables_changed"):
        await binding.dataset_source_relations(
            database, "catalog_schema", named_handoff["identity"]["dataset_id"], (storage.RESOURCE,)
        )


@pytest.mark.asyncio
async def test_unbound_legacy_dataset_resolves_only_its_fixed_qualified_roots():
    database = SimpleNamespace(first=AsyncMock(return_value={"candidate_storage_json": None}))
    tables = await binding.dataset_source_relations(database, "legacy_schema", "legacy_dataset", (storage.RESOURCE,))
    assert tables == {storage.RESOURCE: {"schema": "legacy_schema", "name": storage.RESOURCE}}
    database.first.return_value = None
    with pytest.raises(Exception, match="dataset_missing"):
        await binding.dataset_source_relations(database, "legacy_schema", "legacy_dataset", (storage.RESOURCE,))


def test_initial_work_and_closure_sql_use_the_resolved_predecessor_names():
    from process.provider_directory_rooted_graph_bulk import _initial_work_sql
    from process.provider_directory_rooted_graph_persistence_sql import root_closure_sql

    relation = '"retained_snapshot"."resources"'
    initial = _initial_work_sql("catalog_schema", relation)
    assert "FROM " + relation + " AS member" in initial
    assert "member.dataset_id = header.root_dataset_id" in initial
    assert "query_identity_sha256" in initial and "ORDER BY member.resource_id" in initial
    assert "read_provider_directory_rooted_graph_initial_work" not in initial
    assert "__SCHEMA__" not in initial and "p_after" not in initial
    closure = root_closure_sql(resource_relation=relation)
    assert closure.count("FROM " + relation + " AS member") == 2
    assert "member.dataset_id = header.root_dataset_id" in closure
    assert "EXCEPT" in closure


def test_source_reader_keeps_qualified_names_after_new_generation_selection():
    from process.provider_directory_dataset_rehydrate_pin import _SourceReader

    session = SimpleNamespace(in_transaction=lambda: True)
    reader = _SourceReader(
        session, "catalog_schema", {storage.RESOURCE: {"schema": "retained_snapshot", "name": "resources"}}
    )
    assert reader.relation(storage.RESOURCE) == '"retained_snapshot"."resources"'
    replacement_by_field = {storage.RESOURCE: {"schema": "new_snapshot", "name": "resources"}}
    assert replacement_by_field != reader.tables_by_root
    assert reader.relation(storage.RESOURCE) == '"retained_snapshot"."resources"'
    reader.deactivate()
    with pytest.raises(RuntimeError, match="reader_transaction_ended"):
        reader.relation(storage.RESOURCE)


def test_old_physical_handoff_without_input_names_projects_only_historical_canonical_inputs(named_handoff):
    document = json.loads(candidate.candidate_json(named_handoff))
    del document["inputs"]
    document["catalog"] = []
    projected = binding.named_source_document(document)
    assert all(
        table["schema"] == "catalog_schema" and table["name"] == root for root, table in projected["inputs"].items()
    )
    assert candidate.validate_rooted_handoff_document(projected, named_handoff["storage"]) == projected
    del document["catalog"]
    projected = binding.named_source_document(document)
    with pytest.raises(Exception, match="rooted_handoff_shape_changed"):
        candidate.validate_rooted_handoff_document(projected, named_handoff["storage"])


@pytest.mark.asyncio
async def test_rooted_write_without_isolated_runtime_fails_before_database_io(monkeypatch):
    from process import provider_directory_rooted_graph_source_runtime as runtime
    from process import provider_directory_rooted_graph_store_support as support

    monkeypatch.setattr(runtime, "current_rooted_graph_runtime", lambda database=None: None)
    database = SimpleNamespace(transaction=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(RuntimeError, match="configured isolated generation"):
        async with support.rooted_graph_transaction(database):
            pytest.fail("unconfigured writer entered a transaction")
    database.transaction.assert_not_called()
    database.scalar.assert_not_called()


def test_frozen_query_joins_a_retained_named_comparison_acquisition(named_handoff):
    comparison_by_root = {
        root: {"schema": "retained_snapshot", "name": table["name"]} for root, table in named_handoff["inputs"].items()
    }
    query = candidate._frozen_validation_query(
        None,
        "catalog_schema",
        named_handoff,
        predecessor_resource='"retained_snapshot"."resources"',
        comparison_inputs=comparison_by_root,
    )
    assert 'FROM "retained_snapshot"."provider_directory_rooted_graph_acquisition" WHERE acquisition_id=$3' in query
    assert 'FROM "retained_snapshot"."resources" WHERE dataset_id=$2' in query
    assert "to_regclass" not in query and "relfilenode" not in query


def test_admin_upgrade_retires_truncated_historical_trigger_names():
    connection = Mock()
    connection.in_transaction.return_value = True
    connection.execute.return_value.mappings.return_value.one.return_value = {
        "body": "SELECT true",
        "definition": "SELECT true",
    }
    connection.execute.return_value.scalar_one.return_value = True
    freeze.upgrade_rooted_publication_freeze(connection, "snapshot_schema")
    statements = [call.args[0] for call in connection.exec_driver_sql.call_args_list]
    historical = "pd_rooted_graph_provider_directory_endpoint_dataset_dependency_guard"
    assert len(historical) > 63
    assert (
        f'DROP TRIGGER IF EXISTS "{historical[:63]}" ON "snapshot_schema"."provider_directory_endpoint_dataset"'
        in statements
    )
    assert all(
        len(statement.split('"', 2)[1]) <= 63 for statement in statements if statement.startswith("DROP TRIGGER")
    )
