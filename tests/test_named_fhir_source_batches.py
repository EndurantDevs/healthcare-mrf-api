# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated Rooted batches retain lease, budget, semantic and atomic failure behavior."""

import re
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_bulk as batch
from process import provider_directory_rooted_graph_source_runtime as runtime_module
from process.provider_directory_rooted_graph_query import build_provider_directory_practitioner_role_query
from process.provider_directory_rooted_graph_result_contract import build_provider_directory_rooted_graph_query_result
from process.provider_directory_rooted_graph_result_store import complete_provider_directory_rooted_graph_result
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphStoreError,
    build_provider_directory_rooted_graph_work_spec,
)
from tests.test_provider_directory_rooted_graph_store_contract import API_BASE, ROOT_ID, _claim, _scope


def test_initial_work_query_keeps_declared_result_names_and_page_binds():
    from sqlalchemy import text
    from sqlalchemy.dialects.postgresql.asyncpg import dialect

    source = (Path(batch.__file__).resolve().parents[1] / "db/sql/rooted_graph_set_validation.sql").read_text()
    declaration = source.split("CREATE FUNCTION __SCHEMA__.read_provider_directory_rooted_graph_initial_work(", 1)[1]
    result_columns = re.search(r"RETURNS TABLE\((.*?)\)\s*LANGUAGE", declaration, re.S)
    assert result_columns is not None
    expected_names = tuple(column.strip().split()[0] for column in result_columns.group(1).split(","))
    query = batch._initial_work_sql("candidate_generation", '"published_generation"."resources"')
    named_columns = re.match(r"WITH initial_work \(([^)]+)\) AS \(", query)
    assert named_columns is not None and tuple(named_columns.group(1).split(",")) == expected_names
    compiled = text(query).compile(dialect=dialect())
    assert set(compiled.params) == {"acquisition_id", "after_id", "limit"}
    assert '"published_generation"."resources"' in str(compiled)
    assert query.endswith("ORDER BY reference_id")


@pytest.mark.asyncio
async def test_candidate_relationships_use_private_inputs_and_canonical_registry(named_runtime, monkeypatch):
    from process import provider_directory_generic_source_kind_candidate as candidate

    source = "provider_directory_rooted_graph_dataset_resource"
    monkeypatch.setattr(
        candidate,
        "_expected_bulk_relationships",
        lambda: {
            (
                source,
                "provider_directory_rooted_graph_acquisition",
                ("publication_acquisition_id",),
                ("acquisition_id",),
                "a",
            ),
            (source, "provider_directory_source", ("source_id",), ("source_id",), "b"),
        },
    )
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    await candidate._validate_relationships(
        connection, "snapshot_a", {source: "candidate_provenance"}, "dataset", canonical_schema="catalog_schema"
    )
    statements = [call.args[0] for call in connection.fetchval.await_args_list]
    assert all('FROM "snapshot_a"."candidate_provenance" s' in statement for statement in statements)
    assert any(
        'FROM "snapshot_a"."provider_directory_rooted_graph_acquisition" t' in statement for statement in statements
    )
    assert any('FROM "catalog_schema"."provider_directory_source" t' in statement for statement in statements)
    assert all(call.args[1] == "dataset" for call in connection.fetchval.await_args_list)


@pytest.fixture
def named_runtime(monkeypatch):
    events = []

    @asynccontextmanager
    async def transaction(database, **options):
        events.append("begin")
        try:
            yield SimpleNamespace(session=database, connection=database)
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    runtime = SimpleNamespace(
        catalog_schema="catalog_schema",
        candidate_schema="snapshot_a",
        transaction=transaction,
        relation_coordinates=lambda schema, root, name: (
            ("snapshot_a", name) if root.startswith("provider_directory_rooted_graph_") else (schema, name)
        ),
    )
    monkeypatch.setattr(runtime_module, "current_rooted_graph_runtime", lambda database=None: runtime)
    return events


@pytest.fixture
def result_pair():
    spec = build_provider_directory_rooted_graph_work_spec(
        _scope().scope_id, build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID), closure_scope="root"
    )
    claim = _claim(spec)
    result = build_provider_directory_rooted_graph_query_result(
        claim,
        [
            {
                "resourceType": "PractitionerRole",
                "id": "role.synthetic-1",
                "practitioner": {"reference": "Practitioner/" + ROOT_ID},
            }
        ],
    )
    return claim, result


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "lease", "resource", "edge", "budget", "terminal"])
async def test_result_batch_validates_then_accounts_and_terminalizes_in_one_transaction(
    named_runtime, result_pair, monkeypatch, failure
):
    """Keep lease checks, data accounting and terminal state in one commit or rollback."""
    claim, query_result = result_pair
    bytes_count = sum(len(resource_row.payload_json_text.encode()) for resource_row in query_result.resources)
    first_rows = (
        [None]
        if failure == "lease"
        else [
            {"status": "leased"},
            (len(query_result.resources), bytes_count, failure == "resource"),
            (len(query_result.edges), failure == "edge"),
        ]
    )
    statements = []

    async def status(sql, **parameters):
        statements.append((sql, parameters))
        if "SET used_resource_rows=" in sql and failure == "budget":
            return 0
        if "SET status = 'completed'" in sql and failure == "terminal":
            return 0
        return 1

    database = SimpleNamespace(first=AsyncMock(side_effect=first_rows), status=AsyncMock(side_effect=status))
    copied_batches = []

    async def copy(name, *, schema_name, columns, records):
        copied_batches.append((schema_name, name, columns, records))
        return f"COPY {len(records)}"

    monkeypatch.setattr(batch, "_copy_driver", AsyncMock(return_value=SimpleNamespace(copy_records_to_table=copy)))
    from process import provider_directory_rooted_graph_result_store as store

    monkeypatch.setattr(store, "register_rooted_graph_frontier", AsyncMock())
    if failure:
        with pytest.raises(ProviderDirectoryRootedGraphStoreError):
            await complete_provider_directory_rooted_graph_result(claim, query_result, database=database)
        assert named_runtime == ["begin", "rollback"]
        store.register_rooted_graph_frontier.assert_not_awaited()
    else:
        await complete_provider_directory_rooted_graph_result(claim, query_result, database=database)
        assert named_runtime == ["begin", "commit"]
        store.register_rooted_graph_frontier.assert_awaited_once()
    _assert_result_batch_writes(database, claim, query_result, bytes_count, statements, copied_batches, failure)


def _assert_result_batch_writes(database, claim, query_result, bytes_count, statements, copied_batches, failure):
    """Check exact COPY isolation and mutation scope across every modeled failure boundary."""
    lease_sql, lease_params = database.first.await_args_list[0].args[0], database.first.await_args_list[0].kwargs
    assert "lease_expires_at>clock_timestamp() FOR UPDATE" in lease_sql
    assert lease_params["attempt"] == claim.attempt and lease_params["lease_token"] == claim.lease_token
    insert_statements = [sql for sql, _ in statements if sql.startswith("INSERT INTO")]
    if failure in {"lease", "resource", "edge", "budget"}:
        assert insert_statements == []
    else:
        assert len(insert_statements) == 2 and all('"snapshot_a".' in sql for sql in insert_statements)
        budget = next(parameters for sql, parameters in statements if "SET used_resource_rows=" in sql)
        assert budget == {
            "acquisition_id": claim.acquisition_id,
            "resources": len(query_result.resources),
            "edges": len(query_result.edges),
            "payload_bytes": bytes_count,
        }
    assert all(schema == "pg_temp" for schema, _name, _columns, _rows in copied_batches)
    assert not any("admit_provider_directory_rooted_graph" in sql for sql, _ in statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "count", "data", "budget"])
async def test_work_batch_keeps_complete_set_validation_and_actual_insert_budget(named_runtime, monkeypatch, failure):
    from process import provider_directory_source_storage_binding as binding

    root = "provider_directory_dataset_resource"
    monkeypatch.setattr(
        binding,
        "dataset_source_relations",
        AsyncMock(return_value={root: {"schema": "retained_snapshot", "name": "resources"}}),
    )
    count = 1 if failure == "count" else 2
    database = SimpleNamespace(
        first=AsyncMock(
            return_value={"scope_id": "scope_one", "root_dataset_id": "root_one", "root_resource_count": 2}
        ),
        scalar=AsyncMock(side_effect=[count, ["invalid"] if failure == "data" else [], 2]),
        status=AsyncMock(
            side_effect=lambda sql, **params: 0 if failure == "budget" and sql.startswith("UPDATE") else 1
        ),
    )
    if failure:
        with pytest.raises(ProviderDirectoryRootedGraphStoreError):
            await batch._named_work_admission(database, "acquisition_one", "initialize")
    else:
        assert await batch._named_work_admission(database, "acquisition_one", "initialize") == 2
    scalar_calls = database.scalar.await_args_list
    if failure != "count":
        assert '"retained_snapshot"."resources"' in scalar_calls[1].args[0]
        assert "expected_header.root_dataset_id" in scalar_calls[1].args[0]
    if failure in {"count", "data"}:
        assert not any("INSERT INTO" in call.args[0] for call in scalar_calls)
    else:
        assert "RETURNING 1" in scalar_calls[-1].args[0]
        budget = next(call for call in database.status.await_args_list if call.args[0].startswith("UPDATE"))
        assert budget.kwargs["inserted"] == 2
        assert "used_work_items+:inserted<=max_work_items" in budget.args[0]


def test_resource_and_edge_sql_preserve_each_original_data_query():
    original = batch._data_module("20261005100000_rooted_graph_set_validation.py")._sql("snapshot_a")
    body = original.split('CREATE FUNCTION "snapshot_a".validate_provider_directory_rooted_graph_witnesses(', 1)[1]
    body = body.split("AS $function$", 1)[1].split("$function$;", 1)[0]
    queries = re.findall(r"EXECUTE format\(\$query\$(.*?)\$query\$", body, re.S)
    assert len(queries) == 2
    for name, original_query in zip(("resource", "edge"), queries):
        actual = batch._candidate_sql("rooted_graph_" + name + "_candidate_validation.sql", "snapshot_a")
        expected = original_query.replace("%%", "%").replace("($1).", "(expected.claim).")
        if name == "resource":
            expected = expected.replace("%s", "pg_temp.pdrg_resource_stage")
            expected = expected.replace(
                "WITH RECURSIVE payloads",
                'WITH RECURSIVE expected AS (SELECT work AS claim FROM "snapshot_a".provider_directory_rooted_graph_work work WHERE acquisition_id=:acquisition_id AND query_id=:query_id), payloads',
            )
            expected = expected.replace(
                "FROM payloads AS incoming LEFT JOIN extension_checks USING (resource_type, resource_id)",
                "FROM payloads AS incoming LEFT JOIN extension_checks USING (resource_type, resource_id) CROSS JOIN expected",
            )
        else:
            expected = expected.replace("%s", "pg_temp.pdrg_edge_stage", 1).replace(
                "%s", "pg_temp.pdrg_resource_stage", 1
            )
            expected = expected.replace(
                "WITH field_contract",
                'WITH expected AS (SELECT work AS claim FROM "snapshot_a".provider_directory_rooted_graph_work work WHERE acquisition_id=:acquisition_id AND query_id=:query_id), field_contract',
            )
            expected = expected.replace("FROM joined", "FROM joined CROSS JOIN expected")
        actual = "\n".join(line for line in actual.splitlines() if not line.startswith("--"))
        assert re.sub(r"\s+", " ", actual).strip().removesuffix(";") == re.sub(r"\s+", " ", expected).strip()


@pytest.mark.asyncio
@pytest.mark.parametrize("inherited,failed,total,expected", [(0, 0, 2, None), (0, 1, 60, 1), (1, 0, 60, 1)])
async def test_request_coverage_keeps_inherited_counts_and_strict_failure_budget(
    named_runtime, inherited, failed, total, expected
):
    database = SimpleNamespace(
        first=AsyncMock(
            side_effect=[
                {
                    "npi_count": 100,
                    "metadata": {"retry_exhausted_count": inherited, "cohort_complete": inherited == 0},
                    "cohort_complete": inherited == 0,
                },
                {"total": total, "failed": failed, "incomplete": 0, "invalid_errors": 0},
            ]
        )
    )
    coverage = await batch.named_request_coverage(database, "acquisition_one")
    if expected is None:
        assert coverage is None
    else:
        assert coverage["failed_requests"] == expected
        assert coverage["total_requests"] == 100 + total
        assert coverage["rooted_total_requests"] == total
        assert coverage["rooted_failed_requests"] == failed


@pytest.mark.asyncio
async def test_request_coverage_refuses_nonterminal_or_over_budget_work(named_runtime):
    for census in (
        {"total": 60, "failed": 0, "incomplete": 1, "invalid_errors": 0},
        {"total": 60, "failed": 4, "incomplete": 0, "invalid_errors": 0},
    ):
        database = SimpleNamespace(
            first=AsyncMock(
                side_effect=[{"npi_count": 100, "metadata": {"cohort_complete": True}, "cohort_complete": True}, census]
            )
        )
        with pytest.raises((ProviderDirectoryRootedGraphStoreError, ValueError)):
            await batch.named_request_coverage(database, "acquisition_one")
