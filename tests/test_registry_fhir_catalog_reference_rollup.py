# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published resource scopes preserve bounded SQL rollups and temporary workspace custody."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_reference_rollup as rollup


@pytest.fixture
def published_scope(monkeypatch):
    from process.registry_fhir_catalog_published_read import FHIRCatalogPublishedRead

    verify = AsyncMock()
    select = Mock(
        return_value='SELECT * FROM ONLY "source_schema"."leaf_one" '
        'UNION ALL SELECT * FROM ONLY "source_schema"."leaf_two"'
    )
    monkeypatch.setattr(FHIRCatalogPublishedRead, "verify", verify)
    monkeypatch.setattr(FHIRCatalogPublishedRead, "resource_sql", select)
    return SimpleNamespace(read=object.__new__(FHIRCatalogPublishedRead), verify=verify, select=select)


def report(rows=0, *, last_id=None):
    return {
        "source_rows": rows,
        "reference_rows": rows,
        "without_refs": 0,
        "source_bytes": rows * 256,
        "invalid_rows": 0,
        "invalid_references": 0,
        "last_type": "PractitionerRole" if rows else None,
        "last_id": last_id,
        "resource_counts": json.dumps({"PractitionerRole": rows}) if rows else "{}",
    }


def connection_mock(*, read_only="off", reports=()):
    return Mock(
        is_in_transaction=Mock(return_value=True),
        is_closed=Mock(return_value=False),
        execute=AsyncMock(),
        fetchrow=AsyncMock(side_effect=[{"isolation": "repeatable read", "read_only": read_only}, *reports]),
        fetchval=AsyncMock(return_value=0),
        fetch=AsyncMock(return_value=[]),
    )


@pytest.mark.asyncio
async def test_legacy_reference_rollup_preserves_its_resource_relation_and_cleanup():
    connection = connection_mock(reports=[report()])
    async with rollup.staged_fhir_catalog_references(
        connection, schema_name="source_schema", dataset_id="dataset"
    ) as stage:
        assert stage.accounting.source_rows == 0
        assert [batch async for batch in stage.iter_batches()] == []
    query = connection.fetchrow.await_args_list[1].args[0]
    assert 'FROM "source_schema".provider_directory_dataset_resource' in query
    assert "catalog_resources" not in query
    assert connection.execute.await_args_list[0].args[0].startswith("CREATE TEMP TABLE ")
    assert connection.execute.await_args_list[-1].args[0].startswith("DROP TABLE IF EXISTS pg_temp.")
    assert not stage.active


@pytest.mark.asyncio
async def test_published_rollup_uses_global_sql_pages_and_precreated_workspace(published_scope):
    connection = connection_mock(
        read_only="on", reports=[report(2, last_id="role-002"), report(1, last_id="role-003"), report()]
    )
    connection.is_in_transaction.return_value = False
    connection.fetchval.side_effect = [123, 123, 1]
    connection.fetch.side_effect = [
        [
            {
                "resource_type": "PractitionerRole",
                "resource_id": "role-001",
                "payload_hash": "a" * 64,
                "reference_ordinal": 0,
                "reference_rows": 3,
                "network_ref": "Organization/network",
            }
        ],
        [],
    ]
    async with rollup.fhir_catalog_reference_workspace(connection) as workspace:
        assert "ON COMMIT PRESERVE ROWS" in connection.execute.await_args.args[0]
        connection.is_in_transaction.return_value = True
        async with rollup.staged_fhir_catalog_references(
            connection,
            schema_name="source_schema",
            dataset_id="dataset",
            source_read=published_scope.read,
            workspace=workspace,
            page_rows=2,
            page_bytes=1024,
        ) as stage:
            observations = [observation async for batch in stage.iter_batches() for observation in batch]
            assert stage.accounting.source_rows == stage.accounting.reference_rows == 3
            assert stage.accounting.distinct_references == 1
            assert stage.accounting.resource_counts == {"OrganizationAffiliation": 0, "PractitionerRole": 3}
            assert observations[0].reference_rows == 3 and observations[0].edge.network_resource_id == "network"
        assert not stage.active and workspace.active
        assert connection.execute.await_count == 2
        assert connection.execute.await_args.args[0].startswith("DELETE FROM pg_temp.")
        connection.is_in_transaction.return_value = False
    assert not workspace.active
    assert connection.execute.await_args.args[0].startswith("DROP TABLE IF EXISTS pg_temp.")
    assert published_scope.verify.await_count == 6
    assert published_scope.verify.await_args_list[0].kwargs == {
        "schema_name": "source_schema",
        "dataset_id": "dataset",
    }
    queries = connection.fetchrow.await_args_list[1:]
    for query in queries:
        sql = query.args[0]
        assert "FROM (" + published_scope.select.return_value + ") AS catalog_resources" in sql
        assert "provider_directory_dataset_resource" not in sql
        assert sql.count("LIMIT $5") == 1 and sql.count("sum(source_bytes) OVER") == 1
        assert "jsonb_array_elements" in sql and "GROUP BY resource_type,reference_json::text" in sql
        assert query.args[-3:-1] == (2, 1024)
    assert queries[1].args[3:5] == ("PractitionerRole", "role-002")
    assert queries[2].args[3:5] == ("PractitionerRole", "role-003")
    assert all("payload_json" not in call.args[0] for call in connection.fetch.await_args_list)


@pytest.mark.asyncio
async def test_reference_expansion_reduces_batch_size_without_counting_the_rejected_page(monkeypatch):
    monkeypatch.setattr(rollup, "MAX_PAGE_REFERENCES", 3)
    oversized_report_by_field = {**report(2, last_id="role-002"), "reference_rows": 4}
    connection = connection_mock(reports=[oversized_report_by_field, report(1, last_id="role-001"), report()])
    async with rollup.staged_fhir_catalog_references(
        connection, schema_name="source_schema", dataset_id="dataset", page_rows=2, page_bytes=1024
    ) as stage:
        assert stage.accounting.source_rows == stage.accounting.reference_rows == 1
    requests = connection.fetchrow.await_args_list[1:]
    assert [request.args[-3:-1] for request in requests] == [(2, 1024), (1, 1024), (1, 1024)]
    assert requests[0].args[3:5] == requests[1].args[3:5] == ("", "")
    assert requests[2].args[3:5] == ("PractitionerRole", "role-001")
    assert not stage.active


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes,reason", [({"source_bytes": 1025}, "source_page_bounds"), ({"invalid_rows": 1}, "source_page_invalid")]
)
async def test_invalid_reference_page_refuses_output_and_drops_its_owned_stage(changes, reason):
    connection = connection_mock(reports=[{**report(1, last_id="role-001"), **changes}])
    with pytest.raises(ValueError, match=reason):
        async with rollup.staged_fhir_catalog_references(
            connection, schema_name="source_schema", dataset_id="dataset", page_bytes=1024
        ):
            pytest.fail("invalid source page reached output")
    connection.fetch.assert_not_awaited()
    assert connection.execute.await_args.args[0].startswith("DROP TABLE IF EXISTS pg_temp.")


@pytest.mark.asyncio
async def test_reference_workspace_cannot_be_borrowed_by_another_task():
    import asyncio

    connection = connection_mock()
    connection.is_in_transaction.return_value = False
    connection.fetchval.return_value = 123
    async with rollup.fhir_catalog_reference_workspace(connection) as workspace:
        await workspace.verify(connection)
        with pytest.raises(ValueError, match="workspace_closed"):
            await asyncio.create_task(workspace.verify(connection))
    assert not workspace.active


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["duck", "coordinates", "workspace"])
async def test_published_reference_scope_refuses_before_temporary_writes(published_scope, mode):
    connection = connection_mock(read_only="on")
    source_read = published_scope.read
    if mode == "duck":
        source_read = SimpleNamespace(verify=AsyncMock(), resource_sql=Mock())
        connection.fetchrow.side_effect = [{"isolation": "repeatable read", "read_only": "off"}]
    elif mode == "coordinates":
        connection.fetchrow.side_effect = [{"isolation": "repeatable read", "read_only": "off"}]
        published_scope.verify.side_effect = ValueError("dataset changed")
    with pytest.raises(ValueError):
        async with rollup.staged_fhir_catalog_references(
            connection, schema_name="source_schema", dataset_id="dataset", source_read=source_read
        ):
            pytest.fail("invalid scope yielded a stage")
    connection.execute.assert_not_awaited()
    published_scope.select.assert_not_called()


@pytest.mark.asyncio
async def test_published_reference_stage_reverifies_before_output(published_scope):
    connection = connection_mock(reports=[report()])
    async with rollup.staged_fhir_catalog_references(
        connection, schema_name="source_schema", dataset_id="dataset", source_read=published_scope.read
    ) as stage:
        published_scope.verify.side_effect = ValueError("published read changed")
        with pytest.raises(ValueError, match="published read changed"):
            await anext(stage.iter_batches())
    connection.fetch.assert_not_awaited()
    assert not stage.active
    assert connection.execute.await_args.args[0].startswith("DROP TABLE IF EXISTS pg_temp.")


@pytest.mark.asyncio
async def test_workspace_must_be_created_before_the_snapshot_transaction():
    connection = connection_mock()
    with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="workspace_transaction"):
        async with rollup.fhir_catalog_reference_workspace(connection):
            pytest.fail("workspace created inside an existing snapshot")
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_creation_failure_cleans_owned_name_and_preserves_original_failure():
    connection = connection_mock()
    connection.is_in_transaction.return_value = False
    error = RuntimeError("synthetic stage creation failure")
    connection.execute.side_effect = [error, None]
    with pytest.raises(RuntimeError) as caught:
        async with rollup.fhir_catalog_reference_workspace(connection):
            pytest.fail("failed workspace creation yielded a handle")
    assert caught.value is error
    assert connection.execute.await_count == 2
    assert connection.execute.await_args.args[0].startswith("DROP TABLE IF EXISTS pg_temp.")
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_cleanup_does_not_query_a_closed_connection():
    connection = connection_mock()
    connection.is_in_transaction.return_value = False
    connection.fetchval.return_value = 123
    async with rollup.fhir_catalog_reference_workspace(connection) as workspace:
        connection.is_closed.return_value = True
    assert not workspace.active
    assert connection.execute.await_count == 1


@pytest.mark.asyncio
async def test_workspace_cleanup_leaves_failed_transaction_to_its_existing_rollback():
    import asyncpg

    connection = connection_mock()
    connection.is_in_transaction.return_value = False
    connection.fetchval.return_value = 123
    connection.execute.side_effect = [None, asyncpg.InFailedSQLTransactionError("synthetic transaction failure")]
    async with rollup.fhir_catalog_reference_workspace(connection) as workspace:
        assert workspace.active
    assert not workspace.active
    assert connection.execute.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        {"schema_name": "source_schema;other"},
        {"dataset_id": " padded"},
        {"page_rows": True},
        {"page_bytes": 0},
    ],
)
async def test_reference_rollup_refuses_invalid_bounds_and_identifiers_before_queries(arguments):
    connection = connection_mock()
    options_by_name = {"schema_name": "source_schema", "dataset_id": "dataset", **arguments}
    with pytest.raises(rollup.FHIRCatalogReferenceRollupError):
        async with rollup.staged_fhir_catalog_references(connection, **options_by_name):
            pytest.fail("invalid source page arguments reached staging")
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["no_transaction", "readonly_without_workspace", "unissued_workspace"])
async def test_reference_rollup_requires_pinned_snapshot_and_original_workspace(state):
    connection = connection_mock(read_only="on" if state != "no_transaction" else "off")
    connection.is_in_transaction.return_value = state != "no_transaction"
    options_by_name = {"workspace": {}} if state == "unissued_workspace" else {}
    with pytest.raises(rollup.FHIRCatalogReferenceRollupError):
        async with rollup.staged_fhir_catalog_references(
            connection, schema_name="source_schema", dataset_id="dataset", **options_by_name
        ):
            pytest.fail("unqualified snapshot or workspace reached source reads")
    connection.execute.assert_not_awaited()
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_workspace_cannot_move_to_a_different_connection_or_survive_its_context():
    connection = connection_mock()
    connection.is_in_transaction.return_value = False
    connection.fetchval.return_value = 123
    other = connection_mock(read_only="on")
    async with rollup.fhir_catalog_reference_workspace(connection) as workspace:
        with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="workspace_closed"):
            async with rollup.staged_fhir_catalog_references(
                other, schema_name="source_schema", dataset_id="dataset", workspace=workspace
            ):
                pytest.fail("workspace borrowed another session")
        other.execute.assert_not_awaited()
    with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="workspace_closed"):
        await workspace.verify(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("page_rows", [0, True, rollup.MAX_OUTPUT_ROWS + 1])
async def test_stage_output_keeps_its_bounded_page_contract(page_rows):
    connection = connection_mock(reports=[report()])
    async with rollup.staged_fhir_catalog_references(
        connection, schema_name="source_schema", dataset_id="dataset"
    ) as stage:
        with pytest.raises(rollup.FHIRCatalogReferenceRollupError):
            await anext(stage.iter_batches(page_rows=page_rows))
        connection.fetch.assert_not_awaited()
    assert not stage.active


@pytest.mark.asyncio
async def test_one_source_record_cannot_exceed_reference_expansion_budget():
    selected_report_by_field = {**report(1, last_id="role-one"), "reference_rows": rollup.MAX_PAGE_REFERENCES + 1}
    connection = connection_mock(reports=[selected_report_by_field])
    with pytest.raises(rollup.FHIRCatalogReferenceRollupError, match="expansion_bounds"):
        async with rollup.staged_fhir_catalog_references(
            connection, schema_name="source_schema", dataset_id="dataset", page_rows=1
        ):
            pytest.fail("unbounded reference expansion yielded a stage")
    assert connection.execute.await_args.args[0].startswith("DROP TABLE IF EXISTS pg_temp.")
    connection.fetch.assert_not_awaited()
