# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native control evidence reads stay separate from retained Source sessions."""

import importlib
import json
from contextlib import AsyncExitStack
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateTable

from db.connection import Database
from db.models.system import ImportRun, ProviderDirectoryPaginationCheckpoint
from db.plan_release_storage import storage_identifier
from process import provider_directory_generic_source_policy as source
from process import provider_directory_import_completion as completion
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.provider_directory_source_postgres_support import native_source_component
from tests.test_provider_directory_named_checkpoint_dataflow_postgres import _native_factory

directory = importlib.import_module("process.provider_directory_fhir")
ROOT = "run_" + "1" * 32
FAILED = "run_" + "2" * 32
CURRENT = "run_" + "3" * 32
SOURCE = "synthetic-source"
DATASET = "synthetic-candidate"
NEXT_URL = "https://example.invalid/fhir/Organization?_getpages=opaque%2Bnext"


def _request(parent):
    return {
        "policy_id": "healthporta.import-completion.v1",
        "candidate_id": DATASET,
        "acquisition_root_run_id": ROOT,
        "retry_of_run_id": parent,
        "retained_counts": {"Organization": 199},
        "expected_counts": {"Organization": None},
        "accept_unknown_coverage": True,
    }


def _params(parent=None):
    params_by_name = {
        "source_ids": [SOURCE],
        "provider_directory_endpoint_scope": "https://example.invalid/fhir",
        "provider_directory_pagination_root_run_id": ROOT,
        "allow_retained_pagination_reset": False,
    }
    if parent is not None:
        params_by_name.update(retry_of_run_id=parent, import_completion=_request(parent))
    return params_by_name


async def _tables(connection, owner, control):
    await connection.execute(text("CREATE SCHEMA catalog"))
    run_table = ImportRun.__table__.to_metadata(MetaData(), schema="catalog")
    await connection.execute(CreateTable(run_table))
    checkpoint = ProviderDirectoryPaginationCheckpoint.__table__.to_metadata(MetaData(), schema="source_schema")
    for constraint in tuple(checkpoint.foreign_key_constraints):
        checkpoint.constraints.remove(constraint)
    await connection.execute(CreateTable(checkpoint))
    await connection.execute(
        text(f"ALTER TABLE source_schema.provider_directory_pagination_checkpoint OWNER TO {owner}")
    )
    await connection.execute(text(f"GRANT USAGE ON SCHEMA catalog TO {owner},{control}"))
    await connection.execute(text(f"GRANT SELECT,INSERT,UPDATE ON catalog.import_run TO {control}"))


async def _run_row(connection, run_id, parent, metrics):
    await connection.execute(
        text(
            "INSERT INTO catalog.import_run(run_id,engine,importer,status,params,metrics,retry_of_run_id,"
            "created_at,finished_at) VALUES (:run,'healthcare-mrf-api','provider-directory-fhir','failed',"
            "CAST(:params AS json),CAST(:metrics AS json),:parent,now(),now())"
        ),
        {"run": run_id, "parent": parent, "params": json.dumps(_params(parent)), "metrics": json.dumps(metrics)},
    )


async def _checkpoint(connection):
    await connection.execute(
        text(
            "INSERT INTO source_schema.provider_directory_pagination_checkpoint "
            "(canonical_api_base,resource_type,source_scope_hash,dataset_id,source_ids,acquisition_root_run_id,"
            "owner_run_id,retry_of_run_id,start_url_hash,next_url,state,pages_processed,rows_processed,"
            "recent_cursor_hashes,completeness_json,created_at,updated_at) "
            "VALUES ('https://example.invalid/fhir','Organization','scope',:dataset,CAST(:sources AS json),"
            ":root,:owner,:parent,'hash',:cursor,'active',2,199,'[]','{}',now(),now())"
        ),
        {
            "dataset": DATASET,
            "sources": json.dumps([SOURCE]),
            "root": ROOT,
            "owner": FAILED,
            "parent": ROOT,
            "cursor": NEXT_URL,
        },
    )


@pytest_asyncio.fixture
async def retained_completion_control_database(monkeypatch):
    """Reuse native role and owned-database cleanup with two independent login pools."""
    async with _owned_database(_database_url().set(database="postgres")) as (url, _admin):
        async with AsyncExitStack() as cleanup:
            admin = create_async_engine(url)
            cleanup.push_async_callback(admin.dispose)
            async with admin.begin() as connection:
                await connection.execute(text("CREATE SCHEMA source_schema"))
            component = await cleanup.enter_async_context(
                native_source_component(admin, "source_schema", monkeypatch, legacy_builder=True, bind_factory=False)
            )
            control_engine = create_async_engine(url.set(username=component.builder))
            cleanup.push_async_callback(control_engine.dispose)
            monkeypatch.setenv("HLTHPRT_DB_DATABASE", url.database)
            monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
            database = Database(engine=control_engine, session_factory=async_sessionmaker(control_engine))
            cleanup.push_async_callback(database.disconnect)
            async with admin.begin() as connection:
                await _tables(connection, storage_identifier(component.owner), storage_identifier(component.builder))
                await _run_row(
                    connection,
                    ROOT,
                    None,
                    {
                        "resource_fetch_stats": {
                            "Organization": {
                                "pagination_cooldown_retries": 2,
                                "pagination_cooldown_exhausted_sources": 1,
                            }
                        }
                    },
                )
                await _run_row(
                    connection, FAILED, ROOT, {"resource_rows": {"Organization": 0}, "resource_fetch_stats": {}}
                )
                await _checkpoint(connection)
            monkeypatch.setattr(directory, "db", database)
            monkeypatch.setattr(directory, "_schema", lambda: "catalog")
            factory = _native_factory(component.database.engine, component.policy)
            async with source.bind_generic_source_transaction_factory(factory):
                yield SimpleNamespace(database=database, component=component, factory=factory, admin=admin)


async def _assert_source_denied(case):
    async with case.admin.connect() as connection:
        for role in (case.component.owner, case.component.publisher):
            with pytest.raises(DBAPIError) as refused:
                async with connection.begin():
                    await connection.execute(text("SET LOCAL ROLE " + storage_identifier(role)))
                    await connection.execute(text("SELECT status FROM catalog.import_run"))
            assert refused.value.orig.sqlstate == "42501"
    with pytest.raises(DBAPIError) as refused:
        async with source.source_transaction(case.database):
            await case.database.first("SELECT status FROM catalog.import_run WHERE run_id=:run", run=ROOT)
    assert refused.value.orig.sqlstate == "42501"
    assert case.database._transaction_binding() is None and source._PUBLISHER.get() is None


def _observe_control_reads(case, monkeypatch):
    original = case.database.first
    reads = []

    async def read(statement, **params):
        result = await original(statement, **params)
        if "import_run" in str(statement):
            assert source._FACTORY.get() is case.factory and source._PUBLISHER.get() is None
            mode = await original("SHOW transaction_read_only")
            assert mode[0] == "on"
            reads.append(params["run_id"])
        return result

    monkeypatch.setattr(case.database, "first", read)
    return reads


def _candidate():
    context = directory.PaginationCheckpointContext(
        canonical_api_base="https://example.invalid/fhir",
        source_scope_hash="scope",
        source_ids=(SOURCE,),
        owner_run_id=CURRENT,
        acquisition_root_run_id=ROOT,
        retry_of_run_id=FAILED,
        endpoint_id="synthetic-endpoint",
        dataset_id=DATASET,
        lineage_verified=True,
        allow_retained_pagination_reset=False,
    )
    return directory.EndpointDatasetCandidate(
        endpoint_id=context.endpoint_id,
        dataset_id=DATASET,
        acquisition_root_run_id=ROOT,
        source_ids=(SOURCE,),
        selected_resources=("Organization",),
        expected_resources=("Organization",),
        import_run_id=CURRENT,
        previous_dataset_id=None,
        checkpoint_context=context,
        reused_from_checkpoint=True,
    )


@pytest.mark.asyncio
async def test_retained_completion_reads_control_parent_outside_source_role(
    retained_completion_control_database, monkeypatch
):
    case = retained_completion_control_database
    await _assert_source_denied(case)
    reads = _observe_control_reads(case, monkeypatch)
    candidate = _candidate()
    result, diagnostics = await completion.retained_completion(
        directory, candidate, _request(FAILED), retry_of_run_id=FAILED
    )
    assert reads == [FAILED, ROOT]
    assert result.import_completion["request"] == _request(FAILED)
    assert result.import_completion["retry_evidence"]["Organization"] == {
        "retries": 2,
        "exhausted": True,
        "operator_stopped": False,
    }
    assert diagnostics["Organization"]["retry_evidence_run_ids"] == [FAILED, ROOT]
    assert diagnostics["Organization"]["complete"] is False
    assert diagnostics["Organization"]["next_url_remaining"] is True
    assert diagnostics["Organization"]["rows_fetched"] == 199
    assert candidate.checkpoint_context.allow_retained_pagination_reset is False
    async with case.admin.connect() as connection:
        assert (
            await connection.scalar(text("SELECT next_url FROM source_schema.provider_directory_pagination_checkpoint"))
            == NEXT_URL
        )
    await _assert_source_denied(case)
