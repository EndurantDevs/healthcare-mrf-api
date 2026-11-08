# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Integrated current-version census execution against the shared importer."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_FETCH_MODE,
    current_version_census_completed_proof,
    current_version_census_initial_proof,
)

importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _install_census_fixture_actors(monkeypatch, events, failure, error):
    """Install owned database/component lifetimes and return their original actors."""
    from tests import provider_directory_current_census_postgres_support as support

    url = SimpleNamespace(host="localhost", port=1, database="owned_census_test", username="admin", password=None)
    administrator = SimpleNamespace(engine=SimpleNamespace(url=url), connect=AsyncMock(), disconnect=AsyncMock())
    policy = object()
    publisher = SimpleNamespace(connection=object(), policy=policy)

    @asynccontextmanager
    async def owner_transaction():
        events.append("owner_enter")
        yield
        events.append("owner_exit")

    database = SimpleNamespace(transaction=owner_transaction)
    component = SimpleNamespace(database=database, policy=policy)

    @asynccontextmanager
    async def owned_database(actual_url):
        assert actual_url is url
        events.append("database_enter")
        try:
            yield url, object()
        finally:
            events.append("database_exit")

    @asynccontextmanager
    async def begin():
        yield SimpleNamespace(exec_driver_sql=AsyncMock())

    async def dispose():
        events.append("engine_dispose")

    engine = SimpleNamespace(begin=begin, dispose=dispose)

    @asynccontextmanager
    async def native_component(actual_engine, schema, settings, *, legacy_builder):
        assert actual_engine is engine and legacy_builder is True
        assert schema.startswith("provider_current_census_")
        assert settings is not monkeypatch
        events.append("component_enter")
        try:
            yield component
        finally:
            events.append("component_exit")
            if failure == "component_cleanup":
                raise error

    monkeypatch.setattr(support, "Database", Mock(return_value=administrator))
    monkeypatch.setattr(support, "_owned_database", owned_database)
    monkeypatch.setattr(support, "create_async_engine", Mock(return_value=engine))
    monkeypatch.setattr(support, "native_source_component", native_component)
    return administrator, policy, publisher, database, engine


@pytest.mark.parametrize(
    "failure", [None, "bootstrap", "provision", "custody", "seed", "body", "cancel", "component_cleanup"]
)
async def test_census_native_fixture_owns_component_and_cleans_every_exit(monkeypatch, failure):
    """Preserve every setup/body/cancellation refusal and exact cleanup order."""
    import asyncio

    from tests import provider_directory_current_census_postgres_support as support

    events = []
    error = asyncio.CancelledError() if failure == "cancel" else RuntimeError("synthetic census failure")
    original_database = importer.db
    model_table = importer.ProviderDirectoryDatasetResource.__table__
    original_schema = model_table.schema
    administrator, policy, publisher, database, engine = _install_census_fixture_actors(
        monkeypatch, events, failure, error
    )

    async def prepare(name, *args):
        assert args == (engine, policy)
        events.append(name)
        if failure == name:
            raise error

    async def seed(actual_database, schema):
        assert actual_database is database and importer.db is database
        assert schema == importer._schema() == model_table.schema
        events.append("seed")
        if failure == "seed":
            raise error

    monkeypatch.setattr(support, "_require_disposable_postgres", AsyncMock())
    monkeypatch.setattr(support, "bootstrap_source_component", lambda *args: prepare("bootstrap", *args))
    monkeypatch.setattr(support, "provision_source_component", lambda *args: prepare("provision", *args))
    monkeypatch.setattr(support.source, "bound_publisher", Mock(return_value=publisher))
    capture = AsyncMock(side_effect=error if failure == "custody" else None)
    proof = AsyncMock()
    monkeypatch.setattr(support, "capture_checkpoint_controls", capture)
    monkeypatch.setattr(support, "ensure_dataset_proof_shard_table", proof)
    monkeypatch.setattr(support, "_seed_endpoint_dataset", AsyncMock())
    monkeypatch.setattr(support, "_seed_direct_fetch_dataset", seed)

    async def use_fixture():
        async with support.census_database(monkeypatch) as (actual_database, schema):
            assert actual_database is database and schema == model_table.schema
            assert events.index("bootstrap") < events.index("provision") < events.index("seed")
            capture.assert_awaited_once_with(publisher.connection, policy)
            proof.assert_awaited_once_with(database, schema)
            if failure in {"body", "cancel"}:
                raise error

    if failure is None:
        await use_fixture()
    else:
        with pytest.raises(type(error)) as caught:
            await use_fixture()
        assert caught.value is error
    assert events[-3:] == ["component_exit", "engine_dispose", "database_exit"]
    administrator.disconnect.assert_awaited_once()
    assert importer.db is original_database and model_table.schema == original_schema


@pytest.mark.parametrize("phase", ["loading", "prepared", "published"])
async def test_census_fixture_reads_exact_source_candidate_without_legacy_fallback(monkeypatch, phase):
    from tests import provider_directory_current_census_postgres_support as support

    identity_by_field = {"dataset_id": support.DATASET_ID}
    binding_by_field = {
        "identity": identity_by_field,
        "phase": phase,
        "resets": {"*": 1, support.RESOURCE_TYPE: 2},
        "tables": {"resource_input": {"name": "private_input"}, support.storage.RESOURCE: {"name": "private_final"}},
    }
    queries = []

    async def fetchrow(statement, dataset_id):
        assert dataset_id == support.DATASET_ID
        return {"candidate_storage_json": binding_by_field}

    async def cursor(statement, cutoff, resets, *, prefetch):
        assert phase == "loading" and '"private_input"' in statement
        assert cutoff == 1 and support.storage._json(resets) == binding_by_field["resets"] and prefetch == 4096
        assert "batch_ordinal>greatest" in statement
        queries.append(statement)
        for row in (
            {"resource_type": support.RESOURCE_TYPE, "resource_id": "p-1"},
            {"resource_type": support.RESOURCE_TYPE, "resource_id": "p-1"},
            {"resource_type": "Organization", "resource_id": "foreign"},
        ):
            yield row

    async def fetch(statement, *arguments):
        assert phase != "loading" and '"private_final"' in statement
        assert arguments == (support.DATASET_ID, support.RESOURCE_TYPE)
        queries.append(statement)
        return [{"resource_id": "p-1"}]

    connection = SimpleNamespace(fetchrow=fetchrow, cursor=cursor, fetch=fetch)
    policy = SimpleNamespace(schema="census_test")
    publisher = SimpleNamespace(connection=connection, policy=policy)
    database = object()

    @asynccontextmanager
    async def transaction(actual_database):
        assert actual_database is database
        yield

    locked = AsyncMock(return_value=binding_by_field)
    verify = AsyncMock()
    monkeypatch.setattr(support.source, "source_transaction", transaction)
    monkeypatch.setattr(support.source, "bound_publisher", Mock(return_value=publisher))
    monkeypatch.setattr(support.storage, "_locked", locked)
    monkeypatch.setattr(support.storage, "_verify_tables", verify)
    assert await support.candidate_resource_ids(database, policy.schema) == ["p-1"]
    locked.assert_awaited_once_with(connection, policy, identity_by_field, phases=(phase,))
    verify.assert_awaited_once_with(connection, policy, binding_by_field)
    assert len(queries) == 1 and all('"provider_directory_dataset_resource"' not in query for query in queries)


def _contract(*, resources=("Organization",)):
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=resources,
        expected_nonempty_resources=resources,
        start_urls=tuple(
            (
                resource_type,
                f"{BASE}/{resource_type}?active=true",
            )
            for resource_type in resources
        ),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _source_record(*, resources=("Organization",)):
    return {
        "source_id": "synthetic-source",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            "provider_directory_supported_resources": list(resources),
            "provider_directory_fully_enumerable_resources": list(resources),
        },
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: _contract(resources=resources),
    }


def _checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="run-1",
        retry_of_run_id=None,
        acquisition_root_run_id="run-1",
        dataset_id="dataset-1",
    )


def _organization_bundle(*, next_url=None):
    links = [] if next_url is None else [{"relation": "next", "url": next_url}]
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": links,
        "entry": [
            {
                "fullUrl": f"{BASE}/Organization/org-1",
                "resource": {"resourceType": "Organization", "id": "org-1"},
            }
        ],
    }


def _complete_diagnostic(source_record, resource_type, count):
    contract = source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD]
    initial_proof_by_field = current_version_census_initial_proof(
        contract,
        resource_type,
        count,
        expected_page_count=250,
    )
    return {
        "fetch_mode": CURRENT_VERSION_CENSUS_FETCH_MODE,
        "current_version_census_completeness": (
            current_version_census_completed_proof(
                initial_proof_by_field,
                post_count=count,
                processed_rows=count,
                unique_candidate_rows=count,
                pages_processed=1,
                expected_page_count=250,
                terminal_page_entry_count=count,
            )
        ),
    }


def test_bound_manual_source_is_admitted_without_weakening_generic_gate():
    bound_source = _source_record()
    generic_manual_source_by_field = {
        **bound_source,
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: None,
    }

    assert importer._source_rows_allowed_for_probe([bound_source]) == [bound_source]
    assert importer._source_rows_allowed_for_probe([generic_manual_source_by_field]) == []

    selected_sources, metrics_by_name = importer._select_resource_import_sources(
        [bound_source],
        valid_source_ids={"synthetic-source"},
        open_only=True,
        include_auth_required=False,
        requested_resource_types=["Organization"],
    )
    assert selected_sources == [bound_source]
    assert metrics_by_name["source_import_skipped_manual_only"] == 0


def test_start_url_and_checkpoint_scope_bind_cutoff_and_contract():
    source_record = _source_record()
    start_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    first_scope = importer._pagination_checkpoint_scope_identity(
        source_record,
        ["synthetic-source"],
    )
    source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD] = _contract(resources=("Organization",))
    source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD] = source_record[
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD
    ].__class__(
        **{
            **source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD].__dict__,
            "cutoff": "2026-08-01T13:00:00.000000Z",
        }
    )
    second_scope = importer._pagination_checkpoint_scope_identity(
        source_record,
        ["synthetic-source"],
    )

    assert "active=true" in start_url
    assert "_lastUpdated=lt2026-08-01T12%3A00%3A00.000000Z" in start_url
    assert "_count=250" in start_url
    assert first_scope is not None and second_scope is not None
    assert first_scope[1] != second_scope[1]


def test_strict_next_url_keeps_raw_cursor_and_canonicalizes_replay_identity():
    source_record = _source_record()
    current_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    assert current_url is not None
    raw_next_url = f"{BASE}?_getpages=opaque&_getpagesoffset=250&_count=250&_pretty=true"

    assert (
        importer._resolved_fhir_next_url(
            source_record,
            current_url,
            raw_next_url,
            resource_type="Organization",
            page_entry_count=250,
            pre_total=500,
        )
        == raw_next_url
    )
    assert importer._pagination_url_identity(raw_next_url) == (
        importer._pagination_url_identity(f"{BASE}?_pretty=true&_count=250&_getpagesoffset=250&_getpages=opaque")
    )


def test_multiple_next_links_and_wrong_resource_type_fail_closed():
    multiple_links = _organization_bundle(next_url="first")
    multiple_links["link"].append({"relation": "next", "url": "second"})
    with pytest.raises(ValueError, match="next_link_invalid"):
        importer._current_version_census_next_link(multiple_links)

    for malformed_links in ({"next": "not-a-list"}, ["not-an-object"]):
        malformed_bundle = _organization_bundle()
        malformed_bundle["link"] = malformed_links
        with pytest.raises(ValueError, match="next_link_invalid"):
            importer._current_version_census_next_link(malformed_bundle)

    wrong_resource_bundle = _organization_bundle()
    wrong_resource_bundle["entry"][0]["resource"]["resourceType"] = "Location"
    assert (
        importer._current_version_census_bundle_error(
            wrong_resource_bundle,
            "Organization",
        )
        == "provider_directory_current_version_census_resource_type_mismatch"
    )


@pytest.mark.asyncio
async def test_census_source_fetch_uses_pinned_no_redirect_transport(monkeypatch):
    source_record = _source_record()
    request_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    assert request_url is not None
    source_session = SimpleNamespace(closed=False)
    anonymous_fetch = AsyncMock(return_value=(200, {}, None, 1))
    monkeypatch.setattr(
        importer,
        "_fetch_json_with_source_session",
        anonymous_fetch,
    )
    session_token = importer._SOURCE_HTTP_SESSION.set(source_session)
    try:
        await importer._fetch_source_json_once(
            source_record,
            request_url,
            timeout=5,
        )
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)

    anonymous_fetch.assert_awaited_once_with(
        source_session,
        request_url,
        timeout=5,
        allow_redirects=False,
        preserve_url_bytes=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_reset", [False, True])
async def test_direct_retry_enables_terminal_census_checkpoint_restart(monkeypatch, allow_reset):
    context = importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="retry-run",
        acquisition_root_run_id="root-run",
        retry_of_run_id="failed-run",
        dataset_id="dataset-1",
        allow_retained_pagination_reset=allow_reset,
    )
    load_checkpoint = AsyncMock(side_effect=RuntimeError("stop-after-checkpoint-options"))
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )

    with pytest.raises(RuntimeError, match="stop-after-checkpoint-options"):
        await importer._fetch_resource_rows(
            _source_record(),
            "Organization",
            per_resource_limit=0,
            page_limit=0,
            page_count=250,
            timeout=3,
            run_id="retry-run",
            row_batch_handler=AsyncMock(return_value=0),
            row_batch_size=100,
            retain_rows=False,
            pagination_checkpoint=context,
        )

    assert load_checkpoint.await_args.kwargs == {"allow_terminal_census_restart": allow_reset}


@pytest.mark.asyncio
async def test_command_census_preflight_runs_before_startup(monkeypatch):
    event_names = []

    async def record_startup(_runtime_context):
        event_names.append("startup")

    async def record_process(_runtime_context, task_by_field):
        event_names.append("process")
        assert task_by_field["provider_directory_census_cutoff"] == CUTOFF
        assert task_by_field["restart_expired_current_census_slice"] is False
        return {"ok": True}

    async def record_shutdown(_runtime_context):
        event_names.append("shutdown")

    monkeypatch.setattr(importer, "startup", record_startup)
    monkeypatch.setattr(importer, "process_data", record_process)
    monkeypatch.setattr(importer, "shutdown", record_shutdown)
    command_by_field = {
        "provider_directory_acquisition_strategy": ("cutoff-bounded-current-version-census"),
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": ["synthetic-source"],
        "resources": "Organization",
        "import_resources": True,
        "restart_expired_current_census_slice": False,
    }

    assert await importer.run_provider_directory_fhir_command(**command_by_field) == {"ok": True}
    assert event_names == ["process", "shutdown"]

    event_names.clear()
    with pytest.raises(ValueError, match="cutoff_timezone_required"):
        await importer.run_provider_directory_fhir_command(
            **{
                **command_by_field,
                "provider_directory_census_cutoff": "2026-08-01T12:00:00",
            }
        )
    assert event_names == []


def _exact_census_fetch_mock(event_names):
    """Return a three-step fetch mock that records the post-count boundary."""

    fetch_responses = iter(
        (
            (
                200,
                {"resourceType": "Bundle", "type": "searchset", "total": 1},
                None,
                1,
            ),
            (200, _organization_bundle(), None, 2),
            (
                200,
                {"resourceType": "Bundle", "type": "searchset", "total": 1},
                None,
                1,
            ),
        )
    )

    async def fetch_page(*_args, **_kwargs):
        response = next(fetch_responses)
        if len(fetch_mock.await_args_list) == 3:
            event_names.append("post-count")
        return response

    fetch_mock = AsyncMock(side_effect=fetch_page)
    return fetch_mock


def _install_exact_census_fetch_harness(monkeypatch, start_url):
    """Install one successful terminal exact-census lifecycle harness."""

    event_names = []
    fetch_mock = _exact_census_fetch_mock(event_names)

    async def save_completeness(*_args, **_kwargs):
        event_names.append("proof")

    async def save_checkpoint(*_args, **kwargs):
        event_names.append("terminal" if kwargs["next_url"] is None else "cursor")

    async def persist_rows(_model, resource_rows):
        event_names.append("rows")
        assert [resource_row["resource_id"] for resource_row in resource_rows] == ["org-1"]
        return 1

    monkeypatch.setattr(importer, "_fetch_source_json", fetch_mock)
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(
            return_value=importer.PaginationResumeState(
                next_url=start_url,
                pages_processed=0,
                rows_processed=0,
                recent_url_hashes=(),
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        save_completeness,
    )
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(
            return_value=(
                importer.ProviderDirectoryOrganization,
                {"source_id": "synthetic-source", "resource_id": "org-1"},
            )
        ),
    )
    return event_names, fetch_mock, persist_rows


@pytest.mark.asyncio
async def test_exact_census_persists_page_before_verified_terminal_checkpoint(
    monkeypatch,
):
    """Persist rows and exact proof before one verified terminal checkpoint."""

    from db.connection import Database
    from tests.test_provider_directory_generic_source_policy import synthetic_checkpoint_factory

    source_record = _source_record()
    checkpoint_context = _checkpoint_context()
    start_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    assert start_url is not None
    event_names, fetch_mock, persist_rows = _install_exact_census_fetch_harness(
        monkeypatch,
        start_url,
    )

    database = Database()
    monkeypatch.setattr(importer, "db", database)
    async with synthetic_checkpoint_factory(monkeypatch, database, schema=importer._schema()):
        fetch_result = await importer._fetch_resource_rows(
            source_record,
            "Organization",
            per_resource_limit=0,
            page_limit=0,
            page_count=250,
            timeout=5,
            run_id="run-1",
            row_batch_handler=persist_rows,
            row_batch_size=100,
            retain_rows=False,
            pagination_checkpoint=checkpoint_context,
        )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.fetch_mode == CURRENT_VERSION_CENSUS_FETCH_MODE
    assert fetch_result.fetch_diagnostic["verified"] is True
    assert fetch_result.fetch_diagnostic["pre_count"] == 1
    assert fetch_result.fetch_diagnostic["post_count"] == 1
    assert event_names == ["proof", "post-count", "rows", "terminal"]
    requested_urls = [call.args[1] for call in fetch_mock.await_args_list]
    assert "_summary=count" in requested_urls[0]
    assert requested_urls[1] == start_url
    assert requested_urls[2] == requested_urls[0]


def test_cross_resource_proof_rejects_missing_or_zero_expected_collection():
    source_record = _source_record(resources=("Organization", "Location"))
    complete_diagnostic_by_field = _complete_diagnostic(
        source_record,
        "Organization",
        1,
    )
    with pytest.raises(RuntimeError, match="diagnostics_incomplete"):
        importer._validate_current_version_census_diagnostics(
            [source_record],
            {"Organization": complete_diagnostic_by_field},
        )

    with pytest.raises(ValueError, match="expected_nonempty_zero"):
        importer._validate_current_version_census_diagnostics(
            [source_record],
            {
                "Organization": complete_diagnostic_by_field,
                "Location": _complete_diagnostic(
                    source_record,
                    "Location",
                    0,
                ),
            },
        )
