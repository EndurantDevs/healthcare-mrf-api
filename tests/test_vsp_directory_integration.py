# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Official JSON acquisition occupies a normal, isolated endpoint dataset."""

import importlib
import json
from contextlib import asynccontextmanager, nullcontext
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_generic_source_adapters as source_adapters
from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_generic_source_storage as source_storage
from process import vsp_directory_capture as capture_api
from process import vsp_directory_integration as adapter
from process.ext import source_http_proxy as transport
from process.vsp_directory_contract import (
    ADAPTER_ID,
    ENDPOINT_SCOPE,
    RESOURCE_TYPES,
    SOURCE_ID,
    TRANSPORT,
    VSPDirectoryError,
)
from process.vsp_directory_normalization import vsp_network_organizations
from scripts.research import provider_directory_endpoint_acquisition_harness as harness

directory = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def source_record():
    record = adapter.source_row(datetime(2026, 1, 1))
    record["_endpoint_dataset_id"] = "dataset-example"
    record["_resource_hash_contract"] = directory.DEFAULT_RESOURCE_HASH_CONTRACT
    record["_semantic_projection_as_of"] = "2026-01-01"
    record["_pagination_checkpoint_context"] = directory.PaginationCheckpointContext(
        canonical_api_base=ENDPOINT_SCOPE,
        source_scope_hash="example-scope",
        source_ids=(SOURCE_ID,),
        owner_run_id="run-example",
        retry_of_run_id="root-example",
        acquisition_root_run_id="root-example",
        endpoint_id="endpoint-example",
        dataset_id="dataset-example",
        lineage_verified=True,
    )
    return record


@pytest.fixture
def completed_capture():
    resources = [
        {"resourceType": "Location", "id": "site"},
        *vsp_network_organizations(),
        {"resourceType": "Practitioner", "id": "doctor"},
        {"resourceType": "PractitionerRole", "id": "role", "practitioner": {"reference": "Practitioner/doctor"}},
    ]
    counts_by_family = {kind: sum(item["resourceType"] == kind for item in resources) for kind in RESOURCE_TYPES}
    return SimpleNamespace(
        rows=resources,
        iter_resources=lambda: iter(resources),
        resource_counts=counts_by_family,
        proof={
            "adapter_id": ADAPTER_ID,
            "normalized_output_only": True,
            "capture_sha256": "a" * 64,
            "resource_counts": counts_by_family.copy(),
        },
    )


@pytest.fixture
def proxy(monkeypatch, request):
    is_enabled = getattr(request, "param", True)
    monkeypatch.delenv(transport.POLICY_ENV, raising=False)
    for name in (directory.source_proxy.BINDING_ENV, directory.source_proxy.COOLDOWNS_ENV):
        monkeypatch.delenv(name, raising=False)
    if is_enabled is not None:
        rule_by_field = {"source_id": SOURCE_ID, "endpoint_scope": ENDPOINT_SCOPE, "use_proxy": is_enabled}
        monkeypatch.setenv(
            transport.POLICY_ENV, json.dumps({"version": 1, "revision": "example", "sources": [rule_by_field]})
        )
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {})
    token, scope = directory.source_proxy.enter_run(directory)
    try:
        if is_enabled:
            scope.bindings[SOURCE_ID] = object()
        yield scope
    finally:
        directory.source_proxy.exit_run(token)


def test_vsp_catalog_is_runnable_only_on_explicit_selection():
    entry = next(
        entry for entry in provider_directory_source_catalog()["items"] if entry["entry_id"] == adapter.ENTRY_ID
    )
    assert entry["canonical_base"] == ENDPOINT_SCOPE and entry["source_ids"] == [SOURCE_ID]
    assert entry["runnable"] is True and entry["profile_enabled"] is False
    assert entry["resources"] == entry["supported_resources"] == list(RESOURCE_TYPES)
    manifest = harness.load_manifest()
    assert all(item["entry_id"] != adapter.ENTRY_ID for item in harness._selected_entries(manifest, frozenset()))
    (selected,) = harness._selected_entries(manifest, frozenset({adapter.ENTRY_ID}))
    params = harness.entry_params(manifest, selected)
    assert params["source_ids"] == [SOURCE_ID] and params["resources"].split(",") == list(RESOURCE_TYPES)
    assert params["defer_typed_materialization"] and params["full_refresh"]
    assert params["resource_deadline_seconds"] == 518400
    assert not any(
        params[key]
        for key in (
            "probe",
            "stale_cleanup",
            "publish_artifacts",
            "publish_after_acquisition",
        )
    )
    assert manifest["support_documentation"]["entry_support"][adapter.ENTRY_ID]["method"] == "official-json"
    assert adapter.explicit_selection([SOURCE_ID], None) and adapter.explicit_selection([], "VSP Signature")
    assert not adapter.explicit_selection([], None) and not adapter.explicit_selection(["other"], "vsp")
    with pytest.raises(VSPDirectoryError, match="^vsp_single_source_required$"):
        adapter.explicit_selection(["other", SOURCE_ID], None)


def test_vsp_checkpoint_keeps_the_acquisition_root_on_retry():
    record = adapter.source_row(datetime(2026, 1, 1))
    fresh = directory._resource_group_pagination_checkpoint_context(
        [record],
        run_id="run-example",
        retry_of_run_id=None,
        pagination_root_run_id=None,
        is_checkpointing_enabled=True,
    )
    resumed = directory._resource_group_pagination_checkpoint_context(
        [record],
        run_id="retry-example",
        retry_of_run_id="run-example",
        pagination_root_run_id="run-example",
        is_checkpointing_enabled=True,
    )
    assert fresh is not None and resumed is not None
    assert fresh.canonical_api_base == resumed.canonical_api_base == ENDPOINT_SCOPE
    assert fresh.source_ids == resumed.source_ids == (SOURCE_ID,)
    assert fresh.source_scope_hash == resumed.source_scope_hash
    assert fresh.acquisition_root_run_id == resumed.acquisition_root_run_id == "run-example"
    assert resumed.owner_run_id == "retry-example"


async def _vsp_retry_candidate(monkeypatch, source_record, context, header_by_field):
    """Select and build the same candidate used for a retained VSP retry."""
    monkeypatch.setattr(directory, "_checkpoint_candidate_dataset_id", AsyncMock(return_value=None))
    monkeypatch.setattr(directory, "_previous_endpoint_dataset_id", AsyncMock(return_value=None))
    profile = directory.EndpointDatasetVerificationProfile(is_twin_root_required=False)
    selection = await directory._select_resumable_endpoint_dataset_candidate(
        header_by_field,
        context.dataset_id,
        context.endpoint_id,
        RESOURCE_TYPES,
        context.acquisition_root_run_id,
        context.retry_of_run_id,
        context,
        profile,
    )
    return directory._build_endpoint_dataset_candidate(
        [source_record],
        context.endpoint_id,
        RESOURCE_TYPES,
        RESOURCE_TYPES,
        selection,
        context.owner_run_id,
        context,
        profile,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("rearmed", [False, True])
async def test_vsp_retry_reopens_verified_detached_inputs_without_reset(monkeypatch, source_record, rearmed):
    context = source_record["_pagination_checkpoint_context"]
    header_by_field = {
        "endpoint_id": context.endpoint_id,
        "dataset_id": context.dataset_id,
        "acquisition_root_run_id": context.acquisition_root_run_id,
        "import_run_id": context.acquisition_root_run_id if rearmed else context.retry_of_run_id,
        "resource_count": 0,
        "is_current": False,
        "status": directory.ENDPOINT_DATASET_ACQUIRING if rearmed else directory.ENDPOINT_DATASET_FAILED,
    }
    candidate = await _vsp_retry_candidate(monkeypatch, source_record, context, header_by_field)
    assert candidate.checkpoint_context.require_retained_vsp_capture is not rearmed
    header_by_field["publication_metadata_json"] = directory._endpoint_dataset_candidate_metadata(candidate)
    if rearmed:
        header_by_field["publication_metadata_json"]["error"] = "vsp_prepared_candidate_required"
    connection = SimpleNamespace(first=AsyncMock(return_value=header_by_field))
    monkeypatch.setattr(source_policy, "source_transaction", lambda _db: nullcontext())
    monkeypatch.setattr(source_adapters, "require_source_transaction", Mock())
    monkeypatch.setattr(source_adapters, "require_generic_source_layout", AsyncMock())
    monkeypatch.setattr(directory.db, "acquire", lambda: nullcontext(connection))
    monkeypatch.setattr(directory, "_assert_no_conflicting_endpoint_candidate", AsyncMock(return_value=candidate))
    prepare, verify, upsert, reset = AsyncMock(), AsyncMock(return_value=True), AsyncMock(), AsyncMock()
    monkeypatch.setattr(source_storage, "prepare_generic_source_candidate", prepare)
    monkeypatch.setattr(source_storage, "has_empty_isolated_vsp_storage", verify)
    monkeypatch.setattr(directory, "_upsert_endpoint_dataset_candidate", upsert)
    monkeypatch.setattr(directory, "_reset_new_endpoint_candidate", reset)
    legacy_empty = AsyncMock(side_effect=AssertionError("detached inputs cannot use the canonical resource count"))
    monkeypatch.setattr(directory, "_assert_empty_endpoint_dataset_orphan", legacy_empty)

    await directory._ensure_endpoint_dataset_candidate(candidate)
    verify.assert_awaited_once()
    legacy_empty.assert_not_awaited()
    upsert.assert_awaited_once()
    prepare.assert_not_awaited()
    reset.assert_not_awaited()
    header_by_field["import_run_id"] = "other-run"
    with pytest.raises(RuntimeError, match="^provider_directory_endpoint_dataset_candidate_stale$"):
        await directory._ensure_endpoint_dataset_candidate(candidate)
    upsert.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "error"),
    [("import_resources", False, "import_required"), ("publish_after_acquisition", True, "incompatible_mode")],
)
async def test_deferred_directory_rejects_incompatible_work_before_acquisition(monkeypatch, field, value, error):
    manifest = harness.load_manifest()
    (selected,) = harness._selected_entries(manifest, frozenset({adapter.ENTRY_ID}))
    params = harness.entry_params(manifest, selected)
    params[field] = value
    storage, capture = AsyncMock(), AsyncMock()
    monkeypatch.setattr(directory, "ensure_database", storage)
    monkeypatch.setattr(capture_api, "capture_vsp_directory", capture)
    with pytest.raises(ValueError, match=f"^provider_directory_deferred_typed_materialization_{error}$"):
        await directory.process_provider_directory_fhir_data({"context": {}}, params)
    storage.assert_not_awaited()
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_import_seeds_only_the_directory_and_refuses_test_capture(monkeypatch):
    for name in (
        "ensure_database",
        "_prepare_provider_directory_job_tables",
        "_raise_if_resource_import_cancelled",
        "_clear_resource_rows_seen",
        "_mark_provider_directory_progress",
    ):
        monkeypatch.setattr(directory, name, AsyncMock())
    for name in ("_resolve_seed_db", "_resolve_retest_results", "_seed_rows_from_supplemental_catalogs"):
        monkeypatch.setattr(directory, name, Mock(side_effect=AssertionError("unrelated catalog selection")))
    monkeypatch.delenv(transport.POLICY_ENV, raising=False)
    monkeypatch.delenv(directory.source_proxy.BINDING_ENV, raising=False)
    store = AsyncMock(side_effect=OSError("synthetic storage unavailable"))
    monkeypatch.setattr(directory, "_upsert_provider_directory_source_rows", store)
    task_by_field = {"source_ids": [SOURCE_ID], "seed_only": True, "run_id": "example"}
    with pytest.raises(VSPDirectoryError, match="^vsp_test_capture_forbidden$"):
        await directory.process_provider_directory_fhir_data({"context": {}}, {**task_by_field, "test": True})
    store.assert_not_awaited()
    with pytest.raises(OSError, match="^synthetic storage unavailable$"):
        await directory.process_provider_directory_fhir_data({"context": {}}, task_by_field)
    (selected,) = store.await_args.args[0]
    assert adapter.is_source(selected) and selected["metadata_json"]["provider_directory_fhir_endpoint"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("proxy", [True, False, None], indirect=True, ids=["proxied", "direct", "default"])
async def test_probe_reuses_capture_root_without_claiming_completeness(monkeypatch, source_record, proxy):
    proof_by_field = {"network": "Signature", "total_count": 3, "capture_sha256": "b" * 64, "complete": False}
    fetch = AsyncMock(return_value=proof_by_field)
    monkeypatch.setattr(capture_api, "probe_vsp_directory", fetch)
    result, capability = await directory._probe_source(source_record, timeout=3, run_id="root-example")
    fetch.assert_awaited_once_with(
        source_record=source_record, root_run_id="root-example", proxy_binding=proxy.bindings.get(SOURCE_ID)
    )
    assert result["status"] == "valid" and result["source_census"] == proof_by_field and capability is None
    fetch.reset_mock()
    with pytest.raises(VSPDirectoryError, match="^vsp_probe_root_required$"):
        await directory._probe_source(source_record, timeout=3, run_id=None)
    fetch.assert_not_awaited()


def _mock_candidate_storage(monkeypatch, source_record):
    context = source_record["_pagination_checkpoint_context"]

    @asynccontextmanager
    async def checkpoint_guard(checkpoint):
        assert checkpoint is not None and checkpoint.source_ids == (SOURCE_ID,)
        assert checkpoint.acquisition_root_run_id == context.acquisition_root_run_id
        yield

    monkeypatch.setattr(directory, "_pagination_checkpoint_worker_guard", checkpoint_guard)
    candidate = directory.EndpointDatasetCandidate(
        endpoint_id=context.endpoint_id,
        dataset_id=context.dataset_id,
        acquisition_root_run_id=context.acquisition_root_run_id,
        source_ids=(SOURCE_ID,),
        selected_resources=RESOURCE_TYPES,
        import_run_id=context.owner_run_id,
        previous_dataset_id=None,
        expected_resources=RESOURCE_TYPES,
        checkpoint_context=context,
    )
    finalize = AsyncMock(return_value={"status": directory.ENDPOINT_DATASET_VALIDATED})
    fail = AsyncMock()
    write = AsyncMock(side_effect=lambda model, rows, **scope: len(rows))
    for attribute, replacement in {
        "_prepare_resource_import_source_group": AsyncMock(return_value=([source_record], candidate)),
        "_upsert_deferred_resource_rows": write,
        "_finalize_endpoint_dataset_candidate": finalize,
        "_clear_finalized_dataset_checkpoints": AsyncMock(),
        "_update_source_resource_import_metadata": AsyncMock(),
        "_mark_endpoint_dataset_candidate": fail,
        "_fetch_resource_rows": AsyncMock(side_effect=AssertionError("unexpected REST request")),
    }.items():
        monkeypatch.setattr(directory, attribute, replacement)
    return write, finalize, fail


async def _stage(record, **overrides):
    context = record["_pagination_checkpoint_context"]
    params_by_name = dict(
        resources=list(RESOURCE_TYPES),
        per_resource_limit=0,
        page_limit=0,
        page_count=10,
        timeout=3,
        run_id=context.owner_run_id,
        retry_of_run_id=context.retry_of_run_id,
        pagination_root_run_id=context.acquisition_root_run_id,
        stream_batch_size=10,
        is_pagination_checkpointing_enabled=True,
        defer_typed_materialization=True,
        provider_directory_endpoint_scope=ENDPOINT_SCOPE,
    )
    params_by_name.update(overrides)
    return await directory._import_resources([record], **params_by_name)


@pytest.mark.asyncio
async def test_short_candidate_write_closes_the_stream_and_reports_capture_progress(
    monkeypatch, source_record, proxy, completed_capture
):
    write, finalize, fail = _mock_candidate_storage(monkeypatch, source_record)
    write.side_effect = lambda model, rows, **scope: len(rows) - 1
    counts = completed_capture.resource_counts
    counts["Location"] = 513
    completed_capture.proof["resource_counts"] = counts.copy()
    read, closed = [], []

    def stream():
        try:
            for index in range(counts["Location"]):
                read.append(index)
                yield {"resourceType": "Location", "id": f"office-{index:04d}"}
            yield from completed_capture.rows[1:]
        finally:
            closed.append(True)

    completed_capture.iter_resources = stream
    progress_by_field = {"phase": "discovery", "office_count": 7}

    async def capture(**kwargs):
        await kwargs["progress_callback"](progress_by_field)
        return completed_capture

    monkeypatch.setattr(capture_api, "capture_vsp_directory", capture)
    progress = AsyncMock()
    with pytest.raises(VSPDirectoryError, match="^vsp_candidate_write_incomplete$"):
        await _stage(source_record, progress_callback=progress)
    assert any(
        group.get("directory_capture") == progress_by_field
        for call in progress.await_args_list
        for group in (call.args[3] or {}).get("active_source_groups", [])
    )
    write.assert_awaited_once()
    assert len(read) == len(write.await_args.args[1]) == 512 and closed == [True]
    finalize.assert_not_awaited()
    fail.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("proxy", [True, False], indirect=True, ids=["proxied", "direct"])
async def test_normal_candidate_stages_exact_six_family_proof(monkeypatch, source_record, proxy, completed_capture):
    write, finalize, fail = _mock_candidate_storage(monkeypatch, source_record)
    take = AsyncMock(return_value=completed_capture)
    monkeypatch.setattr(capture_api, "capture_vsp_directory", take)
    counts = await _stage(source_record)
    assert counts == completed_capture.resource_counts
    assert take.await_count == 1 and take.await_args.kwargs["root_run_id"] == "root-example"
    assert take.await_args.kwargs["proxy_binding"] is proxy.bindings.get(SOURCE_ID)
    assert {call.kwargs["dataset_id"] for call in write.await_args_list} == {"dataset-example"}
    assert sum(len(call.args[1]) for call in write.await_args_list) == sum(counts.values())
    assert all(row["source_id"] == SOURCE_ID for call in write.await_args_list for row in call.args[1])
    finalize.assert_awaited_once()
    candidate, evidence = finalize.await_args.args
    assert candidate.selected_resources == RESOURCE_TYPES and set(evidence) == set(RESOURCE_TYPES)
    for kind, count in counts.items():
        assert evidence[kind]["complete"] and evidence[kind]["fetch_mode"] == TRANSPORT
        assert evidence[kind]["normalized_output_only"] and evidence[kind]["rows_written"] == count
        assert evidence[kind]["vsp_capture"] == completed_capture.proof
    assert counts["InsurancePlan"] == counts["OrganizationAffiliation"] == 0
    fail.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("damage", "code"),
    [
        ("count", "vsp_capture_counts_changed"),
        ("order", "vsp_capture_resource_order_invalid"),
        ("proof", "vsp_capture_proof_invalid"),
        ("partial", "vsp_full_deferred_acquisition_required"),
    ],
)
async def test_incomplete_directory_cannot_finalize(monkeypatch, source_record, proxy, completed_capture, damage, code):
    _, finalize, fail = _mock_candidate_storage(monkeypatch, source_record)
    take = AsyncMock(return_value=completed_capture)
    monkeypatch.setattr(capture_api, "capture_vsp_directory", take)
    match damage:
        case "count":
            completed_capture.rows.pop()
        case "order":
            completed_capture.rows.insert(1, completed_capture.rows[0])
        case "proof":
            completed_capture.proof["resource_counts"]["Location"] = True
    with pytest.raises(VSPDirectoryError, match=f"^{code}$"):
        await _stage(source_record, per_resource_limit=int(damage == "partial"))
    if damage == "partial":
        take.assert_not_awaited()
    finalize.assert_not_awaited()
    fail.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("proxy", "damage", "error", "code"),
    [
        (True, "missing-binding", transport.SourceProxyError, "source_proxy_scope_unprepared"),
        (False, "missing-scope", VSPDirectoryError, "vsp_source_transport_scope_required"),
        (False, "unexpected-binding", VSPDirectoryError, "vsp_source_binding_changed"),
        (True, "credentials", VSPDirectoryError, "vsp_source_credentials_forbidden"),
        (False, "credentials", VSPDirectoryError, "vsp_source_credentials_forbidden"),
    ],
    indirect=["proxy"],
)
async def test_invalid_transport_never_reaches_capture(monkeypatch, source_record, proxy, damage, error, code):
    fetch = AsyncMock()
    monkeypatch.setattr(capture_api, "probe_vsp_directory", fetch)
    match damage:
        case "missing-binding":
            proxy.bindings.clear()
        case "missing-scope":
            monkeypatch.setattr(directory.source_proxy, "current_run", lambda: None)
        case "unexpected-binding":
            proxy.expected_binding = {"source_id": SOURCE_ID}
        case "credentials" if SOURCE_ID in proxy.bindings:
            proxy.credentials[SOURCE_ID] = {"headers": {"X-Example": "example"}}
        case "credentials":
            monkeypatch.setattr(
                directory, "_load_credentials_config", lambda: {"defaults": {"headers": {"X-Example": "example"}}}
            )
    with pytest.raises(error, match=f"^{code}$"):
        await directory._probe_source(source_record, timeout=3, run_id="root-example")
    fetch.assert_not_awaited()


def test_changed_source_identity_is_rejected_before_using_transport(source_record):
    assert adapter.is_source(source_record) and not adapter.is_source({"source_id": "another-source"})
    source_record["canonical_api_base"] = "https://directory.example.test/fhir"
    with pytest.raises(VSPDirectoryError, match="^vsp_source_identity_changed$"):
        directory._resource_import_group_key(source_record)
