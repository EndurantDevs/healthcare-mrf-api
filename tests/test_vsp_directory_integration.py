# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Official JSON acquisition occupies a normal, isolated endpoint dataset."""

import importlib
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
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
        retry_of_run_id=None,
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


def _mock_candidate_storage(monkeypatch, record):
    context = record["_pagination_checkpoint_context"]
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
    for name, value in {
        "_prepare_resource_import_source_group": AsyncMock(return_value=([record], candidate)),
        "_upsert_deferred_resource_rows": write,
        "_finalize_endpoint_dataset_candidate": finalize,
        "_clear_finalized_dataset_checkpoints": AsyncMock(),
        "_update_source_resource_import_metadata": AsyncMock(),
        "_mark_endpoint_dataset_candidate": fail,
        "_fetch_resource_rows": AsyncMock(side_effect=AssertionError("unexpected REST request")),
    }.items():
        monkeypatch.setattr(directory, name, value)
    return write, finalize, fail


async def _stage(record, **overrides):
    params_by_name = dict(
        resources=list(RESOURCE_TYPES),
        per_resource_limit=0,
        page_limit=0,
        page_count=10,
        timeout=3,
        run_id="run-example",
        stream_batch_size=10,
        is_pagination_checkpointing_enabled=True,
        defer_typed_materialization=True,
        provider_directory_endpoint_scope=ENDPOINT_SCOPE,
    )
    params_by_name.update(overrides)
    return await directory._import_resources([record], **params_by_name)


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
