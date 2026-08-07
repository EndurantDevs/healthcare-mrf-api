# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave residual fail-closed contracts."""

from __future__ import annotations

from tests.test_ptg_wave_failure_controller_residual import (
    AsyncMock,
    Mock,
    _Request,
    _identity,
    _isolation_controller,
    _synthetic_catalog_metadata,
    _wave,
    barrier,
    bounded_file_windows,
    catalog_manifest,
    control_api,
    display_value,
    fence,
    isolation,
    provider_specialty_filters,
    ptg_control,
    pytest,
    receipt_projection,
    receipts,
    reverse_scope,
    routes,
    types,
    v4_scope,
    wave_worker,
)


@pytest.mark.asyncio
async def test_worker_barrier_factory_resolution_covers_async_and_failure_edges(
    monkeypatch,
):
    identity = _identity()
    ready_barrier = types.SimpleNamespace(
        register_ready=AsyncMock(),
        wait_for_release=AsyncMock(),
    )

    async def async_factory(_identity):
        return ready_barrier

    assert await wave_worker._resolve_barrier(identity, async_factory) is ready_barrier
    with pytest.raises(barrier.PTGWaveContractError, match="ready/release barrier"):
        await wave_worker._resolve_barrier(identity, lambda _identity: object())

    monkeypatch.delenv("HLTHPRT_PTG_WAVE_BARRIER_FACTORY", raising=False)
    with pytest.raises(barrier.PTGWaveContractError, match="is required"):
        wave_worker._factory_from_environment()
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
        "missing.module.factory",
    )
    with pytest.raises(barrier.PTGWaveContractError, match="unavailable"):
        wave_worker._factory_from_environment()
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
        "process.ptg_wave_worker.PTG_WAVE_SLOT_COUNT",
    )
    with pytest.raises(barrier.PTGWaveContractError, match="not callable"):
        wave_worker._factory_from_environment()
    monkeypatch.setenv(
        "HLTHPRT_PTG_WAVE_BARRIER_FACTORY",
        "process.ptg_wave_worker._drain_wave_queue",
    )
    assert wave_worker._factory_from_environment() is wave_worker._drain_wave_queue

    monkeypatch.setenv("HLTHPRT_PTG_WAVE_WORKER_SETTINGS", "process.FHIR")
    with pytest.raises(barrier.PTGWaveContractError, match="process.PTGSmall"):
        await wave_worker._drain_wave_queue(identity)

@pytest.mark.asyncio
async def test_controller_isolation_preflight_and_terminal_run_edges(monkeypatch):
    monkeypatch.setattr(isolation, "_require_generic_redis_idle", AsyncMock())
    bundle = types.SimpleNamespace(
        wave=types.SimpleNamespace(wave_id="wave", intent_count=1),
        intents=[object()],
    )

    with pytest.raises(RuntimeError, match="database work"):
        await isolation.require_ptg_only_idle(
            _isolation_controller([("run",)], []),
            bundle,
            object(),
        )
    with pytest.raises(RuntimeError, match="Kubernetes Job"):
        await isolation.require_ptg_only_idle(
            _isolation_controller([], [{"status": {"active": 1}}]),
            bundle,
            object(),
        )
    short_bundle = types.SimpleNamespace(
        wave=types.SimpleNamespace(wave_id="wave", intent_count=2),
        intents=[object()],
    )
    with pytest.raises(ValueError, match="lost an admitted intent"):
        await isolation.require_ptg_only_idle(
            _isolation_controller([], []),
            short_bundle,
            object(),
        )
    await isolation.require_ptg_only_idle(
        _isolation_controller([], []),
        bundle,
        object(),
    )

    assert await isolation.has_only_terminal_wave_runs(
        _isolation_controller([("succeeded",)], []),
        bundle,
    ) is True
    assert await isolation.has_only_terminal_wave_runs(
        _isolation_controller([], []),
        bundle,
    ) is False
    assert await isolation.has_only_terminal_wave_runs(
        _isolation_controller([("running",)], []),
        bundle,
    ) is False

@pytest.mark.asyncio
async def test_claim_reconciliation_none_and_nonrejected_paths(monkeypatch):
    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(return_value=None),
    )
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(),
        {},
        run_id="run",
        claim_attempt_token="token",
    ) is None

    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(return_value={"run_id": "run"}),
    )
    resolution = types.SimpleNamespace(status="claimed")
    monkeypatch.setattr(
        ptg_control,
        "reconcile_wave_claim_exception",
        AsyncMock(return_value=resolution),
    )
    flushed = AsyncMock()
    monkeypatch.setattr(ptg_control, "_flush_terminal_status_events", flushed)
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(),
        {},
        run_id="run",
        claim_attempt_token="token",
    ) is resolution
    flushed.assert_not_awaited()

    marked = AsyncMock()
    monkeypatch.setattr(ptg_control, "mark_control_run", marked)
    await ptg_control._mark_exact_wave_preexecution_failure(
        "run",
        reason="",
        error=None,
    )
    assert marked.await_args.kwargs["error"]["message"] == "worker start failed"

@pytest.mark.asyncio
async def test_incomplete_claim_payload_is_never_reconciled(monkeypatch):
    reconcile = AsyncMock()
    monkeypatch.setattr(
        ptg_control,
        "_claim_exact_wave_worker_start",
        AsyncMock(side_effect=RuntimeError("claim")),
    )
    monkeypatch.setattr(
        ptg_control,
        "_reconcile_exact_wave_claim_exception",
        reconcile,
    )

    with pytest.raises(RuntimeError, match="claim"):
        await ptg_control.ptg_control_start(
            {}, {"run_id": "run", "params": {"_wave_id": "wave"}}
        )
    reconcile.assert_not_awaited()

@pytest.mark.asyncio
async def test_control_without_run_id_never_installs_live_context(monkeypatch):
    monkeypatch.setattr(ptg_control, "_claim_exact_wave_worker_start", AsyncMock())
    monkeypatch.setattr(ptg_control, "guard_ptg_worker_start", AsyncMock(return_value=None))
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", AsyncMock(return_value=None))
    monkeypatch.setattr(
        ptg_control,
        "validated_worker_frozen_rate_params",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(ptg_control, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(ptg_control, "mark_control_run", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_control, "ptg_main", AsyncMock(return_value={}))
    reset = Mock()
    monkeypatch.setattr(ptg_control, "reset_live_progress_context", reset)

    result = await ptg_control.ptg_control_start({}, {"params": {}})

    assert result == {"status": "succeeded", "run_id": ""}
    reset.assert_not_called()

@pytest.mark.asyncio
async def test_fence_scalar_keyword_fallback_and_idle_admission(monkeypatch):
    class KeywordOnlyScalar:
        async def scalar(self, _statement, **parameters):
            return parameters["relation"]

    assert await fence._scalar(
        KeywordOnlyScalar(), object(), {"relation": "mrf.ptg_import_wave"}
    ) == "mrf.ptg_import_wave"

    monkeypatch.setattr(fence, "_capacity_owning_waves", AsyncMock(return_value=[]))
    monkeypatch.setattr(fence, "_all", AsyncMock(return_value=[]))
    await fence.require_wave_admission_capacity(object())

@pytest.mark.asyncio
async def test_successful_wave_and_discovery_get_routes(monkeypatch):
    monkeypatch.setattr(routes, "require_control_auth", Mock())
    outcomes_page = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(routes, "get_wave_outcomes_page", outcomes_page)
    outcome_response = await routes.control_get_import_wave_outcomes(
        _Request(args={"after_ordinal": "1", "limit": "2"}), "wave"
    )
    assert outcome_response.status == 200
    outcomes_page.assert_awaited_once_with("wave", after_ordinal=1, limit=2)

    proof = AsyncMock(return_value={"wave_id": "wave"})
    monkeypatch.setattr(routes, "get_wave_receipts", proof)
    assert (await routes.control_get_import_wave_proof(_Request(), "wave")).status == 200

    monkeypatch.setattr(control_api, "_require_control_auth", Mock())
    monkeypatch.setattr(control_api, "page_limit", Mock(return_value=2))
    source_files = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(control_api, "list_discovery_source_files_page", source_files)
    response = await control_api.control_mrf_discovery_source_files(
        _Request(args={"cursor": "cursor", "limit": "2"}), "source"
    )
    assert response.status == 200
    source_files.assert_awaited_once_with("source", cursor="cursor", limit=2)

def test_matching_slot_membership_and_receipt_projection():
    attested = types.SimpleNamespace(
        pod_uid_by_slot={slot: f"pod-{slot}" for slot in range(12)}
    )
    receipts.assert_slot_membership(
        attested,
        [types.SimpleNamespace(slot=slot, pod_uid=f"pod-{slot}") for slot in range(12)],
    )

    wave = _wave()
    vars(wave).update(
        {
            "state_version": 1,
            "physical_coordinate_count": 1,
            "imported_coordinate_count": 1,
            "reused_coordinate_count": 0,
            "physical_coordinate_digest": "a",
            "imported_coordinate_digest": "b",
            "reused_coordinate_digest": "c",
            "partition_digest": "d",
            "k8s_post_started_at": None,
            "kubernetes_job_receipt": None,
            "kubernetes_ready_attestation_digest": None,
            "redis_release_started_at": None,
            "terminal_evidence_digest": None,
            "redis_cleanup_evidence": None,
            "cleanup_evidence_digest": None,
            "cleanup_summary": None,
            "resolved_at": None,
        }
    )
    assert receipt_projection.wave_receipt_mapping(wave)["capacity_owning"] is True

def test_coverage_margin_pager_stops_before_a_second_plan_dense_row():
    windows, cursor = bounded_file_windows(
        [
            {
                "mrf_file_id": "synthetic-file-one",
                "metadata_json": {"plan_info": [{}]},
            },
            {
                "mrf_file_id": "synthetic-file-two",
                "metadata_json": {"plan_info": [{}]},
            },
        ],
        limit=2,
        cursor_plan_offset=0,
        plan_reference_limit=1,
    )

    assert [window.file_data["mrf_file_id"] for window in windows] == [
        "synthetic-file-one"
    ]
    assert cursor == "synthetic-file-one"

def test_coverage_margin_manifest_rejects_an_unsupported_requested_page_size():
    assert (
        catalog_manifest.catalog_paging_manifest_for_file_page(
            _synthetic_catalog_metadata(),
            page_limit=99,
        )
        is None
    )

@pytest.mark.parametrize(
    "page_totals",
    (
        "not-a-mapping",
        {"100": 1, "250": 1},
        {"100": 1, "250": 0, "500": 1},
    ),
)
def test_coverage_margin_manifest_rejects_malformed_cached_totals(
    page_totals: object,
):
    metadata = _synthetic_catalog_metadata()
    manifest = metadata[catalog_manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY]
    assert isinstance(manifest, dict)
    manifest["page_totals"] = page_totals

    assert (
        catalog_manifest.catalog_paging_manifest_for_file_page(
            metadata,
            page_limit=100,
        )
        is None
    )

@pytest.mark.parametrize("counts", ((True, 0), (0, -1)))
def test_v4_bound_rejects_invalid_counts(
    counts: tuple[object, object],
):
    with pytest.raises(ValueError, match="V4 candidate proof counts"):
        v4_scope.v4_candidate_proof_memory_bound(*counts)

def test_v4_bound_skips_zero_candidate_graph():
    assert v4_scope.v4_candidate_proof_memory_bound(2, 0) == 992

@pytest.mark.parametrize(
    ("counts", "message"),
    (
        ((True, 0), "candidate source code count"),
        ((0, True), "candidate source membership count"),
    ),
)
def test_coverage_margin_source_projection_rejects_non_integer_counts(
    counts: tuple[object, object],
    message: str,
):
    with pytest.raises(ValueError, match=message):
        reverse_scope.source_key_projection_retention_upper_bound(*counts)

def test_coverage_margin_provider_projection_rejects_boolean_count():
    with pytest.raises(ValueError, match="candidate provider projection counts"):
        reverse_scope.provider_candidate_projection_retention_upper_bound(
            0, 0, False, 0
        )

def test_coverage_margin_display_omits_duplicate_casefolded_code():
    assert display_value(
        "synthetic_detail",
        {"display": "Synthetic label", "code": "synthetic label"},
    ) == "Synthetic label"

def test_coverage_margin_specialty_exists_sql_excludes_subspecialties():
    params_by_field: dict[str, object] = {}
    specialty_filter = provider_specialty_filters.ProviderSpecialtyFilter(
        classification="Synthetic Class",
        include_subspecialties=False,
        primary_only=False,
    )

    sql = provider_specialty_filters.provider_specialty_taxonomy_exists_sql(
        "p.npi",
        params_by_field,
        "synthetic",
        specialty_filter,
        schema="mrf",
    )

    assert "NULLIF(BTRIM(COALESCE(synthetic_nucc.specialization, '')), '') IS NULL" in sql
    assert params_by_field == {"synthetic_classification": "Synthetic Class"}
