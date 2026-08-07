# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partitioned exact-wave residual fail-closed contracts."""

from __future__ import annotations

from tests.test_ptg_wave_failure_controller_residual import (
    AsyncMock,
    BadRequest,
    Mock,
    _Acquire,
    _CONFIG_DIGEST,
    _IMAGE,
    _JOBS_DIGEST,
    _MANIFEST_DIGEST,
    _MANIFEST_IDENTITY,
    _RUNTIME_IDENTITY,
    _Request,
    _WAVE_DIGEST,
    _wave,
    asyncio,
    bindings,
    control_api,
    control_imports,
    control_workers,
    derive_terminal_state,
    fence,
    ptg_control,
    pytest,
    receipts,
    runpy,
    types,
)


def _install_admission_replay_mocks(monkeypatch):
    connection = object()
    monkeypatch.setattr(control_imports.db, "acquire", lambda: _Acquire(connection))
    for name in (
        "require_source_attempt_capabilities",
        "guard_source_attempt",
        "acquire_ptg_admission_lock",
        "require_no_capacity_owning_wave",
        "insert_or_compare_frozen_binding",
    ):
        monkeypatch.setattr(control_imports, name, AsyncMock())
    monkeypatch.setattr(
        control_imports,
        "source_file_import_id_from_payload",
        Mock(return_value="source"),
    )
    monkeypatch.setattr(
        control_imports,
        "_locked_ptg_source_replay",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "_is_parallel_active_importer_run_allowed",
        Mock(return_value=False),
    )


@pytest.mark.asyncio
async def test_ptg_source_admission_replay_does_not_insert(monkeypatch):
    _install_admission_replay_mocks(monkeypatch)
    existing_by_field = {"run_id": "run-existing"}
    monkeypatch.setattr(
        control_imports,
        "_active_importer_runs",
        AsyncMock(return_value=[existing_by_field]),
    )
    assert await control_imports._admit_ptg_source_file_run(
        {"params": {}, "idempotency_key": None}
    ) == existing_by_field


@pytest.mark.asyncio
async def test_ptg_fenced_admission_replays_do_not_insert(monkeypatch):
    _install_admission_replay_mocks(monkeypatch)
    existing_by_field = {"run_id": "run-existing"}
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(return_value=existing_by_field),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": "key"}
    ) == existing_by_field
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "_active_idempotency_run",
        AsyncMock(return_value=existing_by_field),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": "key"}
    ) == existing_by_field
    monkeypatch.setattr(
        control_imports,
        "_active_idempotency_run",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=existing_by_field),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": "key"}
    ) == existing_by_field
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "_active_importer_runs",
        AsyncMock(return_value=[existing_by_field]),
    )
    assert await control_imports._admit_wave_fenced_import_run(
        {"importer": "ptg", "idempotency_key": None}
    ) == existing_by_field

@pytest.mark.asyncio
async def test_claim_exception_never_guesses_after_ambiguous_reconciliation(monkeypatch):
    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(side_effect=RuntimeError()),
    )
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(), {}, run_id="run", claim_attempt_token="token"
    ) is None
    fields_by_field = {"run_id": "run"}
    monkeypatch.setattr(
        ptg_control,
        "_exact_wave_claim_values",
        Mock(return_value=fields_by_field),
    )
    monkeypatch.setattr(
        ptg_control,
        "reconcile_wave_claim_exception",
        AsyncMock(side_effect=RuntimeError()),
    )
    assert await ptg_control._reconcile_exact_wave_claim_exception(
        object(), {}, run_id="run", claim_attempt_token="token"
    ) is None
    flushed = AsyncMock()
    monkeypatch.setattr(
        ptg_control,
        "reconcile_wave_claim_exception",
        AsyncMock(return_value=types.SimpleNamespace(status="rejected")),
    )
    monkeypatch.setattr(ptg_control, "_flush_terminal_status_events", flushed)
    assert (await ptg_control._reconcile_exact_wave_claim_exception(
        object(), {}, run_id="run", claim_attempt_token="token"
    )).status == "rejected"
    flushed.assert_awaited_once()
    marked = AsyncMock()
    monkeypatch.setattr(ptg_control, "mark_control_run", marked)
    await ptg_control._mark_exact_wave_preexecution_failure(
        "run", reason="reason", error=RuntimeError("detail")
    )
    marked.assert_awaited_once()

def test_terminal_state_rejects_non_dead_letter_failure_receipt():
    with pytest.raises(Exception, match="all dead letter"):
        derive_terminal_state(
            _wave(failure_receipt_digest="a" * 64, intent_count=1),
            [types.SimpleNamespace(ordinal=0, status="failed")],
        )

@pytest.mark.asyncio
async def test_fence_fallbacks_and_conflicts_are_fail_closed(monkeypatch):
    class ExecuteOnly:
        async def execute(self, *_args):
            return types.SimpleNamespace(
                all=lambda: [("row",)],
                scalar=lambda: "value",
            )

    assert await fence._all(ExecuteOnly(), object()) == [("row",)]
    assert await fence._scalar(ExecuteOnly(), object(), {}) == "value"
    assert await fence._capacity_owning_waves(object()) == []
    monkeypatch.setattr(
        fence,
        "_capacity_owning_waves",
        AsyncMock(return_value=[("wave", "executing"), ("other", "executing")]),
    )
    with pytest.raises(fence.PTGWaveCapacityConflict, match="ambiguous"):
        await fence.require_no_capacity_owning_wave(object())
    monkeypatch.setattr(
        fence,
        "_capacity_owning_waves",
        AsyncMock(return_value=[("wave", "executing")]),
    )
    monkeypatch.setattr(fence, "_all", AsyncMock(return_value=[]))
    with pytest.raises(fence.PTGWaveCapacityConflict, match="reserved"):
        await fence.require_no_capacity_owning_wave(
            object(),
            owner_run_id="not-owned",
        )
    with pytest.raises(fence.PTGWaveCapacityConflict, match="already reserved"):
        await fence.require_wave_admission_capacity(object())
    original_is_ptg_wave_owned_run = fence.is_ptg_wave_owned_run
    monkeypatch.setattr(
        fence,
        "is_ptg_wave_owned_run",
        AsyncMock(return_value=True),
    )
    with pytest.raises(fence.PTGWaveOwnershipConflict):
        await fence.require_not_wave_owned_run(object(), "run")
    monkeypatch.setattr(
        fence,
        "is_ptg_wave_owned_run",
        original_is_ptg_wave_owned_run,
    )
    assert await fence.is_ptg_wave_owned_run(object(), "run") is False

def test_kubernetes_receipt_and_membership_projection(monkeypatch):
    attested = types.SimpleNamespace(
        wave_digest=_WAVE_DIGEST,
        job_uid="job",
        manifest_identity=_MANIFEST_IDENTITY,
        config_identity=_CONFIG_DIGEST,
        image_identity=_IMAGE,
        runtime_image_identity=_RUNTIME_IDENTITY,
        pod_uid_by_slot={slot: f"pod-{slot}" for slot in range(12)},
    )
    monkeypatch.setattr(
        receipts,
        "attest_existing_ptg_wave_job",
        Mock(return_value=attested),
    )
    projected = receipts.kubernetes_job_receipt({}, {})
    assert projected["pinned_image_digest"] == _IMAGE.rsplit("@sha256:", 1)[1]
    monkeypatch.setattr(
        receipts,
        "validate_ptg_wave_job_manifest",
        Mock(
            return_value=types.SimpleNamespace(
                image=_IMAGE,
                runtime_image_identity=_RUNTIME_IDENTITY,
            )
        ),
    )
    assert receipts.kubernetes_ready_receipt({}, attested)["slots"][0]["slot"] == 0
    with pytest.raises(Exception, match="membership"):
        receipts.assert_slot_membership(
            attested,
            [types.SimpleNamespace(slot=0, pod_uid="foreign")],
        )


def test_redis_and_terminal_receipt_projection(monkeypatch):
    monkeypatch.setattr(
        receipts,
        "validate_ptg_wave_job_manifest",
        Mock(
            return_value=types.SimpleNamespace(
                image=_IMAGE,
                runtime_image_identity=_RUNTIME_IDENTITY,
            )
        ),
    )
    release = types.SimpleNamespace(
        wave_id=_WAVE_DIGEST,
        queue_name="queue",
        manifest_digest=_MANIFEST_DIGEST,
        jobs_digest=_JOBS_DIGEST,
        job_count=2,
        protocol_identity="p",
        serializer_identity="s",
        kubernetes_manifest_identity=_MANIFEST_IDENTITY,
        config_identity=_CONFIG_DIGEST,
        image_identity=_IMAGE,
        runtime_image_identity=_RUNTIME_IDENTITY,
        runtime_identity_digest="a" * 64,
        ready_slots=(),
        ready_slots_digest="b" * 64,
        release_digest="c" * 64,
    )
    assert receipts.redis_release_receipt(release)["wave_digest"] == _WAVE_DIGEST
    wave = _wave()
    initial = receipts.initial_kubernetes_attestation(wave)
    assert initial.job_uid == wave.kubernetes_job_uid
    assert receipts.kubernetes_terminal_receipt(
        wave,
        types.SimpleNamespace(as_mapping=lambda: {"ok": True}),
    ) == {"ok": True}
    assert receipts.redis_terminal_receipt(
        types.SimpleNamespace(as_mapping=lambda: {"ok": True})
    ) == {"ok": True}

@pytest.mark.asyncio
async def test_worker_admission_and_binding_replay_short_circuit(monkeypatch):
    connection = object()
    monkeypatch.setattr(control_workers.db, "acquire", lambda: _Acquire(connection))
    monkeypatch.setattr(control_workers, "acquire_ptg_admission_lock", AsyncMock())
    monkeypatch.setattr(control_workers, "require_not_wave_owned_run", AsyncMock())
    monkeypatch.setattr(control_workers, "require_no_capacity_owning_wave", AsyncMock())
    failed_by_field = {"status": "failed"}
    monkeypatch.setattr(
        control_workers,
        "_admit_worker_ensure",
        AsyncMock(return_value=failed_by_field),
    )
    assert await control_workers._guarded_ptg_family_ensure(
        {}, run_id="run", importer="ptg", selected_specs=[]
    ) is failed_by_field
    expected_by_field = {"digest": "a" * 64}
    monkeypatch.setattr(
        bindings,
        "frozen_rate_binding_from_params",
        Mock(return_value=expected_by_field),
    )
    monkeypatch.setattr(
        bindings,
        "source_file_import_id_from_params",
        Mock(return_value=None),
    )
    assert await bindings.recheck_frozen_binding_on_connection(connection, {}) == expected_by_field

@pytest.mark.asyncio
async def test_control_api_translation_edges(monkeypatch):
    monkeypatch.setattr(control_api, "_require_control_auth", Mock())
    monkeypatch.setattr(control_api, "page_limit", Mock(side_effect=ValueError("bad")))
    with pytest.raises(BadRequest):
        await control_api.control_mrf_discovery_sources(_Request())
    with pytest.raises(BadRequest):
        await control_api.control_mrf_discovery_source_files(_Request(), "source")
    monkeypatch.setattr(control_api, "_ptg_import_file_payload", Mock(return_value={}))
    monkeypatch.setattr(
        control_api,
        "create_import_run",
        AsyncMock(return_value=({"run_id": "run"}, False)),
    )
    assert (await control_api.control_ptg_import_file(_Request(json={}))).status == 409
    monkeypatch.setattr(
        control_api,
        "promote_ptg2_source_snapshot",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        control_api,
        "_is_ptg_snapshot_refresh_requested",
        Mock(return_value=True),
    )
    monkeypatch.setattr(
        control_api,
        "_ptg_source_snapshot_refresh_payload",
        Mock(side_effect=ValueError("bad")),
    )
    with pytest.raises(BadRequest):
        await control_api.control_ptg_source_snapshot_promote(
            _Request(json={"source_key": "source", "snapshot_id": "snapshot"})
        )

def test_worker_module_main_entrypoint_calls_asyncio_run(monkeypatch):
    seen_values = []

    def capture(coroutine):
        seen_values.append(coroutine)
        coroutine.close()

    monkeypatch.setattr(asyncio, "run", capture)
    with pytest.warns(RuntimeWarning, match="found in sys.modules"):
        runpy.run_module("process.ptg_wave_worker", run_name="__main__")
    assert len(seen_values) == 1
