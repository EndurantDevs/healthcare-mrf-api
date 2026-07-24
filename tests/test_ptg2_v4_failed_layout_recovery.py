# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit safety gates for exact failed PTG V4 layout recovery."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_v4_failed_layout_recovery as recovery
from process.ptg_parts import ptg2_v4_failed_layout_marker as marker
from process.ptg_parts import ptg2_v4_failed_layout_request as request_contract
from process.ptg_parts import ptg2_v4_failed_layout_state as state
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    v4_layout_fingerprint,
)


def _failed_owner_fixture() -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, int],
]:
    snapshot_id = "ptg2:202607:test"
    import_run_id = "ptg2:test-run"
    shared_fingerprint = hashlib.sha256(b"failed-owner").digest()
    snapshot_by_field = {
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "status": "failed",
        "published_at": None,
    }
    run_by_field = {
        "import_run_id": import_run_id,
        "status": "failed",
        "report": {
            "shared_snapshot_key": 491,
            "shared_semantic_fingerprint": shared_fingerprint.hex(),
            "shared_layout_abandoned": False,
            "shared_layout_abandonment_deferred": True,
        },
    }
    layout_by_field = {
        "snapshot_key": 491,
        "generation": PTG2_V4_SHARED_GENERATION,
        "state": "building",
        "build_token": "secret-build-token",
        "published_at": None,
        "root_state": "building",
        "semantic_fingerprint": v4_layout_fingerprint(shared_fingerprint),
    }
    count_by_name = {
        "bindings": 0,
        "fingerprints": 1,
        "attestations": 0,
        "global_pointers": 0,
        "source_pointers": 0,
        "plan_pointers": 0,
        "pins": 0,
        "scopes": 0,
        "sources": 0,
    }
    return snapshot_by_field, run_by_field, layout_by_field, count_by_name


def test_failed_owner_gates_accept_only_exact_unpublished_layout() -> None:
    (
        snapshot_by_field,
        run_by_field,
        layout_by_field,
        count_by_name,
    ) = _failed_owner_fixture()

    gates = recovery._require_failed_owner(
        snapshot_by_field=snapshot_by_field,
        run_by_field=run_by_field,
        layout_by_field=layout_by_field,
        snapshot_id=str(snapshot_by_field["snapshot_id"]),
        import_run_id=str(run_by_field["import_run_id"]),
        snapshot_key=491,
        count_by_name=count_by_name,
    )

    assert gates
    assert all(gates.values())


@pytest.mark.parametrize(
    ("count_name", "count"),
    (
        ("bindings", 1),
        ("fingerprints", 2),
        ("source_pointers", 1),
    ),
)
def test_failed_owner_gates_reject_referenced_or_ambiguous_layout(
    count_name: str,
    count: int,
) -> None:
    (
        snapshot_by_field,
        run_by_field,
        layout_by_field,
        count_by_name,
    ) = _failed_owner_fixture()
    count_by_name[count_name] = count

    with pytest.raises(
        recovery.PTG2V4RecoveryConflict,
        match="recovery gates did not pass",
    ):
        recovery._require_failed_owner(
            snapshot_by_field=snapshot_by_field,
            run_by_field=run_by_field,
            layout_by_field=layout_by_field,
            snapshot_id=str(snapshot_by_field["snapshot_id"]),
            import_run_id=str(run_by_field["import_run_id"]),
            snapshot_key=491,
            count_by_name=count_by_name,
        )


@pytest.mark.parametrize("digest", ("", "g" * 64, "a" * 63))
@pytest.mark.asyncio
async def test_recovery_execute_rejects_invalid_plan_digest(
    digest: str,
) -> None:
    with pytest.raises(ValueError, match="64 hexadecimal"):
        await recovery.recover_ptg2_v4_layout(
            snapshot_id="ptg2:202607:test",
            import_run_id="ptg2:test-run",
            snapshot_key=491,
            expected_plan_digest=digest,
        )


_RECOVERY_SNAPSHOT_ID = "ptg2:202607:test"
_RECOVERY_RUN_ID = "ptg2:test-run"
_ZERO_POSTCONDITION_BY_NAME = {
    "layouts": 0,
    "fingerprints": 0,
    "mappings": 0,
    "map_roots": 0,
    "map_packs": 0,
    "relation_manifests": 0,
    "dense_rows": 0,
}


def _completed_report(
    **marker_overrides: object,
) -> dict[str, object]:
    marker_by_field = {
        "contract": marker.PTG2_V4_FAILED_LAYOUT_RECOVERY_CONTRACT,
        "snapshot_id": _RECOVERY_SNAPSHOT_ID,
        "import_run_id": _RECOVERY_RUN_ID,
        "snapshot_key": 491,
        "plan_digest": "a" * 64,
        "representation": "direct_v1",
        "recovered_at": "2026-07-24T00:00:00+00:00",
        "released_layouts": 1,
        "queued_candidate_hashes": 3,
        "queued_candidate_stored_bytes": 18,
        "cas_payloads_deleted": 0,
        "postconditions": dict(_ZERO_POSTCONDITION_BY_NAME),
        **marker_overrides,
    }
    return {
        "shared_snapshot_key": 491,
        "shared_layout_abandoned": True,
        "shared_layout_abandonment_deferred": False,
        "shared_layout_recovery": marker_by_field,
    }


def _completed_result(
    report_by_field: dict[str, object],
) -> dict[str, object] | None:
    return marker.completed_recovery_result(
        snapshot_by_field={
            "snapshot_id": _RECOVERY_SNAPSHOT_ID,
            "import_run_id": _RECOVERY_RUN_ID,
            "status": "failed",
            "published_at": None,
        },
        run_by_field={
            "import_run_id": _RECOVERY_RUN_ID,
            "status": "failed",
            "report": report_by_field,
        },
        snapshot_id=_RECOVERY_SNAPSHOT_ID,
        import_run_id=_RECOVERY_RUN_ID,
        snapshot_key=491,
        count_by_name={},
        postconditions_by_name=_ZERO_POSTCONDITION_BY_NAME,
    )


def test_completed_recovery_marker_is_exact_and_idempotent() -> None:
    replay_by_field = _completed_result(_completed_report())

    assert replay_by_field is not None
    assert replay_by_field["idempotent"] is True
    assert replay_by_field["queued_candidate_hashes"] == 3
    assert replay_by_field["plan_digest"] == "a" * 64


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    (
        ("plan_digest", "not-a-digest"),
        ("released_layouts", 2),
        ("cas_payloads_deleted", 1),
        ("recovered_at", ""),
        ("recovered_at", "not-a-timestamp"),
        ("snapshot_key", {}),
        ("postconditions", {}),
        (
            "postconditions",
            {
                "layouts": {},
                "fingerprints": 0,
                "mappings": 0,
                "map_roots": 0,
                "map_packs": 0,
                "relation_manifests": 0,
                "dense_rows": 0,
            },
        ),
    ),
)
def test_completed_recovery_marker_rejects_weak_evidence(
    field_name: str,
    field_value: object,
) -> None:
    """Reject malformed persisted evidence without raising."""

    replay_by_field = _completed_result(
        _completed_report(**{field_name: field_value})
    )

    assert replay_by_field is None


def test_state_mapping_helpers_fail_closed() -> None:
    database_record = SimpleNamespace(_mapping={"snapshot_key": 491})

    assert state.row_mapping(None) == {}
    assert state.row_mapping({"snapshot_key": 491}) == {
        "snapshot_key": 491
    }
    assert state.row_mapping(database_record) == {"snapshot_key": 491}
    assert state.json_mapping('{"status":"failed"}') == {
        "status": "failed"
    }
    assert state.json_mapping("[1]") == {}
    assert state.json_mapping("{") == {}
    assert state.json_mapping(7) == {}


def test_recovery_request_rejects_nonpositive_snapshot_key() -> None:
    with pytest.raises(ValueError, match="positive snapshot_key"):
        request_contract.normalize_recovery_request(
            snapshot_id=_RECOVERY_SNAPSHOT_ID,
            import_run_id=_RECOVERY_RUN_ID,
            snapshot_key=0,
        )


def test_failed_owner_rejects_malformed_fingerprint() -> None:
    (
        snapshot_by_field,
        run_by_field,
        layout_by_field,
        count_by_name,
    ) = _failed_owner_fixture()
    run_report_by_field = dict(run_by_field["report"])
    run_report_by_field["shared_semantic_fingerprint"] = "not-hex"
    run_by_field["report"] = run_report_by_field

    with pytest.raises(
        recovery.PTG2V4RecoveryConflict,
        match="no valid shared semantic fingerprint",
    ):
        recovery._require_failed_owner(
            snapshot_by_field=snapshot_by_field,
            run_by_field=run_by_field,
            layout_by_field=layout_by_field,
            snapshot_id=_RECOVERY_SNAPSHOT_ID,
            import_run_id=_RECOVERY_RUN_ID,
            snapshot_key=491,
            count_by_name=count_by_name,
        )


@pytest.mark.parametrize("lock_outcome", (False, RuntimeError("corrupt")))
@pytest.mark.asyncio
async def test_owner_lock_failures_become_recovery_conflicts(
    monkeypatch,
    lock_outcome: object,
) -> None:
    monkeypatch.setattr(
        recovery,
        "load_recovery_records",
        AsyncMock(return_value=({}, {}, {"build_token": "owned"})),
    )
    lock_mock = (
        AsyncMock(side_effect=lock_outcome)
        if isinstance(lock_outcome, Exception)
        else AsyncMock(return_value=lock_outcome)
    )
    monkeypatch.setattr(recovery, "_is_owned_v4_layout_locked", lock_mock)

    with pytest.raises(recovery.PTG2V4RecoveryConflict):
        await recovery._owner_records(
            object(),
            schema_name="mrf",
            snapshot_id=_RECOVERY_SNAPSHOT_ID,
            import_run_id=_RECOVERY_RUN_ID,
            snapshot_key=491,
            lock_owned_layout=True,
        )


@pytest.mark.asyncio
async def test_recovery_rejects_missing_cas_blocks(monkeypatch) -> None:
    monkeypatch.setattr(
        recovery,
        "_v4_reachable_hashes",
        AsyncMock(return_value={b"x"}),
    )
    monkeypatch.setattr(
        recovery,
        "load_block_stats",
        AsyncMock(
            return_value={
                "candidate_hashes": 2,
                "resolved_hashes": 1,
                "stored_bytes": 6,
            }
        ),
    )

    with pytest.raises(
        recovery.PTG2V4RecoveryConflict,
        match="missing CAS blocks",
    ):
        await recovery._verified_block_stats(
            object(),
            schema_name="mrf",
            snapshot_key=491,
        )


def _recovery_context() -> recovery._RecoveryContext:
    return recovery._RecoveryContext(
        snapshot_id=_RECOVERY_SNAPSHOT_ID,
        import_run_id=_RECOVERY_RUN_ID,
        snapshot_key=491,
        build_token="owned",
        expected_report={},
        plan_by_field={"plan_digest": "a" * 64},
    )


@pytest.mark.parametrize(
    ("released_layouts", "postcondition_by_name", "message"),
    (
        (0, {}, "ownership changed"),
        (1, {"layouts": 1}, "left physical ownership rows"),
    ),
)
@pytest.mark.asyncio
async def test_release_context_rejects_changed_physical_state(
    monkeypatch,
    released_layouts: int,
    postcondition_by_name: dict[str, int],
    message: str,
) -> None:
    monkeypatch.setattr(
        recovery,
        "abandon_owned_v4_layout",
        AsyncMock(
            return_value=SimpleNamespace(
                logical_layout_count=released_layouts,
                candidate_hash_count=0,
                stored_bytes=0,
            )
        ),
    )
    monkeypatch.setattr(
        recovery,
        "load_recovery_postconditions",
        AsyncMock(return_value=postcondition_by_name),
    )

    with pytest.raises(RuntimeError, match=message):
        await recovery._release_recovery_context(
            object(),
            schema_name="mrf",
            context=_recovery_context(),
        )


@pytest.mark.asyncio
async def test_marker_persistence_requires_one_failed_run() -> None:
    executor = SimpleNamespace(all=AsyncMock(return_value=[]))
    marker_write = marker.RecoveryMarkerWrite(
        snapshot_id=_RECOVERY_SNAPSHOT_ID,
        import_run_id=_RECOVERY_RUN_ID,
        snapshot_key=491,
        expected_report_by_field={},
        plan_by_field={"plan_digest": "a" * 64},
        released_layouts=1,
        queued_candidate_hashes=0,
        queued_candidate_stored_bytes=0,
        postcondition_by_name=_ZERO_POSTCONDITION_BY_NAME,
    )

    with pytest.raises(RuntimeError, match="owner changed"):
        await marker.persist_recovery_marker(
            executor,
            schema_name="mrf",
            marker_write=marker_write,
        )
