"""Pure contract coverage for legacy PTG V3 metadata reconciliation."""

from __future__ import annotations

import copy
import datetime as dt

import pytest

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    LEGACY_V3_RECONCILE_CONTRACT,
)
from process.ptg_parts.ptg2_legacy_v3_metadata_contract import (
    build_legacy_v3_reconcile_plan,
)
from process.ptg_parts import ptg2_legacy_v3_operational_absence
from process.ptg_parts.ptg2_legacy_v3_metadata_store import (
    ALLOWED_ATTACHMENT_NAMES,
)
from process.ptg_parts.ptg2_v4_attempt_registry import ATTEMPT_ATTACHMENTS
from process.ptg_parts.ptg_source_attempt_guard import (
    source_attempt_lock_key,
    source_file_import_id_from_payload,
)
from process.ptg_parts import ptg2_legacy_v3_metadata_reconcile as reconcile


_SNAPSHOT_ID = "ptg2:202607:synthetic-v3-orphan"
_INTERNAL_RUN_ID = "ptg2:0000000000000000000000000000000g"
_OUTER_RUN_ID = "run_synthetic_legacy_v3"
_SOURCE_IMPORT_ID = "synthetic-source-import-v3"


def test_attempt_authority_schema_is_required_and_byte_preserving(
    monkeypatch,
) -> None:
    monkeypatch.delenv(reconcile._ATTEMPT_AUTHORITY_SCHEMA_ENV, raising=False)
    with pytest.raises(
        reconcile.LegacyV3MetadataConflict,
        match="source-attempt authority schema is not configured correctly",
    ):
        reconcile._attempt_authority_schema_name()

    monkeypatch.setenv(
        reconcile._ATTEMPT_AUTHORITY_SCHEMA_ENV,
        "source_attempt_authority_test",
    )
    assert reconcile._attempt_authority_schema_name() == (
        "source_attempt_authority_test"
    )


@pytest.mark.parametrize(
    "schema_name",
    (
        " source_attempt_authority_test",
        "source_attempt_authority_test ",
        "source-attempt-authority-test",
        "1_source_attempt_authority_test",
        "s" * 64,
    ),
)
def test_attempt_authority_schema_rejects_nonidentifiers(
    monkeypatch,
    schema_name: str,
) -> None:
    monkeypatch.setenv(reconcile._ATTEMPT_AUTHORITY_SCHEMA_ENV, schema_name)
    with pytest.raises(
        reconcile.LegacyV3MetadataConflict,
        match="source-attempt authority schema is not configured correctly",
    ):
        reconcile._attempt_authority_schema_name()


def _attachment_counts_by_name() -> dict[str, int]:
    return {
        attachment.name: (
            1 if attachment.name in ALLOWED_ATTACHMENT_NAMES else 0
        )
        for attachment in ATTEMPT_ATTACHMENTS
    }


def _snapshot_by_field() -> dict:
    return {
        "snapshot_id": _SNAPSHOT_ID,
        "import_run_id": _INTERNAL_RUN_ID,
        "status": "building",
        "created_at": "2026-07-01T00:00:00+00:00",
        "validated_at": None,
        "published_at": None,
        "manifest": {},
    }


def _internal_run_by_field() -> dict:
    return {
        "import_run_id": _INTERNAL_RUN_ID,
        "status": "running",
        "started_at": "2026-07-01T00:00:00+00:00",
        "heartbeat_at": "2026-07-01T00:01:00+00:00",
        "finished_at": None,
        "options": {
            "storage_generation": "shared_blocks_v3",
            "snapshot_arch": "postgres_binary_v3",
            "source_file_import_id": _SOURCE_IMPORT_ID,
        },
    }


def _outer_target_by_field() -> dict:
    return {
        "run_id": _OUTER_RUN_ID,
        "importer": "ptg",
        "status": "failed",
        "source_file_import_id": _SOURCE_IMPORT_ID,
        "import_id": _SOURCE_IMPORT_ID,
        "params": {
            "source_file_import_id": _SOURCE_IMPORT_ID,
            "import_id": _SOURCE_IMPORT_ID,
        },
        "finished_at": "2026-07-01T00:03:00+00:00",
    }


def _core_observation_by_name() -> dict:
    snapshot_by_field = _snapshot_by_field()
    internal_run_by_field = _internal_run_by_field()
    return {
        "snapshot": {"payload": snapshot_by_field, "row_xmin": "1"},
        "internal_run": {
            "payload": internal_run_by_field,
            "row_xmin": "2",
        },
        "run_snapshots": [
            {
                "snapshot_id": _SNAPSHOT_ID,
                "import_run_id": _INTERNAL_RUN_ID,
                "status": "building",
            }
        ],
        "source_internal_runs": [
            {
                "payload": copy.deepcopy(internal_run_by_field),
                "row_xmin": "2",
            }
        ],
        "source_snapshots": [
            {
                "snapshot_id": _SNAPSHOT_ID,
                "import_run_id": _INTERNAL_RUN_ID,
                "status": "building",
            }
        ],
    }


def _outer_observation_by_name() -> dict:
    outer_target_by_field = _outer_target_by_field()
    return {
        "source_file_import_id": _SOURCE_IMPORT_ID,
        "outer_runs": [outer_target_by_field],
        "outer_target": outer_target_by_field,
        "control_run_mirrors": [
            {
                "run_id": _OUTER_RUN_ID,
                "importer": "ptg",
                "status": "failed",
                "params": {
                    "source_file_import_id": _SOURCE_IMPORT_ID,
                    "import_id": _SOURCE_IMPORT_ID,
                },
                "finished_at": "2026-07-01T00:03:00+00:00",
            }
        ],
        "source_import_rows": [
            {
                "payload": {
                    "source_file_import_id": _SOURCE_IMPORT_ID,
                    "status": "failed",
                    "engine_run_id": _OUTER_RUN_ID,
                    "snapshot_id": _SNAPSHOT_ID,
                    "removed_at": None,
                },
                "row_xmin": "3",
            }
        ],
        "placement_rows": [],
        "event_rows": [],
        "event_high_water_mark": 0,
        "event_digest": "0" * 64,
        "audit": None,
    }


def _attachment_observation_by_name() -> dict:
    return {
        "attachment_counts": _attachment_counts_by_name(),
        "attachment_rows": {
            name: [{"row": {"synthetic": name}, "xmin": "4"}]
            for name in ALLOWED_ATTACHMENT_NAMES
        },
        "attachment_digest": "1" * 64,
        "catalog_digest": "2" * 64,
        "dynamic_relations": {
            "suffix_valid": True,
            "relation_count": 0,
            "digest": "3" * 64,
        },
    }


def _observation() -> dict:
    """Build one complete synthetic legacy-V3 repair observation."""

    return {
        **_core_observation_by_name(),
        **_outer_observation_by_name(),
        **_attachment_observation_by_name(),
    }


def _operational_absence() -> dict:
    return {
        "contract": "ptg_source_attempt_external_absence_v1",
        "job_identity_count": 1,
        "queue_count": 6,
        "queue_memberships": 0,
        "redis_exact_key_count": 0,
        "worker_running_count": 0,
        "worker_present_count": 0,
        "exact_external_absence": True,
    }


def _plan(observation: dict, observed_at: dt.datetime) -> dict:
    return build_legacy_v3_reconcile_plan(
        observation,
        _operational_absence(),
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
        observed_at=observed_at,
        capabilities_ready=True,
    )


def test_ready_plan_is_stable_as_stale_age_increases() -> None:
    observation = _observation()
    first = _plan(
        observation,
        dt.datetime(2026, 7, 2, tzinfo=dt.UTC),
    )
    second = _plan(
        observation,
        dt.datetime(2026, 7, 3, tzinfo=dt.UTC),
    )

    assert first["contract"] == LEGACY_V3_RECONCILE_CONTRACT
    assert first["status"] == "ready"
    assert first["planned_effects"] == {
        "snapshot_rows_updated": 1,
        "internal_run_rows_updated": 1,
        "audit_rows_inserted": 1,
        "attachment_rows_changed": 0,
        "external_effects": 0,
    }
    assert first["plan_digest"] == second["plan_digest"]
    assert second["stale_age_seconds"] > first["stale_age_seconds"]


def test_ineligible_plan_has_no_executable_digest() -> None:
    observation = _observation()
    too_early = _plan(
        observation,
        dt.datetime(2026, 7, 1, 1, tzinfo=dt.UTC),
    )
    ready = _plan(
        observation,
        dt.datetime(2026, 7, 2, tzinfo=dt.UTC),
    )

    assert too_early["status"] == "ineligible"
    assert too_early["plan_digest"] is None
    assert "internal_run_not_stale" in too_early["reason_codes"]
    assert ready["status"] == "ready"
    assert ready["plan_digest"] is not None


def test_live_snapshot_cardinality_fails_closed() -> None:
    observation = _observation()
    observation["run_snapshots"].append(
        {
            "snapshot_id": "ptg2:202607:second-synthetic",
            "import_run_id": _INTERNAL_RUN_ID,
            "status": "building",
        }
    )

    plan = _plan(observation, dt.datetime(2026, 7, 2, tzinfo=dt.UTC))

    assert plan["status"] == "ineligible"
    assert "internal_run_snapshot_cardinality_changed" in plan["reason_codes"]


def test_attachment_set_must_be_exact() -> None:
    observation = _observation()
    observation["attachment_counts"]["artifact_manifest"] = 1

    plan = _plan(observation, dt.datetime(2026, 7, 2, tzinfo=dt.UTC))

    assert plan["status"] == "ineligible"
    assert "attachment_set_not_exact" in plan["reason_codes"]


def test_new_action_or_external_identity_invalidates_plan() -> None:
    observation = _observation()
    first = _plan(observation, dt.datetime(2026, 7, 2, tzinfo=dt.UTC))
    changed = copy.deepcopy(observation)
    changed["event_high_water_mark"] = 1
    changed["event_digest"] = "f" * 64
    second = _plan(changed, dt.datetime(2026, 7, 2, tzinfo=dt.UTC))
    external = _operational_absence()
    external["worker_present_count"] = 1
    external["exact_external_absence"] = False
    blocked = build_legacy_v3_reconcile_plan(
        observation,
        external,
        snapshot_id=_SNAPSHOT_ID,
        internal_run_id=_INTERNAL_RUN_ID,
        outer_run_id=_OUTER_RUN_ID,
        observed_at=dt.datetime(2026, 7, 2, tzinfo=dt.UTC),
        capabilities_ready=True,
    )

    assert second["plan_digest"] != first["plan_digest"]
    assert blocked["status"] == "ineligible"
    assert "external_attempt_identity_present" in blocked["reason_codes"]


def test_source_event_must_belong_to_exact_outer_retry_chain() -> None:
    observation = _observation()
    observation["event_rows"] = [
        {
            "event_id": 1,
            "event_kind": "worker_start_admitted",
            "outer_run_id": "run_unknown_synthetic",
            "attempt_id": "job_unknown_synthetic",
            "state_digest": "a" * 64,
        }
    ]
    observation["event_high_water_mark"] = 1

    plan = _plan(observation, dt.datetime(2026, 7, 2, tzinfo=dt.UTC))

    assert plan["status"] == "ineligible"
    assert "source_event_outer_lineage_changed" in plan["reason_codes"]


def test_shared_lock_key_and_identity_conflicts_are_exact() -> None:
    assert source_attempt_lock_key(_SOURCE_IMPORT_ID) == (
        "ptg-source-import:-1-attempt:synthetic-source-import-v3"
    )
    assert source_file_import_id_from_payload(
        {
            "source_file_import_id": _SOURCE_IMPORT_ID,
            "import_id": _SOURCE_IMPORT_ID,
            "params": {
                "source_file_import_id": _SOURCE_IMPORT_ID,
                "import_id": _SOURCE_IMPORT_ID,
            },
        },
        required=True,
    ) == _SOURCE_IMPORT_ID


def test_import_id_alone_does_not_opt_into_source_attempt_protocol() -> None:
    assert source_file_import_id_from_payload(
        {"params": {"import_id": "20260801"}},
        required=False,
    ) is None


def test_explicit_source_identity_rejects_conflicting_import_alias() -> None:
    with pytest.raises(ValueError, match="identity views conflict"):
        source_file_import_id_from_payload(
            {
                "source_file_import_id": _SOURCE_IMPORT_ID,
                "params": {"import_id": "ordinary-import-id"},
            },
            required=True,
        )


@pytest.mark.asyncio
async def test_process_launcher_absence_probe_is_strictly_no_write(
    monkeypatch,
    tmp_path,
) -> None:
    state_file = tmp_path / "existing.pid"
    state_file.write_text("synthetic-state", encoding="utf-8")

    def reject_worker_state(_worker_payload):
        raise AssertionError("process-mode worker state must not be probed")

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(
        ptg2_legacy_v3_operational_absence,
        "worker_state",
        reject_worker_state,
    )

    evidence = await (
        ptg2_legacy_v3_operational_absence.load_exact_operational_absence(
            [_outer_target_by_field()],
            [],
        )
    )

    assert evidence["worker_probe_supported"] is False
    assert evidence["exact_external_absence"] is False
    assert state_file.read_text(encoding="utf-8") == "synthetic-state"
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "existing.pid"
    ]
