"""Focused coverage for PTG V4 import-state fail-closed boundaries."""

from __future__ import annotations

from contextlib import asynccontextmanager
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control
from tests.ptg_frozen_test_support import protected_control_payload


ptg = importlib.import_module("process.ptg")


def _scope_row() -> dict[str, object]:
    return {
        "name": "provider_npi_scope",
        "binding_sha256": "11" * 32,
        "provider_npi_group_sha256": "22" * 32,
        "provider_npi_group_record_format": "dense-v1",
        "provider_npi_group_byte_count": 80,
        "provider_npi_group_owner_count": 2,
        "provider_npi_group_member_count": 3,
        "provider_npi_group_member_global_count": 3,
    }


def _group_row() -> dict[str, object]:
    return {
        "name": "provider_npi_group",
        "sha256": "22" * 32,
        "record_format": "dense-v1",
        "byte_count": 80,
        "owner_count": 2,
        "member_count": 3,
        "member_global_count": 3,
    }


@pytest.mark.asyncio
async def test_frozen_file_over_streaming_cap_fails_before_database(
    monkeypatch,
) -> None:
    """Reject an authenticated frozen file whose declared bytes exceed the cap."""

    request_payload = control._validated_control_import_payload(
        protected_control_payload()
    )
    params = request_payload["params"]
    monkeypatch.setattr(ptg, "fetch_max_bytes", lambda _default: 1)
    ensure_database = AsyncMock()
    monkeypatch.setattr(ptg, "ensure_database", ensure_database)

    with pytest.raises(ValueError, match="content_length exceeds"):
        await ptg._main_with_artifact_lease(
            test_mode=True,
            source_file_import_id=request_payload["source_file_import_id"],
            import_id=request_payload["import_id"],
            source_key=params["source_key"],
            import_month=params["import_month"],
            plan_ids=params["plan_ids"],
            plan_market_types=params["plan_market_types"],
            frozen_rate_file_set_contract=params["frozen_rate_file_set_contract"],
            frozen_rate_files=params["frozen_rate_files"],
            frozen_rate_file_set_sha256=params["frozen_rate_file_set_sha256"],
            frozen_rate_file_count=params["frozen_rate_file_count"],
        )

    ensure_database.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("serving", "scoped", "source_key", "reason"),
    (
        (False, True, "source", "no-serving-files"),
        (True, False, "source", "not-source-scoped"),
        (True, True, None, "missing-source-key"),
    ),
)
async def test_address_refresh_prerequisites_skip_without_import(
    serving,
    scoped,
    source_key,
    reason,
) -> None:
    """Skip address refresh before importing its control-plane dependency."""

    refresh_outcome = await ptg._enqueue_address_refresh_after_import(
        source_key=source_key,
        snapshot_id="snapshot",
        import_run_id="run",
        has_serving_files=serving,
        source_scoped_compact=scoped,
        test_mode=False,
    )

    assert refresh_outcome == {"status": "skipped", "reason": reason}


def test_scope_shard_binding_requires_one_matching_reciprocal() -> None:
    """Reject missing or drifted reciprocal evidence, then bind a valid shard."""

    scope = _scope_row()
    with pytest.raises(RuntimeError, match="incomplete"):
        ptg._bind_npi_scope_to_source_shard(
            [scope, dict(scope)],
            source_shard_id="shard-a",
        )
    with pytest.raises(RuntimeError, match="reciprocal binding changed"):
        ptg._bind_npi_scope_to_source_shard(
            [scope, {**_group_row(), "sha256": "33" * 32}],
            source_shard_id="shard-a",
        )

    valid_scope = _scope_row()
    ptg._bind_npi_scope_to_source_shard(
        [valid_scope, _group_row()],
        source_shard_id="shard-a",
    )
    assert valid_scope["shard_binding_contract"] == (
        ptg._PTG2_PROVIDER_NPI_SCOPE_SHARD_BINDING_CONTRACT
    )
    assert len(str(valid_scope["shard_binding_sha256"])) == 64


def _copy_payload(*npis: int, trailer: bytes = b"\xff\xff") -> bytes:
    rows = b"".join(
        b"\x00\x01" + b"\x00\x00\x00\x08" + npi.to_bytes(8, "big", signed=True)
        for npi in npis
    )
    return ptg._PTG2_PG_BINARY_COPY_HEADER + rows + trailer


@pytest.mark.parametrize(
    ("payload", "row_count", "message"),
    (
        (b"bad", 0, "header"),
        (ptg._PTG2_PG_BINARY_COPY_HEADER + b"\x00\x02", 1, "row"),
        (
            ptg._PTG2_PG_BINARY_COPY_HEADER + b"\x00\x01\x00\x00\x00\x08\x00",
            1,
            "truncated",
        ),
        (_copy_payload(999_999_999), 1, "strict NPI order"),
        (_copy_payload(1_000_000_000, trailer=b"\x00\x00"), 1, "trailer"),
    ),
)
def test_provider_scope_copy_rejects_malformed_frames(
    tmp_path: Path,
    payload: bytes,
    row_count: int,
    message: str,
) -> None:
    """Reject every malformed PostgreSQL binary scope boundary."""

    scope_path = tmp_path / "scope.copy"
    scope_path.write_bytes(payload)
    with pytest.raises(RuntimeError, match=message):
        ptg._validate_provider_npi_scope_copy(scope_path, row_count=row_count)


def test_manifest_scope_requires_reciprocal_membership(tmp_path: Path) -> None:
    """Reject an NPI scope sidecar when its reverse graph is absent."""

    scope_path = tmp_path / "scope.copy"
    scope_path.write_bytes(_copy_payload(1_000_000_000))
    with pytest.raises(RuntimeError, match="lacks its reciprocal"):
        ptg._collect_ptg2_manifest_sidecar_artifacts({"provider_npi_scope": scope_path})


@pytest.mark.asyncio
async def test_strict_file_requires_stage_and_supported_arch(monkeypatch) -> None:
    """Reject missing stage identity and a non-V3 architecture before writes."""

    strict_file_arguments_by_name = {
        "file_path": "unused",
        "file_id": 1,
        "meta": {},
        "plan_info": None,
        "test_mode": True,
        "import_log_cls": object,
        "source_url": "https://example.test/rate.json",
        "source_version": None,
        "snapshot_id": "snapshot",
        "coverage_scope_id": "scope",
        "import_month": ptg.datetime.date(2026, 7, 1),
    }
    with pytest.raises(RuntimeError, match="require manifest serving"):
        await ptg._parse_strict_v3_file(**strict_file_arguments_by_name)

    monkeypatch.setattr(ptg, "_derive_plan_fields", lambda *_args: {})
    monkeypatch.setattr(ptg, "_normalize_source_network_names", lambda _value: set())
    monkeypatch.setattr(ptg, "_ptg2_snapshot_arch_from_env", lambda: "legacy")
    with pytest.raises(RuntimeError, match="only postgres_binary_v3"):
        await ptg._parse_strict_v3_file(
            **strict_file_arguments_by_name,
            ptg2_manifest_stage_table="stage",
        )


@pytest.mark.asyncio
async def test_published_allowed_only_snapshot_reconciles_allowed_pointer(
    monkeypatch,
) -> None:
    """Return the allowed pointer result when no negotiated lifecycle exists."""

    allowed = AsyncMock(return_value={"status": "promoted"})
    monkeypatch.setattr(
        ptg,
        "_reconcile_serving_snapshot_pointer",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(ptg, "_reconcile_allowed_snapshot_pointer", allowed)

    reconciliation_outcome = await ptg._reconcile_already_published_snapshot(
        snapshot_attributes={
            "manifest": {
                "allowed_amount_index": {
                    "contract": ptg.PTG2_ALLOWED_AMOUNT_CONTRACT,
                    "previous_snapshot_id": "previous",
                }
            }
        },
        snapshot_id="snapshot",
        source_key="source",
        import_month=ptg.datetime.date(2026, 7, 1),
    )

    assert reconciliation_outcome == {"status": "promoted"}
    assert allowed.await_args.kwargs["previous_snapshot_id"] == "previous"


@pytest.mark.asyncio
async def test_published_snapshot_without_lifecycle_is_not_applicable(
    monkeypatch,
) -> None:
    """Describe a published manifest that owns no current pointer."""

    monkeypatch.setattr(
        ptg,
        "_reconcile_serving_snapshot_pointer",
        AsyncMock(return_value=None),
    )
    lifecycle_outcome = await ptg._reconcile_already_published_snapshot(
        snapshot_attributes={},
        snapshot_id="snapshot",
        source_key="source",
        import_month=ptg.datetime.date(2026, 7, 1),
    )
    assert lifecycle_outcome["status"] == "not_applicable"


@pytest.mark.asyncio
async def test_allowed_pointer_reconciliation_supersedes_or_promotes(
    monkeypatch,
) -> None:
    """Refuse a newer pointer and delegate compatible states to publication."""

    current = AsyncMock(side_effect=["newer", "previous"])
    publish = AsyncMock(return_value={"status": "promoted"})
    monkeypatch.setattr(ptg, "_current_allowed_snapshot_id", current)
    monkeypatch.setattr(ptg, "_publish_allowed_current_pointer", publish)
    pointer_arguments_by_name = {
        "source_key": "source",
        "snapshot_id": "snapshot",
        "previous_snapshot_id": "previous",
        "import_month": ptg.datetime.date(2026, 7, 1),
    }

    superseded = await ptg._reconcile_allowed_snapshot_pointer(
        **pointer_arguments_by_name
    )
    promoted = await ptg._reconcile_allowed_snapshot_pointer(
        **pointer_arguments_by_name
    )

    assert superseded["status"] == "superseded"
    assert superseded["current_snapshot_id"] == "newer"
    assert promoted == {"status": "promoted"}
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_allowed_pointer_publication_uses_locked_transaction(
    monkeypatch,
) -> None:
    """Advance the isolated pointer under the shared pointer lock."""

    session = object()

    @asynccontextmanager
    async def transaction():
        yield session

    acquire = AsyncMock()
    swap = AsyncMock()
    monkeypatch.setattr(ptg.db, "transaction", transaction)
    monkeypatch.setattr(ptg, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(ptg, "_acquire_source_pointer_gc_lock", acquire)
    monkeypatch.setattr(ptg, "_compare_and_swap_source_pointer", swap)
    updated_at = ptg.datetime.datetime.now(ptg.datetime.timezone.utc)

    publication_outcome = await ptg._publish_allowed_current_pointer(
        source_key="source",
        snapshot_id="snapshot",
        previous_snapshot_id=None,
        import_month=ptg.datetime.date(2026, 7, 1),
        updated_at=updated_at,
    )

    assert publication_outcome["status"] == "promoted"
    assert publication_outcome["source_key"] == ptg._allowed_source_pointer_key(
        "source"
    )
    acquire.assert_awaited_once_with(session)
    swap.assert_awaited_once()


@pytest.mark.asyncio
async def test_price_copy_preflight_rejects_incomplete_artifacts(
    tmp_path: Path,
) -> None:
    """Reject inconsistent source counts and incomplete strict-price families."""

    kinds = ("price_atom", "price_set_atom")
    empty_price_files_by_kind = {kind: [] for kind in kinds}
    disabled = await ptg._copy_strict_v3_price_files(
        kinds,
        empty_price_files_by_kind,
        {},
        {kind: 0 for kind in kinds},
        "stage",
    )
    assert disabled["reason"] == "no_strict_v3_price_copy_files"
    with pytest.raises(RuntimeError, match="source counts disagree"):
        await ptg._copy_strict_v3_price_files(
            kinds,
            empty_price_files_by_kind,
            {},
            {"price_atom": 1, "price_set_atom": 2},
            "stage",
        )
    with pytest.raises(RuntimeError, match="omitted required"):
        await ptg._copy_strict_v3_price_files(
            kinds,
            {"price_atom": [tmp_path / "atom"], "price_set_atom": []},
            {},
            {kind: 1 for kind in kinds},
            "stage",
        )


def test_publication_progress_failures_are_nonfatal(monkeypatch) -> None:
    """Keep both legacy and measured publication progress advisory."""

    monkeypatch.setattr(
        ptg,
        "write_live_progress",
        lambda **_fields: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    ptg._emit_ptg2_publish_progress(
        "stage",
        completed_steps=1,
        total_steps=2,
        message_text="working",
    )
    progress = ptg._PTG2V4PublicationProgress()
    progress.observe("provider graph publication", {"published_rows": 2})
    assert progress._event_count == 1


@pytest.mark.asyncio
async def test_failed_cleanup_pointer_error_preserves_candidate(monkeypatch) -> None:
    """Preserve candidate tables when the live-pointer recheck is unavailable."""

    monkeypatch.setattr(
        ptg,
        "_current_source_snapshot_id",
        AsyncMock(side_effect=RuntimeError("offline")),
    )
    context = SimpleNamespace(
        is_known_published=False,
        candidate_staged=False,
        serving_index={},
        source_key="source",
        snapshot_id="snapshot",
    )
    assert await ptg._should_preserve_failed_candidate_tables(context) is True


def test_abandonment_progress_accumulates_committed_work(monkeypatch) -> None:
    """Accumulate bounded cleanup work and expose a sorted progress snapshot."""

    events = []
    monkeypatch.setattr(
        ptg, "write_live_progress", lambda **fields: events.append(fields)
    )
    cleanup_report_by_field: dict[str, object] = {}
    progress = ptg._AbandonmentProgress(cleanup_report_by_field)
    progress.report("removed_blocks", 2)
    progress.report("removed_blocks", 3)
    assert cleanup_report_by_field["shared_layout_abandonment_progress"] == {
        "removed_blocks": 5
    }
    assert events[-1]["pct"] == 99
