from __future__ import annotations

import argparse
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_source_snapshot_gc_cli as gc_cli
from process.ptg_parts.ptg2_source_snapshot_gc import (
    PTG2SnapshotGCTable,
    PTG2SourceSnapshotGCPlan,
)


def _plan():
    return PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=("current",),
        candidate_snapshot_ids=("candidate-a", "candidate-b"),
        candidate_reasons=(
            ("candidate-a", "failed"),
            ("candidate-b", "failed"),
        ),
        tables=(
            PTG2SnapshotGCTable("candidate-a", "source", "prices_1", 100),
            PTG2SnapshotGCTable("candidate-b", "source", "prices_2", 200),
            PTG2SnapshotGCTable("candidate-b", "source", "codes_1", 50),
        ),
        shared_snapshot_ids=("shared",),
        shared_layout_count=2,
        shared_candidate_hash_count=3,
        shared_stored_bytes=400,
    )


def test_print_plan_groups_table_families_and_reasons(capsys):
    gc_cli._print_plan(_plan())

    output = capsys.readouterr().out
    assert "current_snapshot_refs=1" in output
    assert "candidate_reason=failed snapshots=2" in output
    assert "candidate_tables=3" in output
    assert "candidate_bytes=750" in output
    assert "family=prices_* tables=2 bytes=300" in output
    assert "family=codes_* tables=1 bytes=50" in output
    assert "shared_candidate_hashes=3" in output


def test_non_negative_integer_parser_accepts_zero_and_rejects_negative():
    assert gc_cli._non_negative_int("0") == 0
    with pytest.raises(argparse.ArgumentTypeError, match="non-negative"):
        gc_cli._non_negative_int("-1")


@pytest.mark.asyncio
async def test_dry_run_builds_validates_and_prints_plan(monkeypatch, capsys):
    plan = _plan()
    builder = AsyncMock(return_value=plan)
    validator_calls = []
    monkeypatch.setattr(gc_cli, "build_ptg2_source_snapshot_gc_plan", builder)
    monkeypatch.setattr(
        gc_cli,
        "validate_ptg2_source_snapshot_gc_plan",
        lambda candidate, **limits: validator_calls.append((candidate, limits)),
    )

    await gc_cli._amain(
        [
            "--schema",
            "audit_schema",
            "--max-snapshots",
            "2",
            "--max-tables",
            "3",
            "--max-bytes-gb",
            "4",
            "--retain-current-lineage",
            "5",
        ]
    )

    assert builder.await_args.kwargs == {
        "schema_name": "audit_schema",
        "retain_current_lineage": 5,
        "max_snapshots": 2,
        "max_tables": 3,
        "max_bytes": 4 * 1024**3,
    }
    assert validator_calls[0][0] is plan
    assert "cleanup_executed=false" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_execute_applies_and_prints_exact_limits(monkeypatch, capsys):
    executor = AsyncMock(return_value=_plan())
    monkeypatch.setattr(gc_cli, "execute_ptg2_source_snapshot_gc_plan", executor)

    await gc_cli._amain(
        [
            "--execute",
            "--schema",
            "audit_schema",
            "--lock-timeout",
            "2s",
        ]
    )

    assert executor.await_args.kwargs["schema_name"] == "audit_schema"
    assert executor.await_args.kwargs["lock_timeout"] == "2s"
    assert executor.await_args.kwargs["max_bytes"] == 80 * 1024**3
    assert "cleanup_executed=true" in capsys.readouterr().out


def test_main_runs_async_entrypoint(monkeypatch):
    calls = []

    async def fake_amain():
        calls.append("amain")

    monkeypatch.setattr(gc_cli, "_amain", fake_amain)

    gc_cli.main()

    assert calls == ["amain"]
