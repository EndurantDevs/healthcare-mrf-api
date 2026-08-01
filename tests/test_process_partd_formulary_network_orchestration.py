# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import importlib
from pathlib import Path
from unittest.mock import AsyncMock


module = importlib.import_module("process.partd_formulary_network")


def _artifact():
    return module.SourceArtifact(
        "quarterly",
        "https://example.test/a.zip",
        "a.zip",
        datetime.date(2026, 1, 1),
        datetime.date(2026, 1, 1),
    )


def test_artifact_member_classification_preserves_unknown_fallbacks(
    tmp_path,
    monkeypatch,
):
    plan_path = tmp_path / "plan.csv"
    formulary_path = tmp_path / "formulary.csv"
    unknown_path_list = [tmp_path / f"unknown-{index}.csv" for index in range(4)]
    extracted_file_list = [
        (plan_path, "Plan Information File.csv"),
        (formulary_path, "Basic Drugs Formulary File.csv"),
        *[(path, path.name) for path in unknown_path_list],
    ]
    monkeypatch.setattr(
        module,
        "_load_plan_formulary_map",
        lambda _path: {("p", "1", "0"): "f"},
    )
    monkeypatch.setattr(
        module,
        "_load_formulary_ndc_map",
        lambda _path: {("f", "n"): "r"},
    )

    members = module._classify_artifact_members(extracted_file_list, "quarterly")

    assert members.activity == tuple((path, path.name) for path in unknown_path_list[:3])
    assert members.pricing == members.activity
    assert members.plan_to_formulary == {("p", "1", "0"): "f"}
    assert members.formulary_ndc_to_rxnorm == {("f", "n"): "r"}


def test_activity_member_import_adds_parallel_recovery_count(tmp_path, monkeypatch):
    activity_path = tmp_path / "activity.csv"
    activity_path.write_text("header\n", encoding="utf-8")
    monkeypatch.setenv("HLTHPRT_MAX_PARTD_JOBS", "4")
    monkeypatch.setattr(module, "_init_activity_chunk_state", AsyncMock())
    monkeypatch.setattr(
        module,
        "_enqueue_activity_chunks",
        AsyncMock(return_value=[("snapshot:0", activity_path)]),
    )
    monkeypatch.setattr(
        module,
        "_wait_for_activity_chunks",
        AsyncMock(return_value=(5, set())),
    )
    monkeypatch.setattr(
        module,
        "_recover_incomplete_activity_chunks",
        AsyncMock(return_value=7),
    )

    imported_count = asyncio.run(
        module._import_activity_members(
            ((activity_path, activity_path.name),),
            _artifact(),
            "snapshot",
            False,
            object(),
            "run-1",
        )
    )

    assert imported_count == 12


def test_pricing_member_import_applies_one_cross_file_test_limit(tmp_path, monkeypatch):
    pricing_path_list = [tmp_path / f"pricing-{index}.csv" for index in range(2)]
    for path in pricing_path_list:
        path.write_text("code\na\nb\n", encoding="utf-8")
    flushed_row_list = []

    async def flush_rows(_activity_rows, pricing_rows):
        flushed_row_list.extend(pricing_rows)
        pricing_rows.clear()

    monkeypatch.setattr(module, "PARTD_TEST_MAX_ROWS_PER_FILE", 3)
    monkeypatch.setattr(
        module,
        "_pricing_rows_from_source",
        lambda *_args, **_kwargs: [{"ok": True}],
    )
    monkeypatch.setattr(module, "_flush_batches", flush_rows)

    imported_count = asyncio.run(
        module._import_pricing_members(
            tuple((path, path.name) for path in pricing_path_list),
            _artifact(),
            "snapshot",
            {},
            {},
            True,
        )
    )

    assert imported_count == 3
    assert len(flushed_row_list) == 3


def test_import_artifact_preserves_materialization_order(tmp_path, monkeypatch):
    event_list = []
    members = module._ArtifactMembers((), (), {}, {})

    async def stage_artifact(_url, target_path):
        event_list.append("stage")
        Path(target_path).write_bytes(b"archive")

    async def import_activity(*_args):
        event_list.append("activity")
        return 2

    async def import_pricing(*_args):
        event_list.append("pricing")
        return 3

    async def materialize_activity(*_args):
        event_list.append("materialize_activity")

    async def materialize_pricing(*_args):
        event_list.append("materialize_pricing")

    monkeypatch.setattr(module, "PARTD_WORKDIR", str(tmp_path))
    monkeypatch.setattr(module, "_stage_artifact_file", stage_artifact)
    monkeypatch.setattr(module, "_extract_data_files", lambda *_args: [])
    monkeypatch.setattr(module, "_classify_artifact_members", lambda *_args: members)
    monkeypatch.setattr(module, "_import_activity_members", import_activity)
    monkeypatch.setattr(module, "_import_pricing_members", import_pricing)
    monkeypatch.setattr(module, "_materialize_activity_snapshot", materialize_activity)
    monkeypatch.setattr(module, "_materialize_pricing_snapshot", materialize_pricing)

    counts = asyncio.run(
        module._import_artifact(
            _artifact(),
            "snapshot",
            schema="mrf",
            test_mode=False,
        )
    )

    assert counts == (2, 3)
    assert event_list == [
        "stage",
        "activity",
        "pricing",
        "materialize_activity",
        "materialize_pricing",
    ]
