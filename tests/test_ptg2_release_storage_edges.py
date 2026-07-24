from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import provider_references, snapshot_cleanup


def test_snapshot_cleanup_selects_required_materialized_tables() -> None:
    serving_index = {
        "table": "mrf.ptg2_serving_rate_compact_transient",
        "serving_table_retained": False,
        "materialized_tables": {
            "primary": "mrf.ptg2_serving_rate_compact_primary",
            "invalid": "unrelated_table",
        },
    }
    assert snapshot_cleanup._required_snapshot_table_names(serving_index) == [
        "ptg2_serving_rate_compact_primary"
    ]
    assert snapshot_cleanup._required_snapshot_table_names(
        {
            "table": "mrf.ptg2_serving_rate_compact_transient",
            "serving_table_retained": False,
        }
    ) == []


def test_snapshot_cleanup_classifies_and_locates_artifact_references(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_artifact_ids: set[str] = set()
    local_artifact_references: set[str] = set()
    for reference, include_local in (
        (None, True),
        ("db://ptg2_artifact/artifact_1", True),
        ("relative.bin", True),
        ("ignored.bin", False),
    ):
        snapshot_cleanup._record_snapshot_artifact_reference(
            reference,
            database_artifact_ids,
            local_artifact_references,
            include_local_artifacts=include_local,
        )
    assert database_artifact_ids == {"artifact_1"}
    assert local_artifact_references == {"relative.bin"}

    local_file = tmp_path / "artifact.bin"
    local_file.write_bytes(b"ok")
    monkeypatch.setattr(
        snapshot_cleanup,
        "resolve_ptg2_artifact_dir",
        lambda: tmp_path,
    )
    assert snapshot_cleanup._is_local_artifact_available(str(local_file))
    assert snapshot_cleanup._is_local_artifact_available(local_file.as_uri())
    assert snapshot_cleanup._is_local_artifact_available(local_file.name)
    assert not snapshot_cleanup._is_local_artifact_available("missing.bin")


def test_snapshot_cleanup_tracks_shared_table_owners() -> None:
    owners_by_table = snapshot_cleanup._snapshot_table_owners(
        [
            {
                "snapshot_id": "candidate_a",
                "manifest": {
                    "serving_index": {
                        "table": "mrf.ptg2_serving_rate_compact_shared"
                    }
                },
            },
            {
                "snapshot_id": "candidate_b",
                "manifest": '{"serving_index":{"table":"mrf.ptg2_serving_rate_compact_shared"}}',
            },
            {"snapshot_id": "", "manifest": {}},
            {"snapshot_id": "candidate_c", "manifest": "not-json"},
        ]
    )
    assert owners_by_table == {
        "ptg2_serving_rate_compact_shared": {"candidate_a", "candidate_b"}
    }


@pytest.mark.asyncio
async def test_snapshot_db_artifact_availability_requires_complete_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert await snapshot_cleanup._available_snapshot_db_artifact_ids(
        "candidate_schema", "candidate", set()
    ) == set()

    monkeypatch.setattr(
        snapshot_cleanup,
        "_table_exists",
        AsyncMock(return_value=False),
    )
    assert await snapshot_cleanup._available_snapshot_db_artifact_ids(
        "candidate_schema", "candidate", {"artifact_1"}
    ) == set()

    monkeypatch.setattr(
        snapshot_cleanup,
        "_table_exists",
        AsyncMock(side_effect=(True, True)),
    )
    monkeypatch.setattr(
        snapshot_cleanup.db,
        "all",
        AsyncMock(
            return_value=[
                {"artifact_id": "artifact_1"},
                SimpleNamespace(_mapping={"artifact_id": "artifact_2"}),
            ]
        ),
    )
    assert await snapshot_cleanup._available_snapshot_db_artifact_ids(
        "candidate_schema",
        "candidate",
        {"artifact_1", "artifact_2"},
    ) == {"artifact_1", "artifact_2"}


def _provider_reference_facade(
    tmp_path: Path,
    materialize_json_source: AsyncMock,
) -> SimpleNamespace:
    return SimpleNamespace(
        ptg2_temp_parent=lambda: tmp_path,
        materialize_json_source=materialize_json_source,
        _record_source_version=AsyncMock(),
        _push_ptg2_objects=AsyncMock(),
        push_objects=AsyncMock(),
        flush_error_log=AsyncMock(),
        TEST_PROVIDER_GROUPS=1,
    )


def _provider_reference_classes() -> dict[str, type]:
    return {
        "PTGProviderGroup": object,
        "PTGFile": object,
        "ImportLog": object,
    }


@pytest.mark.asyncio
async def test_provider_reference_materialization_returns_empty_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    materialize_json_source = AsyncMock(side_effect=RuntimeError("unavailable"))
    facade = _provider_reference_facade(tmp_path, materialize_json_source)
    monkeypatch.setattr(provider_references, "_ptg_facade", lambda: facade)

    assert await provider_references._process_provider_reference_file(
        "https://invalid.example/reference.json",
        _provider_reference_classes(),
        test_mode=False,
    ) == {}


@pytest.mark.asyncio
async def test_provider_reference_materialization_records_group_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logical_artifact = SimpleNamespace(logical_path=tmp_path / "logical.json")
    materialize_json_source = AsyncMock(
        return_value=(SimpleNamespace(), logical_artifact)
    )
    facade = _provider_reference_facade(tmp_path, materialize_json_source)
    monkeypatch.setattr(provider_references, "_ptg_facade", lambda: facade)
    monkeypatch.setattr(
        provider_references,
        "load_json_artifact",
        lambda _path: {
            "version": "1",
            "provider_groups": [
                {
                    "provider_group_id": "7",
                    "tin": {"type": "ein", "value": "000000000"},
                    "npi": [1000000001],
                    "network_name": ["primary"],
                },
                {"provider_group_ref": 8, "npi": [1000000002]},
            ],
        },
    )

    provider_groups_by_reference = (
        await provider_references._process_provider_reference_file(
            "https://invalid.example/reference.json",
            _provider_reference_classes(),
            test_mode=True,
            import_run_id="run_1",
        )
    )

    assert list(provider_groups_by_reference) == [7]
    assert provider_groups_by_reference[7][0]["provider_group_id"] == 7
    facade._record_source_version.assert_awaited_once()
    facade._push_ptg2_objects.assert_awaited_once()
    facade.push_objects.assert_awaited_once()
    facade.flush_error_log.assert_awaited_once()
