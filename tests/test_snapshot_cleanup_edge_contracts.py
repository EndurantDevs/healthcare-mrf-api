# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Edge-contract coverage for source snapshot cleanup helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import snapshot_cleanup


def _shared_serving_index(
    *,
    generation: str = "shared_blocks_v3",
) -> dict[str, object]:
    return {
        "arch_version": "postgres_binary_v3",
        "storage_generation": generation,
    }


def test_required_tables_honor_materialized_and_retained_table_contracts() -> None:
    assert snapshot_cleanup._snapshot_manifest_table_names({}) == []
    assert snapshot_cleanup._required_snapshot_table_names(
        {
            "materialized_tables": {
                "serving": "mrf.ptg2_serving_materialized",
            },
        }
    ) == ["ptg2_serving_materialized"]
    assert snapshot_cleanup._required_snapshot_table_names(
        {
            "table": "mrf.ptg2_serving_transient",
            "serving_binary_table": "mrf.ptg2_serving_binary_retained",
        }
    ) == [
        "ptg2_serving_transient",
        "ptg2_serving_binary_retained",
    ]
    assert snapshot_cleanup._required_snapshot_table_names(
        {
            "table": "mrf.ptg2_serving_transient",
            "serving_binary_table": "mrf.ptg2_serving_binary_retained",
            "serving_table_retained": False,
        }
    ) == ["ptg2_serving_binary_retained"]


def test_artifact_reference_classification_respects_storage_contract() -> None:
    database_artifact_ids: set[str] = set()
    local_artifact_references: set[str] = set()

    snapshot_cleanup._record_snapshot_artifact_reference(
        None,
        database_artifact_ids,
        local_artifact_references,
        include_local_artifacts=False,
    )
    snapshot_cleanup._record_snapshot_artifact_reference(
        "db://ptg2_artifact/artifact-a",
        database_artifact_ids,
        local_artifact_references,
        include_local_artifacts=False,
    )
    snapshot_cleanup._record_snapshot_artifact_reference(
        "ignored-local.bin",
        database_artifact_ids,
        local_artifact_references,
        include_local_artifacts=False,
    )
    snapshot_cleanup._record_snapshot_artifact_reference(
        "retained-local.bin",
        database_artifact_ids,
        local_artifact_references,
        include_local_artifacts=True,
    )

    assert database_artifact_ids == {"artifact-a"}
    assert local_artifact_references == {"retained-local.bin"}


def test_snapshot_artifact_references_include_nested_sidecars_and_paths() -> None:
    database_artifact_ids, local_artifact_references = (
        snapshot_cleanup._snapshot_artifact_references(
            {
                "artifact_uri": "db://ptg2_artifact/root",
                "storage_uri": "root-local.bin",
                "artifacts": {
                    "sidecars": [
                        {"storage_uri": "db://ptg2_artifact/sidecar"},
                        {"path": "sidecar-local.bin"},
                        "ignored",
                    ],
                    "primary": {"path": "primary-local.bin"},
                    "ignored": [],
                },
            }
        )
    )

    assert database_artifact_ids == {"root", "sidecar"}
    assert local_artifact_references == {
        "root-local.bin",
        "sidecar-local.bin",
        "primary-local.bin",
    }


def test_strict_shared_snapshot_ignores_local_artifact_paths() -> None:
    database_artifact_ids, local_artifact_references = (
        snapshot_cleanup._snapshot_artifact_references(
            {
                **_shared_serving_index(),
                "artifact_uri": "local-root.bin",
                "artifacts": {
                    "primary": {"path": "local-primary.bin"},
                },
            }
        )
    )

    assert database_artifact_ids == set()
    assert local_artifact_references == set()


def test_local_artifact_availability_resolves_uri_absolute_and_relative_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    relative_artifact = artifact_root / "relative.bin"
    relative_artifact.write_bytes(b"relative")
    absolute_artifact = tmp_path / "absolute.bin"
    absolute_artifact.write_bytes(b"absolute")
    monkeypatch.setattr(
        snapshot_cleanup,
        "resolve_ptg2_artifact_dir",
        lambda: artifact_root,
    )

    assert snapshot_cleanup._is_local_artifact_available(
        absolute_artifact.as_uri()
    )
    assert snapshot_cleanup._is_local_artifact_available(str(absolute_artifact))
    assert snapshot_cleanup._is_local_artifact_available("relative.bin")
    assert not snapshot_cleanup._is_local_artifact_available("missing.bin")


@pytest.mark.asyncio
async def test_available_database_artifacts_short_circuit_empty_and_missing_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert (
        await snapshot_cleanup._available_snapshot_db_artifact_ids(
            "mrf",
            "snapshot-a",
            set(),
        )
        == set()
    )

    table_exists = AsyncMock(return_value=False)
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", table_exists)
    assert (
        await snapshot_cleanup._available_snapshot_db_artifact_ids(
            "mrf",
            "snapshot-a",
            {"artifact-a"},
        )
        == set()
    )
    table_exists.assert_awaited_once_with("mrf", "ptg2_artifact_manifest")


@pytest.mark.asyncio
async def test_available_database_artifacts_return_only_complete_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table_exists = AsyncMock(side_effect=[True, True])
    artifact_rows = AsyncMock(
        return_value=[
            {"artifact_id": "artifact-a"},
            SimpleNamespace(_mapping={"artifact_id": "artifact-b"}),
        ]
    )
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", table_exists)
    monkeypatch.setattr(snapshot_cleanup.db, "all", artifact_rows)

    available_ids = await snapshot_cleanup._available_snapshot_db_artifact_ids(
        "mrf",
        "snapshot-a",
        {"artifact-b", "artifact-a"},
    )

    assert available_ids == {"artifact-a", "artifact-b"}
    assert artifact_rows.await_args.kwargs == {
        "snapshot_id": "snapshot-a",
        "artifact_ids": ["artifact-a", "artifact-b"],
    }


@pytest.mark.asyncio
async def test_stage_table_cleanup_requires_complete_attempt_identity() -> None:
    await snapshot_cleanup._drop_ptg2_snapshot_table_names([])

    with pytest.raises(
        ValueError,
        match="requires snapshot and run identifiers",
    ):
        await snapshot_cleanup._drop_ptg2_snapshot_table_names(
            ["ptg2_serving_stage"],
            snapshot_id="snapshot-a",
        )


@pytest.mark.asyncio
async def test_strict_shared_manifest_cleanup_never_drops_snapshot_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drop_tables = AsyncMock()
    monkeypatch.setattr(
        snapshot_cleanup,
        "_drop_ptg2_snapshot_table_names",
        drop_tables,
    )

    await snapshot_cleanup._drop_ptg2_snapshot_tables_for_manifest(
        _shared_serving_index()
    )

    drop_tables.assert_awaited_once_with([])


def test_lineage_configuration_and_missing_predecessor_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV,
        "invalid",
    )
    assert snapshot_cleanup._source_snapshot_lineage_limit() == 4
    assert snapshot_cleanup._source_snapshot_keep_ids(
        [{"snapshot_id": "snapshot-a", "previous_snapshot_id": None}],
        {"snapshot-a"},
    ) == {"snapshot-a"}


def test_snapshot_serving_index_parses_serialized_and_invalid_manifests() -> None:
    assert snapshot_cleanup._snapshot_serving_index_dict(
        {
            "manifest": (
                '{"serving_index": {"source_key": "source-a", '
                '"storage_generation": "shared_blocks_v4"}}'
            )
        }
    ) == {
        "source_key": "source-a",
        "storage_generation": "shared_blocks_v4",
    }
    assert snapshot_cleanup._snapshot_serving_index_dict(
        {"manifest": "not-json"}
    ) == {}
    assert snapshot_cleanup._snapshot_serving_index_dict(
        {"manifest": ["not", "an", "object"]}
    ) == {}


def test_table_ownership_excludes_shared_and_anonymous_snapshots() -> None:
    snapshot_rows = [
        {"snapshot_id": "", "manifest": {}},
        {
            "snapshot_id": "snapshot-a",
            "manifest": {
                "serving_index": {
                    "table": "mrf.ptg2_serving_owned",
                }
            },
        },
        SimpleNamespace(
            _mapping={
                "snapshot_id": "snapshot-b",
                "manifest": {
                    "serving_index": {
                        "table": "mrf.ptg2_serving_shared",
                    }
                },
            }
        ),
        {
            "snapshot_id": "snapshot-c",
            "manifest": {
                "serving_index": {
                    "table": "mrf.ptg2_serving_shared",
                }
            },
        },
    ]

    assert snapshot_cleanup._snapshot_table_owners(snapshot_rows) == {
        "ptg2_serving_owned": {"snapshot-a"},
        "ptg2_serving_shared": {"snapshot-b", "snapshot-c"},
    }
    assert snapshot_cleanup._exclusively_owned_snapshot_table_names(
        "snapshot-a",
        [
            "ptg2_serving_owned",
            "ptg2_serving_shared",
            "ptg2_serving_owned",
        ],
        snapshot_rows,
    ) == ["ptg2_serving_owned"]


@pytest.mark.asyncio
async def test_unlocked_cleanup_delegates_to_source_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_source_tables = AsyncMock()
    monkeypatch.setattr(
        snapshot_cleanup,
        "_cleanup_source_tables",
        cleanup_source_tables,
    )

    await snapshot_cleanup._cleanup_old_ptg2_source_tables(
        "source-a",
        {"snapshot-a"},
        lock_pointer_state=False,
    )

    cleanup_source_tables.assert_awaited_once_with(
        snapshot_cleanup.db,
        source_key="source-a",
        keep_snapshot_ids={"snapshot-a"},
    )
