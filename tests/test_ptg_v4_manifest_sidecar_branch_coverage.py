from __future__ import annotations

import io
import os
import stat
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_manifest_publish as manifest


class _MappingRow:
    _mapping = {"value": "mapping"}


class _BrokenPath:
    def exists(self):
        raise OSError("unavailable")


class _RawConnection:
    def __init__(self, copy_to_table=None) -> None:
        self.driver_connection = SimpleNamespace()
        if copy_to_table is not None:
            self.driver_connection.copy_to_table = copy_to_table


@asynccontextmanager
async def _acquire_connection(raw_connection):
    yield SimpleNamespace(raw_connection=raw_connection)


def test_manifest_scalar_and_artifact_helpers_fail_closed(tmp_path) -> None:
    """Normalize supported row shapes and reject unusable artifact metadata."""

    assert manifest._row_value(_MappingRow(), "value") == "mapping"
    assert manifest._row_value({"value": "dict"}, "value") == "dict"
    assert manifest._row_value(SimpleNamespace(value="attribute"), "value") == "attribute"
    assert manifest._row_value(("position",), "value") == "position"
    assert manifest._row_value(object(), "value") is None
    assert manifest._path_byte_count(None) is None
    assert manifest._path_byte_count(_BrokenPath()) is None
    artifact_path = tmp_path / "artifact"
    artifact_path.write_bytes(b"abc")
    assert manifest._path_byte_count(artifact_path) == 3

    assert manifest._ptg2_manifest_sidecar_upload_count(None) == 0
    sidecars_by_name = {
        "sidecars": [
            "invalid",
            {"path": "/tmp/a"},
            {"path": "/tmp/b", "storage_uri": "db://ptg2_artifact/id"},
        ],
        "direct": {"path": "/tmp/c"},
        "metadata": {"storage_uri": "file:///tmp/no-path"},
    }
    assert manifest._ptg2_manifest_sidecar_upload_count(sidecars_by_name) == 2
    assert manifest._artifact_chunk_count({"byte_count": "bad"}) is None
    assert manifest._artifact_chunk_count({"byte_count": 1, "chunk_bytes": 0}) is None
    assert manifest._artifact_chunk_count({"byte_count": 9, "chunk_bytes": 4}) == 3


def test_manifest_environment_switches_cover_every_policy(monkeypatch) -> None:
    """Honor explicit direct-lean switches and conservative profile fallback."""

    profile_env = manifest.PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_ENV
    monkeypatch.setenv(profile_env, "legacy")
    assert manifest._provider_group_location_index_profile() == "full"
    monkeypatch.setenv(profile_env, "minimal")
    assert manifest._provider_group_location_index_profile() == "lean"
    monkeypatch.setenv(profile_env, "unknown")
    assert manifest._provider_group_location_index_profile() == "lean"

    parallel_env = manifest.PTG2_MANIFEST_LEAN_REWRITE_PARALLEL_DICTS_ENV
    natural_env = manifest.PTG2_MANIFEST_POSTGRES_BINARY_NATURAL_LEAN_STREAM_ENV
    monkeypatch.setenv(parallel_env, "false")
    monkeypatch.setenv(natural_env, "true")
    assert manifest._use_parallel_dictionary_rewrite() is False
    assert manifest._is_natural_lean_stream_enabled() is True
    monkeypatch.delenv(parallel_env)
    monkeypatch.delenv(natural_env)
    monkeypatch.setattr(
        manifest,
        "_ptg2_manifest_snapshot_arch",
        lambda: manifest.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3,
    )
    assert manifest._use_parallel_dictionary_rewrite() is True
    assert manifest._is_natural_lean_stream_enabled() is True


@pytest.mark.asyncio
async def test_v4_serving_stage_requires_coordinates_and_registration(
    monkeypatch,
) -> None:
    """Register all deterministic V4 stages before issuing physical DDL."""

    monkeypatch.setattr(manifest, "_ptg2_manifest_snapshot_arch", lambda: "wrong")
    with pytest.raises(RuntimeError, match="only postgres_binary_v3"):
        await manifest._create_serving_stage_table("token")

    monkeypatch.setattr(
        manifest,
        "_ptg2_manifest_snapshot_arch",
        lambda: manifest.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3,
    )
    with pytest.raises(RuntimeError, match="require snapshot"):
        await manifest._create_serving_stage_table(
            "token",
            storage_generation=manifest.PTG2_V4_SHARED_GENERATION,
        )

    register = AsyncMock()
    monkeypatch.setattr(manifest, "register_attempt_stage_tables", register)
    monkeypatch.setattr(manifest, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(manifest.db, "status", AsyncMock())
    monkeypatch.setattr(manifest, "_create_price_atom_stage_table", AsyncMock())
    monkeypatch.setattr(manifest, "_create_price_atom_member_stage_table", AsyncMock())
    monkeypatch.setattr(manifest, "_create_price_set_summary_stage_table", AsyncMock())
    stage_table = await manifest._create_serving_stage_table(
        "token",
        snapshot_id="snapshot",
        internal_run_id="run",
        storage_generation=manifest.PTG2_V4_SHARED_GENERATION,
    )
    assert stage_table.endswith("token")
    assert len(register.await_args.kwargs["table_names"]) == 4
    register.reset_mock()
    await manifest._create_serving_stage_table("ordinary")
    register.assert_not_awaited()


@pytest.mark.asyncio
async def test_materialized_tables_must_be_qualified_and_durable(monkeypatch) -> None:
    """Deduplicate valid tables and reject malformed materialized relations."""

    status = AsyncMock()
    monkeypatch.setattr(manifest.db, "status", status)
    await manifest._ensure_materialized_tables_logged({})
    with pytest.raises(RuntimeError, match="Invalid PTG2 materialized table"):
        await manifest._ensure_materialized_tables_logged({"bad": "unqualified"})
    await manifest._ensure_materialized_tables_logged(
        {"first": "mrf.table", "duplicate": "mrf.table"}
    )
    status.assert_awaited_once()
    assert "SET LOGGED" in status.await_args.args[0]


def test_measured_copy_reader_reports_only_real_bytes() -> None:
    """Emit byte progress only when both data and a callback are present."""

    observed_bytes: list[int] = []
    reader = manifest._MeasuredCopyReader(io.BytesIO(b"abc"), observed_bytes.append)
    assert reader.read(2) == b"ab"
    assert reader.read(2) == b"c"
    assert reader.read(2) == b""
    assert observed_bytes == [2, 1]
    silent_reader = manifest._MeasuredCopyReader(io.BytesIO(b"x"), None)
    assert silent_reader.read() == b"x"


@pytest.mark.asyncio
async def test_manifest_copy_skips_empty_inputs_and_requires_driver(
    monkeypatch,
    tmp_path,
) -> None:
    """Avoid empty COPY operations and reject drivers without binary COPY support."""

    missing_path = tmp_path / "missing"
    await manifest._copy_ptg2_manifest_file(
        missing_path,
        target_table="target",
        columns=["value"],
    )
    empty_path = tmp_path / "empty"
    empty_path.write_bytes(b"")
    await manifest._copy_ptg2_manifest_file(
        empty_path,
        target_table="target",
        columns=["value"],
    )

    copy_path = tmp_path / "copy"
    copy_path.write_bytes(b"value")
    monkeypatch.setattr(manifest.db, "acquire", lambda: _acquire_connection(_RawConnection()))
    with pytest.raises(NotImplementedError, match="copy_to_table"):
        await manifest._copy_ptg2_manifest_file(
            copy_path,
            target_table="target",
            columns=["value"],
        )


@pytest.mark.asyncio
async def test_manifest_copy_forwards_measured_source_and_schema(
    monkeypatch,
    tmp_path,
) -> None:
    """Copy nonempty rows through the configured schema with exact progress."""

    copied_by_name = {}

    async def copy_to_table(table_name, **copy_arguments_by_name):
        copied_by_name["table_name"] = table_name
        copied_by_name.update(copy_arguments_by_name)
        assert copy_arguments_by_name["source"].read() == b"value"

    copy_path = tmp_path / "copy"
    copy_path.write_bytes(b"value")
    monkeypatch.setattr(manifest, "resolve_ptg2_schema", lambda: "tenant")
    monkeypatch.setattr(
        manifest.db,
        "acquire",
        lambda: _acquire_connection(_RawConnection(copy_to_table)),
    )
    observed_bytes: list[int] = []
    await manifest._copy_ptg2_manifest_file(
        copy_path,
        target_table="target",
        columns=["value"],
        progress_callback=observed_bytes.append,
    )
    assert copied_by_name["schema_name"] == "tenant"
    assert copied_by_name["table_name"] == "target"
    assert observed_bytes == [5]


def test_duplicate_and_provider_inverted_detection_cover_nested_shapes() -> None:
    """Recognize nested driver duplicates and every supported sidecar envelope."""

    assert manifest._is_unique_index_duplicate(RuntimeError("unique violation"))
    plain_error = RuntimeError("ordinary")
    plain_error.orig = RuntimeError("could not create unique index: duplicated")
    assert manifest._is_unique_index_duplicate(plain_error)
    nested_error = RuntimeError("ordinary")
    nested_error.orig = RuntimeError("wrapper")
    nested_error.orig.__cause__ = RuntimeError("UniqueViolation")
    assert manifest._is_unique_index_duplicate(nested_error)
    assert not manifest._is_unique_index_duplicate(RuntimeError("ordinary"))

    direct_artifacts_by_name = {"provider_inverted": {"name": "direct"}}
    assert manifest._manifest_provider_inverted_entry(
        direct_artifacts_by_name
    ) == {"name": "direct"}
    listed_artifacts_by_name = {
        "sidecars": [
            "invalid",
            {"name": "other"},
            {"name": "provider_inverted", "path": "listed"},
        ]
    }
    assert manifest._manifest_provider_inverted_entry(
        listed_artifacts_by_name
    )["path"] == "listed"
    nested_artifacts_by_name = {
        "artifact": {"name": "provider_inverted", "path": "nested"}
    }
    assert manifest._manifest_provider_inverted_entry(
        None,
        nested_artifacts_by_name,
    )["path"] == "nested"
    assert manifest._manifest_provider_inverted_entry({}, {"artifact": "invalid"}) is None


def test_sidecar_paths_resolve_local_manifest_and_fallback(
    monkeypatch,
    tmp_path,
) -> None:
    """Prefer real sidecar locations and otherwise use deterministic storage."""

    absolute_path = tmp_path / "absolute"
    absolute_path.write_text("x")
    assert manifest._ptg2_manifest_publish_sidecar_path(
        {"path": str(absolute_path)},
        None,
    ) == absolute_path
    manifest_path = tmp_path / "manifest.jsonl"
    manifest_path.write_text("{}")
    relative_path = tmp_path / "relative"
    relative_path.write_text("x")
    assert manifest._ptg2_manifest_publish_sidecar_path(
        {"path": "relative"},
        {"manifest_uri": f"file://{manifest_path}"},
    ) == relative_path
    assert manifest._ptg2_manifest_publish_sidecar_path(
        {"path": "absent"},
        {"manifest_uri": f"file://{manifest_path}"},
    ) == Path("absent")

    assert manifest._ptg2_manifest_serving_sidecar_dir(
        artifacts={"direct": {"path": str(absolute_path)}},
        source_key="source",
        snapshot_id="snapshot",
    ) == tmp_path
    assert manifest._ptg2_manifest_serving_sidecar_dir(
        artifacts={"direct": "invalid", "sidecars": [{"path": str(relative_path)}]},
        source_key="source",
        snapshot_id="snapshot",
    ) == tmp_path
    fallback = tmp_path / "fallback"
    monkeypatch.setattr(manifest, "resolve_ptg2_artifact_dir", lambda: fallback)
    fallback_path = manifest._ptg2_manifest_serving_sidecar_dir(
        artifacts={"sidecars": ["invalid"]},
        source_key="source",
        snapshot_id="snapshot",
    )
    assert str(fallback_path).startswith(str(fallback / "serving"))


def test_v3_layout_guard_rejects_nonlean_serving(monkeypatch) -> None:
    """Require lean provider-key serving only for PostgreSQL V3 snapshots."""

    monkeypatch.setattr(manifest, "_ptg2_manifest_serving_layout", lambda: "wrong")
    with pytest.raises(RuntimeError, match="requires the lean"):
        manifest._require_v3_serving_layout(
            manifest.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3
        )
    manifest._require_v3_serving_layout("legacy")
