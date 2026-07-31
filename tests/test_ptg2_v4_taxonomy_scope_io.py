# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import importlib
import struct
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as publication


ptg = importlib.import_module("process.ptg")


def _copy_spec(payload: bytes) -> publication._V4CompilerCopySpec:
    return publication._V4CompilerCopySpec(
        schema_name="pg_temp",
        stage_table="scope_stage",
        columns=("npi_key", "npi"),
        expected_byte_count=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        label="NPI scope prepass",
    )


def test_scope_copy_opens_the_authenticated_regular_descriptor(tmp_path) -> None:
    payload = b"authenticated-copy"
    copy_path = tmp_path / "scope.copy"
    copy_path.write_bytes(payload)

    with publication._authenticated_v4_copy_file(
        copy_path,
        _copy_spec(payload),
    ) as (copy_file, _metadata):
        copy_file.seek(0)
        assert copy_file.read() == payload


def test_scope_copy_refuses_a_symlink_without_opening_its_target(
    tmp_path,
) -> None:
    payload = b"authenticated-copy"
    target_path = tmp_path / "target.copy"
    target_path.write_bytes(payload)
    copy_path = tmp_path / "scope.copy"
    copy_path.symlink_to(target_path)

    with pytest.raises(RuntimeError, match="NPI scope prepass is unavailable"):
        with publication._authenticated_v4_copy_file(
            copy_path,
            _copy_spec(payload),
        ):
            raise AssertionError("unreachable")

    assert target_path.read_bytes() == payload


def test_scope_copy_refuses_bytes_that_differ_from_the_seal(tmp_path) -> None:
    expected_payload = b"expected-copy"
    copy_path = tmp_path / "scope.copy"
    copy_path.write_bytes(b"changed-copy!")

    with pytest.raises(RuntimeError, match="changed at COPY open"):
        with publication._authenticated_v4_copy_file(
            copy_path,
            _copy_spec(expected_payload),
        ):
            raise AssertionError("unreachable")


def test_scope_manifest_binds_binary_rows_to_the_reciprocal_graph(
    tmp_path,
) -> None:
    scope_path = tmp_path / "scope.copy"
    scope_payload = bytearray(ptg._PTG2_PG_BINARY_COPY_HEADER)
    for npi in (1003002106, 1003007311):
        scope_payload.extend(struct.pack(">hiq", 1, 8, npi))
    scope_payload.extend(struct.pack(">h", -1))
    scope_path.write_bytes(scope_payload)
    reciprocal_by_field = {
        "record_format": ptg.PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
        "sha256": "a" * 64,
        "byte_count": 41,
        "owner_count": 2,
        "member_count": 3,
        "member_global_count": 3,
    }

    artifact_by_field = ptg._validated_provider_npi_scope_artifact(
        scope_path,
        summary={
            "provider_npi_scope_copy_path": str(scope_path),
            "provider_npi_scope_copy_format": (ptg._PTG2_PROVIDER_NPI_SCOPE_FORMAT),
            "provider_npi_scope_copy_rows": 2,
            "provider_npi_scope_copy_bytes": len(scope_payload),
            "provider_npi_group_bytes": 41,
        },
        provider_npi_group=reciprocal_by_field,
    )

    assert artifact_by_field["row_count"] == 2
    assert artifact_by_field["provider_npi_group_sha256"] == "a" * 64
    assert len(artifact_by_field["binding_sha256"]) == 64


def test_taxonomy_boundaries_emit_exact_terminal_counters(tmp_path) -> None:
    progress_events: list[tuple[str, dict[str, int]]] = []

    def capture_progress(stage: str, counters) -> None:
        progress_events.append((stage, dict(counters)))

    publication._taxonomy_input_complete(
        {
            "members": {"byte_count": 28},
            "rules": [
                {"member_count": 2},
                {"member_count": 5},
            ],
        },
        capture_progress,
    )
    publication._taxonomy_copy_complete(
        SimpleNamespace(
            output_artifacts=(
                SimpleNamespace(
                    name="inferred_taxonomy_candidates",
                    path=tmp_path / "selected.copy",
                    byte_count=88,
                    sha256="a" * 64,
                    row_count=7,
                ),
            )
        ),
        capture_progress,
    )

    assert progress_events == [
        (
            "taxonomy input preparation",
            {
                "completed_rows": 7,
                "completed_bytes": 28,
                "completed_batches": 1,
            },
        ),
        (
            "selected taxonomy copy publication",
            {
                "published_rows": 7,
                "published_bytes": 88,
                "completed_batches": 1,
            },
        ),
    ]


def _install_graph_input_preparation(monkeypatch, cleanup_calls):
    async def prepare_scope(*, output_path, **_kwargs):
        output_path.write_bytes(b"scope")
        return SimpleNamespace(
            graph_artifact_entries=(),
            cleanup=lambda: cleanup_calls.append("scope"),
        )

    async def prepare_taxonomy(_scope, *, work_directory, **_kwargs):
        member_path = work_directory / "taxonomy-members.copy"
        member_path.write_bytes(b"taxonomy")
        return {"members": {"path": str(member_path)}}

    monkeypatch.setattr(
        publication,
        "prepare_provider_graph_v4_npi_scope_rust",
        prepare_scope,
    )
    monkeypatch.setattr(
        publication,
        "_prepare_v4_taxonomy_compiler_input",
        prepare_taxonomy,
    )


async def _touch_build() -> None:
    return None


@pytest.mark.asyncio
async def test_graph_compile_failure_removes_all_run_owned_inputs(
    monkeypatch,
    tmp_path,
) -> None:
    cleanup_calls: list[str] = []
    _install_graph_input_preparation(monkeypatch, cleanup_calls)

    async def fail_compile(**_kwargs):
        raise RuntimeError("compile failed")

    monkeypatch.setattr(
        publication,
        "compile_provider_graph_v4_rust",
        fail_compile,
    )
    with pytest.raises(RuntimeError, match="compile failed"):
        await publication._compile_v4_provider_graph(
            graph_artifact_entries=(),
            provider_set_key_map_path=tmp_path / "provider-sets.copy",
            work_directory=tmp_path,
            schema_name="mrf",
            touch_build=_touch_build,
            progress_callback=None,
        )

    assert cleanup_calls == ["scope"]
    assert not tuple(tmp_path.glob("provider-graph-v4-input-*"))


@pytest.mark.asyncio
async def test_graph_compile_cancellation_removes_all_run_owned_inputs(
    monkeypatch,
    tmp_path,
) -> None:
    cleanup_calls: list[str] = []
    compile_started = asyncio.Event()
    compile_cancelled = asyncio.Event()
    _install_graph_input_preparation(monkeypatch, cleanup_calls)

    async def wait_for_cancel(**_kwargs):
        compile_started.set()
        try:
            await asyncio.Future()
        finally:
            compile_cancelled.set()

    monkeypatch.setattr(
        publication,
        "compile_provider_graph_v4_rust",
        wait_for_cancel,
    )
    compile_task = asyncio.create_task(
        publication._compile_v4_provider_graph(
            graph_artifact_entries=(),
            provider_set_key_map_path=tmp_path / "provider-sets.copy",
            work_directory=tmp_path,
            schema_name="mrf",
            touch_build=_touch_build,
            progress_callback=None,
        )
    )
    await compile_started.wait()
    compile_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await compile_task

    assert compile_cancelled.is_set()
    assert cleanup_calls == ["scope"]
    assert not tuple(tmp_path.glob("provider-graph-v4-input-*"))
