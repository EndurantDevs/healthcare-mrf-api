# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Completed price COPY families release scratch after their final consumer."""

import asyncio
import importlib

import pytest

ptg = importlib.import_module("process.ptg")
COPY_KINDS = ("price_atom", "price_set_atom", "price_set_summary")


def _price_copy_fixture(tmp_path, *, shared=False):
    paths_by_kind = {}
    for index, kind in enumerate(COPY_KINDS):
        path = tmp_path / f"{kind}.copy"
        path.write_bytes(bytes([index + 1]) * (4096 << index))
        paths_by_kind[kind] = [path]
    if shared:
        shared_path = tmp_path / "shared.copy"
        shared_path.write_bytes(b"s" * 1024)
        paths_by_kind[COPY_KINDS[0]].append(shared_path)
        paths_by_kind[COPY_KINDS[-1]].append(shared_path)
    sidecar_path = tmp_path / "provider_runs.bin"
    sidecar_path.write_bytes(b"retained downstream input")
    return paths_by_kind, sidecar_path


def _source_summaries(paths_by_kind):
    return [{"summary": {"manifest": {"copy_files": {
        kind: [{"path": str(path), "row_count": 1} for path in paths]
        for kind, paths in paths_by_kind.items()
    }}}}]


def _remaining_bytes(paths):
    return sum(path.stat().st_size for path in paths if path.exists())


def _install_copy(monkeypatch, copy_files):
    monkeypatch.setattr(ptg, "_ptg2_snapshot_arch_from_env", lambda: "postgres_binary_v3")
    monkeypatch.setattr(ptg, "_copy_manifest_files_direct_with_progress", copy_files)
    monkeypatch.setattr(ptg, "_emit_screen_line", lambda _line: None)


async def _merge(paths_by_kind):
    return await ptg._merge_ptg2_manifest_files(
        successful_files=_source_summaries(paths_by_kind),
        manifest_stage_table="manifest_stage",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("shared", [False, True])
async def test_price_copy_releases_completed_families(tmp_path, monkeypatch, shared):
    paths_by_kind, sidecar_path = _price_copy_fixture(tmp_path, shared=shared)
    all_paths = {path for paths in paths_by_kind.values() for path in paths}
    retained_bytes = []
    copied_bytes_by_kind = {}

    async def copy_files(kind, *, input_paths, emitted_rows, **_kwargs):
        retained_bytes.append(_remaining_bytes(all_paths))
        copied_bytes_by_kind[kind] = sum(len(path.read_bytes()) for path in input_paths)
        return {"input_bytes": copied_bytes_by_kind[kind], "input_rows": emitted_rows}

    _install_copy(monkeypatch, copy_files)
    result = await _merge(paths_by_kind)

    extra_bytes = 1024 if shared else 0
    assert retained_bytes == [28672 + extra_bytes, 24576 + extra_bytes, 16384 + extra_bytes]
    assert copied_bytes_by_kind == {
        "price_atom": 4096 + extra_bytes,
        "price_set_atom": 8192,
        "price_set_summary": 16384 + extra_bytes,
    }
    assert result["kinds"] == {
        kind: {"input_bytes": size, "input_rows": len(paths_by_kind[kind])}
        for kind, size in copied_bytes_by_kind.items()
    }
    assert _remaining_bytes(all_paths) == 0
    assert sidecar_path.read_bytes() == b"retained downstream input"
    ptg._cleanup_manifest_copy_paths(paths_by_kind)


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_price_copy_failure_keeps_active_inputs_until_cleanup(
    tmp_path, monkeypatch, error_type,
):
    paths_by_kind, sidecar_path = _price_copy_fixture(tmp_path, shared=True)
    all_paths = {path for paths in paths_by_kind.values() for path in paths}
    failure_byte_samples = []
    failure = error_type("injected price COPY failure")

    async def copy_files(kind, *, input_paths, **_kwargs):
        assert all(path.read_bytes() for path in input_paths)
        if kind == "price_set_atom":
            failure_byte_samples.append(_remaining_bytes(all_paths))
            raise failure
        return {"input_bytes": sum(path.stat().st_size for path in input_paths)}

    _install_copy(monkeypatch, copy_files)
    with pytest.raises(error_type) as raised:
        await _merge(paths_by_kind)

    assert raised.value is failure
    assert failure_byte_samples == [25600]
    assert not any(path.exists() for path in all_paths)
    assert sidecar_path.exists()


@pytest.mark.asyncio
async def test_price_copy_caller_cancellation_finishes_before_cleanup(tmp_path, monkeypatch):
    paths_by_kind, sidecar_path = _price_copy_fixture(tmp_path, shared=True)
    all_paths = {path for paths in paths_by_kind.values() for path in paths}
    second_started = asyncio.Event()
    release_second = asyncio.Event()
    cleanup_byte_samples = []

    async def copy_files(kind, *, input_paths, **_kwargs):
        assert all(path.read_bytes() for path in input_paths)
        if kind == "price_set_atom":
            second_started.set()
            try:
                await release_second.wait()
            finally:
                await asyncio.sleep(0)
                cleanup_byte_samples.append(_remaining_bytes(all_paths))
        return {}

    _install_copy(monkeypatch, copy_files)
    copy_task = asyncio.create_task(_merge(paths_by_kind))
    try:
        await asyncio.wait_for(second_started.wait(), timeout=1)
        copy_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await copy_task
        assert cleanup_byte_samples == [25600]
        assert not any(path.exists() for path in all_paths)
        assert sidecar_path.exists()
    finally:
        release_second.set()
        await asyncio.gather(copy_task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("alias_kind", ["parent", "symlink", "empty_worker"])
async def test_price_copy_preserves_later_family_aliases(tmp_path, monkeypatch, alias_kind):
    paths_by_kind, _sidecar_path = _price_copy_fixture(tmp_path)
    first_path = paths_by_kind["price_atom"][0]
    if alias_kind == "parent":
        alias_dir = tmp_path / "alias"
        alias_dir.mkdir()
        later_path = alias_dir / ".." / first_path.name
    elif alias_kind == "symlink":
        later_path = tmp_path / "alias.copy"
        later_path.symlink_to(first_path)
    else:
        later_path = tmp_path / f"{first_path.name}.worker1"
        later_path.write_bytes(b"")
    paths_by_kind["price_set_summary"].append(later_path)
    copied_later_inputs = []

    async def copy_files(kind, *, input_paths, **_kwargs):
        for path in input_paths:
            data = path.read_bytes()
            if kind == "price_set_summary" and path == later_path:
                copied_later_inputs.append(data)
        return {}

    _install_copy(monkeypatch, copy_files)
    await _merge(paths_by_kind)

    assert copied_later_inputs == [b"" if alias_kind == "empty_worker" else b"\x01" * 4096]
    assert not first_path.exists()
    assert not later_path.is_symlink()
    assert not later_path.exists()
