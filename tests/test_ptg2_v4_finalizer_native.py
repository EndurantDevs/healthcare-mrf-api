from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_v4_finalizer_native as native
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)


def _summary(source_directory: Path) -> dict[str, object]:
    price = source_directory / "price.copy"
    serving = source_directory / "serving.copy"
    price.write_bytes(b"price")
    serving.write_bytes(b"serving")
    return {
        "output_directory": str(source_directory),
        "blocks": {
            "price_dictionary": {
                "path": price.name,
                "copy_bytes": price.stat().st_size,
                "copy_sha256": "1" * 64,
                "row_count": 1,
                "stored_payload_bytes": 1,
                "artifact_record_counts": {
                    "by_code_price_dictionary": 1,
                },
            },
            "serving": {
                "path": serving.name,
                "copy_bytes": serving.stat().st_size,
                "copy_sha256": "2" * 64,
                "row_count": 5,
                "stored_payload_bytes": 5,
                "artifact_record_counts": {
                    kind: 1
                    for kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
                    if kind != "by_code_price_dictionary"
                },
            },
        },
    }


@pytest.mark.asyncio
async def test_native_pack_cancellation_removes_unacknowledged_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    work_directory = tmp_path / "work"
    work_directory.mkdir()
    output_directory = work_directory / "v4-finalizer-packed"

    class Process:
        pid = 731
        returncode = 0

        async def communicate(self):
            output_directory.mkdir()
            (output_directory / "unclaimed").write_bytes(b"partial")
            raise asyncio.CancelledError

    async def spawn(*_args, **_kwargs):
        return Process()

    monkeypatch.setattr(native, "_ptg2_rust_scanner_binary", lambda: Path("scanner"))
    monkeypatch.setattr(
        native,
        "_load_v3_finalizer_resource_configuration",
        lambda: SimpleNamespace(identity_map_max_bytes=1024),
    )
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    terminate = AsyncMock()
    monkeypatch.setattr(native, "_terminate_asyncio_subprocess_group", terminate)

    with pytest.raises(asyncio.CancelledError):
        await native.pack_v4_finalizer_copies(
            _summary(source_directory),
            work_directory=work_directory,
        )

    terminate.assert_awaited_once()
    assert not output_directory.exists()
    assert not (work_directory / "v4-finalizer-pack-input.json").exists()


@pytest.mark.asyncio
async def test_native_pack_configuration_failure_creates_no_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    work_directory = tmp_path / "work"
    work_directory.mkdir()
    monkeypatch.setattr(native, "_ptg2_rust_scanner_binary", lambda: Path("scanner"))

    def invalid_configuration():
        raise RuntimeError("invalid resource configuration")

    monkeypatch.setattr(
        native,
        "_load_v3_finalizer_resource_configuration",
        invalid_configuration,
    )

    with pytest.raises(RuntimeError, match="invalid resource configuration"):
        await native.pack_v4_finalizer_copies(
            _summary(source_directory),
            work_directory=work_directory,
        )

    assert list(work_directory.iterdir()) == []


@pytest.mark.asyncio
async def test_native_pack_manifest_write_failure_removes_partial_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_directory = tmp_path / "source"
    source_directory.mkdir()
    work_directory = tmp_path / "work"
    work_directory.mkdir()
    monkeypatch.setattr(native, "_ptg2_rust_scanner_binary", lambda: Path("scanner"))
    monkeypatch.setattr(
        native,
        "_load_v3_finalizer_resource_configuration",
        lambda: SimpleNamespace(identity_map_max_bytes=1024),
    )
    original_write_text = Path.write_text

    def fail_manifest_write(path: Path, *args, **kwargs):
        if path.name == "v4-finalizer-pack-input.json":
            path.write_bytes(b"{")
            raise OSError("manifest write failed")
        return original_write_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", fail_manifest_write)
    with pytest.raises(OSError, match="manifest write failed"):
        await native.pack_v4_finalizer_copies(
            _summary(source_directory),
            work_directory=work_directory,
        )

    assert list(work_directory.iterdir()) == []
