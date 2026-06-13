import zipfile

import pytest

from process.ext.archive import unzip


@pytest.mark.asyncio
async def test_unzip_extracts_archive(tmp_path):
    archive_path = tmp_path / "source.zip"
    target_dir = tmp_path / "target"

    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("nested/file.txt", "ok")

    await unzip(str(archive_path), str(target_dir))

    assert (target_dir / "nested" / "file.txt").read_text() == "ok"


@pytest.mark.asyncio
async def test_unzip_rejects_unsafe_member(tmp_path):
    archive_path = tmp_path / "unsafe.zip"

    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../outside.txt", "no")

    with pytest.raises(ValueError, match="Unsafe zip member path"):
        await unzip(str(archive_path), str(tmp_path / "target"))
