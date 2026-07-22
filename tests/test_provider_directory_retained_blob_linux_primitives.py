# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from process import provider_directory_retained_blob_install_support as install_io
from process import provider_directory_retained_blob_producer as blob_producer
from process.provider_directory_retained_artifact_contract import RetainedArtifactError


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def _mock_supported_linux(monkeypatch) -> None:
    monkeypatch.setattr(install_io.sys, "platform", "linux")
    monkeypatch.setattr(install_io, "_TMPFILE_FLAG", 0x40000000)
    monkeypatch.setattr(
        install_io.os,
        "supports_dir_fd",
        set(install_io._DIR_FD_OPERATIONS),
    )
    monkeypatch.setattr(
        install_io.os,
        "supports_follow_symlinks",
        {install_io.os.link},
    )
    monkeypatch.setattr(
        install_io.os,
        "stat",
        lambda *args, **kwargs: SimpleNamespace(st_mode=stat.S_IFDIR),
    )


def test_linux_publication_capability_probe_accepts_complete_platform(
    monkeypatch,
) -> None:
    _mock_supported_linux(monkeypatch)
    install_io._require_install_platform()


@pytest.mark.parametrize(
    "missing_capability",
    ("platform", "tmpfile", "follow", "proc_missing", "proc_not_directory"),
)
def test_linux_publication_capability_probe_fails_closed(
    monkeypatch,
    missing_capability: str,
) -> None:
    _mock_supported_linux(monkeypatch)
    if missing_capability == "platform":
        monkeypatch.setattr(install_io.sys, "platform", "darwin")
    elif missing_capability == "tmpfile":
        monkeypatch.setattr(install_io, "_TMPFILE_FLAG", None)
    elif missing_capability == "follow":
        monkeypatch.setattr(install_io.os, "supports_follow_symlinks", set())
    elif missing_capability == "proc_missing":
        monkeypatch.setattr(
            install_io.os,
            "stat",
            lambda *args, **kwargs: (_ for _ in ()).throw(OSError()),
        )
    else:
        monkeypatch.setattr(
            install_io.os,
            "stat",
            lambda *args, **kwargs: SimpleNamespace(st_mode=stat.S_IFREG),
        )
    with pytest.raises(RetainedArtifactError, match="install_unavailable"):
        install_io._require_install_platform()


def test_procfd_link_uses_open_descriptor_and_no_source_path(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path = retained_artifact_test_root / "procfd-source"
    source_path.write_bytes(b"procfd")
    source_descriptor = os.open(source_path, os.O_RDONLY)
    link_call_by_field = {}

    def record_link(source_pathname, target_name, **kwargs) -> None:
        link_call_by_field.update(
            source_pathname=source_pathname,
            target_name=target_name,
            kwargs=kwargs,
        )

    monkeypatch.setattr(install_io.os, "link", record_link)
    try:
        install_io._link_open_descriptor(source_descriptor, 123, "canonical")
    finally:
        os.close(source_descriptor)
    assert link_call_by_field == {
        "source_pathname": f"/proc/self/fd/{source_descriptor}",
        "target_name": "canonical",
        "kwargs": {"dst_dir_fd": 123, "follow_symlinks": True},
    }


@pytest.mark.parametrize(
    ("failure_operation", "expected_code"),
    (
        ("fstat", "publish_identity_unavailable"),
        ("link", "publish_failed"),
    ),
)
def test_procfd_link_failures_are_typed(
    monkeypatch,
    retained_artifact_test_root: Path,
    failure_operation: str,
    expected_code: str,
) -> None:
    source_path = retained_artifact_test_root / f"procfd-{failure_operation}"
    source_path.write_bytes(b"procfd")
    source_descriptor = os.open(source_path, os.O_RDONLY)
    original_operation = getattr(install_io.os, failure_operation)
    monkeypatch.setattr(
        install_io.os,
        failure_operation,
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError()),
    )
    try:
        with pytest.raises(RetainedArtifactError, match=expected_code):
            install_io._link_open_descriptor(source_descriptor, 123, "canonical")
    finally:
        monkeypatch.setattr(
            install_io.os,
            failure_operation,
            original_operation,
        )
        os.close(source_descriptor)


def test_public_procfd_link_oserror_is_publish_failed(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"public procfd link failure\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "procfd-public-source"
    source_path.write_bytes(artifact_bytes)
    canonical_path = retained_artifact_test_root.joinpath(
        *install_io.retained_artifact_blob_components(artifact_sha256)
    )

    def fail_link(*args, **kwargs):
        raise OSError("fixture procfd link failure")

    monkeypatch.setattr(blob_producer, "_require_install_platform", lambda: None)
    monkeypatch.setattr(
        blob_producer,
        "_link_open_descriptor",
        install_io._link_open_descriptor,
    )
    monkeypatch.setattr(install_io.os, "link", fail_link)
    with pytest.raises(RetainedArtifactError) as raised:
        blob_producer.install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert str(raised.value) == "retained_blob_publish_failed"
    assert not canonical_path.exists()


def test_procfd_link_preserves_file_exists(monkeypatch, retained_artifact_test_root):
    source_path = retained_artifact_test_root / "procfd-existing"
    source_path.write_bytes(b"procfd")
    source_descriptor = os.open(source_path, os.O_RDONLY)
    monkeypatch.setattr(
        install_io.os,
        "link",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileExistsError()),
    )
    try:
        with pytest.raises(FileExistsError):
            install_io._link_open_descriptor(source_descriptor, 123, "canonical")
    finally:
        os.close(source_descriptor)


def test_publisher_preserves_typed_primitive_error(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_sha256 = hashlib.sha256(b"typed-publish").hexdigest()
    target = install_io._open_install_target(artifact_sha256)
    temporary = install_io._TemporaryBlob(descriptor=-1)

    def fail_typed(*args, **kwargs):
        raise RetainedArtifactError("retained_blob_install_unavailable")

    monkeypatch.setattr(blob_producer, "_link_open_descriptor", fail_typed)
    try:
        with pytest.raises(RetainedArtifactError, match="install_unavailable"):
            blob_producer._publish_temporary_blob(temporary, target)
    finally:
        target.close()


def test_cleanup_note_combination_flattens_nested_notes() -> None:
    primary_error = RetainedArtifactError("retained_blob_source_mismatch")
    cleanup_error = RetainedArtifactError("retained_blob_directory_close_failed")
    cleanup_error.add_note("cleanup failure: retained_blob_target_close_failed")
    blob_producer._combine_cleanup_errors(primary_error, [cleanup_error])
    assert getattr(primary_error, "__notes__", ()) == [
        "cleanup failure: retained_blob_directory_close_failed",
        "cleanup failure: retained_blob_target_close_failed",
    ]


def test_otmpfile_creation_uses_target_directory_and_wraps_failure(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_sha256 = hashlib.sha256(b"otmpfile").hexdigest()
    install_target = install_io._open_install_target(artifact_sha256)
    original_open = install_io.os.open
    returned_descriptor = original_open("/dev/null", os.O_RDWR)
    open_call_by_field = {}

    def record_open(path, flags, mode=0o777, **kwargs):
        open_call_by_field.update(path=path, flags=flags, mode=mode, kwargs=kwargs)
        return returned_descriptor

    monkeypatch.setattr(install_io, "_TMPFILE_FLAG", 0x40000000)
    monkeypatch.setattr(install_io.os, "open", record_open)
    temporary = install_io._create_temporary_blob(
        install_target,
        artifact_sha256,
    )
    assert temporary.descriptor == returned_descriptor
    assert open_call_by_field["path"] == "."
    assert open_call_by_field["flags"] & 0x40000000
    assert open_call_by_field["kwargs"] == {
        "dir_fd": install_target.directory_chain.parent_descriptor
    }
    monkeypatch.setattr(install_io.os, "open", original_open)
    temporary.close()

    monkeypatch.setattr(
        install_io.os,
        "open",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError()),
    )
    with pytest.raises(RetainedArtifactError, match="install_unavailable"):
        install_io._create_temporary_blob(install_target, artifact_sha256)
    monkeypatch.setattr(install_io.os, "open", original_open)
    install_target.close()


def test_otmpfile_creation_fails_closed_without_flag(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_sha256 = hashlib.sha256(b"otmpfile-missing").hexdigest()
    target = install_io._open_install_target(artifact_sha256)
    monkeypatch.setattr(install_io, "_TMPFILE_FLAG", None)
    try:
        with pytest.raises(RetainedArtifactError, match="install_unavailable"):
            install_io._create_temporary_blob(target, artifact_sha256)
    finally:
        target.close()
