# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from process import provider_directory_retained_blob_producer as blob_producer
from process import provider_directory_retained_blob_install_support as install_io
from process.provider_directory_retained_artifact_contract import RetainedArtifactError
from process.provider_directory_retained_blob_producer import (
    install_retained_artifact_blob,
)
from process.provider_directory_retained_blob_store import (
    retained_artifact_blob_components,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def _producer_input(artifact_root: Path, label: str = "fault") -> tuple[Path, bytes, str]:
    artifact_bytes = f"producer-{label}\n".encode()
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = artifact_root / f"source-{label}"
    source_path.write_bytes(artifact_bytes)
    return source_path, artifact_bytes, artifact_sha256


@pytest.mark.parametrize("platform_gap", ("nofollow", "dir_fd"))
def test_producer_fails_closed_without_descriptor_relative_platform_support(
    monkeypatch,
    retained_artifact_test_root: Path,
    platform_gap: str,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        platform_gap,
    )
    if platform_gap == "nofollow":
        monkeypatch.setattr(install_io, "_NOFOLLOW_FLAG", None)
    else:
        monkeypatch.setattr(install_io.os, "supports_dir_fd", set())
    monkeypatch.setattr(
        blob_producer,
        "_require_install_platform",
        install_io._require_install_platform,
    )

    with pytest.raises(RetainedArtifactError, match="install_unavailable"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )


@pytest.mark.parametrize(
    ("source_path_factory", "expected_code"),
    (
        (lambda artifact_root: "", "retained_blob_source_path_invalid"),
        (lambda artifact_root: "relative-source", "retained_blob_source_path_invalid"),
        (lambda artifact_root: b"/bytes-source", "retained_blob_source_path_invalid"),
        (lambda artifact_root: object(), "retained_blob_source_path_invalid"),
        (lambda artifact_root: artifact_root, "retained_blob_source_path_unsafe"),
    ),
)
def test_producer_rejects_invalid_or_non_file_source_paths(
    retained_artifact_test_root: Path,
    source_path_factory,
    expected_code: str,
) -> None:
    artifact_bytes = b"invalid source\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = source_path_factory(retained_artifact_test_root)

    with pytest.raises(RetainedArtifactError) as raised:
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert str(raised.value) == expected_code
    artifact_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    assert not artifact_path.exists()
    assert list(retained_artifact_test_root.iterdir()) == []


class _FailingSourcePath:
    """Expose a valid path protocol whose implementation fails."""

    def __init__(self, error_type: type[Exception]) -> None:
        self.error_type = error_type

    def __fspath__(self) -> str:
        raise self.error_type("fixture source path conversion failure")


@pytest.mark.parametrize("error_type", (OSError, ValueError))
def test_public_pathlike_conversion_failure_is_typed_without_descriptors(
    monkeypatch,
    retained_artifact_test_root: Path,
    error_type: type[Exception],
) -> None:
    artifact_bytes = b"pathlike conversion failure\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    open_calls: list[tuple[tuple, dict]] = []
    close_calls: list[int] = []
    original_open = install_io.os.open
    original_close = install_io.os.close

    def record_open(*args, **kwargs):
        open_calls.append((args, kwargs))
        return original_open(*args, **kwargs)

    def record_close(descriptor: int) -> None:
        close_calls.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(install_io.os, "open", record_open)
    monkeypatch.setattr(install_io.os, "close", record_close)
    with pytest.raises(RetainedArtifactError) as raised:
        install_retained_artifact_blob(
            _FailingSourcePath(error_type),
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert str(raised.value) == "retained_blob_source_path_unsafe"
    assert isinstance(raised.value.__cause__, error_type)
    assert open_calls == []
    assert close_calls == []
    assert list(retained_artifact_test_root.iterdir()) == []


def test_public_embedded_nul_is_typed_without_descriptors(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"embedded nul path failure\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = f"{retained_artifact_test_root}/source\x00suffix"
    open_calls: list[tuple[tuple, dict]] = []
    close_calls: list[int] = []
    original_open = install_io.os.open
    original_close = install_io.os.close

    def record_open(*args, **kwargs):
        open_calls.append((args, kwargs))
        return original_open(*args, **kwargs)

    def record_close(descriptor: int) -> None:
        close_calls.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(install_io.os, "open", record_open)
    monkeypatch.setattr(install_io.os, "close", record_close)
    with pytest.raises(RetainedArtifactError) as raised:
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert str(raised.value) == "retained_blob_source_path_unsafe"
    assert raised.value.__cause__ is None
    assert open_calls == []
    assert close_calls == []
    assert list(retained_artifact_test_root.iterdir()) == []


def test_public_source_open_value_error_closes_every_directory_descriptor(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "open-value-error",
    )
    artifact_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    original_open = install_io.os.open
    original_close = install_io.os.close
    opened_descriptors: list[int] = []
    closed_descriptors: list[int] = []

    def open_directories_then_fail(path, *args, **kwargs):
        if path == source_path.name:
            raise ValueError("fixture source open value error")
        descriptor = original_open(path, *args, **kwargs)
        opened_descriptors.append(descriptor)
        return descriptor

    def record_close(descriptor: int) -> None:
        closed_descriptors.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(install_io.os, "open", open_directories_then_fail)
    monkeypatch.setattr(install_io.os, "close", record_close)
    with pytest.raises(RetainedArtifactError) as raised:
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert str(raised.value) == "retained_blob_source_path_unsafe"
    assert isinstance(raised.value.__cause__, ValueError)
    assert opened_descriptors
    assert closed_descriptors == list(reversed(opened_descriptors))
    for descriptor in opened_descriptors:
        with pytest.raises(OSError):
            install_io.os.fstat(descriptor)
    assert getattr(raised.value, "__notes__", ()) == ()
    assert not artifact_path.exists()
    assert list(retained_artifact_test_root.iterdir()) == [source_path]


def test_opened_producer_source_cleanup_is_idempotent(
    retained_artifact_test_root: Path,
) -> None:
    source_path, _artifact_bytes, _artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "idempotent-cleanup",
    )
    opened_source = install_io._open_source(source_path)
    opened_source.close()
    opened_source.close()


@pytest.mark.parametrize(
    ("failure_kind", "expected_code"),
    (
        ("mkdir", "path_unsafe"),
        ("fsync", "durability_failed"),
        ("temporary_open", "install_unavailable"),
        ("pread", "install_failed"),
        ("write", "install_failed"),
        ("link", "publish_failed"),
    ),
)
def test_producer_wraps_install_io_failures(
    monkeypatch,
    retained_artifact_test_root: Path,
    failure_kind: str,
    expected_code: str,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        failure_kind,
    )
    failure_by_kind = {
        "mkdir": ("mkdir", lambda *args, **kwargs: (_ for _ in ()).throw(OSError())),
        "fsync": ("fsync", lambda *args, **kwargs: (_ for _ in ()).throw(OSError())),
        "pread": ("pread", lambda *args, **kwargs: (_ for _ in ()).throw(OSError())),
        "write": ("write", lambda *args, **kwargs: 0),
        "link": ("link", lambda *args, **kwargs: (_ for _ in ()).throw(OSError())),
    }
    if failure_kind == "temporary_open":
        monkeypatch.setattr(
            blob_producer,
            "_create_temporary_blob",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                RetainedArtifactError("retained_blob_install_unavailable")
            ),
        )
    else:
        operation_name, failure = failure_by_kind[failure_kind]
        operation_module = blob_producer.os if failure_kind == "link" else install_io.os
        monkeypatch.setattr(operation_module, operation_name, failure)

    with pytest.raises(RetainedArtifactError, match=expected_code):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )


def test_producer_wraps_unexpected_publish_oserror(monkeypatch) -> None:
    monkeypatch.setattr(
        blob_producer,
        "_link_open_descriptor",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError()),
    )
    temporary = SimpleNamespace(descriptor=11)
    target = SimpleNamespace(
        directory_chain=SimpleNamespace(parent_descriptor=12),
        leaf_name="canonical",
    )

    with pytest.raises(RetainedArtifactError, match="publish_failed"):
        blob_producer._publish_temporary_blob(temporary, target)


def test_producer_accepts_concurrent_identical_publish(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "concurrent",
    )
    original_link = blob_producer.os.link

    def publish_then_report_existing(*args, **kwargs):
        original_link(*args, **kwargs)
        raise FileExistsError("fixture concurrent publisher")

    monkeypatch.setattr(blob_producer.os, "link", publish_then_report_existing)
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )
    blob_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    assert blob_path.read_bytes() == artifact_bytes


def test_producer_rejects_canonical_leaf_symlink(
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "leaf-symlink",
    )
    blob_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    blob_path.parent.mkdir(parents=True)
    blob_path.symlink_to(source_path)

    with pytest.raises(RetainedArtifactError, match="blob_path_unsafe"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )


def test_producer_detects_source_replacement_during_copy(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "source-replacement",
    )
    original_copy = install_io._copy_verified_source

    def copy_then_replace(*args, **kwargs):
        source_identity = original_copy(*args, **kwargs)
        replacement_path = source_path.with_suffix(".replacement")
        replacement_path.write_bytes(artifact_bytes)
        os.replace(replacement_path, source_path)
        return source_identity

    monkeypatch.setattr(install_io, "_copy_verified_source", copy_then_replace)
    with pytest.raises(RetainedArtifactError, match="source_identity_changed"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
