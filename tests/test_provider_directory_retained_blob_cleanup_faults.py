# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from process import provider_directory_retained_blob_install_support as install_io
from process import provider_directory_retained_blob_producer as blob_producer
from process.provider_directory_retained_artifact_contract import RetainedArtifactError
from process.provider_directory_retained_blob_producer import (
    install_retained_artifact_blob,
)
from process.provider_directory_retained_blob_store import (
    retained_artifact_blob_components,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def _opened_target(label: str) -> install_io._BlobInstallTarget:
    artifact_sha256 = hashlib.sha256(label.encode()).hexdigest()
    return install_io._open_install_target(artifact_sha256)


@pytest.mark.parametrize("failure_mode", ("one_shot", "persistent"))
def test_public_close_failure_on_real_unnamed_partial_allows_retry(
    monkeypatch,
    retained_artifact_test_root: Path,
    failure_mode: str,
) -> None:
    artifact_bytes = f"close-{failure_mode}\n".encode()
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / f"source-{failure_mode}"
    source_path.write_bytes(artifact_bytes)
    original_create = blob_producer._create_temporary_blob
    original_close = install_io.os.close
    temporary_by_field = {}
    close_state_by_field = {"has_failed": False, "was_really_closed": False}

    def create_and_record(*args, **kwargs):
        temporary = original_create(*args, **kwargs)
        assert os.fstat(temporary.descriptor).st_nlink == 0
        temporary_by_field["value"] = temporary
        temporary_by_field["descriptor"] = temporary.descriptor
        return temporary

    def fail_temporary_close(descriptor: int) -> None:
        temporary_descriptor = temporary_by_field.get("descriptor")
        if descriptor == temporary_descriptor:
            if failure_mode == "persistent" or not close_state_by_field["has_failed"]:
                close_state_by_field["has_failed"] = True
                if failure_mode == "one_shot":
                    original_close(descriptor)
                    close_state_by_field["was_really_closed"] = True
                raise OSError("fixture temporary close failure")
        original_close(descriptor)

    monkeypatch.setattr(blob_producer, "_create_temporary_blob", create_and_record)
    monkeypatch.setattr(install_io.os, "close", fail_temporary_close)
    with pytest.raises(RetainedArtifactError, match="temporary_close_failed"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    blob_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    assert blob_path.read_bytes() == artifact_bytes
    assert list(blob_path.parent.glob("*.partial")) == []

    monkeypatch.setattr(install_io.os, "close", original_close)
    if not close_state_by_field["was_really_closed"]:
        original_close(temporary_by_field["descriptor"])
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )


def test_outer_close_failures_are_notes_and_do_not_mask_primary(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"outer-close-primary\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "source-outer-close"
    source_path.write_bytes(artifact_bytes)
    original_source_close = install_io._OpenedFile.close
    original_target_close = install_io._BlobInstallTarget.close
    close_events: list[str] = []

    def fail_copy(*args, **kwargs):
        raise RetainedArtifactError("retained_blob_source_mismatch")

    def close_target_then_fail(target) -> None:
        original_target_close(target)
        close_events.append("target")
        raise RetainedArtifactError("retained_blob_target_close_failed")

    def close_source_then_fail(source) -> None:
        original_source_close(source)
        close_events.append("source")
        raise RetainedArtifactError("retained_blob_source_close_failed")

    monkeypatch.setattr(install_io, "_copy_verified_source", fail_copy)
    monkeypatch.setattr(install_io._BlobInstallTarget, "close", close_target_then_fail)
    monkeypatch.setattr(install_io._OpenedFile, "close", close_source_then_fail)
    with pytest.raises(RetainedArtifactError, match="source_mismatch") as raised:
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert close_events == ["target", "source"]
    notes = getattr(raised.value, "__notes__", ())
    assert any("target_close_failed" in note for note in notes)
    assert any("source_close_failed" in note for note in notes)


def test_persistent_temporary_close_failure_is_note_under_copy_primary(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"temporary-close-primary\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "source-temporary-close-primary"
    source_path.write_bytes(artifact_bytes)
    original_create = blob_producer._create_temporary_blob
    original_close = install_io.os.close
    temporary_by_field = {}

    def create_with_failing_close(*args, **kwargs):
        temporary = original_create(*args, **kwargs)
        temporary_by_field["descriptor"] = temporary.descriptor

        def fail_close() -> None:
            raise RetainedArtifactError("retained_blob_temporary_close_failed")

        temporary.close = fail_close
        return temporary

    def fail_copy(*args, **kwargs):
        raise RetainedArtifactError("retained_blob_source_mismatch")

    monkeypatch.setattr(
        blob_producer,
        "_create_temporary_blob",
        create_with_failing_close,
    )
    monkeypatch.setattr(install_io, "_copy_verified_source", fail_copy)
    with pytest.raises(RetainedArtifactError, match="source_mismatch") as raised:
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert any(
        "temporary_close_failed" in note
        for note in getattr(raised.value, "__notes__", ())
    )
    original_close(temporary_by_field["descriptor"])


def test_directory_chain_close_attempts_every_descriptor(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    del retained_artifact_test_root
    descriptors = [os.open("/dev/null", os.O_RDONLY) for _index in range(3)]
    directory_chain = install_io._OpenedDirectoryChain(
        descriptors=list(descriptors),
        identities=(),
        components=(),
    )
    original_close = install_io.os.close
    attempted_descriptors: list[int] = []

    def fail_close(descriptor: int) -> None:
        attempted_descriptors.append(descriptor)
        raise OSError("fixture directory close failure")

    monkeypatch.setattr(install_io.os, "close", fail_close)
    with pytest.raises(RetainedArtifactError, match="directory_close_failed") as raised:
        directory_chain.close()
    assert attempted_descriptors == list(reversed(descriptors))
    assert directory_chain.descriptors == []
    assert len(getattr(raised.value, "__notes__", ())) == 2
    monkeypatch.setattr(install_io.os, "close", original_close)
    for descriptor in descriptors:
        original_close(descriptor)


def test_identity_and_target_missing_errors_are_wrapped(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    del retained_artifact_test_root
    install_target = _opened_target("missing-identity")
    parent_descriptor = install_target.directory_chain.parent_descriptor
    assert install_io._named_inode_identity(parent_descriptor, "missing") is None
    named_descriptor = os.open(
        "named",
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
        dir_fd=parent_descriptor,
    )
    os.close(named_descriptor)
    assert install_io._named_inode_identity(parent_descriptor, "named") is not None
    os.unlink("named", dir_fd=parent_descriptor)
    original_stat = install_io.os.stat

    def fail_stat(*args, **kwargs):
        raise OSError("fixture stat failure")

    monkeypatch.setattr(install_io.os, "stat", fail_stat)
    with pytest.raises(RetainedArtifactError, match="identity_unavailable"):
        install_io._named_inode_identity(parent_descriptor, "unavailable")
    monkeypatch.setattr(install_io.os, "stat", original_stat)
    with pytest.raises(RetainedArtifactError, match="blob_unavailable"):
        install_io._verify_target(
            install_target,
            install_target.leaf_name,
            1,
        )
    install_target.close()


def test_opened_source_close_attempts_file_and_every_directory(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path = retained_artifact_test_root / "source-close-all"
    source_path.write_bytes(b"close")
    source_file = install_io._open_source(source_path)
    source_descriptor = source_file.descriptor
    directory_descriptors = tuple(source_file.directory_chain.descriptors)
    all_descriptors = (source_descriptor,) + tuple(reversed(directory_descriptors))
    original_close = install_io.os.close
    attempted_descriptors: list[int] = []

    def fail_close(descriptor: int) -> None:
        attempted_descriptors.append(descriptor)
        raise OSError("fixture source close failure")

    monkeypatch.setattr(install_io.os, "close", fail_close)
    with pytest.raises(RetainedArtifactError, match="source_close_failed") as raised:
        source_file.close()
    assert tuple(attempted_descriptors) == all_descriptors
    assert len(getattr(raised.value, "__notes__", ())) == len(all_descriptors) - 1
    monkeypatch.setattr(install_io.os, "close", original_close)
    for descriptor in all_descriptors:
        original_close(descriptor)


def test_directory_open_fstat_failure_is_typed_and_closes_chain(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    del retained_artifact_test_root
    original_directory_identity = install_io._directory_identity

    def fail_directory_identity(*args, **kwargs):
        raise OSError("fixture directory identity failure")

    monkeypatch.setattr(
        install_io,
        "_directory_identity",
        fail_directory_identity,
    )
    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        install_io._open_directory_chain(())
    monkeypatch.setattr(
        install_io,
        "_directory_identity",
        original_directory_identity,
    )


class _TargetChainFstatFailure:
    """Inject one target identity failure and one cleanup close failure."""

    def __init__(self, shard_component_count: int) -> None:
        self.shard_component_count = shard_component_count
        self.created_component_count = 0
        self.is_armed = False
        self.has_fstat_failed = False
        self.has_close_failed = False
        self.close_attempts: list[int] = []
        self.close_batches: list[tuple[tuple[int, ...], tuple[int, ...]]] = []
        self.original_open_or_create = install_io._open_or_create_directory
        self.original_fstat = install_io.os.fstat
        self.original_close = install_io.os.close
        self.original_close_sequence = install_io._close_descriptor_sequence

    def open_or_create_and_arm(self, parent_descriptor: int, component: str) -> int:
        descriptor = self.original_open_or_create(parent_descriptor, component)
        self.created_component_count += 1
        if self.created_component_count == self.shard_component_count:
            self.is_armed = True
        return descriptor

    def fail_target_identity_fstat(self, descriptor: int):
        if self.is_armed and not self.has_fstat_failed:
            self.has_fstat_failed = True
            raise OSError("fixture target identity fstat failure")
        return self.original_fstat(descriptor)

    def close_once_then_fail(self, descriptor: int) -> None:
        if self.has_fstat_failed:
            self.close_attempts.append(descriptor)
        if self.has_fstat_failed and not self.has_close_failed:
            self.has_close_failed = True
            self.original_close(descriptor)
            raise OSError("fixture target cleanup close failure")
        self.original_close(descriptor)

    def observe_close_sequence(
        self,
        descriptors: list[int],
        error_code: str,
    ) -> None:
        owned_descriptors = tuple(descriptors)
        attempt_start = len(self.close_attempts)
        try:
            self.original_close_sequence(descriptors, error_code)
        finally:
            if self.has_fstat_failed:
                self.close_batches.append(
                    (
                        owned_descriptors,
                        tuple(self.close_attempts[attempt_start:]),
                    )
                )


def test_public_target_chain_fstat_failure_is_typed_and_cleanup_is_exhaustive(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"target chain fstat failure\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "source-target-chain-fstat"
    source_path.write_bytes(artifact_bytes)
    canonical_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    shard_component_count = len(retained_artifact_blob_components(artifact_sha256)) - 1
    target_failure = _TargetChainFstatFailure(shard_component_count)

    monkeypatch.setattr(
        install_io,
        "_open_or_create_directory",
        target_failure.open_or_create_and_arm,
    )
    monkeypatch.setattr(
        install_io.os,
        "fstat",
        target_failure.fail_target_identity_fstat,
    )
    monkeypatch.setattr(install_io.os, "close", target_failure.close_once_then_fail)
    monkeypatch.setattr(
        install_io,
        "_close_descriptor_sequence",
        target_failure.observe_close_sequence,
    )

    with pytest.raises(RetainedArtifactError) as raised:
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )

    assert str(raised.value) == "retained_artifact_path_unsafe"
    assert any(
        "retained_blob_directory_close_failed" in note
        for note in getattr(raised.value, "__notes__", ())
    )
    assert target_failure.created_component_count == shard_component_count
    assert target_failure.is_armed
    assert target_failure.has_fstat_failed
    assert target_failure.has_close_failed
    target_descriptors, target_close_attempts = target_failure.close_batches[0]
    assert len(target_descriptors) > 1
    assert target_close_attempts == tuple(reversed(target_descriptors))
    assert not canonical_path.exists()


def test_source_open_preserves_nonstandard_primary_while_closing_resources(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path = retained_artifact_test_root / "source-nonstandard-primary"
    source_path.write_bytes(b"source")
    original_file_identity = install_io._file_identity

    class FixtureAbort(BaseException):
        pass

    def abort_identity(*args, **kwargs):
        raise FixtureAbort("fixture abort")

    monkeypatch.setattr(install_io, "_file_identity", abort_identity)
    with pytest.raises(FixtureAbort, match="fixture abort"):
        install_io._open_source(source_path)
    monkeypatch.setattr(install_io, "_file_identity", original_file_identity)


def test_source_open_preserves_nonstandard_open_failure_without_file_descriptor(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path = retained_artifact_test_root / "source-open-abort"
    source_path.write_bytes(b"source")
    original_open = install_io.os.open

    class FixtureAbort(BaseException):
        pass

    def abort_source_open(path, *args, **kwargs):
        if path == source_path.name:
            raise FixtureAbort("fixture open abort")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(install_io.os, "open", abort_source_open)
    with pytest.raises(FixtureAbort, match="fixture open abort"):
        install_io._open_source(source_path)
    monkeypatch.setattr(install_io.os, "open", original_open)


def test_path_verification_preserves_nonstandard_primary(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path = retained_artifact_test_root / "path-primary-source"
    source_path.write_bytes(b"path")
    source_file = install_io._open_source(source_path)
    original_open_chain = install_io._open_directory_chain

    class FixtureAbort(BaseException):
        pass

    def abort_open(*args, **kwargs):
        raise FixtureAbort("fixture path abort")

    monkeypatch.setattr(install_io, "_open_directory_chain", abort_open)
    with pytest.raises(FixtureAbort, match="fixture path abort"):
        install_io._verify_path_identity(
            source_file.directory_chain,
            source_file.leaf_name,
            install_io._file_identity(os.fstat(source_file.descriptor)),
            "fixture_mismatch",
        )
    monkeypatch.setattr(install_io, "_open_directory_chain", original_open_chain)
    source_file.close()
