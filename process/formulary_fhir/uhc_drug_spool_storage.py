# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Descriptor-pinned private SQLite access for UHC drug spools."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import os
from pathlib import Path
import sqlite3
import stat
from typing import Iterator

from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence


@dataclass(frozen=True, slots=True, repr=False)
class PinnedUHCDrugSpool:
    """Hold one exact private spool inode across asynchronous repository work."""

    descriptor: int = field(repr=False)
    device: int
    inode: int
    byte_count: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True, slots=True, repr=False)
class _VerifiedUHCDrugSpool(PinnedUHCDrugSpool):
    """Bind one pinned inode to a fully recomputed spool and artifact proof."""

    source_id: str
    spool_content_sha256: str = field(repr=False)
    artifact_set_sha256: str = field(repr=False)
    verification_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.verification_token is not _VERIFIED_SPOOL_TOKEN:
            raise ValueError("UHC drug spool verification capability is invalid")


_VERIFIED_SPOOL_TOKEN = object()


def _open_spool_descriptor(exact_path: Path) -> int:
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    nofollow_flag = getattr(os, "O_NOFOLLOW", 0)
    close_on_exec_flag = getattr(os, "O_CLOEXEC", 0)
    directory_descriptors: list[int] = []
    try:
        directory_descriptors.append(
            os.open("/", directory_flags | close_on_exec_flag)
        )
        path_components = exact_path.parts[1:]
        if (
            not exact_path.is_absolute()
            or not path_components
            or any(component in {"", ".", ".."} for component in path_components)
        ):
            raise OSError("invalid path")
        for component in path_components[:-1]:
            directory_descriptors.append(
                os.open(
                    component,
                    directory_flags | nofollow_flag | close_on_exec_flag,
                    dir_fd=directory_descriptors[-1],
                )
            )
        parent_state = os.fstat(directory_descriptors[-1])
        if (
            parent_state.st_uid != os.geteuid()
            or stat.S_IMODE(parent_state.st_mode) & 0o077
        ):
            raise OSError("unsafe parent")
        descriptor = os.open(
            path_components[-1],
            os.O_RDONLY | nofollow_flag | close_on_exec_flag,
            dir_fd=directory_descriptors[-1],
        )
        path_state = os.fstat(descriptor)
        if (
            not stat.S_ISREG(path_state.st_mode)
            or path_state.st_uid != os.geteuid()
            or stat.S_IMODE(path_state.st_mode) & 0o077
        ):
            os.close(descriptor)
            raise OSError("unsafe spool")
        return descriptor
    except (OSError, TypeError, ValueError):
        raise ValueError("UHC drug spool is unavailable") from None
    finally:
        for directory_descriptor in reversed(directory_descriptors):
            os.close(directory_descriptor)


def _duplicate_pinned_descriptor(spool: PinnedUHCDrugSpool) -> int:
    try:
        descriptor = os.dup(spool.descriptor)
        descriptor_state = os.fstat(descriptor)
    except OSError:
        raise ValueError("UHC drug spool is unavailable") from None
    observed_state = (
        descriptor_state.st_dev,
        descriptor_state.st_ino,
        descriptor_state.st_size,
        descriptor_state.st_mtime_ns,
        descriptor_state.st_ctime_ns,
    )
    expected_state = (
        spool.device,
        spool.inode,
        spool.byte_count,
        spool.modified_ns,
        spool.changed_ns,
    )
    if observed_state != expected_state:
        os.close(descriptor)
        raise ValueError("UHC drug spool is unavailable")
    return descriptor


@contextmanager
def pin_uhc_drug_spool(spool_path: Path | str) -> Iterator[PinnedUHCDrugSpool]:
    """Hold one owner-private spool descriptor until the caller exits."""

    try:
        exact_path = Path(spool_path)
    except (TypeError, ValueError):
        raise ValueError("UHC drug spool is unavailable") from None
    descriptor = _open_spool_descriptor(exact_path)
    descriptor_state = os.fstat(descriptor)
    try:
        yield PinnedUHCDrugSpool(
            descriptor=descriptor,
            device=descriptor_state.st_dev,
            inode=descriptor_state.st_ino,
            byte_count=descriptor_state.st_size,
            modified_ns=descriptor_state.st_mtime_ns,
            changed_ns=descriptor_state.st_ctime_ns,
        )
    finally:
        os.close(descriptor)


@contextmanager
def open_uhc_drug_spool(
    spool: Path | str | PinnedUHCDrugSpool,
) -> Iterator[sqlite3.Connection]:
    """Open SQLite against a path or an already pinned exact inode."""

    if isinstance(spool, PinnedUHCDrugSpool):
        descriptor = _duplicate_pinned_descriptor(spool)
    else:
        try:
            exact_path = Path(spool)
        except (TypeError, ValueError):
            raise ValueError("UHC drug spool is unavailable") from None
        descriptor = _open_spool_descriptor(exact_path)
    connection: sqlite3.Connection | None = None
    try:
        database_uri = f"file:/dev/fd/{descriptor}?mode=ro&immutable=1"
        connection = sqlite3.connect(database_uri, uri=True)
        connection.row_factory = sqlite3.Row
        yield connection
    except sqlite3.Error:
        raise ValueError("UHC drug spool is unavailable") from None
    finally:
        if connection is not None:
            connection.close()
        os.close(descriptor)


def verified_spool_capability(
    spool: PinnedUHCDrugSpool,
    evidence: UHCDrugSpoolEvidence,
) -> _VerifiedUHCDrugSpool:
    """Create one internal capability only after the caller recomputes proof."""

    return _VerifiedUHCDrugSpool(
        descriptor=spool.descriptor,
        device=spool.device,
        inode=spool.inode,
        byte_count=spool.byte_count,
        modified_ns=spool.modified_ns,
        changed_ns=spool.changed_ns,
        source_id=evidence.source_id,
        spool_content_sha256=evidence.spool_content_sha256,
        artifact_set_sha256=evidence.artifact_set_sha256,
        verification_token=_VERIFIED_SPOOL_TOKEN,
    )


__all__ = (
    "PinnedUHCDrugSpool",
    "open_uhc_drug_spool",
    "pin_uhc_drug_spool",
)
