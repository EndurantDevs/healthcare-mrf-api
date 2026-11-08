# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reuse protected PVC custody without Kubernetes Secret permissions."""

import hashlib
import os
from pathlib import Path

from process.custom_import import family_shard_handoff as custody
from process.custom_import import retained_capture_copy_contract as contract

INPUT = Path("/run/custom-import-retained-copy")
NAMES = frozenset({"delegation.json", "bearer", "committed"})


def directory_name(identifier):
    """Keep copy custody separate from original owner work directories."""
    if type(identifier) is not str or len(identifier) != 64 or any(c not in "0123456789abcdef" for c in identifier):
        custody.unavailable()
    return "capture-copy-" + identifier


def _reopen(root, identifier):
    directory = custody._directory(root)
    try:
        if set(os.listdir(directory)) != NAMES:
            custody.unavailable()
        metadata = os.fstat(directory)
        marker = custody.document(custody.read_file(directory, "committed", 512))
        if marker != {"copy_id": identifier, "device": metadata.st_dev, "inode": metadata.st_ino}:
            custody.unavailable()
        signed = custody.document(custody.read_file(directory, "delegation.json", custody.MAX_DOCUMENT_BYTES))
        bearer = custody.checked_bearer(custody.read_file(directory, "bearer", 43))
        if (
            contract.identity(signed) != identifier
            or hashlib.sha256(bearer).hexdigest() != signed["descriptor"]["bearer_sha256"]
        ):
            custody.unavailable()
        return signed, bearer, {"root_device": metadata.st_dev, "root_inode": metadata.st_ino}
    finally:
        os.close(directory)


def reopen(identifier, *, parent=custody.OWNER_PARENT):
    """Reopen only an exact committed directory owned by this producer."""
    parent_fd = custody._directory(parent)
    try:
        return _reopen(parent / directory_name(identifier), identifier)
    finally:
        os.close(parent_fd)


def reopen_job(identifier):
    """Use the fixed read-only mount, never caller-supplied filesystem paths."""
    return _reopen(INPUT, identifier)


def stage(signed, bearer, *, parent=custody.OWNER_PARENT):
    """Commit immutable exact inputs before any Job dispatch is possible."""
    identifier = contract.identity(signed)
    custody.checked_bearer(bearer)
    if hashlib.sha256(bearer).hexdigest() != signed["descriptor"]["bearer_sha256"]:
        custody.unavailable()
    parent_fd = custody._directory(parent)
    try:
        name = directory_name(identifier)
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            os.close(custody._directory(parent / name))
        root = parent / name
        directory = custody._directory(root)
        try:
            if set(os.listdir(directory)) - NAMES:
                custody.unavailable()
            custody._stage_inputs(directory, {"delegation.json": custody.canonical(signed), "bearer": bearer})
            metadata = os.fstat(directory)
            marker = custody.canonical({"copy_id": identifier, "device": metadata.st_dev, "inode": metadata.st_ino})
            custody._stage_inputs(directory, {"committed": marker})
            os.fsync(directory)
            os.fsync(parent_fd)
        finally:
            os.close(directory)
        return reopen(identifier, parent=parent)[2]
    finally:
        os.close(parent_fd)


def cleanup(signed, receipt, *, parent=custody.OWNER_PARENT):
    """Consume a native sealed cleanup intent; permit exact authenticated partial retry."""
    identifier = contract.identity(signed)
    root = parent / directory_name(identifier)
    parent_fd = custody._directory(parent)
    try:
        try:
            directory = custody._directory(root)
        except FileNotFoundError:
            return
        try:
            metadata = os.fstat(directory)
            if (metadata.st_dev, metadata.st_ino) != (receipt["root_device"], receipt["root_inode"]):
                custody.unavailable()
            names = set(os.listdir(directory))
            if names - NAMES:
                custody.unavailable()
            marker_by_field = {"copy_id": identifier, "device": metadata.st_dev, "inode": metadata.st_ino}
            for name in names:
                raw = custody.read_file(directory, name, custody.MAX_DOCUMENT_BYTES)
                if name == "delegation.json" and raw != custody.canonical(signed):
                    custody.unavailable()
                if name == "committed" and custody.document(raw) != marker_by_field:
                    custody.unavailable()
                if (
                    name == "bearer"
                    and hashlib.sha256(custody.checked_bearer(raw)).hexdigest() != signed["descriptor"]["bearer_sha256"]
                ):
                    custody.unavailable()
            for name in ("delegation.json", "bearer", "committed"):
                if name in names:
                    os.unlink(name, dir_fd=directory)
            os.fsync(directory)
            observed = root.lstat()
            if (observed.st_dev, observed.st_ino) != (metadata.st_dev, metadata.st_ino):
                custody.unavailable()
            os.rmdir(directory_name(identifier), dir_fd=parent_fd)
            os.fsync(parent_fd)
        finally:
            os.close(directory)
    finally:
        os.close(parent_fd)
