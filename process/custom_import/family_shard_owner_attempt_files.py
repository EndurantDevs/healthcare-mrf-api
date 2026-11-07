# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected child mandates beside, never in place of, original owner custody."""

import hashlib
import os
import re
from contextlib import suppress
from pathlib import Path

from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_attempt as attempt
from process.custom_import.family_shard_owner_lock import _NAME, acquire_owner_job_lock


def parent_root(parent, descriptor, *, root=None):
    """Authenticate the original committed directory and exact signed device/inode."""
    identifier = handoff.identity(parent)
    root = handoff.OWNER_PARENT / identifier if root is None else Path(root)
    signed, bearer = handoff._reopen_root(root, identifier)
    fd = handoff._directory(root)
    try:
        stat = os.fstat(fd)
        if signed != parent or (stat.st_dev, stat.st_ino) != (descriptor["root_device"], descriptor["root_inode"]):
            handoff.unavailable()
        return root, bearer
    finally:
        os.close(fd)


def attempt_lock(root):
    """Serialize resource sends/observations across API processes with an existing lock."""
    fd = handoff._directory(root)
    try:
        with suppress(FileExistsError):
            os.mkdir("attempts", mode=0o700, dir_fd=fd)
        os.fsync(fd)
        path = root / "attempts"
        child = handoff._directory(path)
        try:
            stat = os.fstat(child)
            names = os.listdir(child)
            if any(n != _NAME and re.fullmatch(r"[0-9a-f]{64}", n) is None for n in names) or (
                sum(n != _NAME for n in names) > 32
            ):
                handoff.unavailable()
            return acquire_owner_job_lock(path, identity=(stat.st_dev, stat.st_ino))
        finally:
            os.close(child)
    finally:
        os.close(fd)


def stage(parent, signed, token, *, root):
    """Commit one immutable child mandate and protected nonce beside the parent."""
    descriptor = attempt.checked_descriptor(signed["descriptor"], parent)
    digest = attempt.identity(signed, parent)
    root, _bearer = parent_root(parent, descriptor, root=root)
    directory = root / "attempts"
    fd = handoff._directory(directory)
    try:
        names = set(os.listdir(fd))
        if digest not in names and len(names - {_NAME}) >= 32:
            handoff.unavailable()
        with suppress(FileExistsError):
            os.mkdir(digest, mode=0o700, dir_fd=fd)
        path = directory / digest
        child = handoff._directory(path)
        try:
            if set(os.listdir(child)) - {"delegation.json", "nonce", "committed", "launch.json", "job.json"}:
                handoff.unavailable()
            inputs_by_name = {"delegation.json": handoff.canonical(signed), "nonce": token}
            handoff._stage_inputs(child, inputs_by_name)
            marker = handoff.canonical({"attempt_sha256": digest, "nonce_sha256": hashlib.sha256(token).hexdigest()})
            handoff._stage_inputs(child, {"committed": marker})
            os.fsync(child)
            os.fsync(fd)
            return path
        finally:
            os.close(child)
    finally:
        os.close(fd)


def reopen(parent, digest, *, root):
    """Reject partial, replaced or unknown child input custody without repairing it."""
    if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        handoff.unavailable()
    directory = Path(root) / "attempts" / digest
    fd = handoff._directory(directory)
    try:
        if set(os.listdir(fd)) - {"delegation.json", "nonce", "committed", "launch.json", "job.json"}:
            handoff.unavailable()
        signed = handoff.document(handoff.read_file(fd, "delegation.json", handoff.MAX_DOCUMENT_BYTES))
        descriptor = attempt.checked_descriptor(signed["descriptor"], parent)
        if attempt.identity(signed, parent) != digest:
            handoff.unavailable()
        token = handoff.read_file(fd, "nonce", 43)
        marker = handoff.document(handoff.read_file(fd, "committed", 512))
        if marker != {"attempt_sha256": digest, "nonce_sha256": hashlib.sha256(token).hexdigest()}:
            handoff.unavailable()
        parent_root(parent, descriptor, root=root)
        return signed, token
    finally:
        os.close(fd)


def is_marker_created(path, name, value):
    """Create a fixed marker once; later observations must match its exact bytes."""
    if name not in {"launch.json", "job.json"}:
        handoff.unavailable()
    fd = handoff._directory(path)
    try:
        is_present = name in os.listdir(fd)
        handoff._stage_inputs(fd, {name: handoff.canonical(value)})
        os.fsync(fd)
        return not is_present
    finally:
        os.close(fd)
