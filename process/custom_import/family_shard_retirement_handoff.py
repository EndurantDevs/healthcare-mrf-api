# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected terminal cleanup intent, separate from every writer delegation."""

import hashlib
import os
import stat
from pathlib import Path

from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_terminal_files as producer
from process.custom_import.family_shard_retirement_files import RetirementFiles, _directory

CONTRACT = "custom-import/retired-owner-handoff/v1"
INTENT = "retirement.json"
SENT = "retirement-sent.json"
JOB = "retirement-job.json"
STATE = "retirement"
COMPLETION = "completion.json"
COMPLETE = "retirement-complete.json"
SUCCEEDED = "retirement-succeeded.json"
_FIELDS = {
    "contract",
    "handoff_id",
    "native_sha256",
    "root_identity",
    "state_identity",
    "lock_identity",
    "maximum_bytes",
}


def identity(document):
    """Hash this closed cleanup intent, independently of data or writer digests."""
    return hashlib.sha256(handoff.canonical(checked(document))).hexdigest()


def checked(document):
    """Accept one original owner/state/lock identity on the same private filesystem."""
    if type(document) is not dict or document.keys() != _FIELDS or document["contract"] != CONTRACT:
        handoff.unavailable()
    for name in ("handoff_id", "native_sha256"):
        value = document[name]
        if type(value) is not str or len(value) != 64 or any(part not in "0123456789abcdef" for part in value):
            handoff.unavailable()
    for name, count in (("root_identity", 4), ("state_identity", 4), ("lock_identity", 2)):
        value = document[name]
        if type(value) is not list or len(value) != count or any(type(part) is not int or part < 0 for part in value):
            handoff.unavailable()
    for name in ("root_identity", "state_identity"):
        if document[name][2:] != [os.geteuid(), stat.S_IFDIR | 0o700]:
            handoff.unavailable()
    if (
        document["root_identity"][0] != document["state_identity"][0]
        or document["root_identity"][0] != document["lock_identity"][0]
        or type(document["maximum_bytes"]) is not int
        or document["maximum_bytes"] <= 0
    ):
        handoff.unavailable()
    return document


def is_marker_created(root, name, value):
    """Write one exact immutable marker; a lost reply cannot replace its first UID."""
    if name not in {INTENT, SENT, JOB, COMPLETE, SUCCEEDED}:
        handoff.unavailable()
    directory = handoff._directory(root)
    raw = handoff.canonical(value)
    try:
        try:
            descriptor = os.open(
                name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o400, dir_fd=directory
            )
        except FileExistsError:
            if handoff.read_file(directory, name, handoff.MAX_DOCUMENT_BYTES) != raw:
                handoff.unavailable()
            return False
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(directory)
        if _directory(root)[:2] != (os.fstat(directory).st_dev, os.fstat(directory).st_ino):
            handoff.unavailable()
        return True
    finally:
        os.close(directory)


def read(root, name, *, optional=False):
    """Read one protected marker without following links or reflecting its contents."""
    directory = handoff._directory(root)
    try:
        if optional and name not in os.listdir(directory):
            return None
        return handoff.document(handoff.read_file(directory, name, handoff.MAX_DOCUMENT_BYTES))
    finally:
        os.close(directory)


def stage(root, parent, native, profile):
    """Snapshot only the original execution's existing work root and lifetime lock."""
    existing = read(root, INTENT, optional=True)
    if existing is not None:
        checked(existing)
        if existing["handoff_id"] != handoff.identity(parent) or existing["native_sha256"] != native.sha256:
            handoff.unavailable()
        return existing
    work = root / "work"
    directory = handoff._directory(work)
    try:
        if set(os.listdir(directory)) not in ({"owner"}, {"owner", STATE}, {STATE}):
            handoff.unavailable()
    finally:
        os.close(directory)
    owner, state = work / "owner", work / STATE
    if os.path.lexists(state / producer.SEALED):
        context = producer.reopen(owner, parent, profile, native)
        original, lock_identity = _producer_root(context)
    else:
        original, lock_identity = _original_root(owner)
    if not os.path.lexists(state):
        state.mkdir(mode=0o700)
    state_identity = _directory(state)
    for path in (state, work):
        producer._sync_parent(path)
    intent_by_field = {
        "contract": CONTRACT,
        "handoff_id": handoff.identity(parent),
        "native_sha256": native.sha256,
        "root_identity": list(original),
        "state_identity": list(state_identity),
        "lock_identity": list(lock_identity),
        "maximum_bytes": profile["disk_bytes"],
    }
    is_marker_created(root, INTENT, checked(intent_by_field))
    return intent_by_field


def _producer_root(context):
    """Accept a missing lock only with its durable drained seal and an empty original root."""
    _entries, digest = producer.read(context)
    owner = context.root
    if os.path.lexists(owner):
        _directory(owner, context.root_identity)
        if os.path.lexists(owner / ".owner-job.lock"):
            if _original_root(owner)[1] != context.lock_identity:
                handoff.unavailable()
        else:
            directory = producer.files._state_directory(context)
            try:
                if os.listdir(owner) or producer.files._read_file(directory, producer.DRAINED, maximum=32)[2] != digest:
                    handoff.unavailable()
            finally:
                os.close(directory)
    return context.root_identity, context.lock_identity


def _original_root(owner):
    """Require the existing original lock for the legacy complete-policy path."""
    original = _directory(owner)
    lock = (owner / ".owner-job.lock").lstat()
    if (
        not stat.S_ISREG(lock.st_mode)
        or lock.st_nlink != 1
        or lock.st_uid != os.geteuid()
        or stat.S_IMODE(lock.st_mode) != 0o600
        or lock.st_size != 0
        or lock.st_dev != original[0]
    ):
        handoff.unavailable()
    return original, (lock.st_dev, lock.st_ino)


def context(document, *, work):
    """Locate the original absolute mount paths; never translate a checkpoint path."""
    checked(document)
    return RetirementFiles(
        Path(work) / "owner",
        Path(work) / STATE,
        document["native_sha256"],
        tuple(document["root_identity"]),
        tuple(document["state_identity"]),
        tuple(document["lock_identity"]),
        document["maximum_bytes"],
    )
