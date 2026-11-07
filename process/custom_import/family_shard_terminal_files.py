# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Producer census custody, joined to permanent retirement only by its consumer."""

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path

import msgpack

from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_retirement_files as files

CONTRACT = "custom-import/terminal-producer-files/v1"
STATE = "retirement"
INVENTORY = "producer-inventory.mpk"
SEALED = "producer-sealed.mpk"
DRAINED = "producer-drained"
_PRODUCER = {INVENTORY, SEALED, DRAINED}
_PENDING = {name + ".pending" for name in _PRODUCER}
_RETIRED = {files._INVENTORY, files._SEALED, files._DRAINED, files._DONE, "completion.json"}
_REBIND = {files._INVENTORY + ".pending", files._SEALED + ".pending", files._DRAINED + ".pending"}


def _binding(parent, profile, native, root):
    """Join the signed parent/profile to the complete immutable native scope and head."""
    profile_sha = hashlib.sha256(handoff.canonical(profile)).hexdigest()
    if parent["descriptor"]["owner_profile_sha256"] != profile_sha:
        handoff.unavailable()
    return (
        handoff.identity(parent),
        profile_sha,
        hashlib.sha256(msgpack.packb(native.scope, use_bin_type=True)).digest(),
        native.head.revision,
        native.head.sha256,
        files._directory(root.parent),
    )


@dataclass(frozen=True)
class ProducerFiles:
    """A checked census, deliberately without a native retirement digest or writer."""

    root: Path
    state: Path
    binding: tuple
    root_identity: tuple
    state_identity: tuple
    lock_identity: tuple
    maximum_bytes: int

    def __post_init__(self):
        files._check_context(self)
        if self.state.name != STATE:
            files._refuse()
        for identity in (self.root_identity, self.state_identity):
            if identity[2:] != (os.geteuid(), stat.S_IFDIR | 0o700):
                files._refuse()
        if self.root_identity[0] != self.state_identity[0] or self.lock_identity[0] != self.root_identity[0]:
            files._refuse()

    @property
    def header(self):
        """Bind fixed mount slot and original inodes across the API/owner mount aliases."""
        return (
            CONTRACT,
            self.binding,
            self.root.name,
            self.root_identity,
            self.state_identity,
            self.lock_identity,
            self.maximum_bytes,
        )


def _names(directory):
    names = set(os.listdir(directory))
    if names - (_PRODUCER | _PENDING | _RETIRED | _REBIND):
        files._refuse()
    return names


def _unlink(directory, names):
    """Remove only checked fixed evidence files, keeping the compact seal until last."""
    for name in sorted(names & set(os.listdir(directory)), key=lambda item: (item == SEALED, item)):
        files._read_file(directory, name, maximum=files.MAX_INVENTORY_BYTES)
        os.unlink(name, dir_fd=directory)
        os.fsync(directory)


def _capsule(directory):
    header, digest = msgpack.unpackb(
        files._read_file(directory, SEALED, maximum=4096)[2],
        raw=False,
        use_list=False,
        strict_map_key=True,
        max_array_len=16,
        max_map_len=0,
        max_str_len=256,
        max_bin_len=64,
    )
    if (
        type(header) is not tuple
        or len(header) != 7
        or header[0] != CONTRACT
        or (type(digest) is not bytes or len(digest) != 32)
    ):
        files._refuse()
    return header, digest


def reopen(root, parent, profile, native):
    """Match a compact producer seal to independent current native custody."""
    state = root.parent / STATE
    directory = files._open_directory(state)
    try:
        _names(directory)
        header, _digest = _capsule(directory)
    finally:
        os.close(directory)
    context = ProducerFiles(root, state, _binding(parent, profile, native, root), *header[3:6], profile["disk_bytes"])
    if header != context.header:
        files._refuse()
    files._directory(state, context.state_identity)
    return context


def read(context):
    """Missing census bytes are allowed only after its exact root is already absent."""
    directory = files._state_directory(context)
    try:
        names = _names(directory)
        header, digest = _capsule(directory)
        if header != context.header:
            files._refuse()
        if DRAINED in names and files._read_file(directory, DRAINED, maximum=32)[2] != digest:
            files._refuse()
        if INVENTORY not in names:
            if os.path.lexists(context.root):
                files._refuse()
            return (), digest
        _stamp, observed, content = files._read_file(directory, INVENTORY, maximum=files.MAX_INVENTORY_BYTES)
        if observed != digest:
            files._refuse()
        return files._decode_inventory(context, content), digest
    finally:
        os.close(directory)


def seal(context, policy, check):
    """Prove the complete original tree before discarding any torn, unsealed evidence."""
    directory = files._state_directory(context)
    try:
        names = _names(directory)
        if names - (_PRODUCER | _PENDING):
            files._refuse()
        if SEALED in names:
            entries, digest = read(context)
            files._check_remaining(context, entries, check)
            return entries, digest
        entries, owned_bytes = files._census_entries(context, policy, check)
        content = files._inventory_content(context, entries)
        digest = hashlib.sha256(content).digest()
        capsule = msgpack.packb((context.header, digest), use_bin_type=True)
        # Reproof above is mandatory: a temporary filename is never authority.
        _unlink(directory, names)
        files._reserve_state(context, owned_bytes, (len(content), len(capsule), 32))
        files._atomic_write(directory, INVENTORY, content)
        files._atomic_write(directory, SEALED, capsule)
        return entries, digest
    finally:
        os.close(directory)


def _remaining(context, entries, check):
    """Check the surviving subset and charge its actual allocation before a rebind."""
    if not os.path.lexists(context.root):
        return 0
    files._check_remaining(context, entries, check)
    total = 0
    for relative, kind, _stamp, _digest in entries:
        path = context.root / relative
        if os.path.lexists(path):
            metadata = path.lstat()
            total += 4096 if kind == "directory" else max(metadata.st_size, metadata.st_blocks * 512) + 4096
    return total


def mark_drained(context, check):
    """Keep the original lock until every inventoried data file has been removed."""
    entries, digest = read(context)
    files._check_remaining(context, entries, check)
    if set(os.listdir(context.root)) != {".owner-job.lock"}:
        files._refuse()
    files._reserve_state(context, _remaining(context, entries, check), (32,))
    _sync_parent(context.root)
    directory = files._state_directory(context)
    try:
        files._atomic_write(directory, DRAINED, digest)
    finally:
        os.close(directory)


def finish(context):
    """Delete bounded evidence only after root removal, retaining the compact seal last."""
    if os.path.lexists(context.root):
        files._refuse()
    _sync_parent(context.root.parent)
    if not os.path.lexists(context.state):
        _sync_parent(context.state.parent)
        return
    directory = files._state_directory(context)
    try:
        names = _names(directory)
        if names:
            read(context)
        if names - (_PRODUCER | _PENDING):
            files._refuse()
        _unlink(directory, names)
    finally:
        os.close(directory)
    context.state.rmdir()
    _sync_parent(context.state.parent)


def _sync_parent(path):
    directory = files._open_directory(path)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def original_context(root, parent, profile, native, lock):
    """Allocate the sole fixed evidence sibling without replacing existing state."""
    state = root.parent / STATE
    if not os.path.lexists(state):
        state.mkdir(mode=0o700)
        _sync_parent(state.parent)
    return ProducerFiles(
        root,
        state,
        _binding(parent, profile, native, root),
        files._directory(root),
        files._directory(state),
        lock.file_identity,
        profile["disk_bytes"],
    )


def is_rebind_resumed(context, check):
    """A durable actual-retirement inventory owns recovery of leftover producer files."""
    directory = files._state_directory(context)
    try:
        names = _names(directory)
        if files._SEALED not in names:
            return False
        entries, digest = files._read_inventory_payload(context, directory, names)
        owned_bytes = _remaining(context, entries, check)
        if not os.path.lexists(context.root / ".owner-job.lock") and files._DRAINED not in names:
            files._reserve_state(context, owned_bytes, (32,))
            if os.path.lexists(context.root):
                _header, producer_digest = _capsule(directory)
                if os.listdir(context.root) or files._read_file(directory, DRAINED, maximum=32)[2] != producer_digest:
                    files._refuse()
            files._atomic_write(directory, files._DRAINED, digest)
        _unlink(directory, names & (_PRODUCER | _PENDING | _REBIND))
        return True
    finally:
        os.close(directory)


def rebind(context, parent, profile, native, check):
    """Join producer evidence only after the caller proves actual permanent retirement."""
    if context.native_sha256 != native.sha256:
        files._refuse()
    producer = reopen(context.root, parent, profile, native)
    if (producer.root_identity, producer.state_identity, producer.lock_identity, producer.maximum_bytes) != (
        context.root_identity,
        context.state_identity,
        context.lock_identity,
        context.maximum_bytes,
    ):
        files._refuse()
    entries, _producer_digest = read(producer)
    owned_bytes = _remaining(producer, entries, check)
    content = files._inventory_content(context, entries)
    digest = hashlib.sha256(content).digest()
    files._reserve_state(context, owned_bytes, (len(content), 32, 32))
    directory = files._state_directory(context)
    try:
        names = _names(directory)
        if names & {files._DONE, "completion.json"}:
            files._refuse()
        files._atomic_write(directory, files._INVENTORY, content)
        files._atomic_write(directory, files._SEALED, digest)
        if not os.path.lexists(context.root / ".owner-job.lock"):
            if os.path.lexists(context.root):
                files._directory(context.root, context.root_identity)
                if os.listdir(context.root) or files._read_file(directory, DRAINED, maximum=32)[2] != _producer_digest:
                    files._refuse()
            files._atomic_write(directory, files._DRAINED, digest)
        _unlink(directory, names & (_PRODUCER | _PENDING | _REBIND))
    finally:
        os.close(directory)
    return files.read_inventory(context)


def discard_unsealed(context):
    """Called only after independent complete original-policy proof under the original lock."""
    directory = files._state_directory(context)
    try:
        names = _names(directory)
        if SEALED in names or names - {INVENTORY, INVENTORY + ".pending", SEALED + ".pending"}:
            files._refuse()
        _unlink(directory, names)
    finally:
        os.close(directory)


def is_empty_residue_removed(root):
    """After actual retirement/stopped-owner proof, remove only an empty fixed sibling."""
    work = root / "work"
    directory = files._open_directory(work)
    try:
        if set(os.listdir(directory)) != {STATE}:
            return False
        state = work / STATE
        state_directory = files._open_directory(state)
        try:
            if os.listdir(state_directory):
                return False
            state.rmdir()
            os.fsync(directory)
            return True
        finally:
            os.close(state_directory)
    finally:
        os.close(directory)
