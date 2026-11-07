# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Sealed exact-file retirement after permanent native and stopped-owner proof."""

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import msgpack

from process.custom_import.family_block_binding import FamilyBlockUnavailable

CONTRACT = "custom-import/retired-file-inventory/v1"
MAX_ENTRIES = 1_000_000
MAX_INVENTORY_BYTES = 256 * 1024**2
_INVENTORY = "inventory.mpk"
_SEALED = "sealed"
_DRAINED = "drained"
_DONE = "done"


def _refuse():
    raise FamilyBlockUnavailable("retired file custody is unavailable")


def _identity(metadata):
    return metadata.st_dev, metadata.st_ino, metadata.st_uid, metadata.st_mode


def _stamp(metadata):
    return (*_identity(metadata), metadata.st_nlink, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns)


def _directory(path, expected=None):
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or expected is not None
        and _identity(metadata) != expected
    ):
        _refuse()
    return _identity(metadata)


def _read_file(parent, name, *, maximum=None, allowed_modes=(0o400, 0o600), check=None):
    metadata = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) not in allowed_modes
        or maximum is not None
        and metadata.st_size > maximum
    ):
        _refuse()
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    digest = hashlib.sha256()
    content = bytearray() if maximum is not None else None
    try:
        if _stamp(os.fstat(descriptor)) != _stamp(metadata):
            _refuse()
        while chunk := os.read(descriptor, 1024**2):
            if check is not None:
                check()
            digest.update(chunk)
            if content is not None:
                content.extend(chunk)
                if len(content) > maximum:
                    _refuse()
        if _stamp(os.fstat(descriptor)) != _stamp(metadata) or _stamp(
            os.stat(name, dir_fd=parent, follow_symlinks=False)
        ) != _stamp(metadata):
            _refuse()
    finally:
        os.close(descriptor)
    return _stamp(metadata), digest.digest(), bytes(content) if content is not None else None


def _open_directory(path):
    identity = _directory(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    if _identity(os.fstat(descriptor)) != identity:
        os.close(descriptor)
        _refuse()
    return descriptor


@dataclass(frozen=True)
class RetirementFiles:
    """One original root, immutable terminal native identity and bounded disk budget."""

    root: Path
    state: Path
    native_sha256: str
    root_identity: tuple
    state_identity: tuple
    lock_identity: tuple
    maximum_bytes: int

    def __post_init__(self):
        _check_context(self)
        if len(self.native_sha256) != 64 or bytes.fromhex(self.native_sha256).hex() != self.native_sha256:
            _refuse()

    @property
    def header(self):
        """Bind the original native, root, state and lock identities to this inventory."""
        return (
            CONTRACT,
            self.native_sha256,
            str(self.root),
            self.root_identity,
            self.state_identity,
            self.lock_identity,
            self.maximum_bytes,
        )


def _check_context(context):
    """Require one bounded sibling state and the original private filesystem identities."""
    if (
        not context.root.is_absolute()
        or context.state.parent != context.root.parent
        or context.state == context.root
        or len(context.root_identity) != 4
        or len(context.state_identity) != 4
        or len(context.lock_identity) != 2
        or type(context.maximum_bytes) is not int
        or context.maximum_bytes <= 0
    ):
        _refuse()


def _state_directory(context):
    _directory(context.state, context.state_identity)
    return _open_directory(context.state)


def _relative(value):
    if type(value) is not str:
        _refuse()
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or len(path.parts) > 12 or any(part in {".", ".."} for part in path.parts):
        _refuse()
    if str(path) != value or "\0" in value:
        _refuse()
    return path


def _census_entries(context, policy, check):
    entries = []
    bytes_by_kind = {"retained": 0}

    def visit(path, relative):
        """Read one admitted directory without links or ownership changes."""
        check()
        identity = _directory(path)
        if identity[0] != context.root_identity[0]:
            _refuse()
        directory = _open_directory(path)
        try:
            names = sorted(os.listdir(directory))
            if names != sorted(policy.names(relative, names)):
                _refuse()
            for name in names:
                check()
                child = name if not relative else relative + "/" + name
                metadata = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if stat.S_ISDIR(metadata.st_mode):
                    if not policy.is_directory(child):
                        _refuse()
                    visit(path / name, child)
                    continue
                if policy.is_directory(child):
                    _refuse()
                stamp, digest, _content = _read_file(
                    directory, name, allowed_modes=policy.file_modes(child), check=check
                )
                if stamp[0] != context.root_identity[0]:
                    _refuse()
                bytes_by_kind["retained"] += max(stamp[5], metadata.st_blocks * 512) + 4096
                entries.append((child, "file", stamp, digest))
                if len(entries) > MAX_ENTRIES or bytes_by_kind["retained"] > context.maximum_bytes:
                    _refuse()
            if _identity(os.fstat(directory)) != identity or _directory(path) != identity:
                _refuse()
            if relative:
                entries.append((relative, "directory", identity, None))
                bytes_by_kind["retained"] += 4096
                if len(entries) > MAX_ENTRIES or bytes_by_kind["retained"] > context.maximum_bytes:
                    _refuse()
        finally:
            os.close(directory)

    _directory(context.root, context.root_identity)
    lock = (context.root / ".owner-job.lock").lstat()
    if (lock.st_dev, lock.st_ino) != context.lock_identity:
        _refuse()
    visit(context.root, "")
    return tuple(entries), bytes_by_kind["retained"]


def _census(context, policy, check):
    """Retain the existing exact-entry census interface."""
    return _census_entries(context, policy, check)[0]


def _write_once(directory, name, content):
    try:
        descriptor = os.open(
            name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory
        )
    except FileExistsError:
        if _read_file(directory, name, maximum=len(content))[2] != content:
            _refuse()
        return
    try:
        view = memoryview(content)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.fsync(directory)


def _atomic_write(directory, name, content):
    """Seal one bounded file under exclusive custody, retaining torn temporary writes."""
    if len(content) > MAX_INVENTORY_BYTES:
        _refuse()
    if name in os.listdir(directory):
        if _read_file(directory, name, maximum=len(content))[2] != content:
            _refuse()
        return
    pending = name + ".pending"
    if pending in os.listdir(directory):
        _read_file(directory, pending, maximum=MAX_INVENTORY_BYTES, allowed_modes=(0o600,))
        os.unlink(pending, dir_fd=directory)
        os.fsync(directory)
    _write_once(directory, pending, content)
    os.rename(pending, name, src_dir_fd=directory, dst_dir_fd=directory)
    os.fsync(directory)


def _reserve_state(context, owned_bytes, content_sizes):
    """Charge existing metadata and the pending encoded write inside the original disk limit."""
    directory = _state_directory(context)
    try:
        fragment_bytes = os.statvfs(context.state).f_frsize
        if fragment_bytes <= 0:
            _refuse()
        total = owned_bytes + 8192
        for size in content_sizes:
            total += ((size + fragment_bytes - 1) // fragment_bytes) * fragment_bytes + 4096
        for name in os.listdir(directory):
            metadata = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) not in (0o400, 0o600)
            ):
                _refuse()
            total += max(metadata.st_size, metadata.st_blocks * 512) + 4096
        if total > context.maximum_bytes:
            _refuse()
    finally:
        os.close(directory)


def _inventory_content(context, entries):
    """Encode only the existing bounded exact-entry format."""
    content = msgpack.packb((context.header, entries), use_bin_type=True)
    if len(content) > MAX_INVENTORY_BYTES:
        _refuse()
    return content


def _decode_inventory(context, content):
    """Validate one exact header and closed entry shapes without accepting state filenames."""
    header, entries = msgpack.unpackb(
        content,
        raw=False,
        use_list=False,
        strict_map_key=True,
        max_array_len=MAX_ENTRIES + 1,
        max_map_len=0,
        max_str_len=4096,
        max_bin_len=64,
    )
    if header != context.header or type(entries) is not tuple or len(entries) > MAX_ENTRIES:
        _refuse()
    seen_paths = set()
    for entry in entries:
        if type(entry) is not tuple or len(entry) != 4:
            _refuse()
        relative, kind, stamp, expected = entry
        _relative(relative)
        if relative in seen_paths or kind not in {"file", "directory"} or type(stamp) is not tuple:
            _refuse()
        if kind == "file" and (len(stamp) != 8 or type(expected) is not bytes or len(expected) != 32):
            _refuse()
        if kind == "directory" and (len(stamp) != 4 or expected is not None):
            _refuse()
        seen_paths.add(relative)
    return entries


def seal_inventory(context, policy, check):
    """Inventory is durable before the first unlink; uncertain partial writes stay put."""
    directory = _state_directory(context)
    try:
        if os.listdir(directory):
            return read_inventory(context)
        entries = _census(context, policy, check)
        content = _inventory_content(context, entries)
        digest = hashlib.sha256(content).digest()
        _write_once(directory, _INVENTORY, content)
        _write_once(directory, _SEALED, digest)
        return entries, digest
    finally:
        os.close(directory)


def read_inventory(context):
    """Refuse unsealed, changed or foreign inventories before considering any unlink."""
    directory = _state_directory(context)
    try:
        names = set(os.listdir(directory))
        if not {_INVENTORY, _SEALED} <= names or names - {_INVENTORY, _SEALED, _DRAINED, _DONE, "completion.json"}:
            _refuse()
        if "completion.json" in names and _DONE not in names:
            _refuse()
        return _read_inventory_payload(context, directory, names)
    finally:
        os.close(directory)


def _read_inventory_payload(context, directory, names):
    """Validate bytes and markers after the caller checks its closed state filenames."""
    _stamp_value, digest, content = _read_file(directory, _INVENTORY, maximum=MAX_INVENTORY_BYTES)
    if _read_file(directory, _SEALED, maximum=32)[2] != digest:
        _refuse()
    entries = _decode_inventory(context, content)
    for marker in names & {_DRAINED, _DONE}:
        if _read_file(directory, marker, maximum=32)[2] != digest:
            _refuse()
    return entries, digest


def _check_remaining(context, entries, check):
    by_path = {entry[0]: entry for entry in entries}

    def visit(path, relative):
        """Allow only missing recorded entries; retain every added or changed entry."""
        check()
        directory = _open_directory(path)
        try:
            for name in os.listdir(directory):
                child = name if not relative else relative + "/" + name
                entry = by_path.get(child)
                if entry is None:
                    _refuse()
                if entry[1] == "directory":
                    _directory(path / name, entry[2])
                    visit(path / name, child)
                elif (
                    _read_file(directory, name, allowed_modes=(stat.S_IMODE(entry[2][3]),), check=check)[:2]
                    != entry[2:]
                ):
                    _refuse()
        finally:
            os.close(directory)

    _directory(context.root, context.root_identity)
    visit(context.root, "")


def _remove_entry(context, entry, by_path, check):
    relative, kind, expected, digest = entry
    components = _relative(relative).parts
    directory = _open_directory(context.root)
    try:
        parent_parts = []
        for name in components[:-1]:
            try:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
            except FileNotFoundError:
                return
            parent_parts.append(name)
            parent_entry = by_path.get("/".join(parent_parts))
            if parent_entry is None or parent_entry[1] != "directory" or _identity(os.fstat(child)) != parent_entry[2]:
                os.close(child)
                _refuse()
            os.close(directory)
            directory = child
        try:
            metadata = os.stat(components[-1], dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            return
        if kind == "directory":
            if _identity(metadata) != expected:
                _refuse()
            os.rmdir(components[-1], dir_fd=directory)
        else:
            if _read_file(directory, components[-1], allowed_modes=(stat.S_IMODE(expected[3]),), check=check)[:2] != (
                expected,
                digest,
            ):
                _refuse()
            os.unlink(components[-1], dir_fd=directory)
        os.fsync(directory)
    finally:
        os.close(directory)


def consume_inventory(context, check):
    """Retry only missing exact entries, keeping immutable custody evidence until last."""
    entries, digest = read_inventory(context)
    _check_remaining(context, entries, check)

    _consume_entries(context, entries, check)
    directory = _state_directory(context)
    try:
        _write_once(directory, _DRAINED, digest)
    finally:
        os.close(directory)
    return digest


def _consume_entries(context, entries, check):
    """Remove only validated exact entries; the caller owns its distinct evidence seal."""

    # Journal, descriptors and the lifetime lock are last so an interrupted first
    # cleanup keeps original authenticated evidence until the inventory is sealed.
    def order(entry):
        """Keep descriptors and journal files after data, then remove empty directories."""
        path = entry[0]
        if entry[1] == "directory":
            category = 3
        elif path.startswith("source/allocation-journal"):
            category = 2
        else:
            category = int(path.endswith(("source-ready.mpk", "owner-metadata.json")))
        return category, -path.count("/"), path

    by_path = {entry[0]: entry for entry in entries}
    for entry in sorted(entries, key=order):
        check()
        if entry[0] != ".owner-job.lock":
            _remove_entry(context, entry, by_path, check)
