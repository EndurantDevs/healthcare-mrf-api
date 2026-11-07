# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unix lifetime lock shared with workers at spawn/forkserver process startup.

Close only after local work drains. Never explicitly unlock or unlink during
recoverable progress: surviving workers retain the same open-file-description
lock. Handles belong in initializer arguments, not queued tasks or artifacts.
"""

import fcntl
import os
import stat
from multiprocessing.context import get_spawning_popen
from multiprocessing.reduction import DupFd
from pathlib import Path

_NAME = ".owner-job.lock"
_ERROR = "owner job lock custody differs"


def _identity(observed):
    return observed.st_dev, observed.st_ino


def _check_root(directory, root, identity):
    opened, named = os.fstat(directory), root.lstat()
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(named.st_mode)
        or _identity(opened) != identity
        or _identity(named) != identity
        or opened.st_uid != os.geteuid()
        or stat.S_IMODE(opened.st_mode) != 0o700
    ):
        raise ValueError(_ERROR)


def _open_root(root, identity):
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        _check_root(directory, root, identity)
        return directory
    except BaseException:
        os.close(directory)
        raise


def _check_file(descriptor, directory, identity):
    opened = os.fstat(descriptor)
    named = os.stat(_NAME, dir_fd=directory, follow_symlinks=False)
    if (
        not stat.S_ISREG(opened.st_mode)
        or _identity(opened) != identity
        or _identity(named) != identity
        or opened.st_uid != os.geteuid()
        or stat.S_IMODE(opened.st_mode) != 0o600
        or opened.st_nlink != 1
        or opened.st_size != 0
    ):
        raise ValueError(_ERROR)


class OwnerJobLock:
    """An owned descriptor; closing one copy does not release workers' copies."""

    def __init__(self, root, identity, file_identity, directory, descriptor):
        self.root, self.identity, self.file_identity = root, identity, file_identity
        self._directory, self._descriptor = directory, descriptor
        self._terminal_unlinked = False

    def check(self):
        """Refuse closed or replaced directory/file custody without relocking."""
        if self._descriptor is None:
            raise ValueError("owner job lock is closed")
        if self._terminal_unlinked:
            raise ValueError("owner job lock terminal cleanup has started")
        _check_root(self._directory, self.root, self.identity)
        _check_file(self._descriptor, self._directory, self.file_identity)

    def worker_handle(self):
        """Borrow until process start completes; child must adopt before work."""
        self.check()
        return _WorkerHandle(self)

    def close(self):
        """Close only this copy; caller must first drain its local artifact work."""
        if self._descriptor is not None:
            descriptor, directory = self._descriptor, self._directory
            self._descriptor = self._directory = None
            try:
                os.close(descriptor)
            finally:
                os.close(directory)

    def remove_terminal_root(self):
        """Remove the exact empty owner root only after the caller drains all consumers.

        A failed deletion retains descriptors for terminal cleanup retry. An
        unlinked terminal entry is never recreated or reused for worker startup.
        """
        if not self._terminal_unlinked:
            self.check()
            if os.listdir(self._directory) != [_NAME]:
                raise ValueError("owner job root still contains artifacts")
            os.unlink(_NAME, dir_fd=self._directory)
            self._terminal_unlinked = True
        _check_root(self._directory, self.root, self.identity)
        os.rmdir(self.root)
        self.close()

    def __enter__(self):
        self.check()
        return self

    def __exit__(self, *exception):
        self.close()


class _WorkerHandle:
    def __init__(self, owner):
        self._owner, self._descriptor = owner, None

    def __reduce__(self):
        if self._owner is None or get_spawning_popen() is None:
            raise TypeError("owner job lock handles require spawn/forkserver startup")
        owner = self._owner
        owner.check()
        return _inherited_handle, (DupFd(owner._descriptor), owner.root, owner.identity, owner.file_identity)


def _inherited_handle(descriptor, root, identity, file_identity):
    handle = _WorkerHandle(None)
    handle._descriptor = descriptor
    handle.root, handle.identity, handle.file_identity = root, identity, file_identity
    return handle


def acquire_owner_job_lock(root, *, identity):
    """Nonblocking exclusive acquire for an existing private, pinned directory."""
    root = Path(root).absolute()
    directory = _open_root(root, identity)
    descriptor = None
    try:
        descriptor = os.open(_NAME, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        file_identity = _identity(os.fstat(descriptor))
        _check_file(descriptor, directory, file_identity)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock = OwnerJobLock(root, identity, file_identity, directory, descriptor)
        lock.check()
        return lock
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory)
        raise


def adopt_worker_job_lock(handle):
    """Adopt the already inherited OFD without acquiring or unlocking it."""
    if type(handle) is not _WorkerHandle or handle._owner is not None or handle._descriptor is None:
        raise ValueError("owner job lock handle is not an unused inherited handle")
    inherited, handle._descriptor = handle._descriptor, None
    descriptor, directory = inherited.detach(), None
    try:
        os.set_inheritable(descriptor, False)
        directory = _open_root(handle.root, handle.identity)
        lock = OwnerJobLock(handle.root, handle.identity, handle.file_identity, directory, descriptor)
        lock.check()
        return lock
    except BaseException:
        os.close(descriptor)
        if directory is not None:
            os.close(directory)
        raise
