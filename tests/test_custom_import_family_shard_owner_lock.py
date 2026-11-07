# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real Unix descriptor lifetimes, without database or worker-pipeline mocks."""

import multiprocessing
import os
import pickle
import signal

import pytest

from process.custom_import import family_shard_owner_lock as locks


@pytest.fixture
def resources(tmp_path, request):
    processes, connections = [], []

    def cleanup():
        for process in processes:
            if process.pid is not None:
                if process.is_alive():
                    process.kill()
                process.join(10)
                assert not process.is_alive()
                process.close()
        for connection in connections:
            connection.close()

    request.addfinalizer(cleanup)
    root = tmp_path / "original-job"
    root.mkdir(mode=0o700)
    observed = root.stat()
    return root, (observed.st_dev, observed.st_ino), processes, connections


def _refused_adoption(handle):
    try:
        locks.adopt_worker_job_lock(handle)
    except ValueError:
        return "refused"
    raise AssertionError("invalid adoption succeeded")


def _worker(handle, connection):
    owned = None
    try:
        connection.send("inherited")
        while command := connection.recv():
            if command == "adopt":
                owned = locks.adopt_worker_job_lock(handle)
                connection.send("adopted")
                continue
            if command == "adopt-again":
                connection.send(_refused_adoption(handle))
                continue
            assert command == "close"
            if owned is None:
                owned = locks.adopt_worker_job_lock(handle)
            owned.close()
            connection.send("closed")
            return
    finally:
        if owned is not None:
            owned.close()
        connection.close()


def _refusing_worker(handle, connection):
    try:
        connection.send("inherited")
        assert connection.recv() == "adopt"
        connection.send(_refused_adoption(handle))
        assert connection.recv() == "exit"
    finally:
        connection.close()


def _spawn(context, owner, resources, *, target=_worker):
    _, _, processes, connections = resources
    parent, child = context.Pipe()
    connections.extend((parent, child))
    process = context.Process(target=target, args=(owner.worker_handle(), child))
    processes.append(process)
    process.start()
    child.close()
    return process, parent


def _inherited(connection):
    assert connection.poll(15), "worker did not inherit descriptor"
    assert connection.recv() == "inherited"


def _start(context, owner, resources, *, target=_worker):
    process, connection = _spawn(context, owner, resources, target=target)
    _inherited(connection)
    return process, connection


def _command(connection, command, expected):
    connection.send(command)
    assert connection.poll(15), "worker did not complete command"
    assert connection.recv() == expected


@pytest.mark.parametrize(
    "method", [name for name in ("spawn", "forkserver") if name in multiprocessing.get_all_start_methods()]
)
def test_coordinator_death_keeps_lock_in_all_workers_even_before_adoption(resources, method):
    root, identity, _, _ = resources
    context = multiprocessing.get_context(method)
    with locks.acquire_owner_job_lock(root, identity=identity) as owner:
        coordinator, coordinator_pipe = _spawn(context, owner, resources)
        first, first_pipe = _spawn(context, owner, resources)
        second, second_pipe = _spawn(context, owner, resources)
        for connection in (coordinator_pipe, first_pipe, second_pipe):
            _inherited(connection)
        _command(coordinator_pipe, "adopt", "adopted")
    coordinator.kill()
    coordinator.join(10)
    assert coordinator.exitcode == -signal.SIGKILL
    with pytest.raises(BlockingIOError):
        locks.acquire_owner_job_lock(root, identity=identity)
    _command(first_pipe, "adopt", "adopted")
    _command(first_pipe, "adopt-again", "refused")
    _command(first_pipe, "close", "closed")
    first.join(10)
    assert first.exitcode == 0
    with pytest.raises(BlockingIOError):
        locks.acquire_owner_job_lock(root, identity=identity)
    _command(second_pipe, "close", "closed")
    second.join(10)
    assert second.exitcode == 0
    with locks.acquire_owner_job_lock(root, identity=identity) as replacement:
        replacement.check()
    assert (root / locks._NAME).is_file()


def test_worker_close_never_unlocks_live_parent(resources):
    root, identity, _, _ = resources
    with locks.acquire_owner_job_lock(root, identity=identity) as owner:
        child, connection = _start(multiprocessing.get_context("spawn"), owner, resources)
        _command(connection, "adopt", "adopted")
        _command(connection, "close", "closed")
        child.join(10)
        assert child.exitcode == 0
        owner.check()
        with pytest.raises(BlockingIOError):
            locks.acquire_owner_job_lock(root, identity=identity)
    with locks.acquire_owner_job_lock(root, identity=identity) as replacement:
        replacement.check()


def test_startup_only_one_time_handle_and_closed_owner_refusal(resources):
    root, identity, _, _ = resources
    owner = locks.acquire_owner_job_lock(root, identity=identity)
    try:
        handle = owner.worker_handle()
        with pytest.raises(TypeError, match="startup"):
            pickle.dumps(handle)
        with pytest.raises(ValueError, match="inherited"):
            locks.adopt_worker_job_lock(handle)
        owner.close()
        owner.close()
        with pytest.raises(ValueError, match="closed"):
            owner.worker_handle()
        context = multiprocessing.get_context("spawn")
        child = context.Process(target=_worker, args=(handle, None))
        resources[2].append(child)
        with pytest.raises(ValueError, match="closed"):
            child.start()
    finally:
        owner.close()


def _unsafe_custody(root, identity, kind):
    path = root / locks._NAME
    if kind == "root-mode":
        root.chmod(0o750)
        return identity
    if kind == "root-inode":
        return identity[0], identity[1] + 1
    if kind == "symlink":
        target = root / "target"
        target.touch(mode=0o600)
        path.symlink_to(target.name)
        return identity
    path.touch(mode=0o640 if kind == "file-mode" else 0o600)
    if kind == "hardlink":
        os.link(path, root / "other-link")
    elif kind == "nonempty":
        path.write_text("existing")
    return identity


@pytest.mark.parametrize("kind", ["root-mode", "root-inode", "file-mode", "symlink", "hardlink", "nonempty"])
def test_unsafe_custody_refused_without_changing_existing_file(resources, kind):
    root, identity, _, _ = resources
    identity = _unsafe_custody(root, identity, kind)
    path = root / locks._NAME
    before = path.lstat() if path.exists() else None
    with pytest.raises((ValueError, OSError)):
        locks.acquire_owner_job_lock(root, identity=identity)
    if before is not None:
        assert path.lstat() == before


def test_file_replacement_during_acquire_is_refused_and_original_fd_closed(resources, monkeypatch):
    root, identity, _, _ = resources
    path, saved = root / locks._NAME, root / "saved-lock"
    original = locks.fcntl.flock

    def replace_after_lock(descriptor, operation):
        original(descriptor, operation)
        path.rename(saved)
        path.touch(mode=0o600)

    monkeypatch.setattr(locks.fcntl, "flock", replace_after_lock)
    with pytest.raises(ValueError, match="custody"):
        locks.acquire_owner_job_lock(root, identity=identity)
    monkeypatch.setattr(locks.fcntl, "flock", original)
    path.unlink()
    saved.rename(path)
    with locks.acquire_owner_job_lock(root, identity=identity) as replacement:
        replacement.check()


def test_wrong_owner_refused(resources, monkeypatch):
    root, identity, _, _ = resources
    uid = os.geteuid()
    monkeypatch.setattr(locks.os, "geteuid", lambda: uid + 1)
    with pytest.raises(ValueError, match="custody"):
        locks.acquire_owner_job_lock(root, identity=identity)


def test_live_file_identity_change_refused(resources):
    root, identity, _, _ = resources
    path, saved = root / locks._NAME, root / "saved-lock"
    with locks.acquire_owner_job_lock(root, identity=identity) as owner:
        path.rename(saved)
        path.touch(mode=0o600)
        with pytest.raises(ValueError, match="custody"):
            owner.check()
        path.unlink()
        saved.rename(path)
        owner.check()


def test_failed_worker_adoption_closes_descriptor_while_worker_lives(resources):
    root, identity, _, _ = resources
    path, saved = root / locks._NAME, root / "saved-lock"
    with locks.acquire_owner_job_lock(root, identity=identity) as owner:
        child, connection = _start(multiprocessing.get_context("spawn"), owner, resources, target=_refusing_worker)
        path.rename(saved)
        path.touch(mode=0o600)
        _command(connection, "adopt", "refused")
        path.unlink()
        saved.rename(path)
    assert child.is_alive()
    with locks.acquire_owner_job_lock(root, identity=identity) as replacement:
        replacement.check()
    connection.send("exit")
    child.join(10)
    assert child.exitcode == 0


def test_terminal_cleanup_refuses_remaining_artifacts_and_retains_lock(resources):
    root, identity, _, _ = resources
    artifact = root / "retained-output"
    with locks.acquire_owner_job_lock(root, identity=identity) as owner:
        artifact.touch()
        with pytest.raises(ValueError, match="contains artifacts"):
            owner.remove_terminal_root()
        owner.check()
        with pytest.raises(BlockingIOError):
            locks.acquire_owner_job_lock(root, identity=identity)
        artifact.unlink()
        owner.remove_terminal_root()
    assert not root.exists()


@pytest.mark.parametrize("operation", ["unlink", "rmdir"])
def test_terminal_deletion_failure_preserves_original_custody_for_retry(resources, monkeypatch, operation):
    root, identity, _, _ = resources
    owner = locks.acquire_owner_job_lock(root, identity=identity)
    original = getattr(locks.os, operation)

    def failed(*_args, **_kwargs):
        raise PermissionError("synthetic terminal deletion failure")

    try:
        monkeypatch.setattr(locks.os, operation, failed)
        with pytest.raises(PermissionError, match="terminal deletion"):
            owner.remove_terminal_root()
        assert root.exists() and owner._descriptor is not None
        assert (root.stat().st_dev, root.stat().st_ino) == identity
        if operation == "unlink":
            owner.check()
            with pytest.raises(BlockingIOError):
                locks.acquire_owner_job_lock(root, identity=identity)
        else:
            assert not (root / locks._NAME).exists()
            with pytest.raises(ValueError, match="terminal cleanup"):
                owner.worker_handle()
        monkeypatch.setattr(locks.os, operation, original)
        owner.remove_terminal_root()
        assert not root.exists() and owner._descriptor is None
    finally:
        owner.close()
