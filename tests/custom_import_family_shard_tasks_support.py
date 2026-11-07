# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Spawned workers avoid importing the test module and its fixture dependencies."""

import os
import time
from pathlib import Path

from process.custom_import import family_shard_tasks as tasks
from process.custom_import.family_shard_worker import run_source_shard


def _slow_worker(plan, root, limits):
    time.sleep(3)
    return run_source_shard(plan, root, limits)


def _locked_source_worker(plan, root, limits):
    holder = tasks._WORKER_CUSTODY.owner_lock
    holder.check()
    (holder.root / f"observed-{os.getpid()}").touch()
    deadline = time.monotonic() + 15
    while len(tuple(holder.root.glob("observed-*"))) != 2:
        if time.monotonic() >= deadline:
            raise TimeoutError("both inherited worker locks were not observed")
        time.sleep(0.01)
    return run_source_shard(plan, root, limits)


def _crash_second_source_worker(plan, root, limits):
    if plan.shard.shard_id == 1:
        Path(root).mkdir(mode=0o700)
        (Path(root) / "interrupted").write_text("unfinished owned shard")
        os._exit(17)
    return run_source_shard(plan, root, limits)


def _refuse_completed_worker_execution(plan, root, limits):
    if plan.shard.shard_id == 0:
        raise AssertionError("completed source shard executed again")
    return run_source_shard(plan, root, limits)
