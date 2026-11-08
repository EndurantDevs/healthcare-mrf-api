# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed trusted terminal-file consumer; never opens a writer or continuation."""

import argparse
import asyncio
import os
import time
from pathlib import Path

from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_retirement_files as files
from process.custom_import import family_shard_retirement_handoff as custody
from process.custom_import import family_shard_terminal_files as producer
from process.custom_import.family_shard_owner_cli import profile, require_profile
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_retirement_native import read_retired_scope
from process.custom_import.family_shard_retirement_policy import original_policy

_SECONDS = 3600


def _finish_root(context, lock, digest):
    if lock is not None:
        lock.remove_terminal_root()
    elif os.path.lexists(context.root):
        # A prior attempt durably marked drained before unlinking its original
        # lock. Never recreate that lock after an interrupted final rmdir.
        files._directory(context.root, context.root_identity)
        if list(context.root.iterdir()):
            handoff.unavailable()
        context.root.rmdir()
    producer._sync_parent(context.root.parent)
    directory = files._state_directory(context)
    try:
        files._write_once(directory, files._DONE, digest)
    finally:
        os.close(directory)


def _seal_original(context, native, limits, work_mem_mb, check):
    """Discard unsealed staging only after the existing complete policy independently proves custody."""
    root = context.root / "source/allocation-journal/checkpoints"
    owner_sha, _bytes = checkpoint._owner_file(root, native.scope, limits.checkpoint, create=False)
    snapshot = checkpoint.CommittedOwnerCheckpoint(str(root), native.scope, native.head, owner_sha, limits.checkpoint)
    policy, original = original_policy(snapshot, context.root, limits, work_mem_mb, check)
    if original.lock_identity != context.lock_identity or original.directories[0][1] != context.root_identity:
        handoff.unavailable()
    producer.discard_unsealed(context)
    files.seal_inventory(context, policy, check)


def _consume(document, native, limits, work_mem_mb, parent=None, profile=None):
    """Consume exact retired custody, joining any earlier producer census before unlink."""
    context = custody.context(document, work=handoff.OWNER_PARENT)
    files._directory(context.state, context.state_identity)
    deadline, lock = time.monotonic() + _SECONDS, None

    def check():
        """Keep cleanup finite and refuse replacement of the original lifetime lock."""
        if time.monotonic() >= deadline:
            handoff.unavailable()
        if lock is not None:
            lock.check()

    state_names = set(os.listdir(context.state))
    try:
        if os.path.lexists(context.root) and os.path.lexists(context.root / ".owner-job.lock"):
            files._directory(context.root, context.root_identity)
            lock = acquire_owner_job_lock(context.root, identity=context.root_identity[:2])
            if lock.file_identity != context.lock_identity:
                handoff.unavailable()
        if files._SEALED in state_names:
            producer.is_rebind_resumed(context, check)
        elif producer.SEALED in state_names:
            producer.rebind(context, parent, profile, native, check)
        else:
            if lock is None:
                handoff.unavailable()
            _seal_original(context, native, limits, work_mem_mb, check)
        entries, digest = files.read_inventory(context)
        if lock is not None:
            files.consume_inventory(context, check)
        else:
            directory = files._state_directory(context)
            try:
                if files._read_file(directory, files._DRAINED, maximum=32)[2] != digest:
                    handoff.unavailable()
            finally:
                os.close(directory)
            if any(entry[0] == ".owner-job.lock" and entry[2][:2] != context.lock_identity for entry in entries):
                handoff.unavailable()
        _finish_root(context, lock, digest)
        lock = None
        completion_by_field = {
            "intent_sha256": custody.identity(document),
            "native_sha256": native.sha256,
            "inventory_sha256": digest.hex(),
        }
        directory = files._state_directory(context)
        try:
            files._write_once(directory, custody.COMPLETION, handoff.canonical(completion_by_field))
        finally:
            os.close(directory)
    finally:
        if lock is not None:
            lock.close()


async def run_cleanup(handoff_id, intent_sha256, database):
    """Authenticate immutable native retirement before any file census or unlink."""
    configuration, key, limits = profile()
    parent, bearer = handoff.reopen_job(handoff_id)
    handoff.verify(parent, verifying_key=key, bearer=bearer)
    require_profile(parent, configuration)
    document = custody.checked(custody.read(handoff.OWNER_INPUT, custody.INTENT))
    if document["handoff_id"] != handoff_id or custody.identity(document) != intent_sha256:
        handoff.unavailable()
    await database.connect()
    try:
        native = await read_retired_scope(database.session_factory, parent, configuration)
        if native.sha256 != document["native_sha256"] or document["maximum_bytes"] != configuration["disk_bytes"]:
            handoff.unavailable()
        await asyncio.to_thread(_consume, document, native, limits, configuration["work_mem_mb"], parent, configuration)
        return {"status": "retired_files_cleaned", "handoff_id": handoff_id, "intent_sha256": intent_sha256}
    finally:
        await database.disconnect()


def main(argv=None):
    """Run the fixed terminal operation and emit only its bounded safe receipt."""
    parser = argparse.ArgumentParser(description="Clean one permanently retired owner")
    parser.add_argument("handoff_id")
    parser.add_argument("intent_sha256")
    arguments = parser.parse_args(argv)
    from db.connection import db

    try:
        result = asyncio.run(run_cleanup(arguments.handoff_id, arguments.intent_sha256, db))
        print(handoff.canonical(result).decode("ascii"))
        return 0
    except Exception:
        print('{"status":"retired_files_unavailable"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
