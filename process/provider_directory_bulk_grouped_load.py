# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stage a merged-role source's saved PractitionerRole outputs once per merged role instead of once per line.

After the unchanged download, worker processes prepare each saved output and write its rows into group files by
merged role (group). Each partition is then reduced in a worker and appended in order by this process (load).
Last, every role output is completed in one transaction (finish). Markers on local disk make each stage resumable.
"""

from __future__ import annotations

import asyncio
import datetime
import multiprocessing
import os
from collections import deque
from compression import zstd
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import orjson

from process import provider_directory_bulk_grouping as grouping
from process import provider_directory_bulk_parallel as bulk_parallel
from process import provider_directory_bulk_spool as spool
from process.provider_directory_bulk_prepare import PrepareContext, persist_prepared_rows, prepare_lines
from process.provider_directory_fhir_response import BULK_EXPORT_WAIT_CALLBACK
from process.provider_directory_resource_hash import is_semantic_resource_hash_contract
from process.provider_directory_role_merge import is_role_repeat_source

WORKERS_ENV = "HLTHPRT_PROVIDER_DIRECTORY_BULK_GROUP_WORKERS"
# Cancellation and the deadline are observed this often while workers run; the lease is renewed less often.
_PROBE_SECONDS = 1.0
_RENEW_SECONDS = 60.0
_WORKER_STATE_BY_NAME = {}  # the cancel event, set in each worker by the pool initializer


def uses_grouped_load(directory, source_record, identity, options) -> bool:
    """Whether this saved export's outputs take the grouped load; the caller already checked the saved copies."""
    # shortcut: skips persist_batch's partial progress and linked-reference capture (linked_resource_limit), which
    # merged-role sources do not use today; route them through here before enabling a source that needs either.
    if not (
        is_role_repeat_source([source_record.get("source_id")])
        and identity.resource_type == "PractitionerRole"
        and not options.retain_rows
        # Reverse-lookup sources capture role rows through the row handler, which the grouped load bypasses.
        and not directory._practitioner_role_reverse_lookup_resources(source_record)
    ):
        return False
    # Without a semantic contract roles are not merged, and the reducer would refuse every unmerged role.
    scope = directory._endpoint_dataset_write_scope(source_record)
    return scope is not None and is_semantic_resource_hash_contract(scope.resource_hash_contract)


def group_workers() -> int:
    """Worker processes for the Job's CPU limit, one CPU left for this process; 0 runs the work in process."""
    if configured := os.getenv(WORKERS_ENV):
        if int(configured) < 0:
            raise ValueError("bulk_export_group_workers_invalid")
        return int(configured)
    try:
        # os.process_cpu_count() reports the host's CPUs, not the container's limit.
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        return max(1, int(quota) // int(period) - 1)
    except OSError, ValueError:  # no cgroup v2 file, or an unlimited quota ("max")
        return 1


@dataclass
class _Load:
    copy: Any  # the bulk_parallel copy context of this export
    stream_state: Any
    group_root: Path
    workers: int
    partitions: int
    renewed_at: float
    progress_by_field: dict[str, Any] = field(default_factory=dict)


async def grouped_export(directory, session, source_record, identity, manifest, options, stream_state):
    """Every role output of the export waits on one shared group and load; its success completes all of them."""
    load = _Load(
        bulk_parallel._CopyContext(directory, session, source_record, identity, manifest, options),
        stream_state,
        spool.group_directory(identity.checkpoint_id),
        group_workers(),
        grouping.PARTITIONS,
        directory.time.monotonic(),
    )
    return await bulk_parallel.shared_task(
        directory, source_record, "grouped_exports", identity.checkpoint_id, partial(_grouped_load, load)
    )


async def _grouped_load(load):
    directory, checkpoint_id = load.copy.directory, load.copy.identity.checkpoint_id
    checkpoints = await directory._load_bulk_output_checkpoints(checkpoint_id)
    output_pairs = [
        (output, directory._bulk_output_checkpoint_for_manifest_output(checkpoint_id, output, checkpoints))
        for output in load.copy.manifest.outputs
    ]
    mode_by_field = {"partitions": load.partitions, "version": 1}
    if error := await asyncio.to_thread(
        _claim_mode, load.group_root, mode_by_field, [pair[1] for pair in output_pairs]
    ):
        return error
    if error := await bulk_parallel._spool_export(load.copy):
        return error
    pending_pairs = [pair for pair in output_pairs if not directory._is_complete_bulk_output_proof(pair[1])]
    cancel = multiprocessing.get_context("spawn").Event()
    executor = _executor(load.workers, cancel)
    try:
        error = await _group(load, executor, pending_pairs) or await _load_partitions(load, executor, pending_pairs)
    finally:
        cancel.set()  # running tasks stop at their next batch; queued ones are cancelled by the drain
        from process.provider_directory_cms_npd import _drain_stage_executor

        await _drain_stage_executor(executor)
    if error:
        return error
    await _finish(load, pending_pairs)
    return None


def _claim_mode(group_root, mode_by_field, checkpoints):
    """Keep one mode per acquisition: an output staged by the ordinary stream would be counted twice at finish."""
    mode_path = group_root / "mode"
    if mode_path.exists():
        return None if orjson.loads(mode_path.read_bytes()) == mode_by_field else "bulk_export_group_mode_changed"
    if any(checkpoint.get("committed_bytes") or checkpoint.get("rows_written") for checkpoint in checkpoints):
        return "bulk_export_group_mode_changed"
    group_root.mkdir(parents=True, exist_ok=True)
    grouping._write_atomically(mode_path, orjson.dumps(mode_by_field))
    return None


def _executor(workers, cancel):
    if workers == 0:
        return ThreadPoolExecutor(1, initializer=_set_cancel, initargs=(cancel,))
    spawn = multiprocessing.get_context("spawn")
    return ProcessPoolExecutor(workers, mp_context=spawn, initializer=_set_cancel, initargs=(cancel,))


def _set_cancel(cancel):
    _WORKER_STATE_BY_NAME["cancel"] = cancel


def _write_scope(load):
    directory, source_record = load.copy.directory, load.copy.source_record
    scope = directory._endpoint_dataset_write_scope(source_record)
    if scope is None or scope.resource_hash_contract is None:
        raise RuntimeError("provider_directory_output_snapshot_dataset_scope_required")
    return scope


def _prepare_context(load):
    directory, source_record, scope = load.copy.directory, load.copy.source_record, _write_scope(load)
    projection_as_of = directory._source_semantic_projection_as_of(source_record)
    return PrepareContext(
        source_id=source_record["source_id"],
        resource_type=load.copy.identity.resource_type,
        run_id=load.copy.options.run_id,
        log_identity=None,
        dataset_id=scope.dataset_id,
        resource_hash_contract=scope.resource_hash_contract,
        semantic_projection_as_of=projection_as_of.isoformat() if projection_as_of else None,
        merge_roles=is_role_repeat_source([source_record["source_id"]]),
        strict=True,  # a checkpointed output always has a progress handler
    )


async def _group(load, executor, pending_pairs):
    """Group every saved output that has no marker yet, one output per worker task."""
    directory, loop, base_context = load.copy.directory, asyncio.get_running_loop(), _prepare_context(load)
    futures = set()
    for output, checkpoint in pending_pairs:
        validator = directory._stored_bulk_output_validator(checkpoint)
        spooled = spool.spooled_output(load.copy.identity.checkpoint_id, output.output_index, validator)
        if grouping.is_grouped(load.group_root, output.output_index):
            spooled.path.unlink(missing_ok=True)  # the copy outlived its marker when the worker stopped in between
            continue
        context = replace(base_context, log_identity=directory._bulk_capability_log_identity(output.url))
        arguments = (spooled.path, load.group_root, output.output_index, spooled.content_length, context)
        futures.add(
            loop.run_in_executor(executor, group_output, *arguments, load.copy.options.row_batch_size, load.partitions)
        )
    load.progress_by_field = {"stage": "group", "completed": 0, "total": len(futures)}
    while futures:
        completed = await _wait_probing(load, futures)
        futures -= completed
        load.progress_by_field["completed"] += len(completed)
        for future in completed:
            if error := _worker_error(future):
                return error
    return None


def group_output(spool_path, group_root, output_index, content_length, context, batch_size, partitions):
    """Worker task: prepare one saved output into its group files, then free its copy; returns an error or None."""
    writer = grouping.GroupWriter(group_root, output_index, partitions)
    fetched_count = read_bytes = 0
    lines = []
    with zstd.open(spool_path, "rb") as handle:
        for line in handle:
            read_bytes += len(line)
            lines.append(line)
            if len(lines) < max(1, batch_size):
                continue
            prepared = prepare_lines(lines, context)
            if prepared.error:
                return prepared.error
            writer.write(prepared.rows)
            fetched_count += prepared.rows_fetched
            lines = []
            if (cancel := _WORKER_STATE_BY_NAME.get("cancel")) is not None and cancel.is_set():
                return "bulk_export_group_cancelled"
    prepared = prepare_lines(lines, context)
    if prepared.error:
        return prepared.error
    if read_bytes != content_length:
        return "bulk_export_output_length_mismatch"
    writer.write(prepared.rows)
    writer.finish(fetched_count + prepared.rows_fetched)
    Path(spool_path).unlink(missing_ok=True)  # the marker is durable, so the copy is no longer needed
    return None


def _worker_error(future):
    """Map a worker's failure as the streaming path maps a failed batch."""
    try:
        return future.result()
    except BrokenProcessPool as exc:
        raise RuntimeError("bulk_export_group_worker_lost") from exc
    except ValueError as exc:
        return str(exc)


async def _wait_probing(load, futures):
    """Wait for any worker task while checking ownership as a stream does; no rows are written meanwhile."""
    directory = load.copy.directory
    while True:
        completed, _ = await asyncio.wait(futures, timeout=_PROBE_SECONDS, return_when=asyncio.FIRST_COMPLETED)
        await bulk_parallel._stream_probe(load.copy)
        if directory.time.monotonic() - load.renewed_at >= _RENEW_SECONDS:
            await bulk_parallel._renew_lease(load.copy)
            await _report_progress(load)
            load.renewed_at = directory.time.monotonic()
        if completed:
            return completed


async def _report_progress(load):
    """Keep the run's progress live while no row counter moves."""
    if (callback := BULK_EXPORT_WAIT_CALLBACK.get()) is not None:
        observed_at = datetime.datetime.now(datetime.UTC).isoformat()
        resource_type = load.copy.identity.resource_type
        await callback({"resource": resource_type, "observed_at": observed_at, **load.progress_by_field})


async def _load_partitions(load, executor, pending_pairs):
    """Reduce partitions in workers, a few ahead, and append each one in partition order."""
    loop, contract = asyncio.get_running_loop(), _write_scope(load).resource_hash_contract
    output_indexes = [output.output_index for output, _ in pending_pairs]
    partitions = [
        partition for partition in range(load.partitions) if not grouping.is_loaded(load.group_root, partition)
    ]
    load.progress_by_field = {"stage": "load", "completed": 0, "total": len(partitions)}
    in_flight = deque()
    for partition in partitions:
        reduce = partial(
            grouping.reduce_partition, load.group_root, partition, output_indexes, contract, load.partitions
        )
        in_flight.append((partition, loop.run_in_executor(executor, reduce)))
        if len(in_flight) < max(1, load.workers):
            continue
        if error := await _append_partition(load, *in_flight.popleft()):
            return error
    while in_flight:
        if error := await _append_partition(load, *in_flight.popleft()):
            return error
    return None


async def _append_partition(load, partition, future):
    await _wait_probing(load, {future})
    try:
        reduced_rows = future.result()
    except BrokenProcessPool as exc:
        raise RuntimeError("bulk_export_group_worker_lost") from exc
    except ValueError as exc:
        return str(exc)
    await _append(load, reduced_rows)
    await load.copy.options.ownership_probe()
    await asyncio.to_thread(grouping.mark_loaded, load.group_root, partition)
    load.progress_by_field["completed"] += 1
    return None


async def _append(load, reduced_rows):
    """Append through the streaming path's transaction, writer slot, ownership check and lease renewal."""
    directory, options, scope = load.copy.directory, load.copy.options, _write_scope(load)
    batch_size = max(1, options.row_batch_size)
    for offset in range(0, len(reduced_rows), batch_size):
        batch = reduced_rows[offset : offset + batch_size]
        transaction = directory.source_checkpoint_transaction(
            directory.db, schema=directory._schema(), is_batch_loading=True
        )
        async with bulk_parallel.write_slot(directory, load.copy.source_record), transaction:
            await options.ownership_probe()
            await persist_prepared_rows(
                directory,
                batch,
                dataset_id=scope.dataset_id,
                resource_hash_contract=scope.resource_hash_contract,
                semantic_projection_as_of=scope.semantic_projection_as_of,
            )
            await directory._refresh_bulk_export_rows_written(load.copy.identity)
        load.stream_state.rows_written_this_run += len(batch)


async def _finish(load, pending_pairs):
    """Complete every role output in one transaction with the staged row count, then delete the local files."""
    directory, identity, options = load.copy.directory, load.copy.identity, load.copy.options
    # Copy names need the validator, which completion clears.
    copy_paths = [
        spool.spooled_output(
            identity.checkpoint_id, output.output_index, directory._stored_bulk_output_validator(checkpoint)
        ).path
        for output, checkpoint in pending_pairs
    ]
    await asyncio.to_thread(_discard_group_files, load.group_root, copy_paths, pending_pairs)
    from process.provider_directory_generic_source_adapters import candidate_resource_count

    count_scope = SimpleNamespace(
        dataset_id=identity.dataset_id,
        endpoint_id=identity.endpoint_id,
        acquisition_root_run_id=identity.acquisition_root_run_id,
        source_ids=list(
            load.copy.source_record.get("_partition_checkpoint_source_ids") or [load.copy.source_record["source_id"]]
        ),
    )
    async with directory.source_checkpoint_transaction(
        directory.db, schema=directory._schema(), bulk_identity=identity
    ):
        staged_count = await candidate_resource_count(directory, count_scope, identity.resource_type, distinct=False)
        for (output, checkpoint), rows_written in zip(
            pending_pairs, _split(directory, staged_count, pending_pairs), strict=True
        ):
            content_length = directory._stored_bulk_output_validator(checkpoint).content_length_bytes
            await options.ownership_probe()
            await directory._begin_bulk_export_output(identity, checkpoint["output_id"], preserve_progress=True)
            await directory._complete_bulk_export_output(
                identity, checkpoint["output_id"], rows_written, content_length, require_validator=True
            )
    load.stream_state.outputs_processed += len(pending_pairs)
    directory._bulk_export_log(
        "grouped_load", resource=identity.resource_type, outputs=len(pending_pairs), staged=staged_count
    )
    await asyncio.to_thread(grouping.discard, load.group_root.parent)


def _discard_group_files(group_root, copy_paths, pending_pairs):
    """Delete copies and group files; the markers stay until the outputs are complete."""
    for copy_path in copy_paths:
        copy_path.unlink(missing_ok=True)
    for output, _ in pending_pairs:
        grouping.discard(group_root / str(output.output_index))


def _split(directory, staged_count, pending_pairs):
    """Spread the staged rows over the outputs, none above its byte length, as the transition census requires."""
    shares = []
    for _, checkpoint in pending_pairs:
        shares.append(
            min(staged_count - sum(shares), directory._stored_bulk_output_validator(checkpoint).content_length_bytes)
        )
    if sum(shares) != staged_count:
        raise ValueError("bulk_export_group_rows_exceed_bytes")
    return shares
