# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stream CMS NPD release files through the typed encoder into binary COPY.

Per file: a thread decompresses the zstd file into the encoder's stdin; the encoder writes one
PostgreSQL binary COPY stream per output table into a pipe; each pipe feeds ``copy_to_table`` on
its own connection. Nothing is written to disk between the source file and PostgreSQL.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from compression import zstd
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path

from process.cms_npd_source import ObservedRelease
from process.cms_npd_typed_schema import RESOURCE_FILES

STAGE_CONCURRENCY = 3
CHUNK_BYTES = 1024 * 1024


class CmsNpdTypedImportError(RuntimeError):
    """A failed typed import; ``control_error_code`` is the stable reason."""

    def __init__(self, code, detail=None):
        super().__init__(code if detail is None else f"{code}: {detail}")
        self.control_error_code = code


@dataclass(frozen=True)
class FilePlan:
    name: str  # manifest file name, e.g. "01-Organization.ndjson"
    resource_type: str
    main_table: str
    address_table: str | None
    path: Path
    compressed_bytes: int
    original_bytes: int
    etag: str

    @property
    def tables(self):
        """Output tables in encoder stream order: the main table, then the address table if any."""
        return (self.main_table,) if self.address_table is None else (self.main_table, self.address_table)


def file_plan(directory, observed: ObservedRelease):
    """Files in staging order (largest first), with the manifest sizes each must match."""
    entries_by_name = {
        spec.name: (spec, probe) for spec, probe in zip(observed.manifest.files, observed.probes, strict=True)
    }
    plans = []
    for stem, resource_type, main_table, address_table in RESOURCE_FILES:
        spec, probe = entries_by_name[f"{stem}.ndjson"]
        if spec.resource_type != resource_type:
            raise CmsNpdTypedImportError("cms_npd_typed_manifest_mismatch", spec.name)
        path = Path(directory) / f"{spec.name}.zst"
        plans.append(
            FilePlan(
                spec.name,
                resource_type,
                main_table,
                address_table,
                path,
                spec.compressed_bytes,
                spec.original_bytes,
                probe.etag,
            )
        )
    return tuple(plans)


def encoder_threads(cpu_count, concurrency):
    """Encoder threads per concurrent file so that all encoders together use about every CPU."""
    return max(1, (cpu_count or 1) // max(1, concurrency))


async def stage_files(plans, schema, connect, scanner, *, concurrency=STAGE_CONCURRENCY, on_file_done=None):
    """Stage every file, at most ``concurrency`` at a time in plan order; return rows per table."""
    limit = asyncio.Semaphore(concurrency)
    threads = encoder_threads(os.cpu_count(), concurrency)
    counts_by_table, finished_names = {}, []

    async def _stage_when_allowed(plan):
        async with limit:
            counts_by_table.update(await copy_file_into_tables(plan, schema, connect, scanner, threads))
        finished_names.append(plan.name)
        if on_file_done is not None:
            await on_file_done(plan, len(finished_names))

    try:
        async with asyncio.TaskGroup() as group:
            for plan in plans:
                group.create_task(_stage_when_allowed(plan))
    except ExceptionGroup as error:
        raise error.exceptions[0] from error
    return counts_by_table


@dataclass
class _Streams:
    """Pipes, connections and the encoder process of one file; ``close`` releases whatever is left."""

    output_pipes: list
    stdin_read: int
    stdin_write: int
    open_fds: set
    connections: list
    transports: list
    process: asyncio.subprocess.Process | None = None

    @classmethod
    def create(cls, table_count):
        """Open one pipe per output table plus the encoder stdin pipe."""
        output_pipes = [os.pipe() for _ in range(table_count)]
        stdin_read, stdin_write = os.pipe()
        open_fds = {stdin_read, stdin_write, *(fd for pipe in output_pipes for fd in pipe)}
        return cls(output_pipes, stdin_read, stdin_write, open_fds, [], [])

    def hand_over(self, fd):
        """Stop tracking an fd that another owner (the encoder, a transport, the feeder) now closes."""
        self.open_fds.discard(fd)

    async def close(self):
        """Kill a running encoder, abort unfinished COPYs and close every remaining fd."""
        if self.process is not None and self.process.returncode is None:
            self.process.kill()
            await self.process.wait()
        for transport in self.transports:
            transport.close()
        for connection in self.connections:
            connection.terminate()  # aborts an unfinished COPY server-side
        for fd in self.open_fds:
            os.close(fd)


async def copy_file_into_tables(plan, schema, connect, scanner, threads):
    """Encode one file into its tables and verify bytes and rows against the manifest and encoder."""
    if not plan.path.is_file() or plan.path.stat().st_size != plan.compressed_bytes:
        raise CmsNpdTypedImportError("cms_npd_typed_download_size_mismatch", plan.name)
    streams = _Streams.create(len(plan.tables))
    try:
        for _ in plan.tables:  # appended one by one so a failed connect still closes the earlier ones
            streams.connections.append(await connect())
        streams.process = await _spawn_encoder(plan, scanner, threads, streams)
        readers = [await _open_reader(read_fd, streams) for read_fd, _ in streams.output_pipes]
        fed = _start_feeder(plan.path, streams.stdin_write)
        streams.hand_over(streams.stdin_write)
        copied, (stdout, stderr), decoded = await _run_streams(plan, schema, streams.connections, readers, fed, streams)
        counts_by_table = _verified_counts(plan, streams.process.returncode, stdout, stderr, copied, decoded)
        for connection in streams.connections:
            await connection.close()
        streams.connections = []
        return counts_by_table
    finally:
        await streams.close()


async def _spawn_encoder(plan, scanner, threads, streams):
    write_fds = [write_fd for _, write_fd in streams.output_pipes]
    process = await asyncio.create_subprocess_exec(
        str(scanner),
        "--cms-npd-typed-copy",
        plan.resource_type,
        *(f"/dev/fd/{write_fd}" for write_fd in write_fds),
        stdin=streams.stdin_read,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        pass_fds=write_fds,
        env={**os.environ, "RAYON_NUM_THREADS": str(threads)},
    )
    for fd in (streams.stdin_read, *write_fds):
        os.close(fd)  # the encoder holds these now; COPY sees EOF only once they are all closed
        streams.hand_over(fd)
    return process


async def _run_streams(plan, schema, connections, readers, fed, streams):
    process = streams.process
    try:
        async with asyncio.TaskGroup() as group:
            copies = [
                group.create_task(_copy(connection, schema, table, reader))
                for connection, table, reader in zip(connections, plan.tables, readers, strict=True)
            ]
            scanner = group.create_task(process.communicate())
            feeder = group.create_task(_wait_future(fed))
    except ExceptionGroup as error:
        await _raise_scanner_failure(plan, process, error)
        raise error.exceptions[0] from error
    return [task.result() for task in copies], scanner.result(), feeder.result()


async def _raise_scanner_failure(plan, process, error):
    """A truncated COPY usually means the encoder stopped: report the encoder's own reason."""
    try:
        await asyncio.wait_for(process.wait(), 2)
    except TimeoutError:
        return
    if process.returncode != 0:
        stderr = await process.stderr.read() if process.stderr is not None else b""
        raise CmsNpdTypedImportError("cms_npd_typed_scanner_failed", _scanner_detail(plan, stderr)) from error


def _verified_counts(plan, returncode, stdout, stderr, copied, decoded):
    if returncode != 0:
        raise CmsNpdTypedImportError("cms_npd_typed_scanner_failed", _scanner_detail(plan, stderr))
    try:
        summary = json.loads(stdout.decode().strip().splitlines()[-1])
    except (ValueError, IndexError, UnicodeError) as error:
        raise CmsNpdTypedImportError("cms_npd_typed_scanner_summary_invalid", plan.name) from error
    if summary.get("resource_type") != plan.resource_type:
        raise CmsNpdTypedImportError("cms_npd_typed_scanner_summary_invalid", plan.name)
    if not decoded == summary.get("input_bytes") == plan.original_bytes:
        raise CmsNpdTypedImportError("cms_npd_typed_decoded_size_mismatch", plan.name)
    expected = [summary.get("rows"), summary.get("address_rows")][: len(plan.tables)]
    if copied != expected or (plan.address_table is None and summary.get("address_rows") != 0):
        raise CmsNpdTypedImportError("cms_npd_typed_copy_count_mismatch", plan.name)
    return dict(zip(plan.tables, copied, strict=True))


def _scanner_detail(plan, stderr):
    lines = stderr.decode(errors="replace").strip().splitlines()
    return f"{plan.name}: {(lines[-1] if lines else 'no message')[:500]}"


async def _copy(connection, schema, table, reader):
    status = await connection.copy_to_table(table, schema_name=schema, source=_chunks(reader), format="binary")
    try:
        return int(status.removeprefix("COPY "))
    except (AttributeError, ValueError) as error:
        raise CmsNpdTypedImportError("cms_npd_typed_copy_status_invalid", table) from error


async def _chunks(reader):
    while chunk := await reader.read(CHUNK_BYTES):
        yield chunk


async def _open_reader(fd, streams):
    """Read a pipe on the event loop: no executor thread waits on encoder output."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=CHUNK_BYTES)
    pipe = open(fd, "rb", buffering=0)
    streams.hand_over(fd)
    try:
        transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), pipe)
    except BaseException:
        pipe.close()
        raise
    streams.transports.append(transport)
    return reader


def _start_feeder(path, fd):
    """Decompress on a dedicated thread (not the default executor) and return its decoded byte count."""
    result = Future()

    def _feed():
        try:
            result.set_result(_decompress_into(path, fd))
        except BaseException as error:  # handed to the awaiting task
            result.set_exception(error)

    threading.Thread(target=_feed, name=f"cms-npd-feed-{path.name}", daemon=True).start()
    return result


def _decompress_into(path, fd):
    decoded = 0
    try:
        with zstd.open(path, "rb") as source:
            while chunk := source.read(CHUNK_BYTES):
                view = memoryview(chunk)
                while view:
                    view = view[os.write(fd, view) :]
                decoded += len(chunk)
    except BrokenPipeError:
        return decoded  # the encoder stopped; its exit status carries the reason
    except (EOFError, zstd.ZstdError) as error:
        raise CmsNpdTypedImportError("cms_npd_typed_zstd_invalid", path.name) from error
    finally:
        os.close(fd)
    return decoded


async def _wait_future(future):
    return await asyncio.wrap_future(future)
