# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bulk import of one CMS National Provider Directory release into its own typed schema.

observe -> download -> stage (native binary COPY) -> finish (keys, indexes) -> check -> publish -> addresses.
One session-level advisory lock covers the whole run. A failed run drops its candidate schema;
the serving release and its predecessor are never touched until the one-transaction publish.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import shutil
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import httpx

from process import cms_npd_source as source_module
from process import cms_npd_typed_addresses as addresses
from process import cms_npd_typed_release as release_sql
from process.cms_npd_typed_schema import release_schema_name
from process.cms_npd_typed_stage import (
    STAGE_CONCURRENCY,
    CmsNpdTypedImportError,
    file_plan,
    stage_files,
)
from process.live_progress import write_phase_progress
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary

__all__ = ["CmsNpdTypedImportError", "run"]

DOWNLOAD_PARALLEL = 4
LOCK_KEY = int.from_bytes(hashlib.sha256(b"cms_npd_typed_import").digest()[:8], "big", signed=True)
_RELEASE_DIRECTORY = re.compile(r"[0-9a-f]{64}")


class HttpSource:
    """The public CMS download site; release directories live under the durable artifact root."""

    def __init__(self, root, base_url=source_module.DOWNLOADS_URL):
        self.root = Path(root)
        self.base_url = base_url

    def observe(self):
        """Read the manifest and probe every file (no download)."""
        with httpx.Client(timeout=httpx.Timeout(60.0, connect=30.0)) as client:
            return source_module.observe_release(client=client, base_url=self.base_url)

    def fetch(self, observed):
        """Download or resume the observed release; return its directory."""
        return source_module.download_release(self.root, observed, base_url=self.base_url, parallel=DOWNLOAD_PARALLEL)

    def prune(self, keep_release_ids):
        """Delete release directories other than the kept ones (the current and previous release)."""
        releases = self.root / "cms-npd" / "releases"
        for directory in releases.iterdir() if releases.is_dir() else ():
            if (
                _RELEASE_DIRECTORY.fullmatch(directory.name)
                and directory.name not in keep_release_ids
                and directory.is_dir()
                and not directory.is_symlink()
            ):
                shutil.rmtree(directory, ignore_errors=True)


def _artifact_root():
    """Own subdirectory, so pruning never touches files of other importers.

    Without a configured durable root the files go to the temporary directory and
    are simply downloaded again by a later worker.
    """
    configured = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT") or tempfile.gettempdir()
    root = Path(configured) / "cms-npd-typed"
    root.mkdir(parents=True, exist_ok=True)
    return root


async def connect_default():
    """A dedicated asyncpg connection with the application's configured credentials."""
    from db.models import db

    if db.engine is None:
        await db.connect()
    return await asyncpg.connect(dsn=db.engine.url.set(drivername="postgresql").render_as_string(hide_password=False))


def _scanner_binary():
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise CmsNpdTypedImportError("cms_npd_typed_scanner_missing")
    return binary


@asynccontextmanager
async def _phase(run_id, seconds, name):
    await write_phase_progress(run_id, f"cms-npd-typed-{name}", unit="phase", done=0, total=1, message=name)
    started = time.monotonic()
    yield
    seconds[name] = round(time.monotonic() - started, 3)


async def run(ctx, task, run_id, *, source=None, connect=None, scanner=None, resolve_archive=None):
    """Import the upstream release unless it is already current; return the release summary.

    ``task["refresh_addresses"] = False`` skips queueing the entity-address refresh; it stays unrecorded,
    so a later run without the flag queues it.
    """
    source = source or HttpSource(_artifact_root())
    connect = connect or connect_default
    scanner = scanner or _scanner_binary()
    seconds_by_phase = {}
    async with _phase(run_id, seconds_by_phase, "observe"):
        observed = await asyncio.to_thread(source.observe)
    lock_connection = await connect()
    try:
        if not await lock_connection.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_KEY):
            raise CmsNpdTypedImportError("cms_npd_typed_import_in_progress")
        summary = await _import_locked(lock_connection, observed, source, connect, scanner, run_id, seconds_by_phase)
        return await _addresses_locked(lock_connection, summary, task, run_id, resolve_archive)
    finally:
        await lock_connection.close()  # also releases the session advisory lock


async def _import_locked(connection, observed, downloads, connect, scanner, run_id, seconds):
    """Publish the observed release unless it is current; report what the address phase still owes."""
    control = release_sql.control_schema()
    release_id = observed.vector_sha256
    await release_sql.ensure_release_table(connection, control)
    releases_by_status = await release_sql.release_rows(connection, control)
    current, previous = releases_by_status.get("current"), releases_by_status.get("previous")
    if current is not None and current["release_id"] == release_id:
        return await _unchanged(connection, control, current, seconds)
    if previous is not None and previous["release_id"] == release_id:
        # shortcut: upstream went back to the previous release; an operator decides whether to roll back.
        raise CmsNpdTypedImportError("cms_npd_typed_release_is_previous")
    schema = release_schema_name(release_id)
    if schema in {recorded["schema_name"] for recorded in releases_by_status.values()}:
        raise CmsNpdTypedImportError("cms_npd_typed_schema_collision")
    async with _phase(run_id, seconds, "download"):
        directory = await asyncio.to_thread(downloads.fetch, observed)
    plans = file_plan(directory, observed)
    try:
        row_counts, unresolved = await _build_candidate(connection, schema, plans, connect, scanner, run_id, seconds)
        release_sql.assert_publishable(row_counts, current)
        await release_sql.grant_api_read(connection, schema)
    except BaseException:
        await _drop_candidate(connect, schema)
        raise
    release_by_field = {
        "release_id": release_id,
        "schema": schema,
        "generated_at": observed.manifest.generated_at,
        "run_id": run_id,
        "row_counts": row_counts,
        "unresolved_counts": unresolved,
        "files": _file_summary(plans),
    }
    async with _phase(run_id, seconds, "publish"):
        dropped = await release_sql.publish(connection, control, release_by_field)
        if dropped is not None and dropped["schema_name"] != schema:
            await release_sql.drop_schema(connection, dropped["schema_name"])
        keep_ids = {release_id} | ({current["release_id"]} if current is not None else set())
        await asyncio.to_thread(downloads.prune, keep_ids)
    return {
        "status": "published",
        "release_id": release_id,
        "schema": schema,
        "previous_release_id": None if current is None else current["release_id"],
        "dropped_release_id": None if dropped is None else dropped["release_id"],
        "rows": sum(row_counts.values()),
        "row_counts": row_counts,
        "unresolved_counts": unresolved,
        "addresses_pending": True,
        "refresh_pending": True,
        "seconds": seconds,
    }


async def _unchanged(connection, control, current, seconds):
    """The current release again: report whether its address table or refresh is still owed."""
    return {
        "status": "unchanged",
        "release_id": current["release_id"],
        "schema": current["schema_name"],
        "addresses_pending": addresses.has_pending_addresses(current)
        or not await addresses.table_exists(connection, control),
        "refresh_pending": addresses.has_pending_refresh(current),
        "seconds": seconds,
    }


async def _addresses_locked(connection, summary, task, run_id, resolve_archive):
    """Build the address table from the current release when it still owes one, then queue one refresh.

    A built table whose refresh was never admitted retries only the enqueue.
    """
    control = release_sql.control_schema()
    build_pending, refresh_pending = summary.pop("addresses_pending"), summary.pop("refresh_pending")
    if build_pending:
        async with _phase(run_id, summary["seconds"], "addresses"):
            summary["address_rows"] = await addresses.build(
                connection,
                control,
                summary["schema"],
                summary["release_id"],
                resolve=resolve_archive or addresses.resolve_archive,
            )
    if (build_pending or refresh_pending) and task.get("refresh_addresses", True) is not False:
        summary["address_refresh"] = await addresses.enqueue_refresh(connection, control, summary["release_id"])
    return summary


async def _build_candidate(connection, schema, plans, connect, scanner, run_id, seconds):
    async def _file_done(plan, done):
        await write_phase_progress(
            run_id, "cms-npd-typed-stage", unit="files", done=done, total=len(plans), message=plan.name
        )

    async with _phase(run_id, seconds, "stage"):
        await release_sql.create_release_schema(connection, schema)
        row_counts = await stage_files(
            plans, schema, connect, scanner, concurrency=STAGE_CONCURRENCY, on_file_done=_file_done
        )
    async with _phase(run_id, seconds, "finish"):
        await release_sql.finish_release_schema(connection, schema)
    async with _phase(run_id, seconds, "check"):
        unresolved, network_rows = await release_sql.unresolved_counts(connection, schema)
    return {**row_counts, "network": network_rows}, unresolved


async def _drop_candidate(connect, schema):
    """Drop the failed candidate on a fresh connection: the run's own one may be mid-statement."""
    connection = await connect()
    try:
        await release_sql.drop_schema(connection, schema)
    finally:
        await connection.close()


def _file_summary(plans):
    return {
        plan.name: {
            "compressed_bytes": plan.compressed_bytes,
            "original_bytes": plan.original_bytes,
            "etag": plan.etag,
        }
        for plan in plans
    }
