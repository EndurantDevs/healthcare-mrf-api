# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete discovery TOCs through bounded local spools and existing row writers."""

from __future__ import annotations

import asyncio
import os
import struct
import zipfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import aiohttp

from process.mrf_toc_spool import DecodedTocBudget, IncompleteToc, open_toc_store

_MAX_REDIRECT_HOPS = 10


@asynccontextmanager
async def managed_resolver_results(tasks):
    """Drain all resolver children before releasing a failed or canceled source."""
    try:
        yield asyncio.as_completed(tasks)
    finally:
        for resolver_task in tasks:
            if not resolver_task.done():
                resolver_task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@dataclass(frozen=True)
class PersistedTocResult:
    """Carry counts for rows already written instead of retaining their payloads."""

    plan_count: int
    file_count: int
    observations: list[dict[str, Any]]
    url: str


def toc_spool_bytes() -> int:
    """Validate the trusted per-target scratch budget without changing concurrency."""
    from process import mrf_source_discovery as discovery

    configured = os.getenv("HLTHPRT_MRF_DISCOVERY_SPOOL_BYTES")
    if configured is None:
        return 1024 * 1024 * 1024
    parsed = discovery._parse_size_bytes(configured)
    if parsed is None or parsed <= 0:
        raise ValueError("HLTHPRT_MRF_DISCOVERY_SPOOL_BYTES must be positive")
    return parsed


def is_streaming_toc(target) -> bool:
    """Select complete JSON/ZIP TOCs while retaining other target semantics."""
    metadata = target.metadata or {}
    kind = metadata.get("target_kind")
    if kind == "file_reference":
        return metadata.get("target_file_type") == "table-of-contents"
    return kind not in {"source_landing_page", "metadata_text"}


def toc_rows_from_entries(source_dict, entries, schema_version, *, filter_to_target_query=False):
    """Reuse the existing per-entry normalization for ordinary and streamed callers."""
    from process import mrf_source_discovery as discovery

    target_query = discovery._source_target_payer_query(source_dict)
    target_queries = discovery._source_payer_query_candidates(source_dict)
    plan_rows_by_id = {}
    file_rows = []
    for entry in entries:
        plan_info = discovery._query_expanded_plan_info(
            entry.plan_info or (),
            target_query,
            filter_to_target_query=filter_to_target_query,
            target_queries=target_queries,
        )
        if filter_to_target_query and target_queries and not plan_info:
            continue
        for plan in plan_info:
            plan_id, plan_row = discovery._toc_plan_row(source_dict, entry, plan)
            plan_rows_by_id[plan_id] = plan_row
        if entry.source_type != "table-of-contents" and str(entry.original_url or "").startswith(
            ("http://", "https://")
        ):
            file_rows.append(discovery._toc_file_row(source_dict, entry, plan_info, schema_version=schema_version))
    return list(plan_rows_by_id.values()), file_rows


class _PrefixedReader:
    def __init__(self, prefix: bytes, reader):
        self.prefix = prefix
        self.reader = reader

    async def read(self, size: int) -> bytes:
        """Return the already-inspected prefix before reading more network bytes."""
        if self.prefix:
            chunk, self.prefix = self.prefix[:size], self.prefix[size:]
            return chunk
        return await self.reader.read(size)


class _ArchiveReader:
    def __init__(self, stream):
        self.stream = stream

    async def read(self, size: int) -> bytes:
        """Yield between bounded ZIP-member reads so cancellation remains effective."""
        await asyncio.sleep(0)
        return self.stream.read(size)


def _zip_directory_size(stream, tail, offset, size) -> tuple[int, int]:
    fields = struct.unpack_from("<4s4H2IH", tail, offset)
    if b"PK\x06\x07" != tail[max(0, offset - 20) : max(0, offset - 16)]:
        return fields[4], fields[5]
    _, disk, record_offset, disk_count = struct.unpack_from("<4sIQI", tail, offset - 20)
    if disk or disk_count != 1 or record_offset > size - 56:
        raise IncompleteToc("TOC ZIP64 directory is incomplete or multi-disk")
    stream.seek(record_offset)
    record = stream.read(56)
    if len(record) != 56 or record[:4] != b"PK\x06\x06":
        raise IncompleteToc("TOC ZIP64 directory is invalid")
    values = struct.unpack("<4sQ2H2I4Q", record)
    return values[7], values[8]


def _validate_zip_directory(path, row_bytes: int) -> None:
    """Bound central-directory allocations before ZipFile materializes its index."""
    with path.open("rb") as stream:
        stream.seek(0, 2)
        size = stream.tell()
        stream.seek(max(0, size - 65_557))
        tail = stream.read(65_557)
        offset = tail.rfind(b"PK\x05\x06")
        if offset < 0 or len(tail) - offset < 22:
            raise IncompleteToc("TOC ZIP directory is missing or truncated")
        entries, directory_bytes = _zip_directory_size(stream, tail, offset, size)
    if entries > row_bytes // 1024 or directory_bytes > row_bytes // 4:
        raise IncompleteToc("TOC ZIP directory exceeds the row memory budget")


def _append_healthsparq_rows(spool, target, file_dict, observed_at) -> None:
    from process import mrf_source_discovery as discovery

    file_url = discovery._healthsparq_file_url(target.url, discovery._healthsparq_file_path(file_dict))
    if not file_url:
        return
    plan_info = discovery._healthsparq_plan_info(
        file_dict, target_query=discovery._source_target_payer_query(target.source)
    )
    for raw, normalized in zip(discovery._healthsparq_reporting_plans(file_dict), plan_info):
        _, row = discovery._healthsparq_plan_row(target.source, file_dict, raw, normalized, now=observed_at)
        spool.add_rows([row], [])
    _, row = discovery._healthsparq_file_row(
        target.source,
        file_dict,
        file_url,
        metadata_url=target.url,
        plan_info=plan_info,
        now=observed_at,
    )
    spool.add_rows([], discovery._apply_crawl_target_context_to_file_rows([row], target))


def _append_entry_rows(spool, target, entry, should_filter) -> None:
    from process import mrf_source_discovery as discovery

    queries = discovery._source_payer_query_candidates(target.source)
    plans = spool.normalized_entry_plans(
        entry,
        lambda raw: discovery._query_expanded_plan_info(
            raw or (),
            discovery._source_target_payer_query(target.source),
            filter_to_target_query=should_filter,
            target_queries=queries,
        ),
    )
    if should_filter and queries and not plans:
        return
    if spool.is_plan_rows_write_needed():
        for plan in plans:
            _, row = discovery._toc_plan_row(target.source, entry, plan)
            spool.add_rows([row], [])
    if entry.source_type != "table-of-contents" and str(entry.original_url or "").startswith(("http://", "https://")):
        row = discovery._toc_file_row(
            target.source, entry, plans, schema_version=discovery._truncate_text(spool.metadata.get("version"), 32)
        )
        spool.add_rows([], discovery._apply_crawl_target_context_to_file_rows([row], target))


async def _append_document_rows(spool, target, should_filter: bool) -> None:
    from process import mrf_source_discovery as discovery

    if spool.is_healthsparq:
        observed_at = discovery._utc_now()
        async for file_dict in spool.healthsparq_files():
            _append_healthsparq_rows(spool, target, file_dict, observed_at)
    else:
        async for entry in spool.catalog_entries():
            _append_entry_rows(spool, target, entry, should_filter)


async def _read_zip_documents(spool, reader, toc_target, max_bytes: int, should_filter: bool) -> None:
    from process import mrf_source_discovery as discovery

    path = await spool.save_archive(reader, max_bytes=max_bytes)
    _validate_zip_directory(path, spool.row_bytes)
    decoded_budget = DecodedTocBudget(max_bytes)
    try:
        with zipfile.ZipFile(path) as archive:
            members = sorted(
                (member for member in archive.infolist() if not member.is_dir()),
                key=lambda member: (
                    discovery._mrf_file_type_from_text(member.filename, member.filename) != "table-of-contents",
                    member.filename,
                ),
            )
            found_members = 0
            for member in members:
                if not member.filename.lower().endswith((".json", ".json.gz")):
                    continue
                if member.file_size > max_bytes:
                    raise IncompleteToc("TOC ZIP member exceeds the decoded discovery byte limit")
                with archive.open(member) as stream:
                    await spool.read_document(
                        _ArchiveReader(stream), max_bytes=max_bytes, decoded_budget=decoded_budget
                    )
                await _append_document_rows(spool, toc_target, should_filter)
                found_members += 1
            if not found_members:
                raise IncompleteToc("TOC ZIP contains no JSON members")
    except zipfile.BadZipFile as exc:
        raise IncompleteToc("TOC ZIP is invalid or incomplete") from exc


@asynccontextmanager
async def _target_directory(target, run_id):
    """Keep shared URL directories stable; each spool owns only its unique scratch."""
    from process.initial import _mrf_chunk_dir

    directory = _mrf_chunk_dir({"context": {"control_run_id": run_id}}, "discovery-toc", target.url)
    try:
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except FileExistsError as exc:
            raise IncompleteToc("TOC worker-state location is not a directory") from exc
        yield directory
    except OSError as exc:
        raise IncompleteToc("TOC worker-state directory is unavailable") from exc


def _validate_response(response, *, allow_fallback: bool, expect_json: bool, max_bytes: int) -> None:
    from process import mrf_source_discovery as discovery

    if allow_fallback and response.status in discovery._BROWSER_FALLBACK_HTTP_STATUSES:
        raise discovery._BrowserFallbackRequired()
    if response.status >= 400:
        raise ValueError(f"TOC request failed with HTTP {response.status}")
    content_type = str(response.headers.get("Content-Type") or "").lower()
    if expect_json and any(
        marker in content_type for marker in ("text/html", "application/xhtml", "application/pdf", "xml")
    ):
        if allow_fallback:
            raise discovery._BrowserFallbackRequired()
        raise ValueError("response content-type is not JSON")
    length = discovery._as_int(response.headers.get("Content-Length"))
    if length and length > max_bytes:
        raise IncompleteToc("TOC response exceeds the discovery byte limit")


@asynccontextmanager
async def _guarded_response(session, url):
    from process import mrf_source_discovery as discovery

    for hop in range(_MAX_REDIRECT_HOPS + 1):
        await discovery._assert_fetch_url_allowed(url)
        async with session.get(
            url,
            headers={"Accept-Encoding": "identity"},
            auto_decompress=False,
            allow_redirects=False,
            **discovery._request_ssl_kwargs(url),
        ) as response:
            await discovery._assert_fetch_url_allowed(str(response.url))
            location = response.headers.get("Location") or response.headers.get("URI")
            if response.status in {301, 302, 303, 307, 308} and location:
                if hop == _MAX_REDIRECT_HOPS:
                    raise ValueError("TOC redirect hop limit exceeded")
                url = urljoin(str(response.url), str(location))
                continue
            yield response
            return


async def _process_response(target, session, max_bytes: int, run_id, *, allow_fallback: bool) -> tuple[int, int]:
    from process import mrf_source_discovery as discovery

    is_zip = (target.metadata or {}).get("container_format") == "zip"
    async with _guarded_response(session, target.url) as response:
        _validate_response(response, allow_fallback=allow_fallback, expect_json=not is_zip, max_bytes=max_bytes)
        try:
            prefix = await response.content.read(64)
        except (aiohttp.ClientPayloadError, TimeoutError) as exc:
            raise IncompleteToc("TOC response stream did not complete") from exc
        if not is_zip and prefix.lstrip().lower().startswith((b"<!doctype", b"<html", b"<?xml")):
            if allow_fallback:
                raise discovery._BrowserFallbackRequired()
            raise ValueError("response body is not JSON")
        reader = _PrefixedReader(prefix, response.content)
        return await _persist_target_response(target, reader, response.charset, max_bytes, run_id, is_zip=is_zip)


async def _persist_target_response(toc_target, reader, charset, max_bytes, run_id, *, is_zip: bool) -> tuple[int, int]:
    from process import mrf_source_discovery as discovery

    should_filter = (toc_target.metadata or {}).get(
        "query_expansion_match_scope"
    ) == "toc_plan" and discovery._supports_toc_plan_query_scan(toc_target)
    queries = discovery._source_payer_query_candidates(toc_target.source)
    predicate = (lambda plan: discovery._is_plan_info_query_match(plan, queries)) if should_filter and queries else None
    async with _target_directory(toc_target, run_id) as directory:
        async with open_toc_store(
            toc_target.url,
            row_bytes=discovery.WRITE_BATCH_BYTES,
            spool_bytes=toc_spool_bytes(),
            plan_predicate=predicate,
            temporary_parent=directory,
        ) as spool:
            if is_zip:
                await _read_zip_documents(spool, reader, toc_target, max_bytes, should_filter)
            else:
                await spool.read_document(reader, max_bytes=max_bytes, charset=charset or "utf-8")
                await _append_document_rows(spool, toc_target, should_filter)

            async def sink(plans, files):
                """Use the existing idempotent writer and await each bounded batch."""
                await discovery._push_crawl_row_batches(
                    plans,
                    files,
                    [],
                    batch_size=discovery.WRITE_BATCH_SIZE,
                    row_write_timeout=discovery._float_env("HLTHPRT_MRF_CRAWL_ROW_WRITE_TIMEOUT_SECONDS", 0.0),
                )

            return await spool.drain_rows(
                sink, batch_size=discovery.WRITE_BATCH_SIZE, batch_bytes=discovery.WRITE_BATCH_BYTES
            )


async def stream_toc_target(target, session, *, max_bytes: int, run_id) -> tuple[int, int]:
    """Retain browser retry and redirect policy around a complete bounded target."""
    from process import mrf_source_discovery as discovery

    try:
        return await _process_response(target, session, max_bytes, run_id, allow_fallback=True)
    except aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, discovery._BrowserFallbackRequired:
        headers_by_name = {
            "User-Agent": discovery.BROWSER_FALLBACK_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Connection": "close",
        }
        timeout = getattr(session, "timeout", None) or aiohttp.ClientTimeout(
            total=discovery.HTTP_TOTAL_TIMEOUT,
            connect=15,
            sock_read=discovery.HTTP_READ_TIMEOUT,
        )
        async with aiohttp.ClientSession(
            headers=headers_by_name, timeout=timeout, connector=discovery._tcp_connector(limit=0), trust_env=False
        ) as retry:
            return await _process_response(target, retry, max_bytes, run_id, allow_fallback=False)
