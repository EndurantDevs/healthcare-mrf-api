# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import json
import os
import re
import ssl
import time
from pathlib import Path, PurePath
from random import choice

import aiohttp
import humanize
import pytz
from aiofile import async_open
from aiohttp_socks import ProxyConnector
from aioshutil import copyfile
from arq import Retry
from asyncpg.exceptions import (CardinalityViolationError, InterfaceError,
                                InvalidColumnReferenceError,
                                UndefinedTableError, UniqueViolationError)
from dateutil.parser import parse as parse_date
from fastcrc import crc16, crc32
from sqlalchemy import Index, and_, inspect
from sqlalchemy.exc import SQLAlchemyError

from db.connection import db, init_db
from db.json_mixin import JSONOutputMixin

HTTP_CHUNK_SIZE = 1024 * 1024
PARALLEL_DOWNLOAD_THRESHOLD_BYTES = int(
    os.getenv("HLTHPRT_PARALLEL_DOWNLOAD_THRESHOLD_BYTES", str(100 * 1024 * 1024))
)
PARALLEL_DOWNLOAD_WORKERS = max(int(os.getenv("HLTHPRT_PARALLEL_DOWNLOAD_WORKERS", "8")), 2)


def _parse_size_bytes(raw_value: str, default_bytes: int) -> int:
    value = str(raw_value or "").strip().lower()
    if not value:
        return default_bytes
    if value.isdigit():
        parsed = int(value)
        # Backward/UX-friendly behavior: small integers are interpreted as MiB.
        if parsed < 1024:
            return parsed * 1024 * 1024
        return parsed
    match = re.fullmatch(r"(\d+)\s*([kmgt]?b?)", value)
    if not match:
        return default_bytes
    number = int(match.group(1))
    suffix = match.group(2).strip("b")
    multiplier = {
        "": 1,
        "k": 1024,
        "m": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "t": 1024 * 1024 * 1024 * 1024,
    }.get(suffix, 1)
    return number * multiplier


PARALLEL_DOWNLOAD_RANGE_SIZE = max(
    _parse_size_bytes(
        os.getenv("HLTHPRT_PARALLEL_DOWNLOAD_RANGE_SIZE", str(8 * 1024 * 1024)),
        8 * 1024 * 1024,
    ),
    HTTP_CHUNK_SIZE,
)
PROGRESS_INTERVAL_SECONDS = max(float(os.getenv("HLTHPRT_DOWNLOAD_PROGRESS_INTERVAL_SECONDS", "2")), 0.5)
PREFER_COMPRESSED_STREAM = os.getenv("HLTHPRT_PREFER_COMPRESSED_STREAM", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
headers = {
    'user-agent': 'Mozilla/5.0 (compatible; Healthporta Healthcare MRF API Importer/1.1; +https://github.com/EndurantDevs/healthcare-mrf-api)',
    'accept-encoding': 'gzip, br, deflate',
}
timeout = aiohttp.ClientTimeout(total=120.0)
SECONDS_PER_MEGABYTE = float(os.getenv("HLTHPRT_SECONDS_PER_MB", "3.0"))
HEAD_TIMEOUT_SECONDS = float(os.getenv("HLTHPRT_HEAD_TIMEOUT_SECONDS", "20.0"))
MIN_STREAM_TIMEOUT = float(os.getenv("HLTHPRT_MIN_STREAM_TIMEOUT", "120.0"))
MAX_STREAM_TIMEOUT = float(os.getenv("HLTHPRT_MAX_STREAM_TIMEOUT", "14400.0"))
DOWNLOAD_TIMEOUT_MULTIPLIER = float(os.getenv("HLTHPRT_DOWNLOAD_TIMEOUT_MULTIPLIER", "1.0"))
CONNECT_TIMEOUT_SECONDS = float(os.getenv("HLTHPRT_CONNECT_TIMEOUT_SECONDS", "120.0"))
TEST_DATABASE_SUFFIX = os.getenv("HLTHPRT_TEST_DATABASE_SUFFIX")
_PROGRESS_BAR_WIDTH = 28
PARALLEL_CHUNK_RETRIES = max(int(os.getenv("HLTHPRT_PARALLEL_CHUNK_RETRIES", "4")), 1)
PARALLEL_CHUNK_BACKOFF_SECONDS = max(float(os.getenv("HLTHPRT_PARALLEL_CHUNK_BACKOFF_SECONDS", "1.0")), 0.1)
LARGE_FILE_TIMEOUT_LOG_THRESHOLD_BYTES = 1024 * 1024 * 1024


def _render_progress_line(downloaded: int, total: int | None, speed: float) -> str:
    if total and total > 0:
        ratio = min(max(downloaded / total, 0.0), 1.0)
        filled = int(_PROGRESS_BAR_WIDTH * ratio)
        bar = "#" * filled + "-" * (_PROGRESS_BAR_WIDTH - filled)
        return (
            f"\r[download] [{bar}] {ratio * 100:5.1f}% "
            f"{humanize.naturalsize(downloaded, binary=True)} / "
            f"{humanize.naturalsize(total, binary=True)} "
            f"at {humanize.naturalsize(speed, binary=True)}/s"
        )
    return (
        f"\r[download] {humanize.naturalsize(downloaded, binary=True)} "
        f"at {humanize.naturalsize(speed, binary=True)}/s"
    )


def _print_progress_line(downloaded: int, total: int | None, speed: float, final: bool = False) -> None:
    print(_render_progress_line(downloaded, total, speed), end="\n" if final else "", flush=True)


def _estimate_timeout_seconds(size_bytes: int | None, chunk_size: int | None) -> float | None:
    if not size_bytes or size_bytes <= 0:
        return None
    chunk_bytes = chunk_size or HTTP_CHUNK_SIZE
    size_mb = size_bytes / (1024 * 1024)
    # Provide a base allowance proportional to the number of chunks and overall size.
    per_chunk_seconds = SECONDS_PER_MEGABYTE
    estimated = max(size_mb * SECONDS_PER_MEGABYTE, (size_bytes / chunk_bytes) * per_chunk_seconds)
    estimated = estimated * 3.0 * max(DOWNLOAD_TIMEOUT_MULTIPLIER, 0.1)
    estimated = max(MIN_STREAM_TIMEOUT, estimated)
    return min(estimated, MAX_STREAM_TIMEOUT)


async def _determine_request_timeout(client: aiohttp.ClientSession, url: str, chunk_size: int | None) -> aiohttp.ClientTimeout:
    """
    Infer file size (including range-probe fallback) and scale timeout proportionally.
    Fallback to a generous default when size cannot be determined.
    """
    size_bytes, _ = await _head_download_info(client, url)
    if size_bytes:
        seconds = _estimate_timeout_seconds(size_bytes, chunk_size)
        if seconds:
            return aiohttp.ClientTimeout(
                total=seconds,
                connect=CONNECT_TIMEOUT_SECONDS,
                sock_read=seconds,
            )

    fallback = max(MIN_STREAM_TIMEOUT, (timeout.total or 0) * 10 if timeout.total else 600.0)
    fallback = min(fallback * max(DOWNLOAD_TIMEOUT_MULTIPLIER, 0.1), MAX_STREAM_TIMEOUT)
    return aiohttp.ClientTimeout(total=fallback, connect=CONNECT_TIMEOUT_SECONDS, sock_read=fallback)


async def _head_download_info(
    client: aiohttp.ClientSession, url: str
) -> tuple[int | None, bool]:
    async def _probe_total_via_range() -> int | None:
        try:
            probe_headers = {
                "Range": "bytes=0-0",
                "Accept-Encoding": "identity",
            }
            async with client.get(
                url,
                headers=probe_headers,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=HEAD_TIMEOUT_SECONDS),
            ) as probe_response:
                content_range = probe_response.headers.get("Content-Range") or ""
                if "/" in content_range:
                    total_str = content_range.rsplit("/", 1)[-1].strip()
                    if total_str.isdigit():
                        return int(total_str)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return None
        return None

    try:
        async with client.head(
            url,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=HEAD_TIMEOUT_SECONDS),
        ) as head_response:
            content_length = head_response.headers.get("Content-Length")
            accept_ranges = (head_response.headers.get("Accept-Ranges") or "").lower()
            size_bytes = None
            if content_length:
                try:
                    size_bytes = int(content_length)
                except ValueError:
                    size_bytes = None
            supports_ranges = "bytes" in accept_ranges
            if supports_ranges and (not size_bytes or size_bytes < 1024):
                probed_total = await _probe_total_via_range()
                if probed_total:
                    size_bytes = probed_total
            return size_bytes, supports_ranges
    except (aiohttp.ClientError, asyncio.TimeoutError):
        probed_total = await _probe_total_via_range()
        return probed_total, bool(probed_total)


async def _download_parallel_by_ranges(
    client: aiohttp.ClientSession,
    url: str,
    filepath: str,
    size_bytes: int,
    request_timeout: aiohttp.ClientTimeout,
) -> None:
    effective_range_size = PARALLEL_DOWNLOAD_RANGE_SIZE
    # Auto-tune to keep request count bounded for very large files.
    if size_bytes > 0:
        estimated_ranges = max(size_bytes // effective_range_size, 1)
        if estimated_ranges > PARALLEL_DOWNLOAD_WORKERS * 512:
            effective_range_size = max(effective_range_size, 16 * 1024 * 1024)
        if estimated_ranges > PARALLEL_DOWNLOAD_WORKERS * 1024:
            effective_range_size = max(effective_range_size, 32 * 1024 * 1024)

    ranges: list[tuple[int, int]] = []
    start = 0
    while start < size_bytes:
        end = min(start + effective_range_size - 1, size_bytes - 1)
        ranges.append((start, end))
        start = end + 1

    with open(filepath, "wb") as fp:
        fp.truncate(size_bytes)

    semaphore = asyncio.Semaphore(PARALLEL_DOWNLOAD_WORKERS)
    progress_lock = asyncio.Lock()
    downloaded_bytes = 0
    start_time = time.monotonic()
    last_progress_time = start_time

    file_fd = os.open(filepath, os.O_RDWR)

    def _write_range(fd: int, offset: int, payload: bytes) -> None:
        # pwrite avoids fd seek contention across parallel workers.
        os.pwrite(fd, payload, offset)

    async def _download_range(start_byte: int, end_byte: int) -> None:
        nonlocal downloaded_bytes, last_progress_time
        headers_local = {
            "Range": f"bytes={start_byte}-{end_byte}",
            # Range math requires identity representation for deterministic byte offsets.
            "Accept-Encoding": "identity",
        }
        base_total_timeout = float(request_timeout.total or MAX_STREAM_TIMEOUT)
        base_sock_read_timeout = float(request_timeout.sock_read or base_total_timeout)

        async with semaphore:
            last_error = None
            for attempt in range(1, PARALLEL_CHUNK_RETRIES + 1):
                # Increase timeout on each retry for this specific chunk.
                growth = 1.0 + (attempt - 1) * 0.5
                attempt_total = min(base_total_timeout * growth, MAX_STREAM_TIMEOUT)
                attempt_sock_read = min(base_sock_read_timeout * growth, MAX_STREAM_TIMEOUT)
                attempt_timeout = aiohttp.ClientTimeout(
                    total=attempt_total,
                    connect=CONNECT_TIMEOUT_SECONDS,
                    sock_read=attempt_sock_read,
                )
                try:
                    async with client.get(url, headers=headers_local, timeout=attempt_timeout) as response:
                        if response.status not in (206,):
                            raise RuntimeError(f"Range request not honored (status={response.status})")
                        payload = await response.read()
                        expected = end_byte - start_byte + 1
                        if len(payload) != expected:
                            raise RuntimeError(
                                f"Range payload mismatch for {url}: got {len(payload)} bytes, expected {expected}"
                            )
                        await asyncio.to_thread(_write_range, file_fd, start_byte, payload)
                        async with progress_lock:
                            downloaded_bytes += len(payload)
                            now = time.monotonic()
                            if now - last_progress_time >= PROGRESS_INTERVAL_SECONDS or downloaded_bytes >= size_bytes:
                                elapsed = max(now - start_time, 0.001)
                                speed = downloaded_bytes / elapsed
                                _print_progress_line(
                                    downloaded_bytes,
                                    size_bytes,
                                    speed,
                                    final=downloaded_bytes >= size_bytes,
                                )
                                last_progress_time = now
                        return
                except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as err:
                    last_error = err
                    if attempt >= PARALLEL_CHUNK_RETRIES:
                        break
                    backoff = PARALLEL_CHUNK_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    await asyncio.sleep(min(backoff, 20.0))
            raise RuntimeError(
                f"Failed to download range {start_byte}-{end_byte} after "
                f"{PARALLEL_CHUNK_RETRIES} attempts: {last_error!r}"
            )

    try:
        await asyncio.gather(*(_download_range(s, e) for s, e in ranges))
    finally:
        os.close(file_fd)
def _default_rows_per_insert() -> int:
    try:
        return max(int(os.getenv("HLTHPRT_ROWS_PER_INSERT", "1000")), 1)
    except ValueError:
        return 1000


ROWS_PER_INSERT = _default_rows_per_insert()

_DYNAMIC_CLASS_CACHE = {}

async def get_http_client(use_proxy: bool = True):
    client = None
    proxy_raw = os.environ.get('HLTHPRT_SOCKS_PROXY') if use_proxy else None
    if proxy_raw:
        try:
            proxies = json.loads(proxy_raw)
        except json.JSONDecodeError:
            proxies = []
        if proxies:
            proxy_url = choice(proxies)
            if proxy_url.startswith('socks'):
                connector = ProxyConnector.from_url(proxy_url)
                client = aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector)
            else:
                client = aiohttp.ClientSession(timeout=timeout, headers=headers, proxy=proxy_url)
        else:
            client = aiohttp.ClientSession(timeout=timeout, headers=headers)
    else:
        client = aiohttp.ClientSession(timeout=timeout, headers=headers)

    return client


async def download_it(url, local_timeout=None):
    client = await get_http_client()
    res = None
    async with client:
        if local_timeout:
            local_timeout = aiohttp.ClientTimeout(total=local_timeout)
            r = await client.get(url, timeout=local_timeout)
        else:
            r = await client.get(url)
        res = await r.text()
    return res



async def download_it_and_save_nostream(url, filepath):
    client = await get_http_client()
    async with client:
        async with async_open(filepath, 'wb+') as afp:
            response = await client.get(url)
            if response.status == 200:
                await afp.write(await response.read())
            else:
                print(url, ' returns ', response.status)
                raise Retry(defer=60)


async def db_startup(ctx):
    await my_init_db(db)

async def download_it_and_save(url, filepath, chunk_size=None, context=None, logger=None, cache_dir=None):
    print(f"Downloading {url}")
    max_chunk_size = chunk_size if chunk_size else HTTP_CHUNK_SIZE
    file_with_dir = None
    if cache_dir:
        file_with_dir = str(PurePath(str(cache_dir), str(return_checksum([url]))))
    if cache_dir and file_with_dir and os.path.exists(file_with_dir):
        await copyfile(file_with_dir, filepath)
    else:
        async def _stream_response_to_file(response, mode: str, start_offset: int = 0):
            stream_total = size_bytes
            response_total = None
            content_encoding = (response.headers.get("Content-Encoding") or "identity").lower()
            if response.content_length is not None:
                response_total = response.content_length + start_offset
            # Prefer response-derived total when HEAD is missing/inaccurate.
            if response_total and (
                not stream_total
                or stream_total <= 0
                or stream_total < response_total
                or stream_total < 1024
            ):
                stream_total = response_total
            # With transparent decompression, byte counters are no longer comparable.
            if content_encoding not in {"", "identity"}:
                stream_total = None
            stream_downloaded = start_offset
            stream_start = time.monotonic()
            stream_last = stream_start
            async with async_open(filepath, mode) as afp:
                async for chunk in response.content.iter_chunked(max_chunk_size):
                    await afp.write(chunk)
                    stream_downloaded += len(chunk)
                    now = time.monotonic()
                    if now - stream_last >= PROGRESS_INTERVAL_SECONDS:
                        elapsed = max(now - stream_start, 0.001)
                        speed = (stream_downloaded - start_offset) / elapsed
                        _print_progress_line(stream_downloaded, stream_total, speed, final=False)
                        stream_last = now
            elapsed = max(time.monotonic() - stream_start, 0.001)
            speed = (stream_downloaded - start_offset) / elapsed
            _print_progress_line(stream_downloaded, stream_total, speed, final=True)

        client = await get_http_client()
        async with client:
            size_bytes, accept_ranges = await _head_download_info(client, url)
            request_timeout = await _determine_request_timeout(client, url, max_chunk_size)
            if size_bytes and size_bytes > LARGE_FILE_TIMEOUT_LOG_THRESHOLD_BYTES:
                print(
                    "[timeout] "
                    f"computed aiohttp timeout for large file "
                    f"({humanize.naturalsize(size_bytes, binary=True)}): "
                    f"total={request_timeout.total}s "
                    f"connect={request_timeout.connect}s "
                    f"sock_read={request_timeout.sock_read}s"
                )
            try:
                can_parallel = (
                    bool(size_bytes)
                    and size_bytes >= PARALLEL_DOWNLOAD_THRESHOLD_BYTES
                    and accept_ranges
                    and not PREFER_COMPRESSED_STREAM
                )
                if can_parallel:
                    print(f"Response size: {size_bytes} bytes (parallel download)")
                    try:
                        await _download_parallel_by_ranges(
                            client=client,
                            url=url,
                            filepath=filepath,
                            size_bytes=size_bytes,
                            request_timeout=request_timeout,
                        )
                        return
                    except Exception as parallel_err:  # pylint: disable=broad-exception-caught
                        print(f"[warn] parallel download failed, falling back to stream for {url}: {parallel_err!r}")

                existing_size = os.path.getsize(filepath) if os.path.exists(filepath) else 0
                resume_done = False
                can_resume_stream = bool(existing_size) and bool(accept_ranges) and not PREFER_COMPRESSED_STREAM
                if can_resume_stream and size_bytes and existing_size < size_bytes:
                    print(f"Resuming stream from {existing_size} / {size_bytes} bytes")
                    resume_headers = {
                        "Range": f"bytes={existing_size}-",
                        "Accept-Encoding": "identity",
                    }
                    async with client.get(url, timeout=request_timeout, headers=resume_headers) as response:
                        if response.status == 206:
                            encoding = response.headers.get("Content-Encoding") or "identity"
                            print(f"Response size: {response.content_length} bytes (stream-resume, encoding={encoding})")
                            await _stream_response_to_file(response, "ab", start_offset=existing_size)
                            resume_done = True
                        else:
                            print(f"[warn] resume not supported for {url} (status={response.status}), restarting")
                elif can_resume_stream and size_bytes and size_bytes >= 1024 and existing_size >= size_bytes:
                    print(f"Existing file already complete ({existing_size} bytes), skipping download")
                    return

                if not resume_done:
                    async with client.get(url, timeout=request_timeout) as response:
                        try:
                            encoding = response.headers.get("Content-Encoding") or "identity"
                            print(f"Response size: {response.content_length} bytes (stream, encoding={encoding})")
                        except aiohttp.ClientResponseError as exc:
                            if context and logger:
                                await log_error('err',
                                        f"Error response {exc.status} while requesting {exc.request_info.real_url!r}.",
                                        context['issuer_array'], url, context['source'], 'network', logger)
                            raise Retry(defer=60)
                        await _stream_response_to_file(response, "wb+")
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                print(f"[retry] download_it_and_save request failed for {url}: {err!r}")
                if context and logger:
                    await log_error('err',
                            f"Network error: {err} while downloading {url!r}.",
                            context['issuer_array'], url, context['source'], 'network', logger)
                raise Retry(defer=60)
            except Retry:
                raise
            except RuntimeError as err:
                print(f"[retry] parallel download failed for {url}: {err!r}")
                if context and logger:
                    await log_error(
                        'err',
                        f"Parallel download error: {err} while downloading {url!r}.",
                        context['issuer_array'],
                        url,
                        context['source'],
                        'network',
                        logger,
                    )
                raise Retry(defer=60)
            except ssl.SSLCertVerificationError as err:
                print(f"[retry] download_it_and_save SSL error for {url}: {err!r}")
                if context and logger:
                    await log_error('err',
                            f"SSL Error. {err} URL: {url}.",
                            context['issuer_array'], url, context['source'], 'network', logger)
                raise Retry(defer=60)
        if cache_dir and file_with_dir:
            await copyfile(filepath, file_with_dir)


def make_class(model_cls, table_suffix, schema_override=None):
    table_suffix = str(table_suffix)
    cache_key = (model_cls, table_suffix, schema_override)
    if cache_key in _DYNAMIC_CLASS_CACHE:
        return _DYNAMIC_CLASS_CACHE[cache_key]

    new_table_name = "_".join([model_cls.__tablename__, table_suffix])
    metadata = model_cls.metadata

    if new_table_name in metadata.tables:
        new_table = metadata.tables[new_table_name]
    else:
        new_table = model_cls.__table__.tometadata(metadata, name=new_table_name)
        if schema_override is not None:
            new_table.schema = schema_override

    mapper_args = dict(getattr(model_cls, "__mapper_args__", {}))
    mapper_args["concrete"] = True

    attrs = {
        "__tablename__": new_table_name,
        "__table__": new_table,
        "__mapper_args__": mapper_args,
        "__module__": model_cls.__module__,
    }

    for attr_name in (
        "__my_index_elements__",
        "__my_additional_indexes__",
        "__main_table__",
    ):
        if hasattr(model_cls, attr_name):
            attrs[attr_name] = getattr(model_cls, attr_name)

    bases = tuple(base for base in model_cls.__bases__ if base is not object)
    if not bases:
        bases = (model_cls,)

    dynamic_name = f"{model_cls.__name__}_{table_suffix}"
    dynamic_cls = type(dynamic_name, bases, attrs)
    _DYNAMIC_CLASS_CACHE[cache_key] = dynamic_cls
    return dynamic_cls

err_obj_list = []
err_obj_key = {}

def return_checksum(arr: list, crc=32):
    for i in range(0, len(arr)):
        arr[i] = str(arr[i])
    checksum = '|'.join(arr)
    checksum = bytes(checksum, 'utf-8')
    if crc == 16:
        return crc16.xmodem(checksum)
    return crc32.cksum(checksum)-2147483648


async def log_error(type, error, issuer_array, url, source, level, cls):
    for issuer_id in issuer_array:
        checksum = return_checksum([type, str(error), str(issuer_id), str(url), source, level])
        if checksum in err_obj_key:
            return

        err_obj_key[checksum] = True
        err_obj_list.append({
            'issuer_id': issuer_id,
            'checksum': checksum,
            'type': type,
            'text': error,
            'url': url,
            'source': source,
            'level': level
        })
        if len(err_obj_list) > 200:
            await flush_error_log(cls)


async def flush_error_log(cls):
    if not err_obj_list:
        return
    payload = err_obj_list.copy()
    err_obj_list.clear()
    err_obj_key.clear()
    try:
        await push_objects(payload, cls)
    except Exception:
        err_obj_list.extend(payload)
        for entry in payload:
            err_obj_key[entry['checksum']] = True
        raise


async def push_objects_slow(obj_list, cls):
    if obj_list:
        try:
            if hasattr(cls, "__my_index_elements__"):
                stmt = (
                    db.insert(cls)
                    .values(obj_list)
                    .on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                )
                await stmt.status()
            else:
                await db.insert(cls).values(obj_list).status()
        except (SQLAlchemyError, UniqueViolationError, InterfaceError):
            for obj in obj_list:
                try:
                    stmt = db.insert(cls).values(obj)
                    if hasattr(cls, "__my_index_elements__"):
                        stmt = stmt.on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                    await stmt.status()
                except (SQLAlchemyError, UniqueViolationError) as exc:
                    print(exc)

class IterateList:

    def __init__(self, obj_list, order):
        self.end = len(obj_list)
        self.start = 0
        self.obj_list = obj_list
        self.obj_order = order

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.start < self.end:
            cur_pos = self.start
            self.start += 1
            return [self.obj_list[cur_pos][k] for k in self.obj_order]
        else:
            raise StopAsyncIteration


async def my_init_db(db):
    loop = asyncio.get_event_loop()
    await init_db(db, loop)


async def ensure_database(test_mode: bool) -> None:
    """
    Ensure the async engine points at the correct database for the current run.
    Test runs append the configured suffix to the base database to avoid touching production data.
    """
    base_database = os.getenv("HLTHPRT_DB_DATABASE", "postgres")
    target_database = base_database
    if test_mode and TEST_DATABASE_SUFFIX:
        target_database = f"{base_database}{TEST_DATABASE_SUFFIX}"
    override = target_database if target_database != base_database else None
    if getattr(db, "_database_override", None) != override:
        db._database_override = override  # type: ignore[attr-defined]
    await db.connect()


def get_import_schema(env_var: str, default: str, test_mode: bool) -> str:
    schema = os.getenv(env_var) or default
    return schema


def deduplicate_dicts(dict_list, key_fields):
    seen = {}
    for dict_entry in dict_list:
        key = tuple(dict_entry.get(field) for field in key_fields)
        seen[key] = dict_entry  # keep the last occurrence
    return list(seen.values())


async def push_objects(obj_list, cls, rewrite=False, _missing_table_attempt: int = 0, use_copy: bool = True):
    if obj_list:
        max_missing_table_retries = 5
        default_max_params = 30000

        def _is_missing_table_error(err: BaseException) -> bool:
            err_text = str(err).lower()
            return "undefinedtable" in err_text or ("relation" in err_text and "does not exist" in err_text)

        def _short_error(err: BaseException) -> str:
            message = str(getattr(err, "orig", err)).replace("\n", " ").strip()
            if len(message) > 240:
                return f"{message[:240]}..."
            return message

        async def _retry_after_missing_table(err: BaseException):
            if _missing_table_attempt >= max_missing_table_retries:
                raise err

            print(
                f"Table {cls.__tablename__} is missing, create/retry attempt "
                f"{_missing_table_attempt + 1}/{max_missing_table_retries}: {_short_error(err)}"
            )
            try:
                await db.create_table(cls.__table__, checkfirst=True)
            except SQLAlchemyError as create_err:
                create_err_text = str(create_err).lower()
                is_concurrent_create_race = (
                    "already exists" in create_err_text
                    or "pg_type_typname_nsp_index" in create_err_text
                )
                if not (is_concurrent_create_race or _is_missing_table_error(create_err)):
                    raise
                print(
                    f"Concurrent CREATE TABLE race detected for {cls.__tablename__}; "
                    "retrying insert path."
                )

            await asyncio.sleep(min(0.05 * (_missing_table_attempt + 1), 0.25))
            return await push_objects(
                obj_list,
                cls,
                rewrite=rewrite,
                _missing_table_attempt=_missing_table_attempt + 1,
                use_copy=use_copy,
            )

        def _conflict_targets():
            if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                for index in cls.__my_initial_indexes__:
                    elements = index.get("index_elements")
                    if elements:
                        return list(elements)
            if hasattr(cls, "__my_index_elements__") and cls.__my_index_elements__:
                return list(cls.__my_index_elements__)
            primary_keys = [key.name for key in inspect(cls.__table__).primary_key]
            return primary_keys or None

        def _max_insert_parameters() -> int:
            try:
                requested = int(os.getenv("HLTHPRT_MAX_INSERT_PARAMETERS", str(default_max_params)))
            except ValueError:
                requested = default_max_params
            try:
                driver_limit = int(os.getenv("HLTHPRT_DRIVER_PARAM_LIMIT", "32767"))
            except ValueError:
                driver_limit = 32767
            driver_limit = max(driver_limit, 1)
            if requested <= 0:
                return driver_limit
            return max(1, min(requested, driver_limit))

        def _table_schema_name():
            schema_name = getattr(cls.__table__, "schema", None)
            if schema_name is None:
                table_args = getattr(cls, "__table_args__", None)
                if isinstance(table_args, dict):
                    schema_name = table_args.get("schema")
                elif isinstance(table_args, (tuple, list)):
                    for arg in table_args:
                        if isinstance(arg, dict) and "schema" in arg:
                            schema_name = arg["schema"]
                            break
            return schema_name

        def _chunk_records(sequence):
            chunk_size = ROWS_PER_INSERT or 1000
            if sequence:
                approx_columns = max(len(sequence[0]), 1)
                params_limit = _max_insert_parameters()
                max_rows_for_params = max(params_limit // approx_columns, 1)
                chunk_size = min(chunk_size, max_rows_for_params)
            for start in range(0, len(sequence), chunk_size):
                yield sequence[start:start + chunk_size]

        if rewrite:
            targets = _conflict_targets()
            if targets:
                obj_list = deduplicate_dicts(obj_list, targets)

            if use_copy:
                try:
                    async with db.acquire() as conn:
                        raw_conn = conn.raw_connection
                        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
                        copy_method = getattr(driver_conn, "copy_records_to_table", None)
                        if copy_method is None:
                            raise NotImplementedError("Active database driver does not expose copy_records_to_table")

                        await copy_method(
                            cls.__tablename__,
                            schema_name=_table_schema_name(),
                            columns=list(obj_list[0].keys()),
                            records=IterateList(obj_list, obj_list[0].keys()),
                        )
                    return
                except ValueError as exc:
                    print(f"INPUT arr: {obj_list}")
                    print(exc)
                except (UndefinedTableError, SQLAlchemyError, InterfaceError) as err:
                    if _is_missing_table_error(err):
                        return await _retry_after_missing_table(err)
                    print(f"copy_records_to_table fallback due to {_short_error(err)}")
                except (UniqueViolationError, NotImplementedError, InvalidColumnReferenceError, CardinalityViolationError) as err:
                    print(f"copy_records_to_table fallback due to {_short_error(err)}")

            for chunk in _chunk_records(obj_list):
                stmt = db.insert(cls.__table__).values(chunk)
                if targets:
                    update_cols = [
                        c.name for c in cls.__table__.c
                        if c.name not in targets and not c.primary_key
                    ]
                    set_dict = {col: getattr(stmt.excluded, col) for col in update_cols}
                    if set_dict:
                        stmt = stmt.on_conflict_do_update(
                            index_elements=targets,
                            set_=set_dict,
                        )
                    else:
                        stmt = stmt.on_conflict_do_nothing(index_elements=targets)
                try:
                    await stmt.status()
                except (UndefinedTableError, SQLAlchemyError, InterfaceError) as err:
                    if _is_missing_table_error(err):
                        return await _retry_after_missing_table(err)
                    raise
            return

        if len(obj_list) == 1 and not rewrite:
            return await push_objects_slow(obj_list, cls)
        
        # if hasattr(cls, "__my_initial_indexes__"):
        #     for i in (cls.__my_initial_indexes__):
        #         obj_list = deduplicate_dicts(obj_list, i.get("index_elements"))
        # else:
        #     obj_list = deduplicate_dicts(obj_list, cls.__my_index_elements__)
        
        if not use_copy:
            for chunk in _chunk_records(obj_list):
                stmt = db.insert(cls.__table__).values(chunk)
                if hasattr(cls, "__my_index_elements__"):
                    stmt = stmt.on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                try:
                    await stmt.status()
                except (UndefinedTableError, SQLAlchemyError, InterfaceError) as err:
                    if _is_missing_table_error(err):
                        return await _retry_after_missing_table(err)
                    raise
            return

        try:
            # raise UniqueViolationError
            async with db.acquire() as conn:
                raw_conn = conn.raw_connection
                driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
                copy_method = getattr(driver_conn, "copy_records_to_table", None)
                if copy_method is None:
                    raise NotImplementedError("Active database driver does not expose copy_records_to_table")

                await copy_method(
                    cls.__tablename__,
                    schema_name=_table_schema_name(),
                    columns=list(obj_list[0].keys()),
                    records=IterateList(obj_list, obj_list[0].keys()),
                )
            # print("All good!")
        except ValueError as exc:
            print(f"INPUT arr: {obj_list}")
            print(exc)
        except UndefinedTableError as err:
            return await _retry_after_missing_table(err)
        except (UniqueViolationError, NotImplementedError, InvalidColumnReferenceError, CardinalityViolationError, InterfaceError) as err:
            print(f"copy_records_to_table fallback due to {_short_error(err)}")

            for chunk in _chunk_records(obj_list):
                stmt = db.insert(cls.__table__).values(chunk)
                if hasattr(cls, "__my_index_elements__"):
                    stmt = stmt.on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                try:
                    await stmt.status()
                except (SQLAlchemyError, InterfaceError) as chunk_err:
                    if _is_missing_table_error(chunk_err):
                        return await _retry_after_missing_table(chunk_err)
                    print(f"Batch insert failed ({chunk_err}); retrying one-by-one")
                    for obj in chunk:
                        try:
                            single_stmt = db.insert(cls.__table__).values(obj)
                            if hasattr(cls, "__my_index_elements__"):
                                single_stmt = single_stmt.on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                            await single_stmt.status()
                        except (SQLAlchemyError, UniqueViolationError, InterfaceError) as single_err:
                            if _is_missing_table_error(single_err):
                                return await _retry_after_missing_table(single_err)
                            print(single_err)
            return

                
import logging

from sqlalchemy import or_


def _coerce_datetime(value):
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, dict):
        type_tag = value.get("__type__")
        if type_tag == "datetime":
            iso_value = value.get("value")
            if isinstance(iso_value, str):
                try:
                    return datetime.datetime.fromisoformat(iso_value)
                except ValueError:
                    try:
                        return parse_date(iso_value)
                    except (ValueError, TypeError):
                        return None
        if type_tag == "repr":
            return _coerce_datetime(value.get("repr"))
        # silently ignore other tagged dicts
        if "__type__" not in value:
            return _coerce_datetime(value.get("value"))
        return None
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            try:
                return parse_date(value)
            except (ValueError, TypeError):
                return None
    return None


def print_time_info(start):
    now = datetime.datetime.utcnow()
    start_dt = _coerce_datetime(start)
    if start_dt is None:
        print("Import timing unavailable (start timestamp missing).")
        return

    now = now.replace(tzinfo=pytz.utc)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=pytz.utc)
    delta = now - start_dt
    print('Import Time Delta: ', delta)
    print('Import took ', humanize.naturaldelta(delta))
