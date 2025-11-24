# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import json
import os
import ssl
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
                                UniqueViolationError)
from dateutil.parser import parse as parse_date
from fastcrc import crc16, crc32
from sqlalchemy import Index, and_, inspect
from sqlalchemy.exc import SQLAlchemyError

from db.connection import db, init_db
from db.json_mixin import JSONOutputMixin

HTTP_CHUNK_SIZE = 1024 * 1024
headers = {'user-agent': 'Mozilla/5.0 (compatible; Healthporta Healthcare MRF API Importer/1.1; +https://github.com/EndurantDevs/healthcare-mrf-api)'}
timeout = aiohttp.ClientTimeout(total=60.0)
SECONDS_PER_MEGABYTE = float(os.getenv("HLTHPRT_SECONDS_PER_MB", "2.0"))
HEAD_TIMEOUT_SECONDS = float(os.getenv("HLTHPRT_HEAD_TIMEOUT_SECONDS", "20.0"))
MIN_STREAM_TIMEOUT = float(os.getenv("HLTHPRT_MIN_STREAM_TIMEOUT", "120.0"))
TEST_DATABASE_SUFFIX = os.getenv("HLTHPRT_TEST_DATABASE_SUFFIX")


def _estimate_timeout_seconds(size_bytes: int | None, chunk_size: int | None) -> float | None:
    if not size_bytes or size_bytes <= 0:
        return None
    chunk_bytes = chunk_size or HTTP_CHUNK_SIZE
    size_mb = size_bytes / (1024 * 1024)
    # Provide a base allowance proportional to the number of chunks and overall size.
    per_chunk_seconds = SECONDS_PER_MEGABYTE
    estimated = max(size_mb * SECONDS_PER_MEGABYTE, (size_bytes / chunk_bytes) * per_chunk_seconds)
    return max(MIN_STREAM_TIMEOUT, estimated)


async def _determine_request_timeout(client: aiohttp.ClientSession, url: str, chunk_size: int | None) -> aiohttp.ClientTimeout:
    """
    Issue a lightweight HEAD request to infer the file size, scaling the download timeout proportionally.
    Fallback to a generous default when HEAD is unsupported.
    """
    try:
        async with client.head(
            url,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=HEAD_TIMEOUT_SECONDS),
        ) as head_response:
            content_length = head_response.headers.get("Content-Length")
            if content_length:
                try:
                    size_bytes = int(content_length)
                except ValueError:
                    size_bytes = None
                if size_bytes:
                    seconds = _estimate_timeout_seconds(size_bytes, chunk_size)
                    if seconds:
                        return aiohttp.ClientTimeout(total=seconds, sock_read=seconds)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass

    fallback = max(MIN_STREAM_TIMEOUT, (timeout.total or 0) * 10 if timeout.total else 600.0)
    return aiohttp.ClientTimeout(total=fallback, sock_read=fallback)
def _default_rows_per_insert() -> int:
    try:
        return max(int(os.getenv("HLTHPRT_ROWS_PER_INSERT", "1000")), 1)
    except ValueError:
        return 1000


ROWS_PER_INSERT = _default_rows_per_insert()

_DYNAMIC_CLASS_CACHE = {}

async def get_http_client():
    client = None
    proxy_raw = os.environ.get('HLTHPRT_SOCKS_PROXY')
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
        async with async_open(filepath, 'wb+') as afp:
            client = await get_http_client()
            async with client:
                request_timeout = await _determine_request_timeout(client, url, max_chunk_size)
                try:
                    async with client.get(url, timeout=request_timeout) as response:
                        try:
                            # response.raise_for_status()
                            print(f"Response size: {response.content_length} bytes")
                        except aiohttp.ClientResponseError as exc:
                            if context and logger:
                                await log_error('err',
                                        f"Error response {exc.status} while requesting {exc.request_info.real_url!r}.",
                                        context['issuer_array'], url, context['source'], 'network', logger)
                            raise Retry(defer=60)

                        try:
                            async for chunk in response.content.iter_chunked(max_chunk_size):
                                await afp.write(chunk)
                        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                            print(f"[retry] download_it_and_save chunk read failed for {url}: {err!r}")
                            if context and logger:
                                await log_error('err',
                                        f"Error response while downloading {url!r}.",
                                        context['issuer_array'], url, context['source'], 'network', logger)
                            raise Retry(defer=60)
                except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                    print(f"[retry] download_it_and_save request failed for {url}: {err!r}")
                    if context and logger:
                        await log_error('err',
                                f"Network error: {err} while downloading {url!r}.",
                                context['issuer_array'], url, context['source'], 'network', logger)
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


async def push_objects(obj_list, cls, rewrite=False):
    if obj_list:
        max_params = int(os.getenv("HLTHPRT_MAX_INSERT_PARAMETERS", "60000"))
        if max_params > 0 and obj_list:
            approx_columns = max(len(obj_list[0]), 1)
            max_batch_size = max(max_params // approx_columns, 1)
            if len(obj_list) > max_batch_size:
                for start in range(0, len(obj_list), max_batch_size):
                    await push_objects(obj_list[start:start + max_batch_size], cls, rewrite=rewrite)
                return
        if len(obj_list) == 1 and not rewrite:
            return await push_objects_slow(obj_list, cls)
        
        # if hasattr(cls, "__my_initial_indexes__"):
        #     for i in (cls.__my_initial_indexes__):
        #         obj_list = deduplicate_dicts(obj_list, i.get("index_elements"))
        # else:
        #     obj_list = deduplicate_dicts(obj_list, cls.__my_index_elements__)
        
        try:
            # raise UniqueViolationError
            async with db.acquire() as conn:
                raw_conn = conn.raw_connection
                driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
                copy_method = getattr(driver_conn, "copy_records_to_table", None)
                if copy_method is None:
                    raise NotImplementedError("Active database driver does not expose copy_records_to_table")
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

                await copy_method(
                    cls.__tablename__,
                    schema_name=schema_name,
                    columns=list(obj_list[0].keys()),
                    records=IterateList(obj_list, obj_list[0].keys()),
                )
            # print("All good!")
        except ValueError as exc:
            print(f"INPUT arr: {obj_list}")
            print(exc)
        except (UniqueViolationError, NotImplementedError, InvalidColumnReferenceError, CardinalityViolationError, InterfaceError) as err:
            print(f"copy_records_to_table fallback due to {err}")

            def _conflict_targets():
                if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                    for index in cls.__my_initial_indexes__:
                        elements = index.get("index_elements")
                        if elements:
                            return elements
                primary_keys = [key.name for key in inspect(cls.__table__).primary_key]
                return primary_keys or None

            if rewrite:
                targets = _conflict_targets()
                for obj in obj_list:
                    update_dict = {
                        c.name: obj[c.name]
                        for c in cls.__table__.c
                        if not c.primary_key and c.name in obj
                    }
                    stmt = db.insert(cls.__table__).values(obj)
                    if targets:
                        stmt = stmt.on_conflict_do_update(
                            index_elements=targets,
                            set_=update_dict,
                        )
                    else:
                        stmt = stmt.on_conflict_do_update(
                            constraint=inspect(cls.__table__).primary_key,
                            set_=update_dict,
                        )
                    await stmt.status()
            else:
                def _chunk_records(sequence):
                    chunk_size = ROWS_PER_INSERT or 1000
                    for start in range(0, len(sequence), chunk_size):
                        yield sequence[start:start + chunk_size]

                for chunk in _chunk_records(obj_list):
                    stmt = db.insert(cls.__table__).values(chunk)
                    if hasattr(cls, "__my_index_elements__"):
                        stmt = stmt.on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                    try:
                        await stmt.status()
                    except (SQLAlchemyError, InterfaceError) as chunk_err:
                        print(f"Batch insert failed ({chunk_err}); retrying one-by-one")
                        for obj in chunk:
                            try:
                                single_stmt = db.insert(cls.__table__).values(obj)
                                if hasattr(cls, "__my_index_elements__"):
                                    single_stmt = single_stmt.on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                                await single_stmt.status()
                            except (SQLAlchemyError, UniqueViolationError, InterfaceError) as single_err:
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
