import os
import datetime
from pathlib import Path, PurePath
import asyncio
import aiohttp

import pytz

from db.connection import init_db, db

from aioshutil import copyfile
from sqlalchemy import Index, and_, inspect
import ssl
from sqlalchemy.exc import SQLAlchemyError
from asyncpg.exceptions import UniqueViolationError, InterfaceError, InvalidColumnReferenceError, CardinalityViolationError
from aiofile import async_open
from arq import Retry
from fastcrc import crc32, crc16
import humanize
from random import choice
import json
from db.json_mixin import JSONOutputMixin

HTTP_CHUNK_SIZE = 1024 * 1024
headers = {'user-agent': 'Mozilla/5.0 (compatible; Healthporta Healthcare MRF API Importer/1.1; +https://github.com/EndurantDevs/healthcare-mrf-api)'}
timeout = aiohttp.ClientTimeout(total=60.0)

_DYNAMIC_CLASS_CACHE = {}

async def get_http_client():
    client = None
    if os.environ.get('HLTHPRT_SOCKS_PROXY1'):
        proxies = json.loads(os.environ['HLTHPRT_SOCKS_PROXY'])
        proxy_url = choice(proxies)
        client = aiohttp.ClientSession(timeout=timeout, headers=headers, proxy=proxy_url)
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
                Retry()


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
            async with await get_http_client() as client:
                try:
                    async with client.get(url) as response:
                        try:
                            #response.raise_for_status()
                            print(f"Response size: {response.content_length} bytes")
                        except aiohttp.ClientResponseError as exc:
                            if context and logger:
                                await log_error('err',
                                        f"Error response {exc.status} while requesting {exc.request_info.real_url!r}.",
                                        context['issuer_array'], url, context['source'], 'network', logger)
                            Retry()

                        try:
                            async for chunk in response.content.iter_chunked(max_chunk_size):
                                await afp.write(chunk)
                        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                            if context and logger:
                                await log_error('err',
                                        f"Error response while downloading {url!r}.",
                                        context['issuer_array'], url, context['source'], 'network', logger)
                            Retry()
                except ssl.SSLCertVerificationError as err:
                    if context and logger:
                        await log_error('err',
                                f"SSL Error. {err} URL: {url}.",
                                context['issuer_array'], url, context['source'], 'network', logger)
                Retry()
        if cache_dir and file_with_dir:
            await copyfile(filepath, file_with_dir)


def make_class(model_cls, table_suffix):
    table_suffix = str(table_suffix)
    cache_key = (model_cls, table_suffix)
    if cache_key in _DYNAMIC_CLASS_CACHE:
        return _DYNAMIC_CLASS_CACHE[cache_key]

    new_table_name = "_".join([model_cls.__tablename__, table_suffix])
    metadata = model_cls.metadata

    if new_table_name in metadata.tables:
        new_table = metadata.tables[new_table_name]
    else:
        new_table = model_cls.__table__.tometadata(metadata, name=new_table_name)

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
    await push_objects(err_obj_list, cls)
    err_obj_list.clear()
    err_obj_key.clear()


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
                except (SQLAlchemyError, UniqueViolationError) as e:
                    print(e)

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


def deduplicate_dicts(dict_list, key_fields):
                seen = {}
                for d in dict_list:
                    key = tuple(d.get(k) for k in key_fields)
                    seen[key] = d  # keep the last occurrence
                return list(seen.values())


async def push_objects(obj_list, cls, rewrite=False):
    if obj_list:
        if len(obj_list) == 1 and not rewrite:
            return await push_objects_slow(obj_list, cls)
        
        # if hasattr(cls, "__my_initial_indexes__"):
        #     for i in (cls.__my_initial_indexes__):
        #         obj_list = deduplicate_dicts(obj_list, i.get("index_elements"))
        # else:
        #     obj_list = deduplicate_dicts(obj_list, cls.__my_index_elements__)
        
        try:
            #raise UniqueViolationError
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
        except (UniqueViolationError, NotImplementedError):

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
                stmt = db.insert(cls.__table__).values(obj_list)
                if hasattr(cls, "__my_index_elements__"):
                    stmt = stmt.on_conflict_do_nothing(index_elements=cls.__my_index_elements__)
                await stmt.status()

                
import logging
from sqlalchemy import or_
def print_time_info(start):
    now = datetime.datetime.utcnow()
    now = now.replace(tzinfo=pytz.utc)
    start = start.replace(tzinfo=pytz.utc)
    delta = now - start
    print('Import Time Delta: ', delta)
    print('Import took ', humanize.naturaldelta(delta))
