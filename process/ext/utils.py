import os
import datetime
import httpx
from pathlib import Path, PurePath
import asyncio

import pytz

from db.connection import init_db, db

from aioshutil import copyfile
from sqlalchemy import Index, and_, inspect
import ssl
from gino.exceptions import GinoException
from asyncpg.exceptions import UniqueViolationError, InterfaceError, InvalidColumnReferenceError
from aiofile import async_open
from arq import Retry
from fastcrc import crc32, crc16
import humanize
from sqlalchemy.dialects.postgresql import insert
from random import choice
import json
from db.json_mixin import JSONOutputMixin

if os.environ.get('HLTHPRT_SOCKS_PROXY1'):
    transport = httpx.AsyncHTTPTransport(retries=3,
                                         proxy=httpx.Proxy(choice(json.loads(os.environ['HLTHPRT_SOCKS_PROXY']))))
else:
    transport = httpx.AsyncHTTPTransport(retries=3)

HTTP_CHUNK_SIZE = 1024 * 1024
headers = {'user-agent': 'Mozilla/5.0 (compatible; Healthporta Healthcare MRF API Importer/1.1; +https://github.com/EndurantDevs/healthcare-mrf-api)'}

timeout = httpx.Timeout(30.0)
client = httpx.AsyncClient(transport=transport, headers=headers, timeout=timeout, follow_redirects=True)

async def download_it(url, local_timeout=None):
    if local_timeout:
        local_timeout = httpx.Timeout(local_timeout)
        r = await client.get(url, timeout=local_timeout)
    else:
        r = await client.get(url)
    return r



async def download_it_and_save_nostream(url, filepath):
    async with async_open(filepath, 'wb+') as afp:
        response = await client.get(url)
        if response.status_code == 200:
            await afp.write(response.content)
        else:
            print(url, ' returns ', response.status_code)
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
            try:
                async with client.stream('GET', url) as response:
                    try:
                        response.raise_for_status()
                    except httpx.HTTPStatusError as exc:
                        if context and logger:
                            await log_error('err',
                                            f"Error response {exc.response.status_code} while requesting {exc.request.url!r}.",
                                            context['issuer_array'], url, context['source'], 'network', logger)
                        Retry()

                    try:
                        async for chunk in response.aiter_bytes(chunk_size=max_chunk_size):
                            await afp.write(chunk)
                    except (httpx.RequestError, httpx.ReadTimeout, httpx.TimeoutException, httpx.ReadError, httpx.NetworkError) as err:
                        if context and logger:
                            await log_error('err',
                                            f"Error response while downloading "
                                            f"{exc.request.url!r}.",
                                            context['issuer_array'], url, context['source'], 'network', logger)
                        Retry()
            except ssl.SSLCertVerificationError as err:
                if context and logger:
                    await log_error('err',
                                    f"SSL Error. {err}"
                                    f"URL: {url}.",
                                    context['issuer_array'], url, context['source'], 'network', logger)
                Retry()
        if cache_dir and file_with_dir:
            await copyfile(filepath, file_with_dir)


def make_class(Base, table_suffix):
    temp = None
    if hasattr(Base, '__table__'):
        try:
            temp = Base.__table__
            delattr(Base, '__table__')
        except AttributeError:
            pass

    class MyClass(Base):
        __tablename__ = '_'.join([Base.__tablename__, table_suffix])

    if temp is not None:
        Base.__table__ = temp

    return MyClass

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
                await insert(cls).values(obj_list).on_conflict_do_nothing(index_elements=cls.__my_index_elements__)\
                    .gino.model(cls).status()
            else:
                await cls.insert().gino.all(obj_list)
        except (GinoException, UniqueViolationError, InterfaceError):
            for obj in obj_list:
                try:
                    await cls.insert().gino.all([obj])
                except (GinoException, UniqueViolationError) as e:
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



async def push_objects(obj_list, cls, rewrite=False):
    if obj_list:
        if len(obj_list) == 1:
            return await push_objects_slow(obj_list, cls)

        try:
            #raise UniqueViolationError
            async with db.acquire() as conn:
                await conn.raw_connection.copy_records_to_table(cls.__tablename__,
                                                                schema_name=cls.__table_args__[0]['schema'],
                                                                columns=obj_list[0].keys(), records=IterateList(obj_list, obj_list[0].keys()))
            # print("All good!")
        except ValueError as exc:
            print(f"INPUT arr: {obj_list}")
            print(exc)
        except UniqueViolationError:
            # print("It is here!")
            try:
                rows_per_insert = 200
                obj_length = len(obj_list)
                insert_number = int (obj_length / rows_per_insert)
                #fix this - rewrite the slow part!!
                for i in range(0, insert_number+1):
                    rows_to = (i+1)*rows_per_insert
                    if rows_to > obj_length:
                        rows_to = obj_length
                    if rewrite:
                        async with db.acquire() as conn:
                            for o in obj_list[i * rows_per_insert:rows_to]:
                                stmt = insert(cls.__table__).values(o)
                                primary_keys = [key.name for key in inspect(cls.__table__).primary_key]
                                update_dict = {c.name: o[c.name] for c in stmt.excluded if (not c.primary_key and c.name in o)}
                                s = stmt.on_conflict_do_update(
                                    index_elements= primary_keys,
                                    set_=update_dict)
                                await s.gino.model(cls).status()
                                #await conn.raw_connection.execute(s.compile())

                        # async with db.acquire() as conn:
                        #     for o in obj_list[i * rows_per_insert:rows_to]:
                        #         async with conn.transaction():
                        #             await conn.execute(cls.insert().values(o))

                        #await cls.insert().gino.all(obj_list[i * rows_per_insert:rows_to])
                    else:
                        if hasattr(cls, "__my_index_elements__"):
                            await insert(cls).values(obj_list[i * rows_per_insert:rows_to]).on_conflict_do_nothing(
                                index_elements=cls.__my_index_elements__).gino.model(cls).status()
                        else:
                            async with db.acquire() as conn:
                                for o in obj_list[i * rows_per_insert:rows_to]:
                                    async with conn.transaction():
                                        s = cls.insert().values(o)
                                        await s.gino.model(cls).status()
                            #await cls.insert().gino.all(obj_list[i*rows_per_insert:rows_to])
            except (GinoException, UniqueViolationError, InterfaceError):
                # print("So bad, It is here!")
                for obj in obj_list:
                        if rewrite:
                            # print(f"UPDATING: {obj}")
                            index_array = [{'index_elements': cls.__my_index_elements__},]
                            if hasattr(cls, "__my_initial_indexes__"):
                                for index in (cls.__my_initial_indexes__):
                                    index_name = index.get("name", "_".join(index.get("index_elements")))
                                    index_array.append({'constraint': f'{cls.__tablename__}_idx_{index_name}',
                                                       'index_elements': index.get("index_elements")})

                            for index in (index_array):
                                index_and_array = []
                                for i in index.get("index_elements"):
                                    index_and_array.append(and_(getattr(cls, i) == obj[i]))
                                try:
                                    if ('index_elements' in index) and ('constraint' in index):
                                        # print('NO-NO!')
                                        # print((await cls.query.where(and_(*index_and_array)).gino.first()).to_json_dict())
                                        await insert(cls).values([obj]).on_conflict_do_update(
                                            index_where=and_(*index_and_array),
                                            index_elements=index['index_elements'],
                                            #index['index_elements'],
                                            #constraint=index['constraint'],
                                            set_=obj
                                        ).gino.model(cls).status()
                                        # print((await cls.query.where(and_(*index_and_array)).gino.first()).to_json_dict())
                                        # print('YO-YO!')
                                    else:
                                        await insert(cls).values([obj]).on_conflict_do_update(
                                            index_elements=index['index_elements'],
                                            set_=obj
                                        ).gino.model(cls).status()
                                    break
                                except (GinoException, UniqueViolationError) as e:
                                    if hasattr(cls, "__my_initial_indexes__") and 'constraint' in index:
                                        print(f"FAILED: {obj}, Index: {index}")
                                    continue
                                except (InvalidColumnReferenceError, InterfaceError) as e:
                                    try:
                                        async with db.acquire() as conn:
                                            async with conn.transaction():
                                                del_q = cls.delete
                                                for i in index.get("index_elements"):
                                                    del_q = del_q.where(getattr(cls, i) == obj[i])
                                                # print(del_q)
                                                # print((await cls.query.where(and_(*index_and_array)).gino.first()).to_json_dict())
                                                await del_q.gino.status()
                                                async with db.acquire() as conn:
                                                    for o in obj:
                                                        async with conn.transaction():
                                                            s = cls.insert().values(o)
                                                            await s.gino.model(cls).status()
                                                #await cls.insert().gino.all([obj])

                                                # print((await cls.query.where(and_(*index_and_array)).gino.first()).to_json_dict())
                                    except Exception as e:
                                        pass
                                        #print(e)
                        else:
                            try:
                                async with db.acquire() as conn:
                                    for o in obj:
                                        async with conn.transaction():
                                            s = cls.insert().values(o)
                                            await s.gino.model(cls).status()
                                #await cls.insert().gino.all([obj])
                            except Exception as e:
                                #print(e)
                                pass

def print_time_info(start):
    now = datetime.datetime.utcnow()
    now = now.replace(tzinfo=pytz.utc)
    start = start.replace(tzinfo=pytz.utc)
    delta = now - start
    print('Import Time Delta: ', delta)
    print('Import took ', humanize.naturaldelta(delta))
