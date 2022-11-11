import os
import datetime
import httpx
import gino
import ssl
from gino.exceptions import GinoException
from asyncpg.exceptions import UniqueViolationError, InterfaceError
from aiofile import async_open
from arq import Retry
from fastcrc import crc32
import humanize
from sqlalchemy.dialects.postgresql import insert
from db.connection import db
from db.json_mixin import JSONOutputMixin

if os.environ.get('HLTHPRT_SOCKS_PROXY'):
    transport = httpx.AsyncHTTPTransport(retries=3, proxy=httpx.Proxy(os.environ['HLTHPRT_SOCKS_PROXY']))
else:
    transport = httpx.AsyncHTTPTransport(retries=3)

HTTP_CHUNK_SIZE = 512 * 1024
headers = {'user-agent': 'Mozilla/5.0 (compatible; Healthporta Healthcare MRF API Importer/1.0; +https://github.com/EndurantDevs/healthcare-mrf-api)'}

timeout = httpx.Timeout(20.0)
client = httpx.AsyncClient(transport=transport, timeout=timeout, headers=headers, follow_redirects=True)

async def download_it(url):
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



async def download_it_and_save(url, filepath, context=None, logger=None):
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
                    async for chunk in response.aiter_bytes(chunk_size=HTTP_CHUNK_SIZE):
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

async def log_error(type, error, issuer_array, url, source, level, cls):
    for issuer_id in issuer_array:
        checksum = '|'.join([type, str(error), str(issuer_id), str(url), source, level])
        checksum = bytes(checksum, 'utf-8')
        checksum = crc32.cksum(checksum)
        err_obj_list.append({
            'issuer_id': issuer_id,
            'checksum': checksum,
            'type': type,
            'text': error,
            'url': url,
            'source': source,
            'level': level
        })
        if len(err_obj_list) > 99:
            await flush_error_log(cls)
            

async def flush_error_log(cls):
    await push_objects(err_obj_list, cls)
    err_obj_list.clear()


async def push_objects(obj_list, cls):
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


def print_time_info(start):
    now = datetime.datetime.now()
    delta = now - start
    print('Import Time Delta: ', delta)
    print('Import took ', humanize.naturaldelta(delta))
