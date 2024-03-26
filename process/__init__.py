import os
import asyncio
import uvloop
import click
from ruamel.ext.msgpack import packb, unpackb

from arq.connections import RedisSettings

from process.initial import main as initiate_mrf, finish_main as finish_mrf, init_file, startup as initial_startup, \
    shutdown as initial_shutdown, process_plan, process_json_index, process_provider, save_mrf_data
from process.attributes import main as initiate_plan_attributes, save_attributes, process_state_attributes, \
    process_attributes, process_prices, process_benefits, startup as attr_startup, shutdown as attr_shutdown
from process.npi import main as initiate_npi, process_npi_chunk, save_npi_data, startup as npi_startup, \
    shutdown as npi_shutdown, process_data as process_npi_data
from process.nucc import main as initiate_nucc, startup as nucc_startup, shutdown as nucc_shutdown, \
    process_data as process_nucc_data
from process.ext.utils import db_startup

uvloop.install()

class MRF_start:
    functions = [init_file]
    on_startup = initial_startup
    # on_shutdown = initial_shutdown
    max_jobs = 20
    queue_read_limit = 10
    job_timeout = 3600
    burst = True
    queue_name = 'arq:MRF_start'
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: packb(b, datetime=True)
    job_deserializer = lambda b: unpackb(b, timestamp=3, raw=False)


class MRF:
    functions = [save_mrf_data, process_plan, process_json_index, process_provider]
    on_startup = db_startup
    # on_shutdown = init_shutdown
    max_jobs = int(os.environ.get('HLTHPRT_MAX_MRF_JOBS')) if os.environ.get('HLTHPRT_MAX_MRF_JOBS') else 20
    queue_read_limit = 5*max_jobs
    job_timeout = 7200
    burst = True
    queue_name = 'arq:MRF'
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: packb(b, datetime=True)
    job_deserializer = lambda b: unpackb(b, timestamp=3, raw=False)


class MRF_finish:
    functions = [initial_shutdown]
    on_startup = db_startup
    # on_shutdown = initial_shutdown
    max_jobs = 20
    queue_read_limit = 10
    job_timeout = 14400
    burst = True
    queue_name = 'arq:MRF_finish'
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: packb(b, datetime=True)
    job_deserializer = lambda b: unpackb(b, timestamp=3, raw=False)


class Attributes:
    functions = [process_attributes, process_state_attributes, process_prices, process_benefits, save_attributes]
    on_startup = attr_startup
    on_shutdown = attr_shutdown
    max_jobs=20
    queue_read_limit = 5
    job_timeout = 3600
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: packb(b, datetime=True)
    job_deserializer = lambda b: unpackb(b, timestamp=3, raw=False)


class NPI:
    functions = [process_npi_data, save_npi_data, process_npi_chunk]
    on_startup = npi_startup
    on_shutdown = npi_shutdown
    max_jobs=20
    queue_read_limit = 5
    queue_name = 'arq:NPI'
    job_timeout=86400
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: packb(b, datetime=True)
    job_deserializer = lambda b: unpackb(b, timestamp=3, raw=False)


class NPI_finish:
    functions = [npi_shutdown]
    on_startup = db_startup
    # on_shutdown = initial_shutdown
    max_jobs = 20
    queue_read_limit = 10
    job_timeout = 3600
    burst = True
    queue_name = 'arq:NPI_finish'
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: packb(b, datetime=True)
    job_deserializer = lambda b: unpackb(b, timestamp=3, raw=False)


class NUCC:
    functions = [process_nucc_data]
    on_startup = nucc_startup
    on_shutdown = nucc_shutdown
    max_jobs=20
    queue_read_limit = 5
    job_timeout=86400
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: packb(b, datetime=True)
    job_deserializer = lambda b: unpackb(b, timestamp=3, raw=False)


@click.group()
def process_group():
    """
       Initiate run of importers
    """


@click.group()
def process_group_end():
    """
       Finalize run of importers
    """


@click.command(help="Run CMSGOV MRF Import")
def mrf():
    asyncio.run(initiate_mrf())

@click.command(help="Finish CMSGOV MRF Import")
def mrf_end():
    asyncio.run(finish_mrf())


@click.command(help="Run Plan Attributes Import from CMS.gov")
def plan_attributes():
    asyncio.run(initiate_plan_attributes())

@click.command(help="Run NPPES Import with Weekly updates")
def npi():
    asyncio.run(initiate_npi())


@click.command(help="Run NUCC Taxonomy Import")
def nucc():
    asyncio.run(initiate_nucc())


process_group.add_command(mrf)
process_group_end.add_command(mrf_end, 'mrf')
process_group.add_command(plan_attributes)
process_group.add_command(npi)
process_group.add_command(nucc)
