import os
import asyncio
import uvloop
import click
import msgpack

from arq.connections import RedisSettings

from process.initial import main as initiate_mrf, init_file, startup as initial_startup, shutdown as initial_shutdown, process_plan, process_json_index
from process.attributes import main as initiate_plan_attributes, save_attributes, process_attributes, startup as attr_startup, shutdown as attr_shutdown
from process.npi import main as initiate_npi, process_npi_chunk, save_npi_data, startup as npi_startup, shutdown as npi_shutdown, process_data as process_npi_data
uvloop.install()


class MRF:
    functions = [init_file, process_plan, process_json_index]
    on_startup = initial_startup
    on_shutdown = initial_shutdown
    #max_jobs=10
    #queue_read_limit = 10
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


class Attributes:
    functions = [process_attributes, save_attributes]
    on_startup = attr_startup
    on_shutdown = attr_shutdown
    max_jobs=20
    queue_read_limit = 5
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)


class NPI:
    functions = [process_npi_data, save_npi_data, process_npi_chunk]
    on_startup = npi_startup
    on_shutdown = npi_shutdown
    max_jobs=20
    queue_read_limit = 5
    job_timeout=86400
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = lambda b: msgpack.packb(b, datetime=True)
    job_deserializer = lambda b: msgpack.unpackb(b, timestamp=3, raw=False)

@click.group()
def process_group():
    """
       Initiate run of importers
    """


@click.command(help="Run CMSGOV MRF Import")
def mrf():
    asyncio.run(initiate_mrf())

@click.command(help="Run Plan Attributes Import from CMS.gov")
def plan_attributes():
    asyncio.run(initiate_plan_attributes())

@click.command(help="Run NPPES Import with Weekly updates")
def npi():
    asyncio.run(initiate_npi())


process_group.add_command(mrf)
process_group.add_command(plan_attributes)
process_group.add_command(npi)

