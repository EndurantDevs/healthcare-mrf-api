import os
import asyncio
import uvloop
import click
import msgpack

from arq.connections import RedisSettings

from process.initial import main as initiate_mrf, init_file, startup, shutdown, process_plan, process_json_index
uvloop.install()


class MRF:
    functions = [init_file, process_plan, process_json_index]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs=10
    # queue_read_limit = 5
    redis_settings = RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS'))
    job_serializer = msgpack.packb
    job_deserializer = lambda b: msgpack.unpackb(b, raw=False)



@click.group()
def process_group():
    """
       Initiate run of importers
    """


@click.command(help="Run CMSGOV MRF Import")
def mrf():
    asyncio.run(initiate_mrf())


process_group.add_command(mrf)

