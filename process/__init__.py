# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
import asyncio
import click

from process.initial import main as initiate_mrf, finish_main as finish_mrf, init_file, startup as initial_startup, \
    shutdown as shutdown_mrf, process_plan, process_json_index, process_provider, save_mrf_data
from process.attributes import main as initiate_plan_attributes, save_attributes, process_state_attributes, \
    process_attributes, process_prices, process_benefits, startup as attr_startup, shutdown as attr_shutdown
from process.npi import main as initiate_npi, process_npi_chunk, save_npi_data, startup as npi_startup, \
    shutdown as npi_shutdown, process_data as process_npi_data
from process.nucc import main as initiate_nucc, startup as nucc_startup, shutdown as nucc_shutdown, \
    process_data as process_nucc_data
from process.ext.utils import db_startup
from process.serialization import deserialize_job, serialize_job
from process.redis_config import build_redis_settings


class MRF:
    functions = [init_file, save_mrf_data, process_plan, process_json_index, process_provider]
    on_startup = initial_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_MRF_JOBS')) if os.environ.get('HLTHPRT_MAX_MRF_JOBS') else 20
    queue_read_limit = 2*max_jobs
    job_timeout = 7200
    burst = True
    queue_name = 'arq:MRF'
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class MRF_start:  # pylint: disable=invalid-name
    functions = [init_file]
    on_startup = initial_startup
    max_jobs = 20
    queue_read_limit = 10
    job_timeout = 3600
    burst = True
    queue_name = 'arq:MRF_start'
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class MRF_finish:  # pylint: disable=invalid-name
    functions = [shutdown_mrf]
    on_startup = db_startup
    max_jobs = 20
    queue_read_limit = 10
    job_timeout = 14400
    burst = True
    queue_name = 'arq:MRF_finish'
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class Attributes:
    functions = [process_attributes, process_state_attributes, process_prices, process_benefits, save_attributes]
    on_startup = attr_startup
    on_shutdown = attr_shutdown
    max_jobs = 20
    queue_read_limit = 5
    job_timeout = 3600
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class NPI:
    functions = [process_npi_data, save_npi_data, process_npi_chunk]
    on_startup = npi_startup
    on_shutdown = npi_shutdown
    max_jobs = 20
    queue_read_limit = 5
    queue_name = 'arq:NPI'
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class NPI_finish:  # pylint: disable=invalid-name
    functions = [npi_shutdown]
    on_startup = db_startup
    max_jobs = 20
    queue_read_limit = 10
    job_timeout = 3600
    burst = True
    queue_name = 'arq:NPI_finish'
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class NUCC:
    functions = [process_nucc_data]
    on_startup = nucc_startup
    on_shutdown = nucc_shutdown
    max_jobs = 20
    queue_read_limit = 5
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


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
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def mrf(test: bool):
    asyncio.run(initiate_mrf(test_mode=test))

@click.command(help="Finish CMSGOV MRF Import")
def mrf_end():
    asyncio.run(finish_mrf())


@click.command(help="Run Plan Attributes Import from CMS.gov")
def plan_attributes():
    asyncio.run(initiate_plan_attributes())

@click.command(help="Run NPPES Import with Weekly updates")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def npi(test: bool):
    asyncio.run(initiate_npi(test_mode=test))


@click.command(help="Run NUCC Taxonomy Import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def nucc(test: bool):
    asyncio.run(initiate_nucc(test_mode=test))


process_group.add_command(mrf)
process_group_end.add_command(mrf_end, 'mrf')
process_group.add_command(plan_attributes)
process_group.add_command(npi)
process_group.add_command(nucc)
