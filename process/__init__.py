# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import os

import click

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    uvloop = None  # noqa: F841

from process.attributes import main as initiate_plan_attributes
from process.attributes import (process_attributes, process_benefits,
                                process_prices, process_state_attributes,
                                save_attributes)
from process.attributes import shutdown as attr_shutdown
from process.attributes import startup as attr_startup
from process.ext.utils import db_startup
from process.geo_import import geo_lookup
from process.initial import finish_main as finish_mrf
from process.initial import init_file
from process.initial import main as initiate_mrf
from process.initial import (process_formulary, process_json_index,
                             process_plan, process_provider, save_mrf_data)
from process.initial import shutdown as shutdown_mrf
from process.initial import startup as initial_startup
from process.npi import main as initiate_npi
from process.npi import process_data as process_npi_data
from process.npi import process_npi_chunk, save_npi_data
from process.npi import shutdown as npi_shutdown
from process.npi import startup as npi_startup
from process.nucc import main as initiate_nucc
from process.nucc import process_data as process_nucc_data
from process.nucc import shutdown as nucc_shutdown
from process.nucc import startup as nucc_startup
from process.ptg import main as initiate_ptg
from process.claims_pricing import (claims_pricing_finalize,
                                    claims_pricing_process_chunk,
                                    claims_pricing_start,
                                    finish_main as finish_claims_pricing,
                                    main as initiate_claims_pricing)
from process.drug_claims import (drug_claims_finalize,
                                 drug_claims_process_chunk,
                                 drug_claims_start,
                                 finish_main as finish_drug_claims,
                                 main as initiate_drug_claims)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job


class MRF:
    functions = [init_file, save_mrf_data, process_plan, process_json_index, process_provider, process_formulary]
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
    queue_name = "arq:Attributes"
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
    queue_name = 'arq:NUCC'
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class ClaimsPricing:
    functions = [claims_pricing_start, claims_pricing_process_chunk]
    on_startup = db_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_CLAIMS_JOBS')) if os.environ.get('HLTHPRT_MAX_CLAIMS_JOBS') else 20
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:ClaimsPricing'
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class ClaimsProcedures(ClaimsPricing):
    queue_name = 'arq:ClaimsPricing'


class ClaimsPricing_finish:  # pylint: disable=invalid-name
    functions = [claims_pricing_finalize]
    on_startup = db_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_CLAIMS_FINISH_JOBS')) if os.environ.get('HLTHPRT_MAX_CLAIMS_FINISH_JOBS') else 5
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:ClaimsPricing_finish'
    job_timeout = 86400
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class ClaimsProcedures_finish(ClaimsPricing_finish):  # pylint: disable=invalid-name
    queue_name = 'arq:ClaimsPricing_finish'


class DrugClaims:
    functions = [drug_claims_start, drug_claims_process_chunk]
    on_startup = db_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_DRUG_CLAIMS_JOBS')) if os.environ.get('HLTHPRT_MAX_DRUG_CLAIMS_JOBS') else 20
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:DrugClaims'
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class DrugClaims_finish:  # pylint: disable=invalid-name
    functions = [drug_claims_finalize]
    on_startup = db_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_DRUG_CLAIMS_FINISH_JOBS')) if os.environ.get('HLTHPRT_MAX_DRUG_CLAIMS_FINISH_JOBS') else 5
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:DrugClaims_finish'
    job_timeout = 86400
    burst = True
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
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def plan_attributes(test: bool):
    asyncio.run(initiate_plan_attributes(test_mode=test))

@click.command(help="Run NPPES Import with Weekly updates")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def npi(test: bool):
    asyncio.run(initiate_npi(test_mode=test))

@click.command(help="Run Transparency in Coverage (PTG) Import")
@click.option("--toc-url", multiple=True, help="URL of a table-of-contents file to seed jobs (repeatable).")
@click.option("--toc-list", type=click.Path(exists=True), help="Path to file containing TOC URLs (newline or JSON list).")
@click.option("--in-network-url", help="URL of a single in-network rates file.")
@click.option("--allowed-url", help="URL of a single allowed-amounts file.")
@click.option("--provider-ref-url", help="URL of a provider-reference file.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def ptg(
    toc_url: tuple[str, ...],
    toc_list: str | None,
    in_network_url: str | None,
    allowed_url: str | None,
    provider_ref_url: str | None,
    import_id: str | None,
    test: bool,
):
    asyncio.run(
        initiate_ptg(
            test_mode=test,
            toc_urls=list(toc_url),
            toc_list=toc_list,
            in_network_url=in_network_url,
            allowed_url=allowed_url,
            provider_ref_url=provider_ref_url,
            import_id=import_id,
        )
    )


@click.command(help="Run NUCC Taxonomy Import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def nucc(test: bool):
    asyncio.run(initiate_nucc(test_mode=test))


@click.command(help="Run CMS claims pricing import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def claims_pricing(test: bool, import_id: str | None):
    asyncio.run(initiate_claims_pricing(test_mode=test, import_id=import_id))


@click.command(help="Run CMS procedures claims import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def claims_procedures(test: bool, import_id: str | None):
    asyncio.run(initiate_claims_pricing(test_mode=test, import_id=import_id))


@click.command(help="Finish CMS claims pricing import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for staging tables.")
@click.option("--run-id", required=True, help="Run id emitted by `start claims-pricing`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Optional manifest path override.")
def claims_pricing_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    asyncio.run(
        finish_claims_pricing(
            import_id=import_id,
            run_id=run_id,
            test_mode=test,
            manifest_path=manifest_path,
        )
    )


@click.command(help="Run CMS Part D drug claims import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def drug_claims(test: bool, import_id: str | None):
    asyncio.run(initiate_drug_claims(test_mode=test, import_id=import_id))


@click.command(help="Finish CMS Part D drug claims import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for staging tables.")
@click.option("--run-id", required=True, help="Run id emitted by `start drug-claims`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Optional manifest path override.")
def drug_claims_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    asyncio.run(
        finish_drug_claims(
            import_id=import_id,
            run_id=run_id,
            test_mode=test,
            manifest_path=manifest_path,
        )
    )


process_group.add_command(mrf)
process_group_end.add_command(mrf_end, 'mrf')
process_group_end.add_command(claims_pricing_end, 'claims-pricing')
process_group_end.add_command(claims_pricing_end, 'claims-procedures')
process_group_end.add_command(drug_claims_end, 'drug-claims')
process_group.add_command(plan_attributes)
process_group.add_command(npi)
process_group.add_command(ptg)
process_group.add_command(claims_pricing, name="claims-pricing")
process_group.add_command(claims_procedures, name="claims-procedures")
process_group.add_command(drug_claims, name="drug-claims")
process_group.add_command(geo_lookup, name="geo")
process_group.add_command(nucc)
