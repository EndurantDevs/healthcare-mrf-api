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
from process.geo_census_import import geo_census_lookup
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
from process.provider_quality import (finish_main as finish_provider_quality,
                                      main as initiate_provider_quality,
                                      provider_quality_finalize,
                                      provider_quality_materialize_domain_shard,
                                      provider_quality_materialize_lsh_shard,
                                      provider_quality_materialize_measure_shard,
                                      provider_quality_materialize_score_shard,
                                      provider_quality_process_chunk,
                                      provider_quality_start)
from process.provider_enrichment import (main as initiate_provider_enrichment,
                                         process_data as process_provider_enrichment_data,
                                         save_provider_enrichment_data,
                                         shutdown as provider_enrichment_shutdown,
                                         startup as provider_enrichment_startup)
from process.partd_formulary_network import (
    finish_main as finish_partd_formulary_network,
    main as initiate_partd_formulary_network,
    partd_formulary_network_finalize,
    partd_formulary_network_process_chunk,
    partd_formulary_network_start,
)
from process.pharmacy_license import (
    finish_main as finish_pharmacy_license,
    main as initiate_pharmacy_license,
    pharmacy_license_finalize,
    pharmacy_license_start,
)
from process.places_zcta import main as initiate_places_zcta
from process.places_zcta import process_data as process_places_zcta_data
from process.places_zcta import shutdown as places_zcta_shutdown
from process.places_zcta import startup as places_zcta_startup
from process.lodes import main as initiate_lodes
from process.lodes import process_data as process_lodes_data
from process.lodes import shutdown as lodes_shutdown
from process.lodes import startup as lodes_startup
from process.medicare_enrollment import main as initiate_medicare_enrollment
from process.medicare_enrollment import process_data as process_medicare_enrollment_data
from process.medicare_enrollment import shutdown as medicare_enrollment_shutdown
from process.medicare_enrollment import startup as medicare_enrollment_startup
from process.cms_doctors import main as initiate_cms_doctors
from process.cms_doctors import process_data as process_cms_doctors_data
from process.cms_doctors import shutdown as cms_doctors_shutdown
from process.cms_doctors import startup as cms_doctors_startup
from process.facility_anchors import main as initiate_facility_anchors
from process.facility_anchors import process_data as process_facility_anchors_data
from process.facility_anchors import shutdown as facility_anchors_shutdown
from process.facility_anchors import startup as facility_anchors_startup
from process.pharmacy_economics import main as initiate_pharmacy_economics
from process.pharmacy_economics import process_data as process_pharmacy_economics_data
from process.pharmacy_economics import shutdown as pharmacy_economics_shutdown
from process.pharmacy_economics import startup as pharmacy_economics_startup
from process.entity_address_unified import main as initiate_entity_address_unified
from process.entity_address_unified import process_data as process_entity_address_unified_data
from process.entity_address_unified import shutdown as entity_address_unified_shutdown
from process.entity_address_unified import startup as entity_address_unified_startup
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


class ProviderQuality:
    functions = [
        provider_quality_start,
        provider_quality_process_chunk,
        provider_quality_materialize_lsh_shard,
        provider_quality_materialize_measure_shard,
        provider_quality_materialize_domain_shard,
        provider_quality_materialize_score_shard,
    ]
    on_startup = db_startup
    max_jobs = (
        int(os.environ.get('HLTHPRT_MAX_PROVIDER_QUALITY_JOBS'))
        if os.environ.get('HLTHPRT_MAX_PROVIDER_QUALITY_JOBS')
        else int(os.environ.get('HLTHPRT_PROVIDER_QUALITY_SHARD_PARALLELISM', 8))
    )
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:ProviderQuality'
    job_timeout = 86400
    max_tries = (
        int(os.environ.get('HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARD_MAX_TRIES'))
        if os.environ.get('HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARD_MAX_TRIES')
        else 20
    )
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class ProviderQuality_finish:  # pylint: disable=invalid-name
    functions = [
        provider_quality_finalize,
        provider_quality_materialize_lsh_shard,
        provider_quality_materialize_measure_shard,
        provider_quality_materialize_domain_shard,
        provider_quality_materialize_score_shard,
    ]
    on_startup = db_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_PROVIDER_QUALITY_FINISH_JOBS')) if os.environ.get('HLTHPRT_MAX_PROVIDER_QUALITY_FINISH_JOBS') else 5
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:ProviderQuality_finish'
    job_timeout = 86400
    max_tries = (
        int(os.environ.get('HLTHPRT_PROVIDER_QUALITY_FINALIZE_MAX_TRIES'))
        if os.environ.get('HLTHPRT_PROVIDER_QUALITY_FINALIZE_MAX_TRIES')
        else 720
    )
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class ProviderEnrichment:
    functions = [process_provider_enrichment_data, save_provider_enrichment_data]
    on_startup = provider_enrichment_startup
    on_shutdown = provider_enrichment_shutdown
    max_jobs = int(os.environ.get('HLTHPRT_MAX_PROVIDER_ENRICHMENT_JOBS')) if os.environ.get('HLTHPRT_MAX_PROVIDER_ENRICHMENT_JOBS') else 20
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:ProviderEnrichment'
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class ProviderEnrichment_finish:  # pylint: disable=invalid-name
    functions = [provider_enrichment_shutdown]
    on_startup = db_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_PROVIDER_ENRICHMENT_FINISH_JOBS')) if os.environ.get('HLTHPRT_MAX_PROVIDER_ENRICHMENT_FINISH_JOBS') else 5
    queue_read_limit = 2 * max_jobs
    queue_name = 'arq:ProviderEnrichment_finish'
    job_timeout = 86400
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PartDFormularyNetwork:
    functions = [partd_formulary_network_start, partd_formulary_network_process_chunk]
    on_startup = db_startup
    max_jobs = int(os.environ.get("HLTHPRT_MAX_PARTD_JOBS")) if os.environ.get("HLTHPRT_MAX_PARTD_JOBS") else 4
    queue_read_limit = 2 * max_jobs
    queue_name = "arq:PartDFormularyNetwork"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PartDFormularyNetwork_finish:  # pylint: disable=invalid-name
    functions = [partd_formulary_network_finalize]
    on_startup = db_startup
    max_jobs = int(os.environ.get("HLTHPRT_MAX_PARTD_FINISH_JOBS")) if os.environ.get("HLTHPRT_MAX_PARTD_FINISH_JOBS") else 4
    queue_read_limit = 2 * max_jobs
    queue_name = "arq:PartDFormularyNetwork_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PharmacyLicense:
    functions = [pharmacy_license_start]
    on_startup = db_startup
    max_jobs = int(os.environ.get("HLTHPRT_MAX_PHARM_LICENSE_JOBS")) if os.environ.get("HLTHPRT_MAX_PHARM_LICENSE_JOBS") else 4
    queue_read_limit = 2 * max_jobs
    queue_name = "arq:PharmacyLicense"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PharmacyLicense_finish:  # pylint: disable=invalid-name
    functions = [pharmacy_license_finalize]
    on_startup = db_startup
    max_jobs = int(os.environ.get("HLTHPRT_MAX_PHARM_LICENSE_FINISH_JOBS")) if os.environ.get("HLTHPRT_MAX_PHARM_LICENSE_FINISH_JOBS") else 4
    queue_read_limit = 2 * max_jobs
    queue_name = "arq:PharmacyLicense_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PlacesZcta:
    functions = [process_places_zcta_data]
    on_startup = places_zcta_startup
    on_shutdown = places_zcta_shutdown
    max_jobs = int(os.environ.get("HLTHPRT_MAX_PLACES_ZCTA_JOBS")) if os.environ.get("HLTHPRT_MAX_PLACES_ZCTA_JOBS") else 4
    queue_read_limit = 2 * max_jobs
    queue_name = "arq:PlacesZcta"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PlacesZcta_finish:  # pylint: disable=invalid-name
    functions = [places_zcta_shutdown]
    on_startup = db_startup
    max_jobs = int(os.environ.get("HLTHPRT_MAX_PLACES_ZCTA_FINISH_JOBS")) if os.environ.get("HLTHPRT_MAX_PLACES_ZCTA_FINISH_JOBS") else 4
    queue_read_limit = 2 * max_jobs
    queue_name = "arq:PlacesZcta_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class LODES:
    functions = [process_lodes_data]
    on_startup = lodes_startup
    on_shutdown = lodes_shutdown
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:LODES"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class LODES_finish:  # pylint: disable=invalid-name
    functions = [lodes_shutdown]
    on_startup = db_startup
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:LODES_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class MedicareEnrollment:
    functions = [process_medicare_enrollment_data]
    on_startup = medicare_enrollment_startup
    on_shutdown = medicare_enrollment_shutdown
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:MedicareEnrollment"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class MedicareEnrollment_finish:  # pylint: disable=invalid-name
    functions = [medicare_enrollment_shutdown]
    on_startup = db_startup
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:MedicareEnrollment_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class CMSDoctors:
    functions = [process_cms_doctors_data]
    on_startup = cms_doctors_startup
    on_shutdown = cms_doctors_shutdown
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:CMSDoctors"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class CMSDoctors_finish:  # pylint: disable=invalid-name
    functions = [cms_doctors_shutdown]
    on_startup = db_startup
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:CMSDoctors_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class FacilityAnchors:
    functions = [process_facility_anchors_data]
    on_startup = facility_anchors_startup
    on_shutdown = facility_anchors_shutdown
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:FacilityAnchors"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class FacilityAnchors_finish:  # pylint: disable=invalid-name
    functions = [facility_anchors_shutdown]
    on_startup = db_startup
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:FacilityAnchors_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PharmacyEconomics:
    functions = [process_pharmacy_economics_data]
    on_startup = pharmacy_economics_startup
    on_shutdown = pharmacy_economics_shutdown
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:PharmacyEconomics"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PharmacyEconomics_finish:  # pylint: disable=invalid-name
    functions = [pharmacy_economics_shutdown]
    on_startup = db_startup
    max_jobs = 4
    queue_read_limit = 8
    queue_name = "arq:PharmacyEconomics_finish"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class EntityAddressUnified:
    functions = [process_entity_address_unified_data]
    on_startup = entity_address_unified_startup
    on_shutdown = entity_address_unified_shutdown
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:EntityAddressUnified"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class EntityAddressUnified_finish:  # pylint: disable=invalid-name
    functions = [entity_address_unified_shutdown]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:EntityAddressUnified_finish"
    job_timeout = 3600
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


@click.command(help="Run provider quality import (QPP + SVI + claims-derived quality model)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def provider_quality(test: bool, import_id: str | None):
    asyncio.run(initiate_provider_quality(test_mode=test, import_id=import_id))


@click.command(help="Run provider enrichment import (PECOS + Medicare enrollment)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def provider_enrichment(test: bool):
    asyncio.run(initiate_provider_enrichment(test_mode=test))


@click.command(help="Finish provider quality import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for staging tables.")
@click.option("--run-id", required=True, help="Run id emitted by `start provider-quality`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Optional manifest path override.")
def provider_quality_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    asyncio.run(
        finish_provider_quality(
            import_id=import_id,
            run_id=run_id,
            test_mode=test,
            manifest_path=manifest_path,
        )
    )


@click.command(help="Run CMS Part D formulary + pharmacy network import (quarterly-first)")
@click.option("--test", is_flag=True, help="Process a smaller subset for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def partd_formulary_network(test: bool, import_id: str | None):
    asyncio.run(initiate_partd_formulary_network(test_mode=test, import_id=import_id))


@click.command(help="Run state board pharmacy license import (NPI-first canonical)")
@click.option("--test", is_flag=True, help="Process a smaller synthetic subset for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def pharmacy_license(test: bool, import_id: str | None):
    asyncio.run(initiate_pharmacy_license(test_mode=test, import_id=import_id))


@click.command(help="Run CDC PLACES ZCTA import (latest year only)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def places_zcta(test: bool):
    asyncio.run(initiate_places_zcta(test_mode=test))


@click.command(help="Run LEHD/LODES workplace aggregate import")
@click.option("--test", is_flag=True, help="Process a small subset of states for a quick smoke run.")
def lodes(test: bool):
    asyncio.run(initiate_lodes(test_mode=test))


@click.command(help="Run CMS Medicare Enrollment Dashboard import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def medicare_enrollment(test: bool):
    asyncio.run(initiate_medicare_enrollment(test_mode=test))


@click.command(help="Run CMS Doctors and Clinicians import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def cms_doctors(test: bool):
    asyncio.run(initiate_cms_doctors(test_mode=test))


@click.command(help="Run Facility Anchors import (HRSA FQHCs + CMS Hospitals)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def facility_anchors(test: bool):
    asyncio.run(initiate_facility_anchors(test_mode=test))


@click.command(help="Run Pharmacy Economics import (SDUD + NADAC + FUL margins)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def pharmacy_economics(test: bool):
    asyncio.run(initiate_pharmacy_economics(test_mode=test))


@click.command(help="Run unified entity-address materialization import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def entity_address_unified(test: bool):
    asyncio.run(initiate_entity_address_unified(test_mode=test))


@click.command(help="Finish CMS Part D formulary + pharmacy network import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for the run.")
@click.option("--run-id", required=True, help="Run id emitted by `start partd-formulary-network`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Unused; kept for command signature consistency.")
def partd_formulary_network_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    asyncio.run(
        finish_partd_formulary_network(
            import_id=import_id,
            run_id=run_id,
            test_mode=test,
            manifest_path=manifest_path,
        )
    )


@click.command(help="Finish pharmacy-license import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for the run.")
@click.option("--run-id", required=True, help="Run id emitted by `start pharmacy-license`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Unused; kept for command signature consistency.")
def pharmacy_license_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    asyncio.run(
        finish_pharmacy_license(
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
process_group_end.add_command(provider_quality_end, 'provider-quality')
process_group_end.add_command(partd_formulary_network_end, "partd-formulary-network")
process_group_end.add_command(pharmacy_license_end, "pharmacy-license")
process_group.add_command(plan_attributes)
process_group.add_command(npi)
process_group.add_command(ptg)
process_group.add_command(claims_pricing, name="claims-pricing")
process_group.add_command(claims_procedures, name="claims-procedures")
process_group.add_command(drug_claims, name="drug-claims")
process_group.add_command(provider_quality, name="provider-quality")
process_group.add_command(partd_formulary_network, name="partd-formulary-network")
process_group.add_command(pharmacy_license, name="pharmacy-license")
process_group.add_command(places_zcta, name="places-zcta")
process_group.add_command(provider_enrichment, name="provider-enrichment")
process_group.add_command(lodes, name="lodes")
process_group.add_command(medicare_enrollment, name="medicare-enrollment")
process_group.add_command(cms_doctors, name="cms-doctors")
process_group.add_command(facility_anchors, name="facility-anchors")
process_group.add_command(pharmacy_economics, name="pharmacy-economics")
process_group.add_command(entity_address_unified, name="entity-address-unified")
process_group.add_command(geo_lookup, name="geo")
process_group.add_command(geo_census_lookup, name="geo-census")
process_group.add_command(nucc)
