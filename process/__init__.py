# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import os

import click

try:
    import uvloop
except ImportError:
    uvloop = None  # noqa: F841


_ASYNCIO_RUN = asyncio.run


def _run(coro):
    if uvloop is not None and asyncio.run is _ASYNCIO_RUN:
        return uvloop.run(coro)
    return asyncio.run(coro)


from process.attributes import main as initiate_plan_attributes
from process.attributes import (process_attributes, process_benefits,
                                plan_attributes_control_start,
                                process_prices, process_state_attributes,
                                save_attributes)
from process.attributes import shutdown as attr_shutdown
from process.attributes import startup as attr_startup
from process.control_lifecycle import control_single_job_start
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
from process.ptg_control import ptg_control_start
from process.claims_pricing import (claims_pricing_finalize,
                                    claims_pricing_process_chunk,
                                    claims_pricing_start,
                                    finish_main as finish_claims_pricing,
                                    main as initiate_claims_pricing)
from process.clinical_reference import main as initiate_clinical_reference
from process.code_sets import main as initiate_code_sets
from process.ms_drg import main as initiate_ms_drg
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
from process.mrf_source_discovery import main as initiate_mrf_source_discovery
from process.mrf_source_discovery import process_data as process_mrf_source_discovery_data
from process.mrf_source_discovery import shutdown as mrf_source_discovery_shutdown
from process.mrf_source_discovery import startup as mrf_source_discovery_startup
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
from process.ptg_address import main as initiate_ptg_address
from process.ptg_address import process_data as process_ptg_address_data
from process.ptg_address import shutdown as ptg_address_shutdown
from process.ptg_address import startup as ptg_address_startup
from process.address_archive_migration import main as initiate_address_archive_migration
from process.address_archive_migration import process_data as process_address_archive_migration_data
from process.openaddresses import main as initiate_openaddresses
from process.openaddresses import process_data as process_openaddresses_data
from process.openaddresses import shutdown as openaddresses_shutdown
from process.openaddresses import startup as openaddresses_startup
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job


class MRF:
    functions = [init_file, save_mrf_data, process_plan, process_json_index, process_provider, process_formulary]
    on_startup = initial_startup
    max_jobs = int(os.environ.get('HLTHPRT_MAX_MRF_JOBS')) if os.environ.get('HLTHPRT_MAX_MRF_JOBS') else 20
    queue_read_limit = (
        int(os.environ.get("HLTHPRT_MRF_QUEUE_READ_LIMIT"))
        if os.environ.get("HLTHPRT_MRF_QUEUE_READ_LIMIT")
        else 2 * max_jobs
    )
    job_timeout = int(os.environ.get("HLTHPRT_MRF_JOB_TIMEOUT")) if os.environ.get("HLTHPRT_MRF_JOB_TIMEOUT") else 7200
    burst = True
    queue_name = 'arq:MRF'
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PTG:  # pylint: disable=too-few-public-methods
    functions = [ptg_control_start]
    on_startup = db_startup
    max_jobs = int(os.environ.get("HLTHPRT_MAX_PTG_JOBS")) if os.environ.get("HLTHPRT_MAX_PTG_JOBS") else 1
    queue_read_limit = (
        int(os.environ.get("HLTHPRT_PTG_QUEUE_READ_LIMIT"))
        if os.environ.get("HLTHPRT_PTG_QUEUE_READ_LIMIT")
        else max(16, 4 * max_jobs)
    )
    job_timeout = int(os.environ.get("HLTHPRT_PTG_JOB_TIMEOUT")) if os.environ.get("HLTHPRT_PTG_JOB_TIMEOUT") else 172800
    burst = True
    queue_name = "arq:PTG"
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
    max_jobs = (
        int(os.environ.get("HLTHPRT_MAX_MRF_FINISH_JOBS"))
        if os.environ.get("HLTHPRT_MAX_MRF_FINISH_JOBS")
        else 1
    )
    queue_read_limit = max_jobs
    job_timeout = 14400
    burst = True
    queue_name = 'arq:MRF_finish'
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class Attributes:
    functions = [
        process_attributes,
        process_state_attributes,
        process_prices,
        process_benefits,
        save_attributes,
        control_single_job_start,
        plan_attributes_control_start,
    ]
    on_startup = attr_startup
    on_shutdown = attr_shutdown
    max_jobs = 20
    queue_read_limit = 5
    queue_name = "arq:Attributes"
    job_timeout = 3600
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class CodeSets:
    functions = [control_single_job_start]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:CodeSets"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class MSDRG:
    functions = [control_single_job_start]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:MSDRG"
    job_timeout = 86400
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class ClinicalReference:
    functions = [control_single_job_start]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:ClinicalReference"
    job_timeout = 86400
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class Geo:
    functions = [control_single_job_start]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:Geo"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class GeoCensus:
    functions = [control_single_job_start]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:GeoCensus"
    job_timeout = 3600
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class NPI:
    functions = [process_npi_data, save_npi_data, process_npi_chunk, control_single_job_start]
    on_startup = npi_startup
    on_shutdown = npi_shutdown
    max_jobs = int(os.environ.get("HLTHPRT_MAX_NPI_JOBS")) if os.environ.get("HLTHPRT_MAX_NPI_JOBS") else 20
    queue_read_limit = (
        int(os.environ.get("HLTHPRT_NPI_QUEUE_READ_LIMIT"))
        if os.environ.get("HLTHPRT_NPI_QUEUE_READ_LIMIT")
        else 2 * max_jobs
    )
    queue_name = 'arq:NPI'
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class NPI_finish:  # pylint: disable=invalid-name
    functions = [npi_shutdown]
    on_startup = db_startup
    max_jobs = (
        int(os.environ.get("HLTHPRT_MAX_NPI_FINISH_JOBS"))
        if os.environ.get("HLTHPRT_MAX_NPI_FINISH_JOBS")
        else 1
    )
    queue_read_limit = max_jobs
    job_timeout = 3600
    burst = True
    queue_name = 'arq:NPI_finish'
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class NUCC:
    functions = [process_nucc_data, control_single_job_start]
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
    max_tries = (
        int(os.environ.get('HLTHPRT_CLAIMS_FINALIZE_MAX_TRIES'))
        if os.environ.get('HLTHPRT_CLAIMS_FINALIZE_MAX_TRIES')
        else 720
    )
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
    max_tries = (
        int(os.environ.get('HLTHPRT_DRUG_CLAIMS_FINALIZE_MAX_TRIES'))
        if os.environ.get('HLTHPRT_DRUG_CLAIMS_FINALIZE_MAX_TRIES')
        else int(os.environ.get('HLTHPRT_CLAIMS_FINALIZE_MAX_TRIES', 720))
    )
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
    functions = [process_provider_enrichment_data, save_provider_enrichment_data, control_single_job_start]
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
    functions = [process_places_zcta_data, control_single_job_start]
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
    functions = [process_lodes_data, control_single_job_start]
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
    functions = [process_medicare_enrollment_data, control_single_job_start]
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
    functions = [process_cms_doctors_data, control_single_job_start]
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
    functions = [process_facility_anchors_data, control_single_job_start]
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
    functions = [process_pharmacy_economics_data, control_single_job_start]
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
    functions = [process_entity_address_unified_data, control_single_job_start]
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


class PTGAddress:
    functions = [process_ptg_address_data, control_single_job_start]
    on_startup = ptg_address_startup
    on_shutdown = ptg_address_shutdown
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:PTGAddress"
    job_timeout = 14400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class PTGAddress_finish:  # pylint: disable=invalid-name
    functions = [ptg_address_shutdown]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:PTGAddress_finish"
    job_timeout = 1800
    burst = True
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class AddressArchive:
    functions = [process_address_archive_migration_data, control_single_job_start]
    on_startup = db_startup
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:AddressArchive"
    job_timeout = 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class OpenAddresses:
    functions = [process_openaddresses_data, control_single_job_start]
    on_startup = openaddresses_startup
    on_shutdown = openaddresses_shutdown
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:OpenAddresses"
    job_timeout = int(os.environ.get("HLTHPRT_OPENADDRESSES_JOB_TIMEOUT")) if os.environ.get("HLTHPRT_OPENADDRESSES_JOB_TIMEOUT") else 86400
    redis_settings = build_redis_settings()
    job_serializer = serialize_job
    job_deserializer = deserialize_job


class MRFSourceDiscovery:
    functions = [process_mrf_source_discovery_data, control_single_job_start]
    on_startup = mrf_source_discovery_startup
    on_shutdown = mrf_source_discovery_shutdown
    max_jobs = 1
    queue_read_limit = 2
    queue_name = "arq:MRFSourceDiscovery"
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
    _run(initiate_mrf(test_mode=test))

@click.command(help="Finish CMSGOV MRF Import")
@click.option("--test", is_flag=True, help="Finalize the test-schema import.")
@click.option("--import-id", help="Override the import_id/import_date used during finalize.")
def mrf_end(test: bool, import_id: str | None):
    _run(finish_mrf(test_mode=test, import_id=import_id))


@click.command(help="Run Plan Attributes Import from CMS.gov")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def plan_attributes(test: bool):
    _run(initiate_plan_attributes(test_mode=test))

@click.command(help="Run NPPES Import with Weekly updates")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def npi(test: bool):
    _run(initiate_npi(test_mode=test))

@click.command(help="Run Transparency in Coverage (PTG) Import")
@click.option("--toc-url", multiple=True, help="URL of a table-of-contents file to seed jobs (repeatable).")
@click.option("--toc-list", type=click.Path(exists=True), help="Path to file containing TOC URLs (newline or JSON list).")
@click.option("--in-network-url", help="URL of a single in-network rates file.")
@click.option("--allowed-url", help="URL of a single allowed-amounts file.")
@click.option("--provider-ref-url", help="URL of a provider-reference file.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
@click.option("--source-key", help="Stable PTG2 payer/source key for independent current/previous publishing.")
@click.option("--import-month", help="Snapshot month for PTG2 imports (YYYY-MM-DD or YYYY-MM).")
@click.option("--max-files", type=int, help="Maximum discovered rate files to process.")
@click.option("--max-items", type=int, help="Maximum top-level rate items to parse per file.")
@click.option("--plan-id", multiple=True, help="Only process TOC structures containing this plan id (repeatable).")
@click.option(
    "--plan-name-contains",
    multiple=True,
    help="Only process TOC structures whose plan/sponsor/issuer text contains this value (repeatable).",
)
@click.option("--plan-market-type", multiple=True, help="Only process TOC structures with this market type.")
@click.option(
    "--file-url-contains",
    multiple=True,
    help="Only process discovered rate files whose URL or description contains this value (repeatable).",
)
@click.option(
    "--reuse-raw-artifacts/--no-reuse-raw-artifacts",
    default=True,
    help="Reuse retained PTG2 raw artifacts when server metadata or hashes prove they are unchanged.",
)
@click.option(
    "--keep-partial-artifacts/--cleanup-partial-artifacts",
    default=None,
    help="Keep partial PTG2 raw downloads on failure for resume/debug runs (env: HLTHPRT_PTG2_KEEP_PARTIAL_ARTIFACTS).",
)
@click.option(
    "--keep-artifacts-on-failure",
    is_flag=True,
    help="Alias for --keep-partial-artifacts to explicitly retain failed-download artifacts for reruns.",
)
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def ptg(
    toc_url: tuple[str, ...],
    toc_list: str | None,
    in_network_url: str | None,
    allowed_url: str | None,
    provider_ref_url: str | None,
    import_id: str | None,
    source_key: str | None,
    import_month: str | None = None,
    max_files: int | None = None,
    max_items: int | None = None,
    plan_id: tuple[str, ...] = (),
    plan_name_contains: tuple[str, ...] = (),
    plan_market_type: tuple[str, ...] = (),
    file_url_contains: tuple[str, ...] = (),
    reuse_raw_artifacts: bool = True,
    keep_partial_artifacts: bool | None = None,
    keep_artifacts_on_failure: bool = False,
    test: bool = False,
):
    if keep_artifacts_on_failure:
        keep_partial_artifacts = True
    _run(
        initiate_ptg(
            test_mode=test,
            toc_urls=list(toc_url),
            toc_list=toc_list,
            in_network_url=in_network_url,
            allowed_url=allowed_url,
            provider_ref_url=provider_ref_url,
            import_id=import_id,
            source_key=source_key,
            import_month=import_month,
            max_files=max_files,
            max_items=max_items,
            plan_ids=list(plan_id),
            plan_name_contains=list(plan_name_contains),
            plan_market_types=list(plan_market_type),
            file_url_contains=list(file_url_contains),
            reuse_raw_artifacts=reuse_raw_artifacts,
            keep_partial_artifacts=keep_partial_artifacts,
        )
    )


@click.command(help="Run NUCC Taxonomy Import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def nucc(test: bool):
    _run(initiate_nucc(test_mode=test))


@click.command(help="Run CMS official RC/POS code-set import")
@click.option("--test", is_flag=True, help="Import a small official-code sample for a quick smoke run.")
def code_sets(test: bool):
    _run(initiate_code_sets(test_mode=test))


@click.command(help="Run CMS MS-DRG reference import")
@click.option("--test", is_flag=True, help="Import a small MS-DRG sample for a quick smoke run.")
@click.option(
    "--include-relationships/--skip-relationships",
    default=True,
    help="Load MS-DRG to ICD-10-CM/PCS relationship indexes.",
)
@click.option("--relationship-page-limit", type=int, help="Limit ICD-10 index pages for controlled partial runs.")
@click.option("--concurrency", type=int, help="Maximum concurrent CMS manual index page downloads.")
@click.option("--source-url", help="Override CMS MS-DRG landing page URL.")
@click.option("--manual-toc-url", help="Override CMS MS-DRG definitions manual table-of-contents URL.")
@click.option("--import-id", help="Override staging suffix/import id.")
def ms_drg(
    test: bool,
    include_relationships: bool,
    relationship_page_limit: int | None,
    concurrency: int | None,
    source_url: str | None,
    manual_toc_url: str | None,
    import_id: str | None,
):
    _run(
        initiate_ms_drg(
            test_mode=test,
            include_relationships=include_relationships,
            relationship_page_limit=relationship_page_limit,
            concurrency=concurrency,
            source_url=source_url,
            manual_toc_url=manual_toc_url,
            import_id=import_id,
        )
    )


@click.command(help="Run clinical condition/treatment reference import")
@click.option("--test", is_flag=True, help="Import a small official-code sample for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
@click.option("--sources", help="Comma-separated sources: icd10cm,mesh,rxnorm,snomed,medrt.")
@click.option("--artifact-root", help="Directory for retained terminology source artifacts.")
@click.option("--force-download", is_flag=True, help="Redownload source artifacts even when retained files exist.")
def clinical_reference(test: bool, import_id: str | None, sources: str | None, artifact_root: str | None, force_download: bool):
    _run(
        initiate_clinical_reference(
            test_mode=test,
            import_id=import_id,
            sources=sources,
            artifact_root=artifact_root,
            force_download=force_download,
        )
    )


@click.command(help="Run CMS claims pricing import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def claims_pricing(test: bool, import_id: str | None):
    _run(initiate_claims_pricing(test_mode=test, import_id=import_id))


@click.command(help="Run CMS procedures claims import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def claims_procedures(test: bool, import_id: str | None):
    _run(initiate_claims_pricing(test_mode=test, import_id=import_id))


@click.command(help="Finish CMS claims pricing import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for staging tables.")
@click.option("--run-id", required=True, help="Run id emitted by `start claims-pricing`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Optional manifest path override.")
def claims_pricing_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    _run(
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
    _run(initiate_drug_claims(test_mode=test, import_id=import_id))


@click.command(help="Finish CMS Part D drug claims import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for staging tables.")
@click.option("--run-id", required=True, help="Run id emitted by `start drug-claims`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Optional manifest path override.")
def drug_claims_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    _run(
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
    _run(initiate_provider_quality(test_mode=test, import_id=import_id))


@click.command(help="Run provider enrichment import (PECOS + Medicare enrollment)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def provider_enrichment(test: bool):
    _run(initiate_provider_enrichment(test_mode=test))


@click.command(help="Finish provider quality import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for staging tables.")
@click.option("--run-id", required=True, help="Run id emitted by `start provider-quality`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Optional manifest path override.")
def provider_quality_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    _run(
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
    _run(initiate_partd_formulary_network(test_mode=test, import_id=import_id))


@click.command(help="Run state board pharmacy license import (NPI-first canonical)")
@click.option("--test", is_flag=True, help="Process a smaller synthetic subset for a quick smoke run.")
@click.option("--import-id", help="Override import id/date suffix for table names.")
def pharmacy_license(test: bool, import_id: str | None):
    _run(initiate_pharmacy_license(test_mode=test, import_id=import_id))


@click.command(help="Run CDC PLACES ZCTA import (latest year only)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def places_zcta(test: bool):
    _run(initiate_places_zcta(test_mode=test))


@click.command(help="Run LEHD/LODES workplace aggregate import")
@click.option("--test", is_flag=True, help="Process a small subset of states for a quick smoke run.")
def lodes(test: bool):
    _run(initiate_lodes(test_mode=test))


@click.command(help="Run CMS Medicare Enrollment Dashboard import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def medicare_enrollment(test: bool):
    _run(initiate_medicare_enrollment(test_mode=test))


@click.command(help="Run CMS Doctors and Clinicians import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def cms_doctors(test: bool):
    _run(initiate_cms_doctors(test_mode=test))


@click.command(help="Run Facility Anchors import (HRSA FQHCs + CMS Hospitals)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def facility_anchors(test: bool):
    _run(initiate_facility_anchors(test_mode=test))


@click.command(help="Run Pharmacy Economics import (SDUD + NADAC + FUL margins)")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def pharmacy_economics(test: bool):
    _run(initiate_pharmacy_economics(test_mode=test))


@click.command(help="Run unified entity-address materialization import")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def entity_address_unified(test: bool):
    _run(initiate_entity_address_unified(test_mode=test))


@click.command(help="Run fast PTG provider-location address projection")
@click.option("--test", is_flag=True, help="Process a small sample of data for a quick smoke run.")
def ptg_address(test: bool):
    _run(initiate_ptg_address(test_mode=test))


@click.command(help="Run OpenAddresses US geocode cache refresh and address archive backfill")
@click.option("--test", is_flag=True, help="Process a small OpenAddresses subset for a quick smoke run.")
@click.option("--backfill-only", is_flag=True, help="Backfill archive coordinates from the existing local OpenAddresses cache.")
@click.option("--load-only", is_flag=True, help="Load OpenAddresses data into the stage table without publish/backfill.")
@click.option("--publish-only", is_flag=True, help="Publish/backfill from an existing OpenAddresses stage table.")
@click.option("--resume-stage", is_flag=True, help="Reuse an existing stage table instead of dropping it before load.")
@click.option("--import-id", help="Override import id/date suffix for OpenAddresses stage tables.")
@click.option("--local-file", "local_files", multiple=True, help="Local GeoJSON/GeoJSON.gz source file; may be repeated.")
@click.option("--batch-size", type=int, help="Rows per OpenAddresses flush batch.")
@click.option("--source-concurrency", type=int, help="Number of OpenAddresses source files to load concurrently.")
@click.option("--max-files", type=int, help="Maximum OpenAddresses remote sources to process.")
@click.option("--start-index", type=int, help="One-based remote source index to start from.")
@click.option("--end-index", type=int, help="One-based remote source index to stop at.")
@click.option("--start-source", help="Remote source path to start from, such as us/ca/city.")
@click.option("--min-rows", type=int, help="Minimum staged rows required before publish in non-test mode.")
@click.option("--test-file-limit", type=int, help="Remote source file limit in test mode.")
@click.option("--test-row-limit", type=int, help="Rows per source file in test mode.")
@click.option("--backfill-state-code", help="State code for archive backfill sharding.")
@click.option("--backfill-zip-prefix", help="ZIP prefix for archive backfill sharding.")
def openaddresses(
    test: bool,
    backfill_only: bool,
    load_only: bool,
    publish_only: bool,
    resume_stage: bool,
    import_id: str | None,
    local_files: tuple[str, ...],
    batch_size: int | None,
    source_concurrency: int | None,
    max_files: int | None,
    start_index: int | None,
    end_index: int | None,
    start_source: str | None,
    min_rows: int | None,
    test_file_limit: int | None,
    test_row_limit: int | None,
    backfill_state_code: str | None,
    backfill_zip_prefix: str | None,
):
    _run(
        initiate_openaddresses(
            test_mode=test,
            backfill_only=backfill_only,
            load_only=load_only,
            publish_only=publish_only,
            resume_stage=resume_stage,
            import_id=import_id,
            local_files=local_files,
            batch_size=batch_size,
            source_concurrency=source_concurrency,
            max_files=max_files,
            start_index=start_index,
            end_index=end_index,
            start_source=start_source,
            min_rows=min_rows,
            test_file_limit=test_file_limit,
            test_row_limit=test_row_limit,
            backfill_state_code=backfill_state_code,
            backfill_zip_prefix=backfill_zip_prefix,
        )
    )


@click.command(help="Run one-time legacy address archive to canonical v2 migration")
@click.option("--dry-run", is_flag=True, help="Compute counts and verification queries without writing.")
@click.option("--legacy-table", default="address_archive", show_default=True, help="Legacy archive table name.")
@click.option("--archive-table", default="address_archive_v2", show_default=True, help="Canonical archive table name.")
@click.option("--work-mem", default="512MB", show_default=True, help="PostgreSQL work_mem for the migration transaction.")
@click.option("--timeout", default="30min", show_default=True, help="PostgreSQL statement_timeout for migration statements.")
@click.option("--sample-limit", type=int, default=20, show_default=True, help="Sample rows retained in migration metrics.")
@click.option("--enqueue", is_flag=True, help="Enqueue the migration on arq:AddressArchive instead of running inline.")
@click.option("--test", is_flag=True, help="Pass test mode through the controlled importer payload.")
def address_archive_v2_migrate(
    dry_run: bool,
    legacy_table: str,
    archive_table: str,
    work_mem: str,
    timeout: str,
    sample_limit: int,
    enqueue: bool,
    test: bool,
):
    _run(
        initiate_address_archive_migration(
            dry_run=dry_run,
            legacy_table=legacy_table,
            archive_table=archive_table,
            work_mem=work_mem,
            timeout=timeout,
            sample_limit=sample_limit,
            enqueue=enqueue,
            test_mode=test,
        )
    )


@click.command(help="Run lightweight MRF payer/source discovery catalog import")
@click.option("--provider", help="Provider list: all or master-list.")
@click.option("--limit", type=int, help="Maximum deduped source candidates to process.")
@click.option("--source-entity-types", help="Comma-separated payer entity types to seed/check/crawl, for example tpa.")
@click.option("--source-payer-query", help="Case-insensitive payer-name substring for seed/check/crawl.")
@click.option("--dry-run", is_flag=True, help="Collect and dedupe candidates without writing catalog tables.")
@click.option("--check-urls", is_flag=True, help="Run lightweight HEAD checks for discovered URLs.")
@click.option("--crawl", is_flag=True, help="Fetch and parse TOC/index metadata only; never download full rate bodies.")
@click.option("--probe-files", is_flag=True, help="Run HEAD probes for stored MRF body files and cache size/ETag/Last-Modified.")
@click.option("--file-probe-limit", type=int, help="Maximum stored MRF body-file URLs to probe.")
@click.option("--file-probe-types", help="Comma-separated file types to probe. Defaults to in-network,allowed-amounts.")
@click.option("--file-probe-entity-types", help="Comma-separated payer entity types to probe, for example tpa or network/tpa.")
@click.option("--file-probe-payer-query", help="Case-insensitive payer-name substring for file probes.")
@click.option("--sync-import-control", is_flag=True, help="Push discovered source seeds to the configured import-control service.")
@click.option("--max-toc-bytes", type=int, help="Maximum TOC/index response bytes to fetch during discovery.")
@click.option("--concurrency", type=int, default=None, help="Maximum concurrent URL checks/TOC fetches. Defaults to 10.")
@click.option("--crawl-target-limit", type=int, help="Maximum resolved TOC targets to crawl after platform expansion.")
@click.option("--test", is_flag=True, help="Use the local curated master list sample and avoid external network checks.")
def mrf_source_discovery_command(
    provider: str | None,
    limit: int | None,
    source_entity_types: str | None,
    source_payer_query: str | None,
    dry_run: bool,
    check_urls: bool,
    crawl: bool,
    probe_files: bool,
    file_probe_limit: int | None,
    file_probe_types: str | None,
    file_probe_entity_types: str | None,
    file_probe_payer_query: str | None,
    sync_import_control: bool,
    max_toc_bytes: int | None,
    concurrency: int | None,
    crawl_target_limit: int | None,
    test: bool,
):
    _run(
        initiate_mrf_source_discovery(
            test_mode=test,
            provider=provider,
            limit=limit,
            source_entity_types=source_entity_types,
            source_payer_query=source_payer_query,
            dry_run=dry_run,
            check_urls=check_urls,
            crawl=crawl,
            probe_files=probe_files,
            file_probe_limit=file_probe_limit or None,
            file_probe_types=file_probe_types,
            file_probe_entity_types=file_probe_entity_types,
            file_probe_payer_query=file_probe_payer_query,
            sync_import_control=sync_import_control,
            max_toc_bytes=max_toc_bytes or None,
            concurrency=concurrency or None,
            crawl_target_limit=crawl_target_limit or None,
        )
    )


@click.command(help="Finish CMS Part D formulary + pharmacy network import for a queued run id")
@click.option("--import-id", required=True, help="Import id/date suffix used for the run.")
@click.option("--run-id", required=True, help="Run id emitted by `start partd-formulary-network`.")
@click.option("--test", is_flag=True, help="Use test DB suffix when finalizing.")
@click.option("--manifest-path", help="Unused; kept for command signature consistency.")
def partd_formulary_network_end(import_id: str, run_id: str, test: bool, manifest_path: str | None):
    _run(
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
    _run(
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
process_group.add_command(mrf_source_discovery_command, name="mrf-source-discovery")
process_group.add_command(cms_doctors, name="cms-doctors")
process_group.add_command(facility_anchors, name="facility-anchors")
process_group.add_command(pharmacy_economics, name="pharmacy-economics")
process_group.add_command(entity_address_unified, name="entity-address-unified")
process_group.add_command(ptg_address, name="ptg-address")
process_group.add_command(openaddresses, name="openaddresses")
process_group.add_command(address_archive_v2_migrate, name="address-archive-v2-migrate")
process_group.add_command(geo_lookup, name="geo")
process_group.add_command(geo_census_lookup, name="geo-census")
process_group.add_command(nucc)
process_group.add_command(code_sets, name="code-sets")
process_group.add_command(ms_drg, name="ms-drg")
process_group.add_command(clinical_reference, name="clinical-reference")
