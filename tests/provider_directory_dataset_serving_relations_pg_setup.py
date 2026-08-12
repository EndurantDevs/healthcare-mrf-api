# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable schema setup for dataset-serving relation tests."""

from db.connection import Database
from tests import provider_directory_subset_completion_pg_setup as subset_setup


async def create_serving_relation_tables(database: Database, schema: str) -> None:
    """Create the current dataset and serving-relation fixture tables."""

    await _create_dataset_tables(database, schema)
    await _create_relation_tables(database, schema)


async def _create_dataset_tables(database: Database, schema: str) -> None:
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_source ("
        "source_id varchar(64) PRIMARY KEY, org_name varchar(256) NOT NULL, "
        "endpoint_id varchar(64), canonical_api_base text, metadata_json jsonb);"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_endpoint_dataset ("
        "dataset_id varchar(96) PRIMARY KEY, endpoint_id varchar(64) NOT NULL, "
        "acquisition_root_run_id varchar(64), import_run_id varchar(64) NOT NULL, "
        "previous_dataset_id varchar(96), dataset_hash varchar(64), "
        "resource_count bigint, status varchar(32) NOT NULL DEFAULT 'published', "
        "is_current boolean NOT NULL DEFAULT true, superseded_at timestamptz, "
        "publication_metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb, "
        "publication_metadata_summary_json jsonb, publication_metadata_sha256 varchar(64), "
        "content_proof_admission_version smallint, content_proof_admission_kind varchar(32), "
        "content_proof_admission_sha256 varchar(64), content_proof_resource_types varchar(64)[], "
        "completion_proof_required_version integer, completion_proof_json jsonb, "
        "completion_proof_sha256 varchar(64));"
    )
    await subset_setup.install_subset_canonical_functions(database, schema)
    await database.status(
        f"""
        CREATE FUNCTION {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
            metadata_summary jsonb, admission_version smallint, admission_kind text,
            proof_sha256 text, resource_types varchar[]
        ) RETURNS varchar LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $function$
            SELECT {schema}.provider_directory_subset_payload_sha256(
                jsonb_build_object(
                    'contract', 'provider-directory-admission-seal-v1',
                    'metadata_summary', metadata_summary,
                    'admission_version', admission_version,
                    'admission_kind', admission_kind,
                    'proof_sha256', proof_sha256,
                    'resource_types', to_jsonb(resource_types)
                )
            )::varchar
        $function$;
        """
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
        "dataset_id varchar(96) NOT NULL, resource_type varchar(64) NOT NULL, "
        "resource_id varchar(256) NOT NULL, payload_hash varchar(64) NOT NULL, "
        "payload_json jsonb NOT NULL, acquired_resource_sha256 varchar(64), "
        "PRIMARY KEY (dataset_id, resource_type, resource_id));"
    )


async def _create_relation_tables(database: Database, schema: str) -> None:
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_insurance_plan ("
        "dataset_id varchar(96) NOT NULL, resource_id varchar(256) NOT NULL, "
        "payload_hash varchar(64) NOT NULL, payload_json jsonb NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_id));"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_network_plan ("
        "dataset_id varchar(96) NOT NULL, network_resource_id varchar(256) NOT NULL, "
        "insurance_plan_resource_id varchar(256) NOT NULL, "
        "PRIMARY KEY (dataset_id, network_resource_id, insurance_plan_resource_id));"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_affiliation_organization ("
        "dataset_id varchar(96) NOT NULL, "
        "participating_organization_resource_id varchar(256) NOT NULL, "
        "affiliation_resource_id varchar(256) NOT NULL, "
        "PRIMARY KEY (dataset_id, participating_organization_resource_id, "
        "affiliation_resource_id));"
    )
