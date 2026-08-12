# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared candidate writer for dataset-artifact PostgreSQL tests."""

import importlib
import json

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")


async def insert_validated_shared_dataset(
    database: Database,
    schema: str,
    *,
    dataset_id: str = "dataset_candidate",
    root_run_id: str = "root-candidate",
    seal: bool = True,
) -> None:
    """Insert one validated candidate and its single resource."""

    metadata = {
        "acquisition_root_run_id": root_run_id,
        "selected_resources": ["Location"],
        "expected_resources": ["Location"],
        "source_ids": ["source_primary", "source_sibling"],
        "resource_diagnostics": {
            "Location": {
                "complete": True,
                "bounded": False,
                "error": None,
                "next_url_remaining": False,
            }
        },
    }
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "previous_dataset_id, dataset_hash, status, is_current, resource_count, "
        "validated_at, publication_metadata_json) VALUES ("
        ":dataset_id, 'endpoint_shared', 'run-candidate', :root_run_id, "
        "'dataset_shared', :dataset_hash, :validated_status, false, 1, now(), "
        "CAST(:metadata AS json));",
        dataset_id=dataset_id,
        root_run_id=root_run_id,
        dataset_hash="e" * 64,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        metadata=json.dumps(metadata),
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json) "
        "VALUES (:dataset_id, 'Location', 'location-candidate', :payload_hash, "
        "CAST(:payload_json AS json));",
        dataset_id=dataset_id,
        payload_hash="f" * 64,
        payload_json=json.dumps({
            "status": "active",
            "name": "Candidate Clinic",
            "first_line": "2 Scope Way",
            "city_name": "Austin",
            "state_code": "TX",
            "postal_code": "78702",
        }),
    )
    if seal:
        await seal_validated_dataset(database, schema, dataset_id)


async def seal_validated_dataset(
    database: Database,
    schema: str,
    dataset_id: str,
    *,
    resource_types: tuple[str, ...] = ("Location",),
) -> None:
    """Install one shape-valid test receipt for cutover-only fixtures."""

    await database.status(
        f"""
        UPDATE {schema}.provider_directory_endpoint_dataset
           SET publication_metadata_summary_json = publication_metadata_json,
               content_proof_admission_version = {importer.ADMISSION_SEAL_VERSION},
               content_proof_admission_kind = '{importer.ADMISSION_KIND_GENERIC}',
               content_proof_admission_sha256 = repeat('a', 64),
               content_proof_resource_types = CAST(:resource_types AS varchar[]),
               publication_metadata_sha256 =
                   {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_json,
                       {importer.ADMISSION_SEAL_VERSION}::smallint,
                       '{importer.ADMISSION_KIND_GENERIC}'::text,
                       repeat('a', 64),
                       CAST(:resource_types AS varchar[])
                   )
         WHERE dataset_id = :dataset_id;
        """,
        dataset_id=dataset_id,
        resource_types=list(resource_types),
    )
