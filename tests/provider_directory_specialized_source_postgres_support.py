# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current specialized publications using genuine Source custody and cohort issuance."""

import importlib
import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

from process.provider_directory_generic_source_policy import source_transaction
from process.uhc_flex_official_cohort_snapshot import prepare_cohort
from tests import provider_directory_uhc_flex_npi_cohort_pg_support as official
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.nppes_candidate_postgres_support import candidate_component, nppes_candidate_schema

directory = importlib.import_module("process.provider_directory_fhir")
BASE = "https://official.example.test/directory"


def _resources(dataset_id):
    payloads = [
        ("Practitioner", f"practitioner-{ordinal}", official._practitioner_payload(npi))
        for ordinal, npi in enumerate(official.PRACTITIONER_NPIS, 1)
    ]
    payloads.extend(
        (kind, f"{kind.lower()}-1", {}) for kind in directory.UHC_SUPPORTED_RESOURCES if kind != "Practitioner"
    )
    return [
        dict(
            dataset_id=dataset_id,
            resource_type=kind,
            resource_id=identity,
            payload_json=payload,
            payload_hash=directory.resource_payload_sha256_for_contract(
                payload, directory.LEGACY_RESOURCE_HASH_CONTRACT, resource_type=kind
            ),
        )
        for kind, identity, payload in payloads
    ]


def _official_metadata(resources, dataset_id, root_run_id):
    from process.uhc_retained_dataset import _summary_input_hash
    from tests.test_provider_directory_uhc_flex_practitioner_twin_postgres import _bound_official_content_proof
    from tests.uhc_final_publication_test_support import _build_summary_input, _materialization_identity

    summary = _build_summary_input("7" * 64)
    summary["count_by_field"].update(
        raw_provider_records=len(official.PRACTITIONER_NPIS) + 1,
        raw_individual_records=len(official.PRACTITIONER_NPIS),
        accepting_null_records=len(official.PRACTITIONER_NPIS) + 1,
        distinct_npis=len(official.MEMBER_NPIS),
    )
    summary["input_sha256"] = _summary_input_hash(summary)
    proof = _bound_official_content_proof(
        resource_specs=[
            (resource_row["resource_type"], resource_row["resource_id"], resource_row["payload_hash"])
            for resource_row in resources
        ],
        dataset_id=dataset_id,
        root_run_id=root_run_id,
        materialization_identity=_materialization_identity(summary),
    )
    return {
        directory.UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: proof,
        directory.UHC_RETAINED_SUMMARY_INPUT_METADATA_KEY: summary,
        directory.UHC_RETAINED_PUBLICATION_METADATA_KEY: directory._expected_uhc_publication_identity(
            summary, dataset_id=dataset_id, acquisition_root_run_id=root_run_id
        ),
    }


async def publish_official_source(
    database, *, dataset_id=official.DATASET_ID, root_run_id=official.ACQUISITION_ROOT_RUN_ID, previous_dataset_id=None
):
    """Use actual candidate COPY, native proof validation, attachment and publication."""
    resources = _resources(dataset_id)
    metadata = _official_metadata(resources, dataset_id, root_run_id)
    proof = metadata[directory.UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY]
    candidate = directory.EndpointDatasetCandidate(
        endpoint_id=official.ENDPOINT_ID,
        dataset_id=dataset_id,
        acquisition_root_run_id=root_run_id,
        source_ids=(official.UHC_PROVIDER_FILE_SOURCE_ID,),
        selected_resources=tuple(sorted(directory.UHC_SUPPORTED_RESOURCES)),
        expected_resources=tuple(sorted(directory.UHC_SUPPORTED_RESOURCES)),
        import_run_id=root_run_id,
        previous_dataset_id=previous_dataset_id,
        resource_hash_contract=directory.LEGACY_RESOURCE_HASH_CONTRACT,
    )
    async with source_transaction(database), database.acquire() as connection:
        await directory._ensure_endpoint_dataset_candidate(candidate)
        for kind in candidate.selected_resources:
            await directory._upsert_dataset_resource_rows_on_connection(
                connection,
                [resource_row for resource_row in resources if resource_row["resource_type"] == kind],
                persist_content_proof=True,
                resource_hash_contract=candidate.resource_hash_contract,
            )
        await database.status(
            "UPDATE mrf.provider_directory_endpoint_dataset SET publication_metadata_json = "
            "publication_metadata_json || CAST(:metadata AS jsonb) WHERE dataset_id=:dataset",
            metadata=json.dumps(metadata),
            dataset=candidate.dataset_id,
        )
    validated = await directory._validate_endpoint_dataset_candidate(
        candidate,
        {
            kind: dict(
                rows_fetched=sum(resource_row["resource_type"] == kind for resource_row in resources), complete=True
            )
            for kind in candidate.selected_resources
        },
    )
    assert validated["validated"] and not validated["published"]
    assert proof["dataset_hash"] == validated["dataset_hash"]
    await _publish_official_header(database, candidate, proof)
    return (await prepare_cohort(database)).cohort


async def _publish_official_header(database, candidate, proof):
    """Supersede and publish through the ordinary cutover statements atomically."""
    publication = directory.ProviderDirectoryArtifactDataset(
        official.UHC_PROVIDER_FILE_SOURCE_ID,
        candidate.endpoint_id,
        candidate.dataset_id,
        candidate.acquisition_root_run_id,
        dataset_hash=proof["dataset_hash"],
        selected_resources=candidate.selected_resources,
        retained_resources=candidate.selected_resources,
        expected_incumbent_dataset_id=candidate.previous_dataset_id,
    )
    async with source_transaction(database):
        await directory._supersede_artifact_dataset_incumbent(publication)
        await directory._publish_validated_artifact_dataset(publication)


@asynccontextmanager
async def specialized_source_scope(monkeypatch):
    """Reuse the full native callable/namespace/acquisition fixture and its cleanup."""
    with monkeypatch.context() as settings:
        source_dsn = os.environ.get("HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN")
        if "HLTHPRT_PUBLIC_EVIDENCE_STORAGE_POSTGRES_DSN" not in os.environ and source_dsn:
            settings.setenv("HLTHPRT_PUBLIC_EVIDENCE_STORAGE_POSTGRES_DSN", source_dsn)
        async with nppes_candidate_schema() as (engine, url, schema, _):
            component = candidate_component(schema)
            database = component.database
            settings.setattr(directory, "db", database)
            settings.setenv(
                "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN",
                url.set(drivername="postgresql").render_as_string(hide_password=False),
            )
            for name, setting_value in dict(
                HOST=url.host,
                PORT=url.port,
                DATABASE=url.database,
                USER=component.publisher,
                READER_USER=component.reader,
                PASSWORD=url.password or "",
                READER_PASSWORD=url.password or "",
            ).items():
                settings.setenv("HLTHPRT_DB_" + name, str(setting_value))
            settings.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
            await database._connect_reader()
            async with source_transaction(database):
                await database.status(
                    "INSERT INTO mrf.provider_directory_api_endpoint "
                    "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
                    "VALUES (:endpoint,:base,repeat('c',64),repeat('d',64))",
                    endpoint=official.ENDPOINT_ID,
                    base=BASE,
                )
                await database.status(
                    "INSERT INTO mrf.provider_directory_source "
                    "(source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) "
                    "VALUES (:source,'Official fixture',:endpoint,:base,false,false)",
                    source=official.UHC_PROVIDER_FILE_SOURCE_ID,
                    endpoint=official.ENDPOINT_ID,
                    base=BASE,
                )
            cohort = await publish_official_source(database)
            settings.setattr(official, "DATASET_HASH", cohort.official_dataset_hash)
            settings.setattr(official, "CONTENT_PROOF_SHA256", cohort.official_content_proof_sha256)
            connection = await connect(url)
            try:
                yield SimpleNamespace(
                    database=database,
                    connection=connection,
                    engine=engine,
                    url=url,
                    schema='"mrf"',
                    schema_name=schema,
                    component=component,
                    cohort=cohort,
                )
            finally:
                await connection.close()
