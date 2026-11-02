# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original proof-free synthetic publication, seeded before admission-era migrations."""

import json

from sqlalchemy import text

from process.network_fhir_source_witness_contract import OBSERVED_LEGACY_PROFILE
from tests import generic_fhir_capture_postgres_support as legacy
from tests import nppes_candidate_postgres_support as nppes

SOURCE = "legacy-source"
BASE = "https://legacy.example.test/fhir"


def legacy_resources():
    resources = legacy._contents(SOURCE)
    role = next(normalized_payload for kind, _, normalized_payload in resources if kind == "PractitionerRole")
    resources = [resource_row for resource_row in resources if resource_row[0] != "PractitionerRole"]
    resources.extend(
        [
            ("PractitionerRole", "role-office", {**role, "resource_id": "role-office", "insurance_plan_refs": []}),
            (
                "Practitioner",
                "provider-only",
                {"source_id": SOURCE, "resource_id": "provider-only", "npi": "1000000004"},
            ),
            (
                "PractitionerRole",
                "role-provider-only",
                {
                    **role,
                    "resource_id": "role-provider-only",
                    "practitioner_ref": "Practitioner/provider-only",
                    "npi": "1000000004",
                    "location_refs": [],
                    "insurance_plan_refs": [],
                },
            ),
            (
                "InsurancePlan",
                "plan-unresolved",
                {
                    "source_id": SOURCE,
                    "resource_id": "plan-unresolved",
                    "network_refs": ["Organization/absent", "Organization/absent"],
                },
            ),
        ]
    )
    return resources


def _seed_original_publication(connection):
    resource_records, root, counts, hashes = legacy._proof_rows(legacy_resources())
    summary = legacy._fixture_summary(root, counts, hashes, False, legacy_resources(), source_id=SOURCE)
    metadata_by_field = {"source_ids": [SOURCE], "source_summary_v1": summary}
    connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_api_endpoint "
            "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
            "VALUES ('endpoint-one',:base,repeat('a',64),repeat('b',64))"
        ),
        {"base": BASE},
    )
    connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_source "
            "(source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) "
            "VALUES (:source,'Synthetic legacy','endpoint-one',:base,false,false)"
        ),
        {"source": SOURCE, "base": BASE},
    )
    connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_endpoint_dataset "
            "(dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,"
            "resource_count,published_at,publication_metadata_json) VALUES "
            "('dataset-one','endpoint-one','run-one',:root,'published',true,:count,now(),CAST(:metadata AS jsonb))"
        ),
        {"root": root, "count": len(resource_records), "metadata": json.dumps(metadata_by_field)},
    )
    connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_dataset_resource "
            "(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
            "VALUES (:dataset,:kind,:identity,:digest,CAST(:normalized_payload AS jsonb))"
        ),
        [
            dict(zip(("dataset", "kind", "identity", "digest", "normalized_payload"), resource_row[:5]))
            for resource_row in resource_records
        ],
    )
    connection.commit()


def install_original_legacy_seed(monkeypatch):
    """Run every original migration, inserting only before published-header guards existed."""
    original = nppes._run_migrations
    seed_calls = []

    def migrate(connection, prefixes):
        boundary = prefixes.index("20260729110000")
        original(connection, prefixes[:boundary])
        assert not connection.scalar(
            text(
                "SELECT EXISTS (SELECT FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN "
                "('mrf.provider_directory_endpoint_dataset'::regclass,'mrf.provider_directory_dataset_resource'::regclass))"
            )
        )
        _seed_original_publication(connection)
        seed_calls.append(True)
        original(connection, prefixes[boundary:])

    monkeypatch.setattr(nppes, "_run_migrations", migrate)
    return seed_calls


async def reviewed_legacy_origin(runtime, profile, catalog, producer):
    """Compare the Source constructor to independent original header and synthetic root observations."""
    resource_records, root, counts, hashes = legacy._proof_rows(legacy_resources())
    summary = legacy._fixture_summary(root, counts, hashes, False, legacy_resources(), source_id=SOURCE)
    async with catalog._catalog_source_export_session(runtime, source_profile=profile) as scope:
        assert await scope.connection.fetchval("SELECT session_user::text") == runtime.config.publisher_role
        native, _ = await catalog._source_catalog_physical(scope.connection, profile, dataset_id="dataset-one")
        header = await scope.connection.fetchrow(
            "SELECT *,publication_metadata_json::text AS original_metadata_text,"
            "octet_length(publication_metadata_json::text)::bigint AS metadata_bytes,"
            "encode(sha256(convert_to(publication_metadata_json::text,'UTF8')),'hex') AS metadata_sha256 "
            f'FROM "{profile.schema_name}".provider_directory_endpoint_dataset WHERE dataset_id=$1',
            "dataset-one",
        )
        assert header["dataset_hash"] == root and header["resource_count"] == len(resource_records)
        assert json.loads(header["original_metadata_text"]) == {"source_ids": [SOURCE], "source_summary_v1": summary}
        for field in (
            "publication_metadata_summary_json",
            "publication_metadata_sha256",
            "content_proof_admission_version",
            "content_proof_admission_kind",
            "content_proof_admission_sha256",
            "content_proof_resource_types",
            "candidate_storage_json",
            "completion_proof_required_version",
            "completion_proof_json",
            "completion_proof_sha256",
        ):
            assert header[field] is None
        identity = scope.database_identity_by_field
        return producer.FHIRSourceMembershipInput(
            schema_name=profile.schema_name,
            source_id=SOURCE,
            endpoint_id="endpoint-one",
            dataset_id="dataset-one",
            dataset_sha256=root,
            root_run_id="run-one",
            header_oid=native["header_table_oid"],
            resource_parent_oid=native["resource_table_oid"],
            resource_leaf_oid=native["resource_leaf_oids"][0],
            metadata_bytes=header["metadata_bytes"],
            metadata_text_sha256=header["metadata_sha256"],
            summary_sha256=summary["summary_sha256"],
            proof_sha256=None,
            resource_counts=tuple(sorted(counts.items())),
            resource_hashes=tuple(sorted(hashes.items())),
            descriptor_counts=None,
            admission_profile=OBSERVED_LEGACY_PROFILE,
            admission_metadata_sha256=None,
            semantic_projection_as_of=None,
            unresolved_plan_network_count=1,
            source_uri=BASE,
            source_aliases=(SOURCE,),
            native_catalog_sha256=native["native_catalog_sha256"],
            database_oid=identity["database_oid"],
            database_system_identifier=identity["system_identifier"],
        )
