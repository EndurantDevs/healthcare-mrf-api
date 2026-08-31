# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic publication store for admitted rooted combined datasets."""

from __future__ import annotations

from datetime import date
import re
from typing import Any

from db.connection import db
from process.provider_directory_dataset_scoped_publication import (
    exact_current_matches_root,
    exact_uhc_dataset_pair,
    EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    lock_exact_current_dataset,
    ProviderDirectoryDatasetScopedPublicationError,
    supersede_exact_current_dataset,
)
from process.provider_directory_fhir import (
    build_provider_directory_dataset_serving_relations,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.provider_directory_rooted_graph_publication import (
    build_provider_directory_rooted_graph_dataset_identity,
    canonical_json,
    provider_directory_rooted_graph_publication_metadata,
    ProviderDirectoryRootedGraphDatasetIdentity,
    ProviderDirectoryRootedGraphPublicationError,
    ProviderDirectoryRootedGraphPublicationResult,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND,
)
from process.provider_directory_rooted_graph_publication_materialization import (
    materialize_provider_directory_rooted_graph_dataset,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS,
)
from process.provider_directory_rooted_graph_publication_readiness_store import (
    load_dataset_readiness,
    load_replay_readiness,
)
from process.provider_directory_rooted_graph_publication_store_support import (
    publication_row_fields as _row_fields,
    publication_table as _table,
)
from process.provider_directory_rooted_graph_twin_contract import (
    ProviderDirectoryRootedGraphTwinAdmission,
)
from process.provider_directory_rooted_graph_twin_store import (
    require_provider_directory_rooted_graph_admission,
)


_HEADER = "provider_directory_rooted_graph_dataset"
_PARENT = "provider_directory_endpoint_dataset"


async def _preflight_counts(
    database: Any,
    admission: ProviderDirectoryRootedGraphTwinAdmission,
) -> dict[str, int]:
    conflict = await database.scalar(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_table('provider_directory_rooted_graph_resource')} AS raw
              JOIN {_table('provider_directory_rooted_graph_work')} AS work
                ON work.acquisition_id = raw.acquisition_id
               AND work.query_id = raw.query_id
               AND work.attempt_count = raw.attempt
             WHERE raw.acquisition_id = :acquisition_id
               AND work.status = 'completed'
               AND raw.closure_scope IN ('root','plan')
               AND raw.resource_type <> 'Practitioner'
             GROUP BY raw.resource_type, raw.resource_id
            HAVING count(DISTINCT raw.payload_sha256) <> 1
        );
        """,
        acquisition_id=admission.publication_acquisition_id,
    )
    if conflict is not False:
        raise ProviderDirectoryRootedGraphPublicationError("content")
    count_rows = await database.all(
        f"""
        SELECT raw.resource_type, count(DISTINCT raw.resource_id)::bigint AS count
          FROM {_table('provider_directory_rooted_graph_resource')} AS raw
          JOIN {_table('provider_directory_rooted_graph_work')} AS work
            ON work.acquisition_id = raw.acquisition_id
           AND work.query_id = raw.query_id
           AND work.attempt_count = raw.attempt
         WHERE raw.acquisition_id = :acquisition_id
           AND work.status = 'completed'
           AND raw.closure_scope IN ('root','plan')
           AND raw.resource_type <> 'Practitioner'
         GROUP BY raw.resource_type;
        """,
        acquisition_id=admission.publication_acquisition_id,
    )
    count_by_resource_type = {
        resource_type: 0
        for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
    }
    count_by_resource_type["Practitioner"] = admission.root_resource_count
    for count_row in count_rows:
        fields = _row_fields(count_row)
        resource_type = fields.get("resource_type")
        count = fields.get("count")
        if (
            resource_type not in count_by_resource_type
            or resource_type == "Practitioner"
        ):
            raise ProviderDirectoryRootedGraphPublicationError("content")
        if type(count) is not int or count < 0:
            raise ProviderDirectoryRootedGraphPublicationError("content")
        count_by_resource_type[resource_type] = count
    return count_by_resource_type


async def _assert_no_orphan_parent(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
) -> None:
    count = await database.scalar(
        f"SELECT count(*) FROM {_table(_PARENT)} WHERE dataset_id = :dataset_id;",
        dataset_id=identity.dataset_id,
    )
    if count != 0:
        raise ProviderDirectoryRootedGraphPublicationError("source_drift")


def _identity_header_by_field(
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
) -> dict[str, object]:
    field_names = (
        "dataset_id",
        "publication_contract_id",
        "source_id",
        "endpoint_id",
        "source_authority_id",
        "root_dataset_variant",
        "root_publication_contract_id",
        "root_source_id",
        "root_endpoint_id",
        "practitioner_origin_source_id",
        "practitioner_origin_endpoint_id",
        "acquisition_root_run_id",
        "operation_key",
        "root_dataset_id",
        "root_dataset_hash",
        "root_content_proof_sha256",
        "root_cohort_id",
        "root_practitioner_resource_count",
        "cohort_complete",
    )
    identity_by_field = {
        field_name: getattr(identity, field_name) for field_name in field_names
    }
    identity_by_field["publication_kind"] = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_KIND
    )
    identity_by_field["semantic_projection_as_of"] = date.fromisoformat(
        identity.semantic_projection_as_of
    )
    identity_by_field["previous_dataset_id"] = identity.root_dataset_id
    return identity_by_field


def _admission_header_by_field(
    admission: ProviderDirectoryRootedGraphTwinAdmission,
) -> dict[str, object]:
    field_names = (
        "admission_id",
        "attempt_id",
        "publication_acquisition_id",
        "comparison_acquisition_id",
        "publication_run_id",
        "acquisition_source_id",
        "acquisition_endpoint_id",
        "endpoint_signature_sha256",
        "scope_id",
        "dataset_intent_id",
        "connector_id",
        "storage_contract_id",
        "graph_contract_sha256",
        "query_contract_sha256",
        "max_work_items",
        "max_resource_rows",
        "max_edge_rows",
        "max_payload_bytes",
        "used_work_items",
        "used_resource_rows",
        "used_edge_rows",
        "used_payload_bytes",
        "completed_count",
        "insurance_plan_page_count",
        "terminal_set_sha256",
        "resource_set_sha256",
        "edge_set_sha256",
        "rooted_graph_sha256",
    )
    admission_by_field = {
        field_name: getattr(admission, field_name) for field_name in field_names
    }
    admission_by_field.update(
        graph_resource_count=admission.resource_count,
        graph_edge_count=admission.edge_count,
        census_insurance_plan_count=admission.insurance_plan_count,
    )
    return admission_by_field


def _count_header_by_field(count_by_resource_type: dict[str, int]) -> dict[str, int]:
    return {
        "resource_count": sum(count_by_resource_type.values()),
        "practitioner_resource_count": count_by_resource_type["Practitioner"],
        "practitioner_role_resource_count": count_by_resource_type["PractitionerRole"],
        "organization_affiliation_resource_count": count_by_resource_type[
            "OrganizationAffiliation"
        ],
        "organization_resource_count": count_by_resource_type["Organization"],
        "location_resource_count": count_by_resource_type["Location"],
        "healthcare_service_resource_count": count_by_resource_type[
            "HealthcareService"
        ],
        "insurance_plan_resource_count": count_by_resource_type["InsurancePlan"],
        "endpoint_resource_count": count_by_resource_type["Endpoint"],
    }


async def _insert_parent_header(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    admission: ProviderDirectoryRootedGraphTwinAdmission,
    count_by_resource_type: dict[str, int],
) -> int:
    metadata = canonical_json(
        provider_directory_rooted_graph_publication_metadata(
            identity,
            admission,
            previous_dataset_id=identity.root_dataset_id,
            resource_counts=count_by_resource_type,
        )
    )
    return await database.status(
        f"""
        INSERT INTO {_table(_PARENT)} (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, dataset_hash, status, is_current,
            resource_count, created_at, validated_at, published_at,
            superseded_at, publication_metadata_json,
            completion_proof_required_version, completion_proof_json,
            completion_proof_sha256
        ) VALUES (
            :dataset_id, :endpoint_id, :publication_run_id,
            :acquisition_root_run_id, :previous_dataset_id, NULL,
            'building', false, :resource_count, transaction_timestamp(),
            NULL, NULL, NULL, CAST(:metadata AS jsonb), NULL, NULL, NULL
        );
        """,
        dataset_id=identity.dataset_id,
        endpoint_id=identity.endpoint_id,
        publication_run_id=admission.publication_run_id,
        acquisition_root_run_id=identity.acquisition_root_run_id,
        previous_dataset_id=identity.root_dataset_id,
        resource_count=sum(count_by_resource_type.values()),
        metadata=metadata,
    )


async def _insert_rooted_header(
    database: Any,
    header_by_field: dict[str, object],
) -> int:
    columns = tuple(header_by_field)
    return await database.status(
        f"""
        INSERT INTO {_table(_HEADER)} ({', '.join(columns)},
            dataset_hash, resource_hash_contract,
            rooted_graph_complete, endpoint_collection_complete,
            endpoint_complete, status, is_current, created_at,
            validated_at, published_at, superseded_at
        ) VALUES ({', '.join(':' + column for column in columns)},
            NULL, :resource_hash_contract, true, false, false,
            'building', false, transaction_timestamp(), NULL, NULL, NULL
        );
        """,
        **header_by_field,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )


async def _insert_headers(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    admission: ProviderDirectoryRootedGraphTwinAdmission,
    count_by_resource_type: dict[str, int],
) -> None:
    parent_inserted = await _insert_parent_header(
        database,
        identity,
        admission,
        count_by_resource_type,
    )
    header_by_field = {
        **_identity_header_by_field(identity),
        **_admission_header_by_field(admission),
        **_count_header_by_field(count_by_resource_type),
    }
    header_inserted = await _insert_rooted_header(database, header_by_field)
    if parent_inserted != 1 or header_inserted != 1:
        raise ProviderDirectoryRootedGraphPublicationError("state")


async def _dataset_hash(database: Any, dataset_id: str) -> str:
    value = await database.scalar(
        f"""
        SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                   COALESCE(pg_catalog.string_agg(
                       '["' || resource_type || '","' || resource_id ||
                       '","' || payload_hash || '"]', E'\\n'
                       ORDER BY resource_type, resource_id), ''), 'UTF8')), 'hex')
          FROM {_table('provider_directory_dataset_resource')}
         WHERE dataset_id = :dataset_id;
        """,
        dataset_id=dataset_id,
    )
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ProviderDirectoryRootedGraphPublicationError("content")
    return value


async def _validate_and_publish(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    previous: Any,
) -> None:
    dataset_hash = await _dataset_hash(database, identity.dataset_id)
    parent_validated = await database.status(
        f"""
        UPDATE {_table(_PARENT)} SET status = 'validated',
               dataset_hash = :dataset_hash,
               validated_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'building'
           AND is_current IS FALSE AND dataset_hash IS NULL;
        """,
        dataset_id=identity.dataset_id,
        dataset_hash=dataset_hash,
    )
    header_validated = await database.status(
        f"""
        UPDATE {_table(_HEADER)} SET status = 'validated',
               dataset_hash = :dataset_hash,
               validated_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'building'
           AND is_current IS FALSE AND dataset_hash IS NULL;
        """,
        dataset_id=identity.dataset_id,
        dataset_hash=dataset_hash,
    )
    if parent_validated != 1 or header_validated != 1:
        raise ProviderDirectoryRootedGraphPublicationError("state")
    await supersede_exact_current_dataset(database, previous)
    parent_published = await database.status(
        f"""
        UPDATE {_table(_PARENT)} SET status = 'published', is_current = true,
               published_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'validated'
           AND is_current IS FALSE;
        """,
        dataset_id=identity.dataset_id,
    )
    header_published = await database.status(
        f"""
        UPDATE {_table(_HEADER)} SET status = 'published', is_current = true,
               published_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'validated'
           AND is_current IS FALSE;
        """,
        dataset_id=identity.dataset_id,
    )
    if parent_published != 1 or header_published != 1:
        raise ProviderDirectoryRootedGraphPublicationError("state")


async def _materialize_and_publish(
    database: Any,
    admission: ProviderDirectoryRootedGraphTwinAdmission,
    current: Any,
    batch_size: int,
) -> ProviderDirectoryRootedGraphPublicationResult:
    identity = build_provider_directory_rooted_graph_dataset_identity(
        admission,
        current,
    )
    await _assert_no_orphan_parent(database, identity)
    count_by_resource_type = await _preflight_counts(database, admission)
    await _insert_headers(
        database,
        identity,
        admission,
        count_by_resource_type,
    )
    materialized = await materialize_provider_directory_rooted_graph_dataset(
        database,
        identity,
        publication_run_id=admission.publication_run_id,
        batch_size=batch_size,
    )
    if materialized.resource_counts != count_by_resource_type:
        raise ProviderDirectoryRootedGraphPublicationError("content")
    await build_provider_directory_dataset_serving_relations(
        database,
        identity.dataset_id,
        build_run_id=admission.publication_run_id,
        expected_acquisition_root_run_id=identity.acquisition_root_run_id,
    )
    await _validate_and_publish(database, identity, current)
    readiness = await load_dataset_readiness(
        identity.dataset_id,
        database=database,
    )
    if readiness is None:
        raise ProviderDirectoryRootedGraphPublicationError("state")
    return ProviderDirectoryRootedGraphPublicationResult(readiness, replayed=False)


async def _locked_publication_result(
    database: Any,
    publication_acquisition_id: str,
    batch_size: int,
) -> ProviderDirectoryRootedGraphPublicationResult:
    current = await lock_exact_current_dataset(
        database,
        pair=exact_uhc_dataset_pair(),
    )
    admission = await require_provider_directory_rooted_graph_admission(
        publication_acquisition_id,
        database=database,
    )
    replay = await load_replay_readiness(database, publication_acquisition_id)
    if replay is not None:
        return ProviderDirectoryRootedGraphPublicationResult(replay, replayed=True)
    if current is None or not exact_current_matches_root(current, admission):
        raise ProviderDirectoryRootedGraphPublicationError("foreign_current")
    return await _materialize_and_publish(
        database,
        admission,
        current,
        batch_size,
    )


async def publish_admitted_rooted_graph_dataset(
    publication_acquisition_id: str,
    *,
    database: Any = db,
    batch_size: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS,
) -> ProviderDirectoryRootedGraphPublicationResult:
    """Build, prove, supersede, and publish under one logical-current lock."""

    if (
        type(batch_size) is not int
        or not 1
        <= batch_size
        <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS
    ):
        raise ValueError("provider_directory_rooted_graph_batch_size_invalid")
    try:
        async with database.transaction():
            await database.scalar(
                "SELECT pg_catalog.pg_advisory_xact_lock("
                "pg_catalog.hashtextextended(:lock_identity, 0));",
                lock_identity=EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
            )
            return await _locked_publication_result(
                database,
                publication_acquisition_id,
                batch_size,
            )
    except ProviderDirectoryDatasetScopedPublicationError as error:
        code = (
            error.code if error.code in {"foreign_current", "source_drift"} else "state"
        )
        raise ProviderDirectoryRootedGraphPublicationError(code) from error


__all__ = (
    "load_dataset_readiness",
    "publish_admitted_rooted_graph_dataset",
)
