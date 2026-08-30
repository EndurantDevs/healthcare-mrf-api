# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional store operations for admitted Flex dataset publication."""
from __future__ import annotations

from datetime import date
from typing import Any

from db.connection import db
from process.provider_directory_dataset_scoped_publication import (
    ExactCurrentDataset,
    exact_uhc_dataset_pair,
    lock_exact_current_dataset,
    ProviderDirectoryDatasetScopedPublicationError,
    supersede_exact_current_dataset,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
    UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_publication import (
    build_uhc_flex_practitioner_dataset_identity,
    _canonical_json,
    _DATASET_RESOURCE,
    _ENDPOINT_DATASET,
    _function,
    _HEADER,
    _PROVENANCE,
    _READY_FUNCTION,
    _row_fields,
    _SOURCE,
    _table,
    uhc_flex_practitioner_publication_metadata,
    UHCFlexPractitionerDatasetIdentity,
    UHCFlexPractitionerDatasetReadiness,
    UHCFlexPractitionerPublicationError,
    UHCFlexPractitionerPublicationResult,
)
from process.uhc_flex_practitioner_publication_materialization import (
    _materialize_candidate,
    _validate_candidate,
)
from process.uhc_flex_practitioner_single_root_contract import (
    UHCFlexPractitionerAdmission,
    UHCFlexPractitionerSingleRootAdmission,
    UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_store_support import ACQUISITION_TABLE
from process.uhc_flex_practitioner_twin_store import (
    require_uhc_flex_practitioner_admission,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    UHCFlexPractitionerTwinAdmission,
    UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID,
)


def _readiness_from_row(database_row: Any) -> UHCFlexPractitionerDatasetReadiness:
    database_fields = _row_fields(database_row)
    try:
        projection = database_fields.get("semantic_projection_as_of")
        projection_text = (
            projection.isoformat() if hasattr(projection, "isoformat") else projection
        )
        return UHCFlexPractitionerDatasetReadiness(
            dataset_id=database_fields.get("dataset_id"),
            previous_dataset_id=database_fields.get("previous_dataset_id"),
            admission_id=database_fields.get("admission_id"),
            candidate_acquisition_id=database_fields.get("candidate_acquisition_id"),
            acquisition_root_run_id=database_fields.get("acquisition_root_run_id"),
            cohort_id=database_fields.get("cohort_id"),
            dataset_intent_id=database_fields.get("dataset_intent_id"),
            endpoint_id=database_fields.get("endpoint_id"),
            semantic_projection_as_of=projection_text,
            operation_key=database_fields.get("operation_key"),
            dataset_hash=database_fields.get("dataset_hash"),
            resource_count=database_fields.get("resource_count"),
            source_id=database_fields.get("source_id"),
            source_authority_id=database_fields.get("source_authority_id"),
            cohort_complete=database_fields.get("cohort_complete"),
            endpoint_collection_complete=database_fields.get(
                "endpoint_collection_complete"
            ),
            endpoint_complete=database_fields.get("endpoint_complete"),
            retry_exhausted_count=database_fields.get("retry_exhausted_count"),
        )
    except (TypeError, ValueError):
        raise UHCFlexPractitionerPublicationError("state") from None


def _readiness_select_sql(filter_sql: str) -> str:
    return f"""
        SELECT header.dataset_id, header.previous_dataset_id,
               header.admission_id, header.candidate_acquisition_id,
               header.acquisition_root_run_id,
               header.cohort_id, header.dataset_intent_id,
               header.endpoint_id, header.semantic_projection_as_of,
               header.operation_key, header.dataset_hash,
               header.resource_count, header.source_id,
               header.source_authority_id, header.cohort_complete,
               header.endpoint_collection_complete, header.endpoint_complete,
               candidate.error_count AS retry_exhausted_count
          FROM {_table(_HEADER)} AS header
          JOIN {_table(ACQUISITION_TABLE)} AS candidate
            ON candidate.acquisition_id = header.candidate_acquisition_id
         WHERE {filter_sql}
           AND header.status = 'published'
           AND header.is_current IS TRUE
           AND {_function(_READY_FUNCTION)}(header.dataset_id);
    """


async def load_dataset_readiness(
    dataset_id: str,
    *,
    database: Any = db,
) -> UHCFlexPractitionerDatasetReadiness | None:
    """Read one dataset only when its database predicate is ready."""
    database_row = await database.first(
        _readiness_select_sql("header.dataset_id = :dataset_id"),
        dataset_id=dataset_id,
    )
    return None if database_row is None else _readiness_from_row(database_row)


async def load_current_readiness(
    *,
    database: Any = db,
) -> UHCFlexPractitionerDatasetReadiness | None:
    """Read the one current source-local dataset without endpoint claims."""
    database_row = await database.first(
        _readiness_select_sql("header.source_id = :source_id"),
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
    )
    return None if database_row is None else _readiness_from_row(database_row)


async def _locked_existing_dataset(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
) -> dict[str, Any]:
    return _row_fields(
        await database.first(
            f"""
            SELECT header.dataset_id, header.status, header.is_current
              FROM {_table(_HEADER)} AS header
             WHERE header.dataset_id = :dataset_id
             FOR UPDATE;
            """,
            dataset_id=identity.dataset_id,
        )
    )


async def _assert_no_orphan_parent(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
) -> None:
    parent_count = await database.scalar(
        f"SELECT count(*) FROM {_table(_ENDPOINT_DATASET)} "
        "WHERE dataset_id = :dataset_id;",
        dataset_id=identity.dataset_id,
    )
    if parent_count not in {0, 1}:
        raise UHCFlexPractitionerPublicationError("state")
    if parent_count == 1:
        raise UHCFlexPractitionerPublicationError("source_drift")


async def _locked_current_dataset(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
) -> ExactCurrentDataset | None:
    try:
        return await lock_exact_current_dataset(
            database,
            pair=exact_uhc_dataset_pair(),
            require_ready=False,
        )
    except ProviderDirectoryDatasetScopedPublicationError as error:
        code = (
            error.code if error.code in {"foreign_current", "source_drift"} else "state"
        )
        raise UHCFlexPractitionerPublicationError(code) from error


async def _insert_parent_header(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    previous_dataset_id: str | None,
    metadata_json: str,
) -> None:
    inserted = await database.status(
        f"""
        INSERT INTO {_table(_ENDPOINT_DATASET)} (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, dataset_hash, status, is_current,
            resource_count, created_at, validated_at, published_at,
            superseded_at, publication_metadata_json,
            completion_proof_required_version, completion_proof_json,
            completion_proof_sha256
        ) VALUES (
            :dataset_id, :endpoint_id, :candidate_run_id,
            :acquisition_root_run_id, :previous_dataset_id, NULL,
            'building', false, :resource_count, transaction_timestamp(),
            NULL, NULL, NULL, CAST(:metadata_json AS jsonb), NULL, NULL, NULL
        );
        """,
        dataset_id=identity.dataset_id,
        endpoint_id=identity.endpoint_id,
        candidate_run_id=admission.candidate_run_id,
        acquisition_root_run_id=identity.acquisition_root_run_id,
        previous_dataset_id=previous_dataset_id,
        resource_count=admission.resource_count,
        metadata_json=metadata_json,
    )
    if inserted != 1:
        raise UHCFlexPractitionerPublicationError("state")


async def _insert_dedicated_header(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    previous_dataset_id: str | None,
    retry_exhausted_count: int = 0,
) -> None:
    inserted = await database.status(
        f"""
        INSERT INTO {_table(_HEADER)} (
            dataset_id, publication_contract_id, admission_id,
            candidate_acquisition_id, source_id, endpoint_id, cohort_id,
            dataset_intent_id, acquisition_root_run_id,
            semantic_projection_as_of, operation_key, source_authority_id,
            terminal_set_sha256, previous_dataset_id, dataset_hash,
            resource_count, resource_hash_contract, selected_resource_type,
            expected_resource_type, cohort_complete,
            endpoint_collection_complete, endpoint_complete, status,
            is_current, created_at, validated_at, published_at, superseded_at
        ) VALUES (
            :dataset_id, :publication_contract_id, :admission_id,
            :candidate_acquisition_id, :source_id, :endpoint_id, :cohort_id,
            :dataset_intent_id, :acquisition_root_run_id,
            :semantic_projection_as_of, :operation_key, :source_authority_id,
            :terminal_set_sha256, :previous_dataset_id, NULL, :resource_count,
            :resource_hash_contract, :resource_type, :resource_type,
            :cohort_complete, false, false, 'building', false,
            transaction_timestamp(), NULL, NULL, NULL
        );
        """,
        dataset_id=identity.dataset_id,
        publication_contract_id=identity.publication_contract_id,
        admission_id=admission.admission_id,
        candidate_acquisition_id=admission.candidate_acquisition_id,
        source_id=admission.source_id,
        endpoint_id=identity.endpoint_id,
        cohort_id=admission.cohort_id,
        dataset_intent_id=admission.dataset_intent_id,
        acquisition_root_run_id=identity.acquisition_root_run_id,
        semantic_projection_as_of=date.fromisoformat(
            admission.semantic_projection_as_of
        ),
        operation_key=admission.operation_key,
        source_authority_id=UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        terminal_set_sha256=admission.terminal_set_sha256,
        previous_dataset_id=previous_dataset_id,
        resource_count=admission.resource_count,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        resource_type=UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
        cohort_complete=retry_exhausted_count == 0,
    )
    if inserted != 1:
        raise UHCFlexPractitionerPublicationError("state")


async def _insert_building_headers(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    previous_dataset_id: str | None,
    retry_exhausted_count: int = 0,
) -> None:
    metadata_json = _canonical_json(
        uhc_flex_practitioner_publication_metadata(
            identity,
            admission,
            retry_exhausted_count,
        )
    )
    await _insert_parent_header(
        database,
        identity,
        admission,
        previous_dataset_id,
        metadata_json,
    )
    await _insert_dedicated_header(
        database,
        identity,
        admission,
        previous_dataset_id,
        retry_exhausted_count,
    )


async def _supersede_previous(
    database: Any,
    previous_dataset: ExactCurrentDataset | None,
) -> None:
    try:
        await supersede_exact_current_dataset(database, previous_dataset)
    except ProviderDirectoryDatasetScopedPublicationError as error:
        code = (
            error.code if error.code in {"foreign_current", "source_drift"} else "state"
        )
        raise UHCFlexPractitionerPublicationError(code) from error


async def _publish_candidate(database: Any, dataset_id: str) -> None:
    parent_updated = await database.status(
        f"""
        UPDATE {_table(_ENDPOINT_DATASET)}
           SET status = 'published', is_current = true,
               published_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'validated'
           AND is_current IS FALSE;
        """,
        dataset_id=dataset_id,
    )
    header_updated = await database.status(
        f"""
        UPDATE {_table(_HEADER)}
           SET status = 'published', is_current = true,
               published_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'validated'
           AND is_current IS FALSE;
        """,
        dataset_id=dataset_id,
    )
    if parent_updated != 1 or header_updated != 1:
        raise UHCFlexPractitionerPublicationError("state")


def _is_expected_admission(
    admission: object,
    candidate_acquisition_id: str,
) -> bool:
    has_expected_contract = (
        type(admission) is UHCFlexPractitionerTwinAdmission
        and admission.admission_contract_id
        == UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID
    ) or (
        type(admission) is UHCFlexPractitionerSingleRootAdmission
        and admission.admission_contract_id
        == UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID
    )
    return bool(
        has_expected_contract
        and admission.candidate_acquisition_id == candidate_acquisition_id
        and admission.source_id == UHC_FLEX_PRACTITIONER_SOURCE_ID
        and admission.connector_id == UHC_FLEX_PRACTITIONER_CONNECTOR_ID
        and admission.query_contract_id == UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID
        and admission.publication_authority is True
    )


async def _lock_admission(
    database: Any,
    candidate_acquisition_id: str,
    endpoint_id: str,
) -> tuple[UHCFlexPractitionerAdmission, int]:
    await database.scalar(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        "pg_catalog.hashtextextended(:lock_identity, 0));",
        lock_identity=UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY,
    )
    locked_source = await database.first(
        f"""
        SELECT source.source_id
          FROM {_table(_SOURCE)} AS source
          JOIN {_table('provider_directory_api_endpoint')} AS endpoint
            ON endpoint.endpoint_id = source.endpoint_id
         WHERE source.source_id = :source_id
           AND source.endpoint_id = :endpoint_id
         FOR SHARE OF source, endpoint;
        """,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        endpoint_id=endpoint_id,
    )
    if locked_source is None:
        raise UHCFlexPractitionerPublicationError("source_drift")
    admission = await require_uhc_flex_practitioner_admission(
        candidate_acquisition_id,
        database=database,
    )
    if not _is_expected_admission(admission, candidate_acquisition_id):
        raise UHCFlexPractitionerPublicationError("admission")
    candidate = _row_fields(
        await database.first(
            f"""
            SELECT status, cohort_complete, error_count,
                   terminal_set_sha256, resource_count
              FROM {_table(ACQUISITION_TABLE)}
             WHERE acquisition_id = :candidate_acquisition_id
             FOR SHARE;
            """,
            candidate_acquisition_id=candidate_acquisition_id,
        )
    )
    retry_exhausted_count = candidate.get("error_count")
    if (
        candidate.get("status") != "sealed"
        or type(retry_exhausted_count) is not int
        or retry_exhausted_count < 0
        or candidate.get("cohort_complete") is not (retry_exhausted_count == 0)
        or candidate.get("terminal_set_sha256") != admission.terminal_set_sha256
        or candidate.get("resource_count") != admission.resource_count
        or (
            retry_exhausted_count > 0
            and type(admission) is not UHCFlexPractitionerSingleRootAdmission
        )
    ):
        raise UHCFlexPractitionerPublicationError("admission")
    return admission, retry_exhausted_count


async def _publish_admitted_dataset(
    database: Any,
    admission: UHCFlexPractitionerAdmission,
    endpoint_id: str,
    batch_size: int,
    retry_exhausted_count: int = 0,
) -> UHCFlexPractitionerPublicationResult:
    identity = build_uhc_flex_practitioner_dataset_identity(
        admission,
        endpoint_id=endpoint_id,
    )
    if await _locked_existing_dataset(database, identity):
        readiness = await load_dataset_readiness(
            identity.dataset_id,
            database=database,
        )
        if readiness is None:
            raise UHCFlexPractitionerPublicationError("replay")
        return UHCFlexPractitionerPublicationResult(readiness, replayed=True)
    await _assert_no_orphan_parent(database, identity)
    previous_dataset = await _locked_current_dataset(database, identity)
    previous_dataset_id = (
        previous_dataset.dataset_id if previous_dataset is not None else None
    )
    await _insert_building_headers(
        database,
        identity,
        admission,
        previous_dataset_id,
        retry_exhausted_count,
    )
    await _materialize_candidate(database, identity, admission, batch_size)
    await _validate_candidate(database, identity, admission, batch_size)
    await _supersede_previous(database, previous_dataset)
    await _publish_candidate(database, identity.dataset_id)
    readiness = await load_dataset_readiness(
        identity.dataset_id,
        database=database,
    )
    if readiness is None:
        raise UHCFlexPractitionerPublicationError("state")
    return UHCFlexPractitionerPublicationResult(readiness, replayed=False)


async def publish_registered_uhc_flex_dataset(
    candidate_acquisition_id: str,
    endpoint_id: str,
    batch_size: int,
    *,
    database: Any = db,
) -> UHCFlexPractitionerPublicationResult:
    """Publish within one lock-protected transaction after registration."""

    async with database.transaction():
        admission, retry_exhausted_count = await _lock_admission(
            database,
            candidate_acquisition_id,
            endpoint_id,
        )
        return await _publish_admitted_dataset(
            database,
            admission,
            endpoint_id,
            batch_size,
            retry_exhausted_count,
        )


__all__ = (
    "load_current_readiness",
    "load_dataset_readiness",
    "publish_registered_uhc_flex_dataset",
)
