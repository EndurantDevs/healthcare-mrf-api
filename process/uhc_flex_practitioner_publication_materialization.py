# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded raw paging and semantic proof for Flex dataset publication."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)
from process.uhc_flex_practitioner_materialization import (
    materialize_uhc_flex_practitioner_stored_resource,
)
from process.uhc_flex_practitioner_publication import (
    _canonical_json,
    _DATASET_RESOURCE,
    _ENDPOINT_DATASET,
    _function,
    _HEADER,
    _PROVENANCE,
    _row_fields,
    _table,
    _VALID_FUNCTION,
    UHCFlexPractitionerDatasetIdentity,
    UHCFlexPractitionerPublicationError,
)
from process.uhc_flex_practitioner_result_store import (
    read_uhc_flex_practitioner_resource_page,
)
from process.uhc_flex_practitioner_single_root_contract import (
    UHCFlexPractitionerAdmission,
)


async def _insert_materialized_page(
    database: Any,
    page_rows: list[dict[str, Any]],
) -> None:
    if not page_rows:
        return
    rows_json = _canonical_json(page_rows)
    inserted_resources = await database.status(
        f"""
        INSERT INTO {_table(_DATASET_RESOURCE)} (
            dataset_id, resource_type, resource_id, payload_hash,
            payload_json, acquired_resource_sha256
        )
        SELECT input.dataset_id, input.resource_type, input.resource_id,
               input.payload_hash, input.payload_json, NULL
          FROM pg_catalog.jsonb_to_recordset(CAST(:rows_json AS jsonb)) AS input(
               dataset_id text, resource_type text, resource_id text,
               payload_hash text, payload_json jsonb,
               requested_npi bigint, candidate_acquisition_id text,
               acquired_resource_sha256 text
          );
        """,
        rows_json=rows_json,
    )
    if inserted_resources != len(page_rows):
        raise UHCFlexPractitionerPublicationError("content")
    inserted_provenance = await database.status(
        f"""
        INSERT INTO {_table(_PROVENANCE)} (
            dataset_id, resource_type, resource_id, requested_npi,
            candidate_acquisition_id, payload_hash,
            acquired_resource_sha256
        )
        SELECT input.dataset_id, input.resource_type, input.resource_id,
               input.requested_npi, input.candidate_acquisition_id,
               input.payload_hash, input.acquired_resource_sha256
          FROM pg_catalog.jsonb_to_recordset(CAST(:rows_json AS jsonb)) AS input(
               dataset_id text, resource_type text, resource_id text,
               payload_hash text, payload_json jsonb,
               requested_npi bigint, candidate_acquisition_id text,
               acquired_resource_sha256 text
          );
        """,
        rows_json=rows_json,
    )
    if inserted_provenance != len(page_rows):
        raise UHCFlexPractitionerPublicationError("content")


async def _materialize_candidate(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    batch_size: int,
) -> int:
    after_npi = 0
    after_resource_id = ""
    inserted_count = 0
    while True:
        stored_page = await read_uhc_flex_practitioner_resource_page(
            admission.candidate_acquisition_id,
            after_npi=after_npi,
            after_resource_id=after_resource_id,
            limit=batch_size,
            database=database,
        )
        if not stored_page:
            break
        page_rows: list[dict[str, Any]] = []
        for stored_resource in stored_page:
            materialized = materialize_uhc_flex_practitioner_stored_resource(
                stored_resource,
                dataset_id=identity.dataset_id,
                source_id=admission.source_id,
                run_id=admission.candidate_run_id,
                semantic_projection_as_of=admission.semantic_projection_as_of,
            )
            page_rows.append(
                {
                    **materialized.dataset_resource,
                    "requested_npi": materialized.requested_npi,
                    "candidate_acquisition_id": admission.candidate_acquisition_id,
                }
            )
        await _insert_materialized_page(database, page_rows)
        inserted_count += len(page_rows)
        if inserted_count > admission.resource_count:
            raise UHCFlexPractitionerPublicationError("content")
        after_npi = stored_page[-1].requested_npi
        after_resource_id = stored_page[-1].resource_id
    if inserted_count != admission.resource_count:
        raise UHCFlexPractitionerPublicationError("content")
    return inserted_count


def _semantic_resource_identity(
    database_fields: dict[str, Any],
) -> tuple[str, str, str]:
    payload_by_field = database_fields.get("payload_json")
    if isinstance(payload_by_field, str):
        try:
            payload_by_field = json.loads(payload_by_field)
        except ValueError:
            raise UHCFlexPractitionerPublicationError("content") from None
    if (
        database_fields.get("resource_type")
        != UHC_FLEX_OFFICIAL_RESOURCE_TYPE
        or not isinstance(payload_by_field, Mapping)
        or database_fields.get("acquired_resource_sha256") is not None
    ):
        raise UHCFlexPractitionerPublicationError("content")
    expected_payload_hash = resource_payload_sha256_for_contract(
        payload_by_field,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    if database_fields.get("payload_hash") != expected_payload_hash:
        raise UHCFlexPractitionerPublicationError("content")
    return (
        database_fields["resource_type"],
        database_fields["resource_id"],
        database_fields["payload_hash"],
    )


async def _semantic_dataset_proof(
    database: Any,
    dataset_id: str,
    batch_size: int,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    resource_count = 0
    after_resource_type = ""
    after_resource_id = ""
    while True:
        database_resources = await database.all(
            f"""
            SELECT resource_type, resource_id, payload_hash, payload_json,
                   acquired_resource_sha256
              FROM {_table(_DATASET_RESOURCE)}
             WHERE dataset_id = :dataset_id
               AND (resource_type, resource_id) >
                   (:after_resource_type, :after_resource_id)
             ORDER BY resource_type, resource_id
             LIMIT :batch_size;
            """,
            dataset_id=dataset_id,
            after_resource_type=after_resource_type,
            after_resource_id=after_resource_id,
            batch_size=batch_size,
        )
        if not database_resources:
            break
        for database_resource in database_resources:
            semantic_identity = _semantic_resource_identity(
                _row_fields(database_resource)
            )
            if resource_count:
                digest.update(b"\n")
            digest.update(_canonical_json(list(semantic_identity)).encode("utf-8"))
            resource_count += 1
        last_resource = _row_fields(database_resources[-1])
        after_resource_type = last_resource["resource_type"]
        after_resource_id = last_resource["resource_id"]
    return digest.hexdigest(), resource_count


async def _validate_candidate(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    batch_size: int,
) -> str:
    dataset_hash, resource_count = await _semantic_dataset_proof(
        database,
        identity.dataset_id,
        batch_size,
    )
    if resource_count != admission.resource_count:
        raise UHCFlexPractitionerPublicationError("content")
    parent_updated = await database.status(
        f"""
        UPDATE {_table(_ENDPOINT_DATASET)}
           SET dataset_hash = :dataset_hash, status = 'validated',
               validated_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'building'
           AND is_current IS FALSE AND dataset_hash IS NULL
           AND resource_count = :resource_count;
        """,
        dataset_hash=dataset_hash,
        dataset_id=identity.dataset_id,
        resource_count=resource_count,
    )
    header_updated = await database.status(
        f"""
        UPDATE {_table(_HEADER)}
           SET dataset_hash = :dataset_hash, status = 'validated',
               validated_at = transaction_timestamp()
         WHERE dataset_id = :dataset_id AND status = 'building'
           AND is_current IS FALSE AND dataset_hash IS NULL
           AND resource_count = :resource_count;
        """,
        dataset_hash=dataset_hash,
        dataset_id=identity.dataset_id,
        resource_count=resource_count,
    )
    if parent_updated != 1 or header_updated != 1:
        raise UHCFlexPractitionerPublicationError("state")
    if not await database.scalar(
        f"SELECT {_function(_VALID_FUNCTION)}(:dataset_id);",
        dataset_id=identity.dataset_id,
    ):
        raise UHCFlexPractitionerPublicationError("content")
    return dataset_hash


__all__ = ("_materialize_candidate", "_validate_candidate")
