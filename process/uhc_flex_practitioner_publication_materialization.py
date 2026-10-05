# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded raw paging and semantic proof for Flex dataset publication."""

from __future__ import annotations

from datetime import date
import hashlib
import json
from typing import Any

from process.provider_directory_dataset_candidate import copy_dataset_candidate_rows
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process.uhc_flex_practitioner_materialization import (
    _materialize_admitted_practitioner_resource,
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
    _resource_page_records,
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
    inserted_resources = await copy_dataset_candidate_rows(
        database, _DATASET_RESOURCE,
        ("dataset_id", "resource_type", "resource_id", "payload_hash",
         "payload_json", "acquired_resource_sha256"),
        [(row["dataset_id"], row["resource_type"], row["resource_id"], row["payload_hash"],
          _canonical_json(row["payload_json"]), None) for row in page_rows],
    )
    if inserted_resources != len(page_rows):
        raise UHCFlexPractitionerPublicationError("content")
    provenance_columns = (
        "dataset_id", "resource_type", "resource_id", "requested_npi",
        "candidate_acquisition_id", "payload_hash", "acquired_resource_sha256",
    )
    inserted_provenance = await copy_dataset_candidate_rows(
        database, _PROVENANCE, provenance_columns,
        [tuple(row[name] for name in provenance_columns) for row in page_rows],
    )
    if inserted_provenance != len(page_rows):
        raise UHCFlexPractitionerPublicationError("content")


async def _materialize_candidate(
    database: Any,
    identity: UHCFlexPractitionerDatasetIdentity,
    admission: UHCFlexPractitionerAdmission,
    batch_size: int,
) -> int:
    # Admission set validation binds immutable raw identity, hash and NPI.
    projection_date = date.fromisoformat(admission.semantic_projection_as_of)
    after_npi = 0
    after_resource_id = ""
    inserted_count = 0
    while True:
        stored_page = await _resource_page_records(
            database, admission.candidate_acquisition_id,
            after_npi, after_resource_id, batch_size,
        )
        if not stored_page:
            break
        page_rows = [
            {
                **_materialize_admitted_practitioner_resource(
                    _row_fields(stored_resource), dataset_id=identity.dataset_id,
                    source_id=admission.source_id, run_id=admission.candidate_run_id,
                    projection_date=projection_date,
                ),
                "candidate_acquisition_id": admission.candidate_acquisition_id,
            }
            for stored_resource in stored_page
        ]
        await _insert_materialized_page(database, page_rows)
        inserted_count += len(page_rows)
        if inserted_count > admission.resource_count:
            raise UHCFlexPractitionerPublicationError("content")
        final_fields = _row_fields(stored_page[-1])
        after_npi = final_fields["npi"]
        after_resource_id = final_fields["resource_id"]
    if inserted_count != admission.resource_count:
        raise UHCFlexPractitionerPublicationError("content")
    return inserted_count


def _semantic_resource_identity(
    database_fields: dict[str, Any],
) -> tuple[str, str, str]:
    # The candidate set proof owns shape, identity and provenance validation.
    # Recompute only the exact Python content commitment after JSONB storage;
    # replacing this with a SQL JSON formatter changes numeric/name semantics.
    payload_by_field = database_fields["payload_json"]
    if isinstance(payload_by_field, str):
        payload_by_field = json.loads(payload_by_field)
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
