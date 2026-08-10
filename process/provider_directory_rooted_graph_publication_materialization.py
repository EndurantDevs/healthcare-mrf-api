# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Materialize one admitted rooted graph into generic dataset resources."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Mapping

from process.provider_directory_fhir import (
    materialize_provider_directory_dataset_fhir_resource,
)
from process.provider_directory_rooted_graph_publication import (
    canonical_json,
    ProviderDirectoryRootedGraphDatasetIdentity,
    ProviderDirectoryRootedGraphPublicationError,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_OUTPUT_RESOURCES,
)
from process.provider_directory_rooted_graph_store_contract import RUN_PATTERN


_RESOURCE_ID_PATTERN = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")
PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS = 4096
PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphMaterialization:
    """Exact row counts inserted for one combined generic dataset."""

    resource_counts: dict[str, int]

    def __post_init__(self) -> None:
        if (
            type(self.resource_counts) is not dict
            or set(self.resource_counts)
            != set(PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES)
            or any(
                type(value) is not int or value < 0
                for value in self.resource_counts.values()
            )
            or self.resource_counts["Practitioner"] < 1
        ):
            raise ValueError("provider_directory_rooted_graph_materialization_invalid")

    @property
    def resource_count(self) -> int:
        """Return the total rows across the eight closed resource families."""

        return sum(self.resource_counts.values())


def _schema() -> str:
    import os

    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise ProviderDirectoryRootedGraphPublicationError("state")
    schema = runtime or legacy or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ProviderDirectoryRootedGraphPublicationError("state")
    return schema


def _table(name: str) -> str:
    return f'"{_schema()}"."{name}"'


def _row_fields(row: Any) -> dict[str, Any]:
    mapping = row._mapping if hasattr(row, "_mapping") else row
    if not isinstance(mapping, Mapping):
        raise ProviderDirectoryRootedGraphPublicationError("state")
    return dict(mapping)


async def _insert_rows(
    database: Any,
    materialized_pairs: list[tuple[dict[str, Any], dict[str, Any]]],
) -> None:
    """Bulk-insert one bounded normalized page and its exact provenance."""

    if not materialized_pairs:
        return
    resource_rows_json = canonical_json(
        [resource for resource, _ in materialized_pairs]
    )
    evidence_rows_json = canonical_json(
        [evidence for _, evidence in materialized_pairs]
    )
    inserted_resources = await database.status(
        f"""
        INSERT INTO {_table('provider_directory_dataset_resource')} (
            dataset_id, resource_type, resource_id, payload_hash,
            payload_json, acquired_resource_sha256
        )
        SELECT incoming.dataset_id, incoming.resource_type,
               incoming.resource_id, incoming.payload_hash,
               incoming.payload_json, NULL
          FROM pg_catalog.jsonb_to_recordset(
               CAST(:resource_rows_json AS jsonb)
          ) AS incoming(
               dataset_id text, resource_type text, resource_id text,
               payload_hash text, payload_json jsonb
          );
        """,
        resource_rows_json=resource_rows_json,
    )
    inserted_evidence = await database.status(
        f"""
        INSERT INTO {_table('provider_directory_rooted_graph_dataset_resource')} (
            dataset_id, resource_type, resource_id, origin_kind,
            root_dataset_id, publication_acquisition_id, query_id, attempt,
            closure_scope, source_payload_sha256, published_payload_hash
        )
        SELECT incoming.dataset_id, incoming.resource_type,
               incoming.resource_id, incoming.origin_kind,
               incoming.root_dataset_id, incoming.publication_acquisition_id,
               incoming.query_id, incoming.attempt,
               incoming.closure_scope, incoming.source_payload_sha256,
               incoming.published_payload_hash
          FROM pg_catalog.jsonb_to_recordset(
               CAST(:evidence_rows_json AS jsonb)
          ) AS incoming(
               dataset_id text, resource_type text, resource_id text,
               origin_kind text, root_dataset_id text,
               publication_acquisition_id text, query_id text,
               attempt integer, closure_scope text,
               source_payload_sha256 text, published_payload_hash text
          );
        """,
        evidence_rows_json=evidence_rows_json,
    )
    _require_insert_counts(materialized_pairs, inserted_resources, inserted_evidence)


def _require_insert_counts(
    materialized_pairs: list[tuple[dict[str, Any], dict[str, Any]]],
    inserted_resources: int,
    inserted_evidence: int,
) -> None:
    expected_count = len(materialized_pairs)
    if inserted_resources != expected_count or inserted_evidence != expected_count:
        raise ProviderDirectoryRootedGraphPublicationError("content")


def _resource_record(
    row: Mapping[str, Any],
    dataset_id: str,
) -> dict[str, Any]:
    resource_type = row.get("resource_type")
    resource_id = row.get("resource_id")
    payload_hash = row.get("payload_hash")
    payload = row.get("payload_json")
    if (
        resource_type not in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
        or type(resource_id) is not str
        or _RESOURCE_ID_PATTERN.fullmatch(resource_id) is None
        or type(payload_hash) is not str
        or re.fullmatch(r"[0-9a-f]{64}", payload_hash) is None
        or not isinstance(payload, dict)
    ):
        raise ProviderDirectoryRootedGraphPublicationError("content")
    return {
        "dataset_id": dataset_id,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_hash": payload_hash,
        "payload_json": payload,
    }


async def _copy_root_practitioners(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
) -> int:
    # The caller holds an exact ready root under the logical-current lock. Its
    # semantic_content_v3 hashes are therefore already proven by the dedicated
    # readiness function; the final rooted validity proof also compares every
    # copied payload/hash to that locked root. Keep the million-row copy set-wise.
    copied = await database.status(
        f"""
        INSERT INTO {_table('provider_directory_dataset_resource')} (
            dataset_id, resource_type, resource_id, payload_hash,
            payload_json, acquired_resource_sha256
        )
        SELECT :dataset_id, member.resource_type, member.resource_id,
               member.payload_hash, member.payload_json, NULL
          FROM {_table('provider_directory_dataset_resource')} AS member
         WHERE member.dataset_id = :root_dataset_id
           AND member.resource_type = 'Practitioner'
           AND member.acquired_resource_sha256 IS NULL;
        """,
        dataset_id=identity.dataset_id,
        root_dataset_id=identity.root_dataset_id,
    )
    if copied != identity.root_practitioner_resource_count:
        raise ProviderDirectoryRootedGraphPublicationError("content")
    evidence_count = await database.status(
        f"""
        INSERT INTO {_table('provider_directory_rooted_graph_dataset_resource')} (
            dataset_id, resource_type, resource_id, origin_kind,
            root_dataset_id, publication_acquisition_id, query_id, attempt,
            closure_scope, source_payload_sha256, published_payload_hash
        )
        SELECT member.dataset_id, member.resource_type, member.resource_id,
               'root_practitioner', :root_dataset_id,
               :publication_acquisition_id, NULL, NULL, NULL, NULL,
               member.payload_hash
          FROM {_table('provider_directory_dataset_resource')} AS member
         WHERE member.dataset_id = :dataset_id
           AND member.resource_type = 'Practitioner';
        """,
        dataset_id=identity.dataset_id,
        root_dataset_id=identity.root_dataset_id,
        publication_acquisition_id=identity.publication_acquisition_id,
    )
    if evidence_count != copied:
        raise ProviderDirectoryRootedGraphPublicationError("content")
    return copied


def _raw_cursor(fields: Mapping[str, Any]) -> tuple[object, ...]:
    return tuple(
        fields[name]
        for name in (
            "resource_type",
            "resource_id",
            "payload_sha256",
            "query_id",
            "attempt",
        )
    )


def _graph_page_sql() -> str:
    return f"""
        WITH limited AS MATERIALIZED (
            SELECT raw.resource_type, raw.resource_id, raw.payload_sha256,
                   raw.payload_json_text, raw.query_id, raw.attempt,
                   raw.closure_scope
              FROM {_table('provider_directory_rooted_graph_resource')} AS raw
              JOIN {_table('provider_directory_rooted_graph_work')} AS work
                ON work.acquisition_id = raw.acquisition_id
               AND work.query_id = raw.query_id
               AND work.attempt_count = raw.attempt
             WHERE raw.acquisition_id = :acquisition_id
               AND work.status = 'completed'
               AND raw.closure_scope IN ('root', 'plan')
               AND raw.resource_type IN (
                   'InsurancePlan','PractitionerRole',
                   'OrganizationAffiliation','Organization','Location',
                   'HealthcareService','Endpoint'
               )
               AND ROW(raw.resource_type, raw.resource_id,
                       raw.payload_sha256, raw.query_id, raw.attempt) >
                   ROW(:cursor_type, :cursor_id, :cursor_hash,
                       :cursor_query, :cursor_attempt)
             ORDER BY raw.resource_type, raw.resource_id,
                      raw.payload_sha256, raw.query_id, raw.attempt
             LIMIT :batch_size
        ), bounded AS (
            SELECT limited.*,
                   row_number() OVER (ORDER BY resource_type, resource_id,
                                      payload_sha256, query_id, attempt) AS row_number,
                   sum(octet_length(payload_json_text)) OVER (
                       ORDER BY resource_type, resource_id,
                                payload_sha256, query_id, attempt
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cumulative_payload_bytes
              FROM limited
        )
        SELECT resource_type, resource_id, payload_sha256,
               payload_json_text, query_id, attempt, closure_scope
          FROM bounded
         WHERE row_number = 1 OR cumulative_payload_bytes <= :batch_payload_bytes
         ORDER BY resource_type, resource_id, payload_sha256, query_id, attempt;
    """


async def _load_graph_page(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    *,
    cursor: tuple[object, ...],
    batch_size: int,
) -> list[Any]:
    return await database.all(
        _graph_page_sql(),
        acquisition_id=identity.publication_acquisition_id,
        cursor_type=cursor[0],
        cursor_id=cursor[1],
        cursor_hash=cursor[2],
        cursor_query=cursor[3],
        cursor_attempt=cursor[4],
        batch_size=batch_size,
        batch_payload_bytes=PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_BYTES,
    )


def _raw_graph_resource(
    fields: Mapping[str, Any],
    key: tuple[str, str],
) -> dict[str, Any]:
    try:
        raw_payload = json.loads(fields["payload_json_text"])
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphPublicationError("content") from None
    if (
        not isinstance(raw_payload, dict)
        or raw_payload.get("resourceType") != key[0]
        or raw_payload.get("id") != key[1]
    ):
        raise ProviderDirectoryRootedGraphPublicationError("content")
    return raw_payload


def _materialized_graph_pair(
    fields: Mapping[str, Any],
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    publication_run_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    key = (fields["resource_type"], fields["resource_id"])
    raw_payload = _raw_graph_resource(fields, key)
    normalized = materialize_provider_directory_dataset_fhir_resource(
        source_id=identity.source_id,
        dataset_id=identity.dataset_id,
        resource=raw_payload,
        run_id=publication_run_id,
        semantic_projection_as_of=identity.semantic_projection_as_of,
    )
    resource_by_field = _resource_record(normalized, identity.dataset_id)
    if (
        resource_by_field["resource_type"] != key[0]
        or resource_by_field["resource_id"] != key[1]
    ):
        raise ProviderDirectoryRootedGraphPublicationError("content")
    evidence_by_field = {
        "dataset_id": identity.dataset_id,
        "resource_type": key[0],
        "resource_id": key[1],
        "origin_kind": "rooted_graph",
        "root_dataset_id": identity.root_dataset_id,
        "publication_acquisition_id": identity.publication_acquisition_id,
        "query_id": fields["query_id"],
        "attempt": fields["attempt"],
        "closure_scope": fields["closure_scope"],
        "source_payload_sha256": fields["payload_sha256"],
        "published_payload_hash": resource_by_field["payload_hash"],
    }
    return resource_by_field, evidence_by_field


async def _materialize_graph_rows(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    *,
    publication_run_id: str,
    batch_size: int,
) -> dict[str, int]:
    cursor: tuple[object, ...] = ("", "", "", "", 0)
    count_by_resource_type = {
        resource_type: 0
        for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_OUTPUT_RESOURCES
    }
    last_key: tuple[str, str] | None = None
    last_source_hash: str | None = None
    while True:
        graph_rows = await _load_graph_page(
            database,
            identity,
            cursor=cursor,
            batch_size=batch_size,
        )
        if not graph_rows:
            break
        materialized_pairs = []
        for graph_row in graph_rows:
            fields = _row_fields(graph_row)
            cursor = _raw_cursor(fields)
            key = (fields["resource_type"], fields["resource_id"])
            if key == last_key:
                # The DB guard binds payload_sha256 to canonical raw JSON, so
                # an identical SHA is an identical normalization input.
                if fields["payload_sha256"] != last_source_hash:
                    raise ProviderDirectoryRootedGraphPublicationError("content")
                continue
            materialized_pairs.append(
                _materialized_graph_pair(fields, identity, publication_run_id)
            )
            count_by_resource_type[key[0]] += 1
            last_key = key
            last_source_hash = fields["payload_sha256"]
        await _insert_rows(database, materialized_pairs)
    return count_by_resource_type


async def materialize_provider_directory_rooted_graph_dataset(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    *,
    publication_run_id: str,
    batch_size: int,
) -> ProviderDirectoryRootedGraphMaterialization:
    """Copy the exact root subset and normalize only rooted closure witnesses."""

    if (
        type(identity) is not ProviderDirectoryRootedGraphDatasetIdentity
        or type(publication_run_id) is not str
        or RUN_PATTERN.fullmatch(publication_run_id) is None
        or type(batch_size) is not int
        or not 1
        <= batch_size
        <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS
    ):
        raise ValueError("provider_directory_rooted_graph_materialization_invalid")
    practitioner_count = await _copy_root_practitioners(
        database,
        identity,
    )
    graph_counts = await _materialize_graph_rows(
        database,
        identity,
        publication_run_id=publication_run_id,
        batch_size=batch_size,
    )
    count_by_resource_type = {
        resource_type: (
            practitioner_count
            if resource_type == "Practitioner"
            else graph_counts[resource_type]
        )
        for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_DATASET_RESOURCES
    }
    return ProviderDirectoryRootedGraphMaterialization(count_by_resource_type)


__all__ = (
    "materialize_provider_directory_rooted_graph_dataset",
    "ProviderDirectoryRootedGraphMaterialization",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_BYTES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS",
)
