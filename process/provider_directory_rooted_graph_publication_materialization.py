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
    from importlib import import_module

    return import_module("process.provider_directory_fhir")._qt(_schema(), name)


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
    from process.provider_directory_dataset_candidate import copy_dataset_candidate_rows

    resource_columns = (
        "dataset_id", "resource_type", "resource_id", "payload_hash",
        "payload_json", "acquired_resource_sha256",
    )
    evidence_columns = (
        "dataset_id", "resource_type", "resource_id", "origin_kind",
        "root_dataset_id", "publication_acquisition_id", "query_id", "attempt",
        "closure_scope", "source_payload_sha256", "published_payload_hash",
    )
    resources = []
    for resource, _ in materialized_pairs:
        payload_json = resource["payload_json"]
        resources.append(tuple(
            (payload_json if isinstance(payload_json, str) else canonical_json(payload_json))
            if name == "payload_json" else resource.get(name)
            for name in resource_columns
        ))
    inserted_resources = await copy_dataset_candidate_rows(
        database, "provider_directory_dataset_resource", resource_columns, resources
    )
    inserted_evidence = await copy_dataset_candidate_rows(
        database, "provider_directory_rooted_graph_dataset_resource", evidence_columns,
        [tuple(evidence[name] for name in evidence_columns) for _, evidence in materialized_pairs],
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


async def _copy_root_practitioners(
    database: Any,
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
) -> int:
    # The ready predecessor remains pinned for the caller's publication transaction.
    source_relation = f'"{_schema()}"."provider_directory_dataset_resource"'
    copied = 0
    after_id = ""
    while True:
        page = await database.all(
            f"""
            WITH bounded AS MATERIALIZED (
                SELECT resource_id, payload_hash, payload_json::text AS payload_json
                  FROM {source_relation}
                 WHERE dataset_id = :root_dataset_id
                   AND resource_type = 'Practitioner'
                   AND acquired_resource_sha256 IS NULL
                   AND resource_id > :after_id
                 ORDER BY resource_id LIMIT :row_limit
            ), measured AS (
                SELECT *, sum(octet_length(payload_json)) OVER (ORDER BY resource_id)
                    AS payload_bytes FROM bounded
            ) SELECT resource_id, payload_hash, payload_json FROM measured
               WHERE payload_bytes <= :byte_limit ORDER BY resource_id;
            """,
            root_dataset_id=identity.root_dataset_id,
            after_id=after_id,
            row_limit=PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS,
            byte_limit=PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_BYTES,
        )
        if not page:
            break
        pairs = []
        for resource_row in page:
            member = _row_fields(resource_row)
            resource_by_field = {
                "dataset_id": identity.dataset_id,
                "resource_type": "Practitioner",
                **member,
            }
            evidence_by_field = {
                "dataset_id": identity.dataset_id,
                "resource_type": "Practitioner",
                "resource_id": member["resource_id"],
                "origin_kind": "root_practitioner",
                "root_dataset_id": identity.root_dataset_id,
                "publication_acquisition_id": identity.publication_acquisition_id,
                "query_id": None, "attempt": None, "closure_scope": None,
                "source_payload_sha256": None,
                "published_payload_hash": member["payload_hash"],
            }
            pairs.append((resource_by_field, evidence_by_field))
        await _insert_rows(database, pairs)
        copied += len(pairs)
        after_id = pairs[-1][0]["resource_id"]
    if copied != identity.root_practitioner_resource_count:
        raise ProviderDirectoryRootedGraphPublicationError("content")
    return copied


def _graph_source_sql() -> str:
    return f"""
        SELECT raw.resource_type, raw.resource_id, raw.payload_sha256,
               raw.payload_json_text, raw.query_id, raw.attempt, raw.closure_scope
          FROM {_table('provider_directory_rooted_graph_resource')} AS raw
          JOIN {_table('provider_directory_rooted_graph_work')} AS work
            ON work.acquisition_id = raw.acquisition_id
           AND work.query_id = raw.query_id AND work.attempt_count = raw.attempt
         WHERE raw.acquisition_id = :acquisition_id AND work.status = 'completed'
           AND raw.closure_scope IN ('root', 'plan')
           AND raw.resource_type IN (
               'InsurancePlan','PractitionerRole','OrganizationAffiliation',
               'Organization','Location','HealthcareService','Endpoint'
           )
    """


def _graph_page_sql() -> str:
    return f"""
        WITH limited AS MATERIALIZED (
            SELECT DISTINCT ON (resource_type, resource_id) *
              FROM ({_graph_source_sql()}) AS source
             WHERE ROW(resource_type, resource_id) > ROW(:cursor_type, :cursor_id)
             ORDER BY resource_type, resource_id, payload_sha256, query_id, attempt
             LIMIT :batch_size
        ), bounded AS (
            SELECT limited.*,
                   row_number() OVER (ORDER BY resource_type, resource_id) AS row_number,
                   sum(octet_length(payload_json_text)) OVER (
                       ORDER BY resource_type, resource_id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cumulative_payload_bytes
              FROM limited
        )
        SELECT resource_type, resource_id, payload_sha256,
               payload_json_text, query_id, attempt, closure_scope
          FROM bounded
         WHERE row_number = 1 OR cumulative_payload_bytes <= :batch_payload_bytes
         ORDER BY resource_type, resource_id;
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
        batch_size=batch_size,
        batch_payload_bytes=PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_BYTES,
    )


def _materialized_graph_pair(
    fields: Mapping[str, Any],
    identity: ProviderDirectoryRootedGraphDatasetIdentity,
    publication_run_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    key = (fields["resource_type"], fields["resource_id"])
    raw_payload = json.loads(fields["payload_json_text"])
    normalized = materialize_provider_directory_dataset_fhir_resource(
        source_id=identity.source_id,
        dataset_id=identity.dataset_id,
        resource=raw_payload,
        run_id=publication_run_id,
        semantic_projection_as_of=identity.semantic_projection_as_of,
    )
    resource_by_field = {**normalized, "dataset_id": identity.dataset_id}
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
    # Publication preflight proved one source hash per identity on the sealed set.
    cursor: tuple[object, ...] = ("", "")
    count_by_resource_type = {
        resource_type: 0
        for resource_type in PROVIDER_DIRECTORY_ROOTED_GRAPH_OUTPUT_RESOURCES
    }
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
            key = (fields["resource_type"], fields["resource_id"])
            cursor = key
            materialized_pairs.append(
                _materialized_graph_pair(fields, identity, publication_run_id)
            )
            count_by_resource_type[key[0]] += 1
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
