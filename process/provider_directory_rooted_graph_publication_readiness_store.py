# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read only database-backed rooted publication readiness."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Mapping

from db.connection import db
from process.provider_directory_rooted_graph_publication_contract import (
    ProviderDirectoryRootedGraphDatasetReadiness,
    ProviderDirectoryRootedGraphPublicationError,
)


def _schema() -> str:
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


def _projection_text(value: object) -> str:
    if hasattr(value, "isoformat"):
        value = value.isoformat()
    if type(value) is not str:
        raise ProviderDirectoryRootedGraphPublicationError("state")
    return value


def _readiness_from_row(
    database_row: Any,
) -> ProviderDirectoryRootedGraphDatasetReadiness:
    fields = _row_fields(database_row)
    resource_counts = fields.get("resource_counts")
    if type(resource_counts) is str:
        try:
            resource_counts = json.loads(resource_counts)
        except ValueError:
            raise ProviderDirectoryRootedGraphPublicationError("state") from None
    try:
        return ProviderDirectoryRootedGraphDatasetReadiness(
            dataset_id=fields.get("dataset_id"),
            previous_dataset_id=fields.get("previous_dataset_id"),
            admission_id=fields.get("admission_id"),
            publication_acquisition_id=fields.get("publication_acquisition_id"),
            acquisition_root_run_id=fields.get("acquisition_root_run_id"),
            source_id=fields.get("source_id"),
            endpoint_id=fields.get("endpoint_id"),
            source_authority_id=fields.get("source_authority_id"),
            root_dataset_variant=fields.get("root_dataset_variant"),
            root_publication_contract_id=fields.get("root_publication_contract_id"),
            root_dataset_id=fields.get("root_dataset_id"),
            root_dataset_hash=fields.get("root_dataset_hash"),
            root_content_proof_sha256=fields.get("root_content_proof_sha256"),
            root_cohort_id=fields.get("root_cohort_id"),
            practitioner_resource_count=fields.get("practitioner_resource_count"),
            semantic_projection_as_of=_projection_text(
                fields.get("semantic_projection_as_of")
            ),
            operation_key=fields.get("operation_key"),
            dataset_hash=fields.get("dataset_hash"),
            resource_count=fields.get("resource_count"),
            resource_counts=resource_counts,
            publication_kind=fields.get("publication_kind"),
            cohort_complete=fields.get("cohort_complete"),
            rooted_graph_complete=fields.get("rooted_graph_complete"),
            endpoint_collection_complete=fields.get("endpoint_collection_complete"),
            endpoint_complete=fields.get("endpoint_complete"),
        )
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphPublicationError("state") from None


def _readiness_select(filter_sql: str) -> str:
    counts = (
        "jsonb_build_object("
        + ",".join(
            (
                "'InsurancePlan', header.insurance_plan_resource_count",
                "'PractitionerRole', header.practitioner_role_resource_count",
                "'Practitioner', header.practitioner_resource_count",
                "'Organization', header.organization_resource_count",
                "'Location', header.location_resource_count",
                "'HealthcareService', header.healthcare_service_resource_count",
                "'OrganizationAffiliation', "
                "header.organization_affiliation_resource_count",
                "'Endpoint', header.endpoint_resource_count",
            )
        )
        + ")"
    )
    return f"""
        SELECT header.dataset_id, header.previous_dataset_id,
               header.admission_id, header.publication_acquisition_id,
               header.acquisition_root_run_id,
               header.source_id, header.endpoint_id,
               header.source_authority_id, header.root_dataset_variant,
               header.root_publication_contract_id, header.root_dataset_id,
               header.root_dataset_hash, header.root_content_proof_sha256,
               header.root_cohort_id, header.practitioner_resource_count,
               header.semantic_projection_as_of, header.operation_key,
               header.dataset_hash, header.resource_count,
               {counts} AS resource_counts, header.publication_kind,
               header.cohort_complete, header.rooted_graph_complete,
               header.endpoint_collection_complete, header.endpoint_complete
          FROM {_table('provider_directory_rooted_graph_dataset')} AS header
         WHERE {filter_sql}
           AND header.status = 'published'
           AND header.is_current IS TRUE
           AND {_table('provider_directory_rooted_graph_dataset_ready')}(
                   header.dataset_id);
    """


async def load_dataset_readiness(
    dataset_id: str,
    *,
    database: Any = db,
) -> ProviderDirectoryRootedGraphDatasetReadiness | None:
    """Load one exact dataset only when every relation proof is ready."""

    row = await database.first(
        _readiness_select("header.dataset_id = :dataset_id"),
        dataset_id=dataset_id,
    )
    return None if row is None else _readiness_from_row(row)


async def load_replay_readiness(
    database: Any,
    publication_acquisition_id: str,
) -> ProviderDirectoryRootedGraphDatasetReadiness | None:
    """Load a current rooted publication by its acquisition replay key."""

    row = await database.first(
        _readiness_select(
            "header.publication_acquisition_id = :publication_acquisition_id"
        ),
        publication_acquisition_id=publication_acquisition_id,
    )
    return None if row is None else _readiness_from_row(row)


__all__ = ("load_dataset_readiness", "load_replay_readiness")
