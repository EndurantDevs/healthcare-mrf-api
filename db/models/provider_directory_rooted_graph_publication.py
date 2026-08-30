# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admitted rooted combined dataset header and resource provenance."""

from __future__ import annotations

import os

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, Date
from sqlalchemy import ForeignKeyConstraint, Integer, PrimaryKeyConstraint
from sqlalchemy import String, TIMESTAMP, UniqueConstraint, text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()
_HEADER = "provider_directory_rooted_graph_dataset"
_PROVENANCE = "provider_directory_rooted_graph_dataset_resource"
_PARENT = "provider_directory_endpoint_dataset"
_GENERIC_RESOURCE = "provider_directory_dataset_resource"
_ADMISSION = "provider_directory_rooted_graph_twin_admission"
_ACQUISITION = "provider_directory_rooted_graph_acquisition"
_SOURCE = "provider_directory_source"
_ENDPOINT = "provider_directory_api_endpoint"
_PUBLICATION_CONTRACT = "healthporta.provider-directory.rooted-graph-publication.v1"
_LEGACY_PUBLICATION_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1"
)
_ROOTED_SOURCE_ID = "pdfhir_2b088f28554b9e51505b455e"
_ROOTED_ENDPOINT_ID = "42d85e85d6214cf898aef33591756d0231d11f1ef250d8c404c804cda8f36161"
_ROOTED_ENDPOINT_SIGNATURE = (
    "ec925b980d5f937abd5ca144a2041dda0c2b224fbe3fa8b70ccbe088f2222140"
)
_LEGACY_SOURCE_ID = "pdfhir_1ceb7c0986c320b7eb924881"
_LEGACY_ENDPOINT_ID = "ad53a7446514ed65b3a8ea7ab68ceb9a1ef85bf6c04fcb882219ecb50928bab5"
_SOURCE_AUTHORITY = "unitedhealthcare"


def _ref(table: str, column: str) -> str:
    return f"{_SCHEMA}.{table}.{column}"


class ProviderDirectoryRootedGraphDataset(Base, JSONOutputMixin):
    """One exact Practitioner root plus the seven admitted closure families."""

    __tablename__ = _HEADER
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("dataset_id", name="pd_rooted_graph_dataset_pkey"),
        UniqueConstraint("admission_id", name="pd_rooted_graph_dataset_admission_key"),
        UniqueConstraint(
            "publication_acquisition_id",
            name="pd_rooted_graph_dataset_acquisition_key",
        ),
        UniqueConstraint(
            "acquisition_root_run_id",
            name="pd_rooted_graph_dataset_root_run_key",
        ),
        ForeignKeyConstraint(
            ["dataset_id"],
            [_ref(_PARENT, "dataset_id")],
            name="pd_rooted_graph_dataset_parent_fkey",
        ),
        ForeignKeyConstraint(
            ["admission_id"],
            [_ref(_ADMISSION, "admission_id")],
            name="pd_rooted_graph_dataset_admission_fkey",
        ),
        ForeignKeyConstraint(
            ["attempt_id"],
            [_ref("provider_directory_rooted_graph_twin_attempt", "attempt_id")],
            name="pd_rooted_graph_dataset_attempt_fkey",
        ),
        ForeignKeyConstraint(
            ["publication_acquisition_id"],
            [_ref(_ADMISSION, "publication_acquisition_id")],
            name="pd_rooted_graph_dataset_publication_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id"],
            [_ref(_SOURCE, "source_id")],
            name="pd_rooted_graph_dataset_source_fkey",
        ),
        ForeignKeyConstraint(
            ["endpoint_id"],
            [_ref(_ENDPOINT, "endpoint_id")],
            name="pd_rooted_graph_dataset_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["acquisition_source_id"],
            [_ref(_SOURCE, "source_id")],
            name="pd_rooted_graph_dataset_acquisition_source_fkey",
        ),
        ForeignKeyConstraint(
            ["acquisition_endpoint_id"],
            [_ref(_ENDPOINT, "endpoint_id")],
            name="pd_rooted_graph_dataset_acquisition_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["root_source_id"],
            [_ref(_SOURCE, "source_id")],
            name="pd_rooted_graph_dataset_root_source_fkey",
        ),
        ForeignKeyConstraint(
            ["root_endpoint_id"],
            [_ref(_ENDPOINT, "endpoint_id")],
            name="pd_rooted_graph_dataset_root_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["practitioner_origin_source_id"],
            [_ref(_SOURCE, "source_id")],
            name="pd_rooted_graph_dataset_origin_source_fkey",
        ),
        ForeignKeyConstraint(
            ["practitioner_origin_endpoint_id"],
            [_ref(_ENDPOINT, "endpoint_id")],
            name="pd_rooted_graph_dataset_origin_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["root_dataset_id"],
            [_ref(_PARENT, "dataset_id")],
            name="pd_rooted_graph_dataset_root_dataset_fkey",
        ),
        ForeignKeyConstraint(
            ["previous_dataset_id"],
            [_ref(_PARENT, "dataset_id")],
            name="pd_rooted_graph_dataset_previous_fkey",
        ),
        CheckConstraint(
            f"publication_contract_id = '{_PUBLICATION_CONTRACT}' AND "
            "publication_kind = 'rooted_combined' AND "
            "dataset_id ~ '^pdrgpd_[0-9a-f]{48}$' AND "
            "acquisition_root_run_id ~ '^pdrgpr_[0-9a-f]{48}$' AND "
            f"source_id = '{_ROOTED_SOURCE_ID}' AND "
            f"endpoint_id = '{_ROOTED_ENDPOINT_ID}' AND "
            "acquisition_source_id = source_id AND "
            "acquisition_endpoint_id = endpoint_id AND "
            f"source_authority_id = '{_SOURCE_AUTHORITY}' AND "
            f"endpoint_signature_sha256 = '{_ROOTED_ENDPOINT_SIGNATURE}' AND "
            f"practitioner_origin_source_id = '{_LEGACY_SOURCE_ID}' AND "
            f"practitioner_origin_endpoint_id = '{_LEGACY_ENDPOINT_ID}' AND "
            "((root_dataset_variant = 'uhc_flex_practitioner' AND "
            f"root_publication_contract_id = '{_LEGACY_PUBLICATION_CONTRACT}' AND "
            f"root_source_id = '{_LEGACY_SOURCE_ID}' AND "
            f"root_endpoint_id = '{_LEGACY_ENDPOINT_ID}') OR "
            "(root_dataset_variant = 'rooted_combined' AND "
            f"root_publication_contract_id = '{_PUBLICATION_CONTRACT}' AND "
            "root_source_id = source_id AND root_endpoint_id = endpoint_id)) AND "
            "root_dataset_hash ~ '^[0-9a-f]{64}$' AND "
            "root_content_proof_sha256 ~ '^[0-9a-f]{64}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "rooted_graph_sha256 ~ '^[0-9a-f]{64}$' AND "
            "resource_hash_contract = 'semantic_content_v3' AND "
            "cohort_complete IN (TRUE, FALSE) AND rooted_graph_complete IS TRUE AND "
            "endpoint_collection_complete IS FALSE AND endpoint_complete IS FALSE "
            "AND max_work_items > root_practitioner_resource_count "
            "AND max_work_items BETWEEN 1 AND 16500000 "
            "AND max_resource_rows BETWEEN 1 AND 25000000 "
            "AND max_edge_rows BETWEEN 1 AND 100000000 "
            "AND max_payload_bytes BETWEEN 1 AND 274877906944 "
            "AND used_work_items BETWEEN 1 AND max_work_items "
            "AND used_resource_rows BETWEEN 0 AND max_resource_rows "
            "AND used_edge_rows BETWEEN 0 AND max_edge_rows "
            "AND used_payload_bytes BETWEEN 0 AND max_payload_bytes "
            "AND completed_count = used_work_items "
            "AND graph_resource_count = used_resource_rows "
            "AND graph_edge_count = used_edge_rows "
            "AND root_practitioner_resource_count > 0 "
            "AND practitioner_resource_count = root_practitioner_resource_count "
            "AND practitioner_role_resource_count >= 0 "
            "AND organization_affiliation_resource_count >= 0 "
            "AND organization_resource_count >= 0 AND location_resource_count >= 0 "
            "AND healthcare_service_resource_count >= 0 "
            "AND insurance_plan_resource_count >= 0 "
            "AND endpoint_resource_count >= 0 AND "
            "resource_count = practitioner_resource_count + "
            "practitioner_role_resource_count + organization_affiliation_resource_count + "
            "organization_resource_count + location_resource_count + "
            "healthcare_service_resource_count + insurance_plan_resource_count + "
            "endpoint_resource_count AND "
            "((status = 'building' AND is_current IS FALSE AND dataset_hash IS NULL "
            "AND validated_at IS NULL AND published_at IS NULL AND superseded_at IS NULL) "
            "OR (status = 'validated' AND is_current IS FALSE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NULL AND superseded_at IS NULL) OR "
            "(status = 'published' AND is_current IS TRUE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NULL) OR "
            "(status = 'superseded' AND is_current IS FALSE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NOT NULL))",
            name="pd_rooted_graph_dataset_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["dataset_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("source_id",),
            "name": "pd_rooted_graph_dataset_current_idx",
            "unique": True,
            "where": "is_current = true",
        },
        {
            "index_elements": ("dataset_hash",),
            "name": "pd_rooted_graph_dataset_hash_idx",
        },
    ]

    dataset_id = Column(String(55), nullable=False)
    publication_contract_id = Column(String(96), nullable=False)
    publication_kind = Column(String(32), nullable=False)
    admission_id = Column(String(55), nullable=False)
    attempt_id = Column(String(55))
    publication_acquisition_id = Column(String(54), nullable=False)
    comparison_acquisition_id = Column(String(54))
    publication_run_id = Column(String(54), nullable=False)
    source_id = Column(String(64), nullable=False)
    endpoint_id = Column(String(64), nullable=False)
    acquisition_source_id = Column(String(64), nullable=False)
    acquisition_endpoint_id = Column(String(64), nullable=False)
    source_authority_id = Column(String(64), nullable=False)
    root_dataset_variant = Column(String(32), nullable=False)
    root_publication_contract_id = Column(String(96), nullable=False)
    root_source_id = Column(String(64), nullable=False)
    root_endpoint_id = Column(String(64), nullable=False)
    practitioner_origin_source_id = Column(String(64), nullable=False)
    practitioner_origin_endpoint_id = Column(String(64), nullable=False)
    endpoint_signature_sha256 = Column(String(64), nullable=False)
    scope_id = Column(String(54), nullable=False)
    dataset_intent_id = Column(String(54), nullable=False)
    acquisition_root_run_id = Column(String(55), nullable=False)
    semantic_projection_as_of = Column(Date, nullable=False)
    operation_key = Column(String(64), nullable=False)
    root_dataset_id = Column(String(96), nullable=False)
    root_dataset_hash = Column(String(64), nullable=False)
    root_content_proof_sha256 = Column(String(64), nullable=False)
    root_cohort_id = Column(String(128), nullable=False)
    root_practitioner_resource_count = Column(BigInteger, nullable=False)
    connector_id = Column(String(64), nullable=False)
    storage_contract_id = Column(String(96), nullable=False)
    graph_contract_sha256 = Column(String(64), nullable=False)
    query_contract_sha256 = Column(String(64), nullable=False)
    max_work_items = Column(BigInteger, nullable=False)
    max_resource_rows = Column(BigInteger, nullable=False)
    max_edge_rows = Column(BigInteger, nullable=False)
    max_payload_bytes = Column(BigInteger, nullable=False)
    used_work_items = Column(BigInteger, nullable=False)
    used_resource_rows = Column(BigInteger, nullable=False)
    used_edge_rows = Column(BigInteger, nullable=False)
    used_payload_bytes = Column(BigInteger, nullable=False)
    completed_count = Column(BigInteger, nullable=False)
    graph_resource_count = Column(BigInteger, nullable=False)
    graph_edge_count = Column(BigInteger, nullable=False)
    census_insurance_plan_count = Column(BigInteger, nullable=False)
    insurance_plan_page_count = Column(Integer, nullable=False)
    terminal_set_sha256 = Column(String(64), nullable=False)
    resource_set_sha256 = Column(String(64), nullable=False)
    edge_set_sha256 = Column(String(64), nullable=False)
    rooted_graph_sha256 = Column(String(64), nullable=False)
    previous_dataset_id = Column(String(96))
    dataset_hash = Column(String(64))
    resource_count = Column(BigInteger, nullable=False)
    practitioner_resource_count = Column(BigInteger, nullable=False)
    practitioner_role_resource_count = Column(BigInteger, nullable=False)
    organization_affiliation_resource_count = Column(BigInteger, nullable=False)
    organization_resource_count = Column(BigInteger, nullable=False)
    location_resource_count = Column(BigInteger, nullable=False)
    healthcare_service_resource_count = Column(BigInteger, nullable=False)
    insurance_plan_resource_count = Column(BigInteger, nullable=False)
    endpoint_resource_count = Column(BigInteger, nullable=False)
    resource_hash_contract = Column(String(32), nullable=False)
    cohort_complete = Column(Boolean, nullable=False)
    rooted_graph_complete = Column(Boolean, nullable=False)
    endpoint_collection_complete = Column(Boolean, nullable=False)
    endpoint_complete = Column(Boolean, nullable=False)
    status = Column(String(16), nullable=False)
    is_current = Column(Boolean, nullable=False, server_default=text("false"))
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )
    validated_at = Column(TIMESTAMP(timezone=True))
    published_at = Column(TIMESTAMP(timezone=True))
    superseded_at = Column(TIMESTAMP(timezone=True))


class ProviderDirectoryRootedGraphDatasetResource(Base, JSONOutputMixin):
    """Bind each canonical output key to exact root or acquisition evidence."""

    __tablename__ = _PROVENANCE
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "dataset_id",
            "resource_type",
            "resource_id",
            name="pd_rooted_graph_dataset_resource_pkey",
        ),
        ForeignKeyConstraint(
            ["dataset_id"],
            [_ref(_HEADER, "dataset_id")],
            name="pd_rooted_graph_dataset_resource_dataset_fkey",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "resource_type", "resource_id"],
            [
                _ref(_GENERIC_RESOURCE, "dataset_id"),
                _ref(_GENERIC_RESOURCE, "resource_type"),
                _ref(_GENERIC_RESOURCE, "resource_id"),
            ],
            name="pd_rooted_graph_dataset_resource_parent_fkey",
        ),
        ForeignKeyConstraint(
            ["publication_acquisition_id"],
            [_ref(_ACQUISITION, "acquisition_id")],
            name="pd_rooted_graph_dataset_resource_acquisition_fkey",
        ),
        CheckConstraint(
            "resource_type IN ('Practitioner','PractitionerRole',"
            "'OrganizationAffiliation','Organization','Location',"
            "'HealthcareService','InsurancePlan','Endpoint') AND "
            "resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "published_payload_hash ~ '^[0-9a-f]{64}$' AND "
            "((origin_kind = 'root_practitioner' AND resource_type = 'Practitioner' "
            "AND query_id IS NULL AND attempt IS NULL AND closure_scope IS NULL "
            "AND source_payload_sha256 IS NULL) OR "
            "(origin_kind = 'rooted_graph' AND resource_type <> 'Practitioner' "
            "AND query_id ~ '^pdrgq_[0-9a-f]{48}$' AND attempt > 0 "
            "AND closure_scope IN ('root','plan') AND "
            "source_payload_sha256 ~ '^[0-9a-f]{64}$'))",
            name="pd_rooted_graph_dataset_resource_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["dataset_id", "resource_type", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "publication_acquisition_id",
                "resource_type",
                "resource_id",
            ),
            "name": "pd_rooted_graph_dataset_resource_origin_idx",
        }
    ]

    dataset_id = Column(String(55), nullable=False)
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    origin_kind = Column(String(32), nullable=False)
    root_dataset_id = Column(String(96), nullable=False)
    publication_acquisition_id = Column(String(54), nullable=False)
    query_id = Column(String(54))
    attempt = Column(Integer)
    closure_scope = Column(String(16))
    source_payload_sha256 = Column(String(64))
    published_payload_hash = Column(String(64), nullable=False)


__all__ = (
    "ProviderDirectoryRootedGraphDataset",
    "ProviderDirectoryRootedGraphDatasetResource",
)
