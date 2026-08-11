# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant source-neutral rooted Provider Directory graph storage."""

from __future__ import annotations

import os

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import TIMESTAMP
from sqlalchemy import UniqueConstraint
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()
_ACQUISITION = "provider_directory_rooted_graph_acquisition"
_WORK = "provider_directory_rooted_graph_work"
_RESOURCE = "provider_directory_rooted_graph_resource"
_EDGE = "provider_directory_rooted_graph_edge"
_ENDPOINT = "provider_directory_api_endpoint"
_SOURCE = "provider_directory_source"
_DATASET = "provider_directory_endpoint_dataset"
_STORAGE_CONTRACT = "healthporta.provider-directory.rooted-graph-acquisition.v1"
_CONNECTOR_ID = "pdrgc_66b9a3c04ecb2368db3a6cbc33de3e8d9203b4e0002cc80a"
_GRAPH_CONTRACT_SHA256 = (
    "66b9a3c04ecb2368db3a6cbc33de3e8d9203b4e0002cc80a6147a09ba2f61351"
)
_QUERY_CONTRACT_SHA256 = (
    "4b93928781ea6a3d821a1ac21bd4d7f533ee5ada25184a540d31dfcbdfb2ea28"
)
_ROOTED_ENDPOINT_SIGNATURE = (
    "ec925b980d5f937abd5ca144a2041dda0c2b224fbe3fa8b70ccbe088f2222140"
)
_SOURCE_AUTHORITY = "unitedhealthcare"


def _reference(table_name: str, column_name: str) -> str:
    return f"{_SCHEMA}.{table_name}.{column_name}"


def _table_args(*constraints):
    return (*constraints, {"schema": _SCHEMA, "extend_existing": True})


def _timestamp_column():
    return Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class ProviderDirectoryRootedGraphAcquisition(Base, JSONOutputMixin):
    """One independently resumable baseline or candidate graph census."""

    __tablename__ = _ACQUISITION
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "acquisition_id",
            name="provider_directory_rooted_graph_acquisition_pkey",
        ),
        UniqueConstraint(
            "acquisition_id",
            "scope_id",
            name="provider_directory_rooted_graph_acquisition_scope_key",
        ),
        UniqueConstraint(
            "scope_id",
            "dataset_intent_id",
            "acquisition_role",
            name="provider_directory_rooted_graph_intent_role_key",
        ),
        UniqueConstraint(
            "run_id",
            name="provider_directory_rooted_graph_run_key",
        ),
        ForeignKeyConstraint(
            ["root_source_id"],
            [_reference(_SOURCE, "source_id")],
            name="provider_directory_rooted_graph_root_source_fkey",
        ),
        ForeignKeyConstraint(
            ["root_endpoint_id"],
            [_reference(_ENDPOINT, "endpoint_id")],
            name="provider_directory_rooted_graph_root_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["acquisition_source_id"],
            [_reference(_SOURCE, "source_id")],
            name="provider_directory_rooted_graph_acquisition_source_fkey",
        ),
        ForeignKeyConstraint(
            ["acquisition_endpoint_id"],
            [_reference(_ENDPOINT, "endpoint_id")],
            name="provider_directory_rooted_graph_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["root_dataset_id"],
            [_reference(_DATASET, "dataset_id")],
            name="provider_directory_rooted_graph_dataset_fkey",
        ),
        CheckConstraint(
            "acquisition_id ~ '^pdrga_[0-9a-f]{48}$' AND "
            "scope_id ~ '^pdrgs_[0-9a-f]{48}$' AND "
            f"storage_contract_id = '{_STORAGE_CONTRACT}' AND "
            f"connector_id = '{_CONNECTOR_ID}' AND "
            "acquisition_endpoint_id ~ '^[0-9a-f]{64}$' AND "
            "root_endpoint_id ~ '^[0-9a-f]{64}$' AND "
            f"endpoint_signature_sha256 = '{_ROOTED_ENDPOINT_SIGNATURE}' AND "
            f"source_authority_id = '{_SOURCE_AUTHORITY}' AND "
            "root_dataset_hash ~ '^[0-9a-f]{64}$' AND "
            "root_content_proof_sha256 ~ '^[0-9a-f]{64}$' AND "
            f"graph_contract_sha256 = '{_GRAPH_CONTRACT_SHA256}' AND "
            f"query_contract_sha256 = '{_QUERY_CONTRACT_SHA256}' AND "
            "root_resource_type = 'Practitioner' AND root_resource_count > 0 "
            "AND ((root_dataset_variant = 'uhc_flex_practitioner' "
            "AND root_publication_contract_id = "
            "'healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1' "
            "AND root_source_id <> acquisition_source_id "
            "AND root_endpoint_id <> acquisition_endpoint_id) OR "
            "(root_dataset_variant = 'rooted_combined' "
            "AND root_publication_contract_id = "
            "'healthporta.provider-directory.rooted-graph-publication.v1' "
            "AND root_source_id = acquisition_source_id "
            "AND root_endpoint_id = acquisition_endpoint_id)) "
            "AND max_work_items > root_resource_count "
            "AND max_work_items BETWEEN 1 AND 16500000 "
            "AND max_resource_rows BETWEEN 1 AND 25000000 "
            "AND max_edge_rows BETWEEN 1 AND 100000000 "
            "AND max_payload_bytes BETWEEN 1 AND 274877906944 "
            "AND used_work_items BETWEEN 0 AND max_work_items "
            "AND used_resource_rows BETWEEN 0 AND max_resource_rows "
            "AND used_edge_rows BETWEEN 0 AND max_edge_rows "
            "AND used_payload_bytes BETWEEN 0 AND max_payload_bytes "
            "AND acquisition_role IN ('baseline', 'candidate') AND "
            "run_id ~ '^pdrgr_[0-9a-f]{48}$' AND "
            "dataset_intent_id ~ '^pdrgi_[0-9a-f]{48}$' AND "
            "endpoint_collection_complete IS FALSE AND endpoint_complete IS FALSE",
            name="provider_directory_rooted_graph_acquisition_identity_check",
        ),
        CheckConstraint(
            "(status = 'building' AND rooted_graph_complete IS FALSE AND "
            "pending_count IS NULL AND leased_count IS NULL AND "
            "completed_count IS NULL AND error_count IS NULL AND "
            "resource_count IS NULL AND edge_count IS NULL AND "
            "insurance_plan_count IS NULL AND insurance_plan_page_count IS NULL "
            "AND terminal_set_sha256 IS NULL AND resource_set_sha256 IS NULL "
            "AND edge_set_sha256 IS NULL AND rooted_graph_sha256 IS NULL "
            "AND sealed_at IS NULL) OR "
            "(status = 'sealed' AND rooted_graph_complete IS TRUE AND "
            "pending_count = 0 AND leased_count = 0 AND completed_count > 0 "
            "AND error_count = 0 AND resource_count >= 0 AND edge_count >= 0 "
            "AND insurance_plan_count >= 0 AND insurance_plan_page_count > 0 "
            "AND terminal_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND resource_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND edge_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND rooted_graph_sha256 ~ '^[0-9a-f]{64}$' "
            "AND sealed_at IS NOT NULL)",
            name="provider_directory_rooted_graph_acquisition_state_check",
        ),
    )
    __my_index_elements__ = ["acquisition_id"]

    acquisition_id = Column(String(54), nullable=False)
    storage_contract_id = Column(String(96), nullable=False)
    scope_id = Column(String(54), nullable=False)
    root_source_id = Column(String(64), nullable=False)
    root_endpoint_id = Column(String(64), nullable=False)
    acquisition_source_id = Column(String(64), nullable=False)
    acquisition_endpoint_id = Column(String(64), nullable=False)
    source_authority_id = Column(String(64), nullable=False)
    root_dataset_variant = Column(String(32), nullable=False)
    root_publication_contract_id = Column(String(96), nullable=False)
    endpoint_signature_sha256 = Column(String(64), nullable=False)
    root_dataset_id = Column(String(96), nullable=False)
    root_dataset_hash = Column(String(64), nullable=False)
    root_content_proof_sha256 = Column(String(64), nullable=False)
    root_cohort_id = Column(String(128), nullable=False)
    root_resource_type = Column(String(64), nullable=False)
    root_resource_count = Column(BigInteger, nullable=False)
    connector_id = Column(String(54), nullable=False)
    graph_contract_sha256 = Column(String(64), nullable=False)
    query_contract_sha256 = Column(String(64), nullable=False)
    acquisition_role = Column(String(16), nullable=False)
    run_id = Column(String(54), nullable=False)
    dataset_intent_id = Column(String(54), nullable=False)
    max_work_items = Column(BigInteger, nullable=False)
    max_resource_rows = Column(BigInteger, nullable=False)
    max_edge_rows = Column(BigInteger, nullable=False)
    max_payload_bytes = Column(BigInteger, nullable=False)
    used_work_items = Column(BigInteger, nullable=False, server_default=text("0"))
    used_resource_rows = Column(BigInteger, nullable=False, server_default=text("0"))
    used_edge_rows = Column(BigInteger, nullable=False, server_default=text("0"))
    used_payload_bytes = Column(BigInteger, nullable=False, server_default=text("0"))
    status = Column(String(16), nullable=False)
    rooted_graph_complete = Column(Boolean, nullable=False)
    endpoint_collection_complete = Column(Boolean, nullable=False)
    endpoint_complete = Column(Boolean, nullable=False)
    pending_count = Column(BigInteger)
    leased_count = Column(BigInteger)
    completed_count = Column(BigInteger)
    error_count = Column(BigInteger)
    resource_count = Column(BigInteger)
    edge_count = Column(BigInteger)
    insurance_plan_count = Column(BigInteger)
    insurance_plan_page_count = Column(Integer)
    terminal_set_sha256 = Column(String(64))
    resource_set_sha256 = Column(String(64))
    edge_set_sha256 = Column(String(64))
    rooted_graph_sha256 = Column(String(64))
    created_at = _timestamp_column()
    updated_at = _timestamp_column()
    sealed_at = Column(TIMESTAMP(timezone=True))


class ProviderDirectoryRootedGraphWork(Base, JSONOutputMixin):
    """One fenced initial or witness-discovered query generation."""

    __tablename__ = _WORK
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "acquisition_id",
            "query_id",
            name="provider_directory_rooted_graph_work_pkey",
        ),
        UniqueConstraint(
            "acquisition_id",
            "scope_id",
            "query_id",
            name="provider_directory_rooted_graph_work_scope_key",
        ),
        ForeignKeyConstraint(
            ["acquisition_id", "scope_id"],
            [
                _reference(_ACQUISITION, "acquisition_id"),
                _reference(_ACQUISITION, "scope_id"),
            ],
            name="provider_directory_rooted_graph_work_acquisition_fkey",
        ),
        CheckConstraint(
            "query_id ~ '^pdrgq_[0-9a-f]{48}$' AND "
            "query_identity_sha256 ~ '^[0-9a-f]{64}$' AND "
            "octet_length(query_identity_json_text) BETWEEN 2 AND 8192 AND "
            "kind IN ('exact_reference_search', 'direct_read', "
            "'full_insurance_plan_census') AND closure_scope IN "
            "('root', 'plan', 'census') AND attempt_count >= 0 AND "
            "(lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$')",
            name="provider_directory_rooted_graph_work_value_check",
        ),
        CheckConstraint(
            "(kind = 'exact_reference_search' AND ((resource_type = "
            "'PractitionerRole' AND search_parameter = 'practitioner' AND "
            "reference_type = 'Practitioner' AND closure_scope = 'root' AND "
            "discovered_by_query_id IS NULL AND discovered_source_type IS NULL "
            "AND discovered_source_id IS NULL AND discovered_edge_sha256 IS NULL) "
            "OR (resource_type = 'OrganizationAffiliation' AND "
            "search_parameter = 'participating-organization' AND "
            "reference_type = 'Organization' AND closure_scope IN ('root', 'plan') "
            "AND discovered_by_query_id IS NOT NULL AND "
            "discovered_source_type = 'Organization' AND "
            "discovered_source_id = reference_id AND "
            "discovered_edge_sha256 IS NULL))) OR "
            "(kind = 'direct_read' AND search_parameter IS NULL AND "
            "resource_type IN ('Organization', 'Location', 'HealthcareService', "
            "'Endpoint') AND reference_type = resource_type AND closure_scope IN "
            "('root', 'plan') AND discovered_by_query_id IS NOT NULL AND "
            "discovered_source_type IS NOT NULL AND discovered_source_id IS NOT NULL "
            "AND discovered_edge_sha256 ~ '^[0-9a-f]{64}$') OR "
            "(kind = 'full_insurance_plan_census' AND resource_type = "
            "'InsurancePlan' AND search_parameter IS NULL AND reference_type IS NULL "
            "AND reference_id IS NULL AND closure_scope = 'census' AND "
            "discovered_by_query_id IS NULL AND discovered_source_type IS NULL "
            "AND discovered_source_id IS NULL AND discovered_edge_sha256 IS NULL)",
            name="provider_directory_rooted_graph_work_shape_check",
        ),
        CheckConstraint(
            "(status = 'pending' AND lease_token IS NULL AND "
            "lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "result_sha256 IS NULL AND resource_count IS NULL AND edge_count IS NULL "
            "AND resource_set_sha256 IS NULL AND edge_set_sha256 IS NULL AND "
            "advertised_total IS NULL AND terminal_page_count IS NULL AND "
            "pagination_terminal IS FALSE AND missing_http_status IS NULL AND "
            "missing_response_sha256 IS NULL AND missing_response_bytes IS NULL AND "
            "missing_response_json_text IS NULL AND "
            "error_code IS NULL AND "
            "terminal_record_sha256 IS NULL AND terminal_at IS NULL) OR "
            "(status = 'leased' AND attempt_count > 0 AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL AND lease_heartbeat_at IS NOT NULL "
            "AND result_sha256 IS NULL AND resource_count IS NULL AND edge_count IS NULL "
            "AND resource_set_sha256 IS NULL AND edge_set_sha256 IS NULL AND "
            "advertised_total IS NULL AND terminal_page_count IS NULL AND "
            "pagination_terminal IS FALSE AND missing_http_status IS NULL AND "
            "missing_response_sha256 IS NULL AND missing_response_bytes IS NULL AND "
            "missing_response_json_text IS NULL AND "
            "error_code IS NULL AND "
            "terminal_record_sha256 IS NULL AND terminal_at IS NULL) OR "
            "(status = 'completed' AND attempt_count > 0 AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "result_sha256 ~ '^[0-9a-f]{64}$' AND resource_count >= 0 AND "
            "edge_count >= 0 AND resource_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "edge_set_sha256 ~ '^[0-9a-f]{64}$' AND terminal_page_count > 0 "
            "AND pagination_terminal IS TRUE AND error_code IS NULL AND "
            "terminal_record_sha256 ~ '^[0-9a-f]{64}$' AND terminal_at IS NOT NULL "
            "AND ((kind = 'full_insurance_plan_census' AND "
            "advertised_total = resource_count) OR "
            "(kind = 'exact_reference_search' AND (advertised_total IS NULL OR "
            "advertised_total = resource_count)) OR "
            "(kind = 'direct_read' AND advertised_total IS NULL)) AND "
            "((missing_http_status IS NULL AND missing_response_sha256 IS NULL "
            "AND missing_response_bytes IS NULL AND missing_response_json_text IS NULL "
            "AND (kind <> 'direct_read' OR "
            "(terminal_page_count = 1 AND resource_count = 1))) OR "
            "(kind = 'direct_read' AND terminal_page_count = 1 AND "
            "missing_http_status IN (404, 410) AND "
            "missing_response_sha256 ~ '^[0-9a-f]{64}$' AND "
            "missing_response_bytes BETWEEN 1 AND 65536 AND "
            "octet_length(missing_response_json_text) = missing_response_bytes AND "
            "resource_count = 0 AND edge_count = 0))) OR "
            "(status = 'error' AND attempt_count > 0 AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "result_sha256 IS NULL AND resource_count = 0 AND edge_count = 0 "
            "AND resource_set_sha256 IS NULL AND edge_set_sha256 IS NULL AND "
            "advertised_total IS NULL AND terminal_page_count = 0 AND "
            "pagination_terminal IS FALSE AND missing_http_status IS NULL AND "
            "missing_response_sha256 IS NULL AND missing_response_bytes IS NULL AND "
            "missing_response_json_text IS NULL AND "
            "error_code ~ '^[a-z][a-z0-9_]{0,127}$' AND "
            "terminal_record_sha256 ~ '^[0-9a-f]{64}$' AND terminal_at IS NOT NULL)",
            name="provider_directory_rooted_graph_work_state_check",
        ),
    )
    __my_index_elements__ = ["acquisition_id", "query_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "acquisition_id",
                "status",
                "lease_expires_at",
                "query_id",
            ),
            "name": "provider_directory_rooted_graph_work_claim_idx",
        },
        {
            "index_elements": ("acquisition_id", "kind"),
            "name": "provider_directory_rooted_graph_plan_census_key",
            "unique": True,
            "where": "kind = 'full_insurance_plan_census'",
        },
    ]

    acquisition_id = Column(String(54), nullable=False)
    scope_id = Column(String(54), nullable=False)
    query_id = Column(String(54), nullable=False)
    query_identity_sha256 = Column(String(64), nullable=False)
    query_identity_json_text = Column(Text, nullable=False)
    kind = Column(String(32), nullable=False)
    resource_type = Column(String(64), nullable=False)
    search_parameter = Column(String(64))
    reference_type = Column(String(64))
    reference_id = Column(String(64))
    closure_scope = Column(String(16), nullable=False)
    discovered_by_query_id = Column(String(54))
    discovered_source_type = Column(String(64))
    discovered_source_id = Column(String(64))
    discovered_edge_sha256 = Column(String(64))
    status = Column(String(16), nullable=False)
    attempt_count = Column(Integer, nullable=False)
    lease_token = Column(String(64))
    lease_expires_at = Column(TIMESTAMP(timezone=True))
    lease_heartbeat_at = Column(TIMESTAMP(timezone=True))
    result_sha256 = Column(String(64))
    resource_count = Column(Integer)
    edge_count = Column(Integer)
    resource_set_sha256 = Column(String(64))
    edge_set_sha256 = Column(String(64))
    advertised_total = Column(BigInteger)
    terminal_page_count = Column(Integer)
    pagination_terminal = Column(Boolean, nullable=False)
    missing_http_status = Column(SmallInteger)
    missing_response_sha256 = Column(String(64))
    missing_response_bytes = Column(BigInteger)
    missing_response_json_text = Column(Text)
    error_code = Column(String(128))
    terminal_record_sha256 = Column(String(64))
    created_at = _timestamp_column()
    updated_at = _timestamp_column()
    terminal_at = Column(TIMESTAMP(timezone=True))


from db.models.provider_directory_rooted_graph_witness import (
    ProviderDirectoryRootedGraphEdge,
    ProviderDirectoryRootedGraphResource,
)


__all__ = (
    "ProviderDirectoryRootedGraphAcquisition",
    "ProviderDirectoryRootedGraphEdge",
    "ProviderDirectoryRootedGraphResource",
    "ProviderDirectoryRootedGraphWork",
)
