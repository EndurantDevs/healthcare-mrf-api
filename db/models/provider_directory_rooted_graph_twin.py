# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Role-neutral rooted graph comparison attempts and matched authority."""

from __future__ import annotations

import os

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column
from sqlalchemy import ForeignKeyConstraint, PrimaryKeyConstraint, String
from sqlalchemy import TIMESTAMP, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB

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
_ATTEMPT = "provider_directory_rooted_graph_twin_attempt"
_ADMISSION = "provider_directory_rooted_graph_twin_admission"
_STORAGE_CONTRACT = "healthporta.provider-directory.rooted-graph-acquisition.v1"


def _ref(table: str, column: str) -> str:
    return f"{_SCHEMA}.{table}.{column}"


class ProviderDirectoryRootedGraphTwinAttempt(Base, JSONOutputMixin):
    """One immutable comparison, including mismatches."""

    __tablename__ = _ATTEMPT
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("attempt_id", name="pd_rooted_graph_twin_attempt_pkey"),
        UniqueConstraint(
            "first_acquisition_id",
            "second_acquisition_id",
            name="pd_rooted_graph_twin_attempt_pair_key",
        ),
        ForeignKeyConstraint(
            ["first_acquisition_id"],
            [_ref(_ACQUISITION, "acquisition_id")],
            name="pd_rooted_graph_twin_attempt_first_fkey",
        ),
        ForeignKeyConstraint(
            ["second_acquisition_id"],
            [_ref(_ACQUISITION, "acquisition_id")],
            name="pd_rooted_graph_twin_attempt_second_fkey",
        ),
        CheckConstraint(
            "attempt_id ~ '^pdrgat_[0-9a-f]{48}$' AND "
            "attempt_contract_id = "
            "'healthporta.provider-directory.rooted-graph-twin-attempt.v1' AND "
            f"storage_contract_id = '{_STORAGE_CONTRACT}' AND "
            "first_acquisition_id < second_acquisition_id",
            name="pd_rooted_graph_twin_attempt_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["attempt_id"]

    attempt_id = Column(String(55), nullable=False)
    attempt_contract_id = Column(String(96), nullable=False)
    storage_contract_id = Column(String(96), nullable=False)
    first_acquisition_id = Column(String(54), nullable=False)
    second_acquisition_id = Column(String(54), nullable=False)
    dataset_intent_id = Column(String(54), nullable=False)
    scope_id = Column(String(54), nullable=False)
    root_source_id = Column(String(64), nullable=False)
    root_endpoint_id = Column(String(64), nullable=False)
    acquisition_source_id = Column(String(64), nullable=False)
    acquisition_endpoint_id = Column(String(64), nullable=False)
    source_authority_id = Column(String(64), nullable=False)
    endpoint_signature_sha256 = Column(String(64), nullable=False)
    root_dataset_id = Column(String(96), nullable=False)
    root_dataset_variant = Column(String(32), nullable=False)
    root_publication_contract_id = Column(String(96), nullable=False)
    root_dataset_hash = Column(String(64), nullable=False)
    root_content_proof_sha256 = Column(String(64), nullable=False)
    root_cohort_id = Column(String(128), nullable=False)
    root_resource_count = Column(BigInteger, nullable=False)
    connector_id = Column(String(64), nullable=False)
    graph_contract_sha256 = Column(String(64), nullable=False)
    query_contract_sha256 = Column(String(64), nullable=False)
    max_work_items = Column(BigInteger, nullable=False)
    max_resource_rows = Column(BigInteger, nullable=False)
    max_edge_rows = Column(BigInteger, nullable=False)
    max_payload_bytes = Column(BigInteger, nullable=False)
    first_pending_count = Column(BigInteger, nullable=False)
    second_pending_count = Column(BigInteger, nullable=False)
    first_leased_count = Column(BigInteger, nullable=False)
    second_leased_count = Column(BigInteger, nullable=False)
    first_completed_count = Column(BigInteger, nullable=False)
    second_completed_count = Column(BigInteger, nullable=False)
    first_error_count = Column(BigInteger, nullable=False)
    second_error_count = Column(BigInteger, nullable=False)
    first_resource_count = Column(BigInteger, nullable=False)
    second_resource_count = Column(BigInteger, nullable=False)
    first_edge_count = Column(BigInteger, nullable=False)
    second_edge_count = Column(BigInteger, nullable=False)
    first_insurance_plan_count = Column(BigInteger, nullable=False)
    second_insurance_plan_count = Column(BigInteger, nullable=False)
    first_insurance_plan_page_count = Column(BigInteger, nullable=False)
    second_insurance_plan_page_count = Column(BigInteger, nullable=False)
    first_used_work_items = Column(BigInteger, nullable=False)
    second_used_work_items = Column(BigInteger, nullable=False)
    first_used_resource_rows = Column(BigInteger, nullable=False)
    second_used_resource_rows = Column(BigInteger, nullable=False)
    first_used_edge_rows = Column(BigInteger, nullable=False)
    second_used_edge_rows = Column(BigInteger, nullable=False)
    first_used_payload_bytes = Column(BigInteger, nullable=False)
    second_used_payload_bytes = Column(BigInteger, nullable=False)
    first_terminal_set_sha256 = Column(String(64), nullable=False)
    second_terminal_set_sha256 = Column(String(64), nullable=False)
    first_resource_set_sha256 = Column(String(64), nullable=False)
    second_resource_set_sha256 = Column(String(64), nullable=False)
    first_edge_set_sha256 = Column(String(64), nullable=False)
    second_edge_set_sha256 = Column(String(64), nullable=False)
    first_rooted_graph_sha256 = Column(String(64), nullable=False)
    second_rooted_graph_sha256 = Column(String(64), nullable=False)
    matched = Column(Boolean, nullable=False)
    attempted_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class ProviderDirectoryRootedGraphTwinAdmission(Base, JSONOutputMixin):
    """One immutable authority for the candidate-role member of a match."""

    __tablename__ = _ADMISSION
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "admission_id", name="pd_rooted_graph_twin_admission_pkey"
        ),
        UniqueConstraint(
            "publication_acquisition_id",
            name="pd_rooted_graph_twin_admission_publication_key",
        ),
        ForeignKeyConstraint(
            ["attempt_id"],
            [_ref(_ATTEMPT, "attempt_id")],
            name="pd_rooted_graph_twin_admission_attempt_fkey",
        ),
        ForeignKeyConstraint(
            ["publication_acquisition_id"],
            [_ref(_ACQUISITION, "acquisition_id")],
            name="pd_rooted_graph_twin_admission_publication_fkey",
        ),
        ForeignKeyConstraint(
            ["comparison_acquisition_id"],
            [_ref(_ACQUISITION, "acquisition_id")],
            name="pd_rooted_graph_twin_admission_comparison_fkey",
        ),
        CheckConstraint(
            "admission_id ~ '^pdrgad_[0-9a-f]{48}$' AND "
            f"storage_contract_id = '{_STORAGE_CONTRACT}' AND "
            "((admission_contract_id = "
            "'healthporta.provider-directory.rooted-graph-matched-admission.v1' "
            "AND attempt_id IS NOT NULL AND comparison_acquisition_id IS NOT NULL "
            "AND reviewed_root_policy_json IS NULL "
            "AND acquisition_operation_key IS NULL) OR "
            "(admission_contract_id = "
            "'healthporta.provider-directory.rooted-graph-single-root-admission.v1' "
            "AND attempt_id IS NULL AND comparison_acquisition_id IS NULL "
            "AND reviewed_root_policy_json = CAST("
            "'{\"policy_version\":\"provider-directory-reviewed-root-policy-v1\","
            "\"required_root_count\"\\:1}' AS jsonb) "
            "AND acquisition_operation_key ~ '^[0-9a-f]{64}$')) "
            "AND publication_authority IS TRUE",
            name="pd_rooted_graph_twin_admission_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["admission_id"]

    admission_id = Column(String(55), nullable=False)
    admission_contract_id = Column(String(96), nullable=False)
    storage_contract_id = Column(String(96), nullable=False)
    attempt_id = Column(String(55))
    publication_acquisition_id = Column(String(54), nullable=False)
    comparison_acquisition_id = Column(String(54))
    reviewed_root_policy_json = Column(JSONB)
    acquisition_operation_key = Column(String(64))
    publication_run_id = Column(String(54), nullable=False)
    dataset_intent_id = Column(String(54), nullable=False)
    scope_id = Column(String(54), nullable=False)
    root_source_id = Column(String(64), nullable=False)
    root_endpoint_id = Column(String(64), nullable=False)
    acquisition_source_id = Column(String(64), nullable=False)
    acquisition_endpoint_id = Column(String(64), nullable=False)
    source_authority_id = Column(String(64), nullable=False)
    endpoint_signature_sha256 = Column(String(64), nullable=False)
    root_dataset_id = Column(String(96), nullable=False)
    root_dataset_variant = Column(String(32), nullable=False)
    root_publication_contract_id = Column(String(96), nullable=False)
    root_dataset_hash = Column(String(64), nullable=False)
    root_content_proof_sha256 = Column(String(64), nullable=False)
    root_cohort_id = Column(String(128), nullable=False)
    root_resource_count = Column(BigInteger, nullable=False)
    connector_id = Column(String(64), nullable=False)
    graph_contract_sha256 = Column(String(64), nullable=False)
    query_contract_sha256 = Column(String(64), nullable=False)
    max_work_items = Column(BigInteger, nullable=False)
    max_resource_rows = Column(BigInteger, nullable=False)
    max_edge_rows = Column(BigInteger, nullable=False)
    max_payload_bytes = Column(BigInteger, nullable=False)
    completed_count = Column(BigInteger, nullable=False)
    resource_count = Column(BigInteger, nullable=False)
    edge_count = Column(BigInteger, nullable=False)
    insurance_plan_count = Column(BigInteger, nullable=False)
    insurance_plan_page_count = Column(BigInteger, nullable=False)
    used_work_items = Column(BigInteger, nullable=False)
    used_resource_rows = Column(BigInteger, nullable=False)
    used_edge_rows = Column(BigInteger, nullable=False)
    used_payload_bytes = Column(BigInteger, nullable=False)
    terminal_set_sha256 = Column(String(64), nullable=False)
    resource_set_sha256 = Column(String(64), nullable=False)
    edge_set_sha256 = Column(String(64), nullable=False)
    rooted_graph_sha256 = Column(String(64), nullable=False)
    publication_authority = Column(Boolean, nullable=False)
    admitted_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


__all__ = (
    "ProviderDirectoryRootedGraphTwinAdmission",
    "ProviderDirectoryRootedGraphTwinAttempt",
)
