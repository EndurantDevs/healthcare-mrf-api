# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact-cohort Flex Practitioner publication and row provenance."""

from __future__ import annotations

import os

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy import UniqueConstraint
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


__all__ = (
    "ProviderDirectoryUHCFlexPractitionerDataset",
    "ProviderDirectoryUHCFlexPractitionerDatasetResource",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()
_DATASET = "provider_directory_uhc_flex_practitioner_dataset"
_DATASET_RESOURCE = "provider_directory_uhc_flex_practitioner_dataset_resource"
_ENDPOINT_DATASET = "provider_directory_endpoint_dataset"
_GENERIC_RESOURCE = "provider_directory_dataset_resource"
_ADMISSION = "provider_directory_uhc_flex_practitioner_twin_admission"
_SOURCE = "provider_directory_source"
_ENDPOINT = "provider_directory_api_endpoint"
_PUBLICATION_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1"
)


def _reference(table_name: str, column_name: str) -> str:
    return f"{_SCHEMA}.{table_name}.{column_name}"


class ProviderDirectoryUHCFlexPractitionerDataset(Base, JSONOutputMixin):
    """One admitted semantic-v3 Practitioner cohort generation."""

    __tablename__ = _DATASET
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "dataset_id",
            name="pd_uhc_flex_practitioner_dataset_pkey",
        ),
        UniqueConstraint(
            "admission_id",
            name="pd_uhc_flex_practitioner_dataset_admission_key",
        ),
        UniqueConstraint(
            "candidate_acquisition_id",
            name="pd_uhc_flex_practitioner_dataset_candidate_key",
        ),
        UniqueConstraint(
            "acquisition_root_run_id",
            name="pd_uhc_flex_practitioner_dataset_root_key",
        ),
        ForeignKeyConstraint(
            ["dataset_id"],
            [_reference(_ENDPOINT_DATASET, "dataset_id")],
            name="pd_uhc_flex_practitioner_dataset_parent_fkey",
        ),
        ForeignKeyConstraint(
            ["admission_id"],
            [_reference(_ADMISSION, "admission_id")],
            name="pd_uhc_flex_practitioner_dataset_admission_fkey",
        ),
        ForeignKeyConstraint(
            ["candidate_acquisition_id"],
            [_reference(_ADMISSION, "candidate_acquisition_id")],
            name="pd_uhc_flex_practitioner_dataset_candidate_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference(_SOURCE, "source_id")],
            name="pd_uhc_flex_practitioner_dataset_source_fkey",
        ),
        ForeignKeyConstraint(
            ["endpoint_id"],
            [_reference(_ENDPOINT, "endpoint_id")],
            name="pd_uhc_flex_practitioner_dataset_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["previous_dataset_id"],
            [_reference(_ENDPOINT_DATASET, "dataset_id")],
            name="pd_uhc_flex_practitioner_dataset_previous_fkey",
        ),
        CheckConstraint(
            f"publication_contract_id = '{_PUBLICATION_CONTRACT}' AND "
            "dataset_id ~ '^pdufpd_[0-9a-f]{48}$' AND "
            "acquisition_root_run_id ~ '^pdufpar_[0-9a-f]{48}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "resource_hash_contract = 'semantic_content_v3' AND "
            "selected_resource_type = 'Practitioner' AND "
            "expected_resource_type = 'Practitioner' AND "
            "cohort_complete IS TRUE AND "
            "endpoint_collection_complete IS FALSE AND "
            "endpoint_complete IS FALSE AND resource_count >= 0 AND "
            "semantic_projection_as_of BETWEEN DATE '0001-01-01' "
            "AND DATE '9999-12-31' AND "
            "((status = 'building' AND is_current IS FALSE AND "
            "dataset_hash IS NULL AND validated_at IS NULL AND "
            "published_at IS NULL AND superseded_at IS NULL) OR "
            "(status = 'validated' AND is_current IS FALSE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NULL AND superseded_at IS NULL) OR "
            "(status = 'published' AND is_current IS TRUE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NULL) OR "
            "(status = 'superseded' AND is_current IS FALSE AND "
            "dataset_hash ~ '^[0-9a-f]{64}$' AND validated_at IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NOT NULL "
            "AND superseded_at >= published_at))",
            name="pd_uhc_flex_practitioner_dataset_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["dataset_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("source_id",),
            "name": "pd_uhc_flex_practitioner_dataset_current_idx",
            "unique": True,
            "where": "is_current = true",
        },
        {
            "index_elements": ("dataset_hash",),
            "name": "pd_uhc_flex_practitioner_dataset_hash_idx",
        },
    ]

    dataset_id = Column(String(55), nullable=False)
    publication_contract_id = Column(String(96), nullable=False)
    admission_id = Column(String(56), nullable=False)
    candidate_acquisition_id = Column(String(55), nullable=False)
    source_id = Column(String(64), nullable=False)
    endpoint_id = Column(String(64), nullable=False)
    cohort_id = Column(String(54), nullable=False)
    dataset_intent_id = Column(String(55), nullable=False)
    acquisition_root_run_id = Column(String(56), nullable=False)
    semantic_projection_as_of = Column(Date, nullable=False)
    operation_key = Column(String(64), nullable=False)
    source_authority_id = Column(String(64), nullable=False)
    terminal_set_sha256 = Column(String(64), nullable=False)
    previous_dataset_id = Column(String(55))
    dataset_hash = Column(String(64))
    resource_count = Column(BigInteger, nullable=False, server_default=text("0"))
    resource_hash_contract = Column(String(32), nullable=False)
    selected_resource_type = Column(String(64), nullable=False)
    expected_resource_type = Column(String(64), nullable=False)
    cohort_complete = Column(Boolean, nullable=False)
    endpoint_collection_complete = Column(Boolean, nullable=False)
    endpoint_complete = Column(Boolean, nullable=False)
    status = Column(String(16), nullable=False)
    is_current = Column(Boolean, nullable=False, server_default=text("false"))
    created_at = Column(
        TIMESTAMP(),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )
    validated_at = Column(TIMESTAMP())
    published_at = Column(TIMESTAMP())
    superseded_at = Column(TIMESTAMP())


class ProviderDirectoryFlexPractitionerDatasetResource(
    Base,
    JSONOutputMixin,
):
    """Bind one semantic row to the admitted raw candidate resource."""

    __tablename__ = _DATASET_RESOURCE
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "dataset_id",
            "resource_id",
            name="pd_uhc_flex_dataset_resource_pkey",
        ),
        ForeignKeyConstraint(
            ["dataset_id"],
            [_reference(_DATASET, "dataset_id")],
            name="pd_uhc_flex_dataset_resource_dataset_fkey",
        ),
        ForeignKeyConstraint(
            ["candidate_acquisition_id"],
            [_reference(_ADMISSION, "candidate_acquisition_id")],
            name="pd_uhc_flex_dataset_resource_candidate_fkey",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "resource_type", "resource_id"],
            [
                _reference(_GENERIC_RESOURCE, "dataset_id"),
                _reference(_GENERIC_RESOURCE, "resource_type"),
                _reference(_GENERIC_RESOURCE, "resource_id"),
            ],
            name="pd_uhc_flex_dataset_resource_parent_fkey",
        ),
        CheckConstraint(
            "resource_type = 'Practitioner' AND "
            "resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "requested_npi BETWEEN 1000000000 AND 2999999999 AND "
            "payload_hash ~ '^[0-9a-f]{64}$' AND "
            "acquired_resource_sha256 ~ '^[0-9a-f]{64}$'",
            name="pd_uhc_flex_dataset_resource_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["dataset_id", "resource_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("dataset_id", "requested_npi"),
            "name": "pd_uhc_flex_dataset_resource_npi_idx",
        },
    ]

    dataset_id = Column(String(55), nullable=False)
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    requested_npi = Column(BigInteger, nullable=False)
    candidate_acquisition_id = Column(String(55), nullable=False)
    payload_hash = Column(String(64), nullable=False)
    acquired_resource_sha256 = Column(String(64), nullable=False)


ProviderDirectoryUHCFlexPractitionerDatasetResource = (
    ProviderDirectoryFlexPractitionerDatasetResource
)
