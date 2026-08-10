# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable official-Practitioner cohort models for UHC Flex enrichment."""

from __future__ import annotations

import os

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy import UniqueConstraint
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


__all__ = (
    "ProviderDirectoryUHCFlexNPICohort",
    "ProviderDirectoryUHCFlexNPIMember",
)


_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-official-practitioner-"
    "npi-cohort.v1"
)
_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"


def _reference(table: str, column: str) -> str:
    return f"{_SCHEMA}.{table}.{column}"


def _table_args(*constraints):
    return (*constraints, {"schema": _SCHEMA, "extend_existing": True})


class ProviderDirectoryUHCFlexNPICohort(Base, JSONOutputMixin):
    """One sealed NPI set derived from a current official UHC dataset."""

    __tablename__ = "provider_directory_uhc_flex_npi_cohort"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("cohort_id", name="pd_uhc_flex_npi_cohort_pkey"),
        UniqueConstraint(
            "official_dataset_id",
            name="pd_uhc_flex_npi_cohort_dataset_key",
        ),
        ForeignKeyConstraint(
            ["official_source_id"],
            [_reference("provider_directory_source", "source_id")],
            name="pd_uhc_flex_npi_cohort_source_fkey",
        ),
        ForeignKeyConstraint(
            ["official_endpoint_id"],
            [_reference("provider_directory_api_endpoint", "endpoint_id")],
            name="pd_uhc_flex_npi_cohort_endpoint_fkey",
        ),
        ForeignKeyConstraint(
            ["official_dataset_id"],
            [_reference("provider_directory_endpoint_dataset", "dataset_id")],
            name="pd_uhc_flex_npi_cohort_dataset_fkey",
        ),
        CheckConstraint(
            "cohort_id ~ '^pdufc_[0-9a-f]{48}$' AND "
            f"contract_id = '{_CONTRACT}' AND "
            "authority_id = 'unitedhealthcare' AND "
            f"official_source_id = '{_SOURCE_ID}' AND "
            "official_acquisition_root_run_id <> '' AND "
            "official_dataset_hash ~ '^[0-9a-f]{64}$' AND "
            "official_content_proof_sha256 ~ '^[0-9a-f]{64}$' AND "
            "resource_type = 'Practitioner' AND "
            "practitioner_resource_count > 0 AND npi_count > 0 AND "
            "npi_count <= practitioner_resource_count AND "
            "cohort_complete IS TRUE AND "
            "endpoint_collection_complete IS FALSE AND "
            "endpoint_complete IS FALSE",
            name="pd_uhc_flex_npi_cohort_identity_check",
        ),
    )
    __my_index_elements__ = ["cohort_id"]

    cohort_id = Column(String(54), nullable=False)
    contract_id = Column(String(96), nullable=False)
    authority_id = Column(String(64), nullable=False)
    official_source_id = Column(String(64), nullable=False)
    official_endpoint_id = Column(String(64), nullable=False)
    official_dataset_id = Column(String(96), nullable=False)
    official_acquisition_root_run_id = Column(String(64), nullable=False)
    official_dataset_hash = Column(String(64), nullable=False)
    official_content_proof_sha256 = Column(String(64), nullable=False)
    resource_type = Column(String(64), nullable=False)
    practitioner_resource_count = Column(BigInteger, nullable=False)
    npi_count = Column(BigInteger, nullable=False)
    cohort_complete = Column(Boolean, nullable=False)
    endpoint_collection_complete = Column(Boolean, nullable=False)
    endpoint_complete = Column(Boolean, nullable=False)
    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )


class ProviderDirectoryUHCFlexNPIMember(Base, JSONOutputMixin):
    """One validated NPI in a child-first, header-sealed cohort."""

    __tablename__ = "provider_directory_uhc_flex_npi_member"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "cohort_id",
            "npi",
            name="pd_uhc_flex_npi_member_pkey",
        ),
        ForeignKeyConstraint(
            ["cohort_id"],
            [_reference("provider_directory_uhc_flex_npi_cohort", "cohort_id")],
            name="pd_uhc_flex_npi_member_cohort_fkey",
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "npi BETWEEN 1000000000 AND 2999999999",
            name="pd_uhc_flex_npi_member_npi_check",
        ),
    )
    __my_index_elements__ = ["cohort_id", "npi"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("npi", "cohort_id"),
            "name": "pd_uhc_flex_npi_member_npi_idx",
        }
    ]

    cohort_id = Column(String(54), nullable=False)
    npi = Column(BigInteger, nullable=False)
    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )
