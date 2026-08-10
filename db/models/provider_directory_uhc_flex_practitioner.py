# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant exact-cohort Flex Practitioner acquisition models."""

from __future__ import annotations

import os

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import TIMESTAMP
from sqlalchemy import UniqueConstraint
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


__all__ = (
    "ProviderDirectoryUHCFlexPractitionerAcquisition",
    "ProviderDirectoryUHCFlexPractitionerResource",
    "ProviderDirectoryUHCFlexPractitionerWork",
)


_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
_STORAGE_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-acquisition.v1"
)
_SOURCE_ID = "pdfhir_1ceb7c0986c320b7eb924881"
_CONNECTOR_ID = (
    "pdufpc_16ebdbf260dc9815ae38830a6991fea5d6533ab8db7389da"
)
_QUERY_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-exact-npi.v1"
)


def _reference(table: str, column: str) -> str:
    return f"{_SCHEMA}.{table}.{column}"


def _table_args(*constraints):
    return (*constraints, {"schema": _SCHEMA, "extend_existing": True})


class ProviderDirectoryUHCFlexPractitionerAcquisition(Base, JSONOutputMixin):
    """One immutable-role run over an exact official NPI cohort."""

    __tablename__ = "provider_directory_uhc_flex_practitioner_acquisition"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "acquisition_id",
            name="pd_uhc_flex_practitioner_acquisition_pkey",
        ),
        UniqueConstraint(
            "acquisition_id",
            "cohort_id",
            name="pd_uhc_flex_practitioner_acquisition_cohort_key",
        ),
        UniqueConstraint(
            "cohort_id",
            "dataset_intent_id",
            "acquisition_role",
            name="pd_uhc_flex_practitioner_intent_role_key",
        ),
        UniqueConstraint(
            "run_id",
            name="pd_uhc_flex_practitioner_run_key",
        ),
        ForeignKeyConstraint(
            ["cohort_id"],
            [_reference("provider_directory_uhc_flex_npi_cohort", "cohort_id")],
            name="pd_uhc_flex_practitioner_acquisition_cohort_fkey",
        ),
        CheckConstraint(
            "acquisition_id ~ '^pdufpa_[0-9a-f]{48}$' AND "
            f"storage_contract_id = '{_STORAGE_CONTRACT}' AND "
            "acquisition_role IN ('baseline', 'candidate') AND "
            f"source_id = '{_SOURCE_ID}' AND connector_id = '{_CONNECTOR_ID}' "
            f"AND query_contract_id = '{_QUERY_CONTRACT}' AND "
            "run_id ~ '^pdufpr_[0-9a-f]{48}$' AND "
            "dataset_intent_id ~ '^pdufdi_[0-9a-f]{48}$' AND "
            "expected_npi_count > 0 AND endpoint_collection_complete IS FALSE "
            "AND endpoint_complete IS FALSE",
            name="pd_uhc_flex_practitioner_acquisition_identity_check",
        ),
        CheckConstraint(
            "(status = 'building' AND cohort_complete IS FALSE AND "
            "pending_count IS NULL AND leased_count IS NULL AND "
            "matched_count IS NULL AND unmatched_count IS NULL AND "
            "error_count IS NULL AND resource_count IS NULL AND "
            "terminal_set_sha256 IS NULL AND sealed_at IS NULL) OR "
            "(status = 'sealed' AND cohort_complete IS TRUE AND "
            "pending_count = 0 AND leased_count = 0 AND matched_count >= 0 "
            "AND unmatched_count >= 0 AND error_count = 0 AND "
            "matched_count + unmatched_count = "
            "expected_npi_count AND resource_count >= 0 AND "
            "terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND sealed_at IS NOT NULL)",
            name="pd_uhc_flex_practitioner_acquisition_state_check",
        ),
    )
    __my_index_elements__ = ["acquisition_id"]

    acquisition_id = Column(String(55), nullable=False)
    storage_contract_id = Column(String(96), nullable=False)
    cohort_id = Column(String(54), nullable=False)
    acquisition_role = Column(String(16), nullable=False)
    source_id = Column(String(64), nullable=False)
    connector_id = Column(String(64), nullable=False)
    query_contract_id = Column(String(96), nullable=False)
    run_id = Column(String(55), nullable=False)
    dataset_intent_id = Column(String(55), nullable=False)
    expected_npi_count = Column(BigInteger, nullable=False)
    status = Column(String(16), nullable=False)
    cohort_complete = Column(Boolean, nullable=False)
    endpoint_collection_complete = Column(Boolean, nullable=False)
    endpoint_complete = Column(Boolean, nullable=False)
    pending_count = Column(BigInteger)
    leased_count = Column(BigInteger)
    matched_count = Column(BigInteger)
    unmatched_count = Column(BigInteger)
    error_count = Column(BigInteger)
    resource_count = Column(BigInteger)
    terminal_set_sha256 = Column(String(64))
    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )
    updated_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )
    sealed_at = Column(TIMESTAMP(timezone=True))


class ProviderDirectoryUHCFlexPractitionerWork(Base, JSONOutputMixin):
    """One fenced pending, leased, or terminal exact-NPI query."""

    __tablename__ = "provider_directory_uhc_flex_practitioner_work"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "acquisition_id",
            "npi",
            name="pd_uhc_flex_practitioner_work_pkey",
        ),
        UniqueConstraint(
            "acquisition_id",
            "cohort_id",
            "npi",
            name="pd_uhc_flex_practitioner_work_cohort_key",
        ),
        ForeignKeyConstraint(
            ["acquisition_id", "cohort_id"],
            [
                _reference(
                    "provider_directory_uhc_flex_practitioner_acquisition",
                    "acquisition_id",
                ),
                _reference(
                    "provider_directory_uhc_flex_practitioner_acquisition",
                    "cohort_id",
                ),
            ],
            name="pd_uhc_flex_practitioner_work_acquisition_fkey",
        ),
        ForeignKeyConstraint(
            ["cohort_id", "npi"],
            [
                _reference("provider_directory_uhc_flex_npi_member", "cohort_id"),
                _reference("provider_directory_uhc_flex_npi_member", "npi"),
            ],
            name="pd_uhc_flex_practitioner_work_member_fkey",
        ),
        CheckConstraint(
            "npi BETWEEN 1000000000 AND 2999999999 AND attempt_count >= 0 "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$') "
            "AND (result_sha256 IS NULL OR result_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (terminal_record_sha256 IS NULL OR "
            "terminal_record_sha256 ~ '^[0-9a-f]{64}$')",
            name="pd_uhc_flex_practitioner_work_value_check",
        ),
        CheckConstraint(
            "(status = 'pending' AND attempt_count >= 0 AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "result_sha256 IS NULL AND resource_count IS NULL AND "
            "error_code IS NULL AND terminal_record_sha256 IS NULL AND "
            "terminal_at IS NULL) OR "
            "(status = 'leased' AND attempt_count > 0 AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL AND lease_heartbeat_at IS NOT NULL "
            "AND result_sha256 IS NULL AND resource_count IS NULL AND "
            "error_code IS NULL AND terminal_record_sha256 IS NULL AND "
            "terminal_at IS NULL) OR "
            "(status IN ('matched', 'unmatched') AND attempt_count > 0 AND "
            "lease_token IS NULL AND lease_expires_at IS NULL AND "
            "lease_heartbeat_at IS NULL AND result_sha256 IS NOT NULL AND "
            "resource_count >= 0 AND error_code IS NULL AND "
            "terminal_record_sha256 IS NOT NULL AND terminal_at IS NOT NULL) OR "
            "(status = 'error' AND attempt_count > 0 AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "result_sha256 IS NULL AND resource_count = 0 AND "
            "error_code ~ '^[a-z][a-z0-9_]{0,127}$' AND "
            "terminal_record_sha256 IS NOT NULL AND terminal_at IS NOT NULL)",
            name="pd_uhc_flex_practitioner_work_state_check",
        ),
    )
    __my_index_elements__ = ["acquisition_id", "npi"]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "acquisition_id",
                "status",
                "lease_expires_at",
                "npi",
            ),
            "name": "pd_uhc_flex_practitioner_work_claim_idx",
        }
    ]

    acquisition_id = Column(String(55), nullable=False)
    cohort_id = Column(String(54), nullable=False)
    npi = Column(BigInteger, nullable=False)
    status = Column(String(16), nullable=False)
    attempt_count = Column(Integer, nullable=False)
    lease_token = Column(String(64))
    lease_expires_at = Column(TIMESTAMP(timezone=True))
    lease_heartbeat_at = Column(TIMESTAMP(timezone=True))
    result_sha256 = Column(String(64))
    resource_count = Column(Integer)
    error_code = Column(String(128))
    terminal_record_sha256 = Column(String(64))
    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )
    updated_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )
    terminal_at = Column(TIMESTAMP(timezone=True))


class ProviderDirectoryUHCFlexPractitionerResource(Base, JSONOutputMixin):
    """One immutable canonical FHIR payload for one lease attempt."""

    __tablename__ = "provider_directory_uhc_flex_practitioner_resource"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "acquisition_id",
            "npi",
            "attempt",
            "resource_id",
            name="pd_uhc_flex_practitioner_resource_pkey",
        ),
        ForeignKeyConstraint(
            ["acquisition_id", "cohort_id", "npi"],
            [
                _reference(
                    "provider_directory_uhc_flex_practitioner_work",
                    "acquisition_id",
                ),
                _reference(
                    "provider_directory_uhc_flex_practitioner_work",
                    "cohort_id",
                ),
                _reference(
                    "provider_directory_uhc_flex_practitioner_work",
                    "npi",
                ),
            ],
            name="pd_uhc_flex_practitioner_resource_work_fkey",
        ),
        CheckConstraint(
            "npi BETWEEN 1000000000 AND 2999999999 AND attempt > 0 AND "
            "resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "payload_sha256 ~ '^[0-9a-f]{64}$' AND "
            "octet_length(payload_json_text) BETWEEN 2 AND 1048576",
            name="pd_uhc_flex_practitioner_resource_value_check",
        ),
    )
    __my_index_elements__ = [
        "acquisition_id",
        "npi",
        "attempt",
        "resource_id",
    ]

    acquisition_id = Column(String(55), nullable=False)
    cohort_id = Column(String(54), nullable=False)
    npi = Column(BigInteger, nullable=False)
    attempt = Column(Integer, nullable=False)
    resource_id = Column(String(64), nullable=False)
    payload_sha256 = Column(String(64), nullable=False)
    payload_json_text = Column(Text, nullable=False)
    created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )
