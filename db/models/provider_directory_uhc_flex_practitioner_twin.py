# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable sealed-pair comparison and matched publication authority."""

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
    "ProviderDirectoryUHCFlexPractitionerTwinAdmission",
    "ProviderDirectoryUHCFlexPractitionerTwinAttempt",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()
_ACQUISITION = "provider_directory_uhc_flex_practitioner_acquisition"
_ATTEMPT = "provider_directory_uhc_flex_practitioner_twin_attempt"
_ATTEMPT_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-twin-attempt.v1"
)
_ADMISSION_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-matched-admission.v1"
)


def _reference(table_name: str, column_name: str) -> str:
    return f"{_SCHEMA}.{table_name}.{column_name}"


class UHCFlexPractitionerTwinAttemptModel(Base, JSONOutputMixin):
    """One immutable, one-use comparison of two independently sealed roots."""

    __tablename__ = _ATTEMPT
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "attempt_id",
            name="pd_uhc_flex_practitioner_twin_attempt_pkey",
        ),
        UniqueConstraint(
            "baseline_acquisition_id",
            name="pd_uhc_flex_practitioner_twin_baseline_key",
        ),
        UniqueConstraint(
            "candidate_acquisition_id",
            name="pd_uhc_flex_practitioner_twin_candidate_key",
        ),
        UniqueConstraint(
            "dataset_intent_id",
            name="pd_uhc_flex_practitioner_twin_intent_key",
        ),
        UniqueConstraint(
            "baseline_acquisition_id",
            "candidate_acquisition_id",
            name="pd_uhc_flex_practitioner_twin_pair_key",
        ),
        ForeignKeyConstraint(
            ["baseline_acquisition_id"],
            [_reference(_ACQUISITION, "acquisition_id")],
            name="pd_uhc_flex_practitioner_twin_baseline_fkey",
        ),
        ForeignKeyConstraint(
            ["candidate_acquisition_id"],
            [_reference(_ACQUISITION, "acquisition_id")],
            name="pd_uhc_flex_practitioner_twin_candidate_fkey",
        ),
        CheckConstraint(
            f"attempt_contract_id = '{_ATTEMPT_CONTRACT}' AND "
            "attempt_id ~ '^pdufpta_[0-9a-f]{48}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "semantic_projection_as_of BETWEEN DATE '0001-01-01' "
            "AND DATE '9999-12-31' AND "
            "baseline_acquisition_id <> candidate_acquisition_id AND "
            "baseline_run_id <> candidate_run_id AND "
            "expected_npi_count > 0 AND baseline_resource_count >= 0 AND "
            "candidate_resource_count >= 0 AND "
            "baseline_terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "candidate_terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "matched = (baseline_terminal_set_sha256 = "
            "candidate_terminal_set_sha256 AND baseline_resource_count = "
            "candidate_resource_count)",
            name="pd_uhc_flex_practitioner_twin_attempt_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["attempt_id"]

    attempt_id = Column(String(56), nullable=False)
    attempt_contract_id = Column(String(96), nullable=False)
    semantic_projection_as_of = Column(Date, nullable=False)
    operation_key = Column(String(64), nullable=False)
    baseline_acquisition_id = Column(String(55), nullable=False)
    candidate_acquisition_id = Column(String(55), nullable=False)
    cohort_id = Column(String(54), nullable=False)
    dataset_intent_id = Column(String(55), nullable=False)
    source_id = Column(String(64), nullable=False)
    connector_id = Column(String(64), nullable=False)
    query_contract_id = Column(String(96), nullable=False)
    storage_contract_id = Column(String(96), nullable=False)
    baseline_run_id = Column(String(55), nullable=False)
    candidate_run_id = Column(String(55), nullable=False)
    expected_npi_count = Column(BigInteger, nullable=False)
    baseline_terminal_set_sha256 = Column(String(64), nullable=False)
    candidate_terminal_set_sha256 = Column(String(64), nullable=False)
    baseline_resource_count = Column(BigInteger, nullable=False)
    candidate_resource_count = Column(BigInteger, nullable=False)
    matched = Column(Boolean, nullable=False)
    attempted_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class UHCFlexPractitionerTwinAdmissionModel(Base, JSONOutputMixin):
    """Immutable publication authority keyed to one matched candidate."""

    __tablename__ = "provider_directory_uhc_flex_practitioner_twin_admission"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "admission_id",
            name="pd_uhc_flex_practitioner_twin_admission_pkey",
        ),
        UniqueConstraint(
            "attempt_id",
            name="pd_uhc_flex_practitioner_admission_attempt_key",
        ),
        UniqueConstraint(
            "candidate_acquisition_id",
            name="pd_uhc_flex_practitioner_admission_candidate_key",
        ),
        ForeignKeyConstraint(
            ["attempt_id"],
            [_reference(_ATTEMPT, "attempt_id")],
            name="pd_uhc_flex_practitioner_admission_attempt_fkey",
        ),
        ForeignKeyConstraint(
            ["baseline_acquisition_id"],
            [_reference(_ACQUISITION, "acquisition_id")],
            name="pd_uhc_flex_practitioner_admission_baseline_fkey",
        ),
        ForeignKeyConstraint(
            ["candidate_acquisition_id"],
            [_reference(_ACQUISITION, "acquisition_id")],
            name="pd_uhc_flex_practitioner_admission_candidate_fkey",
        ),
        CheckConstraint(
            f"admission_contract_id = '{_ADMISSION_CONTRACT}' AND "
            "admission_id ~ '^pdufpad_[0-9a-f]{48}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "semantic_projection_as_of BETWEEN DATE '0001-01-01' "
            "AND DATE '9999-12-31' AND "
            "baseline_acquisition_id <> candidate_acquisition_id AND "
            "baseline_run_id <> candidate_run_id AND "
            "expected_npi_count > 0 AND resource_count >= 0 AND "
            "terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "publication_authority IS TRUE",
            name="pd_uhc_flex_practitioner_twin_admission_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["candidate_acquisition_id"]

    admission_id = Column(String(56), nullable=False)
    admission_contract_id = Column(String(96), nullable=False)
    semantic_projection_as_of = Column(Date, nullable=False)
    operation_key = Column(String(64), nullable=False)
    attempt_id = Column(String(56), nullable=False)
    baseline_acquisition_id = Column(String(55), nullable=False)
    candidate_acquisition_id = Column(String(55), nullable=False)
    cohort_id = Column(String(54), nullable=False)
    dataset_intent_id = Column(String(55), nullable=False)
    source_id = Column(String(64), nullable=False)
    connector_id = Column(String(64), nullable=False)
    query_contract_id = Column(String(96), nullable=False)
    storage_contract_id = Column(String(96), nullable=False)
    baseline_run_id = Column(String(55), nullable=False)
    candidate_run_id = Column(String(55), nullable=False)
    expected_npi_count = Column(BigInteger, nullable=False)
    terminal_set_sha256 = Column(String(64), nullable=False)
    resource_count = Column(BigInteger, nullable=False)
    publication_authority = Column(Boolean, nullable=False)
    admitted_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


ProviderDirectoryUHCFlexPractitionerTwinAttempt = (
    UHCFlexPractitionerTwinAttemptModel
)
ProviderDirectoryUHCFlexPractitionerTwinAdmission = (
    UHCFlexPractitionerTwinAdmissionModel
)
