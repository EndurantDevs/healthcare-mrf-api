# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable evidence for one matched FHIR formulary acquisition pair."""

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
from sqlalchemy import TIMESTAMP
from sqlalchemy import UniqueConstraint
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


__all__ = ("FHIRFormularyTwinAdmission", "FHIRFormularyTwinAttempt")


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()


def _dataset_reference(column: str) -> str:
    return f"{_SCHEMA}.fhir_formulary_dataset.{column}"


def _attempt_reference(column: str) -> str:
    return f"{_SCHEMA}.fhir_formulary_twin_attempt.{column}"


class FHIRFormularyTwinAttempt(Base, JSONOutputMixin):
    """One-use immutable evidence for a fully evaluated root pair."""

    __tablename__ = "fhir_formulary_twin_attempt"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "source_id",
            "baseline_dataset_id",
            "candidate_dataset_id",
            name="fhir_formulary_twin_attempt_pkey",
        ),
        UniqueConstraint(
            "baseline_dataset_id",
            name="fhir_formulary_twin_attempt_baseline_key",
        ),
        UniqueConstraint(
            "candidate_dataset_id",
            name="fhir_formulary_twin_attempt_candidate_key",
        ),
        UniqueConstraint(
            "source_id",
            "baseline_dataset_id",
            "baseline_run_id",
            "candidate_dataset_id",
            "candidate_run_id",
            name="fhir_formulary_twin_attempt_binding_key",
        ),
        ForeignKeyConstraint(
            ["source_id", "baseline_dataset_id", "baseline_run_id"],
            [
                _dataset_reference("source_id"),
                _dataset_reference("dataset_id"),
                _dataset_reference("run_id"),
            ],
            name="fhir_formulary_twin_attempt_baseline_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "candidate_dataset_id", "candidate_run_id"],
            [
                _dataset_reference("source_id"),
                _dataset_reference("dataset_id"),
                _dataset_reference("run_id"),
            ],
            name="fhir_formulary_twin_attempt_candidate_fkey",
        ),
        CheckConstraint(
            "baseline_dataset_id <> candidate_dataset_id "
            "AND baseline_run_id <> candidate_run_id",
            name="fhir_formulary_twin_attempt_identity_check",
        ),
        CheckConstraint(
            "source_configuration_hash ~ '^[0-9a-f]{64}$' "
            "AND acquisition_contract_hash ~ '^[0-9a-f]{64}$' "
            "AND baseline_evidence_hash ~ '^[0-9a-f]{64}$' "
            "AND candidate_evidence_hash ~ '^[0-9a-f]{64}$' "
            "AND matched = (baseline_evidence_hash = candidate_evidence_hash)",
            name="fhir_formulary_twin_attempt_proof_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = [
        "source_id",
        "baseline_dataset_id",
        "candidate_dataset_id",
    ]

    source_id = Column(String(64), nullable=False)
    baseline_dataset_id = Column(String(64), nullable=False)
    baseline_run_id = Column(String(64), nullable=False)
    candidate_dataset_id = Column(String(64), nullable=False)
    candidate_run_id = Column(String(64), nullable=False)
    cutoff_at = Column(TIMESTAMP(timezone=True), nullable=False)
    source_configuration_hash = Column(String(64), nullable=False)
    acquisition_contract_hash = Column(String(64), nullable=False)
    baseline_evidence_hash = Column(String(64), nullable=False)
    candidate_evidence_hash = Column(String(64), nullable=False)
    matched = Column(Boolean, nullable=False)
    attempted_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class FHIRFormularyTwinAdmission(Base, JSONOutputMixin):
    """Source-qualified immutable proof for two matching acquisitions."""

    __tablename__ = "fhir_formulary_twin_admission"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "source_id",
            "candidate_dataset_id",
            name="fhir_formulary_twin_admission_pkey",
        ),
        UniqueConstraint(
            "candidate_dataset_id",
            name="fhir_formulary_twin_admission_candidate_key",
        ),
        UniqueConstraint(
            "baseline_dataset_id",
            name="fhir_formulary_twin_admission_baseline_key",
        ),
        ForeignKeyConstraint(
            [
                "source_id",
                "baseline_dataset_id",
                "baseline_run_id",
                "candidate_dataset_id",
                "candidate_run_id",
            ],
            [
                _attempt_reference("source_id"),
                _attempt_reference("baseline_dataset_id"),
                _attempt_reference("baseline_run_id"),
                _attempt_reference("candidate_dataset_id"),
                _attempt_reference("candidate_run_id"),
            ],
            name="fhir_formulary_twin_admission_attempt_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "baseline_dataset_id", "baseline_run_id"],
            [
                _dataset_reference("source_id"),
                _dataset_reference("dataset_id"),
                _dataset_reference("run_id"),
            ],
            name="fhir_formulary_twin_admission_baseline_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "candidate_dataset_id", "candidate_run_id"],
            [
                _dataset_reference("source_id"),
                _dataset_reference("dataset_id"),
                _dataset_reference("run_id"),
            ],
            name="fhir_formulary_twin_admission_candidate_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "predecessor_dataset_id"],
            [_dataset_reference("source_id"), _dataset_reference("dataset_id")],
            name="fhir_formulary_twin_admission_predecessor_fkey",
        ),
        CheckConstraint(
            "baseline_dataset_id <> candidate_dataset_id "
            "AND baseline_run_id <> candidate_run_id",
            name="fhir_formulary_twin_admission_identity_check",
        ),
        CheckConstraint(
            "list_count > 0 AND alias_count > 0 AND medication_count > 0 "
            "AND alternative_count >= 0 "
            "AND source_configuration_hash ~ '^[0-9a-f]{64}$' "
            "AND acquisition_contract_hash ~ '^[0-9a-f]{64}$' "
            "AND coverage_hash ~ '^[0-9a-f]{64}$' "
            "AND membership_hash ~ '^[0-9a-f]{64}$' "
            "AND alternative_hash ~ '^[0-9a-f]{64}$'",
            name="fhir_formulary_twin_admission_proof_check",
        ),
        CheckConstraint(
            "baseline_verified_at <= candidate_verified_at "
            "AND candidate_verified_at <= admitted_at",
            name="fhir_formulary_twin_admission_time_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )
    __my_index_elements__ = ["source_id", "candidate_dataset_id"]

    source_id = Column(String(64), nullable=False)
    baseline_dataset_id = Column(String(64), nullable=False)
    baseline_run_id = Column(String(64), nullable=False)
    candidate_dataset_id = Column(String(64), nullable=False)
    candidate_run_id = Column(String(64), nullable=False)
    predecessor_dataset_id = Column(String(64))
    cutoff_at = Column(TIMESTAMP(timezone=True), nullable=False)
    source_configuration_hash = Column(String(64), nullable=False)
    acquisition_contract_hash = Column(String(64), nullable=False)
    list_count = Column(Integer, nullable=False)
    alias_count = Column(Integer, nullable=False)
    medication_count = Column(BigInteger, nullable=False)
    coverage_hash = Column(String(64), nullable=False)
    membership_hash = Column(String(64), nullable=False)
    alternative_count = Column(BigInteger, nullable=False)
    alternative_hash = Column(String(64), nullable=False)
    baseline_verified_at = Column(TIMESTAMP(timezone=True), nullable=False)
    candidate_verified_at = Column(TIMESTAMP(timezone=True), nullable=False)
    admitted_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )
