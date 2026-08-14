# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable UHC formulary release evidence models."""

from __future__ import annotations

from sqlalchemy import BigInteger
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy import text
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY

from db.connection import Base
from db.json_mixin import JSONOutputMixin
from db.models.formulary_fhir import _reference
from db.models.formulary_fhir import _table_args


__all__ = ("FHIRFormularyUHCAdmissionReceipt",)


class FHIRFormularyUHCAdmissionReceipt(Base, JSONOutputMixin):
    """Restart-safe UHC evidence bound to one generic twin admission."""

    __tablename__ = "fhir_formulary_uhc_admission_receipt"
    __main_table__ = __tablename__
    EXCLUDE_FIELDS = ("selected_source_file_ids",)
    __table_args__ = _table_args(
        PrimaryKeyConstraint("receipt_id"),
        UniqueConstraint(
            "candidate_dataset_id",
            name="fhir_formulary_uhc_admission_receipt_candidate_key",
        ),
        ForeignKeyConstraint(
            ["source_id", "candidate_dataset_id"],
            [
                _reference("fhir_formulary_twin_admission", "source_id"),
                _reference(
                    "fhir_formulary_twin_admission",
                    "candidate_dataset_id",
                ),
            ],
            name="fhir_formulary_uhc_admission_receipt_admission_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "source_file_set_sha256"],
            [
                _reference("fhir_formulary_source_artifact_set", "source_id"),
                _reference(
                    "fhir_formulary_source_artifact_set",
                    "source_file_set_sha256",
                ),
            ],
            name="fhir_formulary_uhc_admission_receipt_set_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "source_observation_sha256"],
            [
                _reference(
                    "fhir_formulary_source_artifact_observation",
                    "source_id",
                ),
                _reference(
                    "fhir_formulary_source_artifact_observation",
                    "source_observation_sha256",
                ),
            ],
            name="fhir_formulary_uhc_admission_receipt_observation_fkey",
        ),
        CheckConstraint(
            "receipt_id ~ '^ffur_[0-9a-f]{48}$' AND "
            "source_id = 'uhc-official-formulary-mrf' AND "
            "source_observation_sha256 ~ '^[0-9a-f]{64}$' AND "
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "artifact_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "spool_content_sha256 ~ '^[0-9a-f]{64}$' AND "
            "expected_file_count = 48 AND file_count BETWEEN 1 AND 48 AND "
            "excluded_file_count = expected_file_count - file_count AND "
            "cardinality(selected_source_file_ids) = file_count AND "
            "((excluded_file_count = 0 AND exclusion_code IS NULL) OR "
            "(excluded_file_count > 0 AND "
            "exclusion_code = 'not_selected')) AND "
            "raw_record_count > 0 AND "
            "raw_plan_entry_count > 0 AND plan_count > 0 AND "
            "medication_membership_count > 0 AND duplicate_count >= 0 AND "
            "superseded_count >= 0 AND isfinite(max_last_updated_at) AND "
            "max_last_updated_at >= TIMESTAMPTZ '2000-01-01 00:00:00+00' "
            "AND max_last_updated_at < "
            "TIMESTAMPTZ '2101-01-01 00:00:00+00'",
            name="fhir_formulary_uhc_admission_receipt_values_check",
        ),
    )
    __my_index_elements__ = ["receipt_id"]

    receipt_id = Column(String(53), nullable=False)
    source_id = Column(String(64), nullable=False)
    source_observation_sha256 = Column(String(64), nullable=False)
    source_file_set_sha256 = Column(String(64), nullable=False)
    artifact_set_sha256 = Column(String(64), nullable=False)
    candidate_dataset_id = Column(String(64), nullable=False)
    spool_content_sha256 = Column(String(64), nullable=False)
    file_count = Column(Integer, nullable=False)
    expected_file_count = Column(
        Integer,
        server_default=text("48"),
        nullable=False,
    )
    excluded_file_count = Column(
        Integer,
        server_default=text("0"),
        nullable=False,
    )
    selected_source_file_ids = Column(ARRAY(String(64)), nullable=False)
    exclusion_code = Column(String(32))
    raw_record_count = Column(BigInteger, nullable=False)
    raw_plan_entry_count = Column(BigInteger, nullable=False)
    plan_count = Column(BigInteger, nullable=False)
    medication_membership_count = Column(BigInteger, nullable=False)
    duplicate_count = Column(BigInteger, nullable=False)
    superseded_count = Column(BigInteger, nullable=False)
    max_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=False)
    recorded_at = Column(
        TIMESTAMP(timezone=True),
        server_default=text("transaction_timestamp()"),
        nullable=False,
    )
