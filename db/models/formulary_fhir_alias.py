# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-qualified FHIR formulary alias generation models."""

from __future__ import annotations

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base
from db.json_mixin import JSONOutputMixin
from db.models.formulary_fhir import _reference, _table_args


__all__ = (
    "FHIRFormularyAlias",
    "FHIRFormularyAliasVersion",
    "FHIRFormularyDatasetAlias",
)


class FHIRFormularyAlias(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_drug_plan_alias"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("alias_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        ForeignKeyConstraint(
            ["source_id", "public_id"],
            [
                _reference("fhir_formulary_coverage_plan", "source_id"),
                _reference("fhir_formulary_coverage_plan", "public_id"),
            ],
            name="fhir_formulary_alias_coverage_owner_fkey",
        ),
        UniqueConstraint(
            "public_id",
            "source_plan_identifier",
            name="fhir_formulary_alias_plan_key",
        ),
        UniqueConstraint(
            "source_id",
            "alias_id",
            name="fhir_formulary_alias_source_alias_key",
        ),
        UniqueConstraint(
            "source_id",
            "alias_id",
            "source_plan_identifier",
            name="fhir_formulary_alias_checkpoint_owner_key",
        ),
    )
    __my_index_elements__ = ["alias_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("source_plan_identifier",),
            "name": "fhir_formulary_alias_source_plan_idx",
        },
    ]

    alias_id = Column(String(64), nullable=False)
    source_id = Column(String(64), nullable=False)
    public_id = Column(String(31), nullable=False)
    source_plan_identifier = Column(String(512), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyAliasVersion(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_drug_plan_alias_version"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("alias_version_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        ForeignKeyConstraint(
            ["source_id", "alias_id"],
            [
                _reference("fhir_formulary_drug_plan_alias", "source_id"),
                _reference("fhir_formulary_drug_plan_alias", "alias_id"),
            ],
            name="fhir_formulary_alias_version_alias_owner_fkey",
        ),
        UniqueConstraint(
            "alias_id",
            "alias_version_id",
            name="fhir_formulary_alias_version_owner_key",
        ),
        UniqueConstraint(
            "source_id",
            "alias_version_id",
            name="fhir_formulary_alias_version_source_version_key",
        ),
        UniqueConstraint(
            "alias_id",
            "membership_hash",
            name="fhir_formulary_alias_version_membership_key",
        ),
        CheckConstraint(
            "expected_count >= 0 AND membership_count >= 0",
            name="fhir_formulary_alias_version_count_check",
        ),
        CheckConstraint(
            "expected_count = membership_count",
            name="fhir_formulary_alias_version_exact_count_check",
        ),
        CheckConstraint(
            "acquisition_mode IN ('full', 'delta')",
            name="fhir_formulary_alias_version_mode_check",
        ),
        CheckConstraint(
            "jsonb_typeof(summary_json) = 'object'",
            name="fhir_formulary_alias_version_summary_check",
        ),
    )
    __my_index_elements__ = ["alias_version_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("alias_id", "created_at DESC"),
            "name": "fhir_formulary_alias_version_created_idx",
        },
    ]

    alias_version_id = Column(String(64), nullable=False)
    source_id = Column(String(64), nullable=False)
    alias_id = Column(String(64), nullable=False)
    expected_count = Column(BigInteger, nullable=False)
    membership_count = Column(BigInteger, nullable=False)
    membership_hash = Column(String(64), nullable=False)
    cutoff_at = Column(TIMESTAMP(timezone=True), nullable=False)
    acquisition_mode = Column(String(32), nullable=False)
    summary_json = Column(JSONB, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyDatasetAlias(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_dataset_alias"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id", "alias_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        ForeignKeyConstraint(
            ["source_id", "dataset_id"],
            [
                _reference("fhir_formulary_dataset", "source_id"),
                _reference("fhir_formulary_dataset", "dataset_id"),
            ],
            name="fhir_formulary_dataset_alias_dataset_owner_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "alias_id"],
            [
                _reference("fhir_formulary_drug_plan_alias", "source_id"),
                _reference("fhir_formulary_drug_plan_alias", "alias_id"),
            ],
            name="fhir_formulary_dataset_alias_alias_owner_fkey",
        ),
        ForeignKeyConstraint(
            ["alias_id", "alias_version_id"],
            [
                _reference("fhir_formulary_drug_plan_alias_version", "alias_id"),
                _reference(
                    "fhir_formulary_drug_plan_alias_version",
                    "alias_version_id",
                ),
            ],
            name="fhir_formulary_dataset_alias_version_owner_fkey",
        ),
    )
    __my_index_elements__ = ["dataset_id", "alias_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("dataset_id", "alias_version_id"),
            "name": "fhir_formulary_dataset_alias_version_idx",
        },
    ]

    source_id = Column(String(64), nullable=False)
    dataset_id = Column(String(64), nullable=False)
    alias_id = Column(String(64), nullable=False)
    alias_version_id = Column(String(64), nullable=False)
