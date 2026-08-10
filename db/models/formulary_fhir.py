# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant FHIR formulary generation storage models."""

from __future__ import annotations

import os

from sqlalchemy import (
    TEXT,
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base
from db.json_mixin import JSONOutputMixin


__all__ = (
    "FHIRFormularyAlias",
    "FHIRFormularyAliasMembership",
    "FHIRFormularyAliasVersion",
    "FHIRFormularyAlternative",
    "FHIRFormularyCheckpoint",
    "FHIRFormularyCoveragePlan",
    "FHIRFormularyCoveragePlanVersion",
    "FHIRFormularyCurrent",
    "FHIRFormularyDataset",
    "FHIRFormularyDatasetAlias",
    "FHIRFormularyDatasetCoveragePlan",
    "FHIRFormularyMedication",
    "FHIRFormularySource",
    "FHIRFormularySourceAcquisitionLease",
    "FHIRFormularySourceArtifact",
    "FHIRFormularySourceArtifactObservation",
    "FHIRFormularySourceArtifactSet",
    "FHIRFormularyUHCAdmissionReceipt",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()


def _table_args(*constraints):
    return (*constraints, {"schema": _SCHEMA, "extend_existing": True})


def _reference(table: str, column: str) -> str:
    return f"{_SCHEMA}.{table}.{column}"


class FHIRFormularySource(Base, JSONOutputMixin):
    """Configured source row and serialization lock for publication."""

    __tablename__ = "fhir_formulary_source"
    __main_table__ = __tablename__
    EXCLUDE_FIELDS = ("runtime_config_json",)
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id"),
        UniqueConstraint(
            "canonical_base",
            name="fhir_formulary_source_base_key",
        ),
        CheckConstraint(
            "jsonb_typeof(runtime_config_json) = 'object'",
            name="fhir_formulary_source_runtime_config_check",
        ),
        CheckConstraint(
            "jsonb_typeof(metadata_json) = 'object'",
            name="fhir_formulary_source_metadata_check",
        ),
    )
    __my_index_elements__ = ["source_id"]

    source_id = Column(String(64), nullable=False)
    canonical_base = Column(TEXT, nullable=False)
    display_name = Column(String(256), nullable=False)
    enabled = Column(Boolean, nullable=False, default=False)
    runtime_config_json = Column(JSONB, nullable=False, default=dict)
    metadata_json = Column(JSONB, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyDataset(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_dataset"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        ForeignKeyConstraint(
            ["source_id", "previous_dataset_id"],
            [
                _reference("fhir_formulary_dataset", "source_id"),
                _reference("fhir_formulary_dataset", "dataset_id"),
            ],
            name="fhir_formulary_dataset_previous_owner_fkey",
        ),
        UniqueConstraint("run_id", name="fhir_formulary_dataset_run_key"),
        UniqueConstraint(
            "source_id",
            "dataset_id",
            name="fhir_formulary_dataset_source_dataset_key",
        ),
        UniqueConstraint(
            "source_id",
            "dataset_id",
            "run_id",
            name="fhir_formulary_dataset_checkpoint_owner_key",
        ),
        CheckConstraint(
            "status IN ('building', 'verified', 'published', 'failed')",
            name="fhir_formulary_dataset_status_check",
        ),
        CheckConstraint(
            "list_count >= 0 AND alias_count >= 0 AND medication_count >= 0",
            name="fhir_formulary_dataset_count_check",
        ),
        CheckConstraint(
            "jsonb_typeof(summary_json) = 'object'",
            name="fhir_formulary_dataset_summary_check",
        ),
    )
    __my_index_elements__ = ["dataset_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("source_id", "created_at DESC"),
            "name": "fhir_formulary_dataset_source_created_idx",
        },
        {
            "index_elements": ("status", "created_at DESC"),
            "name": "fhir_formulary_dataset_status_created_idx",
        },
    ]

    dataset_id = Column(String(64), nullable=False)
    source_id = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    previous_dataset_id = Column(String(64))
    cutoff_at = Column(TIMESTAMP(timezone=True), nullable=False)
    status = Column(String(16), nullable=False)
    publish_requested = Column(Boolean, nullable=False, default=False)
    seed_eligible = Column(Boolean, nullable=False, default=False)
    list_count = Column(Integer, nullable=False, default=0)
    alias_count = Column(Integer, nullable=False, default=0)
    medication_count = Column(BigInteger, nullable=False, default=0)
    coverage_hash = Column(String(64))
    membership_hash = Column(String(64))
    summary_json = Column(JSONB, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    verified_at = Column(TIMESTAMP(timezone=True))
    published_at = Column(TIMESTAMP(timezone=True))
    failed_at = Column(TIMESTAMP(timezone=True))
    error_json = Column(JSONB)


class FHIRFormularyCurrent(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_current"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id"),
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
            name="fhir_formulary_current_source_dataset_fkey",
        ),
        UniqueConstraint(
            "dataset_id",
            name="fhir_formulary_current_dataset_key",
        ),
        CheckConstraint(
            "generation > 0",
            name="fhir_formulary_current_generation_check",
        ),
    )
    __my_index_elements__ = ["source_id"]

    source_id = Column(String(64), nullable=False)
    dataset_id = Column(String(64), nullable=False)
    generation = Column(BigInteger, nullable=False, default=1)
    published_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyCoveragePlan(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_coverage_plan"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("public_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        UniqueConstraint(
            "source_id",
            "canonical_identity",
            name="fhir_formulary_coverage_plan_identity_key",
        ),
        UniqueConstraint(
            "source_id",
            "public_id",
            name="fhir_formulary_coverage_plan_source_public_key",
        ),
        CheckConstraint(
            "public_id ~ '^fhir_[a-z2-7]{26}$'",
            name="fhir_formulary_coverage_plan_public_id_check",
        ),
    )
    __my_index_elements__ = ["public_id"]

    public_id = Column(String(31), nullable=False)
    source_id = Column(String(64), nullable=False)
    upstream_list_id = Column(String(256), nullable=False)
    canonical_identity = Column(TEXT, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyCoveragePlanVersion(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_coverage_plan_version"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("coverage_version_id"),
        ForeignKeyConstraint(
            ["public_id"],
            [_reference("fhir_formulary_coverage_plan", "public_id")],
        ),
        UniqueConstraint(
            "public_id",
            "coverage_version_id",
            name="fhir_formulary_coverage_plan_version_owner_key",
        ),
        UniqueConstraint(
            "public_id",
            "content_hash",
            name="fhir_formulary_coverage_plan_version_content_key",
        ),
        CheckConstraint(
            "jsonb_typeof(metadata_json) = 'object'",
            name="fhir_formulary_coverage_plan_version_metadata_check",
        ),
    )
    __my_index_elements__ = ["coverage_version_id"]

    coverage_version_id = Column(String(64), nullable=False)
    public_id = Column(String(31), nullable=False)
    upstream_version_id = Column(String(256))
    upstream_last_updated = Column(TIMESTAMP(timezone=True))
    status = Column(String(32))
    title = Column(TEXT)
    name = Column(TEXT)
    period_start = Column(TIMESTAMP(timezone=True))
    period_end = Column(TIMESTAMP(timezone=True))
    upstream_date = Column(TIMESTAMP(timezone=True))
    content_hash = Column(String(64), nullable=False)
    metadata_json = Column(JSONB, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyDatasetCoveragePlan(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_dataset_coverage_plan"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id", "public_id"),
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
            name="fhir_formulary_dataset_coverage_dataset_owner_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "public_id"],
            [
                _reference("fhir_formulary_coverage_plan", "source_id"),
                _reference("fhir_formulary_coverage_plan", "public_id"),
            ],
            name="fhir_formulary_dataset_coverage_plan_owner_fkey",
        ),
        ForeignKeyConstraint(
            ["public_id", "coverage_version_id"],
            [
                _reference("fhir_formulary_coverage_plan_version", "public_id"),
                _reference(
                    "fhir_formulary_coverage_plan_version",
                    "coverage_version_id",
                ),
            ],
            name="fhir_formulary_dataset_coverage_version_owner_fkey",
        ),
        UniqueConstraint(
            "dataset_id",
            "coverage_version_id",
            name="fhir_formulary_dataset_coverage_version_key",
        ),
    )
    __my_index_elements__ = ["dataset_id", "public_id"]

    source_id = Column(String(64), nullable=False)
    dataset_id = Column(String(64), nullable=False)
    public_id = Column(String(31), nullable=False)
    coverage_version_id = Column(String(64), nullable=False)


from db.models.formulary_fhir_alias import (
    FHIRFormularyAlias,
    FHIRFormularyAliasVersion,
    FHIRFormularyDatasetAlias,
)
from db.models.formulary_fhir_artifact import (
    FHIRFormularySourceAcquisitionLease,
    FHIRFormularySourceArtifact,
    FHIRFormularySourceArtifactObservation,
    FHIRFormularySourceArtifactSet,
)
from db.models.formulary_fhir_content import (
    FHIRFormularyAliasMembership,
    FHIRFormularyAlternative,
    FHIRFormularyCheckpoint,
    FHIRFormularyMedication,
)
from db.models.formulary_fhir_uhc import FHIRFormularyUHCAdmissionReceipt
