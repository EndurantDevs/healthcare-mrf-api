# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable FHIR formulary generations and their atomic serving pointer."""

from __future__ import annotations

import os

from sqlalchemy import (
    JSON,
    TEXT,
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)

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
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _table_args(*constraints):
    return (*constraints, {"schema": _schema(), "extend_existing": True})


class FHIRFormularySource(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_source"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id"),
        UniqueConstraint("canonical_base", name="fhir_formulary_source_base_key"),
    )
    __my_index_elements__ = ["source_id"]

    source_id = Column(String(64), nullable=False)
    canonical_base = Column(TEXT, nullable=False)
    display_name = Column(String(256), nullable=False)
    enabled = Column(Boolean, nullable=False, default=False)
    metadata_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyDataset(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_dataset"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id"),
        UniqueConstraint("run_id", name="fhir_formulary_dataset_run_key"),
        CheckConstraint(
            "status IN ('building', 'verified', 'published', 'failed')",
            name="fhir_formulary_dataset_status_check",
        ),
    )
    __my_index_elements__ = ["dataset_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("source_id", "created_at"),
            "name": "fhir_formulary_dataset_source_created_idx",
        },
        {
            "index_elements": ("status", "created_at"),
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
    list_count = Column(Integer, nullable=False, default=0)
    alias_count = Column(Integer, nullable=False, default=0)
    medication_count = Column(BigInteger, nullable=False, default=0)
    coverage_hash = Column(String(64))
    membership_hash = Column(String(64))
    summary_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    verified_at = Column(TIMESTAMP(timezone=True))
    published_at = Column(TIMESTAMP(timezone=True))
    failed_at = Column(TIMESTAMP(timezone=True))
    error_json = Column(JSON)


class FHIRFormularyCurrent(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_current"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id"),
        UniqueConstraint("dataset_id", name="fhir_formulary_current_dataset_key"),
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
        UniqueConstraint(
            "source_id",
            "canonical_identity",
            name="fhir_formulary_coverage_plan_identity_key",
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
        UniqueConstraint(
            "public_id",
            "content_hash",
            name="fhir_formulary_coverage_plan_version_content_key",
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
    metadata_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyDatasetCoveragePlan(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_dataset_coverage_plan"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id", "public_id"),
        UniqueConstraint(
            "dataset_id",
            "coverage_version_id",
            name="fhir_formulary_dataset_coverage_version_key",
        ),
    )
    __my_index_elements__ = ["dataset_id", "public_id"]

    dataset_id = Column(String(64), nullable=False)
    public_id = Column(String(31), nullable=False)
    coverage_version_id = Column(String(64), nullable=False)


class FHIRFormularyAlias(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_drug_plan_alias"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("alias_id"),
        UniqueConstraint(
            "public_id",
            "source_plan_identifier",
            name="fhir_formulary_alias_plan_key",
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
    public_id = Column(String(31), nullable=False)
    source_plan_identifier = Column(String(512), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyAliasVersion(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_drug_plan_alias_version"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("alias_version_id"),
        UniqueConstraint(
            "alias_id",
            "membership_hash",
            name="fhir_formulary_alias_version_membership_key",
        ),
    )
    __my_index_elements__ = ["alias_version_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("alias_id", "created_at"),
            "name": "fhir_formulary_alias_version_created_idx",
        },
    ]

    alias_version_id = Column(String(64), nullable=False)
    alias_id = Column(String(64), nullable=False)
    expected_count = Column(BigInteger, nullable=False)
    membership_count = Column(BigInteger, nullable=False)
    membership_hash = Column(String(64), nullable=False)
    cutoff_at = Column(TIMESTAMP(timezone=True), nullable=False)
    acquisition_mode = Column(String(32), nullable=False)
    reused_from_alias_version_id = Column(String(64))
    summary_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyDatasetAlias(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_dataset_alias"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id", "alias_id"),
    )
    __my_index_elements__ = ["dataset_id", "alias_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("dataset_id", "alias_version_id"),
            "name": "fhir_formulary_dataset_alias_version_idx",
        },
    ]

    dataset_id = Column(String(64), nullable=False)
    alias_id = Column(String(64), nullable=False)
    alias_version_id = Column(String(64), nullable=False)


class FHIRFormularyMedication(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_medication"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("medication_version_id"),
        UniqueConstraint(
            "source_id",
            "upstream_medication_id",
            "content_hash",
            name="fhir_formulary_medication_content_key",
        ),
    )
    __my_index_elements__ = ["medication_version_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("rxnorm_id",),
            "name": "fhir_formulary_medication_rxnorm_idx",
        },
        {
            "index_elements": ("ndc11",),
            "name": "fhir_formulary_medication_ndc11_idx",
        },
    ]

    medication_version_id = Column(String(64), nullable=False)
    source_id = Column(String(64), nullable=False)
    upstream_medication_id = Column(String(256), nullable=False)
    upstream_version_id = Column(String(256))
    upstream_last_updated = Column(TIMESTAMP(timezone=True))
    status = Column(String(32))
    drug_name = Column(TEXT)
    rxnorm_id = Column(String(64))
    ndc11 = Column(String(11))
    codings_json = Column(JSON, nullable=False)
    content_hash = Column(String(64), nullable=False)
    metadata_json = Column(JSON, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyAliasMembership(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_alias_membership"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("alias_version_id", "upstream_medication_id"),
    )
    __my_index_elements__ = ["alias_version_id", "upstream_medication_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("alias_version_id", "rxnorm_id"),
            "name": "fhir_formulary_membership_rxnorm_idx",
        },
    ]

    alias_version_id = Column(String(64), nullable=False)
    upstream_medication_id = Column(String(256), nullable=False)
    medication_version_id = Column(String(64), nullable=False)
    rxnorm_id = Column(String(64))
    drug_tier = Column(TEXT)
    prior_authorization = Column(Boolean)
    step_therapy = Column(Boolean)
    quantity_limit = Column(Boolean)
    variant_hash = Column(String(64), nullable=False)


class FHIRFormularyAlternative(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_alternative"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "alias_version_id",
            "upstream_medication_id",
            "raw_reference",
        ),
    )
    __my_index_elements__ = [
        "alias_version_id",
        "upstream_medication_id",
        "raw_reference",
    ]

    alias_version_id = Column(String(64), nullable=False)
    upstream_medication_id = Column(String(256), nullable=False)
    raw_reference = Column(TEXT, nullable=False)
    corrected_reference = Column(TEXT)
    resolved_medication_id = Column(String(256))
    resolved = Column(Boolean, nullable=False, default=False)
    rule_version = Column(String(64))
    evidence_json = Column(JSON, nullable=False, default=dict)


class FHIRFormularyCheckpoint(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id", "alias_id", "run_id"),
        CheckConstraint("fence_token > 0", name="fhir_formulary_checkpoint_fence_check"),
    )
    __my_index_elements__ = ["source_id", "alias_id", "run_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("run_id", "fence_token"),
            "name": "fhir_formulary_checkpoint_run_fence_idx",
        },
    ]

    source_id = Column(String(64), nullable=False)
    alias_id = Column(String(64), nullable=False)
    source_plan_identifier = Column(String(512), nullable=False)
    run_id = Column(String(64), nullable=False)
    dataset_id = Column(String(64), nullable=False)
    fence_token = Column(BigInteger, nullable=False)
    cutoff_at = Column(TIMESTAMP(timezone=True), nullable=False)
    acquisition_mode = Column(String(32), nullable=False)
    next_url = Column(TEXT)
    expected_count = Column(BigInteger)
    processed_count = Column(BigInteger, nullable=False, default=0)
    membership_hash = Column(String(64))
    completed = Column(Boolean, nullable=False, default=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False)
