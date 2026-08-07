# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""FHIR formulary medication, membership, and checkpoint models."""

from __future__ import annotations

from sqlalchemy import (
    TEXT,
    TIMESTAMP,
    BigInteger,
    Boolean,
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
    "FHIRFormularyAliasMembership",
    "FHIRFormularyAlternative",
    "FHIRFormularyCheckpoint",
    "FHIRFormularyMedication",
)


class FHIRFormularyMedication(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_medication"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("medication_version_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        UniqueConstraint(
            "source_id",
            "upstream_medication_id",
            "content_hash",
            name="fhir_formulary_medication_content_key",
        ),
        UniqueConstraint(
            "source_id",
            "medication_version_id",
            name="fhir_formulary_medication_source_version_key",
        ),
        UniqueConstraint(
            "source_id",
            "upstream_medication_id",
            "medication_version_id",
            name="fhir_formulary_medication_membership_owner_key",
        ),
        CheckConstraint(
            "ndc11 IS NULL OR ndc11 ~ '^[0-9]{11}$'",
            name="fhir_formulary_medication_ndc11_check",
        ),
        CheckConstraint(
            "jsonb_typeof(codings_json) = 'array'",
            name="fhir_formulary_medication_codings_check",
        ),
        CheckConstraint(
            "jsonb_typeof(metadata_json) = 'object'",
            name="fhir_formulary_medication_metadata_check",
        ),
    )
    __my_index_elements__ = ["medication_version_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("rxnorm_id",),
            "name": "fhir_formulary_medication_rxnorm_idx",
            "where": "rxnorm_id IS NOT NULL",
        },
        {
            "index_elements": ("ndc11",),
            "name": "fhir_formulary_medication_ndc11_idx",
            "where": "ndc11 IS NOT NULL",
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
    codings_json = Column(JSONB, nullable=False)
    content_hash = Column(String(64), nullable=False)
    metadata_json = Column(JSONB, nullable=False, default=dict)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularyAliasMembership(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_alias_membership"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("alias_version_id", "upstream_medication_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        ForeignKeyConstraint(
            ["source_id", "alias_version_id"],
            [
                _reference(
                    "fhir_formulary_drug_plan_alias_version",
                    "source_id",
                ),
                _reference(
                    "fhir_formulary_drug_plan_alias_version",
                    "alias_version_id",
                ),
            ],
            name="fhir_formulary_membership_alias_owner_fkey",
        ),
        ForeignKeyConstraint(
            [
                "source_id",
                "upstream_medication_id",
                "medication_version_id",
            ],
            [
                _reference("fhir_formulary_medication", "source_id"),
                _reference(
                    "fhir_formulary_medication",
                    "upstream_medication_id",
                ),
                _reference("fhir_formulary_medication", "medication_version_id"),
            ],
            name="fhir_formulary_membership_medication_owner_fkey",
        ),
    )
    __my_index_elements__ = ["alias_version_id", "upstream_medication_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("alias_version_id", "rxnorm_id"),
            "name": "fhir_formulary_membership_rxnorm_idx",
            "where": "rxnorm_id IS NOT NULL",
        },
    ]

    source_id = Column(String(64), nullable=False)
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
        ForeignKeyConstraint(
            ["alias_version_id", "upstream_medication_id"],
            [
                _reference("fhir_formulary_alias_membership", "alias_version_id"),
                _reference(
                    "fhir_formulary_alias_membership",
                    "upstream_medication_id",
                ),
            ],
            name="fhir_formulary_alternative_membership_fkey",
        ),
        ForeignKeyConstraint(
            ["alias_version_id", "resolved_medication_id"],
            [
                _reference("fhir_formulary_alias_membership", "alias_version_id"),
                _reference(
                    "fhir_formulary_alias_membership",
                    "upstream_medication_id",
                ),
            ],
            name="fhir_formulary_alternative_target_owner_fkey",
        ),
        CheckConstraint(
            "(resolved AND resolved_medication_id IS NOT NULL) OR "
            "(NOT resolved AND resolved_medication_id IS NULL)",
            name="fhir_formulary_alternative_resolution_check",
        ),
        CheckConstraint(
            "jsonb_typeof(evidence_json) = 'object'",
            name="fhir_formulary_alternative_evidence_check",
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
    evidence_json = Column(JSONB, nullable=False, default=dict)


class FHIRFormularyCheckpoint(Base, JSONOutputMixin):
    __tablename__ = "fhir_formulary_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id", "alias_id", "run_id"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
        ),
        ForeignKeyConstraint(
            ["source_id", "dataset_id", "run_id"],
            [
                _reference("fhir_formulary_dataset", "source_id"),
                _reference("fhir_formulary_dataset", "dataset_id"),
                _reference("fhir_formulary_dataset", "run_id"),
            ],
            name="fhir_formulary_checkpoint_dataset_owner_fkey",
        ),
        ForeignKeyConstraint(
            ["source_id", "alias_id", "source_plan_identifier"],
            [
                _reference("fhir_formulary_drug_plan_alias", "source_id"),
                _reference("fhir_formulary_drug_plan_alias", "alias_id"),
                _reference(
                    "fhir_formulary_drug_plan_alias",
                    "source_plan_identifier",
                ),
            ],
            name="fhir_formulary_checkpoint_alias_owner_fkey",
        ),
        CheckConstraint(
            "fence_token > 0",
            name="fhir_formulary_checkpoint_fence_check",
        ),
        CheckConstraint(
            "acquisition_mode IN ('full', 'delta', 'reuse')",
            name="fhir_formulary_checkpoint_mode_check",
        ),
        CheckConstraint(
            "processed_count >= 0 AND (expected_count IS NULL OR "
            "(expected_count >= 0 AND processed_count <= expected_count))",
            name="fhir_formulary_checkpoint_count_check",
        ),
        CheckConstraint(
            "NOT completed OR (expected_count IS NOT NULL AND "
            "processed_count = expected_count AND "
            "membership_hash IS NOT NULL AND "
            "membership_hash ~ '^[0-9a-f]{64}$')",
            name="fhir_formulary_checkpoint_completion_check",
        ),
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
    expected_count = Column(BigInteger)
    processed_count = Column(BigInteger, nullable=False, default=0)
    membership_hash = Column(String(64))
    completed = Column(Boolean, nullable=False, default=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False)
