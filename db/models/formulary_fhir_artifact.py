# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable retained source-artifact models for formulary imports."""

from __future__ import annotations

from sqlalchemy import (
    TEXT,
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)

from db.connection import Base
from db.json_mixin import JSONOutputMixin
from db.models.formulary_fhir import _reference, _table_args


__all__ = (
    "FHIRFormularySourceArtifact",
    "FHIRFormularySourceArtifactObservation",
    "FHIRFormularySourceArtifactSet",
)


class FHIRFormularySourceArtifactSet(Base, JSONOutputMixin):
    """Immutable header binding one exact source file set and its census."""

    __tablename__ = "fhir_formulary_source_artifact_set"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id", "source_file_set_sha256"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
            name="fhir_formulary_source_artifact_set_source_fkey",
        ),
        UniqueConstraint(
            "source_id",
            "source_file_set_sha256",
            "raw_listing_projection_sha256",
            name="fhir_formulary_source_artifact_set_projection_key",
        ),
        CheckConstraint(
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "raw_listing_projection_sha256 ~ '^[0-9a-f]{64}$' AND "
            "expected_file_count > 0 AND expected_file_count <= 100000",
            name="fhir_formulary_source_artifact_set_identity_check",
        ),
    )
    __my_index_elements__ = ["source_id", "source_file_set_sha256"]

    source_id = Column(String(64), nullable=False)
    source_file_set_sha256 = Column(String(64), nullable=False)
    raw_listing_projection_sha256 = Column(String(64), nullable=False)
    expected_file_count = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularySourceArtifactObservation(Base, JSONOutputMixin):
    """Immutable mapping from retained listing bytes to one file set."""

    __tablename__ = "fhir_formulary_source_artifact_observation"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("source_id", "source_observation_sha256"),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
            name="fhir_formulary_source_artifact_observation_source_fkey",
        ),
        ForeignKeyConstraint(
            [
                "source_id",
                "source_file_set_sha256",
                "raw_listing_projection_sha256",
            ],
            [
                _reference("fhir_formulary_source_artifact_set", "source_id"),
                _reference(
                    "fhir_formulary_source_artifact_set",
                    "source_file_set_sha256",
                ),
                _reference(
                    "fhir_formulary_source_artifact_set",
                    "raw_listing_projection_sha256",
                ),
            ],
            name="fhir_formulary_source_artifact_observation_set_fkey",
        ),
        CheckConstraint(
            "source_observation_sha256 ~ '^[0-9a-f]{64}$' AND "
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "raw_listing_projection_sha256 ~ '^[0-9a-f]{64}$'",
            name="fhir_formulary_source_artifact_observation_identity_check",
        ),
    )
    __my_index_elements__ = ["source_id", "source_observation_sha256"]

    source_id = Column(String(64), nullable=False)
    source_observation_sha256 = Column(String(64), nullable=False)
    source_file_set_sha256 = Column(String(64), nullable=False)
    raw_listing_projection_sha256 = Column(String(64), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class FHIRFormularySourceArtifact(Base, JSONOutputMixin):
    """Immutable source-file identity with one pending-to-verified fill."""

    __tablename__ = "fhir_formulary_source_artifact"
    __main_table__ = __tablename__
    EXCLUDE_FIELDS = ("source_url",)
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "source_id",
            "source_file_set_sha256",
            "source_file_id",
        ),
        ForeignKeyConstraint(
            ["source_id"],
            [_reference("fhir_formulary_source", "source_id")],
            name="fhir_formulary_source_artifact_source_fkey",
        ),
        ForeignKeyConstraint(
            [
                "source_id",
                "source_file_set_sha256",
                "raw_listing_projection_sha256",
            ],
            [
                _reference("fhir_formulary_source_artifact_set", "source_id"),
                _reference(
                    "fhir_formulary_source_artifact_set",
                    "source_file_set_sha256",
                ),
                _reference(
                    "fhir_formulary_source_artifact_set",
                    "raw_listing_projection_sha256",
                ),
            ],
            name="fhir_formulary_source_artifact_set_fkey",
        ),
        UniqueConstraint(
            "source_id",
            "source_file_set_sha256",
            "family",
            "file_name",
            name="fhir_formulary_source_artifact_logical_key",
        ),
        CheckConstraint(
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "source_file_id ~ '^[0-9a-f]{64}$' AND "
            "raw_listing_projection_sha256 ~ '^[0-9a-f]{64}$' AND "
            "catalog_entry_sha256 ~ '^[0-9a-f]{64}$' AND "
            "family ~ '^[a-z0-9][a-z0-9_-]{0,31}$' AND "
            "length(file_name) > 0 AND length(source_url) > 0 AND "
            "length(catalog_modified_at) > 0 AND "
            "(expected_byte_count IS NULL OR expected_byte_count > 0)",
            name="fhir_formulary_source_artifact_identity_check",
        ),
        CheckConstraint(
            "(status = 'pending' AND artifact_sha256 IS NULL AND "
            "artifact_byte_count IS NULL AND verified_at IS NULL) OR "
            "(status = 'verified' AND "
            "artifact_sha256 ~ '^[0-9a-f]{64}$' AND "
            "artifact_byte_count > 0 AND verified_at IS NOT NULL AND "
            "(expected_byte_count IS NULL OR "
            "artifact_byte_count = expected_byte_count))",
            name="fhir_formulary_source_artifact_state_check",
        ),
    )
    __my_index_elements__ = ["source_id", "source_file_set_sha256"]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "source_id",
                "source_file_set_sha256",
                "status",
                "family",
            ),
            "name": "fhir_formulary_source_artifact_pending_idx",
        }
    ]

    source_id = Column(String(64), nullable=False)
    source_file_set_sha256 = Column(String(64), nullable=False)
    source_file_id = Column(String(64), nullable=False)
    raw_listing_projection_sha256 = Column(String(64), nullable=False)
    family = Column(String(32), nullable=False)
    file_name = Column(String(256), nullable=False)
    source_url = Column(TEXT, nullable=False)
    catalog_modified_at = Column(String(64), nullable=False)
    catalog_entry_sha256 = Column(String(64), nullable=False)
    expected_byte_count = Column(BigInteger)
    artifact_sha256 = Column(String(64))
    artifact_byte_count = Column(BigInteger)
    status = Column(String(16), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    verified_at = Column(TIMESTAMP(timezone=True))
