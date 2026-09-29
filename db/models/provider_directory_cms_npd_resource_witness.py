# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete CMS bulk FHIR witnesses for one admitted dataset and release."""

import os

from sqlalchemy import CheckConstraint, Column, ForeignKeyConstraint, PrimaryKeyConstraint, String
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base

__all__ = ("ProviderDirectoryCMSNPDResourceWitness",)

_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


class ProviderDirectoryCMSNPDResourceWitness(Base):
    """Raw source facts kept separate from the public normalized projection."""

    __tablename__ = "provider_directory_cms_npd_resource_witness"
    __table_args__ = (
        PrimaryKeyConstraint("dataset_id", "resource_type", "resource_id"),
        ForeignKeyConstraint(
            ("dataset_id", "resource_type", "resource_id"),
            (
                f"{_SCHEMA}.provider_directory_dataset_resource.dataset_id",
                f"{_SCHEMA}.provider_directory_dataset_resource.resource_type",
                f"{_SCHEMA}.provider_directory_dataset_resource.resource_id",
            ),
            ondelete="CASCADE",
        ),
        CheckConstraint("source_id = 'cms-npd'", name="cms_npd_witness_source_check"),
        CheckConstraint("release_id ~ '^[0-9a-f]{64}$'", name="cms_npd_witness_release_check"),
        CheckConstraint("raw_payload_sha256 ~ '^[0-9a-f]{64}$'", name="cms_npd_witness_raw_hash_check"),
        CheckConstraint("normalized_payload_hash ~ '^[0-9a-f]{64}$'", name="cms_npd_witness_normalized_hash_check"),
        {"schema": _SCHEMA},
    )

    dataset_id = Column(String(96), nullable=False)
    source_id = Column(String(64), nullable=False)
    release_id = Column(String(64), nullable=False)
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(256), nullable=False)
    raw_payload_sha256 = Column(String(64), nullable=False)
    normalized_payload_hash = Column(String(64), nullable=False)
    raw_payload_json = Column(JSONB, nullable=False)
