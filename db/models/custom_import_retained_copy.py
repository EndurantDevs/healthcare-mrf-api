# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Small retained-copy provenance and active source pins, without foreign keys."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)

from db.models.custom_import import _CustomImportModel, _table_args

__all__ = ("CustomImportRetainedCaptureCopy",)


class CustomImportRetainedCaptureCopy(_CustomImportModel):
    __tablename__ = "custom_import_retained_capture_copy"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("execution_id", name="custom_import_retained_copy_execution_key"),
        CheckConstraint(
            "copy_id ~ '^[0-9a-f]{64}$' AND dataset_id>0 AND execution_id>0 AND source_execution_id>0 "
            "AND source_capture_bundle_id>0 AND capture_bundle_id>0 AND source_fence>0 AND fence=1 "
            "AND source_capture_bundle_id<>capture_bundle_id AND source_execution_id<>execution_id "
            "AND octet_length(source_token_sha256)=32 AND octet_length(source_manifest_sha256)=32",
            name="custom_import_retained_copy_identity_check",
        ),
        CheckConstraint(
            "octet_length(canonical_authorization) BETWEEN 2 AND 16384 "
            "AND copy_id=encode(sha256(convert_to(canonical_authorization,'UTF8')),'hex') "
            "AND jsonb_typeof(canonical_authorization::jsonb)='object'",
            name="custom_import_retained_copy_authorization_check",
        ),
        CheckConstraint(
            "phase IN ('active','sealed','closed') AND source_pin_active=(phase='active') "
            "AND ((root_device IS NULL)=(root_inode IS NULL)) AND (root_device IS NULL OR (root_device>0 AND root_inode>0)) "
            "AND (cleanup_started_at IS NULL OR (root_device IS NOT NULL AND phase<>'active')) "
            "AND (first_job_uid IS NULL OR first_job_uid ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND (terminal_job_state IS NULL OR (first_job_uid IS NOT NULL AND terminal_job_state IN ('succeeded','failed'))) "
            "AND (pods_stopped_at IS NULL OR first_job_uid IS NOT NULL) "
            "AND (cleaned_at IS NULL OR (phase<>'active' AND (pods_stopped_at IS NOT NULL OR launch_sent_at IS NULL)))",
            name="custom_import_retained_copy_phase_check",
        ),
    )
    copy_id = Column(String(64), primary_key=True)
    canonical_authorization = Column(Text, nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    fence = Column(BigInteger, nullable=False)
    source_execution_id = Column(BigInteger, nullable=False)
    source_fence = Column(BigInteger, nullable=False)
    source_token_sha256 = Column(LargeBinary(32), nullable=False)
    source_capture_bundle_id = Column(BigInteger, nullable=False)
    source_manifest_sha256 = Column(LargeBinary(32), nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    phase = Column(String(16), nullable=False)
    source_pin_active = Column(Boolean, nullable=False)
    root_device = Column(BigInteger)
    root_inode = Column(BigInteger)
    cleanup_started_at = Column(TIMESTAMP(timezone=True))
    launch_sent_at = Column(TIMESTAMP(timezone=True))
    first_job_uid = Column(String(128))
    terminal_job_state = Column(String(16))
    pods_stopped_at = Column(TIMESTAMP(timezone=True))
    cleaned_at = Column(TIMESTAMP(timezone=True))
