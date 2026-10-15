# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded original objects retained separately from normalized Parquet parts."""

from sqlalchemy import BigInteger, CheckConstraint, Column, Index, LargeBinary, SmallInteger, String

from db.models.custom_import import _CustomImportModel, _table_args

__all__ = ("CustomImportCaptureRawObject",)

RAW_OBJECT_CONTRACT = "custom-import/raw-object/v1"
MAX_RAW_OBJECT_BYTES = 64 * 1024 * 1024


class CustomImportCaptureRawObject(_CustomImportModel):
    """One original stream object; its registrar validates complete bundle ownership."""

    __tablename__ = "custom_import_capture_raw_object"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint(
            "capture_bundle_id > 0 AND stream_slot > 0 AND dataset_id > 0 AND "
            "definition_revision_id > 0 AND schema_revision_id > 0",
            name="custom_import_capture_raw_object_identity_check",
        ),
        CheckConstraint(
            f"payload_contract = '{RAW_OBJECT_CONTRACT}' AND byte_count BETWEEN 0 AND {MAX_RAW_OBJECT_BYTES} AND "
            "octet_length(payload) = byte_count AND octet_length(payload_sha256) = 32 AND "
            "payload_sha256 = pg_catalog.sha256(payload)",
            name="custom_import_capture_raw_object_payload_check",
        ),
        Index("custom_import_capture_raw_object_dataset_idx", "dataset_id"),
    )

    capture_bundle_id = Column(BigInteger, primary_key=True, autoincrement=False)
    stream_slot = Column(SmallInteger, primary_key=True, autoincrement=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    payload_contract = Column(String(63), nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    payload = Column(LargeBinary, nullable=False)
    payload_sha256 = Column(LargeBinary(32), nullable=False)
