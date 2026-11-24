# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compact snapshot data layout and completion state."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    LargeBinary,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.models.custom_import import _CustomImportModel, _table_args

__all__ = "CustomImportFamilyBlockLayout"


class CustomImportFamilyBlockLayout(_CustomImportModel):
    __tablename__ = "custom_import_family_block_layout"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint("family_id > 0 AND octet_length(definition_sha256) = 32"),
        CheckConstraint(
            "layout_contract = 'custom-import/family-block-layout/v1' AND jsonb_typeof(collection_slots) = 'object'"
        ),
        CheckConstraint(
            "(phase = 'loading' AND source_accepted_family_count IS NULL AND source_rejection_count IS NULL) OR "
            "(source_accepted_family_count IS NOT NULL AND source_rejection_count IS NOT NULL AND "
            "source_accepted_family_count >= 0 AND source_rejection_count >= 0)"
        ),
        CheckConstraint(
            "(phase IN ('loading','closed') AND source_record_count IS NULL AND role_counts IS NULL) OR "
            "(phase IN ('verified','logged') AND "
            "source_record_count IS NOT NULL AND source_record_count >= 0 AND role_counts IS NOT NULL AND jsonb_typeof(role_counts)='object')"
        ),
        CheckConstraint(
            "phase IN ('loading','closed','verified','logged') AND "
            "((phase='loading' AND closed_at IS NULL) OR (phase<>'loading' AND closed_at IS NOT NULL)) AND "
            "((phase IN ('loading','closed') AND validation_sha256 IS NULL) OR "
            "(phase IN ('verified','logged') AND validation_sha256 IS NOT NULL AND octet_length(validation_sha256) = 32))"
        ),
    )
    family_id = Column(BigInteger, primary_key=True, autoincrement=False)
    layout_contract = Column(String(63), nullable=False)
    definition_sha256 = Column(LargeBinary(32), nullable=False)
    collection_slots = Column(JSONB, nullable=False)
    phase = Column(String(16), nullable=False)
    source_accepted_family_count = Column(BigInteger)
    source_rejection_count = Column(BigInteger)
    closed_at = Column(TIMESTAMP(timezone=True))
    validation_sha256 = Column(LargeBinary(32))
    source_record_count = Column(BigInteger)
    role_counts = Column(JSONB)
