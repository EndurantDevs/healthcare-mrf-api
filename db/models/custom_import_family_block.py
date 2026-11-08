# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only snapshot layout and native relation custody; no per-family JSON."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    LargeBinary,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.models.custom_import import _CustomImportModel, _table_args

__all__ = ("CustomImportFamilyBlockLayout", "CustomImportFamilyBlockRelation")


class CustomImportFamilyBlockLayout(_CustomImportModel):
    __tablename__ = "custom_import_family_block_layout"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint("family_id > 0 AND octet_length(definition_sha256) = 32 AND octet_length(layout_sha256) = 32"),
        CheckConstraint(
            "layout_contract = 'custom-import/family-block-layout/v1' AND jsonb_typeof(collection_slots) = 'object'"
        ),
        CheckConstraint(
            "(owner_checkpoint_revision IS NULL AND owner_checkpoint_sha256 IS NULL) OR "
            "(owner_checkpoint_revision IS NOT NULL AND owner_checkpoint_revision > 0 AND "
            "owner_checkpoint_sha256 IS NOT NULL AND octet_length(owner_checkpoint_sha256) = 32)"
        ),
        CheckConstraint(
            "(phase = 'loading' AND source_accepted_family_count IS NULL AND source_rejection_count IS NULL) OR "
            "(source_accepted_family_count IS NOT NULL AND source_rejection_count IS NOT NULL AND "
            "source_accepted_family_count >= 0 AND source_rejection_count >= 0)"
        ),
        CheckConstraint(
            "(phase IN ('loading','closed') AND expected_completed_shards IS NULL AND source_record_count IS NULL AND role_counts IS NULL) OR "
            "(phase IN ('verified','logged') AND expected_completed_shards IS NOT NULL AND expected_completed_shards > 0 AND "
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
    layout_sha256 = Column(LargeBinary(32), nullable=False)
    collection_slots = Column(JSONB, nullable=False)
    phase = Column(String(16), nullable=False)
    owner_checkpoint_revision = Column(BigInteger)
    owner_checkpoint_sha256 = Column(LargeBinary(32))
    source_accepted_family_count = Column(BigInteger)
    source_rejection_count = Column(BigInteger)
    closed_at = Column(TIMESTAMP(timezone=True))
    validation_sha256 = Column(LargeBinary(32))
    expected_completed_shards = Column(BigInteger)
    source_record_count = Column(BigInteger)
    role_counts = Column(JSONB)


class CustomImportFamilyBlockRelation(_CustomImportModel):
    __tablename__ = "custom_import_family_block_relation"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("table_oid"),
        UniqueConstraint("family_id", "role_name"),
        CheckConstraint(
            "family_id > 0 AND role_slot IN (1,2,3,4,9,12,101,102,103,104,105,106,107,108) AND "
            "table_oid > 0 AND table_owner > 0 AND octet_length(columns_sha256) = 32 AND octet_length(constraints_sha256) = 32"
        ),
    )
    family_id = Column(BigInteger, primary_key=True, autoincrement=False)
    role_slot = Column(SmallInteger, primary_key=True)
    role_name = Column(Text, nullable=False)
    table_oid = Column(BigInteger, nullable=False)
    table_owner = Column(BigInteger, nullable=False)
    columns_sha256 = Column(LargeBinary(32), nullable=False)
    constraints_sha256 = Column(LargeBinary(32), nullable=False)
