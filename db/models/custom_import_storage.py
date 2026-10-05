# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Low-volume registry for fixed, replaceable custom-import table families."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    Computed,
    ForeignKey,
    ForeignKeyConstraint,
    LargeBinary,
    SmallInteger,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import INT8MULTIRANGE, ExcludeConstraint

from db.models.custom_import import _CustomImportModel, _reference, _table_args, _timestamp_column

__all__ = ("CustomImportSnapshotFamily", "CustomImportSnapshotRelation", "CustomImportRevisionHome")


class CustomImportSnapshotFamily(_CustomImportModel):
    """One immutable producer binding and durable write-closure boundary."""

    __tablename__ = "custom_import_snapshot_family"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("execution_id", "producing_fence", name="custom_import_snapshot_attempt_key"),
        UniqueConstraint("generation_id", name="custom_import_snapshot_generation_key"),
        ForeignKeyConstraint(
            ["execution_id", "dataset_id", "definition_revision_id", "schema_revision_id", "capture_bundle_id"],
            [
                _reference("custom_import_execution", name)
                for name in (
                    "execution_id",
                    "dataset_id",
                    "definition_revision_id",
                    "schema_revision_id",
                    "capture_bundle_id",
                )
            ],
            name="custom_import_snapshot_execution_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "capture_bundle_id",
            ],
            [
                _reference("custom_import_generation", name)
                for name in (
                    "generation_id",
                    "dataset_id",
                    "definition_revision_id",
                    "schema_revision_id",
                    "execution_id",
                    "capture_bundle_id",
                )
            ],
            name="custom_import_snapshot_generation_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "producing_fence > 0 AND octet_length(producing_token_sha256) = 32",
            name="custom_import_snapshot_producer_shape",
        ),
        CheckConstraint(
            "(landing_table_oid IS NULL AND landing_table_owner IS NULL AND landing_columns_sha256 IS NULL) OR "
            "(landing_table_oid IS NOT NULL AND landing_table_owner IS NOT NULL AND landing_columns_sha256 IS NOT NULL "
            "AND landing_table_oid > 0 AND landing_table_owner > 0 AND octet_length(landing_columns_sha256) = 32)",
            name="custom_import_snapshot_landing_shape",
        ),
        CheckConstraint(
            "(origin_table_oid IS NULL AND origin_table_owner IS NULL AND origin_columns_sha256 IS NULL) OR "
            "(origin_table_oid IS NOT NULL AND origin_table_owner IS NOT NULL AND origin_columns_sha256 IS NOT NULL "
            "AND origin_table_oid > 0 AND origin_table_owner > 0 AND octet_length(origin_columns_sha256) = 32)",
            name="custom_import_snapshot_origin_shape",
        ),
    )

    family_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    producing_fence = Column(BigInteger, nullable=False)
    producing_token_sha256 = Column(LargeBinary(32), nullable=False)
    generation_id = Column(BigInteger)
    created_at = _timestamp_column()
    frozen_at = Column(TIMESTAMP(timezone=True))
    landing_table_oid = Column(BigInteger)
    landing_table_owner = Column(BigInteger)
    landing_columns_sha256 = Column(LargeBinary(32))
    origin_table_oid = Column(BigInteger)
    origin_table_owner = Column(BigInteger)
    origin_columns_sha256 = Column(LargeBinary(32))


class CustomImportSnapshotRelation(_CustomImportModel):
    """Exact native OID for one of the fixed fifteen model-defined tables."""

    __tablename__ = "custom_import_snapshot_relation"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("table_oid", name="custom_import_snapshot_relation_oid_key"),
        CheckConstraint(
            "relation_slot BETWEEN 1 AND 15 AND table_oid > 0", name="custom_import_snapshot_relation_shape"
        ),
        CheckConstraint(
            "table_owner > 0 AND octet_length(columns_sha256) = 32", name="custom_import_snapshot_layout_shape"
        ),
    )

    family_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_snapshot_family", "family_id"), ondelete="RESTRICT"),
        primary_key=True,
    )
    relation_slot = Column(SmallInteger, primary_key=True)
    table_oid = Column(BigInteger, nullable=False)
    table_owner = Column(BigInteger, nullable=False)
    columns_sha256 = Column(LargeBinary(32), nullable=False)


class CustomImportRevisionHome(_CustomImportModel):
    """Exact immutable ID sets from one fresh insertion batch and revision kind."""

    __tablename__ = "custom_import_revision_home"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint("revision_kind IN (1, 2)"),
        CheckConstraint("NOT isempty(revision_ids) AND revision_ids <@ int8multirange(int8range(1, NULL, '[)'))"),
        ExcludeConstraint(("revision_ids", "&&"), using="gist", where=text("revision_kind = 1")),
        ExcludeConstraint(("revision_ids", "&&"), using="gist", where=text("revision_kind = 2")),
    )

    family_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_snapshot_family", "family_id"), ondelete="RESTRICT"),
        nullable=False,
    )
    revision_kind = Column(SmallInteger, primary_key=True)
    revision_ids = Column(INT8MULTIRANGE, nullable=False)
    first_revision_id = Column(BigInteger, Computed("lower(revision_ids)", persisted=True), primary_key=True)
