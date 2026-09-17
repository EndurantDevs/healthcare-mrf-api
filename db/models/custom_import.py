# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Migration-owned, source-neutral storage for ``custom-import/v1``.

Canonical definition, provenance, and payload text is retained separately from
typed hot-path projections.  The relations are registered with SQLAlchemy for
Alembic, but are deliberately excluded from the legacy runtime DDL synchronizer.
"""

from __future__ import annotations

import os

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = (
    "CustomImportCapture",
    "CustomImportCaptureBundle",
    "CustomImportChildCollection",
    "CustomImportChildRevision",
    "CustomImportChildScalar",
    "CustomImportCurrentGeneration",
    "CustomImportDataset",
    "CustomImportDefinitionRevision",
    "CustomImportEntityBinding",
    "CustomImportExecution",
    "CustomImportFamilyChild",
    "CustomImportFamilyRevision",
    "CustomImportField",
    "CustomImportFieldAlias",
    "CustomImportFieldSlot",
    "CustomImportGeneration",
    "CustomImportGenerationSeal",
    "CustomImportGenerationFamily",
    "CustomImportLease",
    "CustomImportNoChangeSeal",
    "CustomImportPack",
    "CustomImportPublicationEvent",
    "CustomImportRejection",
    "CustomImportRootRecord",
    "CustomImportRootRevision",
    "CustomImportRootScalar",
    "CustomImportSchemaRevision",
    "CustomImportSelectionProfile",
    "CustomImportSourceStream",
    "CustomImportWinner",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()


def _reference(table_name: str, column_name: str) -> str:
    return f"{_SCHEMA}.{table_name}.{column_name}"


def _table_args(*constraints):
    return (*constraints, {"schema": _SCHEMA, "extend_existing": True})


def _timestamp_column():
    return Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


def _sha256_check(column: str) -> str:
    return f"octet_length({column}) = 32"


def _scalar_check(name: str, *, root: bool = False) -> CheckConstraint:
    typed_value = (
        "((field_type = 'string' AND string_value IS NOT NULL AND integer_value IS NULL AND "
        "decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR "
        "(field_type = 'integer' AND string_value IS NULL AND integer_value IS NOT NULL AND "
        "decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR "
        "(field_type = 'decimal' AND string_value IS NULL AND integer_value IS NULL AND "
        "decimal_value IS NOT NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL) OR "
        "(field_type = 'boolean' AND string_value IS NULL AND integer_value IS NULL AND "
        "decimal_value IS NULL AND boolean_value IS NOT NULL AND date_value IS NULL AND timestamp_value IS NULL) OR "
        "(field_type = 'date' AND string_value IS NULL AND integer_value IS NULL AND "
        "decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NOT NULL AND timestamp_value IS NULL) OR "
        "(field_type = 'timestamp' AND string_value IS NULL AND integer_value IS NULL AND "
        "decimal_value IS NULL AND boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NOT NULL))"
    )
    null_value = (
        "string_value IS NULL AND integer_value IS NULL AND decimal_value IS NULL AND "
        "boolean_value IS NULL AND date_value IS NULL AND timestamp_value IS NULL"
    )
    scope = "field_collection_slot = 0 AND " if root else "field_collection_slot = collection_slot AND "
    return CheckConstraint(
        scope
        + "projection_slot > 0 AND value_state IN ('value', 'null') AND ((value_state = 'value' AND "
        + typed_value
        + ") OR (value_state = 'null' AND "
        + null_value
        + "))",
        name=name,
    )


class _CustomImportModel(Base, JSONOutputMixin):
    """Common opt-out for schema relations managed exclusively by Alembic."""

    __abstract__ = True
    __runtime_schema_sync__ = False


class CustomImportDataset(_CustomImportModel):
    """Stable engine-local identity for one generic dataset."""

    __tablename__ = "custom_import_dataset"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id", name="custom_import_dataset_pkey"),
        UniqueConstraint("dataset_key", name="custom_import_dataset_key"),
        CheckConstraint(
            "dataset_key ~ '^[a-z][a-z0-9_]{0,62}$'",
            name="custom_import_dataset_key_check",
        ),
    )

    dataset_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_key = Column(String(63), nullable=False)
    created_at = _timestamp_column()


class CustomImportSchemaRevision(_CustomImportModel):
    """Immutable field/key/relationship shape for a dataset revision."""

    __tablename__ = "custom_import_schema_revision"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("schema_revision_id", name="custom_import_schema_rev_pkey"),
        UniqueConstraint("dataset_id", "revision_number", name="custom_import_schema_rev_number_key"),
        UniqueConstraint("dataset_id", "schema_sha256", name="custom_import_schema_rev_hash_key"),
        UniqueConstraint(
            "schema_revision_id",
            "dataset_id",
            name="custom_import_schema_rev_owner_key",
        ),
        CheckConstraint(
            "revision_number > 0 AND " + _sha256_check("schema_sha256"),
            name="custom_import_schema_rev_shape_check",
        ),
    )

    schema_revision_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_dataset", "dataset_id"), ondelete="RESTRICT"),
        nullable=False,
    )
    revision_number = Column(Integer, nullable=False)
    canonical_schema = Column(Text, nullable=False)
    schema_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportFieldSlot(_CustomImportModel):
    """Stable dataset-level field identity retained across schema revisions."""

    __tablename__ = "custom_import_field_slot"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id", "field_slot", name="custom_import_field_slot_pkey"),
        UniqueConstraint("dataset_id", "field_id", name="custom_import_field_slot_id_key"),
        CheckConstraint(
            "field_slot > 0 AND field_slot < 32768 AND field_id ~ '^[a-z][a-z0-9_]{0,62}$'",
            name="custom_import_field_slot_shape_check",
        ),
    )

    dataset_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_dataset", "dataset_id"), ondelete="RESTRICT"),
        primary_key=True,
    )
    field_slot = Column(SmallInteger, primary_key=True)
    field_id = Column(String(63), nullable=False)
    created_at = _timestamp_column()


class CustomImportChildCollection(_CustomImportModel):
    """One named child collection in an immutable schema revision."""

    __tablename__ = "custom_import_child_collection"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "schema_revision_id",
            "collection_slot",
            name="custom_import_child_collection_pkey",
        ),
        UniqueConstraint(
            "schema_revision_id",
            "collection_name",
            name="custom_import_child_collection_name_key",
        ),
        UniqueConstraint(
            "schema_revision_id",
            "dataset_id",
            "collection_slot",
            name="custom_import_child_collection_owner_key",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id"],
            [
                _reference("custom_import_schema_revision", "schema_revision_id"),
                _reference("custom_import_schema_revision", "dataset_id"),
            ],
            name="custom_import_child_collection_schema_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "collection_slot > 0 AND collection_name ~ '^[a-z][a-z0-9_]{0,62}$' AND "
            + _sha256_check("key_shape_sha256"),
            name="custom_import_child_collection_shape_check",
        ),
    )

    schema_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    collection_slot = Column(SmallInteger, primary_key=True)
    collection_name = Column(String(63), nullable=False)
    canonical_key_shape = Column(Text, nullable=False)
    key_shape_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportField(_CustomImportModel):
    """A field's type and optional hot projection slot in one schema revision."""

    __tablename__ = "custom_import_field"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("schema_revision_id", "field_slot", name="custom_import_field_pkey"),
        UniqueConstraint(
            "schema_revision_id",
            "collection_slot",
            "field_name",
            name="custom_import_field_name_key",
        ),
        UniqueConstraint(
            "schema_revision_id",
            "dataset_id",
            "field_slot",
            name="custom_import_field_owner_key",
        ),
        UniqueConstraint(
            "schema_revision_id",
            "dataset_id",
            "field_slot",
            "field_type",
            "collection_slot",
            "projection_slot",
            name="custom_import_field_projection_owner_key",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id"],
            [
                _reference("custom_import_schema_revision", "schema_revision_id"),
                _reference("custom_import_schema_revision", "dataset_id"),
            ],
            name="custom_import_field_schema_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["dataset_id", "field_slot"],
            [
                _reference("custom_import_field_slot", "dataset_id"),
                _reference("custom_import_field_slot", "field_slot"),
            ],
            name="custom_import_field_slot_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "collection_slot >= 0 AND "
            "field_name ~ '^[a-z][a-z0-9_]{0,62}$' AND "
            "field_type IN ('string', 'integer', 'decimal', 'boolean', 'date', 'timestamp') AND "
            "projection_slot >= 0 AND projection_slot <= 20",
            name="custom_import_field_shape_check",
        ),
    )

    schema_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    field_slot = Column(SmallInteger, primary_key=True)
    collection_slot = Column(SmallInteger, nullable=False, server_default=text("0"))
    field_name = Column(String(63), nullable=False)
    field_type = Column(String(16), nullable=False)
    is_nullable = Column(Boolean, nullable=False)
    projection_slot = Column(SmallInteger, nullable=False, server_default=text("0"))
    created_at = _timestamp_column()


class CustomImportDefinitionRevision(_CustomImportModel):
    """Immutable source/query/selection revision bound to one schema revision."""

    __tablename__ = "custom_import_definition_revision"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("definition_revision_id", name="custom_import_definition_rev_pkey"),
        UniqueConstraint(
            "dataset_id",
            "revision_number",
            name="custom_import_definition_rev_number_key",
        ),
        UniqueConstraint(
            "dataset_id",
            "definition_sha256",
            name="custom_import_definition_rev_hash_key",
        ),
        UniqueConstraint(
            "definition_revision_id",
            "dataset_id",
            "schema_revision_id",
            name="custom_import_definition_rev_owner_key",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id"],
            [
                _reference("custom_import_schema_revision", "schema_revision_id"),
                _reference("custom_import_schema_revision", "dataset_id"),
            ],
            name="custom_import_definition_rev_schema_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "contract_version = 'custom-import/v1' AND revision_number > 0 AND "
            "refresh_mode IN ('upsert', 'snapshot') AND " + _sha256_check("definition_sha256"),
            name="custom_import_definition_rev_shape_check",
        ),
    )

    definition_revision_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    revision_number = Column(Integer, nullable=False)
    contract_version = Column(String(32), nullable=False)
    refresh_mode = Column(String(16), nullable=False)
    canonical_definition = Column(Text, nullable=False)
    definition_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportSourceStream(_CustomImportModel):
    """Declarative source shape without connection credentials or executable input."""

    __tablename__ = "custom_import_source_stream"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "definition_revision_id",
            "stream_slot",
            name="custom_import_source_stream_pkey",
        ),
        UniqueConstraint(
            "definition_revision_id",
            "stream_id",
            name="custom_import_source_stream_id_key",
        ),
        UniqueConstraint(
            "definition_revision_id",
            "dataset_id",
            "schema_revision_id",
            "stream_slot",
            name="custom_import_source_stream_owner_key",
        ),
        ForeignKeyConstraint(
            ["definition_revision_id", "dataset_id", "schema_revision_id"],
            [
                _reference("custom_import_definition_revision", "definition_revision_id"),
                _reference("custom_import_definition_revision", "dataset_id"),
                _reference("custom_import_definition_revision", "schema_revision_id"),
            ],
            name="custom_import_source_stream_definition_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id", "collection_slot"],
            [
                _reference("custom_import_child_collection", "schema_revision_id"),
                _reference("custom_import_child_collection", "dataset_id"),
                _reference("custom_import_child_collection", "collection_slot"),
            ],
            name="custom_import_source_stream_collection_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "stream_slot > 0 AND "
            "stream_id ~ '^[a-z][a-z0-9_]{0,62}$' AND "
            "record_kind IN ('root', 'child') AND "
            "decoder IN ('csv', 'tsv', 'json', 'ndjson', 'xml', 'parquet') AND "
            "compression IN ('none', 'gzip') AND "
            "((record_kind = 'root' AND collection_slot IS NULL) OR "
            "(record_kind = 'child' AND collection_slot IS NOT NULL))",
            name="custom_import_source_stream_shape_check",
        ),
        CheckConstraint(
            "((decoder = 'xml' AND record_path IS NOT NULL AND "
            "record_path ~ '^[a-z][a-z0-9_]{0,62}$') OR "
            "(decoder <> 'xml' AND record_path IS NULL))",
            name="custom_import_source_stream_record_path_check",
        ),
    )

    definition_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    stream_slot = Column(SmallInteger, primary_key=True)
    stream_id = Column(String(63), nullable=False)
    record_kind = Column(String(8), nullable=False)
    collection_slot = Column(SmallInteger)
    decoder = Column(String(16), nullable=False)
    compression = Column(String(8), nullable=False)
    snapshot_token_selector = Column(String(255), nullable=False)
    record_path = Column(String(63))
    created_at = _timestamp_column()


class CustomImportFieldAlias(_CustomImportModel):
    """Exact per-stream external alias to a stable field slot."""

    __tablename__ = "custom_import_field_alias"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "definition_revision_id",
            "stream_slot",
            "alias_name",
            name="custom_import_field_alias_pkey",
        ),
        ForeignKeyConstraint(
            [
                "definition_revision_id",
                "dataset_id",
                "schema_revision_id",
                "stream_slot",
            ],
            [
                _reference("custom_import_source_stream", "definition_revision_id"),
                _reference("custom_import_source_stream", "dataset_id"),
                _reference("custom_import_source_stream", "schema_revision_id"),
                _reference("custom_import_source_stream", "stream_slot"),
            ],
            name="custom_import_field_alias_stream_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id", "field_slot"],
            [
                _reference("custom_import_field", "schema_revision_id"),
                _reference("custom_import_field", "dataset_id"),
                _reference("custom_import_field", "field_slot"),
            ],
            name="custom_import_field_alias_field_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "octet_length(alias_name) > 0 AND octet_length(alias_name) <= 255 AND alias_name !~ '[[:cntrl:]]'",
            name="custom_import_field_alias_shape_check",
        ),
    )

    definition_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    stream_slot = Column(SmallInteger, primary_key=True)
    alias_name = Column(String(255), primary_key=True)
    field_slot = Column(SmallInteger, nullable=False)
    created_at = _timestamp_column()


class CustomImportSelectionProfile(_CustomImportModel):
    """Immutable bounded winner-selection profile for one definition revision."""

    __tablename__ = "custom_import_selection_profile"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "definition_revision_id",
            "profile_slot",
            name="custom_import_selection_profile_pkey",
        ),
        UniqueConstraint(
            "definition_revision_id",
            "profile_id",
            name="custom_import_selection_profile_id_key",
        ),
        UniqueConstraint(
            "definition_revision_id",
            "dataset_id",
            "schema_revision_id",
            "profile_slot",
            name="custom_import_selection_profile_owner_key",
        ),
        ForeignKeyConstraint(
            ["definition_revision_id", "dataset_id", "schema_revision_id"],
            [
                _reference("custom_import_definition_revision", "definition_revision_id"),
                _reference("custom_import_definition_revision", "dataset_id"),
                _reference("custom_import_definition_revision", "schema_revision_id"),
            ],
            name="custom_import_selection_profile_definition_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id", "context_collection_slot"],
            [
                _reference("custom_import_child_collection", "schema_revision_id"),
                _reference("custom_import_child_collection", "dataset_id"),
                _reference("custom_import_child_collection", "collection_slot"),
            ],
            name="custom_import_selection_profile_context_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "profile_slot > 0 AND profile_slot <= 4 AND "
            "profile_id ~ '^[a-z][a-z0-9_]{0,62}$' AND " + _sha256_check("profile_sha256"),
            name="custom_import_selection_profile_shape_check",
        ),
    )

    definition_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    profile_slot = Column(SmallInteger, primary_key=True)
    profile_id = Column(String(63), nullable=False)
    context_collection_slot = Column(SmallInteger)
    canonical_profile = Column(Text, nullable=False)
    profile_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportExecution(_CustomImportModel):
    """Mutable execution state; it contains no owner, grant, or credential data."""

    __tablename__ = "custom_import_execution"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("execution_id", name="custom_import_execution_pkey"),
        UniqueConstraint(
            "definition_revision_id",
            "idempotency_key",
            name="custom_import_execution_request_key",
        ),
        UniqueConstraint(
            "execution_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            name="custom_import_execution_owner_key",
        ),
        UniqueConstraint(
            "execution_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "capture_bundle_id",
            name="custom_import_execution_bundle_key",
        ),
        ForeignKeyConstraint(
            ["definition_revision_id", "dataset_id", "schema_revision_id"],
            [
                _reference("custom_import_definition_revision", "definition_revision_id"),
                _reference("custom_import_definition_revision", "dataset_id"),
                _reference("custom_import_definition_revision", "schema_revision_id"),
            ],
            name="custom_import_execution_definition_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "capture_bundle_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_capture_bundle", "capture_bundle_id"),
                _reference("custom_import_capture_bundle", "dataset_id"),
                _reference("custom_import_capture_bundle", "definition_revision_id"),
                _reference("custom_import_capture_bundle", "schema_revision_id"),
            ],
            name="custom_import_execution_bundle_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "mechanism IN ('local', 'queued', 'external') AND "
            "state IN ('queued', 'running', 'canceling', 'canceled', 'failed', 'completed', 'no_change')",
            name="custom_import_execution_state_check",
        ),
    )

    execution_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    idempotency_key = Column(String(128), nullable=False)
    mechanism = Column(String(16), nullable=False)
    state = Column(String(16), nullable=False)
    capture_bundle_id = Column(BigInteger)
    terminal_reason = Column(String(64))
    started_at = Column(TIMESTAMP(timezone=True))
    finished_at = Column(TIMESTAMP(timezone=True))
    created_at = _timestamp_column()
    updated_at = _timestamp_column()


class CustomImportLease(_CustomImportModel):
    """Fenced mutable lease retaining only a token digest."""

    __tablename__ = "custom_import_lease"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("execution_id", name="custom_import_lease_pkey"),
        CheckConstraint(
            "fence >= 0 AND ((fence = 0 AND token_sha256 IS NULL AND expires_at IS NULL) "
            "OR (fence > 0 AND token_sha256 IS NOT NULL AND "
            + _sha256_check("token_sha256")
            + " AND expires_at IS NOT NULL))",
            name="custom_import_lease_shape_check",
        ),
    )

    execution_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_execution", "execution_id"), ondelete="CASCADE"),
        primary_key=True,
    )
    fence = Column(BigInteger, nullable=False, server_default=text("0"))
    token_sha256 = Column(LargeBinary(32))
    heartbeat_at = Column(TIMESTAMP(timezone=True))
    expires_at = Column(TIMESTAMP(timezone=True))
    updated_at = _timestamp_column()


class CustomImportCaptureBundle(_CustomImportModel):
    """Sealed common source snapshot for every stream in one candidate."""

    __tablename__ = "custom_import_capture_bundle"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("capture_bundle_id", name="custom_import_capture_bundle_pkey"),
        UniqueConstraint(
            "capture_bundle_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            name="custom_import_capture_bundle_owner_key",
        ),
        ForeignKeyConstraint(
            ["definition_revision_id", "dataset_id", "schema_revision_id"],
            [
                _reference("custom_import_definition_revision", "definition_revision_id"),
                _reference("custom_import_definition_revision", "dataset_id"),
                _reference("custom_import_definition_revision", "schema_revision_id"),
            ],
            name="custom_import_capture_bundle_definition_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "octet_length(snapshot_token) > 0 AND stream_count > 0 AND "
            + _sha256_check("snapshot_token_sha256")
            + " AND "
            + _sha256_check("manifest_sha256"),
            name="custom_import_capture_bundle_shape_check",
        ),
    )

    capture_bundle_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    snapshot_token = Column(Text, nullable=False)
    snapshot_token_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_manifest = Column(Text, nullable=False)
    manifest_sha256 = Column(LargeBinary(32), nullable=False)
    stream_count = Column(SmallInteger, nullable=False)
    sealed_at = _timestamp_column()


class CustomImportCapture(_CustomImportModel):
    """One immutable stream capture belonging to a sealed capture bundle."""

    __tablename__ = "custom_import_capture"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("capture_bundle_id", "stream_slot", name="custom_import_capture_pkey"),
        UniqueConstraint(
            "capture_bundle_id",
            "definition_revision_id",
            "stream_slot",
            name="custom_import_capture_owner_key",
        ),
        ForeignKeyConstraint(
            [
                "capture_bundle_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_capture_bundle", "capture_bundle_id"),
                _reference("custom_import_capture_bundle", "dataset_id"),
                _reference("custom_import_capture_bundle", "definition_revision_id"),
                _reference("custom_import_capture_bundle", "schema_revision_id"),
            ],
            name="custom_import_capture_bundle_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "definition_revision_id",
                "dataset_id",
                "schema_revision_id",
                "stream_slot",
            ],
            [
                _reference("custom_import_source_stream", "definition_revision_id"),
                _reference("custom_import_source_stream", "dataset_id"),
                _reference("custom_import_source_stream", "schema_revision_id"),
                _reference("custom_import_source_stream", "stream_slot"),
            ],
            name="custom_import_capture_stream_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "byte_count >= 0 AND " + _sha256_check("content_sha256") + " AND " + _sha256_check("manifest_sha256"),
            name="custom_import_capture_shape_check",
        ),
    )

    capture_bundle_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    stream_slot = Column(SmallInteger, primary_key=True)
    content_sha256 = Column(LargeBinary(32), nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    canonical_manifest = Column(Text, nullable=False)
    manifest_sha256 = Column(LargeBinary(32), nullable=False)
    sealed_at = _timestamp_column()


class CustomImportPack(_CustomImportModel):
    """One replayable decoded pack; multiple packs per stream are permitted."""

    __tablename__ = "custom_import_pack"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("pack_id", name="custom_import_pack_pkey"),
        UniqueConstraint(
            "execution_id",
            "producing_fence",
            "stream_slot",
            "pack_ordinal",
            name="custom_import_pack_attempt_key",
        ),
        UniqueConstraint(
            "pack_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            name="custom_import_pack_owner_key",
        ),
        ForeignKeyConstraint(
            [
                "execution_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_execution", "execution_id"),
                _reference("custom_import_execution", "dataset_id"),
                _reference("custom_import_execution", "definition_revision_id"),
                _reference("custom_import_execution", "schema_revision_id"),
            ],
            name="custom_import_pack_execution_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "execution_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "capture_bundle_id",
            ],
            [
                _reference("custom_import_execution", "execution_id"),
                _reference("custom_import_execution", "dataset_id"),
                _reference("custom_import_execution", "definition_revision_id"),
                _reference("custom_import_execution", "schema_revision_id"),
                _reference("custom_import_execution", "capture_bundle_id"),
            ],
            name="custom_import_pack_execution_bundle_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["capture_bundle_id", "definition_revision_id", "stream_slot"],
            [
                _reference("custom_import_capture", "capture_bundle_id"),
                _reference("custom_import_capture", "definition_revision_id"),
                _reference("custom_import_capture", "stream_slot"),
            ],
            name="custom_import_pack_capture_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "pack_ordinal >= 0 AND record_count >= 0 AND "
            + _sha256_check("pack_sha256")
            + " AND ((producing_fence IS NULL AND producing_token_sha256 IS NULL) OR "
            "(producing_fence > 0 AND " + _sha256_check("producing_token_sha256") + "))",
            name="custom_import_pack_shape_check",
        ),
    )

    pack_id = Column(BigInteger, primary_key=True, autoincrement=True)
    execution_id = Column(BigInteger, nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    stream_slot = Column(SmallInteger, nullable=False)
    pack_ordinal = Column(Integer, nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    record_count = Column(BigInteger, nullable=False)
    pack_sha256 = Column(LargeBinary(32), nullable=False)
    # New rows are fenced by the finality migration.  Nullable storage keeps
    # retained pre-finality evidence readable without fabricating authority.
    producing_fence = Column(BigInteger)
    producing_token_sha256 = Column(LargeBinary(32))
    created_at = _timestamp_column()


class CustomImportRejection(_CustomImportModel):
    """Immutable, generic evidence explaining a rejected source record/family."""

    __tablename__ = "custom_import_rejection"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("rejection_id", name="custom_import_rejection_pkey"),
        UniqueConstraint(
            "execution_id",
            "producing_fence",
            "rejection_ordinal",
            name="custom_import_rejection_attempt_key",
        ),
        ForeignKeyConstraint(
            [
                "execution_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_execution", "execution_id"),
                _reference("custom_import_execution", "dataset_id"),
                _reference("custom_import_execution", "definition_revision_id"),
                _reference("custom_import_execution", "schema_revision_id"),
            ],
            name="custom_import_rejection_execution_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["pack_id", "dataset_id", "definition_revision_id", "schema_revision_id"],
            [
                _reference("custom_import_pack", "pack_id"),
                _reference("custom_import_pack", "dataset_id"),
                _reference("custom_import_pack", "definition_revision_id"),
                _reference("custom_import_pack", "schema_revision_id"),
            ],
            name="custom_import_rejection_pack_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "rejection_ordinal >= 0 AND code ~ '^[a-z][a-z0-9_]{0,62}$' AND "
            "(source_ordinal IS NULL OR source_ordinal >= 0) AND "
            "(field_slot IS NULL OR field_slot > 0) AND "
            "(root_key_sha256 IS NULL OR "
            + _sha256_check("root_key_sha256")
            + ") AND ((producing_fence IS NULL AND producing_token_sha256 IS NULL) OR "
            "(producing_fence > 0 AND " + _sha256_check("producing_token_sha256") + "))",
            name="custom_import_rejection_shape_check",
        ),
    )

    rejection_id = Column(BigInteger, primary_key=True, autoincrement=True)
    execution_id = Column(BigInteger, nullable=False)
    rejection_ordinal = Column(BigInteger, nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    pack_id = Column(BigInteger)
    root_key_sha256 = Column(LargeBinary(32))
    canonical_root_key = Column(Text)
    collection_slot = Column(SmallInteger)
    source_ordinal = Column(BigInteger)
    code = Column(String(63), nullable=False)
    field_slot = Column(SmallInteger)
    canonical_evidence = Column(Text, nullable=False)
    producing_fence = Column(BigInteger)
    producing_token_sha256 = Column(LargeBinary(32))
    created_at = _timestamp_column()


class CustomImportRootRecord(_CustomImportModel):
    """Dataset-local stable root logical identity independent of a definition."""

    __tablename__ = "custom_import_root_record"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("root_record_id", name="custom_import_root_record_pkey"),
        UniqueConstraint(
            "dataset_id",
            "key_contract_sha256",
            "logical_key_sha256",
            name="custom_import_root_record_key",
        ),
        UniqueConstraint("root_record_id", "dataset_id", name="custom_import_root_record_owner_key"),
        CheckConstraint(
            _sha256_check("key_contract_sha256") + " AND " + _sha256_check("logical_key_sha256"),
            name="custom_import_root_record_shape_check",
        ),
    )

    root_record_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_dataset", "dataset_id"), ondelete="RESTRICT"),
        nullable=False,
    )
    key_contract_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_logical_key = Column(Text, nullable=False)
    logical_key_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportRootRevision(_CustomImportModel):
    """One retained root record payload and pack provenance."""

    __tablename__ = "custom_import_root_revision"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("root_revision_id", name="custom_import_root_revision_pkey"),
        UniqueConstraint(
            "root_revision_id",
            "dataset_id",
            "schema_revision_id",
            "root_record_id",
            name="custom_import_root_revision_owner_key",
        ),
        ForeignKeyConstraint(
            ["root_record_id", "dataset_id"],
            [
                _reference("custom_import_root_record", "root_record_id"),
                _reference("custom_import_root_record", "dataset_id"),
            ],
            name="custom_import_root_revision_record_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id"],
            [
                _reference("custom_import_schema_revision", "schema_revision_id"),
                _reference("custom_import_schema_revision", "dataset_id"),
            ],
            name="custom_import_root_revision_schema_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["pack_id", "dataset_id", "definition_revision_id", "schema_revision_id"],
            [
                _reference("custom_import_pack", "pack_id"),
                _reference("custom_import_pack", "dataset_id"),
                _reference("custom_import_pack", "definition_revision_id"),
                _reference("custom_import_pack", "schema_revision_id"),
            ],
            name="custom_import_root_revision_pack_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "source_ordinal >= 0 AND " + _sha256_check("payload_sha256"),
            name="custom_import_root_revision_shape_check",
        ),
    )

    root_revision_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    root_record_id = Column(BigInteger, nullable=False)
    pack_id = Column(BigInteger, nullable=False)
    source_ordinal = Column(BigInteger, nullable=False)
    canonical_payload = Column(Text, nullable=False)
    payload_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportChildRevision(_CustomImportModel):
    """One retained child payload with parent and deterministic child identity."""

    __tablename__ = "custom_import_child_revision"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("child_revision_id", name="custom_import_child_revision_pkey"),
        UniqueConstraint(
            "child_revision_id",
            "dataset_id",
            "schema_revision_id",
            "root_record_id",
            "collection_slot",
            name="custom_import_child_revision_owner_key",
        ),
        ForeignKeyConstraint(
            ["root_record_id", "dataset_id"],
            [
                _reference("custom_import_root_record", "root_record_id"),
                _reference("custom_import_root_record", "dataset_id"),
            ],
            name="custom_import_child_revision_record_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["schema_revision_id", "dataset_id", "collection_slot"],
            [
                _reference("custom_import_child_collection", "schema_revision_id"),
                _reference("custom_import_child_collection", "dataset_id"),
                _reference("custom_import_child_collection", "collection_slot"),
            ],
            name="custom_import_child_revision_collection_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["pack_id", "dataset_id", "definition_revision_id", "schema_revision_id"],
            [
                _reference("custom_import_pack", "pack_id"),
                _reference("custom_import_pack", "dataset_id"),
                _reference("custom_import_pack", "definition_revision_id"),
                _reference("custom_import_pack", "schema_revision_id"),
            ],
            name="custom_import_child_revision_pack_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "source_ordinal >= 0 AND "
            + _sha256_check("parent_key_sha256")
            + " AND "
            + _sha256_check("child_key_sha256")
            + " AND "
            + _sha256_check("payload_sha256"),
            name="custom_import_child_revision_shape_check",
        ),
    )

    child_revision_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    root_record_id = Column(BigInteger, nullable=False)
    collection_slot = Column(SmallInteger, nullable=False)
    pack_id = Column(BigInteger, nullable=False)
    source_ordinal = Column(BigInteger, nullable=False)
    canonical_parent_key = Column(Text, nullable=False)
    parent_key_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_child_key = Column(Text, nullable=False)
    child_key_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_payload = Column(Text, nullable=False)
    payload_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportFamilyRevision(_CustomImportModel):
    """Immutable atomic root family independent of any generation pointer."""

    __tablename__ = "custom_import_family_revision"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("family_revision_id", name="custom_import_family_revision_pkey"),
        UniqueConstraint(
            "dataset_id",
            "schema_revision_id",
            "root_record_id",
            "family_sha256",
            "producing_execution_id",
            "producing_fence",
            name="custom_import_family_revision_attempt_content_key",
        ),
        UniqueConstraint(
            "family_revision_id",
            "dataset_id",
            "schema_revision_id",
            "root_record_id",
            name="custom_import_family_revision_owner_key",
        ),
        UniqueConstraint(
            "family_revision_id",
            "entity_binding_id",
            name="custom_import_family_revision_entity_key",
        ),
        ForeignKeyConstraint(
            ["root_revision_id", "dataset_id", "schema_revision_id", "root_record_id"],
            [
                _reference("custom_import_root_revision", "root_revision_id"),
                _reference("custom_import_root_revision", "dataset_id"),
                _reference("custom_import_root_revision", "schema_revision_id"),
                _reference("custom_import_root_revision", "root_record_id"),
            ],
            name="custom_import_family_revision_root_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["entity_binding_id", "dataset_id"],
            [
                _reference("custom_import_entity_binding", "entity_binding_id"),
                _reference("custom_import_entity_binding", "dataset_id"),
            ],
            name="custom_import_family_revision_entity_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "child_count >= 0 AND "
            + _sha256_check("family_sha256")
            + " AND ((producing_execution_id IS NULL AND producing_fence IS NULL "
            "AND producing_token_sha256 IS NULL) OR (producing_execution_id > 0 "
            "AND producing_fence > 0 AND " + _sha256_check("producing_token_sha256") + "))",
            name="custom_import_family_revision_shape_check",
        ),
    )

    family_revision_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    root_record_id = Column(BigInteger, nullable=False)
    root_revision_id = Column(BigInteger, nullable=False)
    entity_binding_id = Column(BigInteger, nullable=False)
    family_sha256 = Column(LargeBinary(32), nullable=False)
    child_count = Column(BigInteger, nullable=False)
    producing_execution_id = Column(BigInteger)
    producing_fence = Column(BigInteger)
    producing_token_sha256 = Column(LargeBinary(32))
    created_at = _timestamp_column()


class CustomImportFamilyChild(_CustomImportModel):
    """Exact child membership of one atomic family revision."""

    __tablename__ = "custom_import_family_child"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "family_revision_id",
            "collection_slot",
            "child_revision_id",
            name="custom_import_family_child_pkey",
        ),
        ForeignKeyConstraint(
            [
                "family_revision_id",
                "dataset_id",
                "schema_revision_id",
                "root_record_id",
            ],
            [
                _reference("custom_import_family_revision", "family_revision_id"),
                _reference("custom_import_family_revision", "dataset_id"),
                _reference("custom_import_family_revision", "schema_revision_id"),
                _reference("custom_import_family_revision", "root_record_id"),
            ],
            name="custom_import_family_child_family_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "child_revision_id",
                "dataset_id",
                "schema_revision_id",
                "root_record_id",
                "collection_slot",
            ],
            [
                _reference("custom_import_child_revision", "child_revision_id"),
                _reference("custom_import_child_revision", "dataset_id"),
                _reference("custom_import_child_revision", "schema_revision_id"),
                _reference("custom_import_child_revision", "root_record_id"),
                _reference("custom_import_child_revision", "collection_slot"),
            ],
            name="custom_import_family_child_revision_fkey",
            ondelete="RESTRICT",
        ),
    )

    family_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    root_record_id = Column(BigInteger, nullable=False)
    collection_slot = Column(SmallInteger, primary_key=True)
    child_revision_id = Column(BigInteger, primary_key=True)


class CustomImportGeneration(_CustomImportModel):
    """One immutable per-fence candidate; finality owns content identity."""

    __tablename__ = "custom_import_generation"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("generation_id", name="custom_import_generation_pkey"),
        UniqueConstraint(
            "execution_id",
            "producing_fence",
            name="custom_import_generation_execution_fence_key",
        ),
        UniqueConstraint("generation_id", "dataset_id", name="custom_import_generation_dataset_key"),
        UniqueConstraint(
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            name="custom_import_generation_owner_key",
        ),
        UniqueConstraint(
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "capture_bundle_id",
            name="custom_import_generation_seal_reference_key",
        ),
        ForeignKeyConstraint(
            [
                "execution_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "capture_bundle_id",
            ],
            [
                _reference("custom_import_execution", "execution_id"),
                _reference("custom_import_execution", "dataset_id"),
                _reference("custom_import_execution", "definition_revision_id"),
                _reference("custom_import_execution", "schema_revision_id"),
                _reference("custom_import_execution", "capture_bundle_id"),
            ],
            name="custom_import_generation_execution_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["base_generation_id", "base_dataset_id"],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
            ],
            name="custom_import_generation_base_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "root_count >= 0 AND family_count >= 0 AND "
            + _sha256_check("source_bundle_sha256")
            + " AND "
            + _sha256_check("candidate_sha256")
            + " AND "
            "((base_generation_id IS NULL AND base_dataset_id IS NULL) OR "
            "(base_generation_id IS NOT NULL AND base_dataset_id IS NOT NULL AND "
            "base_dataset_id = dataset_id))",
            name="custom_import_generation_shape_check",
        ),
        CheckConstraint(
            "(producing_fence IS NULL AND producing_token_sha256 IS NULL) OR "
            "(producing_fence > 0 AND " + _sha256_check("producing_token_sha256") + ")",
            name="custom_import_generation_producing_authority_check",
        ),
    )

    generation_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    base_generation_id = Column(BigInteger)
    base_dataset_id = Column(BigInteger)
    source_bundle_sha256 = Column(LargeBinary(32), nullable=False)
    # This is a worker-supplied attempt fingerprint, not content identity.
    # The immutable generation seal's materialization_sha256 is the sole
    # authoritative digest used by publication and no-change proof.
    candidate_sha256 = Column(LargeBinary(32), nullable=False)
    root_count = Column(BigInteger, nullable=False)
    family_count = Column(BigInteger, nullable=False)
    # These nullable fields are intentionally legacy-safe.  The follow-on
    # finality migration's insert guard requires both fields on new rows,
    # while retained generations from before fenced production stay readable
    # but cannot become current without a matching immutable seal.
    producing_fence = Column(BigInteger)
    producing_token_sha256 = Column(LargeBinary(32))
    created_at = _timestamp_column()


class CustomImportGenerationSeal(_CustomImportModel):
    """One immutable finality receipt for a fully materialized generation."""

    __tablename__ = "custom_import_generation_seal"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("generation_id", name="custom_import_generation_seal_pkey"),
        UniqueConstraint(
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            name="custom_import_generation_seal_owner_key",
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
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
                _reference("custom_import_generation", "definition_revision_id"),
                _reference("custom_import_generation", "schema_revision_id"),
                _reference("custom_import_generation", "execution_id"),
                _reference("custom_import_generation", "capture_bundle_id"),
            ],
            name="custom_import_generation_seal_generation_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "execution_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "capture_bundle_id",
            ],
            [
                _reference("custom_import_execution", "execution_id"),
                _reference("custom_import_execution", "dataset_id"),
                _reference("custom_import_execution", "definition_revision_id"),
                _reference("custom_import_execution", "schema_revision_id"),
                _reference("custom_import_execution", "capture_bundle_id"),
            ],
            name="custom_import_generation_seal_execution_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "seal_contract = 'custom-import-generation-seal/v1' AND sealing_fence > 0 AND "
            "root_count >= 0 AND family_count >= 0 AND generation_family_count >= 0 AND "
            "family_child_count >= 0 AND winner_count >= 0 AND profile_count >= 0 AND "
            "root_scalar_count >= 0 AND child_scalar_count >= 0 AND "
            + _sha256_check("sealing_token_sha256")
            + " AND "
            + _sha256_check("materialization_sha256")
            + " AND "
            + _sha256_check("effective_output_sha256"),
            name="custom_import_generation_seal_shape_check",
        ),
    )

    generation_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    seal_contract = Column(String(63), nullable=False)
    sealing_fence = Column(BigInteger, nullable=False)
    sealing_token_sha256 = Column(LargeBinary(32), nullable=False)
    root_count = Column(BigInteger, nullable=False)
    family_count = Column(BigInteger, nullable=False)
    generation_family_count = Column(BigInteger, nullable=False)
    family_child_count = Column(BigInteger, nullable=False)
    winner_count = Column(BigInteger, nullable=False)
    profile_count = Column(BigInteger, nullable=False)
    root_scalar_count = Column(BigInteger, nullable=False)
    child_scalar_count = Column(BigInteger, nullable=False)
    materialization_sha256 = Column(LargeBinary(32), nullable=False)
    # Source/provenance-inclusive receipt versus served-output equivalence.
    effective_output_sha256 = Column(LargeBinary(32), nullable=False)
    sealed_at = _timestamp_column()


class CustomImportNoChangeSeal(_CustomImportModel):
    """Immutable proof that one sealed candidate has the current effective output."""

    __tablename__ = "custom_import_no_change_seal"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("execution_id", name="custom_import_no_change_seal_pkey"),
        UniqueConstraint(
            "execution_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            name="custom_import_no_change_seal_owner_key",
        ),
        ForeignKeyConstraint(
            [
                "execution_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "capture_bundle_id",
            ],
            [
                _reference("custom_import_execution", "execution_id"),
                _reference("custom_import_execution", "dataset_id"),
                _reference("custom_import_execution", "definition_revision_id"),
                _reference("custom_import_execution", "schema_revision_id"),
                _reference("custom_import_execution", "capture_bundle_id"),
            ],
            name="custom_import_no_change_seal_execution_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "candidate_generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "capture_bundle_id",
            ],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
                _reference("custom_import_generation", "definition_revision_id"),
                _reference("custom_import_generation", "schema_revision_id"),
                _reference("custom_import_generation", "execution_id"),
                _reference("custom_import_generation", "capture_bundle_id"),
            ],
            name="custom_import_no_change_seal_candidate_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "candidate_generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation_seal", "generation_id"),
                _reference("custom_import_generation_seal", "dataset_id"),
                _reference("custom_import_generation_seal", "definition_revision_id"),
                _reference("custom_import_generation_seal", "schema_revision_id"),
            ],
            name="custom_import_no_change_seal_candidate_finality_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "base_generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
                _reference("custom_import_generation", "definition_revision_id"),
                _reference("custom_import_generation", "schema_revision_id"),
            ],
            name="custom_import_no_change_seal_base_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "base_generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation_seal", "generation_id"),
                _reference("custom_import_generation_seal", "dataset_id"),
                _reference("custom_import_generation_seal", "definition_revision_id"),
                _reference("custom_import_generation_seal", "schema_revision_id"),
            ],
            name="custom_import_no_change_seal_base_finality_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "seal_contract = 'custom-import-no-change-seal/v1' AND base_pointer_version >= 0 AND "
            "candidate_generation_id <> base_generation_id AND sealing_fence > 0 AND "
            + _sha256_check("base_source_bundle_sha256")
            + " AND "
            + _sha256_check("candidate_source_bundle_sha256")
            + " AND "
            + _sha256_check("effective_output_sha256")
            + " AND "
            + _sha256_check("sealing_token_sha256")
            + " AND "
            + _sha256_check("receipt_sha256"),
            name="custom_import_no_change_seal_shape_check",
        ),
    )

    execution_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    base_generation_id = Column(BigInteger, nullable=False)
    candidate_generation_id = Column(BigInteger, nullable=False)
    base_pointer_version = Column(BigInteger, nullable=False)
    seal_contract = Column(String(63), nullable=False)
    base_source_bundle_sha256 = Column(LargeBinary(32), nullable=False)
    candidate_source_bundle_sha256 = Column(LargeBinary(32), nullable=False)
    effective_output_sha256 = Column(LargeBinary(32), nullable=False)
    sealing_fence = Column(BigInteger, nullable=False)
    sealing_token_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_receipt = Column(Text, nullable=False)
    receipt_sha256 = Column(LargeBinary(32), nullable=False)
    sealed_at = _timestamp_column()


class CustomImportGenerationFamily(_CustomImportModel):
    """Exact root-to-family map implementing immutable upsert replacement semantics."""

    __tablename__ = "custom_import_generation_family"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "generation_id",
            "root_record_id",
            name="custom_import_generation_family_pkey",
        ),
        UniqueConstraint(
            "generation_id",
            "dataset_id",
            "family_revision_id",
            name="custom_import_generation_family_member_key",
        ),
        ForeignKeyConstraint(
            [
                "generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
                _reference("custom_import_generation", "definition_revision_id"),
                _reference("custom_import_generation", "schema_revision_id"),
            ],
            name="custom_import_generation_family_generation_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "family_revision_id",
                "dataset_id",
                "schema_revision_id",
                "root_record_id",
            ],
            [
                _reference("custom_import_family_revision", "family_revision_id"),
                _reference("custom_import_family_revision", "dataset_id"),
                _reference("custom_import_family_revision", "schema_revision_id"),
                _reference("custom_import_family_revision", "root_record_id"),
            ],
            name="custom_import_generation_family_family_fkey",
            ondelete="RESTRICT",
        ),
    )

    generation_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    root_record_id = Column(BigInteger, primary_key=True)
    family_revision_id = Column(BigInteger, nullable=False)


class CustomImportRootScalar(_CustomImportModel):
    """Typed root hot fields; explicit null is distinct from a missing projection row."""

    __tablename__ = "custom_import_root_scalar"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("root_revision_id", "field_slot", name="custom_import_root_scalar_pkey"),
        ForeignKeyConstraint(
            ["root_revision_id", "dataset_id", "schema_revision_id", "root_record_id"],
            [
                _reference("custom_import_root_revision", "root_revision_id"),
                _reference("custom_import_root_revision", "dataset_id"),
                _reference("custom_import_root_revision", "schema_revision_id"),
                _reference("custom_import_root_revision", "root_record_id"),
            ],
            name="custom_import_root_scalar_revision_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "schema_revision_id",
                "dataset_id",
                "field_slot",
                "field_type",
                "field_collection_slot",
                "projection_slot",
            ],
            [
                _reference("custom_import_field", "schema_revision_id"),
                _reference("custom_import_field", "dataset_id"),
                _reference("custom_import_field", "field_slot"),
                _reference("custom_import_field", "field_type"),
                _reference("custom_import_field", "collection_slot"),
                _reference("custom_import_field", "projection_slot"),
            ],
            name="custom_import_root_scalar_field_fkey",
            ondelete="RESTRICT",
        ),
        _scalar_check("custom_import_root_scalar_shape_check", root=True),
    )

    root_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    root_record_id = Column(BigInteger, nullable=False)
    field_slot = Column(SmallInteger, primary_key=True)
    field_collection_slot = Column(SmallInteger, nullable=False, server_default=text("0"))
    projection_slot = Column(SmallInteger, nullable=False)
    field_type = Column(String(16), nullable=False)
    value_state = Column(String(8), nullable=False)
    string_value = Column(String(4096))
    integer_value = Column(BigInteger)
    decimal_value = Column(Numeric(30, 12))
    boolean_value = Column(Boolean)
    date_value = Column(Date)
    timestamp_value = Column(TIMESTAMP(timezone=True))


class CustomImportChildScalar(_CustomImportModel):
    """Typed child hot fields with schema-qualified field ownership."""

    __tablename__ = "custom_import_child_scalar"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("child_revision_id", "field_slot", name="custom_import_child_scalar_pkey"),
        ForeignKeyConstraint(
            [
                "child_revision_id",
                "dataset_id",
                "schema_revision_id",
                "root_record_id",
                "collection_slot",
            ],
            [
                _reference("custom_import_child_revision", "child_revision_id"),
                _reference("custom_import_child_revision", "dataset_id"),
                _reference("custom_import_child_revision", "schema_revision_id"),
                _reference("custom_import_child_revision", "root_record_id"),
                _reference("custom_import_child_revision", "collection_slot"),
            ],
            name="custom_import_child_scalar_revision_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "schema_revision_id",
                "dataset_id",
                "field_slot",
                "field_type",
                "field_collection_slot",
                "projection_slot",
            ],
            [
                _reference("custom_import_field", "schema_revision_id"),
                _reference("custom_import_field", "dataset_id"),
                _reference("custom_import_field", "field_slot"),
                _reference("custom_import_field", "field_type"),
                _reference("custom_import_field", "collection_slot"),
                _reference("custom_import_field", "projection_slot"),
            ],
            name="custom_import_child_scalar_field_fkey",
            ondelete="RESTRICT",
        ),
        _scalar_check("custom_import_child_scalar_shape_check"),
    )

    child_revision_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    root_record_id = Column(BigInteger, nullable=False)
    collection_slot = Column(SmallInteger, nullable=False)
    field_slot = Column(SmallInteger, primary_key=True)
    field_collection_slot = Column(SmallInteger, nullable=False)
    projection_slot = Column(SmallInteger, nullable=False)
    field_type = Column(String(16), nullable=False)
    value_state = Column(String(8), nullable=False)
    string_value = Column(String(4096))
    integer_value = Column(BigInteger)
    decimal_value = Column(Numeric(30, 12))
    boolean_value = Column(Boolean)
    date_value = Column(Date)
    timestamp_value = Column(TIMESTAMP(timezone=True))


class CustomImportEntityBinding(_CustomImportModel):
    """Stable dataset-level generic entity identity used by all family bindings."""

    __tablename__ = "custom_import_entity_binding"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("entity_binding_id", name="custom_import_entity_binding_pkey"),
        UniqueConstraint(
            "dataset_id",
            "adapter_id",
            "canonical_value",
            name="custom_import_entity_binding_value_key",
        ),
        UniqueConstraint(
            "entity_binding_id",
            "dataset_id",
            name="custom_import_entity_binding_owner_key",
        ),
        ForeignKeyConstraint(
            ["dataset_id"],
            [_reference("custom_import_dataset", "dataset_id")],
            name="custom_import_entity_binding_dataset_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "adapter_id ~ '^[a-z][a-z0-9_]{0,62}$' AND "
            "octet_length(canonical_value) > 0 AND octet_length(canonical_value) <= 512 AND "
            + _sha256_check("value_sha256"),
            name="custom_import_entity_binding_shape_check",
        ),
    )

    entity_binding_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    adapter_id = Column(String(63), nullable=False)
    canonical_value = Column(String(512), nullable=False)
    value_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


class CustomImportWinner(_CustomImportModel):
    """Precomputed selected family context using compact binding/profile identifiers."""

    __tablename__ = "custom_import_winner"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "generation_id",
            "profile_slot",
            "entity_binding_id",
            "context_key_sha256",
            name="custom_import_winner_pkey",
        ),
        ForeignKeyConstraint(
            [
                "generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
                _reference("custom_import_generation", "definition_revision_id"),
                _reference("custom_import_generation", "schema_revision_id"),
            ],
            name="custom_import_winner_generation_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "definition_revision_id",
                "dataset_id",
                "schema_revision_id",
                "profile_slot",
            ],
            [
                _reference("custom_import_selection_profile", "definition_revision_id"),
                _reference("custom_import_selection_profile", "dataset_id"),
                _reference("custom_import_selection_profile", "schema_revision_id"),
                _reference("custom_import_selection_profile", "profile_slot"),
            ],
            name="custom_import_winner_profile_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["entity_binding_id", "dataset_id"],
            [
                _reference("custom_import_entity_binding", "entity_binding_id"),
                _reference("custom_import_entity_binding", "dataset_id"),
            ],
            name="custom_import_winner_binding_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["generation_id", "dataset_id", "family_revision_id"],
            [
                _reference("custom_import_generation_family", "generation_id"),
                _reference("custom_import_generation_family", "dataset_id"),
                _reference("custom_import_generation_family", "family_revision_id"),
            ],
            name="custom_import_winner_family_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["family_revision_id", "entity_binding_id"],
            [
                _reference("custom_import_family_revision", "family_revision_id"),
                _reference("custom_import_family_revision", "entity_binding_id"),
            ],
            name="custom_import_winner_family_entity_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "family_revision_id",
                "context_collection_slot",
                "context_child_revision_id",
            ],
            [
                _reference("custom_import_family_child", "family_revision_id"),
                _reference("custom_import_family_child", "collection_slot"),
                _reference("custom_import_family_child", "child_revision_id"),
            ],
            name="custom_import_winner_context_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            _sha256_check("context_key_sha256") + " AND "
            "((context_collection_slot = 0 AND context_child_revision_id IS NULL) OR "
            "(context_collection_slot > 0 AND context_child_revision_id IS NOT NULL))",
            name="custom_import_winner_context_check",
        ),
    )

    generation_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    profile_slot = Column(SmallInteger, primary_key=True)
    entity_binding_id = Column(BigInteger, primary_key=True)
    family_revision_id = Column(BigInteger, nullable=False)
    context_collection_slot = Column(SmallInteger, nullable=False, server_default=text("0"))
    context_key_sha256 = Column(LargeBinary(32), primary_key=True)
    context_child_revision_id = Column(BigInteger)


class CustomImportCurrentGeneration(_CustomImportModel):
    """The sole mutable current-generation compare-and-swap pointer."""

    __tablename__ = "custom_import_current_generation"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("dataset_id", name="custom_import_current_generation_pkey"),
        ForeignKeyConstraint(
            [
                "generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
                _reference("custom_import_generation", "definition_revision_id"),
                _reference("custom_import_generation", "schema_revision_id"),
            ],
            name="custom_import_current_generation_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation_seal", "generation_id"),
                _reference("custom_import_generation_seal", "dataset_id"),
                _reference("custom_import_generation_seal", "definition_revision_id"),
                _reference("custom_import_generation_seal", "schema_revision_id"),
            ],
            name="custom_import_current_generation_seal_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint("pointer_version > 0", name="custom_import_current_generation_version_check"),
    )

    dataset_id = Column(BigInteger, primary_key=True)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    generation_id = Column(BigInteger, nullable=False)
    pointer_version = Column(BigInteger, nullable=False)
    changed_at = _timestamp_column()


class CustomImportPublicationEvent(_CustomImportModel):
    """Immutable publication transition or no-change receipt."""

    __tablename__ = "custom_import_publication_event"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("publication_event_id", name="custom_import_publication_event_pkey"),
        ForeignKeyConstraint(
            [
                "execution_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_execution", "execution_id"),
                _reference("custom_import_execution", "dataset_id"),
                _reference("custom_import_execution", "definition_revision_id"),
                _reference("custom_import_execution", "schema_revision_id"),
            ],
            name="custom_import_publication_event_execution_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "to_generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
                _reference("custom_import_generation", "definition_revision_id"),
                _reference("custom_import_generation", "schema_revision_id"),
            ],
            name="custom_import_publication_event_to_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            [
                "to_generation_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_generation_seal", "generation_id"),
                _reference("custom_import_generation_seal", "dataset_id"),
                _reference("custom_import_generation_seal", "definition_revision_id"),
                _reference("custom_import_generation_seal", "schema_revision_id"),
            ],
            name="custom_import_publication_event_to_seal_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["from_generation_id", "dataset_id"],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
            ],
            name="custom_import_publication_event_from_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "event_kind IN ('activated', 'rolled_back', 'no_change') AND "
            "expected_pointer_version >= 0 AND committed_pointer_version >= 0 AND "
            "((event_kind = 'activated' AND committed_pointer_version = expected_pointer_version + 1) OR "
            "(event_kind = 'rolled_back' AND from_generation_id IS NOT NULL AND "
            "committed_pointer_version = expected_pointer_version + 1) OR "
            "(event_kind = 'no_change' AND from_generation_id IS NOT NULL AND "
            "from_generation_id = to_generation_id AND "
            "committed_pointer_version = expected_pointer_version)) AND "
            "(finality_contract IS NULL OR finality_contract = 'custom-import-finality/v1') AND "
            + _sha256_check("event_sha256"),
            name="custom_import_publication_event_shape_check",
        ),
    )

    publication_event_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    event_kind = Column(String(16), nullable=False)
    from_generation_id = Column(BigInteger)
    to_generation_id = Column(BigInteger, nullable=False)
    expected_pointer_version = Column(BigInteger, nullable=False)
    committed_pointer_version = Column(BigInteger, nullable=False)
    # Legacy events remain intentionally unmarked.  New finality events opt
    # into partial uniqueness without making a populated v1 upgrade fail.
    finality_contract = Column(String(63))
    canonical_event = Column(Text, nullable=False)
    event_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()


Index(
    "custom_import_publication_event_identity_key",
    CustomImportPublicationEvent.dataset_id,
    CustomImportPublicationEvent.execution_id,
    CustomImportPublicationEvent.event_kind,
    func.coalesce(CustomImportPublicationEvent.from_generation_id, 0),
    CustomImportPublicationEvent.to_generation_id,
    CustomImportPublicationEvent.expected_pointer_version,
    CustomImportPublicationEvent.committed_pointer_version,
    unique=True,
    postgresql_where=text("finality_contract = 'custom-import-finality/v1'"),
)
Index(
    "custom_import_publication_event_pointer_version_key",
    CustomImportPublicationEvent.dataset_id,
    CustomImportPublicationEvent.committed_pointer_version,
    unique=True,
    postgresql_where=text(
        "finality_contract = 'custom-import-finality/v1' AND event_kind IN ('activated', 'rolled_back')"
    ),
)
Index(
    "custom_import_publication_event_no_change_execution_key",
    CustomImportPublicationEvent.execution_id,
    unique=True,
    postgresql_where=text("finality_contract = 'custom-import-finality/v1' AND event_kind = 'no_change'"),
)
