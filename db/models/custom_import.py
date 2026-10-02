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
    "CustomImportBuildAttempt",
    "CustomImportBuildStream",
    "CustomImportBuildOccurrence",
    "CustomImportBuildFamily",
    "CustomImportBuildCandidateContext",
    "CustomImportBuildVerification",
    "CustomImportCapture",
    "CustomImportCaptureBundle",
    "CustomImportCaptureParquetPart",
    "CustomImportCaptureUsage",
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
    "CustomImportRegistrationAuthority",
    "CustomImportRejection",
    "CustomImportRootRecord",
    "CustomImportRootRevision",
    "CustomImportRootScalar",
    "CustomImportSchemaRevision",
    "CustomImportSelectionProfile",
    "CustomImportSourceBindingRevision",
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


def _capture_counter(name: str):
    return Column(BigInteger, CheckConstraint(f"{name} >= 0"), nullable=False, server_default=text("0"))


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


class CustomImportRegistrationAuthority(_CustomImportModel):
    """One immutable registration capability with retained revoke/result evidence."""

    __tablename__ = "custom_import_registration_authority"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("authority_id", name="custom_import_reg_authority_pkey"),
        CheckConstraint(
            "authority_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'",
            name="custom_import_reg_authority_id_check",
        ),
        CheckConstraint(
            "((input_sha256 IS NOT NULL AND token_sha256 IS NOT NULL AND expires_at IS NOT NULL AND "
            + _sha256_check("input_sha256")
            + " AND "
            + _sha256_check("token_sha256")
            + ") OR (input_sha256 IS NULL AND token_sha256 IS NULL AND expires_at IS NULL AND "
            "revoked_at IS NOT NULL AND result_receipt IS NULL))",
            name="custom_import_reg_authority_pins_check",
        ),
        CheckConstraint(
            "result_receipt IS NULL OR (input_sha256 IS NOT NULL AND octet_length(result_receipt) BETWEEN 2 AND 4096)",
            name="custom_import_reg_authority_result_check",
        ),
    )

    authority_id = Column(String(128), primary_key=True)
    input_sha256 = Column(LargeBinary(32))
    token_sha256 = Column(LargeBinary(32))
    expires_at = Column(TIMESTAMP(timezone=True))
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("clock_timestamp()"))
    revoked_at = Column(TIMESTAMP(timezone=True))
    result_receipt = Column(Text)


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


class CustomImportSourceBindingRevision(_CustomImportModel):
    """Immutable connector configuration bound to one definition digest."""

    __tablename__ = "custom_import_source_binding_revision"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "source_binding_revision_id",
            name="custom_import_source_binding_revision_pkey",
        ),
        UniqueConstraint(
            "definition_revision_id",
            "revision_number",
            name="custom_import_source_binding_revision_number_key",
        ),
        UniqueConstraint(
            "definition_revision_id",
            "binding_sha256",
            name="custom_import_source_binding_revision_hash_key",
        ),
        UniqueConstraint(
            "source_binding_revision_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            name="custom_import_source_binding_revision_owner_key",
        ),
        ForeignKeyConstraint(
            ["definition_revision_id", "dataset_id", "schema_revision_id"],
            [
                _reference("custom_import_definition_revision", "definition_revision_id"),
                _reference("custom_import_definition_revision", "dataset_id"),
                _reference("custom_import_definition_revision", "schema_revision_id"),
            ],
            name="custom_import_source_binding_revision_definition_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "binding_contract IN ('custom-import/source-binding/v1', 'custom-import/source-binding/v2') AND "
            "connector_kind = 'snowflake_bundle' AND revision_number > 0 AND "
            + _sha256_check("definition_sha256")
            + " AND "
            + _sha256_check("schema_sha256")
            + " AND "
            + _sha256_check("source_object_fingerprint_sha256")
            + " AND "
            + _sha256_check("binding_sha256")
            + " AND source_object_version ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'"
            + " AND octet_length(canonical_binding) BETWEEN 2 AND 1048576"
            + " AND binding_sha256 = pg_catalog.sha256("
            "convert_to(binding_contract || ':' || canonical_binding, 'UTF8'))",
            name="custom_import_source_binding_revision_shape_check",
        ),
    )

    source_binding_revision_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    revision_number = Column(Integer, nullable=False)
    binding_contract = Column(String(63), nullable=False)
    connector_kind = Column(String(32), nullable=False)
    definition_sha256 = Column(LargeBinary(32), nullable=False)
    schema_sha256 = Column(LargeBinary(32), nullable=False)
    source_object_fingerprint_sha256 = Column(LargeBinary(32), nullable=False)
    source_object_version = Column(String(255), nullable=False)
    canonical_binding = Column(Text, nullable=False)
    binding_sha256 = Column(LargeBinary(32), nullable=False)
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
        ForeignKeyConstraint(
            [
                "source_binding_revision_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
            ],
            [
                _reference("custom_import_source_binding_revision", "source_binding_revision_id"),
                _reference("custom_import_source_binding_revision", "dataset_id"),
                _reference("custom_import_source_binding_revision", "definition_revision_id"),
                _reference("custom_import_source_binding_revision", "schema_revision_id"),
            ],
            name="custom_import_execution_source_binding_revision_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "mechanism IN ('local', 'queued', 'external') AND "
            "state IN ('queued', 'running', 'canceling', 'canceled', 'failed', 'completed', 'no_change')",
            name="custom_import_execution_state_check",
        ),
        CheckConstraint(
            "request_identity_sha256 IS NULL OR " + _sha256_check("request_identity_sha256"),
            name="custom_import_execution_request_identity_shape_check",
        ),
        CheckConstraint(
            "source_binding_revision_id IS NULL OR request_identity_sha256 IS NOT NULL",
            name="custom_import_execution_source_binding_identity_check",
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
    request_identity_sha256 = Column(LargeBinary(32))
    source_binding_revision_id = Column(BigInteger)
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
    """Legacy sealed snapshot or a fenced segmented capture in progress."""

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
        Index(
            "custom_import_capture_bundle_snapshot_digest_idx",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "snapshot_token_sha256",
        ),
        UniqueConstraint("producing_execution_id", "producing_fence", name="custom_import_capture_bundle_attempt_key"),
        ForeignKeyConstraint(
            ["producing_execution_id", "dataset_id", "definition_revision_id", "schema_revision_id"],
            [
                _reference("custom_import_execution", column)
                for column in ("execution_id", "dataset_id", "definition_revision_id", "schema_revision_id")
            ],
            name="custom_import_capture_bundle_producer_fkey",
        ),
        CheckConstraint(
            "(capture_state = 'pending' AND canonical_manifest IS NULL AND manifest_sha256 IS NULL AND sealed_at IS NULL) OR "
            "(capture_state = 'sealed' AND canonical_manifest IS NOT NULL AND manifest_sha256 IS NOT NULL AND sealed_at IS NOT NULL)",
            name="custom_import_capture_bundle_final_shape_check",
        ),
        CheckConstraint(
            "(payload_contract IS NULL AND capture_state = 'sealed' AND producing_execution_id IS NULL AND "
            "producing_fence IS NULL AND producing_token_sha256 IS NULL AND request_identity_sha256 IS NULL AND "
            "source_binding_revision_id IS NULL AND source_binding_sha256 IS NULL AND source_request_sha256 IS NULL AND "
            "statement_sha256 IS NULL AND canonical_policy IS NULL AND policy_sha256 IS NULL AND "
            "acquisition_started_at IS NULL AND acquisition_deadline_at IS NULL) OR "
            "(payload_contract IS NOT NULL AND payload_contract = 'custom-import/parquet-parts/v2' AND capture_state IN ('pending','sealed') AND "
            "producing_execution_id IS NOT NULL AND producing_fence IS NOT NULL AND producing_fence > 0 AND "
            "producing_token_sha256 IS NOT NULL AND octet_length(producing_token_sha256) = 32 AND "
            "request_identity_sha256 IS NOT NULL AND octet_length(request_identity_sha256) = 32 AND "
            "source_request_sha256 IS NOT NULL AND octet_length(source_request_sha256) = 32 AND "
            "statement_sha256 IS NOT NULL AND octet_length(statement_sha256) = 32 AND "
            "policy_sha256 IS NOT NULL AND octet_length(policy_sha256) = 32 AND "
            "canonical_policy IS NOT NULL AND octet_length(canonical_policy) BETWEEN 2 AND 16384 AND "
            "acquisition_started_at IS NOT NULL AND acquisition_deadline_at IS NOT NULL AND "
            "acquisition_deadline_at > acquisition_started_at AND "
            "((source_binding_revision_id IS NULL AND source_binding_sha256 IS NULL) OR "
            "(source_binding_revision_id IS NOT NULL AND source_binding_sha256 IS NOT NULL AND octet_length(source_binding_sha256) = 32)))",
            name="custom_import_capture_bundle_lifecycle_check",
        ),
    )

    capture_bundle_id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    snapshot_token = Column(Text, nullable=False)
    snapshot_token_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_manifest = Column(Text)
    manifest_sha256 = Column(LargeBinary(32))
    stream_count = Column(SmallInteger, nullable=False)
    sealed_at = Column(TIMESTAMP(timezone=True), server_default=text("transaction_timestamp()"))
    payload_contract = Column(String(63))
    capture_state = Column(String(16), nullable=False, server_default=text("'sealed'"))
    producing_execution_id = Column(BigInteger)
    producing_fence = Column(BigInteger)
    producing_token_sha256 = Column(LargeBinary(32))
    request_identity_sha256 = Column(LargeBinary(32))
    source_binding_revision_id = Column(BigInteger)
    source_binding_sha256 = Column(LargeBinary(32))
    source_request_sha256 = Column(LargeBinary(32))
    statement_sha256 = Column(LargeBinary(32))
    canonical_policy = Column(Text)
    policy_sha256 = Column(LargeBinary(32))
    acquisition_started_at = Column(TIMESTAMP(timezone=True))
    acquisition_deadline_at = Column(TIMESTAMP(timezone=True))
    committed_part_count = _capture_counter("committed_part_count")
    committed_byte_count = _capture_counter("committed_byte_count")
    committed_decoded_byte_count = _capture_counter("committed_decoded_byte_count")
    committed_arrow_byte_count = _capture_counter("committed_arrow_byte_count")
    committed_record_count = _capture_counter("committed_record_count")
    committed_manifest_byte_count = _capture_counter("committed_manifest_byte_count")


class CustomImportCapture(_CustomImportModel):
    """One stream header, immutable after its capture is sealed."""

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
        CheckConstraint(
            "(payload_contract IS NULL AND payload_part_count IS NULL AND payload_set_sha256 IS NULL "
            "AND capture_state = 'sealed' AND eof_at IS NULL AND manifest_set_sha256 IS NULL) OR "
            "(payload_contract IS NOT NULL AND payload_contract = 'custom-import/parquet-parts/v1' AND "
            "capture_state = 'sealed' AND eof_at IS NULL AND manifest_set_sha256 IS NULL AND "
            "byte_count BETWEEN 1 AND 67108864 AND payload_part_count IS NOT NULL AND "
            "payload_part_count BETWEEN 1 AND 4096 AND payload_set_sha256 IS NOT NULL AND "
            + _sha256_check("payload_set_sha256")
            + ") OR (payload_contract IS NOT NULL AND payload_contract = 'custom-import/parquet-parts/v2' AND "
            "((capture_state = 'pending' AND payload_part_count IS NULL AND payload_set_sha256 IS NULL AND manifest_set_sha256 IS NULL) OR "
            "(capture_state = 'sealed' AND payload_part_count BETWEEN 1 AND 131072 AND payload_part_count IS NOT NULL AND "
            "payload_set_sha256 IS NOT NULL AND octet_length(payload_set_sha256) = 32 AND "
            "manifest_set_sha256 IS NOT NULL AND octet_length(manifest_set_sha256) = 32 AND eof_at IS NOT NULL)))",
            name="custom_import_capture_payload_shape_check",
        ),
        CheckConstraint(
            "(capture_state = 'pending' AND content_sha256 IS NULL AND byte_count IS NULL AND canonical_manifest IS NULL "
            "AND manifest_sha256 IS NULL AND sealed_at IS NULL) OR (capture_state = 'sealed' AND content_sha256 IS NOT NULL "
            "AND byte_count IS NOT NULL AND canonical_manifest IS NOT NULL AND manifest_sha256 IS NOT NULL AND sealed_at IS NOT NULL)",
            name="custom_import_capture_stream_final_shape_check",
        ),
    )

    capture_bundle_id = Column(BigInteger, primary_key=True)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    stream_slot = Column(SmallInteger, primary_key=True)
    content_sha256 = Column(LargeBinary(32))
    byte_count = Column(BigInteger)
    canonical_manifest = Column(Text)
    manifest_sha256 = Column(LargeBinary(32))
    payload_contract = Column(String(63))
    payload_part_count = Column(Integer)
    payload_set_sha256 = Column(LargeBinary(32))
    sealed_at = Column(TIMESTAMP(timezone=True), server_default=text("transaction_timestamp()"))
    capture_state = Column(String(16), nullable=False, server_default=text("'sealed'"))
    eof_at = Column(TIMESTAMP(timezone=True))
    manifest_set_sha256 = Column(LargeBinary(32))
    committed_part_count = _capture_counter("committed_part_count")
    committed_byte_count = _capture_counter("committed_byte_count")
    committed_decoded_byte_count = _capture_counter("committed_decoded_byte_count")
    committed_arrow_byte_count = _capture_counter("committed_arrow_byte_count")
    committed_record_count = _capture_counter("committed_record_count")
    committed_manifest_byte_count = _capture_counter("committed_manifest_byte_count")


class CustomImportCaptureParquetPart(_CustomImportModel):
    """One immutable retained Parquet payload part for a durable capture."""

    __tablename__ = "custom_import_capture_parquet_part"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "capture_bundle_id",
            "stream_slot",
            "part_ordinal",
            name="custom_import_capture_parquet_part_pkey",
        ),
        ForeignKeyConstraint(
            ["capture_bundle_id", "stream_slot"],
            [
                _reference("custom_import_capture", "capture_bundle_id"),
                _reference("custom_import_capture", "stream_slot"),
            ],
            name="custom_import_capture_parquet_part_capture_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "part_ordinal BETWEEN 1 AND 131072 AND byte_count BETWEEN 1 AND 67108864 AND "
            "octet_length(payload) = byte_count AND "
            + _sha256_check("payload_sha256")
            + " AND payload_sha256 = pg_catalog.sha256(payload)",
            name="custom_import_capture_parquet_part_shape_check",
        ),
        CheckConstraint(
            "(canonical_capture_manifest IS NULL AND capture_manifest_sha256 IS NULL AND decoded_byte_count IS NULL "
            "AND arrow_byte_count IS NULL AND record_count IS NULL) OR "
            "(canonical_capture_manifest IS NOT NULL AND capture_manifest_sha256 IS NOT NULL AND "
            "octet_length(canonical_capture_manifest) BETWEEN 2 AND 2097152 AND "
            "capture_manifest_sha256 = pg_catalog.sha256(convert_to(canonical_capture_manifest, 'UTF8')) AND "
            "decoded_byte_count IS NOT NULL AND decoded_byte_count BETWEEN 1 AND 268435456 AND "
            "arrow_byte_count IS NOT NULL AND arrow_byte_count BETWEEN 0 AND 268435456 AND "
            "record_count IS NOT NULL AND record_count BETWEEN 0 AND 1000000)",
            name="custom_import_capture_parquet_manifest_shape_check",
        ),
    )

    capture_bundle_id = Column(BigInteger, primary_key=True)
    stream_slot = Column(SmallInteger, primary_key=True)
    part_ordinal = Column(Integer, primary_key=True)
    byte_count = Column(BigInteger, nullable=False)
    payload = Column(LargeBinary, nullable=False)
    payload_sha256 = Column(LargeBinary(32), nullable=False)
    sealed_at = _timestamp_column()
    canonical_capture_manifest = Column(Text)
    capture_manifest_sha256 = Column(LargeBinary(32))
    decoded_byte_count = Column(BigInteger)
    arrow_byte_count = Column(BigInteger)
    record_count = Column(BigInteger)


class CustomImportCaptureUsage(_CustomImportModel):
    """Logical retained payload and per-part manifest bytes, including abandoned captures."""

    __tablename__ = "custom_import_capture_usage"
    __main_table__ = __tablename__
    __table_args__ = _table_args(CheckConstraint("retained_bytes >= 0", name="custom_import_capture_usage_shape_check"))
    dataset_id = Column(BigInteger, ForeignKey(_reference("custom_import_dataset", "dataset_id")), primary_key=True)
    retained_bytes = Column(BigInteger, nullable=False)


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


class CustomImportBuildAttempt(_CustomImportModel):
    """One fenced, monotonically frozen bounded family build."""

    __tablename__ = "custom_import_build_attempt"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("execution_id", "producing_fence", name="custom_import_build_attempt_key"),
        UniqueConstraint("generation_id", name="custom_import_build_generation_key"),
        ForeignKeyConstraint(
            ["execution_id", "dataset_id", "definition_revision_id", "schema_revision_id", "capture_bundle_id"],
            [
                _reference("custom_import_execution", key)
                for key in (
                    "execution_id",
                    "dataset_id",
                    "definition_revision_id",
                    "schema_revision_id",
                    "capture_bundle_id",
                )
            ],
            ondelete="RESTRICT",
            name="custom_import_build_execution_fkey",
        ),
        ForeignKeyConstraint(
            ["base_generation_id", "dataset_id"],
            [
                _reference("custom_import_generation", "generation_id"),
                _reference("custom_import_generation", "dataset_id"),
            ],
            ondelete="RESTRICT",
            name="custom_import_build_base_fkey",
        ),
        CheckConstraint(
            "build_contract = 'custom-import/build/v1' AND producing_fence > 0 AND "
            "octet_length(producing_token_sha256) = 32 AND "
            "(request_identity_sha256 IS NULL OR octet_length(request_identity_sha256) = 32) AND "
            "refresh_mode IN ('upsert','snapshot') AND page_row_limit BETWEEN 1 AND 256 AND "
            "page_byte_limit BETWEEN 1 AND 268435456 AND statement_timeout_ms > 0 AND "
            "((base_generation_id IS NULL AND base_pointer_version = 0) OR "
            "(base_generation_id IS NOT NULL AND base_pointer_version > 0)) AND "
            "plan_stage IN ('base','source','complete')",
            name="custom_import_build_attempt_shape",
        ),
        CheckConstraint(
            "((phase = 'source' AND source_frozen_at IS NULL AND graph_frozen_at IS NULL "
            "AND output_frozen_at IS NULL AND generation_id IS NULL AND verified_at IS NULL) OR "
            "(phase IN ('admission','graph','rejected') AND source_frozen_at IS NOT NULL "
            "AND graph_frozen_at IS NULL AND output_frozen_at IS NULL AND generation_id IS NULL "
            "AND verified_at IS NULL) OR "
            "(phase = 'output' AND source_frozen_at IS NOT NULL AND graph_frozen_at IS NOT NULL "
            "AND output_frozen_at IS NULL AND generation_id IS NOT NULL AND verified_at IS NULL) OR "
            "(phase IN ('verifying','verified') AND source_frozen_at IS NOT NULL AND graph_frozen_at IS NOT NULL "
            "AND output_frozen_at IS NOT NULL AND generation_id IS NOT NULL "
            "AND ((phase = 'verifying' AND verified_at IS NULL) OR (phase = 'verified' AND verified_at IS NOT NULL))))",
            name="custom_import_build_attempt_phase",
        ),
        CheckConstraint(
            "(output_after_profile_slot IS NULL AND output_after_entity_binding_id IS NULL "
            "AND output_after_context_key_sha256 IS NULL) OR "
            "(output_after_profile_slot IS NOT NULL AND output_after_entity_binding_id IS NOT NULL "
            "AND output_after_context_key_sha256 IS NOT NULL AND octet_length(output_after_context_key_sha256) = 32)",
            name="custom_import_build_output_cursor",
        ),
        Index("custom_import_build_definition_idx", "definition_revision_id"),
        Index("custom_import_build_schema_idx", "schema_revision_id"),
    )

    build_id = Column(BigInteger, primary_key=True, autoincrement=True)
    build_contract = Column(String(63), nullable=False, server_default=text("'custom-import/build/v1'"))
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    producing_fence = Column(BigInteger, nullable=False)
    producing_token_sha256 = Column(LargeBinary(32), nullable=False)
    request_identity_sha256 = Column(LargeBinary(32))
    base_generation_id = Column(BigInteger)
    base_pointer_version = Column(BigInteger, nullable=False)
    refresh_mode = Column(String(16), nullable=False)
    complete_scope = Column(Boolean, nullable=False)
    phase = Column(String(16), nullable=False, server_default=text("'source'"))
    generation_id = Column(BigInteger, ForeignKey(_reference("custom_import_generation", "generation_id")))
    page_row_limit = Column(Integer, nullable=False)
    page_byte_limit = Column(BigInteger, nullable=False)
    statement_timeout_ms = Column(Integer, nullable=False)
    build_deadline_at = Column(TIMESTAMP(timezone=True), nullable=False)
    admission_after_occurrence_id = _capture_counter("admission_after_occurrence_id")
    plan_stage = Column(String(8), nullable=False, server_default=text("'base'"))
    plan_page_sequence = _capture_counter("plan_page_sequence")
    plan_after_base_root_record_id = _capture_counter("plan_after_base_root_record_id")
    plan_after_source_root_record_id = _capture_counter("plan_after_source_root_record_id")
    plan_complete_at = Column(TIMESTAMP(timezone=True))
    output_after_profile_slot = Column(SmallInteger)
    output_after_entity_binding_id = Column(BigInteger)
    output_after_context_key_sha256 = Column(LargeBinary(32))
    source_occurrence_count = _capture_counter("source_occurrence_count")
    candidate_error_count = _capture_counter("candidate_error_count")
    selected_family_count = _capture_counter("selected_family_count")
    completed_family_count = _capture_counter("completed_family_count")
    candidate_context_count = _capture_counter("candidate_context_count")
    generation_family_count = _capture_counter("generation_family_count")
    winner_count = _capture_counter("winner_count")
    next_rejection_ordinal = _capture_counter("next_rejection_ordinal")
    source_frozen_at = Column(TIMESTAMP(timezone=True))
    graph_frozen_at = Column(TIMESTAMP(timezone=True))
    output_frozen_at = Column(TIMESTAMP(timezone=True))
    verified_at = Column(TIMESTAMP(timezone=True))
    created_at = _timestamp_column()


class CustomImportBuildStream(_CustomImportModel):
    """Protected source replay cursor and current-attempt pack allocator."""

    __tablename__ = "custom_import_build_stream"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint(
            "stream_slot > 0 AND next_part_ordinal > 0 AND next_part_row_ordinal >= 0 AND "
            "next_source_ordinal >= 0 AND next_pack_ordinal >= 0",
            name="custom_import_build_stream_shape",
        )
    )

    build_id = Column(BigInteger, ForeignKey(_reference("custom_import_build_attempt", "build_id")), primary_key=True)
    stream_slot = Column(SmallInteger, primary_key=True)
    next_part_ordinal = Column(Integer, nullable=False, server_default=text("1"))
    next_part_row_ordinal = _capture_counter("next_part_row_ordinal")
    next_source_ordinal = _capture_counter("next_source_ordinal")
    next_pack_ordinal = Column(Integer, nullable=False, server_default=text("0"))
    replay_verified_at = Column(TIMESTAMP(timezone=True))


class CustomImportBuildOccurrence(_CustomImportModel):
    """Position/equality evidence referring to existing revisions or rejections."""

    __tablename__ = "custom_import_build_occurrence"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        ForeignKeyConstraint(
            ["build_id", "stream_slot"],
            [
                _reference("custom_import_build_stream", "build_id"),
                _reference("custom_import_build_stream", "stream_slot"),
            ],
            name="custom_import_build_occurrence_stream_fkey",
        ),
        CheckConstraint(
            "((origin = 'source' AND source_part_ordinal IS NOT NULL AND source_part_ordinal > 0 "
            "AND part_row_ordinal IS NOT NULL AND part_row_ordinal >= 0 AND source_ordinal IS NOT NULL "
            "AND source_ordinal >= 0 AND base_family_revision_id IS NULL AND base_root_revision_id IS NULL "
            "AND base_child_revision_id IS NULL) OR "
            "(origin = 'retained' AND source_part_ordinal IS NULL AND part_row_ordinal IS NULL "
            "AND source_ordinal IS NULL AND base_family_revision_id IS NOT NULL "
            "AND num_nonnulls(base_root_revision_id,base_child_revision_id) = 1 "
            "AND raw_parent_key_canonical IS NULL AND raw_parent_key_sha256 IS NULL "
            "AND rejection_id IS NULL AND resolved_rejection_id IS NULL)) AND "
            "((record_kind = 'root' AND collection_slot = 0 AND child_revision_id IS NULL "
            "AND child_key_sha256 IS NULL AND base_child_revision_id IS NULL) OR "
            "(record_kind = 'child' AND collection_slot > 0 AND root_revision_id IS NULL "
            "AND base_root_revision_id IS NULL)) AND "
            "num_nonnulls(root_revision_id,child_revision_id,rejection_id) = 1 AND "
            "((raw_parent_key_canonical IS NULL AND raw_parent_key_sha256 IS NULL) OR "
            "(raw_parent_key_canonical IS NOT NULL AND raw_parent_key_sha256 IS NOT NULL "
            "AND octet_length(raw_parent_key_sha256) = 32)) AND "
            "((child_revision_id IS NULL AND child_key_sha256 IS NULL) OR "
            "(child_revision_id IS NOT NULL AND child_key_sha256 IS NOT NULL AND octet_length(child_key_sha256) = 32))",
            name="custom_import_build_occurrence_shape",
        ),
        Index(
            "custom_import_build_source_position_key",
            "build_id",
            "stream_slot",
            "source_part_ordinal",
            "part_row_ordinal",
            unique=True,
            postgresql_where=text("origin = 'source'"),
        ),
        Index(
            "custom_import_build_source_ordinal_key",
            "build_id",
            "stream_slot",
            "source_ordinal",
            unique=True,
            postgresql_where=text("origin = 'source'"),
        ),
        Index(
            "custom_import_build_copy_root_key",
            "build_id",
            "base_family_revision_id",
            unique=True,
            postgresql_where=text("base_root_revision_id IS NOT NULL"),
        ),
        Index(
            "custom_import_build_copy_child_key",
            "build_id",
            "base_family_revision_id",
            "base_child_revision_id",
            unique=True,
            postgresql_where=text("base_child_revision_id IS NOT NULL"),
        ),
        Index(
            "custom_import_build_raw_parent_idx", "build_id", "record_kind", "raw_parent_key_sha256", "occurrence_id"
        ),
        Index(
            "custom_import_build_typed_root_idx", "build_id", "origin", "record_kind", "root_record_id", "occurrence_id"
        ),
        Index(
            "custom_import_build_source_child_idx",
            "build_id",
            "raw_parent_key_sha256",
            "collection_slot",
            "child_key_sha256",
            "occurrence_id",
            postgresql_where=text("origin = 'source'"),
        ),
        Index(
            "custom_import_build_graph_child_idx",
            "build_id",
            "origin",
            "root_record_id",
            "collection_slot",
            "child_key_sha256",
            "child_revision_id",
            postgresql_where=text("child_revision_id IS NOT NULL"),
        ),
        Index("custom_import_build_occurrence_pack_idx", "pack_id", "occurrence_id"),
        Index("custom_import_build_occurrence_page_idx", "build_id", "origin", "occurrence_id"),
    )

    occurrence_id = Column(BigInteger, primary_key=True, autoincrement=True)
    build_id = Column(BigInteger, nullable=False)
    stream_slot = Column(SmallInteger, nullable=False)
    pack_id = Column(BigInteger, ForeignKey(_reference("custom_import_pack", "pack_id")), nullable=False)
    origin = Column(String(8), nullable=False)
    source_part_ordinal = Column(Integer)
    part_row_ordinal = Column(BigInteger)
    source_ordinal = Column(BigInteger)
    base_family_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_family_revision", "family_revision_id"))
    )
    base_root_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_root_revision", "root_revision_id"))
    )
    base_child_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_child_revision", "child_revision_id"))
    )
    record_kind = Column(String(8), nullable=False)
    collection_slot = Column(SmallInteger, nullable=False)
    raw_parent_key_canonical = Column(Text)
    raw_parent_key_sha256 = Column(LargeBinary(32))
    root_record_id = Column(BigInteger, ForeignKey(_reference("custom_import_root_record", "root_record_id")))
    child_key_sha256 = Column(LargeBinary(32))
    root_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_root_revision", "root_revision_id")), unique=True
    )
    child_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_child_revision", "child_revision_id")), unique=True
    )
    rejection_id = Column(BigInteger, ForeignKey(_reference("custom_import_rejection", "rejection_id")))
    resolved_rejection_id = Column(BigInteger, ForeignKey(_reference("custom_import_rejection", "rejection_id")))


class CustomImportBuildFamily(_CustomImportModel):
    """SQL-selected source or retained family and its bounded build cursor."""

    __tablename__ = "custom_import_build_family"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("build_id", "family_revision_id", name="custom_import_build_family_output_key"),
        CheckConstraint(
            "((selection_kind = 'source' AND source_root_occurrence_id IS NOT NULL AND base_family_revision_id IS NULL) "
            "OR (selection_kind = 'retained' AND source_root_occurrence_id IS NULL AND base_family_revision_id IS NOT NULL)) "
            "AND (complete_at IS NULL OR family_revision_id IS NOT NULL) AND "
            "((last_child_collection_slot IS NULL AND last_child_key_sha256 IS NULL AND last_input_child_revision_id IS NULL) "
            "OR (last_child_collection_slot IS NOT NULL AND last_input_child_revision_id IS NOT NULL AND "
            "((selection_kind = 'source' AND last_child_key_sha256 IS NOT NULL AND octet_length(last_child_key_sha256)=32) "
            "OR (selection_kind = 'retained' AND last_child_key_sha256 IS NULL))))",
            name="custom_import_build_family_shape",
        ),
        Index(
            "custom_import_build_family_pending_idx",
            "build_id",
            "root_record_id",
            postgresql_where=text("complete_at IS NULL"),
        ),
        Index("custom_import_build_family_hash_idx", "build_id", "root_key_sha256", "root_record_id"),
        CheckConstraint("octet_length(root_key_sha256) = 32", name="custom_import_build_family_hash_shape"),
    )

    build_id = Column(BigInteger, ForeignKey(_reference("custom_import_build_attempt", "build_id")), primary_key=True)
    root_record_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_root_record", "root_record_id")), primary_key=True
    )
    root_key_sha256 = Column(LargeBinary(32), nullable=False)
    selection_kind = Column(String(8), nullable=False)
    source_root_occurrence_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_build_occurrence", "occurrence_id"))
    )
    base_family_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_family_revision", "family_revision_id"))
    )
    family_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_family_revision", "family_revision_id"))
    )
    last_child_collection_slot = Column(SmallInteger)
    last_child_key_sha256 = Column(LargeBinary(32))
    last_input_child_revision_id = Column(BigInteger)
    attached_child_count = _capture_counter("attached_child_count")
    complete_at = Column(TIMESTAMP(timezone=True))


class CustomImportBuildCandidateContext(_CustomImportModel):
    """All immutable selection candidates, including eventual losers."""

    __tablename__ = "custom_import_build_candidate_context"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        ForeignKeyConstraint(
            ["build_id", "family_revision_id"],
            [
                _reference("custom_import_build_family", "build_id"),
                _reference("custom_import_build_family", "family_revision_id"),
            ],
            name="custom_import_build_context_family_fkey",
            deferrable=True,
            initially="DEFERRED",
        ),
        UniqueConstraint(
            "build_id",
            "profile_slot",
            "family_revision_id",
            "context_child_revision_id",
            name="custom_import_build_context_candidate_key",
            postgresql_nulls_not_distinct=True,
        ),
        CheckConstraint(
            "profile_slot > 0 AND octet_length(canonical_context_key) BETWEEN 1 AND 8192 AND "
            "octet_length(context_key_sha256) = 32 AND "
            "((context_collection_slot = 0 AND context_child_revision_id IS NULL) OR "
            "(context_collection_slot > 0 AND context_child_revision_id IS NOT NULL))",
            name="custom_import_build_context_shape",
        ),
        Index(
            "custom_import_build_context_order_idx",
            "build_id",
            "profile_slot",
            "entity_binding_id",
            "context_key_sha256",
            "candidate_context_id",
        ),
    )

    candidate_context_id = Column(BigInteger, primary_key=True, autoincrement=True)
    build_id = Column(BigInteger, nullable=False)
    profile_slot = Column(SmallInteger, nullable=False)
    entity_binding_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_entity_binding", "entity_binding_id")), nullable=False
    )
    family_revision_id = Column(BigInteger, nullable=False)
    context_collection_slot = Column(SmallInteger, nullable=False)
    context_child_revision_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_child_revision", "child_revision_id"))
    )
    canonical_context_key = Column(Text, nullable=False)
    context_key_sha256 = Column(LargeBinary(32), nullable=False)


class CustomImportBuildVerification(_CustomImportModel):
    """SQL-derived structural scan progress; immutable when complete."""

    __tablename__ = "custom_import_build_verification"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint(
            "verification_contract = 'custom-import/build-structure/v1' AND "
            "((verification_state = 'scanning' AND scan_stage IN "
            "('capture','profiles','families','root_scalars','child_scalars','winners') AND verified_at IS NULL) OR "
            "(verification_state = 'complete' AND scan_stage = 'complete' AND verified_at IS NOT NULL)) AND "
            "((current_family_revision_id IS NULL AND current_family_expected_child_count IS NULL "
            "AND current_family_seen_child_count IS NULL) OR "
            "(current_family_revision_id IS NOT NULL AND current_family_expected_child_count IS NOT NULL "
            "AND current_family_seen_child_count IS NOT NULL AND current_family_expected_child_count >= 0 "
            "AND current_family_seen_child_count >= 0))",
            name="custom_import_build_verification_shape",
        ),
        CheckConstraint(
            "num_nonnulls(after_child_collection_slot,after_child_revision_id) IN (0,2) AND "
            "num_nonnulls(after_root_scalar_revision_id,after_root_scalar_field_slot) IN (0,2) AND "
            "num_nonnulls(after_child_scalar_revision_id,after_child_scalar_field_slot) IN (0,2) AND "
            "(num_nonnulls(after_winner_profile_slot,after_winner_entity_binding_id,after_winner_context_key_sha256)=0 OR "
            "(num_nonnulls(after_winner_profile_slot,after_winner_entity_binding_id,after_winner_context_key_sha256)=3 "
            "AND octet_length(after_winner_context_key_sha256)=32))",
            name="custom_import_build_verification_cursors",
        ),
    )

    build_id = Column(BigInteger, ForeignKey(_reference("custom_import_build_attempt", "build_id")), primary_key=True)
    generation_id = Column(
        BigInteger, ForeignKey(_reference("custom_import_generation", "generation_id")), nullable=False, unique=True
    )
    verification_contract = Column(
        String(63), nullable=False, server_default=text("'custom-import/build-structure/v1'")
    )
    verification_state = Column(String(16), nullable=False, server_default=text("'scanning'"))
    scan_stage = Column(String(16), nullable=False, server_default=text("'capture'"))
    page_sequence = _capture_counter("page_sequence")
    after_capture_stream_slot = Column(SmallInteger)
    after_profile_slot = Column(SmallInteger)
    after_root_record_id = Column(BigInteger)
    current_family_revision_id = Column(BigInteger)
    current_family_expected_child_count = Column(BigInteger)
    current_family_seen_child_count = Column(BigInteger)
    after_child_collection_slot = Column(SmallInteger)
    after_child_revision_id = Column(BigInteger)
    after_root_scalar_revision_id = Column(BigInteger)
    after_root_scalar_field_slot = Column(SmallInteger)
    after_child_scalar_revision_id = Column(BigInteger)
    after_child_scalar_field_slot = Column(SmallInteger)
    after_winner_profile_slot = Column(SmallInteger)
    after_winner_entity_binding_id = Column(BigInteger)
    after_winner_context_key_sha256 = Column(LargeBinary(32))
    root_count = _capture_counter("root_count")
    family_count = _capture_counter("family_count")
    generation_family_count = _capture_counter("generation_family_count")
    family_child_count = _capture_counter("family_child_count")
    winner_count = _capture_counter("winner_count")
    profile_count = _capture_counter("profile_count")
    root_scalar_count = _capture_counter("root_scalar_count")
    child_scalar_count = _capture_counter("child_scalar_count")
    source_frozen_at = Column(TIMESTAMP(timezone=True), nullable=False)
    graph_frozen_at = Column(TIMESTAMP(timezone=True), nullable=False)
    output_frozen_at = Column(TIMESTAMP(timezone=True), nullable=False)
    verified_at = Column(TIMESTAMP(timezone=True))
