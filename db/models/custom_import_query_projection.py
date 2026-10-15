# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Low-volume custody and completion evidence for derived query cells."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    LargeBinary,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)

from db.models.custom_import import _CustomImportModel, _reference, _table_args, _timestamp_column

__all__ = ("CustomImportQueryProjection",)

QUERY_PROJECTION_CONTRACT = "custom-import/query-projection/v1"


class CustomImportQueryProjection(_CustomImportModel):
    """One fenced snapshot projection, immutable after the normal snapshot freeze."""

    __tablename__ = "custom_import_query_projection"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("generation_id", name="custom_import_query_projection_generation_key"),
        ForeignKeyConstraint(
            ["generation_id", "dataset_id", "definition_revision_id", "schema_revision_id"],
            [
                _reference("custom_import_generation", name)
                for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id")
            ],
            name="custom_import_query_projection_generation_fkey",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            f"projection_contract = '{QUERY_PROJECTION_CONTRACT}' AND "
            "dataset_id > 0 AND definition_revision_id > 0 AND schema_revision_id > 0 AND "
            "execution_id > 0 AND producing_fence > 0 AND octet_length(producing_token_sha256) = 32 AND "
            "octet_length(definition_sha256) = 32 AND octet_length(capability_sha256) = 32 AND "
            "after_entity_binding_id >= 0 AND root_count >= 0 AND child_count >= 0",
            name="custom_import_query_projection_shape",
        ),
        CheckConstraint(
            "root_table_oid > 0 AND root_table_owner > 0 AND octet_length(root_columns_sha256) = 32 AND "
            "child_table_oid > 0 AND child_table_owner > 0 AND octet_length(child_columns_sha256) = 32 AND "
            "root_table_oid <> child_table_oid",
            name="custom_import_query_projection_custody",
        ),
        CheckConstraint(
            "phase IN ('root','child','indexes','verify_root','verify_child','digest_root','digest_child','complete') AND "
            "((phase <> 'complete' AND complete_at IS NULL AND projection_sha256 IS NULL) OR "
            "(phase = 'complete' AND complete_at IS NOT NULL AND projection_sha256 IS NOT NULL AND "
            "octet_length(projection_sha256) = 32))",
            name="custom_import_query_projection_completion",
        ),
        CheckConstraint(
            "((after_entity_binding_id = 0 AND after_selected_value IS NULL AND after_child_key_sha256 IS NULL) OR "
            "(after_entity_binding_id > 0 AND after_selected_value IS NOT NULL AND "
            "(after_child_key_sha256 IS NULL OR octet_length(after_child_key_sha256) = 32))) AND "
            "(phase <> 'root' OR after_child_key_sha256 IS NULL)",
            name="custom_import_query_projection_cursor",
        ),
        CheckConstraint(
            "index_count BETWEEN 0 AND 33 AND (phase NOT IN ('root','child') OR index_count = 0)",
            name="custom_import_query_projection_index_progress",
        ),
        CheckConstraint(
            "verified_root_count BETWEEN 0 AND root_count AND verified_child_count BETWEEN 0 AND child_count AND "
            "digested_root_count BETWEEN 0 AND root_count AND digested_child_count BETWEEN 0 AND child_count AND "
            "verified_root_count::numeric + verified_child_count + digested_root_count + digested_child_count <= 9223372036854775807 AND "
            "(digest_chain_sha256 IS NULL OR octet_length(digest_chain_sha256) = 32) AND "
            "(phase <> 'complete' OR (verified_root_count = root_count AND verified_child_count = child_count AND "
            "digested_root_count = root_count AND digested_child_count = child_count AND digest_chain_sha256 IS NOT NULL))",
            name="custom_import_query_projection_proof_progress",
        ),
        CheckConstraint(
            "((digest_after_entity IS NULL AND digest_after_selected_value IS NULL AND digest_after_child_key IS NULL AND "
            "digested_root_count = 0 AND digested_child_count = 0) OR "
            "(digest_after_entity IS NOT NULL AND octet_length(digest_after_entity) BETWEEN 1 AND 512 AND "
            "digest_after_selected_value IS NOT NULL AND digested_root_count::numeric + digested_child_count > 0 AND "
            "((digested_child_count = 0 AND digest_after_child_key IS NULL) OR "
            "(digested_child_count > 0 AND digest_after_child_key IS NOT NULL AND "
            "octet_length(digest_after_child_key) BETWEEN 1 AND 8192))))",
            name="custom_import_query_projection_digest_cursor",
        ),
    )

    family_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_snapshot_family", "family_id"), ondelete="RESTRICT"),
        primary_key=True,
        autoincrement=False,
    )
    generation_id = Column(BigInteger, nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    producing_fence = Column(BigInteger, nullable=False)
    producing_token_sha256 = Column(LargeBinary(32), nullable=False)
    projection_contract = Column(String(63), nullable=False, server_default=text(f"'{QUERY_PROJECTION_CONTRACT}'"))
    definition_sha256 = Column(LargeBinary(32), nullable=False)
    capability_sha256 = Column(LargeBinary(32), nullable=False)
    root_table_oid = Column(BigInteger, nullable=False)
    root_table_owner = Column(BigInteger, nullable=False)
    root_columns_sha256 = Column(LargeBinary(32), nullable=False)
    child_table_oid = Column(BigInteger, nullable=False)
    child_table_owner = Column(BigInteger, nullable=False)
    child_columns_sha256 = Column(LargeBinary(32), nullable=False)
    phase = Column(String(16), nullable=False, server_default=text("'root'"))
    after_entity_binding_id = Column(BigInteger, nullable=False, server_default=text("0"))
    after_selected_value = Column(BigInteger)
    after_child_key_sha256 = Column(LargeBinary(32))
    index_count = Column(SmallInteger, nullable=False, server_default=text("0"))
    root_count = Column(BigInteger, nullable=False, server_default=text("0"))
    child_count = Column(BigInteger, nullable=False, server_default=text("0"))
    verified_root_count = Column(BigInteger, nullable=False, server_default=text("0"))
    verified_child_count = Column(BigInteger, nullable=False, server_default=text("0"))
    digested_root_count = Column(BigInteger, nullable=False, server_default=text("0"))
    digested_child_count = Column(BigInteger, nullable=False, server_default=text("0"))
    digest_chain_sha256 = Column(LargeBinary(32))
    digest_after_entity = Column(Text(collation="C"))
    digest_after_selected_value = Column(BigInteger)
    digest_after_child_key = Column(Text(collation="C"))
    projection_sha256 = Column(LargeBinary(32))
    complete_at = Column(TIMESTAMP(timezone=True))
    created_at = _timestamp_column()
