# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit managed model for immutable common serving receipts."""

import os

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    Computed,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.types import UserDefinedType

from db.cms_registry_source_pair_slot import CHECK_NAME, CHECK_SQL, INDEX_NAME
from db.connection import Base

_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


class _Xid8(UserDefinedType):
    """SQLAlchemy has no built-in PostgreSQL full transaction-id type."""

    cache_ok = True

    def get_col_spec(self, **_options):
        """Compile the existing native transaction-id column without coercion."""
        return "xid8"


class ProviderDirectoryCMSServingReceipt(Base):
    """Native migrations own all generated columns, immutability and deferred guards."""

    __runtime_schema_sync__ = False
    __tablename__ = "provider_directory_cms_serving_receipt"
    __table_args__ = (
        CheckConstraint("receipt_id=encode(sha256(convert_to(payload::text,'UTF8')),'hex')"),
        CheckConstraint(CHECK_SQL, name=CHECK_NAME),
        UniqueConstraint("publication_xid"),
        UniqueConstraint("address_lineage_id", "address_generation", "profile_generation_id"),
        UniqueConstraint("predecessor_receipt_id"),
        Index(
            "cms_serving_receipt_one_root",
            text("(true)"),
            unique=True,
            postgresql_where=text("predecessor_receipt_id IS NULL"),
        ),
        Index("cms_serving_receipt_profile_history", "profile_generation_id", "created_at", "receipt_id"),
        Index(
            INDEX_NAME,
            text("(registry_source_pair_json->>'capture_id')"),
            unique=True,
            postgresql_where=text("registry_source_pair_json IS NOT NULL"),
        ),
        {"schema": _SCHEMA, "info": {"managed_immutable_storage": True}},
    )

    receipt_id = Column(String(64), primary_key=True)
    payload = Column(JSONB, nullable=False)
    publication_xid = Column(_Xid8(), nullable=False, server_default=text("pg_current_xact_id()"))
    predecessor_receipt_id = Column(
        String(64),
        Computed("payload->>'predecessor_receipt_id'", persisted=True),
        ForeignKey(f"{_SCHEMA}.provider_directory_cms_serving_receipt.receipt_id"),
    )
    address_lineage_id = Column(
        UUID, Computed("(payload->'address'->>'local_lineage_id')::uuid", persisted=True), nullable=False
    )
    address_generation = Column(
        BigInteger, Computed("(payload->'address'->>'local_generation')::bigint", persisted=True), nullable=False
    )
    profile_generation_id = Column(
        Text, Computed("payload->'profile'->>'generation_id'", persisted=True), nullable=False
    )
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("clock_timestamp()"))
    registry_source_pair_json = Column(JSONB, nullable=True)
