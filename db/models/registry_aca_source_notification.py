# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected accepted ACA receipts also hold their exact retained source archive."""

from sqlalchemy import CheckConstraint, Column, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import OID, TIMESTAMP, UUID

from db.connection import Base
from db.registry_aca_source_notification import MAX_BYTES, TABLE
from db.registry_schema import registry_schema

__all__ = ("RegistryACASourceNotification",)


class RegistryACASourceNotification(Base):
    """No ordinary runtime can update, delete or retire an accepted receipt."""

    __tablename__ = TABLE
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("capture_id<>'00000000-0000-0000-0000-000000000000'::uuid"),
        UniqueConstraint("source_schema"),
        CheckConstraint("source_schema='reference_family_archive_'||replace(capture_id::text,'-','')"),
        CheckConstraint("source_schema_oid<>0"),
        CheckConstraint("recipe_sha256 ~ '^[0-9a-f]{64}$'"),
        CheckConstraint("notification_sha256 ~ '^[0-9a-f]{64}$'"),
        CheckConstraint(f"octet_length(notification_json)<={MAX_BYTES}"),
        CheckConstraint("notification_sha256=encode(sha256(convert_to(notification_json,'UTF8')),'hex')"),
        {"schema": registry_schema(), "info": {"managed_immutable_storage": True}},
    )

    capture_id = Column(UUID(as_uuid=True), primary_key=True, nullable=False)
    source_schema = Column(Text, nullable=False)
    source_schema_oid = Column(OID, nullable=False)
    recipe_sha256 = Column(Text, nullable=False)
    notification_sha256 = Column(Text, nullable=False)
    notification_json = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("clock_timestamp()"))
