# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable publication requests carry session hashes, never browser credentials."""

from sqlalchemy import TIMESTAMP, CheckConstraint, Column, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("RegistryPublicationRequest",)


class RegistryPublicationRequest(Base):
    __tablename__ = "registry_publication_request"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("request_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="registry_request_identity"),
        CheckConstraint("state IN ('queued','running','completed','rejected')", name="registry_request_state"),
        CheckConstraint(
            "actor_key ~ '^[0-9a-f]{64}$' AND session_token_sha256 ~ '^[0-9a-f]{64}$' AND request_sha256 ~ '^[0-9a-f]{64}$'",
            name="registry_request_digests",
        ),
        CheckConstraint(
            "jsonb_typeof(actor_json)='object' AND jsonb_typeof(command_json)='object' AND (result_json IS NULL OR jsonb_typeof(result_json)='object')",
            name="registry_request_json",
        ),
        CheckConstraint("btrim(idempotency_key)<>''", name="registry_request_replay_key"),
        CheckConstraint(
            "(state='running' AND lease_id IS NOT NULL AND lease_expires_at IS NOT NULL) OR (state<>'running' AND lease_id IS NULL AND lease_expires_at IS NULL)",
            name="registry_request_lease",
        ),
        UniqueConstraint("actor_key", "idempotency_key", name="registry_request_replay"),
        {"schema": registry_schema()},
    )
    request_id = Column(UUID(as_uuid=True), primary_key=True)
    actor_key = Column(String(64), nullable=False)
    session_token_sha256 = Column(String(64), nullable=False)
    actor_json = Column(JSONB, nullable=False)
    command_json = Column(JSONB, nullable=False)
    idempotency_key = Column(String(128), nullable=False)
    request_sha256 = Column(String(64), nullable=False)
    state = Column(String(16), nullable=False, server_default="queued")
    lease_id = Column(UUID(as_uuid=True))
    lease_expires_at = Column(TIMESTAMP(timezone=True))
    result_json = Column(JSONB)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
