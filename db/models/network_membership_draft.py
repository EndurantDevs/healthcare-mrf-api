# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit custom office memberships survive imported source replacements."""

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, CheckConstraint, Column, Integer, func, text
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("NetworkMembershipDraft",)


class NetworkMembershipDraft(Base):
    __tablename__ = "network_membership_draft"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("network_id>0 AND revision>0", name="registry_membership_identity"),
        CheckConstraint("jsonb_typeof(memberships_json)='array'", name="registry_membership_rows"),
        {"schema": registry_schema()},
    )

    network_id = Column(Integer, primary_key=True)
    memberships_json = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
