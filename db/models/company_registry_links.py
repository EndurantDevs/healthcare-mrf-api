# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit manual company relationships with independent draft revisions."""

import os

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("CompanyRegistryLinks", "RegistryCompanyLinkBatch")


class CompanyRegistryLinks(Base):
    __tablename__ = "company_registry_links"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("revision > 0", name="registry_company_links_revision"),
        CheckConstraint(
            "cardinality(network_ids)<=5000 AND array_position(network_ids,NULL) IS NULL AND 0<ALL(network_ids)",
            name="registry_company_links_networks",
        ),
        CheckConstraint(
            "group_id IS NULL OR group_id<>'00000000-0000-0000-0000-000000000000'::uuid",
            name="registry_company_links_group",
        ),
        CheckConstraint(
            "jsonb_typeof(network_assertions)='array' AND jsonb_array_length(network_assertions)<=5000 "
            "AND octet_length(network_assertions::text)<=16777216",
            name="registry_company_links_assertions",
        ),
        {"schema": registry_schema()},
    )
    company_id = Column(UUID(as_uuid=True), primary_key=True)
    network_ids = Column(ARRAY(Integer), nullable=False, server_default=text("'{}'::integer[]"))
    group_id = Column(UUID(as_uuid=True))
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    network_assertions = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))


class RegistryCompanyLinkBatch(Base):
    """Immutable actor-scoped receipts for one whole selected-company revision."""

    __tablename__ = "registry_company_link_batch"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "batch_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="registry_company_link_batch_identity"
        ),
        CheckConstraint("actor_sha256 ~ '^[0-9a-f]{64}$'", name="registry_company_link_batch_actor"),
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$'", name="registry_company_link_batch_request"),
        CheckConstraint(
            "idempotency_key<>'' AND idempotency_key=btrim(idempotency_key) AND idempotency_key !~ '[[:cntrl:]]'",
            name="registry_company_link_batch_key",
        ),
        CheckConstraint("custom_revision>0", name="registry_company_link_batch_revision"),
        CheckConstraint(
            "jsonb_typeof(result_json)='object' AND octet_length(result_json::text)<=1048576",
            name="registry_company_link_batch_result",
        ),
        UniqueConstraint("actor_sha256", "idempotency_key", name="registry_company_link_batch_replay"),
        {"schema": registry_schema()},
    )
    batch_id = Column(UUID(as_uuid=True), primary_key=True)
    actor_sha256 = Column(String(64), nullable=False)
    idempotency_key = Column(String(128), nullable=False)
    request_sha256 = Column(String(64), nullable=False)
    custom_revision = Column(BigInteger, nullable=False)
    result_json = Column(JSONB, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
