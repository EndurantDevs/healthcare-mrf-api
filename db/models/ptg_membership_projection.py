# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable model family for one physical-source/code membership index."""

from sqlalchemy import BigInteger, CheckConstraint, Column, Index, Integer, PrimaryKeyConstraint, String
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base


class PTGMembershipManifest(Base):
    __tablename__ = "ptg_membership_manifest"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("singleton=1"),
        CheckConstraint("content_digest ~ '^[0-9a-f]{64}$'"),
        {"schema": "mrf"},
    )
    singleton = Column(Integer, primary_key=True, autoincrement=False)
    identity = Column(JSONB, nullable=False)
    content_digest = Column(String(64), nullable=False)
    counts = Column(JSONB, nullable=False)


class PTGMembershipFirst(Base):
    __tablename__ = "ptg_membership_first"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("owner_key", "member_key"),
        CheckConstraint("owner_key BETWEEN 0 AND 4294967295 AND member_key BETWEEN 0 AND 4294967295"),
        {"schema": "mrf"},
    )
    owner_key = Column(BigInteger, nullable=False)
    member_key = Column(BigInteger, nullable=False)


class PTGMembershipSecond(Base):
    __tablename__ = "ptg_membership_second"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("owner_key", "member_key"),
        CheckConstraint("owner_key BETWEEN 0 AND 4294967295 AND member_key BETWEEN 0 AND 4294967295"),
        Index("ptg_membership_second_member_idx", "member_key", "owner_key"),
        {"schema": "mrf"},
    )
    owner_key = Column(BigInteger, nullable=False)
    member_key = Column(BigInteger, nullable=False)


class PTGMembershipCode(Base):
    __tablename__ = "ptg_membership_code"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("code_key", "provider_set_key"),
        CheckConstraint("code_key BETWEEN 0 AND 4294967295 AND provider_set_key BETWEEN 0 AND 4294967295"),
        {"schema": "mrf"},
    )
    code_key = Column(BigInteger, nullable=False)
    provider_set_key = Column(BigInteger, nullable=False)


class PTGMembershipCodeCoverage(Base):
    __tablename__ = "ptg_membership_code_coverage"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "code_key BETWEEN 0 AND 4294967295 AND occurrence_count >= 0 AND set_count >= 0 AND set_count <= occurrence_count"
        ),
        CheckConstraint("set_digest ~ '^[0-9a-f]{64}$'"),
        {"schema": "mrf"},
    )
    code_key = Column(BigInteger, primary_key=True, autoincrement=False)
    occurrence_count = Column(BigInteger, nullable=False)
    set_count = Column(BigInteger, nullable=False)
    set_digest = Column(String(64), nullable=False)
