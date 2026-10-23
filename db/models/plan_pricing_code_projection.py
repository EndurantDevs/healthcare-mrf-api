# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Isolated native storage for one complete release/procedure capability."""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from db.connection import Base


class PlanPricingCodeManifest(Base):
    __tablename__ = "plan_pricing_code_manifest"
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


class PlanPricingCodeMembership(Base):
    __tablename__ = "plan_pricing_code_membership"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("binding_ordinal", "provider_set_key", "npi"),
        CheckConstraint("binding_ordinal>=0 AND provider_set_key>=0 AND npi>0"),
        Index("plan_pricing_code_membership_npi_idx", "npi", "binding_ordinal", "provider_set_key"),
        {"schema": "mrf"},
    )
    binding_ordinal = Column(Integer, nullable=False)
    provider_set_key = Column(BigInteger, nullable=False)
    npi = Column(BigInteger, nullable=False)


class PlanPricingCodeProviderCell(Base):
    __tablename__ = "plan_pricing_code_provider_cell"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("npi", "geo_cell"),
        CheckConstraint("npi>0 AND geo_cell ~ '^[0-9]{5}$'"),
        {"schema": "mrf"},
    )
    npi = Column(BigInteger, nullable=False)
    geo_cell = Column(String(5), nullable=False)
    entity_type_code = Column(SmallInteger, nullable=True)
    taxonomy_codes = Column(ARRAY(String), nullable=False)
    fragment = Column(LargeBinary, nullable=False)


class PlanPricingCodeProviderState(Base):
    __tablename__ = "plan_pricing_code_provider_state"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("state", "npi"),
        CheckConstraint("npi>0 AND state ~ '^[A-Z]{2}$'"),
        {"schema": "mrf"},
    )
    state = Column(String(2), nullable=False)
    npi = Column(BigInteger, nullable=False)
    provider_fragment = Column(LargeBinary, nullable=False)


class PlanPricingCodeOccurrence(Base):
    __tablename__ = "plan_pricing_code_occurrence"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("binding_ordinal", "occurrence_ordinal"),
        CheckConstraint(
            "binding_ordinal>=0 AND occurrence_ordinal>=0 AND provider_set_key>=0 AND price_key>=0 AND source_artifact_key>=0 AND provider_count>=0 AND occurrence_multiplicity>0"
        ),
        Index("plan_pricing_code_occurrence_set_idx", "binding_ordinal", "provider_set_key", "occurrence_ordinal"),
        {"schema": "mrf"},
    )
    binding_ordinal = Column(Integer, nullable=False)
    occurrence_ordinal = Column(BigInteger, nullable=False)
    provider_set_key = Column(BigInteger, nullable=False)
    provider_set_ref = Column(String(32), nullable=False)
    price_key = Column(BigInteger, nullable=False)
    price_set_ref = Column(String(32), nullable=False)
    rate_pack_ref = Column(String(32), nullable=False)
    source_artifact_key = Column(BigInteger, nullable=False)
    provider_count = Column(BigInteger, nullable=False)
    group_fragment = Column(JSONB, nullable=False)
    occurrence_multiplicity = Column(BigInteger, nullable=False)


class PlanPricingCodeAggregatePack(Base):
    __tablename__ = "plan_pricing_code_aggregate_pack"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("zip_prefix_2 ~ '^[0-9]{2}$' AND entry_count>0 AND raw_byte_count>0 AND stored_byte_count>0"),
        CheckConstraint(
            "logical_digest ~ '^[0-9a-f]{64}$' AND octet_length(payload_sha256)=32 AND octet_length(payload)=stored_byte_count"
        ),
        {"schema": "mrf"},
    )
    zip_prefix_2 = Column(String(2), primary_key=True, autoincrement=False)
    entry_count = Column(BigInteger, nullable=False)
    raw_byte_count = Column(BigInteger, nullable=False)
    stored_byte_count = Column(BigInteger, nullable=False)
    logical_digest = Column(String(64), nullable=False)
    payload_sha256 = Column(LargeBinary, nullable=False)
    payload = Column(LargeBinary, nullable=False)


class PlanPricingCodeRateProfile(Base):
    __tablename__ = "plan_pricing_code_rate_profile"
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("binding_ordinal", "provider_set_key"),
        CheckConstraint(
            "binding_ordinal>=0 AND provider_set_key>=0 AND membership_count>0 AND minimum_negotiated_rate>=0 AND maximum_negotiated_rate>=minimum_negotiated_rate AND rate_count>0"
        ),
        {"schema": "mrf"},
    )
    binding_ordinal = Column(Integer, nullable=False)
    provider_set_key = Column(BigInteger, nullable=False)
    membership_count = Column(BigInteger, nullable=False)
    minimum_negotiated_rate = Column(Numeric, nullable=False)
    maximum_negotiated_rate = Column(Numeric, nullable=False)
    rate_count = Column(BigInteger, nullable=False)
    negotiated_rates = Column(ARRAY(Numeric), nullable=False)
    rate_multiplicities = Column(ARRAY(BigInteger), nullable=False)
