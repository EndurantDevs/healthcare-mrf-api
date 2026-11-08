# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete native canonical-address model shared by ordinary and snapshot paths."""

import os

from sqlalchemy import (
    DATE,
    SMALLINT,
    TEXT,
    TIMESTAMP,
    CheckConstraint,
    Column,
    Enum,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = ("AddressArchiveV2",)


class AddressArchiveV2(Base, JSONOutputMixin):
    __tablename__ = "address_archive_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("address_key"),
        UniqueConstraint("identity_key"),
        CheckConstraint("precision IN ('street', 'city_zip')"),
        CheckConstraint(
            "strict_source_bits >= 0 AND (strict_source_bits & source_bits) = strict_source_bits",
            name="address_archive_v2_strict_source_bits_ck",
        ),
        Index("address_archive_v2_zip5_line1_norm_idx", "zip5", "line1_norm"),
        Index("address_archive_v2_state_city_idx", "state_code", "city_norm"),
        Index("address_archive_v2_premise_key_idx", "premise_key", postgresql_where=text("premise_key IS NOT NULL")),
        Index(
            "address_archive_v2_missing_geo_shard_idx",
            "state_code",
            "zip5",
            postgresql_where=text(
                "lat IS NULL AND long IS NULL AND COALESCE(country_code, 'US') = 'US' "
                "AND precision = 'street' AND state_code IS NOT NULL AND zip5 IS NOT NULL"
            ),
        ),
        Index(
            "address_archive_v2_completion_scope_idx",
            "state_code",
            "zip5",
            text("COALESCE(country_code, 'US')"),
            text("COALESCE(unit_norm, '')"),
            postgresql_where=text(
                "address_key IS NOT NULL AND identity_key IS NOT NULL "
                "AND COALESCE(precision, split_part(identity_key, '|', 8)) = 'street' "
                "AND merged_into IS NULL AND state_code IS NOT NULL AND zip5 IS NOT NULL"
            ),
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["address_key"]

    address_key = Column(PG_UUID(as_uuid=True), nullable=False)
    identity_key = Column(TEXT, nullable=False)
    identity_version = Column(SMALLINT, nullable=False, server_default=text("2"))
    precision = Column(TEXT, nullable=False, server_default=text("'street'"))
    premise_key = Column(PG_UUID(as_uuid=True))
    line1_norm = Column(TEXT)
    unit_norm = Column(TEXT, nullable=False, server_default=text("''"))
    city_norm = Column(TEXT)
    state_code = Column(String(32))
    zip5 = Column(String(5))
    zip4 = Column(String(4))
    country_code = Column(String(8), nullable=False, server_default=text("'US'"))
    first_line = Column(TEXT)
    second_line = Column(TEXT)
    city_name = Column(TEXT)
    state_name = Column(TEXT)
    postal_code = Column(TEXT)
    telephone_number = Column(TEXT)
    fax_number = Column(TEXT)
    formatted_address = Column(TEXT)
    formatted_address_version = Column(SMALLINT)
    formatted_address_source = Column(String(32))
    lat = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    long = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    place_id = Column(TEXT)
    geo_source = Column(
        Enum(
            "mapbox",
            "google",
            "tiger",
            "manual",
            "openaddresses",
            name="address_archive_geo_source",
            native_enum=True,
            create_type=False,
            schema=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
        )
    )
    geocode_source = Column(TEXT)
    geocode_quality = Column(TEXT)
    postal_validation_status = Column(TEXT)
    geocoded_at = Column(TIMESTAMP(timezone=True))
    source_bits = Column(Integer, nullable=False, server_default=text("0"))
    strict_source_bits = Column(Integer, nullable=False, server_default=text("0"))
    first_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("now()"))
    last_seen_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("now()"))
    date_added = Column(DATE)
    display_priority = Column(SMALLINT, nullable=False, server_default=text("9"))
    merged_into = Column(PG_UUID(as_uuid=True))
