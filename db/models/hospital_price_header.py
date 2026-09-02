# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Verified hospital-price version headers and ordered source evidence."""

from __future__ import annotations

from sqlalchemy import TEXT
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy import UniqueConstraint
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin
from db.models.hospital_price import _reference, _table_args


__all__ = (
    "HospitalPriceContractProvision",
    "HospitalPriceVersion",
    "HospitalPriceVersionHospital",
    "HospitalPriceVersionLicense",
    "HospitalPriceVersionLocation",
    "HospitalPriceVersionNPI",
)


class HospitalPriceVersion(Base, JSONOutputMixin):
    """One verified parser-contract projection of immutable source content."""

    __tablename__ = "hospital_price_version"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id"),
        ForeignKeyConstraint(
            ["content_sha256"],
            [_reference("hospital_price_content", "content_sha256")],
            name="hospital_price_version_content_fkey",
        ),
        UniqueConstraint(
            "content_sha256",
            "parser_contract_sha256",
            name="hospital_price_version_projection_key",
        ),
        CheckConstraint(
            "version_id ~ '^[0-9a-f]{64}$' "
            "AND semantic_sha256 ~ '^[0-9a-f]{64}$' "
            "AND source_format IN ('json', 'csv-tall', 'csv-wide') "
            "AND ((parser_contract_sha256 IN ("
            "'6de516d11a99e85c00b9fe6488698a2a165436bf39d4351a0c54f58729150a66', "
            "'3857e492234361a91ebf6baa8c0c0d8832427b4bf5fce87729f15cd767c9be75') "
            "AND npi_count > 0 AND attester_name IS NOT NULL) OR ("
            "parser_contract_sha256 = "
            "'0048bd71229567de7ab5cbed73e7547d6718140dd8c4c9e39e3816c9798b8699' "
            "AND ((source_format = 'json' AND template_version IN "
            "('2.2.0', '2.2.1', '3.0.0')) OR (source_format IN "
            "('csv-tall', 'csv-wide') AND template_version IN "
            "('2.0.0', '2.2.0', '2.2.1', '3.0.0'))) "
            "AND ((template_version = '3.0.0' AND npi_count > 0 "
            "AND attester_name IS NOT NULL) OR (template_version IN "
            "('2.0.0', '2.2.0', '2.2.1') AND npi_count = 0 "
            "AND attester_name IS NULL))) OR ("
            "parser_contract_sha256 = "
            "'b432ff0aa9aec898d59d303344c63dd3805f37608a81dfd0118c99019afc16a1' "
            "AND ((source_format = 'json' AND template_version IN "
            "('2.2.0', '2.2.1', '3.0.0')) OR (source_format IN "
            "('csv-tall', 'csv-wide') AND template_version IN "
            "('2.0.0', '2.2.0', '2.2.1', '3.0.0'))) "
            "AND ((template_version = '3.0.0' AND npi_count > 0 "
            "AND attester_name IS NOT NULL) OR (template_version IN "
            "('2.0.0', '2.2.0', '2.2.1') AND ((source_format = 'json' "
            "AND npi_count = 0 AND attester_name IS NULL) OR "
            "(source_format IN ('csv-tall', 'csv-wide') AND npi_count >= 0 "
            "AND (attester_name IS NULL OR btrim(attester_name) <> ''))))) "
            "OR (parser_contract_sha256 = "
            "'1a632748216eb5373e2c55a29f328c2ce81aee3d3ae13e024bbc1c300fa10173' "
            "AND ((source_format = 'json' AND template_version IN "
            "('2.2.0', '2.2.1', '3.0.0')) OR (source_format IN "
            "('csv-tall', 'csv-wide') AND template_version IN "
            "('2', '2.0.0', '2.2.0', '2.2.1', '3.0.0'))) "
            "AND ((template_version = '3.0.0' AND npi_count > 0 "
            "AND attester_name IS NOT NULL) OR (template_version IN "
            "('2', '2.0.0', '2.2.0', '2.2.1') AND "
            "((source_format = 'json' AND npi_count = 0 "
            "AND attester_name IS NULL) OR (source_format IN "
            "('csv-tall', 'csv-wide') AND npi_count >= 0 "
            "AND (attester_name IS NULL OR btrim(attester_name) <> '')))))))) "
            "AND location_count > 0 AND license_count > 0 "
            "AND service_count > 0 AND charge_count > 0 "
            "AND payer_charge_count >= 0",
            name="hospital_price_version_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("content_sha256",),
            "name": "hospital_price_version_content_idx",
        },
    ]

    version_id = Column(String(64), nullable=False)
    content_sha256 = Column(String(64), nullable=False)
    parser_contract_sha256 = Column(String(64), nullable=False)
    semantic_sha256 = Column(String(64), nullable=False)
    source_format = Column(String(16), nullable=False)
    source_hospital_name = Column(TEXT, nullable=False)
    last_updated_on = Column(Date, nullable=False)
    template_version = Column(String(32), nullable=False)
    attestation_text = Column(TEXT, nullable=False)
    confirm_attestation = Column(Boolean, nullable=False)
    attester_name = Column(TEXT)
    location_count = Column(Integer, nullable=False)
    npi_count = Column(Integer, nullable=False)
    license_count = Column(Integer, nullable=False)
    service_count = Column(BigInteger, nullable=False)
    charge_count = Column(BigInteger, nullable=False)
    payer_charge_count = Column(BigInteger, nullable=False)
    verified_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )
    financial_aid_policy = Column(TEXT)


class HospitalPriceVersionLocation(Base, JSONOutputMixin):
    """One ordered source location name and corresponding physical address."""

    __tablename__ = "hospital_price_version_location"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "location_ordinal"),
        ForeignKeyConstraint(
            ["version_id"],
            [_reference("hospital_price_version", "version_id")],
            name="hospital_price_version_location_version_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "location_ordinal >= 0 AND ("
            "(location_name IS NOT NULL AND btrim(location_name) <> '') OR "
            "(hospital_address IS NOT NULL AND btrim(hospital_address) <> '')"
            ")",
            name="hospital_price_version_location_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "location_ordinal"]

    version_id = Column(String(64), nullable=False)
    location_ordinal = Column(Integer, nullable=False)
    location_name = Column(TEXT)
    hospital_address = Column(TEXT)


class HospitalPriceVersionNPI(Base, JSONOutputMixin):
    """One exact source-ordered type-2 NPI header value."""

    __tablename__ = "hospital_price_version_npi"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "npi_ordinal"),
        ForeignKeyConstraint(
            ["version_id"],
            [_reference("hospital_price_version", "version_id")],
            name="hospital_price_version_npi_version_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "npi_ordinal >= 0 AND npi <> ''",
            name="hospital_price_version_npi_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "npi_ordinal"]

    version_id = Column(String(64), nullable=False)
    npi_ordinal = Column(Integer, nullable=False)
    npi = Column(TEXT, nullable=False)


class HospitalPriceVersionLicense(Base, JSONOutputMixin):
    """One exact ordered license and jurisdiction from the source header."""

    __tablename__ = "hospital_price_version_license"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "license_ordinal"),
        ForeignKeyConstraint(
            ["version_id"],
            [_reference("hospital_price_version", "version_id")],
            name="hospital_price_version_license_version_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "license_ordinal >= 0 AND state <> ''",
            name="hospital_price_version_license_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "license_ordinal"]

    version_id = Column(String(64), nullable=False)
    license_ordinal = Column(Integer, nullable=False)
    state = Column(String(2), nullable=False)
    license_number = Column(TEXT)


class HospitalPriceContractProvision(Base, JSONOutputMixin):
    """One ordered source contract provision for a verified version."""

    __tablename__ = "hospital_price_contract_provision"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "provision_ordinal"),
        ForeignKeyConstraint(
            ["version_id"],
            [_reference("hospital_price_version", "version_id")],
            name="hospital_price_contract_provision_version_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "provision_ordinal >= 0 AND btrim(provisions) <> '' "
            "AND (payer_name IS NULL OR btrim(payer_name) <> '') "
            "AND (plan_name IS NULL OR btrim(plan_name) <> '')",
            name="hospital_price_contract_provision_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "provision_ordinal"]

    version_id = Column(String(64), nullable=False)
    provision_ordinal = Column(Integer, nullable=False)
    payer_name = Column(TEXT)
    plan_name = Column(TEXT)
    provisions = Column(TEXT, nullable=False)


class HospitalPriceVersionHospital(Base, JSONOutputMixin):
    """Bind one physical version to every hospital whose prices it supplies."""

    __tablename__ = "hospital_price_version_hospital"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "hospital_id"),
        ForeignKeyConstraint(
            ["version_id"],
            [_reference("hospital_price_version", "version_id")],
            name="hospital_price_version_hospital_version_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["hospital_id"],
            [_reference("hospital_price_hospital", "hospital_id")],
            name="hospital_price_version_hospital_hospital_fkey",
        ),
        ForeignKeyConstraint(
            ["version_id", "source_location_ordinal"],
            [
                _reference("hospital_price_version_location", "version_id"),
                _reference("hospital_price_version_location", "location_ordinal"),
            ],
            name="hospital_price_version_hospital_location_fkey",
        ),
    )
    __my_index_elements__ = ["version_id", "hospital_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("hospital_id", "version_id"),
            "name": "hospital_price_version_hospital_lookup_idx",
        },
    ]

    version_id = Column(String(64), nullable=False)
    hospital_id = Column(String(64), nullable=False)
    source_location_ordinal = Column(Integer)
