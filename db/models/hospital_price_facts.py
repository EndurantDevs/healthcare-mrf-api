# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless normalized CMS v3 hospital-price fact models."""

from __future__ import annotations

from sqlalchemy import ARRAY
from sqlalchemy import TEXT
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String

from db.connection import Base
from db.json_mixin import JSONOutputMixin
from db.models.hospital_price_header import _reference, _table_args


__all__ = (
    "HospitalPriceCharge",
    "HospitalPriceModifier",
    "HospitalPriceModifierPayer",
    "HospitalPricePayerCharge",
    "HospitalPriceService",
    "HospitalPriceServiceCode",
)


class HospitalPriceService(Base, JSONOutputMixin):
    """One source-ordered item or service within a verified version."""

    __tablename__ = "hospital_price_service"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "service_ordinal"),
        ForeignKeyConstraint(
            ["version_id"],
            [_reference("hospital_price_version", "version_id")],
            name="hospital_price_service_version_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "service_ordinal >= 0 AND description <> '' AND "
            "((drug_unit IS NULL AND drug_type IS NULL) OR "
            "(drug_unit > 0 AND drug_type IN "
            "('GR', 'ML', 'ME', 'UN', 'F2', 'GM', 'EA')))",
            name="hospital_price_service_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "service_ordinal"]

    version_id = Column(String(64), nullable=False)
    service_ordinal = Column(Integer, nullable=False)
    description = Column(TEXT, nullable=False)
    drug_unit = Column(Numeric)
    drug_type = Column(String(2))


class HospitalPriceServiceCode(Base, JSONOutputMixin):
    """One ordered billing code attached to an item or service."""

    __tablename__ = "hospital_price_service_code"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "service_ordinal", "code_ordinal"),
        ForeignKeyConstraint(
            ["version_id", "service_ordinal"],
            [
                _reference("hospital_price_service", "version_id"),
                _reference("hospital_price_service", "service_ordinal"),
            ],
            name="hospital_price_service_code_service_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "code_ordinal >= 0 AND code <> '' AND code_type IN "
            "('CPT', 'HCPCS', 'ICD', 'DRG', 'MS-DRG', 'R-DRG', "
            "'S-DRG', 'APS-DRG', 'AP-DRG', 'APR-DRG', 'TRIS-DRG', "
            "'APC', 'NDC', 'HIPPS', 'LOCAL', 'EAPG', 'CDT', 'RC', "
            "'CDM', 'CMG', 'MS-LTC-DRG')",
            name="hospital_price_service_code_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "service_ordinal", "code_ordinal"]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "code_type",
                "code",
                "version_id",
                "service_ordinal",
            ),
            "name": "hospital_price_service_code_lookup_idx",
        },
    ]

    version_id = Column(String(64), nullable=False)
    service_ordinal = Column(Integer, nullable=False)
    code_ordinal = Column(Integer, nullable=False)
    code_type = Column(String(16), nullable=False)
    code = Column(TEXT, nullable=False)


class HospitalPriceCharge(Base, JSONOutputMixin):
    """One source-ordered standard-charge object for an item or service."""

    __tablename__ = "hospital_price_charge"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "service_ordinal", "charge_ordinal"),
        ForeignKeyConstraint(
            ["version_id", "service_ordinal"],
            [
                _reference("hospital_price_service", "version_id"),
                _reference("hospital_price_service", "service_ordinal"),
            ],
            name="hospital_price_charge_service_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "charge_ordinal >= 0 AND setting IN ('inpatient', 'outpatient', 'both') "
            "AND (minimum IS NULL OR minimum > 0) "
            "AND (maximum IS NULL OR maximum > 0) "
            "AND (gross_charge IS NULL OR gross_charge > 0) "
            "AND (discounted_cash IS NULL OR discounted_cash > 0) "
            "AND (billing_class IS NULL OR billing_class IN "
            "('professional', 'facility', 'both'))",
            name="hospital_price_charge_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "service_ordinal", "charge_ordinal"]

    version_id = Column(String(64), nullable=False)
    service_ordinal = Column(Integer, nullable=False)
    charge_ordinal = Column(Integer, nullable=False)
    setting = Column(String(16), nullable=False)
    minimum = Column(Numeric)
    maximum = Column(Numeric)
    gross_charge = Column(Numeric)
    discounted_cash = Column(Numeric)
    modifier_codes = Column(ARRAY(TEXT))
    additional_generic_notes = Column(TEXT)
    billing_class = Column(String(16))


class HospitalPricePayerCharge(Base, JSONOutputMixin):
    """One exact payer-plan charge nested under a standard charge."""

    __tablename__ = "hospital_price_payer_charge"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "version_id",
            "service_ordinal",
            "charge_ordinal",
            "payer_ordinal",
        ),
        ForeignKeyConstraint(
            ["version_id", "service_ordinal", "charge_ordinal"],
            [
                _reference("hospital_price_charge", "version_id"),
                _reference("hospital_price_charge", "service_ordinal"),
                _reference("hospital_price_charge", "charge_ordinal"),
            ],
            name="hospital_price_payer_charge_charge_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "payer_ordinal >= 0 AND payer_name <> '' AND plan_name <> '' "
            "AND methodology IN ('case rate', 'fee schedule', "
            "'percent of total billed charges', 'per diem', 'other') "
            "AND (standard_charge_dollar IS NULL OR standard_charge_dollar > 0) "
            "AND (standard_charge_percentage IS NULL OR standard_charge_percentage > 0) "
            "AND (median_amount IS NULL OR median_amount > 0) "
            "AND (percentile_10 IS NULL OR percentile_10 > 0) "
            "AND (percentile_90 IS NULL OR percentile_90 > 0) "
            "AND (standard_charge_dollar IS NOT NULL "
            "OR standard_charge_percentage IS NOT NULL "
            "OR standard_charge_algorithm IS NOT NULL) "
            "AND (allowed_count IS NULL OR allowed_count ~ "
            "'^(0|1 through 10|1[1-9]|[2-9][0-9]+|[1-9][0-9]{2,})$') "
            "AND ((standard_charge_percentage IS NULL "
            "AND standard_charge_algorithm IS NULL) OR allowed_count IS NOT NULL)",
            name="hospital_price_payer_charge_shape_check",
        ),
    )
    __my_index_elements__ = [
        "version_id",
        "service_ordinal",
        "charge_ordinal",
        "payer_ordinal",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "payer_name",
                "plan_name",
                "version_id",
                "service_ordinal",
                "charge_ordinal",
            ),
            "name": "hospital_price_payer_charge_lookup_idx",
        },
    ]

    version_id = Column(String(64), nullable=False)
    service_ordinal = Column(Integer, nullable=False)
    charge_ordinal = Column(Integer, nullable=False)
    payer_ordinal = Column(Integer, nullable=False)
    payer_name = Column(TEXT, nullable=False)
    plan_name = Column(TEXT, nullable=False)
    methodology = Column(String(64), nullable=False)
    standard_charge_dollar = Column(Numeric)
    standard_charge_percentage = Column(Numeric)
    standard_charge_algorithm = Column(TEXT)
    median_amount = Column(Numeric)
    percentile_10 = Column(Numeric)
    percentile_90 = Column(Numeric)
    allowed_count = Column(String(32))
    additional_payer_notes = Column(TEXT)


class HospitalPriceModifier(Base, JSONOutputMixin):
    """One source-ordered top-level modifier definition."""

    __tablename__ = "hospital_price_modifier"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "modifier_ordinal"),
        ForeignKeyConstraint(
            ["version_id"],
            [_reference("hospital_price_version", "version_id")],
            name="hospital_price_modifier_version_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "modifier_ordinal >= 0 AND code <> '' AND description <> '' "
            "AND (setting IS NULL OR setting IN ('inpatient', 'outpatient', 'both')) "
            "AND (additional_generic_notes IS NULL "
            "OR btrim(additional_generic_notes) <> '')",
            name="hospital_price_modifier_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "modifier_ordinal"]

    version_id = Column(String(64), nullable=False)
    modifier_ordinal = Column(Integer, nullable=False)
    code = Column(TEXT, nullable=False)
    description = Column(TEXT, nullable=False)
    setting = Column(String(16))
    additional_generic_notes = Column(TEXT)


class HospitalPriceModifierPayer(Base, JSONOutputMixin):
    """One source-ordered payer-plan explanation for a modifier."""

    __tablename__ = "hospital_price_modifier_payer"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("version_id", "modifier_ordinal", "payer_ordinal"),
        ForeignKeyConstraint(
            ["version_id", "modifier_ordinal"],
            [
                _reference("hospital_price_modifier", "version_id"),
                _reference("hospital_price_modifier", "modifier_ordinal"),
            ],
            name="hospital_price_modifier_payer_modifier_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "payer_ordinal >= 0 AND payer_name <> '' AND plan_name <> '' "
            "AND (description IS NULL OR btrim(description) <> '') "
            "AND (standard_charge_dollar IS NULL OR standard_charge_dollar > 0) "
            "AND (standard_charge_percentage IS NULL "
            "OR standard_charge_percentage > 0) "
            "AND (standard_charge_algorithm IS NULL "
            "OR btrim(standard_charge_algorithm) <> '') "
            "AND (description IS NOT NULL OR standard_charge_dollar IS NOT NULL "
            "OR standard_charge_percentage IS NOT NULL "
            "OR standard_charge_algorithm IS NOT NULL)",
            name="hospital_price_modifier_payer_shape_check",
        ),
    )
    __my_index_elements__ = ["version_id", "modifier_ordinal", "payer_ordinal"]

    version_id = Column(String(64), nullable=False)
    modifier_ordinal = Column(Integer, nullable=False)
    payer_ordinal = Column(Integer, nullable=False)
    payer_name = Column(TEXT, nullable=False)
    plan_name = Column(TEXT, nullable=False)
    description = Column(TEXT)
    standard_charge_dollar = Column(Numeric)
    standard_charge_percentage = Column(Numeric)
    standard_charge_algorithm = Column(TEXT)
