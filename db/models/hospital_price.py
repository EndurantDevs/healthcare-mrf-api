# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Hospital price registry, acquisition, publication, and provenance models."""

from __future__ import annotations

import os

from sqlalchemy import TEXT
from sqlalchemy import BigInteger
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy import UniqueConstraint
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


__all__ = (
    "HospitalPriceContent",
    "HospitalPriceCurrent",
    "HospitalPriceHospital",
    "HospitalPriceHospitalNPI",
    "HospitalPriceHospitalTaxIdentity",
    "HospitalPriceImportAttempt",
    "HospitalPriceLocator",
    "HospitalPriceLocatorObservation",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()


def _table_args(*constraints):
    return (*constraints, {"schema": _SCHEMA, "extend_existing": True})


def _reference(table: str, column: str) -> str:
    return f"{_SCHEMA}.{table}.{column}"


class HospitalPriceLocator(Base, JSONOutputMixin):
    """One deduplicated hospital-hosted machine-readable-file locator."""

    __tablename__ = "hospital_price_locator"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("locator_id"),
        UniqueConstraint("cms_hpt_url", name="hospital_price_locator_url_key"),
        CheckConstraint(
            "locator_id = btrim(locator_id) AND locator_id <> ''",
            name="hospital_price_locator_id_check",
        ),
    )
    __my_index_elements__ = ["locator_id"]

    locator_id = Column(String(64), nullable=False)
    cms_hpt_url = Column(TEXT, nullable=False)
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class HospitalPriceLocatorObservation(Base, JSONOutputMixin):
    """One retained locator check, including redirect and failure evidence."""

    __tablename__ = "hospital_price_locator_observation"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("observation_id"),
        ForeignKeyConstraint(
            ["locator_id"],
            [_reference("hospital_price_locator", "locator_id")],
            name="hospital_price_locator_observation_locator_fkey",
        ),
        UniqueConstraint(
            "locator_id",
            "observation_id",
            name="hospital_price_locator_observation_owner_key",
        ),
        CheckConstraint(
            "registry_version > 0 AND requested_url <> '' "
            "AND result_status = btrim(result_status) AND result_status <> '' "
            "AND (http_status IS NULL OR http_status BETWEEN 100 AND 599) "
            "AND ((response_sha256 IS NULL AND response_byte_count IS NULL) OR "
            "(response_sha256 ~ '^[0-9a-f]{64}$' AND response_byte_count > 0))",
            name="hospital_price_locator_observation_shape_check",
        ),
    )
    __my_index_elements__ = ["observation_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("locator_id", "checked_at DESC"),
            "name": "hospital_price_locator_observation_checked_idx",
        },
    ]

    observation_id = Column(String(64), nullable=False)
    locator_id = Column(String(64), nullable=False)
    registry_version = Column(Integer, nullable=False)
    requested_url = Column(TEXT, nullable=False)
    final_url = Column(TEXT)
    result_status = Column(String(32), nullable=False)
    http_status = Column(Integer)
    response_sha256 = Column(String(64))
    response_byte_count = Column(BigInteger)
    checked_at = Column(TIMESTAMP(timezone=True), nullable=False)
    error_code = Column(String(64))
    error_detail = Column(TEXT)


class HospitalPriceHospital(Base, JSONOutputMixin):
    """Stable project hospital identity with an optional canonical anchor ID."""

    __tablename__ = "hospital_price_hospital"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("hospital_id"),
        ForeignKeyConstraint(
            ["locator_id"],
            [_reference("hospital_price_locator", "locator_id")],
            name="hospital_price_hospital_locator_fkey",
        ),
        UniqueConstraint(
            "facility_anchor_id",
            name="hospital_price_hospital_facility_anchor_key",
        ),
        CheckConstraint(
            "hospital_id = btrim(hospital_id) AND hospital_id <> '' "
            "AND name = btrim(name) AND name <> '' AND registry_version > 0",
            name="hospital_price_hospital_identity_check",
        ),
    )
    __my_index_elements__ = ["hospital_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("locator_id",), "name": "hospital_price_hospital_locator_idx"},
    ]

    hospital_id = Column(String(64), nullable=False)
    facility_anchor_id = Column(String(128))
    locator_id = Column(String(64), nullable=False)
    name = Column(String(256), nullable=False)
    registry_version = Column(Integer, nullable=False)
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class HospitalPriceContent(Base, JSONOutputMixin):
    """Deduplicated immutable bytes acquired by one or more attempts."""

    __tablename__ = "hospital_price_content"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("content_sha256"),
        CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$' AND byte_count > 0",
            name="hospital_price_content_identity_check",
        ),
    )
    __my_index_elements__ = ["content_sha256"]

    content_sha256 = Column(String(64), nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    media_type = Column(String(128))
    acquired_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class HospitalPriceImportAttempt(Base, JSONOutputMixin):
    """Per-hospital refresh attempt retaining its optimistic publication fence."""

    __tablename__ = "hospital_price_import_attempt"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("attempt_id"),
        ForeignKeyConstraint(
            ["hospital_id"],
            [_reference("hospital_price_hospital", "hospital_id")],
            name="hospital_price_import_attempt_hospital_fkey",
        ),
        ForeignKeyConstraint(
            ["locator_id", "locator_observation_id"],
            [
                _reference("hospital_price_locator_observation", "locator_id"),
                _reference("hospital_price_locator_observation", "observation_id"),
            ],
            name="hospital_price_import_attempt_observation_owner_fkey",
        ),
        ForeignKeyConstraint(
            ["content_sha256"],
            [_reference("hospital_price_content", "content_sha256")],
            name="hospital_price_import_attempt_content_fkey",
        ),
        ForeignKeyConstraint(
            ["version_id", "hospital_id"],
            [
                _reference("hospital_price_version_hospital", "version_id"),
                _reference("hospital_price_version_hospital", "hospital_id"),
            ],
            name="hospital_price_import_attempt_version_hospital_fkey",
        ),
        UniqueConstraint(
            "hospital_id",
            "attempt_id",
            name="hospital_price_import_attempt_owner_key",
        ),
        UniqueConstraint(
            "hospital_id",
            "version_id",
            "attempt_id",
            name="hospital_price_import_attempt_version_owner_key",
        ),
        CheckConstraint(
            "registry_version > 0 AND expected_generation >= 0 "
            "AND (source_http_status IS NULL OR source_http_status BETWEEN 100 AND 599) "
            "AND status IN "
            "('queued', 'running', 'verified', 'published', 'unchanged', 'failed', 'superseded') "
            "AND lease_owner = btrim(lease_owner) AND lease_owner <> '' "
            "AND started_at <= heartbeat_at AND heartbeat_at < lease_expires_at "
            "AND ((status IN ('queued', 'running', 'verified') AND finished_at IS NULL) "
            "OR (status IN ('published', 'unchanged', 'failed', 'superseded') "
            "AND finished_at IS NOT NULL))",
            name="hospital_price_import_attempt_state_check",
        ),
    )
    __my_index_elements__ = ["attempt_id"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("hospital_id", "started_at DESC"),
            "name": "hospital_price_import_attempt_hospital_started_idx",
        },
    ]

    attempt_id = Column(String(64), nullable=False)
    hospital_id = Column(String(64), nullable=False)
    locator_id = Column(String(64), nullable=False)
    locator_observation_id = Column(String(64), nullable=False)
    registry_version = Column(Integer, nullable=False)
    requested_source_url = Column(TEXT, nullable=False)
    final_source_url = Column(TEXT)
    source_http_status = Column(Integer)
    expected_generation = Column(BigInteger, nullable=False)
    status = Column(String(16), nullable=False)
    content_sha256 = Column(String(64))
    version_id = Column(String(64))
    started_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )
    lease_owner = Column(String(128), nullable=False)
    heartbeat_at = Column(TIMESTAMP(timezone=True), nullable=False)
    lease_expires_at = Column(TIMESTAMP(timezone=True), nullable=False)
    finished_at = Column(TIMESTAMP(timezone=True))
    error_code = Column(String(64))
    error_detail = Column(TEXT)


class HospitalPriceCurrent(Base, JSONOutputMixin):
    """Last-known-good pointer and current per-hospital serving counts."""

    __tablename__ = "hospital_price_current"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("hospital_id"),
        ForeignKeyConstraint(
            ["hospital_id"],
            [_reference("hospital_price_hospital", "hospital_id")],
            name="hospital_price_current_hospital_fkey",
        ),
        ForeignKeyConstraint(
            ["version_id", "hospital_id"],
            [
                _reference("hospital_price_version_hospital", "version_id"),
                _reference("hospital_price_version_hospital", "hospital_id"),
            ],
            name="hospital_price_current_version_hospital_fkey",
        ),
        ForeignKeyConstraint(
            ["hospital_id", "version_id", "published_attempt_id"],
            [
                _reference("hospital_price_import_attempt", "hospital_id"),
                _reference("hospital_price_import_attempt", "version_id"),
                _reference("hospital_price_import_attempt", "attempt_id"),
            ],
            name="hospital_price_current_published_attempt_fkey",
        ),
        ForeignKeyConstraint(
            ["hospital_id", "latest_attempt_id"],
            [
                _reference("hospital_price_import_attempt", "hospital_id"),
                _reference("hospital_price_import_attempt", "attempt_id"),
            ],
            name="hospital_price_current_latest_attempt_fkey",
        ),
        CheckConstraint(
            "generation >= 0 AND service_count >= 0 AND charge_count >= 0 "
            "AND payer_charge_count >= 0 AND npi_count >= 0 "
            "AND tax_identity_count >= 0 AND "
            "((version_id IS NULL AND generation = 0 "
            "AND published_attempt_id IS NULL AND last_success_at IS NULL) OR "
            "(version_id IS NOT NULL AND generation > 0 "
            "AND published_attempt_id IS NOT NULL AND last_success_at IS NOT NULL))",
            name="hospital_price_current_state_check",
        ),
    )
    __my_index_elements__ = ["hospital_id"]

    hospital_id = Column(String(64), nullable=False)
    version_id = Column(String(64))
    generation = Column(BigInteger, nullable=False, server_default=text("0"))
    published_attempt_id = Column(String(64))
    latest_attempt_id = Column(String(64))
    service_count = Column(BigInteger, nullable=False, server_default=text("0"))
    charge_count = Column(BigInteger, nullable=False, server_default=text("0"))
    payer_charge_count = Column(BigInteger, nullable=False, server_default=text("0"))
    npi_count = Column(Integer, nullable=False, server_default=text("0"))
    tax_identity_count = Column(Integer, nullable=False, server_default=text("0"))
    last_success_at = Column(TIMESTAMP(timezone=True))
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class HospitalPriceHospitalNPI(Base, JSONOutputMixin):
    """Source-ordered file-level NPI evidence for one hospital version."""

    __tablename__ = "hospital_price_hospital_npi"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint("hospital_id", "version_id", "source_ordinal"),
        ForeignKeyConstraint(
            ["version_id", "hospital_id"],
            [
                _reference("hospital_price_version_hospital", "version_id"),
                _reference("hospital_price_version_hospital", "hospital_id"),
            ],
            name="hospital_price_hospital_npi_version_hospital_fkey",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "npi ~ '^[0-9]{10}$' AND source_ordinal >= 0 "
            "AND source_kind = 'mrf_header_file'",
            name="hospital_price_hospital_npi_shape_check",
        ),
    )
    __my_index_elements__ = ["hospital_id", "version_id", "source_ordinal"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi", "hospital_id"), "name": "hospital_price_hospital_npi_lookup_idx"},
    ]

    hospital_id = Column(String(64), nullable=False)
    version_id = Column(String(64), nullable=False)
    source_ordinal = Column(Integer, nullable=False)
    npi = Column(String(10), nullable=False)
    source_kind = Column(String(32), nullable=False)


class HospitalPriceHospitalTaxIdentity(Base, JSONOutputMixin):
    """Exact public tax identity plus the acquisition that established it."""

    __tablename__ = "hospital_price_hospital_tax_identity"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "hospital_id",
            "version_id",
            "tin_type",
            "tin_value",
            "source_kind",
            "source_ordinal",
        ),
        ForeignKeyConstraint(
            ["version_id", "hospital_id"],
            [
                _reference("hospital_price_version_hospital", "version_id"),
                _reference("hospital_price_version_hospital", "hospital_id"),
            ],
            name="hospital_price_hospital_tax_version_hospital_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["hospital_id", "version_id", "attempt_id"],
            [
                _reference("hospital_price_import_attempt", "hospital_id"),
                _reference("hospital_price_import_attempt", "version_id"),
                _reference("hospital_price_import_attempt", "attempt_id"),
            ],
            name="hospital_price_hospital_tax_attempt_fkey",
        ),
        CheckConstraint(
            "tin_type = btrim(tin_type) AND tin_type <> '' "
            "AND tin_value = btrim(tin_value) AND tin_value <> '' "
            "AND source_kind = btrim(source_kind) AND source_kind <> '' "
            "AND source_ordinal >= 0",
            name="hospital_price_hospital_tax_shape_check",
        ),
    )
    __my_index_elements__ = [
        "hospital_id",
        "version_id",
        "tin_type",
        "tin_value",
        "source_kind",
        "source_ordinal",
    ]
    __my_additional_indexes__ = [
        {"index_elements": ("tin_type", "tin_value", "hospital_id"), "name": "hospital_price_hospital_tax_lookup_idx"},
    ]

    hospital_id = Column(String(64), nullable=False)
    version_id = Column(String(64), nullable=False)
    attempt_id = Column(String(64), nullable=False)
    tin_type = Column(String(16), nullable=False)
    tin_value = Column(String(64), nullable=False)
    source_kind = Column(String(32), nullable=False)
    source_ordinal = Column(Integer, nullable=False)
