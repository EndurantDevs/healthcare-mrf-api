# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration-owned immutable payload roots; publication uses prepared cohorts."""

from sqlalchemy import Boolean, CheckConstraint, Column, DateTime, Integer, PrimaryKeyConstraint, String, text
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base
from db.json_mixin import JSONOutputMixin
from db.plan_release_storage import BINDING_CHECKS, PIN_CHECKS, plan_release_schema

__all__ = ("PlanReleaseSnapshotBinding", "PlanReleaseSnapshotPin")


class PlanReleaseSnapshotBinding(Base, JSONOutputMixin):
    """One immutable ordinal member of an exact revision's binding set."""

    __tablename__ = "plan_release_snapshot_binding"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("serving_revision_id", "role", "binding_ordinal"),
        *(CheckConstraint(expression, name=name) for name, expression in BINDING_CHECKS.items()),
        {"schema": plan_release_schema(), "postgresql_partition_by": "LIST (serving_revision_id)"},
    )
    __my_index_elements__ = ["serving_revision_id", "role", "binding_ordinal"]
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "plan_release_snapshot_binding_snapshot_idx"},
    ]

    serving_revision_id = Column(String(64), nullable=False)
    binding_ordinal = Column(Integer, nullable=False)
    snapshot_id = Column(String(128), nullable=False)
    source_key = Column(String(128), nullable=False)
    plan_id = Column(String(128), nullable=False)
    plan_market_type = Column(String(64))
    role = Column(String(32), nullable=False)
    required = Column(Boolean, nullable=False, server_default=text("true"))
    metadata_json = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("transaction_timestamp()"))


class PlanReleaseSnapshotPin(Base, JSONOutputMixin):
    """Plan-owned retention only; unrelated shared pin owners retain their store."""

    __tablename__ = "plan_release_snapshot_pin"
    __main_table__ = __tablename__
    __runtime_schema_sync__ = False
    __table_args__ = (
        PrimaryKeyConstraint("serving_revision_id", "snapshot_id"),
        *(CheckConstraint(expression, name=name) for name, expression in PIN_CHECKS.items()),
        {"schema": plan_release_schema(), "postgresql_partition_by": "LIST (serving_revision_id)"},
    )
    __my_index_elements__ = ["serving_revision_id", "snapshot_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "plan_release_snapshot_pin_snapshot_idx"},
    ]

    serving_revision_id = Column(String(64), nullable=False)
    snapshot_id = Column(String(128), nullable=False)
    reason = Column(String(256))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("transaction_timestamp()"))
