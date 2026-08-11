# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable rooted Provider Directory graph resource and edge witnesses."""

from __future__ import annotations

import os

from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import TIMESTAMP
from sqlalchemy import text

from db.connection import Base
from db.json_mixin import JSONOutputMixin


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


_SCHEMA = _schema()
_WORK = "provider_directory_rooted_graph_work"
_RESOURCE = "provider_directory_rooted_graph_resource"
_EDGE = "provider_directory_rooted_graph_edge"


def _reference(table_name: str, column_name: str) -> str:
    return f"{_SCHEMA}.{table_name}.{column_name}"


def _table_args(*constraints):
    return (*constraints, {"schema": _SCHEMA, "extend_existing": True})


def _timestamp_column():
    return Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("transaction_timestamp()"),
    )


class ProviderDirectoryRootedGraphResource(Base, JSONOutputMixin):
    """One immutable canonical payload for one exact query attempt."""

    __tablename__ = _RESOURCE
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "acquisition_id",
            "query_id",
            "attempt",
            "resource_type",
            "resource_id",
            name="provider_directory_rooted_graph_resource_pkey",
        ),
        ForeignKeyConstraint(
            ["acquisition_id", "scope_id", "query_id"],
            [
                _reference(_WORK, "acquisition_id"),
                _reference(_WORK, "scope_id"),
                _reference(_WORK, "query_id"),
            ],
            name="provider_directory_rooted_graph_resource_work_fkey",
        ),
        CheckConstraint(
            "attempt > 0 AND resource_type IN ('PractitionerRole', "
            "'OrganizationAffiliation', 'Organization', 'Location', "
            "'HealthcareService', 'InsurancePlan', 'Endpoint') AND "
            "resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "payload_sha256 ~ '^[0-9a-f]{64}$' AND closure_scope IN "
            "('root', 'plan', 'census') AND octet_length(payload_json_text) "
            "BETWEEN 2 AND 1048576",
            name="provider_directory_rooted_graph_resource_value_check",
        ),
    )
    __my_index_elements__ = [
        "acquisition_id",
        "query_id",
        "attempt",
        "resource_type",
        "resource_id",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "acquisition_id",
                "closure_scope",
                "resource_type",
                "resource_id",
            ),
            "name": "provider_directory_rooted_graph_resource_closure_idx",
        }
    ]

    acquisition_id = Column(String(54), nullable=False)
    scope_id = Column(String(54), nullable=False)
    query_id = Column(String(54), nullable=False)
    attempt = Column(Integer, nullable=False)
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(64), nullable=False)
    payload_sha256 = Column(String(64), nullable=False)
    payload_json_text = Column(Text, nullable=False)
    closure_scope = Column(String(16), nullable=False)
    created_at = _timestamp_column()


class ProviderDirectoryRootedGraphEdge(Base, JSONOutputMixin):
    """One immutable local-reference witness from a retained payload."""

    __tablename__ = _EDGE
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        PrimaryKeyConstraint(
            "acquisition_id",
            "query_id",
            "attempt",
            "edge_sha256",
            name="provider_directory_rooted_graph_edge_pkey",
        ),
        ForeignKeyConstraint(
            [
                "acquisition_id",
                "query_id",
                "attempt",
                "source_resource_type",
                "source_resource_id",
            ],
            [
                _reference(_RESOURCE, "acquisition_id"),
                _reference(_RESOURCE, "query_id"),
                _reference(_RESOURCE, "attempt"),
                _reference(_RESOURCE, "resource_type"),
                _reference(_RESOURCE, "resource_id"),
            ],
            name="provider_directory_rooted_graph_edge_resource_fkey",
        ),
        CheckConstraint(
            "attempt > 0 AND source_resource_type IN ('PractitionerRole', "
            "'OrganizationAffiliation', 'Organization', 'Location', "
            "'HealthcareService', 'InsurancePlan', 'Endpoint') AND "
            "source_resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "field_path ~ '^([A-Za-z][A-Za-z0-9]*(\\[[0-9]+\\])?|"
            "extension\\[[0-9]+\\](\\.extension\\[[0-9]+\\]){0,5}"
            "\\.valueReference)$' AND "
            "target_resource_type IN ('Practitioner', 'PractitionerRole', "
            "'OrganizationAffiliation', 'Organization', 'Location', "
            "'HealthcareService', 'InsurancePlan', 'Endpoint') AND "
            "target_resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "edge_sha256 ~ '^[0-9a-f]{64}$' AND closure_scope IN "
            "('root', 'plan', 'census')",
            name="provider_directory_rooted_graph_edge_value_check",
        ),
    )
    __my_index_elements__ = [
        "acquisition_id",
        "query_id",
        "attempt",
        "edge_sha256",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": (
                "acquisition_id",
                "closure_scope",
                "target_resource_type",
                "target_resource_id",
            ),
            "name": "provider_directory_rooted_graph_edge_target_idx",
        }
    ]

    acquisition_id = Column(String(54), nullable=False)
    scope_id = Column(String(54), nullable=False)
    query_id = Column(String(54), nullable=False)
    attempt = Column(Integer, nullable=False)
    source_resource_type = Column(String(64), nullable=False)
    source_resource_id = Column(String(64), nullable=False)
    field_path = Column(String(128), nullable=False)
    target_resource_type = Column(String(64), nullable=False)
    target_resource_id = Column(String(64), nullable=False)
    edge_sha256 = Column(String(64), nullable=False)
    closure_scope = Column(String(16), nullable=False)
    created_at = _timestamp_column()


__all__ = (
    "ProviderDirectoryRootedGraphEdge",
    "ProviderDirectoryRootedGraphResource",
)
